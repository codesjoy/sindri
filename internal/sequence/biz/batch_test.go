// Copyright 2026 Codesjoy
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package biz

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFetchNextNReturnsContiguousBlocks(t *testing.T) {
	allocator := readyAllocatorForKeys(t, "orders")

	first, err := allocator.FetchNextN(context.Background(), "orders", 5)
	require.NoError(t, err)
	assert.Equal(t, SequenceAllocation{ID: 1, Count: 5}, first)

	next, err := allocator.FetchNext(context.Background(), "orders")
	require.NoError(t, err)
	assert.Equal(t, int64(6), next)

	second, err := allocator.FetchNextN(context.Background(), "orders", 3)
	require.NoError(t, err)
	assert.Equal(t, SequenceAllocation{ID: 7, Count: 3}, second)
}

func TestFetchNextNReservesFreshContiguousRange(t *testing.T) {
	allocator := NewAllocator(
		&AllocatorConfig{
			DefaultStep:   10,
			MaxStep:       10,
			PrefetchRatio: 0.99,
		},
		&rangeStore{max: make(map[string]int64)},
		unlimitedMemorySampler,
		nil,
	)
	allocator.Open(1, 0, []uint32{SlotForKey("orders")})
	allocator.ApplyRoute(0)

	first, err := allocator.FetchNextN(context.Background(), "orders", 6)
	require.NoError(t, err)
	assert.Equal(t, SequenceAllocation{ID: 1, Count: 6}, first)

	second, err := allocator.FetchNextN(context.Background(), "orders", 6)
	require.NoError(t, err)
	assert.Equal(t, SequenceAllocation{ID: 11, Count: 6}, second)
}

func TestFetchNextBatchMergesColdReservations(t *testing.T) {
	store := &countingRangeStore{max: make(map[string]int64)}
	keys := distinctSlotKeys(3)
	allocator := NewAllocator(
		&AllocatorConfig{DefaultStep: 10, MaxStep: 100},
		store,
		unlimitedMemorySampler,
		nil,
	)
	slots := make([]uint32, len(keys))
	for index, key := range keys {
		slots[index] = SlotForKey(key)
	}
	allocator.Open(1, 0, slots)
	allocator.ApplyRoute(0)

	requests := []SequenceRequest{
		{Key: keys[0], Count: 2},
		{Key: keys[1], Count: 1},
		{Key: keys[2], Count: 3},
	}
	results, err := allocator.FetchNextBatch(context.Background(), requests)
	require.NoError(t, err)
	assert.Equal(t, []SequenceAllocation{
		{ID: 1, Count: 2},
		{ID: 1, Count: 1},
		{ID: 1, Count: 3},
	}, results)
	assert.Equal(t, 1, store.calls())

	results, err = allocator.FetchNextBatch(context.Background(), requests)
	require.NoError(t, err)
	assert.Equal(t, []SequenceAllocation{
		{ID: 3, Count: 2},
		{ID: 2, Count: 1},
		{ID: 4, Count: 3},
	}, results)
	assert.Equal(t, 1, store.calls())
}

func TestFetchNextBatchRejectsDuplicateKeys(t *testing.T) {
	allocator := readyAllocatorForKeys(t, "orders")
	_, err := allocator.FetchNextBatch(context.Background(), []SequenceRequest{
		{Key: "orders"},
		{Key: "orders"},
	})
	require.Error(t, err)
	assert.Zero(t, allocator.Stats().CachedKeys)
}

func TestFetchNextBatchRejectsOversizedRequests(t *testing.T) {
	allocator := readyAllocatorForKeys(t, "orders")

	manyKeys := make([]SequenceRequest, MaxBatchKeys+1)
	for index := range manyKeys {
		manyKeys[index] = SequenceRequest{Key: fmt.Sprintf("limit-key-%d", index)}
	}

	totalKeys := make([]SequenceRequest, MaxIDsPerRequest/MaxIDsPerKey+1)
	for index := range totalKeys {
		totalKeys[index] = SequenceRequest{
			Key:   fmt.Sprintf("total-key-%d", index),
			Count: MaxIDsPerKey,
		}
	}

	longKey := strings.Repeat("k", 257)
	tests := []struct {
		name     string
		requests []SequenceRequest
	}{
		{name: "empty"},
		{name: "empty key", requests: []SequenceRequest{{Key: ""}}},
		{name: "key too long", requests: []SequenceRequest{{
			Key: longKey,
		}}},
		{name: "count above per-key limit", requests: []SequenceRequest{{
			Key:   "orders",
			Count: MaxIDsPerKey + 1,
		}}},
		{name: "keys above batch limit", requests: manyKeys},
		{name: "ids above request limit", requests: totalKeys},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := allocator.FetchNextBatch(context.Background(), test.requests)
			require.Error(t, err)
			assert.Zero(t, allocator.Stats().CachedKeys)
		})
	}
}

func TestFetchNextNPropagatesReservationOverflow(t *testing.T) {
	store := &overflowRangeStore{max: map[string]int64{
		"orders": math.MaxInt64 - 2,
	}}
	allocator := NewAllocator(
		&AllocatorConfig{DefaultStep: 10, MaxStep: 10},
		store,
		unlimitedMemorySampler,
		nil,
	)
	allocator.Open(1, 0, []uint32{SlotForKey("orders")})
	allocator.ApplyRoute(0)

	_, err := allocator.FetchNextN(context.Background(), "orders", 3)
	require.ErrorIs(t, err, errRangeOverflow)

	_, err = allocator.FetchNext(context.Background(), "orders")
	require.ErrorIs(t, err, errRangeOverflow)
}

var errRangeOverflow = errors.New("sequence maximum overflow")

type overflowRangeStore struct {
	mu  sync.Mutex
	max map[string]int64
}

func (s *overflowRangeStore) ReserveRanges(
	_ context.Context,
	requests []ReservationRequest,
) ([]SequenceRange, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, request := range requests {
		if s.max[request.Key] > math.MaxInt64-request.Step {
			return nil, fmt.Errorf("%w: %q", errRangeOverflow, request.Key)
		}
	}
	reserved := make([]SequenceRange, len(requests))
	for index, request := range requests {
		start := s.max[request.Key] + 1
		s.max[request.Key] += request.Step
		reserved[index] = SequenceRange{Start: start, End: s.max[request.Key]}
	}
	return reserved, nil
}

func TestFetchNextNConcurrentBlocksDoNotOverlap(t *testing.T) {
	allocator := readyAllocatorForKeys(t, "orders")
	const (
		workers = 8
		count   = 7
	)

	results := make(chan SequenceAllocation, workers)
	errs := make(chan error, workers)
	var wait sync.WaitGroup
	for range workers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			allocation, err := allocator.FetchNextN(context.Background(), "orders", count)
			if err != nil {
				errs <- err
				return
			}
			results <- allocation
		}()
	}
	wait.Wait()
	close(results)
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	seen := make(map[int64]struct{}, workers*count)
	for allocation := range results {
		for id := allocation.ID; id < allocation.ID+int64(allocation.Count); id++ {
			_, duplicate := seen[id]
			assert.False(t, duplicate, "duplicate ID %d", id)
			seen[id] = struct{}{}
		}
	}
	assert.Len(t, seen, workers*count)
}

func TestConcurrentSingleAndBlockAllocationsDoNotOverlap(t *testing.T) {
	allocator := readyAllocatorForKeys(t, "orders")
	const (
		singleWorkers = 8
		blockWorkers  = 8
		blockSize     = 5
	)

	ids := make(chan int64, singleWorkers+blockWorkers*blockSize)
	errs := make(chan error, singleWorkers+blockWorkers)
	var wait sync.WaitGroup
	for range singleWorkers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			id, err := allocator.FetchNext(context.Background(), "orders")
			if err != nil {
				errs <- err
				return
			}
			ids <- id
		}()
	}
	for range blockWorkers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			allocation, err := allocator.FetchNextN(
				context.Background(),
				"orders",
				blockSize,
			)
			if err != nil {
				errs <- err
				return
			}
			for id := allocation.ID; id < allocation.ID+int64(allocation.Count); id++ {
				ids <- id
			}
		}()
	}
	wait.Wait()
	close(ids)
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	seen := make(map[int64]struct{}, singleWorkers+blockWorkers*blockSize)
	for id := range ids {
		_, duplicate := seen[id]
		assert.False(t, duplicate, "duplicate ID %d", id)
		seen[id] = struct{}{}
	}
	assert.Len(t, seen, singleWorkers+blockWorkers*blockSize)
}

type countingRangeStore struct {
	mu    sync.Mutex
	max   map[string]int64
	count int
}

func (s *countingRangeStore) ReserveRanges(
	_ context.Context,
	requests []ReservationRequest,
) ([]SequenceRange, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.count++
	reserved := make([]SequenceRange, len(requests))
	for index, request := range requests {
		start := s.max[request.Key] + 1
		s.max[request.Key] += request.Step
		reserved[index] = SequenceRange{Start: start, End: s.max[request.Key]}
	}
	return reserved, nil
}

func (s *countingRangeStore) calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.count
}

func TestFetchNextBatchHonorsContextCancellationWhileWaiting(t *testing.T) {
	key := "orders"
	store := &blockingRangeStore{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	allocator := readyAllocatorForCleanup(
		t,
		store,
		&fakeClock{now: time.Unix(1, 0)},
		key,
	)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := allocator.FetchNextBatch(ctx, []SequenceRequest{{Key: key, Count: 2}})
		done <- err
	}()
	<-store.started
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}
