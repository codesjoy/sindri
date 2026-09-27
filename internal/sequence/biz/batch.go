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
	"fmt"
	"math"
	"time"

	"github.com/codesjoy/pkg/basic/xerror"
	"github.com/codesjoy/sindri/gen/go/sequence/reason"
)

const (
	// MaxBatchKeys is the maximum number of keys accepted by one batch request.
	MaxBatchKeys = 1000
	// MaxIDsPerKey is the maximum number of IDs one key may request at once.
	MaxIDsPerKey = 10000
	// MaxIDsPerRequest is the maximum number of IDs accepted by one batch request.
	MaxIDsPerRequest = 100000
)

// SequenceRequest describes one key allocation in a batch.
type SequenceRequest struct {
	Key   string
	Count uint32
}

// SequenceAllocation is a contiguous allocation for one key.
type SequenceAllocation struct {
	ID    int64
	Count uint32
}

// FetchNextN returns a contiguous block of IDs for a locally owned key.
func (obj *Allocator) FetchNextN(
	ctx context.Context,
	key string,
	count uint32,
) (SequenceAllocation, error) {
	results, err := obj.FetchNextBatch(ctx, []SequenceRequest{{Key: key, Count: count}})
	if err != nil {
		return SequenceAllocation{}, err
	}
	return results[0], nil
}

// FetchNextBatch allocates IDs for keys owned by this allocator. The response
// preserves request order and is returned only when every allocation succeeds.
func (obj *Allocator) FetchNextBatch(
	ctx context.Context,
	requests []SequenceRequest,
) ([]SequenceAllocation, error) {
	normalized, err := normalizeRequests(requests)
	if err != nil {
		return nil, err
	}

	obj.slotsMu.RLock()
	defer obj.slotsMu.RUnlock()
	if obj.Paused() {
		return nil, xerror.NewWithReason(
			reason.Reason_SEQUENCE_ALLOCATOR_PAUSED,
			"allocator is already paused",
			nil,
		)
	}

	states := make([]*keyState, len(normalized))
	missing := 0
	for index, request := range normalized {
		slot, ok := obj.slots[SlotForKey(request.Key)]
		if !ok {
			return nil, xerror.NewWithReason(
				reason.Reason_SEQUENCE_SLOT_NOT_OWNER,
				"slot not found",
				nil,
			)
		}
		state, ok := slot.Load(request.Key)
		if !ok {
			missing++
		}
		states[index] = state
	}
	if missing > 0 && obj.memoryHighWatermarkReached() {
		obj.admissionRejected.Add(1)
		return nil, xerror.NewWithReason(
			reason.Reason_SEQUENCE_CAPACITY_EXHAUSTED,
			"sequence allocator memory capacity is exhausted",
			nil,
		)
	}

	for index, request := range normalized {
		if states[index] != nil {
			continue
		}
		slot := obj.slots[SlotForKey(request.Key)]
		state, err := obj.loadOrCreateState(request.Key, slot)
		if err != nil {
			return nil, err
		}
		states[index] = state
	}

	if err := obj.prepareBatchRanges(ctx, normalized, states); err != nil {
		return nil, err
	}

	results := make([]SequenceAllocation, len(normalized))
	for index, request := range normalized {
		allocation, err := states[index].allocateBlock(
			ctx,
			obj.store,
			request.Key,
			int64(request.Count),
			obj.cfg,
			obj.now,
		)
		if err != nil {
			return nil, err
		}
		results[index] = allocation
	}
	return results, nil
}

func normalizeRequests(requests []SequenceRequest) ([]SequenceRequest, error) {
	if len(requests) == 0 {
		return nil, fmt.Errorf("sequence allocator: requests must not be empty")
	}
	if len(requests) > MaxBatchKeys {
		return nil, fmt.Errorf(
			"sequence allocator: at most %d keys are allowed",
			MaxBatchKeys,
		)
	}

	normalized := make([]SequenceRequest, len(requests))
	seen := make(map[string]struct{}, len(requests))
	var total int64
	for index, request := range requests {
		if request.Key == "" || len(request.Key) > 256 {
			return nil, fmt.Errorf("sequence allocator: key must contain 1..256 bytes")
		}
		if _, exists := seen[request.Key]; exists {
			return nil, fmt.Errorf("sequence allocator: duplicate key %q", request.Key)
		}
		seen[request.Key] = struct{}{}
		count := request.Count
		if count == 0 {
			count = 1
		}
		if count > MaxIDsPerKey {
			return nil, fmt.Errorf(
				"sequence allocator: count for %q must not exceed %d",
				request.Key,
				MaxIDsPerKey,
			)
		}
		total += int64(count)
		if total > MaxIDsPerRequest {
			return nil, fmt.Errorf(
				"sequence allocator: at most %d IDs are allowed per request",
				MaxIDsPerRequest,
			)
		}
		normalized[index] = SequenceRequest{Key: request.Key, Count: count}
	}
	return normalized, nil
}

type pendingReservation struct {
	state *keyState
	fetch *rangeFetch
	key   string
	step  int64
}

func (obj *Allocator) prepareBatchRanges(
	ctx context.Context,
	requests []SequenceRequest,
	states []*keyState,
) error {
	pending := make([]pendingReservation, 0)
	for index, request := range requests {
		state := states[index]
		fetch, step := state.prepareBatchRange(
			int64(request.Count),
			obj.cfg,
			obj.now(),
		)
		if fetch == nil {
			continue
		}
		pending = append(pending, pendingReservation{
			state: state,
			fetch: fetch,
			key:   request.Key,
			step:  step,
		})
	}
	if len(pending) == 0 {
		return nil
	}

	reservationRequests := make([]ReservationRequest, len(pending))
	for index, item := range pending {
		reservationRequests[index] = ReservationRequest{Key: item.key, Step: item.step}
	}
	reserveStarted := time.Now()
	reserved, err := reserveRanges(ctx, obj.store, reservationRequests)
	if len(reservationRequests) == 1 {
		obj.observeReserve(reserveStarted, err)
	}
	if err != nil {
		for _, item := range pending {
			item.state.completeFetch(
				item.fetch,
				SequenceRange{},
				err,
				obj.cfg,
				obj.now(),
				item.key,
			)
		}
		return err
	}
	for index, item := range pending {
		item.state.completeFetch(
			item.fetch,
			reserved[index],
			nil,
			obj.cfg,
			obj.now(),
			item.key,
		)
	}
	return nil
}

func (k *keyState) prepareBatchRange(
	count int64,
	cfg AllocatorConfig,
	now time.Time,
) (*rangeFetch, int64) {
	k.mu.Lock()
	defer k.mu.Unlock()

	if k.hasReadyBlockLocked(count) || k.fetch != nil {
		return nil, 0
	}
	if k.standby != nil {
		k.standby = nil
	}

	step := cfg.DefaultStep
	if k.initialized.Load() {
		activeSize := k.end.Load() - k.start.Load() + 1
		step = k.nextStepLocked(activeSize, cfg)
	}
	if step < count {
		step = count
	}
	fetch := &rangeFetch{done: make(chan struct{})}
	k.fetch = fetch
	k.clearRetryLocked()
	return fetch, step
}

func (k *keyState) hasReadyBlockLocked(count int64) bool {
	if k.initialized.Load() {
		generation := k.generation.Load()
		if generation%2 == 0 {
			start := k.start.Load()
			next := k.next.Load()
			end := k.end.Load()
			if start <= next && next <= end && end-next >= count {
				return true
			}
		}
	}
	return k.standby != nil && k.standby.End-k.standby.Start+1 >= count
}

func (k *keyState) allocateBlock(
	ctx context.Context,
	store SequenceRepo,
	key string,
	count int64,
	cfg AllocatorConfig,
	now func() time.Time,
) (SequenceAllocation, error) {
	if count <= 1 {
		id, err := k.allocate(ctx, store, key, cfg, now)
		if err != nil {
			return SequenceAllocation{}, err
		}
		return SequenceAllocation{ID: id, Count: 1}, nil
	}

	if id, generation, ok := k.tryAllocateBlock(count); ok {
		last := id + count - 1
		k.afterAllocate(store, key, cfg, now, last, count, generation)
		return SequenceAllocation{ID: id, Count: uint32(count)}, nil
	}

	id, generation, err := k.allocateBlockSlow(ctx, store, key, count, cfg, now)
	if err != nil {
		return SequenceAllocation{}, err
	}
	last := id + count - 1
	k.afterAllocate(store, key, cfg, now, last, count, generation)
	return SequenceAllocation{ID: id, Count: uint32(count)}, nil
}

func (k *keyState) tryAllocateBlock(count int64) (int64, uint64, bool) {
	if !k.initialized.Load() {
		return 0, 0, false
	}
	generation := k.generation.Load()
	if generation%2 != 0 {
		return 0, 0, false
	}
	current := k.next.Load()
	start := k.start.Load()
	end := k.end.Load()
	if current < start || current > math.MaxInt64-count || current+count > end {
		return 0, 0, false
	}
	last := current + count
	if !k.next.CompareAndSwap(current, last) {
		return 0, 0, false
	}
	if generation != k.generation.Load() {
		return 0, 0, false
	}
	return current + 1, generation, true
}

func (k *keyState) allocateBlockSlow(
	ctx context.Context,
	store SequenceRepo,
	key string,
	count int64,
	cfg AllocatorConfig,
	now func() time.Time,
) (int64, uint64, error) {
	for {
		k.mu.Lock()
		if id, generation, ok := k.tryAllocateBlock(count); ok {
			k.touch(now())
			k.mu.Unlock()
			return id, generation, nil
		}

		if k.standby != nil {
			size := k.standby.End - k.standby.Start + 1
			if size >= count {
				first, generation := k.activateStandbyLocked(now(), count, cfg)
				k.touch(now())
				k.mu.Unlock()
				return first, generation, nil
			}
			k.standby = nil
		}

		if k.fetch != nil {
			fetch := k.fetch
			k.mu.Unlock()
			select {
			case <-ctx.Done():
				return 0, 0, ctx.Err()
			case <-fetch.done:
				if fetch.err != nil && !fetch.background {
					return 0, 0, fetch.err
				}
				continue
			}
		}

		step := cfg.DefaultStep
		if k.initialized.Load() {
			activeSize := k.end.Load() - k.start.Load() + 1
			step = k.nextStepLocked(activeSize, cfg)
		}
		if step < count {
			step = count
		}
		fetch := &rangeFetch{done: make(chan struct{})}
		k.fetch = fetch
		k.clearRetryLocked()
		k.mu.Unlock()

		started := time.Now()
		reserved, err := reserveRange(ctx, store, key, step)
		k.observeAllocationReserve(started, err)
		k.completeFetch(fetch, reserved, err, cfg, now(), key)
		if err != nil {
			return 0, 0, err
		}
	}
}
