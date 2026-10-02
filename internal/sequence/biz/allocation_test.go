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

	"github.com/codesjoy/pkg/basic/xerror"
	"github.com/codesjoy/sindri/gen/go/sequence/reason"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKeyStateContinuesFromPersistedWatermark(t *testing.T) {
	store := &rangeStore{max: map[string]int64{"orders": 100}}
	state := &keyState{}

	got, err := state.allocate(
		context.Background(),
		reservationScope{store: store},
		"orders",
		testAllocatorConfig(),
		time.Now,
	)
	if err != nil {
		t.Fatal(err)
	}
	if got != 101 {
		t.Fatalf("first id after handoff = %d, want 101", got)
	}
}

func TestKeyStateAllocatesAcrossRanges(t *testing.T) {
	store := &rangeStore{max: map[string]int64{}}
	state := &keyState{}

	for want := int64(1); want <= 25; want++ {
		got, err := state.allocate(
			context.Background(),
			reservationScope{store: store},
			"orders",
			testAllocatorConfig(),
			time.Now,
		)
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("allocated id = %d, want %d", got, want)
		}
	}
}

func TestKeyStateConcurrentInitializationIsUnique(t *testing.T) {
	store := &rangeStore{max: map[string]int64{"orders": 100}}
	state := &keyState{}

	const workers = 128
	values := make(chan int64, workers)
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cfg := testAllocatorConfig()
			cfg.DefaultStep = 8
			value, err := state.allocate(
				context.Background(),
				reservationScope{store: store},
				"orders",
				cfg,
				time.Now,
			)
			if err != nil {
				errs <- err
				return
			}
			values <- value
		}()
	}
	wg.Wait()
	close(values)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	seen := make(map[int64]struct{}, workers)
	for value := range values {
		if value <= 100 {
			t.Fatalf("allocated stale id %d", value)
		}
		if _, exists := seen[value]; exists {
			t.Fatalf("allocated duplicate id %d", value)
		}
		seen[value] = struct{}{}
	}
	if len(seen) != workers {
		t.Fatalf("allocated %d ids, want %d", len(seen), workers)
	}
}

type invalidRangeStore struct{}

func (invalidRangeStore) ReserveRanges(
	context.Context,
	ReservationAuthority,
	[]ReservationRequest,
) ([]SequenceRange, error) {
	return []SequenceRange{{Start: 10, End: 10}}, nil
}

func TestKeyStateRejectsInvalidReservedRange(t *testing.T) {
	state := &keyState{}
	if _, err := state.allocate(
		context.Background(),
		reservationScope{store: invalidRangeStore{}},
		"orders",
		testAllocatorConfig(),
		time.Now,
	); err == nil {
		t.Fatal("expected invalid reserved range error")
	}
}

// mismatchedRangeStore answers a two-key request with one range, which is an
// answer that cannot be attributed to the keys that asked for it.
type mismatchedRangeStore struct{}

func (mismatchedRangeStore) ReserveRanges(
	context.Context,
	ReservationAuthority,
	[]ReservationRequest,
) ([]SequenceRange, error) {
	return []SequenceRange{{Start: 1, End: 10}}, nil
}

// TestReserveRangesRefusesAnAnswerThatDoesNotLineUp covers the two ways the
// authority's answer can be unusable while still being an answer: a set that
// does not line up with the request, and a range that is not a range.
//
// Both must reach the caller as the argument reason. It is the one reason in the
// contract that tells a caller not to retry unchanged, and the alternative --
// the bare error -- arrives with no reason and no retry class at all, which the
// caller cannot tell from its own mistake. Neither is a fencing condition, so
// neither may be dressed as one: that would tell the caller to refresh a
// directory that is not stale.
func TestReserveRangesRefusesAnAnswerThatDoesNotLineUp(t *testing.T) {
	for name, testCase := range map[string]struct {
		store    SequenceRepo
		requests []ReservationRequest
		want     string
	}{
		"count mismatch": {
			store: mismatchedRangeStore{},
			requests: []ReservationRequest{
				{Key: "orders", Step: 10},
				{Key: "invoices", Step: 10},
			},
			want: "got 1 ranges for 2 requests",
		},
		"invalid range": {
			store:    invalidRangeStore{},
			requests: []ReservationRequest{{Key: "orders", Step: 10}},
			want:     "invalid range",
		},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := reserveRanges(
				context.Background(),
				reservationScope{store: testCase.store},
				testCase.requests,
			)
			require.Error(t, err)
			assert.True(
				t,
				xerror.IsReason(err, reason.Reason_SEQUENCE_INVALID_ARGUMENT),
				"want %s, got %v",
				reason.Reason_SEQUENCE_INVALID_ARGUMENT,
				err,
			)
			assert.Contains(t, err.Error(), testCase.want, "the detail must survive")
			for _, fencing := range []reason.Reason{
				reason.Reason_SEQUENCE_SLOT_NOT_OWNER,
				reason.Reason_SEQUENCE_LEASE_EXPIRED,
				reason.Reason_SEQUENCE_COMMIT_UNCERTAIN,
				reason.Reason_SEQUENCE_STORAGE_UNAVAILABLE,
			} {
				assert.False(
					t,
					xerror.IsReason(err, fencing),
					"must not be reported as %s",
					fencing,
				)
			}
		})
	}
}

type recordingRangeStore struct {
	mu      sync.Mutex
	max     int64
	steps   []int64
	started chan int64
	release chan struct{}
	err     error
}

type blockingRangeStore struct {
	started chan struct{}
	release chan struct{}
}

func (s *blockingRangeStore) ReserveRanges(
	ctx context.Context,
	_ ReservationAuthority,
	requests []ReservationRequest,
) ([]SequenceRange, error) {
	select {
	case <-s.started:
	default:
		close(s.started)
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.release:
		reserved := make([]SequenceRange, len(requests))
		var start int64 = 1
		for index, request := range requests {
			reserved[index] = SequenceRange{Start: start, End: start + request.Step - 1}
			start += request.Step
		}
		return reserved, nil
	}
}

func (s *recordingRangeStore) ReserveRanges(
	ctx context.Context,
	_ ReservationAuthority,
	requests []ReservationRequest,
) ([]SequenceRange, error) {
	s.mu.Lock()
	call := len(s.steps) + 1
	for _, request := range requests {
		s.steps = append(s.steps, request.Step)
	}
	if call == 1 {
		reserved := s.reserveLocked(requests)
		s.mu.Unlock()
		return reserved, nil
	}
	s.mu.Unlock()
	if s.started != nil {
		for _, request := range requests {
			select {
			case s.started <- request.Step:
			default:
			}
		}
	}
	if s.release != nil {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-s.release:
		}
	}
	if s.err != nil {
		return nil, s.err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reserveLocked(requests), nil
}

func (s *recordingRangeStore) reserveLocked(
	requests []ReservationRequest,
) []SequenceRange {
	reserved := make([]SequenceRange, len(requests))
	for index, request := range requests {
		start := s.max + 1
		s.max += request.Step
		reserved[index] = SequenceRange{Start: start, End: s.max}
	}
	return reserved
}

func (s *recordingRangeStore) recordedSteps() []int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]int64(nil), s.steps...)
}

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(duration time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(duration)
}

// MonoNow is the monotonic reading the allocator fences on. It moves with Now,
// so a test that advances the wall clock advances the pause measurement too.
func (c *fakeClock) MonoNow() int64 {
	return c.Now().UnixNano()
}

func TestKeyStatePrefetchesOnceAtConfiguredRatio(t *testing.T) {
	cfg := testAllocatorConfig()
	cfg.PrefetchRatio = 0.6
	clock := &fakeClock{now: time.Unix(100, 0)}
	store := &recordingRangeStore{
		started: make(chan int64, 4),
		release: make(chan struct{}),
	}
	state := &keyState{}

	for want := int64(1); want <= 5; want++ {
		got, err := state.allocate(
			context.Background(),
			reservationScope{store: store},
			"orders",
			cfg,
			clock.Now,
		)
		require.NoError(t, err)
		assert.Equal(t, want, got)
	}
	select {
	case <-store.started:
		t.Fatal("prefetch started before configured ratio")
	default:
	}

	got, err := state.allocate(
		context.Background(),
		reservationScope{store: store},
		"orders",
		cfg,
		clock.Now,
	)
	require.NoError(t, err)
	assert.Equal(t, int64(6), got)
	select {
	case step := <-store.started:
		assert.Equal(t, int64(10), step)
	case <-time.After(time.Second):
		t.Fatal("prefetch did not start at configured ratio")
	}

	for range 3 {
		_, err = state.allocate(
			context.Background(),
			reservationScope{store: store},
			"orders",
			cfg,
			clock.Now,
		)
		require.NoError(t, err)
	}
	assert.Equal(t, []int64{10, 10}, store.recordedSteps())
	close(store.release)
}

func TestKeyStateAdjustsStepFromEstimatedExhaustion(t *testing.T) {
	tests := []struct {
		name         string
		elapsed      time.Duration
		activeStep   int64
		maxStep      int64
		wantNextStep int64
	}{
		{
			name: "increase", elapsed: 5 * time.Minute,
			activeStep: 20, maxStep: 100, wantNextStep: 40,
		},
		{
			name: "increase capped", elapsed: 5 * time.Minute,
			activeStep: 80, maxStep: 100, wantNextStep: 100,
		},
		{
			name: "keep", elapsed: 10 * time.Minute,
			activeStep: 20, maxStep: 100, wantNextStep: 20,
		},
		{
			name: "decrease", elapsed: 20 * time.Minute,
			activeStep: 40, maxStep: 100, wantNextStep: 20,
		},
		{
			name: "decrease floored", elapsed: 20 * time.Minute,
			activeStep: 10, maxStep: 100, wantNextStep: 10,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := testAllocatorConfig()
			cfg.MaxStep = test.maxStep
			state := &keyState{activeStep: test.activeStep}
			state.rate.ready = true
			state.rate.rate = 5 / test.elapsed.Seconds()
			got := state.nextStepLocked(10, cfg)
			assert.Equal(t, test.wantNextStep, got)
		})
	}
}

func TestKeyStateWaitsForInflightPrefetchAtExhaustion(t *testing.T) {
	cfg := testAllocatorConfig()
	store := &recordingRangeStore{
		started: make(chan int64, 2),
		release: make(chan struct{}),
	}
	state := &keyState{}
	for range cfg.DefaultStep {
		_, err := state.allocate(
			context.Background(),
			reservationScope{store: store},
			"orders",
			cfg,
			time.Now,
		)
		require.NoError(t, err)
	}
	select {
	case <-store.started:
	case <-time.After(time.Second):
		t.Fatal("prefetch did not start")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := state.allocate(ctx, reservationScope{store: store}, "orders", cfg, time.Now)
	assert.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, []int64{10, 10}, store.recordedSteps())
	close(store.release)

	got, err := state.allocate(
		context.Background(),
		reservationScope{store: store},
		"orders",
		cfg,
		time.Now,
	)
	require.NoError(t, err)
	assert.Equal(t, int64(11), got)
}

func TestKeyStateFallsBackAfterPrefetchFailure(t *testing.T) {
	cfg := testAllocatorConfig()
	// Prefetch only at exhaustion, so the last allocation starts the single
	// background attempt that can overlap the fallback. A bare keyState has no
	// allocator to arm the retry backoff, so at a lower ratio each allocation
	// after the failure would launch another prefetch and the count would race.
	cfg.PrefetchRatio = 0.99
	prefetchErr := errors.New("prefetch failed")
	store := &recordingRangeStore{err: prefetchErr, started: make(chan int64, 2)}
	state := &keyState{}

	for range cfg.DefaultStep {
		_, err := state.allocate(
			context.Background(),
			reservationScope{store: store},
			"orders",
			cfg,
			time.Now,
		)
		require.NoError(t, err)
	}
	select {
	case <-store.started:
	case <-time.After(time.Second):
		t.Fatal("prefetch did not start")
	}
	require.Eventually(t, func() bool {
		state.mu.Lock()
		defer state.mu.Unlock()
		return state.fetch == nil
	}, time.Second, time.Millisecond)

	_, err := state.allocate(
		context.Background(),
		reservationScope{store: store},
		"orders",
		cfg,
		time.Now,
	)
	assert.ErrorIs(t, err, prefetchErr)
	assert.Len(t, store.recordedSteps(), 3)
}

func TestAllocatorConcurrentMissCreatesOneState(t *testing.T) {
	const workers = 64
	allocator := readyAllocatorForKeys(t, "orders")
	allocator.memorySampler = memorySamplerFunc(func() (uint64, uint64) { return 1, 100 })
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := allocator.FetchNext(context.Background(), "orders")
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	assert.Equal(t, int64(1), allocator.Stats().CachedKeys)
	slot := allocator.slots[SlotForKey("orders")]
	assert.Equal(t, int64(1), slot.count.Load())
}

// TestAuthorityErrorCarriesTheReasonACallerMustActOn pins that a fencing failure
// from the range authority reaches the caller with the reason it needs, including
// through the %w wrapping the repository uses around the sentinel.
func TestAuthorityErrorCarriesTheReasonACallerMustActOn(t *testing.T) {
	cases := []struct {
		name   string
		store  error
		reason reason.Reason
	}{
		{
			name:   "slot not owned",
			store:  fmt.Errorf("reserve ranges: %w: slot 3 has no ownership row", ErrSlotNotOwned),
			reason: reason.Reason_SEQUENCE_SLOT_NOT_OWNER,
		},
		{
			name:   "lease expired",
			store:  fmt.Errorf("reserve ranges: %w: slot 3", ErrLeaseExpired),
			reason: reason.Reason_SEQUENCE_LEASE_EXPIRED,
		},
		{
			name:   "uncertain commit",
			store:  fmt.Errorf("%w: connection reset by peer", ErrCommitUncertain),
			reason: reason.Reason_SEQUENCE_COMMIT_UNCERTAIN,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := authorityError(tc.store)
			assert.True(t, xerror.IsReason(got, tc.reason), "want %s, got %v", tc.reason, got)
			assert.Contains(t, got.Error(), tc.store.Error(), "the message must survive")
		})
	}
}

// TestAuthorityErrorReportsAStorageFaultAsRetriable covers the failure that is
// neither fencing nor uncertain: the store simply did not complete the
// reservation. It must reach the caller as a retriable storage reason, because
// the alternative -- a bare error -- arrives with no reason and no retry class,
// and a caller that cannot tell it from its own mistake will not retry a blip it
// could have survived.
//
// It must not be dressed as one of the fencing reasons: those tell the caller to
// refresh a directory that is not stale, which is a reaction to something that
// did not happen.
func TestAuthorityErrorReportsAStorageFaultAsRetriable(t *testing.T) {
	store := errors.New("connection refused")
	got := authorityError(store)

	assert.True(
		t,
		xerror.IsReason(got, reason.Reason_SEQUENCE_STORAGE_UNAVAILABLE),
		"want %s, got %v",
		reason.Reason_SEQUENCE_STORAGE_UNAVAILABLE,
		got,
	)
	assert.Contains(t, got.Error(), store.Error(), "the message must survive")
	assert.ErrorIs(t, got, store, "the cause must survive the reason wrapper")
	for _, fencing := range []reason.Reason{
		reason.Reason_SEQUENCE_SLOT_NOT_OWNER,
		reason.Reason_SEQUENCE_LEASE_EXPIRED,
		reason.Reason_SEQUENCE_COMMIT_UNCERTAIN,
	} {
		assert.False(t, xerror.IsReason(got, fencing), "must not be reported as %s", fencing)
	}
}

// TestAuthorityErrorKeepsACancellationRecognisable pins the other half of the
// wrapper: a failure raised by the caller's own cancellation must stay findable
// as a cancellation, not be flattened into a storage failure. Only the reason is
// added; what the store reported is still in the chain.
func TestAuthorityErrorKeepsACancellationRecognisable(t *testing.T) {
	got := authorityError(fmt.Errorf("reserve ranges: %w", context.Canceled))

	require.True(
		t,
		xerror.IsReason(got, reason.Reason_SEQUENCE_STORAGE_UNAVAILABLE),
		"want %s, got %v",
		reason.Reason_SEQUENCE_STORAGE_UNAVAILABLE,
		got,
	)
	assert.ErrorIs(t, got, context.Canceled)
	assert.Contains(t, got.Error(), "reserve ranges: context canceled")
}

func TestFetchNextNReturnsContiguousBlocks(t *testing.T) {
	allocator := readyAllocatorForKeys(t, "orders")

	first, err := allocator.FetchNextN(context.Background(), "orders", 5)
	require.NoError(t, err)
	assert.Equal(t, SequenceAllocation{ID: 1, Count: 5, SlotEpoch: 1}, first)

	next, err := allocator.FetchNext(context.Background(), "orders")
	require.NoError(t, err)
	assert.Equal(t, int64(6), next)

	second, err := allocator.FetchNextN(context.Background(), "orders", 3)
	require.NoError(t, err)
	assert.Equal(t, SequenceAllocation{ID: 7, Count: 3, SlotEpoch: 1}, second)
}

// TestBatchReportsTheOwnershipEpoch pins the value a caller compares between two
// responses for the same key. It must be the epoch the allocation was fenced by
// rather than a later one, so it is read while the batch holds the gate.
func TestBatchReportsTheOwnershipEpoch(t *testing.T) {
	key := "epoch-reporting"
	allocator, _ := leaseAllocator(t, key, func() int64 { return 0 })

	allocations, err := allocator.FetchNextBatch(
		context.Background(),
		[]SequenceRequest{{Key: key}},
	)
	require.NoError(t, err)
	require.Len(t, allocations, 1)
	require.EqualValues(t, 1, allocator.slots[SlotForKey(key)].epoch.Load())
	assert.Equal(t, uint64(1), allocations[0].SlotEpoch)
}

func TestFetchNextNReservesFreshContiguousRange(t *testing.T) {
	allocator := newHATestAllocator(
		&AllocatorConfig{
			DefaultStep:   10,
			MaxStep:       10,
			PrefetchRatio: 0.99,
		},
		&rangeStore{max: make(map[string]int64)},
		nil,
		unlimitedMemorySampler,
		nil,
	)
	allocator.testAssignSlots([]uint32{SlotForKey("orders")})

	first, err := allocator.FetchNextN(context.Background(), "orders", 6)
	require.NoError(t, err)
	assert.Equal(t, SequenceAllocation{ID: 1, Count: 6, SlotEpoch: 1}, first)

	second, err := allocator.FetchNextN(context.Background(), "orders", 6)
	require.NoError(t, err)
	assert.Equal(t, SequenceAllocation{ID: 11, Count: 6, SlotEpoch: 1}, second)
}

func TestFetchNextBatchMergesColdReservations(t *testing.T) {
	store := &countingRangeStore{max: make(map[string]int64)}
	keys := distinctSlotKeys(3)
	allocator := newHATestAllocator(
		&AllocatorConfig{DefaultStep: 10, MaxStep: 100, PrefetchRatio: 0.99},
		store,
		nil,
		unlimitedMemorySampler,
		nil,
	)
	slots := make([]uint32, len(keys))
	for index, key := range keys {
		slots[index] = SlotForKey(key)
	}
	allocator.testAssignSlots(slots)

	requests := []SequenceRequest{
		{Key: keys[0], Count: 2},
		{Key: keys[1], Count: 1},
		{Key: keys[2], Count: 3},
	}
	results, err := allocator.FetchNextBatch(context.Background(), requests)
	require.NoError(t, err)
	assert.Equal(t, []SequenceAllocation{
		{ID: 1, Count: 2, SlotEpoch: 1},
		{ID: 1, Count: 1, SlotEpoch: 1},
		{ID: 1, Count: 3, SlotEpoch: 1},
	}, results)
	assert.Equal(t, 1, store.calls())

	results, err = allocator.FetchNextBatch(context.Background(), requests)
	require.NoError(t, err)
	assert.Equal(t, []SequenceAllocation{
		{ID: 3, Count: 2, SlotEpoch: 1},
		{ID: 2, Count: 1, SlotEpoch: 1},
		{ID: 4, Count: 3, SlotEpoch: 1},
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
	allocator := newHATestAllocator(
		&AllocatorConfig{DefaultStep: 10, MaxStep: 10},
		store,
		nil,
		unlimitedMemorySampler,
		nil,
	)
	allocator.testAssignSlots([]uint32{SlotForKey("orders")})

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
	_ ReservationAuthority,
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
	_ ReservationAuthority,
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

func observationAt(key string, id int64, from, to time.Time) linearizationObservation {
	return linearizationObservation{
		Key:              key,
		ID:               id,
		OwnerInstanceID:  "instance-a",
		Epoch:            1,
		LinearizationSeq: uint64(id),
		RequestStart:     from,
		ResponseReceived: to,
	}
}

// TestLinearizationRecorderAcceptsASequentialKey pins the property itself: a key
// whose allocations each complete before the next begins is in order, and the
// counters that must stay at zero do.
func TestLinearizationRecorderAcceptsASequentialKey(t *testing.T) {
	recorder := newLinearizationRecorder(16)
	start := time.Unix(0, 0)
	for index, id := range []int64{1, 2, 3} {
		requestStart := start.Add(time.Duration(index) * time.Second)
		recorder.record(
			observationAt("orders", id, requestStart, requestStart.Add(time.Millisecond)),
		)
	}

	counters := recorder.snapshot()
	assert.Equal(t, int64(3), counters.Recorded)
	assert.Zero(t, counters.OrderViolations)
	assert.Zero(t, counters.StaleDeliveries)
	assert.Zero(t, counters.DuplicateDeliveries)
}

// TestLinearizationRecorderFlagsAStaleDelivery is the failure this whole design
// exists to prevent: a caller that had already received a larger id receives a
// smaller one afterwards.
func TestLinearizationRecorderFlagsAStaleDelivery(t *testing.T) {
	recorder := newLinearizationRecorder(16)
	start := time.Unix(0, 0)
	recorder.record(observationAt("orders", 101, start, start.Add(time.Millisecond)))
	recorder.record(observationAt(
		"orders", 1,
		start.Add(time.Second), start.Add(time.Second+time.Millisecond),
	))

	counters := recorder.snapshot()
	assert.Equal(t, int64(1), counters.StaleDeliveries)
	assert.Equal(t, int64(1), counters.OrderViolations)
	assert.Zero(t, counters.DuplicateDeliveries)
	assert.Contains(t, counters.OrderViolationDetail, "handed out 101 then")
}

// TestLinearizationRecorderFlagsARepeatWithinAKey pins the second of the three
// counters, which a stale check alone would also catch but which means something
// different to whoever reads the alert.
func TestLinearizationRecorderFlagsARepeatWithinAKey(t *testing.T) {
	recorder := newLinearizationRecorder(16)
	start := time.Unix(0, 0)
	recorder.record(observationAt("orders", 7, start, start.Add(time.Millisecond)))
	recorder.record(observationAt(
		"orders", 7,
		start.Add(time.Second), start.Add(time.Second+time.Millisecond),
	))

	counters := recorder.snapshot()
	assert.Equal(t, int64(1), counters.DuplicateDeliveries)
	assert.Equal(t, int64(1), counters.OrderViolations)
	assert.Zero(t, counters.StaleDeliveries)
}

// TestLinearizationRecorderIgnoresOverlappingRequests pins section 1.4: two
// requests in flight at once is not a violation, whatever order their responses
// arrive in, because neither had returned when the other started.
func TestLinearizationRecorderIgnoresOverlappingRequests(t *testing.T) {
	recorder := newLinearizationRecorder(16)
	start := time.Unix(0, 0)
	// The second request starts before the first one finishes, and its id is
	// lower. That is legal: only the caller's own completed-before-started
	// relation makes an order claim.
	recorder.record(observationAt("orders", 9, start, start.Add(time.Second)))
	recorder.record(observationAt(
		"orders", 8,
		start.Add(500*time.Millisecond), start.Add(1500*time.Millisecond),
	))

	assert.Zero(t, recorder.snapshot().OrderViolations)
}

// TestLinearizationRecorderBoundsItsRing keeps the memory cost fixed: a long run
// keeps the newest observations and the counters, and drops the oldest records.
func TestLinearizationRecorderBoundsItsRing(t *testing.T) {
	recorder := newLinearizationRecorder(4)
	start := time.Unix(0, 0)
	for index := int64(0); index < 10; index++ {
		requestStart := start.Add(time.Duration(index) * time.Second)
		recorder.record(observationAt("orders", index+1, requestStart, requestStart))
	}

	observations := recorder.observations()
	require.Len(t, observations, 4)
	assert.Equal(t, int64(7), observations[0].ID, "the ring keeps the newest records")
	assert.Equal(t, int64(10), observations[3].ID)
	// The counters survive the ring, which is what makes them the alertable part.
	assert.Equal(t, int64(10), recorder.snapshot().Recorded)
}

func TestLinearizationRecorderBoundsItsKeyHistory(t *testing.T) {
	recorder := newLinearizationRecorder(2)
	start := time.Unix(0, 0)
	for index, key := range []string{"a", "b", "a", "c", "b"} {
		at := start.Add(time.Duration(index) * time.Second)
		recorder.record(observationAt(key, int64(index+1), at, at))
		assert.LessOrEqual(t, len(recorder.lastComplete), 2)
		assert.LessOrEqual(t, recorder.lru.Len(), 2)
	}
	assert.Equal(t, int64(2), recorder.snapshot().EvictedKeys)
	assert.Equal(t, int64(5), recorder.snapshot().Recorded)
	assert.NotContains(t, recorder.lastComplete, "a")
	assert.Contains(t, recorder.lastComplete, "b")
	assert.Contains(t, recorder.lastComplete, "c")
	assert.Zero(t, recorder.snapshot().OrderViolations)
}

func TestLinearizationRecorderDefaultCapacityAndEvictedBaseline(t *testing.T) {
	recorder := newLinearizationRecorder(0)
	require.Equal(t, MaxLinearizationSamples, recorder.capacity)
	start := time.Unix(0, 0)
	recorder.record(observationAt("anchor", 2, start, start))
	recorder.record(observationAt("anchor", 1, start.Add(time.Second), start.Add(time.Second)))
	for index := range MaxLinearizationSamples + 32 {
		at := start.Add(time.Duration(index+2) * time.Second)
		recorder.record(observationAt(fmt.Sprintf("bounded-key-%d", index), 1, at, at))
	}
	require.Len(t, recorder.ring, MaxLinearizationSamples)
	require.Len(t, recorder.lastComplete, MaxLinearizationSamples)
	require.Equal(t, MaxLinearizationSamples, recorder.lru.Len())
	require.NotContains(t, recorder.lastComplete, "anchor")
	at := start.Add(time.Hour)
	recorder.record(observationAt("anchor", 1, at, at))
	counters := recorder.snapshot()
	assert.Equal(t, int64(34), counters.EvictedKeys)
	assert.Equal(t, int64(MaxLinearizationSamples+35), counters.Recorded)
	assert.Equal(
		t,
		int64(1),
		counters.OrderViolations,
		"an evicted key gets a new baseline; old counters survive",
	)
}

// TestAllocatorRecordsHandedOutAllocations pins the integration: with the record
// switched on, every allocation the process hands out is filed with the owner
// identity, the epoch it was fenced by, and its place in the process's own
// sequence.
func TestAllocatorRecordsHandedOutAllocations(t *testing.T) {
	ownership := newLeaseOwnershipFake()
	plane := testDataPlaneConfig(AllocatorConfig{
		DefaultStep:     10,
		MaxStep:         100,
		IdleTimeout:     time.Hour,
		CleanupInterval: time.Minute,
	})
	plane.HA.LeaseDuration = time.Hour
	plane.HA.LinearizationRecording = true
	allocator := NewAllocator(
		plane,
		&rangeStore{max: make(map[string]int64)},
		ownership,
		unlimitedMemorySampler,
		nil,
	)
	allocator.linearization = newLinearizationRecorder(16)
	allocator.testAssignSlots([]uint32{SlotForKey("orders")})

	for index := 0; index < 2; index++ {
		_, err := allocator.FetchNext(context.Background(), "orders")
		require.NoError(t, err)
	}

	counters, recording := allocator.LinearizationCounters()
	require.True(t, recording)
	assert.Equal(t, int64(2), counters.Recorded)
	assert.Zero(t, counters.OrderViolations)
	observations := allocator.linearization.observations()
	require.Len(t, observations, 2)
	assert.Equal(t, allocator.InstanceID(), observations[0].OwnerInstanceID)
	assert.Equal(t, "orders", observations[0].Key)
	assert.Less(t, observations[0].ID, observations[1].ID)
	assert.Equal(t, uint64(1), observations[0].LinearizationSeq)
	assert.Equal(t, uint64(2), observations[1].LinearizationSeq)
}

// TestAllocatorWithoutRecordingReportsNoCounters pins the other half of the
// reporting rule: without a record there is nothing to read, and the metrics
// layer must be able to tell that apart from a clean zero.
func TestAllocatorWithoutRecordingReportsNoCounters(t *testing.T) {
	ownership := newLeaseOwnershipFake()
	allocator, _ := leaseAllocatorWith(t, "unrecorded", ownership, nil)

	_, recording := allocator.LinearizationCounters()
	assert.False(t, recording)
}
