package biz

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testAllocatorConfig() AllocatorConfig {
	return AllocatorConfig{
		DefaultStep:           10,
		MaxStep:               100,
		PrefetchRatio:         0.5,
		StepIncreaseThreshold: 15 * time.Minute,
		StepDecreaseThreshold: 30 * time.Minute,
		ReserveTimeout:        100 * time.Millisecond,
	}
}

type rangeStore struct {
	mu  sync.Mutex
	max map[string]int64
}

func (s *rangeStore) ReserveRange(
	_ context.Context,
	key string,
	step int64,
) (SequenceRange, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	start := s.max[key] + 1
	s.max[key] += step
	return SequenceRange{Start: start, End: s.max[key]}, nil
}

func TestKeyStateContinuesFromPersistedWatermark(t *testing.T) {
	store := &rangeStore{max: map[string]int64{"orders": 100}}
	state := &keyState{}

	got, err := state.allocate(
		context.Background(),
		store,
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
			store,
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
				store,
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

func (invalidRangeStore) ReserveRange(context.Context, string, int64) (SequenceRange, error) {
	return SequenceRange{Start: 10, End: 10}, nil
}

func TestKeyStateRejectsInvalidReservedRange(t *testing.T) {
	state := &keyState{}
	if _, err := state.allocate(
		context.Background(),
		invalidRangeStore{},
		"orders",
		testAllocatorConfig(),
		time.Now,
	); err == nil {
		t.Fatal("expected invalid reserved range error")
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

func (s *blockingRangeStore) ReserveRange(
	ctx context.Context,
	_ string,
	step int64,
) (SequenceRange, error) {
	select {
	case <-s.started:
	default:
		close(s.started)
	}
	select {
	case <-ctx.Done():
		return SequenceRange{}, ctx.Err()
	case <-s.release:
		return SequenceRange{Start: 1, End: step}, nil
	}
}

func (s *recordingRangeStore) ReserveRange(
	ctx context.Context,
	_ string,
	step int64,
) (SequenceRange, error) {
	s.mu.Lock()
	s.steps = append(s.steps, step)
	call := len(s.steps)
	if call == 1 {
		start := s.max + 1
		s.max += step
		reserved := SequenceRange{Start: start, End: s.max}
		s.mu.Unlock()
		return reserved, nil
	}
	s.mu.Unlock()
	if s.started != nil {
		select {
		case s.started <- step:
		default:
		}
	}
	if s.release != nil {
		select {
		case <-ctx.Done():
			return SequenceRange{}, ctx.Err()
		case <-s.release:
		}
	}
	if s.err != nil {
		return SequenceRange{}, s.err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	start := s.max + 1
	s.max += step
	return SequenceRange{Start: start, End: s.max}, nil
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
		got, err := state.allocate(context.Background(), store, "orders", cfg, clock.Now)
		require.NoError(t, err)
		assert.Equal(t, want, got)
	}
	select {
	case <-store.started:
		t.Fatal("prefetch started before configured ratio")
	default:
	}

	got, err := state.allocate(context.Background(), store, "orders", cfg, clock.Now)
	require.NoError(t, err)
	assert.Equal(t, int64(6), got)
	select {
	case step := <-store.started:
		assert.Equal(t, int64(20), step)
	case <-time.After(time.Second):
		t.Fatal("prefetch did not start at configured ratio")
	}

	for range 3 {
		_, err = state.allocate(context.Background(), store, "orders", cfg, clock.Now)
		require.NoError(t, err)
	}
	assert.Equal(t, []int64{10, 20}, store.recordedSteps())
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
			activatedAt := time.Unix(100, 0)
			state := &keyState{activeStep: test.activeStep, activatedAt: activatedAt}
			got := state.nextStepLocked(
				activatedAt.Add(test.elapsed),
				5,
				10,
				cfg,
			)
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
		_, err := state.allocate(context.Background(), store, "orders", cfg, time.Now)
		require.NoError(t, err)
	}
	select {
	case <-store.started:
	case <-time.After(time.Second):
		t.Fatal("prefetch did not start")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := state.allocate(ctx, store, "orders", cfg, time.Now)
	assert.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, []int64{10, 20}, store.recordedSteps())
	close(store.release)

	got, err := state.allocate(context.Background(), store, "orders", cfg, time.Now)
	require.NoError(t, err)
	assert.Equal(t, int64(11), got)
}

func TestKeyStateFallsBackAfterPrefetchFailure(t *testing.T) {
	cfg := testAllocatorConfig()
	prefetchErr := errors.New("prefetch failed")
	store := &recordingRangeStore{err: prefetchErr, started: make(chan int64, 2)}
	state := &keyState{}

	for range cfg.DefaultStep {
		_, err := state.allocate(context.Background(), store, "orders", cfg, time.Now)
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

	_, err := state.allocate(context.Background(), store, "orders", cfg, time.Now)
	assert.ErrorIs(t, err, prefetchErr)
	assert.Len(t, store.recordedSteps(), 3)
}

func TestAllocatorAppliesRouteVersionWithoutNewSlots(t *testing.T) {
	tests := []struct {
		name   string
		routes []struct {
			version int64
			slots   []uint32
			apply   bool
		}
		wantVersion int64
	}{
		{
			name: "unchanged slots",
			routes: []struct {
				version int64
				slots   []uint32
				apply   bool
			}{
				{version: 1, slots: []uint32{1}, apply: true},
				{version: 2, slots: []uint32{1}, apply: true},
			},
			wantVersion: 2,
		},
		{
			name: "empty slots",
			routes: []struct {
				version int64
				slots   []uint32
				apply   bool
			}{
				{version: 1, slots: []uint32{1}, apply: true},
				{version: 2, apply: true},
			},
			wantVersion: 2,
		},
		{
			name: "skipped intermediate snapshot",
			routes: []struct {
				version int64
				slots   []uint32
				apply   bool
			}{
				{version: 1, slots: []uint32{1}, apply: true},
				{version: 2, slots: []uint32{1}},
				{version: 3, slots: []uint32{1}, apply: true},
			},
			wantVersion: 3,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			allocator := NewAllocator(
				&AllocatorConfig{DefaultStep: 10, MaxStep: 100},
				&rangeStore{max: make(map[string]int64)},
				slog.Default(),
			)
			for _, route := range test.routes {
				allocator.CommitRoute(route.version, 0, route.slots)
				if route.apply {
					allocator.ApplyRoute(0)
				}
			}
			if got := allocator.CurrentVersion(); got != test.wantVersion {
				t.Fatalf("route version = %d, want %d", got, test.wantVersion)
			}
		})
	}
}

func readyAllocatorForCleanup(
	t *testing.T,
	store SequenceRepo,
	clock *fakeClock,
	key string,
) *Allocator {
	t.Helper()
	allocator := NewAllocator(&AllocatorConfig{
		DefaultStep:     10,
		MaxStep:         100,
		IdleTimeout:     10 * time.Minute,
		CleanupInterval: time.Minute,
	}, store, slog.Default())
	allocator.now = clock.Now
	allocator.Open(1, 0, []uint32{SlotForKey(key)})
	allocator.ApplyRoute(0)
	return allocator
}

func allocatorKeyState(t *testing.T, allocator *Allocator, key string) (*sync.Map, *keyState) {
	t.Helper()
	allocator.slotsMu.RLock()
	defer allocator.slotsMu.RUnlock()
	slot := allocator.slots[SlotForKey(key)]
	require.NotNil(t, slot)
	value, ok := slot.Load(key)
	require.True(t, ok)
	return slot, value.(*keyState)
}

func TestAllocatorCleanupEvictsIdleStateAndContinuesFromWatermark(t *testing.T) {
	key := "idle-orders"
	clock := &fakeClock{now: time.Unix(1000, 0)}
	store := &rangeStore{max: make(map[string]int64)}
	allocator := readyAllocatorForCleanup(t, store, clock, key)

	first, err := allocator.FetchNext(context.Background(), key)
	require.NoError(t, err)
	assert.Equal(t, int64(1), first)
	oldSlot, oldState := allocatorKeyState(t, allocator, key)

	clock.Advance(11 * time.Minute)
	allocator.cleanupIdle()
	_, loaded := oldSlot.Load(key)
	assert.False(t, loaded)

	next, err := allocator.FetchNext(context.Background(), key)
	require.NoError(t, err)
	assert.Equal(t, int64(11), next)
	_, newState := allocatorKeyState(t, allocator, key)
	assert.NotSame(t, oldState, newState)
}

func TestAllocatorCleanupRespectsInterval(t *testing.T) {
	key := "scheduled-cleanup-orders"
	clock := &fakeClock{now: time.Unix(1500, 0)}
	allocator := readyAllocatorForCleanup(
		t,
		&rangeStore{max: make(map[string]int64)},
		clock,
		key,
	)
	slot := allocator.slots[SlotForKey(key)]
	state := &keyState{}
	state.lastUsed.Store(clock.Now().Add(-11 * time.Minute).UnixNano())
	slot.Store(key, state)
	allocator.lastCleanup.Store(clock.Now().Add(-30 * time.Second).UnixNano())

	allocator.cleanupIdle()
	_, loaded := slot.Load(key)
	assert.True(t, loaded)

	clock.Advance(31 * time.Second)
	allocator.cleanupIdle()
	_, loaded = slot.Load(key)
	assert.False(t, loaded)
}

func TestNodeBaseTickCleansIdleAllocatorState(t *testing.T) {
	key := "node-cleanup-orders"
	clock := &fakeClock{now: time.Unix(1750, 0)}
	allocator := readyAllocatorForCleanup(
		t,
		&rangeStore{max: make(map[string]int64)},
		clock,
		key,
	)
	slot := allocator.slots[SlotForKey(key)]
	state := &keyState{}
	state.lastUsed.Store(clock.Now().Add(-11 * time.Minute).UnixNano())
	slot.Store(key, state)
	manager := &NodeManager{allocator: allocator, heartbeatTimeout: 3}

	manager.BaseTick()
	_, loaded := slot.Load(key)
	assert.False(t, loaded)
}

func TestAllocatorCleanupReportsDiscardedRanges(t *testing.T) {
	key := "discarded-orders"
	clock := &fakeClock{now: time.Unix(2000, 0)}
	allocator := readyAllocatorForCleanup(
		t,
		&rangeStore{max: make(map[string]int64)},
		clock,
		key,
	)
	slot := allocator.slots[SlotForKey(key)]
	state := &keyState{standby: &SequenceRange{Start: 11, End: 30}}
	state.initialized.Store(true)
	state.start.Store(1)
	state.end.Store(10)
	state.returned.Store(3)
	state.lastUsed.Store(clock.Now().Add(-11 * time.Minute).UnixNano())
	slot.Store(key, state)

	candidates, scanned := allocator.collectIdleCandidates(
		clock.Now().Add(-10 * time.Minute).UnixNano(),
	)
	stats := allocator.evictIdleCandidates(
		candidates,
		clock.Now().Add(-10*time.Minute).UnixNano(),
	)

	assert.Equal(t, 1, scanned)
	assert.Equal(t, 1, stats.evicted)
	assert.Equal(t, int64(7), stats.discardedActive)
	assert.Equal(t, int64(20), stats.discardedStandby)
}

func TestAllocatorCleanupRechecksRecentUse(t *testing.T) {
	key := "recent-orders"
	clock := &fakeClock{now: time.Unix(3000, 0)}
	allocator := readyAllocatorForCleanup(
		t,
		&rangeStore{max: make(map[string]int64)},
		clock,
		key,
	)
	slot := allocator.slots[SlotForKey(key)]
	state := &keyState{}
	state.lastUsed.Store(clock.Now().Add(-11 * time.Minute).UnixNano())
	slot.Store(key, state)
	cutoff := clock.Now().Add(-10 * time.Minute).UnixNano()
	candidates, _ := allocator.collectIdleCandidates(cutoff)
	require.Len(t, candidates, 1)

	state.touch(clock.Now())
	stats := allocator.evictIdleCandidates(candidates, cutoff)
	assert.Zero(t, stats.evicted)
	_, loaded := slot.Load(key)
	assert.True(t, loaded)
}

func TestAllocatorCleanupSkipsInflightFetch(t *testing.T) {
	key := "inflight-orders"
	clock := &fakeClock{now: time.Unix(4000, 0)}
	allocator := readyAllocatorForCleanup(
		t,
		&rangeStore{max: make(map[string]int64)},
		clock,
		key,
	)
	slot := allocator.slots[SlotForKey(key)]
	state := &keyState{fetch: &rangeFetch{done: make(chan struct{})}}
	state.lastUsed.Store(clock.Now().Add(-11 * time.Minute).UnixNano())
	slot.Store(key, state)
	cutoff := clock.Now().Add(-10 * time.Minute).UnixNano()
	candidates, _ := allocator.collectIdleCandidates(cutoff)

	stats := allocator.evictIdleCandidates(candidates, cutoff)
	assert.Equal(t, 1, stats.inflight)
	assert.Zero(t, stats.evicted)
	_, loaded := slot.Load(key)
	assert.True(t, loaded)
}

func TestAllocatorCleanupIgnoresReplacedSlotMap(t *testing.T) {
	key := "rerouted-orders"
	clock := &fakeClock{now: time.Unix(5000, 0)}
	allocator := readyAllocatorForCleanup(
		t,
		&rangeStore{max: make(map[string]int64)},
		clock,
		key,
	)
	oldSlot := allocator.slots[SlotForKey(key)]
	oldState := &keyState{}
	oldState.lastUsed.Store(clock.Now().Add(-11 * time.Minute).UnixNano())
	oldSlot.Store(key, oldState)
	cutoff := clock.Now().Add(-10 * time.Minute).UnixNano()
	candidates, _ := allocator.collectIdleCandidates(cutoff)

	newSlot := &sync.Map{}
	newState := &keyState{}
	newSlot.Store(key, newState)
	allocator.slotsMu.Lock()
	allocator.slots[SlotForKey(key)] = newSlot
	allocator.slotsMu.Unlock()

	stats := allocator.evictIdleCandidates(candidates, cutoff)
	assert.Zero(t, stats.evicted)
	value, loaded := newSlot.Load(key)
	assert.True(t, loaded)
	assert.Same(t, newState, value)
}

func TestAllocatorCleanupWaitsForAllocationAndRechecksUse(t *testing.T) {
	key := "blocked-orders"
	clock := &fakeClock{now: time.Unix(6000, 0)}
	store := &blockingRangeStore{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	allocator := readyAllocatorForCleanup(t, store, clock, key)

	fetchDone := make(chan error, 1)
	go func() {
		_, err := allocator.FetchNext(context.Background(), key)
		fetchDone <- err
	}()
	<-store.started

	cleanupDone := make(chan struct{})
	go func() {
		allocator.cleanupIdle()
		close(cleanupDone)
	}()
	select {
	case <-cleanupDone:
		t.Fatal("cleanup completed while allocation held the slot read lock")
	case <-time.After(20 * time.Millisecond):
	}

	close(store.release)
	require.NoError(t, <-fetchDone)
	select {
	case <-cleanupDone:
	case <-time.After(time.Second):
		t.Fatal("cleanup did not finish after allocation completed")
	}
	_, state := allocatorKeyState(t, allocator, key)
	assert.Equal(t, clock.Now().UnixNano(), state.lastUsed.Load())
}

func TestAllocatorConcurrentCleanupKeepsIDsUnique(t *testing.T) {
	key := "concurrent-cleanup-orders"
	clock := &fakeClock{now: time.Unix(7000, 0)}
	store := &rangeStore{max: make(map[string]int64)}
	allocator := readyAllocatorForCleanup(t, store, clock, key)
	allocator.cfg.IdleTimeout = time.Nanosecond
	allocator.cfg.CleanupInterval = time.Nanosecond

	const workers = 8
	const allocations = 100
	values := make(chan int64, workers*allocations)
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range allocations {
				value, err := allocator.FetchNext(context.Background(), key)
				if err != nil {
					errs <- err
					return
				}
				values <- value
			}
		}()
	}
	cleanupDone := make(chan struct{})
	go func() {
		defer close(cleanupDone)
		for range allocations {
			clock.Advance(time.Nanosecond)
			allocator.cleanupIdle()
		}
	}()
	wg.Wait()
	<-cleanupDone
	close(values)
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	seen := make(map[int64]struct{}, workers*allocations)
	for value := range values {
		_, duplicate := seen[value]
		assert.False(t, duplicate, "duplicate ID %d", value)
		seen[value] = struct{}{}
	}
	assert.Len(t, seen, workers*allocations)
}
