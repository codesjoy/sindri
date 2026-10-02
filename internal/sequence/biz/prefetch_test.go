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
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReserveLatencyTrackerUsesRollingTrustedP99(t *testing.T) {
	clock := &fakeClock{now: time.Unix(100, 0)}
	tracker := newReserveLatencyTracker(time.Minute, 3, clock.Now)

	tracker.observe(time.Millisecond, true)
	tracker.observe(2*time.Millisecond, true)
	assert.Zero(t, tracker.observedP99())

	tracker.observe(3*time.Millisecond, true)
	tracker.observe(4*time.Millisecond, false)
	clock.Advance(time.Second)
	assert.Equal(t, 4*time.Millisecond, tracker.observedP99())

	clock.Advance(2 * time.Minute)
	assert.Zero(t, tracker.observedP99())
}

func TestReserveLatencyTrackerCountsOnlySuccessfulSamplesForTrust(t *testing.T) {
	clock := &fakeClock{now: time.Unix(100, 0)}
	tracker := newReserveLatencyTracker(time.Minute, 2, clock.Now)

	tracker.observe(time.Millisecond, true)
	tracker.observe(time.Second, false)

	assert.Zero(t, tracker.observedP99())
}

func TestReserveLatencyTrackerNotifiesObserver(t *testing.T) {
	clock := &fakeClock{now: time.Unix(100, 0)}
	tracker := newReserveLatencyTracker(time.Minute, 1, clock.Now)
	var (
		gotDuration time.Duration
		gotSuccess  bool
	)
	tracker.setObserver(func(duration time.Duration, success bool) {
		gotDuration = duration
		gotSuccess = success
	})

	tracker.observe(7*time.Millisecond, true)

	assert.Equal(t, 7*time.Millisecond, gotDuration)
	assert.True(t, gotSuccess)
}

func TestAllocatorPrefetchLeadUsesTrustedP99WithinTimeout(t *testing.T) {
	clock := &fakeClock{now: time.Unix(100, 0)}
	cfg := testAllocatorConfig()
	cfg.ReserveTimeout = 100 * time.Millisecond
	cfg.PrefetchLatencyMultiplier = 4
	allocator := &Allocator{
		cfg:            cfg,
		now:            clock.Now,
		reserveLatency: newReserveLatencyTracker(time.Minute, 1, clock.Now),
	}

	assert.Equal(t, cfg.ReserveTimeout, allocator.prefetchLead(cfg))

	allocator.reserveLatency.observe(10*time.Millisecond, true)
	assert.Equal(t, 40*time.Millisecond, allocator.prefetchLead(cfg))

	clock.Advance(time.Second)
	allocator.reserveLatency.observe(80*time.Millisecond, true)
	assert.Equal(t, cfg.ReserveTimeout, allocator.prefetchLead(cfg))
}

func TestObserveReserveTruncatesTimeoutAtConfiguredBound(t *testing.T) {
	clock := &fakeClock{now: time.Unix(100, 0)}
	cfg := testAllocatorConfig()
	cfg.ReserveTimeout = 50 * time.Millisecond
	allocator := &Allocator{
		cfg:            cfg,
		reserveLatency: newReserveLatencyTracker(time.Minute, 1, clock.Now),
	}

	allocator.observeReserve(time.Now().Add(-time.Second), nil)

	assert.Equal(t, cfg.ReserveTimeout, allocator.observedReserveP99())
}

func TestKeyStateUsesLatencyLeadBeforeFallbackRatio(t *testing.T) {
	cfg := testAllocatorConfig()
	cfg.DefaultStep = 100
	cfg.MaxStep = 100
	cfg.PrefetchRatio = 0.5
	cfg.PrefetchLatencyMultiplier = 4
	cfg.ReserveTimeout = time.Second
	cfg.PrefetchRateResetAfter = time.Minute
	clock := &fakeClock{now: time.Unix(100, 0)}
	latency := newReserveLatencyTracker(time.Minute, 1, clock.Now)
	latency.observe(150*time.Millisecond, true)
	allocator := &Allocator{
		cfg:            cfg,
		now:            clock.Now,
		monoNow:        clock.MonoNow,
		reserveLatency: latency,
	}
	state := newAdaptiveKeyState(allocator, cfg, clock.Now())
	store := newBlockingPrefetchStore()

	triggeredAt := int64(0)
	for want := int64(1); want <= 50; want++ {
		if want > 1 {
			clock.Advance(10 * time.Millisecond)
		}
		got, err := state.allocate(
			context.Background(),
			reservationScope{store: store},
			"orders",
			cfg,
			clock.Now,
		)
		require.NoError(t, err)
		assert.Equal(t, want, got)
		select {
		case <-store.started:
			triggeredAt = got
		default:
		}
		if triggeredAt == 0 {
			state.mu.Lock()
			started := state.fetch != nil && state.fetch.background
			state.mu.Unlock()
			if started {
				triggeredAt = got
			}
		}
		if triggeredAt != 0 {
			break
		}
	}
	require.NotZero(t, triggeredAt)
	assert.Greater(t, triggeredAt, int64(1), "the first ID must not start prefetch")
	assert.Less(t, triggeredAt, int64(50), "latency lead should beat the fallback ratio")
	close(store.release)
}

func TestKeyStateResetsRateEstimateAfterIdle(t *testing.T) {
	cfg := testAllocatorConfig()
	cfg.DefaultStep = 100
	cfg.MaxStep = 100
	cfg.PrefetchRatio = 0.5
	cfg.PrefetchLatencyMultiplier = 4
	cfg.ReserveTimeout = time.Second
	cfg.PrefetchRateResetAfter = time.Minute
	clock := &fakeClock{now: time.Unix(200, 0)}
	latency := newReserveLatencyTracker(time.Minute, 1, clock.Now)
	latency.observe(10*time.Millisecond, true)
	allocator := &Allocator{
		cfg:            cfg,
		now:            clock.Now,
		monoNow:        clock.MonoNow,
		reserveLatency: latency,
	}
	state := newAdaptiveKeyState(allocator, cfg, clock.Now())
	store := newBlockingPrefetchStore()

	for want := int64(1); want <= 11; want++ {
		if want > 1 {
			clock.Advance(10 * time.Millisecond)
		}
		got, err := state.allocate(
			context.Background(),
			reservationScope{store: store},
			"orders",
			cfg,
			clock.Now,
		)
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
	select {
	case <-store.started:
		t.Fatal("prefetch started while key was active")
	default:
	}

	clock.Advance(2 * time.Minute)
	_, err := state.allocate(
		context.Background(),
		reservationScope{store: store},
		"orders",
		cfg,
		clock.Now,
	)
	require.NoError(t, err)
	select {
	case <-store.started:
		t.Fatal("prefetch started immediately after idle reset")
	default:
	}

	for want := int64(13); want <= 50; want++ {
		clock.Advance(10 * time.Millisecond)
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
	case <-time.After(time.Second):
		t.Fatal("fallback ratio did not start after rate reset")
	}
	close(store.release)
}

func TestKeyStateFallbackRatioPreservesConsumedThreshold(t *testing.T) {
	cfg := testAllocatorConfig()
	cfg.PrefetchRatio = 0.6
	state := &keyState{}
	state.start.Store(1)
	state.end.Store(10)

	allowance, fallback := state.prefetchAllowanceRateLocked(cfg)

	assert.True(t, fallback)
	assert.Equal(t, int64(4), allowance)
	assert.True(t, 10-6 <= allowance)
}

func TestKeyStateRecentBlockExtendsPrefetchLead(t *testing.T) {
	cfg := testAllocatorConfig()
	cfg.ReserveTimeout = 100 * time.Millisecond
	cfg.PrefetchLatencyMultiplier = 2
	state := &keyState{}
	state.start.Store(1)
	state.end.Store(100)
	state.recentBlock.Store(80)
	state.rate.ready = true
	state.rate.rate = 10

	allowance, fallback := state.prefetchAllowanceRateLocked(cfg)

	assert.False(t, fallback)
	assert.Equal(t, int64(81), allowance)
	assert.True(t, 100-20 <= allowance)
}

func TestKeyStateIdleResetClearsRecentBlockGuard(t *testing.T) {
	cfg := testAllocatorConfig()
	cfg.PrefetchRatio = 0.5
	cfg.PrefetchRateResetAfter = time.Minute
	clock := &fakeClock{now: time.Unix(100, 0)}
	state := newAdaptiveKeyState(nil, cfg, clock.Now())

	state.observeAllocation(clock.Now(), 1, 80, 2, cfg)
	assert.Equal(t, int64(80), state.recentBlock.Load())

	clock.Advance(2 * time.Minute)
	state.observeAllocation(clock.Now(), 2, 1, 2, cfg)

	assert.Equal(t, int64(1), state.recentBlock.Load())
	_, ready := state.rateSnapshot()
	assert.False(t, ready)
}

func TestAllocatorRetryDelayGrowsAndJitters(t *testing.T) {
	allocator := &Allocator{
		cfg:           AllocatorConfig{ReserveTimeout: time.Second},
		randomFloat64: func() float64 { return 0.5 },
	}
	first := allocator.retryDelay(1, time.Second, 10*time.Millisecond)
	second := allocator.retryDelay(2, time.Second, 10*time.Millisecond)
	last := allocator.retryDelay(20, time.Second, 10*time.Millisecond)

	assert.Greater(t, second, first)
	assert.LessOrEqual(t, last, maxRetryBackoff)
}

type fakeRetryTimer struct {
	mu       sync.Mutex
	callback func()
	stopped  bool
}

func (t *fakeRetryTimer) Stop() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	wasStopped := t.stopped
	t.stopped = true
	return !wasStopped
}

func (t *fakeRetryTimer) trigger() {
	t.mu.Lock()
	callback := t.callback
	stopped := t.stopped
	t.mu.Unlock()
	if !stopped && callback != nil {
		callback()
	}
}

type fakeTimerFactory struct {
	mu     sync.Mutex
	delays []time.Duration
	timers []*fakeRetryTimer
}

func (f *fakeTimerFactory) after(delay time.Duration, callback func()) retryTimer {
	timer := &fakeRetryTimer{callback: callback}
	f.mu.Lock()
	f.delays = append(f.delays, delay)
	f.timers = append(f.timers, timer)
	f.mu.Unlock()
	return timer
}

func (f *fakeTimerFactory) last() (*fakeRetryTimer, time.Duration, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.timers) == 0 {
		return nil, 0, false
	}
	return f.timers[len(f.timers)-1], f.delays[len(f.delays)-1], true
}

func (f *fakeTimerFactory) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.timers)
}

type flakyRangeStore struct {
	mu      sync.Mutex
	calls   int
	retried chan struct{}
}

func (s *flakyRangeStore) ReserveRanges(
	_ context.Context,
	_ ReservationAuthority,
	requests []ReservationRequest,
) ([]SequenceRange, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	switch s.calls {
	case 1:
		return []SequenceRange{{Start: 1, End: requests[0].Step}}, nil
	case 2:
		return nil, errors.New("database unavailable")
	default:
		select {
		case <-s.retried:
		default:
			close(s.retried)
		}
		start := requests[0].Step + 1
		return []SequenceRange{{
			Start: start,
			End:   start + requests[0].Step - 1,
		}}, nil
	}
}

type blockingFailureRangeStore struct {
	mu      sync.Mutex
	calls   int
	active  int
	maxSeen int
	release chan struct{}
}

func newBlockingFailureRangeStore() *blockingFailureRangeStore {
	return &blockingFailureRangeStore{release: make(chan struct{}, 8)}
}

func (s *blockingFailureRangeStore) ReserveRanges(
	_ context.Context,
	_ ReservationAuthority,
	requests []ReservationRequest,
) ([]SequenceRange, error) {
	s.mu.Lock()
	s.calls++
	call := s.calls
	if call == 1 {
		s.mu.Unlock()
		return []SequenceRange{{
			Start: 1,
			End:   requests[0].Step,
		}}, nil
	}
	s.active++
	s.maxSeen = max(s.maxSeen, s.active)
	s.mu.Unlock()

	<-s.release

	s.mu.Lock()
	s.active--
	s.mu.Unlock()
	return nil, errors.New("database unavailable")
}

func (s *blockingFailureRangeStore) allowRetry() {
	s.release <- struct{}{}
}

func (s *blockingFailureRangeStore) stats() (calls, active, maxSeen int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls, s.active, s.maxSeen
}

func TestAllocatorRetriesBackgroundPrefetchBeforeExhaustion(t *testing.T) {
	clock := &fakeClock{now: time.Unix(300, 0)}
	store := &flakyRangeStore{retried: make(chan struct{})}
	allocator := newHATestAllocator(
		&AllocatorConfig{
			DefaultStep:    10,
			MaxStep:        10,
			PrefetchRatio:  0.5,
			ReserveTimeout: time.Second,
		},
		store,
		nil,
		unlimitedMemorySampler,
		slog.Default(),
	)
	timers := &fakeTimerFactory{}
	allocator.now = clock.Now
	allocator.afterFunc = timers.after
	allocator.randomFloat64 = func() float64 { return 0.5 }
	allocator.Open(1, 0, []uint32{SlotForKey("orders")})
	allocator.ApplyRoute(0)

	for want := int64(1); want <= 5; want++ {
		got, err := allocator.FetchNext(context.Background(), "orders")
		require.NoError(t, err)
		require.Equal(t, want, got)
	}

	var timer *fakeRetryTimer
	var delay time.Duration
	require.Eventually(t, func() bool {
		var ok bool
		timer, delay, ok = timers.last()
		return ok
	}, time.Second, time.Millisecond)

	for want := int64(6); want <= 10; want++ {
		got, err := allocator.FetchNext(context.Background(), "orders")
		require.NoError(t, err)
		assert.Equal(t, want, got, "active range must remain available during backoff")
	}

	clock.Advance(delay)
	timer.trigger()
	select {
	case <-store.retried:
	case <-time.After(time.Second):
		t.Fatal("background retry did not execute")
	}
	require.Eventually(t, func() bool {
		_, state := allocatorKeyState(t, allocator, "orders")
		state.mu.Lock()
		defer state.mu.Unlock()
		return state.standby != nil
	}, time.Second, time.Millisecond)

	got, err := allocator.FetchNext(context.Background(), "orders")
	require.NoError(t, err)
	assert.Equal(t, int64(11), got)
	stats := allocator.Stats()
	assert.Equal(t, int64(2), stats.PrefetchStarted)
	assert.Equal(t, int64(1), stats.PrefetchFailed)
	assert.Equal(t, int64(1), stats.PrefetchSucceeded)
	assert.Equal(t, int64(1), stats.PrefetchRetries)
}

func TestAllocatorRetryFailureKeepsSingleFlight(t *testing.T) {
	clock := &fakeClock{now: time.Unix(350, 0)}
	store := newBlockingFailureRangeStore()
	allocator := newHATestAllocator(
		&AllocatorConfig{
			DefaultStep:    10,
			MaxStep:        10,
			PrefetchRatio:  0.5,
			ReserveTimeout: time.Second,
		},
		store,
		nil,
		unlimitedMemorySampler,
		slog.Default(),
	)
	timers := &fakeTimerFactory{}
	allocator.now = clock.Now
	allocator.afterFunc = timers.after
	allocator.randomFloat64 = func() float64 { return 0.5 }
	allocator.Open(1, 0, []uint32{SlotForKey("orders")})
	allocator.ApplyRoute(0)

	for want := int64(1); want <= 5; want++ {
		got, err := allocator.FetchNext(context.Background(), "orders")
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
	require.Eventually(t, func() bool {
		calls, active, maxSeen := store.stats()
		return calls == 2 && active == 1 && maxSeen == 1
	}, time.Second, time.Millisecond)

	store.allowRetry()
	var timer *fakeRetryTimer
	var delay time.Duration
	require.Eventually(t, func() bool {
		var ok bool
		timer, delay, ok = timers.last()
		return ok
	}, time.Second, time.Millisecond)

	clock.Advance(delay)
	timer.trigger()
	require.Eventually(t, func() bool {
		calls, active, maxSeen := store.stats()
		return calls == 3 && active == 1 && maxSeen == 1
	}, time.Second, time.Millisecond)

	timer.trigger()
	calls, active, maxSeen := store.stats()
	assert.Equal(t, 3, calls, "a stale timer must not start a concurrent retry")
	assert.Equal(t, 1, active)
	assert.Equal(t, 1, maxSeen)

	store.allowRetry()
	require.Eventually(t, func() bool {
		calls, active, maxSeen := store.stats()
		return timers.count() >= 2 && calls == 3 && active == 0 && maxSeen == 1
	}, time.Second, time.Millisecond)
	stats := allocator.Stats()
	assert.Equal(t, int64(2), stats.PrefetchFailed)
	assert.Equal(t, int64(2), stats.PrefetchRetries)
}

func TestAllocatorPausePreventsScheduledRetry(t *testing.T) {
	clock := &fakeClock{now: time.Unix(375, 0)}
	store := newBlockingFailureRangeStore()
	allocator := newHATestAllocator(
		&AllocatorConfig{
			DefaultStep:    10,
			MaxStep:        10,
			PrefetchRatio:  0.5,
			ReserveTimeout: time.Second,
		},
		store,
		nil,
		unlimitedMemorySampler,
		slog.Default(),
	)
	timers := &fakeTimerFactory{}
	allocator.now = clock.Now
	allocator.afterFunc = timers.after
	allocator.randomFloat64 = func() float64 { return 0.5 }
	allocator.Open(1, 0, []uint32{SlotForKey("orders")})
	allocator.ApplyRoute(0)

	for want := int64(1); want <= 5; want++ {
		got, err := allocator.FetchNext(context.Background(), "orders")
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
	require.Eventually(t, func() bool {
		calls, active, _ := store.stats()
		return calls == 2 && active == 1
	}, time.Second, time.Millisecond)
	store.allowRetry()

	var timer *fakeRetryTimer
	var delay time.Duration
	require.Eventually(t, func() bool {
		var ok bool
		timer, delay, ok = timers.last()
		return ok
	}, time.Second, time.Millisecond)
	allocator.Pause()
	clock.Advance(delay)
	timer.trigger()

	require.Eventually(t, func() bool {
		_, state := allocatorKeyState(t, allocator, "orders")
		state.mu.Lock()
		defer state.mu.Unlock()
		return state.retryTimer == nil && state.retryAfter.IsZero() && state.fetch == nil
	}, time.Second, time.Millisecond)
	calls, active, _ := store.stats()
	assert.Equal(t, 2, calls)
	assert.Zero(t, active)
}

func TestAllocatorCleanupCancelsRetryTimer(t *testing.T) {
	clock := &fakeClock{now: time.Unix(390, 0)}
	store := newBlockingFailureRangeStore()
	allocator := newHATestAllocator(
		&AllocatorConfig{
			DefaultStep:     10,
			MaxStep:         10,
			PrefetchRatio:   0.5,
			ReserveTimeout:  time.Second,
			IdleTimeout:     10 * time.Minute,
			CleanupInterval: time.Minute,
		},
		store,
		nil,
		unlimitedMemorySampler,
		slog.Default(),
	)
	timers := &fakeTimerFactory{}
	allocator.now = clock.Now
	allocator.afterFunc = timers.after
	allocator.randomFloat64 = func() float64 { return 0.5 }
	key := "orders"
	allocator.Open(1, 0, []uint32{SlotForKey(key)})
	allocator.ApplyRoute(0)

	for want := int64(1); want <= 5; want++ {
		got, err := allocator.FetchNext(context.Background(), key)
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
	require.Eventually(t, func() bool {
		calls, active, _ := store.stats()
		return calls == 2 && active == 1
	}, time.Second, time.Millisecond)
	store.allowRetry()

	var timer *fakeRetryTimer
	require.Eventually(t, func() bool {
		var ok bool
		timer, _, ok = timers.last()
		return ok
	}, time.Second, time.Millisecond)

	clock.Advance(11 * time.Minute)
	allocator.cleanupIdle()

	assert.False(t, timer.Stop(), "cleanup must stop the pending retry timer")
	slot := allocator.slots[SlotForKey(key)]
	_, loaded := slot.Load(key)
	assert.False(t, loaded)
}

func TestAllocatorRouteRemovalCancelsRetryTimer(t *testing.T) {
	allocator := readyAllocatorForKeys(t, "orders")
	key := "orders"
	_, err := allocator.FetchNext(context.Background(), key)
	require.NoError(t, err)
	_, state := allocatorKeyState(t, allocator, key)
	timer := &fakeRetryTimer{callback: func() {}}
	state.mu.Lock()
	state.retryTimer = timer
	state.retryAfter = time.Now().Add(time.Second)
	state.mu.Unlock()

	allocator.CommitRoute(2, 0, nil)

	assert.False(t, timer.Stop(), "route removal must stop the timer first")
	state.mu.Lock()
	assert.Nil(t, state.retryTimer)
	state.mu.Unlock()
	allocator.slotsMu.RLock()
	_, loaded := allocator.slots[SlotForKey(key)]
	allocator.slotsMu.RUnlock()
	assert.False(t, loaded)
}

func TestAllocatorRouteRemovalDiscardsInFlightRetry(t *testing.T) {
	clock := &fakeClock{now: time.Unix(405, 0)}
	store := newBlockingFailureRangeStore()
	allocator := newHATestAllocator(
		&AllocatorConfig{
			DefaultStep:    10,
			MaxStep:        10,
			PrefetchRatio:  0.5,
			ReserveTimeout: time.Second,
		},
		store,
		nil,
		unlimitedMemorySampler,
		slog.Default(),
	)
	timers := &fakeTimerFactory{}
	allocator.now = clock.Now
	allocator.afterFunc = timers.after
	allocator.randomFloat64 = func() float64 { return 0.5 }
	key := "orders"
	allocator.Open(1, 0, []uint32{SlotForKey(key)})
	allocator.ApplyRoute(0)

	for want := int64(1); want <= 5; want++ {
		got, err := allocator.FetchNext(context.Background(), key)
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
	require.Eventually(t, func() bool {
		calls, active, _ := store.stats()
		return calls == 2 && active == 1
	}, time.Second, time.Millisecond)
	_, state := allocatorKeyState(t, allocator, key)

	allocator.CommitRoute(2, 0, nil)
	store.allowRetry()

	require.Eventually(t, func() bool {
		state.mu.Lock()
		defer state.mu.Unlock()
		return state.fetch == nil
	}, time.Second, time.Millisecond)
	assert.True(t, state.retired.Load())
	assert.Zero(t, timers.count())
	state.mu.Lock()
	assert.Nil(t, state.retryTimer)
	state.mu.Unlock()
}

func TestAllocatorBlockAllocationTriggersAdaptivePrefetch(t *testing.T) {
	tests := []struct {
		name     string
		allocate func(*Allocator, string) (SequenceAllocation, error)
	}{
		{
			name: "FetchNextN",
			allocate: func(allocator *Allocator, key string) (SequenceAllocation, error) {
				return allocator.FetchNextN(context.Background(), key, 20)
			},
		},
		{
			name: "FetchNextBatch",
			allocate: func(allocator *Allocator, key string) (SequenceAllocation, error) {
				results, err := allocator.FetchNextBatch(
					context.Background(),
					[]SequenceRequest{{Key: key, Count: 20}},
				)
				if err != nil {
					return SequenceAllocation{}, err
				}
				return results[0], nil
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			key := "orders"
			store := newBlockingPrefetchStore()
			store.max = 100
			allocator := newAdaptiveBlockAllocator(t, store, key)

			allocation, err := test.allocate(allocator, key)
			require.NoError(t, err)
			assert.Equal(t, SequenceAllocation{ID: 81, Count: 20}, allocation)

			_, state := allocatorKeyState(t, allocator, key)
			state.mu.Lock()
			started := state.fetch != nil && state.fetch.background
			state.mu.Unlock()
			require.True(t, started, "a block crossing the time lead must start prefetch")
			assert.Equal(t, int64(1), allocator.Stats().PrefetchStarted)
			assert.Zero(t, allocator.Stats().PrefetchFallback)
			close(store.release)
		})
	}
}

func newAdaptiveBlockAllocator(
	t *testing.T,
	store SequenceRepo,
	key string,
) *Allocator {
	t.Helper()
	clock := &fakeClock{now: time.Unix(400, 0)}
	cfg := testAllocatorConfig()
	cfg.DefaultStep = 100
	cfg.MaxStep = 100
	cfg.PrefetchLatencyMultiplier = 4
	cfg.PrefetchLatencyMinSamples = 1
	cfg.PrefetchRateResetAfter = time.Minute
	cfg.ReserveTimeout = time.Second
	allocator := newHATestAllocator(&cfg, store, nil, unlimitedMemorySampler, slog.Default())
	allocator.now = clock.Now
	allocator.reserveLatency = newReserveLatencyTracker(time.Minute, 1, clock.Now)
	allocator.reserveLatency.observe(10*time.Millisecond, true)
	allocator.Open(1, 0, []uint32{SlotForKey(key)})
	allocator.ApplyRoute(0)

	state := newAdaptiveKeyState(allocator, allocator.cfg, clock.Now())
	state.next.Store(80)
	state.rateMu.Lock()
	state.rate.observedAt = clock.Now()
	state.rate.anchorAt = clock.Now().Add(-time.Second)
	state.rate.observedIDs = 80
	state.rate.observations = 2
	state.rate.rate = 100
	state.rate.ready = true
	state.configurePrefetchThresholdLocked(allocator.cfg)
	state.rateMu.Unlock()

	slot := allocator.slots[SlotForKey(key)]
	slot.Store(key, state)
	slot.count.Store(1)
	allocator.cachedKeys.Store(1)
	return allocator
}

func newAdaptiveKeyState(
	allocator *Allocator,
	cfg AllocatorConfig,
	now time.Time,
) *keyState {
	state := &keyState{
		allocator:  allocator,
		activeStep: 100,
	}
	state.initialized.Store(true)
	state.generation.Store(2)
	state.start.Store(1)
	state.end.Store(100)
	state.resetPrefetchState(2, now, 0, cfg)
	return state
}

type blockingPrefetchStore struct {
	mu      sync.Mutex
	max     int64
	started chan int64
	release chan struct{}
}

func newBlockingPrefetchStore() *blockingPrefetchStore {
	return &blockingPrefetchStore{
		started: make(chan int64, 1),
		release: make(chan struct{}),
	}
}

func (s *blockingPrefetchStore) ReserveRanges(
	ctx context.Context,
	_ ReservationAuthority,
	requests []ReservationRequest,
) ([]SequenceRange, error) {
	select {
	case s.started <- requests[0].Step:
	default:
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.release:
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	start := s.max + 1
	s.max += requests[0].Step
	return []SequenceRange{{Start: start, End: s.max}}, nil
}

func observationAt(key string, id int64, from, to time.Time) LinearizationObservation {
	return LinearizationObservation{
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
	recorder := NewLinearizationRecorder(16)
	start := time.Unix(0, 0)
	for index, id := range []int64{1, 2, 3} {
		requestStart := start.Add(time.Duration(index) * time.Second)
		recorder.Record(
			observationAt("orders", id, requestStart, requestStart.Add(time.Millisecond)),
		)
	}

	counters := recorder.Counters()
	assert.Equal(t, int64(3), counters.Recorded)
	assert.Zero(t, counters.OrderViolations)
	assert.Zero(t, counters.StaleDeliveries)
	assert.Zero(t, counters.DuplicateDeliveries)
}

// TestLinearizationRecorderFlagsAStaleDelivery is the failure this whole design
// exists to prevent: a caller that had already received a larger id receives a
// smaller one afterwards.
func TestLinearizationRecorderFlagsAStaleDelivery(t *testing.T) {
	recorder := NewLinearizationRecorder(16)
	start := time.Unix(0, 0)
	recorder.Record(observationAt("orders", 101, start, start.Add(time.Millisecond)))
	recorder.Record(observationAt(
		"orders", 1,
		start.Add(time.Second), start.Add(time.Second+time.Millisecond),
	))

	counters := recorder.Counters()
	assert.Equal(t, int64(1), counters.StaleDeliveries)
	assert.Equal(t, int64(1), counters.OrderViolations)
	assert.Zero(t, counters.DuplicateDeliveries)
	assert.Contains(t, counters.OrderViolationDetail, "handed out 101 then")
}

// TestLinearizationRecorderFlagsARepeatWithinAKey pins the second of the three
// counters, which a stale check alone would also catch but which means something
// different to whoever reads the alert.
func TestLinearizationRecorderFlagsARepeatWithinAKey(t *testing.T) {
	recorder := NewLinearizationRecorder(16)
	start := time.Unix(0, 0)
	recorder.Record(observationAt("orders", 7, start, start.Add(time.Millisecond)))
	recorder.Record(observationAt(
		"orders", 7,
		start.Add(time.Second), start.Add(time.Second+time.Millisecond),
	))

	counters := recorder.Counters()
	assert.Equal(t, int64(1), counters.DuplicateDeliveries)
	assert.Equal(t, int64(1), counters.OrderViolations)
	assert.Zero(t, counters.StaleDeliveries)
}

// TestLinearizationRecorderIgnoresOverlappingRequests pins section 1.4: two
// requests in flight at once is not a violation, whatever order their responses
// arrive in, because neither had returned when the other started.
func TestLinearizationRecorderIgnoresOverlappingRequests(t *testing.T) {
	recorder := NewLinearizationRecorder(16)
	start := time.Unix(0, 0)
	// The second request starts before the first one finishes, and its id is
	// lower. That is legal: only the caller's own completed-before-started
	// relation makes an order claim.
	recorder.Record(observationAt("orders", 9, start, start.Add(time.Second)))
	recorder.Record(observationAt(
		"orders", 8,
		start.Add(500*time.Millisecond), start.Add(1500*time.Millisecond),
	))

	assert.Zero(t, recorder.Counters().OrderViolations)
}

// TestLinearizationRecorderBoundsItsRing keeps the memory cost fixed: a long run
// keeps the newest observations and the counters, and drops the oldest records.
func TestLinearizationRecorderBoundsItsRing(t *testing.T) {
	recorder := NewLinearizationRecorder(4)
	start := time.Unix(0, 0)
	for index := int64(0); index < 10; index++ {
		requestStart := start.Add(time.Duration(index) * time.Second)
		recorder.Record(observationAt("orders", index+1, requestStart, requestStart))
	}

	observations := recorder.Observations()
	require.Len(t, observations, 4)
	assert.Equal(t, int64(7), observations[0].ID, "the ring keeps the newest records")
	assert.Equal(t, int64(10), observations[3].ID)
	// The counters survive the ring, which is what makes them the alertable part.
	assert.Equal(t, int64(10), recorder.Counters().Recorded)
}

func TestLinearizationRecorderBoundsItsKeyHistory(t *testing.T) {
	recorder := NewLinearizationRecorder(2)
	start := time.Unix(0, 0)
	for index, key := range []string{"a", "b", "a", "c", "b"} {
		at := start.Add(time.Duration(index) * time.Second)
		recorder.Record(observationAt(key, int64(index+1), at, at))
		assert.LessOrEqual(t, len(recorder.lastComplete), 2)
		assert.LessOrEqual(t, recorder.lru.Len(), 2)
	}
	assert.Equal(t, int64(2), recorder.Counters().EvictedKeys)
	assert.Equal(t, int64(5), recorder.Counters().Recorded)
	assert.NotContains(t, recorder.lastComplete, "a")
	assert.Contains(t, recorder.lastComplete, "b")
	assert.Contains(t, recorder.lastComplete, "c")
	assert.Zero(t, recorder.Counters().OrderViolations)
}

func TestLinearizationRecorderDefaultCapacityAndEvictedBaseline(t *testing.T) {
	recorder := NewLinearizationRecorder(0)
	require.Equal(t, MaxLinearizationSamples, recorder.capacity)
	start := time.Unix(0, 0)
	recorder.Record(observationAt("anchor", 2, start, start))
	recorder.Record(observationAt("anchor", 1, start.Add(time.Second), start.Add(time.Second)))
	for index := range MaxLinearizationSamples + 32 {
		at := start.Add(time.Duration(index+2) * time.Second)
		recorder.Record(observationAt(fmt.Sprintf("bounded-key-%d", index), 1, at, at))
	}
	require.Len(t, recorder.ring, MaxLinearizationSamples)
	require.Len(t, recorder.lastComplete, MaxLinearizationSamples)
	require.Equal(t, MaxLinearizationSamples, recorder.lru.Len())
	require.NotContains(t, recorder.lastComplete, "anchor")
	at := start.Add(time.Hour)
	recorder.Record(observationAt("anchor", 1, at, at))
	counters := recorder.Counters()
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
	allocator.linearization = NewLinearizationRecorder(16)
	allocator.Open(1, 0, []uint32{SlotForKey("orders")})
	allocator.ApplyRoute(0)

	for index := 0; index < 2; index++ {
		_, err := allocator.FetchNext(context.Background(), "orders")
		require.NoError(t, err)
	}

	counters, recording := allocator.LinearizationCounters()
	require.True(t, recording)
	assert.Equal(t, int64(2), counters.Recorded)
	assert.Zero(t, counters.OrderViolations)
	observations := allocator.linearization.Observations()
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
