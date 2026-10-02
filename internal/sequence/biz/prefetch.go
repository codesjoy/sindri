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
	"container/list"
	"context"
	"math"
	rand "math/rand/v2"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// ReserveLatencyObserver receives each completed single-key range reservation.
type ReserveLatencyObserver func(duration time.Duration, success bool)

type reserveLatencySample struct {
	at       time.Time
	duration time.Duration
	success  bool
}

type reserveLatencyTracker struct {
	mu sync.Mutex

	window     time.Duration
	minSamples int
	now        func() time.Time

	samples [MaxReserveLatencySamples]reserveLatencySample
	next    int
	count   int

	p99            time.Duration
	trusted        bool
	lastRecomputed time.Time
	updates        int
	observerMu     sync.RWMutex
	observer       ReserveLatencyObserver
}

func newReserveLatencyTracker(
	window time.Duration,
	minSamples int,
	now func() time.Time,
) *reserveLatencyTracker {
	return &reserveLatencyTracker{
		window:     window,
		minSamples: minSamples,
		now:        now,
	}
}

func (t *reserveLatencyTracker) observe(duration time.Duration, success bool) {
	if t == nil {
		return
	}
	if duration < 0 {
		duration = 0
	}
	now := t.now()

	t.mu.Lock()
	t.samples[t.next] = reserveLatencySample{
		at:       now,
		duration: duration,
		success:  success,
	}
	t.next = (t.next + 1) % len(t.samples)
	if t.count < len(t.samples) {
		t.count++
	}
	t.updates++
	if t.lastRecomputed.IsZero() ||
		(!t.trusted && t.count >= t.minSamples) ||
		now.Sub(t.lastRecomputed) >= latencyRecomputeInterval ||
		t.updates >= latencyRecomputeSamples {
		t.recomputeLocked(now)
	}
	t.mu.Unlock()

	t.observerMu.RLock()
	observer := t.observer
	t.observerMu.RUnlock()
	if observer != nil {
		observer(duration, success)
	}
}

func (t *reserveLatencyTracker) setObserver(observer ReserveLatencyObserver) {
	if t == nil {
		return
	}
	t.observerMu.Lock()
	t.observer = observer
	t.observerMu.Unlock()
}

func (t *reserveLatencyTracker) observedP99() time.Duration {
	if t == nil {
		return 0
	}
	now := t.now()
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.lastRecomputed.IsZero() || now.Sub(t.lastRecomputed) >= latencyRecomputeInterval {
		t.recomputeLocked(now)
	}
	if !t.trusted {
		return 0
	}
	return t.p99
}

func (t *reserveLatencyTracker) recomputeLocked(now time.Time) {
	cutoff := now.Add(-t.window)
	values := make([]time.Duration, 0, t.count)
	successes := 0
	for index := range t.count {
		sample := &t.samples[index]
		if sample.at.Before(cutoff) {
			continue
		}
		values = append(values, sample.duration)
		if sample.success {
			successes++
		}
	}
	t.lastRecomputed = now
	t.updates = 0
	if successes < t.minSamples || len(values) == 0 {
		t.p99 = 0
		t.trusted = false
		return
	}
	slices.Sort(values)
	index := (99*len(values)+99)/100 - 1
	t.p99 = values[index]
	t.trusted = true
}

type keyRateEstimator struct {
	generation uint64
	anchorAt   time.Time
	anchorIDs  int64

	observedAt   time.Time
	observedIDs  int64
	observations int
	rate         float64
	ready        bool
}

func (e *keyRateEstimator) reset(
	generation uint64,
	now time.Time,
	consumed int64,
) {
	e.generation = generation
	e.anchorAt = now
	e.anchorIDs = consumed
	e.observedAt = now
	e.observedIDs = consumed
	e.observations = 0
	e.rate = 0
	e.ready = false
}

func (e *keyRateEstimator) observe(
	generation uint64,
	now time.Time,
	consumed int64,
	resetAfter time.Duration,
) (changed, reset bool) {
	if generation != e.generation {
		return false, false
	}
	if !e.observedAt.IsZero() && resetAfter > 0 && now.Sub(e.observedAt) > resetAfter {
		e.reset(generation, now, consumed)
		return true, true
	}
	if consumed <= e.observedIDs {
		return false, false
	}

	e.observedAt = now
	e.observedIDs = consumed
	e.observations++
	elapsed := now.Sub(e.anchorAt)
	if e.observations < 2 || elapsed < rateMinObservation {
		e.ready = false
		return true, false
	}
	elapsedSeconds := elapsed.Seconds()
	if elapsedSeconds <= 0 {
		e.ready = false
		return true, false
	}
	e.rate = float64(consumed-e.anchorIDs) / elapsedSeconds
	e.ready = e.rate > 0 && !math.IsInf(e.rate, 0) && !math.IsNaN(e.rate)
	return true, false
}

func (e *keyRateEstimator) snapshot() (float64, bool) {
	if e == nil || !e.ready || e.rate <= 0 {
		return 0, false
	}
	return e.rate, true
}

func (obj *Allocator) observeReserve(start time.Time, err error) {
	if obj == nil || obj.reserveLatency == nil || start.IsZero() {
		return
	}
	duration := time.Since(start)
	if obj.cfg.ReserveTimeout > 0 && duration > obj.cfg.ReserveTimeout {
		duration = obj.cfg.ReserveTimeout
	}
	obj.reserveLatency.observe(duration, err == nil)
}

// SetReserveLatencyObserver registers a low-cardinality observer for reserve latency.
func (obj *Allocator) SetReserveLatencyObserver(observer ReserveLatencyObserver) {
	if obj == nil || obj.reserveLatency == nil {
		return
	}
	obj.reserveLatency.setObserver(observer)
}

func (obj *Allocator) observedReserveP99() time.Duration {
	if obj == nil || obj.reserveLatency == nil {
		return 0
	}
	return obj.reserveLatency.observedP99()
}

func (obj *Allocator) prefetchLead(cfg AllocatorConfig) time.Duration {
	p99 := time.Duration(0)
	if obj != nil {
		p99 = obj.observedReserveP99()
	}
	if p99 <= 0 {
		p99 = cfg.ReserveTimeout
	}
	lead := time.Duration(float64(p99) * cfg.PrefetchLatencyMultiplier)
	if lead < minPrefetchLead {
		lead = minPrefetchLead
	}
	if cfg.ReserveTimeout > 0 && lead > cfg.ReserveTimeout {
		lead = cfg.ReserveTimeout
	}
	return lead
}

func (obj *Allocator) retryDelay(
	attempt int,
	remaining time.Duration,
	p99 time.Duration,
) time.Duration {
	ceiling := maxRetryBackoff
	if obj != nil && obj.cfg.ReserveTimeout > 0 && obj.cfg.ReserveTimeout < ceiling {
		ceiling = obj.cfg.ReserveTimeout
	}
	if remaining > 0 {
		half := remaining / 2
		if half < minRetryBackoff {
			half = minRetryBackoff
		}
		if half < ceiling {
			ceiling = half
		}
	}
	if ceiling < minRetryBackoff {
		ceiling = minRetryBackoff
	}

	base := p99 / 2
	if base < minRetryBackoff {
		base = minRetryBackoff
	}
	if base > ceiling {
		base = ceiling
	}
	window := base
	for range max(0, attempt-1) {
		if window >= ceiling/2 {
			window = ceiling
			break
		}
		window *= 2
	}
	if window > ceiling {
		window = ceiling
	}

	half := window / 2
	if half <= 0 {
		return window
	}
	random := 0.5
	if obj != nil && obj.randomFloat64 != nil {
		random = obj.randomFloat64()
	}
	if random < 0 {
		random = 0
	}
	if random > 1 {
		random = 1
	}
	return half + time.Duration(float64(half)*random)
}

type retryTimer interface {
	Stop() bool
}

func defaultRetryAfter(delay time.Duration, callback func()) retryTimer {
	return time.AfterFunc(delay, callback)
}

func defaultRandomFloat64() float64 {
	return rand.Float64() //nolint:gosec // Retry jitter is not security-sensitive.
}

func atomicMaxInt64(value *atomic.Int64, candidate int64) bool {
	updated := false
	for current := value.Load(); candidate > current; current = value.Load() {
		if value.CompareAndSwap(current, candidate) {
			updated = true
			break
		}
	}
	return updated
}

func (k *keyState) observeAllocation(
	now time.Time,
	consumed int64,
	blockSize int64,
	generation uint64,
	cfg AllocatorConfig,
) {
	if blockSize <= 0 {
		blockSize = 1
	}
	blockChanged := atomicMaxInt64(&k.recentBlock, blockSize)

	nowUnixNano := now.UnixNano()
	nextSample := nowUnixNano + rateSampleInterval.Nanoseconds()
	sample := false
	for {
		gate := k.rateGate.Load()
		if gate != 0 && nowUnixNano < gate {
			break
		}
		if k.rateGate.CompareAndSwap(gate, nextSample) {
			sample = true
			break
		}
	}
	if !sample && !blockChanged {
		return
	}

	k.rateMu.Lock()
	defer k.rateMu.Unlock()
	changed := blockChanged
	if sample {
		rateChanged, reset := k.rate.observe(
			generation,
			now,
			consumed,
			cfg.PrefetchRateResetAfter,
		)
		if reset {
			k.recentBlock.Store(blockSize)
		}
		changed = rateChanged || changed
	}
	if changed {
		k.configurePrefetchThresholdLocked(cfg)
	}
}

func (k *keyState) resetPrefetchState(
	generation uint64,
	now time.Time,
	consumed int64,
	cfg AllocatorConfig,
) {
	k.rateMu.Lock()
	defer k.rateMu.Unlock()
	k.rate.reset(generation, now, consumed)
	k.prefetchAt.Store(0)
	k.recentBlock.Store(0)
	k.rateGate.Store(0)
	k.configurePrefetchThresholdLocked(cfg)
}

func (k *keyState) configurePrefetchThresholdLocked(cfg AllocatorConfig) {
	if !k.initialized.Load() {
		return
	}
	allowance, _ := k.prefetchAllowanceRateLocked(cfg)
	start := k.start.Load()
	end := k.end.Load()
	size := end - start + 1
	if start <= 0 || size <= 0 {
		return
	}
	if allowance >= size {
		k.prefetchAt.Store(start)
		return
	}
	k.prefetchAt.Store(end - allowance)
}

func (k *keyState) prefetchAllowanceRateLocked(
	cfg AllocatorConfig,
) (int64, bool) {
	start := k.start.Load()
	end := k.end.Load()
	size := end - start + 1
	if start <= 0 || size <= 0 {
		return 1, true
	}

	rate, ready := k.rate.snapshot()
	if !ready {
		consumedThreshold := int64(math.Ceil(float64(size) * cfg.PrefetchRatio))
		if consumedThreshold < 1 {
			consumedThreshold = 1
		}
		if consumedThreshold > size {
			consumedThreshold = size
		}
		return size - consumedThreshold, true
	}

	block := max(int64(1), k.recentBlock.Load())
	lead := k.prefetchLead(cfg)
	leadIDs := int64(math.Ceil(rate * lead.Seconds()))
	if leadIDs < 0 {
		leadIDs = 0
	}
	allowance := leadIDs + block
	if allowance < 1 {
		allowance = 1
	}
	return min(allowance, size), false
}

func (k *keyState) prefetchDecisionLocked(
	id int64,
	cfg AllocatorConfig,
) (bool, bool) {
	start := k.start.Load()
	end := k.end.Load()
	if start <= 0 || end < start || id < start {
		return false, true
	}
	k.rateMu.Lock()
	allowance, fallback := k.prefetchAllowanceRateLocked(cfg)
	k.rateMu.Unlock()
	return end-id <= allowance, fallback
}

func (k *keyState) rateSnapshot() (float64, bool) {
	k.rateMu.Lock()
	defer k.rateMu.Unlock()
	return k.rate.snapshot()
}

func (k *keyState) remainingDuration(
	now time.Time,
	_ AllocatorConfig,
) (time.Duration, bool) {
	rate, ready := k.rateSnapshot()
	if !ready {
		return 0, false
	}
	start := k.start.Load()
	end := k.end.Load()
	size := end - start + 1
	next := k.next.Load()
	remaining := end - next
	if size <= 0 || remaining <= 0 {
		return 0, true
	}
	seconds := float64(remaining) / rate
	if seconds >= float64(math.MaxInt64)/float64(time.Second) {
		return time.Duration(math.MaxInt64), true
	}
	return time.Duration(seconds * float64(time.Second)), true
}

func (k *keyState) prefetchLead(cfg AllocatorConfig) time.Duration {
	if k.allocator != nil {
		return k.allocator.prefetchLead(cfg)
	}
	lead := time.Duration(float64(cfg.ReserveTimeout) * cfg.PrefetchLatencyMultiplier)
	if lead < minPrefetchLead {
		lead = minPrefetchLead
	}
	if cfg.ReserveTimeout > 0 && lead > cfg.ReserveTimeout {
		lead = cfg.ReserveTimeout
	}
	return lead
}

func (k *keyState) observeAllocationReserve(start time.Time, err error) {
	if k == nil || k.allocator == nil {
		return
	}
	k.allocator.observeReserve(start, err)
}

func (k *keyState) launchBackgroundPrefetch(
	scope reservationScope,
	key string,
	fetch *rangeFetch,
	step int64,
	cfg AllocatorConfig,
) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), cfg.ReserveTimeout)
		defer cancel()
		started := time.Now()
		reserved, err := reserveRange(ctx, k.withEpoch(scope, key), key, step)
		k.observeAllocationReserve(started, err)
		now := time.Now
		if k.allocator != nil {
			now = k.allocator.now
		}
		k.completeFetch(fetch, reserved, err, cfg, now(), key)
	}()
}

func (k *keyState) clearRetryLocked() {
	if k.retryTimer != nil {
		k.retryTimer.Stop()
		k.retryTimer = nil
	}
	k.retryAfter = time.Time{}
	k.retryAttempt = 0
}

func (k *keyState) cancelRetry() {
	if k == nil {
		return
	}
	k.retired.Store(true)
	k.mu.Lock()
	k.clearRetryLocked()
	k.mu.Unlock()
}

func (k *keyState) scheduleRetryLocked(key string, cfg AllocatorConfig, now time.Time) {
	if k.retired.Load() || k.allocator == nil || k.allocator.afterFunc == nil {
		return
	}
	k.retryAttempt++
	p99 := k.allocator.observedReserveP99()
	if p99 <= 0 {
		p99 = cfg.ReserveTimeout
	}
	remaining, _ := k.remainingDuration(now, cfg)
	delay := k.allocator.retryDelay(k.retryAttempt, remaining, p99)
	if k.retryTimer != nil {
		k.retryTimer.Stop()
	}
	k.retryAfter = now.Add(delay)
	generation := k.generation.Load()
	k.retryTimer = k.allocator.afterFunc(delay, func() {
		k.allocator.retryPrefetch(key, k, generation)
	})
	k.allocator.prefetchRetries.Add(1)
}

func (obj *Allocator) retryPrefetch(
	key string,
	state *keyState,
	generation uint64,
) {
	if obj == nil || state == nil {
		return
	}
	slotID := SlotForKey(key)
	obj.slotsMu.RLock()
	slot := obj.slots[slotID]
	if slot == nil {
		obj.slotsMu.RUnlock()
		return
	}
	if current, ok := slot.Load(key); !ok || current != state {
		obj.slotsMu.RUnlock()
		return
	}

	now := obj.now()
	state.mu.Lock()
	if state.fetch != nil || state.standby != nil ||
		state.generation.Load() != generation {
		state.retryTimer = nil
		state.mu.Unlock()
		obj.slotsMu.RUnlock()
		return
	}
	if obj.Paused() {
		state.retryTimer = nil
		state.retryAfter = time.Time{}
		state.mu.Unlock()
		obj.slotsMu.RUnlock()
		return
	}
	if !state.retryAfter.IsZero() && now.Before(state.retryAfter) {
		state.mu.Unlock()
		obj.slotsMu.RUnlock()
		return
	}
	if lastUsed := state.lastUsed.Load(); lastUsed > 0 &&
		now.Sub(time.Unix(0, lastUsed)) > obj.cfg.PrefetchRateResetAfter {
		state.clearRetryLocked()
		state.mu.Unlock()
		obj.slotsMu.RUnlock()
		return
	}

	activeSize := state.end.Load() - state.start.Load() + 1
	if activeSize <= 0 {
		state.clearRetryLocked()
		state.mu.Unlock()
		obj.slotsMu.RUnlock()
		return
	}
	step := state.nextStepLocked(activeSize, obj.cfg)
	fetch := &rangeFetch{
		done:       make(chan struct{}),
		background: true,
	}
	state.fetch = fetch
	state.retryTimer = nil
	state.retryAfter = time.Time{}
	obj.prefetchStarted.Add(1)
	state.mu.Unlock()

	state.launchBackgroundPrefetch(obj.scope(), key, fetch, step, obj.cfg)
	obj.slotsMu.RUnlock()
}

// LinearizationObservation is one allocation as it was handed out.
//
// It carries what appendix F.2 asks a test to record, because the property the
// protocol promises is about this sequence and cannot be checked from a single
// process's state: an allocation that hands out a lower id than one a caller
// already received is the violation, and only the sequence shows it.
type LinearizationObservation struct {
	Key              string
	ID               int64
	OwnerInstanceID  string
	Epoch            uint64
	Generation       uint64
	LinearizationSeq uint64
	RequestStart     time.Time
	ResponseReceived time.Time
}

// LinearizationCounters are the three that must stay at zero (appendix F.1).
//
// They are separate counters rather than one because they mean different things
// to whoever has to read them: an ordering violation says the protocol's own
// promise was broken, a duplicate says the same id reached two callers, and a
// stale delivery says an id arrived after a larger one. The third is the mildest
// to read and the most direct symptom of the failure this whole design exists to
// prevent.
type LinearizationCounters struct {
	// EvictedKeys counts forgotten LRU baselines, not ordering violations.
	EvictedKeys          int64
	OrderViolations      int64
	DuplicateDeliveries  int64
	StaleDeliveries      int64
	Recorded             int64
	OrderViolationDetail string
}

// LinearizationRecorder keeps a bounded record of allocations and checks the
// ordering property as they arrive.
//
// The check is the caller's own observable one (section 1.3): if allocation A
// returned before request B started, then B's id must be greater. That is a
// narrower test than a global linearisation order -- two overlapping requests are
// not compared -- but everything it flags is a real violation, and it needs no
// coordination between nodes. Only this process's retained observations are
// compared: the recorder is not a complete cross-node ordering audit.
//
// Recording is off unless a deployment asks for it. The bound is what keeps it
// affordable when it is on: both the observation ring and the per-key LRU are
// bounded by capacity. Eviction forgets a key's baseline; cumulative counters
// survive eviction and ring replacement.
type LinearizationRecorder struct {
	capacity int

	mu           sync.Mutex
	ring         []LinearizationObservation
	next         int
	lastComplete map[string]*list.Element
	lru          list.List
	counters     LinearizationCounters
}

// NewLinearizationRecorder constructs a recorder holding at most capacity
// observations.
func NewLinearizationRecorder(capacity int) *LinearizationRecorder {
	if capacity <= 0 {
		capacity = MaxLinearizationSamples
	}
	return &LinearizationRecorder{
		capacity:     capacity,
		ring:         make([]LinearizationObservation, 0, capacity),
		lastComplete: make(map[string]*list.Element),
	}
}

// Record files one allocation and checks it against the last one for its key that
// had already returned when this request started.
func (r *LinearizationRecorder) Record(observation LinearizationObservation) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.counters.Recorded++

	entry := r.lastComplete[observation.Key]
	if entry != nil &&
		entry.Value.(LinearizationObservation).ResponseReceived.Before(observation.RequestStart) {
		previous := entry.Value.(LinearizationObservation)
		switch {
		case observation.ID < previous.ID:
			r.counters.StaleDeliveries++
			r.counters.OrderViolations++
			r.counters.OrderViolationDetail = detail(previous, observation)
		case observation.ID == previous.ID:
			r.counters.DuplicateDeliveries++
			r.counters.OrderViolations++
			r.counters.OrderViolationDetail = detail(previous, observation)
		}
	}
	if entry == nil {
		if len(r.lastComplete) == r.capacity {
			oldest := r.lru.Back()
			delete(r.lastComplete, oldest.Value.(LinearizationObservation).Key)
			r.lru.Remove(oldest)
			r.counters.EvictedKeys++
		}
		r.lastComplete[observation.Key] = r.lru.PushFront(observation)
	} else {
		if observation.ResponseReceived.After(
			entry.Value.(LinearizationObservation).ResponseReceived,
		) {
			entry.Value = observation
		}
		r.lru.MoveToFront(entry)
	}

	if len(r.ring) < r.capacity {
		r.ring = append(r.ring, observation)
		return
	}
	r.ring[r.next] = observation
	r.next = (r.next + 1) % r.capacity
}

func detail(previous, current LinearizationObservation) string {
	return previous.OwnerInstanceID + " handed out " +
		strconv.FormatInt(previous.ID, 10) + " then " + current.OwnerInstanceID + " handed out " +
		strconv.FormatInt(current.ID, 10) + " for key " + current.Key
}

// Observations returns a copy of the recorded observations, oldest first.
func (r *LinearizationRecorder) Observations() []LinearizationObservation {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	ordered := make([]LinearizationObservation, 0, len(r.ring))
	ordered = append(ordered, r.ring[r.next:]...)
	ordered = append(ordered, r.ring[:r.next]...)
	return ordered
}

// Counters returns the accumulated counters.
func (r *LinearizationRecorder) Counters() LinearizationCounters {
	if r == nil {
		return LinearizationCounters{}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.counters
}
