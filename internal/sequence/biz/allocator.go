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
	"log/slog"
	"math"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/codesjoy/pkg/basic/xerror"
	"github.com/codesjoy/sindri/gen/go/sequence/reason"
)

const (
	// DefaultStep is the default and minimum range size for each key.
	DefaultStep int64 = 100
	// MinStep is the smallest configurable default range size.
	MinStep int64 = 10
	// DefaultMaxStep is the default upper bound for dynamically sized ranges.
	DefaultMaxStep int64 = 10000

	// DefaultPrefetchRatio starts reserving the next range halfway through the active range.
	DefaultPrefetchRatio = 0.5
	// DefaultStepIncreaseThreshold grows ranges expected to be exhausted quickly.
	DefaultStepIncreaseThreshold = 15 * time.Minute
	// DefaultStepDecreaseThreshold shrinks ranges expected to last a long time.
	DefaultStepDecreaseThreshold = 30 * time.Minute
	// DefaultReserveTimeout bounds each asynchronous range reservation.
	DefaultReserveTimeout = time.Second
	// DefaultIdleTimeout evicts key state that has not allocated an ID for a day.
	DefaultIdleTimeout = 24 * time.Hour
	// DefaultCleanupInterval advances incremental idle cleanup once per second.
	DefaultCleanupInterval = time.Second
	// DefaultCleanupSlotsPerRun bounds routing slots scanned in one cleanup pass.
	DefaultCleanupSlotsPerRun = 64
	// DefaultMemoryHighWatermarkRatio leaves headroom below GOMEMLIMIT.
	DefaultMemoryHighWatermarkRatio = 0.9

	maxCleanupCandidates = 1024
)

// AllocatorConfig contains immutable range allocation settings.
type AllocatorConfig struct {
	// LegacyStep only detects the removed allocator.step configuration.
	LegacyStep               *int64        `mapstructure:"step"`
	DefaultStep              int64         `mapstructure:"default_step"`
	MaxStep                  int64         `mapstructure:"max_step"`
	PrefetchRatio            float64       `mapstructure:"prefetch_ratio"`
	StepIncreaseThreshold    time.Duration `mapstructure:"step_increase_threshold"`
	StepDecreaseThreshold    time.Duration `mapstructure:"step_decrease_threshold"`
	ReserveTimeout           time.Duration `mapstructure:"reserve_timeout"`
	IdleTimeout              time.Duration `mapstructure:"idle_timeout"`
	CleanupInterval          time.Duration `mapstructure:"cleanup_interval"`
	CleanupSlotsPerRun       int           `mapstructure:"cleanup_slots_per_run"`
	MemoryHighWatermarkRatio float64       `mapstructure:"memory_high_watermark_ratio"`
}

func (c *AllocatorConfig) setDefaults() {
	if c.DefaultStep == 0 {
		c.DefaultStep = DefaultStep
	}
	if c.MaxStep == 0 {
		c.MaxStep = DefaultMaxStep
	}
	if c.PrefetchRatio == 0 {
		c.PrefetchRatio = DefaultPrefetchRatio
	}
	if c.StepIncreaseThreshold == 0 {
		c.StepIncreaseThreshold = DefaultStepIncreaseThreshold
	}
	if c.StepDecreaseThreshold == 0 {
		c.StepDecreaseThreshold = DefaultStepDecreaseThreshold
	}
	if c.ReserveTimeout == 0 {
		c.ReserveTimeout = DefaultReserveTimeout
	}
	if c.IdleTimeout == 0 {
		c.IdleTimeout = DefaultIdleTimeout
	}
	if c.CleanupInterval == 0 {
		c.CleanupInterval = DefaultCleanupInterval
	}
	if c.CleanupSlotsPerRun == 0 {
		c.CleanupSlotsPerRun = DefaultCleanupSlotsPerRun
	}
	if c.MemoryHighWatermarkRatio == 0 {
		c.MemoryHighWatermarkRatio = DefaultMemoryHighWatermarkRatio
	}
}

const (
	// StatePaused indicates that the node must reject allocations.
	StatePaused uint32 = iota + 1
	// StateReady indicates that the node may allocate owned keys.
	StateReady
)

// SequenceRepo persists ranges reserved for sequence keys.
type SequenceRepo interface {
	ReserveRange(ctx context.Context, key string, step int64) (SequenceRange, error)
}

// MemorySampler reports Go-managed memory and the configured Go memory limit.
type MemorySampler interface {
	MemoryUsage() (managedBytes, limitBytes uint64)
}

// SequenceRange is a database-reserved inclusive range.
type SequenceRange struct {
	Start int64
	End   int64
}

type rangeFetch struct {
	done       chan struct{}
	background bool
	reserved   SequenceRange
	err        error
}

type keyState struct {
	next        atomic.Int64
	start       atomic.Int64
	end         atomic.Int64
	generation  atomic.Uint64
	initialized atomic.Bool
	lastUsed    atomic.Int64
	returned    atomic.Int64

	mu          sync.Mutex
	activeStep  int64
	activatedAt time.Time
	standby     *SequenceRange
	fetch       *rangeFetch
	retryAfter  time.Time
}

func (k *keyState) allocate(
	ctx context.Context,
	store SequenceRepo,
	key string,
	cfg AllocatorConfig,
	now func() time.Time,
) (int64, error) {
	var (
		candidate  int64
		generation uint64
		hadRange   bool
	)
	if k.initialized.Load() {
		hadRange = true
		generation = k.generation.Load()
		candidate = k.next.Add(1)
		if generation%2 == 0 && k.inActiveRange(candidate) &&
			generation == k.generation.Load() {
			k.afterAllocate(store, key, cfg, now, candidate, generation)
			return candidate, nil
		}
	}

	id, idGeneration, err := k.allocateSlow(
		ctx,
		store,
		key,
		cfg,
		now,
		candidate,
		generation,
		hadRange,
	)
	if err != nil {
		return 0, err
	}
	k.afterAllocate(store, key, cfg, now, id, idGeneration)
	return id, nil
}

func (k *keyState) allocateSlow(
	ctx context.Context,
	store SequenceRepo,
	key string,
	cfg AllocatorConfig,
	now func() time.Time,
	candidate int64,
	generation uint64,
	hadRange bool,
) (int64, uint64, error) {
	for {
		k.mu.Lock()
		if k.initialized.Load() {
			currentGeneration := k.generation.Load()
			if hadRange && generation == currentGeneration && k.inActiveRange(candidate) {
				k.touch(now())
				k.mu.Unlock()
				return candidate, currentGeneration, nil
			}

			candidate = k.next.Add(1)
			if k.inActiveRange(candidate) {
				k.touch(now())
				k.mu.Unlock()
				return candidate, currentGeneration, nil
			}
		}

		if k.standby != nil {
			id, nextGeneration := k.activateStandbyLocked(now())
			k.mu.Unlock()
			return id, nextGeneration, nil
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
			step = k.nextStepLocked(now(), activeSize, activeSize, cfg)
		}
		fetch := &rangeFetch{done: make(chan struct{})}
		k.fetch = fetch
		k.mu.Unlock()

		reserved, err := reserveRange(ctx, store, key, step)
		k.completeFetch(fetch, reserved, err, cfg, now())
		if err != nil {
			return 0, 0, err
		}
	}
}

func (k *keyState) afterAllocate(
	store SequenceRepo,
	key string,
	cfg AllocatorConfig,
	now func() time.Time,
	id int64,
	generation uint64,
) {
	k.recordReturned(id)
	k.touch(now())
	start := k.start.Load()
	end := k.end.Load()
	if start <= 0 || end < start {
		return
	}
	consumed := id - start + 1
	size := end - start + 1
	if consumed <= 0 || float64(consumed)/float64(size) < cfg.PrefetchRatio {
		return
	}

	k.mu.Lock()
	if generation != k.generation.Load() || k.standby != nil || k.fetch != nil {
		k.mu.Unlock()
		return
	}
	currentTime := now()
	if currentTime.Before(k.retryAfter) {
		k.mu.Unlock()
		return
	}
	currentStart := k.start.Load()
	currentEnd := k.end.Load()
	currentConsumed := id - currentStart + 1
	currentSize := currentEnd - currentStart + 1
	if currentConsumed <= 0 || currentSize <= 0 ||
		float64(currentConsumed)/float64(currentSize) < cfg.PrefetchRatio {
		k.mu.Unlock()
		return
	}
	step := k.nextStepLocked(currentTime, currentConsumed, currentSize, cfg)
	fetch := &rangeFetch{
		done:       make(chan struct{}),
		background: true,
	}
	k.fetch = fetch
	k.mu.Unlock()

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), cfg.ReserveTimeout)
		defer cancel()
		reserved, err := reserveRange(ctx, store, key, step)
		k.completeFetch(fetch, reserved, err, cfg, now())
	}()
}

func (k *keyState) nextStepLocked(
	now time.Time,
	consumed int64,
	size int64,
	cfg AllocatorConfig,
) int64 {
	step := k.activeStep
	if step < cfg.DefaultStep {
		step = cfg.DefaultStep
	}
	consumedRatio := float64(consumed) / float64(size)
	estimatedDuration := time.Duration(float64(now.Sub(k.activatedAt)) / consumedRatio)
	switch {
	case estimatedDuration <= cfg.StepIncreaseThreshold:
		if step >= cfg.MaxStep/2 {
			return cfg.MaxStep
		}
		return step * 2
	case estimatedDuration >= cfg.StepDecreaseThreshold:
		step /= 2
		if step < cfg.DefaultStep {
			return cfg.DefaultStep
		}
		return step
	default:
		return step
	}
}

func (k *keyState) completeFetch(
	fetch *rangeFetch,
	reserved SequenceRange,
	err error,
	cfg AllocatorConfig,
	now time.Time,
) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.fetch != fetch {
		return
	}
	fetch.reserved = reserved
	fetch.err = err
	if err == nil {
		k.standby = &fetch.reserved
		k.retryAfter = time.Time{}
	} else if fetch.background {
		k.retryAfter = now.Add(cfg.ReserveTimeout)
	}
	k.fetch = nil
	close(fetch.done)
}

func (k *keyState) activateStandbyLocked(now time.Time) (int64, uint64) {
	reserved := *k.standby
	k.standby = nil
	// Odd generations prevent optimistic readers from consuming partially
	// published bounds while the active range is changing.
	k.generation.Add(1)
	k.next.Store(reserved.Start)
	k.activeStep = reserved.End - reserved.Start + 1
	k.activatedAt = now
	k.start.Store(reserved.Start)
	k.end.Store(reserved.End)
	generation := k.generation.Add(1)
	k.initialized.Store(true)
	k.touch(now)
	return reserved.Start, generation
}

func reserveRange(
	ctx context.Context,
	store SequenceRepo,
	key string,
	step int64,
) (SequenceRange, error) {
	reserved, err := store.ReserveRange(ctx, key, step)
	if err != nil {
		return SequenceRange{}, err
	}
	if reserved.Start <= 0 || reserved.End < reserved.Start ||
		reserved.End-reserved.Start+1 != step {
		return SequenceRange{}, fmt.Errorf(
			"reserve sequence range for %q: invalid range [%d,%d]",
			key,
			reserved.Start,
			reserved.End,
		)
	}
	return reserved, nil
}

func (k *keyState) inActiveRange(id int64) bool {
	start := k.start.Load()
	end := k.end.Load()
	return id >= start && id <= end && start == k.start.Load()
}

func (k *keyState) touch(now time.Time) {
	k.lastUsed.Store(now.UnixNano())
}

func (k *keyState) recordReturned(id int64) {
	for current := k.returned.Load(); id > current; current = k.returned.Load() {
		if k.returned.CompareAndSwap(current, id) {
			return
		}
	}
}

type cleanupCandidate struct {
	slotID uint32
	slot   *allocationSlot
	key    string
	state  *keyState
}

type allocationSlot struct {
	missMu sync.Mutex
	states sync.Map
	count  atomic.Int64
}

func (s *allocationSlot) Load(key string) (*keyState, bool) {
	value, ok := s.states.Load(key)
	if !ok {
		return nil, false
	}
	return value.(*keyState), true
}

func (s *allocationSlot) Store(key string, state *keyState) {
	s.states.Store(key, state)
}

func (s *allocationSlot) Delete(key string) { s.states.Delete(key) }

func (s *allocationSlot) Range(f func(key, value any) bool) { s.states.Range(f) }

// AllocatorStats is a low-cardinality snapshot for allocator telemetry.
type AllocatorStats struct {
	CachedKeys        int64
	AdmissionRejected int64
	CleanupScanned    int64
	CleanupEvicted    int64
}

type cleanupStats struct {
	scanned          int
	evicted          int
	inflight         int
	discardedActive  int64
	discardedStandby int64
}

// PrepareApply describes a route update scheduled for a future tick.
type PrepareApply struct {
	Version   int64
	ApplyTick int64
	Slots     []uint32
}

// Allocator allocates monotonically increasing IDs from reserved ranges.
type Allocator struct {
	state atomic.Uint32

	slotsMu       sync.RWMutex
	slots         map[uint32]*allocationSlot
	version       int64
	versionCh     chan struct{}
	cleanupSlots  []uint32
	cleanupCursor int

	store         SequenceRepo
	cfg           AllocatorConfig
	now           func() time.Time
	memorySampler MemorySampler

	prepareApply      *PrepareApply
	lastCleanup       atomic.Int64
	cachedKeys        atomic.Int64
	admissionRejected atomic.Int64
	cleanupScanned    atomic.Int64
	cleanupEvicted    atomic.Int64

	logger *slog.Logger
}

// NewAllocator constructs a paused allocator with no locally owned slots.
func NewAllocator(
	cfg *AllocatorConfig,
	store SequenceRepo,
	memorySampler MemorySampler,
	logger *slog.Logger,
) *Allocator {
	if logger == nil {
		logger = slog.Default()
	}
	if memorySampler == nil {
		panic("sequence allocator memory sampler is required")
	}
	allocatorConfig := *cfg
	allocatorConfig.setDefaults()
	obj := &Allocator{
		slots:         make(map[uint32]*allocationSlot),
		versionCh:     make(chan struct{}),
		store:         store,
		cfg:           allocatorConfig,
		now:           time.Now,
		memorySampler: memorySampler,
		logger:        logger,
	}
	obj.state.Store(StatePaused)
	return obj
}

// FetchNext returns the next ID for a locally owned key.
func (obj *Allocator) FetchNext(ctx context.Context, key string) (int64, error) {
	obj.slotsMu.RLock()
	defer obj.slotsMu.RUnlock()
	if obj.Paused() {
		return 0, xerror.NewWithReason(
			reason.Reason_SEQUENCE_ALLOCATOR_PAUSED,
			"allocator is already paused",
			nil,
		)
	}

	slot, ok := obj.slots[SlotForKey(key)]
	if !ok {
		return 0, xerror.NewWithReason(reason.Reason_SEQUENCE_SLOT_NOT_OWNER, "slot not found", nil)
	}
	state, ok := slot.Load(key)
	if !ok {
		var err error
		state, err = obj.loadOrCreateState(key, slot)
		if err != nil {
			return 0, err
		}
	}
	id, err := state.allocate(ctx, obj.store, key, obj.cfg, obj.now)
	if err != nil {
		return 0, err
	}
	return id, nil
}

func (obj *Allocator) loadOrCreateState(key string, slot *allocationSlot) (*keyState, error) {
	slot.missMu.Lock()
	defer slot.missMu.Unlock()
	if state, ok := slot.Load(key); ok {
		return state, nil
	}
	if obj.memoryHighWatermarkReached() {
		obj.admissionRejected.Add(1)
		return nil, xerror.NewWithReason(
			reason.Reason_SEQUENCE_CAPACITY_EXHAUSTED,
			"sequence allocator memory capacity is exhausted",
			nil,
		)
	}
	state := &keyState{}
	slot.Store(key, state)
	slot.count.Add(1)
	obj.cachedKeys.Add(1)
	return state, nil
}

func (obj *Allocator) memoryHighWatermarkReached() bool {
	managed, limit := obj.memorySampler.MemoryUsage()
	if limit == 0 || limit >= math.MaxInt64 {
		return false
	}
	return float64(managed) >= float64(limit)*obj.cfg.MemoryHighWatermarkRatio
}

func (obj *Allocator) cleanupIdle() {
	now := obj.now()
	lastCleanup := obj.lastCleanup.Load()
	if lastCleanup != 0 && now.Sub(time.Unix(0, lastCleanup)) < obj.cfg.CleanupInterval {
		return
	}
	if !obj.lastCleanup.CompareAndSwap(lastCleanup, now.UnixNano()) {
		return
	}

	cutoff := now.Add(-obj.cfg.IdleTimeout).UnixNano()
	candidates, scanned := obj.collectIdleCandidates(cutoff)
	stats := obj.evictIdleCandidates(candidates, cutoff)
	stats.scanned = scanned
	obj.cleanupScanned.Add(int64(scanned))
	obj.cleanupEvicted.Add(int64(stats.evicted))
	if stats.evicted > 0 || stats.inflight > 0 ||
		stats.discardedActive > 0 || stats.discardedStandby > 0 {
		obj.logger.Info(
			"cleaned idle sequence key state",
			slog.Int("scanned", stats.scanned),
			slog.Int("evicted", stats.evicted),
			slog.Int("inflight", stats.inflight),
			slog.Int64("discarded_active", stats.discardedActive),
			slog.Int64("discarded_standby", stats.discardedStandby),
		)
	}
}

func (obj *Allocator) collectIdleCandidates(cutoff int64) ([]cleanupCandidate, int) {
	obj.slotsMu.Lock()
	count := min(obj.cfg.CleanupSlotsPerRun, len(obj.cleanupSlots))
	slots := make([]cleanupCandidate, 0, count)
	candidateCapacity := 0
	for range count {
		if obj.cleanupCursor >= len(obj.cleanupSlots) {
			obj.cleanupCursor = 0
		}
		slotID := obj.cleanupSlots[obj.cleanupCursor]
		obj.cleanupCursor++
		if slot := obj.slots[slotID]; slot != nil {
			slots = append(slots, cleanupCandidate{slotID: slotID, slot: slot})
			candidateCapacity += int(min(slot.count.Load(), maxCleanupCandidates))
			candidateCapacity = min(candidateCapacity, maxCleanupCandidates)
		}
	}
	obj.slotsMu.Unlock()

	candidates := make([]cleanupCandidate, 0, candidateCapacity)
	scanned := 0
	for _, ownedSlot := range slots {
		ownedSlot.slot.Range(func(key, value any) bool {
			scanned++
			state, ok := value.(*keyState)
			if !ok || state.lastUsed.Load() > cutoff {
				return true
			}
			keyString, ok := key.(string)
			if !ok {
				return true
			}
			candidates = append(candidates, cleanupCandidate{
				slotID: ownedSlot.slotID,
				slot:   ownedSlot.slot,
				key:    keyString,
				state:  state,
			})
			return len(candidates) < maxCleanupCandidates
		})
		if len(candidates) >= maxCleanupCandidates {
			break
		}
	}
	return candidates, scanned
}

func (obj *Allocator) evictIdleCandidates(
	candidates []cleanupCandidate,
	cutoff int64,
) cleanupStats {
	var stats cleanupStats
	for _, candidate := range candidates {
		obj.slotsMu.Lock()
		currentSlot, owned := obj.slots[candidate.slotID]
		if !owned || currentSlot != candidate.slot {
			obj.slotsMu.Unlock()
			continue
		}
		currentState, loaded := currentSlot.Load(candidate.key)
		if !loaded || currentState != candidate.state || candidate.state.lastUsed.Load() > cutoff {
			obj.slotsMu.Unlock()
			continue
		}

		candidate.state.mu.Lock()
		if candidate.state.lastUsed.Load() > cutoff {
			candidate.state.mu.Unlock()
			obj.slotsMu.Unlock()
			continue
		}
		if candidate.state.fetch != nil {
			stats.inflight++
			candidate.state.mu.Unlock()
			obj.slotsMu.Unlock()
			continue
		}

		if candidate.state.initialized.Load() {
			returned := candidate.state.returned.Load()
			start := candidate.state.start.Load()
			end := candidate.state.end.Load()
			if returned < start {
				returned = start - 1
			}
			if returned < end {
				stats.discardedActive += end - returned
			}
		}
		if candidate.state.standby != nil {
			stats.discardedStandby += candidate.state.standby.End -
				candidate.state.standby.Start + 1
		}
		currentSlot.Delete(candidate.key)
		currentSlot.count.Add(-1)
		obj.cachedKeys.Add(-1)
		stats.evicted++
		candidate.state.mu.Unlock()
		obj.slotsMu.Unlock()
	}
	return stats
}

// CurrentVersion returns the version of the active route.
func (obj *Allocator) CurrentVersion() int64 {
	obj.slotsMu.RLock()
	defer obj.slotsMu.RUnlock()
	return obj.version
}

// WaitForVersion waits until the allocator has applied at least version.
func (obj *Allocator) WaitForVersion(ctx context.Context, version int64) error {
	for {
		obj.slotsMu.RLock()
		current := obj.version
		changed := obj.versionCh
		obj.slotsMu.RUnlock()
		if current >= version {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}

// Stats returns allocator telemetry.
func (obj *Allocator) Stats() AllocatorStats {
	return AllocatorStats{
		CachedKeys:        obj.cachedKeys.Load(),
		AdmissionRejected: obj.admissionRejected.Load(),
		CleanupScanned:    obj.cleanupScanned.Load(),
		CleanupEvicted:    obj.cleanupEvicted.Load(),
	}
}

// Pause disables allocation while retaining already reserved local ranges.
func (obj *Allocator) Pause() {
	obj.state.Store(StatePaused)
}

// Paused reports whether allocation is currently disabled.
func (obj *Allocator) Paused() bool { return obj.state.Load() == StatePaused }

// Open schedules a route for activation while reopening allocation.
func (obj *Allocator) Open(version int64, applyTick int64, slots []uint32) {
	obj.slotsMu.Lock()
	defer obj.slotsMu.Unlock()
	if version > obj.version {
		for slot := range obj.slots {
			obj.cachedKeys.Add(-obj.slots[slot].count.Load())
			delete(obj.slots, slot)
		}
		obj.rebuildCleanupSlotsLocked()
	}
	obj.commitRoute(version, applyTick, slots)
	obj.state.Store(StateReady)
}

// CommitRoute schedules a route for activation on the next allocation tick.
func (obj *Allocator) CommitRoute(version int64, applyTick int64, slots []uint32) {
	obj.slotsMu.Lock()
	defer obj.slotsMu.Unlock()
	obj.commitRoute(version, applyTick, slots)
}

func (obj *Allocator) commitRoute(version int64, applyTick int64, slots []uint32) {
	needDel := make([]uint32, 0, len(obj.slots))
	for slot := range obj.slots {
		if slices.Index(slots, slot) == -1 {
			needDel = append(needDel, slot)
		}
	}
	for _, slot := range needDel {
		obj.cachedKeys.Add(-obj.slots[slot].count.Load())
		delete(obj.slots, slot)
	}
	obj.rebuildCleanupSlotsLocked()

	prepareApply := &PrepareApply{
		Version:   version,
		ApplyTick: applyTick,
		Slots:     []uint32{},
	}
	for _, slot := range slots {
		if _, ok := obj.slots[slot]; !ok {
			prepareApply.Slots = append(prepareApply.Slots, slot)
		}
	}

	// A route version must be applied even when this node's slot set is
	// unchanged (or empty). Consumers use the allocator version as the
	// handoff barrier, so leaving prepareApply unset would make them wait
	// forever after skipping an intermediate route snapshot.
	if version > obj.version {
		obj.prepareApply = prepareApply
		obj.logger.Info(
			"slot change",
			slog.Int64("version", version),
			slog.Int("new_slot_count", len(prepareApply.Slots)),
		)
	}
}

// ApplyRoute atomically replaces the active route and clears drain markers.
func (obj *Allocator) ApplyRoute(tick int64) {
	obj.slotsMu.RLock()
	if obj.prepareApply == nil || obj.prepareApply.ApplyTick > tick {
		obj.slotsMu.RUnlock()
		return
	}
	obj.slotsMu.RUnlock()
	obj.slotsMu.Lock()
	defer obj.slotsMu.Unlock()
	if obj.prepareApply == nil || obj.prepareApply.ApplyTick > tick {
		return
	}
	for _, slot := range obj.prepareApply.Slots {
		obj.slots[slot] = &allocationSlot{}
	}
	obj.version = obj.prepareApply.Version
	obj.rebuildCleanupSlotsLocked()
	close(obj.versionCh)
	obj.versionCh = make(chan struct{})
	obj.logger.Info("apply route change", slog.Int64("version", obj.prepareApply.Version))
	obj.prepareApply = nil
}

func (obj *Allocator) rebuildCleanupSlotsLocked() {
	obj.cleanupSlots = obj.cleanupSlots[:0]
	for slot := range obj.slots {
		obj.cleanupSlots = append(obj.cleanupSlots, slot)
	}
	sort.Slice(obj.cleanupSlots, func(i, j int) bool {
		return obj.cleanupSlots[i] < obj.cleanupSlots[j]
	})
	if obj.cleanupCursor >= len(obj.cleanupSlots) {
		obj.cleanupCursor = 0
	}
}
