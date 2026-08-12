package biz

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/codesjoy/pkg/basic/xerror"
	"github.com/codesjoy/skuld/gen/go/reason"
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
)

// AllocatorConfig contains immutable range allocation settings.
type AllocatorConfig struct {
	// LegacyStep only detects the removed allocator.step configuration.
	LegacyStep            *int64        `mapstructure:"step"`
	DefaultStep           int64         `mapstructure:"default_step"`
	MaxStep               int64         `mapstructure:"max_step"`
	PrefetchRatio         float64       `mapstructure:"prefetch_ratio"`
	StepIncreaseThreshold time.Duration `mapstructure:"step_increase_threshold"`
	StepDecreaseThreshold time.Duration `mapstructure:"step_decrease_threshold"`
	ReserveTimeout        time.Duration `mapstructure:"reserve_timeout"`
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
	k.lastUsed.Store(now.Unix())
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

	slotsMu sync.RWMutex
	slots   map[uint32]*sync.Map
	version int64

	store SequenceRepo
	cfg   AllocatorConfig
	now   func() time.Time

	prepareApply *PrepareApply

	logger *slog.Logger
}

// NewAllocator constructs a paused allocator with no locally owned slots.
func NewAllocator(cfg *AllocatorConfig, store SequenceRepo, logger *slog.Logger) *Allocator {
	if logger == nil {
		logger = slog.Default()
	}
	allocatorConfig := *cfg
	allocatorConfig.setDefaults()
	obj := &Allocator{
		slots:  make(map[uint32]*sync.Map),
		store:  store,
		cfg:    allocatorConfig,
		now:    time.Now,
		logger: logger,
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
	stateValue, _ := slot.LoadOrStore(key, &keyState{})
	id, err := stateValue.(*keyState).allocate(ctx, obj.store, key, obj.cfg, obj.now)
	if err != nil {
		return 0, err
	}
	return id, nil
}

// CurrentVersion returns the version of the active route.
func (obj *Allocator) CurrentVersion() int64 {
	obj.slotsMu.RLock()
	defer obj.slotsMu.RUnlock()
	return obj.version
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
			delete(obj.slots, slot)
		}
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
		delete(obj.slots, slot)
	}

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
			slog.Any("slots", prepareApply.Slots),
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
		obj.slots[slot] = &sync.Map{}
	}
	obj.version = obj.prepareApply.Version
	obj.logger.Info("apply route change", slog.Int64("version", obj.prepareApply.Version))
	obj.prepareApply = nil
}
