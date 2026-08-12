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
	// DefaultStep is the default number of IDs reserved per range.
	DefaultStep int64 = 100
	MinStep     int64 = 10
	MaxStep     int64 = 1000
)

// AllocatorConfig contains immutable range allocation settings.
type AllocatorConfig struct {
	Step int64 `mapstructure:"step"`
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

type keyState struct {
	next        atomic.Int64
	start       atomic.Int64
	end         atomic.Int64
	generation  atomic.Uint64
	initialized atomic.Bool
	mu          sync.Mutex
	lastUsed    atomic.Int64
}

func (k *keyState) allocate(
	ctx context.Context,
	store SequenceRepo,
	key string,
	step int64,
) (int64, error) {
	if k.initialized.Load() {
		generation := k.generation.Load()
		id := k.next.Add(1)
		if k.inActiveRange(id) && generation == k.generation.Load() {
			k.touch()
			return id, nil
		}
		return k.allocateSlow(ctx, store, key, step, id, generation, true)
	}
	return k.allocateSlow(ctx, store, key, step, 0, 0, false)
}

func (k *keyState) allocateSlow(
	ctx context.Context,
	store SequenceRepo,
	key string,
	step int64,
	candidate int64,
	generation uint64,
	hadRange bool,
) (int64, error) {
	k.mu.Lock()
	defer k.mu.Unlock()

	if k.initialized.Load() {
		currentGeneration := k.generation.Load()
		if hadRange && generation == currentGeneration && k.inActiveRange(candidate) {
			k.touch()
			return candidate, nil
		}

		// A caller that observed an uninitialized or superseded range must
		// discard its optimistic candidate before consuming the active range.
		if !hadRange || generation != currentGeneration || candidate < k.start.Load() {
			candidate = k.next.Add(1)
			if k.inActiveRange(candidate) {
				k.touch()
				return candidate, nil
			}
		}
	}

	reserved, err := store.ReserveRange(ctx, key, step)
	if err != nil {
		return 0, err
	}
	if reserved.Start <= 0 || reserved.End < reserved.Start ||
		reserved.End-reserved.Start+1 != step {
		return 0, fmt.Errorf(
			"reserve sequence range for %q: invalid range [%d,%d]",
			key,
			reserved.Start,
			reserved.End,
		)
	}

	resetCursor := candidate < reserved.Start || candidate > reserved.End
	if resetCursor {
		// Invalidate optimistic readers before publishing a reset cursor.
		k.generation.Add(1)
		candidate = reserved.Start
		k.next.Store(candidate)
	}
	// Publish the bounds after resetting next. Fast-path callers can only
	// consume the new range after both bounds describe it.
	k.start.Store(reserved.Start)
	k.end.Store(reserved.End)
	k.initialized.Store(true)
	k.touch()
	return candidate, nil
}

func (k *keyState) inActiveRange(id int64) bool {
	start := k.start.Load()
	end := k.end.Load()
	return id >= start && id <= end && start == k.start.Load()
}

func (k *keyState) touch() {
	k.lastUsed.Store(time.Now().Unix())
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
	step  int64

	prepareApply *PrepareApply

	logger *slog.Logger
}

// NewAllocator constructs a paused allocator with no locally owned slots.
func NewAllocator(cfg *AllocatorConfig, store SequenceRepo, logger *slog.Logger) *Allocator {
	if logger == nil {
		logger = slog.Default()
	}
	obj := &Allocator{
		slots:  make(map[uint32]*sync.Map),
		store:  store,
		step:   cfg.Step,
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
	id, err := stateValue.(*keyState).allocate(ctx, obj.store, key, obj.step)
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
	}
	for _, slot := range slots {
		if _, ok := obj.slots[slot]; !ok {
			prepareApply.Slots = append(prepareApply.Slots, slot)
		}
	}

	if len(prepareApply.Slots) > 0 {
		obj.prepareApply = prepareApply
	}

	if len(needDel) > 0 || len(prepareApply.Slots) > 0 {
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
