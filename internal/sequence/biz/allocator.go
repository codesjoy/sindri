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

// Package biz defines the sequence domain: range allocation, slot ownership,
// placement planning, and the repository interfaces persistence implements.

package biz

import (
	"context"
	"log/slog"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

const (
	// MinStep is the smallest configurable default range size.
	MinStep int64 = 10

	maxCleanupCandidates = 1024
	// MaxReserveLatencySamples bounds the process-local latency sample ring.
	MaxReserveLatencySamples = 1024
	// MaxLinearizationSamples bounds the appendix F.2 allocation record. It is a
	// ring, so a long run costs a fixed amount of memory and the counters are what
	// survive it.
	MaxLinearizationSamples  = 4096
	rateSampleInterval       = 10 * time.Millisecond
	rateMinObservation       = 100 * time.Millisecond
	latencyRecomputeInterval = time.Second
	latencyRecomputeSamples  = 32
	minPrefetchLead          = time.Millisecond
	minRetryBackoff          = 5 * time.Millisecond
	maxRetryBackoff          = 250 * time.Millisecond
)

const (
	// StatePaused indicates that the node must reject allocations.
	StatePaused uint32 = iota + 1
	// StateReady indicates that the node may allocate owned keys.
	StateReady
)

// SequenceRepo persists ranges reserved for sequence keys.
type SequenceRepo interface {
	// ReserveRanges atomically advances the high watermark of every requested
	// key and returns the reserved ranges. Every involved slot must still be
	// held by the presented authority when the transaction runs (section 6.1);
	// a result that cannot be confirmed must be reported as ErrCommitUncertain
	// rather than inferred from a later read.
	ReserveRanges(
		ctx context.Context,
		authority ReservationAuthority,
		requests []ReservationRequest,
	) ([]SequenceRange, error)
}

// reservationScope is the store and authority a reservation runs under. It
// replaces the bare store parameter so a reservation can never be issued
// without stating the authority it claims.
type reservationScope struct {
	store     SequenceRepo
	authority ReservationAuthority
}

// ReservationRequest asks the repository to reserve a range for one key.
type ReservationRequest struct {
	Key  string
	Step int64
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

// AllocatorStats is a low-cardinality snapshot for allocator telemetry.
type AllocatorStats struct {
	CachedKeys        int64
	AdmissionRejected int64
	CleanupScanned    int64
	CleanupEvicted    int64
	PrefetchStarted   int64
	PrefetchSucceeded int64
	PrefetchFailed    int64
	PrefetchRetries   int64
	PrefetchFallback  int64
	ReserveLatencyP99 time.Duration
}

// PrepareApply describes a route update scheduled for a future tick.
type PrepareApply struct {
	Version   int64
	ApplyTick int64
	Slots     []uint32
	// Generation identifies this plan among the ones computed for the same
	// version. A local replan may change the slot set without changing the
	// published revision, so the version alone cannot tell a claim that
	// belonged to a superseded plan from one that belongs to the current one.
	Generation uint64
}

// Allocator allocates monotonically increasing IDs from reserved ranges.
type Allocator struct {
	state atomic.Uint32
	// stopping latches once the shutdown hook has run. It is separate from
	// state because the two answer different questions: state says whether
	// allocation is currently permitted, which a storage blip can change and
	// change back, while stopping says this process is on its way out, which
	// nothing reverses.
	stopping atomic.Bool
	// linearization records handed-out allocations when a deployment asks for it,
	// and is nil otherwise. Its counters are the three that must stay at zero.
	linearization    *LinearizationRecorder
	linearizationSeq atomic.Uint64
	// The appendix E counters. They are plain atomics because each one is
	// incremented on a path that already holds whatever lock it needs, and the
	// metrics layer reads them without touching the allocator's locks.
	takeoversGranted atomic.Int64
	takeoversRefused atomic.Int64
	epochChanges     atomic.Int64
	releasesReleased atomic.Int64
	releasesFailed   atomic.Int64
	drains           atomic.Int64
	lastDrainMicros  atomic.Int64
	lastPauseMicros  atomic.Int64
	// fenced latches once a platform contract violation has been observed, and
	// holds the reason. It is separate from stopping and from state because it is
	// neither a transient condition nor an orderly exit: it says the bounds this
	// instance's decisions rested on are no longer known to have held, which
	// nothing in this process can undo.
	fenced atomic.Pointer[string]

	slotsMu       sync.RWMutex
	slots         map[uint32]*allocationSlot
	version       int64
	versionCh     chan struct{}
	cleanupSlots  []uint32
	cleanupCursor int

	store          SequenceRepo
	ownership      OwnershipRepo
	instanceID     string
	cfg            AllocatorConfig
	ha             HAConfig
	nodeID         string
	now            func() time.Time
	memorySampler  MemorySampler
	reserveLatency *reserveLatencyTracker
	afterFunc      func(time.Duration, func()) retryTimer
	randomFloat64  func() float64

	prepareApply *PrepareApply
	// planGeneration counts the local plans computed for this instance. It is
	// what tells a claim that belongs to the plan in force from one that
	// belonged to a plan a later heartbeat already replaced; both may carry the
	// same published version, so the version cannot answer that question.
	planGeneration    uint64
	claimRetryAfter   atomic.Int64
	lastCleanup       atomic.Int64
	cachedKeys        atomic.Int64
	admissionRejected atomic.Int64
	cleanupScanned    atomic.Int64
	cleanupEvicted    atomic.Int64
	prefetchStarted   atomic.Int64
	prefetchSucceeded atomic.Int64
	prefetchFailed    atomic.Int64
	prefetchRetries   atomic.Int64
	prefetchFallback  atomic.Int64

	// monoNow is the process-anchored monotonic source for every local-lease
	// deadline and pause measurement. Wall time is never used for either. It is
	// a field rather than a method call so a test can drive a process pause,
	// mirroring the wall-clock now seam above.
	monoNow func() int64
	// leaseExpired counts allocations refused because the local lease lapsed.
	leaseExpired atomic.Int64
	// pauseViolations counts linearisations that outran MaxPause. Any non-zero
	// value means the platform broke the bound the local lease was sized for.
	pauseViolations atomic.Int64
	// renewalSucceeded and renewalFailed count renewal rounds per slot.
	renewalSucceeded atomic.Int64
	renewalFailed    atomic.Int64
	// renewalObserver receives the duration and size of each grouped renewal
	// round trip. It is nil unless a deployment asked for metrics.
	renewalObserver atomic.Pointer[RenewalBatchObserver]
	// gateFenced counts slots closed by a local fence rather than by a route
	// change.
	gateFenced atomic.Int64

	logger *slog.Logger
}

// NewAllocator constructs a paused allocator with no locally owned slots.
//
// ownership may be nil, which disables the storage authority fence entirely;
// production wiring always supplies it. The instance
// identity is generated here and is unique per process start, which is what
// makes it a valid fencing identity (section 5.1).
func NewAllocator(
	cfg DataPlaneConfig,
	store SequenceRepo,
	ownership OwnershipRepo,
	memorySampler MemorySampler,
	logger *slog.Logger,
) *Allocator {
	if logger == nil {
		logger = slog.Default()
	}
	if memorySampler == nil {
		panic("sequence allocator memory sampler is required")
	}
	obj := &Allocator{
		slots:         make(map[uint32]*allocationSlot),
		versionCh:     make(chan struct{}),
		store:         store,
		ownership:     ownership,
		instanceID:    uuid.NewString(),
		cfg:           cfg.Allocator,
		ha:            cfg.HA,
		nodeID:        cfg.Node.ID,
		now:           time.Now,
		memorySampler: memorySampler,
		reserveLatency: newReserveLatencyTracker(
			cfg.Allocator.PrefetchLatencyWindow,
			cfg.Allocator.PrefetchLatencyMinSamples,
			time.Now,
		),
		afterFunc:     defaultRetryAfter,
		randomFloat64: defaultRandomFloat64,
		logger:        logger,
	}
	obj.monoNow = newMonotonicClock().now
	obj.state.Store(StatePaused)
	if cfg.HA.LinearizationRecording {
		// Off by default: a record per allocation is not something the hot path
		// carries unless a deployment has asked to prove the ordering property
		// from its own traffic rather than from a test (appendix F.2).
		obj.linearization = NewLinearizationRecorder(MaxLinearizationSamples)
	}
	return obj
}

// LinearizationCounters returns the appendix F.1 counters and whether recording
// is on. The second result matters as much as the first: without it a zero would
// read as "checked and clean" rather than "not checked at all".
func (obj *Allocator) LinearizationCounters() (LinearizationCounters, bool) {
	if obj.linearization == nil {
		return LinearizationCounters{}, false
	}
	return obj.linearization.Counters(), true
}

// recordLinearization files a handed-out allocation when recording is on.
//
// The interval it records is the one this process can see: the request arriving
// and the response leaving. That is narrower than the caller's own observable
// window, so it only flags violations that happened strictly inside this
// process's view, and everything it flags is a real one.
func (obj *Allocator) recordLinearization(
	key string,
	state *keyState,
	allocation SequenceAllocation,
	started time.Time,
) {
	if obj.linearization == nil {
		return
	}
	observation := LinearizationObservation{
		Key:              key,
		ID:               allocation.ID,
		OwnerInstanceID:  obj.instanceID,
		Epoch:            allocation.SlotEpoch,
		LinearizationSeq: obj.linearizationSeq.Add(1),
		RequestStart:     started,
		ResponseReceived: obj.now(),
	}
	if state != nil {
		observation.Generation = state.generation.Load()
	}
	obj.linearization.Record(observation)
}

// InstanceID returns the process-start identity presented as owner_instance_id.
func (obj *Allocator) InstanceID() string { return obj.instanceID }

// Stats returns allocator telemetry.
func (obj *Allocator) Stats() AllocatorStats {
	return AllocatorStats{
		CachedKeys:        obj.cachedKeys.Load(),
		AdmissionRejected: obj.admissionRejected.Load(),
		CleanupScanned:    obj.cleanupScanned.Load(),
		CleanupEvicted:    obj.cleanupEvicted.Load(),
		PrefetchStarted:   obj.prefetchStarted.Load(),
		PrefetchSucceeded: obj.prefetchSucceeded.Load(),
		PrefetchFailed:    obj.prefetchFailed.Load(),
		PrefetchRetries:   obj.prefetchRetries.Load(),
		PrefetchFallback:  obj.prefetchFallback.Load(),
		ReserveLatencyP99: obj.observedReserveP99(),
	}
}

// Pause disables allocation while retaining already reserved local ranges.
func (obj *Allocator) Pause() {
	obj.state.Store(StatePaused)
}

// Paused reports whether allocation is currently disabled.
//
// A fenced instance is paused by definition, and not merely because Fence calls
// Pause: the pause state alone is not durable enough to carry a fence. Open,
// which is how applying a route reopens allocation, stores StateReady
// unconditionally, so an instance that relied on Pause() would resume serving on
// the next route it applied -- which for a fenced instance is a heartbeat away,
// since the heartbeat reloads the cached route whenever it finds the allocator
// paused. Reading the fence here is what makes every Paused() guard on the
// serving path agree with the readiness report that says this instance is out of
// service.
func (obj *Allocator) Paused() bool {
	return obj.state.Load() == StatePaused || obj.fenced.Load() != nil
}

// Readiness reports whether this instance should receive traffic, and why.
//
// The reason is meant to be read by whoever is looking at a failing probe, so
// it names the state rather than the counter behind it.
type Readiness struct {
	// Ready is true when the instance can be sent requests.
	Ready bool
	// Reason is the state name: serving, initializing, stopping or fenced.
	Reason string
}

// Readiness implements the section D.3 readiness signal.
//
// Three states are unready, and each is a state a restart repairs or a human
// must look at: an instance that has never received a route, one that has begun
// to stop, and one whose platform contract was violated. A storage blip and
// holding zero slots are deliberately *ready*. The protocol already fails those
// requests closed with their own retriable reason, and a restart cannot improve
// either condition, so reporting them unready would turn a recoverable condition
// into a rolling restart without making anything safer.
//
// The fenced state is the exception among the three, and it is the reason this
// method reads it: once the storage clock has moved past its asserted bound, the
// bounds every earlier decision rested on are no longer known to have held, so
// the instance must leave the service rather than keep answering from them.
//
// A route version of zero means no route has ever been applied. Route versions
// come from the directory revision, which starts at one, so this cannot be
// confused with a deployment that legitimately serves nothing.
func (obj *Allocator) Readiness() Readiness {
	// The fence is reported first because it is the one unready state of the three
	// that a restart does not clear and only a human can act on, and because an
	// instance can be both fenced and stopping: a shutdown would otherwise hide
	// the fence behind a "stopping" that reads like an ordinary rollout.
	if _, fenced := obj.Fenced(); fenced {
		return Readiness{Reason: "fenced"}
	}
	if obj.stopping.Load() {
		return Readiness{Reason: "stopping"}
	}
	obj.slotsMu.RLock()
	version := obj.version
	obj.slotsMu.RUnlock()
	if version == 0 {
		return Readiness{Reason: "initializing"}
	}
	return Readiness{Ready: true, Reason: "serving"}
}

// Fence stops this instance from serving and records why.
//
// It is the instance-level counterpart of the per-slot gate, and it exists for
// failures that no retry of one request can repair: a storage clock that moved
// past its asserted bound invalidates the bounds every allocation was made
// under, so no slot is safe to serve from, not merely the one that noticed.
//
// The state is set once and never cleared. The instance cannot reconstruct which
// of its earlier decisions were made while the bound still held, so clearing it
// would be asserting something it does not know; the process leaves the service
// until a human has looked at the platform.
func (obj *Allocator) Fence(reason string) {
	if obj.fenced.CompareAndSwap(nil, &reason) {
		obj.logger.Error(
			"sequence instance fenced",
			append(obj.ownershipLogArgs(), "reason", reason)...,
		)
	}
	obj.Pause()
}

// Fenced reports whether this instance has been fenced, and why.
func (obj *Allocator) Fenced() (string, bool) {
	reason := obj.fenced.Load()
	if reason == nil {
		return "", false
	}
	return *reason, true
}

// Shutdown stops serving and gives back the slot authority this instance holds.
//
// It marks the instance unready first, so a probe can fail the instance before
// the process stops accepting. Then it closes every slot and drains and
// releases each one, following the section 6.3 order: a slot whose in-flight
// allocations do not reach zero is deliberately left held, because releasing it
// could let a new owner start while this instance can still hand out an id from
// its cached range.
//
// A release that does not finish leaves the authority to expire through the
// quiet window. That is slower for whoever takes over but never incorrect,
// which is why this is allowed to return without an error: the caller has
// nothing useful to do about it, and failing the shutdown would not release
// anything.
func (obj *Allocator) Shutdown() {
	obj.stopping.Store(true)
	obj.state.Store(StatePaused)
	obj.slotsMu.Lock()
	detached := obj.detachAllLocked()
	obj.slotsMu.Unlock()
	obj.drainAndRelease(detached)
}

// HAStats is the appendix E view of what the ownership protocol has been doing.
//
// It is deliberately low cardinality. The two questions an operator asks about a
// takeover are "how many" and "how long", and neither needs a per-slot label:
// sixteen thousand slots would turn every counter into sixteen thousand series
// and the answer would be unreadable.
type HAStats struct {
	// TakeoversGranted counts slots this instance took over, each of which moved
	// the epoch; TakeoversRefused counts attempts that were still inside the
	// quiet window.
	TakeoversGranted int64
	TakeoversRefused int64
	// EpochChanges counts the slots whose epoch this instance moved. It equals the
	// grants above today, and is kept separately because a release that is later
	// re-acquired is an epoch change that is not a takeover.
	EpochChanges int64
	// ReleasesReleased counts slots this instance gave back through the explicit
	// CAS; ReleasesFailed counts release attempts that errored, which leave the
	// authority in place until its lease lapses.
	ReleasesReleased int64
	ReleasesFailed   int64
	// Drains counts drain-and-release passes, and LastDrainSeconds how long the
	// most recent one took. A drain that keeps timing out is the operationally
	// interesting case, and it is the one that leaves authority held.
	Drains           int64
	LastDrainSeconds float64
	// LastPauseSeconds is the longest interval the most recent allocation spent
	// between opening its fences and advancing its cursor. It is what P_max is a
	// bound on, so it is the value an operator compares the bound against.
	LastPauseSeconds float64
}

// HAStats returns the appendix E counters.
func (obj *Allocator) HAStats() HAStats {
	stats := HAStats{
		TakeoversGranted: obj.takeoversGranted.Load(),
		TakeoversRefused: obj.takeoversRefused.Load(),
		EpochChanges:     obj.epochChanges.Load(),
		ReleasesReleased: obj.releasesReleased.Load(),
		ReleasesFailed:   obj.releasesFailed.Load(),
		Drains:           obj.drains.Load(),
		LastDrainSeconds: microsToSeconds(obj.lastDrainMicros.Load()),
		LastPauseSeconds: microsToSeconds(obj.lastPauseMicros.Load()),
	}
	return stats
}

// SlotStateCounts reports how many slots this instance holds and how many it has
// closed. The totals are what the slot_state series is for: a fleet whose owned
// count falls without a matching handover is losing coverage, and that is visible
// without a per-slot label.
func (obj *Allocator) SlotStateCounts() (owned int64, fenced int64) {
	obj.slotsMu.RLock()
	defer obj.slotsMu.RUnlock()
	for _, slot := range obj.slots {
		if slot.draining.Load() {
			fenced++
			continue
		}
		owned++
	}
	return owned, fenced
}

func microsToSeconds(micros int64) float64 {
	return float64(micros) / float64(time.Second/time.Microsecond)
}

func secondsToMicros(duration time.Duration) int64 {
	return duration.Microseconds()
}

// allocationSlot holds the key states of one routing slot plus the local
// allocation gate for it.
type allocationSlot struct {
	missMu sync.Mutex
	states sync.Map
	count  atomic.Int64
	// epoch is the ownership generation this instance holds for the slot. It is
	// the value releases and renewals CAS against, so it must never be reused.
	epoch atomic.Uint64
	// inflight counts allocations that are inside the gate. A drain publishes
	// draining and then waits for this to reach zero, which is what makes
	// "the old owner stopped first" a decidable fact rather than an assumption
	// (section 6.3).
	inflight atomic.Int64
	// draining closes the slot: no new allocation may linearise once it is set.
	draining atomic.Bool
	// localDeadline is the monotonic instant at which the storage lease stops
	// being trusted locally, or zero when the slot carries no local lease. It is
	// only consulted on the local-lease execution path.
	localDeadline atomic.Int64
	// renewedAt is the monotonic instant of the last successful renewal, used to
	// pace the next one. A failed renewal leaves it untouched, so the lease may
	// expire early but never late.
	renewedAt atomic.Int64
	// renewing keeps at most one renewal in flight per slot.
	renewing atomic.Bool
}

// enter registers an in-flight allocation and reports whether the slot is still
// open. The counter is incremented before the drain flag is read, so a drain
// either observes this allocation or rejects it; there is no window in which
// both are false.
func (s *allocationSlot) enter() bool {
	s.inflight.Add(1)
	if s.draining.Load() {
		s.inflight.Add(-1)
		return false
	}
	return true
}

func (s *allocationSlot) leave() { s.inflight.Add(-1) }

// drain closes the slot and waits for in-flight allocations to reach zero.
// It reports whether the slot reached a quiesced state.
func (s *allocationSlot) drain(timeout time.Duration) bool {
	s.draining.Store(true)
	deadline := time.Now().Add(timeout)
	for {
		if s.inflight.Load() == 0 {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(time.Millisecond)
	}
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

type cleanupCandidate struct {
	slotID uint32
	slot   *allocationSlot
	key    string
	state  *keyState
}
type cleanupStats struct {
	scanned          int
	evicted          int
	inflight         int
	discardedActive  int64
	discardedStandby int64
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
		candidate.state.clearRetryLocked()

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
