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
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/codesjoy/pkg/basic/xerror"
	"github.com/codesjoy/sindri/gen/go/sequence/reason"
)

type rangeFetch struct {
	done       chan struct{}
	background bool
	reserved   SequenceRange
	err        error
}

type keyState struct {
	allocator *Allocator
	// slot is the allocation slot this key belongs to, which owns the epoch,
	// in-flight counter and drain flag. It is nil only for states built
	// directly by tests.
	slot        *allocationSlot
	next        atomic.Int64
	start       atomic.Int64
	end         atomic.Int64
	generation  atomic.Uint64
	initialized atomic.Bool
	lastUsed    atomic.Int64
	returned    atomic.Int64
	prefetchAt  atomic.Int64
	recentBlock atomic.Int64
	rateGate    atomic.Int64
	retired     atomic.Bool

	mu           sync.Mutex
	activeStep   int64
	standby      *SequenceRange
	fetch        *rangeFetch
	retryAfter   time.Time
	retryAttempt int
	retryTimer   retryTimer

	rateMu sync.Mutex
	rate   keyRateEstimator
}

func (k *keyState) allocate(
	ctx context.Context,
	scope reservationScope,
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
		gateStart, err := k.beginLinearization()
		if err != nil {
			return 0, err
		}
		candidate = k.next.Add(1)
		if generation%2 == 0 && k.inActiveRange(candidate) &&
			generation == k.generation.Load() {
			if err := k.checkLinearizationBound(gateStart); err != nil {
				return 0, err
			}
			k.afterAllocate(scope, key, cfg, now, candidate, 1, generation)
			return candidate, nil
		}
		// Even an exhausted or superseded cursor advance must measure its pause;
		// entering a new slow-path gate must not erase that observation.
		if err := k.checkLinearizationBound(gateStart); err != nil {
			return 0, err
		}
	}

	id, idGeneration, err := k.allocateSlow(
		ctx,
		scope,
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
	k.afterAllocate(scope, key, cfg, now, id, 1, idGeneration)
	return id, nil
}

// gateReading takes the monotonic reading that opens the in-memory
// linearisation.
//
// Every allocation is bracketed by this reading and the bound check that
// follows the cursor advance. The interval between them is the one place a stall
// can hide: the local deadline was already checked when the batch opened, so
// nothing consulted before the stall can speak for what happened during it. A
// key state built directly by tests reports zero without touching the clock.
func (k *keyState) gateReading() int64 {
	if k.allocator == nil {
		return 0
	}
	return k.allocator.monoNow()
}

func (k *keyState) beginLinearization() (int64, error) {
	started := k.gateReading()
	if k.allocator != nil {
		if err := k.allocator.checkAllocationSlot(k.slot); err != nil {
			return 0, err
		}
	}
	return started, nil
}

// checkLinearizationBound discards an allocation whose in-memory linearisation
// outran the configured process-pause bound (section 3.4).
func (k *keyState) checkLinearizationBound(started int64) error {
	if k.allocator == nil {
		return nil
	}
	if err := k.allocator.checkFastPathBound(k.slot, started); err != nil {
		return err
	}
	return k.allocator.checkAllocationSlot(k.slot)
}

func (k *keyState) allocateSlow(
	ctx context.Context,
	scope reservationScope,
	key string,
	cfg AllocatorConfig,
	now func() time.Time,
	candidate int64,
	generation uint64,
	hadRange bool,
) (int64, uint64, error) {
	for {
		k.mu.Lock()
		gateStart, err := k.beginLinearization()
		if err != nil {
			k.mu.Unlock()
			return 0, 0, err
		}
		if k.initialized.Load() {
			currentGeneration := k.generation.Load()
			if hadRange && generation == currentGeneration && k.inActiveRange(candidate) {
				k.touch(now())
				k.mu.Unlock()
				return candidate, currentGeneration, k.checkLinearizationBound(gateStart)
			}

			candidate = k.next.Add(1)
			if k.inActiveRange(candidate) {
				k.touch(now())
				k.mu.Unlock()
				return candidate, currentGeneration, k.checkLinearizationBound(gateStart)
			}
			if err := k.checkLinearizationBound(gateStart); err != nil {
				k.mu.Unlock()
				return 0, 0, err
			}
		}

		if k.standby != nil {
			id, nextGeneration := k.activateStandbyLocked(now(), 1, cfg)
			k.mu.Unlock()
			return id, nextGeneration, k.checkLinearizationBound(gateStart)
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
		fetch := &rangeFetch{done: make(chan struct{})}
		k.fetch = fetch
		k.clearRetryLocked()
		k.mu.Unlock()

		started := time.Now()
		reserved, err := reserveRange(ctx, k.withEpoch(scope, key), key, step)
		k.observeAllocationReserve(started, err)
		k.completeFetch(fetch, reserved, err, cfg, now(), key)
		if err != nil {
			return 0, 0, err
		}
	}
}

func (k *keyState) afterAllocate(
	scope reservationScope,
	key string,
	cfg AllocatorConfig,
	now func() time.Time,
	id int64,
	blockSize int64,
	generation uint64,
) {
	k.recordReturned(id)
	currentTime := now()
	k.touch(currentTime)
	start := k.start.Load()
	end := k.end.Load()
	if start <= 0 || end < start {
		return
	}
	consumed := id - start + 1
	if consumed <= 0 {
		return
	}
	k.observeAllocation(currentTime, consumed, blockSize, generation, cfg)
	if id < k.prefetchAt.Load() {
		return
	}

	k.mu.Lock()
	if generation != k.generation.Load() || k.standby != nil || k.fetch != nil {
		k.mu.Unlock()
		return
	}
	if currentTime.Before(k.retryAfter) {
		k.mu.Unlock()
		return
	}
	currentStart := k.start.Load()
	currentEnd := k.end.Load()
	currentConsumed := id - currentStart + 1
	currentSize := currentEnd - currentStart + 1
	shouldPrefetch, fallback := k.prefetchDecisionLocked(id, cfg)
	if currentConsumed <= 0 || currentSize <= 0 || !shouldPrefetch {
		k.mu.Unlock()
		return
	}
	step := k.nextStepLocked(currentSize, cfg)
	fetch := &rangeFetch{
		done:       make(chan struct{}),
		background: true,
	}
	k.fetch = fetch
	if k.retryTimer != nil {
		k.retryTimer.Stop()
		k.retryTimer = nil
	}
	k.retryAfter = time.Time{}
	if k.allocator != nil {
		k.allocator.prefetchStarted.Add(1)
		if fallback {
			k.allocator.prefetchFallback.Add(1)
		}
	}
	k.mu.Unlock()

	k.launchBackgroundPrefetch(scope, key, fetch, step, cfg)
}

func (k *keyState) nextStepLocked(size int64, cfg AllocatorConfig) int64 {
	step := k.activeStep
	if step < cfg.DefaultStep {
		step = cfg.DefaultStep
	}
	rate, ready := k.rateSnapshot()
	if !ready || size <= 0 {
		return step
	}
	seconds := float64(size) / rate
	if seconds <= 0 {
		return step
	}
	estimatedDuration := time.Duration(math.MaxInt64)
	if seconds < float64(math.MaxInt64)/float64(time.Second) {
		estimatedDuration = time.Duration(seconds * float64(time.Second))
	}
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
	key string,
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
		k.clearRetryLocked()
		if k.allocator != nil && fetch.background {
			k.allocator.prefetchSucceeded.Add(1)
		}
	} else if fetch.background {
		if k.allocator != nil {
			k.allocator.prefetchFailed.Add(1)
		}
		k.scheduleRetryLocked(key, cfg, now)
	}
	k.fetch = nil
	close(fetch.done)
}

func (k *keyState) activateStandbyLocked(
	now time.Time,
	advance int64,
	cfg AllocatorConfig,
) (int64, uint64) {
	reserved := *k.standby
	k.standby = nil
	// Odd generations prevent optimistic readers from consuming partially
	// published bounds while the active range is changing.
	k.generation.Add(1)
	k.next.Store(reserved.Start + advance - 1)
	k.activeStep = reserved.End - reserved.Start + 1
	k.start.Store(reserved.Start)
	k.end.Store(reserved.End)
	generation := k.generation.Add(1)
	k.initialized.Store(true)
	k.clearRetryLocked()
	k.resetPrefetchState(generation, now, 0, cfg)
	k.touch(now)
	return reserved.Start, generation
}

func reserveRange(
	ctx context.Context,
	scope reservationScope,
	key string,
	step int64,
) (SequenceRange, error) {
	ranges, err := reserveRanges(ctx, scope, []ReservationRequest{{Key: key, Step: step}})
	if err != nil {
		return SequenceRange{}, err
	}
	return ranges[0], nil
}

func reserveRanges(
	ctx context.Context,
	scope reservationScope,
	requests []ReservationRequest,
) ([]SequenceRange, error) {
	if len(requests) == 0 {
		return nil, nil
	}
	if scope.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, scope.timeout)
		defer cancel()
	}
	reserved, err := scope.store.ReserveRanges(ctx, scope.authority, requests)
	if err != nil {
		return nil, authorityError(err)
	}
	if len(reserved) != len(requests) {
		return nil, refusedReservation(
			"reserve sequence ranges: got %d ranges for %d requests",
			len(reserved),
			len(requests),
		)
	}
	for index, request := range requests {
		item := reserved[index]
		if err := validateReservedRange(request.Key, request.Step, item); err != nil {
			return nil, refusedReservation("%s", err)
		}
	}
	return reserved, nil
}

// refusedReservation reports a reservation whose answer cannot be served as it
// stands: the store returned a set that does not line up with the request, or a
// range that is not a range.
//
// It is carried as the argument reason because that is the one reason in the
// contract that tells a caller not to retry unchanged, and because the
// alternative -- returning the bare error -- is what reaches a caller with no
// reason and no retry class at all. The message keeps the detail, so the caller
// and this service's own logs both see what did not line up.
func refusedReservation(format string, args ...any) error {
	return xerror.NewWithReason(
		reason.Reason_SEQUENCE_INVALID_ARGUMENT,
		fmt.Sprintf(format, args...),
		nil,
	)
}

// authorityError attaches the reason a caller must act on to a failure from the
// range authority.
//
// The authority reports these as sentinels, and a sentinel carries no reason, so
// without this the service cannot build the appendix A.4 envelope for them: a
// reservation that failed a fencing condition would reach the caller as a bare
// error with no retry class, no owner hint and no way to tell it from an ordinary
// storage fault. That is worst for the uncertain commit, where the difference
// matters most: it is the one failure the caller must resolve by discarding the
// range and retrying rather than by re-reading the high watermark to guess, and a
// caller that cannot recognise it has no safe retry at all.
//
// Everything else becomes SEQUENCE_STORAGE_UNAVAILABLE, because that is what a
// failure from here that is not one of those sentinels is: the store did not
// complete the reservation. It carries a retry class so the caller retries
// instead of treating a storage blip as its own error, which is safe in this
// direction -- a retry reserves a new range and can never reissue an id -- and it
// is deliberately not one of the fencing reasons, which would tell the caller to
// react to a condition that did not happen.
//
// Every branch wraps rather than rebuilds the error, so the reason and code are
// added without losing the cause: the message the store produced still reaches
// the caller verbatim, and errors.Is still finds what it reported. That is not
// cosmetic -- a cancelled context must stay recognisable as a cancellation rather
// than be flattened into a storage failure.
func authorityError(err error) error {
	switch {
	case errors.Is(err, ErrSlotNotOwned):
		return xerror.WrapWithReason(err, reason.Reason_SEQUENCE_SLOT_NOT_OWNER, "", nil)
	case errors.Is(err, ErrLeaseExpired):
		return xerror.WrapWithReason(err, reason.Reason_SEQUENCE_LEASE_EXPIRED, "", nil)
	case errors.Is(err, ErrCommitUncertain):
		return xerror.WrapWithReason(err, reason.Reason_SEQUENCE_COMMIT_UNCERTAIN, "", nil)
	default:
		return xerror.WrapWithReason(err, reason.Reason_SEQUENCE_STORAGE_UNAVAILABLE, "", nil)
	}
}

func validateReservedRange(key string, step int64, reserved SequenceRange) error {
	if reserved.Start <= 0 || reserved.End < reserved.Start ||
		reserved.End-reserved.Start+1 != step {
		return fmt.Errorf(
			"reserve sequence range for %q: invalid range [%d,%d]",
			key,
			reserved.Start,
			reserved.End,
		)
	}
	return nil
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

// FetchNext returns the next ID for a locally owned key.
func (obj *Allocator) FetchNext(ctx context.Context, key string) (int64, error) {
	started := obj.now()
	slotID := SlotForKey(key)

	// The read lock covers only the pause check, the slot lookup and entering the
	// gate; the rest of the request -- authorisation, range reservation and the
	// storage round trips it may need -- runs after it is released. The held
	// slot pointer is safe to use because enter() has registered this allocation
	// as in flight, so a drain that drops the slot from the map still has to wait
	// for this request to leave before it may release the authority.
	slot, err := obj.enterSlot(slotID)
	if err != nil {
		return 0, err
	}
	defer slot.leave()
	if err := obj.authorize(
		[]uint32{slotID},
		[]*allocationSlot{slot},
	); err != nil {
		return 0, err
	}
	state, ok := slot.Load(key)
	if !ok {
		var err error
		state, err = obj.loadOrCreateState(key, slot)
		if err != nil {
			return 0, err
		}
	}
	id, err := state.allocate(ctx, obj.scope(), key, obj.cfg, obj.now)
	if err != nil {
		return 0, err
	}
	obj.recordLinearization(key, state, SequenceAllocation{
		ID:        id,
		Count:     1,
		SlotEpoch: slot.epoch.Load(),
	}, started)
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
	state := &keyState{allocator: obj, slot: slot}
	slot.Store(key, state)
	slot.count.Add(1)
	obj.cachedKeys.Add(1)
	return state, nil
}

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
	// SlotEpoch is the ownership generation of the slot the key routes to, read
	// while the allocation was inside the gate. A caller that sees it decrease
	// between two allocations for the same key has proof of an ordering
	// violation, so it is reported rather than kept internal.
	SlotEpoch uint64
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
	started := obj.now()

	states := make([]*keyState, len(normalized))
	slotOf := make([]*allocationSlot, len(normalized))
	entered := make([]*allocationSlot, 0, len(normalized))
	slots := make([]uint32, 0, len(normalized))

	// The read lock covers the pause check, the slot lookups and opening every
	// involved gate. It is released before any allocation runs, so the storage
	// I/O a batch may need cannot block a reconciliation that needs the write
	// lock. Keeping each slot pointer is safe: its gate is open, so a drain that
	// drops the slot from the map must wait for this batch to leave before it
	// releases the authority.
	obj.slotsMu.RLock()
	if obj.Paused() {
		obj.slotsMu.RUnlock()
		return nil, xerror.NewWithReason(
			reason.Reason_SEQUENCE_ALLOCATOR_PAUSED,
			"allocator is already paused",
			nil,
		)
	}
	for index, request := range normalized {
		slotID := SlotForKey(request.Key)
		slot, ok := obj.slots[slotID]
		if !ok {
			obj.slotsMu.RUnlock()
			return nil, xerror.NewWithReason(
				reason.Reason_SEQUENCE_SLOT_NOT_OWNER,
				"slot not found",
				nil,
			)
		}
		slotOf[index] = slot
		if !slices.Contains(entered, slot) {
			entered = append(entered, slot)
			slots = append(slots, slotID)
		}
	}
	// Every involved slot is opened before any allocation runs and closed on
	// return, so a concurrent drain either sees this batch or rejects it.
	for index, slot := range entered {
		if !slot.enter() {
			for _, opened := range entered[:index] {
				opened.leave()
			}
			obj.slotsMu.RUnlock()
			return nil, xerror.NewWithReason(
				reason.Reason_SEQUENCE_SLOT_NOT_OWNER,
				"slot is draining",
				nil,
			)
		}
	}
	obj.slotsMu.RUnlock()
	defer func() {
		for _, slot := range entered {
			slot.leave()
		}
	}()
	if err := obj.authorize(slots, entered); err != nil {
		return nil, err
	}
	missing := 0
	for index, request := range normalized {
		state, ok := slotOf[index].Load(request.Key)
		if !ok {
			missing++
			continue
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
		state, err := obj.loadOrCreateState(request.Key, slotOf[index])
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
			obj.scope(),
			request.Key,
			int64(request.Count),
			obj.cfg,
			obj.now,
		)
		if err != nil {
			return nil, err
		}
		// The epoch is read inside the gate, which a drain cannot close until
		// this batch leaves, so it is the generation this block was fenced by
		// rather than whatever the slot has moved on to.
		allocation.SlotEpoch = slotOf[index].epoch.Load()
		results[index] = allocation
	}
	for index, request := range normalized {
		obj.recordLinearization(request.Key, states[index], results[index], started)
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
	scope := obj.scope()
	for _, item := range pending {
		if epochs := item.state.withEpoch(scope, item.key).authority.Epochs; len(epochs) > 0 {
			if scope.authority.Epochs == nil {
				scope.authority.Epochs = make(map[uint32]uint64, len(pending))
			}
			for slotID, epoch := range epochs {
				scope.authority.Epochs[slotID] = epoch
			}
		}
	}
	reserveStarted := time.Now()
	reserved, err := reserveRanges(ctx, scope, reservationRequests)
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
	scope reservationScope,
	key string,
	count int64,
	cfg AllocatorConfig,
	now func() time.Time,
) (SequenceAllocation, error) {
	if count <= 1 {
		id, err := k.allocate(ctx, scope, key, cfg, now)
		if err != nil {
			return SequenceAllocation{}, err
		}
		return SequenceAllocation{ID: id, Count: 1}, nil
	}

	id, generation, ok, err := k.tryAllocateBlock(count)
	if err != nil {
		return SequenceAllocation{}, err
	}
	if ok {
		last := id + count - 1
		k.afterAllocate(scope, key, cfg, now, last, count, generation)
		return SequenceAllocation{ID: id, Count: uint32(count)}, nil
	}

	id, generation, err = k.allocateBlockSlow(ctx, scope, key, count, cfg, now)
	if err != nil {
		return SequenceAllocation{}, err
	}
	last := id + count - 1
	k.afterAllocate(scope, key, cfg, now, last, count, generation)
	return SequenceAllocation{ID: id, Count: uint32(count)}, nil
}

func (k *keyState) tryAllocateBlock(count int64) (int64, uint64, bool, error) {
	if !k.initialized.Load() {
		return 0, 0, false, nil
	}
	generation := k.generation.Load()
	if generation%2 != 0 {
		return 0, 0, false, nil
	}
	current := k.next.Load()
	start := k.start.Load()
	end := k.end.Load()
	if current < start || current > math.MaxInt64-count || current+count > end {
		return 0, 0, false, nil
	}
	last := current + count
	gateStart, err := k.beginLinearization()
	if err != nil {
		return 0, 0, false, err
	}
	if !k.next.CompareAndSwap(current, last) {
		return 0, 0, false, nil
	}
	sameGeneration := generation == k.generation.Load()
	if err := k.checkLinearizationBound(gateStart); err != nil {
		return 0, 0, false, err
	}
	if !sameGeneration {
		return 0, 0, false, nil
	}
	return current + 1, generation, true, nil
}

func (k *keyState) allocateBlockSlow(
	ctx context.Context,
	scope reservationScope,
	key string,
	count int64,
	cfg AllocatorConfig,
	now func() time.Time,
) (int64, uint64, error) {
	for {
		k.mu.Lock()
		id, generation, ok, err := k.tryAllocateBlock(count)
		if err != nil {
			k.mu.Unlock()
			return 0, 0, err
		}
		if ok {
			k.touch(now())
			k.mu.Unlock()
			return id, generation, nil
		}

		if k.standby != nil {
			size := k.standby.End - k.standby.Start + 1
			if size >= count {
				gateStart, guardErr := k.beginLinearization()
				if guardErr != nil {
					k.mu.Unlock()
					return 0, 0, guardErr
				}
				first, generation := k.activateStandbyLocked(now(), count, cfg)
				k.touch(now())
				k.mu.Unlock()
				return first, generation, k.checkLinearizationBound(gateStart)
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
		reserved, err := reserveRange(ctx, k.withEpoch(scope, key), key, step)
		k.observeAllocationReserve(started, err)
		k.completeFetch(fetch, reserved, err, cfg, now(), key)
		if err != nil {
			return 0, 0, err
		}
	}
}
