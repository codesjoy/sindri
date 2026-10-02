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
	"sort"
	"time"

	"github.com/codesjoy/pkg/basic/xerror"
	"github.com/codesjoy/sindri/gen/go/sequence/reason"
)

// scope returns the store and authority a reservation issued by this process
// runs under.
func (obj *Allocator) scope() reservationScope {
	return reservationScope{
		store:     obj.store,
		authority: ReservationAuthority{InstanceID: obj.instanceID, Lease: obj.ha.LeaseDuration},
		timeout:   obj.storeTimeout(),
	}
}

// ownershipBatchSize bounds how many slots one authority statement carries. A
// node claims or releases every slot it gains or loses at once, which can be all
// of them, and both dialects cap how many bind parameters a single statement may
// carry.
const ownershipBatchSize = 1000

// claimRetryInterval is how long ApplyRoute waits after a deferred claim before
// trying again. A slot whose previous owner has not crossed the quiet window
// cannot be claimed until it does, so retrying on every base tick would turn a
// slow window into a statement storm without ever succeeding sooner.
func (obj *Allocator) claimRetryInterval() time.Duration {
	interval := obj.ha.QuietWindow / 4
	if interval <= 0 || interval > 250*time.Millisecond {
		interval = 250 * time.Millisecond
	}
	return interval
}

// ownershipLogArgs identifies this process in ownership logs. A takeover, a
// release or a deferred claim is unreadable without knowing which instance and
// node produced it.
func (obj *Allocator) ownershipLogArgs() []any {
	return []any{
		slog.String("node_id", obj.nodeID),
		slog.String("instance_id", obj.instanceID),
	}
}

type localGrant struct {
	epoch  uint64
	anchor int64
}

// claimSlots keeps confirmed batches even when the pass budget is exhausted.
func (obj *Allocator) claimSlots(
	ctx context.Context,
	slots []uint32,
) (map[uint32]localGrant, bool) {
	if obj.ownership == nil || len(slots) == 0 {
		return nil, true
	}
	grants := make(map[uint32]localGrant, len(slots))
	complete := true
	for start := 0; start < len(slots); start += ownershipBatchSize {
		end := min(start+ownershipBatchSize, len(slots))
		if ctx.Err() != nil {
			return grants, false
		}
		batchCtx, cancel := context.WithTimeout(ctx, obj.storeTimeout())
		anchor := obj.monoNow()
		outcomes, err := obj.ownership.ClaimSlots(batchCtx, ClaimRequest{
			Slots:       slots[start:end],
			NodeID:      obj.nodeID,
			InstanceID:  obj.instanceID,
			QuietWindow: obj.ha.QuietWindow,
		})
		cancel()
		if err != nil {
			obj.logger.Error(
				"claim slot ownership",
				append(obj.ownershipLogArgs(), "error", err)...,
			)
			return grants, false
		}
		for _, outcome := range outcomes {
			if !outcome.Granted {
				complete = false
				obj.takeoversRefused.Add(1)
				continue
			}
			grants[outcome.Ownership.SlotID] = localGrant{
				epoch: outcome.Ownership.Epoch, anchor: anchor,
			}
			obj.takeoversGranted.Add(1)
			obj.epochChanges.Add(1)
		}
	}
	return grants, complete
}

// releaseSlots performs the explicit epoch-CAS release of section 6.5. It is
// only ever called on slots whose in-flight allocation count has reached zero.
func (obj *Allocator) releaseSlots(ctx context.Context, targets []SlotAuthority) int64 {
	if obj.ownership == nil || len(targets) == 0 {
		return 0
	}
	var released int64
	for start := 0; start < len(targets); start += ownershipBatchSize {
		end := min(start+ownershipBatchSize, len(targets))
		// One bound per statement, for the reason claimSlots uses one: a release
		// covers every slot this instance gives up at once, and a bound shared
		// across the batches is spent serially, so a slow but legal release stops
		// partway and leaves the rest this instance's authority.
		batchCtx, cancel := context.WithTimeout(ctx, obj.storeTimeout())
		count, err := obj.ownership.ReleaseSlots(batchCtx, targets[start:end])
		cancel()
		if err != nil {
			obj.releasesFailed.Add(1)
			obj.logger.Error(
				"release slot ownership",
				append(obj.ownershipLogArgs(), "error", err)...,
			)
			break
		}
		released += count
	}
	obj.releasesReleased.Add(released)
	return released
}

func (obj *Allocator) storeTimeout() time.Duration {
	if obj.cfg.ReserveTimeout > 0 {
		return obj.cfg.ReserveTimeout
	}
	return time.Second
}

// detachedSlot is a slot removed from the active set and closed to new
// allocations, still awaiting the in-flight barrier before its authority is
// released.
type detachedSlot struct {
	slotID uint32
	slot   *allocationSlot
}

// drainAndRelease enforces the section 6.3 order for slots this instance has
// stopped serving: the gate is already closed, so wait for in-flight
// allocations to reach zero and only then release the authority. A slot whose
// drain times out is deliberately not released: releasing it would let a new
// owner start while this instance could still hand out an id from its cached
// range, which is exactly the regression the whole protocol exists to prevent.
func (obj *Allocator) drainAndRelease(detached []detachedSlot) {
	obj.drainAndReleaseWithin(context.Background(), detached)
}

// drainAndReleaseWithin also respects a route-apply pass's total work budget.
// If it expires, unreleased authority is left to age out, never released early.
func (obj *Allocator) drainAndReleaseWithin(ctx context.Context, detached []detachedSlot) {
	if len(detached) == 0 {
		return
	}
	started := obj.now()
	timeout := obj.ha.ReleaseDrainTimeout
	if timeout <= 0 {
		timeout = time.Second
	}
	targets := make([]SlotAuthority, 0, len(detached))
	for _, item := range detached {
		remaining := timeout
		if deadline, ok := ctx.Deadline(); ok {
			remaining = min(remaining, max(time.Until(deadline), 0))
		}
		if !item.slot.drain(remaining) {
			obj.logger.Error(
				"slot drain timed out; authority retained until the lease expires",
				append(obj.ownershipLogArgs(),
					slog.Uint64("slot_id", uint64(item.slotID)),
					slog.Int64("inflight", item.slot.inflight.Load()),
				)...,
			)
			continue
		}
		epoch := item.slot.epoch.Load()
		if epoch == 0 {
			continue
		}
		targets = append(targets, SlotAuthority{
			SlotID:     item.slotID,
			InstanceID: obj.instanceID,
			Epoch:      epoch,
		})
	}
	if len(targets) == 0 {
		return
	}
	released := obj.releaseSlots(ctx, targets)
	obj.drains.Add(1)
	obj.lastDrainMicros.Store(secondsToMicros(obj.now().Sub(started)))
	obj.logger.Info(
		"released slot ownership",
		append(obj.ownershipLogArgs(),
			slog.Int("requested", len(targets)),
			slog.Int64("released", released),
		)...,
	)
}

// withEpoch returns scope carrying the ownership epoch this instance holds for
// the key's slot, when the local gate knows one.
//
// The epoch is read through the slot pointer, which is an atomic load on a
// pointer that never changes, so this is safe from the background prefetch
// goroutine, which holds no slot lock. A slot whose epoch the gate does not know
// stays absent from the map, which makes the reservation skip the comparison
// rather than fail it.
func (k *keyState) withEpoch(scope reservationScope, key string) reservationScope {
	if k == nil || k.slot == nil {
		return scope
	}
	epoch := k.slot.epoch.Load()
	if epoch == 0 {
		return scope
	}
	scope.authority.Epochs = map[uint32]uint64{SlotForKey(key): epoch}
	if k.allocator == nil {
		return scope
	}
	// The lease travels with the reservation so the storage re-checks it against
	// the same clock that granted it. That is what lets the steady-state
	// allocation path never touch storage: a range reserved outside the lease is
	// refused by the authority itself, not by a read this node would have to
	// trust.
	scope.authority.Lease = k.allocator.ha.LeaseDuration
	return scope
}

// detachAllLocked closes and removes every active slot, returning them for the
// in-flight barrier and release.
func (obj *Allocator) detachAllLocked() []detachedSlot {
	detached := make([]detachedSlot, 0, len(obj.slots))
	for slotID, slot := range obj.slots {
		obj.cachedKeys.Add(-slot.count.Load())
		obj.cancelSlotRetries(slot)
		slot.draining.Store(true)
		detached = append(detached, detachedSlot{slotID: slotID, slot: slot})
		delete(obj.slots, slotID)
	}
	return detached
}

func (obj *Allocator) cancelSlotRetries(slot *allocationSlot) {
	if slot == nil {
		return
	}
	slot.Range(func(_, value any) bool {
		if state, ok := value.(*keyState); ok {
			state.cancelRetry()
		}
		return true
	})
}

// hasAuthority reports whether an ownership authority store is configured.
//
// It is deliberately a single condition rather than a conjunction of the
// parameters: whether the lease is armed and whether the pause bound is measured
// must never disagree, or allocation could run with only half of its safety
// argument. Configuration validation is what guarantees LeaseDuration and
// MaxPause are present. An allocator without an authority store has nothing to
// fence against, which is why the in-process constructions that omit one skip
// the gate entirely.
func (obj *Allocator) hasAuthority() bool {
	return obj.ownership != nil
}

// enterSlot resolves one slot under the read lock and opens its gate.
//
// It is the only part of the allocation path that has to run under slotsMu: the
// pause check and the lookup need the map, and enter() must register the
// allocation before the lock is released so a drain that removes the slot
// cannot miss it. Everything after this returns runs outside the lock, which is
// what keeps a slow storage call from blocking a reconciliation that needs the
// write lock.
func (obj *Allocator) enterSlot(slotID uint32) (*allocationSlot, error) {
	obj.slotsMu.RLock()
	defer obj.slotsMu.RUnlock()
	if obj.Paused() {
		return nil, xerror.NewWithReason(
			reason.Reason_SEQUENCE_ALLOCATOR_PAUSED,
			"allocator is already paused",
			nil,
		)
	}
	slot, ok := obj.slots[slotID]
	if !ok {
		return nil, xerror.NewWithReason(
			reason.Reason_SEQUENCE_SLOT_NOT_OWNER,
			"slot not found",
			nil,
		)
	}
	if !slot.enter() {
		return nil, xerror.NewWithReason(
			reason.Reason_SEQUENCE_SLOT_NOT_OWNER,
			"slot is draining",
			nil,
		)
	}
	return slot, nil
}

// authorize fences the slots behind a request before any allocation linearises.
// The caller passes the slots it resolved and entered; it does not hold slotsMu
// while calling this, because the authority checks below include a clock read
// and a re-read of the slot's deadline, neither of which needs the map.
//
// The fence is local, and nothing here touches storage: that is what keeps
// steady-state allocation off the database. It is sound only because the
// reservation itself carries the lease and the storage re-checks it against the
// same clock that granted it, and because checkFastPathBound measures the one
// interval this gate cannot see.
func (obj *Allocator) authorize(slots []uint32, held []*allocationSlot) error {
	if !obj.hasAuthority() {
		return nil
	}
	for index, slot := range held {
		if slot == nil {
			return xerror.NewWithReason(
				reason.Reason_SEQUENCE_SLOT_NOT_OWNER,
				fmt.Sprintf("slot %d is not held by this instance", slots[index]),
				nil,
			)
		}
		if err := obj.checkLocalLease(slot); err != nil {
			return err
		}
	}
	return nil
}

// checkLocalLease reports whether the slot's local lease still authorises work.
func (obj *Allocator) checkLocalLease(slot *allocationSlot) error {
	deadline := slot.localDeadline.Load()
	if deadline != 0 && obj.monoNow() < deadline {
		return nil
	}
	obj.leaseExpired.Add(1)
	return xerror.NewWithReason(
		reason.Reason_SEQUENCE_LEASE_EXPIRED,
		"local lease for the slot has expired; waiting for a renewal",
		nil,
	)
}

// checkAllocationSlot also guards requests that waited after entering the gate.
func (obj *Allocator) checkAllocationSlot(slot *allocationSlot) error {
	if slot == nil || !obj.hasAuthority() {
		return nil
	}
	if slot.draining.Load() || obj.fenced.Load() != nil {
		return xerror.NewWithReason(reason.Reason_SEQUENCE_OWNER_RECOVERING,
			"slot is no longer serving this allocation", nil)
	}
	return obj.checkLocalLease(slot)
}

// checkFastPathBound measures the in-memory linearisation of section 3.4.
//
// A pause inside the interval between passing the gate and advancing the cursor
// is the one failure no fence that reads storage can cover: the gate has already
// passed by then, so it cannot see a takeover that commits during the stall, and
// the cursor may land after the new owner's first ID. The cursor has already
// advanced when this runs, so the ID is discarded rather than returned, and the
// slot is fenced instead of being left to serve the next request.
//
// This is why P_max belongs in the quiet-window bound and is not an alternative
// to it: W bounds how long a stopped owner may keep serving, while this measures
// the stall that can land after an owner has already checked.
func (obj *Allocator) checkFastPathBound(slot *allocationSlot, started int64) error {
	if slot == nil {
		return nil
	}
	pause := time.Duration(obj.monoNow() - started)
	// The observation is recorded whether or not it broke the bound: the value an
	// operator compares against P_max is the one that nearly did.
	obj.lastPauseMicros.Store(secondsToMicros(pause))
	if pause <= obj.ha.MaxPause {
		return nil
	}
	obj.pauseViolations.Add(1)
	obj.fenceSlot(slot, "process pause exceeded the configured bound")
	return xerror.NewWithReason(
		reason.Reason_SEQUENCE_OWNER_RECOVERING,
		"allocation discarded: the process paused beyond the configured bound",
		nil,
	)
}

// fenceSlot closes a slot and drops its local lease.
//
// The slot is left in the map so the drain path still finds it and can release
// it through the authority store; dropping the epoch without releasing would
// strand the slot until its storage lease lapsed.
func (obj *Allocator) fenceSlot(slot *allocationSlot, why string) {
	slot.localDeadline.Store(0)
	slot.draining.Store(true)
	obj.gateFenced.Add(1)
	obj.logger.Error(
		"sequence slot fenced",
		append(obj.ownershipLogArgs(),
			slog.String("reason", why),
			slog.Uint64("epoch", slot.epoch.Load()),
			slog.Int64("inflight", slot.inflight.Load()),
		)...,
	)
}

// installEpoch records the epoch this instance holds for a slot, dates the
// renewal cadence, and arms the local lease.
//
// The anchor precedes the storage request, not the arrival of its response.
func (obj *Allocator) installGrant(slot *allocationSlot, grant localGrant) {
	slot.epoch.Store(grant.epoch)
	slot.renewedAt.Store(grant.anchor)
	if !obj.hasAuthority() {
		return
	}
	slot.localDeadline.Store(grant.anchor + int64(obj.ha.LeaseDeadline()))
}

// RenewalBatchObserver receives the duration and the number of slots of one
// grouped renewal round trip.
type RenewalBatchObserver func(duration time.Duration, slots int)

// SetRenewalBatchObserver registers a low-cardinality observer for renewal
// batches, replacing any previous one. A nil observer clears it.
func (obj *Allocator) SetRenewalBatchObserver(observer RenewalBatchObserver) {
	if obj == nil {
		return
	}
	if observer == nil {
		obj.renewalObserver.Store(nil)
		return
	}
	obj.renewalObserver.Store(&observer)
}

// RenewLeases refreshes the storage grant of every slot this instance holds.
//
// It is driven by the node heartbeat rather than the logical tick, because the
// lease is a wall-clock quantity: tying it to a tick would change how strong
// the safety window is whenever the tick interval changed.
//
// The refreshed grant is what keeps the local deadline meaningful, so a renewal
// that cannot be confirmed deliberately leaves the local deadline alone. That
// can only make a lease expire earlier than the storage allows, never later.
func (obj *Allocator) RenewLeases() {
	// Renewal is a write to the authority store, so an allocator without one has
	// nothing to renew. Production wiring always supplies the store; this is the
	// guard for the in-process constructions that do not.
	if obj.ownership == nil {
		return
	}
	// A fenced instance stops renewing. Renewal is what keeps a grant inside the
	// planner's freshness bound, so an instance that kept renewing would hold
	// every slot it can no longer serve pinned to itself by the rule that leaves a
	// live owner with a fresh grant alone, and nothing would ever reassign them.
	// Letting the grants age hands the whole assignment to the fleet at the quiet
	// window, which is exactly the failover a silently failing owner is meant to
	// have, and it asserts nothing new about a time base this instance no longer
	// trusts -- which is why the slots are not released outright instead.
	if _, fenced := obj.Fenced(); fenced {
		return
	}
	now := obj.monoNow()
	type candidate struct {
		slotID uint32
		slot   *allocationSlot
		epoch  uint64
	}
	var pending []candidate
	obj.slotsMu.RLock()
	for slotID, slot := range obj.slots {
		if slot.draining.Load() {
			continue
		}
		epoch := slot.epoch.Load()
		if epoch == 0 {
			continue
		}
		if obj.ha.RenewInterval > 0 &&
			time.Duration(now-slot.renewedAt.Load()) < obj.ha.RenewInterval {
			continue
		}
		if !slot.renewing.CompareAndSwap(false, true) {
			continue
		}
		pending = append(pending, candidate{slotID: slotID, slot: slot, epoch: epoch})
	}
	obj.slotsMu.RUnlock()
	if len(pending) == 0 {
		return
	}
	defer func() {
		for _, item := range pending {
			item.slot.renewing.Store(false)
		}
	}()

	authorities := make([]SlotAuthority, 0, len(pending))
	// The renewal is grouped by the fence the authority CASes on. Everything
	// this instance holds at the same epoch shares one predicate, so it can be
	// renewed by one statement rather than one statement per slot; only a slot
	// whose epoch moved needs a group of its own.
	byEpoch := make(map[uint64][]uint32)
	for _, item := range pending {
		authorities = append(authorities, SlotAuthority{
			SlotID:     item.slotID,
			InstanceID: obj.instanceID,
			Epoch:      item.epoch,
		})
		byEpoch[item.epoch] = append(byEpoch[item.epoch], item.slotID)
	}
	epochs := make([]uint64, 0, len(byEpoch))
	for epoch := range byEpoch {
		epochs = append(epochs, epoch)
	}
	sort.Slice(epochs, func(i, j int) bool { return epochs[i] < epochs[j] })
	groups := make([]RenewGroup, 0, len(epochs))
	for _, epoch := range epochs {
		slots := byEpoch[epoch]
		sort.Slice(slots, func(i, j int) bool { return slots[i] < slots[j] })
		groups = append(groups, RenewGroup{
			InstanceID: obj.instanceID,
			Epoch:      epoch,
			Slots:      slots,
		})
	}
	ctx, cancel := context.WithTimeout(context.Background(), obj.storeTimeout())
	defer cancel()
	// The renewal is guarded by the epoch CAS in the authority store, not by the
	// lease: only a row this instance still owns at this epoch is refreshed, so a
	// renewal can never resurrect authority a takeover already replaced, and a
	// grant that lapsed without a takeover stays recoverable.
	anchor := obj.monoNow()
	renewStarted := time.Now()
	renewed, err := obj.ownership.RenewSlots(ctx, RenewRequest{
		Groups: groups,
	})
	renewDuration := time.Since(renewStarted)
	if observer := obj.renewalObserver.Load(); observer != nil {
		(*observer)(renewDuration, len(authorities))
	}
	if err != nil {
		obj.renewalFailed.Add(int64(len(authorities)))
		obj.logger.Warn(
			"sequence lease renewal failed; local leases will expire early",
			append(obj.ownershipLogArgs(),
				slog.Any("err", err),
				slog.Int("slots", len(authorities)),
			)...,
		)
		return
	}
	renewedEpochs := make(map[uint32]uint64, len(renewed))
	for _, row := range renewed {
		renewedEpochs[row.SlotID] = row.Epoch
	}
	deadline := anchor + int64(obj.ha.LeaseDeadline())
	for _, item := range pending {
		// A row that did not come back is not held any more, so it is fenced
		// rather than re-armed with a deadline it cannot honour.
		if renewedEpochs[item.slotID] != item.epoch || obj.monoNow() >= deadline {
			obj.fenceSlot(item.slot, "lease renewal did not confirm the grant")
			continue
		}
		item.slot.renewedAt.Store(anchor)
		item.slot.localDeadline.Store(deadline)
		obj.renewalSucceeded.Add(1)
	}
}

// InflightAllocations reports how many allocations are currently inside a slot
// gate, summed over the slots this instance holds.
//
// A drain waits for this to reach zero, so it is the number that says whether an
// explicit release will finish or time out. It is a gauge and not a barrier: a
// slot can leave the map while one of its allocations is still in flight, so
// the sum can read low at the instant a slot is being dropped.
func (obj *Allocator) InflightAllocations() int64 {
	var total int64
	obj.slotsMu.RLock()
	for _, slot := range obj.slots {
		total += slot.inflight.Load()
	}
	obj.slotsMu.RUnlock()
	return total
}

// RetryAfter reports how long a caller should wait before retrying a failure
// this instance expects to recover from on its own, for the appendix A.4
// envelope.
//
// Only the recovering case has an answer here. A slot whose owner is recovering
// becomes serviceable once the next renewal confirms the grant, so the renewal
// cadence is the wait; every other reason is either a directory problem the
// caller fixes by refreshing, or a permanent one, and a duration would be
// misleading for both.
func (obj *Allocator) RetryAfter(r reason.Reason) time.Duration {
	if r != reason.Reason_SEQUENCE_OWNER_RECOVERING {
		return 0
	}
	return obj.ha.RenewInterval
}

// LeaseStats is a low-cardinality snapshot of the local gate.
type LeaseStats struct {
	LeaseExpired     int64
	PauseViolations  int64
	RenewalSucceeded int64
	RenewalFailed    int64
	GateFenced       int64
}

// LeaseStats returns the local-gate counters.
func (obj *Allocator) LeaseStats() LeaseStats {
	return LeaseStats{
		LeaseExpired:     obj.leaseExpired.Load(),
		PauseViolations:  obj.pauseViolations.Load(),
		RenewalSucceeded: obj.renewalSucceeded.Load(),
		RenewalFailed:    obj.renewalFailed.Load(),
		GateFenced:       obj.gateFenced.Load(),
	}
}
