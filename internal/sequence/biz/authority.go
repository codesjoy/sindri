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
	"sort"
	"time"

	"github.com/codesjoy/pkg/basic/xerror"
	"github.com/codesjoy/sindri/gen/go/sequence/reason"
	"github.com/google/uuid"
)

const ownershipBatchSize = 1000

// InstanceLease is the only renewable allocation capability.
type InstanceLease struct {
	InstanceID string
	NodeID     string
	Revision   uint64
	GrantedAt  time.Time
	State      string
}

// AuthoritySnapshot is read while ownership changes for the instance are excluded.
type AuthoritySnapshot struct {
	Lease InstanceLease
	Slots []Ownership
}

func (a *Allocator) scope() reservationScope {
	return reservationScope{
		store: a.store,
		authority: ReservationAuthority{
			InstanceID: a.instanceID,
			Revision:   a.ownershipRevision.Load(),
			Lease:      a.ha.LeaseDuration,
		},
		timeout: a.storeTimeout(),
	}
}

func (a *Allocator) storeTimeout() time.Duration {
	if a.cfg.ReserveTimeout > 0 {
		return a.cfg.ReserveTimeout
	}
	return time.Second
}

// authorityRefreshTimeout bounds one authority refresh pass: reading the
// fleet's ownership and liveness view, planning from it, and claiming the slots
// the plan hands this node.
//
// It is deliberately not the directory read's bound. Every claim batch inside
// the pass is bounded by ReserveTimeout, so a single bound sized for one read
// would be spent serially across those statements and the pass would fail on
// its first batch whenever the storage is slower than that read. Covering the
// batches a full slot space needs is what keeps the pass from being cut off
// mid-convergence; the pass stays finite because the batch count is.
func (a *Allocator) authorityRefreshTimeout() time.Duration {
	batches := int64(SlotCount/ownershipBatchSize + 1)
	return time.Duration(batches) * a.storeTimeout()
}

func (a *Allocator) ownershipLogArgs() []any {
	return []any{slog.String("node_id", a.nodeID), slog.String("instance_id", a.instanceID)}
}

func (k *keyState) withEpoch(scope reservationScope, key string) reservationScope {
	if k != nil && k.slot != nil {
		scope.authority.Epochs = map[uint32]uint64{SlotForKey(key): k.slot.epoch.Load()}
		if k.allocator != nil {
			scope.authority.Revision = k.allocator.ownershipRevision.Load()
			scope.authority.Lease = k.allocator.ha.LeaseDuration
		}
	}
	return scope
}

type detachedSlot struct {
	slotID uint32
	slot   *allocationSlot
}

func (a *Allocator) detachAllLocked() []detachedSlot {
	var detached []detachedSlot
	for id, slot := range a.slots {
		slot.draining.Store(true)
		a.cancelSlotRetries(slot)
		a.cachedKeys.Add(-slot.count.Load())
		detached = append(detached, detachedSlot{slotID: id, slot: slot})
		a.drainingSlots[id] = slot
	}
	a.slots = make(map[uint32]*allocationSlot)
	a.pendingActive = nil
	a.rebuildCleanupSlotsLocked()
	return detached
}

// applyAuthorityLocked closes withdrawn gates before publishing the acknowledged revision.
func (a *Allocator) applyAuthorityLocked(snapshot AuthoritySnapshot) error {
	if a.stopping.Load() || a.fenced.Load() != nil {
		return ErrAuthorityChanged
	}
	if snapshot.Lease.InstanceID != a.instanceID || snapshot.Lease.NodeID != a.nodeID ||
		snapshot.Lease.State != "ACTIVE" ||
		snapshot.Lease.Revision < a.ownershipRevision.Load() {
		return ErrAuthorityChanged
	}
	owned := make(map[uint32]Ownership, len(snapshot.Slots))
	for _, o := range snapshot.Slots {
		if o.SlotID >= SlotCount || o.OwnerInstanceID != a.instanceID || o.Epoch == 0 {
			return ErrAuthorityChanged
		}
		if _, duplicate := owned[o.SlotID]; duplicate {
			return ErrAuthorityChanged
		}
		owned[o.SlotID] = o
	}
	a.slotsMu.Lock()
	defer a.slotsMu.Unlock()
	if a.stopping.Load() || a.fenced.Load() != nil {
		return ErrAuthorityChanged
	}
	for id, slot := range a.slots {
		o, ok := owned[id]
		if ok && o.State == SlotOwned && o.Epoch == slot.epoch.Load() {
			continue
		}
		slot.draining.Store(true)
		a.cancelSlotRetries(slot)
		a.cachedKeys.Add(-slot.count.Load())
		a.drainingSlots[id] = slot
		delete(a.slots, id)
	}
	// Rebuild only on authority changes; steady-state renewal never visits this map.
	a.pendingActive = nil
	for id, o := range owned {
		if o.State != SlotOwned {
			continue
		}
		if slot, ok := a.slots[id]; ok {
			if slot.draining.Load() {
				a.pendingActive = append(a.pendingActive, slot)
			}
			continue
		}
		slot := &allocationSlot{}
		slot.epoch.Store(o.Epoch)
		slot.draining.Store(true)
		a.slots[id] = slot
		a.pendingActive = append(a.pendingActive, slot)
	}
	a.rebuildCleanupSlotsLocked()
	a.ownershipRevision.Store(snapshot.Lease.Revision)
	return nil
}

func (a *Allocator) syncAuthorityLocked(ctx context.Context) error {
	snapshot, err := a.ownership.InstanceAuthority(ctx, a.instanceID)
	if err != nil {
		return err
	}
	return a.applyAuthorityLocked(snapshot)
}

// SyncAuthority re-reads and applies the instance's full authority snapshot.
func (a *Allocator) SyncAuthority(ctx context.Context) error {
	a.authorityMu.Lock()
	defer a.authorityMu.Unlock()
	if !a.initialized.Load() {
		return ErrAuthorityChanged
	}
	return a.syncAuthorityLocked(ctx)
}

func (a *Allocator) renewLocked(ctx context.Context) error {
	for attempt := 0; attempt < 2; attempt++ {
		anchor := a.monoNow()
		lease, err := a.ownership.RenewInstance(ctx, a.instanceID, a.ownershipRevision.Load())
		if errors.Is(err, ErrAuthorityChanged) && attempt == 0 {
			if err := a.syncAuthorityLocked(ctx); err != nil {
				return err
			}
			continue
		}
		if err != nil {
			return err
		}
		deadline := anchor + int64(a.ha.LeaseDeadline())
		if lease.InstanceID != a.instanceID || lease.Revision != a.ownershipRevision.Load() ||
			lease.State != "ACTIVE" ||
			a.monoNow() >= deadline ||
			a.stopping.Load() ||
			a.fenced.Load() != nil {
			return ErrLeaseExpired
		}
		a.localDeadline.Store(deadline)
		a.slotsMu.Lock()
		for _, slot := range a.pendingActive {
			slot.draining.Store(false)
		}
		a.pendingActive = nil
		a.slotsMu.Unlock()
		a.state.Store(StateReady)
		return nil
	}
	return ErrAuthorityChanged
}

// RenewLeases updates one storage row and one shared deadline regardless of slot count.
func (a *Allocator) RenewLeases() {
	if a.stopping.Load() || a.fenced.Load() != nil || !a.renewing.CompareAndSwap(false, true) {
		return
	}
	defer a.renewing.Store(false)
	ctx, cancel := context.WithTimeout(context.Background(), a.storeTimeout())
	defer cancel()
	a.authorityMu.Lock()
	defer a.authorityMu.Unlock()
	started := time.Now()
	if a.stopping.Load() || a.fenced.Load() != nil {
		return
	}
	err := func() error {
		if !a.initialized.Load() {
			if err := a.ownership.RegisterInstance(ctx, a.nodeID, a.instanceID); err != nil {
				return err
			}
			if err := a.syncAuthorityLocked(ctx); err != nil {
				return err
			}
			a.initialized.Store(true)
		}
		return a.renewLocked(ctx)
	}()
	if o := a.renewalObserver.Load(); o != nil {
		(*o)(time.Since(started), 1)
	}
	if err != nil {
		a.renewalFailed.Add(1)
		a.logger.Error("sequence instance renewal failed", "error", err)
		return
	}
	a.renewalSucceeded.Add(1)
}

// Claim takes over the listed slots in bounded batches, fenced by the instance
// revision, and applies the resulting authority.
func (a *Allocator) Claim(ctx context.Context, slots []uint32) error {
	if a.stopping.Load() || a.fenced.Load() != nil || !a.initialized.Load() {
		return ErrAuthorityChanged
	}
	var needed []uint32
	a.slotsMu.RLock()
	for _, id := range slots {
		if _, held := a.slots[id]; !held {
			needed = append(needed, id)
		}
	}
	a.slotsMu.RUnlock()
	for start := 0; start < len(needed); start += ownershipBatchSize {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		batchCtx, cancel := context.WithTimeout(ctx, a.storeTimeout())
		a.authorityMu.Lock()
		outcomes, err := a.ownership.ClaimSlots(
			batchCtx,
			ClaimRequest{
				Slots:       needed[start:min(start+ownershipBatchSize, len(needed))],
				NodeID:      a.nodeID,
				InstanceID:  a.instanceID,
				Revision:    a.ownershipRevision.Load(),
				Lease:       a.ha.LeaseDuration,
				QuietWindow: a.ha.QuietWindow,
			},
		)
		if err == nil {
			for _, outcome := range outcomes {
				if outcome.Granted {
					a.takeoversGranted.Add(1)
					a.epochChanges.Add(1)
				} else {
					a.takeoversRefused.Add(1)
				}
			}
			err = a.syncAuthorityLocked(batchCtx)
			if err == nil {
				err = a.renewLocked(batchCtx)
			}
		} else if errors.Is(err, ErrAuthorityChanged) {
			_ = a.syncAuthorityLocked(batchCtx)
		}
		a.authorityMu.Unlock()
		cancel()
		if err != nil {
			return err
		}
	}
	return nil
}

// SetHandoffs installs the handoff repository and migration limits.
func (a *Allocator) SetHandoffs(repo HandoffRepo, cfg MigrationConfig) {
	a.handoffs = repo
	a.migration = cfg
}

// HandoffPass drives the handoff state machine for this instance.
func (a *Allocator) HandoffPass(ctx context.Context) error {
	if a.handoffs == nil || !a.initialized.Load() || a.fenced.Load() != nil || a.stopping.Load() {
		return nil
	}
	handoffs, err := a.handoffs.Handoffs(ctx, a.instanceID, SlotCount)
	if err != nil {
		return err
	}
	// Retain withdrawn pointers until the exact drain acknowledgment is durable.
	needsDrain := make(map[uint32]uint64)
	for _, h := range handoffs {
		if h.SourceInstanceID == a.instanceID && h.Phase == "DRAINING" && !h.Drained {
			needsDrain[h.SlotID] = h.SourceEpoch
		}
	}
	a.slotsMu.Lock()
	for id, slot := range a.drainingSlots {
		if needsDrain[id] != slot.epoch.Load() && slot.inflight.Load() == 0 {
			delete(a.drainingSlots, id)
		}
	}
	a.slotsMu.Unlock()
	if len(handoffs) == 0 {
		return nil
	}
	if err := a.SyncAuthority(ctx); err != nil {
		return err
	}
	var commands []HandoffCommand
	sort.SliceStable(
		handoffs,
		func(i, j int) bool { return handoffPriority(handoffs[i]) < handoffPriority(handoffs[j]) },
	)
	for _, h := range handoffs {
		action := ""
		if h.TargetInstanceID == a.instanceID && a.monoNow() < a.localDeadline.Load() {
			switch h.Phase {
			case "PLANNED":
				action = "PREPARE"
			case "READY":
				action = "BEGIN"
			case "DRAINING":
				if h.TargetReady {
					action = "TRANSFER"
				} else {
					action = "PREPARE"
				}
			case "TRANSFERRED":
				a.slotsMu.RLock()
				slot := a.slots[h.SlotID]
				ready := slot != nil && !slot.draining.Load() &&
					slot.epoch.Load() == h.SourceEpoch+1
				a.slotsMu.RUnlock()
				if ready {
					action = "ACK_ACTIVE"
				}
			}
		} else if h.SourceInstanceID == a.instanceID && h.Phase == "DRAINING" {
			a.slotsMu.RLock()
			slot := a.drainingSlots[h.SlotID]
			drained := slot != nil && slot.epoch.Load() == h.SourceEpoch && slot.draining.Load() &&
				slot.inflight.Load() == 0
			a.slotsMu.RUnlock()
			if drained && !h.Drained {
				action = "ACK_DRAIN"
			} else if h.Kind == "RELEASE" {
				action = "TRANSFER"
			}
		}
		if action != "" {
			commands = append(commands, HandoffCommand{Handoff: h, Action: action})
			if len(commands) == a.migration.BatchSlots {
				break
			}
		}
	}
	return a.handoffs.ApplyHandoffs(
		ctx,
		HandoffRequest{
			InstanceID: a.instanceID,
			Revision:   a.ownershipRevision.Load(),
			Commands:   commands,
			HA:         a.ha,
			Limits:     a.migration,
		},
	)
}

func handoffPriority(h Handoff) int {
	switch h.Phase {
	case "TRANSFERRED":
		return 0
	case "DRAINING":
		return 1
	case "READY":
		return 2
	default:
		return 3
	}
}

// includeHeldAuthority adds the slots the instance holds in storage but has not
// installed in its cache.
//
// The cache is not the authority. A handoff can grant this instance a slot
// between two local syncs, and a shutdown in that window still has to give the
// slot back, or the successor waits out a full quiet window for authority this
// instance is no longer using. The stored snapshot is read under this
// instance's own identity, so a slot another instance holds is never added, and
// only fully owned slots -- the ones a release can revoke -- are considered.
// Slots the cache already knows keep the gate they were serving under; the
// added slots have no gate to close because the serving path never saw them.
func (a *Allocator) includeHeldAuthority(
	ctx context.Context,
	detached []detachedSlot,
) []detachedSlot {
	readCtx := ctx
	if _, ok := readCtx.Deadline(); !ok {
		var cancel context.CancelFunc
		readCtx, cancel = context.WithTimeout(
			readCtx,
			a.ha.ReleaseDrainTimeout+a.storeTimeout(),
		)
		defer cancel()
	}
	snapshot, err := a.ownership.InstanceAuthority(readCtx, a.instanceID)
	if err != nil {
		// The cache is still released, and anything read here is picked up by
		// the recovery path once the instance lease lapses.
		a.logger.Warn("sequence authority read failed during shutdown", "error", err)
		return detached
	}
	known := make(map[uint32]struct{}, len(detached))
	for _, item := range detached {
		known[item.slotID] = struct{}{}
	}
	for _, o := range snapshot.Slots {
		if o.State != SlotOwned || o.Epoch == 0 {
			continue
		}
		if _, ok := known[o.SlotID]; ok {
			continue
		}
		slot := &allocationSlot{}
		slot.epoch.Store(o.Epoch)
		slot.draining.Store(true)
		detached = append(detached, detachedSlot{slotID: o.SlotID, slot: slot})
	}
	return detached
}

func (a *Allocator) drainAndRelease(ctx context.Context, detached []detachedSlot) {
	if a.handoffs == nil || len(detached) == 0 {
		return
	}
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(
			ctx,
			a.ha.ReleaseDrainTimeout+a.storeTimeout(),
		)
		defer cancel()
	}
	for start := 0; start < len(detached); start += a.migration.BatchSlots {
		batch := detached[start:min(start+a.migration.BatchSlots, len(detached))]
		var commands []HandoffCommand
		for _, item := range batch {
			commands = append(
				commands,
				HandoffCommand{
					Action: "WITHDRAW",
					Handoff: Handoff{
						SlotID:           item.slotID,
						ID:               uuid.NewString(),
						SourceInstanceID: a.instanceID,
						SourceEpoch:      item.slot.epoch.Load(),
					},
				},
			)
		}
		if err := a.handoffs.ApplyHandoffs(
			ctx,
			HandoffRequest{
				InstanceID: a.instanceID,
				Commands:   commands,
				HA:         a.ha,
				Limits:     a.migration,
			},
		); err != nil {
			a.releasesFailed.Add(1)
			a.logger.Error("sequence shutdown withdraw failed", "error", err)
			break
		}
		pending, err := a.handoffs.Handoffs(ctx, a.instanceID, a.migration.MaxPlanned)
		if err != nil {
			a.releasesFailed.Add(1)
			a.logger.Error("sequence shutdown handoff read failed", "error", err)
			break
		}
		bySlot := make(map[uint32]Handoff)
		for _, h := range pending {
			bySlot[h.SlotID] = h
		}
		commands = nil
		for _, item := range batch {
			h, ok := bySlot[item.slotID]
			if !ok || h.Phase != "DRAINING" || h.SourceEpoch != item.slot.epoch.Load() {
				continue
			}
			if item.slot.drain(max(time.Until(deadlineOf(ctx)), 0)) {
				commands = append(commands, HandoffCommand{Action: "ACK_DRAIN", Handoff: h})
			}
		}
		if err := a.handoffs.ApplyHandoffs(
			ctx,
			HandoffRequest{
				InstanceID: a.instanceID,
				Commands:   commands,
				HA:         a.ha,
				Limits:     a.migration,
			},
		); err != nil {
			a.releasesFailed.Add(1)
			a.logger.Error("sequence shutdown drain ack failed", "error", err)
			break
		}
		for i := range commands {
			commands[i].Action = "TRANSFER"
		}
		if err := a.handoffs.ApplyHandoffs(
			ctx,
			HandoffRequest{
				InstanceID: a.instanceID,
				Commands:   commands,
				HA:         a.ha,
				Limits:     a.migration,
			},
		); err != nil {
			a.releasesFailed.Add(1)
			a.logger.Error("sequence shutdown transfer failed", "error", err)
			break
		}
		a.releasesReleased.Add(int64(len(commands)))
		a.drains.Add(1)
	}
}
func deadlineOf(ctx context.Context) time.Time { deadline, _ := ctx.Deadline(); return deadline }
func (a *Allocator) cancelSlotRetries(slot *allocationSlot) {
	slot.states.Range(func(_, value any) bool { value.(*keyState).cancelRetry(); return true })
}

func (a *Allocator) enterSlot(id uint32) (*allocationSlot, error) {
	a.slotsMu.RLock()
	defer a.slotsMu.RUnlock()
	if a.Paused() {
		return nil, xerror.NewWithReason(
			reason.Reason_SEQUENCE_ALLOCATOR_PAUSED,
			"allocator is paused",
			nil,
		)
	}
	slot := a.slots[id]
	if slot == nil || !slot.enter() {
		return nil, xerror.NewWithReason(
			reason.Reason_SEQUENCE_SLOT_NOT_OWNER,
			"slot is not serving",
			nil,
		)
	}
	return slot, nil
}

func (a *Allocator) authorize(_ []uint32, held []*allocationSlot) error {
	for _, slot := range held {
		if err := a.checkAllocationSlot(slot); err != nil {
			return err
		}
	}
	return nil
}

func (a *Allocator) checkLocalLease(_ *allocationSlot) error {
	if deadline := a.localDeadline.Load(); deadline > 0 && a.monoNow() < deadline {
		return nil
	}
	a.leaseExpired.Add(1)
	return xerror.NewWithReason(
		reason.Reason_SEQUENCE_LEASE_EXPIRED,
		"instance lease has expired",
		nil,
	)
}

func (a *Allocator) checkAllocationSlot(slot *allocationSlot) error {
	if slot == nil || slot.draining.Load() || a.stopping.Load() || a.fenced.Load() != nil {
		return xerror.NewWithReason(
			reason.Reason_SEQUENCE_OWNER_RECOVERING,
			"slot is not serving",
			nil,
		)
	}
	return a.checkLocalLease(slot)
}

func (a *Allocator) checkFastPathBound(slot *allocationSlot, started int64) error {
	pause := time.Duration(a.monoNow() - started)
	a.lastPauseMicros.Store(secondsToMicros(pause))
	if pause <= a.ha.MaxPause {
		return nil
	}
	a.pauseViolations.Add(1)
	a.fenceSlot(slot, "process pause exceeded its bound")
	a.Fence("process pause exceeded its bound")
	return xerror.NewWithReason(
		reason.Reason_SEQUENCE_OWNER_RECOVERING,
		"process pause exceeded its bound",
		nil,
	)
}

func (a *Allocator) fenceSlot(slot *allocationSlot, message string) {
	if slot != nil && slot.draining.CompareAndSwap(false, true) {
		a.gateFenced.Add(1)
		a.logger.Error("sequence slot fenced", "reason", message)
	}
}

// RenewalBatchObserver receives the duration and size of one renewal batch.
type RenewalBatchObserver func(time.Duration, int)

// SetRenewalBatchObserver installs the renewal batch observer, or clears it.
func (a *Allocator) SetRenewalBatchObserver(observer RenewalBatchObserver) {
	if observer == nil {
		a.renewalObserver.Store(nil)
	} else {
		a.renewalObserver.Store(&observer)
	}
}

// Inflight reports the number of allocations currently in flight.
func (a *Allocator) Inflight() int64 {
	a.slotsMu.RLock()
	defer a.slotsMu.RUnlock()
	var total int64
	for _, slot := range a.slots {
		total += slot.inflight.Load()
	}
	for _, slot := range a.drainingSlots {
		total += slot.inflight.Load()
	}
	return total
}

// RetryAfter returns the backoff a caller should observe for a reason.
func (a *Allocator) RetryAfter(r reason.Reason) time.Duration {
	if r == reason.Reason_SEQUENCE_OWNER_RECOVERING {
		return 250 * time.Millisecond
	}
	return 0
}

// LeaseStats reports the allocator's lease and fence counters.
type LeaseStats struct {
	LeaseExpired     int64
	PauseViolations  int64
	RenewalSucceeded int64
	RenewalFailed    int64
	GateFenced       int64
}

// LeaseStats returns a snapshot of the lease counters.
func (a *Allocator) LeaseStats() LeaseStats {
	return LeaseStats{
		LeaseExpired:     a.leaseExpired.Load(),
		PauseViolations:  a.pauseViolations.Load(),
		RenewalSucceeded: a.renewalSucceeded.Load(),
		RenewalFailed:    a.renewalFailed.Load(),
		GateFenced:       a.gateFenced.Load(),
	}
}

// QuietWindow returns the configured quiet window.
func (a *Allocator) QuietWindow() time.Duration { return a.ha.QuietWindow }

func (a *Allocator) String() string { return fmt.Sprintf("sequence instance %s", a.instanceID) }
