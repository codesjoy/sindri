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

// This file holds the slot-placement model shared by the process that serves ids
// and the process that decides where its slots live.
//
// The types here are the authority's vocabulary -- what a slot's ownership is,
// what a target node is, which nodes are live -- and the rules that turn them
// into intent. They live outside either service because a second copy of a rule
// this load-bearing is a second place for it to be wrong: the two processes must
// agree slot for slot, and an alias is how they share one definition instead of
// two that happen to look alike.
//
// The publisher lives here too: it is the control-plane loop that materialises
// this vocabulary into the directory every node follows, and it reads ownership
// through the same repository interfaces.

package biz

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync/atomic"
	"time"

	sequencepkg "github.com/codesjoy/sindri/pkg/sequence"
)

// SlotCount is the fixed number of routing slots. It is re-exported from the
// routing contract rather than restated, so the planner and the routers that
// consume its output cannot drift apart.
const SlotCount = sequencepkg.SlotCount

// SlotState is the authoritative ownership state of one routing slot.
//
// SlotUnowned means the slot has no owner: either it was never granted, or an
// explicit release committed after the in-flight barrier drained. It must never
// be used to mean "a candidate guesses the old owner is dead" (section 5.1).
type SlotState string

const (
	// SlotUnowned indicates that no instance currently holds the slot.
	SlotUnowned SlotState = "UNOWNED"
	// SlotOwned indicates that exactly one instance holds the slot for one epoch.
	SlotOwned SlotState = "OWNED"
)

// Ownership is the authoritative record for one slot.
type Ownership struct {
	SlotID          uint32
	OwnerNodeID     string
	OwnerInstanceID string
	Epoch           uint64
	// GrantedAgo is how long ago the storage granted or last renewed the slot,
	// measured by the storage clock in the statement that read the row.
	//
	// The node must not derive this from its own clock. The quiet window is a
	// question about storage time, and platforms exist whose node clock cannot be
	// trusted, so comparing a local reading against a stored instant would put
	// that untrusted clock back into the safety argument. Reading the difference
	// from the database keeps one clock on both sides of the comparison.
	GrantedAgo time.Duration
	// GrantAgeKnown reports whether GrantedAgo was read. A row that claims OWNED
	// without a grant time is refused rather than served on an age of zero.
	GrantAgeKnown bool
	State         SlotState
}

// Holds reports whether instanceID is the authority for the slot at epoch.
func (o Ownership) Holds(instanceID string, epoch uint64) bool {
	return o.State == SlotOwned && o.OwnerInstanceID == instanceID && o.Epoch == epoch
}

// OwnershipSegment is one contiguous run of slots that shares one authority.
//
// The authority table holds one row per slot, but every decision the planner
// and the publisher make is about a run: slots are claimed and released in
// whole assigned ranges, and consecutive slots with the same owner, instance,
// epoch and grant age are decided identically. Reading the compact form keeps
// the control-plane read proportional to the number of runs -- O(live nodes)
// in a healthy fleet -- rather than to SlotCount.
//
// The quiet-window classification is part of the grouping key on purpose: a
// run whose earlier slots have aged past W while its later ones have not is two
// segments, because the planner decides them differently.
type OwnershipSegment struct {
	StartSlot uint32
	EndSlot   uint32
	// OwnerNodeID and OwnerInstanceID are empty exactly when the slots are
	// unowned. An unowned run is paused rather than routed.
	OwnerNodeID     string
	OwnerInstanceID string
	Epoch           uint64
	State           SlotState
	// GrantAgeKnown reports whether the reader saw a grant time for the run. A
	// run that claims OWNED without one is refused by the planner rather than
	// served on an age of zero.
	GrantAgeKnown bool
	// QuietWindowOverdue reports whether the run's grant had already aged past
	// the reader's quiet window when the storage answered. It is only ever true
	// for a run whose grant age is known.
	QuietWindowOverdue bool
}

// Slots returns how many slots the segment covers.
func (s OwnershipSegment) Slots() uint32 {
	if s.EndSlot < s.StartSlot {
		return 0
	}
	return s.EndSlot - s.StartSlot + 1
}

// Owned reports whether the segment names a complete authority.
func (s OwnershipSegment) Owned() bool {
	return s.State == SlotOwned && s.OwnerNodeID != "" && s.OwnerInstanceID != ""
}

// ValidateOwnershipSegments checks that a compact ownership read is an ordered,
// gap-free, non-overlapping cover of the whole slot space, and that every
// segment names a consistent authority.
//
// It is the check both readers run before they act on a view. A partial view is
// indistinguishable from a fleet that owns nothing, so a reader that skipped
// this check could release every slot it serves on the strength of a short
// read, or publish a directory with missing coverage.
func ValidateOwnershipSegments(segments []OwnershipSegment) error {
	if len(segments) == 0 {
		return errors.New("sequence placement: ownership segment view is empty")
	}
	next := uint32(0)
	for _, segment := range segments {
		if segment.EndSlot >= SlotCount || segment.EndSlot < segment.StartSlot {
			return fmt.Errorf(
				"sequence placement: ownership segment [%d,%d] is out of range",
				segment.StartSlot,
				segment.EndSlot,
			)
		}
		if segment.StartSlot != next {
			return fmt.Errorf(
				"sequence placement: ownership segment [%d,%d] does not continue "+
					"the coverage at slot %d",
				segment.StartSlot,
				segment.EndSlot,
				next,
			)
		}
		switch segment.State {
		case SlotOwned:
			if segment.OwnerNodeID == "" || segment.OwnerInstanceID == "" {
				return fmt.Errorf(
					"sequence placement: ownership segment [%d,%d] is OWNED "+
						"without a node and instance",
					segment.StartSlot,
					segment.EndSlot,
				)
			}
		case SlotUnowned:
			if segment.OwnerNodeID != "" || segment.OwnerInstanceID != "" {
				return fmt.Errorf(
					"sequence placement: ownership segment [%d,%d] is UNOWNED "+
						"but names an owner",
					segment.StartSlot,
					segment.EndSlot,
				)
			}
		default:
			return fmt.Errorf(
				"sequence placement: ownership segment [%d,%d] has unknown state %q",
				segment.StartSlot,
				segment.EndSlot,
				segment.State,
			)
		}
		if segment.QuietWindowOverdue && !segment.GrantAgeKnown {
			return fmt.Errorf(
				"sequence placement: ownership segment [%d,%d] is overdue "+
					"without a grant time",
				segment.StartSlot,
				segment.EndSlot,
			)
		}
		next = segment.EndSlot + 1
	}
	if next != SlotCount {
		return fmt.Errorf(
			"sequence placement: ownership segment view covers %d of %d slots",
			next,
			SlotCount,
		)
	}
	return nil
}

// OwnershipSegmentsFromView compacts a per-slot authority view into the segment
// form. It is the bridge for readers that hold the diagnostic slot view, and it
// refuses a view that is not a complete cover rather than compacting a hole
// into a wrong assignment.
func OwnershipSegmentsFromView(
	view []Ownership,
	quietWindow time.Duration,
) ([]OwnershipSegment, error) {
	return ownershipSegmentsFromView(view, func(slot Ownership) bool {
		return slot.State == SlotOwned && slot.GrantAgeKnown &&
			slot.GrantedAgo >= quietWindow
	})
}

// ownershipSegmentsFromView is the shared compaction. The classification is a
// parameter so the publisher can compact a view without minting a quiet-window
// judgement it does not make.
func ownershipSegmentsFromView(
	view []Ownership,
	overdue func(Ownership) bool,
) ([]OwnershipSegment, error) {
	if len(view) != SlotCount {
		return nil, fmt.Errorf(
			"sequence placement: ownership view covers %d of %d slots",
			len(view),
			SlotCount,
		)
	}
	ordered := append([]Ownership(nil), view...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].SlotID < ordered[j].SlotID })
	segments := make([]OwnershipSegment, 0, 8)
	for index, slot := range ordered {
		if slot.SlotID != uint32(index) {
			return nil, fmt.Errorf(
				"sequence placement: ownership view is missing slot %d",
				index,
			)
		}
		isOverdue := slot.State == SlotOwned && overdue(slot)
		last := len(segments) - 1
		if last >= 0 && segments[last].EndSlot+1 == slot.SlotID &&
			segments[last].State == slot.State &&
			segments[last].OwnerNodeID == slot.OwnerNodeID &&
			segments[last].OwnerInstanceID == slot.OwnerInstanceID &&
			segments[last].Epoch == slot.Epoch &&
			segments[last].GrantAgeKnown == (slot.GrantAgeKnown && slot.State == SlotOwned) &&
			segments[last].QuietWindowOverdue == isOverdue {
			segments[last].EndSlot = slot.SlotID
			continue
		}
		segments = append(segments, OwnershipSegment{
			StartSlot:          slot.SlotID,
			EndSlot:            slot.SlotID,
			OwnerNodeID:        slot.OwnerNodeID,
			OwnerInstanceID:    slot.OwnerInstanceID,
			Epoch:              slot.Epoch,
			State:              slot.State,
			GrantAgeKnown:      slot.GrantAgeKnown && slot.State == SlotOwned,
			QuietWindowOverdue: isOverdue,
		})
	}
	if err := ValidateOwnershipSegments(segments); err != nil {
		return nil, err
	}
	return segments, nil
}

// NodeInfo identifies a sequence service node that is currently live.
type NodeInfo struct {
	ID string
	// InstanceID is the process that most recently registered this node id. The
	// node id names a position in the fleet; the instance is what fences
	// ownership, so a restarted node takes its id over from its predecessor.
	InstanceID string
	// LastSeenAt is when the node last registered. It is a wall-clock instant,
	// not a tick, so liveness does not depend on a configured tick interval.
	LastSeenAt time.Time
}

// SlotTarget is the intended owner of one slot.
type SlotTarget struct {
	SlotID uint32
	// TargetNodeID is empty when the slot must not be served: it is either
	// waiting for a departing owner to release it, or waiting to be claimed. An
	// empty target holds a slot back; it never preempts an owner.
	TargetNodeID string
}

// PlanTargets computes the intent for every slot from the authority view and the
// live node set.
//
// A target is intent, never authority: the store still decides, and a candidate's
// claim is what has to pass the quiet window. That is why these rules can be
// optimistic about a slot whose owner cannot serve it -- a claim the window holds
// back preempts nothing.
//
// The live set is the fleet's membership as the storage records it, including
// which process most recently registered each node id. Every node computes this
// same function over the same two tables, so the whole fleet agrees on the
// placement without a decision travelling between the processes.
//
//   - a slot nobody owns is assigned to a live node, deterministically;
//   - a slot whose owner is live and whose grant is fresh stays with that owner,
//     so a node joining does not move a single serving slot (D18);
//   - a slot whose node id is live but whose live row names a different process
//     stays with that node id: the node's id names a position in the fleet, and
//     the restarted process on it takes the slot back once its own claim clears
//     the quiet window;
//   - a slot whose owner is not live is assigned to another live node. Leaving it
//     unassigned would strand it: a lapsed lease does not rewrite the ownership
//     row, and only an explicit release does, so a dead instance would keep the
//     slot forever and no one could take it over;
//   - a slot whose owner is live but whose grant has lapsed past the quiet window
//     is assigned away too, because such an owner already refuses to serve it:
//     its own read stops strictly inside that window.
//
// The last two rules are what make a node failure recoverable by another node.
// Neither preempts anything: it is the claim, inside the authority store, that
// waits for the window before it may succeed.
func PlanTargets(
	view []Ownership,
	liveNodes []NodeInfo,
	quietWindow time.Duration,
) []SlotTarget {
	facts := make([]slotFacts, 0, len(view))
	for _, slot := range view {
		facts = append(facts, slotFacts{
			slotID:          slot.SlotID,
			state:           slot.State,
			ownerNodeID:     slot.OwnerNodeID,
			ownerInstanceID: slot.OwnerInstanceID,
			grantAgeKnown:   slot.GrantAgeKnown,
			overdue: slot.State == SlotOwned && slot.GrantAgeKnown &&
				slot.GrantedAgo >= quietWindow,
		})
	}
	return planTargets(facts, liveNodes)
}

// PlanTargetsFromSegments computes the intent for every slot from the compact
// ownership view and the live node set.
//
// The segment view is refused unless it is a complete, ordered, gap-free cover
// of the space, which is what makes it safe to expand into per-slot intent: a
// short read must not look like a fleet that owns nothing.
func PlanTargetsFromSegments(
	segments []OwnershipSegment,
	liveNodes []NodeInfo,
) ([]SlotTarget, error) {
	if err := ValidateOwnershipSegments(segments); err != nil {
		return nil, err
	}
	facts := make([]slotFacts, 0, SlotCount)
	for _, segment := range segments {
		for slot := segment.StartSlot; slot <= segment.EndSlot; slot++ {
			facts = append(facts, slotFacts{
				slotID:          slot,
				state:           segment.State,
				ownerNodeID:     segment.OwnerNodeID,
				ownerInstanceID: segment.OwnerInstanceID,
				grantAgeKnown:   segment.GrantAgeKnown,
				overdue:         segment.QuietWindowOverdue,
			})
		}
	}
	return planTargets(facts, liveNodes), nil
}

// slotFacts is the per-slot form of both ownership reads. The planner takes one
// shape so the slot view and the segment view cannot disagree about a rule.
type slotFacts struct {
	slotID          uint32
	state           SlotState
	ownerNodeID     string
	ownerInstanceID string
	grantAgeKnown   bool
	overdue         bool
}

func planTargets(facts []slotFacts, liveNodes []NodeInfo) []SlotTarget {
	liveIDs := ActiveNodeIDs(liveNodes)
	live := make(map[string]struct{}, len(liveIDs))
	// instanceOf is the process the fleet last heard from for a node id. It is
	// what tells "the same instance is still serving" from "the id came back as
	// a new process", and the two get opposite answers for an owned slot.
	instanceOf := make(map[string]string, len(liveIDs))
	for _, node := range liveNodes {
		live[node.ID] = struct{}{}
		instanceOf[node.ID] = node.InstanceID
	}
	// The split for "everyone except this owner", cached per owner: a node whose
	// slots can no longer be served must not have them assigned back to it, and
	// there are only ever a handful of distinct owners.
	without := make(map[string][]string, 2)
	otherNodes := func(avoid string) []string {
		if cached, ok := without[avoid]; ok {
			return cached
		}
		filtered := make([]string, 0, len(liveIDs))
		for _, nodeID := range liveIDs {
			if nodeID != avoid {
				filtered = append(filtered, nodeID)
			}
		}
		without[avoid] = filtered
		return filtered
	}

	targets := make([]SlotTarget, 0, len(facts))
	for _, slot := range facts {
		target := SlotTarget{SlotID: slot.slotID}
		_, ownerLive := live[slot.ownerNodeID]
		switch {
		case slot.state != SlotOwned:
			// Nobody holds it: bootstrap, or it just came back from a release.
			target.TargetNodeID = SplitOwner(slot.slotID, liveIDs)
		case !ownerLive:
			// The owner is not in the live set at all. Its row still names it,
			// because a crash cannot release, so the slot has to be assigned to
			// somebody who can claim it once the quiet window allows.
			target.TargetNodeID = SplitOwner(slot.slotID, otherNodes(slot.ownerNodeID))
		case slot.ownerNodeID != "" && slot.ownerInstanceID != "" &&
			instanceChanged(slot.ownerInstanceID, instanceOf[slot.ownerNodeID]):
			// The node id is back under a new process. The slot belongs to the
			// position, not to the process that vacated it, so the restart
			// reclaims it: the claim it makes waits out the quiet window, which
			// is exactly the safety margin the old process's lease needs.
			target.TargetNodeID = slot.ownerNodeID
		case slot.overdue:
			// The owner is the same live process but its grant has lapsed past
			// the window, so it already refuses to serve the slot. Assign it
			// away rather than back to a node whose own read stops inside the
			// window.
			target.TargetNodeID = SplitOwner(slot.slotID, otherNodes(slot.ownerNodeID))
		default:
			// A live owner with a fresh grant keeps its slots. An unknown age is
			// treated as fresh: without one there is nothing that says the grant
			// lapsed, and moving a serving slot on a guess is what D18 forbids.
			target.TargetNodeID = slot.ownerNodeID
		}
		targets = append(targets, target)
	}
	return targets
}

// instanceChanged reports whether the live row names a process other than the
// one holding the slot. Only a difference between two known instance ids counts:
// a row that does not carry one says nothing, and treating it as a change would
// move a slot on a missing field rather than on evidence.
func instanceChanged(ownerInstanceID, liveInstanceID string) bool {
	return ownerInstanceID != "" &&
		liveInstanceID != "" &&
		liveInstanceID != ownerInstanceID
}

// SplitOwner returns the node the deterministic even split assigns to a slot.
//
// The remainder goes to the lowest node ids, and the answer is a pure function of
// the sorted node set, so every node computes the same placement for the same
// fleet without coordinating. That is deliberate: an assignment that can be
// computed is one that can be audited by reading it, unlike a rendezvous hash
// whose membership change would silently remap most of the space.
func SplitOwner(slot uint32, liveNodes []string) string {
	if len(liveNodes) == 0 {
		return ""
	}
	// Slot counts are small enough that this division is exact in uint32, and the
	// space is divided here rather than by hashing so the split stays readable.
	base := uint32(SlotCount) / uint32(len(liveNodes))
	extra := uint32(SlotCount) % uint32(len(liveNodes))
	offset := uint32(0)
	for index, nodeID := range liveNodes {
		size := base
		if uint32(index) < extra {
			size++
		}
		if slot < offset+size {
			return nodeID
		}
		offset += size
	}
	return ""
}

// ActiveNodeIDs returns the live nodes ordered by node id, which is the order
// PlanTargets divides the space in.
func ActiveNodeIDs(nodes []NodeInfo) []string {
	ids := make([]string, 0, len(nodes))
	for _, node := range nodes {
		ids = append(ids, node.ID)
	}
	sort.Strings(ids)
	return ids
}

// PlacementRepo is the storage view a node plans its own slots from.
//
// It is the read half of the placement tables that the node already shares with
// the rest of the fleet, and it is deliberately small: slot ownership is the
// authority a claim has to match, and liveness is the fleet membership that
// decides which node id a slot belongs to. A node that read a stale or partial
// answer must not replan against it, so both reads are all-or-nothing.
type PlacementRepo interface {
	// OwnershipSegments returns the compact authority view: contiguous runs of
	// slots sharing one owner, instance, epoch, state and quiet-window
	// classification, ordered by slot, with each run's grant age measured by the
	// storage clock. The view must cover every slot exactly once; a caller
	// refuses anything else rather than planning against a partial read.
	OwnershipSegments(
		ctx context.Context,
		quietWindow time.Duration,
	) ([]OwnershipSegment, error)
	// LiveNodes returns the nodes whose liveness lease has not lapsed within
	// ttl, judged by the storage clock rather than by this process's clock.
	LiveNodes(ctx context.Context, ttl time.Duration) ([]NodeInfo, error)
}

// PublishResult reports what one published revision cost.
type PublishResult struct {
	// Revision is the directory revision the view was published under. A view
	// identical to the one already stored returns the existing revision.
	Revision int64
	// PayloadBytes is the encoded size of the snapshot, whether it was written
	// or matched against the one already stored.
	PayloadBytes int
	// RetentionDeleted counts route rows pruned by the retention bound in this
	// publish. Rows removed by an earlier publish are not counted again.
	RetentionDeleted int64
}

// PublisherRepo is the storage side of the directory publisher.
type PublisherRepo interface {
	// OwnershipSegments returns the compact authority view, ordered by slot.
	// The publisher snapshots exactly this view, which is why it reads no
	// liveness and computes no targets: which node holds a slot is already the
	// answer.
	OwnershipSegments(
		ctx context.Context,
		quietWindow time.Duration,
	) ([]OwnershipSegment, error)
	// MaterialiseRoute publishes a snapshot of the ownership view under a
	// revision it advances atomically. The write is refused with
	// ErrCoordinatorLost when the presented tenure is no longer the one in
	// force. retention is how many of the newest revisions stay in the table;
	// older rows are deleted in the same transaction.
	MaterialiseRoute(
		ctx context.Context,
		segments []OwnershipSegment,
		layoutVersion int64,
		retention int,
		lease CoordinatorLease,
	) (PublishResult, error)
}

// CoordinatorRepo is the lease that keeps a single publisher in the fleet.
type CoordinatorRepo interface {
	// AcquireCoordinator takes the lease when it is free or has lapsed, or
	// renews it when this instance already holds it, and reports this
	// instance's tenure afterwards.
	AcquireCoordinator(
		ctx context.Context,
		instanceID string,
		lease time.Duration,
	) (CoordinatorLease, error)
}

// CoordinatorLease is one replica's tenure of the reconciler role.
//
// It is not a capability: it authorises intent, never the right to hand out an id.
// What it is for is telling the two halves of a reconciler pass apart -- the
// replica that acquired the role at the start of a pass and the one that holds it
// when the pass writes. A pass acquires the role, may then spend a while planning,
// and presents this value to each write, so a write that arrives after another
// replica took the role over is refused rather than published on behalf of a role
// nobody holds any more.
//
// The publisher is the only writer of a directory revision, and it writes
// through this one definition: a second copy of a credential check this
// load-bearing is a second place for it to be wrong.
type CoordinatorLease struct {
	// Held is whether the caller held the role at all. A replica that did not take
	// it must not plan, so this is what a non-holder checks before doing anything.
	Held bool
	// InstanceID is which replica holds the role. It distinguishes tenures across
	// a process restart, where the epoch alone could repeat.
	InstanceID string
	// Epoch is the fencing token. It moves only when the role actually changes
	// hands -- not on every renewal -- so a replica renewing its own lease leaves
	// the writes it has in flight valid, while a takeover invalidates every write
	// the previous holder had not yet committed.
	Epoch uint64
}

// ErrCoordinatorLost reports that a placement write was refused because the
// tenure it presented is no longer the one in force.
//
// It is a refusal rather than a failure: the replica that took the role over
// recomputes whatever the abandoned pass did not finish, and a slot's intended
// owner is only intent, so nothing the fleet serves depends on the decision that
// was dropped. A caller counts these rather than reporting them as errors,
// because a fleet that is repeatedly losing the role is not converging, and that
// is worth an alert instead of a log line.
var ErrCoordinatorLost = errors.New("placement: coordinator lease was lost")

// SlotAuthority is the exact authority a node believes it holds for one slot.
type SlotAuthority struct {
	SlotID     uint32
	InstanceID string
	Epoch      uint64
}

// ClaimRequest asks the authority store to grant slots to one instance.
//
// QuietWindow is W from section 3.3: an owned slot may only be taken over once
// the storage lease clock has advanced that far past the last grant. An unowned
// slot is granted immediately, which is what makes an explicit release take
// effect without waiting (decisions D7 and D16).
type ClaimRequest struct {
	Slots       []uint32
	NodeID      string
	InstanceID  string
	QuietWindow time.Duration
}

// ClaimOutcome reports what the authority store decided for one slot.
type ClaimOutcome struct {
	Ownership Ownership
	Granted   bool
	// NotBefore is when a refused takeover may be retried. It is the zero time
	// when the claim was granted.
	NotBefore time.Time
}

// RenewGroup is the set of slots that share one authority fence: the same
// instance holds them at the same epoch.
type RenewGroup struct {
	InstanceID string
	Epoch      uint64
	Slots      []uint32
}

// RenewRequest refreshes the grant time of slots the caller already holds.
//
// Authorities are grouped by instance and epoch rather than listed per slot.
// The epoch-CAS predicate is what makes a renewal safe, and it is the same
// predicate for every slot in a group, so one statement can carry the whole
// group: a node renewing every slot it owns costs one round trip per ownership
// generation instead of one per slot.
type RenewRequest struct {
	Groups []RenewGroup
}

// ReservationAuthority is the authority a node presents when reserving ranges.
//
// InstanceID is the process-start UUID. A nil Epochs map skips the per-slot
// epoch comparison, leaving the instance identity as the fence; a non-nil map
// additionally requires epoch equality. A positive Lease enforces the storage
// lease of section 6.1: the storage re-checks granted_at against the same clock
// that granted it, so a reservation made after the grant lapsed is refused by
// the authority rather than by a read the node would have to trust itself. Every
// reservation carries both, because the steady-state allocation path never
// consults storage otherwise.
type ReservationAuthority struct {
	InstanceID string
	Epochs     map[uint32]uint64
	Lease      time.Duration
}

// OwnershipRepo is the storage authority for slot ownership.
type OwnershipRepo interface {
	// LoadOwnership reads the authority rows for the given slots, with each row's
	// grant age measured by the storage clock. It must always be served by the
	// primary: a stale replica read would report an age that is short by the
	// replication lag. The allocation path does not call it -- the lease carried
	// on each reservation is what fences that -- so it serves diagnostics, the
	// placement planner and the tests that pin the readable shape.
	LoadOwnership(ctx context.Context, slots []uint32) ([]Ownership, error)
	// ClaimSlots grants each slot to the caller when it is unowned, or once the
	// quiet window has elapsed since the last grant (section 6.4).
	ClaimSlots(ctx context.Context, request ClaimRequest) ([]ClaimOutcome, error)
	// RenewSlots refreshes granted_at for slots the caller still holds. The
	// epoch-CAS predicate is the guard: a slot whose epoch or instance has moved
	// is left alone, so a renewal can never resurrect authority a takeover
	// replaced, and the lease is deliberately not consulted. A lease may expire
	// early but never late, so a renewal that cannot be confirmed must not
	// extend a local deadline.
	RenewSlots(ctx context.Context, request RenewRequest) ([]Ownership, error)
	// ReleaseSlots revokes authority only where the epoch-CAS predicate
	// slot_id + owner_instance_id + epoch + state = OWNED still matches, and
	// returns how many rows it revoked (section 6.5).
	ReleaseSlots(ctx context.Context, targets []SlotAuthority) (int64, error)
	// StorageClock reads the authority store's lease clock.
	//
	// It is on this interface because the clock belongs to the authority: every
	// bound in the protocol is expressed in storage time, so the thing that
	// grants authority is the thing whose clock has to be observed. The monitor
	// compares successive readings with the local clock to turn the asserted J_max
	// and delta into evidence (appendix E).
	StorageClock(ctx context.Context) (time.Time, error)
}

var (
	// ErrSlotNotOwned reports that the caller is not the authority for a slot it
	// tried to reserve under.
	ErrSlotNotOwned = errors.New("sequence: slot is not owned by this instance")
	// ErrLeaseExpired reports that the storage lease for a slot had already
	// lapsed when the reservation ran.
	ErrLeaseExpired = errors.New("sequence: slot lease has expired")
	// ErrCommitUncertain reports that a write may or may not have committed.
	// The caller must discard the result: it must not install the range and must
	// not re-read the high watermark to infer success (section 6.1).
	ErrCommitUncertain = errors.New("sequence: write outcome is uncertain")
)

// LivenessRepo is the storage record of which nodes are alive.
//
// It exists so the placement authority can learn the live node set without
// asking the nodes anything: a node renews its row on every heartbeat, through
// the storage it is already using for slot ownership, and the control plane
// reads that same table. Neither process needs a channel to the other, which is
// what keeps a node unaware that a control plane exists.
//
// The write is deliberately the node's whole side of the contract. There is no
// TTL here, because expiry is not a property of the row: it is the reader's
// comparison against its own node_ttl, and a writer that also decided expiry
// would be a second place for that number to live.
type LivenessRepo interface {
	// RenewLiveness records that nodeID is alive, with the instant taken from the
	// storage clock.
	//
	// The clock matters for the same reason it does on slot ownership: the reader
	// compares the stored instant against a TTL, so a node clock on one side of
	// that comparison would make liveness depend on every node's time being right.
	//
	// The first report and every later one are the same statement: a node that has
	// never been seen is inserted, and one that has is updated in place. That is
	// what makes the row count the fleet size rather than the uptime of the fleet.
	//
	// instanceID is recorded because a restarted node must be distinguishable from
	// its predecessor. Reusing the node id is expected -- a rolling update brings
	// the same id back -- so the field is what lets a reader see that the process
	// behind a row changed rather than inferring it from a gap in renewals.
	RenewLiveness(ctx context.Context, nodeID, instanceID string) error
	// DropLiveness removes nodeID's row on graceful shutdown.
	//
	// It is a courtesy, not a correctness requirement: a node that crashes leaves
	// its row behind and is expired by the reader after node_ttl, which is the
	// path that has to be right anyway. Dropping the row is what keeps an ordinary
	// rolling update from making the fleet wait out a full TTL before the slots of
	// the node that just left become assignable.
	//
	// The instance id is part of the predicate so a shutdown racing a restart
	// cannot delete the row the new process just wrote: the old process only
	// removes the row if it is still the one recorded there.
	DropLiveness(ctx context.Context, nodeID, instanceID string) error
}

// Publisher materialises the directory the fleet follows.
//
// It runs on every replica, not only the elected one, but only the holder of
// the coordinator lease publishes: two writers would mint two revisions for one
// ownership view. Delivering a revision is each node's own read of the table
// this writes, so the publisher's whole job is to snapshot authority -- it
// decides no placement and needs no view of liveness.
type Publisher struct {
	cfg         ControlPlaneConfig
	quietWindow time.Duration
	instanceID  string
	placement   PublisherRepo
	coordinator CoordinatorRepo
	logger      *slog.Logger

	// passes counts completed passes, which is what tells an operator the loop
	// is alive without reading the placement tables.
	passes atomic.Int64
	// unownedSlots is the last pass's count of slots nobody holds. The alert is
	// how long it stays above zero: a slot without an owner is one nobody can
	// allocate from.
	unownedSlots atomic.Int64
	// coordinatorLost counts passes that lost the role while they were
	// publishing. It is not a failure -- the new publisher recomputes what the
	// pass abandoned -- but a control plane that is repeatedly losing the role
	// is not converging, and this is the counter that shows it.
	coordinatorLost atomic.Int64
	// revision is the newest directory revision this replica published. Only
	// the coordinator advances it, so a follower reports the revision it last
	// published and the fleet's current one is the maximum across replicas.
	revision atomic.Int64
	// lastPass is when the last pass finished, in UnixNano, and lastPassFailed
	// is whether it returned an error. They are what Readiness answers from.
	lastPass       atomic.Int64
	lastPassFailed atomic.Bool
	// viewSegments, routePayloadBytes and retentionDeleted are the
	// low-cardinality facts about the most recent successful pass. They are kept
	// here so the metrics layer reads the same values the unit tests do.
	viewSegments      atomic.Int64
	routePayloadBytes atomic.Int64
	retentionDeleted  atomic.Int64
	// observer receives the two pass facts that are only meaningful as events:
	// how long the ownership read took, and how large the published payload was.
	// It is nil unless a deployment asked for metrics.
	observer atomic.Pointer[PublisherObserver]
}

// PublisherObserver receives low-cardinality facts about a publisher pass.
//
// The callbacks run on the pass goroutine, so they must not block; the metrics
// layer records into OpenTelemetry instruments, which is a bounded operation.
type PublisherObserver struct {
	// OwnershipView reports the compact read's segment count and duration.
	OwnershipView func(segments int, duration time.Duration)
	// Published reports the encoded snapshot size and how many old revisions
	// the retention bound pruned in this publish.
	Published func(payloadBytes int, retentionDeleted int64)
}

// SetObserver installs the publisher's pass observer, replacing any previous
// one. Passing a zero observer clears it, which is what the metrics layer does
// when it is torn down.
func (p *Publisher) SetObserver(observer PublisherObserver) {
	if p == nil {
		return
	}
	if observer.OwnershipView == nil && observer.Published == nil {
		p.observer.Store(nil)
		return
	}
	p.observer.Store(&observer)
}

// publisherReadinessStallFactor is how many pass intervals may pass with no
// completed pass before a replica reports unready.
const publisherReadinessStallFactor = 3

// PublisherReadiness is the control plane's answer to a readiness probe.
type PublisherReadiness struct {
	// Ready is whether the loop completed a pass recently.
	Ready bool
	// Reason names the state the verdict came from, so a probe's operator gets
	// the state rather than a bare status code.
	Reason string
	// LastPassFailed reports whether the most recent pass returned an error. It
	// is deliberately not part of Ready: the control plane's availability never
	// decides correctness, and a pass that failed because storage is unreachable
	// is not improved by restarting the replica.
	LastPassFailed bool
}

// PublisherStats is the publisher's observable state.
type PublisherStats struct {
	// Passes counts completed passes.
	Passes int64
	// UnownedSlots is the last pass's count of slots nobody holds.
	UnownedSlots int64
	// CoordinatorLost counts passes that lost the role while publishing.
	CoordinatorLost int64
	// Revision is the newest directory revision this replica published.
	Revision int64
	// OwnershipViewSegments is the segment count of the most recent successful
	// compact ownership read.
	OwnershipViewSegments int64
	// RoutePayloadBytes is the encoded size of the most recent published (or
	// matched) snapshot.
	RoutePayloadBytes int64
	// RetentionDeleted counts route revisions pruned by the retention bound.
	RetentionDeleted int64
}

// NewPublisher constructs the publisher.
func NewPublisher(
	cfg ControlPlaneConfig,
	quietWindow time.Duration,
	instanceID string,
	placementRepo PublisherRepo,
	coordinator CoordinatorRepo,
	logger *slog.Logger,
) *Publisher {
	if logger == nil {
		logger = slog.Default()
	}
	return &Publisher{
		cfg:         cfg,
		quietWindow: quietWindow,
		instanceID:  instanceID,
		placement:   placementRepo,
		coordinator: coordinator,
		logger:      logger,
	}
}

// Run drives passes until ctx is cancelled.
func (p *Publisher) Run(ctx context.Context) {
	passTimer := time.NewTimer(p.cfg.ReconcileInterval)
	defer passTimer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-passTimer.C:
		}
		if err := p.Pass(ctx); err != nil && ctx.Err() == nil {
			p.logger.Error("placement pass failed", slog.Any("err", err))
		}
		passTimer.Reset(p.cfg.ReconcileInterval)
	}
}

// Pass runs one publish: a pass acquires the role, and the replica that holds
// it snapshots the ownership view into a new revision.
//
// The whole pass is bounded, because it publishes under the tenure it acquired
// at its start: one that overran its own timeout would still be free to publish
// while another replica had already become the coordinator. Losing the role
// inside the bound is handled by the write itself, which presents the tenure and
// is refused once it is no longer the one in force.
func (p *Publisher) Pass(ctx context.Context) (passErr error) {
	defer func() {
		p.lastPass.Store(time.Now().UnixNano())
		p.lastPassFailed.Store(passErr != nil)
	}()
	if p.cfg.PassTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, p.cfg.PassTimeout)
		defer cancel()
	}
	lease, err := p.coordinator.AcquireCoordinator(ctx, p.instanceID, p.cfg.CoordinatorLease)
	if err != nil {
		return err
	}
	if lease.Held {
		if err := p.publish(ctx, lease); err != nil {
			if !errors.Is(err, ErrCoordinatorLost) {
				return err
			}
			// Another replica took the role while this pass was publishing.
			// The pass is abandoned rather than reported as a failure: the new
			// publisher republishes the current view, so nothing the fleet
			// observes depends on the abandoned revision.
			p.coordinatorLost.Add(1)
		}
	}
	p.passes.Add(1)
	return nil
}

// publish snapshots the authority and records the revision it produced.
func (p *Publisher) publish(ctx context.Context, lease CoordinatorLease) error {
	viewStarted := time.Now()
	segments, err := p.placement.OwnershipSegments(ctx, p.quietWindow)
	if err != nil {
		return err
	}
	viewDuration := time.Since(viewStarted)
	// A short or overlapping read is refused rather than published: a snapshot
	// that misses slots would tell the fleet those slots are unowned, and every
	// reader rejects it anyway, so writing it would only turn a clear failure
	// into an unreadable directory.
	if err := ValidateOwnershipSegments(segments); err != nil {
		return err
	}
	result, err := p.placement.MaterialiseRoute(
		ctx,
		segments,
		p.cfg.LayoutVersion,
		p.cfg.RouteRetention,
		lease,
	)
	if err != nil {
		return err
	}
	p.unownedSlots.Store(countUnowned(segments))
	p.revision.Store(result.Revision)
	p.viewSegments.Store(int64(len(segments)))
	p.routePayloadBytes.Store(int64(result.PayloadBytes))
	p.retentionDeleted.Add(result.RetentionDeleted)
	if observer := p.observer.Load(); observer != nil {
		if observer.OwnershipView != nil {
			observer.OwnershipView(len(segments), viewDuration)
		}
		if observer.Published != nil {
			observer.Published(result.PayloadBytes, result.RetentionDeleted)
		}
	}
	return nil
}

// Readiness reports whether this replica's loop is still turning.
//
// What it measures is pass activity, not convergence: a replica whose passes
// are failing is still a working process that will converge again once storage
// answers, so it reports ready with LastPassFailed set.
func (p *Publisher) Readiness() PublisherReadiness {
	last := p.lastPass.Load()
	if last == 0 {
		return PublisherReadiness{Reason: "initializing"}
	}
	if window := publisherReadinessStallFactor * p.cfg.ReconcileInterval; window > 0 &&
		time.Since(time.Unix(0, last)) > window {
		return PublisherReadiness{Reason: "stalled"}
	}
	return PublisherReadiness{
		Ready:          true,
		Reason:         "serving",
		LastPassFailed: p.lastPassFailed.Load(),
	}
}

// Stats returns the publisher's counters.
func (p *Publisher) Stats() PublisherStats {
	return PublisherStats{
		Passes:                p.passes.Load(),
		UnownedSlots:          p.unownedSlots.Load(),
		CoordinatorLost:       p.coordinatorLost.Load(),
		Revision:              p.revision.Load(),
		OwnershipViewSegments: p.viewSegments.Load(),
		RoutePayloadBytes:     p.routePayloadBytes.Load(),
		RetentionDeleted:      p.retentionDeleted.Load(),
	}
}

func countUnowned(segments []OwnershipSegment) int64 {
	unowned := int64(0)
	for _, segment := range segments {
		if segment.State != SlotOwned {
			unowned += int64(segment.Slots())
		}
	}
	return unowned
}
