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
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSplitOwnerDividesTheSpaceAndGivesTheRemainderToTheLowestIDs(t *testing.T) {
	// The remainder goes to the lowest node ids, so the split is a pure function
	// of the sorted node set. Three nodes cannot divide 16384 evenly, and the
	// difference must be visible here rather than in a fleet's slot counts.
	nodes := []string{"node-a", "node-b", "node-c"}
	counts := map[string]uint32{}
	ownerOf := map[uint32]string{}
	for slot := uint32(0); slot < SlotCount; slot++ {
		owner := SplitOwner(slot, nodes)
		counts[owner]++
		ownerOf[slot] = owner
		// The split must be contiguous: a slot's owner changes only at a
		// boundary, which is what lets one assignment be read as a table.
		if slot > 0 {
			previous := ownerOf[slot-1]
			require.LessOrEqual(t, indexOf(nodes, previous), indexOf(nodes, owner),
				"slot %d moved backwards", slot)
		}
	}
	require.Equal(t, uint32(SlotCount), counts["node-a"]+counts["node-b"]+counts["node-c"])
	// 16384 divides into three with a remainder of one, and the remainder goes to
	// the lowest node id, so only node-a carries the extra slot.
	assert.Equal(t, uint32(SlotCount)/3+1, counts["node-a"])
	assert.Equal(t, uint32(SlotCount)/3, counts["node-b"])
	assert.Equal(t, uint32(SlotCount)/3, counts["node-c"])
	// With no live node there is nowhere to put a slot, and saying so is what
	// keeps an empty fleet from assigning slots to the empty string.
	assert.Empty(t, SplitOwner(0, nil))
}

func indexOf(values []string, want string) int {
	for index, value := range values {
		if value == want {
			return index
		}
	}
	return -1
}

func active(id, instance string) NodeInfo {
	return NodeInfo{ID: id, InstanceID: instance, State: NodeActive}
}

// TestPlanTargetsNeverMovesAHealthySlot is decision D18 in the planner: a node
// joining the fleet takes only slots nobody owns. Every serving slot stays where
// it is, because moving one costs a takeover and a takeover is the expensive,
// risky operation the whole protocol is built around.
func TestPlanTargetsNeverMovesAHealthySlot(t *testing.T) {
	view := []Ownership{
		{
			SlotID: 0, State: SlotOwned, OwnerNodeID: "node-a",
			OwnerInstanceID: "instance-a", Epoch: 1,
		},
		{
			SlotID: 1, State: SlotOwned, OwnerNodeID: "node-b",
			OwnerInstanceID: "instance-b", Epoch: 1,
		},
		{SlotID: 2, State: SlotUnowned},
	}
	live := []NodeInfo{
		active("node-a", "instance-a"),
		active("node-b", "instance-b"),
		active("node-c", "instance-c"),
	}
	targets := PlanTargets(view, live, time.Second)

	assert.Equal(t, "node-a", targets[0].TargetNodeID)
	assert.Equal(t, "node-b", targets[1].TargetNodeID)
	// Slot 2 is unowned, so it is assigned by the split, which with three nodes
	// puts the first third with node-a.
	assert.Equal(t, "node-a", targets[2].TargetNodeID)
}

// TestPlanTargetsReassignsASlotWhoseOwnerIsGone is the failover path. A lapsed
// lease does not rewrite the ownership row: only an explicit release does, and a
// crashed instance cannot issue one. So leaving such a slot without a target
// strands it forever, and no other node can take it over.
func TestPlanTargetsReassignsASlotWhoseOwnerIsGone(t *testing.T) {
	view := []Ownership{
		{
			SlotID: 0, State: SlotOwned, OwnerNodeID: "node-gone",
			OwnerInstanceID: "instance-gone", Epoch: 4,
		},
	}
	targets := PlanTargets(view, []NodeInfo{
		active("node-a", "instance-a"),
		active("node-b", "instance-b"),
	}, time.Second)

	require.Len(t, targets, 1)
	assert.NotEmpty(t, targets[0].TargetNodeID,
		"a slot whose owner vanished must be assignable again")
	assert.NotEqual(t, "node-gone", targets[0].TargetNodeID)
}

// TestPlanTargetsKeepsASlotOnItsRestartedNodeID is the rolling-restart rule:
// the node id names a position in the fleet, so a new process under the same id
// reclaims the position's slots instead of having them scattered across the
// fleet. The claim it makes still waits out the quiet window, so the old
// process's lease is covered.
func TestPlanTargetsKeepsASlotOnItsRestartedNodeID(t *testing.T) {
	view := []Ownership{{
		SlotID: 0, State: SlotOwned, OwnerNodeID: "node-a",
		OwnerInstanceID: "instance-old", Epoch: 7, GrantedAgo: time.Hour,
		GrantAgeKnown: true,
	}}
	targets := PlanTargets(view, []NodeInfo{
		active("node-a", "instance-new"),
		active("node-b", "instance-b"),
	}, time.Second)

	require.Len(t, targets, 1)
	assert.Equal(t, "node-a", targets[0].TargetNodeID,
		"a restart reclaims its node id's slots; the claim covers the window")
}

// TestPlanTargetsMovesASlotWhoseGrantLapsed covers an owner that is still the
// same live process but is no longer serving: its grant has passed the quiet
// window, and its own read stops strictly inside that window, so the slot is
// already unavailable from it. A fresh grant, or one whose age is unknown, keeps
// it -- moving a serving slot on a guess is what D18 forbids.
func TestPlanTargetsMovesASlotWhoseGrantLapsed(t *testing.T) {
	const owner = "node-a"
	const other = "node-b"
	lapsed := func(ago time.Duration, known bool) []Ownership {
		return []Ownership{{
			SlotID: 0, State: SlotOwned, OwnerNodeID: owner,
			OwnerInstanceID: "instance-a", Epoch: 3,
			GrantedAgo: ago, GrantAgeKnown: known,
		}}
	}
	live := []NodeInfo{active(owner, "instance-a"), active(other, "instance-b")}
	window := time.Second

	assert.Equal(
		t,
		other,
		PlanTargets(lapsed(2*time.Second, true), live, window)[0].TargetNodeID,
		"a grant past the window cannot be served by its owner",
	)
	assert.Equal(
		t,
		owner,
		PlanTargets(lapsed(window-time.Millisecond, true), live, window)[0].TargetNodeID,
		"a grant inside the window keeps its owner",
	)
	assert.Equal(
		t,
		owner,
		PlanTargets(lapsed(time.Hour, false), live, window)[0].TargetNodeID,
		"an unknown age is treated as fresh",
	)
}

// TestPlanTargetsExcludesLeavingNodes covers the rollout instruction: a node
// marked LEAVING is not in the live set, so its slots are assigned away and it is
// never chosen as the destination of anybody else's. Its own pass is what
// releases the authority; the plan never preempts it.
func TestPlanTargetsExcludesLeavingNodes(t *testing.T) {
	view := []Ownership{
		{
			SlotID: 0, State: SlotOwned, OwnerNodeID: "node-a",
			OwnerInstanceID: "instance-a", Epoch: 2,
		},
		{SlotID: 1, State: SlotUnowned},
	}
	live := []NodeInfo{
		{ID: "node-a", InstanceID: "instance-a", State: NodeLeaving},
		active("node-b", "instance-b"),
	}
	targets := PlanTargets(view, live, time.Second)

	require.Len(t, targets, 2)
	assert.Equal(t, "node-b", targets[0].TargetNodeID,
		"a leaving owner's slots have to move")
	assert.Equal(t, "node-b", targets[1].TargetNodeID,
		"the leaving node must not receive new slots")
}

// TestPlanTargetsAssignsNothingWithoutAUsableNode keeps the empty answers
// explicit: with no live node, or with only the node that cannot be its own
// reassignment target, the answer is "nobody claims this" rather than handing
// slots back to a node that cannot serve them.
func TestPlanTargetsAssignsNothingWithoutAUsableNode(t *testing.T) {
	targets := PlanTargets([]Ownership{{SlotID: 0, State: SlotUnowned}}, nil, time.Second)
	require.Len(t, targets, 1)
	assert.Empty(t, targets[0].TargetNodeID)

	only := PlanTargets([]Ownership{{
		SlotID: 0, State: SlotOwned, OwnerNodeID: "node-a",
		OwnerInstanceID: "instance-a", Epoch: 1,
	}}, nil, time.Second)
	require.Len(t, only, 1)
	assert.Empty(t, only[0].TargetNodeID)

	// The owner is marked LEAVING, so it is not live and its own node is not a
	// destination; with nobody else in the fleet the slot is held back.
	leaving := PlanTargets([]Ownership{{
		SlotID: 0, State: SlotOwned, OwnerNodeID: "node-a",
		OwnerInstanceID: "instance-a", Epoch: 1,
	}}, []NodeInfo{{
		ID: "node-a", InstanceID: "instance-a", State: NodeLeaving,
	}}, time.Second)
	require.Len(t, leaving, 1)
	assert.Empty(t, leaving[0].TargetNodeID)
}

func TestActiveNodeIDsSkipsLeavingNodesAndOrdersTheRest(t *testing.T) {
	nodes := []NodeInfo{
		{ID: "node-b", State: NodeActive},
		{ID: "node-a", State: NodeActive},
		{ID: "node-c", State: NodeLeaving},
	}
	assert.Equal(t, []string{"node-a", "node-b"}, ActiveNodeIDs(nodes))
}

// It has no method that could write placement at all. That is deliberate: the
// publisher must materialise authority and nothing else, and a test double that
// could accept intent would let a regression back in without failing here.
type fakePublisherRepo struct {
	view       []Ownership
	viewErr    error
	publishErr error
	// published records the view handed to the last write, so a test can pin
	// that it is exactly the authority that was read.
	published []Ownership
	// leases records, in order, the tenure every write was made under.
	leases   []CoordinatorLease
	revision int64
	writes   int
}

func (f *fakePublisherRepo) OwnershipView(context.Context) ([]Ownership, error) {
	if f.viewErr != nil {
		return nil, f.viewErr
	}
	return f.view, nil
}

func (f *fakePublisherRepo) MaterialiseRoute(
	_ context.Context,
	view []Ownership,
	layoutVersion int64,
	lease CoordinatorLease,
) (int64, error) {
	if f.publishErr != nil {
		return 0, f.publishErr
	}
	f.leases = append(f.leases, lease)
	if f.writes > 0 && reflect.DeepEqual(f.published, view) {
		return f.revision, nil
	}
	f.writes++
	f.published = view
	f.revision++
	return f.revision, nil
}

// blockingPublisherRepo holds a pass inside its first read until the pass's own
// context ends, which is how a test observes the bound the pass runs under.
type blockingPublisherRepo struct {
	*fakePublisherRepo
}

func (b *blockingPublisherRepo) OwnershipView(ctx context.Context) ([]Ownership, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

type fakeCoordinator struct {
	held bool
	// epoch is the tenure this replica is reported to hold. It is what the
	// publish is expected to present.
	epoch uint64
}

func (f *fakeCoordinator) AcquireCoordinator(
	_ context.Context,
	instanceID string,
	_ time.Duration,
) (CoordinatorLease, error) {
	if !f.held {
		return CoordinatorLease{}, nil
	}
	return CoordinatorLease{Held: true, InstanceID: instanceID, Epoch: f.epoch}, nil
}

// testPublisherInstanceID is the identity every publisher under test is fenced
// with. The coordinator records it, so a test can pin which replica wrote.
const testPublisherInstanceID = "instance-1"

func testPublisherConfig() ControlPlaneConfig {
	return ControlPlaneConfig{
		LayoutVersion:     1,
		CoordinatorLease:  10 * time.Second,
		ReconcileInterval: time.Second,
		// Bounded well inside the lease, as production validates it, so a test
		// that hangs fails as a timeout rather than as a stuck suite.
		PassTimeout: time.Second,
	}
}

func oneSlotView() []Ownership {
	return []Ownership{
		{SlotID: 0, State: SlotOwned, OwnerNodeID: "node-a", Epoch: 1},
	}
}

// TestPassPublishesOnlyWhileHoldingTheLease pins the single-writer rule: a
// replica without the role reads nothing and writes nothing.
func TestPassPublishesOnlyWhileHoldingTheLease(t *testing.T) {
	repo := &fakePublisherRepo{view: oneSlotView()}

	follower := NewPublisher(
		testPublisherConfig(),
		testPublisherInstanceID,
		repo,
		&fakeCoordinator{held: false},
		nil,
	)
	require.NoError(t, follower.Pass(context.Background()))
	assert.Zero(t, repo.writes, "a replica without the lease must not publish")

	leader := NewPublisher(
		testPublisherConfig(),
		testPublisherInstanceID,
		repo,
		&fakeCoordinator{held: true},
		nil,
	)
	require.NoError(t, leader.Pass(context.Background()))
	assert.Equal(t, 1, repo.writes)
}

// TestPassMaterialisesTheAuthorityViewItRead is the whole of the publisher's
// job. It decides no placement: the directory it writes is the ownership view,
// slot for slot, and the epoch of each slot travels with it.
func TestPassMaterialisesTheAuthorityViewItRead(t *testing.T) {
	view := []Ownership{
		{SlotID: 0, State: SlotOwned, OwnerNodeID: "node-a", Epoch: 4},
		{SlotID: 1, State: SlotUnowned},
	}
	repo := &fakePublisherRepo{view: view}
	publisher := NewPublisher(
		testPublisherConfig(), testPublisherInstanceID, repo, &fakeCoordinator{held: true}, nil,
	)

	require.NoError(t, publisher.Pass(context.Background()))

	assert.Equal(t, view, repo.published, "the directory is the authority view")
	assert.Equal(t, int64(1), publisher.Stats().Revision)
	assert.Equal(t, int64(1), publisher.Stats().UnownedSlots)
	assert.Equal(t, int64(1), publisher.Stats().Passes)
}

// TestPassPublishesUnderTheTenureItAcquired pins the credential the write
// carries. A pass that wrote with an assumed epoch, or with none, would be
// asking storage to take its word for holding the role -- which is exactly what
// the epoch replaces with a comparison, and the comparison is what a takeover
// turns false.
func TestPassPublishesUnderTheTenureItAcquired(t *testing.T) {
	repo := &fakePublisherRepo{view: oneSlotView()}
	publisher := NewPublisher(
		testPublisherConfig(), testPublisherInstanceID,
		repo,
		&fakeCoordinator{held: true, epoch: 7},
		nil,
	)

	require.NoError(t, publisher.Pass(context.Background()))

	require.Len(t, repo.leases, 1)
	assert.True(t, repo.leases[0].Held)
	assert.Equal(t, "instance-1", repo.leases[0].InstanceID)
	assert.Equal(t, uint64(7), repo.leases[0].Epoch)
}

// TestPassAbandonsAPublishItLostTheRoleFor covers the takeover that lands before
// the write: the pass acquired the role, another replica took it, and the write
// is refused rather than made on behalf of a role that is no longer this
// replica's. The pass is not a failure -- the new publisher republishes the
// current view -- but the refusal has to be visible.
func TestPassAbandonsAPublishItLostTheRoleFor(t *testing.T) {
	repo := &fakePublisherRepo{view: oneSlotView(), publishErr: ErrCoordinatorLost}
	publisher := NewPublisher(
		testPublisherConfig(), testPublisherInstanceID,
		repo,
		&fakeCoordinator{held: true, epoch: 7},
		nil,
	)

	require.NoError(t, publisher.Pass(context.Background()))

	assert.Nil(t, repo.published, "a lost tenure must not publish a directory")
	assert.Equal(t, int64(1), publisher.Stats().CoordinatorLost)
	assert.Equal(t, int64(1), publisher.Stats().Passes)
}

// TestPassIsBoundedSoItCannotOutliveItsLease pins the bound that keeps a stalled
// pass from writing under a tenure it has already spent.
func TestPassIsBoundedSoItCannotOutliveItsLease(t *testing.T) {
	cfg := testPublisherConfig()
	cfg.PassTimeout = 10 * time.Millisecond
	publisher := NewPublisher(
		cfg, testPublisherInstanceID,
		&blockingPublisherRepo{&fakePublisherRepo{}},
		&fakeCoordinator{held: true, epoch: 7},
		nil,
	)

	err := publisher.Pass(context.Background())
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

// TestPassRepublishesNothingWhileTheAuthorityIsUnchanged is what keeps a stable
// fleet from growing the route table: an identical view returns the revision
// already published rather than minting a new one.
func TestPassRepublishesNothingWhileTheAuthorityIsUnchanged(t *testing.T) {
	repo := &fakePublisherRepo{view: oneSlotView()}
	publisher := NewPublisher(
		testPublisherConfig(), testPublisherInstanceID, repo, &fakeCoordinator{held: true}, nil,
	)

	require.NoError(t, publisher.Pass(context.Background()))
	require.NoError(t, publisher.Pass(context.Background()))

	assert.Equal(t, 1, repo.writes)
	assert.Equal(t, int64(1), publisher.Stats().Revision)
	assert.Equal(t, int64(2), publisher.Stats().Passes)
}

// TestReadinessReportsTheLoopsState covers the answers a probe can get from a
// replica: nothing to report on yet, a loop that is turning, a loop whose passes
// are failing, and a loop that has stopped passing at all.
//
// Failing passes are reported rather than made unready: a restart does not fix
// unreachable storage, and a probe that failed on it would pull every replica out
// at once.
func TestReadinessReportsTheLoopsState(t *testing.T) {
	// Nothing has run yet, so there is no verdict to give and a probe must not be
	// told the replica is serving.
	fresh := NewPublisher(
		testPublisherConfig(),
		testPublisherInstanceID,
		&fakePublisherRepo{},
		&fakeCoordinator{held: true},
		nil,
	)
	assert.Equal(t, PublisherReadiness{Reason: "initializing"}, fresh.Readiness())

	require.NoError(t, fresh.Pass(context.Background()))
	assert.Equal(t, PublisherReadiness{Ready: true, Reason: "serving"}, fresh.Readiness())

	// A pass that cannot read the authority fails. The replica keeps reporting
	// ready -- the loop is still turning -- and says so in LastPassFailed, which
	// is what keeps a storage blip from reading as a dead process.
	failing := testPublisherConfig()
	failing.PassTimeout = 10 * time.Millisecond
	failed := NewPublisher(
		failing, testPublisherInstanceID,
		&blockingPublisherRepo{&fakePublisherRepo{}},
		&fakeCoordinator{held: true},
		nil,
	)
	require.Error(t, failed.Pass(context.Background()))
	assert.Equal(t, PublisherReadiness{
		Ready:          true,
		Reason:         "serving",
		LastPassFailed: true,
	}, failed.Readiness())

	// The stall window is a multiple of the pass interval, so a replica whose
	// loop has stopped calling Pass is reported stalled rather than serving on
	// the strength of its last successful pass.
	stalling := testPublisherConfig()
	stalling.ReconcileInterval = time.Millisecond
	stalled := NewPublisher(
		stalling, testPublisherInstanceID, &fakePublisherRepo{}, &fakeCoordinator{held: true}, nil,
	)
	require.NoError(t, stalled.Pass(context.Background()))
	time.Sleep(5 * time.Millisecond)
	assert.Equal(t, PublisherReadiness{Reason: "stalled"}, stalled.Readiness())
}

// TestReadinessDoesNotMakeAFailedPassUnready is the section D.17 rule on its own:
// the verdict comes from loop activity, and a failing pass is reported next to
// it rather than folded into it.
func TestReadinessDoesNotMakeAFailedPassUnready(t *testing.T) {
	repo := &fakePublisherRepo{viewErr: errUnreachable}
	publisher := NewPublisher(
		testPublisherConfig(), testPublisherInstanceID, repo, &fakeCoordinator{held: true}, nil,
	)

	require.Error(t, publisher.Pass(context.Background()))

	readiness := publisher.Readiness()
	assert.True(t, readiness.Ready)
	assert.True(t, readiness.LastPassFailed)
	assert.Equal(t, "serving", readiness.Reason)
}

// errUnreachable stands in for a storage failure that is not worth a restart.
var errUnreachable = errStorageUnreachable{}

type errStorageUnreachable struct{}

func (errStorageUnreachable) Error() string { return "storage is unreachable" }
