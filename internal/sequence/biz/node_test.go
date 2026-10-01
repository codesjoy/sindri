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
	"log/slog"
	"testing"
	"time"

	"github.com/codesjoy/pkg/basic/xerror"
	"github.com/codesjoy/sindri/gen/go/sequence/reason"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// livenessFake records the renewals a heartbeat makes, because the identity a
// node reports is the whole of what the placement authority learns from it.
type livenessFake struct {
	renewals []livenessRenewal
	err      error
}

type livenessRenewal struct {
	nodeID     string
	instanceID string
}

func (f *livenessFake) RenewLiveness(_ context.Context, nodeID, instanceID string) error {
	f.renewals = append(f.renewals, livenessRenewal{nodeID: nodeID, instanceID: instanceID})
	return f.err
}

func (f *livenessFake) DropLiveness(context.Context, string, string) error { return nil }

// routeRepoFake answers every heartbeat with the same directory and counts the
// reads, so a test can tell "the heartbeat carried on" from "it returned early".
type routeRepoFake struct {
	route *Route
	calls int
	err   error
}

func (f *routeRepoFake) GetNewerRoute(context.Context, int64) (*Route, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	return f.route, nil
}

// placementFake is the shared authority view a test drives directly. The view
// and the live set are values rather than calls because a test changes what the
// fleet looks like between heartbeats and watches the node react to it.
type placementFake struct {
	view     []Ownership
	nodes    []NodeInfo
	viewErr  error
	nodesErr error
	reads    int
}

func (f *placementFake) OwnershipSegments(
	_ context.Context,
	quietWindow time.Duration,
) ([]OwnershipSegment, error) {
	f.reads++
	if f.viewErr != nil {
		return nil, f.viewErr
	}
	return OwnershipSegmentsFromView(f.view, quietWindow)
}

func (f *placementFake) LiveNodes(
	context.Context,
	time.Duration,
) ([]NodeInfo, error) {
	if f.nodesErr != nil {
		return nil, f.nodesErr
	}
	return f.nodes, nil
}

// newTestNodeManager builds a node manager over the given liveness repo and
// route repo. The allocator is a real one with a fake authority, because the
// instance id the heartbeat reports has to come from the same object that
// claims slots.
func newTestNodeManager(
	t *testing.T,
	liveness LivenessRepo,
	routes RouteRepo,
) *NodeManager {
	return newPlanningNodeManager(t, liveness, routes, nil, nil)
}

// newPlanningNodeManager builds a node manager over the supplied placement view,
// so a test can drive the plan the heartbeat computes.
func newPlanningNodeManager(
	t *testing.T,
	liveness LivenessRepo,
	routes RouteRepo,
	placementRepo PlacementRepo,
	ownership OwnershipRepo,
) *NodeManager {
	t.Helper()
	if ownership == nil {
		ownership = newLeaseOwnershipFake()
	}
	plane := testDataPlaneConfig(AllocatorConfig{
		DefaultStep:     10,
		MaxStep:         100,
		IdleTimeout:     time.Hour,
		CleanupInterval: time.Minute,
	})
	plane.Node.ID = "node-a"
	plane.HA.LeaseDuration = 10 * time.Second
	plane.HA.RenewInterval = 3 * time.Second
	plane.HA.SafetyMargin = time.Second
	allocator := NewAllocator(
		plane,
		&rangeStore{max: make(map[string]int64)},
		ownership,
		unlimitedMemorySampler,
		slog.Default(),
	)
	return NewNodeManager(
		plane,
		allocator,
		routes,
		liveness,
		placementRepo,
		NewRouteCache(),
		nil,
		slog.Default(),
	)
}

// TestHeartbeatRenewsTheNodeNewLivenessRowUnderItsProcessIdentity pins what the
// node contributes to the placement authority's view of the fleet: a renewal on
// every heartbeat, under the node id the planner knows it by and the instance id
// that distinguishes this process from its predecessor.
//
// The instance id is not decorative. A restarted node comes back under the same
// node id, so if the renewal reported only that, the authority could not tell a
// process that is still running from one that was replaced.
func TestHeartbeatRenewsTheNodeLivenessRowUnderItsProcessIdentity(t *testing.T) {
	liveness := &livenessFake{}
	manager := newTestNodeManager(t, liveness, &routeRepoFake{})

	manager.Heartbeat()

	require.Len(t, liveness.renewals, 1)
	assert.Equal(t, "node-a", liveness.renewals[0].nodeID)
	assert.Equal(t, manager.InstanceID(), liveness.renewals[0].instanceID)
}

// TestHeartbeatStillReadsTheRouteWhenTheLivenessRenewalFails pins the failure
// policy: the two writes cost different things, so a liveness failure must not
// abort the heartbeat.
//
// An unrenewed route leaves the node serving a directory it has not checked,
// while an unrenewed liveness row only makes the planner look elsewhere for the
// node's slots -- the node keeps its authority anyway, because authority comes
// from the ownership lease and not from this row.
func TestHeartbeatStillReadsTheRouteWhenTheLivenessRenewalFails(t *testing.T) {
	liveness := &livenessFake{err: errors.New("storage is unreachable")}
	routes := &routeRepoFake{route: &Route{
		Version: 1,
		Nodes:   []RouteNode{{NodeID: "node-a", Slots: []uint32{0, 1, 2}}},
	}}
	manager := newTestNodeManager(t, liveness, routes)

	manager.Heartbeat()

	require.Len(t, liveness.renewals, 1, "the attempt is made on every heartbeat")
	assert.Equal(t, 1, routes.calls, "a failed renewal must not skip the route read")
	assert.Equal(t, int64(1), manager.route.Version(), "the directory still lands")
}

// ownershipViewWithNodeA builds a complete authority view in which slot belongs
// to node-a and every other slot belongs to node-b. The whole space is required:
// a partial view is rejected rather than planned against.
func ownershipViewWithNodeA(slot uint32) []Ownership {
	view := make([]Ownership, int(SlotCount))
	for index := range view {
		view[index] = Ownership{
			SlotID: uint32(index), State: SlotOwned,
			OwnerNodeID: "node-b", OwnerInstanceID: "instance-b",
			Epoch: 3, GrantedAgo: time.Millisecond, GrantAgeKnown: true,
		}
	}
	view[slot] = Ownership{
		SlotID: slot, State: SlotOwned,
		OwnerNodeID: "node-a", OwnerInstanceID: "instance-old",
		Epoch: 3, GrantedAgo: time.Millisecond, GrantAgeKnown: true,
	}
	return view
}

func testFleet() []NodeInfo {
	return []NodeInfo{
		{ID: "node-a", InstanceID: "instance-new"},
		{ID: "node-b", InstanceID: "instance-b"},
	}
}

// TestHeartbeatPlansEveryTimeRatherThanOnlyOnANewRevision pins the change that
// makes local takeover work: a node dying, an instance being replaced, or a
// grant lapsing changes no directory revision, so a heartbeat that only replanned
// when the revision moved would never act on any of them.
func TestHeartbeatPlansEveryTimeRatherThanOnlyOnANewRevision(t *testing.T) {
	const slot = uint32(7)
	fake := &placementFake{view: ownershipViewWithNodeA(slot), nodes: testFleet()}
	manager := newPlanningNodeManager(
		t,
		&livenessFake{},
		&routeRepoFake{},
		fake,
		newLeaseOwnershipFake(),
	)

	manager.Heartbeat()
	require.NotNil(t, manager.allocator.prepareApply)
	assert.Contains(t, manager.allocator.prepareApply.Slots, slot,
		"the restarted node id reclaims the slot its predecessor held")

	manager.Heartbeat()
	assert.Equal(t, 2, fake.reads, "the plan is recomputed on every heartbeat")
}

// TestHeartbeatReleasesSlotsThePlanNoLongerWants covers the other half of local
// reconciliation: a target that moved away is dropped, drained, and its authority
// released, rather than being served until its lease runs out.
func TestHeartbeatReleasesSlotsThePlanNoLongerWants(t *testing.T) {
	const slot = uint32(7)
	ownership := newLeaseOwnershipFake()
	fake := &placementFake{view: ownershipViewWithNodeA(slot), nodes: testFleet()}
	manager := newPlanningNodeManager(
		t,
		&livenessFake{},
		&routeRepoFake{},
		fake,
		ownership,
	)

	manager.Heartbeat()
	require.NotNil(t, manager.allocator.prepareApply)
	manager.allocator.ApplyRoute(3)
	require.Contains(t, manager.allocator.slots, slot, "the claim installed the slot")

	// node-b takes the position: it is live and its grant is fresh, so the plan
	// hands the slot to it and this node has to let go.
	moved := ownershipViewWithNodeA(slot)
	moved[slot] = Ownership{
		SlotID: slot, State: SlotOwned,
		OwnerNodeID: "node-b", OwnerInstanceID: "instance-b",
		Epoch: 4, GrantedAgo: time.Millisecond, GrantAgeKnown: true,
	}
	fake.view = moved

	manager.Heartbeat()

	assert.NotContains(t, manager.allocator.slots, slot, "the slot is dropped first")
	assert.EqualValues(t, 1, ownership.released,
		"the dropped slot's authority is released after the drain")
}

// TestHeartbeatKeepsThePreviousPlanWhenTheAuthorityCannotBeRead pins the failure
// policy: a read that failed says nothing about the fleet, so the node keeps
// serving what it already planned instead of releasing every slot on a blip.
func TestHeartbeatKeepsThePreviousPlanWhenTheAuthorityCannotBeRead(t *testing.T) {
	const slot = uint32(7)
	fake := &placementFake{view: ownershipViewWithNodeA(slot), nodes: testFleet()}
	manager := newPlanningNodeManager(
		t,
		&livenessFake{},
		&routeRepoFake{},
		fake,
		newLeaseOwnershipFake(),
	)
	manager.Heartbeat()
	require.NotNil(t, manager.allocator.prepareApply)
	previous := append([]uint32(nil), manager.allocator.prepareApply.Slots...)

	fake.viewErr = errors.New("storage is unreachable")
	manager.Heartbeat()

	require.NotNil(t, manager.allocator.prepareApply)
	assert.Equal(t, previous, manager.allocator.prepareApply.Slots,
		"a failed read must leave the previous plan in force")
}

// clockProbe is a pair of clocks a test advances independently, which is the only
// way to produce the comparisons this monitor exists to make.
type clockProbe struct {
	local   time.Time
	storage time.Time
	err     error
}

func newClockProbe() *clockProbe {
	start := time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC)
	return &clockProbe{local: start, storage: start}
}

func (p *clockProbe) Now() time.Time { return p.local }

func (p *clockProbe) StorageClock(context.Context) (time.Time, error) {
	if p.err != nil {
		return time.Time{}, p.err
	}
	return p.storage, nil
}

// advance moves the local and storage clocks by different amounts, which is
// exactly what a drift or a jump looks like from inside the process.
func (p *clockProbe) advance(local, storage time.Duration) {
	p.local = p.local.Add(local)
	p.storage = p.storage.Add(storage)
}

func TestStorageClockMonitorArmsOnTheFirstReading(t *testing.T) {
	probe := newClockProbe()
	monitor := NewStorageClockMonitor(time.Second, time.Second, probe.Now, probe.StorageClock)

	// Two clocks that disagree about the absolute time say nothing about their
	// rates, so the first reading only establishes a baseline.
	probe.storage = probe.storage.Add(time.Hour)
	require.NoError(t, monitor.Observe(context.Background()))
	assert.False(t, monitor.Violated())
	assert.Zero(t, monitor.Stats().Drift)
}

func TestStorageClockMonitorAcceptsDriftWithinTheBound(t *testing.T) {
	probe := newClockProbe()
	monitor := NewStorageClockMonitor(time.Second, time.Second, probe.Now, probe.StorageClock)
	require.NoError(t, monitor.Observe(context.Background()))

	probe.advance(time.Second, time.Second+500*time.Millisecond)
	require.NoError(t, monitor.Observe(context.Background()))
	assert.False(t, monitor.Violated())
	assert.Equal(t, 500*time.Millisecond, monitor.Stats().Drift)
}

// TestStorageClockMonitorRejectsBothDirectionsOfDrift pins why the bound is on the
// magnitude. A lagging storage clock looks harmless from the takeover side, but
// it is not: the local deadline is measured locally and has to sit inside a lease
// the authority measures, so a lag beyond delta means the local deadline can
// outlive the lease it is supposed to be inside.
func TestStorageClockMonitorRejectsBothDirectionsOfDrift(t *testing.T) {
	for name, storageDelta := range map[string]time.Duration{
		"storage ahead":  10 * time.Second,
		"storage behind": -10 * time.Second,
	} {
		t.Run(name, func(t *testing.T) {
			probe := newClockProbe()
			monitor := NewStorageClockMonitor(
				time.Second, time.Second, probe.Now, probe.StorageClock,
			)
			require.NoError(t, monitor.Observe(context.Background()))

			probe.advance(time.Second, time.Second+storageDelta)
			err := monitor.Observe(context.Background())
			assert.ErrorIs(t, err, ErrStorageClockViolation)
			assert.True(t, monitor.Violated())
		})
	}
}

// TestStorageClockMonitorRejectsAForwardJump pins the term that neither database
// can bound: a single step of the storage clock forward by more than J_max.
func TestStorageClockMonitorRejectsAForwardJump(t *testing.T) {
	probe := newClockProbe()
	monitor := NewStorageClockMonitor(
		time.Hour, // a wide drift bound, so only the jump can trip it
		100*time.Millisecond,
		probe.Now,
		probe.StorageClock,
	)
	require.NoError(t, monitor.Observe(context.Background()))

	probe.advance(time.Second, time.Second+5*time.Second)
	err := monitor.Observe(context.Background())
	assert.ErrorIs(t, err, ErrStorageClockViolation)
	assert.Equal(t, 5*time.Second, monitor.Stats().ForwardJump)
}

// TestStorageClockMonitorStaysViolated pins that the state is sticky: the process
// cannot reconstruct which of its earlier decisions were made while the bound
// held, so a later healthy reading must not clear it on its own.
func TestStorageClockMonitorStaysViolated(t *testing.T) {
	probe := newClockProbe()
	monitor := NewStorageClockMonitor(time.Second, time.Second, probe.Now, probe.StorageClock)
	require.NoError(t, monitor.Observe(context.Background()))

	probe.advance(time.Second, time.Minute)
	require.ErrorIs(t, monitor.Observe(context.Background()), ErrStorageClockViolation)

	probe.advance(time.Second, time.Second)
	assert.ErrorIs(t, monitor.Observe(context.Background()), ErrStorageClockViolation)
	assert.True(t, monitor.Violated())
}

// TestStorageClockMonitorSeparatesAnUnreachableStore pins that a storage failure
// is reported as itself. Turning it into a clock violation would fence a healthy
// instance on a blip that the allocation path already handles.
func TestStorageClockMonitorSeparatesAnUnreachableStore(t *testing.T) {
	probe := newClockProbe()
	monitor := NewStorageClockMonitor(time.Second, time.Second, probe.Now, probe.StorageClock)
	probe.err = errors.New("connection refused")

	err := monitor.Observe(context.Background())
	require.Error(t, err)
	assert.NotErrorIs(t, err, ErrStorageClockViolation)
	assert.False(t, monitor.Violated())
}

// TestFencedInstanceLeavesTheService pins what a violation does to the instance:
// it stops allocating and it stops reporting ready, because the bounds its
// earlier decisions rested on are no longer known to have held.
func TestFencedInstanceLeavesTheService(t *testing.T) {
	ownership := newLeaseOwnershipFake()
	allocator, _ := leaseAllocatorWith(t, "fenced", ownership, nil)
	allocator.Open(1, 0, nil)
	require.True(t, allocator.Readiness().Ready)

	allocator.Fence("storage clock exceeded its asserted drift or jump bound")

	reason, fenced := allocator.Fenced()
	require.True(t, fenced)
	assert.Contains(t, reason, "storage clock")
	assert.True(t, allocator.Paused(), "a fenced instance stops allocating")
	assert.Equal(t, "fenced", allocator.Readiness().Reason)
	assert.False(t, allocator.Readiness().Ready)

	// A fence is not a shutdown: the process is still here, it is just not
	// allowed to answer from bounds it can no longer justify.
	assert.NotEqual(t, "stopping", allocator.Readiness().Reason)
}

// TestFenceOutlivesTheRouteThatReopensAllocation pins the durability of the
// fence against the path that would otherwise undo it.
//
// Entering the fence also pauses the allocator, but the pause is deliberately not
// what carries the fence: applying a route reopens allocation by storing the ready
// state, and the heartbeat re-applies the cached route on every pass in which it
// finds the allocator paused -- which a fenced instance always does. If the fence
// lived only in the pause, the instance would be serving again one heartbeat after
// the violation, and the "permanently isolated until a human looks" contract would
// last about a second.
func TestFenceOutlivesTheRouteThatReopensAllocation(t *testing.T) {
	ownership := newLeaseOwnershipFake()
	allocator, _ := leaseAllocatorWith(t, "fenced", ownership, nil)
	allocator.Open(1, 0, nil)
	require.True(t, allocator.Readiness().Ready)

	allocator.Fence("storage clock exceeded its asserted drift or jump bound")

	// This is what the heartbeat does when it finds a paused allocator with a
	// route already loaded: it re-applies the cached route, which opens.
	allocator.Open(1, 0, nil)
	assert.True(t, allocator.Paused(), "a route must not reopen a fenced instance")
	assert.Equal(t, "fenced", allocator.Readiness().Reason)

	_, err := allocator.FetchNext(context.Background(), "any-key")
	require.Error(t, err)
	assert.True(
		t,
		xerror.IsReason(err, reason.Reason_SEQUENCE_ALLOCATOR_PAUSED),
		"a fenced instance must still refuse to allocate, got %v",
		err,
	)

	// Committing is the other half of applying a route, and it must not clear a
	// fence either.
	allocator.CommitRoute(2, 0, nil)
	assert.True(t, allocator.Paused(), "committing a route must not clear a fence")
	assert.Equal(t, "fenced", allocator.Readiness().Reason)
}
