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

	testkit "github.com/codesjoy/sindri/internal/pkg/tests"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// recordingHandoffRepo captures the command batches the allocator issues while
// delegating the durable state machine to the shared ownership fake.
type recordingHandoffRepo struct {
	inner      HandoffRepo
	listErr    error
	applyErr   error
	listCalls  int
	applyCalls int
	applied    []HandoffRequest
}

func (r *recordingHandoffRepo) Handoffs(
	ctx context.Context,
	instance string,
	limit int,
) ([]Handoff, error) {
	r.listCalls++
	if r.listErr != nil {
		return nil, r.listErr
	}
	return r.inner.Handoffs(ctx, instance, limit)
}

func (r *recordingHandoffRepo) PlanHandoffs(
	ctx context.Context,
	handoffs []Handoff,
	lease CoordinatorLease,
	limits MigrationConfig,
	leaseDuration time.Duration,
) error {
	return r.inner.PlanHandoffs(ctx, handoffs, lease, limits, leaseDuration)
}

func (r *recordingHandoffRepo) RecoverHandoffs(
	ctx context.Context,
	lease CoordinatorLease,
	nodes []NodeInfo,
	ha HAConfig,
	limit int,
) error {
	return r.inner.RecoverHandoffs(ctx, lease, nodes, ha, limit)
}

func (r *recordingHandoffRepo) CollectInstances(
	ctx context.Context,
	quiet time.Duration,
	limit int,
) error {
	return r.inner.CollectInstances(ctx, quiet, limit)
}

func (r *recordingHandoffRepo) ApplyHandoffs(ctx context.Context, req HandoffRequest) error {
	r.applyCalls++
	r.applied = append(r.applied, req)
	if r.applyErr != nil {
		return r.applyErr
	}
	return r.inner.ApplyHandoffs(ctx, req)
}

func (r *recordingHandoffRepo) lastActions() []string {
	if len(r.applied) == 0 {
		return nil
	}
	batch := r.applied[len(r.applied)-1]
	actions := make([]string, len(batch.Commands))
	for index, command := range batch.Commands {
		actions[index] = command.Action
	}
	return actions
}

func seedHandoffIntent(f *leaseOwnershipFake, handoff Handoff) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.intents[handoff.SlotID] = handoff
}

func handoffPassAllocator(
	t *testing.T,
	key string,
) (*Allocator, *leaseOwnershipFake, *recordingHandoffRepo) {
	t.Helper()
	a, f := leaseAllocator(t, key, func() int64 { return 0 })
	recorder := &recordingHandoffRepo{inner: f}
	a.SetHandoffs(recorder, defaultMigrationConfig())
	return a, f, recorder
}

func TestHandoffPassSelectsTargetActions(t *testing.T) {
	key := "handoff-target"
	slot := SlotForKey(key)
	cases := []struct {
		name        string
		phase       string
		targetReady bool
		readySlot   bool
		want        string
	}{
		{name: "planned prepares", phase: "PLANNED", want: "PREPARE"},
		{name: "ready begins", phase: "READY", want: "BEGIN"},
		{
			name:        "draining with ready target transfers",
			phase:       "DRAINING",
			targetReady: true,
			want:        "TRANSFER",
		},
		{name: "draining without ready target prepares", phase: "DRAINING", want: "PREPARE"},
		{
			name:      "transferred ready slot acknowledges",
			phase:     "TRANSFERRED",
			readySlot: true,
			want:      "ACK_ACTIVE",
		},
		{name: "transferred unready slot stays silent", phase: "TRANSFERRED", want: ""},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			a, f, recorder := handoffPassAllocator(t, key)
			var sourceEpoch uint64 = 1
			if test.readySlot {
				// The local gate is ready when it serves the successor epoch.
				sourceEpoch = a.slots[slot].epoch.Load() - 1
			}
			seedHandoffIntent(f, Handoff{
				SlotID:           slot,
				ID:               "intent-" + test.phase,
				Kind:             "TRANSFER",
				SourceInstanceID: "other-instance",
				SourceEpoch:      sourceEpoch,
				TargetInstanceID: a.instanceID,
				Phase:            test.phase,
				TargetReady:      test.targetReady,
			})
			require.NoError(t, a.HandoffPass(context.Background()))
			actions := recorder.lastActions()
			if test.want == "" {
				assert.NotContains(t, actions, "ACK_ACTIVE")
				return
			}
			require.Len(t, actions, 1)
			assert.Equal(t, test.want, actions[0])
		})
	}
}

func TestHandoffPassSelectsSourceActionsAndHoldsTheDrainToken(t *testing.T) {
	key := "handoff-source"
	slot := SlotForKey(key)
	a, f, recorder := handoffPassAllocator(t, key)
	gate := a.slots[slot]
	require.NotNil(t, gate)
	gate.draining.Store(true)
	a.slotsMu.Lock()
	a.drainingSlots[slot] = gate
	a.slotsMu.Unlock()
	epoch := gate.epoch.Load()
	seedHandoffIntent(f, Handoff{
		SlotID:           slot,
		ID:               "source-transfer",
		Kind:             "TRANSFER",
		SourceInstanceID: a.instanceID,
		SourceEpoch:      epoch,
		TargetInstanceID: "target-instance",
		Phase:            "DRAINING",
	})

	gate.inflight.Store(1)
	require.NoError(t, a.HandoffPass(context.Background()))
	assert.NotContains(t, recorder.lastActions(), "ACK_DRAIN",
		"an in-flight allocation must block the drain acknowledgement")
	a.slotsMu.RLock()
	retained := a.drainingSlots[slot]
	a.slotsMu.RUnlock()
	assert.Same(t, gate, retained, "the withdrawn gate must be retained until the ack is durable")

	gate.inflight.Store(0)
	require.NoError(t, a.HandoffPass(context.Background()))
	assert.Contains(t, recorder.lastActions(), "ACK_DRAIN")

	// The ack is durable now, so the next pass may drop the withdrawn gate.
	require.NoError(t, a.HandoffPass(context.Background()))
	a.slotsMu.RLock()
	_, stillRetained := a.drainingSlots[slot]
	a.slotsMu.RUnlock()
	assert.False(t, stillRetained, "a durable drain ack releases the withdrawn gate")
}

func TestHandoffPassReleaseEmitsTransferWithoutDrainAck(t *testing.T) {
	key := "handoff-release"
	slot := SlotForKey(key)
	a, f, recorder := handoffPassAllocator(t, key)
	gate := a.slots[slot]
	require.NotNil(t, gate)
	gate.draining.Store(true)
	a.slotsMu.Lock()
	a.drainingSlots[slot] = gate
	a.slotsMu.Unlock()
	gate.inflight.Store(1)
	seedHandoffIntent(f, Handoff{
		SlotID:           slot,
		ID:               "source-release",
		Kind:             "RELEASE",
		SourceInstanceID: a.instanceID,
		SourceEpoch:      gate.epoch.Load(),
		Phase:            "DRAINING",
	})

	require.NoError(t, a.HandoffPass(context.Background()))
	assert.Equal(t, []string{"TRANSFER"}, recorder.lastActions(),
		"a release retries the transfer while the data layer holds it behind the drain gate")
	f.mu.Lock()
	phase := f.intents[slot].Phase
	drained := f.intents[slot].Drained
	f.mu.Unlock()
	assert.Equal(t, "DRAINING", phase, "an in-flight release must not complete")
	assert.False(t, drained)
}

func TestHandoffPassOrdersAndBoundsCommands(t *testing.T) {
	a, f, recorder := handoffPassAllocator(t, "handoff-batch")
	limits := defaultMigrationConfig()
	limits.BatchSlots = 2
	a.SetHandoffs(recorder, limits)
	for slot, phase := range map[uint32]string{0: "PLANNED", 1: "READY", 2: "DRAINING"} {
		seedHandoffIntent(f, Handoff{
			SlotID:           slot,
			ID:               "batch-" + phase,
			Kind:             "TRANSFER",
			SourceInstanceID: "other-instance",
			SourceEpoch:      1,
			TargetInstanceID: a.instanceID,
			Phase:            phase,
			TargetReady:      phase == "DRAINING",
		})
	}
	require.NoError(t, a.HandoffPass(context.Background()))
	assert.Equal(t, []string{"TRANSFER", "BEGIN"}, recorder.lastActions(),
		"draining work first, then ready work, bounded by batch_slots")
}

func TestHandoffPassGuards(t *testing.T) {
	ctx := context.Background()
	t.Run("uninitialized", func(t *testing.T) {
		f := newLeaseOwnershipFake()
		cfg := testDataPlaneConfig(testAllocatorConfig())
		a := NewAllocator(cfg, &rangeStore{max: map[string]int64{}}, f, unlimitedMemorySampler, nil)
		recorder := &recordingHandoffRepo{inner: f}
		a.SetHandoffs(recorder, defaultMigrationConfig())
		require.NoError(t, a.HandoffPass(ctx))
		assert.Zero(t, recorder.listCalls)
	})
	t.Run("stopping", func(t *testing.T) {
		a, _, recorder := handoffPassAllocator(t, "handoff-stopping")
		a.stopping.Store(true)
		require.NoError(t, a.HandoffPass(ctx))
		assert.Zero(t, recorder.listCalls)
	})
	t.Run("fenced", func(t *testing.T) {
		a, _, recorder := handoffPassAllocator(t, "handoff-fenced")
		a.Fence("test fence")
		require.NoError(t, a.HandoffPass(ctx))
		assert.Zero(t, recorder.listCalls)
	})
	t.Run("no repo", func(t *testing.T) {
		a, _ := leaseAllocator(t, "handoff-nil", nil)
		a.SetHandoffs(nil, defaultMigrationConfig())
		require.NoError(t, a.HandoffPass(ctx))
	})
}

func TestHandoffPassReturnsRepositoryErrors(t *testing.T) {
	a, f, recorder := handoffPassAllocator(t, "handoff-errors")
	seedHandoffIntent(f, Handoff{
		SlotID:           SlotForKey("handoff-errors"),
		ID:               "error-intent",
		Kind:             "TRANSFER",
		SourceInstanceID: "other-instance",
		SourceEpoch:      1,
		TargetInstanceID: a.instanceID,
		Phase:            "PLANNED",
	})
	wantErr := errors.New("apply failed")
	recorder.applyErr = wantErr
	require.ErrorIs(t, a.HandoffPass(context.Background()), wantErr)

	recorder.applyErr = nil
	recorder.listErr = errors.New("list failed")
	require.ErrorIs(t, a.HandoffPass(context.Background()), recorder.listErr)
}

func TestShutdownContextDrainsAndReleasesHeldSlots(t *testing.T) {
	key := "handoff-shutdown"
	slot := SlotForKey(key)
	a, f, recorder := handoffPassAllocator(t, key)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	a.ShutdownContext(ctx)

	require.Positive(t, recorder.applyCalls)
	f.mu.Lock()
	released := f.released
	phase := f.intents[slot].Phase
	row := f.rows[slot]
	lease := f.leases[a.instanceID]
	f.mu.Unlock()
	assert.EqualValues(t, 1, released, "the graceful path must release the held slot")
	assert.Equal(t, "COMPLETED", phase)
	assert.Equal(t, SlotUnowned, row.State)
	assert.Equal(t, "RETIRED", lease.State)
}

type rebalancerPlacementFake struct {
	calls        *[]string
	nodes        []NodeInfo
	segments     []OwnershipSegment
	nodesErr     error
	segmentsErr  error
	segmentCalls int
}

func (f *rebalancerPlacementFake) LiveNodes(context.Context, time.Duration) ([]NodeInfo, error) {
	*f.calls = append(*f.calls, "live")
	if f.nodesErr != nil {
		return nil, f.nodesErr
	}
	return f.nodes, nil
}

func (f *rebalancerPlacementFake) OwnershipSegments(
	context.Context,
	time.Duration,
) ([]OwnershipSegment, error) {
	f.segmentCalls++
	*f.calls = append(*f.calls, "segments")
	if f.segmentsErr != nil {
		return nil, f.segmentsErr
	}
	return f.segments, nil
}

type rebalancerHandoffFake struct {
	calls          *[]string
	pending        []Handoff
	planned        []Handoff
	recoverErr     error
	listErr        error
	collectErr     error
	planErr        error
	recoverCalls   int
	listCalls      int
	collectCalls   int
	planCalls      int
	plannedLimits  MigrationConfig
	recoverLease   CoordinatorLease
	recoverNodeLen int
}

func (f *rebalancerHandoffFake) Handoffs(
	context.Context,
	string,
	int,
) ([]Handoff, error) {
	f.listCalls++
	*f.calls = append(*f.calls, "list")
	if f.listErr != nil {
		return nil, f.listErr
	}
	return append([]Handoff(nil), f.pending...), nil
}

func (f *rebalancerHandoffFake) PlanHandoffs(
	_ context.Context,
	handoffs []Handoff,
	_ CoordinatorLease,
	limits MigrationConfig,
	_ time.Duration,
) error {
	f.planCalls++
	*f.calls = append(*f.calls, "plan")
	if f.planErr != nil {
		return f.planErr
	}
	f.planned = append(f.planned, handoffs...)
	f.plannedLimits = limits
	return nil
}

func (f *rebalancerHandoffFake) RecoverHandoffs(
	_ context.Context,
	lease CoordinatorLease,
	nodes []NodeInfo,
	_ HAConfig,
	_ int,
) error {
	f.recoverCalls++
	*f.calls = append(*f.calls, "recover")
	f.recoverLease = lease
	f.recoverNodeLen = len(nodes)
	return f.recoverErr
}

func (f *rebalancerHandoffFake) CollectInstances(context.Context, time.Duration, int) error {
	f.collectCalls++
	*f.calls = append(*f.calls, "collect")
	return f.collectErr
}

func (f *rebalancerHandoffFake) ApplyHandoffs(context.Context, HandoffRequest) error {
	panic("the rebalancer must not apply handoffs directly")
}

func rebalancerFixture(t *testing.T) (
	*Rebalancer,
	*rebalancerPlacementFake,
	*rebalancerHandoffFake,
	*[]string,
) {
	t.Helper()
	var control ControlPlaneConfig
	require.NoError(t, testkit.DecodeDefaults(&control))
	control.ReconcileInterval = time.Minute
	var ha HAConfig
	require.NoError(t, testkit.DecodeDefaults(&ha))
	ha.NodeTTL = 15 * time.Second
	ha.QuietWindow = 5 * time.Second
	control.Migration.BatchSlots = 4
	control.Migration.MaxPlanned = 100
	control.Migration.JoinStabilityWindow = time.Second

	calls := []string{}
	placement := &rebalancerPlacementFake{
		calls: &calls,
		nodes: []NodeInfo{
			{ID: "node-a", InstanceID: "ia"},
			{ID: "node-b", InstanceID: "ib", StableFor: time.Hour},
		},
		segments: []OwnershipSegment{{
			StartSlot:       0,
			EndSlot:         SlotCount - 1,
			OwnerNodeID:     "node-a",
			OwnerInstanceID: "ia",
			Epoch:           1,
			State:           SlotOwned,
			GrantAgeKnown:   true,
		}},
	}
	repo := &rebalancerHandoffFake{calls: &calls}
	return NewRebalancer(control, ha, placement, repo), placement, repo, &calls
}

func TestRebalancerPassRecoversBeforePlanning(t *testing.T) {
	rebalancer, placement, repo, calls := rebalancerFixture(t)
	lease := CoordinatorLease{Held: true, InstanceID: "control", Epoch: 3}

	active, err := rebalancer.Pass(context.Background(), lease)
	require.NoError(t, err)
	assert.True(t, active, "planned migrations keep the publisher active")
	require.Equal(t, []string{"live", "recover", "list", "collect", "segments", "plan"}, *calls)
	assert.Equal(t, lease, repo.recoverLease)
	assert.Len(t, repositoryPlanned(repo), 4, "one pass plans at most batch_slots")
	assert.Equal(t, len(placement.nodes), repo.recoverNodeLen)
}

func TestRebalancerPassThrottlesCollectionAndPlanning(t *testing.T) {
	rebalancer, placement, repo, calls := rebalancerFixture(t)
	rebalancer.lastCollection = time.Now()
	rebalancer.lastPlan = time.Now()

	active, err := rebalancer.Pass(context.Background(), CoordinatorLease{Held: true})
	require.NoError(t, err)
	assert.False(t, active)
	assert.Equal(t, []string{"live", "recover", "list"}, *calls)
	assert.Zero(t, repo.collectCalls)
	assert.Zero(t, placement.segmentCalls)
	assert.Zero(t, repo.planCalls)
}

func TestRebalancerPassStopsPlanningAtMaxPlanned(t *testing.T) {
	rebalancer, placement, repo, calls := rebalancerFixture(t)
	rebalancer.cfg.Migration.MaxPlanned = 3
	repo.pending = []Handoff{
		{
			SlotID:           0,
			Kind:             "TRANSFER",
			SourceInstanceID: "ia",
			TargetInstanceID: "ib",
			Phase:            "PLANNED",
		},
		{
			SlotID:           1,
			Kind:             "TRANSFER",
			SourceInstanceID: "ia",
			TargetInstanceID: "ib",
			Phase:            "PLANNED",
		},
		{
			SlotID:           2,
			Kind:             "TRANSFER",
			SourceInstanceID: "ia",
			TargetInstanceID: "ib",
			Phase:            "PLANNED",
		},
	}

	active, err := rebalancer.Pass(context.Background(), CoordinatorLease{Held: true})
	require.NoError(t, err)
	assert.True(t, active)
	assert.Equal(t, []string{"live", "recover", "list", "collect"}, *calls)
	assert.Zero(t, placement.segmentCalls)
	assert.Zero(t, repo.planCalls)
}

func TestRebalancerPassCapsPlanningByRemainingCapacity(t *testing.T) {
	rebalancer, _, repo, _ := rebalancerFixture(t)
	rebalancer.cfg.Migration.MaxPlanned = 6
	repo.pending = []Handoff{
		{
			SlotID:           0,
			Kind:             "TRANSFER",
			SourceInstanceID: "ia",
			TargetInstanceID: "ib",
			Phase:            "PLANNED",
		},
		{
			SlotID:           1,
			Kind:             "TRANSFER",
			SourceInstanceID: "ia",
			TargetInstanceID: "ib",
			Phase:            "PLANNED",
		},
	}

	_, err := rebalancer.Pass(context.Background(), CoordinatorLease{Held: true})
	require.NoError(t, err)
	assert.Len(t, repositoryPlanned(repo), 4,
		"planning must not exceed the remaining max_planned budget")
}

func TestRebalancerPassErrorPropagation(t *testing.T) {
	wantErr := errors.New("repository failed")
	cases := []struct {
		name      string
		breakIt   func(*rebalancerPlacementFake, *rebalancerHandoffFake)
		pending   bool
		wantErr   error
		wantState bool
	}{
		{
			name:    "live nodes",
			breakIt: func(p *rebalancerPlacementFake, _ *rebalancerHandoffFake) { p.nodesErr = wantErr },
			wantErr: wantErr,
		},
		{
			name:    "recovery",
			breakIt: func(_ *rebalancerPlacementFake, r *rebalancerHandoffFake) { r.recoverErr = wantErr },
			wantErr: wantErr,
		},
		{
			name:    "pending list",
			breakIt: func(_ *rebalancerPlacementFake, r *rebalancerHandoffFake) { r.listErr = wantErr },
			wantErr: wantErr,
		},
		{
			name:      "collection with pending work",
			breakIt:   func(_ *rebalancerPlacementFake, r *rebalancerHandoffFake) { r.collectErr = wantErr },
			pending:   true,
			wantErr:   wantErr,
			wantState: true,
		},
		{
			name:      "ownership view with pending work",
			breakIt:   func(p *rebalancerPlacementFake, _ *rebalancerHandoffFake) { p.segmentsErr = wantErr },
			pending:   true,
			wantErr:   wantErr,
			wantState: true,
		},
		{
			name:      "planning with pending work",
			breakIt:   func(_ *rebalancerPlacementFake, r *rebalancerHandoffFake) { r.planErr = wantErr },
			pending:   true,
			wantErr:   wantErr,
			wantState: true,
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			rebalancer, placement, repo, _ := rebalancerFixture(t)
			if test.pending {
				repo.pending = []Handoff{{
					SlotID: 0, Kind: "TRANSFER", SourceInstanceID: "ia",
					TargetInstanceID: "ib", Phase: "PLANNED",
				}}
			}
			test.breakIt(placement, repo)
			active, err := rebalancer.Pass(context.Background(), CoordinatorLease{Held: true})
			require.ErrorIs(t, err, test.wantErr)
			assert.Equal(t, test.wantState, active)
		})
	}
}

func repositoryPlanned(repo *rebalancerHandoffFake) []Handoff {
	return repo.planned
}

type clockProbe struct {
	local, storage time.Time
	err            error
}

func newClockProbe() *clockProbe {
	now := time.Date(2026, 10, 2, 0, 0, 0, 0, time.UTC)
	return &clockProbe{local: now, storage: now}
}
func (p *clockProbe) Now() time.Time                                  { return p.local }
func (p *clockProbe) StorageClock(context.Context) (time.Time, error) { return p.storage, p.err }
func (p *clockProbe) advance(local, storage time.Duration) {
	p.local = p.local.Add(local)
	p.storage = p.storage.Add(storage)
}

func TestStorageClockMonitorBoundsAndStickyFence(t *testing.T) {
	for _, drift := range []time.Duration{-250 * time.Millisecond, 50 * time.Millisecond, 250 * time.Millisecond} {
		t.Run(drift.String(), func(t *testing.T) {
			p := newClockProbe()
			m := NewStorageClockMonitor(
				100*time.Millisecond,
				100*time.Millisecond,
				p.Now,
				p.StorageClock,
			)
			require.NoError(t, m.Observe(context.Background()))
			assert.Zero(t, m.Stats().Drift)
			p.advance(time.Second, time.Second+drift)
			err := m.Observe(context.Background())
			if drift == 50*time.Millisecond {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, errStorageClockViolation)
				p.advance(time.Second, time.Second)
				require.ErrorIs(t, m.Observe(context.Background()), errStorageClockViolation)
				assert.True(t, m.Violated())
			}
		})
	}
}

func TestStorageClockMonitorSamplingLatencyAndUnavailableStore(t *testing.T) {
	p := newClockProbe()
	read := func(ctx context.Context) (time.Time, error) {
		value, err := p.StorageClock(ctx)
		p.local = p.local.Add(300 * time.Millisecond)
		return value, err
	}
	m := NewStorageClockMonitor(100*time.Millisecond, 100*time.Millisecond, p.Now, read)
	require.NoError(t, m.Observe(context.Background()))
	p.advance(time.Second, time.Second)
	require.NoError(t, m.Observe(context.Background()))
	assert.False(t, m.Violated())
	assert.Equal(t, 300*time.Millisecond, m.Stats().SampleRTT)
	p.err = errors.New("unavailable")
	require.ErrorIs(t, m.Observe(context.Background()), p.err)
	assert.False(t, m.Violated())
}

func TestStorageClockMonitorForwardJump(t *testing.T) {
	p := newClockProbe()
	m := NewStorageClockMonitor(time.Hour, 100*time.Millisecond, p.Now, p.StorageClock)
	require.NoError(t, m.Observe(context.Background()))
	p.advance(time.Second, 6*time.Second)
	require.ErrorIs(t, m.Observe(context.Background()), errStorageClockViolation)
	assert.Equal(t, 5*time.Second, m.Stats().ForwardJump)
}

func TestFenceOutlivesFurtherRenewalAndShutdown(t *testing.T) {
	a, f := leaseAllocator(t, "fenced", nil)
	a.Fence("clock violation")
	a.RenewLeases()
	assert.False(t, a.Readiness().Ready)
	assert.True(t, a.Paused())
	assert.Equal(t, "fenced", a.Readiness().Reason)
	a.Shutdown()
	f.mu.Lock()
	defer f.mu.Unlock()
	assert.Equal(t, "RETIRED", f.leases[a.InstanceID()].State)
}

func TestMaintenanceRetainsPendingDrainAcknowledgment(t *testing.T) {
	a, _ := leaseAllocator(t, "drain", nil)
	slot := a.slots[SlotForKey("drain")]
	a.drainingSlots[SlotForKey("drain")] = slot
	m := &NodeManager{allocator: a}
	m.Maintain(context.Background())
	assert.Same(t, slot, a.drainingSlots[SlotForKey("drain")])
}

// staticRouteRepo serves one directory snapshot for a refresh test.
type staticRouteRepo struct {
	version int64
	err     error
}

func (r staticRouteRepo) GetNewerRoute(context.Context, int64) (*Route, error) {
	if r.err != nil {
		return nil, r.err
	}
	return &Route{Version: r.version, LayoutVersion: 1}, nil
}

// TestRefreshClaimsUnderItsOwnBudgetNotTheRouteQueryTimeout pins the split
// between the directory read and the authority pass that follows it.
//
// The read is bounded by the route query timeout, but the pass claims the slots
// the plan hands this node one storage batch at a time. Sharing the read's
// bound spends it on the first batch, so a node whose storage is slower than one
// directory read never installs the slots it owns and reports "slot not found"
// for keys it should be serving.
func TestRefreshClaimsUnderItsOwnBudgetNotTheRouteQueryTimeout(t *testing.T) {
	cfg := testDataPlaneConfig(testAllocatorConfig())
	cfg.Node.ID = "node-a"
	cfg.Node.RouteQueryTimeout = 20 * time.Millisecond
	cfg.Allocator.ReserveTimeout = 250 * time.Millisecond
	cfg.HA.LeaseDuration = 10 * time.Second
	cfg.HA.RenewInterval = 3 * time.Second
	cfg.HA.SafetyMargin = time.Second

	authority := newLeaseOwnershipFake()
	allocator := NewAllocator(
		cfg,
		&rangeStore{max: map[string]int64{}},
		authority,
		unlimitedMemorySampler,
		slog.Default(),
	)
	allocator.SetHandoffs(authority, defaultMigrationConfig())
	allocator.RenewLeases()
	require.True(
		t,
		allocator.initialized.Load(),
		"renewal must register and initialize the instance",
	)

	// The first claim batch is slower than the directory read's bound. With the
	// read's bound shared by the whole pass the batch is cancelled; with the
	// pass's own bound it commits and the plan is installed.
	var batches int
	allocator.ownership = ownershipProbe{
		OwnershipRepo: authority,
		claim: func(ctx context.Context, req ClaimRequest) ([]ClaimOutcome, error) {
			batches++
			if batches == 1 {
				select {
				case <-time.After(4 * cfg.Node.RouteQueryTimeout):
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			return authority.ClaimSlots(ctx, req)
		},
	}

	instance := allocator.InstanceID()
	manager := NewNodeManager(
		cfg,
		allocator,
		staticRouteRepo{version: 1},
		nil,
		&placementFake{
			// The whole slot space already belongs to this node, so the plan
			// hands it every slot rather than only the ones it has to take over.
			segments: []OwnershipSegment{{
				StartSlot:       0,
				EndSlot:         SlotCount - 1,
				OwnerNodeID:     cfg.Node.ID,
				OwnerInstanceID: instance,
				Epoch:           1,
				State:           SlotOwned,
				GrantAgeKnown:   true,
			}},
			nodes: []NodeInfo{{
				ID:         cfg.Node.ID,
				InstanceID: instance,
				StableFor:  time.Hour,
			}},
		},
		NewRouteCache(),
		nil,
		authority,
		ControlPlaneConfig{},
		slog.Default(),
	)
	manager.Refresh(context.Background())

	require.Positive(t, batches, "the pass must claim at least one batch")
	_, installed := allocator.slots[7]
	assert.True(t, installed, "the node must install the slots the plan handed it")
}
