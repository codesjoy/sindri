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
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/codesjoy/pkg/basic/xerror"
	"github.com/codesjoy/sindri/gen/go/sequence/reason"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This authority drives tests through the same registration/revision/renewal
// protocol as production; range tests do not disable authorization.
type leaseOwnershipFake struct {
	mu         sync.Mutex
	rows       map[uint32]Ownership
	leases     map[string]InstanceLease
	intents    map[uint32]Handoff
	renewErr   error
	claimErr   error
	storageNow time.Time
	released   int64
	claims     int64
	loadCalls  int64
	renewCalls int64
}

func newLeaseOwnershipFake() *leaseOwnershipFake {
	return &leaseOwnershipFake{
		rows:    map[uint32]Ownership{},
		leases:  map[string]InstanceLease{},
		intents: map[uint32]Handoff{},
	}
}

func (f *leaseOwnershipFake) RegisterInstance(_ context.Context, node, instance string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if old, ok := f.leases[instance]; ok {
		if old.NodeID != node || old.State != "ACTIVE" {
			return ErrAuthorityChanged
		}
		return nil
	}
	f.leases[instance] = InstanceLease{
		InstanceID: instance,
		NodeID:     node,
		State:      "ACTIVE",
		GrantedAt:  time.Now(),
	}
	return nil
}

func (f *leaseOwnershipFake) RenewInstance(
	_ context.Context,
	instance string,
	revision uint64,
) (InstanceLease, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.renewCalls++
	if f.renewErr != nil {
		return InstanceLease{}, f.renewErr
	}
	row, ok := f.leases[instance]
	if !ok || row.State != "ACTIVE" || row.Revision != revision {
		return InstanceLease{}, ErrAuthorityChanged
	}
	row.GrantedAt = time.Now()
	f.leases[instance] = row
	return row, nil
}

func (f *leaseOwnershipFake) InstanceAuthority(
	_ context.Context,
	instance string,
) (AuthoritySnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.loadCalls++
	lease, ok := f.leases[instance]
	if !ok || lease.State != "ACTIVE" {
		return AuthoritySnapshot{}, ErrAuthorityChanged
	}
	snapshot := AuthoritySnapshot{Lease: lease}
	for _, o := range f.rows {
		if o.OwnerInstanceID == instance {
			snapshot.Slots = append(snapshot.Slots, o)
		}
	}
	sort.Slice(
		snapshot.Slots,
		func(i, j int) bool { return snapshot.Slots[i].SlotID < snapshot.Slots[j].SlotID },
	)
	return snapshot, nil
}

func (f *leaseOwnershipFake) RetireInstance(_ context.Context, instance string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	row := f.leases[instance]
	row.State = "RETIRED"
	row.Revision++
	f.leases[instance] = row
	return nil
}

func (f *leaseOwnershipFake) ClaimSlots(
	_ context.Context,
	req ClaimRequest,
) ([]ClaimOutcome, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.claimErr != nil {
		return nil, f.claimErr
	}
	lease := f.leases[req.InstanceID]
	if lease.State != "ACTIVE" || lease.Revision != req.Revision {
		return nil, ErrAuthorityChanged
	}
	var results []ClaimOutcome
	changed := false
	for _, id := range req.Slots {
		row := f.rows[id]
		if row.State == SlotDraining {
			results = append(results, ClaimOutcome{Ownership: row})
			continue
		}
		if row.OwnerInstanceID != req.InstanceID {
			row.Epoch++
			changed = true
		}
		row.SlotID = id
		row.State = SlotOwned
		row.OwnerInstanceID = req.InstanceID
		row.OwnerNodeID = req.NodeID
		f.rows[id] = row
		f.claims++
		results = append(results, ClaimOutcome{Ownership: row, Granted: true})
	}
	if changed {
		lease.Revision++
		f.leases[req.InstanceID] = lease
	}
	return results, nil
}

func (f *leaseOwnershipFake) StorageClock(context.Context) (time.Time, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.storageNow.IsZero() {
		return time.Now(), nil
	}
	return f.storageNow, nil
}

func (f *leaseOwnershipFake) Handoffs(
	_ context.Context,
	instance string,
	limit int,
) ([]Handoff, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var result []Handoff
	for _, h := range f.intents {
		if h.Active() &&
			(instance == "" || h.SourceInstanceID == instance || h.TargetInstanceID == instance) {
			result = append(result, h)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].SlotID < result[j].SlotID })
	return result[:min(limit, len(result))], nil
}

func (f *leaseOwnershipFake) PlanHandoffs(
	context.Context,
	[]Handoff,
	CoordinatorLease,
	MigrationConfig,
	time.Duration,
) error {
	return nil
}

func (f *leaseOwnershipFake) RecoverHandoffs(
	context.Context,
	CoordinatorLease,
	[]NodeInfo,
	HAConfig,
	int,
) error {
	return nil
}

func (f *leaseOwnershipFake) CollectInstances(
	context.Context,
	time.Duration,
	int,
) error {
	return nil
}

func (f *leaseOwnershipFake) ApplyHandoffs(_ context.Context, req HandoffRequest) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	changed := false
	for _, cmd := range req.Commands {
		h := cmd.Handoff
		o := f.rows[h.SlotID]
		current := f.intents[h.SlotID]
		switch cmd.Action {
		case "WITHDRAW":
			if o.Holds(req.InstanceID, h.SourceEpoch) {
				h.Kind = "RELEASE"
				h.Phase = "DRAINING"
				h.NotBefore = time.Now().Add(req.HA.QuietWindow)
				f.intents[h.SlotID] = h
				o.State = SlotDraining
				f.rows[h.SlotID] = o
				changed = true
			}
		case "ACK_DRAIN":
			if current.ID == h.ID && current.SourceEpoch == h.SourceEpoch {
				current.Drained = true
				f.intents[h.SlotID] = current
			}
		case "TRANSFER":
			if current.ID == h.ID && current.Drained {
				current.Phase = "COMPLETED"
				o.State = SlotUnowned
				o.OwnerNodeID = ""
				o.OwnerInstanceID = ""
				f.rows[h.SlotID] = o
				f.intents[h.SlotID] = current
				f.released++
				changed = true
			}
		}
	}
	if changed {
		lease := f.leases[req.InstanceID]
		lease.Revision++
		f.leases[req.InstanceID] = lease
	}
	return nil
}

// testAssignSlots is a fixture operation, not a route-application protocol. It
// mutates the fake database's authority, then applies a complete snapshot and
// renews before any gate may open.
func (a *Allocator) testAssignSlots(slots []uint32) {
	f, ok := a.ownership.(*leaseOwnershipFake)
	if !ok {
		panic("test authority required")
	}
	a.RenewLeases()
	wanted := map[uint32]bool{}
	for _, id := range slots {
		wanted[id] = true
	}
	f.mu.Lock()
	lease := f.leases[a.instanceID]
	for id, o := range f.rows {
		if o.OwnerInstanceID == a.instanceID && !wanted[id] {
			o.State = SlotUnowned
			o.OwnerInstanceID = ""
			o.OwnerNodeID = ""
			f.rows[id] = o
			lease.Revision++
		}
	}
	f.leases[a.instanceID] = lease
	f.mu.Unlock()
	if err := a.SyncAuthority(context.Background()); err != nil {
		panic(err)
	}
	if err := a.Claim(context.Background(), slots); err != nil {
		panic(err)
	}
	a.RenewLeases()
	a.MarkRouteApplied()
}

func leaseAllocator(t *testing.T, key string, mono func() int64) (*Allocator, *leaseOwnershipFake) {
	t.Helper()
	return leaseAllocatorWith(t, key, newLeaseOwnershipFake(), mono)
}

func leaseAllocatorWith(
	t *testing.T,
	key string,
	f *leaseOwnershipFake,
	mono func() int64,
) (*Allocator, *leaseOwnershipFake) {
	t.Helper()
	cfg := testDataPlaneConfig(testAllocatorConfig())
	cfg.HA.LeaseDuration = 10 * time.Second
	cfg.HA.RenewInterval = 3 * time.Second
	cfg.HA.SafetyMargin = time.Second
	a := NewAllocator(
		cfg,
		&rangeStore{max: map[string]int64{}},
		f,
		unlimitedMemorySampler,
		slog.Default(),
	)
	if mono != nil {
		a.monoNow = mono
	}
	a.SetHandoffs(f, defaultMigrationConfig())
	a.testAssignSlots([]uint32{SlotForKey(key)})
	require.Contains(t, a.slots, SlotForKey(key))
	return a, f
}

func TestInstanceRenewalIsConstantWorkAndFastPathDoesNotReadAuthority(t *testing.T) {
	var mono int64
	a, f := leaseAllocator(t, "orders", func() int64 { return mono })
	initialReads := f.loadCalls
	for range 5 {
		_, err := a.FetchNext(context.Background(), "orders")
		require.NoError(t, err)
	}
	reads, calls := f.loadCalls, f.renewCalls
	mono = int64(3 * time.Second)
	a.RenewLeases()
	assert.Equal(t, calls+1, f.renewCalls)
	assert.Equal(t, initialReads, reads)
	assert.Equal(t, reads, f.loadCalls)
	assert.Equal(t, int64(12*time.Second), a.localDeadline.Load())
}

func TestInstanceLeaseExpiresAndUnchangedAuthorityRecovers(t *testing.T) {
	var mono int64
	a, _ := leaseAllocator(t, "orders", func() int64 { return mono })
	assert.Equal(t, int64(9*time.Second), a.localDeadline.Load())
	mono = int64(9 * time.Second)
	_, err := a.FetchNext(context.Background(), "orders")
	require.True(t, xerror.IsReason(err, reason.Reason_SEQUENCE_LEASE_EXPIRED))
	a.RenewLeases()
	_, err = a.FetchNext(context.Background(), "orders")
	require.NoError(t, err)
}

func TestRevisionConflictClosesWithdrawnGatesBeforeRenewal(t *testing.T) {
	keys := distinctSlotKeys(2)
	a, f := leaseAllocator(t, keys[0], func() int64 { return 0 })
	a.testAssignSlots([]uint32{SlotForKey(keys[0]), SlotForKey(keys[1])})
	_, err := a.FetchNext(context.Background(), keys[1])
	require.NoError(t, err)
	unchanged := a.slots[SlotForKey(keys[1])]
	withdrawn := a.slots[SlotForKey(keys[0])]
	f.mu.Lock()
	o := f.rows[SlotForKey(keys[0])]
	o.State = SlotDraining
	f.rows[o.SlotID] = o
	lease := f.leases[a.instanceID]
	lease.Revision++
	f.leases[a.instanceID] = lease
	f.mu.Unlock()
	a.RenewLeases()
	assert.True(t, withdrawn.draining.Load())
	assert.Same(t, unchanged, a.slots[SlotForKey(keys[1])])
	assert.Equal(t, lease.Revision, a.ownershipRevision.Load())
	_, err = a.FetchNext(context.Background(), keys[1])
	require.NoError(t, err)
}

func TestFailedRenewalAndIrrecoverableFenceNeverExtendDeadline(t *testing.T) {
	var mono int64
	a, f := leaseAllocator(t, "orders", func() int64 { return mono })
	deadline := a.localDeadline.Load()
	f.renewErr = errors.New("unavailable")
	mono = int64(8 * time.Second)
	a.RenewLeases()
	assert.Equal(t, deadline, a.localDeadline.Load())
	mono = int64(9 * time.Second)
	_, err := a.FetchNext(context.Background(), "orders")
	require.Error(t, err)
	a.Fence("clock violation")
	f.renewErr = nil
	calls := f.renewCalls
	a.RenewLeases()
	assert.Equal(t, calls, f.renewCalls)
}

func TestPauseBeyondBoundFencesConsumedAllocation(t *testing.T) {
	a, _ := leaseAllocator(t, "orders", func() int64 { return 0 })
	_, err := a.FetchNext(context.Background(), "orders")
	require.NoError(t, err)
	var mono int64
	a.monoNow = func() int64 { mono += int64(2 * time.Second); return mono }
	_, err = a.FetchNext(context.Background(), "orders")
	require.Error(t, err)
	assert.EqualValues(t, 1, a.LeaseStats().PauseViolations)
	assert.True(t, a.Paused())
}

func TestRetryAfterIsBounded(t *testing.T) {
	a, _ := leaseAllocator(t, "orders", nil)
	assert.Equal(t, 250*time.Millisecond, a.RetryAfter(reason.Reason_SEQUENCE_OWNER_RECOVERING))
	assert.Zero(t, a.RetryAfter(reason.Reason_SEQUENCE_ROUTE_EXPIRED))
}

type reservationProbe func(context.Context, ReservationAuthority, []ReservationRequest) ([]SequenceRange, error)

func (f reservationProbe) ReserveRanges(
	ctx context.Context,
	authority ReservationAuthority,
	req []ReservationRequest,
) ([]SequenceRange, error) {
	return f(ctx, authority, req)
}

type ownershipProbe struct {
	OwnershipRepo
	claim func(context.Context, ClaimRequest) ([]ClaimOutcome, error)
	renew func(context.Context, string, uint64) (InstanceLease, error)
}

func (p ownershipProbe) ClaimSlots(ctx context.Context, req ClaimRequest) ([]ClaimOutcome, error) {
	if p.claim != nil {
		return p.claim(ctx, req)
	}
	return p.OwnershipRepo.ClaimSlots(ctx, req)
}

func (p ownershipProbe) RenewInstance(
	ctx context.Context,
	id string,
	rev uint64,
) (InstanceLease, error) {
	if p.renew != nil {
		return p.renew(ctx, id, rev)
	}
	return p.OwnershipRepo.RenewInstance(ctx, id, rev)
}

func TestBatchReservationsCarryLeaseRevisionAndStatementDeadline(t *testing.T) {
	a, _ := leaseAllocator(t, "authority", func() int64 { return 0 })
	store := a.store
	a.store = reservationProbe(
		func(ctx context.Context, auth ReservationAuthority, req []ReservationRequest) ([]SequenceRange, error) {
			assert.Equal(t, a.InstanceID(), auth.InstanceID)
			assert.Equal(t, a.ha.LeaseDuration, auth.Lease)
			assert.Equal(t, a.ownershipRevision.Load(), auth.Revision)
			assert.EqualValues(t, 1, auth.Epochs[SlotForKey("authority")])
			deadline, ok := ctx.Deadline()
			require.True(t, ok)
			assert.LessOrEqual(t, time.Until(deadline), a.storeTimeout())
			return store.ReserveRanges(ctx, auth, req)
		},
	)
	_, err := a.FetchNextN(context.Background(), "authority", 2)
	require.NoError(t, err)
}

func TestColdReservationWaitCannotServeAnExpiredWarmKey(t *testing.T) {
	var mono atomic.Int64
	keys := distinctSlotKeys(2)
	a, _ := leaseAllocator(t, keys[0], mono.Load)
	a.testAssignSlots([]uint32{SlotForKey(keys[0]), SlotForKey(keys[1])})
	_, err := a.FetchNext(context.Background(), keys[0])
	require.NoError(t, err)
	store := a.store
	a.store = reservationProbe(
		func(ctx context.Context, auth ReservationAuthority, req []ReservationRequest) ([]SequenceRange, error) {
			rows, err := store.ReserveRanges(ctx, auth, req)
			mono.Store(int64(10 * time.Second))
			return rows, err
		},
	)
	_, err = a.FetchNextBatch(
		context.Background(),
		[]SequenceRequest{{Key: keys[0], Count: 1}, {Key: keys[1], Count: 1}},
	)
	require.Error(t, err)
}

func TestFrontReservationHonorsTheShorterDeadline(t *testing.T) {
	a, _ := leaseAllocator(t, "deadline", nil)
	a.store = reservationProbe(
		func(ctx context.Context, _ ReservationAuthority, _ []ReservationRequest) ([]SequenceRange, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err := a.FetchNext(ctx, "deadline")
	require.Error(t, err)
	assert.Less(t, time.Since(started), time.Second)
}

func TestDelayedRenewalUsesSendingAnchorAndDiscardsLateResponse(t *testing.T) {
	for _, delay := range []time.Duration{2 * time.Second, 20 * time.Second} {
		t.Run(delay.String(), func(t *testing.T) {
			var mono int64
			a, f := leaseAllocator(t, "renew", func() int64 { return mono })
			previous := a.localDeadline.Load()
			a.ownership = ownershipProbe{
				OwnershipRepo: f,
				renew: func(ctx context.Context, id string, rev uint64) (InstanceLease, error) {
					lease, err := f.RenewInstance(ctx, id, rev)
					mono += int64(delay)
					return lease, err
				},
			}
			mono = int64(4 * time.Second)
			a.RenewLeases()
			if delay < 9*time.Second {
				assert.Equal(t, int64(13*time.Second), a.localDeadline.Load())
			} else {
				assert.Equal(t, previous, a.localDeadline.Load())
				_, err := a.FetchNext(context.Background(), "renew")
				require.Error(t, err)
			}
		})
	}
}

func TestReclaimNeverInheritsPreviousEpochRange(t *testing.T) {
	a, f := leaseAllocator(t, "reclaim", func() int64 { return 0 })
	first, err := a.FetchNextN(context.Background(), "reclaim", 1)
	require.NoError(t, err)
	old := a.slots[SlotForKey("reclaim")]
	f.mu.Lock()
	o := f.rows[SlotForKey("reclaim")]
	o.Epoch++
	f.rows[o.SlotID] = o
	lease := f.leases[a.instanceID]
	lease.Revision++
	f.leases[a.instanceID] = lease
	f.mu.Unlock()
	a.RenewLeases()
	require.NotSame(t, old, a.slots[SlotForKey("reclaim")])
	second, err := a.FetchNextN(context.Background(), "reclaim", 1)
	require.NoError(t, err)
	assert.Greater(t, second.ID, first.ID+9)
	assert.Greater(t, second.SlotEpoch, first.SlotEpoch)
}

func TestClaimProgressSurvivesBoundedFailure(t *testing.T) {
	a, f := leaseAllocator(t, "progress", func() int64 { return 0 })
	a.cfg.ReserveTimeout = 10 * time.Millisecond
	calls := 0
	a.ownership = ownershipProbe{
		OwnershipRepo: f,
		claim: func(ctx context.Context, req ClaimRequest) ([]ClaimOutcome, error) {
			calls++
			if calls == 2 {
				<-ctx.Done()
				return nil, ctx.Err()
			}
			return f.ClaimSlots(ctx, req)
		},
	}
	slots := make([]uint32, ownershipBatchSize+1)
	for i := range slots {
		slots[i] = uint32(i)
	}
	require.Error(t, a.Claim(context.Background(), slots))
	epoch := a.slots[0].epoch.Load()
	require.NoError(t, a.Claim(context.Background(), slots))
	assert.Equal(t, epoch, a.slots[0].epoch.Load())
	require.Contains(t, a.slots, uint32(ownershipBatchSize))
}

func TestCursorPauseDiscardsConsumedIDs(t *testing.T) {
	var mono int64
	a, _ := leaseAllocator(t, "pause", func() int64 { return mono })
	_, err := a.FetchNext(context.Background(), "pause")
	require.NoError(t, err)
	slot := a.slots[SlotForKey("pause")]
	state, _ := slot.Load("pause")
	started, err := state.beginLinearization()
	require.NoError(t, err)
	consumed := state.next.Add(1)
	mono += int64(a.ha.MaxPause + time.Nanosecond)
	require.Error(t, state.checkLinearizationBound(started))
	assert.Equal(t, consumed, state.next.Load())
	assert.True(t, slot.draining.Load())
}

func TestExhaustedCursorCannotResetPauseObservation(t *testing.T) {
	a, _ := leaseAllocator(t, "cursor", func() int64 { return 0 })
	_, err := a.FetchNext(context.Background(), "cursor")
	require.NoError(t, err)
	slot := a.slots[SlotForKey("cursor")]
	state, _ := slot.Load("cursor")
	state.next.Store(state.end.Load())
	state.standby = &SequenceRange{Start: 100, End: 110}
	end := state.end.Load()
	a.monoNow = func() int64 {
		if state.next.Load() > end {
			return int64(a.ha.MaxPause + time.Nanosecond)
		}
		return 0
	}
	id, err := a.FetchNext(context.Background(), "cursor")
	require.Error(t, err)
	assert.Zero(t, id)
	assert.True(t, slot.draining.Load())
	assert.Less(t, state.next.Load(), int64(100))
}

func TestLockWaitAndStandbyDoNotBypassLeaseOrGate(t *testing.T) {
	for _, withdraw := range []bool{false, true} {
		for _, count := range []uint32{1, 2} {
			var mono atomic.Int64
			a, _ := leaseAllocator(t, "standby", mono.Load)
			_, err := a.FetchNext(context.Background(), "standby")
			require.NoError(t, err)
			slot := a.slots[SlotForKey("standby")]
			state, _ := slot.Load("standby")
			state.next.Store(state.end.Load())
			state.mu.Lock()
			state.standby = &SequenceRange{Start: 100, End: 110}
			done := make(chan error, 1)
			go func() { _, err := a.FetchNextN(context.Background(), "standby", count); done <- err }()
			if withdraw {
				slot.draining.Store(true)
			} else {
				mono.Store(int64(10 * time.Second))
			}
			state.mu.Unlock()
			require.Error(t, <-done)
			assert.Less(t, state.next.Load(), int64(100))
		}
	}
}

func BenchmarkAllocatorFetchNextNHotKey(b *testing.B) {
	a := readyAllocatorForKeys(b, "rpc-hot")
	a.localDeadline.Store(math.MaxInt64)
	slot := a.slots[SlotForKey("rpc-hot")]
	state := &keyState{allocator: a, slot: slot, activeStep: math.MaxInt64}
	state.initialized.Store(true)
	state.start.Store(1)
	state.next.Store(1)
	state.end.Store(math.MaxInt64)
	slot.Store("rpc-hot", state)
	a.store = reservationProbe(
		func(context.Context, ReservationAuthority, []ReservationRequest) ([]SequenceRange, error) {
			return nil, errors.New("unexpected reservation")
		},
	)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := a.FetchNextN(context.Background(), "rpc-hot", 1); err != nil {
			b.Fatal(err)
		}
	}
}
