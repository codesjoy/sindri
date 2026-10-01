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
	"sync"
	"testing"
	"time"

	"github.com/codesjoy/pkg/basic/xerror"
	"github.com/codesjoy/sindri/gen/go/sequence/reason"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAFencedInstanceDoesNotClaimAPendingRoute pins the route-driven claim path,
// which is the other way this instance could take on authority it cannot serve.
//
// The pending route is deliberately left pending rather than dropped. The fence is
// sticky, so nothing recoverable is lost by never applying it, and the path stays
// honest about what the allocator is holding.
func TestAFencedInstanceDoesNotClaimAPendingRoute(t *testing.T) {
	keys := distinctSlotKeys(2)
	key, other := keys[0], keys[1]
	ownership := newLeaseOwnershipFake()
	allocator, _ := leaseAllocatorWith(t, key, ownership, nil)
	require.Equal(t, 1, ownership.claims, "opening the route claims its own slot")

	allocator.Fence("storage clock exceeded its asserted drift or jump bound")
	allocator.CommitRoute(2, 0, []uint32{SlotForKey(key), SlotForKey(other)})
	allocator.ApplyRoute(0)

	assert.Equal(
		t,
		1,
		ownership.claims,
		"a fenced instance must not claim a pending route's slots",
	)
	assert.NotContains(t, allocator.slots, SlotForKey(other))
}

// TestAllocatorReclaimsASlotFencedByAFailedRenewal pins the recovery path the
// local planner needs: a slot whose renewal did not confirm the grant is closed
// to allocations, and the plan that still hands that slot to this node is what
// re-arms it.
//
// The failure this guards against is a fence that is never lifted. The slot
// stays in the allocator's map, so a plan that treats "installed" as "nothing to
// claim" would leave it closed for the life of the process even though every
// heartbeat keeps confirming that this node is the one that should serve it.
func TestAllocatorReclaimsASlotFencedByAFailedRenewal(t *testing.T) {
	key := "fenced-reclaim"
	var now int64
	allocator, ownership := leaseAllocator(t, key, func() int64 { return now })
	slot := SlotForKey(key)

	ownership.dropRenewals = true
	now = int64(4 * time.Second)
	allocator.RenewLeases()
	require.True(
		t,
		allocator.slots[slot].draining.Load(),
		"a renewal that did not confirm the grant must close the slot",
	)
	_, err := allocator.FetchNext(context.Background(), key)
	require.Error(t, err, "a fenced slot must not serve")
	assert.True(
		t,
		xerror.IsReason(err, reason.Reason_SEQUENCE_SLOT_NOT_OWNER),
		"want a slot-not-owner reason, got %v",
		err,
	)

	ownership.dropRenewals = false
	allocator.Reconcile(allocator.CurrentVersion(), 0, []uint32{slot}, false)
	allocator.ApplyRoute(0)

	assert.False(
		t,
		allocator.slots[slot].draining.Load(),
		"the plan still hands the slot to this node, so it must be re-armed",
	)
	first, err := allocator.FetchNext(context.Background(), key)
	require.NoError(t, err)
	assert.Equal(t, int64(1), first)
}

// leaseOwnershipFake is a storage authority whose decisions a test drives
// directly. It models the parts of the contract the allocator depends on: an
// epoch that only moves forward, and a renewal that may fail or drop a grant the
// caller thought it held.
type leaseOwnershipFake struct {
	mu           sync.Mutex
	rows         map[uint32]Ownership
	loadCalls    int
	released     int64
	renewErr     error
	dropRenewals bool
	storageNow   time.Time
	// claims counts granted claims and claimedSlots records which slots they
	// granted, so a test can assert exactly what a caller asked the authority for.
	claims       int
	claimedSlots []uint32
	// renewRequests records, in order, what each renewal asked the authority to
	// refresh, so a test can pin the batching as well as the outcome.
	renewRequests []RenewRequest
}

func newLeaseOwnershipFake() *leaseOwnershipFake {
	return &leaseOwnershipFake{rows: make(map[uint32]Ownership)}
}

func (f *leaseOwnershipFake) LoadOwnership(
	_ context.Context,
	slots []uint32,
) ([]Ownership, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.loadCalls++
	rows := make([]Ownership, 0, len(slots))
	for _, slotID := range slots {
		row, ok := f.rows[slotID]
		if !ok {
			continue
		}
		if row.State == SlotOwned {
			row.GrantAgeKnown = true
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func (f *leaseOwnershipFake) ClaimSlots(
	_ context.Context,
	request ClaimRequest,
) ([]ClaimOutcome, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	outcomes := make([]ClaimOutcome, 0, len(request.Slots))
	for _, slotID := range request.Slots {
		row := f.rows[slotID]
		row.SlotID = slotID
		row.Epoch++
		row.State = SlotOwned
		row.OwnerNodeID = request.NodeID
		row.OwnerInstanceID = request.InstanceID
		f.rows[slotID] = row
		f.claims++
		f.claimedSlots = append(f.claimedSlots, slotID)
		outcomes = append(outcomes, ClaimOutcome{Ownership: row, Granted: true})
	}
	return outcomes, nil
}

// StorageClock reports the storage lease clock. A test that drives the clock
// monitor sets storageNow; otherwise it follows the process clock, which is what
// a real authority's clock does most of the time.
func (f *leaseOwnershipFake) StorageClock(context.Context) (time.Time, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.storageNow.IsZero() {
		return time.Now().UTC(), nil
	}
	return f.storageNow, nil
}

func (f *leaseOwnershipFake) RenewSlots(
	_ context.Context,
	request RenewRequest,
) ([]Ownership, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.renewRequests = append(f.renewRequests, request)
	if f.renewErr != nil {
		return nil, f.renewErr
	}
	if f.dropRenewals {
		return nil, nil
	}
	rows := make([]Ownership, 0, len(request.Groups))
	for _, group := range request.Groups {
		for _, slotID := range group.Slots {
			row, ok := f.rows[slotID]
			if !ok || row.Epoch != group.Epoch ||
				row.OwnerInstanceID != group.InstanceID {
				continue
			}
			rows = append(rows, row)
		}
	}
	return rows, nil
}

func (f *leaseOwnershipFake) ReleaseSlots(
	_ context.Context,
	targets []SlotAuthority,
) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var released int64
	for _, target := range targets {
		row, ok := f.rows[target.SlotID]
		if !ok || row.State != SlotOwned ||
			row.OwnerInstanceID != target.InstanceID || row.Epoch != target.Epoch {
			continue
		}
		row.State = SlotUnowned
		row.OwnerInstanceID = ""
		f.rows[target.SlotID] = row
		released++
	}
	f.released += released
	return released, nil
}

// leaseAllocator builds an allocator with an authority store and the slot for
// key already claimed and installed. monoNow is the test's clock.
func leaseAllocator(
	t *testing.T,
	key string,
	monoNow func() int64,
) (*Allocator, *leaseOwnershipFake) {
	t.Helper()
	return leaseAllocatorWith(t, key, newLeaseOwnershipFake(), monoNow)
}

// leaseAllocatorWith is leaseAllocator on an ownership store the caller already
// holds, for the tests that assert on what the store saw. A nil monoNow leaves
// the allocator's own monotonic clock in place.
func leaseAllocatorWith(
	t *testing.T,
	key string,
	ownership *leaseOwnershipFake,
	monoNow func() int64,
) (*Allocator, *leaseOwnershipFake) {
	t.Helper()
	plane := testDataPlaneConfig(AllocatorConfig{
		DefaultStep:     10,
		MaxStep:         100,
		IdleTimeout:     time.Hour,
		CleanupInterval: time.Minute,
	})
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
	if monoNow != nil {
		allocator.monoNow = monoNow
	}
	allocator.Open(1, 0, []uint32{SlotForKey(key)})
	allocator.ApplyRoute(0)
	require.Contains(t, allocator.slots, SlotForKey(key))
	return allocator, ownership
}

// TestLocalLeaseArmsTheDeadlineInsideTheStorageLease pins the arithmetic of
// section 3.4: the local deadline must sit a safety margin inside the lease, so
// that a deadline that has not passed implies the storage still grants the slot.
func TestTheDeadlineIsArmedInsideTheStorageLease(t *testing.T) {
	key := "lease-arming"
	var now int64
	allocator, _ := leaseAllocator(t, key, func() int64 { return now })

	slot := allocator.slots[SlotForKey(key)]
	require.EqualValues(t, 1, slot.epoch.Load())
	assert.Equal(
		t,
		int64(9*time.Second),
		slot.localDeadline.Load(),
		"deadline must be lease_duration - safety_margin after the arming reading",
	)
}

func TestAllocationIsRefusedOnceTheDeadlinePasses(t *testing.T) {
	key := "lease-expiry"
	var now int64
	allocator, _ := leaseAllocator(t, key, func() int64 { return now })

	first, err := allocator.FetchNext(context.Background(), key)
	require.NoError(t, err)
	assert.Equal(t, int64(1), first)

	now = int64(9 * time.Second)
	_, err = allocator.FetchNext(context.Background(), key)
	require.Error(t, err)
	assert.True(
		t,
		xerror.IsReason(err, reason.Reason_SEQUENCE_LEASE_EXPIRED),
		"want a lease-expired reason, got %v",
		err,
	)
	assert.Equal(t, int64(1), allocator.LeaseStats().LeaseExpired)
}

func TestRenewalExtendsTheLocalDeadline(t *testing.T) {
	key := "lease-renewal"
	var now int64
	allocator, _ := leaseAllocator(t, key, func() int64 { return now })

	_, err := allocator.FetchNext(context.Background(), key)
	require.NoError(t, err)

	// Renew one second inside the margin, then jump past the original deadline.
	now = int64(8 * time.Second)
	allocator.RenewLeases()
	require.Equal(t, int64(1), allocator.LeaseStats().RenewalSucceeded)

	now = int64(12 * time.Second)
	_, err = allocator.FetchNext(context.Background(), key)
	require.NoError(t, err, "the renewal must have moved the deadline past this instant")
}

func TestRenewalIsPacedByTheConfiguredInterval(t *testing.T) {
	key := "lease-renewal-pacing"
	var now int64
	allocator, _ := leaseAllocator(t, key, func() int64 { return now })

	now = int64(2 * time.Second)
	allocator.RenewLeases()
	assert.Zero(
		t,
		allocator.LeaseStats().RenewalSucceeded,
		"a renewal inside renew_interval must be skipped",
	)

	now = int64(3 * time.Second)
	allocator.RenewLeases()
	assert.Equal(t, int64(1), allocator.LeaseStats().RenewalSucceeded)
}

// TestRenewalIsGroupedByEpochAndSkipsDrainingSlots pins the batched contract:
// every slot this instance holds at the same epoch travels in one group, so the
// renewal costs one statement per ownership generation, and a slot that is
// draining is not renewed at all -- its authority is on its way out, and
// refreshing it would pin the slot to a node that is giving it up.
func TestRenewalIsGroupedByEpochAndSkipsDrainingSlots(t *testing.T) {
	key := "lease-renewal-grouping"
	var now int64
	allocator, ownership := leaseAllocator(t, key, func() int64 { return now })
	firstSlot := SlotForKey(key)

	// A second slot at a different epoch, as if it were taken over separately.
	secondSlotID := SlotForKey(key + "-second")
	require.NotEqual(t, firstSlot, secondSlotID)
	ownership.rows[secondSlotID] = Ownership{
		SlotID:          secondSlotID,
		State:           SlotOwned,
		OwnerInstanceID: allocator.instanceID,
		Epoch:           9,
	}
	second := &allocationSlot{}
	second.epoch.Store(9)
	allocator.slotsMu.Lock()
	allocator.slots[secondSlotID] = second
	allocator.slotsMu.Unlock()

	now = int64(4 * time.Second)
	allocator.RenewLeases()
	require.Len(t, ownership.renewRequests, 1)
	groups := ownership.renewRequests[0].Groups
	require.Len(t, groups, 2, "each ownership generation is one group")
	assert.Equal(t, allocator.instanceID, groups[0].InstanceID)
	assert.Equal(t, uint64(1), groups[0].Epoch)
	assert.Equal(t, []uint32{firstSlot}, groups[0].Slots)
	assert.Equal(t, uint64(9), groups[1].Epoch)
	assert.Equal(t, []uint32{secondSlotID}, groups[1].Slots)

	// Draining the second slot keeps it out of the next renewal, which now
	// carries a single group.
	second.draining.Store(true)
	now = int64(8 * time.Second)
	allocator.RenewLeases()
	require.Len(t, ownership.renewRequests, 2)
	groups = ownership.renewRequests[1].Groups
	require.Len(t, groups, 1)
	assert.Equal(t, []uint32{firstSlot}, groups[0].Slots)
}

// TestAFencedInstanceStopsRenewingItsGrants pins the half of the fence that lets
// the fleet take over, using a live instance on the same timeline as the control.
//
// Renewal is what keeps a grant inside the planner's freshness bound, so an
// instance that kept renewing would hold every slot it can no longer serve pinned
// to itself by the rule that leaves a live owner with a fresh grant alone. Nothing
// would ever reassign them, and losing the instance would be permanent instead of
// bounded by the quiet window.
func TestAFencedInstanceStopsRenewingItsGrants(t *testing.T) {
	key := "lease-fenced"
	var now int64
	clock := func() int64 { return now }
	healthy, _ := leaseAllocator(t, key, clock)
	fenced, _ := leaseAllocator(t, key, clock)
	for _, allocator := range []*Allocator{healthy, fenced} {
		_, err := allocator.FetchNext(context.Background(), key)
		require.NoError(t, err)
	}

	fenced.Fence("storage clock exceeded its asserted drift or jump bound")

	now = int64(3 * time.Second)
	healthy.RenewLeases()
	fenced.RenewLeases()

	assert.Equal(
		t,
		int64(1),
		healthy.LeaseStats().RenewalSucceeded,
		"the shared timeline must renew an instance that is still serving",
	)
	assert.Zero(
		t,
		fenced.LeaseStats().RenewalSucceeded,
		"a fenced instance must let its grants age out so the planner reassigns them",
	)
}

// TestFailedRenewalNeverExtendsTheDeadline is the "expire early, never late"
// rule: a renewal that cannot be confirmed must leave the deadline, so the
// lease lapses while the storage still believes it is held rather than the
// other way round.
func TestFailedRenewalNeverExtendsTheDeadline(t *testing.T) {
	key := "lease-renewal-failure"
	var now int64
	allocator, ownership := leaseAllocator(t, key, func() int64 { return now })

	_, err := allocator.FetchNext(context.Background(), key)
	require.NoError(t, err)

	ownership.renewErr = errors.New("authority store is unavailable")
	now = int64(8 * time.Second)
	allocator.RenewLeases()
	require.Equal(t, int64(1), allocator.LeaseStats().RenewalFailed)

	now = int64(9 * time.Second)
	_, err = allocator.FetchNext(context.Background(), key)
	require.Error(t, err)
	assert.True(t, xerror.IsReason(err, reason.Reason_SEQUENCE_LEASE_EXPIRED))
}

// TestRenewalThatDropsTheGrantFencesTheSlot covers the case where the store
// answers successfully but no longer lists the slot: the instance must stop
// serving it rather than keep allocating under a lease nobody honours.
func TestRenewalThatDropsTheGrantFencesTheSlot(t *testing.T) {
	key := "lease-grant-dropped"
	var now int64
	allocator, ownership := leaseAllocator(t, key, func() int64 { return now })

	_, err := allocator.FetchNext(context.Background(), key)
	require.NoError(t, err)

	ownership.dropRenewals = true
	now = int64(4 * time.Second)
	allocator.RenewLeases()

	stats := allocator.LeaseStats()
	require.Equal(t, int64(1), stats.GateFenced)
	require.Zero(t, stats.RenewalSucceeded)

	_, err = allocator.FetchNext(context.Background(), key)
	require.Error(t, err, "a fenced slot must not serve allocations")
}

// TestPauseBeyondTheBoundDiscardsTheAllocation is the section 3.4 self-check.
// The fast path advances the cursor and only then measures how long the
// linearisation took, so a stall longer than P_max is detected after the fact
// and the ID is discarded rather than returned.
func TestPauseBeyondTheBoundDiscardsTheAllocation(t *testing.T) {
	key := "lease-pause"
	var now int64
	allocator, _ := leaseAllocator(t, key, func() int64 { return now })

	// Warm the range so the next allocation takes the in-memory fast path.
	_, err := allocator.FetchNext(context.Background(), key)
	require.NoError(t, err)

	// Each reading now advances by more than P_max, which is what a process
	// pause of that length looks like from inside the linearisation.
	var ticks int64
	allocator.monoNow = func() int64 {
		ticks++
		return ticks * int64(2*time.Second)
	}

	_, err = allocator.FetchNext(context.Background(), key)
	require.Error(t, err)

	stats := allocator.LeaseStats()
	require.Equal(t, int64(1), stats.PauseViolations)
	require.Equal(t, int64(1), stats.GateFenced)

	_, err = allocator.FetchNext(context.Background(), key)
	require.Error(t, err, "a pause violation must close the slot")
}

// TestTheFastPathNeverReadsTheOwnershipTable is what makes the fast path fast:
// an allocation costs no ownership read at all. The gate and the lease carried
// on the reservation stand in for it, rather than a cheaper version of it.
func TestTheFastPathNeverReadsTheOwnershipTable(t *testing.T) {
	key := "lease-no-read"
	var now int64
	allocator, ownership := leaseAllocator(t, key, func() int64 { return now })

	for range 5 {
		_, err := allocator.FetchNext(context.Background(), key)
		require.NoError(t, err)
	}
	now = int64(5 * time.Second)
	allocator.RenewLeases()

	assert.Zero(
		t,
		ownership.loadCalls,
		"the allocation path must not read slot_ownership per batch",
	)
}

// TestRetryAfterAnswersOnlyTheRecoveringCase pins the appendix A.4 timing hint.
// A slot whose owner is recovering becomes serviceable once the next renewal
// confirms the grant, so the renewal cadence is the wait. Every other reason is
// either a directory problem the caller fixes by refreshing, or a permanent one,
// and a duration would be misleading for both.
func TestRetryAfterAnswersOnlyTheRecoveringCase(t *testing.T) {
	allocator, _ := leaseAllocator(t, "retry-after", func() int64 { return 0 })

	assert.Equal(
		t,
		3*time.Second,
		allocator.RetryAfter(reason.Reason_SEQUENCE_OWNER_RECOVERING),
	)
	assert.Zero(t, allocator.RetryAfter(reason.Reason_SEQUENCE_ROUTE_EXPIRED))
	assert.Zero(t, allocator.RetryAfter(reason.Reason_SEQUENCE_CAPACITY_EXHAUSTED))
}
