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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/codesjoy/pkg/basic/xerror"
	"github.com/codesjoy/sindri/gen/go/sequence/reason"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type reservationProbe func(context.Context, ReservationAuthority, []ReservationRequest) ([]SequenceRange, error)

func (f reservationProbe) ReserveRanges(
	ctx context.Context,
	authority ReservationAuthority,
	requests []ReservationRequest,
) ([]SequenceRange, error) {
	return f(ctx, authority, requests)
}

type ownershipProbe struct {
	OwnershipRepo
	claim   func(context.Context, ClaimRequest) ([]ClaimOutcome, error)
	renew   func(context.Context, RenewRequest) ([]Ownership, error)
	release func(context.Context, []SlotAuthority) (int64, error)
}

func (p ownershipProbe) ClaimSlots(
	ctx context.Context,
	request ClaimRequest,
) ([]ClaimOutcome, error) {
	if p.claim != nil {
		return p.claim(ctx, request)
	}
	return p.OwnershipRepo.ClaimSlots(ctx, request)
}

func (p ownershipProbe) RenewSlots(ctx context.Context, request RenewRequest) ([]Ownership, error) {
	if p.renew != nil {
		return p.renew(ctx, request)
	}
	return p.OwnershipRepo.RenewSlots(ctx, request)
}

func (p ownershipProbe) ReleaseSlots(ctx context.Context, targets []SlotAuthority) (int64, error) {
	if p.release != nil {
		return p.release(ctx, targets)
	}
	return p.OwnershipRepo.ReleaseSlots(ctx, targets)
}

func TestBatchReservationsCarryLeaseAndStatementDeadline(t *testing.T) {
	key := "authority"
	allocator, _ := leaseAllocator(t, key, func() int64 { return 0 })
	store := allocator.store
	allocator.store = reservationProbe(
		func(ctx context.Context, authority ReservationAuthority, requests []ReservationRequest) ([]SequenceRange, error) {
			assert.Equal(t, allocator.InstanceID(), authority.InstanceID)
			assert.Equal(t, allocator.ha.LeaseDuration, authority.Lease)
			assert.Equal(t, uint64(1), authority.Epochs[SlotForKey(key)])
			deadline, ok := ctx.Deadline()
			require.True(t, ok)
			assert.LessOrEqual(t, time.Until(deadline), allocator.storeTimeout())
			return store.ReserveRanges(ctx, authority, requests)
		},
	)
	_, err := allocator.FetchNextN(context.Background(), key, 2)
	require.NoError(t, err)
}

func TestColdReservationWaitCannotServeAnExpiredWarmKey(t *testing.T) {
	keys := distinctSlotKeys(2)
	var mono int64
	allocator, _ := leaseAllocator(t, keys[0], func() int64 { return mono })
	allocator.Open(2, 0, []uint32{SlotForKey(keys[0]), SlotForKey(keys[1])})
	allocator.ApplyRoute(0)
	_, err := allocator.FetchNextN(context.Background(), keys[0], 1)
	require.NoError(t, err)
	store := allocator.store
	allocator.store = reservationProbe(
		func(ctx context.Context, authority ReservationAuthority, requests []ReservationRequest) ([]SequenceRange, error) {
			reserved, reserveErr := store.ReserveRanges(ctx, authority, requests)
			mono = int64(10 * time.Second)
			return reserved, reserveErr
		},
	)
	results, err := allocator.FetchNextBatch(
		context.Background(),
		[]SequenceRequest{{Key: keys[0]}, {Key: keys[1]}},
	)
	require.Error(t, err)
	assert.Nil(t, results)
	assert.True(t, xerror.IsReason(err, reason.Reason_SEQUENCE_LEASE_EXPIRED))
}

func TestFrontReservationHonorsTheShorterDeadline(t *testing.T) {
	allocator, _ := leaseAllocator(t, "timeout", func() int64 { return 0 })
	allocator.cfg.ReserveTimeout = 10 * time.Millisecond
	allocator.store = reservationProbe(
		func(ctx context.Context, _ ReservationAuthority, _ []ReservationRequest) ([]SequenceRange, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	)
	_, err := allocator.FetchNextN(context.Background(), "timeout", 1)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = allocator.FetchNextN(ctx, "timeout", 1)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestDelayedRenewalUsesTheSendingAnchor(t *testing.T) {
	var mono int64
	allocator, ownership := leaseAllocator(t, "renew-anchor", func() int64 { return mono })
	allocator.ownership = ownershipProbe{
		OwnershipRepo: ownership,
		renew: func(ctx context.Context, req RenewRequest) ([]Ownership, error) {
			rows, err := ownership.RenewSlots(ctx, req)
			mono += int64(2 * time.Second)
			return rows, err
		},
	}
	mono = int64(4 * time.Second)
	allocator.RenewLeases()
	assert.Equal(
		t,
		int64(13*time.Second),
		allocator.slots[SlotForKey("renew-anchor")].localDeadline.Load(),
	)
}

func TestExpiredRenewalNeverExtendsTrust(t *testing.T) {
	var mono int64
	allocator, ownership := leaseAllocator(t, "expired-renewal", func() int64 { return mono })
	slot := allocator.slots[SlotForKey("expired-renewal")]
	allocator.ownership = ownershipProbe{
		OwnershipRepo: ownership,
		renew: func(ctx context.Context, req RenewRequest) ([]Ownership, error) {
			rows, err := ownership.RenewSlots(ctx, req)
			mono += int64(20 * time.Second)
			return rows, err
		},
	}
	mono = int64(4 * time.Second)
	allocator.RenewLeases()
	assert.Zero(t, slot.localDeadline.Load(), "an expired grant must drop local trust entirely")
	assert.True(t, slot.draining.Load())
}

func TestDelayedClaimUsesTheSendingAnchor(t *testing.T) {
	var mono int64
	allocator, ownership := leaseAllocator(t, "claim-anchor", func() int64 { return mono })
	allocator.ownership = ownershipProbe{
		OwnershipRepo: ownership,
		claim: func(ctx context.Context, req ClaimRequest) ([]ClaimOutcome, error) {
			rows, err := ownership.ClaimSlots(ctx, req)
			mono += int64(2 * time.Second)
			return rows, err
		},
	}
	allocator.slots[SlotForKey("claim-anchor")].draining.Store(true)
	allocator.Reconcile(1, 0, []uint32{SlotForKey("claim-anchor")}, false)
	allocator.ApplyRoute(0)
	assert.Equal(
		t,
		int64(9*time.Second),
		allocator.slots[SlotForKey("claim-anchor")].localDeadline.Load(),
	)
}

func TestReclaimNeverInheritsThePreviousEpochRange(t *testing.T) {
	allocator, _ := leaseAllocator(t, "reclaim-range", func() int64 { return 0 })
	first, err := allocator.FetchNextN(context.Background(), "reclaim-range", 1)
	require.NoError(t, err)
	old := allocator.slots[SlotForKey("reclaim-range")]
	old.draining.Store(true)
	allocator.Reconcile(1, 0, []uint32{SlotForKey("reclaim-range")}, false)
	allocator.ApplyRoute(0)
	require.NotSame(t, old, allocator.slots[SlotForKey("reclaim-range")])
	second, err := allocator.FetchNextN(context.Background(), "reclaim-range", 1)
	require.NoError(t, err)
	assert.Greater(t, second.ID, first.ID+9)
	assert.Greater(t, second.SlotEpoch, first.SlotEpoch)
}

func TestClaimProgressSurvivesAPassTimeout(t *testing.T) {
	allocator, ownership := leaseAllocator(t, "claim-progress", func() int64 { return 0 })
	allocator.cfg.ReserveTimeout = 10 * time.Millisecond
	calls := 0
	allocator.ownership = ownershipProbe{
		OwnershipRepo: ownership,
		claim: func(ctx context.Context, req ClaimRequest) ([]ClaimOutcome, error) {
			calls++
			if calls == 2 {
				<-ctx.Done()
				return nil, ctx.Err()
			}
			return ownership.ClaimSlots(ctx, req)
		},
	}
	slots := make([]uint32, ownershipBatchSize+1)
	for index := range slots {
		slots[index] = uint32(index)
	}
	allocator.Open(2, 0, slots)
	allocator.ApplyRoute(0)
	require.Equal(t, int64(1), allocator.CurrentVersion())
	require.Len(t, allocator.prepareApply.Slots, 1)
	firstEpoch := allocator.slots[0].epoch.Load()
	allocator.Reconcile(2, 0, slots, false)
	allocator.claimRetryAfter.Store(0)
	allocator.ApplyRoute(0)
	assert.Equal(t, int64(2), allocator.CurrentVersion())
	assert.Equal(t, firstEpoch, allocator.slots[0].epoch.Load())
}

func TestClaimAndCleanupShareTheApplyBudget(t *testing.T) {
	keys := distinctSlotKeys(2)
	var mono int64
	allocator, ownership := leaseAllocator(t, keys[0], func() int64 { return mono })
	var claimDeadline time.Time
	allocator.ownership = ownershipProbe{
		OwnershipRepo: ownership,
		claim: func(ctx context.Context, req ClaimRequest) ([]ClaimOutcome, error) {
			claimDeadline, _ = ctx.Deadline()
			rows, err := ownership.ClaimSlots(ctx, req)
			mono = int64(20 * time.Second)
			return rows, err
		},
		release: func(ctx context.Context, targets []SlotAuthority) (int64, error) {
			deadline, _ := ctx.Deadline()
			assert.Equal(t, claimDeadline, deadline, "cleanup must not open a fresh pass budget")
			return ownership.ReleaseSlots(ctx, targets)
		},
	}
	allocator.CommitRoute(2, 0, []uint32{SlotForKey(keys[0]), SlotForKey(keys[1])})
	allocator.ApplyRoute(0)
	assert.NotContains(t, allocator.slots, SlotForKey(keys[1]))
}

func TestUninstallableClaimsAreReleased(t *testing.T) {
	for _, event := range []string{"expiry", "replan", "fence", "shutdown"} {
		t.Run(event, func(t *testing.T) {
			keys := distinctSlotKeys(2)
			var mono int64
			allocator, ownership := leaseAllocator(t, keys[0], func() int64 { return mono })
			slot := SlotForKey(keys[1])
			allocator.ownership = ownershipProbe{
				OwnershipRepo: ownership,
				claim: func(ctx context.Context, req ClaimRequest) ([]ClaimOutcome, error) {
					rows, err := ownership.ClaimSlots(ctx, req)
					switch event {
					case "expiry":
						mono = int64(20 * time.Second)
					case "replan":
						allocator.Reconcile(3, 0, []uint32{SlotForKey(keys[0])}, false)
					case "fence":
						allocator.Fence("claim returned after a clock violation")
					case "shutdown":
						allocator.Shutdown()
					}
					return rows, err
				},
			}
			allocator.Open(2, 0, []uint32{SlotForKey(keys[0]), slot})
			allocator.ApplyRoute(0)
			assert.NotContains(t, allocator.slots, slot)
			assert.Equal(t, int64(1), allocator.CurrentVersion())
			ownership.mu.Lock()
			assert.Empty(t, ownership.rows[slot].OwnerInstanceID)
			ownership.mu.Unlock()
		})
	}
}

func TestCursorPauseDiscardsConsumedIDs(t *testing.T) {
	var mono int64
	allocator, _ := leaseAllocator(t, "cursor-pause", func() int64 { return mono })
	_, err := allocator.FetchNextN(context.Background(), "cursor-pause", 1)
	require.NoError(t, err)
	slot := allocator.slots[SlotForKey("cursor-pause")]
	state, ok := slot.Load("cursor-pause")
	require.True(t, ok)
	started, err := state.beginLinearization()
	require.NoError(t, err)
	consumed := state.next.Add(1)
	mono += int64(allocator.ha.MaxPause + time.Nanosecond)
	require.Error(t, state.checkLinearizationBound(started))
	assert.Equal(t, consumed, state.next.Load(), "a rejected ID must never be rolled back")
	assert.True(t, slot.draining.Load())
}

func TestExhaustedCursorCannotResetThePauseObservation(t *testing.T) {
	key := "exhausted-cursor-pause"
	allocator, _ := leaseAllocator(t, key, func() int64 { return 0 })
	_, err := allocator.FetchNext(context.Background(), key)
	require.NoError(t, err)
	slot := allocator.slots[SlotForKey(key)]
	state, _ := slot.Load(key)
	state.next.Store(state.end.Load())
	state.standby = &SequenceRange{Start: 100, End: 110}
	activeEnd := state.end.Load()
	var mono int64
	allocator.monoNow = func() int64 {
		if state.next.Load() > activeEnd {
			mono = int64(allocator.ha.MaxPause + time.Nanosecond)
		}
		return mono
	}
	id, err := allocator.FetchNext(context.Background(), key)
	require.Error(t, err)
	assert.Zero(t, id)
	assert.True(t, slot.draining.Load())
	assert.Equal(t, int64(1), allocator.LeaseStats().PauseViolations)
	assert.Less(
		t,
		state.next.Load(),
		int64(100),
		"standby cannot conceal a pause on the exhausted cursor",
	)
}

func TestLockWaitAndStandbyDoNotBypassTheLease(t *testing.T) {
	for _, event := range []string{"expiry", "takeover"} {
		for _, count := range []uint32{1, 2} {
			t.Run(fmt.Sprintf("%s/count=%d", event, count), func(t *testing.T) {
				var mono atomic.Int64
				key := "standby-gate"
				allocator, _ := leaseAllocator(t, key, mono.Load)
				_, err := allocator.FetchNextN(context.Background(), key, 1)
				require.NoError(t, err)
				slot := allocator.slots[SlotForKey(key)]
				state, _ := slot.Load(key)
				state.mu.Lock()
				state.next.Store(state.end.Load())
				state.standby = &SequenceRange{Start: 100, End: 110}
				checked := make(chan struct{})
				clockReads := 0
				allocator.now = func() time.Time {
					clockReads++
					if clockReads == 2 {
						close(checked)
					}
					return time.Now()
				}
				finished := make(chan error, 1)
				go func() {
					_, callErr := allocator.FetchNextN(context.Background(), key, count)
					finished <- callErr
				}()
				<-checked // the request passed the initial gate, then waits on state.mu
				if event == "expiry" {
					mono.Store(int64(20 * time.Second))
				} else {
					slot.draining.Store(true)
				}
				state.mu.Unlock()
				require.Error(t, <-finished)
				assert.Less(t, state.next.Load(), int64(100), "expired standby must not activate")
			})
		}
	}
}

type prefetchWaitContext struct {
	context.Context
	seen chan struct{}
	once sync.Once
}

func (ctx *prefetchWaitContext) Done() <-chan struct{} {
	ctx.once.Do(func() { close(ctx.seen) })
	return ctx.Context.Done()
}

func TestPrefetchWaitReentersTheAllocationFence(t *testing.T) {
	for _, event := range []string{"expiry", "takeover"} {
		t.Run(event, func(t *testing.T) {
			var mono atomic.Int64
			key := "prefetch-gate"
			allocator, _ := leaseAllocator(t, key, mono.Load)
			_, err := allocator.FetchNextN(context.Background(), key, 1)
			require.NoError(t, err)
			slot := allocator.slots[SlotForKey(key)]
			state, _ := slot.Load(key)
			fetch := &rangeFetch{done: make(chan struct{}), background: true}
			state.mu.Lock()
			state.next.Store(state.end.Load())
			state.fetch = fetch
			state.mu.Unlock()
			ctx := &prefetchWaitContext{Context: context.Background(), seen: make(chan struct{})}
			finished := make(chan error, 1)
			go func() {
				_, callErr := allocator.FetchNextN(ctx, key, 1)
				finished <- callErr
			}()
			<-ctx.seen // after the slow-path gate, at the prefetch select
			if event == "expiry" {
				mono.Store(int64(20 * time.Second))
			} else {
				slot.draining.Store(true)
			}
			state.completeFetch(
				fetch,
				SequenceRange{Start: 100, End: 110},
				nil,
				allocator.cfg,
				allocator.now(),
				key,
			)
			require.Error(t, <-finished)
			assert.Less(t, state.next.Load(), int64(100))
		})
	}
}

func BenchmarkAllocatorFetchNextNHotKey(b *testing.B) {
	allocator := readyAllocatorForKeys(b, "rpc-hot")
	slot := allocator.slots[SlotForKey("rpc-hot")]
	allocator.ownership = newLeaseOwnershipFake()
	slot.localDeadline.Store(math.MaxInt64)
	state := &keyState{allocator: allocator, slot: slot, activeStep: math.MaxInt64}
	state.initialized.Store(true)
	state.start.Store(1)
	state.next.Store(1)
	state.end.Store(math.MaxInt64)
	slot.Store("rpc-hot", state)
	allocator.store = reservationProbe(
		func(context.Context, ReservationAuthority, []ReservationRequest) ([]SequenceRange, error) {
			return nil, errors.New("unexpected storage reservation on the hot path")
		},
	)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := allocator.FetchNextN(context.Background(), "rpc-hot", 1); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAllocatorFetchNextBatchHotKeys(b *testing.B) {
	keys := distinctSlotKeys(8)
	allocator := readyAllocatorForKeys(b, keys...)
	allocator.ownership = newLeaseOwnershipFake()
	requests := make([]SequenceRequest, len(keys))
	for index, key := range keys {
		slot := allocator.slots[SlotForKey(key)]
		slot.localDeadline.Store(math.MaxInt64)
		state := &keyState{allocator: allocator, slot: slot, activeStep: math.MaxInt64}
		state.initialized.Store(true)
		state.start.Store(1)
		state.next.Store(1)
		state.end.Store(math.MaxInt64)
		slot.Store(key, state)
		requests[index] = SequenceRequest{Key: key, Count: uint32(index + 1)}
	}
	allocator.store = reservationProbe(
		func(context.Context, ReservationAuthority, []ReservationRequest) ([]SequenceRange, error) {
			return nil, errors.New("unexpected storage reservation on the hot path")
		},
	)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := allocator.FetchNextBatch(context.Background(), requests); err != nil {
			b.Fatal(err)
		}
	}
}
