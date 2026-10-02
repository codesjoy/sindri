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

//go:build integration

package sequence_test

import (
	"context"
	"testing"
	"time"

	"github.com/codesjoy/sindri/internal/sequence/biz"
	sequencedata "github.com/codesjoy/sindri/internal/sequence/data"
	"github.com/stretchr/testify/require"
	gorm "gorm.io/gorm"
)

const (
	ownershipOwnerA = "instance-a"
	ownershipOwnerB = "instance-b"
)

func countRows(t *testing.T, db *gorm.DB, table string) int64 {
	t.Helper()
	var result struct {
		Total int64 `gorm:"column:total"`
	}
	require.NoError(t, db.Raw("SELECT COUNT(*) AS total FROM "+table).Scan(&result).Error)
	return result.Total
}

func loadSlotOwnership(
	t *testing.T,
	repo biz.OwnershipRepo,
	slotID uint32,
) biz.Ownership {
	t.Helper()
	rows, err := repo.LoadOwnership(context.Background(), []uint32{slotID})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	return rows[0]
}

// resetSlot forces a slot to UNOWNED and returns the ownership afterwards.
//
// The harness database is shared by every test in the process and the system
// tests claim slots all over the space, so a contract test cannot assume the
// seeded state. Claiming with a zero quiet window always succeeds and a release
// returns the slot to UNOWNED while preserving the epoch, which gives a known
// starting point without depending on which tests ran first.
func resetSlot(
	t *testing.T,
	repo biz.OwnershipRepo,
	slotID uint32,
) biz.Ownership {
	t.Helper()
	ctx := context.Background()
	outcomes, err := repo.ClaimSlots(ctx, biz.ClaimRequest{
		Slots:       []uint32{slotID},
		NodeID:      "node-reset",
		InstanceID:  "instance-reset",
		QuietWindow: 0,
	})
	require.NoError(t, err)
	require.Len(t, outcomes, 1)
	require.True(t, outcomes[0].Granted)

	released, err := repo.ReleaseSlots(ctx, []biz.SlotAuthority{{
		SlotID:     slotID,
		InstanceID: "instance-reset",
		Epoch:      outcomes[0].Ownership.Epoch,
	}})
	require.NoError(t, err)
	require.EqualValues(t, 1, released)

	current := loadSlotOwnership(t, repo, slotID)
	require.Equal(t, biz.SlotUnowned, current.State)
	return current
}

// TestSlotOwnershipContractAcrossDialects exercises the authority semantics that
// section 6 relies on: the quiet window gates a takeover, an epoch is never
// reused, renewal never resurrects a superseded lease, and a release only ever
// revokes the exact instance and epoch it names.
func TestSlotOwnershipContractAcrossDialects(t *testing.T) {
	for _, item := range harnesses {
		t.Run(item.name, func(t *testing.T) {
			db := openGORM(t, item)
			repo := sequencedata.NewOwnershipData(db)
			ctx := context.Background()

			// Every routing slot must exist as an authority row, or a slot could
			// never be claimed at all.
			require.EqualValues(t, biz.SlotCount, countRows(t, db, "slot_ownership"))

			const slotID uint32 = 7
			base := resetSlot(t, repo, slotID).Epoch

			claim := func(instance string, quietWindow time.Duration) []biz.ClaimOutcome {
				t.Helper()
				outcomes, err := repo.ClaimSlots(ctx, biz.ClaimRequest{
					Slots:       []uint32{slotID},
					NodeID:      "node-" + instance,
					InstanceID:  instance,
					QuietWindow: quietWindow,
				})
				require.NoError(t, err)
				require.Len(t, outcomes, 1)
				return outcomes
			}

			// An unowned slot is granted immediately and starts a new epoch.
			granted := claim(ownershipOwnerA, 0)[0]
			require.True(t, granted.Granted)
			require.Equal(t, biz.SlotOwned, granted.Ownership.State)
			require.Equal(t, base+1, granted.Ownership.Epoch)
			require.Equal(t, ownershipOwnerA, granted.Ownership.OwnerInstanceID)
			// The grant's age is what the quiet window is compared against, and it
			// is the storage clock that measures it: a fresh grant reads as
			// seconds old at most, never as an unknown age.
			fresh := loadSlotOwnership(t, repo, slotID)
			require.True(t, fresh.GrantAgeKnown)
			require.Less(t, fresh.GrantedAgo, time.Minute)
			require.GreaterOrEqual(t, fresh.GrantedAgo, time.Duration(0))

			// A fresh grant is inside the quiet window, so a takeover must be
			// refused and must report when it may be retried.
			refused := claim(ownershipOwnerB, time.Hour)[0]
			require.False(t, refused.Granted)
			require.Equal(t, base+1, refused.Ownership.Epoch)
			require.Equal(t, ownershipOwnerA, refused.Ownership.OwnerInstanceID)
			require.WithinDuration(
				t,
				time.Now().Add(time.Hour),
				refused.NotBefore,
				time.Second,
			)

			// Once the quiet window has elapsed the takeover succeeds and the
			// epoch moves forward.
			taken := claim(ownershipOwnerB, 0)[0]
			require.True(t, taken.Granted)
			require.Equal(t, base+2, taken.Ownership.Epoch)
			require.Equal(t, ownershipOwnerB, taken.Ownership.OwnerInstanceID)

			// A stale authority must not renew: that would let a superseded
			// owner keep a lease it no longer holds. The observable effect is
			// that the grant keeps ageing instead of being reset.
			before := loadSlotOwnership(t, repo, slotID).GrantedAgo
			time.Sleep(50 * time.Millisecond)
			staleRenew, err := repo.RenewSlots(ctx, biz.RenewRequest{
				Groups: []biz.RenewGroup{{
					InstanceID: ownershipOwnerA, Epoch: base + 1,
					Slots: []uint32{slotID},
				}},
			})
			require.NoError(t, err)
			require.Empty(t, staleRenew)
			require.Greater(t, loadSlotOwnership(t, repo, slotID).GrantedAgo, before)

			// The live authority renews and the age resets. The comparison is
			// against the age read just before the renewal, not against the one
			// read before the sleep: a fresh grant measures the round trip that
			// created it, so two ages read milliseconds apart are two round trips
			// compared, and the newer one can be the larger of the two.
			beforeRenew := loadSlotOwnership(t, repo, slotID).GrantedAgo
			renewed, err := repo.RenewSlots(ctx, biz.RenewRequest{
				Groups: []biz.RenewGroup{{
					InstanceID: ownershipOwnerB, Epoch: base + 2,
					Slots: []uint32{slotID},
				}},
			})
			require.NoError(t, err)
			require.Len(t, renewed, 1)
			require.Less(t, loadSlotOwnership(t, repo, slotID).GrantedAgo, beforeRenew)

			// A release whose instance predicate does not match must revoke
			// nothing. Writing UNOWNED unconditionally here would silently steal
			// the slot from its real owner.
			released, err := repo.ReleaseSlots(ctx, []biz.SlotAuthority{{
				SlotID: slotID, InstanceID: ownershipOwnerA, Epoch: base + 2,
			}})
			require.NoError(t, err)
			require.Zero(t, released)
			require.Equal(t, biz.SlotOwned, loadSlotOwnership(t, repo, slotID).State)

			// Nor may a stale epoch revoke the current holder.
			released, err = repo.ReleaseSlots(ctx, []biz.SlotAuthority{{
				SlotID: slotID, InstanceID: ownershipOwnerB, Epoch: base + 1,
			}})
			require.NoError(t, err)
			require.Zero(t, released)

			// The exact predicate releases, and the epoch is preserved so it can
			// never be handed out again.
			released, err = repo.ReleaseSlots(ctx, []biz.SlotAuthority{{
				SlotID: slotID, InstanceID: ownershipOwnerB, Epoch: base + 2,
			}})
			require.NoError(t, err)
			require.EqualValues(t, 1, released)
			afterRelease := loadSlotOwnership(t, repo, slotID)
			require.Equal(t, biz.SlotUnowned, afterRelease.State)
			require.Equal(t, base+2, afterRelease.Epoch)

			// Re-acquiring continues the epoch sequence rather than restarting it.
			reacquired := claim(ownershipOwnerA, time.Hour)[0]
			require.True(t, reacquired.Granted)
			require.Equal(t, base+3, reacquired.Ownership.Epoch)

			// The authority row is the record of every change: it names the new
			// generation, which is what a reader of the fleet acts on now that no
			// outbox mirrors it.
			current := loadSlotOwnership(t, repo, slotID)
			require.Equal(t, biz.SlotOwned, current.State)
			require.Equal(t, ownershipOwnerA, current.OwnerInstanceID)
			require.Equal(t, base+3, current.Epoch)
		})
	}
}

// TestReservationOwnershipFenceBlocksTakeover is the section 2.1 write-skew
// test. A reservation validates ownership and advances the high watermark in one
// transaction; if a takeover could commit in between, both would succeed and the
// reservation would run under a superseded epoch.
//
// The reservation holds a shared lock on the ownership row while it runs, so a
// takeover on another connection must block until it ends. That blocking is what
// makes the fence real rather than nominal, and a plain snapshot read would not
// provide it.
func TestReservationOwnershipFenceBlocksTakeover(t *testing.T) {
	for _, item := range harnesses {
		t.Run(item.name, func(t *testing.T) {
			db := openGORM(t, item)
			repo := sequencedata.NewOwnershipData(db)
			ctx := context.Background()

			const slotID uint32 = 11
			base := resetSlot(t, repo, slotID).Epoch
			outcomes, err := repo.ClaimSlots(ctx, biz.ClaimRequest{
				Slots:       []uint32{slotID},
				NodeID:      "node-a",
				InstanceID:  ownershipOwnerA,
				QuietWindow: 0,
			})
			require.NoError(t, err)
			require.True(t, outcomes[0].Granted)
			require.Equal(t, base+1, outcomes[0].Ownership.Epoch)

			reservation, err := db.DB()
			require.NoError(t, err)
			conn, err := reservation.Conn(ctx)
			require.NoError(t, err)
			defer func() { _ = conn.Close() }()

			tx, err := conn.BeginTx(ctx, nil)
			require.NoError(t, err)
			defer func() { _ = tx.Rollback() }()

			// Take the same shared lock the reservation transaction takes.
			placeholder := "?"
			if item.driver == "postgres" {
				placeholder = "$1"
			}
			var locked struct {
				SlotID uint32 `gorm:"column:slot_id"`
			}
			require.NoError(t, tx.QueryRowContext(
				ctx,
				"SELECT slot_id FROM slot_ownership WHERE slot_id = "+placeholder+" FOR SHARE",
				slotID,
			).Scan(&locked.SlotID))
			require.Equal(t, slotID, locked.SlotID)

			// A takeover with no quiet window would otherwise be granted at once,
			// so anything that stops it here is the ownership lock.
			claimDone := make(chan error, 1)
			go func() {
				_, claimErr := repo.ClaimSlots(ctx, biz.ClaimRequest{
					Slots:       []uint32{slotID},
					NodeID:      "node-b",
					InstanceID:  ownershipOwnerB,
					QuietWindow: 0,
				})
				claimDone <- claimErr
			}()

			select {
			case claimErr := <-claimDone:
				t.Fatalf(
					"takeover committed while a reservation held the ownership lock: %v",
					claimErr,
				)
			case <-time.After(300 * time.Millisecond):
			}

			require.NoError(t, tx.Rollback())
			select {
			case claimErr := <-claimDone:
				require.NoError(t, claimErr)
			case <-time.After(10 * time.Second):
				t.Fatal("takeover stayed blocked after the reservation released the lock")
			}

			require.Equal(t, base+2, loadSlotOwnership(t, repo, slotID).Epoch)
		})
	}
}

// TestSlotOwnershipClaimIsIdempotentForItsOwnInstance is the regression test for
// the claim livelock.
//
// A node claims the authority for a new route in batches. Even with retained
// confirmed progress, uncertain replies can require claiming the same slots
// again. The rows an attempt already took must therefore be
// grantable by the attempt that follows it: they belong to the same claimant, and
// the quiet window exists to protect a *previous* owner which may still be
// allocating, not the claimant itself. Refusing them instead made a claim that
// spanned more than one batch unable to ever commit -- each retry refused what
// the attempt before it had taken -- and the route it carried never became
// servable.
func TestSlotOwnershipClaimIsIdempotentForItsOwnInstance(t *testing.T) {
	for _, item := range harnesses {
		t.Run(item.name, func(t *testing.T) {
			db := openGORM(t, item)
			repo := sequencedata.NewOwnershipData(db)
			ctx := context.Background()

			// One slot this instance will hold and one another instance will hold,
			// so a single claim can mix the two cases the predicate has to decide
			// differently.
			const mineSlot uint32 = 13
			const otherSlot uint32 = 15
			mineBase := resetSlot(t, repo, mineSlot).Epoch
			otherBase := resetSlot(t, repo, otherSlot).Epoch

			claim := func(
				instance string,
				quietWindow time.Duration,
				slots ...uint32,
			) []biz.ClaimOutcome {
				t.Helper()
				outcomes, err := repo.ClaimSlots(ctx, biz.ClaimRequest{
					Slots:       slots,
					NodeID:      "node-" + instance,
					InstanceID:  instance,
					QuietWindow: quietWindow,
				})
				require.NoError(t, err)
				require.Len(t, outcomes, len(slots))
				return outcomes
			}

			mine := claim(ownershipOwnerA, time.Hour, mineSlot)[0]
			require.True(t, mine.Granted)
			require.Equal(t, mineBase+1, mine.Ownership.Epoch)
			other := claim(ownershipOwnerB, time.Hour, otherSlot)[0]
			require.True(t, other.Granted)
			require.Equal(t, otherBase+1, other.Ownership.Epoch)

			// A claim mixing a slot the claimant holds with one it does not grants
			// the first and refuses the second.
			mixed := claim(ownershipOwnerA, time.Hour, mineSlot, otherSlot)
			require.True(
				t,
				mixed[0].Granted,
				"a slot the claimant already holds is not a takeover",
			)
			require.Equal(t, ownershipOwnerA, mixed[0].Ownership.OwnerInstanceID)
			require.Greater(
				t,
				mixed[0].Ownership.Epoch,
				mine.Ownership.Epoch,
				"a re-claim must carry a fresh epoch rather than hand back the old one",
			)
			require.False(
				t,
				mixed[1].Granted,
				"another instance's fresh grant still blocks a takeover",
			)
			require.Equal(t, ownershipOwnerB, mixed[1].Ownership.OwnerInstanceID)

			// The retry that follows such a partial grant has to make progress,
			// which for the slot it already holds means granting it again. A
			// refusal here is the livelock: every attempt would undo the
			// completeness the one before it built.
			retry := claim(ownershipOwnerA, time.Hour, mineSlot, otherSlot)
			require.True(t, retry[0].Granted)
			require.False(t, retry[1].Granted)
			// And the refusal still says when the takeover may be retried, which
			// is what makes the caller come back at all.
			require.WithinDuration(
				t,
				time.Now().Add(time.Hour),
				retry[1].NotBefore,
				time.Second,
			)

			// Once the other instance's grant has aged out the same claim
			// completes, and the route it carries becomes servable.
			completed := claim(ownershipOwnerA, 0, mineSlot, otherSlot)
			require.True(t, completed[0].Granted)
			require.True(t, completed[1].Granted)
			require.Equal(
				t,
				ownershipOwnerA,
				loadSlotOwnership(t, repo, otherSlot).OwnerInstanceID,
			)
		})
	}
}

func TestExpiredReservationNeverAdvancesWatermarks(t *testing.T) {
	for _, item := range harnesses {
		t.Run(item.name, func(t *testing.T) {
			db := openGORM(t, item)
			ownership := sequencedata.NewOwnershipData(db)
			store := sequencedata.NewSequenceData(db)
			ctx := context.Background()
			keys := []string{"lease-contract-live", "lease-contract-expired"}
			require.NotEqual(t, biz.SlotForKey(keys[0]), biz.SlotForKey(keys[1]))
			authority := claimOwnedSlots(t, db, "reservation-lease-contract", keys...)
			authority.Lease = 10 * time.Minute
			authority.Epochs = make(map[uint32]uint64)
			for _, key := range keys {
				slot := biz.SlotForKey(key)
				authority.Epochs[slot] = loadSlotOwnership(t, ownership, slot).Epoch
				_, err := reserveRange(ctx, store, authority, key, 5)
				require.NoError(t, err)
			}
			clock, err := ownership.StorageClock(ctx)
			require.NoError(t, err)
			require.NoError(t, db.Table("slot_ownership").
				Where("slot_id = ?", biz.SlotForKey(keys[1])).
				Update("granted_at", clock.Add(-time.Hour)).Error)
			loadEnds := func() []int64 {
				t.Helper()
				ends := make([]int64, len(keys))
				for index, key := range keys {
					var model sequencedata.SequenceModel
					require.NoError(
						t,
						db.Where("namespace = ? AND sequence_key = ?", "", key).First(&model).Error,
					)
					ends[index] = model.ReservedEnd
				}
				return ends
			}
			before := loadEnds()
			_, err = reserveRange(ctx, store, authority, keys[1], 10)
			require.ErrorIs(t, err, biz.ErrLeaseExpired)
			require.Equal(t, before, loadEnds())
			_, err = store.ReserveRanges(ctx, authority, []biz.ReservationRequest{
				{Key: keys[0], Step: 10}, {Key: keys[1], Step: 10},
			})
			require.ErrorIs(t, err, biz.ErrLeaseExpired)
			require.Equal(
				t,
				before,
				loadEnds(),
				"one expired slot must reject the entire reservation",
			)
		})
	}
}
