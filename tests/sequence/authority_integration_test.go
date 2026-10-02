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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	"github.com/codesjoy/sindri/internal/sequence/biz"
	sequencedata "github.com/codesjoy/sindri/internal/sequence/data"
)

// grantInstance registers a fresh instance and claims the named slots for it,
// returning the authority snapshot the allocator would cache. The lease is long
// enough that a contract test never races its own expiry; the freshness checks
// the production paths make are exercised through explicit mutations instead.
func grantInstance(
	t *testing.T,
	db *gorm.DB,
	node, instance string,
	slots ...uint32,
) biz.AuthoritySnapshot {
	t.Helper()
	ctx := context.Background()
	repo := sequencedata.NewOwnershipData(db)
	require.NoError(t, repo.RegisterInstance(ctx, node, instance))
	before, err := repo.InstanceAuthority(ctx, instance)
	require.NoError(t, err)
	outcomes, err := repo.ClaimSlots(ctx, biz.ClaimRequest{
		Slots:       slots,
		NodeID:      node,
		InstanceID:  instance,
		Revision:    before.Lease.Revision,
		Lease:       time.Hour,
		QuietWindow: time.Hour,
	})
	require.NoError(t, err)
	for _, outcome := range outcomes {
		require.True(
			t,
			outcome.Granted,
			"slot %d must be granted to %s",
			outcome.Ownership.SlotID,
			instance,
		)
	}
	after, err := repo.InstanceAuthority(ctx, instance)
	require.NoError(t, err)
	return after
}

type slotOwnershipRow struct {
	SlotID          uint32    `gorm:"column:slot_id"`
	OwnerInstanceID *string   `gorm:"column:owner_instance_id"`
	Epoch           uint64    `gorm:"column:epoch"`
	State           string    `gorm:"column:state"`
	UpdatedAt       time.Time `gorm:"column:updated_at"`
}

func readSlotOwnership(t *testing.T, db *gorm.DB, slots ...uint32) []slotOwnershipRow {
	t.Helper()
	var rows []slotOwnershipRow
	require.NoError(t, db.Table("sequence_slot_ownership").
		Select("slot_id, owner_instance_id, epoch, state, updated_at").
		Where("slot_id IN ?", slots).
		Order("slot_id").
		Find(&rows).
		Error)
	return rows
}

// TestInstanceLeaseAndTakeoverFencingAcrossDialects pins the instance-level
// lease contract on a real server: a slot whose owner is still inside the quiet
// window cannot be taken over, and a lawful takeover under the storage clock
// advances the epoch by exactly one. Renewal and revision fencing are pinned by
// the sqlite ownership contract, where the storage clock is not the variable
// under test.
func TestInstanceLeaseAndTakeoverFencingAcrossDialects(t *testing.T) {
	forEachDialect(t, func(t *testing.T, item *harness) {
		require.NoError(t, resetSequenceDatabase(t, item))
		db := openGORM(t, item)
		ctx := context.Background()
		repo := sequencedata.NewOwnershipData(db)

		source := grantInstance(t, db, "node-a", "ia", 1)
		target := grantInstance(t, db, "node-b", "ib")

		outcomes, err := repo.ClaimSlots(ctx, biz.ClaimRequest{
			Slots:       []uint32{1},
			NodeID:      "node-b",
			InstanceID:  "ib",
			Revision:    target.Lease.Revision,
			Lease:       time.Hour,
			QuietWindow: time.Hour,
		})
		require.NoError(t, err)
		require.Len(t, outcomes, 1)
		assert.False(t, outcomes[0].Granted,
			"a slot whose owner is inside the quiet window must not be taken over")

		// Age the source's grant past the quiet window; the slot is then a
		// lawful takeover and its epoch must advance by exactly one.
		require.NoError(t, db.Exec(
			"UPDATE sequence_instance_leases SET granted_at = ? WHERE instance_id = 'ia'",
			time.Now().Add(-2*time.Hour),
		).Error)
		outcomes, err = repo.ClaimSlots(ctx, biz.ClaimRequest{
			Slots:       []uint32{1},
			NodeID:      "node-b",
			InstanceID:  "ib",
			Revision:    target.Lease.Revision,
			Lease:       time.Hour,
			QuietWindow: time.Hour,
		})
		require.NoError(t, err)
		require.Len(t, outcomes, 1)
		require.True(t, outcomes[0].Granted)
		assert.EqualValues(t, source.Slots[0].Epoch+1, outcomes[0].Ownership.Epoch,
			"a takeover must advance the epoch and never rewind it")
	})
}

// TestSequenceKeyIdentityIsByteWiseAcrossDialects pins the collation contract:
// keys that differ only by case, trailing space or Unicode normalisation are
// distinct rows, and an exact lookup returns the row written for that byte
// sequence. PostgreSQL relies on the deterministic C collation and MySQL on a
// VARBINARY column; a text collation would collapse at least one pair here.
func TestSequenceKeyIdentityIsByteWiseAcrossDialects(t *testing.T) {
	forEachDialect(t, func(t *testing.T, item *harness) {
		require.NoError(t, resetSequenceDatabase(t, item))
		db := openGORM(t, item)
		keys := []string{"key", "KEY", "key ", "key  ", "\u00e9", "e\u0301", "\u8ba2\u5355"}
		for index, key := range keys {
			require.NoError(t, db.Create(&sequencedata.SequenceModel{
				SequenceKey: key,
				ReservedEnd: int64(index + 1),
				UpdatedAt:   time.Now().UTC(),
			}).Error, "insert %q", key)
		}
		var count int64
		require.NoError(t, db.Table("sequence_ranges").Count(&count).Error)
		assert.EqualValues(t, len(keys), count,
			"byte-distinct keys must not be merged by the collation")

		var reservedEnd int64
		require.NoError(t, db.Table("sequence_ranges").
			Select("reserved_end").
			Where("sequence_key = ?", "key ").
			Scan(&reservedEnd).
			Error)
		assert.EqualValues(t, 3, reservedEnd, "an exact byte lookup must find its row")
	})
}

// TestDurableHandoffContractAcrossDialects exercises the persisted handoff
// state machine on a real server: the drain boundary is fixed at withdrawal and
// survives a renewal, a transfer cannot run before drain confirmation, a lost
// target is retargeted on a new token without moving the boundary, a stale token
// cannot drain, and a release ends unowned.
func TestDurableHandoffContractAcrossDialects(t *testing.T) {
	forEachDialect(t, func(t *testing.T, item *harness) {
		require.NoError(t, resetSequenceDatabase(t, item))
		db := openGORM(t, item)
		ctx := context.Background()

		// The lease and quiet window are pinned long so the freshness checks
		// are driven by explicit mutations and the boundary is observable,
		// not by a race against the wall clock.
		ha := biz.HAConfig{LeaseDuration: time.Hour, QuietWindow: time.Hour}
		limits := biz.MigrationConfig{
			JoinStabilityWindow: time.Minute,
			BatchSlots:          64,
			MaxInflight:         256,
			MaxPerSource:        64,
			MaxPerTarget:        64,
			MaxPlanned:          1024,
		}

		source := grantInstance(t, db, "node-a", "ia", 0, 1)
		target := grantInstance(t, db, "node-b", "ib")
		spare := grantInstance(t, db, "node-c", "ic")
		require.Len(t, source.Slots, 2)

		placement := sequencedata.NewPlacementData(db, ha.LeaseDuration, ha.NodeTTL)
		tenure, err := placement.AcquireCoordinator(ctx, "control", time.Minute)
		require.NoError(t, err)
		require.True(t, tenure.Held)
		repo := sequencedata.NewHandoffData(db)

		read := func(slot uint32) biz.Handoff {
			t.Helper()
			rows, err := repo.Handoffs(ctx, "", biz.SlotCount)
			require.NoError(t, err)
			for _, row := range rows {
				if row.SlotID == slot {
					return row
				}
			}
			t.Fatalf("no handoff row for slot %d", slot)
			return biz.Handoff{}
		}
		apply := func(instance string, revision uint64, action string, handoff biz.Handoff) {
			t.Helper()
			require.NoError(t, repo.ApplyHandoffs(ctx, biz.HandoffRequest{
				InstanceID: instance,
				Revision:   revision,
				HA:         ha,
				Limits:     limits,
				Commands:   []biz.HandoffCommand{{Handoff: handoff, Action: action}},
			}))
		}

		transfer := biz.Handoff{
			SlotID:           0,
			ID:               "token-0",
			Kind:             "TRANSFER",
			SourceInstanceID: "ia",
			SourceEpoch:      source.Slots[0].Epoch,
			TargetInstanceID: "ib",
			Phase:            "PLANNED",
		}
		require.NoError(
			t,
			repo.PlanHandoffs(ctx, []biz.Handoff{transfer}, tenure, limits, ha.LeaseDuration),
		)

		apply("ib", target.Lease.Revision, "PREPARE", transfer)
		assert.Equal(t, "READY", read(0).Phase)

		apply("ib", target.Lease.Revision, "BEGIN", transfer)
		draining := read(0)
		require.Equal(t, "DRAINING", draining.Phase)
		require.False(t, draining.NotBefore.IsZero())
		boundary := draining.NotBefore

		// Entering DRAINING withdrew the source and advanced its revision,
		// so renew against the revision now in force. The renewal must not
		// move the fixed boundary.
		sourceAfterBegin, err := sequencedata.NewOwnershipData(db).InstanceAuthority(ctx, "ia")
		require.NoError(t, err)
		_, err = sequencedata.NewOwnershipData(db).
			RenewInstance(ctx, "ia", sourceAfterBegin.Lease.Revision)
		require.NoError(t, err)
		assert.Equal(t, boundary, read(0).NotBefore,
			"a renewal must not extend the fixed drain boundary")

		sourceRevision := sourceAfterBegin.Lease.Revision

		// Without a drain confirmation the transfer waits for the boundary.
		apply("ib", target.Lease.Revision, "TRANSFER", draining)
		assert.Equal(t, "DRAINING", read(0).Phase)

		// A target that dies after BEGIN is retargeted on a new token while
		// the original boundary is preserved.
		require.NoError(t, sequencedata.NewOwnershipData(db).RetireInstance(ctx, "ib"))
		require.NoError(t, repo.RecoverHandoffs(
			ctx,
			tenure,
			[]biz.NodeInfo{{ID: "node-a", InstanceID: "ia"}, {ID: "node-c", InstanceID: "ic"}},
			ha,
			limits.BatchSlots,
		))
		retargeted := read(0)
		assert.NotEqual(t, draining.ID, retargeted.ID)
		assert.Equal(t, boundary, retargeted.NotBefore)
		assert.False(t, retargeted.TargetReady)
		assert.Equal(t, "ic", retargeted.TargetInstanceID)

		// A drain confirmation carrying the superseded token is ignored.
		apply("ia", sourceRevision, "ACK_DRAIN", draining)
		assert.False(t, read(0).Drained)
		apply("ia", sourceRevision, "ACK_DRAIN", retargeted)
		require.True(t, read(0).Drained)

		apply("ic", spare.Lease.Revision, "TRANSFER", retargeted)
		assert.Equal(t, "DRAINING", read(0).Phase,
			"a transfer needs the new target to be ready first")
		apply("ic", spare.Lease.Revision, "PREPARE", retargeted)
		apply("ic", spare.Lease.Revision, "TRANSFER", read(0))
		transferred := read(0)
		require.Equal(t, "TRANSFERRED", transferred.Phase)
		destination, err := sequencedata.NewOwnershipData(db).InstanceAuthority(ctx, "ic")
		require.NoError(t, err)
		require.Len(t, destination.Slots, 1)
		assert.EqualValues(t, source.Slots[0].Epoch+1, destination.Slots[0].Epoch,
			"a transfer must advance the epoch by exactly one")
		apply("ic", destination.Lease.Revision, "ACK_ACTIVE", transferred)
		rows, err := repo.Handoffs(ctx, "", biz.SlotCount)
		require.NoError(t, err)
		assert.Empty(t, rows, "a completed handoff must leave no pending row")

		// A release has no target. The source's WITHDRAW both persists the
		// intent and begins draining it -- a node shutting down must not
		// leave a cancellable intent behind -- and it completes unowned once
		// the drain is confirmed.
		release := biz.Handoff{
			SlotID:           1,
			ID:               "release-1",
			SourceInstanceID: "ia",
			SourceEpoch:      source.Slots[1].Epoch,
		}
		apply("ia", sourceRevision, "WITHDRAW", release)
		withdrawn := read(1)
		assert.Equal(t, "RELEASE", withdrawn.Kind)
		require.Equal(t, "DRAINING", withdrawn.Phase)
		require.False(t, withdrawn.NotBefore.IsZero())
		assert.Empty(t, withdrawn.TargetInstanceID)
		apply("ia", sourceRevision, "TRANSFER", read(1))
		assert.Equal(t, "DRAINING", read(1).Phase, "a release waits for the drain confirmation")
		apply("ia", sourceRevision, "ACK_DRAIN", read(1))
		apply("ia", sourceRevision, "TRANSFER", read(1))
		released := readSlotOwnership(t, db, 1)
		require.Len(t, released, 1)
		assert.Equal(t, "UNOWNED", released[0].State)
		assert.Nil(t, released[0].OwnerInstanceID)
		rows, err = repo.Handoffs(ctx, "", biz.SlotCount)
		require.NoError(t, err)
		assert.Empty(t, rows, "a completed release must leave no pending row")
	})
}

func TestSoleSourceRecoversOrphanedTransferAcrossDialects(t *testing.T) {
	forEachDialect(t, func(t *testing.T, item *harness) {
		for _, drained := range []bool{false, true} {
			t.Run(
				map[bool]string{false: "quiet window", true: "drain acknowledgement"}[drained],
				func(t *testing.T) {
					require.NoError(t, resetSequenceDatabase(t, item))
					db := openGORM(t, item)
					ctx := context.Background()
					source := grantInstance(t, db, "a", "ia", 0)
					target := grantInstance(t, db, "b", "ib")
					ha := biz.HAConfig{LeaseDuration: time.Hour, QuietWindow: time.Hour}
					limits := biz.MigrationConfig{
						BatchSlots:   64,
						MaxInflight:  256,
						MaxPerSource: 64,
						MaxPerTarget: 64,
						MaxPlanned:   1024,
					}
					ownership := sequencedata.NewOwnershipData(db)
					repo := sequencedata.NewHandoffData(db)
					placement := sequencedata.NewPlacementData(db, ha.LeaseDuration, time.Hour)
					tenure, err := placement.AcquireCoordinator(ctx, "control", time.Hour)
					require.NoError(t, err)
					h := biz.Handoff{
						SlotID:           0,
						ID:               "orphan",
						Kind:             "TRANSFER",
						SourceInstanceID: "ia",
						SourceEpoch:      source.Slots[0].Epoch,
						TargetInstanceID: "ib",
						Phase:            "PLANNED",
					}
					require.NoError(
						t,
						repo.PlanHandoffs(ctx, []biz.Handoff{h}, tenure, limits, ha.LeaseDuration),
					)
					apply := func(actor string, revision uint64, action string, handoff biz.Handoff) {
						t.Helper()
						require.NoError(t, repo.ApplyHandoffs(ctx, biz.HandoffRequest{
							InstanceID: actor,
							Revision:   revision,
							Commands: []biz.HandoffCommand{
								{Handoff: handoff, Action: action},
							},
							HA:     ha,
							Limits: limits,
						}))
					}
					apply("ib", target.Lease.Revision, "PREPARE", h)
					apply("ib", target.Lease.Revision, "BEGIN", h)
					pending, err := repo.Handoffs(ctx, "ia", 1)
					require.NoError(t, err)
					require.Len(t, pending, 1)
					before := pending[0]
					snapshot, err := ownership.InstanceAuthority(ctx, "ia")
					require.NoError(t, err)
					if drained {
						apply("ia", snapshot.Lease.Revision, "ACK_DRAIN", before)
					}
					require.NoError(t, ownership.RetireInstance(ctx, "ib"))
					recoverPass := func() {
						require.NoError(
							t,
							repo.RecoverHandoffs(
								ctx,
								tenure,
								[]biz.NodeInfo{{ID: "a", InstanceID: "ia"}},
								ha,
								64,
							),
						)
					}
					recoverPass()
					pending, err = repo.Handoffs(ctx, "ia", 1)
					require.NoError(t, err)
					require.Len(t, pending, 1)
					release := pending[0]
					assert.Equal(t, "RELEASE", release.Kind)
					assert.Equal(t, drained, release.Drained)
					assert.Equal(t, before.NotBefore, release.NotBefore)
					assert.NotEqual(t, before.ID, release.ID)
					apply("ib", target.Lease.Revision, "TRANSFER", before)
					recoverPass()
					claim := biz.ClaimRequest{
						Slots:       []uint32{0},
						NodeID:      "a",
						InstanceID:  "ia",
						Revision:    snapshot.Lease.Revision,
						Lease:       ha.LeaseDuration,
						QuietWindow: ha.QuietWindow,
					}
					if !drained {
						denied, err := ownership.ClaimSlots(ctx, claim)
						require.NoError(t, err)
						assert.False(t, denied[0].Granted)
						require.NoError(
							t,
							db.Exec(
								"UPDATE sequence_slot_handoffs SET not_before = ? WHERE slot_id = 0",
								time.Now().Add(-time.Hour),
							).Error,
						)
					}
					granted, err := ownership.ClaimSlots(ctx, claim)
					require.NoError(t, err)
					require.True(t, granted[0].Granted)
					assert.Equal(t, source.Slots[0].Epoch+1, granted[0].Ownership.Epoch)
				},
			)
		}
	})
}
