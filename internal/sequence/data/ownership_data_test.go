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

package data

import (
	"context"
	"testing"
	"time"

	testkit "github.com/codesjoy/sindri/internal/pkg/tests"
	"github.com/codesjoy/sindri/internal/sequence/biz"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func authorityFor(
	t testing.TB,
	db *gorm.DB,
	node, instance string,
	slots ...uint32,
) biz.ReservationAuthority {
	t.Helper()
	ctx := context.Background()
	repo := NewOwnershipData(db)
	require.NoError(t, repo.RegisterInstance(ctx, node, instance))
	snapshot, err := repo.InstanceAuthority(ctx, instance)
	require.NoError(t, err)
	grants, err := repo.ClaimSlots(
		ctx,
		biz.ClaimRequest{
			Slots:       slots,
			NodeID:      node,
			InstanceID:  instance,
			Revision:    snapshot.Lease.Revision,
			Lease:       3 * time.Second,
			QuietWindow: 5 * time.Second,
		},
	)
	require.NoError(t, err)
	snapshot, err = repo.InstanceAuthority(ctx, instance)
	require.NoError(t, err)
	auth := biz.ReservationAuthority{
		InstanceID: instance,
		Revision:   snapshot.Lease.Revision,
		Lease:      3 * time.Second,
		Epochs:     map[uint32]uint64{},
	}
	for _, o := range grants {
		require.True(t, o.Granted)
		auth.Epochs[o.Ownership.SlotID] = o.Ownership.Epoch
	}
	return auth
}

func TestInstanceRenewalTouchesNoSlotRowsAndCASCannotResurrectRetiredInstance(t *testing.T) {
	db := openPlacementTestDB(t)
	ctx := context.Background()
	auth := authorityFor(t, db, "a", "ia", 1, 2)
	repo := NewOwnershipData(db)
	var before, after []ownershipRow
	require.NoError(
		t,
		db.Table("sequence_slot_ownership").
			Where("slot_id IN ?", []uint32{1, 2}).
			Find(&before).
			Error,
	)
	_, err := repo.RenewInstance(ctx, "ia", auth.Revision)
	require.NoError(t, err)
	require.NoError(
		t,
		db.Table("sequence_slot_ownership").
			Where("slot_id IN ?", []uint32{1, 2}).
			Find(&after).
			Error,
	)
	assert.Equal(t, before, after)
	_, err = repo.RenewInstance(ctx, "ia", auth.Revision-1)
	require.ErrorIs(t, err, biz.ErrAuthorityChanged)
	require.NoError(t, repo.RetireInstance(ctx, "ia"))
	_, err = repo.RenewInstance(ctx, "ia", auth.Revision)
	require.ErrorIs(t, err, biz.ErrAuthorityChanged)
	require.ErrorIs(t, repo.RegisterInstance(ctx, "a", "ia"), biz.ErrAuthorityChanged)
}

func TestStrictReservationAuthorityAndKeyBytes(t *testing.T) {
	db := openPlacementTestDB(t)
	ctx := context.Background()
	keys := []string{"key", "KEY", "key ", "key  ", "é", "é", "订单"}
	var slots []uint32
	seen := map[uint32]bool{}
	for _, k := range keys {
		id := biz.SlotForKey(k)
		if !seen[id] {
			seen[id] = true
			slots = append(slots, id)
		}
	}
	auth := authorityFor(t, db, "a", "ia", slots...)
	repo := NewSequenceData(db)
	for _, k := range keys {
		ranges, err := repo.ReserveRanges(ctx, auth, []biz.ReservationRequest{{Key: k, Step: 10}})
		require.NoError(t, err)
		assert.EqualValues(t, 1, ranges[0].Start)
	}
	for _, bad := range []biz.ReservationAuthority{{}, {InstanceID: auth.InstanceID, Revision: auth.Revision, Lease: auth.Lease}, {InstanceID: auth.InstanceID, Revision: auth.Revision, Epochs: auth.Epochs}, {InstanceID: auth.InstanceID, Revision: auth.Revision + 1, Lease: auth.Lease, Epochs: auth.Epochs}} {
		_, err := repo.ReserveRanges(ctx, bad, []biz.ReservationRequest{{Key: keys[0], Step: 10}})
		require.Error(t, err)
	}
	require.NoError(
		t,
		db.Exec(
			"UPDATE sequence_instance_leases SET granted_at=? WHERE instance_id='ia'",
			time.Now().Add(-time.Hour),
		).Error,
	)
	_, err := repo.ReserveRanges(ctx, auth, []biz.ReservationRequest{{Key: keys[0], Step: 10}})
	require.ErrorIs(t, err, biz.ErrLeaseExpired)
	_, err = NewOwnershipData(db).RenewInstance(ctx, "ia", auth.Revision)
	require.NoError(t, err)
	_, err = repo.ReserveRanges(ctx, auth, []biz.ReservationRequest{{Key: keys[0], Step: 10}})
	require.NoError(t, err)
}

func TestDurableHandoffFixedBoundaryDrainTokenAndRetargetReadiness(t *testing.T) {
	db := openPlacementTestDB(t)
	ctx := context.Background()
	source := authorityFor(t, db, "a", "ia", 0)
	target := authorityFor(t, db, "b", "ib")
	authorityFor(t, db, "c", "ic")
	var ha biz.HAConfig
	var limits biz.MigrationConfig
	require.NoError(t, testkit.DecodeDefaults(&ha))
	require.NoError(t, testkit.DecodeDefaults(&limits))
	p := NewPlacementData(db, ha.LeaseDuration, ha.NodeTTL)
	tenure, err := p.AcquireCoordinator(ctx, "control", time.Minute)
	require.NoError(t, err)
	repo := NewHandoffData(db)
	h := biz.Handoff{
		SlotID:           0,
		ID:               "token",
		Kind:             "TRANSFER",
		SourceInstanceID: "ia",
		SourceEpoch:      source.Epochs[0],
		TargetInstanceID: "ib",
		Phase:            "PLANNED",
	}
	require.NoError(t, repo.PlanHandoffs(ctx, []biz.Handoff{h}, tenure, limits, ha.LeaseDuration))
	apply := func(actor string, revision uint64, action string, h biz.Handoff) {
		t.Helper()
		require.NoError(
			t,
			repo.ApplyHandoffs(
				ctx,
				biz.HandoffRequest{
					InstanceID: actor,
					Revision:   revision,
					HA:         ha,
					Limits:     limits,
					Commands:   []biz.HandoffCommand{{Handoff: h, Action: action}},
				},
			),
		)
	}
	read := func() biz.Handoff {
		t.Helper()
		rows, err := repo.Handoffs(ctx, "", 100)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		return rows[0]
	}
	apply("ib", target.Revision, "TRANSFER", h)
	assert.Equal(t, "PLANNED", read().Phase)
	apply("ib", target.Revision, "PREPARE", h)
	h = read()
	assert.Equal(t, "READY", h.Phase)
	apply("ib", target.Revision, "BEGIN", h)
	h = read()
	assert.Equal(t, "DRAINING", h.Phase)
	boundary := h.NotBefore
	snap, err := NewOwnershipData(db).InstanceAuthority(ctx, "ia")
	require.NoError(t, err)
	_, err = NewOwnershipData(db).RenewInstance(ctx, "ia", snap.Lease.Revision)
	require.NoError(t, err)
	assert.Equal(t, boundary, read().NotBefore)
	apply("ib", target.Revision, "TRANSFER", h)
	assert.Equal(t, "DRAINING", read().Phase)
	require.NoError(t, NewOwnershipData(db).RetireInstance(ctx, "ib"))
	require.NoError(
		t,
		repo.RecoverHandoffs(
			ctx,
			tenure,
			[]biz.NodeInfo{{ID: "a", InstanceID: "ia"}, {ID: "c", InstanceID: "ic"}},
			ha,
			64,
		),
	)
	retargeted := read()
	assert.NotEqual(t, h.ID, retargeted.ID)
	assert.Equal(t, boundary, retargeted.NotBefore)
	assert.False(t, retargeted.TargetReady)
	apply("ia", snap.Lease.Revision, "ACK_DRAIN", h)
	assert.False(t, read().Drained)
	apply("ia", snap.Lease.Revision, "ACK_DRAIN", retargeted)
	apply("ic", 0, "TRANSFER", retargeted)
	assert.Equal(t, "DRAINING", read().Phase)
	apply("ic", 0, "PREPARE", retargeted)
	retargeted = read()
	apply("ic", 0, "TRANSFER", retargeted)
	transferred := read()
	assert.Equal(t, "TRANSFERRED", transferred.Phase)
	destination, err := NewOwnershipData(db).InstanceAuthority(ctx, "ic")
	require.NoError(t, err)
	require.Len(t, destination.Slots, 1)
	assert.EqualValues(t, 2, destination.Slots[0].Epoch)
	_, err = NewOwnershipData(db).RenewInstance(ctx, "ic", destination.Lease.Revision)
	require.NoError(t, err)
	apply("ic", destination.Lease.Revision, "ACK_ACTIVE", transferred)
	rows, err := repo.Handoffs(ctx, "", 100)
	require.NoError(t, err)
	assert.Empty(t, rows)
}

func TestLeaderlessReleaseRecoveryDoesNotBypassTransferPin(t *testing.T) {
	db := openPlacementTestDB(t)
	ctx := context.Background()
	source := authorityFor(t, db, "a", "ia", 0, 1)
	target := authorityFor(t, db, "b", "ib")
	var ha biz.HAConfig
	var limits biz.MigrationConfig
	require.NoError(t, testkit.DecodeDefaults(&ha))
	require.NoError(t, testkit.DecodeDefaults(&limits))
	repo := NewHandoffData(db)
	require.NoError(
		t,
		repo.ApplyHandoffs(
			ctx,
			biz.HandoffRequest{
				InstanceID: "ia",
				HA:         ha,
				Limits:     limits,
				Commands: []biz.HandoffCommand{
					{
						Action: "WITHDRAW",
						Handoff: biz.Handoff{
							SlotID:           0,
							ID:               "release",
							SourceInstanceID: "ia",
							SourceEpoch:      source.Epochs[0],
						},
					},
				},
			},
		),
	)
	withdrawn, err := repo.Handoffs(ctx, "ia", 10)
	require.NoError(t, err)
	require.Len(t, withdrawn, 1)
	assert.Equal(t, "RELEASE", withdrawn[0].Kind)
	assert.Equal(t, "DRAINING", withdrawn[0].Phase,
		"a withdrawal starts the drain; recovery completes it after the quiet window")
	assert.False(t, withdrawn[0].NotBefore.IsZero())
	require.Empty(t, withdrawn[0].TargetInstanceID)
	require.NoError(
		t,
		db.Exec(
			"UPDATE sequence_slot_handoffs SET not_before=? WHERE slot_id=0",
			time.Now().Add(-time.Second),
		).Error,
	)
	outcomes, err := NewOwnershipData(
		db,
	).ClaimSlots(ctx, biz.ClaimRequest{Slots: []uint32{0}, NodeID: "b", InstanceID: "ib", Revision: target.Revision, Lease: ha.LeaseDuration, QuietWindow: ha.QuietWindow})
	require.NoError(t, err)
	require.True(t, outcomes[0].Granted)
	assert.EqualValues(t, 2, outcomes[0].Ownership.Epoch)
}
