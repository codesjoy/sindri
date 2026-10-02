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

	"github.com/codesjoy/sindri/internal/sequence/biz"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func seedTestInstance(t testing.TB, db *gorm.DB, instance string, grantedAt time.Time) {
	t.Helper()
	require.NoError(t, db.Exec(
		"INSERT INTO sequence_instance_leases (instance_id,node_id,ownership_revision,granted_at,state,created_at,updated_at) VALUES (?,?,?,?,?,?,?)",
		instance,
		"node-"+instance,
		1,
		grantedAt,
		"ACTIVE",
		grantedAt,
		grantedAt,
	).Error)
}

func testInstanceExists(t testing.TB, db *gorm.DB, instance string) bool {
	t.Helper()
	var count int64
	require.NoError(t, db.Raw(
		"SELECT COUNT(*) FROM sequence_instance_leases WHERE instance_id=?",
		instance,
	).Scan(&count).Error)
	return count > 0
}

func TestCollectInstancesReapsOnlyUnpinnedAgedInstances(t *testing.T) {
	db := openPlacementTestDB(t)
	repo := NewHandoffData(db)
	ctx := context.Background()
	old := time.Now().Add(-time.Hour)

	for _, instance := range []string{"free", "owned", "live", "handoff", "done"} {
		seedTestInstance(t, db, instance, old)
	}
	seedTestInstance(t, db, "young", time.Now())
	require.NoError(t, db.Exec(
		"UPDATE sequence_slot_ownership SET owner_instance_id='owned', state='OWNED' WHERE slot_id=0",
	).Error)
	require.NoError(t, db.Exec(
		"INSERT INTO sequence_node_liveness (node_id,instance_id,last_seen_at,eligible_since) VALUES ('node-live','live',?,?)",
		time.Now(),
		time.Now(),
	).Error)
	require.NoError(t, db.Exec(
		"INSERT INTO sequence_slot_handoffs (slot_id,handoff_id,kind,source_instance_id,source_epoch,target_instance_id,phase,created_at,updated_at) VALUES (0,'h-live','TRANSFER','source','1','handoff','PLANNED',?,?)",
		time.Now(),
		time.Now(),
	).Error)
	require.NoError(t, db.Exec(
		"INSERT INTO sequence_slot_handoffs (slot_id,handoff_id,kind,source_instance_id,source_epoch,phase,created_at,updated_at) VALUES (1,'h-done','TRANSFER','done','1','COMPLETED',?,?)",
		time.Now(),
		time.Now(),
	).Error)

	require.NoError(t, repo.CollectInstances(ctx, 5*time.Second, 100))

	assert.False(t, testInstanceExists(t, db, "free"), "an aged unpinned instance is collected")
	assert.False(t, testInstanceExists(t, db, "done"),
		"a completed handoff does not pin its source instance")
	assert.True(t, testInstanceExists(t, db, "owned"), "ownership pins the instance")
	assert.True(t, testInstanceExists(t, db, "live"), "node liveness pins the instance")
	assert.True(t, testInstanceExists(t, db, "handoff"), "a pending handoff pins the instance")
	assert.True(t, testInstanceExists(t, db, "young"), "the quiet window must elapse first")
}

func TestCollectInstancesHonorsLimit(t *testing.T) {
	db := openPlacementTestDB(t)
	repo := NewHandoffData(db)
	ctx := context.Background()
	old := time.Now().Add(-time.Hour)
	for _, instance := range []string{"a", "b", "c"} {
		seedTestInstance(t, db, instance, old)
	}

	require.NoError(t, repo.CollectInstances(ctx, 5*time.Second, 1))

	assert.False(t, testInstanceExists(t, db, "a"))
	assert.True(t, testInstanceExists(t, db, "b"), "the limit stops the sweep after one reap")
	assert.True(t, testInstanceExists(t, db, "c"))
}

func TestRecoveryReleasesTransferWhenOnlySourceSurvives(t *testing.T) {
	db := openPlacementTestDB(t)
	ctx := context.Background()
	source := authorityFor(t, db, "a", "ia", 0)
	target := authorityFor(t, db, "b", "ib")
	ha := biz.HAConfig{LeaseDuration: time.Hour, QuietWindow: time.Hour}
	limits := biz.MigrationConfig{
		BatchSlots:   64,
		MaxInflight:  256,
		MaxPerSource: 64,
		MaxPerTarget: 64,
		MaxPlanned:   1024,
	}
	placement := NewPlacementData(db, ha.LeaseDuration, time.Hour)
	tenure, err := placement.AcquireCoordinator(ctx, "control", time.Hour)
	require.NoError(t, err)
	repo := NewHandoffData(db)
	h := biz.Handoff{
		SlotID:           0,
		ID:               "transfer",
		Kind:             "TRANSFER",
		SourceInstanceID: "ia",
		SourceEpoch:      source.Epochs[0],
		TargetInstanceID: "ib",
		Phase:            "PLANNED",
	}
	require.NoError(t, repo.PlanHandoffs(ctx, []biz.Handoff{h}, tenure, limits, ha.LeaseDuration))
	apply := func(action string) {
		t.Helper()
		require.NoError(t, repo.ApplyHandoffs(ctx, biz.HandoffRequest{
			InstanceID: "ib", Revision: target.Revision,
			Commands: []biz.HandoffCommand{{Handoff: h, Action: action}}, HA: ha, Limits: limits,
		}))
	}
	apply("PREPARE")
	apply("BEGIN")
	before, err := repo.Handoffs(ctx, "ia", 1)
	require.NoError(t, err)
	require.Len(t, before, 1)
	require.NoError(t, NewOwnershipData(db).RetireInstance(ctx, "ib"))
	recoverPass := func() {
		require.NoError(
			t,
			repo.RecoverHandoffs(ctx, tenure, []biz.NodeInfo{{ID: "a", InstanceID: "ia"}}, ha, 64),
		)
	}
	recoverPass()
	after, err := repo.Handoffs(ctx, "ia", 1)
	require.NoError(t, err)
	require.Len(t, after, 1)
	assert.Equal(t, "RELEASE", after[0].Kind)
	assert.Empty(t, after[0].TargetInstanceID)
	assert.NotEqual(t, before[0].ID, after[0].ID)
	assert.Equal(t, before[0].NotBefore, after[0].NotBefore)
	apply("TRANSFER")
	recoverPass()
	current, err := repo.Handoffs(ctx, "ia", 1)
	require.NoError(t, err)
	assert.Equal(t, after, current)
	snapshot, err := NewOwnershipData(db).InstanceAuthority(ctx, "ia")
	require.NoError(t, err)
	claim := biz.ClaimRequest{
		Slots:       []uint32{0},
		NodeID:      "a",
		InstanceID:  "ia",
		Revision:    snapshot.Lease.Revision,
		Lease:       ha.LeaseDuration,
		QuietWindow: ha.QuietWindow,
	}
	denied, err := NewOwnershipData(db).ClaimSlots(ctx, claim)
	require.NoError(t, err)
	assert.False(t, denied[0].Granted)
	require.NoError(t, repo.ApplyHandoffs(ctx, biz.HandoffRequest{
		InstanceID: "ia",
		Revision:   snapshot.Lease.Revision,
		Commands: []biz.HandoffCommand{
			{Handoff: after[0], Action: "ACK_DRAIN"},
		},
		HA:     ha,
		Limits: limits,
	}))
	granted, err := NewOwnershipData(db).ClaimSlots(ctx, claim)
	require.NoError(t, err)
	require.True(t, granted[0].Granted)
	assert.Equal(t, source.Epochs[0]+1, granted[0].Ownership.Epoch)
}
