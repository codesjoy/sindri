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
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func openPlacementTestDB(t testing.TB) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(
		sqlite.Open("file:"+t.Name()+"?mode=memory&cache=shared"),
		&gorm.Config{Logger: logger.Default.LogMode(logger.Silent)},
	)
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(1)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })
	for _, sql := range []string{
		"CREATE TABLE sequence_ranges (sequence_key text COLLATE BINARY PRIMARY KEY, reserved_end integer NOT NULL CHECK(reserved_end>0), updated_at datetime NOT NULL)",
		"CREATE TABLE sequence_instance_leases (instance_id text PRIMARY KEY,node_id text NOT NULL,ownership_revision integer NOT NULL DEFAULT 0,granted_at datetime NOT NULL,state text NOT NULL,created_at datetime NOT NULL,updated_at datetime NOT NULL)",
		"CREATE TABLE sequence_slot_ownership (slot_id integer PRIMARY KEY,owner_instance_id text,epoch integer NOT NULL DEFAULT 0,state text NOT NULL DEFAULT 'UNOWNED',updated_at datetime NOT NULL DEFAULT CURRENT_TIMESTAMP)",
		"CREATE TABLE sequence_coordinator (id integer PRIMARY KEY,owner_instance_id text,epoch integer NOT NULL DEFAULT 0,expires_at datetime,updated_at datetime NOT NULL DEFAULT CURRENT_TIMESTAMP)",
		"INSERT INTO sequence_coordinator (id) VALUES (1)",
		"CREATE TABLE sequence_node_liveness (node_id text PRIMARY KEY,instance_id text NOT NULL,last_seen_at datetime NOT NULL,eligible_since datetime NOT NULL)",
		"CREATE TABLE sequence_route_snapshot (id integer PRIMARY KEY CHECK(id=1),version integer NOT NULL,payload blob NOT NULL,updated_at datetime NOT NULL)",
		"CREATE TABLE sequence_slot_handoffs (slot_id integer PRIMARY KEY,handoff_id text NOT NULL,kind text NOT NULL,source_instance_id text NOT NULL,source_epoch integer NOT NULL,target_instance_id text,phase text NOT NULL,drained bool NOT NULL DEFAULT false,target_ready bool NOT NULL DEFAULT false,not_before datetime,created_at datetime NOT NULL,updated_at datetime NOT NULL)",
	} {
		require.NoError(t, db.Exec(sql).Error)
	}
	rows := make([]ownershipRow, biz.SlotCount)
	for i := range rows {
		rows[i] = ownershipRow{SlotID: uint32(i), State: "UNOWNED", UpdatedAt: time.Now()}
	}
	require.NoError(t, db.Table("sequence_slot_ownership").CreateInBatches(rows, 500).Error)
	return db
}

func fullPlacementSegments(node, instance string, epoch uint64) []biz.OwnershipSegment {
	return []biz.OwnershipSegment{
		{
			StartSlot:       0,
			EndSlot:         biz.SlotCount - 1,
			OwnerNodeID:     node,
			OwnerInstanceID: instance,
			Epoch:           epoch,
			State:           biz.SlotOwned,
			GrantAgeKnown:   true,
		},
	}
}

func TestSingletonRouteSnapshotAndCoordinatorFencing(t *testing.T) {
	db := openPlacementTestDB(t)
	p := NewPlacementData(db, 3*time.Second, 15*time.Second)
	ctx := context.Background()
	repo := NewRouteModel(db)
	r, err := repo.GetNewerRoute(ctx, 0)
	require.NoError(t, err)
	assert.Nil(t, r)
	tenure, err := p.AcquireCoordinator(ctx, "control-a", time.Minute)
	require.NoError(t, err)
	require.True(t, tenure.Held)
	again, err := p.AcquireCoordinator(ctx, "control-a", time.Minute)
	require.NoError(t, err)
	assert.Equal(t, tenure, again)
	other, err := p.AcquireCoordinator(ctx, "control-b", time.Minute)
	require.NoError(t, err)
	assert.False(t, other.Held)
	segments := fullPlacementSegments("node-a", "instance-a", 1)
	result, err := p.MaterialiseRoute(ctx, segments, 1, tenure)
	require.NoError(t, err)
	assert.EqualValues(t, 1, result.Revision)
	result, err = p.MaterialiseRoute(ctx, segments, 1, tenure)
	require.NoError(t, err)
	assert.EqualValues(t, 1, result.Revision)
	segments[0].Epoch = 2
	result, err = p.MaterialiseRoute(ctx, segments, 1, tenure)
	require.NoError(t, err)
	assert.EqualValues(t, 2, result.Revision)
	var count int64
	require.NoError(t, db.Table("sequence_route_snapshot").Count(&count).Error)
	assert.EqualValues(t, 1, count)
	require.NoError(
		t,
		db.Exec(
			"UPDATE sequence_coordinator SET expires_at=? WHERE id=1",
			time.Now().Add(-time.Second),
		).Error,
	)
	newTenure, err := p.AcquireCoordinator(ctx, "control-b", time.Minute)
	require.NoError(t, err)
	assert.Greater(t, newTenure.Epoch, tenure.Epoch)
	_, err = p.MaterialiseRoute(ctx, segments, 1, tenure)
	require.ErrorIs(t, err, biz.ErrCoordinatorLost)
}

func TestMembershipCannotOverwriteInstanceAuthorityAndStableAdmissionResets(t *testing.T) {
	db := openPlacementTestDB(t)
	ctx := context.Background()
	ownership := NewOwnershipData(db)
	live := NewLivenessData(db, 15*time.Second)
	p := NewPlacementData(db, 3*time.Second, 15*time.Second)
	require.NoError(t, ownership.RegisterInstance(ctx, "node-a", "instance-a"))
	require.NoError(t, live.RenewLiveness(ctx, "node-a", "instance-a"))
	require.NoError(
		t,
		db.Exec(
			"UPDATE sequence_node_liveness SET eligible_since=?",
			time.Now().Add(-20*time.Second),
		).Error,
	)
	nodes, err := p.LiveNodes(ctx, 15*time.Second)
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	assert.GreaterOrEqual(t, nodes[0].StableFor, 15*time.Second)
	snapshot, err := ownership.InstanceAuthority(ctx, "instance-a")
	require.NoError(t, err)
	require.NoError(t, live.RenewLiveness(ctx, "node-a", "instance-b"))
	after, err := ownership.InstanceAuthority(ctx, "instance-a")
	require.NoError(t, err)
	assert.Equal(t, snapshot, after)
	nodes, err = p.LiveNodes(ctx, 15*time.Second)
	require.NoError(t, err)
	assert.Empty(t, nodes)
	require.NoError(t, ownership.RegisterInstance(ctx, "node-a", "instance-b"))
	nodes, err = p.LiveNodes(ctx, 15*time.Second)
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	assert.Less(t, nodes[0].StableFor, time.Second)
	require.NoError(t, live.DropLiveness(ctx, "node-a", "instance-a"))
	nodes, err = p.LiveNodes(ctx, 15*time.Second)
	require.NoError(t, err)
	require.Len(t, nodes, 1)
}
