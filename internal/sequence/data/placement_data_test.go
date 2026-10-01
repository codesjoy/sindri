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
	gormio "gorm.io/gorm"
)

// These tests run on SQLite because they are about the shape of the statements
// and the comparison the guard makes, not about dialect lock semantics. The
// dialect-specific parts they exercise -- the storage-clock expression and the
// coordinator predicates -- are spelled per dialect in the code under test and
// are covered by the PostgreSQL and MySQL contract tests.

func openPlacementTestDB(t *testing.T) *gormio.DB {
	t.Helper()
	db, err := gormio.Open(
		sqlite.Open("file:sequence-placement-"+t.Name()+"?mode=memory&cache=shared"),
		&gormio.Config{},
	)
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })

	for _, statement := range []string{
		"CREATE TABLE sequence_coordinator (" +
			"id integer PRIMARY KEY, owner_instance_id text, epoch integer NOT NULL DEFAULT 0, " +
			"expires_at datetime, updated_at datetime NOT NULL DEFAULT CURRENT_TIMESTAMP)",
		"INSERT INTO sequence_coordinator (id) VALUES (1)",
		"CREATE TABLE sequence_routes (" +
			"version integer PRIMARY KEY, payload blob NOT NULL, created_at datetime NOT NULL)",
		"CREATE TABLE sequence_route_state (id integer PRIMARY KEY, revision integer NOT NULL)",
		"INSERT INTO sequence_route_state (id, revision) VALUES (1, 0)",
		"CREATE TABLE sequence_node_liveness (" +
			"node_id text PRIMARY KEY, instance_id text NOT NULL, " +
			"last_seen_at datetime NOT NULL DEFAULT CURRENT_TIMESTAMP)",
	} {
		require.NoError(t, db.Exec(statement).Error)
	}
	return db
}

// expireCoordinator moves the lease's expiry into the past, which is how a test
// stages a takeover without waiting for one.
func expireCoordinator(t *testing.T, db *gormio.DB) {
	t.Helper()
	require.NoError(t, db.Exec(
		"UPDATE sequence_coordinator SET expires_at = '2000-01-01 00:00:00' WHERE id = 1",
	).Error)
}

// fullPlacementView builds the complete ownership view a materialised route has
// to cover: the encoder refuses a snapshot that does not describe the whole
// space.
func fullPlacementView(nodeID, instanceID string, epoch uint64) []biz.Ownership {
	view := make([]biz.Ownership, int(biz.SlotCount))
	for slot := range view {
		view[slot] = biz.Ownership{
			SlotID:          uint32(slot),
			State:           biz.SlotOwned,
			OwnerNodeID:     nodeID,
			OwnerInstanceID: instanceID,
			Epoch:           epoch,
		}
	}
	return view
}

// TestAcquireCoordinatorReportsTheTenureItHolds pins the credential every
// publish presents: the epoch is read from the row rather than assumed, it stays
// put while the same replica keeps the role, and it moves when the role changes
// hands, which is what invalidates the writes the previous holder had in flight.
func TestAcquireCoordinatorReportsTheTenureItHolds(t *testing.T) {
	data := NewPlacementData(openPlacementTestDB(t))
	ctx := context.Background()

	first, err := data.AcquireCoordinator(ctx, "instance-a", time.Minute)
	require.NoError(t, err)
	assert.True(t, first.Held)
	assert.Equal(t, "instance-a", first.InstanceID)
	assert.Equal(t, uint64(1), first.Epoch, "the first takeover moves the epoch off zero")

	renewed, err := data.AcquireCoordinator(ctx, "instance-a", time.Minute)
	require.NoError(t, err)
	require.True(t, renewed.Held)
	assert.Equal(t, first.Epoch, renewed.Epoch, "a renewal is not a new tenure")

	other, err := data.AcquireCoordinator(ctx, "instance-b", time.Minute)
	require.NoError(t, err)
	assert.False(t, other.Held, "a live lease is not takeable")
	assert.Zero(t, other.Epoch)

	// The epoch tracks the role changing hands, not the lease lapsing: a tenant
	// that takes its own lapsed lease back is still the same tenant, so every
	// write it had in flight stays valid. Nothing else can hold the role in
	// between, because the takeover is what would have moved the epoch.
	expireCoordinator(t, data.db)
	reacquired, err := data.AcquireCoordinator(ctx, "instance-a", time.Minute)
	require.NoError(t, err)
	require.True(t, reacquired.Held)
	assert.Equal(t, first.Epoch, reacquired.Epoch)

	expireCoordinator(t, data.db)
	taken, err := data.AcquireCoordinator(ctx, "instance-b", time.Minute)
	require.NoError(t, err)
	require.True(t, taken.Held)
	assert.Equal(t, "instance-b", taken.InstanceID)
	assert.Equal(t, first.Epoch+1, taken.Epoch)
}

// TestMaterialiseRouteIsRefusedUnderALostTenure covers the pass that lost the
// role before it wrote: the directory is refused, and no revision is published
// on behalf of a tenure that is no longer in force.
func TestMaterialiseRouteIsRefusedUnderALostTenure(t *testing.T) {
	data := NewPlacementData(openPlacementTestDB(t))
	ctx := context.Background()

	held, err := data.AcquireCoordinator(ctx, "instance-a", time.Minute)
	require.NoError(t, err)
	require.True(t, held.Held)

	revision, err := data.MaterialiseRoute(
		ctx, fullPlacementView("node-a", "instance-a", held.Epoch), 1, held,
	)
	require.NoError(t, err)
	assert.EqualValues(t, 1, revision)

	expireCoordinator(t, data.db)
	takeover, err := data.AcquireCoordinator(ctx, "instance-b", time.Minute)
	require.NoError(t, err)
	require.True(t, takeover.Held)

	_, err = data.MaterialiseRoute(
		ctx, fullPlacementView("node-b", "instance-b", takeover.Epoch), 1, held,
	)
	assert.ErrorIs(t, err, biz.ErrCoordinatorLost)

	var routes int64
	require.NoError(t, data.db.Table("sequence_routes").Count(&routes).Error)
	assert.EqualValues(t, 1, routes, "a refused directory must not publish a revision")
}

// TestMaterialiseRouteIsRefusedForAStaleEpoch is the case the instance id alone
// cannot catch. The replica lost the role to another, took it back, and is now
// presenting the tenure from before the interruption: its instance id matches
// the row again, so only the epoch refuses the write.
func TestMaterialiseRouteIsRefusedForAStaleEpoch(t *testing.T) {
	data := NewPlacementData(openPlacementTestDB(t))
	ctx := context.Background()

	stale, err := data.AcquireCoordinator(ctx, "instance-a", time.Minute)
	require.NoError(t, err)
	require.True(t, stale.Held)

	expireCoordinator(t, data.db)
	_, err = data.AcquireCoordinator(ctx, "instance-b", time.Minute)
	require.NoError(t, err)

	expireCoordinator(t, data.db)
	current, err := data.AcquireCoordinator(ctx, "instance-a", time.Minute)
	require.NoError(t, err)
	require.True(t, current.Held)
	require.Equal(t, "instance-a", current.InstanceID)
	require.Equal(t, stale.Epoch+2, current.Epoch)

	_, err = data.MaterialiseRoute(
		ctx, fullPlacementView("node-a", "instance-a", stale.Epoch), 1, stale,
	)
	assert.ErrorIs(t, err, biz.ErrCoordinatorLost)

	revision, err := data.MaterialiseRoute(
		ctx, fullPlacementView("node-a", "instance-a", current.Epoch), 1, current,
	)
	require.NoError(t, err)
	assert.EqualValues(t, 1, revision, "the tenure in force still publishes")
}

// seedLivenessRow writes a liveness row with an explicit timestamp, which is how
// a test ages a renewal without waiting out a window.
func seedLivenessRow(t *testing.T, db *gormio.DB, nodeID, instanceID string, seen time.Time) {
	t.Helper()
	require.NoError(t, db.Exec(
		"INSERT INTO sequence_node_liveness (node_id, instance_id, last_seen_at) VALUES (?, ?, ?)",
		nodeID, instanceID, seen,
	).Error)
}

// TestLiveNodesIsTheReadersWindowOverTheRenewalTimestamps pins what a node
// contributes to the liveness decision, which is only the window: who is live is
// decided by comparing stored renewals against the authority's clock, so a
// reader that filtered on anything else -- a process-local staleness check, an
// in-memory registration -- would reintroduce exactly the second source of truth
// the storage lease removes.
func TestLiveNodesIsTheReadersWindowOverTheRenewalTimestamps(t *testing.T) {
	data := NewPlacementData(openPlacementTestDB(t))
	ctx := context.Background()

	now := time.Now().UTC()
	seedLivenessRow(t, data.db, "node-b", "instance-b", now)
	seedLivenessRow(t, data.db, "node-a", "instance-a", now.Add(-time.Minute))
	seedLivenessRow(t, data.db, "node-gone", "instance-gone", now.Add(-time.Hour))

	live, err := data.LiveNodes(ctx, 5*time.Minute)
	require.NoError(t, err)
	require.Len(t, live, 2, "the row outside the window is not live")
	assert.Equal(t, []string{"node-a", "node-b"},
		[]string{live[0].ID, live[1].ID}, "the live set is ordered by node id")
	assert.Equal(t, "instance-a", live[0].InstanceID)
	assert.Equal(t, biz.NodeActive, live[0].State)

	// A fleet nobody is renewing reports empty rather than falling back to the set
	// it last saw: the reader holds no state of its own to fall back to.
	require.NoError(t, data.db.Exec(
		"UPDATE sequence_node_liveness SET last_seen_at = '2000-01-01 00:00:00'",
	).Error)
	live, err = data.LiveNodes(ctx, 30*time.Minute)
	require.NoError(t, err)
	assert.Empty(t, live)

	// Expiry is a read: the rows are all still there, which is what makes the
	// table bounded by the fleet size rather than by history.
	var stored int64
	require.NoError(t, data.db.Table("sequence_node_liveness").Count(&stored).Error)
	assert.EqualValues(t, 3, stored)

	_, err = data.LiveNodes(ctx, 0)
	assert.Error(t, err, "a lapsed window is a caller error, not an empty fleet")
}
