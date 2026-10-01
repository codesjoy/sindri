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
	"fmt"
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

func openPlacementTestDB(t testing.TB) *gormio.DB {
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

// fullPlacementSegments builds the complete compact ownership view a
// materialised route has to cover: the encoder refuses a snapshot that does not
// describe the whole space.
func fullPlacementSegments(nodeID, instanceID string, epoch uint64) []biz.OwnershipSegment {
	return []biz.OwnershipSegment{{
		StartSlot:       0,
		EndSlot:         biz.SlotCount - 1,
		OwnerNodeID:     nodeID,
		OwnerInstanceID: instanceID,
		Epoch:           epoch,
		State:           biz.SlotOwned,
		GrantAgeKnown:   true,
	}}
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

	published, err := data.MaterialiseRoute(
		ctx, fullPlacementSegments("node-a", "instance-a", held.Epoch), 1, 64, held,
	)
	require.NoError(t, err)
	assert.EqualValues(t, 1, published.Revision)
	assert.Positive(t, published.PayloadBytes)

	expireCoordinator(t, data.db)
	takeover, err := data.AcquireCoordinator(ctx, "instance-b", time.Minute)
	require.NoError(t, err)
	require.True(t, takeover.Held)

	_, err = data.MaterialiseRoute(
		ctx, fullPlacementSegments("node-b", "instance-b", takeover.Epoch), 1, 64, held,
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
		ctx, fullPlacementSegments("node-a", "instance-a", stale.Epoch), 1, 64, stale,
	)
	assert.ErrorIs(t, err, biz.ErrCoordinatorLost)

	published, err := data.MaterialiseRoute(
		ctx, fullPlacementSegments("node-a", "instance-a", current.Epoch), 1, 64, current,
	)
	require.NoError(t, err)
	assert.EqualValues(t, 1, published.Revision, "the tenure in force still publishes")
}

// openOwnershipSegmentTestDB builds the authority table the compact reader
// aggregates over. The rows are seeded by each test.
func openOwnershipSegmentTestDB(t testing.TB) *gormio.DB {
	t.Helper()
	db := openPlacementTestDB(t)
	require.NoError(t, db.Exec(
		"CREATE TABLE slot_ownership ("+
			"slot_id integer PRIMARY KEY, owner_node_id text, owner_instance_id text, "+
			"epoch integer NOT NULL DEFAULT 0, granted_at datetime, state text NOT NULL, "+
			"updated_at datetime NOT NULL DEFAULT CURRENT_TIMESTAMP)",
	).Error)
	return db
}

// TestOwnershipSegmentsAggregatesRunsAndClassifiesTheQuietWindow pins the
// compact read: consecutive slots sharing an owner story collapse into one run,
// and a run whose grant has aged past the reader's window is split off and
// marked, because the planner decides the two halves differently.
func TestOwnershipSegmentsAggregatesRunsAndClassifiesTheQuietWindow(t *testing.T) {
	data := NewPlacementData(openOwnershipSegmentTestDB(t))
	now := time.Now().UTC()
	seed := func(
		slot uint32,
		node, instance any,
		epoch int64,
		granted any,
		state string,
	) {
		t.Helper()
		require.NoError(t, data.db.Exec(
			"INSERT INTO slot_ownership "+
				"(slot_id, owner_node_id, owner_instance_id, epoch, granted_at, state) "+
				"VALUES (?, ?, ?, ?, ?, ?)",
			slot, node, instance, epoch, granted, state,
		).Error)
	}
	// Slots 0..3 are one fresh run; slot 4 shares the owner story but its grant
	// is old enough to be its own overdue run; slots 5..9 are unowned.
	for slot := uint32(0); slot < 4; slot++ {
		seed(slot, "node-a", "instance-a", 7, now, "OWNED")
	}
	seed(4, "node-a", "instance-a", 7, now.Add(-time.Minute), "OWNED")
	for slot := uint32(5); slot < 10; slot++ {
		seed(slot, nil, nil, 0, nil, "UNOWNED")
	}
	// The rest of the space is unowned; the view has to cover every slot for the
	// reader to accept it.
	require.NoError(t, data.db.Exec(
		"WITH RECURSIVE cnt(x) AS ("+
			"SELECT 10 UNION ALL SELECT x + 1 FROM cnt WHERE x < ?"+
			") INSERT INTO slot_ownership (slot_id, epoch, state) "+
			"SELECT x, 0, 'UNOWNED' FROM cnt",
		biz.SlotCount-1,
	).Error)

	segments, err := data.OwnershipSegments(context.Background(), 30*time.Second)
	require.NoError(t, err)
	require.Len(t, segments, 3)
	assert.Equal(t, uint32(0), segments[0].StartSlot)
	assert.Equal(t, uint32(3), segments[0].EndSlot)
	assert.False(t, segments[0].QuietWindowOverdue)
	assert.Equal(t, uint32(4), segments[1].StartSlot)
	assert.True(t, segments[1].QuietWindowOverdue)
	assert.Equal(t, "node-a", segments[1].OwnerNodeID)
	assert.Equal(t, biz.SlotUnowned, segments[2].State)
	assert.Equal(t, uint32(biz.SlotCount-1), segments[2].EndSlot)

	// The same rows read with a longer window collapse into two runs: the split
	// is a property of the window the reader is planning under, not of storage.
	segments, err = data.OwnershipSegments(context.Background(), 5*time.Minute)
	require.NoError(t, err)
	require.Len(t, segments, 2)
	assert.Equal(t, uint32(4), segments[0].EndSlot)
}

// TestOwnershipSegmentsRefusesAHoleInTheSpace pins the completeness rule on the
// compact read: a short answer is refused rather than planned against, because
// a hole is indistinguishable from a fleet that owns nothing.
func TestOwnershipSegmentsRefusesAHoleInTheSpace(t *testing.T) {
	data := NewPlacementData(openOwnershipSegmentTestDB(t))
	require.NoError(t, data.db.Exec(
		"INSERT INTO slot_ownership (slot_id, epoch, state) VALUES (0, 0, 'UNOWNED')",
	).Error)
	_, err := data.OwnershipSegments(context.Background(), time.Second)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "covers")
}

// BenchmarkOwnershipSegmentsAtHundredNodeSteadyState pins the capacity shape the
// compact read exists for: a 100-node fleet holding the space in even shares is
// read back as one run per node (plus the remainder) rather than SlotCount rows.
// The returned segment count is reported as a metric so a regression that
// re-reads the table per slot shows up as rows/op, not just as latency.
func BenchmarkOwnershipSegmentsAtHundredNodeSteadyState(b *testing.B) {
	data := NewPlacementData(openOwnershipSegmentTestDB(b))
	require.NoError(b, data.db.Exec(
		"WITH RECURSIVE cnt(x) AS ("+
			"SELECT 0 UNION ALL SELECT x + 1 FROM cnt WHERE x < ?"+
			") INSERT INTO slot_ownership (slot_id, epoch, state) "+
			"SELECT x, 0, 'UNOWNED' FROM cnt",
		biz.SlotCount-1,
	).Error)

	const nodes = 100
	perNode := uint32(biz.SlotCount / nodes)
	now := time.Now().UTC()
	for node := 0; node < nodes; node++ {
		from := uint32(node) * perNode
		to := from + perNode - 1
		require.NoError(b, data.db.Exec(
			"UPDATE slot_ownership SET owner_node_id = ?, owner_instance_id = ?, "+
				"epoch = 1, granted_at = ?, state = 'OWNED' "+
				"WHERE slot_id BETWEEN ? AND ?",
			fmt.Sprintf("node-%03d", node),
			fmt.Sprintf("instance-%03d", node),
			now,
			from,
			to,
		).Error)
	}

	segments := 0
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		view, err := data.OwnershipSegments(context.Background(), time.Minute)
		if err != nil {
			b.Fatal(err)
		}
		segments = len(view)
		// One run per node, plus the unowned remainder of the even split.
		if segments > nodes+1 {
			b.Fatalf("compact read returned %d runs for %d nodes", segments, nodes)
		}
	}
	b.ReportMetric(float64(segments), "segments/op")
}

// TestMaterialiseRouteKeepsTheRouteTableBounded pins the retention bound: every
// publish -- including the ones that mint a revision -- prunes the rows older
// than the newest retention revisions.
func TestMaterialiseRouteKeepsTheRouteTableBounded(t *testing.T) {
	data := NewPlacementData(openPlacementTestDB(t))
	ctx := context.Background()
	held, err := data.AcquireCoordinator(ctx, "instance-a", time.Minute)
	require.NoError(t, err)
	require.True(t, held.Held)

	const retention = 3
	for epoch := uint64(1); epoch <= 5; epoch++ {
		published, publishErr := data.MaterialiseRoute(
			ctx,
			fullPlacementSegments("node-a", "instance-a", epoch),
			1,
			retention,
			held,
		)
		require.NoError(t, publishErr)
		assert.EqualValues(t, epoch, published.Revision)
	}

	var routes int64
	require.NoError(t, data.db.Table("sequence_routes").Count(&routes).Error)
	assert.EqualValues(t, retention, routes)

	var oldest int64
	require.NoError(t, data.db.Table("sequence_routes").
		Select("MIN(version)").Scan(&oldest).Error)
	assert.EqualValues(t, 5-retention+1, oldest, "the newest revisions are kept")

	// A payload identical to the newest one returns that revision and still
	// enforces the bound, so a fleet that never changes again cannot grow the
	// table.
	repeated, err := data.MaterialiseRoute(
		ctx,
		fullPlacementSegments("node-a", "instance-a", 5),
		1,
		retention,
		held,
	)
	require.NoError(t, err)
	assert.EqualValues(t, 5, repeated.Revision)
	require.NoError(t, data.db.Table("sequence_routes").Count(&routes).Error)
	assert.EqualValues(t, retention, routes)
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
