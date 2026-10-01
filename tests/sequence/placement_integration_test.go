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
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/codesjoy/sindri/internal/sequence/biz"
	sequencedata "github.com/codesjoy/sindri/internal/sequence/data"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// routeRetentionForTest is the directory bound these tests publish under. The
// value only has to be positive here: retention is exercised where it is
// decided, and the contract tests pin the publish protocol rather than the
// deployment's configured horizon.
const routeRetentionForTest = 64

// TestCoordinatorElectionAdmitsOneWinnerAcrossDialects is the election's
// single-publisher rule under real contention.
//
// The unit tests show that the statements say the right thing on one connection.
// What can only be shown here is that the row lock and the predicate hold when
// several replicas race: a takeover is a conditional write, so the losers block,
// re-evaluate against the committed row, and find it taken. A read-then-write
// election would pass the unit tests and admit two winners here.
func TestCoordinatorElectionAdmitsOneWinnerAcrossDialects(t *testing.T) {
	for _, item := range harnesses {
		t.Run(item.name, func(t *testing.T) {
			data, db := setupPlacement(t, item)
			startEpoch := coordinatorEpoch(t, db)

			const contenders = 8
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			results := make([]biz.CoordinatorLease, contenders)
			errs := make([]error, contenders)
			start := make(chan struct{})
			var wait sync.WaitGroup
			for index := range contenders {
				wait.Add(1)
				go func() {
					defer wait.Done()
					instanceID := fmt.Sprintf("instance-%d", index)
					// All replicas start together, so the election is decided by
					// the row and not by whichever goroutine was scheduled first.
					<-start
					results[index], errs[index] = data.AcquireCoordinator(
						ctx,
						instanceID,
						time.Minute,
					)
				}()
			}
			close(start)
			wait.Wait()

			winners := 0
			for index := range contenders {
				require.NoError(t, errs[index])
				lease := results[index]
				if !lease.Held {
					assert.Zero(
						t,
						lease.Epoch,
						"a replica that did not hold the role has no tenure",
					)
					continue
				}
				winners++
				assert.Equal(t, fmt.Sprintf("instance-%d", index), lease.InstanceID)
				assert.Equal(t, startEpoch+1, lease.Epoch,
					"one handover moves the fencing token exactly once")
			}
			require.Equal(t, 1, winners, "the election must admit exactly one replica")

			// The row is the authority for who won, so it has to agree with the
			// lease the winner was told it held.
			assert.Equal(t, results[winnerIndex(results)].InstanceID, coordinatorOwner(t, db))
			assert.Equal(t, startEpoch+1, coordinatorEpoch(t, db))
		})
	}
}

// TestCoordinatorTenureIsAFencingTokenAcrossDialects pins what the epoch means
// on a real server: it moves when the role changes hands and not otherwise, and
// a write presenting a tenure that is no longer in force is refused.
//
// The refusals are the reason this test exists next to the unit ones: they are
// decided by `<storage clock> >= now` comparisons and by an epoch read back from
// a bigint, both of which are spelled per dialect.
func TestCoordinatorTenureIsAFencingTokenAcrossDialects(t *testing.T) {
	for _, item := range harnesses {
		t.Run(item.name, func(t *testing.T) {
			data, db := setupPlacement(t, item)
			ctx := context.Background()

			first, err := data.AcquireCoordinator(ctx, "instance-a", time.Minute)
			require.NoError(t, err)
			require.True(t, first.Held)

			renewed, err := data.AcquireCoordinator(ctx, "instance-a", time.Minute)
			require.NoError(t, err)
			require.True(t, renewed.Held)
			assert.Equal(t, first.Epoch, renewed.Epoch, "a renewal is not a handover")

			// Nothing published yet, and the tenure in force may publish.
			published, err := data.MaterialiseRoute(
				ctx, fullSegments("node-a", "instance-a", first.Epoch), 1,
				routeRetentionForTest, renewed,
			)
			require.NoError(t, err)
			require.NotZero(t, published.Revision)

			// A live lease is not takeable, so the epoch does not move.
			other, err := data.AcquireCoordinator(ctx, "instance-b", time.Minute)
			require.NoError(t, err)
			assert.False(t, other.Held)
			assert.Equal(t, first.Epoch, coordinatorEpoch(t, db))

			// Once the lease lapses the role moves, and with it the epoch.
			expireCoordinator(t, db)
			taken, err := data.AcquireCoordinator(ctx, "instance-b", time.Minute)
			require.NoError(t, err)
			require.True(t, taken.Held)
			assert.Equal(t, first.Epoch+1, taken.Epoch)

			// The old tenure is refused against the new one even though its writer
			// never saw the takeover: the epoch is what fences it.
			_, err = data.MaterialiseRoute(
				ctx, fullSegments("node-b", "instance-b", taken.Epoch), 1,
				routeRetentionForTest, first,
			)
			require.ErrorIs(t, err, biz.ErrCoordinatorLost)

			_, err = data.MaterialiseRoute(
				ctx, fullSegments("node-b", "instance-b", taken.Epoch), 1,
				routeRetentionForTest, taken,
			)
			require.NoError(t, err)
		})
	}
}

// TestPlacementWriteHoldsTheRowUntilItCommitsAcrossDialects pins the premise the
// guard rests on: a takeover cannot commit while a publish is holding the
// coordinator row.
//
// That is why the guard takes the row with a write rather than reading it. The
// lock here is taken with an ordinary statement instead of the guard's own SQL,
// because what is being tested is the database's behaviour -- a row locked by an
// open transaction keeps another transaction out of it -- and not a second copy
// of the predicate.
func TestPlacementWriteHoldsTheRowUntilItCommitsAcrossDialects(t *testing.T) {
	for _, item := range harnesses {
		t.Run(item.name, func(t *testing.T) {
			data, db := setupPlacement(t, item)
			ctx := context.Background()

			held, err := data.AcquireCoordinator(ctx, "instance-a", time.Minute)
			require.NoError(t, err)
			require.True(t, held.Held)

			// The lease has to have lapsed, or a takeover would be refused by the
			// predicate without ever touching the row and the lock would go
			// untested.
			expireCoordinator(t, db)

			// A second connection holds the row open, the way a pass does between
			// its guard and its commit.
			blocker := db.Begin()
			require.NoError(t, blocker.Error)
			require.NoError(t, blocker.Exec(
				"UPDATE sequence_coordinator SET updated_at = updated_at WHERE id = 1",
			).Error)

			// A takeover now has to wait for that transaction, so it cannot
			// commit in the middle of the write. The deadline stands in for
			// "waited long enough": a takeover that committed regardless would
			// come back held, and one that behaves as it must comes back as an
			// error.
			takeoverCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			defer cancel()
			_, err = data.AcquireCoordinator(takeoverCtx, "instance-b", time.Minute)
			require.Error(t, err,
				"a takeover must not be able to commit while a write holds the row")

			// The blocked takeover wrote nothing, so the row still names the
			// tenure it was taken over from.
			assert.Equal(t, "instance-a", coordinatorOwner(t, db))
			assert.Equal(t, held.Epoch, coordinatorEpoch(t, db))

			require.NoError(t, blocker.Rollback().Error)

			taken, err := data.AcquireCoordinator(ctx, "instance-b", time.Minute)
			require.NoError(t, err)
			require.True(t, taken.Held, "the row is takeable again once the write released it")
			assert.Equal(t, held.Epoch+1, taken.Epoch)
			assert.Equal(t, "instance-b", coordinatorOwner(t, db))
		})
	}
}

// TestPublisherPassPublishesTheDirectoryAcrossDialects runs the real publisher
// against the real store: the authority view is read, encoded, and written as a
// new revision of the directory the nodes poll.
//
// It is the whole control-plane loop in one test, on both dialects, with nothing
// faked. The pass decides no placement and reads no liveness: the directory is
// the authority view itself, so even a node that has stopped renewing appears in
// it with the slots it still owns. The second pass is the part a unit test cannot
// show: a view that did not change must not publish a new revision.
func TestPublisherPassPublishesTheDirectoryAcrossDialects(t *testing.T) {
	for _, item := range harnesses {
		t.Run(item.name, func(t *testing.T) {
			data, db := setupPlacement(t, item)
			seedFleetOwnership(t, db)
			ctx := context.Background()

			publisher := biz.NewPublisher(
				biz.ControlPlaneConfig{
					LayoutVersion:     1,
					RouteRetention:    routeRetentionForTest,
					CoordinatorLease:  time.Minute,
					ReconcileInterval: time.Minute,
					PassTimeout:       30 * time.Second,
				},
				time.Minute,
				"instance-a",
				data,
				data,
				slog.Default(),
			)

			require.NoError(t, publisher.Pass(ctx))

			// The directory carries the authority view: node-a holds 0-99 and
			// node-b holds 100-199, and the rest are unowned. Nothing here decided
			// who should own what -- that is the nodes' own plan.
			stats := publisher.Stats()
			assert.EqualValues(t, biz.SlotCount-200, stats.UnownedSlots)

			published := publishedRoute(t, db)
			require.NotNil(t, published)
			assert.Equal(t, stats.Revision, published.Version)
			assert.Equal(t, int64(1), published.LayoutVersion)
			assert.Equal(t, map[string]int{
				"node-a": 100,
				"node-b": 100,
			}, slotsByNode(published))

			// Nothing about the authority changed, so the second pass has nothing
			// to write and nothing to publish. This is the assertion that catches a
			// payload comparison done on bytes: a jsonb column re-serialises what it
			// stores, so a byte comparison would republish the directory on every
			// pass.
			require.NoError(t, publisher.Pass(ctx))
			stats = publisher.Stats()
			assert.EqualValues(t, 2, stats.Passes)
			assert.EqualValues(t, 1, routeCount(t, db), "a stable view publishes no revision")
			assert.Equal(t, published.Version, stats.Revision)
		})
	}
}

// TestLivenessLeasesExpireAcrossDialects is the reader's half of the liveness
// contract on a real server: the live set is whatever the reader's window
// admits, and the clock that decides it is the authority's rather than the
// reader host's.
//
// The unit tests cannot show this. They compare a value the test wrote to a
// value the test passed in; what has to hold here is that the cutoff is produced
// by the server on both dialects, which spell that arithmetic differently.
func TestLivenessLeasesExpireAcrossDialects(t *testing.T) {
	for _, item := range harnesses {
		t.Run(item.name, func(t *testing.T) {
			data, db := setupPlacement(t, item)
			ctx := context.Background()

			// One node renewing now and one whose last renewal is an hour old.
			seedLiveness(t, db, "node-a", "instance-a", time.Now().UTC())
			seedLiveness(t, db, "node-b", "instance-b", time.Now().UTC().Add(-time.Hour))

			live, err := data.LiveNodes(ctx, time.Minute)
			require.NoError(t, err)
			require.Len(t, live, 1, "a renewal an hour old is outside a one-minute window")
			assert.Equal(t, "node-a", live[0].ID)
			assert.Equal(t, "instance-a", live[0].InstanceID)
			assert.WithinDuration(t, time.Now().UTC(), live[0].LastSeenAt, time.Minute)

			// The same rows under a window that covers both. Nothing about the rows
			// changed -- only the comparison -- which is what makes the expiry the
			// reader's decision rather than a property of the write.
			live, err = data.LiveNodes(ctx, 2*time.Hour)
			require.NoError(t, err)
			require.Len(t, live, 2)
			assert.Equal(t, []string{"node-a", "node-b"},
				[]string{live[0].ID, live[1].ID}, "the live set is ordered by node id")

			// An expired row is left where it is: there is no sweep, and the table
			// is bounded by the fleet size because node_id is the key.
			expireLiveness(t, db, "node-a")
			live, err = data.LiveNodes(ctx, time.Minute)
			require.NoError(t, err)
			assert.Empty(t, live)
			assert.EqualValues(t, 2, livenessCount(t, db), "expiry deletes nothing")
		})
	}
}

func winnerIndex(results []biz.CoordinatorLease) int {
	for index, lease := range results {
		if lease.Held {
			return index
		}
	}
	return 0
}

// fullSegments is the complete ownership view a materialised route has to cover,
// expressed the way the planner and publisher read it: one run covering every
// slot.
func fullSegments(nodeID, instanceID string, epoch uint64) []biz.OwnershipSegment {
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

// seedFleetOwnership stages the authority the pass is run against: node-a
// holding slots 0-99 with a fresh grant, node-b holding 100-199 with a grant
// older than the quiet window, and the rest unowned, which is how the migration
// seeds them.
func seedFleetOwnership(t *testing.T, db *gorm.DB) {
	t.Helper()
	now := time.Now().UTC()
	for _, seed := range []struct {
		from, to   int
		nodeID     string
		instanceID string
		grantedAt  time.Time
	}{
		{from: 0, to: 99, nodeID: "node-a", instanceID: "instance-a", grantedAt: now},
		{
			from: 100, to: 199, nodeID: "node-b", instanceID: "instance-b",
			grantedAt: now.Add(-time.Minute),
		},
	} {
		require.NoError(t, db.Exec(
			"UPDATE slot_ownership SET owner_node_id = ?, owner_instance_id = ?, "+
				"epoch = 1, granted_at = ?, state = 'OWNED' "+
				"WHERE slot_id BETWEEN ? AND ?",
			seed.nodeID, seed.instanceID, seed.grantedAt, seed.from, seed.to,
		).Error)
	}
}

// seedLiveness writes one node's liveness lease as the node's own renewal would:
// the row is keyed by node id and the timestamp is supplied here only because a
// test cannot wait out a window to age one.
func seedLiveness(t *testing.T, db *gorm.DB, nodeID, instanceID string, lastSeenAt time.Time) {
	t.Helper()
	require.NoError(t, db.Exec(
		"INSERT INTO sequence_node_liveness (node_id, instance_id, last_seen_at) "+
			"VALUES (?, ?, ?)",
		nodeID, instanceID, lastSeenAt,
	).Error)
}

// expireLiveness ages a node's renewal past any window a test uses, which is how
// a crashed node looks to the reader.
func expireLiveness(t *testing.T, db *gorm.DB, nodeID string) {
	t.Helper()
	require.NoError(t, db.Exec(
		"UPDATE sequence_node_liveness SET last_seen_at = '2000-01-01 00:00:00' "+
			"WHERE node_id = ?",
		nodeID,
	).Error)
}

func livenessCount(t *testing.T, db *gorm.DB) int64 {
	t.Helper()
	var count int64
	require.NoError(t, db.Table("sequence_node_liveness").Count(&count).Error)
	return count
}

// setupPlacement opens the placement storage and returns it with the raw handle
// the tests assert against, with everything the tests write put back to the
// state the migrations leave it in.
//
// The whole package shares one database per dialect -- one container for the
// package, which is what keeps the run short -- so a test that left a published
// route, a liveness lease or a tenure behind would change what the next one
// sees. The migration's state is the honest starting point: no directory, no
// live nodes, no tenure, and the revision counter at the first value this writer
// may publish.
func setupPlacement(t *testing.T, item *harness) (*sequencedata.PlacementData, *gorm.DB) {
	t.Helper()
	db := openGORM(t, item)
	data := sequencedata.NewPlacementData(db)
	require.NoError(t, db.Exec("DELETE FROM sequence_routes").Error)
	require.NoError(t, db.Exec("UPDATE sequence_route_state SET revision = 1").Error)
	require.NoError(t, db.Exec("DELETE FROM sequence_node_liveness").Error)
	// Ownership is authority, not a scratch table: a row a previous test left
	// behind is a placement every reader would act on. Each test starts from the
	// migration's state -- every slot UNOWNED -- and seeds what it needs.
	require.NoError(t, db.Exec(
		"UPDATE slot_ownership SET owner_node_id = NULL, owner_instance_id = NULL, "+
			"epoch = 0, granted_at = NULL, state = 'UNOWNED'",
	).Error)
	freeCoordinator(t, db)
	return data, db
}

// freeCoordinator puts the coordinator row back to the state the migration
// leaves it in, so a test starts with no tenure in force whatever the tests
// before it did with the role. The epoch is left alone: it is the fencing token,
// and only a handover may move it.
func freeCoordinator(t *testing.T, db *gorm.DB) {
	t.Helper()
	require.NoError(t, db.Exec(
		"UPDATE sequence_coordinator SET owner_instance_id = NULL, expires_at = NULL WHERE id = 1",
	).Error)
}

// expireCoordinator moves the lease's expiry into the past, which is how a test
// stages a takeover without waiting one out.
func expireCoordinator(t *testing.T, db *gorm.DB) {
	t.Helper()
	require.NoError(t, db.Exec(
		"UPDATE sequence_coordinator SET expires_at = '2000-01-01 00:00:00' WHERE id = 1",
	).Error)
}

func coordinatorOwner(t *testing.T, db *gorm.DB) string {
	t.Helper()
	var row struct {
		Owner sql.NullString `gorm:"column:owner_instance_id"`
	}
	require.NoError(t, db.Raw(
		"SELECT owner_instance_id FROM sequence_coordinator WHERE id = 1",
	).Scan(&row).Error)
	return row.Owner.String
}

func coordinatorEpoch(t *testing.T, db *gorm.DB) uint64 {
	t.Helper()
	var row struct {
		Epoch uint64 `gorm:"column:epoch"`
	}
	require.NoError(t, db.Raw(
		"SELECT epoch FROM sequence_coordinator WHERE id = 1",
	).Scan(&row).Error)
	return row.Epoch
}

func routeCount(t *testing.T, db *gorm.DB) int64 {
	t.Helper()
	var count int64
	require.NoError(t, db.Table("sequence_routes").Count(&count).Error)
	return count
}

// publishedRoute reads the newest directory back the way a node does, so the test
// asserts against what the fleet would actually follow rather than against the
// payload the writer handed it.
func publishedRoute(t *testing.T, db *gorm.DB) *biz.Route {
	t.Helper()
	var row struct {
		Version int64  `gorm:"column:version"`
		Payload []byte `gorm:"column:payload"`
	}
	err := db.Table("sequence_routes").
		Select("version, payload").
		Order("version DESC").
		Take(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil
	}
	require.NoError(t, err)
	route, err := biz.DecodeRoute(row.Version, row.Payload)
	require.NoError(t, err)
	return route
}

func slotsByNode(route *biz.Route) map[string]int {
	slots := make(map[string]int, len(route.Nodes))
	for _, node := range route.Nodes {
		slots[node.NodeID] = len(node.Slots)
	}
	return slots
}
