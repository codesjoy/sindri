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

	sequencedata "github.com/codesjoy/sindri/internal/sequence/data"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	gorm "gorm.io/gorm"
)

// livenessRow is one node's lease as the raw table holds it, so a test can assert
// on the stored instance id rather than on what a reader chose to report.
type livenessRow struct {
	NodeID     string    `gorm:"column:node_id"`
	InstanceID string    `gorm:"column:instance_id"`
	LastSeenAt time.Time `gorm:"column:last_seen_at"`
}

func loadLiveness(t *testing.T, db *gorm.DB, nodeID string) livenessRow {
	t.Helper()
	var row livenessRow
	err := db.Table("sequence_node_liveness").Where("node_id = ?", nodeID).Take(&row).Error
	require.NoError(t, err)
	return row
}

// countLivenessRows counts one node's rows rather than the table's: the harness
// database is shared with the other tests in this package, and every one of them
// that runs a node writes its own liveness row.
func countLivenessRows(t *testing.T, db *gorm.DB, nodeID string) int64 {
	t.Helper()
	var count int64
	require.NoError(t, db.Table("sequence_node_liveness").
		Where("node_id = ?", nodeID).
		Count(&count).Error)
	return count
}

// TestNodeLivenessLeaseContractAcrossDialects pins the node's whole side of the
// liveness contract against a real server, because every part of it is spelled per
// dialect: the upsert is PostgreSQL's ON CONFLICT against MySQL's ON DUPLICATE
// KEY, and the timestamp is written by the storage clock rather than bound by the
// process.
//
// The timestamp is the part worth testing here. The control plane decides who is
// live by comparing it against the same server's clock, so a writer that bound its
// own would put the node host's clock into a membership decision, and the two
// sides would disagree by however far that host had drifted.
func TestNodeLivenessLeaseContractAcrossDialects(t *testing.T) {
	forEachDialect(t, func(t *testing.T, item *harness) {
		db := openGORM(t, item)
		repo := sequencedata.NewLivenessData(db, 15*time.Second)
		ctx := context.Background()

		const nodeID = "node-liveness-contract"
		require.NoError(t, db.Exec(
			"DELETE FROM sequence_node_liveness WHERE node_id = ?", nodeID,
		).Error)

		// The first report has no row to update, which is the case a node
		// starting up produces and the one an update-only writer would fail.
		require.NoError(t, repo.RenewLiveness(ctx, nodeID, "instance-first"))
		first := loadLiveness(t, db, nodeID)
		require.Equal(t, "instance-first", first.InstanceID)
		require.WithinDuration(t, time.Now(), first.LastSeenAt, time.Minute)

		// A later renewal is the same statement and must not add a second row:
		// node_id is the key, which is what keeps the table bounded by the
		// fleet size and the expiry a pure read.
		require.NoError(t, repo.RenewLiveness(ctx, nodeID, "instance-first"))
		require.EqualValues(t, 1, countLivenessRows(t, db, nodeID))

		// A restarted node comes back under the same id with a new process
		// identity, and the row follows it rather than doubling.
		require.NoError(t, repo.RenewLiveness(ctx, nodeID, "instance-second"))
		require.EqualValues(t, 1, countLivenessRows(t, db, nodeID))
		assert.Equal(t, "instance-second", loadLiveness(t, db, nodeID).InstanceID)

		// The renewal moves the timestamp forward, which is the whole of what
		// keeps the node inside the reader's window.
		require.NoError(t, db.Exec(
			"UPDATE sequence_node_liveness SET last_seen_at = '2000-01-01 00:00:00' "+
				"WHERE node_id = ?",
			nodeID,
		).Error)
		require.NoError(t, repo.RenewLiveness(ctx, nodeID, "instance-second"))
		assert.WithinDuration(
			t, time.Now(), loadLiveness(t, db, nodeID).LastSeenAt, time.Minute,
		)

		// The graceful-shutdown write is scoped to the instance, so a shutdown
		// racing a restart cannot delete the row the new process just wrote.
		require.NoError(t, repo.DropLiveness(ctx, nodeID, "instance-old"))
		require.EqualValues(t, 1, countLivenessRows(t, db, nodeID),
			"a stale shutdown must not delete its successor's row")

		require.NoError(t, repo.DropLiveness(ctx, nodeID, "instance-second"))
		assert.Zero(t, countLivenessRows(t, db, nodeID))
	})
}
