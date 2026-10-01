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
	gormio "gorm.io/gorm"
)

// These tests run on SQLite because the grouped renewal is about the shape of
// the predicate, not about dialect lock semantics; the PostgreSQL and MySQL
// forms are exercised by the dialect contract tests.

func openOwnershipTestDB(t *testing.T) *gormio.DB {
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

// seedOwnedSlot writes one authority row with the columns a renewal compares.
func seedOwnedSlot(
	t *testing.T,
	db *gormio.DB,
	slot uint32,
	instanceID string,
	epoch int64,
	grantedAt time.Time,
) {
	t.Helper()
	require.NoError(t, db.Exec(
		"INSERT INTO slot_ownership "+
			"(slot_id, owner_node_id, owner_instance_id, epoch, granted_at, state) "+
			"VALUES (?, 'node-a', ?, ?, ?, 'OWNED')",
		slot, instanceID, epoch, grantedAt,
	).Error)
}

// grantTime reads the stored grant time for one slot.
func grantTime(t *testing.T, db *gormio.DB, slot uint32) time.Time {
	t.Helper()
	var row struct {
		GrantedAt *time.Time
	}
	require.NoError(t, db.Raw(
		"SELECT granted_at FROM slot_ownership WHERE slot_id = ?", slot,
	).Scan(&row).Error)
	require.NotNil(t, row.GrantedAt)
	return *row.GrantedAt
}

// TestRenewSlotsRefreshesOnlyTheMatchingGroup pins the epoch-CAS renewal: one
// statement covers a whole group, only rows still held by that instance at that
// epoch come back, and a slot whose owner or epoch moved is left untouched so a
// renewal can never resurrect authority a takeover replaced.
func TestRenewSlotsRefreshesOnlyTheMatchingGroup(t *testing.T) {
	db := openOwnershipTestDB(t)
	data := NewOwnershipData(db)
	ctx := context.Background()
	old := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	seedOwnedSlot(t, db, 0, "instance-a", 1, old)
	seedOwnedSlot(t, db, 1, "instance-a", 1, old)
	seedOwnedSlot(t, db, 2, "instance-b", 2, old)
	seedOwnedSlot(t, db, 3, "instance-a", 3, old)

	renewed, err := data.RenewSlots(ctx, biz.RenewRequest{
		Groups: []biz.RenewGroup{
			{InstanceID: "instance-a", Epoch: 1, Slots: []uint32{0, 1, 2, 3}},
		},
	})
	require.NoError(t, err)
	require.Len(t, renewed, 2)
	assert.Equal(t, []uint32{0, 1}, []uint32{renewed[0].SlotID, renewed[1].SlotID})

	// Only the matching rows were refreshed: the other two keep their original
	// grant times, which is what leaves them to be taken over by the fleet.
	assert.WithinDuration(t, time.Now(), grantTime(t, db, 0), time.Minute)
	assert.WithinDuration(t, time.Now(), grantTime(t, db, 1), time.Minute)
	assert.Equal(t, old, grantTime(t, db, 2))
	assert.Equal(t, old, grantTime(t, db, 3))
}

// TestRenewSlotsReportsOnlyRowsStillHeld covers the fence a renewal doubles as:
// a slot the caller no longer holds does not come back, and the allocator reads
// that as "fence this slot" rather than re-arming a deadline it cannot honour.
func TestRenewSlotsReportsOnlyRowsStillHeld(t *testing.T) {
	db := openOwnershipTestDB(t)
	data := NewOwnershipData(db)
	ctx := context.Background()
	old := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	seedOwnedSlot(t, db, 0, "instance-b", 4, old)

	renewed, err := data.RenewSlots(ctx, biz.RenewRequest{
		Groups: []biz.RenewGroup{
			{InstanceID: "instance-a", Epoch: 1, Slots: []uint32{0}},
		},
	})
	require.NoError(t, err)
	assert.Empty(t, renewed, "a slot another instance took over is not renewed")
	assert.Equal(t, old, grantTime(t, db, 0))
}

// TestRenewSlotsRejectsAnEmptyInstance pins that a group without an identity is
// refused rather than renewing rows a wildcard predicate would match.
func TestRenewSlotsRejectsAnEmptyInstance(t *testing.T) {
	db := openOwnershipTestDB(t)
	data := NewOwnershipData(db)
	_, err := data.RenewSlots(context.Background(), biz.RenewRequest{
		Groups: []biz.RenewGroup{
			{Epoch: 1, Slots: []uint32{0}},
		},
	})
	require.Error(t, err)
}
