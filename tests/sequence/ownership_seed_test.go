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
	"log/slog"
	"testing"
	"time"

	"github.com/codesjoy/sindri/internal/sequence/biz"
	sequencedata "github.com/codesjoy/sindri/internal/sequence/data"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// The placement these tests stage used to be an intent written by a control
// plane. It is now the authority itself: slot_ownership is what every node
// plans from, so a test that wants the fleet to serve a particular slot on a
// particular node writes the ownership row and lets the real publisher
// materialise the directory from it.

const (
	// seededInstanceSuffix marks an ownership row a test staged rather than one
	// a node claimed. It must not collide with a live instance id: a node
	// reclaiming its own node id through the restart rule is exactly the path
	// these tests then exercise.
	seededInstanceSuffix = "-seeded"
	// testPublisherInstanceID is the harness's own publisher identity. It is
	// stable so consecutive publishes renew one tenure rather than racing to
	// take the role from each other.
	testPublisherInstanceID = "sequence-test-publisher"
	// testLayoutVersion is the slot layout every snapshot in these tests is
	// minted under.
	testLayoutVersion = 1
)

// seedSlotOwnership writes the authority view the fleet plans from: the listed
// node holds the listed slots with a grant already past the quiet window, and
// every other slot is unowned.
//
// The grant is aged rather than fresh because a claim has to be admissible
// immediately for a test to converge inside its own deadline; nothing in the
// claim predicate is being skipped, only waited out ahead of time.
//
// The whole view moves in one transaction. A node plans from the table on every
// heartbeat, so a half-applied split would be a view the fleet could act on:
// slots released by one statement would be claimed before the next statement
// assigned them. The stage can still lose a race to a node renewing the rows it
// holds -- the two writes lock the same rows in different orders and the
// storage may pick either one as the deadlock victim -- so the transaction is
// retried. A staged view is not a protocol operation; the retry is the harness
// waiting for a quiet moment, not the fleet relaxing anything.
func seedSlotOwnership(t *testing.T, db *gorm.DB, owners map[string][]uint32) {
	t.Helper()
	grantedAt := time.Now().UTC().Add(-time.Hour)
	stage := func() error {
		return db.Transaction(func(tx *gorm.DB) error {
			if err := tx.Exec(
				"UPDATE slot_ownership SET owner_node_id = NULL, owner_instance_id = NULL, " +
					"epoch = 0, granted_at = NULL, state = 'UNOWNED'",
			).Error; err != nil {
				return err
			}
			for nodeID, slots := range owners {
				if len(slots) == 0 {
					continue
				}
				if err := tx.Exec(
					"UPDATE slot_ownership SET owner_node_id = ?, owner_instance_id = ?, "+
						"epoch = epoch + 1, granted_at = ?, state = 'OWNED' "+
						"WHERE slot_id IN ?",
					nodeID, nodeID+seededInstanceSuffix, grantedAt, slots,
				).Error; err != nil {
					return err
				}
			}
			return nil
		})
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		err := stage()
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			require.NoError(t, err, "stage slot ownership")
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// publishSeededRoute runs the real publisher over the authority view and
// returns the revision it published.
//
// It is the publisher path the deployment runs, not a hand-written payload: the
// snapshot is encoded from slot_ownership and the revision comes from
// sequence_route_state, so a test that feeds its fleet through here is bound by
// the same materialisation rule the fleet is.
func publishSeededRoute(t *testing.T, db *gorm.DB) int64 {
	t.Helper()
	placement := sequencedata.NewPlacementData(db)
	publisher := biz.NewPublisher(
		biz.ControlPlaneConfig{
			LayoutVersion:     testLayoutVersion,
			RouteRetention:    64,
			CoordinatorLease:  time.Minute,
			ReconcileInterval: time.Second,
			PassTimeout:       30 * time.Second,
		},
		processQuietWindow,
		testPublisherInstanceID,
		placement,
		placement,
		slog.Default(),
	)
	require.NoError(t, publisher.Pass(context.Background()))
	return publisher.Stats().Revision
}
