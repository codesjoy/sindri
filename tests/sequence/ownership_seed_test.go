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
	// testPublisherInstanceID is the harness's own publisher identity. It is
	// stable so consecutive publishes renew one tenure rather than racing to
	// take the role from each other.
	testPublisherInstanceID = "sequence-test-publisher"
	// testLayoutVersion is the slot layout every snapshot in these tests is
	// minted under.
	testLayoutVersion = 1
)

// seedSlotOwnership writes the authority view the fleet plans from: the listed
// node's currently live instance holds the listed slots, and every other slot is
// unowned. Using the live instance is required by the ownership foreign key and
// keeps the route's instance fence aligned with the process that is expected to
// serve it.
//
// The instance is resolved through the same live view the planner itself reads
// (liveness joined to a fresh lease), never by trusting a bare ACTIVE lease row.
// Nothing retires a lease when a process dies, so a node that just restarted
// still has its previous instance's row marked ACTIVE until it is reaped; a
// stage that read the lease table alone could therefore hand authority to a dead
// instance and leave the running process to plan the slots away at quota. Going
// through LiveNodes also makes the stage wait for a restarted node to rejoin the
// live set before its slots are handed to it, which is the precondition the
// outage-recovery tests depend on.
//
// The whole view moves in one transaction. A node plans from the table on every
// heartbeat, so a half-applied split would be a view the fleet could act on:
// slots released by one statement would be claimed before the next statement
// assigned them. The stage can still lose a race to a node renewing the rows it
// holds -- the two writes lock the same rows in different orders and the
// storage may pick either one as the deadlock victim -- so the transaction is
// retried. A staged view is not a protocol operation; the retry is the harness
// waiting for a quiet moment, not the fleet relaxing anything.
func seedSlotOwnership(
	t *testing.T,
	db *gorm.DB,
	owners map[string][]uint32,
	lease time.Duration,
	nodeTTL time.Duration,
) {
	t.Helper()
	ctx := context.Background()
	nodeIDs := make([]string, 0, len(owners))
	for nodeID := range owners {
		nodeIDs = append(nodeIDs, nodeID)
	}
	placement := sequencedata.NewPlacementData(db, lease, nodeTTL)
	instances := make(map[string]string, len(owners))
	require.Eventually(t, func() bool {
		live, err := placement.LiveNodes(ctx, nodeTTL)
		if err != nil {
			return false
		}
		clear(instances)
		for _, node := range live {
			instances[node.ID] = node.InstanceID
		}
		for _, nodeID := range nodeIDs {
			if instances[nodeID] == "" {
				return false
			}
		}
		return true
	}, discoveryTestTimeout, 50*time.Millisecond, "seed owners are not live")
	for nodeID := range owners {
		require.NotEmpty(t, instances[nodeID], "node %q is not registered", nodeID)
	}
	stage := func() error {
		return db.Transaction(func(tx *gorm.DB) error {
			// The stage moves authority the way the fleet's own protocol does,
			// because a node's cache is only ever corrected by a revision
			// change: it re-reads the authority when the revision it holds is
			// rejected, and never otherwise. Epochs therefore only advance --
			// rewinding one would leave a node serving from a generation the
			// storage no longer agrees with -- and the same transaction bumps
			// every active instance's revision so each of them re-reads the
			// staged view instead of keeping its previous one.
			if err := tx.Exec(
				"UPDATE sequence_slot_ownership SET owner_instance_id = NULL, " +
					"state = 'UNOWNED'",
			).Error; err != nil {
				return err
			}
			for nodeID, slots := range owners {
				if len(slots) == 0 {
					continue
				}
				if err := tx.Exec(
					"UPDATE sequence_slot_ownership SET owner_instance_id = ?, "+
						"epoch = epoch + 1, state = 'OWNED' "+
						"WHERE slot_id IN ?",
					instances[nodeID], slots,
				).Error; err != nil {
					return err
				}
			}
			return tx.Exec(
				"UPDATE sequence_instance_leases " +
					"SET ownership_revision = ownership_revision + 1 " +
					"WHERE state = 'ACTIVE'",
			).Error
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
// sequence_route_snapshot, so a test that feeds its fleet through here is bound
// by the same materialisation rule the fleet is.
func publishSeededRoute(t *testing.T, db *gorm.DB) int64 {
	t.Helper()
	placement := sequencedata.NewPlacementData(db, time.Minute, time.Minute)
	publisher := biz.NewPublisher(
		biz.ControlPlaneConfig{
			LayoutVersion:     testLayoutVersion,
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
