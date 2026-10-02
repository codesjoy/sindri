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
	"fmt"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/codesjoy/pkg/basic/xerror"
	"github.com/codesjoy/sindri/gen/go/sequence/reason"
	sequencev1 "github.com/codesjoy/sindri/gen/go/sequence/v1"
	testkit "github.com/codesjoy/sindri/internal/pkg/tests"
	"github.com/codesjoy/sindri/internal/sequence/biz"
	sequencedata "github.com/codesjoy/sindri/internal/sequence/data"
	sequencepkg "github.com/codesjoy/sindri/pkg/sequence"
	yapp "github.com/codesjoy/yggdrasil/v3/app"
	"github.com/codesjoy/yggdrasil/v3/config"
	"github.com/codesjoy/yggdrasil/v3/config/source/memory"
	"github.com/codesjoy/yggdrasil/v3/discovery/resolver"
	"github.com/codesjoy/yggdrasil/v3/rpc/metadata"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// migrationMinimumTransfers is how many slots the test waits to see moved
// before it freezes the planner. It is above one executor batch so both the
// planning and the execution loops have to take part, and small enough that the
// release half of the test stays bounded.
const migrationMinimumTransfers = 128

// TestControlPlaneMigrationAndReleaseAcrossDialects runs the deployment shape
// the control plane exists for against real processes on a real database:
// a data node holds the whole slot space, a control-mode process publishes the
// directory and plans quota migrations, and a second data node joins to trigger
// the rebalance. It then stops the migrated node twice: once gracefully, where
// the process releases its own authority, and once with SIGKILL, where the
// release it had started is completed by the control plane's recovery pass.
func TestControlPlaneMigrationAndReleaseAcrossDialects(t *testing.T) {
	forEachDialect(t, func(t *testing.T, database *harness) {
		runControlPlaneMigrationAndRelease(t, database)
	})
}

func runControlPlaneMigrationAndRelease(t *testing.T, database *harness) {
	t.Helper()
	require.NoError(t, resetSequenceDatabase(t, database))
	db := openGORM(t, database)

	const (
		nodeA          = "migration-node-a"
		nodeB          = "migration-node-b"
		controlNode    = "migration-control"
		controlNodeTwo = "migration-control-recovery"
	)
	recorder := newAllocationRecorder()

	// A is the only node at first, and it is staged as the owner of every slot.
	// The data plane does not need a route to claim what its own plan reads, so
	// the authority is live before the directory is.
	portA := freeTCPPort(t)
	processA := startSequenceProcess(t, sequenceProcessOptions{
		database: database, nodeID: nodeA, configuredMode: "data", grpcPort: portA,
	})
	waitForNodesLive(t, db, nodeA)
	clientA := dialSequenceNode(t, "migration-client-a", fmt.Sprintf("127.0.0.1:%d", portA))
	seedSlotOwnership(t, db, allSlots(nodeA), processLeaseDuration, processNodeTTL)
	instanceA := instanceForNode(t, db, nodeA)

	// The control replica publishes the directory. Planning cannot start yet:
	// a single live node has no surplus to move.
	startControl := func(controlID string) *sequenceProcess {
		control := startSequenceProcess(t, sequenceProcessOptions{
			database:       database,
			nodeID:         controlID,
			configuredMode: "control",
			migration: &sequenceMigrationOptions{
				joinStabilityWindow: 500 * time.Millisecond,
				batchSlots:          64,
			},
		})
		waitControlReady(t, control)
		return control
	}
	control := startControl(controlNode)
	version := waitRouteVersion(t, db)

	// A serves the tracked key before any transfer is plannable, so the id it
	// returns is the continuity floor the migration must not rewind.
	trackedSlot := uint32(0)
	trackedKey := keyForSlot(t, trackedSlot)
	beforeMigration := serveKey(t, recorder, clientA, trackedKey, version)
	keptKey := keyForSlot(t, 15000)
	keptBefore := serveKey(t, recorder, clientA, keptKey, version)
	beforeTransferEpoch := epochOfSlot(ownedSlotsOf(t, db, instanceA), trackedSlot)
	require.NotZero(t, beforeTransferEpoch,
		"the source generation must exist before the transfer")

	// B joins; the control plane plans the transfers and the data planes execute
	// them through the handoff state machine.
	portB := freeTCPPort(t)
	processB := startSequenceProcess(t, sequenceProcessOptions{
		database: database, nodeID: nodeB, configuredMode: "data", grpcPort: portB,
	})
	waitForNodesLive(t, db, nodeB)
	clientB := dialSequenceNode(t, "migration-client-b", fmt.Sprintf("127.0.0.1:%d", portB))
	instanceB := instanceForNode(t, db, nodeB)

	require.Eventually(t, func() bool {
		count, err := ownedSlotCount(db, instanceB)
		return err == nil && count >= migrationMinimumTransfers
	}, discoveryTestTimeout, 25*time.Millisecond,
		"the control plane never rebalanced quota onto the joining node")

	// Freeze the planner. The handoffs it already planned keep executing on the
	// data planes' own passes; no further quota movement can arrive.
	stopProcess(t, control)
	require.Eventually(t, func() bool {
		count, err := activeHandoffCount(db)
		return err == nil && count == 0
	}, discoveryTestTimeout, 25*time.Millisecond, "planned handoffs never settled")
	// The join published newer route revisions; requests carrying the pre-join
	// version are refused as expired, so serve the settled state with the
	// version the data planes have caught up to.
	version = waitRouteVersion(t, db)

	transferred := ownedSlotsOf(t, db, instanceB)
	require.GreaterOrEqual(t, len(transferred), migrationMinimumTransfers)
	require.Contains(t, slotSetOf(transferred), trackedSlot,
		"the first planned slot was not migrated")
	migratedEpoch := epochOfSlot(transferred, trackedSlot)
	assert.EqualValues(t, beforeTransferEpoch+1, migratedEpoch,
		"a transfer must start exactly one new epoch")
	assert.GreaterOrEqual(t, mustCompletedHandoffs(t, db, "TRANSFER"), int64(len(transferred)),
		"every migrated slot leaves one completed transfer behind")

	// Both nodes serve: B serves what moved to it, A keeps serving what stayed.
	afterMigration := serveKey(t, recorder, clientB, trackedKey, version)
	assert.Greater(t, afterMigration, beforeMigration,
		"the migrated key must continue above the id the old owner returned")
	keptAfter := serveKey(t, recorder, clientA, keptKey, version)
	assert.Greater(t, keptAfter, keptBefore)
	requireRefusal(t, clientA, trackedKey, version, reason.Reason_SEQUENCE_SLOT_NOT_OWNER)

	// Release one: SIGTERM. The process drains and releases its own generation,
	// the handoff rows complete, and the survivor re-owns the slots one epoch on.
	heldSlots := ownedSlotsOf(t, db, instanceB)
	heldGenerations := loadAuthorityGenerations(t, db, instanceB)
	require.NotEmpty(t, heldSlots)
	require.NotEmpty(t, heldGenerations)
	stopProcess(t, processB)
	requireAuthorityVacated(t, db, heldGenerations)
	require.Eventually(t, func() bool {
		count, err := activeHandoffCount(db)
		return err == nil && count == 0
	}, discoveryTestTimeout, 25*time.Millisecond, "the graceful release left rows behind")
	// A release settles through either lawful terminal state: the departing
	// process completes it to UNOWNED, or the survivor claims the drained slot
	// and the claim cancels the row. One settled row is what is deterministic
	// here, and it proves the release protocol ran for the departing slots.
	require.Eventually(t, func() bool {
		count, err := queryCount(
			db,
			"SELECT COUNT(*) FROM sequence_slot_handoffs "+
				"WHERE slot_id IN ? AND kind = 'RELEASE' AND phase IN ('COMPLETED','CANCELLED')",
			slotIDsOf(heldSlots),
		)
		return err == nil && count > 0
	}, discoveryTestTimeout, 25*time.Millisecond, "the graceful release never settled")
	assert.EqualValues(t, migratedEpoch+1, waitSlotOwnedBy(t, db, instanceA, trackedSlot),
		"the re-claim after a release must start exactly one new epoch")
	idAfterRelease := serveKey(t, recorder, clientA, trackedKey, version)
	assert.Greater(t, idAfterRelease, afterMigration)

	// Release two: SIGKILL. B restarts and receives a fresh batch; the test then
	// writes the withdrawal a shutting-down process writes first, and kills the
	// process before it can drain. With no data node left to claim the result,
	// the control plane's RecoverHandoffs is what has to finish the release.
	portB2 := freeTCPPort(t)
	processB2 := startSequenceProcess(t, sequenceProcessOptions{
		database: database, nodeID: nodeB, configuredMode: "data", grpcPort: portB2,
	})
	waitForNodesLive(t, db, nodeB)
	instanceB2 := instanceForNode(t, db, nodeB)
	control2 := startControl(controlNode)
	waitRouteVersion(t, db)
	require.Eventually(t, func() bool {
		count, err := ownedSlotCount(db, instanceB2)
		return err == nil && count >= migrationMinimumTransfers
	}, discoveryTestTimeout, 25*time.Millisecond,
		"the restarted node never received a migration batch")
	stopProcess(t, control2)
	require.Eventually(t, func() bool {
		count, err := activeHandoffCount(db)
		return err == nil && count == 0
	}, discoveryTestTimeout, 25*time.Millisecond, "planned handoffs never settled again")

	heldByB2 := ownedSlotsOf(t, db, instanceB2)
	require.GreaterOrEqual(t, len(heldByB2), migrationMinimumTransfers)
	releasesBefore := mustCompletedHandoffs(t, db, "RELEASE")
	// A shutting-down process writes releases through the same bounded budget
	// the data plane enforces globally per source, so a SIGKILL can strand at
	// most one wave. Withdraw exactly that wave and keep the ids for recovery.
	wave := withdrawSlots(t, db, instanceB2, heldByB2)
	require.NotEmpty(t, wave)
	require.Contains(t, slotSetOf(wave), trackedSlot,
		"the tracked slot must be part of the interrupted release")
	withdrawn := assert.Eventually(t, func() bool {
		count, err := queryCount(
			db,
			"SELECT COUNT(*) FROM sequence_slot_handoffs WHERE slot_id IN ? AND kind = 'RELEASE' AND phase = 'DRAINING'",
			slotIDsOf(wave),
		)
		return err == nil && count == int64(len(wave))
	}, discoveryTestTimeout, 25*time.Millisecond)
	if !withdrawn {
		var phases []struct {
			Kind  string `gorm:"column:kind"`
			Phase string `gorm:"column:phase"`
			Count int64  `gorm:"column:n"`
		}
		require.NoError(t, db.Raw(
			"SELECT kind, phase, COUNT(*) AS n FROM sequence_slot_handoffs "+
				"WHERE slot_id IN ? GROUP BY kind, phase ORDER BY kind, phase",
			slotIDsOf(wave),
		).Scan(&phases).Error)
		require.FailNowf(t, "the withdrawal never reached DRAINING",
			"withdrew %d slots, handoff phases: %+v", len(wave), phases)
	}

	processB2.kill(t)
	processA.kill(t)

	startControl(controlNodeTwo)
	require.Eventually(t, func() bool {
		active, err := queryCount(
			db,
			"SELECT COUNT(*) FROM sequence_slot_handoffs WHERE phase NOT IN ('COMPLETED','CANCELLED')",
		)
		if err != nil || active != 0 {
			return false
		}
		completed, err := queryCount(
			db,
			"SELECT COUNT(*) FROM sequence_slot_handoffs WHERE kind = 'RELEASE' AND phase = 'COMPLETED'",
		)
		return err == nil && completed >= releasesBefore+int64(len(wave))
	}, 3*discoveryTestTimeout, 100*time.Millisecond,
		"recovery never completed the interrupted release")

	// With no data node alive to claim them, the recovered slots are visibly
	// unowned: this is the RELEASE path's resting state.
	recovered, err := queryCount(
		db,
		"SELECT COUNT(*) FROM sequence_slot_ownership WHERE slot_id IN ? AND state = 'UNOWNED' AND owner_instance_id IS NULL",
		slotIDsOf(wave),
	)
	require.NoError(t, err)
	assert.EqualValues(t, len(wave), recovered,
		"the recovered release must leave every slot unowned")

	// A restarts and picks the released slots back up; the key's watermark is
	// untouched by the crash, so the id keeps climbing.
	portA2 := freeTCPPort(t)
	startSequenceProcess(t, sequenceProcessOptions{
		database: database, nodeID: nodeA, configuredMode: "data", grpcPort: portA2,
	})
	require.Eventually(t, func() bool {
		var instanceID string
		if err := db.Raw(
			"SELECT instance_id FROM sequence_node_liveness WHERE node_id = ?",
			nodeA,
		).Scan(&instanceID).Error; err != nil {
			return false
		}
		return instanceID != "" && instanceID != instanceA
	}, discoveryTestTimeout, 50*time.Millisecond,
		"the restarted node never registered a new instance")
	waitForNodesLive(t, db, nodeA)
	clientA2 := dialSequenceNode(t, "migration-client-a2", fmt.Sprintf("127.0.0.1:%d", portA2))
	versionAfterRecovery := waitRouteVersion(t, db)
	idAfterRecovery := serveKey(t, recorder, clientA2, trackedKey, versionAfterRecovery)
	assert.Greater(t, idAfterRecovery, idAfterRelease,
		"allocation must continue after the recovered release")
	recorder.assertNoViolations(t)
}

// migrationOwnedSlot is one owned slot and the generation that holds it.
type migrationOwnedSlot struct {
	SlotID uint32 `gorm:"column:slot_id"`
	Epoch  uint64 `gorm:"column:epoch"`
}

func ownedSlotsOf(t *testing.T, db *gorm.DB, instanceID string) []migrationOwnedSlot {
	t.Helper()
	var rows []migrationOwnedSlot
	require.NoError(t, db.Raw(
		"SELECT slot_id, epoch FROM sequence_slot_ownership "+
			"WHERE owner_instance_id = ? AND state = 'OWNED' ORDER BY slot_id",
		instanceID,
	).Scan(&rows).Error)
	return rows
}

func slotIDsOf(rows []migrationOwnedSlot) []uint32 {
	ids := make([]uint32, 0, len(rows))
	for _, row := range rows {
		ids = append(ids, row.SlotID)
	}
	return ids
}

func slotSetOf(rows []migrationOwnedSlot) map[uint32]struct{} {
	set := make(map[uint32]struct{}, len(rows))
	for _, row := range rows {
		set[row.SlotID] = struct{}{}
	}
	return set
}

func epochOfSlot(rows []migrationOwnedSlot, slot uint32) uint64 {
	for _, row := range rows {
		if row.SlotID == slot {
			return row.Epoch
		}
	}
	return 0
}

func ownedSlotCount(db *gorm.DB, instanceID string) (int64, error) {
	return queryCount(
		db,
		"SELECT COUNT(*) FROM sequence_slot_ownership WHERE owner_instance_id = ? AND state = 'OWNED'",
		instanceID,
	)
}

func activeHandoffCount(db *gorm.DB) (int64, error) {
	return queryCount(
		db,
		"SELECT COUNT(*) FROM sequence_slot_handoffs WHERE phase NOT IN ('COMPLETED','CANCELLED')",
	)
}

func completedHandoffCount(db *gorm.DB, kind string) (int64, error) {
	return queryCount(
		db,
		"SELECT COUNT(*) FROM sequence_slot_handoffs WHERE kind = ? AND phase = 'COMPLETED'",
		kind,
	)
}

func mustCompletedHandoffs(t *testing.T, db *gorm.DB, kind string) int64 {
	t.Helper()
	count, err := completedHandoffCount(db, kind)
	require.NoError(t, err)
	return count
}

// waitSlotOwnedBy waits until instanceID owns slot and returns the observed
// epoch. It is safe to call from a polling condition and from the test body.
func waitSlotOwnedBy(
	t *testing.T,
	db *gorm.DB,
	instanceID string,
	slot uint32,
) uint64 {
	t.Helper()
	var epoch uint64
	require.Eventually(t, func() bool {
		var row struct {
			OwnerInstanceID *string `gorm:"column:owner_instance_id"`
			Epoch           uint64  `gorm:"column:epoch"`
			State           string  `gorm:"column:state"`
		}
		if err := db.Raw(
			"SELECT owner_instance_id, epoch, state FROM sequence_slot_ownership WHERE slot_id = ?",
			slot,
		).Scan(&row).Error; err != nil {
			return false
		}
		if row.State != "OWNED" || row.OwnerInstanceID == nil ||
			*row.OwnerInstanceID != instanceID {
			return false
		}
		epoch = row.Epoch
		return true
	}, discoveryTestTimeout, 25*time.Millisecond,
		"instance %s never owned slot %d", instanceID, slot)
	return epoch
}

func instanceForNode(t *testing.T, db *gorm.DB, nodeID string) string {
	t.Helper()
	var instanceID string
	require.Eventually(t, func() bool {
		if err := db.Raw(
			"SELECT instance_id FROM sequence_node_liveness WHERE node_id = ?",
			nodeID,
		).Scan(&instanceID).Error; err != nil {
			return false
		}
		return instanceID != ""
	}, discoveryTestTimeout, 50*time.Millisecond, "node %s never registered an instance", nodeID)
	return instanceID
}

// withdrawSlots writes the release wave a shutting-down process writes through
// the same repository: one WITHDRAW per owned slot, which leaves the slot
// DRAINING under a durable RELEASE row. The data plane bounds in-flight
// handoffs globally per source, so only the first wave that fits the budget is
// written -- a process killed mid-drain can strand no more than that. The
// returned rows are the slots that were actually withdrawn.
func withdrawSlots(
	t *testing.T,
	db *gorm.DB,
	instanceID string,
	rows []migrationOwnedSlot,
) []migrationOwnedSlot {
	t.Helper()
	var ha biz.HAConfig
	var limits biz.MigrationConfig
	require.NoError(t, testkit.DecodeDefaults(&ha))
	require.NoError(t, testkit.DecodeDefaults(&limits))
	budget := min(limits.BatchSlots, limits.MaxInflight, limits.MaxPerSource)
	require.Positive(t, budget, "the release wave budget must be positive")
	if len(rows) > budget {
		rows = rows[:budget]
	}
	repo := sequencedata.NewHandoffData(db)
	for start := 0; start < len(rows); start += limits.BatchSlots {
		batch := rows[start:min(start+limits.BatchSlots, len(rows))]
		commands := make([]biz.HandoffCommand, 0, len(batch))
		for _, row := range batch {
			commands = append(commands, biz.HandoffCommand{
				Action: "WITHDRAW",
				Handoff: biz.Handoff{
					SlotID:           row.SlotID,
					ID:               uuid.NewString(),
					SourceInstanceID: instanceID,
					SourceEpoch:      row.Epoch,
				},
			})
		}
		require.NoError(t, repo.ApplyHandoffs(context.Background(), biz.HandoffRequest{
			InstanceID: instanceID,
			Commands:   commands,
			HA:         ha,
			Limits:     limits,
		}))
	}
	return rows
}

// keyForSlot finds a key whose slot is the requested one. SlotForKey is a CRC,
// so the search is a scan; one million candidates make a miss practically
// impossible.
func keyForSlot(t *testing.T, slot uint32) string {
	t.Helper()
	for index := 0; index < 1<<20; index++ {
		candidate := fmt.Sprintf("migration-key-%d", index)
		if biz.SlotForKey(candidate) == slot {
			return candidate
		}
	}
	t.Fatalf("no key found for slot %d", slot)
	return ""
}

// serveKey allocates from one node until it answers, recording every success
// with the timestamps the strict-ordering gate needs.
func serveKey(
	t *testing.T,
	recorder *allocationRecorder,
	client sequencev1.SequenceGeneratorClient,
	key string,
	version int64,
) int64 {
	t.Helper()
	var id int64
	var recordErr error
	var lastErr error
	served := assert.Eventually(t, func() bool {
		started := time.Now()
		got, err := fetchFromProcess(client, key, version)
		received := time.Now()
		if err != nil {
			lastErr = err
			return false
		}
		if err := recorder.record(allocationObservation{
			Key: key, ID: got, Started: started, Received: received,
		}); err != nil {
			recordErr = err
			return false
		}
		id = got
		return true
	}, discoveryTestTimeout, 50*time.Millisecond)
	if !served {
		require.FailNowf(t, "node never served key", "key %q: %v", key, lastErr)
	}
	require.NoError(t, recordErr)
	return id
}

func requireRefusal(
	t *testing.T,
	client sequencev1.SequenceGeneratorClient,
	key string,
	version int64,
	want reason.Reason,
) {
	t.Helper()
	require.Eventually(t, func() bool {
		_, err := fetchFromProcess(client, key, version)
		return xerror.IsReason(err, want)
	}, discoveryTestTimeout, 50*time.Millisecond,
		"node did not refuse key %q with %s", key, want)
}

func fetchFromProcess(
	client sequencev1.SequenceGeneratorClient,
	key string,
	version int64,
) (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ctx = metadata.WithOutContext(
		ctx,
		metadata.Pairs(sequencepkg.VersionMetaKey, strconv.FormatInt(version, 10)),
	)
	response, err := client.FetchNext(ctx, &sequencev1.FetchNextRequest{Key: key})
	if err != nil {
		return 0, err
	}
	return response.GetId(), nil
}

func waitControlReady(t *testing.T, control *sequenceProcess) {
	t.Helper()
	require.Eventually(t, func() bool {
		status, report, err := probeReadiness(control.readyURL)
		return err == nil && status == http.StatusOK &&
			report.Control != nil && report.Control.Ready
	}, discoveryTestTimeout, 50*time.Millisecond, "the control process never became ready")
}

func waitRouteVersion(t *testing.T, db *gorm.DB) int64 {
	t.Helper()
	var version int64
	require.Eventually(t, func() bool {
		if err := db.Raw(
			"SELECT version FROM sequence_route_snapshot WHERE id = 1",
		).Scan(&version).Error; err != nil {
			return false
		}
		return version > 0
	}, discoveryTestTimeout, 50*time.Millisecond, "the control plane never published a route")
	return version
}

func stopProcess(t *testing.T, process *sequenceProcess) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	require.NoError(t, process.stop(ctx))
}

// dialSequenceNode builds a direct client to one real process. It is a
// yggdrasil runtime client so the metadata the service requires travels the
// same way it does in deployment.
func dialSequenceNode(
	t *testing.T,
	name, endpoint string,
) sequencev1.SequenceGeneratorClient {
	t.Helper()
	manager := config.NewManager()
	require.NoError(t, manager.LoadLayer(
		"test",
		config.PriorityOverride,
		memory.NewSource("test", map[string]any{
			"yggdrasil": map[string]any{
				"admin": map[string]any{"governor": map[string]any{"port": 0}},
				"clients": map[string]any{"services": map[string]any{
					sequenceServiceName: map[string]any{
						"fast_fail": true,
						"remote": map[string]any{"endpoints": []resolver.BaseEndpoint{
							{Address: endpoint, Protocol: "grpc"},
						}},
					},
				}},
				"transports": map[string]any{"grpc": map[string]any{
					"client": map[string]any{}, "server": map[string]any{},
				}},
			},
		}),
	))
	runtimeApp, err := yapp.New(
		name,
		yapp.WithConfigManager(manager),
		yapp.WithProcessDefaults(false),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = runtimeApp.Stop(ctx)
		_ = manager.Close()
	})
	client, err := runtimeApp.NewClient(context.Background(), sequenceServiceName)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	return sequencev1.NewSequenceGeneratorClient(client)
}
