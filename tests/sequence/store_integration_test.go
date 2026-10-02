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
	"math"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	mysqlc "github.com/testcontainers/testcontainers-go/modules/mysql"
	postgresc "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
	mysqlgorm "gorm.io/driver/mysql"
	postgresgorm "gorm.io/driver/postgres"
	"gorm.io/gorm"

	sharedgorm "github.com/codesjoy/sindri/internal/pkg/xgorm"
	"github.com/codesjoy/sindri/internal/sequence/biz"
	sequencedata "github.com/codesjoy/sindri/internal/sequence/data"
)

const (
	testDialectsEnv = "SKULD_SEQUENCE_TEST_DIALECTS"
	postgresImage   = "postgres:14-alpine"
	mysqlImage      = "mysql:8.0"
	databaseName    = "skuld_sequence"
	databaseUser    = "skuld_sequence"
	databasePass    = "skuld_sequence"
)

type harness struct {
	name             string
	driver           string
	sqlDriver        string
	dsn              string
	dialect          goose.Dialect
	dialector        func(string) gorm.Dialector
	container        testcontainers.Container
	network          *testcontainers.DockerNetwork
	dbAlias          string
	dbPort           string
	connectionString func(context.Context) (string, error)
	terminate        func(context.Context) error
}

var (
	enabledDialects map[string]bool
	harnesses       []*harness
)

func TestMain(m *testing.M) {
	var err error
	enabledDialects, err = parseTestDialects(os.Getenv(testDialectsEnv))
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "%s: %v\n", testDialectsEnv, err)
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	starters := []struct {
		name  string
		start func(context.Context) (*harness, error)
	}{
		{name: "postgres", start: startPostgres},
		{name: "mysql", start: startMySQL},
	}
	for _, dialect := range starters {
		if !enabledDialects[dialect.name] {
			continue
		}
		item, startErr := dialect.start(ctx)
		if startErr != nil {
			stopHarnesses(context.Background())
			_, _ = fmt.Fprintf(
				os.Stderr,
				"start %s integration container: %v\n",
				dialect.name,
				startErr,
			)
			os.Exit(1)
		}
		harnesses = append(harnesses, item)
		if err := applyMigrations(ctx, item); err != nil {
			stopHarnesses(context.Background())
			_, _ = fmt.Fprintf(os.Stderr, "apply %s migrations: %v\n", item.name, err)
			os.Exit(1)
		}
	}

	exitCode := m.Run()
	cleanupSequenceBinary()
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer stopCancel()
	if err := stopHarnesses(stopCtx); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "stop integration containers: %v\n", err)
		if exitCode == 0 {
			exitCode = 1
		}
	}
	os.Exit(exitCode)
}

func parseTestDialects(value string) (map[string]bool, error) {
	value = strings.TrimSpace(value)
	switch value {
	case "", "postgres,mysql":
		return map[string]bool{"postgres": true, "mysql": true}, nil
	case "postgres", "mysql":
		return map[string]bool{value: true}, nil
	default:
		return nil, fmt.Errorf(
			"unsupported value %q (supported: postgres, mysql, postgres,mysql)",
			value,
		)
	}
}

func TestSequenceStoreContractAcrossDialects(t *testing.T) {
	forEachDialect(t, func(t *testing.T, item *harness) {
		require.NoError(t, resetSequenceDatabase(t, item))
		db := openGORM(t, item)
		runRangeContract(t, db, item.name)
		runBatchRangeContract(t, db, item.name)
		runRouteContract(t, db, item.name)
	})
}

// forEachDialect runs a dialect-scoped integration test once per enabled
// database. Every contract test is written against the dialect as the variable
// under test, so the loop lives in one helper rather than being repeated.
func forEachDialect(t *testing.T, run func(t *testing.T, database *harness)) {
	t.Helper()
	for _, item := range harnesses {
		t.Run(item.name, func(t *testing.T) {
			run(t, item)
		})
	}
}

// TestBaselineMigrationLifecycleInIsolatedDatabaseAcrossDialects pins the
// baseline migration on an empty server per dialect: Up creates exactly the
// seven tables and the 16,384 unowned slots once, a repeated Up is a no-op, the
// legacy tables are absent, the slot bound is enforced, and Down removes the
// final schema before a clean re-Up.
func TestBaselineMigrationLifecycleInIsolatedDatabaseAcrossDialects(t *testing.T) {
	forEachDialect(t, func(t *testing.T, item *harness) {
		ctx := context.Background()
		start := startPostgres
		if item.name == "mysql" {
			start = startMySQL
		}
		isolated, err := start(ctx)
		require.NoError(t, err)
		t.Cleanup(func() {
			stopCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			require.NoError(t, isolated.terminate(stopCtx))
		})
		provider, db, err := openMigrationProvider(isolated)
		require.NoError(t, err)
		defer func() { require.NoError(t, db.Close()) }()
		tables := []string{
			"sequence_ranges",
			"sequence_slot_ownership",
			"sequence_instance_leases",
			"sequence_node_liveness",
			"sequence_coordinator",
			"sequence_route_snapshot",
			"sequence_slot_handoffs",
		}
		for round := 0; round < 2; round++ {
			_, err = provider.Up(ctx)
			require.NoError(t, err)
			results, err := provider.Up(ctx)
			require.NoError(t, err)
			assert.Empty(t, results)
			for _, table := range tables {
				exists, err := tableExists(ctx, db, isolated, table)
				require.NoError(t, err)
				require.True(t, exists, table)
			}
			for _, table := range []string{"ownership_outbox", "slot_ownership", "sequence_routes", "sequence_route_state"} {
				exists, err := tableExists(ctx, db, isolated, table)
				require.NoError(t, err)
				assert.False(t, exists, table)
			}
			var count, minSlot, maxSlot int64
			require.NoError(
				t,
				db.QueryRowContext(ctx, "SELECT COUNT(*),MIN(slot_id),MAX(slot_id) FROM sequence_slot_ownership WHERE state='UNOWNED' AND owner_instance_id IS NULL AND epoch=0").
					Scan(&count, &minSlot, &maxSlot),
			)
			assert.EqualValues(t, biz.SlotCount, count)
			assert.Zero(t, minSlot)
			assert.EqualValues(t, biz.SlotCount-1, maxSlot)
			require.NoError(
				t,
				db.QueryRowContext(ctx, "SELECT COUNT(*) FROM sequence_route_snapshot").
					Scan(&count),
			)
			assert.Zero(t, count)
			_, err = db.ExecContext(
				ctx,
				"INSERT INTO sequence_slot_ownership (slot_id) VALUES (16384)",
			)
			require.Error(t, err)
			_, err = provider.DownTo(ctx, 0)
			require.NoError(t, err)
			for _, table := range tables {
				exists, err := tableExists(ctx, db, isolated, table)
				require.NoError(t, err)
				assert.False(t, exists, table)
			}
		}
	})
}

// TestSequenceProcessLifecycleAcrossDialects runs the real cmd/sequence binary
// against every enabled dialect. It replaces the old in-process bundle
// assertion: what a deployment can observe is the process's readiness surface
// and the database state its shutdown hooks leave behind, so those are what the
// test pins rather than the internal shape of the composition.
func TestSequenceProcessLifecycleAcrossDialects(t *testing.T) {
	forEachDialect(t, func(t *testing.T, item *harness) {
		runSequenceProcessLifecycle(t, item)
	})
}

func runSequenceProcessLifecycle(t *testing.T, database *harness) {
	t.Helper()
	require.NoError(t, resetSequenceDatabase(t, database))
	// One bounded pool for the whole test: the readiness and shutdown checks
	// poll the database, and a fresh pool per poll would exhaust the server's
	// connection slots before the first assertion settles.
	db := openGORM(t, database)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(4)

	t.Run("data", func(t *testing.T) {
		nodeID := "process-data"
		process := startSequenceProcess(t, sequenceProcessOptions{
			database:       database,
			nodeID:         nodeID,
			configuredMode: "data",
		})

		// A data process that has never been given a route cannot serve.
		status, report, err := probeReadiness(process.readyURL)
		require.NoError(t, err)
		assert.Equal(t, http.StatusServiceUnavailable, status)
		assert.False(t, report.Ready)
		assert.Equal(t, "initializing", report.Reason)
		require.NotNil(t, report.Data)
		assert.Nil(t, report.Control, "a data process runs no publisher")
		assert.InDelta(t, processQuietWindow.Seconds(), report.Data.QuietWindowSeconds, 1e-9)
		assert.InDelta(t, processLeaseDuration.Seconds(), report.Data.LeaseDurationSeconds, 1e-9)
		assert.InDelta(t, processMaxPause.Seconds(), report.Data.MaxPauseSeconds, 1e-9)

		// Turning readiness green goes through the real authority: the harness
		// stages the ownership view, the real publisher materialises the
		// directory, and the process claims what its own heartbeat reads.
		publishDiscoveryRoute(t, database, allSlots(nodeID))
		require.Eventually(t, func() bool {
			status, report, probeErr := probeReadiness(process.readyURL)
			return probeErr == nil && status == http.StatusOK && report.Ready &&
				report.Data != nil && report.Data.Reason == "serving"
		}, discoveryTestTimeout, 50*time.Millisecond,
			"readiness never turned green for node %s", nodeID)

		instanceID := ownedInstanceForNode(t, db, nodeID)
		require.NotEmpty(t, instanceID, "the process claimed no slot authority")
		require.Equal(t, int64(1), livenessRows(t, db, nodeID))
		heldAuthority := loadAuthorityGenerations(t, db, instanceID)
		require.NotEmpty(t, heldAuthority, "the process held no slot authority to release")

		stopCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		require.NoError(t, process.stop(stopCtx), "graceful shutdown")

		// The shutdown path is visible in the database, and it is asserted the
		// way a deployment sees it: the process released the authority it held
		// (the generation observed before the stop is gone), it stopped
		// renewing its liveness row so the node ages out of the fleet's live
		// set, and the slots it held are assignable to the next process.
		requireAuthorityVacated(t, db, heldAuthority)
		waitForNodeToLeaveLiveSet(t, db, nodeID)
		requireAuthorityAssignable(t, db, []uint32{0, 1, 2, 3})
	})

	t.Run("control-mode-override", func(t *testing.T) {
		// The file says data and the command line says control. The startup mode
		// an operator states on the command line must win, and the resulting
		// process must run only the publisher half.
		process := startSequenceProcess(t, sequenceProcessOptions{
			database:       database,
			nodeID:         "process-control",
			configuredMode: "data",
			modeOverride:   "control",
		})
		require.Eventually(t, func() bool {
			status, report, probeErr := probeReadiness(process.readyURL)
			return probeErr == nil && status == http.StatusOK && report.Ready &&
				report.Control != nil && report.Data == nil
		}, discoveryTestTimeout, 50*time.Millisecond, "control readiness never turned green")

		stopCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		require.NoError(t, process.stop(stopCtx), "graceful shutdown")
	})
}

// ownedInstanceForNode returns the instance id a node's authority rows carry.
func ownedInstanceForNode(t *testing.T, db *gorm.DB, nodeID string) string {
	t.Helper()
	var instanceID string
	require.NoError(t, db.Raw(
		"SELECT o.owner_instance_id FROM sequence_slot_ownership o JOIN sequence_instance_leases i ON o.owner_instance_id=i.instance_id "+
			"WHERE i.node_id = ? LIMIT 1",
		nodeID,
	).Scan(&instanceID).Error)
	return instanceID
}

// livenessRows counts a node's rows in the shared liveness view.
func livenessRows(t *testing.T, db *gorm.DB, nodeID string) int64 {
	t.Helper()
	count, err := queryCount(
		db,
		"SELECT COUNT(*) FROM sequence_node_liveness WHERE node_id = ?",
		nodeID,
	)
	require.NoError(t, err)
	return count
}

// queryCount runs a counting query. It returns the error rather than failing, so
// it is safe to call from a polling condition.
func queryCount(db *gorm.DB, query string, args ...any) (int64, error) {
	var count int64
	err := db.Raw(query, args...).Scan(&count).Error
	return count, err
}

func runRangeContract(t *testing.T, db *gorm.DB, prefix string) {
	t.Helper()
	store := sequencedata.NewSequenceData(db)
	key := prefix + "-orders"
	invoicesKey := prefix + "-invoices"
	concurrentKey := prefix + "-concurrent"
	overflowKey := prefix + "-overflow"
	authority := claimOwnedSlots(
		t, db, prefix+"-range-instance", key, invoicesKey, concurrentKey, overflowKey,
	)
	first, err := reserveRange(context.Background(), store, authority, key, 10)
	require.NoError(t, err)
	assert.Equal(t, biz.SequenceRange{Start: 1, End: 10}, first)
	second, err := reserveRange(context.Background(), store, authority, key, 10)
	require.NoError(t, err)
	assert.Equal(t, biz.SequenceRange{Start: 11, End: 20}, second)

	restarted := sequencedata.NewSequenceData(db)
	third, err := reserveRange(context.Background(), restarted, authority, key, 5)
	require.NoError(t, err)
	assert.Equal(t, biz.SequenceRange{Start: 21, End: 25}, third)
	independent, err := reserveRange(context.Background(), store, authority, invoicesKey, 3)
	require.NoError(t, err)
	assert.Equal(t, biz.SequenceRange{Start: 1, End: 3}, independent)
	_, err = reserveRange(context.Background(), store, authority, "", 1)
	require.Error(t, err)
	_, err = reserveRange(context.Background(), store, authority, key, 0)
	require.Error(t, err)

	const workers = 32
	const step int64 = 7
	ranges := make(chan biz.SequenceRange, workers)
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			reserved, reserveErr := reserveRange(
				context.Background(),
				store,
				authority,
				concurrentKey,
				step,
			)
			if reserveErr != nil {
				errs <- reserveErr
				return
			}
			ranges <- reserved
		}()
	}
	wg.Wait()
	close(ranges)
	close(errs)
	for reserveErr := range errs {
		require.NoError(t, reserveErr)
	}
	got := make([]biz.SequenceRange, 0, workers)
	for reserved := range ranges {
		got = append(got, reserved)
	}
	sort.Slice(got, func(i, j int) bool { return got[i].Start < got[j].Start })
	for index, reserved := range got {
		start := int64(index)*step + 1
		assert.Equal(t, biz.SequenceRange{Start: start, End: start + step - 1}, reserved)
	}

	require.NoError(t, db.Create(&sequencedata.SequenceModel{
		SequenceKey: overflowKey,
		ReservedEnd: math.MaxInt64 - 2,
		UpdatedAt:   time.Now().UTC(),
	}).Error)
	_, err = reserveRange(context.Background(), store, authority, overflowKey, 3)
	require.Error(t, err)
}

// claimOwnedSlots grants the slots of every key to instanceID in the ownership
// authority and returns the reservation authority to present when reserving.
// The store contract tests assert pure watermark tiling, so the slots they
// touch must be genuinely owned by the instance they reserve as.
func claimOwnedSlots(
	t *testing.T,
	db *gorm.DB,
	instanceID string,
	keys ...string,
) biz.ReservationAuthority {
	t.Helper()
	ownership := sequencedata.NewOwnershipData(db)
	ctx := context.Background()
	require.NoError(t, ownership.RegisterInstance(ctx, "test-node", instanceID))
	snapshot, err := ownership.InstanceAuthority(ctx, instanceID)
	require.NoError(t, err)
	slots := make([]uint32, 0, len(keys))
	seen := make(map[uint32]struct{}, len(keys))
	for _, key := range keys {
		slot := biz.SlotForKey(key)
		if _, ok := seen[slot]; ok {
			continue
		}
		seen[slot] = struct{}{}
		slots = append(slots, slot)
	}
	outcomes, err := ownership.ClaimSlots(ctx, biz.ClaimRequest{
		Slots:       slots,
		NodeID:      "test-node",
		InstanceID:  instanceID,
		Revision:    snapshot.Lease.Revision,
		Lease:       time.Minute,
		QuietWindow: time.Minute,
	})
	require.NoError(t, err)
	require.Len(t, outcomes, len(slots))
	for _, outcome := range outcomes {
		require.True(t, outcome.Granted,
			"slot %d must be granted to %s", outcome.Ownership.SlotID, instanceID)
	}
	snapshot, err = ownership.InstanceAuthority(ctx, instanceID)
	require.NoError(t, err)
	authority := biz.ReservationAuthority{
		InstanceID: instanceID,
		Revision:   snapshot.Lease.Revision,
		Lease:      time.Minute,
		Epochs:     map[uint32]uint64{},
	}
	for _, o := range snapshot.Slots {
		authority.Epochs[o.SlotID] = o.Epoch
	}
	return authority
}

func reserveRange(
	ctx context.Context,
	store biz.SequenceRepo,
	authority biz.ReservationAuthority,
	key string,
	step int64,
) (biz.SequenceRange, error) {
	ranges, err := store.ReserveRanges(
		ctx,
		authority,
		[]biz.ReservationRequest{{Key: key, Step: step}},
	)
	if err != nil {
		return biz.SequenceRange{}, err
	}
	return ranges[0], nil
}

func runBatchRangeContract(t *testing.T, db *gorm.DB, prefix string) {
	t.Helper()
	ctx := context.Background()
	store := sequencedata.NewSequenceData(db)

	const (
		workers   = 8
		batchKeys = 3
		step      = int64(5)
	)
	firstKey := prefix + "-batch-first"
	secondKey := prefix + "-batch-second"
	thirdKey := prefix + "-batch-third"
	batchPrefix := prefix + "-batch-concurrent"
	overflowKey := prefix + "-batch-overflow"
	normalKey := prefix + "-batch-normal"
	claimKeys := []string{
		firstKey, secondKey, thirdKey, overflowKey, normalKey,
		prefix + "-batch-cancelled",
	}
	for index := range batchKeys {
		claimKeys = append(claimKeys, fmt.Sprintf("%s-%d", batchPrefix, index))
	}
	authority := claimOwnedSlots(t, db, prefix+"-batch-instance", claimKeys...)
	reserved, err := store.ReserveRanges(ctx, authority, []biz.ReservationRequest{
		{Key: secondKey, Step: 4},
		{Key: firstKey, Step: 3},
	})
	require.NoError(t, err)
	require.Len(t, reserved, 2)
	assert.Equal(t, biz.SequenceRange{Start: 1, End: 4}, reserved[0],
		"batch results must follow request order")
	assert.Equal(t, biz.SequenceRange{Start: 1, End: 3}, reserved[1])
	assert.Equal(t, int64(3), maxIDFor(t, db, firstKey))
	assert.Equal(t, int64(4), maxIDFor(t, db, secondKey))

	mixed, err := store.ReserveRanges(ctx, authority, []biz.ReservationRequest{
		{Key: firstKey, Step: 2},
		{Key: thirdKey, Step: 5},
	})
	require.NoError(t, err)
	require.Len(t, mixed, 2)
	assert.Equal(t, biz.SequenceRange{Start: 4, End: 5}, mixed[0],
		"existing keys continue from their watermark")
	assert.Equal(t, biz.SequenceRange{Start: 1, End: 5}, mixed[1])

	_, err = store.ReserveRanges(ctx, authority, nil)
	require.Error(t, err, "an empty batch must be rejected")
	_, err = store.ReserveRanges(ctx, authority, []biz.ReservationRequest{
		{Key: firstKey, Step: 1},
		{Key: firstKey, Step: 1},
	})
	require.Error(t, err, "duplicate keys must be rejected")

	results := make(chan []biz.SequenceRange, workers)
	errs := make(chan error, workers)
	var wait sync.WaitGroup
	for range workers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			requests := make([]biz.ReservationRequest, batchKeys)
			for index := range requests {
				requests[index] = biz.ReservationRequest{
					Key:  fmt.Sprintf("%s-%d", batchPrefix, index),
					Step: step,
				}
			}
			ranges, reserveErr := store.ReserveRanges(context.Background(), authority, requests)
			if reserveErr != nil {
				errs <- reserveErr
				return
			}
			results <- ranges
		}()
	}
	wait.Wait()
	close(results)
	close(errs)
	for reserveErr := range errs {
		require.NoError(t, reserveErr, "sorted key batches must not deadlock")
	}
	covered := make(map[string][]biz.SequenceRange, batchKeys)
	for ranges := range results {
		require.Len(t, ranges, batchKeys)
		for index, item := range ranges {
			key := fmt.Sprintf("%s-%d", batchPrefix, index)
			covered[key] = append(covered[key], item)
		}
	}
	for key, ranges := range covered {
		require.Len(t, ranges, workers)
		sort.Slice(ranges, func(i, j int) bool { return ranges[i].Start < ranges[j].Start })
		for index, item := range ranges {
			start := int64(index)*step + 1
			assert.Equal(t, biz.SequenceRange{Start: start, End: start + step - 1}, item, key)
		}
	}

	overflowMax := int64(math.MaxInt64 - 2)
	require.NoError(t, db.Create(&sequencedata.SequenceModel{
		SequenceKey: overflowKey,
		ReservedEnd: overflowMax,
		UpdatedAt:   time.Now().UTC(),
	}).Error)
	_, err = store.ReserveRanges(ctx, authority, []biz.ReservationRequest{
		{Key: normalKey, Step: 6},
		{Key: overflowKey, Step: 3},
	})
	require.Error(t, err, "overflow must fail the whole reservation")
	assert.Zero(t, maxIDFor(t, db, normalKey),
		"a failing batch must not advance the other keys")
	assert.Equal(t, overflowMax, maxIDFor(t, db, overflowKey))

	require.NoError(t, db.Where("sequence_key = ?", overflowKey).
		Delete(&sequencedata.SequenceModel{}).Error)
	reserved, err = store.ReserveRanges(ctx, authority, []biz.ReservationRequest{
		{Key: normalKey, Step: 6},
		{Key: overflowKey, Step: 3},
	})
	require.NoError(t, err)
	require.Len(t, reserved, 2)
	assert.Equal(t, biz.SequenceRange{Start: 1, End: 6}, reserved[0])
	assert.Equal(t, biz.SequenceRange{Start: 1, End: 3}, reserved[1])

	cancelledCtx, cancel := context.WithCancel(ctx)
	cancel()
	_, err = store.ReserveRanges(cancelledCtx, authority, []biz.ReservationRequest{{
		Key:  prefix + "-batch-cancelled",
		Step: 1,
	}})
	require.ErrorIs(t, err, context.Canceled)
}

func maxIDFor(t *testing.T, db *gorm.DB, key string) int64 {
	t.Helper()
	var model sequencedata.SequenceModel
	err := db.Where("sequence_key = ?", key).Take(&model).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return 0
	}
	require.NoError(t, err)
	return model.ReservedEnd
}

func runRouteContract(t *testing.T, db *gorm.DB, prefix string) {
	t.Helper()
	store := sequencedata.NewRouteModel(db)
	baseVersion := int64(10)
	if prefix == "mysql" {
		baseVersion = 20
	}
	require.NoError(t, db.Exec("DELETE FROM sequence_route_snapshot").Error)
	require.NoError(
		t,
		db.Create(
			&sequencedata.RouteModel{
				ID:        1,
				Version:   baseVersion + 1,
				Payload:   completeRoutePayload(t),
				UpdatedAt: time.Now().UTC(),
			},
		).Error,
	)
	route, err := store.GetNewerRoute(context.Background(), 0)
	require.NoError(t, err)
	require.NotNil(t, route)
	assert.Equal(t, baseVersion+1, route.Version)
	unchanged, err := store.GetNewerRoute(context.Background(), baseVersion+1)
	require.NoError(t, err)
	assert.Nil(t, unchanged)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = store.GetNewerRoute(ctx, 0)
	require.Error(t, err)
}

func completeRoutePayload(t *testing.T) []byte {
	t.Helper()
	view := []biz.OwnershipSegment{{
		StartSlot: 0, EndSlot: biz.SlotCount - 1,
		State:           biz.SlotOwned,
		OwnerNodeID:     "node-a",
		OwnerInstanceID: "instance-a",
		Epoch:           1,
		GrantAgeKnown:   true,
	}}
	payload, err := biz.EncodeOwnershipSegments(view, 1)
	require.NoError(t, err)
	return payload
}

func openGORM(t *testing.T, item *harness) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(item.dialector(item.dsn), &gorm.Config{})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })
	return db
}

func applyMigrations(ctx context.Context, item *harness) error {
	provider, db, err := openMigrationProvider(item)
	if err != nil {
		return err
	}
	defer db.Close()
	_, err = provider.Up(ctx)
	return err
}

// openMigrationProvider opens the service's migration directory against the
// harness database. The caller owns the returned pool.
func openMigrationProvider(
	item *harness,
) (*goose.Provider, *sql.DB, error) {
	db, err := sql.Open(item.sqlDriver, item.dsn)
	if err != nil {
		return nil, nil, err
	}
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		_ = db.Close()
		return nil, nil, errors.New("resolve integration test path")
	}
	directory := filepath.Join(
		filepath.Dir(filename), "..", "..", "migrations", "sequence", item.name,
	)
	provider, err := goose.NewProvider(item.dialect, db, os.DirFS(directory))
	if err != nil {
		_ = db.Close()
		return nil, nil, err
	}
	return provider, db, nil
}

// tableExists reports whether a table is present in the harness schema,
// spelled per dialect because the information schema is not portable.
func tableExists(
	ctx context.Context,
	db *sql.DB,
	item *harness,
	table string,
) (bool, error) {
	placeholder, schema := "?", "DATABASE()"
	if item.name == "postgres" {
		placeholder, schema = "$1", "current_schema()"
	}
	var count int64
	err := db.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM information_schema.tables "+
			"WHERE table_name = "+placeholder+" AND table_schema = "+schema,
		table,
	).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func startPostgres(ctx context.Context) (*harness, error) {
	nw, err := network.New(ctx)
	if err != nil {
		return nil, err
	}
	const dbAlias = "postgres"
	container, err := postgresc.Run(
		ctx,
		postgresImage,
		postgresc.WithDatabase(databaseName),
		postgresc.WithUsername(databaseUser),
		postgresc.WithPassword(databasePass),
		network.WithNetwork([]string{dbAlias}, nw),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(2*time.Minute),
		),
	)
	if err != nil {
		_ = nw.Remove(ctx)
		return nil, err
	}
	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		_ = container.Terminate(ctx)
		_ = nw.Remove(ctx)
		return nil, err
	}
	if err := waitForDatabase(ctx, "pgx", dsn); err != nil {
		_ = container.Terminate(ctx)
		_ = nw.Remove(ctx)
		return nil, err
	}
	return &harness{
		name: "postgres", driver: sharedgorm.DriverPostgres, sqlDriver: "pgx", dsn: dsn,
		dialect: goose.DialectPostgres, dialector: func(value string) gorm.Dialector {
			return postgresgorm.Open(value)
		},
		container: container.Container, network: nw, dbAlias: dbAlias, dbPort: "5432",
		connectionString: func(connectionCtx context.Context) (string, error) {
			return container.ConnectionString(connectionCtx, "sslmode=disable")
		},
		terminate: func(stopCtx context.Context) error {
			return errors.Join(container.Terminate(stopCtx), nw.Remove(stopCtx))
		},
	}, nil
}

func startMySQL(ctx context.Context) (*harness, error) {
	nw, err := network.New(ctx)
	if err != nil {
		return nil, err
	}
	const dbAlias = "mysql"
	container, err := mysqlc.Run(
		ctx,
		mysqlImage,
		mysqlc.WithDatabase(databaseName),
		mysqlc.WithUsername(databaseUser),
		mysqlc.WithPassword(databasePass),
		network.WithNetwork([]string{dbAlias}, nw),
		testcontainers.WithWaitStrategy(
			wait.ForLog("ready for connections").
				WithOccurrence(1).
				WithStartupTimeout(2*time.Minute),
		),
	)
	if err != nil {
		_ = nw.Remove(ctx)
		return nil, err
	}
	dsn, err := container.ConnectionString(ctx, "parseTime=true", "multiStatements=true")
	if err != nil {
		_ = container.Terminate(ctx)
		_ = nw.Remove(ctx)
		return nil, err
	}
	if err := waitForDatabase(ctx, "mysql", dsn); err != nil {
		_ = container.Terminate(ctx)
		_ = nw.Remove(ctx)
		return nil, err
	}
	return &harness{
		name: "mysql", driver: sharedgorm.DriverMySQL, sqlDriver: "mysql", dsn: dsn,
		dialect: goose.DialectMySQL, dialector: func(value string) gorm.Dialector {
			return mysqlgorm.Open(value)
		},
		container: container.Container, network: nw, dbAlias: dbAlias, dbPort: "3306",
		connectionString: func(connectionCtx context.Context) (string, error) {
			return container.ConnectionString(
				connectionCtx,
				"parseTime=true",
				"multiStatements=true",
			)
		},
		terminate: func(stopCtx context.Context) error {
			return errors.Join(container.Terminate(stopCtx), nw.Remove(stopCtx))
		},
	}, nil
}

func stopHarnesses(ctx context.Context) error {
	var err error
	for _, item := range harnesses {
		err = errors.Join(err, item.terminate(ctx))
	}
	return err
}

func waitForDatabase(ctx context.Context, driver, dsn string) error {
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	for {
		pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		err = db.PingContext(pingCtx)
		cancel()
		if err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return errors.Join(ctx.Err(), err)
		case <-time.After(250 * time.Millisecond):
		}
	}
}
