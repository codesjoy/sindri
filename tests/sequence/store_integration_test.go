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

	testkit "github.com/codesjoy/sindri/internal/pkg/tests"
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
	for _, item := range harnesses {
		t.Run(item.name, func(t *testing.T) {
			db := openGORM(t, item)
			runRangeContract(t, db, item.name)
			runBatchRangeContract(t, db, item.name)
			runRouteContract(t, db, item.name)
		})
	}
}

// TestSequenceProcessLifecycleAcrossDialects runs the real cmd/sequence binary
// against every enabled dialect. It replaces the old in-process bundle
// assertion: what a deployment can observe is the process's readiness surface
// and the database state its shutdown hooks leave behind, so those are what the
// test pins rather than the internal shape of the composition.
func TestSequenceProcessLifecycleAcrossDialects(t *testing.T) {
	for _, item := range harnesses {
		t.Run(item.name, func(t *testing.T) {
			runSequenceProcessLifecycle(t, item)
		})
	}
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
		status, report, err := probeSequenceReadiness(process.readyURL)
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
			status, report, probeErr := probeSequenceReadiness(process.readyURL)
			return probeErr == nil && status == http.StatusOK && report.Ready &&
				report.Data != nil && report.Data.Reason == "serving"
		}, discoveryTestTimeout, 50*time.Millisecond,
			"readiness never turned green for node %s", nodeID)

		instanceID := ownedInstanceForNode(t, db, nodeID)
		require.NotEmpty(t, instanceID, "the process claimed no slot authority")
		require.Equal(t, int64(1), livenessRows(t, db, nodeID))
		outboxWatermark := ownershipEventWatermark(t, db)

		stopCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		require.NoError(t, process.stop(stopCtx), "graceful shutdown")

		// The shutdown path is visible in the database, and it is asserted the
		// way a deployment sees it: the process released the authority it held
		// (the release events are written by the shutdown hook), it stopped
		// renewing its liveness row so the node ages out of the fleet's live
		// set, and the slots it held are assignable to the next process.
		requireReleaseEventsRecorded(t, db, outboxWatermark)
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
			status, report, probeErr := probeSequenceReadiness(process.readyURL)
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
		"SELECT owner_instance_id FROM slot_ownership "+
			"WHERE owner_node_id = ? AND owner_instance_id IS NOT NULL LIMIT 1",
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
	outcomes, err := ownership.ClaimSlots(context.Background(), biz.ClaimRequest{
		Slots:       slots,
		NodeID:      "test-node",
		InstanceID:  instanceID,
		QuietWindow: 0,
	})
	require.NoError(t, err)
	require.Len(t, outcomes, len(slots))
	for _, outcome := range outcomes {
		require.True(t, outcome.Granted,
			"slot %d must be granted to %s", outcome.Ownership.SlotID, instanceID)
	}
	return biz.ReservationAuthority{InstanceID: instanceID}
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
	for _, version := range []int64{baseVersion, baseVersion + 1} {
		require.NoError(t, db.Create(&sequencedata.RouteModel{
			Version: version, Payload: completeRoutePayload(t), CreatedAt: time.Now().UTC(),
		}).Error)
	}
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
	view := make([]biz.Ownership, int(biz.SlotCount))
	for index := range view {
		view[index] = biz.Ownership{
			SlotID:          uint32(index),
			State:           biz.SlotOwned,
			OwnerNodeID:     "node-a",
			OwnerInstanceID: "instance-a",
			Epoch:           1,
		}
	}
	payload, err := biz.EncodeOwnershipView(view, 1)
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
	db, err := sql.Open(item.sqlDriver, item.dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		return errors.New("resolve integration test path")
	}
	directory := filepath.Join(
		filepath.Dir(filename), "..", "..", "migrations", "sequence", item.name,
	)
	provider, err := goose.NewProvider(item.dialect, db, os.DirFS(directory))
	if err != nil {
		return err
	}
	_, err = provider.Up(ctx)
	return err
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

// TestCrashFailoverAcrossDialects covers the crash-failover path against a real
// database, which is where its correctness lives: a node that stops leaves its
// grant behind, and the claim that follows has to be granted by the authority's
// own clock rather than by anything the departed node did.
//
// The scenario is a node that stopped. Its grant ages past the quiet window, and
// nothing rewrites its ownership row -- only an explicit release does, and a dead
// process cannot issue one -- so the slot stays stranded until another node is
// told to take it. The directory that tells it so is published directly, the way
// an operator or the control plane would: sequence reads routes and claims what
// they grant, and that is the whole path under test.
func TestCrashFailoverAcrossDialects(t *testing.T) {
	for _, item := range harnesses {
		t.Run(item.name, func(t *testing.T) {
			db := openGORM(t, item)
			ctx := context.Background()
			// The directory is published at version 1 below, so the table has to
			// start empty: another test in this package may have left a route
			// behind, and the version is the primary key.
			require.NoError(t, resetSequenceDatabase(t, item))
			const (
				deadNode    = "node-failover-dead"
				deadProcess = "instance-failover-dead"
				survivor    = "node-failover-live"
				quietWindow = 50 * time.Millisecond
				// How many heartbeats the route takes to become active on, which is
				// the node's own HeartbeatTimeoutTicks.
				applyTicks = 3
			)
			slot := uint32(42)

			ownership := sequencedata.NewOwnershipData(db)
			base := resetSlot(t, ownership, slot).Epoch
			granted, err := ownership.ClaimSlots(ctx, biz.ClaimRequest{
				Slots:       []uint32{slot},
				NodeID:      deadNode,
				InstanceID:  deadProcess,
				QuietWindow: 0,
			})
			require.NoError(t, err)
			require.True(t, granted[0].Granted)

			// A takeover is only legal once that much storage time has passed.
			time.Sleep(quietWindow + 50*time.Millisecond)

			plane := biz.DataPlaneConfig{
				Allocator: biz.AllocatorConfig{DefaultStep: 10, MaxStep: 100},
				Node: biz.NodeConfig{
					ID:                    survivor,
					HeartbeatTimeoutTicks: applyTicks,
					RouteQueryTimeout:     time.Second,
				},
				HA: biz.HAConfig{
					QuietWindow:   quietWindow,
					LeaseDuration: 10 * time.Second,
					MaxPause:      time.Second,
					NodeTTL:       time.Minute,
				},
			}
			require.NoError(t, testkit.DecodeDefaults(&plane))
			allocator := biz.NewAllocator(
				plane,
				sequencedata.NewSequenceData(db),
				ownership,
				failoverSampler{},
				nil,
			)

			// The directory is materialised from the authority as it stands: the
			// departed node's grant is still on the row and already past the quiet
			// window, and it renews no liveness lease, so the surviving node's own
			// plan hands it the stranded slot along with everything else. The claim
			// is what makes the handover real.
			publishSeededRoute(t, db)
			placement := sequencedata.NewPlacementData(db)
			manager := biz.NewNodeManager(
				plane,
				allocator,
				sequencedata.NewRouteModel(db),
				sequencedata.NewLivenessData(db),
				placement,
				biz.NewRouteCache(),
				nil,
				nil,
			)
			// The heartbeat installs the directory and schedules the claim for the
			// tick the route becomes active on; driving the allocator to that tick
			// performs the takeover, which a real fleet reaches one base tick later.
			manager.Heartbeat()
			allocator.ApplyRoute(applyTicks)

			after := loadSlotOwnership(t, ownership, slot)
			assert.Equal(t, biz.SlotOwned, after.State)
			assert.Equal(t, allocator.InstanceID(), after.OwnerInstanceID,
				"the surviving node must own the slot the departed one left behind")
			assert.Greater(t, after.Epoch, base+1, "the takeover starts a new epoch")
		})
	}
}

type failoverSampler struct{}

func (failoverSampler) MemoryUsage() (uint64, uint64) { return 1, math.MaxInt64 }
