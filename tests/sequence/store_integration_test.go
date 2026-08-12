//go:build integration

package sequence_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
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

	testkit "github.com/codesjoy/skuld/internal/pkg/tests"
	sharedgorm "github.com/codesjoy/skuld/internal/pkg/xgorm"
	"github.com/codesjoy/skuld/internal/sequence/app"
	"github.com/codesjoy/skuld/internal/sequence/biz"
	"github.com/codesjoy/skuld/internal/sequence/conf"
	gormdata "github.com/codesjoy/skuld/internal/sequence/data/gorm"
	"github.com/codesjoy/skuld/internal/sequence/task"
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
			runRouteContract(t, db, item.name)
		})
	}
}

func TestDatabaseIdentityAndAppInitializationAcrossDialects(t *testing.T) {
	for _, item := range harnesses {
		t.Run(item.name, func(t *testing.T) {
			rt := testkit.NewRuntime(t, map[string]any{})
			cfg := conf.Config{
				Database: sharedgorm.Config{
					Driver: item.driver, DSN: item.dsn,
					ExpectedDatabase: databaseName, ExpectedAccount: databaseUser,
				},
				Allocator: biz.AllocatorConfig{Step: 10},
				Node: biz.NodeConfig{
					ID: "node-a", HeartbeatTimeoutTicks: 3,
					RouteQueryTimeout: time.Second,
				},
				Ticker: task.Config{BaseTickInterval: time.Second, HeartbeatTicks: 1},
			}
			cfg.SetDefaults()
			require.NoError(t, cfg.Validate())
			bundle, err := app.InitializeBundle(rt, &cfg)
			require.NoError(t, err)
			require.Len(t, bundle.RPCBindings, 1)
			require.Len(t, bundle.Tasks, 1)
			require.Len(t, bundle.Hooks, 1)
			require.NoError(t, bundle.Hooks[0].Func(context.Background()))
		})
	}
}

func runRangeContract(t *testing.T, db *gorm.DB, prefix string) {
	t.Helper()
	store := gormdata.NewSequenceData(db)
	key := prefix + "-orders"
	first, err := store.ReserveRange(context.Background(), key, 10)
	require.NoError(t, err)
	assert.Equal(t, biz.SequenceRange{Start: 1, End: 10}, first)
	second, err := store.ReserveRange(context.Background(), key, 10)
	require.NoError(t, err)
	assert.Equal(t, biz.SequenceRange{Start: 11, End: 20}, second)

	restarted := gormdata.NewSequenceData(db)
	third, err := restarted.ReserveRange(context.Background(), key, 5)
	require.NoError(t, err)
	assert.Equal(t, biz.SequenceRange{Start: 21, End: 25}, third)
	independent, err := store.ReserveRange(context.Background(), prefix+"-invoices", 3)
	require.NoError(t, err)
	assert.Equal(t, biz.SequenceRange{Start: 1, End: 3}, independent)
	_, err = store.ReserveRange(context.Background(), "", 1)
	require.Error(t, err)
	_, err = store.ReserveRange(context.Background(), key, 0)
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
			reserved, reserveErr := store.ReserveRange(
				context.Background(), prefix+"-concurrent", step,
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

	overflowKey := prefix + "-overflow"
	require.NoError(t, db.Create(&gormdata.SequenceModel{
		SequenceKey: overflowKey,
		MaxID:       math.MaxInt64 - 2,
		UpdatedAt:   time.Now().UTC(),
	}).Error)
	_, err = store.ReserveRange(context.Background(), overflowKey, 3)
	require.Error(t, err)
}

func runRouteContract(t *testing.T, db *gorm.DB, prefix string) {
	t.Helper()
	store := gormdata.NewRouteModel(db)
	baseVersion := int64(10)
	if prefix == "mysql" {
		baseVersion = 20
	}
	for _, version := range []int64{baseVersion, baseVersion + 1} {
		require.NoError(t, db.Create(&gormdata.RouteModel{
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
	slots := make([]uint32, biz.SlotCount)
	for index := range slots {
		slots[index] = uint32(index)
	}
	payload, err := json.Marshal(map[string]any{
		"nodes": []any{map[string]any{"node_id": "node-a", "slots": slots}},
	})
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
