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
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	sequencev1 "github.com/codesjoy/sindri/gen/go/sequence/v1"
	"github.com/codesjoy/sindri/internal/pkg/xgorm"
	sequenceapp "github.com/codesjoy/sindri/internal/sequence/app"
	"github.com/codesjoy/sindri/internal/sequence/biz"
	"github.com/codesjoy/sindri/internal/sequence/conf"
	gormdata "github.com/codesjoy/sindri/internal/sequence/data/gorm"
	"github.com/codesjoy/sindri/internal/sequence/task"
	sequencepkg "github.com/codesjoy/sindri/pkg/sequence"
	etcdmodule "github.com/codesjoy/yggdrasil-ecosystem/modules/etcd/v3"
	yapp "github.com/codesjoy/yggdrasil/v3/app"
	"github.com/codesjoy/yggdrasil/v3/config"
	"github.com/codesjoy/yggdrasil/v3/config/source/memory"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcetcd "github.com/testcontainers/testcontainers-go/modules/etcd"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	sequenceAppName      = "github.com.codesjoy.skuld.sequence"
	etcdImage            = "gcr.io/etcd-development/etcd:v3.5.14"
	etcdRegistryPrefix   = "/sindri/tests/sequence/registry"
	discoveryTestTimeout = 10 * time.Second
)

type discoveryInstanceRecord struct {
	Name      string            `json:"name"`
	Namespace string            `json:"namespace"`
	Metadata  map[string]string `json:"metadata"`
	Endpoints []struct {
		Scheme  string `json:"scheme"`
		Address string `json:"address"`
	} `json:"endpoints"`
}

type discoveredSequenceApp struct {
	app     *yapp.App
	manager *config.Manager
}

func (a *discoveredSequenceApp) stop(ctx context.Context) error {
	return errors.Join(a.app.Stop(ctx), a.manager.Close())
}

func TestSequenceEtcdDiscoveryAcrossDialects(t *testing.T) {
	for _, item := range harnesses {
		t.Run(item.name, func(t *testing.T) {
			runSequenceEtcdDiscovery(t, item)
		})
	}
}

func runSequenceEtcdDiscovery(t *testing.T, database *harness) {
	t.Helper()
	require.NoError(t, resetSequenceDatabase(t, database))

	ctx := context.Background()
	etcdContainer, err := tcetcd.Run(ctx, etcdImage)
	testcontainers.CleanupContainer(t, etcdContainer)
	require.NoError(t, err)
	etcdEndpoint, err := etcdContainer.ClientEndpoint(ctx)
	require.NoError(t, err)
	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{etcdEndpoint},
		DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, etcdClient.Close()) })

	nodeA := startDiscoveredSequenceApp(t, database, etcdEndpoint, "node-a")
	nodeB := startDiscoveredSequenceApp(t, database, etcdEndpoint, "node-b")
	stoppedNodeA := false
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if !stoppedNodeA {
			require.NoError(t, nodeA.stop(stopCtx))
		}
		require.NoError(t, nodeB.stop(stopCtx))
	})

	waitForRegisteredNodes(t, etcdClient, "node-a", "node-b")

	router, routedClient, closeClient := newDiscoveredSequenceClient(t, etcdEndpoint)
	t.Cleanup(closeClient)

	route := splitSlots()
	version := publishDiscoveryRoute(t, database, 1, route)
	keyA := keyForOwner("node-a", route)
	keyB := keyForOwner("node-b", route)
	firstA := waitForDiscoveredAllocation(t, routedClient, keyA)
	firstB := waitForDiscoveredAllocation(t, routedClient, keyB)
	require.Equal(t, version, router.Version())
	require.Positive(t, firstA)
	require.Positive(t, firstB)

	stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	require.NoError(t, nodeA.stop(stopCtx))
	cancel()
	stoppedNodeA = true
	waitForRegisteredNodes(t, etcdClient, "node-b")

	version = publishDiscoveryRoute(t, database, 2, allSlots("node-b"))
	require.Eventually(t, func() bool {
		refreshCtx, refreshCancel := context.WithTimeout(context.Background(), time.Second)
		defer refreshCancel()
		return router.Refresh(refreshCtx) == nil && router.Version() == version
	}, discoveryTestTimeout, 50*time.Millisecond)
	afterHandoff := waitForDiscoveredAllocation(t, routedClient, keyA)
	require.Greater(t, afterHandoff, firstA)
}

func startDiscoveredSequenceApp(
	t *testing.T,
	database *harness,
	etcdEndpoint string,
	nodeID string,
) *discoveredSequenceApp {
	t.Helper()
	manager := newDiscoveryConfigManager(t, discoveryConfig(etcdEndpoint, nodeID, false))
	runtimeApp, err := yapp.New(
		sequenceAppName,
		yapp.WithConfigManager(manager),
		yapp.WithProcessDefaults(false),
		yapp.WithModules(etcdmodule.Module()),
	)
	require.NoError(t, err)
	cfg := discoverySequenceConfig(database, nodeID)
	require.NoError(t, runtimeApp.ComposeAndInstall(
		context.Background(),
		func(rt yapp.Runtime) (*yapp.BusinessBundle, error) {
			return sequenceapp.InitializeBundle(rt, cfg)
		},
	))
	require.NoError(t, runtimeApp.Start(context.Background()))
	return &discoveredSequenceApp{app: runtimeApp, manager: manager}
}

func newDiscoveredSequenceClient(
	t *testing.T,
	etcdEndpoint string,
) (*sequencepkg.Router, sequencev1.SequenceGeneratorClient, func()) {
	t.Helper()
	manager := newDiscoveryConfigManager(t, discoveryConfig(etcdEndpoint, "", true))
	var routed sequencev1.SequenceGeneratorClient
	router, err := sequencepkg.NewRouter(func(
		ctx context.Context,
		knownVersion int64,
	) (*sequencev1.GetRouteResponse, error) {
		return routed.GetRoute(ctx, &sequencev1.GetRouteRequest{KnownVersion: knownVersion})
	})
	require.NoError(t, err)
	runtimeApp, err := yapp.New(
		"sequence-etcd-discovery-client",
		yapp.WithConfigManager(manager),
		yapp.WithProcessDefaults(false),
		yapp.WithModules(etcdmodule.Module(), sequencepkg.NewRoutingModule(router)),
	)
	require.NoError(t, err)
	client, err := runtimeApp.NewClient(context.Background(), sequenceAppName)
	require.NoError(t, err)
	routed = sequencev1.NewSequenceGeneratorClient(client)
	closeClient := func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		require.NoError(t, client.Close())
		require.NoError(t, runtimeApp.Stop(closeCtx))
		require.NoError(t, manager.Close())
	}
	return router, routed, closeClient
}

func discoveryConfig(etcdEndpoint, nodeID string, client bool) map[string]any {
	yggdrasilConfig := map[string]any{
		"admin": map[string]any{
			"application": map[string]any{
				"namespace": "default",
				"metadata":  map[string]any{sequencepkg.NodeIDAttribute: nodeID},
			},
			"governor": map[string]any{"port": 0},
		},
		"etcd": map[string]any{"clients": map[string]any{
			"default": map[string]any{
				"endpoints":    []string{etcdEndpoint},
				"dial_timeout": "5s",
			},
		}},
		"discovery": map[string]any{
			"registry": map[string]any{
				"type": "etcd",
				"config": map[string]any{
					"client":         "default",
					"prefix":         etcdRegistryPrefix,
					"ttl":            "3s",
					"keep_alive":     true,
					"retry_interval": "200ms",
				},
			},
			"resolvers": map[string]any{
				"etcd": map[string]any{
					"type": "etcd",
					"config": map[string]any{
						"client":    "default",
						"prefix":    etcdRegistryPrefix,
						"namespace": "default",
						"protocols": []string{"grpc"},
						"debounce":  "20ms",
					},
				},
			},
		},
	}
	if client {
		yggdrasilConfig["clients"] = map[string]any{"services": map[string]any{
			sequenceAppName: map[string]any{
				"fast_fail": true,
				"resolver":  "etcd",
				"balancer":  sequencepkg.BalancerType,
				"interceptors": map[string]any{
					"unary": []string{sequencepkg.InterceptorName},
				},
			},
		}}
		yggdrasilConfig["balancers"] = map[string]any{"defaults": map[string]any{
			sequencepkg.BalancerType: map[string]any{"type": sequencepkg.BalancerType},
		}}
		yggdrasilConfig["transports"] = map[string]any{"grpc": map[string]any{
			"client": map[string]any{},
			"server": map[string]any{},
		}}
	} else {
		yggdrasilConfig["server"] = map[string]any{"transports": []string{"grpc"}}
		yggdrasilConfig["transports"] = map[string]any{"grpc": map[string]any{
			"server": map[string]any{"address": "127.0.0.1:0"},
		}}
	}
	return map[string]any{"yggdrasil": yggdrasilConfig}
}

func newDiscoveryConfigManager(t *testing.T, values map[string]any) *config.Manager {
	t.Helper()
	manager := config.NewManager()
	require.NoError(t, manager.LoadLayer(
		"test",
		config.PriorityOverride,
		memory.NewSource("test", values),
	))
	return manager
}

func discoverySequenceConfig(database *harness, nodeID string) *conf.Config {
	cfg := &conf.Config{
		Database: discoveryDatabaseConfig(database),
		Allocator: biz.AllocatorConfig{
			DefaultStep:              100,
			MaxStep:                  10000,
			PrefetchRatio:            0.5,
			StepIncreaseThreshold:    15 * time.Minute,
			StepDecreaseThreshold:    30 * time.Minute,
			ReserveTimeout:           time.Second,
			IdleTimeout:              24 * time.Hour,
			CleanupInterval:          time.Second,
			CleanupSlotsPerRun:       64,
			MemoryHighWatermarkRatio: 0.9,
		},
		Node: biz.NodeConfig{
			ID: nodeID, HeartbeatTimeoutTicks: 3, RouteQueryTimeout: 150 * time.Millisecond,
		},
		Ticker: task.Config{BaseTickInterval: 50 * time.Millisecond, HeartbeatTicks: 1},
	}
	cfg.SetDefaults()
	return cfg
}

func discoveryDatabaseConfig(database *harness) xgorm.Config {
	return xgorm.Config{
		Driver: database.driver, DSN: database.dsn,
		ExpectedDatabase: databaseName, ExpectedAccount: databaseUser,
		MaxOpenConns: 20, MaxIdleConns: 5, ConnMaxLifetime: 30 * time.Minute,
	}
}

func resetSequenceDatabase(t *testing.T, database *harness) error {
	t.Helper()
	db := openGORM(t, database)
	if err := db.Exec("DELETE FROM sequence_ranges").Error; err != nil {
		return err
	}
	return db.Exec("DELETE FROM sequence_routes").Error
}

func publishDiscoveryRoute(
	t *testing.T,
	database *harness,
	version int64,
	owners map[string][]uint32,
) int64 {
	t.Helper()
	nodeIDs := make([]string, 0, len(owners))
	for nodeID := range owners {
		nodeIDs = append(nodeIDs, nodeID)
	}
	sort.Strings(nodeIDs)
	nodes := make([]map[string]any, 0, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		nodes = append(nodes, map[string]any{"node_id": nodeID, "slots": owners[nodeID]})
	}
	payload, err := json.Marshal(map[string]any{"nodes": nodes})
	require.NoError(t, err)
	db := openGORM(t, database)
	require.NoError(t, db.Create(&gormdata.RouteModel{
		Version: version, Payload: payload, CreatedAt: time.Now().UTC(),
	}).Error)
	return version
}

func waitForRegisteredNodes(t *testing.T, etcdClient *clientv3.Client, expected ...string) {
	t.Helper()
	require.Eventually(t, func() bool {
		records, err := registeredSequenceRecords(etcdClient)
		if err != nil || len(records) != len(expected) {
			return false
		}
		remaining := make(map[string]struct{}, len(expected))
		for _, nodeID := range expected {
			remaining[nodeID] = struct{}{}
		}
		for _, record := range records {
			if record.Name != sequenceAppName || record.Namespace != "default" ||
				len(record.Endpoints) != 1 || record.Endpoints[0].Scheme != "grpc" ||
				!strings.HasPrefix(record.Endpoints[0].Address, "127.0.0.1:") {
				return false
			}
			delete(remaining, record.Metadata[sequencepkg.NodeIDAttribute])
		}
		return len(remaining) == 0
	}, discoveryTestTimeout, 50*time.Millisecond)
}

func registeredSequenceRecords(
	etcdClient *clientv3.Client,
) ([]discoveryInstanceRecord, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	response, err := etcdClient.Get(
		ctx,
		fmt.Sprintf("%s/default/%s/", etcdRegistryPrefix, sequenceAppName),
		clientv3.WithPrefix(),
	)
	if err != nil {
		return nil, err
	}
	records := make([]discoveryInstanceRecord, 0, len(response.Kvs))
	for _, item := range response.Kvs {
		var record discoveryInstanceRecord
		if err := json.Unmarshal(item.Value, &record); err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, nil
}

func waitForDiscoveredAllocation(
	t *testing.T,
	client sequencev1.SequenceGeneratorClient,
	key string,
) int64 {
	t.Helper()
	var id int64
	var lastErr error
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		response, err := client.FetchNext(ctx, &sequencev1.FetchNextRequest{Key: key})
		lastErr = err
		if err != nil || response.GetId() <= 0 {
			return false
		}
		id = response.GetId()
		return true
	}, discoveryTestTimeout, 50*time.Millisecond, "last allocation error: %v", lastErr)
	return id
}
