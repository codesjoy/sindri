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
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	sequencev1 "github.com/codesjoy/sindri/gen/go/sequence/v1"
	sequencepkg "github.com/codesjoy/sindri/pkg/sequence"
	etcdmodule "github.com/codesjoy/yggdrasil-ecosystem/modules/etcd/v3"
	yapp "github.com/codesjoy/yggdrasil/v3/app"
	"github.com/codesjoy/yggdrasil/v3/config"
	"github.com/codesjoy/yggdrasil/v3/config/source/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcetcd "github.com/testcontainers/testcontainers-go/modules/etcd"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	sequenceAppName      = "github.com.codesjoy.skuld.sequence"
	etcdImage            = "gcr.io/etcd-development/etcd:v3.5.14"
	etcdRegistryPrefix   = "/sindri/tests/sequence/registry"
	discoveryTestTimeout = 30 * time.Second
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

func TestSequenceEtcdDiscoveryAcrossDialects(t *testing.T) {
	forEachDialect(t, func(t *testing.T, item *harness) {
		runSequenceEtcdDiscovery(t, item)
	})
}

func runSequenceEtcdDiscovery(t *testing.T, database *harness) {
	t.Helper()
	require.NoError(t, resetSequenceDatabase(t, database))
	// One bounded pool for the whole test: the shutdown checks poll the
	// database, and a fresh pool per poll would exhaust the server's slots.
	db := openGORM(t, database)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(4)

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

	nodeA := startSequenceProcess(t, sequenceProcessOptions{
		database: database, etcdEndpoint: etcdEndpoint,
		nodeID: "node-a", configuredMode: "data",
	})
	nodeB := startSequenceProcess(t, sequenceProcessOptions{
		database: database, etcdEndpoint: etcdEndpoint,
		nodeID: "node-b", configuredMode: "data",
	})
	stoppedNodeA := false
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if !stoppedNodeA {
			_ = nodeA.stop(stopCtx)
		}
		_ = nodeB.stop(stopCtx)
	})

	waitForRegisteredNodes(t, etcdClient, "node-a", "node-b")

	router, routedClient, closeClient := newDiscoveredSequenceClient(t, etcdEndpoint)
	t.Cleanup(closeClient)

	route := splitSlots()
	version := publishDiscoveryRoute(t, database, route)
	keyA := keyForOwner("node-a", route)
	keyB := keyForOwner("node-b", route)
	firstA := waitForDiscoveredAllocation(t, routedClient, keyA)
	firstB := waitForDiscoveredAllocation(t, routedClient, keyB)
	require.Equal(t, version, router.Version())
	require.Positive(t, firstA)
	require.Positive(t, firstB)
	for _, node := range []*sequenceProcess{nodeA, nodeB} {
		require.Eventually(t, func() bool {
			status, report, err := probeReadiness(node.readyURL)
			return err == nil && status == http.StatusOK && report.Ready
		}, discoveryTestTimeout, 50*time.Millisecond, "readiness never turned green")
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	require.NoError(t, nodeA.stop(stopCtx))
	cancel()
	stoppedNodeA = true
	waitForRegisteredNodes(t, etcdClient, "node-b")
	// The shutdown path is observable in the database rather than in the
	// composition: the departing node stops renewing its liveness row, so the
	// fleet's live set drops it, and the slots it held become assignable to the
	// node that remains.
	waitForNodeToLeaveLiveSet(t, db, "node-a")
	requireSlotsOwnedBy(t, db, "node-b", route["node-a"][:4])

	version = publishDiscoveryRoute(t, database, allSlots("node-b"))
	require.Eventually(t, func() bool {
		refreshCtx, refreshCancel := context.WithTimeout(context.Background(), time.Second)
		defer refreshCancel()
		return router.Refresh(refreshCtx) == nil && router.Version() == version
	}, discoveryTestTimeout, 50*time.Millisecond)
	afterHandoff := waitForDiscoveredAllocation(t, routedClient, keyA)
	require.Greater(t, afterHandoff, firstA)
}

func newDiscoveredSequenceClient(
	t *testing.T,
	etcdEndpoint string,
) (*sequencepkg.Router, sequencev1.SequenceGeneratorClient, func()) {
	t.Helper()
	manager := newDiscoveryConfigManager(t, discoveryClientConfig(etcdEndpoint))
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
		yapp.WithModules(etcdmodule.Module(), sequencepkg.NewModule(router)),
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

// discoveryClientConfig is the client half of the discovery wiring: it resolves
// the sequence service through etcd. The server half lives in the process
// configuration the harness renders, so the test drives the same registry the
// deployment does rather than a test-only composition.
func discoveryClientConfig(etcdEndpoint string) map[string]any {
	yggdrasilConfig := map[string]any{
		"admin": map[string]any{
			"application": map[string]any{
				"namespace": "default",
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

func resetSequenceDatabase(t *testing.T, database *harness) error {
	t.Helper()
	db := openGORM(t, database)
	// Local planning reads the authority and liveness rows, so a row left behind
	// by a previous test is a placement this test's nodes would act on. Put the
	// tables a node reads or writes back to the state the migration leaves them
	// in, including the ownership view the planner treats as authoritative.
	statements := []string{
		"DELETE FROM sequence_ranges",
		"DELETE FROM sequence_route_snapshot",
		"DELETE FROM sequence_node_liveness",
		"DELETE FROM sequence_slot_handoffs",
		"UPDATE sequence_coordinator SET owner_instance_id = NULL, expires_at = NULL WHERE id = 1",
		"UPDATE sequence_slot_ownership SET owner_instance_id = NULL, epoch = 0, state = 'UNOWNED'",
		"DELETE FROM sequence_instance_leases",
	}
	for _, statement := range statements {
		if err := db.Exec(statement).Error; err != nil {
			return err
		}
	}
	return nil
}

func publishDiscoveryRoute(
	t *testing.T,
	database *harness,
	owners map[string][]uint32,
) int64 {
	t.Helper()
	db := openGORM(t, database)
	seedSlotOwnership(t, db, owners, processLeaseDuration, processNodeTTL)
	return publishSeededRoute(t, db)
}

// waitForRegisteredNodes waits until every expected node is registered with the
// endpoints a sequence instance advertises.
//
// A data node advertises exactly one endpoint: the gRPC listener that serves the
// sequence RPCs and is what a caller resolves. The readiness probe is bound to
// the governor's admin listener, which is not a service transport and is
// deliberately not advertised to callers.
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
				!advertisesExactlyOne(record, "grpc") {
				return false
			}
			delete(remaining, record.Metadata[sequencepkg.NodeIDAttribute])
		}
		return len(remaining) == 0
	}, discoveryTestTimeout, 50*time.Millisecond)
}

// advertisesExactlyOne reports whether the record advertises exactly one
// loopback endpoint with the given scheme.
func advertisesExactlyOne(record discoveryInstanceRecord, scheme string) bool {
	matches := 0
	for _, endpoint := range record.Endpoints {
		if endpoint.Scheme == scheme && strings.HasPrefix(endpoint.Address, "127.0.0.1:") {
			matches++
		}
	}
	return matches == 1
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
	var lastResponse *sequencev1.FetchNextResponse
	converged := assert.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		response, err := client.FetchNext(ctx, &sequencev1.FetchNextRequest{Key: key})
		lastErr = err
		lastResponse = response
		if err != nil || response.GetId() <= 0 {
			return false
		}
		id = response.GetId()
		return true
	}, discoveryTestTimeout, 50*time.Millisecond)
	require.True(
		t,
		converged,
		"allocation for %q never succeeded: last error: %v, response: %v",
		key,
		lastErr,
		lastResponse,
	)
	return id
}
