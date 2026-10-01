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
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	toxiclient "github.com/Shopify/toxiproxy/v2/client"
	"github.com/codesjoy/pkg/basic/xerror"
	"github.com/codesjoy/sindri/gen/go/sequence/reason"
	sequencev1 "github.com/codesjoy/sindri/gen/go/sequence/v1"
	"github.com/codesjoy/sindri/internal/sequence/biz"
	sequencedata "github.com/codesjoy/sindri/internal/sequence/data"
	"github.com/codesjoy/sindri/internal/sequence/service"
	sequencepkg "github.com/codesjoy/sindri/pkg/sequence"
	yapp "github.com/codesjoy/yggdrasil/v3/app"
	"github.com/codesjoy/yggdrasil/v3/config"
	"github.com/codesjoy/yggdrasil/v3/config/source/memory"
	"github.com/codesjoy/yggdrasil/v3/discovery/resolver"
	"github.com/codesjoy/yggdrasil/v3/module"
	"github.com/codesjoy/yggdrasil/v3/rpc/metadata"
	transportclient "github.com/codesjoy/yggdrasil/v3/transport/runtime/client"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/go-connections/nat"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	tctoxiproxy "github.com/testcontainers/testcontainers-go/modules/toxiproxy"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/genproto/googleapis/rpc/code"
)

const (
	sequenceServiceName = "codesjoy.sindri.sequence.v1.SequenceGenerator"
	// recoveryDeadline bounds how long a test waits for a node to become
	// serviceable again. A handoff is no longer just a routing change: the old
	// owner drains, releases its epoch and the new owner claims it, so several
	// database round trips now sit between publishing a route and serving it.
	// The deadline is sized for that work under test-container contention, not
	// for the protocol's behaviour, which the zero-regression assertions cover.
	recoveryDeadline = 15 * time.Second
	// These are the bounds the containers run with. The configuration is
	// rendered from them and the assertions read the same numbers, so a test
	// cannot end up reasoning about a window the fleet is not running with.
	systemQuietWindow   = 3 * time.Second
	systemLeaseDuration = 2 * time.Second
	systemNodeTTL       = 2 * time.Second
)

type systemNode struct {
	id        string
	container testcontainers.Container
	grpcAddr  string
	// readyAddr addresses the node's high-availability report on its governor
	// admin listener. It is recorded at startup rather than derived later because
	// the report is the only way a test can observe a state the process does not
	// otherwise announce, such as the release at shutdown.
	readyAddr string
}

// SequenceSystemSuite owns real proxy and service lifecycles for one database dialect.
// The package TestMain owns the two real databases so store and system tests share them.
type SequenceSystemSuite struct {
	suite.Suite

	dialect        string
	h              *harness
	ctx            context.Context
	proxyContainer *tctoxiproxy.Container
	proxyClient    *toxiclient.Client
	proxies        map[string]*toxiclient.Proxy
	nodes          map[string]*systemNode
	clients        map[string]transportclient.Client
	apps           []*yapp.App
	managers       []*config.Manager
	router         *sequencepkg.Router
}

func (s *SequenceSystemSuite) SetupSuite() {
	s.ctx = context.Background()
	if !enabledDialects[s.dialect] {
		s.T().Skipf("%s is not enabled by %s", s.dialect, testDialectsEnv)
	}
	s.h = harnessByDialect(s.dialect)
	s.Require().NotNil(s.h)
	s.Require().NotEmpty(
		os.Getenv("SKULD_SEQUENCE_TEST_IMAGE"),
		"set SKULD_SEQUENCE_TEST_IMAGE or run make test-sequence-integration",
	)
}

func (s *SequenceSystemSuite) SetupTest() {
	s.proxyContainer = nil
	s.proxyClient = nil
	s.proxies = nil
	s.nodes = nil
	s.clients = make(map[string]transportclient.Client)
	s.apps = nil
	s.managers = nil
	s.router = nil
	s.Require().NoError(s.resetDatabase())
	var err error
	s.proxyContainer, err = tctoxiproxy.Run(
		s.ctx,
		"ghcr.io/shopify/toxiproxy:2.12.0",
		testcontainers.WithExposedPorts("8668/tcp", "8669/tcp"),
		network.WithNetwork([]string{"toxiproxy"}, s.h.network),
	)
	s.Require().NoError(err)
	uri, err := s.proxyContainer.URI(s.ctx)
	s.Require().NoError(err)
	s.proxyClient = toxiclient.NewClient(uri)
	proxyConfigs := []struct {
		name     string
		listen   string
		upstream string
	}{
		{
			name:     "db-a",
			listen:   "0.0.0.0:8666",
			upstream: fmt.Sprintf("%s:%s", s.h.dbAlias, s.h.dbPort),
		},
		{
			name:     "db-b",
			listen:   "0.0.0.0:8667",
			upstream: fmt.Sprintf("%s:%s", s.h.dbAlias, s.h.dbPort),
		},
		{name: "grpc-a", listen: "0.0.0.0:8668", upstream: "node-a:19010"},
		{name: "grpc-b", listen: "0.0.0.0:8669", upstream: "node-b:19010"},
	}
	for _, proxyConfig := range proxyConfigs {
		_, err = s.proxyClient.CreateProxy(
			proxyConfig.name,
			proxyConfig.listen,
			proxyConfig.upstream,
		)
		s.Require().NoError(err)
	}
	s.proxies, err = s.proxyClient.Proxies()
	s.Require().NoError(err)
	s.nodes = make(map[string]*systemNode, 2)
	s.nodes["node-a"] = s.startNode("node-a", 8666, 8668)
	s.nodes["node-b"] = s.startNode("node-b", 8667, 8669)
}

func (s *SequenceSystemSuite) TearDownTest() {
	if s.T().Failed() {
		s.dumpLogs()
	}
	// A test that failed between quiescing and resuming would otherwise leave the
	// next test's nodes talking to a disabled proxy.
	s.resumeDatabase()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for _, client := range s.clients {
		_ = client.Close()
	}
	for _, runtimeApp := range s.apps {
		_ = runtimeApp.Stop(ctx)
	}
	for _, manager := range s.managers {
		_ = manager.Close()
	}
	for _, node := range s.nodes {
		if node != nil && node.container != nil {
			if err := node.container.Terminate(ctx); err != nil {
				if !strings.Contains(err.Error(), "No such container") {
					s.T().Errorf("terminate %s: %v", node.id, err)
				}
			}
		}
	}
	if s.proxyContainer != nil {
		if err := s.proxyContainer.Terminate(ctx); err != nil {
			if !strings.Contains(err.Error(), "No such container") {
				s.T().Errorf("terminate toxiproxy: %v", err)
			}
		}
	}
	s.proxyContainer = nil
	s.proxyClient = nil
	s.proxies = nil
	s.nodes = nil
	s.clients = nil
	s.apps = nil
	s.managers = nil
	s.router = nil
}

func (s *SequenceSystemSuite) startNode(id string, dbProxyPort, grpcProxyPort int) *systemNode {
	dsn := fmt.Sprintf(
		"postgres://%s:%s@toxiproxy:%d/%s?sslmode=disable",
		databaseUser,
		databasePass,
		dbProxyPort,
		databaseName,
	)
	if s.h.name == "mysql" {
		dsn = fmt.Sprintf(
			"%s:%s@tcp(toxiproxy:%d)/%s?parseTime=true",
			databaseUser,
			databasePass,
			dbProxyPort,
			databaseName,
		)
	}
	configYAML := fmt.Sprintf(`yggdrasil:
  mode: dev
  admin:
    # A fixed port, bound to every interface: the harness reads the
    # high-availability report from outside the container, and an ephemeral port
    # could not be declared for mapping. Production binds this to loopback.
    governor: {bind: "0.0.0.0", port: 8080}
  observability:
    telemetry:
      tracer: otlp-grpc
      meter: otlp-grpc
      providers:
        otlp:
          trace: {endpoint: localhost:4317, tls: {insecure: true}}
          metric: {endpoint: localhost:4317, tls: {insecure: true}}
  server:
    transports: [grpc]
    interceptors: {unary: [protovalidate]}
  transports:
    grpc:
      server: {address: ":19010"}
app:
  sequence:
    # Every node in this suite is a data-plane process: it plans and claims its
    # own slots. The tests play the publisher themselves, through the real
    # MaterialiseRoute path, so the deployment shape under test is the
    # production one: StatefulSet members as data, publication elsewhere.
    mode: data
    runtime:
      memory_limit: 512MiB
      auto_memory_limit_ratio: 0.8
    database:
      driver: %s
      dsn: %q
      expected_database: %s
      expected_account: %s
      max_open_conns: 20
      max_idle_conns: 5
      conn_max_lifetime: 30m
    dataplane:
      allocator:
        default_step: 100
        max_step: 10000
        prefetch_ratio: 0.5
        prefetch_latency_multiplier: 4
        prefetch_latency_window: 5m
        prefetch_latency_min_samples: 100
        prefetch_rate_reset_after: 1m
        step_increase_threshold: 15m
        step_decrease_threshold: 30m
        reserve_timeout: 1s
      node:
        id: %s
        heartbeat_timeout_ticks: 3
        route_query_timeout: 150ms
      # The quiet window is deliberately short here: these tests assert protocol
      # behaviour, not the platform's real pause bound, and the suite's recovery
      # deadlines are far below a production-sized window. The other bounds are
      # scaled with it: the window must exceed
      # lease_duration + clock_drift + max_pause + clock_jump + safety_margin
      # (2s + 670ms here), and the local deadline L - epsilon must cover one
      # renewal plus one reservation (250ms + 1s).
      #
      # The clock bounds are not scaled down with the rest. The storage clock
      # monitor compares successive readings the way a node measures its own
      # elapsed time, so what it bounds is not only the platform's drift but also
      # the variance of a storage clock read: a read that returns 100ms faster
      # than the one before it reads as the storage clock having fallen behind by
      # that much. Measured on these containers that variance reaches about
      # 105ms -- the reads go through a proxy hop and the containers share a
      # loaded host -- and a bound below it fences a healthy instance for the life
      # of the process, which then serves nothing for the rest of the test. 300ms
      # on both bounds is above the observed tail.
      #
      # The lease is what a healthy owner serves from between confirmed renewals,
      # and it has to stay above the renewal cost, which is why it is not simply
      # as short as the bounds allow: granted_at is only refreshed when the
      # renewal transaction commits, and that transaction writes a grant for every
      # slot the node holds -- half the slot space here, so thousands of rows
      # through a proxy hop. That cost is where this harness stops being able to
      # compress: it measured around 200ms on PostgreSQL but above 600ms on MySQL
      # once the whole suite was loading the host, and a lease below it makes a
      # healthy owner refuse batches for part of every renewal cycle. A batch call
      # that does not retry then fails. 2s is several times the worst renewal seen
      # here, and the takeover tests below still fit inside their recovery
      # deadline.
      ha:
        quiet_window: %s
        lease_duration: %s
        pause_verified: true
        clock_disciplined: true
        max_pause: 50ms
        clock_drift: 300ms
        clock_jump: 300ms
        safety_margin: 20ms
        renew_interval: 250ms
        release_drain_timeout: 1s
        # A crashed node has to leave the live set inside this suite's deadlines,
        # so the TTL is scaled the same way the lease was. It still has to exceed
        # the heartbeat period by a wide margin (50ms here).
        node_ttl: %s
    ticker:
      base_tick_interval: 50ms
      heartbeat_ticks: 1
`,
		s.h.driver,
		dsn,
		databaseName,
		databaseUser,
		id,
		systemQuietWindow,
		systemLeaseDuration,
		systemNodeTTL,
	)

	container, err := testcontainers.Run(
		s.ctx,
		os.Getenv("SKULD_SEQUENCE_TEST_IMAGE"),
		testcontainers.WithEnv(map[string]string{
			// etcd and Polaris currently ship legacy descriptors with the same
			// filename. Keep the real Sequence module set while protobuf
			// reports that ecosystem conflict as a warning in system tests.
			"GOLANG_PROTOBUF_REGISTRATION_CONFLICT": "warn",
		}),
		testcontainers.WithHostConfigModifier(func(config *container.HostConfig) {
			config.Memory = 640 << 20
		}),
		testcontainers.WithExposedPorts("19010/tcp", "8080/tcp"),
		testcontainers.WithFiles(testcontainers.ContainerFile{
			Reader:            strings.NewReader(configYAML),
			ContainerFilePath: "/tmp/sequence.yaml",
			FileMode:          0o644,
		}),
		testcontainers.WithCmdArgs("--yggdrasil-config=/tmp/sequence.yaml"),
		network.WithNetwork([]string{id}, s.h.network),
		// Two node containers start per test, on top of the dialect's database
		// container and toxiproxy. When both dialects run in one process the
		// host is starting six containers at once, and a 45s budget turned that
		// contention into an unrelated startup failure. Match the store
		// integration harness, which already allows two minutes.
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("19010/tcp").WithStartupTimeout(2*time.Minute),
		),
	)
	if err != nil {
		if container != nil {
			logs, logErr := container.Logs(context.Background())
			if logErr == nil {
				body, _ := io.ReadAll(logs)
				_ = logs.Close()
				s.T().Logf("%s startup logs:\n%s", id, body)
			}
			_ = container.Terminate(context.Background())
		}
		s.Require().NoError(err)
	}
	endpoint, err := s.proxyContainer.PortEndpoint(
		s.ctx,
		nat.Port(fmt.Sprintf("%d/tcp", grpcProxyPort)),
		"",
	)
	s.Require().NoError(err)
	readyPort, err := container.MappedPort(s.ctx, "8080/tcp")
	s.Require().NoError(err)
	host, err := container.Host(s.ctx)
	s.Require().NoError(err)
	return &systemNode{
		id:        id,
		container: container,
		grpcAddr:  endpoint,
		readyAddr: fmt.Sprintf("http://%s:%s%s", host, readyPort.Port(), service.ReadinessPath),
	}
}

func (s *SequenceSystemSuite) restartNode(id string) {
	node := s.nodes[id]
	s.Require().NotNil(node)
	s.Require().NoError(node.container.Start(s.ctx))
	s.Require().Eventually(func() bool {
		state, err := node.container.State(s.ctx)
		return err == nil && state.Running
	}, recoveryDeadline, 50*time.Millisecond)
}

func (s *SequenceSystemSuite) stopNode(id string) {
	zero := time.Duration(0)
	s.Require().NoError(s.nodes[id].container.Stop(s.ctx, &zero))
}

func (s *SequenceSystemSuite) resetDatabase() error {
	db := openGORM(s.T(), s.h)
	// Every table a node reads or writes is put back to the state the migration
	// leaves it in. The authority view matters most: with local planning a row a
	// previous test left behind is a placement the next test's nodes would act
	// on, so a stale owner would decide where a slot lives before the test says
	// anything.
	statements := []string{
		"DELETE FROM sequence_ranges",
		"DELETE FROM sequence_routes",
		"DELETE FROM sequence_node_liveness",
		"UPDATE sequence_route_state SET revision = 1",
		"UPDATE sequence_coordinator SET owner_instance_id = NULL, expires_at = NULL WHERE id = 1",
		"UPDATE slot_ownership SET owner_node_id = NULL, owner_instance_id = NULL, " +
			"epoch = 0, granted_at = NULL, state = 'UNOWNED'",
	}
	for _, statement := range statements {
		if err := db.Exec(statement).Error; err != nil {
			return err
		}
	}
	return nil
}

func (s *SequenceSystemSuite) dumpLogs() {
	for id, node := range s.nodes {
		if node == nil || node.container == nil {
			continue
		}
		logs, err := node.container.Logs(context.Background())
		if err != nil {
			continue
		}
		body, _ := io.ReadAll(logs)
		_ = logs.Close()
		s.T().Logf("%s logs:\n%s", id, body)
	}
	if s.proxyContainer != nil {
		logs, err := s.proxyContainer.Logs(context.Background())
		if err == nil {
			body, _ := io.ReadAll(logs)
			_ = logs.Close()
			s.T().Logf("toxiproxy logs:\n%s", body)
		}
	}
}

func (s *SequenceSystemSuite) publishRoute(owners map[string][]uint32) int64 {
	// The authority view is staged with the data plane held off the database.
	//
	// A node renews the grant of every slot it holds on each heartbeat, and it
	// claims whatever its own plan hands it. A staged view rewrites the same
	// rows, so without this the stage and the renewals would lock the same
	// thousands of rows in different orders -- which is a deadlock the storage
	// resolves by killing one of them -- and a claim that committed after the
	// stage would take a slot the stage had just assigned elsewhere. Cutting the
	// two proxies for the moment the transaction runs makes the stage a change
	// the fleet sees in full, and the nodes reconnect on their next heartbeat.
	//
	// This is the harness's stand-in for a deployment that changes authority
	// while the old owner is unreachable: the nodes never lose their own
	// reasoning, they simply read the new view when the database answers again.
	s.quiesceDatabase()
	db := openGORM(s.T(), s.h)
	seedSlotOwnership(s.T(), db, owners)
	version := publishSeededRoute(s.T(), db)
	s.resumeDatabase()
	return version
}

// quiesceDatabase makes the database unreachable for both node containers.
//
// It is paired with resumeDatabase and is deliberately not a hook: a test that
// fails while the proxies are down must still have them enabled by TearDownTest,
// which calls resumeDatabase after terminating the node containers.
func (s *SequenceSystemSuite) quiesceDatabase() {
	for _, name := range []string{"db-a", "db-b"} {
		s.Require().NoError(s.proxies[name].Disable())
	}
}

// resumeDatabase restores the database proxies. It reports failures instead of
// raising them, so it is safe to call from the teardown path after a failure.
func (s *SequenceSystemSuite) resumeDatabase() {
	for _, name := range []string{"db-a", "db-b"} {
		proxy, ok := s.proxies[name]
		if !ok {
			continue
		}
		if err := proxy.Enable(); err != nil {
			s.T().Errorf("enable %s proxy: %v", name, err)
		}
	}
}

func allSlots(owner string) map[string][]uint32 {
	slots := make([]uint32, biz.SlotCount)
	for index := range slots {
		slots[index] = uint32(index)
	}
	return map[string][]uint32{owner: slots}
}

func splitSlots() map[string][]uint32 {
	route := map[string][]uint32{
		"node-a": make([]uint32, 0, biz.SlotCount/2),
		"node-b": make([]uint32, 0, biz.SlotCount/2),
	}
	for slot := uint32(0); slot < biz.SlotCount; slot++ {
		owner := "node-a"
		if slot%2 != 0 {
			owner = "node-b"
		}
		route[owner] = append(route[owner], slot)
	}
	return route
}

func keyForOwner(owner string, route map[string][]uint32) string {
	return keysForOwner(owner, route, 1)[0]
}

func keysForOwner(owner string, route map[string][]uint32, count int) []string {
	owned := make(map[uint32]struct{}, len(route[owner]))
	for _, slot := range route[owner] {
		owned[slot] = struct{}{}
	}
	keys := make([]string, 0, count)
	for index := 0; len(keys) < count; index++ {
		key := fmt.Sprintf("system-%s-%d", owner, index)
		if _, ok := owned[biz.SlotForKey(key)]; ok {
			keys = append(keys, key)
		}
	}
	return keys
}

// waitForBootstrapAllocation serves one allocation with no directory
// published.
//
// The fleet bootstraps without a publisher: ownership is the authority each
// node plans from, so a live node claims the slots its own plan hands it and
// serves them directly. The owner is read from the table on every attempt
// because the split moves while the second node comes up -- the first node
// claims what is unowned before its peer registers, and the plan then hands
// part of that space to the peer.
func (s *SequenceSystemSuite) waitForBootstrapAllocation(key string) int64 {
	db := openGORM(s.T(), s.h)
	var id int64
	var lastErr error
	converged := assert.Eventually(s.T(), func() bool {
		var owner string
		if err := db.Table("slot_ownership").
			Select("owner_node_id").
			Where("slot_id = ?", biz.SlotForKey(key)).
			Scan(&owner).Error; err != nil || owner == "" {
			lastErr = fmt.Errorf("owner read: %w", err)
			return false
		}
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		response, err := s.fetchDirect(ctx, owner, key, 0)
		if err != nil {
			lastErr = fmt.Errorf("fetch from %s: %w", owner, err)
			return false
		}
		id = response.GetId()
		if id <= 0 {
			lastErr = fmt.Errorf("fetch from %s returned id %d", owner, id)
			return false
		}
		return true
	}, recoveryDeadline, 50*time.Millisecond)
	s.Require().True(converged, "bootstrap allocation for %q: %v", key, lastErr)
	return id
}

// waitForOwnership serves one allocation from the named node and returns its id.
// The node has to hold the key's slot and have applied the route the caller
// names, so the wait covers both halves of "this node owns the key".
func (s *SequenceSystemSuite) waitForOwnership(nodeID, key string, version int64) int64 {
	var id int64
	var lastErr error
	converged := assert.Eventually(s.T(), func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		response, err := s.fetchDirect(ctx, nodeID, key, version)
		if err != nil {
			lastErr = err
			return false
		}
		id = response.GetId()
		return id > 0
	}, recoveryDeadline, 50*time.Millisecond)
	s.Require().True(
		converged,
		"%s did not serve %q at route %d: %v",
		nodeID,
		key,
		version,
		lastErr,
	)
	return id
}

// waitForLiveNode waits until the node is back in the fleet's live set.
//
// A liveness row outlives the process that wrote it -- expiry is a read, not a
// delete -- so a row being present says nothing about whether the node is still
// talking to the storage. Anything that stages authority for a node after an
// outage has to wait for this first: while the node is outside the live set its
// slots are planned onto its peer, and the stage would hand them there instead.
func (s *SequenceSystemSuite) waitForLiveNode(nodeID string) {
	placement := sequencedata.NewPlacementData(openGORM(s.T(), s.h))
	s.Require().Eventually(func() bool {
		live, err := placement.LiveNodes(s.ctx, systemNodeTTL)
		if err != nil {
			return false
		}
		for _, node := range live {
			if node.ID == nodeID {
				return true
			}
		}
		return false
	}, recoveryDeadline, 50*time.Millisecond, "%s never rejoined the live set", nodeID)
}

func (s *SequenceSystemSuite) waitForRoute(nodeID string, version int64) {
	s.Require().Eventually(func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		response, err := s.directClient(nodeID).GetRoute(ctx, &sequencev1.GetRouteRequest{})
		return err == nil && response.GetRoute() != nil &&
			response.GetRoute().GetVersion() >= version
	}, recoveryDeadline, 50*time.Millisecond)
}

func (s *SequenceSystemSuite) waitForRejection(nodeID, key string, version int64) {
	s.Require().Eventually(func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		_, err := s.fetchDirect(ctx, nodeID, key, version)
		return xerror.IsReason(err, reason.Reason_SEQUENCE_SLOT_NOT_OWNER) ||
			xerror.IsReason(err, reason.Reason_SEQUENCE_ROUTE_EXPIRED)
	}, recoveryDeadline, 50*time.Millisecond)
}

func (s *SequenceSystemSuite) fetchDirect(
	ctx context.Context,
	nodeID, key string,
	version int64,
) (*sequencev1.FetchNextResponse, error) {
	ctx = metadata.WithOutContext(
		ctx,
		metadata.Pairs(sequencepkg.VersionMetaKey, fmt.Sprintf("%d", version)),
	)
	return s.directClient(nodeID).FetchNext(ctx, &sequencev1.FetchNextRequest{Key: key})
}

func (s *SequenceSystemSuite) directClient(nodeID string) sequencev1.SequenceGeneratorClient {
	cacheKey := "direct-" + nodeID
	if cached := s.clients[cacheKey]; cached != nil {
		return sequencev1.NewSequenceGeneratorClient(cached)
	}
	client := s.newClient(
		cacheKey,
		[]resolver.BaseEndpoint{{Address: s.nodes[nodeID].grpcAddr, Protocol: "grpc"}},
		"default",
		nil,
		false,
	)
	s.clients[cacheKey] = client
	return sequencev1.NewSequenceGeneratorClient(client)
}

func (s *SequenceSystemSuite) routedClient() sequencev1.SequenceGeneratorClient {
	endpoints := []resolver.BaseEndpoint{
		{
			Address: s.nodes["node-a"].grpcAddr, Protocol: "grpc",
			Attributes: map[string]any{sequencepkg.NodeIDAttribute: "node-a"},
		},
		{
			Address: s.nodes["node-b"].grpcAddr, Protocol: "grpc",
			Attributes: map[string]any{sequencepkg.NodeIDAttribute: "node-b"},
		},
	}
	var routed sequencev1.SequenceGeneratorClient
	router, err := sequencepkg.NewRouter(func(
		ctx context.Context,
		knownVersion int64,
	) (*sequencev1.GetRouteResponse, error) {
		return routed.GetRoute(ctx, &sequencev1.GetRouteRequest{KnownVersion: knownVersion})
	})
	s.Require().NoError(err)
	s.router = router
	client := s.newClient(
		"sequence-routed",
		endpoints,
		sequencepkg.BalancerType,
		sequencepkg.NewRoutingModule(router),
		true,
	)
	s.clients["sequence-routed"] = client
	routed = sequencev1.NewSequenceGeneratorClient(client)
	return routed
}

func (s *SequenceSystemSuite) newClient(
	name string,
	endpoints []resolver.BaseEndpoint,
	balancerName string,
	routingModule module.Module,
	withSequenceInterceptor bool,
) transportclient.Client {
	interceptors := []any{}
	if withSequenceInterceptor {
		interceptors = append(interceptors, sequencepkg.InterceptorName)
	}
	balancers := map[string]any{}
	if balancerName == sequencepkg.BalancerType {
		balancers[sequencepkg.BalancerType] = map[string]any{"type": sequencepkg.BalancerType}
	}
	values := map[string]any{
		"yggdrasil": map[string]any{
			"admin": map[string]any{"governor": map[string]any{"port": 0}},
			"clients": map[string]any{"services": map[string]any{
				sequenceServiceName: map[string]any{
					"fast_fail":    true,
					"balancer":     balancerName,
					"remote":       map[string]any{"endpoints": endpoints},
					"interceptors": map[string]any{"unary": interceptors},
				},
			}},
			"balancers": map[string]any{"defaults": balancers},
			"transports": map[string]any{"grpc": map[string]any{
				"client": map[string]any{}, "server": map[string]any{},
			}},
		},
	}
	manager := config.NewManager()
	s.Require().NoError(manager.LoadLayer(
		"test",
		config.PriorityOverride,
		memory.NewSource("test", values),
	))
	options := []yapp.Option{yapp.WithConfigManager(manager), yapp.WithProcessDefaults(false)}
	if routingModule != nil {
		options = append(options, yapp.WithModules(routingModule))
	}
	runtimeApp, err := yapp.New(name, options...)
	s.Require().NoError(err)
	client, err := runtimeApp.NewClient(s.ctx, sequenceServiceName)
	s.Require().NoError(err)
	s.apps = append(s.apps, runtimeApp)
	s.managers = append(s.managers, manager)
	return client
}

func (s *SequenceSystemSuite) watermark(key string) int64 {
	db := openGORM(s.T(), s.h)
	var model sequencedata.SequenceModel
	s.Require().NoError(db.Where("sequence_key = ?", key).Take(&model).Error)
	return model.ReservedEnd
}

func allowedTransient(err error) bool {
	if err == nil {
		return true
	}
	return xerror.IsCode(err, code.Code_UNAVAILABLE) ||
		xerror.IsCode(err, code.Code_DEADLINE_EXCEEDED) ||
		xerror.IsCode(err, code.Code_CANCELLED) ||
		errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, context.Canceled) ||
		xerror.IsReason(err, reason.Reason_SEQUENCE_ALLOCATOR_PAUSED) ||
		xerror.IsReason(err, reason.Reason_SEQUENCE_ROUTE_EXPIRED) ||
		xerror.IsReason(err, reason.Reason_SEQUENCE_SLOT_NOT_OWNER) ||
		xerror.IsReason(err, reason.Reason_SEQUENCE_ROUTE_UNAVAILABLE)
}

// allocationObservation is one allocation attempt as observed by a client.
//
// The (Started, Received) pair is what makes the strict-ordering contract
// checkable from the client side: §1.3 constrains only requests that were
// initiated after an earlier request had already returned, so overlapping
// requests — whose responses may legitimately arrive out of order (§1.4) — are
// excluded from the ordering check.
type allocationObservation struct {
	Key      string
	ID       int64
	Err      error
	Started  time.Time
	Received time.Time
}

// orderLogCompactionThreshold is the entry count beyond which an
// allocationOrderLog folds its oldest safe prefix into baseMax.
const orderLogCompactionThreshold = 1 << 14

// allocationOrderLog records, per key, the ids of successful allocations in
// completion order together with a prefix maximum, so the largest id that had
// already been delivered when a given request started is answerable in O(log n).
//
// Entries whose completion predates Received-retention can never be the
// already-delivered record for a request that is still to be observed, so they
// are folded into baseMax and dropped. That bounds memory under long load
// without weakening the check.
type allocationOrderLog struct {
	received  []time.Time
	prefixMax []int64
	baseMax   int64
}

// maxDeliveredBefore returns the largest id whose delivery completed strictly
// before t.
func (l *allocationOrderLog) maxDeliveredBefore(t time.Time) int64 {
	index := sort.Search(len(l.received), func(i int) bool { return !l.received[i].Before(t) })
	best := l.baseMax
	if index > 0 && l.prefixMax[index-1] > best {
		best = l.prefixMax[index-1]
	}
	return best
}

func (l *allocationOrderLog) append(received time.Time, id int64) {
	best := id
	if n := len(l.prefixMax); n > 0 && l.prefixMax[n-1] > best {
		best = l.prefixMax[n-1]
	}
	l.received = append(l.received, received)
	l.prefixMax = append(l.prefixMax, best)
}

func (l *allocationOrderLog) compact(cutoff time.Time) {
	drop := 0
	for drop < len(l.received) && l.received[drop].Before(cutoff) {
		drop++
	}
	if drop == 0 {
		return
	}
	if max := l.prefixMax[drop-1]; max > l.baseMax {
		l.baseMax = max
	}
	l.received = append([]time.Time(nil), l.received[drop:]...)
	l.prefixMax = append([]int64(nil), l.prefixMax[drop:]...)
}

// allocationRecorder aggregates every allocation a client observes and enforces
// the Appendix F.1 gate: no duplicated id, no stale and no out-of-order
// delivery.
//
// A client cannot observe the server's linearization order, so what is checked
// here is the observable form of S2 stated in §1.3: if request A has already
// returned and request B is initiated afterwards, then id(B) must be greater
// than id(A). Separating a stale delivery from an ordering violation needs the
// per-slot epoch that Phase 3 adds to responses; until then both collapse into
// orderViolations, which is the stronger client-visible signal.
type allocationRecorder struct {
	mu     sync.Mutex
	ids    map[string]map[int64]struct{}
	max    map[string]int64
	order  map[string]*allocationOrderLog
	errors []error
	phases []string

	duplicateDeliveries int
	orderViolations     int

	// retention bounds how long a request may remain in flight. It must exceed
	// the largest client call timeout in the suite, or compaction could drop an
	// entry that is still the already-delivered record for a live request.
	retention time.Duration
}

func newAllocationRecorder() *allocationRecorder {
	return &allocationRecorder{
		ids:       make(map[string]map[int64]struct{}),
		max:       make(map[string]int64),
		order:     make(map[string]*allocationOrderLog),
		retention: 30 * time.Second,
	}
}

func (r *allocationRecorder) phase(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.phases = append(r.phases, name)
}

func (r *allocationRecorder) record(observation allocationObservation) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := observation.Key
	if observation.Err != nil {
		r.errors = append(r.errors, observation.Err)
		if !allowedTransient(observation.Err) {
			codeValue, hasCode := xerror.CodeOf(observation.Err)
			reasonValue, domain, metadata, hasReason := xerror.ReasonOf(observation.Err)
			return fmt.Errorf(
				"unexpected allocation error: type=%T code=%s has_code=%t "+
					"reason=%q domain=%q metadata=%v has_reason=%t: %w",
				observation.Err,
				codeValue,
				hasCode,
				reasonValue,
				domain,
				metadata,
				hasReason,
				observation.Err,
			)
		}
		return nil
	}
	if r.ids[key] == nil {
		r.ids[key] = make(map[int64]struct{})
	}
	if _, duplicate := r.ids[key][observation.ID]; duplicate {
		r.duplicateDeliveries++
		return fmt.Errorf("duplicate successful id for %q: %d", key, observation.ID)
	}
	r.ids[key][observation.ID] = struct{}{}
	if observation.ID > r.max[key] {
		r.max[key] = observation.ID
	}

	log := r.order[key]
	if log == nil {
		log = &allocationOrderLog{}
		r.order[key] = log
	}
	deliveredBefore := log.maxDeliveredBefore(observation.Started)
	if observation.ID <= deliveredBefore {
		r.orderViolations++
		return fmt.Errorf(
			"allocation order violation for %q: id %d returned for a request started at %s, "+
				"but id %d had already been delivered before it started",
			key,
			observation.ID,
			observation.Started.Format(time.RFC3339Nano),
			deliveredBefore,
		)
	}
	log.append(observation.Received, observation.ID)
	if len(log.received) > orderLogCompactionThreshold {
		log.compact(observation.Received.Add(-r.retention))
	}
	return nil
}

func (r *allocationRecorder) maxID(key string) int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.max[key]
}

// counters returns the Appendix F.1 gate counters. Every one of them must stay
// at zero: a single duplicated, stale or out-of-order delivery is a P0 defect,
// never a bounded or acceptable degradation (D8).
func (r *allocationRecorder) counters() (duplicates, orderViolations int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.duplicateDeliveries, r.orderViolations
}

// assertNoViolations fails the test when the F.1 gate counters are non-zero.
func (r *allocationRecorder) assertNoViolations(t require.TestingT) {
	duplicates, orderViolations := r.counters()
	require.Zero(t, duplicates, "duplicate_delivery_count must be zero")
	require.Zero(t, orderViolations, "allocation_order_violation must be zero")
}

func harnessByDialect(dialect string) *harness {
	for _, item := range harnesses {
		if item.name == dialect {
			return item
		}
	}
	return nil
}

func removeToxic(t require.TestingT, proxy *toxiclient.Proxy, name string) {
	err := proxy.RemoveToxic(name)
	if err != nil && !strings.Contains(err.Error(), "not found") {
		require.NoError(t, err)
	}
}
