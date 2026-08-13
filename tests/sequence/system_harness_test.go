//go:build integration

package sequence_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	toxiclient "github.com/Shopify/toxiproxy/v2/client"
	"github.com/codesjoy/pkg/basic/xerror"
	"github.com/codesjoy/skuld/gen/go/reason"
	sequencev1 "github.com/codesjoy/skuld/gen/go/sequence/v1"
	"github.com/codesjoy/skuld/internal/sequence/biz"
	gormdata "github.com/codesjoy/skuld/internal/sequence/data/gorm"
	sequencepkg "github.com/codesjoy/skuld/pkg/sequence"
	yapp "github.com/codesjoy/yggdrasil/v3/app"
	"github.com/codesjoy/yggdrasil/v3/config"
	"github.com/codesjoy/yggdrasil/v3/config/source/memory"
	"github.com/codesjoy/yggdrasil/v3/discovery/resolver"
	"github.com/codesjoy/yggdrasil/v3/module"
	"github.com/codesjoy/yggdrasil/v3/rpc/metadata"
	transportclient "github.com/codesjoy/yggdrasil/v3/transport/runtime/client"
	"github.com/docker/docker/api/types/container"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	tctoxiproxy "github.com/testcontainers/testcontainers-go/modules/toxiproxy"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/genproto/googleapis/rpc/code"
)

const (
	sequenceServiceName = "codesjoy.skuld.sequence.v1.SequenceGenerator"
	recoveryDeadline    = 5 * time.Second
)

type systemNode struct {
	id        string
	container testcontainers.Container
	grpcAddr  string
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
	routeVersion   int64
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
	s.routeVersion = 0
	s.Require().NoError(s.resetDatabase())
	var err error
	s.proxyContainer, err = tctoxiproxy.Run(
		s.ctx,
		"ghcr.io/shopify/toxiproxy:2.12.0",
		tctoxiproxy.WithProxy("db-a", fmt.Sprintf("%s:%s", s.h.dbAlias, s.h.dbPort)),
		tctoxiproxy.WithProxy("db-b", fmt.Sprintf("%s:%s", s.h.dbAlias, s.h.dbPort)),
		tctoxiproxy.WithProxy("grpc-a", "node-a:19010"),
		tctoxiproxy.WithProxy("grpc-b", "node-b:19010"),
		network.WithNetwork([]string{"toxiproxy"}, s.h.network),
	)
	s.Require().NoError(err)
	uri, err := s.proxyContainer.URI(s.ctx)
	s.Require().NoError(err)
	s.proxyClient = toxiclient.NewClient(uri)
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
    governor: {bind: "0.0.0.0", port: 0}
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
    allocator:
      default_step: 100
      max_step: 10000
      prefetch_ratio: 0.5
      step_increase_threshold: 15m
      step_decrease_threshold: 30m
      reserve_timeout: 1s
    node:
      id: %s
      heartbeat_timeout_ticks: 3
      route_query_timeout: 150ms
    ticker:
      base_tick_interval: 100ms
      heartbeat_ticks: 1
`, s.h.driver, dsn, databaseName, databaseUser, id)

	container, err := testcontainers.Run(
		s.ctx,
		os.Getenv("SKULD_SEQUENCE_TEST_IMAGE"),
		testcontainers.WithEnv(map[string]string{
			// etcd and Polaris currently ship legacy descriptors with the same
			// filename. Keep the real cmd/sequence module set while protobuf
			// reports that ecosystem conflict as a warning in system tests.
			"GOLANG_PROTOBUF_REGISTRATION_CONFLICT": "warn",
		}),
		testcontainers.WithHostConfigModifier(func(config *container.HostConfig) {
			config.Memory = 640 << 20
		}),
		testcontainers.WithExposedPorts("19010/tcp"),
		testcontainers.WithFiles(testcontainers.ContainerFile{
			Reader:            strings.NewReader(configYAML),
			ContainerFilePath: "/tmp/sequence.yaml",
			FileMode:          0o644,
		}),
		testcontainers.WithCmdArgs("--yggdrasil-config=/tmp/sequence.yaml"),
		network.WithNetwork([]string{id}, s.h.network),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("19010/tcp").WithStartupTimeout(45*time.Second),
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
	host, port, err := s.proxyContainer.ProxiedEndpoint(grpcProxyPort)
	s.Require().NoError(err)
	return &systemNode{id: id, container: container, grpcAddr: net.JoinHostPort(host, port)}
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
	if err := db.Exec("DELETE FROM sequence_ranges").Error; err != nil {
		return err
	}
	return db.Exec("DELETE FROM sequence_routes").Error
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
	s.routeVersion++
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
	s.Require().NoError(err)
	db := openGORM(s.T(), s.h)
	s.Require().NoError(db.Create(&gormdata.RouteModel{
		Version: s.routeVersion, Payload: payload, CreatedAt: time.Now().UTC(),
	}).Error)
	return s.routeVersion
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
	owned := make(map[uint32]struct{}, len(route[owner]))
	for _, slot := range route[owner] {
		owned[slot] = struct{}{}
	}
	for index := 0; ; index++ {
		key := fmt.Sprintf("system-%s-%d", owner, index)
		if _, ok := owned[biz.SlotForKey(key)]; ok {
			return key
		}
	}
}

func (s *SequenceSystemSuite) waitForOwnership(nodeID, key string, version int64) int64 {
	var id int64
	s.Require().Eventually(func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		response, err := s.fetchDirect(ctx, nodeID, key, version)
		if err != nil {
			return false
		}
		id = response.GetId()
		return id > 0
	}, recoveryDeadline, 50*time.Millisecond)
	return id
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
	var model gormdata.SequenceModel
	s.Require().NoError(db.Where("sequence_key = ?", key).Take(&model).Error)
	return model.MaxID
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

type allocationRecorder struct {
	mu     sync.Mutex
	ids    map[string]map[int64]struct{}
	max    map[string]int64
	errors []error
	phases []string
}

func newAllocationRecorder() *allocationRecorder {
	return &allocationRecorder{
		ids: make(map[string]map[int64]struct{}),
		max: make(map[string]int64),
	}
}

func (r *allocationRecorder) phase(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.phases = append(r.phases, name)
}

func (r *allocationRecorder) record(key string, id int64, err error) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err != nil {
		r.errors = append(r.errors, err)
		if !allowedTransient(err) {
			codeValue, hasCode := xerror.CodeOf(err)
			reasonValue, domain, metadata, hasReason := xerror.ReasonOf(err)
			return fmt.Errorf(
				"unexpected allocation error: type=%T code=%s has_code=%t "+
					"reason=%q domain=%q metadata=%v has_reason=%t: %w",
				err,
				codeValue,
				hasCode,
				reasonValue,
				domain,
				metadata,
				hasReason,
				err,
			)
		}
		return nil
	}
	if r.ids[key] == nil {
		r.ids[key] = make(map[int64]struct{})
	}
	if _, duplicate := r.ids[key][id]; duplicate {
		return fmt.Errorf("duplicate successful id for %q: %d", key, id)
	}
	r.ids[key][id] = struct{}{}
	if id > r.max[key] {
		r.max[key] = id
	}
	return nil
}

func (r *allocationRecorder) maxID(key string) int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.max[key]
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
