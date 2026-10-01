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
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/codesjoy/sindri/internal/sequence/biz"
	sequencedata "github.com/codesjoy/sindri/internal/sequence/data"
	sequencepkg "github.com/codesjoy/sindri/pkg/sequence"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// The high-availability bounds the rendered configuration carries.
//
// They are the container harness's scaled bounds, sized for a suite that has to
// converge inside its own deadline rather than for a production platform. The
// tests read them from here as well, so an assertion about what a real process
// does cannot drift from the file that process was started with.
const (
	processQuietWindow   = 3 * time.Second
	processLeaseDuration = 2 * time.Second
	processMaxPause      = 50 * time.Millisecond
	processNodeTTL       = 2 * time.Second
)

// sequenceProcessOptions describe one real cmd/sequence process to start.
//
// The process is started from the built binary rather than composed in the test
// process, so what the suite exercises is the deployment entry point: the same
// main, the same Wire assembly and the same shutdown hooks a container runs.
type sequenceProcessOptions struct {
	database *harness
	// etcdEndpoint enables discovery against that endpoint when set.
	etcdEndpoint string
	nodeID       string
	// configuredMode is written to app.sequence.mode.
	configuredMode string
	// modeOverride is passed as --mode, which is how one image runs two shapes.
	// When set it must win over configuredMode.
	modeOverride string
}

// sequenceProcess is one started cmd/sequence child process.
type sequenceProcess struct {
	cmd      *exec.Cmd
	readyURL string
	logPath  string
	waitCh   chan error
	stopOnce sync.Once
	stopErr  error
	// waitErr is written by the wait goroutine before waitCh is closed, so a
	// reader that observes the close also observes the result. The channel is a
	// one-shot signal rather than a value slot: both the startup check and the
	// stop path may look at it.
	waitErr error
}

var (
	sequenceBinaryOnce sync.Once
	sequenceBinaryDir  string
	sequenceBinaryPath string
	sequenceBinaryErr  error
)

// sequenceBinary builds cmd/sequence once per test run and returns the path.
func sequenceBinary(t *testing.T) string {
	t.Helper()
	sequenceBinaryOnce.Do(func() {
		dir, err := os.MkdirTemp("", "sindri-sequence-process-")
		if err != nil {
			sequenceBinaryErr = fmt.Errorf("create sequence build directory: %w", err)
			return
		}
		binary := filepath.Join(dir, "sequence")
		build := exec.Command("go", "build", "-o", binary, "./cmd/sequence")
		build.Dir = sequenceRepoRoot()
		output, err := build.CombinedOutput()
		if err != nil {
			sequenceBinaryErr = fmt.Errorf("build cmd/sequence: %w\n%s", err, output)
			_ = os.RemoveAll(dir)
			return
		}
		sequenceBinaryDir = dir
		sequenceBinaryPath = binary
	})
	require.NoError(t, sequenceBinaryErr)
	return sequenceBinaryPath
}

// cleanupSequenceBinary removes the binary built for the suite.
func cleanupSequenceBinary() {
	if sequenceBinaryDir != "" {
		_ = os.RemoveAll(sequenceBinaryDir)
	}
}

func sequenceRepoRoot() string {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return "."
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}

// startSequenceProcess starts a real Sequence process and waits until its
// governor answers, so a failure to start is reported as a startup failure
// rather than as a later readiness timeout.
func startSequenceProcess(t *testing.T, options sequenceProcessOptions) *sequenceProcess {
	t.Helper()
	binary := sequenceBinary(t)
	governorPort := freeTCPPort(t)
	dir := t.TempDir()
	configPath := filepath.Join(dir, "sequence.yaml")
	require.NoError(t, os.WriteFile(
		configPath,
		[]byte(sequenceProcessYAML(options, governorPort)),
		0o600,
	))

	logPath := filepath.Join(dir, "sequence.log")
	logFile, err := os.Create(logPath)
	require.NoError(t, err)
	args := []string{"--yggdrasil-config=" + configPath}
	if options.modeOverride != "" {
		args = append(args, "--mode="+options.modeOverride)
	}
	cmd := exec.Command(binary, args...)
	cmd.Dir = sequenceRepoRoot()
	cmd.Env = append(os.Environ(),
		// etcd and Polaris currently ship legacy descriptors with the same
		// filename, which the framework reports as a registration conflict.
		"GOLANG_PROTOBUF_REGISTRATION_CONFLICT=warn",
		// The config file carries the shape under test; an ambient value must
		// not restate it behind the test's back.
		"SKULD_SEQUENCE_MODE=",
	)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	require.NoError(t, cmd.Start(), "start sequence process")
	require.NoError(t, logFile.Close())

	process := &sequenceProcess{
		cmd:      cmd,
		readyURL: fmt.Sprintf("http://127.0.0.1:%d%s", governorPort, readinessPathForTest()),
		logPath:  logPath,
		waitCh:   make(chan error, 1),
	}
	go func() {
		process.waitErr = cmd.Wait()
		close(process.waitCh)
	}()
	t.Cleanup(func() { process.stopOrKill(t) })
	process.waitForGovernor(t)
	return process
}

// stop signals the process to shut down and waits for it to exit.
func (p *sequenceProcess) stop(ctx context.Context) error {
	p.stopOnce.Do(func() {
		p.stopErr = p.signalAndWait(ctx)
	})
	return p.stopErr
}

// stopOrKill is the test cleanup path: a process the test did not stop is
// signalled and, if it does not exit in time, killed.
func (p *sequenceProcess) stopOrKill(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := p.stop(ctx); err != nil {
		t.Logf("sequence process %s logs:\n%s", p.cmd.Path, p.logs())
	}
}

func (p *sequenceProcess) signalAndWait(ctx context.Context) error {
	if p.cmd.Process == nil {
		return nil
	}
	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil &&
		!errors.Is(err, os.ErrProcessDone) {
		_ = p.cmd.Process.Kill()
	}
	select {
	case <-p.waitCh:
		return p.waitErr
	case <-ctx.Done():
		_ = p.cmd.Process.Kill()
		<-p.waitCh
		return ctx.Err()
	}
}

func (p *sequenceProcess) logs() string {
	body, err := os.ReadFile(p.logPath)
	if err != nil {
		return fmt.Sprintf("(read logs: %v)", err)
	}
	return string(body)
}

// waitForGovernor waits until the readiness handler answers. The handler exists
// before the instance has a route, so the first answer is expected to be a 503.
func (p *sequenceProcess) waitForGovernor(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-p.waitCh:
			t.Fatalf("sequence process exited during startup: %v\n%s", p.waitErr, p.logs())
		default:
		}
		if _, _, err := probeSequenceReadiness(p.readyURL); err == nil {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("sequence process did not expose readiness at %s\n%s", p.readyURL, p.logs())
}

// probeSequenceReadiness reads a process's readiness endpoint.
func probeSequenceReadiness(url string) (int, readinessReport, error) {
	client := &http.Client{Timeout: 5 * time.Second}
	response, err := client.Get(url)
	if err != nil {
		return 0, readinessReport{}, err
	}
	defer func() { _ = response.Body.Close() }()
	var report readinessReport
	if err := json.NewDecoder(response.Body).Decode(&report); err != nil {
		return response.StatusCode, readinessReport{}, err
	}
	return response.StatusCode, report, nil
}

// authorityGeneration is one instance/epoch pair and how many slots it held.
// The pair is the fence: a release leaves it in place at the same epoch with the
// state changed, and a re-claim by the same instance moves the epoch, so
// watching the pair is what tells a released grant apart from a renewed one.
type authorityGeneration struct {
	InstanceID string
	Epoch      uint64
	Slots      int64
}

// loadAuthorityGenerations records the authority an instance holds now, so a
// shutdown assertion can be about the generation that was in force at the stop
// rather than about rows an earlier test left in the table.
func loadAuthorityGenerations(
	t *testing.T,
	db *gorm.DB,
	instanceID string,
) []authorityGeneration {
	t.Helper()
	var generations []authorityGeneration
	require.NoError(t, db.Raw(
		"SELECT owner_instance_id AS instance_id, epoch, COUNT(*) AS slots "+
			"FROM slot_ownership WHERE owner_instance_id = ? AND state = 'OWNED' "+
			"GROUP BY owner_instance_id, epoch ORDER BY epoch",
		instanceID,
	).Scan(&generations).Error)
	return generations
}

// requireAuthorityVacated asserts that a stopping process no longer holds the
// authority generations it held at the moment of the stop.
//
// The direct state is the assertion: the shutdown hook drives the epoch-CAS
// release, and a released slot leaves that generation as UNOWNED. The last
// heartbeat of a stopping process can claim the slots again before the
// framework stops the ticker, which is why the check is that the observed
// generation is gone -- a re-claim moves the epoch -- rather than that every
// slot reads UNOWNED.
func requireAuthorityVacated(
	t *testing.T,
	db *gorm.DB,
	held []authorityGeneration,
) {
	t.Helper()
	require.NotEmpty(t, held, "the process held no authority to release")
	require.Eventually(t, func() bool {
		for _, generation := range held {
			count, err := queryCount(
				db,
				"SELECT COUNT(*) FROM slot_ownership "+
					"WHERE owner_instance_id = ? AND epoch = ? AND state = 'OWNED'",
				generation.InstanceID,
				generation.Epoch,
			)
			if err != nil || count != 0 {
				return false
			}
		}
		return true
	}, processQuietWindow+discoveryTestTimeout, 50*time.Millisecond,
		"the departing process still holds the authority generation it had at shutdown")
}

// waitForNodeToLeaveLiveSet waits until the fleet stops counting nodeID as live.
//
// A dropped row and a row left to expire are indistinguishable to every other
// node, which is what makes the expiry path the one the fleet depends on: the
// live set is what a planner reads, and a departed process that stopped
// renewing leaves it within ha.node_ttl. A process whose heartbeat outlived its
// shutdown would keep the node live and its slots pinned to a process that is
// gone.
func waitForNodeToLeaveLiveSet(t *testing.T, db *gorm.DB, nodeID string) {
	t.Helper()
	placement := sequencedata.NewPlacementData(db)
	require.Eventually(t, func() bool {
		live, err := placement.LiveNodes(context.Background(), processNodeTTL)
		if err != nil {
			return false
		}
		return !slices.ContainsFunc(live, func(node biz.NodeInfo) bool {
			return node.ID == nodeID
		})
	}, processNodeTTL+discoveryTestTimeout, 50*time.Millisecond,
		"node %s stayed in the fleet's live set after shutdown", nodeID)
}

// requireAuthorityAssignable claims slots the way the process that takes over
// from a departed node would.
//
// This is the deployment-visible half of the release a stopping process
// attempts: whether the authority was revoked outright or left to expire, the
// next process has to be able to take it, and it may not have to wait longer
// than the quiet window that bounds the old owner's lease.
func requireAuthorityAssignable(t *testing.T, db *gorm.DB, slots []uint32) {
	t.Helper()
	successor := "process-successor"
	ownership := sequencedata.NewOwnershipData(db)
	require.Eventually(t, func() bool {
		outcomes, err := ownership.ClaimSlots(context.Background(), biz.ClaimRequest{
			Slots:       slots,
			NodeID:      successor,
			InstanceID:  successor + "-instance",
			QuietWindow: processQuietWindow,
		})
		if err != nil || len(outcomes) != len(slots) {
			return false
		}
		for _, outcome := range outcomes {
			if !outcome.Granted {
				return false
			}
		}
		return true
	}, processQuietWindow+discoveryTestTimeout, 50*time.Millisecond,
		"the departed process's slots never became assignable")
}

// requireSlotsOwnedBy waits until the authority names ownerNodeID for slots.
//
// It is the takeover a stopped node is supposed to trigger on its own: the
// nodes that remain plan a lapsed owner's slots to themselves and claim them
// once the quiet window admits the claim, with no operator action in between.
func requireSlotsOwnedBy(
	t *testing.T,
	db *gorm.DB,
	ownerNodeID string,
	slots []uint32,
) {
	t.Helper()
	require.Eventually(t, func() bool {
		count, err := queryCount(
			db,
			"SELECT COUNT(*) FROM slot_ownership WHERE owner_node_id = ? AND slot_id IN ?",
			ownerNodeID,
			slots,
		)
		return err == nil && count == int64(len(slots))
	}, processNodeTTL+processQuietWindow+discoveryTestTimeout, 100*time.Millisecond,
		"node %s never took over the departed node's slots", ownerNodeID)
}

// readinessPathForTest names the route the process registers without importing
// the service package into the test harness: the string is part of the
// deployment contract, so the harness pins it rather than deriving it.
func readinessPathForTest() string { return "/healthz" }

// freeTCPPort reserves a loopback port and releases it for the child process to
// bind. The window is short and the process binds it before the test uses it.
func freeTCPPort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address, ok := listener.Addr().(*net.TCPAddr)
	require.True(t, ok)
	port := address.Port
	require.NoError(t, listener.Close())
	return port
}

// sequenceProcessYAML renders the configuration one real process runs with.
//
// It writes concrete values rather than environment placeholders so a test
// process cannot be restated by the ambient environment. The HA bounds are the
// container harness's scaled bounds, which are sized for a test that has to
// converge inside its own deadline rather than for a production platform.
func sequenceProcessYAML(options sequenceProcessOptions, governorPort int) string {
	mode := options.configuredMode
	if mode == "" {
		mode = "data"
	}
	var builder strings.Builder
	fmt.Fprintf(&builder, `yggdrasil:
  mode: dev
  admin:
    governor: {bind: "127.0.0.1", port: %d}
    application:
      # The registry record a caller resolves carries these: the namespace is
      # what a resolver scopes its lookup to, and the metadata is how a routed
      # client tells which node answered.
      namespace: default
      metadata:
        %s: %s
  observability:
    telemetry:
      # The OTLP module registers both grpc and http providers, so a process
      # that says nothing about the exporter has no deterministic default. The
      # endpoint is never collected from; startup only has to resolve one.
      tracer: otlp-grpc
      meter: otlp-grpc
      providers:
        otlp:
          trace: {endpoint: localhost:4317, tls: {insecure: true}}
          metric: {endpoint: localhost:4317, tls: {insecure: true}}
`, governorPort, sequencepkg.NodeIDAttribute, options.nodeID)
	if mode != "control" {
		builder.WriteString(`  server:
    transports: [grpc]
    interceptors: {unary: [protovalidate]}
  transports:
    grpc:
      server: {address: "127.0.0.1:0"}
`)
	}
	if options.etcdEndpoint != "" {
		fmt.Fprintf(&builder, `  etcd:
    clients:
      default:
        endpoints: [%q]
        dial_timeout: 5s
  discovery:
    registry:
      type: etcd
      config:
        client: default
        prefix: %q
        ttl: 3s
        keep_alive: true
        retry_interval: 200ms
    resolvers:
      etcd:
        type: etcd
        config:
          client: default
          prefix: %q
          namespace: default
          protocols: [grpc]
          debounce: 20ms
`, options.etcdEndpoint, etcdRegistryPrefix, etcdRegistryPrefix)
	}
	fmt.Fprintf(&builder, `app:
  sequence:
    mode: %s
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
      ha:
        quiet_window: %s
        lease_duration: %s
        max_pause: %s
        clock_drift: 300ms
        clock_jump: 300ms
        safety_margin: 20ms
        renew_interval: 250ms
        release_drain_timeout: 1s
        node_ttl: %s
        pause_verified: true
        clock_disciplined: true
    controlplane:
      layout_version: 1
      coordinator_lease: 10s
      reconcile_interval: 1s
      pass_timeout: 3s
    ticker:
      base_tick_interval: 50ms
      heartbeat_ticks: 1
`,
		mode,
		options.database.driver,
		options.database.dsn,
		databaseName,
		databaseUser,
		options.nodeID,
		processQuietWindow,
		processLeaseDuration,
		processMaxPause,
		processNodeTTL,
	)
	return builder.String()
}
