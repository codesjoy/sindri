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

//go:generate sh ../../scripts/generate-wire.sh
//go:build wireinject

package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"time"

	sequencev1 "github.com/codesjoy/sindri/gen/go/sequence/v1"
	"github.com/codesjoy/sindri/internal/pkg/xgorm"
	"github.com/codesjoy/sindri/internal/sequence/biz"
	"github.com/codesjoy/sindri/internal/sequence/conf"
	sequencedata "github.com/codesjoy/sindri/internal/sequence/data"
	"github.com/codesjoy/sindri/internal/sequence/metrics"
	"github.com/codesjoy/sindri/internal/sequence/service"
	"github.com/codesjoy/sindri/internal/sequence/task"
	"github.com/codesjoy/yggdrasil/v3"
	"github.com/google/wire"
	"go.opentelemetry.io/otel/metric"
)

// configSet exposes the loaded configuration by the section that owns it.
//
// Each section is declared by the package that reads it, so a provider asks for
// the type it needs and never for the whole file. The nesting is resolved here,
// once, instead of being reassembled by per-section merge providers.
var configSet = wire.NewSet(
	wire.FieldsOf(
		new(*conf.Config),
		"Mode",
		"Database",
		"DataPlane",
		"ControlPlane",
		"Ticker",
	),
)

// dataPlaneSet builds the data plane and the storage layer both halves run over.
//
// The pool and the repository constructors are shared: a node that plans from
// slot_ownership and a publisher that materialises it read the same tables
// through the same handle, and one graph is what keeps them from opening two.
var dataPlaneSet = wire.NewSet(
	provideLogger,
	provideAllocatorMeter,
	xgorm.New,
	wire.FieldsOf(new(*xgorm.Database), "DB"),
	sequencedata.NewSequenceData,
	sequencedata.NewOwnershipData,
	sequencedata.NewLivenessData,
	sequencedata.NewRouteModel,
	sequencedata.NewPlacementData,
	wire.Bind(new(biz.PlacementRepo), new(*sequencedata.PlacementData)),
	wire.Bind(new(biz.PublisherRepo), new(*sequencedata.PlacementData)),
	wire.Bind(new(biz.CoordinatorRepo), new(*sequencedata.PlacementData)),
	metrics.NewRuntimeMemorySampler,
	wire.Bind(new(biz.MemorySampler), new(*metrics.RuntimeMemorySampler)),
	biz.NewRouteCache,
	provideStorageClockMonitor,
	biz.NewAllocator,
	biz.NewNodeManager,
	wire.Bind(new(task.NodeLifecycle), new(*biz.NodeManager)),
	task.NewTicker,
	service.NewSequenceService,
	provideDataMetrics,
)

// controlPlaneSet builds the publisher half.
//
// The instance id is minted per process rather than configured, because it
// fences the coordinator lease: two replicas sharing one would both believe they
// held the role.
var controlPlaneSet = wire.NewSet(
	newInstanceID,
	biz.NewPublisher,
	task.NewPublisherTask,
	wire.Bind(new(task.PublisherReconciler), new(*biz.Publisher)),
	providePublisherMetrics,
)

// bundleSet assembles what the startup mode selects.
var bundleSet = wire.NewSet(newBusinessBundle)

func provideLogger(rt yggdrasil.Runtime) *slog.Logger {
	return rt.Logger()
}

func provideAllocatorMeter(rt yggdrasil.Runtime) metric.Meter {
	return rt.MeterProvider().Meter("github.com/codesjoy/sindri/sequence")
}

// provideStorageClockMonitor builds the comparison between the authority's lease
// clock and this process's clock.
//
// Both bounds come from the validated HA section rather than being defaults
// here: they are operator assertions about this platform, and the monitor's only
// job is to report when one of them stops holding.
func provideStorageClockMonitor(
	dataPlane biz.DataPlaneConfig,
	ownership biz.OwnershipRepo,
) *biz.StorageClockMonitor {
	return biz.NewStorageClockMonitor(
		dataPlane.HA.ClockDrift,
		dataPlane.HA.ClockJump,
		time.Now,
		ownership.StorageClock,
	)
}

// provideDataMetrics registers the data plane's series, except in a mode that
// does not run it.
func provideDataMetrics(
	mode conf.Mode,
	meter metric.Meter,
	allocator *biz.Allocator,
	memorySampler biz.MemorySampler,
	dataPlane biz.DataPlaneConfig,
	clock *biz.StorageClockMonitor,
) (*metrics.Metrics, error) {
	if mode == conf.ModeControl {
		return nil, nil
	}
	return metrics.NewMetrics(meter, allocator, memorySampler, dataPlane.HA, clock)
}

// providePublisherMetrics registers the control plane's series, except in a mode
// that does not run it.
func providePublisherMetrics(
	mode conf.Mode,
	meter metric.Meter,
	publisher *biz.Publisher,
) (*metrics.PublisherMetrics, error) {
	if mode == conf.ModeData {
		return nil, nil
	}
	return metrics.NewPublisherMetrics(meter, publisher)
}

// newBusinessBundle registers the components the mode selects.
//
// The data plane and the publisher are independent halves of one process shape:
// a node that only serves ids must not write a directory, and a publisher that
// only mints revisions must not claim a slot or renew a liveness row. Both is
// simply both sets, sharing the database pool the components were built over.
func newBusinessBundle(
	mode conf.Mode,
	dataPlane biz.DataPlaneConfig,
	controlPlane biz.ControlPlaneConfig,
	database *xgorm.Database,
	logger *slog.Logger,
	liveness biz.LivenessRepo,
	allocator *biz.Allocator,
	ticker *task.Ticker,
	sequenceService *service.SequenceService,
	publisher *biz.Publisher,
	publisherTask *task.PublisherTask,
	dataMetrics *metrics.Metrics,
	publisherMetrics *metrics.PublisherMetrics,
) *yggdrasil.BusinessBundle {
	var readinessAllocator *biz.Allocator
	var readinessPublisher *biz.Publisher
	if mode != conf.ModeControl {
		readinessAllocator = allocator
	}
	if mode != conf.ModeData {
		readinessPublisher = publisher
	}
	ha := dataPlane.HA

	bundle := &yggdrasil.BusinessBundle{
		GovernorHTTP: []yggdrasil.GovernorHTTPBinding{{
			Method: "GET",
			Path:   service.ReadinessPath,
			Handler: service.NewReadinessHandler(
				readinessAllocator,
				readinessPublisher,
				service.ReadinessBounds{
					QuietWindow:   ha.QuietWindow,
					LeaseDuration: ha.LeaseDuration,
					MaxPause:      ha.MaxPause,
				},
			),
		}},
		Diagnostics: []yggdrasil.BundleDiag{{
			Code:    "SEQUENCE_MODE",
			Message: "mode=" + string(mode) + " node=" + dataPlane.Node.ID,
		}},
	}

	if mode != conf.ModeControl {
		logDataBounds(ha, logger)
		bundle.RPCBindings = append(bundle.RPCBindings, yggdrasil.RPCBinding{
			ServiceName: sequencev1.SequenceGeneratorServiceDesc.ServiceName,
			Desc:        &sequencev1.SequenceGeneratorServiceDesc,
			Impl:        sequenceService,
		})
		bundle.Tasks = append(bundle.Tasks, ticker)
		bundle.Hooks = append(bundle.Hooks,
			yggdrasil.BusinessHook{
				// The release has to happen before the process stops, while
				// there is still a connection to the storage authority. Doing
				// it after stop would leave every held slot to expire through
				// the quiet window instead.
				Name:  "sequence.release-slot-authority",
				Stage: yggdrasil.BusinessHookBeforeStop,
				Func: func(context.Context) error {
					allocator.Shutdown()
					return nil
				},
			},
			yggdrasil.BusinessHook{
				// Removing the liveness row is a courtesy, not a correctness
				// requirement: a row left behind expires on its own after
				// ha.node_ttl, which is the path that has to work anyway. It is
				// worth the hook because it is the difference between a rolling
				// update that makes a departing node's slots assignable at once
				// and one that waits out a full TTL per step. A failure is
				// logged rather than raised: refusing to stop over a row that
				// will expire by itself would trade a delay for an outage.
				Name:  "sequence.drop-node-liveness",
				Stage: yggdrasil.BusinessHookBeforeStop,
				Func: func(ctx context.Context) error {
					err := liveness.DropLiveness(ctx, dataPlane.Node.ID, allocator.InstanceID())
					if err != nil {
						logger.Warn("sequence liveness drop failed", slog.Any("err", err))
					}
					return nil
				},
			},
		)
	}

	if mode != conf.ModeData {
		logControlBounds(controlPlane, logger)
		bundle.Tasks = append(bundle.Tasks, publisherTask)
	}

	if dataMetrics != nil {
		bundle.Hooks = append(bundle.Hooks, yggdrasil.BusinessHook{
			Name:  "sequence.close-metrics",
			Stage: yggdrasil.BusinessHookAfterStop,
			Func:  func(context.Context) error { return dataMetrics.Close() },
		})
	}
	if publisherMetrics != nil {
		bundle.Hooks = append(bundle.Hooks, yggdrasil.BusinessHook{
			Name:  "sequence.close-publisher-metrics",
			Stage: yggdrasil.BusinessHookAfterStop,
			Func:  func(context.Context) error { return publisherMetrics.Close() },
		})
	}
	// One pool, one close. Both modes share it, so a second hook would close the
	// connection every other component is still using.
	bundle.Hooks = append(bundle.Hooks, yggdrasil.BusinessHook{
		Name:  "sequence.close-gorm-database",
		Stage: yggdrasil.BusinessHookAfterStop,
		Func:  func(context.Context) error { return database.Close() },
	})
	return bundle
}

// logDataBounds records the bounds the ordering guarantee rests on.
//
// Every bound below is operational rather than diagnostic: W is the quiet window
// the claim predicate enforces, L is the local lease an owner serves under, and
// P_max is the pause the fast-path self-check measures. An operator reading this
// line sees the numbers the argument is actually made of.
func logDataBounds(ha biz.HAConfig, logger *slog.Logger) {
	logger.Info(
		"sequence ha bounds",
		slog.Duration("quiet_window", ha.QuietWindow),
		slog.Duration("lease_duration", ha.LeaseDuration),
		slog.Duration("max_pause", ha.MaxPause),
		slog.Duration("clock_jump", ha.ClockJump),
		slog.Duration("safety_margin", ha.SafetyMargin),
		slog.Duration("node_ttl", ha.NodeTTL),
	)
	// The guarantee rests on two facts this process cannot check for itself:
	// that the pause bound was measured on this platform, and that the storage
	// host clock is disciplined. Both were asserted in configuration, so say out
	// loud that the safety argument depends on them and on the bounds below.
	logger.Warn(
		"sequence ha local lease enabled; ordering rests on asserted platform bounds",
		slog.Duration("lease_duration", ha.LeaseDuration),
		slog.Duration("max_pause", ha.MaxPause),
		slog.Duration("clock_drift", ha.ClockDrift),
		slog.Duration("clock_jump", ha.ClockJump),
		slog.Duration("safety_margin", ha.SafetyMargin),
	)
}

// logControlBounds records what decides how quickly a crashed node's slots
// become visible to clients again.
//
// An operator reading this line can compare the cadence with what the nodes were
// configured to use, and the pass timeout with the lease it must stay under.
func logControlBounds(cfg biz.ControlPlaneConfig, logger *slog.Logger) {
	logger.Info(
		"sequence publisher bounds",
		slog.Int64("layout_version", cfg.LayoutVersion),
		slog.Duration("coordinator_lease", cfg.CoordinatorLease),
		slog.Duration("reconcile_interval", cfg.ReconcileInterval),
		slog.Duration("pass_timeout", cfg.PassTimeout),
	)
}

// newInstanceID mints the per-process identity the coordinator lease is fenced
// on. It is random rather than configured because two replicas sharing one would
// both believe they held the role.
func newInstanceID() (string, error) {
	raw := make([]byte, 8)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("sequence: generate instance id: %w", err)
	}
	return hex.EncodeToString(raw), nil
}

// initializeSequence builds every part a sequence process can run, in one graph
// over one database pool. Which parts are started is decided by Config.Mode in
// newBusinessBundle.
func initializeSequence(
	rt yggdrasil.Runtime,
	cfg *conf.Config,
) (*yggdrasil.BusinessBundle, error) {
	wire.Build(configSet, dataPlaneSet, controlPlaneSet, bundleSet)
	return nil, nil
}
