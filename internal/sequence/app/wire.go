//go:generate sh ../../../scripts/generate-wire.sh
//go:build wireinject

package app

import (
	"log/slog"

	"github.com/codesjoy/skuld/internal/pkg/xgorm"
	"github.com/codesjoy/skuld/internal/sequence/biz"
	"github.com/codesjoy/skuld/internal/sequence/conf"
	gormdata "github.com/codesjoy/skuld/internal/sequence/data/gorm"
	"github.com/codesjoy/skuld/internal/sequence/metrics"
	"github.com/codesjoy/skuld/internal/sequence/service"
	"github.com/codesjoy/skuld/internal/sequence/task"
	"github.com/codesjoy/yggdrasil/v3"
	"github.com/google/wire"
	"go.opentelemetry.io/otel/metric"
)

func provideLogger(rt yggdrasil.Runtime) *slog.Logger {
	return rt.Logger()
}

func provideAllocatorMeter(rt yggdrasil.Runtime) metric.Meter {
	return rt.MeterProvider().Meter("github.com/codesjoy/skuld/sequence")
}

// InitializeBundle builds the complete sequence process bundle.
func InitializeBundle(
	rt yggdrasil.Runtime,
	cfg *conf.Config,
) (*yggdrasil.BusinessBundle, error) {
	wire.Build(
		wire.FieldsOf(new(*conf.Config), "Database", "Allocator", "Node", "Ticker"),
		xgorm.New,
		provideLogger,
		provideAllocatorMeter,
		metrics.NewRuntimeMemorySampler,
		wire.FieldsOf(new(*xgorm.Database), "DB"),
		gormdata.NewSequenceData,
		gormdata.NewRouteModel,
		biz.NewRouteCache,
		biz.NewNodeManager,
		biz.NewAllocator,
		metrics.NewMetrics,
		task.NewTicker,
		service.NewSequenceService,
		provideBundle,
		wire.Bind(new(task.NodeLifecycle), new(*biz.NodeManager)),
		wire.Bind(new(biz.MemorySampler), new(*metrics.RuntimeMemorySampler)),
	)
	return nil, nil
}
