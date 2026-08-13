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

//go:generate sh ../../../scripts/generate-wire.sh
//go:build wireinject

package app

import (
	"log/slog"

	"github.com/codesjoy/sindri/internal/pkg/xgorm"
	"github.com/codesjoy/sindri/internal/sequence/biz"
	"github.com/codesjoy/sindri/internal/sequence/conf"
	gormdata "github.com/codesjoy/sindri/internal/sequence/data/gorm"
	"github.com/codesjoy/sindri/internal/sequence/metrics"
	"github.com/codesjoy/sindri/internal/sequence/service"
	"github.com/codesjoy/sindri/internal/sequence/task"
	"github.com/codesjoy/yggdrasil/v3"
	"github.com/google/wire"
	"go.opentelemetry.io/otel/metric"
)

func provideLogger(rt yggdrasil.Runtime) *slog.Logger {
	return rt.Logger()
}

func provideAllocatorMeter(rt yggdrasil.Runtime) metric.Meter {
	return rt.MeterProvider().Meter("github.com/codesjoy/sindri/sequence")
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
