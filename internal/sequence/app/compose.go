// Package app owns sequence process assembly and lifecycle registration.
package app

import (
	"context"
	"fmt"

	sequencev1 "github.com/codesjoy/sindri/gen/go/sequence/v1"
	"github.com/codesjoy/sindri/internal/pkg/xgorm"
	"github.com/codesjoy/sindri/internal/sequence/biz"
	"github.com/codesjoy/sindri/internal/sequence/conf"
	"github.com/codesjoy/sindri/internal/sequence/metrics"
	"github.com/codesjoy/sindri/internal/sequence/service"
	"github.com/codesjoy/sindri/internal/sequence/task"
	"github.com/codesjoy/yggdrasil/v3"
)

// Compose loads the sequence configuration and builds its business bundle.
func Compose(rt yggdrasil.Runtime) (*yggdrasil.BusinessBundle, error) {
	cfg, err := conf.Load(rt)
	if err != nil {
		return nil, err
	}
	if _, err := metrics.ConfigureMemoryLimit(
		cfg.Runtime.MemoryLimit,
		cfg.Runtime.MemoryLimitExplicit(),
		cfg.Runtime.AutoMemoryLimitRatio,
		rt.Logger(),
	); err != nil {
		return nil, fmt.Errorf("configure sequence runtime: %w", err)
	}
	return InitializeBundle(rt, cfg)
}

func provideBundle(
	database *xgorm.Database,
	cfg biz.NodeConfig,
	metrics *metrics.Metrics,
	ticker *task.Ticker,
	sequenceService *service.SequenceService,
) *yggdrasil.BusinessBundle {
	return &yggdrasil.BusinessBundle{
		RPCBindings: []yggdrasil.RPCBinding{{
			ServiceName: sequencev1.SequenceGeneratorServiceDesc.ServiceName,
			Desc:        &sequencev1.SequenceGeneratorServiceDesc,
			Impl:        sequenceService,
		}},
		Tasks: []yggdrasil.BackgroundTask{ticker},
		Hooks: []yggdrasil.BusinessHook{
			{
				Name:  "sequence.close-metrics",
				Stage: yggdrasil.BusinessHookAfterStop,
				Func:  func(context.Context) error { return metrics.Close() },
			},
			{
				Name:  "sequence.close-gorm-database",
				Stage: yggdrasil.BusinessHookAfterStop,
				Func:  func(context.Context) error { return database.Close() },
			},
		},
		Diagnostics: []yggdrasil.BundleDiag{{
			Code:    "SEQUENCE_NODE",
			Message: "node=" + cfg.ID,
		}},
	}
}
