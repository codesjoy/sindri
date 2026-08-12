// Package app owns sequence process assembly and lifecycle registration.
package app

import (
	"context"

	sequencev1 "github.com/codesjoy/skuld/gen/go/sequence/v1"
	"github.com/codesjoy/skuld/internal/pkg/xgorm"
	"github.com/codesjoy/skuld/internal/sequence/biz"
	"github.com/codesjoy/skuld/internal/sequence/conf"
	"github.com/codesjoy/skuld/internal/sequence/service"
	"github.com/codesjoy/skuld/internal/sequence/task"
	"github.com/codesjoy/yggdrasil/v3"
)

// Compose loads the sequence configuration and builds its business bundle.
func Compose(rt yggdrasil.Runtime) (*yggdrasil.BusinessBundle, error) {
	cfg, err := conf.Load(rt)
	if err != nil {
		return nil, err
	}
	return InitializeBundle(rt, cfg)
}

func provideBundle(
	database *xgorm.Database,
	cfg biz.NodeConfig,
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
		Hooks: []yggdrasil.BusinessHook{{
			Name:  "sequence.close-database",
			Stage: yggdrasil.BusinessHookAfterStop,
			Func:  func(context.Context) error { return database.Close() },
		}},
		Diagnostics: []yggdrasil.BundleDiag{{
			Code:    "SEQUENCE_NODE",
			Message: "node=" + cfg.ID,
		}},
	}
}
