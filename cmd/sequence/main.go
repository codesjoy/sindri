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

package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/codesjoy/sindri/internal/sequence/conf"
	"github.com/codesjoy/yggdrasil-ecosystem/modules/etcd/v3"
	otlp "github.com/codesjoy/yggdrasil-ecosystem/modules/otlp/v3"
	"github.com/codesjoy/yggdrasil-ecosystem/modules/polaris/v3"
	protovalidate "github.com/codesjoy/yggdrasil-ecosystem/modules/protovalidate/v3"
	"github.com/codesjoy/yggdrasil/v3"
)

const (
	appNameEnv     = "SKULD_SEQUENCE_APP_NAME"
	defaultAppName = "github.com.codesjoy.skuld.sequence"
)

func resolveAppName(lookupEnv func(string) (string, bool)) string {
	if value, ok := lookupEnv(appNameEnv); ok {
		if name := strings.TrimSpace(value); name != "" {
			return name
		}
	}
	return defaultAppName
}

func main() {
	// The mode is stated through the same variable the configuration layer
	// reads, so CLI and environment share one precedence rule: either of them
	// overrides app.sequence.mode, and the resolved value is validated when the
	// configuration loads.
	if mode, ok, err := conf.ModeFromArgs(os.Args[1:]); err != nil {
		slog.Error("run sequence", "error", err)
		os.Exit(1)
	} else if ok {
		if err := os.Setenv(conf.ModeEnv, mode); err != nil {
			slog.Error("run sequence", "error", err)
			os.Exit(1)
		}
	}
	if err := yggdrasil.Run(
		context.Background(),
		resolveAppName(os.LookupEnv),
		compose,
		yggdrasil.WithConfigPath("configs/sequence.yaml"),
		yggdrasil.WithModules(
			protovalidate.Module(),
			otlp.Module(),
			polaris.Module(),
			etcd.Module(),
		),
	); err != nil {
		slog.Error("run sequence", "error", err)
		os.Exit(1)
	}
}

// compose loads the sequence configuration and builds the process bundle.
func compose(rt yggdrasil.Runtime) (*yggdrasil.BusinessBundle, error) {
	cfg, err := conf.Load(rt)
	if err != nil {
		return nil, err
	}
	if _, err := conf.ConfigureMemoryLimit(
		cfg.Runtime.MemoryLimit,
		cfg.Runtime.AutoMemoryLimitRatio,
		rt.Logger(),
	); err != nil {
		return nil, fmt.Errorf("configure sequence runtime: %w", err)
	}
	return initializeSequence(rt, cfg)
}
