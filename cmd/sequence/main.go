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
	"log/slog"
	"os"

	"github.com/codesjoy/sindri/internal/sequence/app"
	"github.com/codesjoy/yggdrasil-ecosystem/modules/etcd/v3"
	otlp "github.com/codesjoy/yggdrasil-ecosystem/modules/otlp/v3"
	"github.com/codesjoy/yggdrasil-ecosystem/modules/polaris/v3"
	protovalidate "github.com/codesjoy/yggdrasil-ecosystem/modules/protovalidate/v3"
	"github.com/codesjoy/yggdrasil/v3"
)

const appName = "github.com.codesjoy.skuld.sequence"

func main() {
	if err := yggdrasil.Run(
		context.Background(),
		appName,
		app.Compose,
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
