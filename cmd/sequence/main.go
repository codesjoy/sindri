package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/codesjoy/skuld/internal/sequence/app"
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
