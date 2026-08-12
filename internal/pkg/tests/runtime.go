// Package tests contains test helpers shared by service packages.
package tests

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/codesjoy/yggdrasil/v3"
	"github.com/codesjoy/yggdrasil/v3/config"
	"github.com/codesjoy/yggdrasil/v3/config/source/memory"
	transportclient "github.com/codesjoy/yggdrasil/v3/transport/runtime/client"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

// Runtime is a minimal Yggdrasil runtime for configuration and composition tests.
type Runtime struct {
	manager *config.Manager
}

// NewRuntime constructs a runtime backed by one in-memory override layer.
func NewRuntime(t testing.TB, values map[string]any) *Runtime {
	t.Helper()
	manager := config.NewManager()
	require.NoError(t, manager.LoadLayer(
		"test",
		config.PriorityOverride,
		memory.NewSource("test", values),
	))
	t.Cleanup(func() { require.NoError(t, manager.Close()) })
	return &Runtime{manager: manager}
}

// NewClient reports that the test runtime has no transport clients.
func (*Runtime) NewClient(context.Context, string) (transportclient.Client, error) {
	return nil, errors.New("test runtime: client is unavailable")
}

// Config returns the in-memory configuration manager.
func (r *Runtime) Config() *config.Manager { return r.manager }

// Logger returns a discard-backed logger for tests.
func (*Runtime) Logger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// TracerProvider returns a no-op tracer provider for tests.
func (*Runtime) TracerProvider() trace.TracerProvider { return tracenoop.NewTracerProvider() }

// MeterProvider returns a no-op meter provider for tests.
func (*Runtime) MeterProvider() metric.MeterProvider { return metricnoop.NewMeterProvider() }

// Identity returns the zero runtime identity used by tests.
func (*Runtime) Identity() yggdrasil.Identity { return yggdrasil.Identity{} }

// Lookup reports that dependency lookup is unavailable in the test runtime.
func (*Runtime) Lookup(any) error { return errors.New("test runtime: lookup is unavailable") }
