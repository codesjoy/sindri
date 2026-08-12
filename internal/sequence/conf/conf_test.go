package conf

import (
	"strings"
	"testing"
	"time"

	testkit "github.com/codesjoy/skuld/internal/pkg/tests"
	"github.com/codesjoy/skuld/internal/pkg/xgorm"
	"github.com/codesjoy/skuld/internal/sequence/biz"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func validValues(driver string) map[string]any {
	return map[string]any{
		"app": map[string]any{
			"sequence": map[string]any{
				"database": map[string]any{
					"driver": driver, "dsn": "database-dsn",
					"expected_database": sequenceOwner,
					"expected_account":  sequenceOwner,
				},
				"node": map[string]any{"id": "node-a"},
			},
		},
	}
}

func TestLoadRejectsMissingRuntimeAndSection(t *testing.T) {
	_, err := Load(nil)
	require.Error(t, err)
	_, err = Load(testkit.NewRuntime(t, map[string]any{}))
	require.Error(t, err)
}

func TestLoadRejectsDecodeError(t *testing.T) {
	values := validValues(xgorm.DriverPostgres)
	values["app"].(map[string]any)["sequence"].(map[string]any)["allocator"] = map[string]any{
		"default_step": map[string]any{"invalid": true},
	}
	_, err := Load(testkit.NewRuntime(t, values))
	require.Error(t, err)
}

func TestLoadAppliesDefaultsForSupportedDrivers(t *testing.T) {
	for _, driver := range []string{xgorm.DriverPostgres, xgorm.DriverMySQL} {
		t.Run(driver, func(t *testing.T) {
			cfg, err := Load(testkit.NewRuntime(t, validValues(driver)))
			require.NoError(t, err)
			assert.Equal(t, driver, cfg.Database.Driver)
			assert.Equal(t, biz.DefaultStep, cfg.Allocator.DefaultStep)
			assert.Equal(t, biz.DefaultMaxStep, cfg.Allocator.MaxStep)
			assert.Equal(t, biz.DefaultPrefetchRatio, cfg.Allocator.PrefetchRatio)
			assert.Equal(
				t,
				biz.DefaultStepIncreaseThreshold,
				cfg.Allocator.StepIncreaseThreshold,
			)
			assert.Equal(
				t,
				biz.DefaultStepDecreaseThreshold,
				cfg.Allocator.StepDecreaseThreshold,
			)
			assert.Equal(t, biz.DefaultReserveTimeout, cfg.Allocator.ReserveTimeout)
			assert.Equal(t, int64(3), cfg.Node.HeartbeatTimeoutTicks)
			assert.Equal(t, time.Second, cfg.Node.RouteQueryTimeout)
			assert.Equal(t, time.Second, cfg.Ticker.BaseTickInterval)
			assert.Equal(t, int64(1), cfg.Ticker.HeartbeatTicks)
			assert.Equal(t, 20, cfg.Database.MaxOpenConns)
		})
	}
}

func TestLoadDefaultsEmptyDriverToPostgres(t *testing.T) {
	cfg, err := Load(testkit.NewRuntime(t, validValues("")))
	require.NoError(t, err)
	assert.Equal(t, xgorm.DriverPostgres, cfg.Database.Driver)
}

func TestLoadRejectsInvalidContracts(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(map[string]any)
	}{
		{name: "driver", mutate: func(db map[string]any) { db["driver"] = "sqlite" }},
		{name: "dsn", mutate: func(db map[string]any) { db["dsn"] = " " }},
		{
			name:   "database owner",
			mutate: func(db map[string]any) { db["expected_database"] = "other" },
		},
		{
			name:   "account owner",
			mutate: func(db map[string]any) { db["expected_account"] = "other" },
		},
		{name: "pool", mutate: func(db map[string]any) { db["max_open_conns"] = -1 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			values := validValues(xgorm.DriverPostgres)
			section := values["app"].(map[string]any)["sequence"].(map[string]any)
			test.mutate(section["database"].(map[string]any))
			_, err := Load(testkit.NewRuntime(t, values))
			require.Error(t, err)
		})
	}
}

func TestLoadRejectsAllocatorAndSchedulingBoundaries(t *testing.T) {
	tests := []struct {
		name   string
		values map[string]any
	}{
		{
			name: "small default step",
			values: map[string]any{
				"allocator": map[string]any{"default_step": biz.MinStep - 1},
			},
		},
		{
			name:   "legacy step",
			values: map[string]any{"allocator": map[string]any{"step": 100}},
		},
		{
			name:   "legacy zero step",
			values: map[string]any{"allocator": map[string]any{"step": 0}},
		},
		{
			name: "max below default",
			values: map[string]any{
				"allocator": map[string]any{"default_step": 100, "max_step": 99},
			},
		},
		{
			name: "zero prefetch ratio",
			values: map[string]any{
				"allocator": map[string]any{"prefetch_ratio": 0},
			},
		},
		{
			name: "negative prefetch ratio",
			values: map[string]any{
				"allocator": map[string]any{"prefetch_ratio": -0.1},
			},
		},
		{
			name: "full prefetch ratio",
			values: map[string]any{
				"allocator": map[string]any{"prefetch_ratio": 1.0},
			},
		},
		{
			name: "increase threshold",
			values: map[string]any{
				"allocator": map[string]any{"step_increase_threshold": "0s"},
			},
		},
		{
			name: "negative increase threshold",
			values: map[string]any{
				"allocator": map[string]any{"step_increase_threshold": "-1s"},
			},
		},
		{
			name: "threshold order",
			values: map[string]any{
				"allocator": map[string]any{
					"step_increase_threshold": "30m",
					"step_decrease_threshold": "15m",
				},
			},
		},
		{
			name: "reserve timeout",
			values: map[string]any{
				"allocator": map[string]any{"reserve_timeout": "0s"},
			},
		},
		{
			name: "negative reserve timeout",
			values: map[string]any{
				"allocator": map[string]any{"reserve_timeout": "-1s"},
			},
		},
		{name: "empty node", values: map[string]any{"node": map[string]any{"id": " "}}},
		{
			name:   "long node",
			values: map[string]any{"node": map[string]any{"id": strings.Repeat("a", 257)}},
		},
		{
			name: "query timeout",
			values: map[string]any{
				"node": map[string]any{"id": "node-a", "route_query_timeout": "-1s"},
			},
		},
		{
			name:   "base interval",
			values: map[string]any{"ticker": map[string]any{"base_tick_interval": "-1s"}},
		},
		{
			name:   "heartbeat interval",
			values: map[string]any{"ticker": map[string]any{"heartbeat_ticks": -1}},
		},
		{
			name: "heartbeat timeout",
			values: map[string]any{
				"node":   map[string]any{"id": "node-a", "heartbeat_timeout_ticks": 2},
				"ticker": map[string]any{"heartbeat_ticks": 2},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			values := validValues(xgorm.DriverPostgres)
			section := values["app"].(map[string]any)["sequence"].(map[string]any)
			for key, value := range test.values {
				section[key] = value
			}
			_, err := Load(testkit.NewRuntime(t, values))
			require.Error(t, err)
		})
	}
}

func TestAllocatorSnapshotPreservesExplicitZero(t *testing.T) {
	values := validValues(xgorm.DriverPostgres)
	section := values["app"].(map[string]any)["sequence"].(map[string]any)
	section["allocator"] = map[string]any{"prefetch_ratio": 0}
	runtime := testkit.NewRuntime(t, values)
	allocatorValues := runtime.Config().
		Section("app", "sequence", "allocator").
		Map()
	value, ok := allocatorValues["prefetch_ratio"]
	require.True(t, ok, "allocator map = %#v", allocatorValues)
	assert.Equal(t, 0, value)
}
