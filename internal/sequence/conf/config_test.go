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

package conf

import (
	"errors"
	"math"
	"runtime/debug"
	"strings"
	"testing"
	"time"

	testkit "github.com/codesjoy/sindri/internal/pkg/tests"
	"github.com/codesjoy/sindri/internal/pkg/xgorm"
	"github.com/codesjoy/sindri/internal/sequence/biz"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func validValues(driver string) map[string]any {
	return map[string]any{
		"app": map[string]any{
			"sequence": map[string]any{
				// The startup shape is required: a process that does not state
				// one cannot be checked against the components it did start.
				"mode": "both",
				"database": map[string]any{
					"driver": driver, "dsn": "database-dsn",
					"expected_database": sequenceOwner,
					"expected_account":  sequenceOwner,
				},
				"dataplane": map[string]any{
					"node": map[string]any{"id": "node-a"},
					// The platform assertions the ordering argument rests on are
					// mandatory, so a configuration that is valid at all carries
					// them.
					"ha": map[string]any{
						"pause_verified":    true,
						"clock_disciplined": true,
					},
				},
			},
		},
	}
}

// sequenceValues returns the app.sequence map a test mutates.
func sequenceValues(values map[string]any) map[string]any {
	return values["app"].(map[string]any)["sequence"].(map[string]any)
}

// dataPlaneValues returns the app.sequence.dataplane map, creating it when the
// test started from a configuration that has none.
func dataPlaneValues(values map[string]any) map[string]any {
	sequence := sequenceValues(values)
	plane, ok := sequence["dataplane"].(map[string]any)
	if !ok {
		plane = map[string]any{}
		sequence["dataplane"] = plane
	}
	return plane
}

func TestLoadRejectsMissingRuntimeAndSection(t *testing.T) {
	_, err := Load(nil)
	require.Error(t, err)
	_, err = Load(testkit.NewRuntime(t, map[string]any{}))
	require.Error(t, err)
}

func TestLoadRejectsDecodeError(t *testing.T) {
	values := validValues(xgorm.DriverPostgres)
	dataPlaneValues(values)["allocator"] = map[string]any{
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
			assert.Equal(t, int64(100), cfg.DataPlane.Allocator.DefaultStep)
			assert.Equal(t, int64(10000), cfg.DataPlane.Allocator.MaxStep)
			assert.Equal(t, 0.5, cfg.DataPlane.Allocator.PrefetchRatio)
			assert.Equal(t, 5*time.Minute, cfg.DataPlane.Allocator.PrefetchLatencyWindow)
			assert.Equal(t, 100, cfg.DataPlane.Allocator.PrefetchLatencyMinSamples)
			assert.Equal(t, time.Minute, cfg.DataPlane.Allocator.PrefetchRateResetAfter)
			assert.Equal(t, 15*time.Minute, cfg.DataPlane.Allocator.StepIncreaseThreshold)
			assert.Equal(t, 30*time.Minute, cfg.DataPlane.Allocator.StepDecreaseThreshold)
			assert.Equal(t, time.Second, cfg.DataPlane.Allocator.ReserveTimeout)
			assert.Equal(t, 24*time.Hour, cfg.DataPlane.Allocator.IdleTimeout)
			assert.Equal(t, time.Second, cfg.DataPlane.Allocator.CleanupInterval)
			assert.Equal(t, 64, cfg.DataPlane.Allocator.CleanupSlotsPerRun)
			assert.Equal(t, 0.9, cfg.DataPlane.Allocator.MemoryHighWatermarkRatio)
			assert.Equal(t, 5*time.Second, cfg.DataPlane.HA.QuietWindow)
			assert.Equal(t, 3*time.Second, cfg.DataPlane.HA.LeaseDuration)
			assert.Equal(t, time.Second, cfg.DataPlane.HA.RenewInterval)
			assert.Equal(t, time.Second, cfg.DataPlane.Node.HeartbeatInterval)
			assert.Equal(t, time.Second, cfg.DataPlane.Node.RouteQueryTimeout)
			assert.Equal(t, time.Second, cfg.DataPlane.Node.RouteRefreshInterval)
			assert.Equal(t, 250*time.Millisecond, cfg.DataPlane.Node.HandoffInterval)
			assert.Equal(t, 20, cfg.Database.MaxOpenConns)
			require.NotNil(t, cfg.Runtime.MemoryLimit)
			assert.Equal(t, DefaultMemoryLimit, *cfg.Runtime.MemoryLimit)
			assert.Equal(t, DefaultAutoMemoryLimitRatio, cfg.Runtime.AutoMemoryLimitRatio)
		})
	}
}

func TestLoadRuntimeMemoryConfiguration(t *testing.T) {
	values := validValues(xgorm.DriverPostgres)
	sequenceValues(values)["runtime"] = map[string]any{
		"memory_limit":            "256MiB",
		"auto_memory_limit_ratio": 0.75,
	}
	cfg, err := Load(testkit.NewRuntime(t, values))
	require.NoError(t, err)
	require.NotNil(t, cfg.Runtime.MemoryLimit)
	assert.Equal(t, "256MiB", *cfg.Runtime.MemoryLimit)
	assert.Equal(t, 0.75, cfg.Runtime.AutoMemoryLimitRatio)
}

func TestLoadRejectsInvalidRuntimeMemoryConfiguration(t *testing.T) {
	tests := []map[string]any{
		{"memory_limit": ""},
		{"memory_limit": "63MiB"},
		{"memory_limit": "9223372036854775807"},
		{"memory_limit": "1GB"},
		{"memory_limit": "auto", "auto_memory_limit_ratio": 1},
	}
	for _, runtimeValues := range tests {
		values := validValues(xgorm.DriverPostgres)
		sequenceValues(values)["runtime"] = runtimeValues
		_, err := Load(testkit.NewRuntime(t, values))
		assert.Error(t, err, "runtime values: %#v", runtimeValues)
	}
}

func TestLoadDefaultsEmptyDriverToPostgres(t *testing.T) {
	cfg, err := Load(testkit.NewRuntime(t, validValues("")))
	require.NoError(t, err)
	assert.Equal(t, xgorm.DriverPostgres, cfg.Database.Driver)
}

// TestLoadRequiresAStartupMode pins the strict rule the plan calls for: a
// process that does not state which shape to run cannot be checked against the
// components it started, and a typo must not fall back to a default.
func TestLoadRequiresAStartupMode(t *testing.T) {
	tests := []struct {
		name    string
		mode    any
		present bool
	}{
		{name: "missing"},
		{name: "empty", mode: "", present: true},
		{name: "unknown", mode: "publisher", present: true},
		{name: "wrong case", mode: "Data", present: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			values := validValues(xgorm.DriverPostgres)
			section := sequenceValues(values)
			if test.present {
				section["mode"] = test.mode
			} else {
				delete(section, "mode")
			}
			_, err := Load(testkit.NewRuntime(t, values))
			require.Error(t, err)
		})
	}
}

func TestParseModeAcceptsOnlyTheThreeShapes(t *testing.T) {
	for _, mode := range []Mode{ModeData, ModeControl, ModeBoth} {
		parsed, err := parseMode(string(mode))
		require.NoError(t, err)
		assert.Equal(t, mode, parsed)
	}
	_, err := parseMode("")
	require.Error(t, err)
}

func TestModeFromArgsReadsBothSpellings(t *testing.T) {
	tests := []struct {
		name  string
		args  []string
		value string
		found bool
	}{
		{name: "absent", args: []string{"--config", "sequence.yaml"}},
		{name: "equals", args: []string{"--mode=control"}, value: "control", found: true},
		{name: "separate", args: []string{"--mode", "data"}, value: "data", found: true},
		{name: "single dash", args: []string{"-mode=both"}, value: "both", found: true},
		{
			name:  "last wins",
			args:  []string{"--mode=data", "--mode=control"},
			value: "control", found: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value, found, err := ModeFromArgs(test.args)
			require.NoError(t, err)
			assert.Equal(t, test.found, found)
			if test.found {
				assert.Equal(t, test.value, value)
			}
		})
	}
}

// TestModeFromArgsRefusesAnEmptyValue pins the difference between "not stated"
// and "stated with nothing": the first falls back to configuration, the second
// is a startup error rather than a silent default.
func TestModeFromArgsRefusesAnEmptyValue(t *testing.T) {
	for _, args := range [][]string{
		{"--mode"},
		{"--mode="},
		{"--mode", "  "},
	} {
		_, found, err := ModeFromArgs(args)
		assert.True(t, found)
		require.ErrorIs(t, err, errModeValue, "args: %#v", args)
	}
}

// TestLoadAppliesPlaneDefaults covers the sections the plan moved: the publisher
// bounds belong to the control plane, and node_ttl sits with the HA bounds the
// data-plane planner reads.
func TestLoadAppliesPlaneDefaults(t *testing.T) {
	cfg, err := Load(testkit.NewRuntime(t, validValues(xgorm.DriverPostgres)))
	require.NoError(t, err)
	assert.Equal(t, ModeBoth, cfg.Mode)
	assert.Equal(t, 15*time.Second, cfg.DataPlane.HA.NodeTTL)
	assert.Equal(t, int64(1), cfg.ControlPlane.LayoutVersion)
	assert.Equal(t, 64, cfg.ControlPlane.Migration.BatchSlots)
	assert.Equal(t, 10*time.Second, cfg.ControlPlane.CoordinatorLease)
	assert.Equal(t, 5*time.Second, cfg.ControlPlane.ReconcileInterval)
	assert.Equal(t, 3*time.Second, cfg.ControlPlane.PassTimeout)
	assert.Less(t, cfg.ControlPlane.PassTimeout, cfg.ControlPlane.CoordinatorLease)
	assert.Less(t, cfg.ControlPlane.ReconcileInterval, cfg.ControlPlane.CoordinatorLease)
}

func TestControlModeValidatesSharedAuthorityOnly(t *testing.T) {
	values := validValues(xgorm.DriverPostgres)
	sequenceValues(values)["mode"] = "control"
	sequenceValues(values)["dataplane"] = map[string]any{}
	_, err := Load(testkit.NewRuntime(t, values))
	require.NoError(t, err)
	for _, field := range []string{"lease_duration", "max_pause", "node_ttl", "clock_drift", "clock_jump", "safety_margin", "quiet_window"} {
		t.Run(field, func(t *testing.T) {
			sequenceValues(values)["dataplane"] = map[string]any{"ha": map[string]any{field: "-1s"}}
			_, err := Load(testkit.NewRuntime(t, values))
			require.Error(t, err)
		})
	}
}

// TestControlModeSkipsDataPlaneValidation is the scoping rule as a test: a
// control replica runs no allocator and no ticker, so requiring it to carry
// data-plane settings would be requiring configuration for components it never
// starts.
func TestControlModeSkipsDataPlaneValidation(t *testing.T) {
	values := validValues(xgorm.DriverPostgres)
	sequenceValues(values)["mode"] = string(ModeControl)
	dataPlaneValues(values)["allocator"] = map[string]any{"default_step": biz.MinStep - 1}
	cfg, err := Load(testkit.NewRuntime(t, values))
	require.NoError(t, err)
	assert.Equal(t, ModeControl, cfg.Mode)
}

// TestModeEnvironmentRestatesTheConfiguredShape covers the deployment case the
// override exists for: one config file, a StatefulSet of data nodes and one
// publisher, differing by the environment alone.
func TestModeEnvironmentRestatesTheConfiguredShape(t *testing.T) {
	t.Setenv(ModeEnv, string(ModeData))
	cfg, err := Load(testkit.NewRuntime(t, validValues(xgorm.DriverPostgres)))
	require.NoError(t, err)
	assert.Equal(t, ModeData, cfg.Mode)

	t.Setenv(ModeEnv, "not-a-mode")
	_, err = Load(testkit.NewRuntime(t, validValues(xgorm.DriverPostgres)))
	require.Error(t, err, "an override that names no shape must fail the start")
}

// TestLoadTreatsStatedZerosAsDefaults pins the framework rule the tags exist
// for: a key the deployment stated as zero is indistinguishable from one it
// omitted, because the defaults are applied to the decoded value.
func TestLoadTreatsStatedZerosAsDefaults(t *testing.T) {
	values := validValues(xgorm.DriverPostgres)
	plane := dataPlaneValues(values)
	plane["allocator"] = map[string]any{
		"prefetch_ratio":               0,
		"prefetch_latency_min_samples": 0,
		"reserve_timeout":              "0s",
	}
	plane["node"] = map[string]any{"id": "node-a", "route_query_timeout": "0s"}
	plane["ha"].(map[string]any)["lease_duration"] = "0s"
	sequenceValues(values)["controlplane"] = map[string]any{
		"coordinator_lease": "0s",
		"pass_timeout":      "0s",
	}
	sequenceValues(values)["runtime"] = map[string]any{
		"memory_limit":            "auto",
		"auto_memory_limit_ratio": 0,
	}

	cfg, err := Load(testkit.NewRuntime(t, values))
	require.NoError(t, err)
	assert.Equal(t, DefaultAutoMemoryLimitRatio, cfg.Runtime.AutoMemoryLimitRatio)
	assert.Equal(t, 0.5, cfg.DataPlane.Allocator.PrefetchRatio)
	assert.Equal(t, 100, cfg.DataPlane.Allocator.PrefetchLatencyMinSamples)
	assert.Equal(t, time.Second, cfg.DataPlane.Allocator.ReserveTimeout)
	assert.Equal(t, time.Second, cfg.DataPlane.Node.RouteQueryTimeout)
	assert.Equal(t, 3*time.Second, cfg.DataPlane.HA.LeaseDuration)
	assert.Equal(t, 10*time.Second, cfg.ControlPlane.CoordinatorLease)
	assert.Equal(t, 3*time.Second, cfg.ControlPlane.PassTimeout)
	assert.Equal(t, time.Second, cfg.DataPlane.Node.RouteRefreshInterval)
	assert.Equal(t, time.Second, cfg.DataPlane.Node.HeartbeatInterval)
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
			section := sequenceValues(values)
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
			name: "low latency multiplier",
			values: map[string]any{
				"allocator": map[string]any{"prefetch_latency_multiplier": 1},
			},
		},
		{
			name: "high latency multiplier",
			values: map[string]any{
				"allocator": map[string]any{"prefetch_latency_multiplier": 10.1},
			},
		},
		{
			name: "too many latency samples",
			values: map[string]any{
				"allocator": map[string]any{
					"prefetch_latency_min_samples": biz.MaxReserveLatencySamples + 1,
				},
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
			name: "negative reserve timeout",
			values: map[string]any{
				"allocator": map[string]any{"reserve_timeout": "-1s"},
			},
		},
		{
			name: "negative idle timeout",
			values: map[string]any{
				"allocator": map[string]any{"idle_timeout": "-1s"},
			},
		},
		{
			name: "negative cleanup interval",
			values: map[string]any{
				"allocator": map[string]any{"cleanup_interval": "-1s"},
			},
		},
		{
			name: "cleanup interval exceeds idle timeout",
			values: map[string]any{
				"allocator": map[string]any{
					"idle_timeout":     "1m",
					"cleanup_interval": "2m",
				},
			},
		},
		{
			name: "too many cleanup slots",
			values: map[string]any{
				"allocator": map[string]any{"cleanup_slots_per_run": biz.SlotCount + 1},
			},
		},
		{
			name: "full memory watermark",
			values: map[string]any{
				"allocator": map[string]any{"memory_high_watermark_ratio": 1},
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
			name: "base interval",
			values: map[string]any{
				"node": map[string]any{"id": "node-a", "route_refresh_interval": "-1s"},
			},
		},
		{
			name: "heartbeat interval",
			values: map[string]any{
				"node": map[string]any{"id": "node-a", "heartbeat_interval": "-1s"},
			},
		},
		{
			name: "heartbeat timeout",
			values: map[string]any{
				"node": map[string]any{"id": "node-a", "handoff_interval": "-1s"},
			},
		},
		{
			name: "control plane pass timeout",
			values: map[string]any{
				"controlplane": map[string]any{
					"coordinator_lease": "10s",
					"pass_timeout":      "10s",
				},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			values := validValues(xgorm.DriverPostgres)
			section := sequenceValues(values)
			for key, value := range test.values {
				switch key {
				case "allocator", "node", "ha":
					dataPlaneValues(values)[key] = value
				default:
					section[key] = value
				}
			}
			_, err := Load(testkit.NewRuntime(t, values))
			require.Error(t, err)
		})
	}
}

// TestNodeTTLMustExceedTheHeartbeatPeriod pins the rule that crosses the data
// plane and the ticker: a node that renews less often than its TTL would be
// dropped from the fleet between its own renewals.
func TestNodeTTLMustExceedTheHeartbeatPeriod(t *testing.T) {
	values := validValues(xgorm.DriverPostgres)
	dataPlaneValues(values)["ha"].(map[string]any)["node_ttl"] = "1s"
	_, err := Load(testkit.NewRuntime(t, values))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "node_ttl")
}

// TestDeploymentRequiresTheGovernor covers the deployment-shape half of D.12:
// the report is registered on the governor, so a process that has turned the
// governor off has nowhere to state which bounds its ordering argument rests on.
// A deployment that cannot be told what it is claiming is the silent downgrade
// that section forbids.
func TestDeploymentRequiresTheGovernor(t *testing.T) {
	values := validValues(xgorm.DriverPostgres)
	values["yggdrasil"] = map[string]any{
		"admin": map[string]any{
			"governor": map[string]any{"enabled": false},
		},
	}
	_, err := Load(testkit.NewRuntime(t, values))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "yggdrasil.admin.governor")
}

// TestDeploymentAcceptsAnUnmentionedGovernor keeps the framework's tri-state
// honest: an unset enabled flag means enabled, so a deployment that never
// mentions the governor must not be refused for it.
func TestDeploymentAcceptsAnUnmentionedGovernor(t *testing.T) {
	_, err := Load(testkit.NewRuntime(t, validValues(xgorm.DriverPostgres)))
	require.NoError(t, err)
}

// TestValidateDeploymentIsAboutTheReportSurface pins that the rule is about the
// surface the report needs, not about which path is in force: the report is
// registered for every deployment, so none is exempt.
func TestValidateDeploymentIsAboutTheReportSurface(t *testing.T) {
	cfg := Config{}
	require.Error(t, cfg.ValidateDeployment(false))
	require.NoError(t, cfg.ValidateDeployment(true))
}

func TestValidateMemoryLimit(t *testing.T) {
	require.NoError(t, validateMemoryLimit(MinimumMemoryLimit))
	assert.Error(t, validateMemoryLimit(0))
	assert.Error(t, validateMemoryLimit(MinimumMemoryLimit-1))
	assert.Error(t, validateMemoryLimit(math.MaxInt64))
	assert.Error(t, validateMemoryLimit(math.MaxUint64))
}

func TestParseMemoryLimit(t *testing.T) {
	tests := []struct {
		value string
		want  uint64
	}{
		{value: "67108864", want: 64 << 20},
		{value: "64MiB", want: 64 << 20},
		{value: "1GiB", want: 1 << 30},
		{value: "2TiB", want: 2 << 40},
		{value: "1024KiB", want: 1 << 20},
		{value: " 64MiB ", want: 64 << 20},
	}
	for _, test := range tests {
		t.Run(test.value, func(t *testing.T) {
			got, err := parseMemoryLimit(test.value)
			require.NoError(t, err)
			assert.Equal(t, test.want, got)
		})
	}
	for _, value := range []string{"", "auto", "64MB", "1.5GiB", "-1", "18446744073709551615TiB"} {
		t.Run("invalid_"+value, func(t *testing.T) {
			_, err := parseMemoryLimit(value)
			assert.Error(t, err)
		})
	}
}

func TestDetectMemoryLimit(t *testing.T) {
	provider := func(value uint64, err error) memoryLimitProvider {
		return func() (uint64, error) { return value, err }
	}
	tests := []struct {
		name       string
		cgroup     memoryLimitProvider
		system     memoryLimitProvider
		want       uint64
		wantSource string
		wantError  bool
	}{
		{
			name:   "cgroup is smaller",
			cgroup: provider(1<<30, nil), system: provider(8<<30, nil),
			want: 1 << 30, wantSource: "cgroup",
		},
		{
			name:   "system is smaller",
			cgroup: provider(8<<30, nil), system: provider(4<<30, nil),
			want: 4 << 30, wantSource: "system",
		},
		{
			name:   "cgroup fallback",
			cgroup: provider(1<<30, nil), system: provider(0, errors.New("unavailable")),
			want: 1 << 30, wantSource: "cgroup",
		},
		{
			name:   "system fallback",
			cgroup: provider(0, errors.New("unavailable")), system: provider(2<<30, nil),
			want: 2 << 30, wantSource: "system",
		},
		{
			name:      "both unavailable",
			cgroup:    provider(0, errors.New("cgroup unavailable")),
			system:    provider(0, errors.New("system unavailable")),
			wantError: true,
		},
		{
			name:   "unlimited values",
			cgroup: provider(math.MaxUint64, nil), system: provider(math.MaxInt64, nil),
			wantError: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, source, err := detectMemoryLimit(test.cgroup, test.system)
			if test.wantError {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, test.want, got)
			assert.Equal(t, test.wantSource, source)
		})
	}
}

func TestConfigureMemoryLimit(t *testing.T) {
	provider := func(value uint64, err error) memoryLimitProvider {
		return func() (uint64, error) { return value, err }
	}
	tests := []struct {
		name       string
		configured string
		explicit   bool
		envValue   string
		envSet     bool
		ratio      float64
		cgroup     memoryLimitProvider
		system     memoryLimitProvider
		want       uint64
		wantSource string
		wantError  bool
	}{
		{
			name:       "explicit fixed overrides environment",
			configured: "256MiB", explicit: true, envValue: "512MiB", envSet: true,
			ratio: 0.8, cgroup: provider(1<<30, nil), system: provider(8<<30, nil),
			want: 256 << 20, wantSource: "configuration",
		},
		{
			name:       "explicit auto overrides environment",
			configured: "auto", explicit: true, envValue: "512MiB", envSet: true,
			ratio: 0.8, cgroup: provider(1<<30, nil), system: provider(8<<30, nil),
			want: 858993459, wantSource: "cgroup",
		},
		{
			name:       "environment compatibility",
			configured: "auto", explicit: false, envValue: "384MiB", envSet: true,
			ratio: 0.8, cgroup: provider(1<<30, nil), system: provider(8<<30, nil),
			want: 384 << 20, wantSource: "environment",
		},
		{
			name:       "default auto",
			configured: "auto", explicit: false, ratio: 0.75,
			cgroup: provider(2<<30, nil), system: provider(1<<30, nil),
			want: 768 << 20, wantSource: "system",
		},
		{
			name:       "detection failure",
			configured: "auto", explicit: true, ratio: 0.8,
			cgroup:    provider(0, errors.New("unavailable")),
			system:    provider(0, errors.New("unavailable")),
			wantError: true,
		},
		{
			name:       "below minimum",
			configured: "63MiB", explicit: true, ratio: 0.8,
			cgroup: provider(1<<30, nil), system: provider(1<<30, nil),
			wantError: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var applied int64
			var memoryLimit *string
			if test.explicit {
				memoryLimit = &test.configured
			}
			result, err := configureMemoryLimit(
				memoryLimit,
				test.ratio,
				nil,
				memoryLimitDependencies{
					cgroup: test.cgroup,
					system: test.system,
					lookupEnv: func(string) (string, bool) {
						return test.envValue, test.envSet
					},
					setLimit: func(value int64) int64 {
						applied = value
						return math.MaxInt64
					},
				},
			)
			if test.wantError {
				assert.Error(t, err)
				assert.Zero(t, applied)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, test.want, result.LimitBytes)
			assert.Equal(t, test.wantSource, result.Source)
			assert.Equal(t, int64(test.want), applied)
		})
	}
}

func TestConfigureMemoryLimitAppliesRuntimeSetting(t *testing.T) {
	previous := debug.SetMemoryLimit(-1)
	t.Cleanup(func() { debug.SetMemoryLimit(previous) })

	memoryLimit := "128MiB"
	result, err := ConfigureMemoryLimit(&memoryLimit, 0.8, nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(128<<20), result.LimitBytes)
	assert.Equal(t, int64(128<<20), debug.SetMemoryLimit(-1))
}
