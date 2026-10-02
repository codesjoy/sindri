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
	"fmt"
	"log/slog"
	"math"
	"os"
	"runtime/debug"
	"strconv"
	"strings"

	"github.com/KimMachineGun/automemlimit/memlimit"
	"github.com/codesjoy/sindri/internal/pkg/xgorm"
	"github.com/codesjoy/sindri/internal/sequence/biz"
	"github.com/codesjoy/yggdrasil/v3"
)

const sequenceOwner = "skuld_sequence"

// Config is the immutable process-level sequence configuration.
//
// Every section is owned by the package that reads it: the data plane and the
// publisher state their own types, defaults and rules in biz, and the ticker
// states its own in task. This type composes those sections and owns only what
// no single package can state for itself -- the startup mode, the process-wide
// runtime and database settings, and the rules that cross package boundaries.
type Config struct {
	// Mode is the process's startup shape: data, control or both. It is
	// required; a process that does not state one cannot be checked against the
	// components it did start.
	Mode Mode `mapstructure:"mode"`
	// Runtime holds the process-wide Go runtime settings.
	Runtime runtimeConfig `mapstructure:"runtime"`
	// Database holds the shared pool both planes run over.
	Database xgorm.Config `mapstructure:"database"`
	// DataPlane holds the allocator, node and ownership bounds.
	DataPlane biz.DataPlaneConfig `mapstructure:"dataplane"`
	// ControlPlane holds the publisher's parameters.
	ControlPlane biz.ControlPlaneConfig `mapstructure:"controlplane"`
}

// Load decodes and validates the sequence configuration.
//
// Defaults are declared on the fields they belong to: the framework applies the
// default tags while decoding, so a key the deployment omitted -- or stated as
// zero -- keeps the value the owning package declared on the field. What is left
// to this loader is the process-wide shape, the identity normalization, and the
// checks that no single section can make for itself.
func Load(rt yggdrasil.Runtime) (*Config, error) {
	if rt == nil || rt.Config() == nil {
		return nil, errors.New("sequence config: runtime config is required")
	}
	section := rt.Config().Section("app", "sequence")
	if section.Empty() {
		return nil, errors.New("sequence config: app.sequence is required")
	}
	var cfg Config
	if err := section.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("decode sequence config: %w", err)
	}
	if err := cfg.applyModeOverride(); err != nil {
		return nil, err
	}
	cfg.normalize()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	// The governor's enabled flag is tri-state in the framework: unset means
	// enabled. Mirroring that here is what keeps this from failing a deployment
	// that never mentioned the governor at all, while still catching one that
	// turned it off and would otherwise have lost the report surface.
	var governor struct {
		Enabled *bool `mapstructure:"enabled"`
	}
	if err := rt.Config().
		Section("yggdrasil", "admin", "governor").
		Decode(&governor); err != nil {
		return nil, fmt.Errorf("decode governor config: %w", err)
	}
	if err := cfg.ValidateDeployment(governor.Enabled == nil || *governor.Enabled); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// normalize trims the values the file hands over verbatim and applies the shared
// pool defaults.
//
// Each section owns its own defaults through the default tags on its fields, so
// what is left here is the process-wide database shape -- shared infrastructure
// rather than a sequence section -- and the whitespace the identity checks
// compare.
func (c *Config) normalize() {
	c.Database.Driver = strings.TrimSpace(c.Database.Driver)
	c.Database.DSN = strings.TrimSpace(c.Database.DSN)
	c.Database.ExpectedDatabase = strings.TrimSpace(c.Database.ExpectedDatabase)
	c.Database.ExpectedAccount = strings.TrimSpace(c.Database.ExpectedAccount)
	c.Database.SetDefaults()
	c.DataPlane.Node.ID = strings.TrimSpace(c.DataPlane.Node.ID)
}

// ValidateDeployment checks the deployment shape the process needs, as opposed
// to the module-local settings Validate covers.
//
// The governor must be enabled, because the high-availability report is
// registered on it. That registration is unconditional in code, so a process
// whose governor is unavailable already fails to start; checking it here turns
// that install failure into a configuration error that names the setting.
//
// This is section D.12 in its current form. The rule used to name the REST
// listener, which carried the report until the report moved to the admin
// surface. What the rule protects is unchanged: a deployment must not be able to
// run while claiming an ordering guarantee it has nowhere to report.
//
// It is separate from Validate because it needs a fact -- whether a listener
// exists -- that the sequence configuration cannot state for itself, and because
// keeping them apart leaves Validate callable by any test that builds a
// configuration without also building a runtime. The caller reads that fact from
// the yggdrasil framework section, which is the only place it is stated.
func (c Config) ValidateDeployment(governorEnabled bool) error {
	if !governorEnabled {
		return errors.New(
			"sequence config: yggdrasil.admin.governor must be enabled; " +
				"it carries the readiness endpoint that reports the ordering bounds",
		)
	}
	return nil
}

// Validate checks the composition and the invariants no single section owns.
//
// The checks are scoped by mode: a control replica runs no allocator and no
// ticker, so requiring it to carry data-plane settings would be requiring
// configuration for components it never starts. What is validated in every mode
// is what every mode uses: the database identity, the runtime memory setting,
// the publisher, and the startup mode itself. The data plane's own rules live
// with its configuration; what stays here is the coupling between the node's
// heartbeat and the ticker that drives it, which neither package can state
// alone.
func (c Config) Validate() error {
	if _, err := parseMode(string(c.Mode)); err != nil {
		return fmt.Errorf("sequence config: %w", err)
	}
	if err := c.Database.Validate(); err != nil {
		return fmt.Errorf("sequence config: %w", err)
	}
	if c.Database.ExpectedDatabase != sequenceOwner || c.Database.ExpectedAccount != sequenceOwner {
		return fmt.Errorf("sequence config: database identity must be %s", sequenceOwner)
	}
	if c.DataPlane.Allocator.LegacyStep != nil {
		return errors.New(
			"sequence config: dataplane.allocator.step was removed; " +
				"use dataplane.allocator.default_step",
		)
	}
	if err := c.validateRuntime(); err != nil {
		return err
	}
	if err := c.ControlPlane.Validate(); err != nil {
		return fmt.Errorf("sequence config: %w", err)
	}
	if c.Mode == ModeControl {
		return c.DataPlane.HA.ValidateAuthority()
	}
	if err := c.DataPlane.Validate(); err != nil {
		return fmt.Errorf("sequence config: %w", err)
	}
	return c.validateMembershipCadence()
}

// validateTickerCoupling checks what ties the node's heartbeat to the ticker
// that drives it.
//
// Both values are owned by different packages, so neither can check the pair by
// itself, and both rules have a fleet-visible failure: a heartbeat timeout at or
// below the heartbeat interval pauses every node that hiccups, and a liveness TTL
// at or below the period drops a node from the fleet between its own renewals.
func (c Config) validateMembershipCadence() error {
	period := c.DataPlane.Node.HeartbeatInterval
	if c.DataPlane.HA.NodeTTL <= period {
		return fmt.Errorf(
			"sequence config: dataplane.ha.node_ttl (%s) must exceed the heartbeat "+
				"period dataplane.node.heartbeat_interval (%s)",
			c.DataPlane.HA.NodeTTL,
			period,
		)
	}
	return nil
}

// ModeEnv overrides the configured startup mode.
//
// One image serves every deployment shape, so an operator who runs one config
// for a data-plane StatefulSet and a control replica can have them differ by
// this variable alone. The command line wins over it, because --mode is how a
// one-off run states its shape without touching the environment.
const ModeEnv = "SKULD_SEQUENCE_MODE"

// Mode is the startup shape of a sequence process.
type Mode string

const (
	// ModeData serves ids: allocator, ticker, RPC, local planning and liveness.
	ModeData Mode = "data"
	// ModeControl publishes the directory: coordinator lease, route
	// materialisation, control-plane readiness and metrics.
	ModeControl Mode = "control"
	// ModeBoth runs the data plane and the publisher in one process, sharing
	// one database pool.
	ModeBoth Mode = "both"
)

// parseMode reads a startup mode, strictly. A value that is not one of the
// three shapes is refused rather than defaulted: starting the wrong shape is
// silent -- a control process that never publishes looks healthy until clients
// stop refreshing -- so the only safe answer is to fail the start.
func parseMode(value string) (Mode, error) {
	switch mode := Mode(strings.TrimSpace(value)); mode {
	case ModeData, ModeControl, ModeBoth:
		return mode, nil
	default:
		return "", fmt.Errorf("mode must be one of data|control|both, got %q", value)
	}
}

// errModeValue is returned when --mode is present without a value. It is a
// startup error rather than a fallback to the configured mode: a process whose
// shape is ambiguous must not pick one silently.
var errModeValue = errors.New("--mode requires a value")

// ModeFromArgs reads --mode from a command line. It accepts both the
// "--mode=value" and "--mode value" spellings, and a single dash for symmetry
// with the Go flag package the framework reads. The last occurrence wins, which
// is what an operator who restates the flag expects.
//
// The value is not validated here: it is written to ModeEnv, and the
// configuration loader validates it exactly once, so the command line and the
// environment share one precedence rule and one refusal.
func ModeFromArgs(args []string) (string, bool, error) {
	var last string
	found := false
	for index := 0; index < len(args); index++ {
		name, value, hasValue := strings.Cut(strings.TrimSpace(args[index]), "=")
		if name != "--mode" && name != "-mode" {
			continue
		}
		found = true
		if !hasValue {
			if index+1 >= len(args) {
				return "", true, errModeValue
			}
			index++
			value = args[index]
		}
		value = strings.TrimSpace(value)
		if value == "" {
			return "", true, errModeValue
		}
		last = value
	}
	return last, found, nil
}

// applyModeOverride lets the environment restate the configured startup mode.
//
// The command line reaches this through the same variable: cmd/sequence parses
// --mode before the framework loads configuration and writes it here, so there
// is one precedence rule rather than two.
func (c *Config) applyModeOverride() error {
	if value, ok := os.LookupEnv(ModeEnv); ok && strings.TrimSpace(value) != "" {
		c.Mode = Mode(value)
	}
	mode, err := parseMode(string(c.Mode))
	if err != nil {
		return fmt.Errorf("sequence config: %w", err)
	}
	c.Mode = mode
	return nil
}

const (
	// DefaultMemoryLimit enables cgroup-aware automatic memory sizing.
	DefaultMemoryLimit = "auto"
	// DefaultAutoMemoryLimitRatio leaves headroom outside Go-managed memory.
	DefaultAutoMemoryLimitRatio = 0.8
	// MinimumMemoryLimit prevents configurations that would force near-continuous GC.
	MinimumMemoryLimit = 64 << 20
	goMemoryLimitEnv   = "GOMEMLIMIT"
)

type memoryLimitProvider func() (uint64, error)

type memoryLimitDependencies struct {
	cgroup    memoryLimitProvider
	system    memoryLimitProvider
	lookupEnv func(string) (string, bool)
	setLimit  func(int64) int64
}

// MemoryLimitResult describes the process memory limit applied at startup.
type MemoryLimitResult struct {
	Source     string
	BaseBytes  uint64
	LimitBytes uint64
	Ratio      float64
}

// parseMemoryLimit parses the byte syntax supported by the Go runtime's GOMEMLIMIT.
func parseMemoryLimit(value string) (uint64, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, errors.New("must not be empty")
	}

	multiplier := uint64(1)
	number := value
	for _, unit := range []struct {
		suffix string
		scale  uint64
	}{
		{suffix: "TiB", scale: 1 << 40},
		{suffix: "GiB", scale: 1 << 30},
		{suffix: "MiB", scale: 1 << 20},
		{suffix: "KiB", scale: 1 << 10},
		{suffix: "B", scale: 1},
	} {
		if strings.HasSuffix(value, unit.suffix) {
			number = strings.TrimSuffix(value, unit.suffix)
			multiplier = unit.scale
			break
		}
	}
	if number == "" || strings.Trim(number, "0123456789") != "" {
		return 0, fmt.Errorf("invalid byte value %q", value)
	}
	parsed, err := strconv.ParseUint(number, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse byte value %q: %w", value, err)
	}
	if parsed > math.MaxUint64/multiplier {
		return 0, fmt.Errorf("byte value %q overflows uint64", value)
	}
	return parsed * multiplier, nil
}

// validateMemoryLimit rejects unsafe or effectively unlimited Go memory budgets.
func validateMemoryLimit(limitBytes uint64) error {
	if limitBytes < MinimumMemoryLimit {
		return fmt.Errorf("must be at least %d bytes (64MiB)", MinimumMemoryLimit)
	}
	if limitBytes >= math.MaxInt64 {
		return errors.New("must be less than math.MaxInt64")
	}
	return nil
}

// ConfigureMemoryLimit resolves and applies the sequence process memory limit.
//
// A nil memoryLimit means the deployment did not state one: GOMEMLIMIT wins
// when it is present, and automatic detection is the fallback. A stated value
// is authoritative -- including "auto", which then skips the environment --
// because it is the deployment's own answer rather than a compatibility path.
func ConfigureMemoryLimit(
	memoryLimit *string,
	autoRatio float64,
	logger *slog.Logger,
) (MemoryLimitResult, error) {
	return configureMemoryLimit(
		memoryLimit,
		autoRatio,
		logger,
		memoryLimitDependencies{
			cgroup:    memlimit.FromCgroup,
			system:    memlimit.FromSystem,
			lookupEnv: os.LookupEnv,
			setLimit:  debug.SetMemoryLimit,
		},
	)
}

func configureMemoryLimit(
	memoryLimit *string,
	autoRatio float64,
	logger *slog.Logger,
	deps memoryLimitDependencies,
) (MemoryLimitResult, error) {
	value := ""
	source := "configuration"
	if memoryLimit == nil {
		if environmentValue, ok := deps.lookupEnv(goMemoryLimitEnv); ok {
			value = strings.TrimSpace(environmentValue)
			source = "environment"
		} else {
			value = DefaultMemoryLimit
			source = "auto"
		}
	} else {
		value = strings.TrimSpace(*memoryLimit)
	}

	var result MemoryLimitResult
	if value == "auto" {
		if autoRatio <= 0 || autoRatio >= 1 {
			return result, errors.New("auto memory limit ratio must be within (0,1)")
		}
		base, detectedSource, err := detectMemoryLimit(deps.cgroup, deps.system)
		if err != nil {
			return result, fmt.Errorf(
				"detect automatic memory limit: %w; configure a fixed runtime.memory_limit",
				err,
			)
		}
		result = MemoryLimitResult{
			Source:     detectedSource,
			BaseBytes:  base,
			LimitBytes: uint64(float64(base) * autoRatio),
			Ratio:      autoRatio,
		}
	} else {
		limit, err := parseMemoryLimit(value)
		if err != nil {
			return result, fmt.Errorf("parse %s memory limit: %w", source, err)
		}
		result = MemoryLimitResult{Source: source, BaseBytes: limit, LimitBytes: limit, Ratio: 1}
	}
	if err := validateMemoryLimit(result.LimitBytes); err != nil {
		return MemoryLimitResult{}, fmt.Errorf("validate %s memory limit: %w", source, err)
	}
	deps.setLimit(int64(result.LimitBytes))
	if logger != nil {
		logger.Info(
			"configured Go runtime memory limit",
			"source", result.Source,
			"base_bytes", result.BaseBytes,
			"ratio", result.Ratio,
			"limit_bytes", result.LimitBytes,
		)
	}
	return result, nil
}

func detectMemoryLimit(cgroup, system memoryLimitProvider) (uint64, string, error) {
	cgroupLimit, cgroupErr := cgroup()
	systemLimit, systemErr := system()
	cgroupOK := cgroupErr == nil && cgroupLimit > 0 && cgroupLimit < math.MaxInt64
	systemOK := systemErr == nil && systemLimit > 0 && systemLimit < math.MaxInt64
	if cgroupErr == nil && !cgroupOK {
		cgroupErr = fmt.Errorf("invalid or unlimited value %d", cgroupLimit)
	}
	if systemErr == nil && !systemOK {
		systemErr = fmt.Errorf("invalid or unlimited value %d", systemLimit)
	}

	switch {
	case cgroupOK && systemOK && cgroupLimit <= systemLimit:
		return cgroupLimit, "cgroup", nil
	case cgroupOK && systemOK:
		return systemLimit, "system", nil
	case cgroupOK:
		return cgroupLimit, "cgroup", nil
	case systemOK:
		return systemLimit, "system", nil
	default:
		return 0, "", fmt.Errorf("cgroup provider: %v; system provider: %v", cgroupErr, systemErr)
	}
}

// runtimeConfig controls process-wide Go runtime settings.
type runtimeConfig struct {
	// MemoryLimit is a pointer so "stated as empty" stays distinguishable from
	// "omitted": the former is refused, the latter takes the tag default and
	// then follows the GOMEMLIMIT compatibility path.
	MemoryLimit *string `mapstructure:"memory_limit" default:"auto"`
	// AutoMemoryLimitRatio is the fraction of the detected limit to apply when
	// the resolved value is auto.
	AutoMemoryLimitRatio float64 `mapstructure:"auto_memory_limit_ratio" default:"0.8"`
}

// validateRuntime checks the process-wide Go runtime setting.
func (c Config) validateRuntime() error {
	if c.Runtime.MemoryLimit == nil {
		return nil
	}
	memoryLimit := strings.TrimSpace(*c.Runtime.MemoryLimit)
	if memoryLimit == "" {
		return errors.New("sequence config: runtime.memory_limit must not be empty")
	}
	if memoryLimit != DefaultMemoryLimit {
		limit, err := parseMemoryLimit(memoryLimit)
		if err != nil {
			return fmt.Errorf("sequence config: runtime.memory_limit: %w", err)
		}
		if err := validateMemoryLimit(limit); err != nil {
			return fmt.Errorf("sequence config: runtime.memory_limit: %w", err)
		}
	} else if c.Runtime.AutoMemoryLimitRatio <= 0 || c.Runtime.AutoMemoryLimitRatio >= 1 {
		return errors.New(
			"sequence config: runtime.auto_memory_limit_ratio must be within (0,1)",
		)
	}
	return nil
}
