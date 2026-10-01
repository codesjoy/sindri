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
	"os"
	"strings"
	"time"

	"github.com/codesjoy/sindri/internal/pkg/xgorm"
	"github.com/codesjoy/sindri/internal/sequence/biz"
	"github.com/codesjoy/sindri/internal/sequence/task"
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
	Runtime RuntimeConfig `mapstructure:"runtime"`
	// Database holds the shared pool both planes run over.
	Database xgorm.Config `mapstructure:"database"`
	// DataPlane holds the allocator, node and ownership bounds.
	DataPlane biz.DataPlaneConfig `mapstructure:"dataplane"`
	// ControlPlane holds the publisher's parameters.
	ControlPlane biz.ControlPlaneConfig `mapstructure:"controlplane"`
	// Ticker holds the timing every node's heartbeat runs on.
	Ticker task.Config `mapstructure:"ticker"`
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
	if _, err := ParseMode(string(c.Mode)); err != nil {
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
		return nil
	}
	if err := c.DataPlane.Validate(); err != nil {
		return fmt.Errorf("sequence config: %w", err)
	}
	if err := c.Ticker.Validate(); err != nil {
		return fmt.Errorf("sequence config: %w", err)
	}
	return c.validateTickerCoupling()
}

// validateTickerCoupling checks what ties the node's heartbeat to the ticker
// that drives it.
//
// Both values are owned by different packages, so neither can check the pair by
// itself, and both rules have a fleet-visible failure: a heartbeat timeout at or
// below the heartbeat interval pauses every node that hiccups, and a liveness TTL
// at or below the period drops a node from the fleet between its own renewals.
func (c Config) validateTickerCoupling() error {
	if c.DataPlane.Node.HeartbeatTimeoutTicks <= c.Ticker.HeartbeatTicks {
		return errors.New("sequence config: heartbeat timeout must exceed heartbeat interval")
	}
	period := time.Duration(c.Ticker.HeartbeatTicks) * c.Ticker.BaseTickInterval
	if c.DataPlane.HA.NodeTTL <= period {
		return fmt.Errorf(
			"sequence config: dataplane.ha.node_ttl (%s) must exceed the heartbeat "+
				"period ticker.heartbeat_ticks * ticker.base_tick_interval (%s)",
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

// ParseMode reads a startup mode, strictly. A value that is not one of the
// three shapes is refused rather than defaulted: starting the wrong shape is
// silent -- a control process that never publishes looks healthy until clients
// stop refreshing -- so the only safe answer is to fail the start.
func ParseMode(value string) (Mode, error) {
	switch mode := Mode(strings.TrimSpace(value)); mode {
	case ModeData, ModeControl, ModeBoth:
		return mode, nil
	default:
		return "", fmt.Errorf("mode must be one of data|control|both, got %q", value)
	}
}

// ErrModeValue is returned when --mode is present without a value. It is a
// startup error rather than a fallback to the configured mode: a process whose
// shape is ambiguous must not pick one silently.
var ErrModeValue = errors.New("--mode requires a value")

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
				return "", true, ErrModeValue
			}
			index++
			value = args[index]
		}
		value = strings.TrimSpace(value)
		if value == "" {
			return "", true, ErrModeValue
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
	mode, err := ParseMode(string(c.Mode))
	if err != nil {
		return fmt.Errorf("sequence config: %w", err)
	}
	c.Mode = mode
	return nil
}
