package conf

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/codesjoy/skuld/internal/pkg/xgorm"
	"github.com/codesjoy/skuld/internal/sequence/biz"
	"github.com/codesjoy/skuld/internal/sequence/task"
	"github.com/codesjoy/yggdrasil/v3"
)

const sequenceOwner = "skuld_sequence"

// Config is the immutable process-level sequence configuration.
type Config struct {
	Database  xgorm.Config        `mapstructure:"database"`
	Allocator biz.AllocatorConfig `mapstructure:"allocator"`
	Node      biz.NodeConfig      `mapstructure:"node"`
	Ticker    task.Config         `mapstructure:"ticker"`
}

// Load decodes, defaults, and validates the sequence configuration.
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
	setAllocatorDefaults(&cfg.Allocator, section.Section("allocator").Map())
	cfg.SetDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func setAllocatorDefaults(cfg *biz.AllocatorConfig, values map[string]any) {
	if _, ok := values["default_step"]; !ok {
		cfg.DefaultStep = biz.DefaultStep
	}
	if _, ok := values["max_step"]; !ok {
		cfg.MaxStep = biz.DefaultMaxStep
	}
	if _, ok := values["prefetch_ratio"]; !ok {
		cfg.PrefetchRatio = biz.DefaultPrefetchRatio
	}
	if _, ok := values["step_increase_threshold"]; !ok {
		cfg.StepIncreaseThreshold = biz.DefaultStepIncreaseThreshold
	}
	if _, ok := values["step_decrease_threshold"]; !ok {
		cfg.StepDecreaseThreshold = biz.DefaultStepDecreaseThreshold
	}
	if _, ok := values["reserve_timeout"]; !ok {
		cfg.ReserveTimeout = biz.DefaultReserveTimeout
	}
}

// SetDefaults applies process defaults before the configuration becomes immutable.
func (c *Config) SetDefaults() {
	c.Database.Driver = strings.TrimSpace(c.Database.Driver)
	c.Database.DSN = strings.TrimSpace(c.Database.DSN)
	c.Database.ExpectedDatabase = strings.TrimSpace(c.Database.ExpectedDatabase)
	c.Database.ExpectedAccount = strings.TrimSpace(c.Database.ExpectedAccount)
	c.Database.SetDefaults()
	c.Node.ID = strings.TrimSpace(c.Node.ID)
	if c.Node.HeartbeatTimeoutTicks == 0 {
		c.Node.HeartbeatTimeoutTicks = 3
	}
	if c.Node.RouteQueryTimeout == 0 {
		c.Node.RouteQueryTimeout = time.Second
	}
	if c.Ticker.BaseTickInterval == 0 {
		c.Ticker.BaseTickInterval = time.Second
	}
	if c.Ticker.HeartbeatTicks == 0 {
		c.Ticker.HeartbeatTicks = 1
	}
}

// Validate checks module-local settings and their cross-module invariants.
func (c Config) Validate() error {
	if err := c.Database.Validate(); err != nil {
		return fmt.Errorf("sequence config: %w", err)
	}
	if c.Database.ExpectedDatabase != sequenceOwner || c.Database.ExpectedAccount != sequenceOwner {
		return fmt.Errorf("sequence config: database identity must be %s", sequenceOwner)
	}
	if c.Allocator.LegacyStep != nil {
		return errors.New(
			"sequence config: allocator.step was removed; use allocator.default_step",
		)
	}
	if c.Allocator.DefaultStep < biz.MinStep {
		return fmt.Errorf(
			"sequence config: allocator.default_step must be at least %d",
			biz.MinStep,
		)
	}
	if c.Allocator.MaxStep < c.Allocator.DefaultStep {
		return errors.New(
			"sequence config: allocator.max_step must not be less than default_step",
		)
	}
	if c.Allocator.PrefetchRatio <= 0 || c.Allocator.PrefetchRatio >= 1 {
		return errors.New("sequence config: allocator.prefetch_ratio must be within (0,1)")
	}
	if c.Allocator.StepIncreaseThreshold <= 0 {
		return errors.New(
			"sequence config: allocator.step_increase_threshold must be positive",
		)
	}
	if c.Allocator.StepDecreaseThreshold <= c.Allocator.StepIncreaseThreshold {
		return errors.New(
			"sequence config: allocator.step_decrease_threshold must exceed " +
				"step_increase_threshold",
		)
	}
	if c.Allocator.ReserveTimeout <= 0 {
		return errors.New("sequence config: allocator.reserve_timeout must be positive")
	}
	if c.Node.ID == "" || len(c.Node.ID) > 256 {
		return errors.New("sequence config: node.id must contain 1..256 bytes")
	}
	if c.Node.RouteQueryTimeout <= 0 {
		return errors.New("sequence config: node.route_query_timeout must be positive")
	}
	if c.Ticker.BaseTickInterval <= 0 || c.Ticker.HeartbeatTicks <= 0 {
		return errors.New("sequence config: ticker interval and heartbeat ticks must be positive")
	}
	if c.Node.HeartbeatTimeoutTicks <= c.Ticker.HeartbeatTicks {
		return errors.New("sequence config: heartbeat timeout must exceed heartbeat interval")
	}
	return nil
}
