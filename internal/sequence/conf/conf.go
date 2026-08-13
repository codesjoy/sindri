package conf

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/codesjoy/skuld/internal/pkg/xgorm"
	"github.com/codesjoy/skuld/internal/sequence/biz"
	sequencemetrics "github.com/codesjoy/skuld/internal/sequence/metrics"
	"github.com/codesjoy/skuld/internal/sequence/task"
	"github.com/codesjoy/yggdrasil/v3"
)

const (
	sequenceOwner = "skuld_sequence"
	// DefaultMemoryLimit enables cgroup-aware automatic memory sizing.
	DefaultMemoryLimit = "auto"
	// DefaultAutoMemoryLimitRatio leaves headroom outside Go-managed memory.
	DefaultAutoMemoryLimitRatio = 0.8
)

// RuntimeConfig controls process-wide Go runtime settings.
type RuntimeConfig struct {
	MemoryLimit          string  `mapstructure:"memory_limit"`
	AutoMemoryLimitRatio float64 `mapstructure:"auto_memory_limit_ratio"`

	memoryLimitExplicit bool
}

// MemoryLimitExplicit reports whether memory_limit was present in configuration.
func (c RuntimeConfig) MemoryLimitExplicit() bool {
	return c.memoryLimitExplicit
}

// Config is the immutable process-level sequence configuration.
type Config struct {
	Database  xgorm.Config        `mapstructure:"database"`
	Allocator biz.AllocatorConfig `mapstructure:"allocator"`
	Node      biz.NodeConfig      `mapstructure:"node"`
	Runtime   RuntimeConfig       `mapstructure:"runtime"`
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
	runtimeSection := section.Section("runtime")
	var runtimeConfig RuntimeConfig
	if err := runtimeSection.Decode(&runtimeConfig); err != nil {
		return nil, fmt.Errorf("decode sequence runtime config: %w", err)
	}
	cfg.Runtime = runtimeConfig
	setAllocatorDefaults(&cfg.Allocator, section.Section("allocator").Map())
	setRuntimeDefaults(&cfg.Runtime, runtimeSection.Map())
	cfg.SetDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func setRuntimeDefaults(cfg *RuntimeConfig, values map[string]any) {
	_, cfg.memoryLimitExplicit = values["memory_limit"]
	if !cfg.memoryLimitExplicit {
		cfg.MemoryLimit = DefaultMemoryLimit
	}
	if _, ok := values["auto_memory_limit_ratio"]; !ok {
		cfg.AutoMemoryLimitRatio = DefaultAutoMemoryLimitRatio
	}
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
	if _, ok := values["idle_timeout"]; !ok {
		cfg.IdleTimeout = biz.DefaultIdleTimeout
	}
	if _, ok := values["cleanup_interval"]; !ok {
		cfg.CleanupInterval = biz.DefaultCleanupInterval
	}
	if _, ok := values["cleanup_slots_per_run"]; !ok {
		cfg.CleanupSlotsPerRun = biz.DefaultCleanupSlotsPerRun
	}
	if _, ok := values["memory_high_watermark_ratio"]; !ok {
		cfg.MemoryHighWatermarkRatio = biz.DefaultMemoryHighWatermarkRatio
	}
}

// SetDefaults applies process defaults before the configuration becomes immutable.
func (c *Config) SetDefaults() {
	c.Database.Driver = strings.TrimSpace(c.Database.Driver)
	c.Database.DSN = strings.TrimSpace(c.Database.DSN)
	c.Database.ExpectedDatabase = strings.TrimSpace(c.Database.ExpectedDatabase)
	c.Database.ExpectedAccount = strings.TrimSpace(c.Database.ExpectedAccount)
	c.Database.SetDefaults()
	runtimeUnset := !c.Runtime.memoryLimitExplicit && strings.TrimSpace(c.Runtime.MemoryLimit) == ""
	if runtimeUnset {
		c.Runtime.MemoryLimit = DefaultMemoryLimit
		if c.Runtime.AutoMemoryLimitRatio == 0 {
			c.Runtime.AutoMemoryLimitRatio = DefaultAutoMemoryLimitRatio
		}
	}
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
	if c.Allocator.IdleTimeout <= 0 {
		return errors.New("sequence config: allocator.idle_timeout must be positive")
	}
	if c.Allocator.CleanupInterval <= 0 {
		return errors.New("sequence config: allocator.cleanup_interval must be positive")
	}
	if c.Allocator.CleanupSlotsPerRun <= 0 || c.Allocator.CleanupSlotsPerRun > biz.SlotCount {
		return fmt.Errorf(
			"sequence config: allocator.cleanup_slots_per_run must be within 1..%d",
			biz.SlotCount,
		)
	}
	if c.Allocator.MemoryHighWatermarkRatio <= 0 ||
		c.Allocator.MemoryHighWatermarkRatio >= 1 {
		return errors.New(
			"sequence config: allocator.memory_high_watermark_ratio must be within (0,1)",
		)
	}
	if c.Allocator.IdleTimeout < c.Allocator.CleanupInterval {
		return errors.New(
			"sequence config: allocator.idle_timeout must not be less than cleanup_interval",
		)
	}
	memoryLimit := strings.TrimSpace(c.Runtime.MemoryLimit)
	if c.Runtime.memoryLimitExplicit && memoryLimit == "" {
		return errors.New("sequence config: runtime.memory_limit must not be empty")
	}
	if memoryLimit != DefaultMemoryLimit {
		limit, err := sequencemetrics.ParseMemoryLimit(memoryLimit)
		if err != nil {
			return fmt.Errorf("sequence config: runtime.memory_limit: %w", err)
		}
		if err := sequencemetrics.ValidateMemoryLimit(limit); err != nil {
			return fmt.Errorf("sequence config: runtime.memory_limit: %w", err)
		}
	} else if c.Runtime.AutoMemoryLimitRatio <= 0 || c.Runtime.AutoMemoryLimitRatio >= 1 {
		return errors.New(
			"sequence config: runtime.auto_memory_limit_ratio must be within (0,1)",
		)
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
