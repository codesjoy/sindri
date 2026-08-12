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
	cfg.SetDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// SetDefaults applies process defaults before the configuration becomes immutable.
func (c *Config) SetDefaults() {
	c.Database.Driver = strings.TrimSpace(c.Database.Driver)
	c.Database.DSN = strings.TrimSpace(c.Database.DSN)
	c.Database.ExpectedDatabase = strings.TrimSpace(c.Database.ExpectedDatabase)
	c.Database.ExpectedAccount = strings.TrimSpace(c.Database.ExpectedAccount)
	c.Database.SetDefaults()
	c.Node.ID = strings.TrimSpace(c.Node.ID)
	if c.Allocator.Step == 0 {
		c.Allocator.Step = biz.DefaultStep
	}
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
	if c.Allocator.Step < biz.MinStep || c.Allocator.Step > biz.MaxStep {
		return fmt.Errorf(
			"sequence config: allocator.step must be within %d..%d",
			biz.MinStep,
			biz.MaxStep,
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
