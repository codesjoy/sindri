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

package biz

import (
	"errors"
	"fmt"
	"time"
)

// This file holds the data plane's configuration: range allocation, this node's
// heartbeat, and the ownership bounds both read.
//
// Defaults are declared on the fields rather than applied by a method. The
// framework's configuration snapshot calls defaults.Set after decoding, so a key
// the deployment omitted -- or stated as zero -- keeps the value on the field,
// and Validate is left to refuse only values that are wrong at any size. The
// three sections are read together: a slot claim is made under the node identity
// and the quiet window, and every node compares membership against the same
// stored timestamps.

// AllocatorConfig contains immutable range allocation settings.
type AllocatorConfig struct {
	// LegacyStep only detects the removed allocator.step configuration, so it
	// carries no default tag: the framework leaves the nil pointer alone.
	LegacyStep *int64 `mapstructure:"step"`

	// DefaultStep is the range size a key starts with and the floor the adaptive
	// sizer shrinks back to.
	DefaultStep int64 `mapstructure:"default_step" default:"100"`
	// MaxStep is the upper bound for dynamically sized ranges.
	MaxStep int64 `mapstructure:"max_step" default:"10000"`
	// PrefetchRatio reserves the next range at the fallback consumed watermark
	// while the adaptive rate estimate is unavailable.
	PrefetchRatio float64 `mapstructure:"prefetch_ratio" default:"0.5"`
	// PrefetchLatencyMultiplier reserves enough time for several reserve attempts.
	PrefetchLatencyMultiplier float64 `mapstructure:"prefetch_latency_multiplier" default:"4"`
	// PrefetchLatencyWindow bounds the age of reserve latency samples.
	PrefetchLatencyWindow time.Duration `mapstructure:"prefetch_latency_window" default:"5m"`
	// PrefetchLatencyMinSamples is the number of successful samples required
	// before the observed p99 replaces the configured reserve timeout.
	PrefetchLatencyMinSamples int `mapstructure:"prefetch_latency_min_samples" default:"100"`
	// PrefetchRateResetAfter resets a key's rate estimate after an idle gap.
	PrefetchRateResetAfter time.Duration `mapstructure:"prefetch_rate_reset_after" default:"1m"`
	// StepIncreaseThreshold grows ranges expected to be exhausted quickly.
	StepIncreaseThreshold time.Duration `mapstructure:"step_increase_threshold" default:"15m"`
	// StepDecreaseThreshold shrinks ranges expected to last a long time.
	StepDecreaseThreshold time.Duration `mapstructure:"step_decrease_threshold" default:"30m"`
	// ReserveTimeout bounds one storage statement: a range reservation, one
	// lease renewal, or one claim or release batch. It is deliberately not a
	// budget for a whole operation, because an operation can span many
	// statements and a shared bound is spent serially across them.
	ReserveTimeout time.Duration `mapstructure:"reserve_timeout" default:"1s"`
	// IdleTimeout evicts key state that has not allocated an ID for a day.
	IdleTimeout time.Duration `mapstructure:"idle_timeout" default:"24h"`
	// CleanupInterval advances incremental idle cleanup once per second.
	CleanupInterval time.Duration `mapstructure:"cleanup_interval" default:"1s"`
	// CleanupSlotsPerRun bounds routing slots scanned in one cleanup pass.
	CleanupSlotsPerRun int `mapstructure:"cleanup_slots_per_run" default:"64"`
	// MemoryHighWatermarkRatio leaves headroom below GOMEMLIMIT.
	MemoryHighWatermarkRatio float64 `mapstructure:"memory_high_watermark_ratio" default:"0.9"`
}

// Validate checks the allocator's range sizing and admission settings.
func (c AllocatorConfig) Validate() error {
	if c.DefaultStep < MinStep {
		return fmt.Errorf(
			"dataplane.allocator.default_step must be at least %d",
			MinStep,
		)
	}
	if c.MaxStep < c.DefaultStep {
		return errors.New(
			"dataplane.allocator.max_step must not be less than default_step",
		)
	}
	if c.PrefetchRatio <= 0 || c.PrefetchRatio >= 1 {
		return errors.New("dataplane.allocator.prefetch_ratio must be within (0,1)")
	}
	if c.PrefetchLatencyMultiplier <= 1 || c.PrefetchLatencyMultiplier > 10 {
		return errors.New(
			"dataplane.allocator.prefetch_latency_multiplier must be within (1,10]",
		)
	}
	if c.PrefetchLatencyWindow <= 0 {
		return errors.New(
			"dataplane.allocator.prefetch_latency_window must be positive",
		)
	}
	if c.PrefetchLatencyMinSamples <= 0 ||
		c.PrefetchLatencyMinSamples > MaxReserveLatencySamples {
		return fmt.Errorf(
			"dataplane.allocator.prefetch_latency_min_samples must be within 1..%d",
			MaxReserveLatencySamples,
		)
	}
	if c.PrefetchRateResetAfter <= 0 {
		return errors.New(
			"dataplane.allocator.prefetch_rate_reset_after must be positive",
		)
	}
	if c.StepIncreaseThreshold <= 0 {
		return errors.New(
			"dataplane.allocator.step_increase_threshold must be positive",
		)
	}
	if c.StepDecreaseThreshold <= c.StepIncreaseThreshold {
		return errors.New(
			"dataplane.allocator.step_decrease_threshold must exceed " +
				"step_increase_threshold",
		)
	}
	if c.ReserveTimeout <= 0 {
		return errors.New("dataplane.allocator.reserve_timeout must be positive")
	}
	if c.IdleTimeout <= 0 {
		return errors.New("dataplane.allocator.idle_timeout must be positive")
	}
	if c.CleanupInterval <= 0 {
		return errors.New("dataplane.allocator.cleanup_interval must be positive")
	}
	if c.CleanupSlotsPerRun <= 0 || c.CleanupSlotsPerRun > SlotCount {
		return fmt.Errorf(
			"dataplane.allocator.cleanup_slots_per_run must be within 1..%d",
			SlotCount,
		)
	}
	if c.MemoryHighWatermarkRatio <= 0 || c.MemoryHighWatermarkRatio >= 1 {
		return errors.New(
			"dataplane.allocator.memory_high_watermark_ratio must be within (0,1)",
		)
	}
	if c.IdleTimeout < c.CleanupInterval {
		return errors.New(
			"dataplane.allocator.idle_timeout must not be less than cleanup_interval",
		)
	}
	return nil
}

// NodeConfig contains node heartbeat and route polling settings.
type NodeConfig struct {
	// ID identifies this node in the ownership and liveness tables. It names a
	// position in the fleet rather than a process, so it is required.
	ID string `mapstructure:"id"`
	// HeartbeatTimeoutTicks is how many base ticks without a heartbeat pause
	// allocation. The ticker coupling that keeps it above the heartbeat interval
	// is checked by the configuration composer.
	HeartbeatInterval    time.Duration `mapstructure:"heartbeat_interval"     default:"1s"`
	RouteRefreshInterval time.Duration `mapstructure:"route_refresh_interval" default:"1s"`
	HandoffInterval      time.Duration `mapstructure:"handoff_interval"       default:"250ms"`
	// RouteQueryTimeout bounds one directory read on the heartbeat.
	RouteQueryTimeout time.Duration `mapstructure:"route_query_timeout" default:"1s"`
}

// Validate checks the node identity and polling bound.
func (c NodeConfig) Validate() error {
	if c.ID == "" || len(c.ID) > 256 {
		return errors.New("dataplane.node.id must contain 1..256 bytes")
	}
	if c.RouteQueryTimeout <= 0 {
		return errors.New("dataplane.node.route_query_timeout must be positive")
	}
	if c.HeartbeatInterval <= 0 || c.RouteRefreshInterval <= 0 || c.HandoffInterval <= 0 {
		return errors.New("dataplane.node intervals must be positive")
	}
	return nil
}

// HAConfig configures the shared strong-consistency ownership protocol.
//
// There is exactly one execution shape: the segment model, where steady-state
// allocation is served from memory and the storage is consulted only to reserve
// a range, renew a grant or take a slot over. The parameters below are what
// bound the one hazard that shape has -- a silent takeover racing a slow old
// owner.
type HAConfig struct {
	// LeaseDuration is L, the local lease length. It is the dominant term in the
	// quiet window's lower bound and therefore in crash RTO, so it is kept short;
	// the price is that a node tolerates only L - epsilon of storage outage
	// before it stops serving.
	LeaseDuration time.Duration `mapstructure:"lease_duration" default:"3s"`
	// QuietWindow is W, the window a candidate must wait before a silent
	// takeover. The storage clock adjudicates it.
	QuietWindow time.Duration `mapstructure:"quiet_window" default:"5s"`
	// MaxPause is P_max, the assumed hard bound on process pauses.
	MaxPause time.Duration `mapstructure:"max_pause" default:"1s"`
	// ClockDrift is delta, the assumed local-versus-storage drift.
	ClockDrift time.Duration `mapstructure:"clock_drift" default:"100ms"`
	// ClockJump is J_max, the bound on storage clock forward jumps.
	ClockJump time.Duration `mapstructure:"clock_jump" default:"100ms"`
	// SafetyMargin is epsilon, the extra margin in W and the room kept inside L.
	SafetyMargin time.Duration `mapstructure:"safety_margin" default:"500ms"`
	// RenewInterval is how often the instance lease is renewed.
	RenewInterval time.Duration `mapstructure:"renew_interval" default:"1s"`
	// ReleaseDrainTimeout bounds how long an explicit release waits for
	// in-flight allocations to reach zero.
	ReleaseDrainTimeout time.Duration `mapstructure:"release_drain_timeout" default:"2s"`
	// PauseVerified asserts that P_max has been measured on this platform. It
	// has no default: the bound cannot be derived from configuration or from the
	// database, so the deployment must state that it was measured.
	PauseVerified bool `mapstructure:"pause_verified"`
	// ClockDisciplined asserts that the storage clock is disciplined and
	// monitored: no forward jump beyond ClockJump, which is what bounds J_max in
	// practice. Like PauseVerified it has no default, because no database can
	// bound a forward jump of its own clock.
	ClockDisciplined bool `mapstructure:"clock_disciplined"`
	// LinearizationRecording turns on the appendix F.2 allocation record, which
	// is what lets a deployment check the ordering property from its own traffic.
	// It is off by default: the hot path does not carry a record per allocation
	// unless someone has asked for the evidence.
	LinearizationRecording bool `mapstructure:"linearization_recording"`
	// NodeTTL is how long a node counts as live after its last renewal. The
	// data plane reads it when it plans, because every node makes the same
	// membership comparison against the same stored timestamps.
	NodeTTL time.Duration `mapstructure:"node_ttl" default:"15s"`
}

// QuietWindowFloor is the section 3.3 hard lower bound:
// W >= L + delta + P_max + J_max + epsilon.
//
// The L term is the whole point: the old owner may keep serving from memory for
// up to L past its last confirmed grant, so a candidate that starts earlier than
// that can linearise its first id before the old owner's last one. The other
// terms cover what can widen that bound without the old owner observing it --
// process pause, drift between the local monotonic reading and the storage
// clock, and a storage clock forward jump.
//
// Every term is asserted by the operator (or, for delta and J_max, contracted
// from the storage) rather than measured here, so the bound is only as credible
// as the platform evidence behind those assertions.
func (c HAConfig) QuietWindowFloor() time.Duration {
	return c.LeaseDuration + c.ClockDrift + c.MaxPause +
		c.ClockJump + c.SafetyMargin
}

// LeaseDeadline is L - epsilon, the local deadline actually armed on a slot.
//
// It is derived rather than configured, and it must stay strictly inside the
// storage lease so a renewal is always attempted while the row is still valid.
// The allocator arms exactly this value; the validation below is what keeps a
// renewal able to complete before it passes.
func (c HAConfig) LeaseDeadline() time.Duration {
	return c.LeaseDuration - c.SafetyMargin
}

// ValidateAuthority checks the section 8.1 parameters and their hard lower
// bounds.
//
// The quiet window is the only thing standing between a process pause and a
// silent takeover, so section 3.3's hard lower bound is checked first. Section
// 10.2 is equally explicit that the platform cannot supply the clock terms on its
// own: neither P_max nor J_max can be derived from the database, so both must be
// asserted by whoever measured this deployment, or contracted from a storage
// that provides a monotonic commit timestamp.
func (c HAConfig) ValidateAuthority() error {
	if c.LeaseDuration <= 0 || c.MaxPause <= 0 || c.NodeTTL <= 0 {
		return errors.New("dataplane.ha lease_duration, max_pause and node_ttl must be positive")
	}
	if c.ClockDrift < 0 || c.ClockJump < 0 || c.SafetyMargin <= 0 {
		return errors.New(
			"dataplane.ha clock drift and jump bounds must not be negative and safety_margin must be positive",
		)
	}
	if minimum := c.QuietWindowFloor(); c.QuietWindow < minimum {
		return fmt.Errorf(
			"dataplane.ha.quiet_window must be at least "+
				"lease_duration + clock_drift + max_pause + clock_jump + "+
				"safety_margin (%s)",
			minimum,
		)
	}
	return nil
}

// Validate adds the assertions and scheduling bounds needed by a serving node.
func (c HAConfig) Validate() error {
	if err := c.ValidateAuthority(); err != nil {
		return err
	}
	if !c.PauseVerified {
		return errors.New(
			"dataplane.ha.pause_verified is required; the pause bound " +
				"cannot be derived from configuration or from the database, so it " +
				"must be measured on this platform first",
		)
	}
	if !c.ClockDisciplined {
		return errors.New(
			"dataplane.ha.clock_disciplined is required; no database can " +
				"bound a forward jump of its own clock, so the deployment must " +
				"assert that it is disciplined and monitored, or use a storage " +
				"whose transactions carry a monotonic commit timestamp",
		)
	}
	if c.RenewInterval <= 0 || c.RenewInterval >= c.LeaseDuration {
		return errors.New(
			"dataplane.ha.renew_interval must be within (0, lease_duration)",
		)
	}
	if c.ReleaseDrainTimeout <= 0 {
		return errors.New("dataplane.ha.release_drain_timeout must be positive")
	}
	return nil
}

// DataPlaneConfig is the data plane's own configuration.
type DataPlaneConfig struct {
	Allocator AllocatorConfig `mapstructure:"allocator"`
	Node      NodeConfig      `mapstructure:"node"`
	HA        HAConfig        `mapstructure:"ha"`
}

// Validate checks the data plane's parameters and the rules that cross its
// sections.
func (c DataPlaneConfig) Validate() error {
	if err := c.Allocator.Validate(); err != nil {
		return err
	}
	if err := c.Node.Validate(); err != nil {
		return err
	}
	if err := c.HA.Validate(); err != nil {
		return err
	}
	// The local deadline L - epsilon is what a slot actually trusts, and a
	// renewal that cannot finish inside it would leave the slot to expire on a
	// healthy owner: the renewal is due RenewInterval after the last one and is
	// one bounded statement, so both must fit before the deadline passes.
	if deadline := c.HA.LeaseDeadline(); deadline <= c.HA.RenewInterval+c.Allocator.ReserveTimeout {
		return fmt.Errorf(
			"dataplane: lease_duration - safety_margin (%s) must exceed "+
				"renew_interval + allocator.reserve_timeout (%s), or a renewal "+
				"cannot complete before the local deadline it has to beat",
			deadline,
			c.HA.RenewInterval+c.Allocator.ReserveTimeout,
		)
	}
	return nil
}

// ControlPlaneConfig configures the route publisher.
//
// It is the control half of the process: the layout the published directory is
// minted under, the coordinator lease that keeps one publisher in the fleet, and
// the cadence and bound of one publish pass. Nothing here decides placement --
// the nodes compute that themselves from the ownership and liveness tables.
type ControlPlaneConfig struct {
	Migration             MigrationConfig `mapstructure:"migration"`
	ActivePublishInterval time.Duration   `mapstructure:"active_publish_interval" default:"250ms"`
	// LayoutVersion identifies the slot layout the materialised route is minted
	// under. A deployment whose nodes disagree about it routes by a different
	// key-to-slot rule, so the value travels in every snapshot.
	LayoutVersion int64 `mapstructure:"layout_version" default:"1"`
	// CoordinatorLease is how long one replica keeps the publisher role.
	CoordinatorLease time.Duration `mapstructure:"coordinator_lease" default:"10s"`
	// ReconcileInterval is how often the coordinator runs a publish pass. There
	// is no event channel to drive it: a pass reads the current authority state,
	// so the cadence is what bounds how long a change takes to reach the
	// directory.
	ReconcileInterval time.Duration `mapstructure:"reconcile_interval" default:"5s"`
	// PassTimeout bounds one pass. It must stay below CoordinatorLease: a pass
	// publishes under the tenure it acquired when it started, so one allowed to
	// outlive its lease could still be publishing while another replica had
	// become the coordinator.
	PassTimeout time.Duration `mapstructure:"pass_timeout" default:"3s"`
}

// Validate checks the publisher's parameters and their invariants.
func (c ControlPlaneConfig) Validate() error {
	if c.LayoutVersion <= 0 {
		return errors.New("controlplane.layout_version must be positive")
	}
	if c.ActivePublishInterval <= 0 || c.ActivePublishInterval > c.ReconcileInterval {
		return errors.New(
			"controlplane.active_publish_interval must be within (0,reconcile_interval]",
		)
	}
	if err := c.Migration.Validate(); err != nil {
		return err
	}
	if c.CoordinatorLease <= 0 {
		return errors.New("controlplane.coordinator_lease must be positive")
	}
	if c.ReconcileInterval <= 0 {
		return errors.New("controlplane.reconcile_interval must be positive")
	}
	if c.PassTimeout <= 0 {
		return errors.New("controlplane.pass_timeout must be positive")
	}
	// A pass that may outlive the lease it runs under would let two replicas
	// publish at once, which is the one thing the election exists to prevent.
	if c.ReconcileInterval >= c.CoordinatorLease {
		return errors.New(
			"controlplane.reconcile_interval must be shorter than " +
				"controlplane.coordinator_lease",
		)
	}
	if c.PassTimeout >= c.CoordinatorLease {
		return errors.New(
			"controlplane.pass_timeout must be shorter than " +
				"controlplane.coordinator_lease",
		)
	}
	return nil
}

// MigrationConfig bounds the control plane's rebalancing work.
type MigrationConfig struct {
	JoinStabilityWindow time.Duration `mapstructure:"join_stability_window" default:"15s"`
	BatchSlots          int           `mapstructure:"batch_slots"           default:"64"`
	MaxInflight         int           `mapstructure:"max_inflight"          default:"256"`
	MaxPerSource        int           `mapstructure:"max_per_source"        default:"64"`
	MaxPerTarget        int           `mapstructure:"max_per_target"        default:"64"`
	MaxPlanned          int           `mapstructure:"max_planned"           default:"1024"`
}

// Validate checks the migration limits against each other and the slot space.
func (c MigrationConfig) Validate() error {
	if c.JoinStabilityWindow <= 0 || c.BatchSlots <= 0 || c.BatchSlots > 1000 ||
		c.MaxInflight <= 0 ||
		c.MaxPerSource <= 0 ||
		c.MaxPerTarget <= 0 ||
		c.MaxPlanned < c.MaxInflight ||
		c.MaxPlanned > SlotCount ||
		c.BatchSlots > min(c.MaxInflight, c.MaxPerSource, c.MaxPerTarget) {
		return errors.New("controlplane.migration limits are invalid")
	}
	return nil
}
