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
	"testing"
	"time"

	testkit "github.com/codesjoy/sindri/internal/pkg/tests"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// assertedDataPlane is a data plane configuration that satisfies every
// requirement: the platform assertions the ordering argument rests on, and a
// quiet window above the floor. Individual tests relax exactly one field from
// here.
func assertedDataPlane() DataPlaneConfig {
	cfg := DataPlaneConfig{Node: NodeConfig{ID: "node-a"}}
	if err := testkit.DecodeDefaults(&cfg); err != nil {
		panic(err)
	}
	cfg.HA.PauseVerified = true
	cfg.HA.ClockDisciplined = true
	cfg.HA.QuietWindow = 30 * time.Second
	return cfg
}

// TestFrameworkDefaultsFillTheDataPlane pins the values every process needs to
// serve at all: the range sizing a key starts from, the ownership bounds, and
// the node's heartbeat.
//
// The defaults live on the fields as default tags, so this test is what keeps
// them from drifting away from the values the deployment documents.
func TestFrameworkDefaultsFillTheDataPlane(t *testing.T) {
	var plane DataPlaneConfig
	require.NoError(t, testkit.DecodeDefaults(&plane))

	assert.Equal(t, int64(100), plane.Allocator.DefaultStep)
	assert.Equal(t, int64(10000), plane.Allocator.MaxStep)
	assert.Equal(t, 0.5, plane.Allocator.PrefetchRatio)
	assert.Equal(t, 4.0, plane.Allocator.PrefetchLatencyMultiplier)
	assert.Equal(t, 5*time.Minute, plane.Allocator.PrefetchLatencyWindow)
	assert.Equal(t, 100, plane.Allocator.PrefetchLatencyMinSamples)
	assert.Equal(t, time.Minute, plane.Allocator.PrefetchRateResetAfter)
	assert.Equal(t, 15*time.Minute, plane.Allocator.StepIncreaseThreshold)
	assert.Equal(t, 30*time.Minute, plane.Allocator.StepDecreaseThreshold)
	assert.Equal(t, time.Second, plane.Allocator.ReserveTimeout)
	assert.Equal(t, 24*time.Hour, plane.Allocator.IdleTimeout)
	assert.Equal(t, time.Second, plane.Allocator.CleanupInterval)
	assert.Equal(t, 64, plane.Allocator.CleanupSlotsPerRun)
	assert.Equal(t, 0.9, plane.Allocator.MemoryHighWatermarkRatio)
	assert.Equal(t, 5*time.Second, plane.HA.QuietWindow)
	assert.Equal(t, 3*time.Second, plane.HA.LeaseDuration)
	assert.Equal(t, time.Second, plane.HA.MaxPause)
	assert.Equal(t, 100*time.Millisecond, plane.HA.ClockDrift)
	assert.Equal(t, 100*time.Millisecond, plane.HA.ClockJump)
	assert.Equal(t, 500*time.Millisecond, plane.HA.SafetyMargin)
	assert.Equal(t, time.Second, plane.HA.RenewInterval)
	assert.Equal(t, 2*time.Second, plane.HA.ReleaseDrainTimeout)
	assert.Equal(t, 15*time.Second, plane.HA.NodeTTL)
	assert.Equal(t, time.Second, plane.Node.HeartbeatInterval)
	assert.Equal(t, time.Second, plane.Node.RouteQueryTimeout)
}

// TestStatedZerosDecodeToTheDefaults is the framework rule as a test: the
// default tags are applied after decoding, so a key the deployment stated as
// zero is indistinguishable from one it omitted.
func TestStatedZerosDecodeToTheDefaults(t *testing.T) {
	var plane DataPlaneConfig
	require.NoError(t, testkit.Decode(&plane, map[string]any{
		"allocator": map[string]any{"prefetch_ratio": 0},
		"ha":        map[string]any{"lease_duration": 0, "node_ttl": 0},
		"node":      map[string]any{"heartbeat_interval": 0},
	}))

	assert.Equal(t, 0.5, plane.Allocator.PrefetchRatio)
	assert.Equal(t, 3*time.Second, plane.HA.LeaseDuration)
	assert.Equal(t, 15*time.Second, plane.HA.NodeTTL)
	assert.Equal(t, time.Second, plane.Node.HeartbeatInterval)
}

// TestAllocatorReadsTheBoundsFromTheDataPlane pins that the allocator keeps no
// copy of the HA section: the bounds it enforces are the ones on the plane it
// was built from.
func TestAllocatorReadsTheBoundsFromTheDataPlane(t *testing.T) {
	plane := assertedDataPlane()
	plane.Node.ID = "node-a"
	plane.HA.QuietWindow = 7 * time.Second

	allocator := NewAllocator(
		plane,
		&rangeStore{max: map[string]int64{}},
		newLeaseOwnershipFake(),
		unlimitedMemorySampler,
		nil,
	)

	assert.Equal(t, 7*time.Second, allocator.QuietWindow())
	assert.Equal(t, "node-a", allocator.nodeID)
	assert.Equal(t, plane.HA, allocator.ha)
}

// TestDataPlaneRequiresANodeIdentity covers the values the plane cannot plan
// without: a node id is the identity slots are claimed under, and the route
// query bound is what keeps one slow read from pausing a healthy node.
func TestDataPlaneRequiresANodeIdentity(t *testing.T) {
	cfg := assertedDataPlane()
	cfg.Node.ID = ""
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "node.id")

	cfg = assertedDataPlane()
	cfg.Node.RouteQueryTimeout = 0
	err = cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "route_query_timeout")
}

// TestAssertedPlatformIsAccepted is the positive case: the platform assertions
// the ordering argument needs, together with a quiet window above the floor,
// make a configuration that runs.
func TestAssertedPlatformIsAccepted(t *testing.T) {
	cfg := assertedDataPlane()
	require.NoError(t, cfg.Validate())
	assert.Equal(t, 3*time.Second, cfg.HA.LeaseDuration)
	assert.Equal(t, time.Second, cfg.HA.RenewInterval)
}

// TestTheMeasuredPauseBoundIsRequired covers the first half of section 10.2:
// P_max cannot be derived from configuration or from the database, so the
// deployment is refused until it asserts the bound was measured.
func TestTheMeasuredPauseBoundIsRequired(t *testing.T) {
	cfg := assertedDataPlane()
	cfg.HA.PauseVerified = false
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pause_verified")
}

// TestClockDisciplineIsRequired covers the second half: nothing in PostgreSQL or
// MySQL bounds a forward jump of the storage host clock.
func TestClockDisciplineIsRequired(t *testing.T) {
	cfg := assertedDataPlane()
	cfg.HA.ClockDisciplined = false
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "clock_disciplined")
}

// TestQuietWindowFloorIsExact walks both sides of the hard lower bound, because
// an off-by-one here is the difference between a bounded takeover and a silent
// one.
func TestQuietWindowFloorIsExact(t *testing.T) {
	cfg := assertedDataPlane()
	floor := cfg.HA.QuietWindowFloor()
	assert.Equal(
		t,
		cfg.HA.LeaseDuration+cfg.HA.ClockDrift+cfg.HA.MaxPause+
			cfg.HA.ClockJump+cfg.HA.SafetyMargin,
		floor,
		"the floor must be L + delta + P_max + J_max + epsilon",
	)

	below := cfg
	below.HA.QuietWindow = floor - time.Millisecond
	err := below.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "quiet_window")

	exact := cfg
	exact.HA.QuietWindow = floor
	require.NoError(t, exact.Validate(), "the floor itself must be accepted")
}

// TestRenewIntervalMustFitInsideTheLease pins that a renewal cadence at or past
// the lease length cannot keep a healthy lease alive.
func TestRenewIntervalMustFitInsideTheLease(t *testing.T) {
	for _, interval := range []time.Duration{-time.Second, 10 * time.Second, 20 * time.Second} {
		t.Run(interval.String(), func(t *testing.T) {
			cfg := assertedDataPlane()
			cfg.HA.LeaseDuration = 10 * time.Second
			cfg.HA.RenewInterval = interval
			err := cfg.Validate()
			require.Error(t, err)
			assert.Contains(t, err.Error(), "renew_interval")
		})
	}
}

// TestLeaseDeadlineMustCoverARenewal pins the structural constraint the local
// lease rests on: a renewal is due a cadence after the last one and is one
// bounded storage statement, so both have to fit before the deadline the slot
// actually trusts expires. A lease that cannot cover one renewal would make a
// healthy owner stop serving part of every cycle.
func TestLeaseDeadlineMustCoverARenewal(t *testing.T) {
	cfg := assertedDataPlane()
	require.Greater(
		t,
		cfg.HA.LeaseDeadline(),
		cfg.HA.RenewInterval+cfg.Allocator.ReserveTimeout,
	)

	cfg.HA.LeaseDuration = cfg.HA.SafetyMargin +
		cfg.HA.RenewInterval + cfg.Allocator.ReserveTimeout
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "lease_duration")
}

// TestAZeroValueOnARequiredBoundIsRejected pins what a section handed a zero
// directly does: Validate still refuses it, because the default tags answer the
// configuration file, not a struct a caller built by hand.
func TestAZeroValueOnARequiredBoundIsRejected(t *testing.T) {
	zeroPause := assertedDataPlane()
	zeroPause.HA.MaxPause = 0
	require.ErrorContains(t, zeroPause.Validate(), "max_pause")

	zeroTTL := assertedDataPlane()
	zeroTTL.HA.NodeTTL = 0
	require.ErrorContains(t, zeroTTL.Validate(), "node_ttl")
}

// TestControlPlaneDefaultsAreThePublisherBounds pins the values a deployment
// that states nothing gets: the layout every node agrees on, the tenure that
// keeps one publisher in the fleet, and a cadence and pass bound inside it.
func TestControlPlaneDefaultsAreThePublisherBounds(t *testing.T) {
	var cfg ControlPlaneConfig
	require.NoError(t, testkit.DecodeDefaults(&cfg))

	assert.Equal(t, int64(1), cfg.LayoutVersion)
	assert.Equal(t, 10*time.Second, cfg.CoordinatorLease)
	assert.Equal(t, 5*time.Second, cfg.ReconcileInterval)
	assert.Equal(t, 3*time.Second, cfg.PassTimeout)
	require.NoError(t, cfg.Validate())
}

// TestControlPlaneStatedZeroTakesTheDefault keeps the control plane on the same
// framework rule as the data plane.
func TestControlPlaneStatedZeroTakesTheDefault(t *testing.T) {
	var cfg ControlPlaneConfig
	require.NoError(t, testkit.Decode(&cfg, map[string]any{
		"coordinator_lease":  0,
		"reconcile_interval": 0,
		"pass_timeout":       0,
	}))

	assert.Equal(t, 10*time.Second, cfg.CoordinatorLease)
	assert.Equal(t, 5*time.Second, cfg.ReconcileInterval)
	assert.Equal(t, 3*time.Second, cfg.PassTimeout)
	require.NoError(t, cfg.Validate())
}

// TestControlPlaneRejectsAPassThatOutlivesItsLease is the single-writer rule as
// a validation: a pass may not run past the tenure it acquired, because another
// replica may hold the role by then.
func TestControlPlaneRejectsAPassThatOutlivesItsLease(t *testing.T) {
	var cfg ControlPlaneConfig
	require.NoError(t, testkit.DecodeDefaults(&cfg))
	cfg.PassTimeout = cfg.CoordinatorLease
	require.ErrorContains(t, cfg.Validate(), "pass_timeout")
}

// TestControlPlaneRejectsACadenceThatOutlivesTheLease pins the other side of
// the same rule: a replica that does not wake inside its tenure stops
// publishing long before the election would move the role.
func TestControlPlaneRejectsACadenceThatOutlivesTheLease(t *testing.T) {
	var cfg ControlPlaneConfig
	require.NoError(t, testkit.DecodeDefaults(&cfg))
	cfg.ReconcileInterval = cfg.CoordinatorLease
	require.ErrorContains(t, cfg.Validate(), "reconcile_interval")
}
