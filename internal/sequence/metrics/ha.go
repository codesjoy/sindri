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

package metrics

import (
	"context"

	"github.com/codesjoy/sindri/internal/sequence/biz"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// registerHAMetrics publishes the high-availability bounds and the local-lease
// counters.
//
// The parameter gauges are the only place an operator can see the bounds a
// running process is actually using. They are published rather than left in the
// configuration because the bounds are the whole safety argument: a deployment
// whose W or P_max silently changed is claiming a guarantee it no longer has,
// and that is only visible if the live values are exported.
//
// Counters that section 6.3 requires to stay at zero, and the clock-drift and
// takeover counters, are deliberately absent: nothing measures them yet, and a
// permanently zero series reads as a passing check rather than a missing one.
func registerHAMetrics(
	meter metric.Meter,
	allocator *biz.Allocator,
	ha biz.HAConfig,
	clock *biz.StorageClockMonitor,
) (metric.Registration, error) {
	registrar := instrumentRegistrar{meter: meter}
	quietWindow := registrar.float64Gauge(
		"sequence.ha.quiet_window", "s",
		"W, the silent-takeover window in force",
	)
	leaseDuration := registrar.float64Gauge(
		"sequence.ha.lease_duration", "s",
		"L, the local lease length in force",
	)
	maxPause := registrar.float64Gauge(
		"sequence.ha.max_pause", "s",
		"P_max, the process pause bound in force",
	)
	clockDrift := registrar.float64Gauge(
		"sequence.ha.clock_drift_seconds", "s",
		"difference between the storage clock's elapsed time and the local clock's",
	)
	clockJump := registrar.float64Gauge(
		"sequence.ha.storage_clock_forward_jump_seconds", "s",
		"largest forward jump of the storage clock seen; this is the only "+
			"evidence J_max leaves",
	)
	inflight := registrar.int64Gauge(
		"sequence.allocator.inflight", "{request}",
		"allocations currently inside a slot gate",
	)
	leaseExpired := registrar.int64Counter(
		"sequence.ha.lease_expired", "{request}",
		"requests refused because the slot's local lease had lapsed and no "+
			"renewal had confirmed the grant yet",
	)
	pauseViolations := registrar.int64Counter(
		"sequence.ha.pause_violations", "{event}",
		"allocations discarded for exceeding the pause bound",
	)
	gateFenced := registrar.int64Counter(
		"sequence.ha.gate_fenced", "{slot}",
		"slots closed because their lease could not be confirmed",
	)
	renewal := registrar.int64Counter(
		"sequence.ha.renewal", "{attempt}",
		"lease renewal attempts by result",
	)
	takeover := registrar.int64Counter(
		"sequence.ha.takeover", "{slot}",
		"slot takeovers by outcome",
	)
	epochChanges := registrar.int64Counter(
		"sequence.ha.epoch_changes", "{slot}",
		"slots whose ownership epoch this instance moved",
	)
	release := registrar.int64Counter(
		"sequence.ha.release", "{slot}",
		"explicit releases by result",
	)
	drains := registrar.int64Counter(
		"sequence.ha.drain", "{pass}",
		"drain-and-release passes",
	)
	drainSeconds := registrar.float64Gauge(
		"sequence.ha.drain_seconds", "s",
		"how long the most recent drain took",
	)
	pauseSeconds := registrar.float64Gauge(
		"sequence.ha.pause_seconds", "s",
		"the most recent in-memory linearisation interval, which P_max bounds",
	)
	slotState := registrar.int64Gauge(
		"sequence.ha.slot_state", "{slot}",
		"slots this instance holds, by state",
	)
	if registrar.err != nil {
		return nil, registrar.err
	}

	// The three counters that must stay at zero exist only when the allocation
	// record is on. Registering them unconditionally would publish a series that
	// reads zero because nothing measured it, which reads as a passing check
	// rather than a missing one -- the opposite of what they are for.
	linearization := make([]metric.Observable, 0, 3)
	writeCounters, linearizationCounters, countersErr := registerLinearizationCounters(
		allocator,
		&registrar,
	)
	if countersErr != nil {
		return nil, countersErr
	}
	linearization = append(linearization, linearizationCounters...)

	succeeded := attribute.String("result", "succeeded")
	failed := attribute.String("result", "failed")
	granted := attribute.String("outcome", "granted")
	refused := attribute.String("outcome", "refused")
	ownedState := attribute.String("state", "owned")
	fencedState := attribute.String("state", "fenced")

	instruments := append([]metric.Observable{
		quietWindow,
		leaseDuration,
		maxPause,
		clockDrift,
		clockJump,
		inflight,
		leaseExpired,
		pauseViolations,
		gateFenced,
		renewal,
		takeover,
		epochChanges,
		release,
		drains,
		drainSeconds,
		pauseSeconds,
		slotState,
	}, linearization...)
	registration, err := meter.RegisterCallback(
		func(_ context.Context, observer metric.Observer) error {
			stats := allocator.LeaseStats()
			observer.ObserveFloat64(quietWindow, ha.QuietWindow.Seconds())
			observer.ObserveFloat64(leaseDuration, ha.LeaseDuration.Seconds())
			observer.ObserveFloat64(maxPause, ha.MaxPause.Seconds())
			observer.ObserveInt64(inflight, allocator.InflightAllocations())
			clockStats := clock.Stats()
			observer.ObserveFloat64(clockDrift, clockStats.Drift.Seconds())
			observer.ObserveFloat64(clockJump, clockStats.ForwardJump.Seconds())
			observer.ObserveInt64(leaseExpired, stats.LeaseExpired)
			observer.ObserveInt64(pauseViolations, stats.PauseViolations)
			observer.ObserveInt64(gateFenced, stats.GateFenced)
			observer.ObserveInt64(
				renewal,
				stats.RenewalSucceeded,
				metric.WithAttributes(succeeded),
			)
			observer.ObserveInt64(
				renewal,
				stats.RenewalFailed,
				metric.WithAttributes(failed),
			)
			counters := allocator.HAStats()
			observer.ObserveInt64(
				takeover, counters.TakeoversGranted,
				metric.WithAttributes(granted),
			)
			observer.ObserveInt64(
				takeover, counters.TakeoversRefused,
				metric.WithAttributes(refused),
			)
			observer.ObserveInt64(epochChanges, counters.EpochChanges)
			observer.ObserveInt64(
				release, counters.ReleasesReleased,
				metric.WithAttributes(succeeded),
			)
			observer.ObserveInt64(
				release, counters.ReleasesFailed,
				metric.WithAttributes(failed),
			)
			observer.ObserveInt64(drains, counters.Drains)
			observer.ObserveFloat64(drainSeconds, counters.LastDrainSeconds)
			observer.ObserveFloat64(pauseSeconds, counters.LastPauseSeconds)
			owned, fenced := allocator.SlotStateCounts()
			observer.ObserveInt64(slotState, owned, metric.WithAttributes(ownedState))
			observer.ObserveInt64(slotState, fenced, metric.WithAttributes(fencedState))
			writeCounters(observer)
			return nil
		},
		instruments...,
	)
	if err != nil {
		return nil, err
	}
	return registration, nil
}

// registerLinearizationCounters publishes the appendix F.1 counters, and only
// when the allocation record that measures them is on.
func registerLinearizationCounters(
	allocator *biz.Allocator,
	registrar *instrumentRegistrar,
) (func(metric.Observer), []metric.Observable, error) {
	_, recording := allocator.LinearizationCounters()
	if !recording {
		return func(metric.Observer) {}, nil, nil
	}
	staleDeliveries := registrar.int64Counter(
		"sequence.ha.stale_delivery_total", "{delivery}",
		"allocations whose id was not above one a caller had already received; "+
			"must stay zero",
	)
	duplicateDeliveries := registrar.int64Counter(
		"sequence.ha.duplicate_delivery_total", "{delivery}",
		"allocations that repeated an id for a key; must stay zero",
	)
	orderViolations := registrar.int64Counter(
		"sequence.ha.allocation_order_violation_total", "{event}",
		"allocations that broke the ordering property; must stay zero",
	)
	if registrar.err != nil {
		return nil, nil, registrar.err
	}
	write := func(observer metric.Observer) {
		current, ok := allocator.LinearizationCounters()
		if !ok {
			return
		}
		observer.ObserveInt64(staleDeliveries, current.StaleDeliveries)
		observer.ObserveInt64(duplicateDeliveries, current.DuplicateDeliveries)
		observer.ObserveInt64(orderViolations, current.OrderViolations)
	}
	return write, []metric.Observable{
		staleDeliveries,
		duplicateDeliveries,
		orderViolations,
	}, nil
}
