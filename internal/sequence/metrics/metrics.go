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
	"errors"
	"math"
	runtimemetrics "runtime/metrics"
	"time"

	"github.com/codesjoy/sindri/internal/sequence/biz"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Metrics owns the metric callback registration.
type Metrics struct {
	registration   metric.Registration
	haRegistration metric.Registration
	allocator      *biz.Allocator
	latency        metric.Float64Histogram
}

// NewMetrics registers observable metrics.
func NewMetrics(
	meter metric.Meter,
	allocator *biz.Allocator,
	memorySampler biz.MemorySampler,
	ha biz.HAConfig,
	clock *biz.StorageClockMonitor,
) (*Metrics, error) {
	if meter == nil || allocator == nil || memorySampler == nil || clock == nil {
		return nil, errors.New(
			"sequence allocator metrics: meter, allocator, memory sampler and " +
				"storage clock monitor are required",
		)
	}
	registrar := instrumentRegistrar{meter: meter}
	cachedKeys := registrar.int64Gauge("sequence.allocator.cached_keys", "{key}", "")
	managedBytes := registrar.int64Gauge(
		"sequence.allocator.runtime.managed_memory", "By", "",
	)
	memoryLimit := registrar.int64Gauge(
		"sequence.allocator.runtime.memory_limit", "By", "",
	)
	memoryUtilization := registrar.float64Gauge(
		"sequence.allocator.runtime.memory_utilization", "1", "",
	)
	admissionRejected := registrar.int64Counter(
		"sequence.allocator.admission_rejected", "{request}", "",
	)
	cleanupScanned := registrar.int64Counter(
		"sequence.allocator.cleanup_scanned", "{key}", "",
	)
	cleanupEvicted := registrar.int64Counter(
		"sequence.allocator.cleanup_evicted", "{key}", "",
	)
	prefetchStarted := registrar.int64Counter(
		"sequence.allocator.prefetch_started", "{request}", "",
	)
	prefetchSucceeded := registrar.int64Counter(
		"sequence.allocator.prefetch_succeeded", "{request}", "",
	)
	prefetchFailed := registrar.int64Counter(
		"sequence.allocator.prefetch_failed", "{request}", "",
	)
	prefetchRetries := registrar.int64Counter(
		"sequence.allocator.prefetch_retries", "{request}", "",
	)
	prefetchFallback := registrar.int64Counter(
		"sequence.allocator.prefetch_fallback", "{request}", "",
	)
	reserveLatencyP99 := registrar.float64Gauge(
		"sequence.allocator.reserve_latency_p99", "s", "",
	)
	reserveLatency := registrar.float64Histogram(
		"sequence.allocator.reserve_latency", "s", "",
	)
	renewalBatchDuration := registrar.float64Histogram(
		"sequence.dataplane.renewal_batch_duration_seconds", "s",
		"duration of one instance-lease renewal round trip",
	)
	renewalBatchSize := registrar.int64Histogram(
		"sequence.dataplane.renewal_batch_size", "{instance}",
		"instance rows updated by one renewal round trip",
	)
	if registrar.err != nil {
		return nil, registrar.err
	}

	registration, err := meter.RegisterCallback(
		func(_ context.Context, observer metric.Observer) error {
			stats := allocator.Stats()
			managed, limit := memorySampler.MemoryUsage()
			utilization := 0.0
			if limit > 0 && limit < math.MaxInt64 {
				utilization = float64(managed) / float64(limit)
			}
			observer.ObserveInt64(cachedKeys, stats.CachedKeys)
			observer.ObserveInt64(managedBytes, int64(managed))
			observer.ObserveInt64(memoryLimit, int64(limit))
			observer.ObserveFloat64(memoryUtilization, utilization)
			observer.ObserveInt64(admissionRejected, stats.AdmissionRejected)
			observer.ObserveInt64(cleanupScanned, stats.CleanupScanned)
			observer.ObserveInt64(cleanupEvicted, stats.CleanupEvicted)
			observer.ObserveInt64(prefetchStarted, stats.PrefetchStarted)
			observer.ObserveInt64(prefetchSucceeded, stats.PrefetchSucceeded)
			observer.ObserveInt64(prefetchFailed, stats.PrefetchFailed)
			observer.ObserveInt64(prefetchRetries, stats.PrefetchRetries)
			observer.ObserveInt64(prefetchFallback, stats.PrefetchFallback)
			observer.ObserveFloat64(
				reserveLatencyP99,
				stats.ReserveLatencyP99.Seconds(),
			)
			return nil
		},
		cachedKeys,
		managedBytes,
		memoryLimit,
		memoryUtilization,
		admissionRejected,
		cleanupScanned,
		cleanupEvicted,
		prefetchStarted,
		prefetchSucceeded,
		prefetchFailed,
		prefetchRetries,
		prefetchFallback,
		reserveLatencyP99,
	)
	if err != nil {
		return nil, err
	}
	haRegistration, err := registerHAMetrics(meter, allocator, ha, clock)
	if err != nil {
		// The allocator callback is already live; leaving it registered would
		// publish a partial view of the process.
		return nil, unregisterOnFailure(err, registration)
	}
	allocator.SetReserveLatencyObserver(
		func(duration time.Duration, _ bool) {
			reserveLatency.Record(context.Background(), duration.Seconds())
		},
	)
	allocator.SetRenewalBatchObserver(
		func(duration time.Duration, slots int) {
			renewalBatchDuration.Record(context.Background(), duration.Seconds())
			renewalBatchSize.Record(context.Background(), int64(slots))
		},
	)
	return &Metrics{
		registration:   registration,
		haRegistration: haRegistration,
		allocator:      allocator,
		latency:        reserveLatency,
	}, nil
}

// unregisterOnFailure rolls back the callbacks that did register before one of
// them failed, so a construction error leaves nothing of this process published,
// and reports the original failure -- the first unregister error would otherwise
// hide why the metrics were being torn down.
func unregisterOnFailure(cause error, registrations ...metric.Registration) error {
	for _, registration := range registrations {
		if registration == nil {
			continue
		}
		_ = registration.Unregister()
	}
	return cause
}

// Close unregisters every callback this process registered.
func (m *Metrics) Close() error {
	if m == nil {
		return nil
	}
	if m.allocator != nil {
		m.allocator.SetReserveLatencyObserver(nil)
		m.allocator.SetRenewalBatchObserver(nil)
	}
	var closeErr error
	for _, registration := range []metric.Registration{
		m.registration,
		m.haRegistration,
	} {
		if registration == nil {
			continue
		}
		if err := registration.Unregister(); err != nil && closeErr == nil {
			closeErr = err
		}
	}
	return closeErr
}

// RuntimeMemorySampler reads Go-managed memory and the configured Go memory limit.
type RuntimeMemorySampler struct{}

// NewRuntimeMemorySampler creates the process-wide runtime memory sampler.
func NewRuntimeMemorySampler() *RuntimeMemorySampler {
	return &RuntimeMemorySampler{}
}

// MemoryUsage returns runtime-managed bytes excluding released heap memory and GOMEMLIMIT.
func (*RuntimeMemorySampler) MemoryUsage() (managedBytes, limitBytes uint64) {
	samples := [3]runtimemetrics.Sample{
		{Name: "/memory/classes/total:bytes"},
		{Name: "/memory/classes/heap/released:bytes"},
		{Name: "/gc/gomemlimit:bytes"},
	}
	runtimemetrics.Read(samples[:])
	return managedMemory(
		samples[0].Value.Uint64(),
		samples[1].Value.Uint64(),
	), samples[2].Value.Uint64()
}

func managedMemory(total, released uint64) uint64 {
	if total <= released {
		return 0
	}
	return total - released
}

// PublisherMetrics owns the control-plane metric callback registrations.
//
// They are published under the sequence.control. namespace: the same family the
// data-plane series use, so one dashboard shows the whole fleet with the middle
// segment naming whose view a series is.
//
// The publisher holds no issuing authority, so nothing here is a safety signal.
// What these series answer is what the fleet's convergence depends on: whether a
// replica is passing at all, how much of the space nobody owns, and which
// directory revision it has published.
type PublisherMetrics struct {
	registration metric.Registration
	publisher    *biz.Publisher
}

// NewPublisherMetrics registers the directory publisher's observable counters.
//
// Every value comes from Publisher.Stats, which is the same surface the unit
// tests read, so a series cannot report something the publisher does not
// believe. Counters are counters and the revision is a gauge, because it can
// fall when a replica starts or restarts.
func NewPublisherMetrics(
	meter metric.Meter,
	publisher *biz.Publisher,
) (*PublisherMetrics, error) {
	if meter == nil || publisher == nil {
		return nil, errors.New("sequence publisher metrics: meter and publisher are required")
	}
	registrar := instrumentRegistrar{meter: meter}
	passes := registrar.int64Counter(
		"sequence.control.pass", "{pass}",
		"completed publish passes, by replica",
	)
	unownedSlots := registrar.int64Gauge(
		"sequence.control.unowned_slots", "{slot}",
		"slots nobody held at the last evaluated pass; the alert is how long "+
			"this stays above zero",
	)
	coordinatorLost := registrar.int64Counter(
		"sequence.control.coordinator_lost", "{pass}",
		"passes abandoned because another replica took the coordinator role",
	)
	revision := registrar.int64Gauge(
		"sequence.control.revision", "{revision}",
		"newest directory revision this replica published as coordinator; a "+
			"replica that is not the coordinator keeps the last value it "+
			"published, so the fleet's current revision is the maximum across "+
			"replicas",
	)
	viewSegments := registrar.int64Gauge(
		"sequence.control.ownership_view_segments", "{segment}",
		"runs of slots in the most recent compact ownership read; at steady "+
			"state this is O(live nodes), not O(slot count)",
	)
	payloadBytes := registrar.int64Gauge(
		"sequence.control.route_payload_bytes", "By",
		"encoded size of the most recent materialised route snapshot",
	)
	viewDuration := registrar.float64Histogram(
		"sequence.control.ownership_view_duration_seconds", "s",
		"duration of one compact ownership read",
	)
	if registrar.err != nil {
		return nil, registrar.err
	}

	registration, err := meter.RegisterCallback(
		func(_ context.Context, observer metric.Observer) error {
			stats := publisher.Stats()
			observer.ObserveInt64(passes, stats.Passes)
			observer.ObserveInt64(unownedSlots, stats.UnownedSlots)
			observer.ObserveInt64(coordinatorLost, stats.CoordinatorLost)
			observer.ObserveInt64(revision, stats.Revision)
			observer.ObserveInt64(viewSegments, stats.OwnershipViewSegments)
			observer.ObserveInt64(payloadBytes, stats.RoutePayloadBytes)
			return nil
		},
		passes,
		unownedSlots,
		coordinatorLost,
		revision,
		viewSegments,
		payloadBytes,
	)
	if err != nil {
		return nil, err
	}
	publisher.SetObserver(biz.PublisherObserver{
		OwnershipView: func(_ int, duration time.Duration) {
			viewDuration.Record(context.Background(), duration.Seconds())
		},
	})
	return &PublisherMetrics{registration: registration, publisher: publisher}, nil
}

// Close unregisters the publisher's callbacks.
func (m *PublisherMetrics) Close() error {
	if m == nil {
		return nil
	}
	if m.publisher != nil {
		m.publisher.SetObserver(biz.PublisherObserver{})
	}
	if m.registration == nil {
		return nil
	}
	return m.registration.Unregister()
}

// instrumentRegistrar creates observable instruments and latches the first
// failure, so a constructor states its instruments as one block and checks the
// outcome once. After a failure the remaining calls are no-ops, which keeps the
// original stop-at-the-first-error behavior without an error branch between
// every pair of instruments. An empty description is left off rather than
// registered as an empty string, so instruments that never had one keep their
// metadata unchanged.
type instrumentRegistrar struct {
	meter metric.Meter
	err   error
}

func (r *instrumentRegistrar) int64Gauge(
	name string,
	unit string,
	description string,
) metric.Int64ObservableGauge {
	if r.err != nil {
		return nil
	}
	options := []metric.Int64ObservableGaugeOption{metric.WithUnit(unit)}
	if description != "" {
		options = append(options, metric.WithDescription(description))
	}
	instrument, err := r.meter.Int64ObservableGauge(name, options...)
	if err != nil {
		r.err = err
	}
	return instrument
}

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
	clockRTT := registrar.float64Gauge(
		"sequence.ha.clock_sample_rtt_seconds",
		"s",
		"storage clock sampling round-trip time",
	)
	clockUncertain := registrar.int64Counter(
		"sequence.ha.clock_uncertain_samples",
		"{sample}",
		"clock samples that cannot establish a bound violation",
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
		clockRTT,
		clockUncertain,
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
			observer.ObserveInt64(inflight, allocator.Inflight())
			clockStats := clock.Stats()
			observer.ObserveFloat64(clockRTT, clockStats.SampleRTT.Seconds())
			observer.ObserveInt64(clockUncertain, clockStats.UncertainSamples)
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
	evictions := registrar.int64Counter(
		"sequence.ha.recording_evicted_keys",
		"{key}",
		"keys evicted from the bounded local allocation recorder",
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
		observer.ObserveInt64(evictions, current.EvictedKeys)
	}
	return write, []metric.Observable{
		staleDeliveries,
		duplicateDeliveries,
		orderViolations,
		evictions,
	}, nil
}

func (r *instrumentRegistrar) int64Counter(
	name string,
	unit string,
	description string,
) metric.Int64ObservableCounter {
	if r.err != nil {
		return nil
	}
	options := []metric.Int64ObservableCounterOption{metric.WithUnit(unit)}
	if description != "" {
		options = append(options, metric.WithDescription(description))
	}
	instrument, err := r.meter.Int64ObservableCounter(name, options...)
	if err != nil {
		r.err = err
	}
	return instrument
}

func (r *instrumentRegistrar) float64Gauge(
	name string,
	unit string,
	description string,
) metric.Float64ObservableGauge {
	if r.err != nil {
		return nil
	}
	options := []metric.Float64ObservableGaugeOption{metric.WithUnit(unit)}
	if description != "" {
		options = append(options, metric.WithDescription(description))
	}
	instrument, err := r.meter.Float64ObservableGauge(name, options...)
	if err != nil {
		r.err = err
	}
	return instrument
}

func (r *instrumentRegistrar) float64Histogram(
	name string,
	unit string,
	description string,
) metric.Float64Histogram {
	if r.err != nil {
		return nil
	}
	options := []metric.Float64HistogramOption{metric.WithUnit(unit)}
	if description != "" {
		options = append(options, metric.WithDescription(description))
	}
	instrument, err := r.meter.Float64Histogram(name, options...)
	if err != nil {
		r.err = err
	}
	return instrument
}

func (r *instrumentRegistrar) int64Histogram(
	name string,
	unit string,
	description string,
) metric.Int64Histogram {
	if r.err != nil {
		return nil
	}
	options := []metric.Int64HistogramOption{metric.WithUnit(unit)}
	if description != "" {
		options = append(options, metric.WithDescription(description))
	}
	instrument, err := r.meter.Int64Histogram(name, options...)
	if err != nil {
		r.err = err
	}
	return instrument
}
