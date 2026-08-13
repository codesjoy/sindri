package metrics

import (
	"context"
	"errors"
	"math"

	"github.com/codesjoy/skuld/internal/sequence/biz"
	"go.opentelemetry.io/otel/metric"
)

// Metrics owns the metric callback registration.
type Metrics struct {
	registration metric.Registration
}

// NewMetrics registers observable metrics.
func NewMetrics(
	meter metric.Meter,
	allocator *biz.Allocator,
	memorySampler biz.MemorySampler,
) (*Metrics, error) {
	if meter == nil || allocator == nil || memorySampler == nil {
		return nil, errors.New(
			"sequence allocator metrics: meter, allocator, and memory sampler are required",
		)
	}
	cachedKeys, err := meter.Int64ObservableGauge(
		"sequence.allocator.cached_keys",
		metric.WithUnit("{key}"),
	)
	if err != nil {
		return nil, err
	}
	managedBytes, err := meter.Int64ObservableGauge(
		"sequence.allocator.runtime.managed_memory",
		metric.WithUnit("By"),
	)
	if err != nil {
		return nil, err
	}
	memoryLimit, err := meter.Int64ObservableGauge(
		"sequence.allocator.runtime.memory_limit",
		metric.WithUnit("By"),
	)
	if err != nil {
		return nil, err
	}
	memoryUtilization, err := meter.Float64ObservableGauge(
		"sequence.allocator.runtime.memory_utilization",
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, err
	}
	admissionRejected, err := meter.Int64ObservableCounter(
		"sequence.allocator.admission_rejected",
		metric.WithUnit("{request}"),
	)
	if err != nil {
		return nil, err
	}
	cleanupScanned, err := meter.Int64ObservableCounter(
		"sequence.allocator.cleanup_scanned",
		metric.WithUnit("{key}"),
	)
	if err != nil {
		return nil, err
	}
	cleanupEvicted, err := meter.Int64ObservableCounter(
		"sequence.allocator.cleanup_evicted",
		metric.WithUnit("{key}"),
	)
	if err != nil {
		return nil, err
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
			return nil
		},
		cachedKeys,
		managedBytes,
		memoryLimit,
		memoryUtilization,
		admissionRejected,
		cleanupScanned,
		cleanupEvicted,
	)
	if err != nil {
		return nil, err
	}
	return &Metrics{registration: registration}, nil
}

// Close unregisters the allocator callback.
func (m *Metrics) Close() error {
	if m == nil || m.registration == nil {
		return nil
	}
	return m.registration.Unregister()
}
