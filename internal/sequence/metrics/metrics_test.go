package metrics

import (
	"context"
	"log/slog"
	"testing"

	"github.com/codesjoy/skuld/internal/sequence/biz"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

type testSequenceRepo struct{}

func (testSequenceRepo) ReserveRange(
	context.Context,
	string,
	int64,
) (biz.SequenceRange, error) {
	return biz.SequenceRange{Start: 1, End: 10}, nil
}

type testMemorySampler struct {
	managed uint64
	limit   uint64
}

func (s *testMemorySampler) MemoryUsage() (uint64, uint64) {
	return s.managed, s.limit
}

func TestAllocatorMetricsCollectsRuntimeAndAllocatorValues(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })
	sampler := &testMemorySampler{managed: 45, limit: 100}
	allocator := biz.NewAllocator(
		&biz.AllocatorConfig{DefaultStep: 10, MaxStep: 100},
		testSequenceRepo{},
		sampler,
		slog.Default(),
	)

	metrics, err := NewMetrics(provider.Meter("test"), allocator, sampler)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, metrics.Close()) })

	var resourceMetrics metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &resourceMetrics))
	require.Len(t, resourceMetrics.ScopeMetrics, 1)

	values := make(map[string]float64)
	for _, metric := range resourceMetrics.ScopeMetrics[0].Metrics {
		switch data := metric.Data.(type) {
		case metricdata.Gauge[int64]:
			require.Len(t, data.DataPoints, 1)
			values[metric.Name] = float64(data.DataPoints[0].Value)
		case metricdata.Gauge[float64]:
			require.Len(t, data.DataPoints, 1)
			values[metric.Name] = data.DataPoints[0].Value
		case metricdata.Sum[int64]:
			require.Len(t, data.DataPoints, 1)
			values[metric.Name] = float64(data.DataPoints[0].Value)
		}
	}

	assert.Equal(t, float64(0), values["sequence.allocator.cached_keys"])
	assert.Equal(t, float64(45), values["sequence.allocator.runtime.managed_memory"])
	assert.Equal(t, float64(100), values["sequence.allocator.runtime.memory_limit"])
	assert.InDelta(t, 0.45, values["sequence.allocator.runtime.memory_utilization"], 0.0001)
	assert.Equal(t, float64(0), values["sequence.allocator.admission_rejected"])
	assert.Equal(t, float64(0), values["sequence.allocator.cleanup_scanned"])
	assert.Equal(t, float64(0), values["sequence.allocator.cleanup_evicted"])
}

func TestAllocatorMetricsLifecycle(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })
	sampler := &testMemorySampler{managed: 1, limit: 100}
	allocator := biz.NewAllocator(
		&biz.AllocatorConfig{DefaultStep: 10, MaxStep: 100},
		testSequenceRepo{},
		sampler,
		slog.Default(),
	)
	metrics, err := NewMetrics(provider.Meter("test"), allocator, sampler)
	require.NoError(t, err)
	require.NoError(t, metrics.Close())
	require.NoError(t, metrics.Close())
}

func TestAllocatorMetricsRejectsMissingDependencies(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })
	sampler := &testMemorySampler{managed: 1, limit: 100}
	allocator := biz.NewAllocator(
		&biz.AllocatorConfig{DefaultStep: 10, MaxStep: 100},
		testSequenceRepo{},
		sampler,
		slog.Default(),
	)

	tests := []struct {
		name      string
		meter     metric.Meter
		allocator *biz.Allocator
		sampler   biz.MemorySampler
	}{
		{name: "meter", allocator: allocator, sampler: sampler},
		{name: "allocator", meter: provider.Meter("test"), sampler: sampler},
		{name: "sampler", meter: provider.Meter("test"), allocator: allocator},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewMetrics(test.meter, test.allocator, test.sampler)
			require.Error(t, err)
		})
	}
}
