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
	"log/slog"
	"testing"
	"time"

	testkit "github.com/codesjoy/sindri/internal/pkg/tests"
	"github.com/codesjoy/sindri/internal/sequence/biz"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// testDataPlane is a complete data plane configuration with the framework
// defaults applied, so a test that is not about the bounds can be sure the
// failure it sees comes from what it is testing.
func testDataPlane() biz.DataPlaneConfig {
	plane := biz.DataPlaneConfig{
		Allocator: biz.AllocatorConfig{DefaultStep: 10, MaxStep: 100},
		Node:      biz.NodeConfig{ID: "test-node"},
	}
	if err := testkit.DecodeDefaults(&plane); err != nil {
		panic(err)
	}
	plane.HA.MaxPause = time.Second
	return plane
}

type testSequenceRepo struct{}

func (testSequenceRepo) ReserveRanges(
	context.Context,
	biz.ReservationAuthority,
	[]biz.ReservationRequest,
) ([]biz.SequenceRange, error) {
	return []biz.SequenceRange{{Start: 1, End: 10}}, nil
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
		testDataPlane(),
		testSequenceRepo{},
		nil,
		sampler,
		slog.Default(),
	)

	metrics, err := NewMetrics(
		provider.Meter("test"),
		allocator,
		sampler,
		testDataPlane().HA,
		testStorageClockMonitor(),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, metrics.Close()) })

	var resourceMetrics metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &resourceMetrics))
	require.Len(t, resourceMetrics.ScopeMetrics, 1)

	values := make(map[string]float64)
	for _, metric := range resourceMetrics.ScopeMetrics[0].Metrics {
		switch data := metric.Data.(type) {
		case metricdata.Gauge[int64]:
			// A series may legitimately have more than one point, as slot_state
			// does with one per state, so the map holds the total and the
			// per-label split is asserted separately.
			require.NotEmpty(t, data.DataPoints)
			var total int64
			for _, point := range data.DataPoints {
				total += point.Value
			}
			values[metric.Name] = float64(total)
		case metricdata.Gauge[float64]:
			require.NotEmpty(t, data.DataPoints)
			var total float64
			for _, point := range data.DataPoints {
				total += point.Value
			}
			values[metric.Name] = total
		case metricdata.Sum[int64]:
			// A counter may be split across attribute values, so the total is
			// what this map holds; the per-label split is asserted separately.
			var total int64
			for _, point := range data.DataPoints {
				total += point.Value
			}
			values[metric.Name] = float64(total)
		}
	}

	assert.Equal(t, float64(0), values["sequence.allocator.cached_keys"])
	assert.Equal(t, float64(45), values["sequence.allocator.runtime.managed_memory"])
	assert.Equal(t, float64(100), values["sequence.allocator.runtime.memory_limit"])
	assert.InDelta(t, 0.45, values["sequence.allocator.runtime.memory_utilization"], 0.0001)
	assert.Equal(t, float64(0), values["sequence.allocator.admission_rejected"])
	assert.Equal(t, float64(0), values["sequence.allocator.cleanup_scanned"])
	assert.Equal(t, float64(0), values["sequence.allocator.cleanup_evicted"])
	assert.Equal(t, float64(0), values["sequence.allocator.prefetch_started"])
	assert.Equal(t, float64(0), values["sequence.allocator.prefetch_succeeded"])
	assert.Equal(t, float64(0), values["sequence.allocator.prefetch_failed"])
	assert.Equal(t, float64(0), values["sequence.allocator.prefetch_retries"])
	assert.Equal(t, float64(0), values["sequence.allocator.prefetch_fallback"])
	assert.Equal(t, float64(0), values["sequence.allocator.reserve_latency_p99"])
	assert.Equal(t, float64(0), values["sequence.allocator.inflight"])
	assert.Equal(t, float64(5), values["sequence.ha.quiet_window"])
	assert.Equal(t, float64(3), values["sequence.ha.lease_duration"])
	assert.Equal(t, float64(1), values["sequence.ha.max_pause"])
	assert.Equal(t, float64(0), values["sequence.ha.lease_expired"])
	assert.Equal(t, float64(0), values["sequence.ha.pause_violations"])
	assert.Equal(t, float64(0), values["sequence.ha.gate_fenced"])
	assert.Equal(t, float64(0), values["sequence.ha.renewal"])
}

func TestAllocatorMetricsLifecycle(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })
	sampler := &testMemorySampler{managed: 1, limit: 100}
	allocator := biz.NewAllocator(
		testDataPlane(),
		testSequenceRepo{},
		nil,
		sampler,
		slog.Default(),
	)
	metrics, err := NewMetrics(
		provider.Meter("test"),
		allocator,
		sampler,
		testDataPlane().HA,
		testStorageClockMonitor(),
	)
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
		testDataPlane(),
		testSequenceRepo{},
		nil,
		sampler,
		slog.Default(),
	)

	tests := []struct {
		name      string
		meter     metric.Meter
		allocator *biz.Allocator
		sampler   biz.MemorySampler
		ha        biz.HAConfig
	}{
		{name: "meter", allocator: allocator, sampler: sampler, ha: testDataPlane().HA},
		{
			name:    "allocator",
			meter:   provider.Meter("test"),
			sampler: sampler,
			ha:      testDataPlane().HA,
		},
		{
			name:      "sampler",
			meter:     provider.Meter("test"),
			allocator: allocator,
			ha:        testDataPlane().HA,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewMetrics(
				test.meter,
				test.allocator,
				test.sampler,
				test.ha,
				testStorageClockMonitor(),
			)
			require.Error(t, err)
		})
	}
}

// TestHASeriesCarryTheirLabels checks that the renewal counter is split by the
// label an alert would select on. A renewal counter that merged both results
// would be unusable for exactly the query it exists to answer.
func TestHASeriesCarryTheirLabels(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })
	sampler := &testMemorySampler{managed: 1, limit: 100}
	allocator := biz.NewAllocator(
		testDataPlane(),
		testSequenceRepo{},
		nil,
		sampler,
		slog.Default(),
	)
	ha := testDataPlane().HA
	metrics, err := NewMetrics(
		provider.Meter("test"),
		allocator,
		sampler,
		ha,
		testStorageClockMonitor(),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, metrics.Close()) })

	var resourceMetrics metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &resourceMetrics))
	require.Len(t, resourceMetrics.ScopeMetrics, 1)
	byName := make(map[string]metricdata.Metrics)
	for _, series := range resourceMetrics.ScopeMetrics[0].Metrics {
		byName[series.Name] = series
	}

	renewal, ok := byName["sequence.ha.renewal"].Data.(metricdata.Sum[int64])
	require.True(t, ok, "sequence.ha.renewal must be an int64 counter")
	results := make(map[string]int64, len(renewal.DataPoints))
	for _, point := range renewal.DataPoints {
		result, ok := point.Attributes.Value(attribute.Key("result"))
		require.True(t, ok, "every renewal series must carry its result")
		results[result.AsString()] = point.Value
	}
	require.Equal(t, map[string]int64{"succeeded": 0, "failed": 0}, results)
}

// testStorageClockMonitor is the monitor the metric tests publish from. It never
// observes anything, so its gauges read zero, which is what a fleet that has not
// compared its clocks yet reports.
func testStorageClockMonitor() *biz.StorageClockMonitor {
	return biz.NewStorageClockMonitor(
		time.Second,
		time.Second,
		time.Now,
		func(context.Context) (time.Time, error) { return time.Now().UTC(), nil },
	)
}

func TestManagedMemory(t *testing.T) {
	assert.Equal(t, uint64(70), managedMemory(100, 30))
	assert.Equal(t, uint64(0), managedMemory(30, 30))
	assert.Equal(t, uint64(0), managedMemory(30, 40))
}

func TestRuntimeMemorySampler(t *testing.T) {
	managed, limit := NewRuntimeMemorySampler().MemoryUsage()
	assert.Positive(t, managed)
	assert.Positive(t, limit)
}

// publisherMetricRepo answers a pass with a known view and a known directory, so
// the series can be asserted against values that came out of a real pass rather
// than against the zero value of a publisher nobody drove.
type publisherMetricRepo struct {
	segments []biz.OwnershipSegment
	revision int64
}

func (f *publisherMetricRepo) OwnershipSegments(
	context.Context,
	time.Duration,
) ([]biz.OwnershipSegment, error) {
	return f.segments, nil
}

func (f *publisherMetricRepo) MaterialiseRoute(
	context.Context,
	[]biz.OwnershipSegment,
	int64,
	int,
	biz.CoordinatorLease,
) (biz.PublishResult, error) {
	return biz.PublishResult{Revision: f.revision, PayloadBytes: 128}, nil
}

type publisherMetricCoordinator struct{}

func (publisherMetricCoordinator) AcquireCoordinator(
	_ context.Context,
	instanceID string,
	_ time.Duration,
) (biz.CoordinatorLease, error) {
	return biz.CoordinatorLease{Held: true, InstanceID: instanceID, Epoch: 1}, nil
}

// testPublisher is a publisher that has run the single pass the test drives.
func testPublisher(t *testing.T) *biz.Publisher {
	t.Helper()
	repo := &publisherMetricRepo{
		segments: []biz.OwnershipSegment{
			{
				StartSlot: 0, EndSlot: 0,
				OwnerNodeID: "node-a", OwnerInstanceID: "instance-a",
				State: biz.SlotOwned, Epoch: 1, GrantAgeKnown: true,
			},
			{StartSlot: 1, EndSlot: biz.SlotCount - 1, State: biz.SlotUnowned},
		},
		revision: 9,
	}
	publisher := biz.NewPublisher(
		biz.ControlPlaneConfig{
			LayoutVersion:     1,
			RouteRetention:    64,
			CoordinatorLease:  10 * time.Second,
			ReconcileInterval: 5 * time.Second,
			PassTimeout:       3 * time.Second,
		},
		time.Second,
		"instance-a",
		repo,
		publisherMetricCoordinator{},
		slog.Default(),
	)
	require.NoError(t, publisher.Pass(context.Background()))
	return publisher
}

// collectPublisherMetrics flattens the reader into a name-keyed map, the same
// shape collect uses for the data plane.
func collectPublisherMetrics(
	t *testing.T,
	reader *sdkmetric.ManualReader,
) map[string]metricdata.Metrics {
	t.Helper()
	var resourceMetrics metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &resourceMetrics))
	byName := make(map[string]metricdata.Metrics)
	for _, scope := range resourceMetrics.ScopeMetrics {
		for _, series := range scope.Metrics {
			byName[series.Name] = series
		}
	}
	return byName
}

// TestPublisherMetricsPublishThePublisherCounters checks the exported series
// against the values one real pass produced. A series wired to the wrong field,
// or to a constant, would read correctly at zero and be useless for every
// question it exists to answer.
func TestPublisherMetricsPublishThePublisherCounters(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })

	publisherMetrics, err := NewPublisherMetrics(provider.Meter("test"), testPublisher(t))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, publisherMetrics.Close()) })

	byName := collectPublisherMetrics(t, reader)

	passes, ok := byName["sequence.control.pass"].Data.(metricdata.Sum[int64])
	require.True(t, ok, "sequence.control.pass must be an int64 counter")
	require.Len(t, passes.DataPoints, 1)
	assert.Equal(t, int64(1), passes.DataPoints[0].Value)

	// One of the two slots in the view is unowned, so the alert that watches how
	// long this stays above zero is reading the pass's own count.
	unowned, ok := byName["sequence.control.unowned_slots"].Data.(metricdata.Gauge[int64])
	require.True(t, ok, "sequence.control.unowned_slots must be an int64 gauge")
	require.Len(t, unowned.DataPoints, 1)
	assert.Equal(t, int64(biz.SlotCount-1), unowned.DataPoints[0].Value)

	viewSegments, ok := byName["sequence.control.ownership_view_segments"].Data.(metricdata.Gauge[int64])
	require.True(t, ok, "sequence.control.ownership_view_segments must be an int64 gauge")
	require.Len(t, viewSegments.DataPoints, 1)
	assert.Equal(t, int64(2), viewSegments.DataPoints[0].Value)

	payloadBytes, ok := byName["sequence.control.route_payload_bytes"].Data.(metricdata.Gauge[int64])
	require.True(t, ok, "sequence.control.route_payload_bytes must be an int64 gauge")
	require.Len(t, payloadBytes.DataPoints, 1)
	assert.Equal(t, int64(128), payloadBytes.DataPoints[0].Value)

	lost, ok := byName["sequence.control.coordinator_lost"].Data.(metricdata.Sum[int64])
	require.True(t, ok, "sequence.control.coordinator_lost must be an int64 counter")
	require.Len(t, lost.DataPoints, 1)
	assert.Zero(t, lost.DataPoints[0].Value)

	// A gauge rather than a counter: a restarted replica starts its revision from
	// the directory it finds, so this value is allowed to fall.
	revision, ok := byName["sequence.control.revision"].Data.(metricdata.Gauge[int64])
	require.True(t, ok, "sequence.control.revision must be an int64 gauge")
	require.Len(t, revision.DataPoints, 1)
	assert.Equal(t, int64(9), revision.DataPoints[0].Value)
}

// TestPublisherMetricsRejectMissingDependencies pins that a half-wired process
// fails at startup rather than publishing a series nobody updates.
func TestPublisherMetricsRejectMissingDependencies(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })

	_, err := NewPublisherMetrics(nil, testPublisher(t))
	require.Error(t, err)
	_, err = NewPublisherMetrics(provider.Meter("test"), nil)
	require.Error(t, err)
}

// TestPublisherMetricsCloseIsIdempotent keeps the shutdown hook safe to run
// twice, which is what a stop sequence that runs after a failed start does.
func TestPublisherMetricsCloseIsIdempotent(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })

	publisherMetrics, err := NewPublisherMetrics(provider.Meter("test"), testPublisher(t))
	require.NoError(t, err)
	require.NoError(t, publisherMetrics.Close())
	var nilMetrics *PublisherMetrics
	require.NoError(t, nilMetrics.Close())
}

// TestPublisherMetricsCloseUnregisters checks that a stopped process stops
// publishing, so a scraper cannot read a callback that is still running against
// a publisher that has been shut down.
func TestPublisherMetricsCloseUnregisters(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })

	publisherMetrics, err := NewPublisherMetrics(provider.Meter("test"), testPublisher(t))
	require.NoError(t, err)
	require.NotEmpty(t, collectPublisherMetrics(t, reader))

	require.NoError(t, publisherMetrics.Close())
	assert.Empty(t, collectPublisherMetrics(t, reader))
}
