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
	"context"
	"log/slog"
	"math"
	"strconv"
	"sync"
	"testing"
	"time"

	testkit "github.com/codesjoy/sindri/internal/pkg/tests"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testAllocatorConfig returns the allocator's own configuration with the
// framework defaults applied, so a test states only the values it is about.
func testAllocatorConfig() AllocatorConfig {
	cfg := AllocatorConfig{
		DefaultStep:    10,
		MaxStep:        100,
		ReserveTimeout: 100 * time.Millisecond,
	}
	if err := testkit.DecodeDefaults(&cfg); err != nil {
		panic(err)
	}
	return cfg
}

// testDataPlaneConfig returns a complete data plane configuration with the
// framework defaults applied.
//
// The platform assertions are set here because the loader is what normally
// checks them and a test that builds an allocator directly never goes through it.
func testDataPlaneConfig(cfg AllocatorConfig) DataPlaneConfig {
	plane := DataPlaneConfig{Allocator: cfg, Node: NodeConfig{ID: "test-node"}}
	if err := testkit.DecodeDefaults(&plane); err != nil {
		panic(err)
	}
	plane.HA.PauseVerified = true
	plane.HA.ClockDisciplined = true
	return plane
}

// newHATestAllocator builds an allocator whose high-availability block is
// complete enough for the fences to serve.
//
// Those parameters cannot be invented. The pause bound is what makes a stall
// inside the in-memory linearisation detectable, and the lease is what arms the
// local deadline a slot serves from; a zero for either one discards every
// allocation rather than weakening the fence. Production injects all of them
// from the validated HA section, so this exists for tests that are about
// allocation behaviour rather than about the bounds themselves. The arithmetic
// of the bounds is pinned by the configuration tests and the F.3 model, not
// here.
//
// Fields a test set explicitly are left alone; the rest come from the defaults
// the process itself applies, so this helper holds no second copy of them.
func newHATestAllocator(
	cfg *AllocatorConfig,
	store SequenceRepo,
	ownership OwnershipRepo,
	memorySampler MemorySampler,
	logger *slog.Logger,
) *Allocator {
	if ownership == nil {
		ownership = newLeaseOwnershipFake()
	}
	return NewAllocator(testDataPlaneConfig(*cfg), store, ownership, memorySampler, logger)
}

type rangeStore struct {
	mu  sync.Mutex
	max map[string]int64
}

type memorySamplerFunc func() (uint64, uint64)

func (f memorySamplerFunc) MemoryUsage() (uint64, uint64) { return f() }

var unlimitedMemorySampler = memorySamplerFunc(func() (uint64, uint64) {
	return 1, math.MaxInt64
})

func (s *rangeStore) ReserveRanges(
	_ context.Context,
	_ ReservationAuthority,
	requests []ReservationRequest,
) ([]SequenceRange, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	reserved := make([]SequenceRange, len(requests))
	for index, request := range requests {
		start := s.max[request.Key] + 1
		s.max[request.Key] += request.Step
		reserved[index] = SequenceRange{Start: start, End: s.max[request.Key]}
	}
	return reserved, nil
}

func readyAllocatorForKeys(t testing.TB, keys ...string) *Allocator {
	t.Helper()
	slots := make([]uint32, 0, len(keys))
	seen := make(map[uint32]struct{}, len(keys))
	for _, key := range keys {
		slot := SlotForKey(key)
		if _, ok := seen[slot]; ok {
			continue
		}
		seen[slot] = struct{}{}
		slots = append(slots, slot)
	}
	allocator := newHATestAllocator(
		&AllocatorConfig{DefaultStep: 10, MaxStep: 100},
		&rangeStore{max: make(map[string]int64)},
		nil,
		unlimitedMemorySampler,
		slog.Default(),
	)
	allocator.testAssignSlots(slots)
	return allocator
}

func distinctSlotKeys(count int) []string {
	keys := make([]string, 0, count)
	slots := make(map[uint32]struct{}, count)
	for candidate := 0; len(keys) < count; candidate++ {
		key := "cleanup-key-" + strconv.Itoa(candidate)
		slot := SlotForKey(key)
		if _, exists := slots[slot]; exists {
			continue
		}
		slots[slot] = struct{}{}
		keys = append(keys, key)
	}
	return keys
}

func BenchmarkAllocatorExistingKey(b *testing.B) {
	allocator := readyAllocatorForKeys(b, "orders")
	slot := allocator.slots[SlotForKey("orders")]
	state := &keyState{activeStep: math.MaxInt64}
	state.initialized.Store(true)
	state.start.Store(1)
	state.end.Store(math.MaxInt64)
	slot.Store("orders", state)
	slot.count.Store(1)
	allocator.cachedKeys.Store(1)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := allocator.FetchNext(context.Background(), "orders"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAllocatorExistingKeyParallel(b *testing.B) {
	allocator := readyAllocatorForKeys(b, "orders")
	slot := allocator.slots[SlotForKey("orders")]
	state := &keyState{activeStep: math.MaxInt64}
	state.initialized.Store(true)
	state.start.Store(1)
	state.end.Store(math.MaxInt64)
	slot.Store("orders", state)
	slot.count.Store(1)
	allocator.cachedKeys.Store(1)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := allocator.FetchNext(context.Background(), "orders"); err != nil {
				b.Error(err)
			}
		}
	})
}

func BenchmarkAllocatorNewKeys(b *testing.B) {
	keys := make([]string, b.N)
	for i := range keys {
		keys[i] = "key-" + strconv.Itoa(i)
	}
	slots := make([]uint32, SlotCount)
	for i := range slots {
		slots[i] = uint32(i)
	}
	allocator := newHATestAllocator(
		&AllocatorConfig{DefaultStep: 100, MaxStep: 100},
		&rangeStore{max: make(map[string]int64)},
		nil,
		unlimitedMemorySampler,
		slog.Default(),
	)
	allocator.testAssignSlots(slots)
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		if _, err := allocator.FetchNext(context.Background(), keys[i]); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAllocatorRangeTransition(b *testing.B) {
	allocator := readyAllocatorForKeys(b, "orders")
	allocator.cfg.DefaultStep = math.MaxInt64 / 4
	allocator.cfg.MaxStep = math.MaxInt64 / 4
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := allocator.FetchNext(context.Background(), "orders"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAllocatorIncrementalCleanup(b *testing.B) {
	keys := make([]string, 4096)
	for i := range keys {
		keys[i] = "cleanup-key-" + strconv.Itoa(i)
	}
	allocator := readyAllocatorForKeys(b, keys...)
	allocator.cfg.CleanupSlotsPerRun = 64
	for _, key := range keys {
		slot := allocator.slots[SlotForKey(key)]
		if _, loaded := slot.Load(key); loaded {
			continue
		}
		state := &keyState{}
		state.lastUsed.Store(1)
		slot.Store(key, state)
		slot.count.Add(1)
		allocator.cachedKeys.Add(1)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		allocator.collectIdleCandidates(math.MaxInt64)
	}
}

// TestReadinessIsInitializingBeforeAnyRoute pins that an instance which has
// never been given a route is not sent traffic: it has no slot to allocate
// from, so every request would fail anyway.
func TestReadinessIsInitializingBeforeAnyRoute(t *testing.T) {
	allocator := newHATestAllocator(
		&AllocatorConfig{DefaultStep: 10, MaxStep: 100, IdleTimeout: time.Hour},
		&rangeStore{max: make(map[string]int64)},
		nil,
		unlimitedMemorySampler,
		slog.Default(),
	)

	readiness := allocator.Readiness()
	assert.False(t, readiness.Ready)
	assert.Equal(t, "initializing", readiness.Reason)
}

func TestReadinessIsServingOnceARouteIsApplied(t *testing.T) {
	key := "readiness-serving"
	allocator, _ := leaseAllocator(t, key, func() int64 { return 0 })

	readiness := allocator.Readiness()
	assert.True(t, readiness.Ready)
	assert.Equal(t, "serving", readiness.Reason)
}

// TestReadinessSurvivesStorageLossAndEmptySlots is the D.17 rule. Neither a
// storage blip nor holding no slots is repaired by restarting, and the protocol
// already fails those requests closed with their own retriable reason, so
// reporting them unready would only turn a recoverable condition into a rolling
// restart.
func TestReadinessSurvivesStorageLossAndEmptySlots(t *testing.T) {
	allocator := newHATestAllocator(
		&AllocatorConfig{DefaultStep: 10, MaxStep: 100, IdleTimeout: time.Hour},
		&rangeStore{max: make(map[string]int64)},
		nil,
		unlimitedMemorySampler,
		slog.Default(),
	)
	// A route that assigns this instance nothing. The version only advances
	// once the (empty) claim has committed, which is what makes the instance
	// serving rather than initializing.
	allocator.testAssignSlots(nil)
	assert.True(t, allocator.Readiness().Ready, "an empty assignment is still serving")

	// A storage blip pauses allocation but must not take the instance out of
	// service, because the pause is what it recovers from on its own.
	allocator.Pause()
	readiness := allocator.Readiness()
	assert.True(t, readiness.Ready)
	assert.Equal(t, "serving", readiness.Reason)
}

// TestShutdownReleasesTheSlotAuthority covers the section 6.3 order on the
// shutdown path: the instance stops serving, then gives back the authority it
// holds, so a successor does not have to wait out the quiet window (D7/D16).
func TestShutdownReleasesTheSlotAuthority(t *testing.T) {
	key := "readiness-shutdown"
	allocator, ownership := leaseAllocator(t, key, func() int64 { return 0 })
	require.EqualValues(t, 1, allocator.slots[SlotForKey(key)].epoch.Load())

	allocator.Shutdown()

	assert.Equal(t, int64(1), ownership.released)
	readiness := allocator.Readiness()
	assert.False(t, readiness.Ready)
	assert.Equal(t, "stopping", readiness.Reason)
}

// TestShutdownKeepsAuthorityThatDidNotDrain pins the safe direction: a slot
// with an allocation still in flight is not released, because a new owner
// starting now could serve an id this instance is about to hand out.
func TestShutdownKeepsAuthorityThatDidNotDrain(t *testing.T) {
	key := "readiness-shutdown-drain"
	allocator, ownership := leaseAllocator(t, key, func() int64 { return 0 })
	allocator.ha.ReleaseDrainTimeout = 20 * time.Millisecond

	slot := allocator.slots[SlotForKey(key)]
	require.True(t, slot.enter(), "the slot must be open for the allocation to be in flight")
	// leave() is deliberately not called: this stands in for a request that is
	// still inside the gate when the process stops.

	allocator.Shutdown()

	assert.Zero(t, ownership.released, "a slot that did not drain must stay held")
	assert.False(t, allocator.Readiness().Ready)
}

// TestShutdownReleasesHeldAuthorityThatWasNeverInstalled pins the shutdown
// contract against the cache/authority split. A slot can be granted to this
// instance between two local syncs, so the instance holds authority the serving
// path never installed; the departure still has to give it back, or the
// successor waits out the full quiet window for a grant nobody is using.
func TestShutdownReleasesHeldAuthorityThatWasNeverInstalled(t *testing.T) {
	f := newLeaseOwnershipFake()
	cfg := testDataPlaneConfig(testAllocatorConfig())
	cfg.HA.LeaseDuration = 10 * time.Second
	cfg.HA.RenewInterval = 3 * time.Second
	cfg.HA.SafetyMargin = time.Second
	allocator := NewAllocator(
		cfg,
		&rangeStore{max: map[string]int64{}},
		f,
		unlimitedMemorySampler,
		slog.Default(),
	)
	allocator.SetHandoffs(f, defaultMigrationConfig())
	allocator.RenewLeases()
	require.True(t, allocator.initialized.Load())

	// The authority grants the instance slots out of band, exactly like a
	// handoff that lands between local syncs.
	slots := []uint32{0, 1, 2, 3}
	f.mu.Lock()
	lease := f.leases[allocator.instanceID]
	for _, id := range slots {
		f.rows[id] = Ownership{
			SlotID:          id,
			OwnerNodeID:     allocator.nodeID,
			OwnerInstanceID: allocator.instanceID,
			Epoch:           1,
			State:           SlotOwned,
		}
	}
	lease.Revision++
	f.leases[allocator.instanceID] = lease
	f.mu.Unlock()
	require.Empty(t, allocator.slots, "the grant must not have reached the cache")

	allocator.Shutdown()

	f.mu.Lock()
	defer f.mu.Unlock()
	assert.EqualValues(t, len(slots), f.released, "held authority must be released")
}

// TestHAStatsCountsTakeoversAndReleases pins the appendix E counters at the two
// places they are produced: a claim that moves an epoch, and a release that gives
// one back.
func TestHAStatsCountsTakeoversAndReleases(t *testing.T) {
	const key = "ha-stats"
	ownership := newLeaseOwnershipFake()
	allocator, _ := leaseAllocatorWith(t, key, ownership, nil)

	// Claiming through the route path is what a takeover is, and each granted slot
	// moves its epoch.
	allocator.testAssignSlots([]uint32{SlotForKey(key)})

	stats := allocator.HAStats()
	assert.Positive(t, stats.TakeoversGranted)
	assert.Equal(t, stats.TakeoversGranted, stats.EpochChanges)
	assert.Zero(t, stats.TakeoversRefused)
	owned, fenced := allocator.SlotStateCounts()
	assert.Equal(t, int64(1), owned)
	assert.Zero(t, fenced)

	// A shutdown is one drain, and it gives back what it held.
	allocator.Shutdown()
	stats = allocator.HAStats()
	assert.Equal(t, int64(1), stats.Drains)
	assert.Positive(t, stats.ReleasesReleased)
	assert.Zero(t, stats.ReleasesFailed)
	assert.GreaterOrEqual(t, stats.LastDrainSeconds, 0.0)
	owned, fenced = allocator.SlotStateCounts()
	assert.Zero(t, owned+fenced, "a shutdown holds nothing afterwards")
}

func TestAllocatorCleanupScansConfiguredSlotsPerRun(t *testing.T) {
	keys := distinctSlotKeys(4)
	allocator := readyAllocatorForKeys(t, keys...)
	allocator.cfg.CleanupSlotsPerRun = 2
	for _, key := range keys {
		_, err := allocator.FetchNext(context.Background(), key)
		require.NoError(t, err)
	}
	cutoff := time.Now().Add(time.Hour).UnixNano()
	first, scanned := allocator.collectIdleCandidates(cutoff)
	assert.Len(t, first, 2)
	assert.Equal(t, 2, scanned)
	second, scanned := allocator.collectIdleCandidates(cutoff)
	assert.Len(t, second, 2)
	assert.Equal(t, 2, scanned)
}

func readyAllocatorForCleanup(
	t *testing.T,
	store SequenceRepo,
	clock *fakeClock,
	key string,
) *Allocator {
	t.Helper()
	allocator := newHATestAllocator(&AllocatorConfig{
		DefaultStep:     10,
		MaxStep:         100,
		IdleTimeout:     10 * time.Minute,
		CleanupInterval: time.Minute,
	}, store, nil, unlimitedMemorySampler, slog.Default())
	allocator.now = clock.Now
	allocator.testAssignSlots([]uint32{SlotForKey(key)})
	return allocator
}

func allocatorKeyState(
	t *testing.T,
	allocator *Allocator,
	key string,
) (*allocationSlot, *keyState) {
	t.Helper()
	allocator.slotsMu.RLock()
	defer allocator.slotsMu.RUnlock()
	slot := allocator.slots[SlotForKey(key)]
	require.NotNil(t, slot)
	value, ok := slot.Load(key)
	require.True(t, ok)
	return slot, value
}

func TestAllocatorCleanupEvictsIdleStateAndContinuesFromWatermark(t *testing.T) {
	key := "idle-orders"
	clock := &fakeClock{now: time.Unix(1000, 0)}
	store := &rangeStore{max: make(map[string]int64)}
	allocator := readyAllocatorForCleanup(t, store, clock, key)

	first, err := allocator.FetchNext(context.Background(), key)
	require.NoError(t, err)
	assert.Equal(t, int64(1), first)
	oldSlot, oldState := allocatorKeyState(t, allocator, key)

	clock.Advance(11 * time.Minute)
	allocator.cleanupIdle()
	_, loaded := oldSlot.Load(key)
	assert.False(t, loaded)

	next, err := allocator.FetchNext(context.Background(), key)
	require.NoError(t, err)
	assert.Equal(t, int64(11), next)
	_, newState := allocatorKeyState(t, allocator, key)
	assert.NotSame(t, oldState, newState)
}

func TestCleanupRetainsReferencedStateBeforeRangePreparation(t *testing.T) {
	key := "referenced-orders"
	clock := &fakeClock{now: time.Unix(1000, 0)}
	store := &rangeStore{max: map[string]int64{key: 10}}
	allocator := readyAllocatorForCleanup(t, store, clock, key)
	slot := allocator.slots[SlotForKey(key)]
	state := &keyState{allocator: allocator, slot: slot}
	state.start.Store(1)
	state.end.Store(10)
	state.next.Store(9)
	state.generation.Store(2)
	state.initialized.Store(true)
	state.lastUsed.Store(clock.Now().UnixNano())
	slot.Store(key, state)
	slot.count.Store(1)
	allocator.cachedKeys.Store(1)
	clock.Advance(11 * time.Minute)

	// Pause A after acquiring its cache entry, before preparing its two-ID block.
	held, err := allocator.acquireState(key, slot)
	require.NoError(t, err)
	require.Same(t, state, held)
	allocator.cleanupIdle()
	current, ok := slot.Load(key)
	require.True(t, ok)
	require.Same(t, held, current)
	b, err := allocator.FetchNext(context.Background(), key)
	require.NoError(t, err)
	require.EqualValues(t, 10, b)
	a, _, err := held.allocateBlockSlow(context.Background(), allocator.scope(), key, 2,
		allocator.cfg, allocator.now)
	held.release()
	require.NoError(t, err)
	c, err := allocator.FetchNext(context.Background(), key)
	require.NoError(t, err)
	assert.Greater(t, a, b)
	assert.Greater(t, c, a+1)
	assert.Zero(t, state.references.Load())
}

func TestRetiredKeyStateCannotBeAcquiredOrAllocated(t *testing.T) {
	state := &keyState{}
	require.True(t, state.references.CompareAndSwap(0, -1))
	assert.False(t, state.acquire())
	_, err := state.beginLinearization()
	require.Error(t, err)
}

func TestKeyStateAcquisitionAndRetirementAreExclusive(t *testing.T) {
	for range 100 {
		state := &keyState{}
		start := make(chan struct{})
		acquired := make(chan bool, 1)
		go func() { <-start; acquired <- state.acquire() }()
		close(start)
		retired := state.references.CompareAndSwap(0, -1)
		held := <-acquired
		assert.NotEqual(t, retired, held)
		if held {
			state.release()
			assert.Zero(t, state.references.Load())
		} else {
			assert.EqualValues(t, -1, state.references.Load())
		}
	}
}

func TestBatchCancellationReleasesKeyReferences(t *testing.T) {
	key := "cancelled-orders"
	clock := &fakeClock{now: time.Unix(1000, 0)}
	store := &blockingRangeStore{started: make(chan struct{}), release: make(chan struct{})}
	allocator := readyAllocatorForCleanup(t, store, clock, key)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := allocator.FetchNextN(ctx, key, 2); done <- err }()
	<-store.started
	cancel()
	require.Error(t, <-done)
	_, state := allocatorKeyState(t, allocator, key)
	assert.Zero(t, state.references.Load())
}

func TestAllocatorCleanupRespectsInterval(t *testing.T) {
	key := "scheduled-cleanup-orders"
	clock := &fakeClock{now: time.Unix(1500, 0)}
	allocator := readyAllocatorForCleanup(
		t,
		&rangeStore{max: make(map[string]int64)},
		clock,
		key,
	)
	slot := allocator.slots[SlotForKey(key)]
	state := &keyState{}
	state.lastUsed.Store(clock.Now().Add(-11 * time.Minute).UnixNano())
	slot.Store(key, state)
	allocator.lastCleanup.Store(clock.Now().Add(-30 * time.Second).UnixNano())

	allocator.cleanupIdle()
	_, loaded := slot.Load(key)
	assert.True(t, loaded)

	clock.Advance(31 * time.Second)
	allocator.cleanupIdle()
	_, loaded = slot.Load(key)
	assert.False(t, loaded)
}

func TestNodeBaseTickCleansIdleAllocatorState(t *testing.T) {
	key := "node-cleanup-orders"
	clock := &fakeClock{now: time.Unix(1750, 0)}
	allocator := readyAllocatorForCleanup(
		t,
		&rangeStore{max: make(map[string]int64)},
		clock,
		key,
	)
	slot := allocator.slots[SlotForKey(key)]
	state := &keyState{}
	state.lastUsed.Store(clock.Now().Add(-11 * time.Minute).UnixNano())
	slot.Store(key, state)
	manager := &NodeManager{allocator: allocator}

	manager.Maintain(context.Background())
	_, loaded := slot.Load(key)
	assert.False(t, loaded)
}

func TestAllocatorCleanupReportsDiscardedRanges(t *testing.T) {
	key := "discarded-orders"
	clock := &fakeClock{now: time.Unix(2000, 0)}
	allocator := readyAllocatorForCleanup(
		t,
		&rangeStore{max: make(map[string]int64)},
		clock,
		key,
	)
	slot := allocator.slots[SlotForKey(key)]
	state := &keyState{standby: &SequenceRange{Start: 11, End: 30}}
	state.initialized.Store(true)
	state.start.Store(1)
	state.end.Store(10)
	state.returned.Store(3)
	state.lastUsed.Store(clock.Now().Add(-11 * time.Minute).UnixNano())
	slot.Store(key, state)

	candidates, scanned := allocator.collectIdleCandidates(
		clock.Now().Add(-10 * time.Minute).UnixNano(),
	)
	stats := allocator.evictIdleCandidates(
		candidates,
		clock.Now().Add(-10*time.Minute).UnixNano(),
	)

	assert.Equal(t, 1, scanned)
	assert.Equal(t, 1, stats.evicted)
	assert.Equal(t, int64(7), stats.discardedActive)
	assert.Equal(t, int64(20), stats.discardedStandby)
}

func TestAllocatorCleanupRechecksRecentUse(t *testing.T) {
	key := "recent-orders"
	clock := &fakeClock{now: time.Unix(3000, 0)}
	allocator := readyAllocatorForCleanup(
		t,
		&rangeStore{max: make(map[string]int64)},
		clock,
		key,
	)
	slot := allocator.slots[SlotForKey(key)]
	state := &keyState{}
	state.lastUsed.Store(clock.Now().Add(-11 * time.Minute).UnixNano())
	slot.Store(key, state)
	cutoff := clock.Now().Add(-10 * time.Minute).UnixNano()
	candidates, _ := allocator.collectIdleCandidates(cutoff)
	require.Len(t, candidates, 1)

	state.touch(clock.Now())
	stats := allocator.evictIdleCandidates(candidates, cutoff)
	assert.Zero(t, stats.evicted)
	_, loaded := slot.Load(key)
	assert.True(t, loaded)
}

func TestAllocatorCleanupSkipsInflightFetch(t *testing.T) {
	key := "inflight-orders"
	clock := &fakeClock{now: time.Unix(4000, 0)}
	allocator := readyAllocatorForCleanup(
		t,
		&rangeStore{max: make(map[string]int64)},
		clock,
		key,
	)
	slot := allocator.slots[SlotForKey(key)]
	state := &keyState{fetch: &rangeFetch{done: make(chan struct{})}}
	state.lastUsed.Store(clock.Now().Add(-11 * time.Minute).UnixNano())
	slot.Store(key, state)
	cutoff := clock.Now().Add(-10 * time.Minute).UnixNano()
	candidates, _ := allocator.collectIdleCandidates(cutoff)

	stats := allocator.evictIdleCandidates(candidates, cutoff)
	assert.Equal(t, 1, stats.inflight)
	assert.Zero(t, stats.evicted)
	_, loaded := slot.Load(key)
	assert.True(t, loaded)
}

func TestAllocatorCleanupIgnoresReplacedSlotMap(t *testing.T) {
	key := "rerouted-orders"
	clock := &fakeClock{now: time.Unix(5000, 0)}
	allocator := readyAllocatorForCleanup(
		t,
		&rangeStore{max: make(map[string]int64)},
		clock,
		key,
	)
	oldSlot := allocator.slots[SlotForKey(key)]
	oldState := &keyState{}
	oldState.lastUsed.Store(clock.Now().Add(-11 * time.Minute).UnixNano())
	oldSlot.Store(key, oldState)
	cutoff := clock.Now().Add(-10 * time.Minute).UnixNano()
	candidates, _ := allocator.collectIdleCandidates(cutoff)

	newSlot := &allocationSlot{}
	newState := &keyState{}
	newSlot.Store(key, newState)
	allocator.slotsMu.Lock()
	allocator.slots[SlotForKey(key)] = newSlot
	allocator.slotsMu.Unlock()

	stats := allocator.evictIdleCandidates(candidates, cutoff)
	assert.Zero(t, stats.evicted)
	value, loaded := newSlot.Load(key)
	assert.True(t, loaded)
	assert.Same(t, newState, value)
}

// TestAllocatorReconcileAndCleanupDoNotBlockOnReservationIO pins the lock scope
// of the allocation path: a range reservation that is stuck in storage must not
// hold the allocator's slot lock, or every reconciliation would queue behind a
// slow statement and stall the whole node.
func TestAllocatorReconcileAndCleanupDoNotBlockOnReservationIO(t *testing.T) {
	key := "blocked-orders"
	clock := &fakeClock{now: time.Unix(6000, 0)}
	store := &blockingRangeStore{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	allocator := readyAllocatorForCleanup(t, store, clock, key)

	fetchDone := make(chan error, 1)
	go func() {
		_, err := allocator.FetchNext(context.Background(), key)
		fetchDone <- err
	}()
	<-store.started

	// The reservation is inside storage, and the slot lock must already be
	// released: both a reconcile (which takes the write lock) and a cleanup pass
	// have to complete while it is still in flight.
	reconcileDone := make(chan struct{})
	go func() {
		allocator.testAssignSlots([]uint32{SlotForKey(key)})
		close(reconcileDone)
	}()
	select {
	case <-reconcileDone:
	case <-time.After(time.Second):
		t.Fatal("reconcile blocked behind an in-flight reservation")
	}

	cleanupDone := make(chan struct{})
	go func() {
		allocator.cleanupIdle()
		close(cleanupDone)
	}()
	select {
	case <-cleanupDone:
	case <-time.After(time.Second):
		t.Fatal("cleanup blocked behind an in-flight reservation")
	}

	// The in-flight key survives the pass: the eviction re-checks its state and
	// sees the fetch attached to it.
	_, state := allocatorKeyState(t, allocator, key)
	assert.NotNil(t, state.fetch, "the in-flight key was not evicted")

	close(store.release)
	require.NoError(t, <-fetchDone)
	_, state = allocatorKeyState(t, allocator, key)
	assert.Equal(t, clock.Now().UnixNano(), state.lastUsed.Load())
}

func TestAllocatorConcurrentCleanupKeepsIDsUnique(t *testing.T) {
	key := "concurrent-cleanup-orders"
	clock := &fakeClock{now: time.Unix(7000, 0)}
	store := &rangeStore{max: make(map[string]int64)}
	allocator := readyAllocatorForCleanup(t, store, clock, key)
	allocator.cfg.IdleTimeout = time.Nanosecond
	allocator.cfg.CleanupInterval = time.Nanosecond

	const workers = 8
	const allocations = 100
	values := make(chan int64, workers*allocations)
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range allocations {
				value, err := allocator.FetchNext(context.Background(), key)
				if err != nil {
					errs <- err
					return
				}
				values <- value
			}
		}()
	}
	cleanupDone := make(chan struct{})
	go func() {
		defer close(cleanupDone)
		for range allocations {
			clock.Advance(time.Nanosecond)
			allocator.cleanupIdle()
		}
	}()
	wg.Wait()
	<-cleanupDone
	close(values)
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	seen := make(map[int64]struct{}, workers*allocations)
	for value := range values {
		_, duplicate := seen[value]
		assert.False(t, duplicate, "duplicate ID %d", value)
		seen[value] = struct{}{}
	}
	assert.Len(t, seen, workers*allocations)
}
