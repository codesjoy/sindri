package biz

import (
	"context"
	"log/slog"
	"sync"
	"testing"
)

type rangeStore struct {
	mu  sync.Mutex
	max map[string]int64
}

func (s *rangeStore) ReserveRange(
	_ context.Context,
	key string,
	step int64,
) (SequenceRange, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	start := s.max[key] + 1
	s.max[key] += step
	return SequenceRange{Start: start, End: s.max[key]}, nil
}

func TestKeyStateContinuesFromPersistedWatermark(t *testing.T) {
	store := &rangeStore{max: map[string]int64{"orders": 100}}
	state := &keyState{}

	got, err := state.allocate(context.Background(), store, "orders", 10)
	if err != nil {
		t.Fatal(err)
	}
	if got != 101 {
		t.Fatalf("first id after handoff = %d, want 101", got)
	}
}

func TestKeyStateAllocatesAcrossRanges(t *testing.T) {
	store := &rangeStore{max: map[string]int64{}}
	state := &keyState{}

	for want := int64(1); want <= 25; want++ {
		got, err := state.allocate(context.Background(), store, "orders", 10)
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("allocated id = %d, want %d", got, want)
		}
	}
}

func TestKeyStateConcurrentInitializationIsUnique(t *testing.T) {
	store := &rangeStore{max: map[string]int64{"orders": 100}}
	state := &keyState{}

	const workers = 128
	values := make(chan int64, workers)
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			value, err := state.allocate(context.Background(), store, "orders", 8)
			if err != nil {
				errs <- err
				return
			}
			values <- value
		}()
	}
	wg.Wait()
	close(values)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	seen := make(map[int64]struct{}, workers)
	for value := range values {
		if value <= 100 {
			t.Fatalf("allocated stale id %d", value)
		}
		if _, exists := seen[value]; exists {
			t.Fatalf("allocated duplicate id %d", value)
		}
		seen[value] = struct{}{}
	}
	if len(seen) != workers {
		t.Fatalf("allocated %d ids, want %d", len(seen), workers)
	}
}

type invalidRangeStore struct{}

func (invalidRangeStore) ReserveRange(context.Context, string, int64) (SequenceRange, error) {
	return SequenceRange{Start: 10, End: 10}, nil
}

func TestKeyStateRejectsInvalidReservedRange(t *testing.T) {
	state := &keyState{}
	if _, err := state.allocate(
		context.Background(),
		invalidRangeStore{},
		"orders",
		10,
	); err == nil {
		t.Fatal("expected invalid reserved range error")
	}
}

func TestAllocatorAppliesRouteVersionWithoutNewSlots(t *testing.T) {
	tests := []struct {
		name   string
		routes []struct {
			version int64
			slots   []uint32
			apply   bool
		}
		wantVersion int64
	}{
		{
			name: "unchanged slots",
			routes: []struct {
				version int64
				slots   []uint32
				apply   bool
			}{
				{version: 1, slots: []uint32{1}, apply: true},
				{version: 2, slots: []uint32{1}, apply: true},
			},
			wantVersion: 2,
		},
		{
			name: "empty slots",
			routes: []struct {
				version int64
				slots   []uint32
				apply   bool
			}{
				{version: 1, slots: []uint32{1}, apply: true},
				{version: 2, apply: true},
			},
			wantVersion: 2,
		},
		{
			name: "skipped intermediate snapshot",
			routes: []struct {
				version int64
				slots   []uint32
				apply   bool
			}{
				{version: 1, slots: []uint32{1}, apply: true},
				{version: 2, slots: []uint32{1}},
				{version: 3, slots: []uint32{1}, apply: true},
			},
			wantVersion: 3,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			allocator := NewAllocator(
				&AllocatorConfig{Step: 10},
				&rangeStore{max: make(map[string]int64)},
				slog.Default(),
			)
			for _, route := range test.routes {
				allocator.CommitRoute(route.version, 0, route.slots)
				if route.apply {
					allocator.ApplyRoute(0)
				}
			}
			if got := allocator.CurrentVersion(); got != test.wantVersion {
				t.Fatalf("route version = %d, want %d", got, test.wantVersion)
			}
		})
	}
}
