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

package sequence

import (
	"context"
	"errors"
	"hash/crc32"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	sequencev1 "github.com/codesjoy/sindri/gen/go/sequence/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSlotForKeyMatchesIEEECRC32(t *testing.T) {
	for _, key := range []string{"", "orders", "sequence-key-123", "\xe4\xb8\xad\xe6\x96\x87"} {
		assert.Equal(t, crc32.ChecksumIEEE([]byte(key))%SlotCount, SlotForKey(key))
	}
}

// newSegmentedTestRouter publishes a snapshot whose loader keeps returning it, so
// a refreshed attempt sees the same layout and epochs as the first one.
func newSegmentedTestRouter(t *testing.T, snapshot *sequencev1.RouteSnapshot) *Router {
	t.Helper()
	router, err := NewRouter(func(
		context.Context,
		int64,
	) (*sequencev1.GetRouteResponse, error) {
		return &sequencev1.GetRouteResponse{Route: snapshot}, nil
	})
	require.NoError(t, err)
	require.NoError(t, router.Update(snapshot))
	return router
}

// TestRouterCompilesTheOwnershipEpochView covers the caller half of appendix A.2:
// the snapshot's segments become a per-slot epoch view, and the layout version is
// retained because it is what a caller sends so an owner hashing under another
// layout can refuse instead of answering for slot numbers that mean something
// else.
func TestRouterCompilesTheOwnershipEpochView(t *testing.T) {
	router := newSegmentedTestRouter(t, testSegmentedRoute(1, 7,
		testSegment{nodeID: "node-a", epoch: 3, from: 0, to: 99},
		testSegment{nodeID: "node-b", epoch: 4, from: 100, to: SlotCount - 1},
	))

	assert.Equal(t, int64(7), router.LayoutVersion())
	for slot, want := range map[uint32]uint64{0: 3, 99: 3, 100: 4, SlotCount - 1: 4} {
		epoch, ok := router.EpochOf(slot)
		require.True(t, ok, "slot %d must have an epoch", slot)
		assert.Equal(t, want, epoch, "slot %d", slot)
	}
	// The node view still answers, so a caller that reads only it routes the same.
	assert.Equal(t, "node-b", router.ownerTable()[100])
}

// TestRouterWithoutSegmentsHasNoEpochView pins the compatibility case: a snapshot
// from before ownership was authoritative carries no epochs, so a caller sends
// none and an owner compares none.
func TestRouterWithoutSegmentsHasNoEpochView(t *testing.T) {
	router := newSegmentedTestRouter(t, testRoute(1, "node-a"))

	_, ok := router.EpochOf(0)
	assert.False(t, ok, "a snapshot without segments has no epoch to send")
	assert.Zero(t, router.LayoutVersion())
}

// TestRouterRejectsSegmentsThatDoNotCoverTheSpace pins the contract the epoch
// table rests on. A snapshot whose segments leave a gap, overlap or run short
// would leave a caller with an epoch for some slots and none for others, without
// telling it which, so it is refused rather than partly applied.
func TestRouterRejectsSegmentsThatDoNotCoverTheSpace(t *testing.T) {
	for name, segments := range map[string][]testSegment{
		"gap": {
			{nodeID: "node-a", epoch: 1, from: 1, to: SlotCount - 1},
		},
		"short": {
			{nodeID: "node-a", epoch: 1, from: 0, to: SlotCount - 2},
		},
		"overlap": {
			{nodeID: "node-a", epoch: 1, from: 0, to: 99},
			{nodeID: "node-b", epoch: 2, from: 99, to: SlotCount - 1},
		},
		"inverted": {
			{nodeID: "node-a", epoch: 1, from: 5, to: 4},
			{nodeID: "node-b", epoch: 2, from: 5, to: SlotCount - 1},
		},
	} {
		t.Run(name, func(t *testing.T) {
			router, err := NewRouter(func(
				context.Context,
				int64,
			) (*sequencev1.GetRouteResponse, error) {
				return nil, errors.New("unused")
			})
			require.NoError(t, err)
			require.ErrorIs(
				t,
				router.Update(testSegmentedRoute(1, 1, segments...)),
				ErrInvalidRoute,
			)
		})
	}
}

func TestRouterUpdateValidatesCopiesAndOrdersVersions(t *testing.T) {
	router, err := NewRouter(func(context.Context, int64) (*sequencev1.GetRouteResponse, error) {
		return nil, errors.New("unused")
	})
	require.NoError(t, err)

	route := testRoute(2, "node-a", "node-b")
	require.NoError(t, router.Update(route))
	route.Nodes[0].NodeId = "mutated"
	route.Nodes[0].Slots[0] = SlotCount

	snapshot := router.Snapshot()
	assert.Equal(t, int64(2), router.Version())
	assert.Equal(t, "node-a", snapshot.Nodes[0].NodeId)
	assert.Less(t, snapshot.Nodes[0].Slots[0], uint32(SlotCount))
	snapshot.Nodes[0].NodeId = "also-mutated"
	assert.Equal(t, "node-a", router.Snapshot().Nodes[0].NodeId)

	require.ErrorIs(t, router.Update(testRoute(1, "node-a")), ErrRouteVersionRegression)
	require.ErrorIs(t, router.Update(testRoute(2, "node-a")), ErrRouteVersionConflict)
	require.NoError(t, router.Update(router.Snapshot()))
}

func TestRouterRejectsInvalidSnapshots(t *testing.T) {
	router, err := NewRouter(func(context.Context, int64) (*sequencev1.GetRouteResponse, error) {
		return nil, errors.New("unused")
	})
	require.NoError(t, err)

	missing := testRoute(1, "node-a")
	missing.Nodes[0].Slots = missing.Nodes[0].Slots[:SlotCount-1]
	duplicateNode := testRoute(1, "node-a", "node-b")
	duplicateNode.Nodes[1].NodeId = "node-a"
	duplicateSlot := testRoute(1, "node-a", "node-b")
	duplicateSlot.Nodes[1].Slots[0] = duplicateSlot.Nodes[0].Slots[0]
	outOfRange := testRoute(1, "node-a")
	outOfRange.Nodes[0].Slots[0] = SlotCount

	for _, snapshot := range []*sequencev1.RouteSnapshot{
		nil,
		{},
		missing,
		duplicateNode,
		duplicateSlot,
		outOfRange,
	} {
		require.ErrorIs(t, router.Update(snapshot), ErrInvalidRoute)
	}
}

func TestRouterRefreshHandlesNotModified(t *testing.T) {
	responses := []*sequencev1.GetRouteResponse{
		{NotModified: true},
		{Route: testRoute(1, "node-a")},
		{NotModified: true},
	}
	var calls int
	router, err := NewRouter(func(
		_ context.Context,
		known int64,
	) (*sequencev1.GetRouteResponse, error) {
		if calls == 0 {
			assert.Zero(t, known)
		} else {
			assert.Equal(t, int64(calls-1), known)
		}
		response := responses[calls]
		calls++
		return response, nil
	})
	require.NoError(t, err)
	require.ErrorIs(t, router.Refresh(context.Background()), ErrRouteUnavailable)
	require.NoError(t, router.Refresh(context.Background()))
	require.NoError(t, router.Refresh(context.Background()))
	assert.Equal(t, int64(1), router.Version())
}

func TestRouterRefreshCoalescesAndWaitersCanCancel(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	router, err := NewRouter(func(
		context.Context,
		int64,
	) (*sequencev1.GetRouteResponse, error) {
		calls.Add(1)
		close(started)
		<-release
		return &sequencev1.GetRouteResponse{Route: testRoute(1, "node-a")}, nil
	})
	require.NoError(t, err)

	router.mu.Lock()
	const count = 8
	ready := sync.WaitGroup{}
	ready.Add(count)
	done := sync.WaitGroup{}
	done.Add(count)
	errs := make(chan error, count)
	for range count {
		go func() {
			ready.Done()
			errs <- router.Refresh(context.Background())
			done.Done()
		}()
	}
	ready.Wait()
	router.mu.Unlock()
	<-started
	for range 20 {
		runtime.Gosched()
	}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, router.Refresh(cancelled), context.Canceled)
	close(release)
	done.Wait()
	close(errs)
	for refreshErr := range errs {
		require.NoError(t, refreshErr)
	}
	assert.Equal(t, int32(1), calls.Load())
}

func TestRouterRefreshAfterSkipsLoaderWhenRouteAlreadyAdvanced(t *testing.T) {
	var calls int
	router, err := NewRouter(func(
		context.Context,
		int64,
	) (*sequencev1.GetRouteResponse, error) {
		calls++
		return nil, errors.New("unexpected refresh")
	})
	require.NoError(t, err)
	require.NoError(t, router.Update(testRoute(2, "node-a")))
	require.NoError(t, router.refreshAfter(context.Background(), 1))
	assert.Zero(t, calls)
}

func TestNewRouterRequiresLoader(t *testing.T) {
	_, err := NewRouter(nil)
	require.Error(t, err)
}
