package sequence

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	sequencev1 "github.com/codesjoy/skuld/gen/go/sequence/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
