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
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/codesjoy/pkg/basic/xerror"
	"github.com/codesjoy/sindri/gen/go/sequence/reason"
	sequencev1 "github.com/codesjoy/sindri/gen/go/sequence/v1"
	"github.com/codesjoy/yggdrasil/v3/transport/runtime/client/balancer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSlotForKeyIsStableAndBounded(t *testing.T) {
	for _, key := range []string{"orders", "é", "订单", "a ", "a", "A"} {
		assert.Equal(t, SlotForKey(key), SlotForKey(key))
		assert.Less(t, SlotForKey(key), uint32(SlotCount))
	}
}

func TestRouterCompilesCompleteAuthority(t *testing.T) {
	snapshot := testSegmentedRoute(
		1,
		7,
		testSegment{"node-a", 3, 0, 99},
		testSegment{"node-b", 4, 100, SlotCount - 1},
	)
	router := newSegmentedTestRouter(t, snapshot)
	assert.Equal(t, int64(7), router.LayoutVersion())
	for slot, want := range map[uint32]uint64{0: 3, 99: 3, 100: 4, SlotCount - 1: 4} {
		epoch, ok := router.EpochOf(slot)
		require.True(t, ok)
		assert.Equal(t, want, epoch)
	}
	assert.Equal(t, "node-b", router.ownerTable()[100])
	snapshot.Segments[0].OwnerNodeId = ""
	snapshot.Segments[0].OwnerInstanceId = ""
	snapshot.Version = 2
	require.NoError(t, router.Update(snapshot))
	assert.Empty(t, router.ownerTable()[0])
}

func TestRouterRejectsIncompleteAuthority(t *testing.T) {
	mutations := map[string]func(*sequencev1.RouteSnapshot){
		"missing segments": func(s *sequencev1.RouteSnapshot) { s.Segments = nil },
		"missing layout":   func(s *sequencev1.RouteSnapshot) { s.LayoutVersion = 0 },
		"gap":              func(s *sequencev1.RouteSnapshot) { s.Segments[0].StartSlot = 1 },
		"short":            func(s *sequencev1.RouteSnapshot) { s.Segments[0].EndSlot = SlotCount - 2 },
		"overlap":          func(s *sequencev1.RouteSnapshot) { s.Segments = append(s.Segments, s.Segments[0]) },
		"inverted":         func(s *sequencev1.RouteSnapshot) { s.Segments[0].StartSlot = 5; s.Segments[0].EndSlot = 4 },
		"out of range":     func(s *sequencev1.RouteSnapshot) { s.Segments[0].EndSlot = SlotCount },
		"missing epoch":    func(s *sequencev1.RouteSnapshot) { s.Segments[0].SlotEpoch = 0 },
		"missing instance": func(s *sequencev1.RouteSnapshot) { s.Segments[0].OwnerInstanceId = "" },
		"missing node":     func(s *sequencev1.RouteSnapshot) { s.Segments[0].OwnerNodeId = "" },
		"nil segment":      func(s *sequencev1.RouteSnapshot) { s.Segments[0] = nil },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			s := testSegmentedRoute(1, 1, testSegment{"node-a", 1, 0, SlotCount - 1})
			mutate(s)
			_, err := compileRoute(s)
			require.ErrorIs(t, err, ErrInvalidRoute)
		})
	}
	_, err := compileRoute(nil)
	require.ErrorIs(t, err, ErrInvalidRoute)
}

func TestRouterUpdateValidatesCopiesAndOrdersVersions(t *testing.T) {
	router := newSegmentedTestRouter(t, testRoute(2, "node-a", "node-b"))
	snapshot := router.Snapshot()
	snapshot.Segments[0].OwnerNodeId = "mutated"
	assert.Equal(t, "node-a", router.Snapshot().Segments[0].OwnerNodeId)
	require.ErrorIs(t, router.Update(testRoute(1, "node-a")), ErrRouteVersionRegression)
	require.ErrorIs(t, router.Update(testRoute(2, "node-a")), ErrRouteVersionConflict)
	require.NoError(t, router.Update(router.Snapshot()))
}

func TestRouterRefreshHandlesNotModified(t *testing.T) {
	responses := []*sequencev1.GetRouteResponse{
		{NotModified: true},
		{Route: testRoute(1, "node-a")},
		{NotModified: true},
	}
	var calls int
	router, err := NewRouter(
		func(_ context.Context, known int64) (*sequencev1.GetRouteResponse, error) {
			if calls == 0 {
				assert.Zero(t, known)
			} else {
				assert.Equal(t, int64(calls-1), known)
			}
			r := responses[calls]
			calls++
			return r, nil
		},
	)
	require.NoError(t, err)
	require.ErrorIs(t, router.Refresh(context.Background()), ErrRouteUnavailable)
	require.NoError(t, router.Refresh(context.Background()))
	require.NoError(t, router.Refresh(context.Background()))
	assert.Equal(t, int64(1), router.Version())
}

func TestRouterRefreshCoalescesAndWaitersCanCancel(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	router, err := NewRouter(func(context.Context, int64) (*sequencev1.GetRouteResponse, error) {
		calls.Add(1)
		close(started)
		<-release
		return &sequencev1.GetRouteResponse{Route: testRoute(1, "node-a")}, nil
	})
	require.NoError(t, err)
	router.mu.Lock()
	const count = 8
	var ready, done sync.WaitGroup
	ready.Add(count)
	done.Add(count)
	errs := make(chan error, count)
	for range count {
		go func() { ready.Done(); errs <- router.Refresh(context.Background()); done.Done() }()
	}
	ready.Wait()
	router.mu.Unlock()
	<-started
	for range 20 {
		runtime.Gosched()
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, router.Refresh(ctx), context.Canceled)
	close(release)
	done.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	assert.Equal(t, int32(1), calls.Load())
}

func TestRouterRefreshAfterSkipsLoaderWhenRouteAlreadyAdvanced(t *testing.T) {
	var calls int
	router, err := NewRouter(func(context.Context, int64) (*sequencev1.GetRouteResponse, error) {
		calls++
		return nil, errors.New("unexpected refresh")
	})
	require.NoError(t, err)
	require.NoError(t, router.Update(testRoute(2, "node-a")))
	require.NoError(t, router.refreshAfter(context.Background(), 1))
	assert.Zero(t, calls)
}
func TestNewRouterRequiresLoader(t *testing.T) { _, err := NewRouter(nil); require.Error(t, err) }

func TestRetryPolicyDefaultsAndValidation(t *testing.T) {
	defaults := DefaultRetryPolicy()
	assert.Equal(t, 8, defaults.MaxAttempts)
	assert.Equal(t, 10*time.Second, defaults.MaxElapsed)
	assert.Equal(t, 100*time.Millisecond, defaults.InitialBackoff)
	assert.Equal(t, 2*time.Second, defaults.MaxBackoff)
	loader := func(context.Context, int64) (*sequencev1.GetRouteResponse, error) {
		return &sequencev1.GetRouteResponse{Route: testRoute(1, "node-a")}, nil
	}
	for _, field := range []string{"attempts", "elapsed", "initial", "maximum"} {
		t.Run(field, func(t *testing.T) {
			policy := defaults
			switch field {
			case "attempts":
				policy.MaxAttempts = 0
			case "elapsed":
				policy.MaxElapsed = 0
			case "initial":
				policy.InitialBackoff = -time.Second
			case "maximum":
				policy.MaxBackoff = policy.InitialBackoff / 2
			}
			_, err := NewRouter(loader, WithRetryPolicy(policy))
			require.Error(t, err)
		})
	}
	_, err := NewRouter(loader, nil)
	require.Error(t, err)
	defaults.MaxAttempts = 1
	router, err := NewRouter(loader, WithRetryPolicy(defaults))
	require.NoError(t, err)
	calls := 0
	failure := retryError(RetryAfterRefresh, "")
	err = router.runWithRetry(context.Background(), func(context.Context) error {
		calls++
		return failure
	})
	assert.Same(t, failure, err)
	assert.Equal(t, 1, calls)
}

func TestRetryClassificationAndOldServers(t *testing.T) {
	for _, action := range []string{RetryAfterRefresh, RetryAfterBackoff, RetryAfterThrottle, RetryNever} {
		d := classifyRetry(retryError(action, "3s"))
		assert.Equal(t, action, d.action)
		assert.Equal(t, 3*time.Second, d.after)
	}
	for _, hint := range []string{"bad", "-1s", "0s", "999999999999999h"} {
		assert.Zero(t, classifyRetry(retryError(RetryAfterBackoff, hint)).after)
	}
	for _, tc := range []struct {
		err    error
		action string
	}{
		{xerror.NewWithReason(reason.Reason_SEQUENCE_ROUTE_EXPIRED, "stale", nil), RetryAfterRefresh},
		{xerror.NewWithReason(reason.Reason_SEQUENCE_SLOT_NOT_OWNER, "stale", nil), RetryAfterRefresh},
		{xerror.NewWithReason(reason.Reason_SEQUENCE_LEASE_EXPIRED, "expired", nil), RetryAfterRefresh},
		{xerror.NewWithReason(reason.Reason_SEQUENCE_STORAGE_UNAVAILABLE, "storage", nil), RetryAfterBackoff},
		{xerror.NewWithReason(reason.Reason_SEQUENCE_COMMIT_UNCERTAIN, "commit", nil), RetryAfterBackoff},
		{xerror.NewWithReason(reason.Reason_SEQUENCE_CAPACITY_EXHAUSTED, "capacity", nil), RetryAfterThrottle},
		{status.Error(codes.Unavailable, "transport"), RetryAfterRefresh},
		{balancer.ErrNoAvailableInstance, RetryAfterRefresh},
		{context.Canceled, RetryNever},
		{context.DeadlineExceeded, RetryNever},
		{status.Error(codes.Canceled, "cancelled"), RetryNever},
		{status.Error(codes.DeadlineExceeded, "deadline"), RetryNever},
		{status.Error(codes.InvalidArgument, "invalid"), RetryNever},
		{errors.New("terminal"), RetryNever},
	} {
		assert.Equal(t, tc.action, classifyRetry(tc.err).action, "%v", tc.err)
	}
}

func TestRetryEqualJitterAndServerMinimum(t *testing.T) {
	router := mustRouter(t, testRoute(1, "node-a"))
	for attempt, ceiling := range []time.Duration{
		100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond,
		800 * time.Millisecond, 1600 * time.Millisecond, 2 * time.Second, 2 * time.Second,
	} {
		router.retryRandom = func() float64 { return 0 }
		assert.Equal(t, ceiling/2, router.retryDelay(attempt+1, 0))
		router.retryRandom = func() float64 { return 1 }
		assert.Equal(t, ceiling, router.retryDelay(attempt+1, 0))
		assert.Equal(t, 5*time.Second, router.retryDelay(attempt+1, 5*time.Second))
	}
}

func TestRetryActionsAndAttemptLimit(t *testing.T) {
	for _, action := range []string{RetryAfterRefresh, RetryAfterBackoff, RetryAfterThrottle, RetryNever} {
		t.Run(action, func(t *testing.T) {
			loads := 0
			policy := DefaultRetryPolicy()
			policy.MaxAttempts = 3
			router, err := NewRouter(
				func(context.Context, int64) (*sequencev1.GetRouteResponse, error) {
					loads++
					return &sequencev1.GetRouteResponse{
						Route: testRoute(int64(loads), "node-a"),
					}, nil
				},
				WithRetryPolicy(policy),
			)
			require.NoError(t, err)
			require.NoError(t, router.Refresh(context.Background()))
			waits := 0
			router.retryWait = func(_ context.Context, delay time.Duration) error {
				assert.GreaterOrEqual(t, delay, 3*time.Second)
				waits++
				return nil
			}
			calls := 0
			failure := retryError(action, "3s")
			err = router.runWithRetry(context.Background(), func(context.Context) error {
				calls++
				return failure
			})
			assert.Same(t, failure, err, "exhaustion must preserve the last error")
			wantCalls := 3
			if action == RetryNever {
				wantCalls = 1
			}
			assert.Equal(t, wantCalls, calls)
			assert.Equal(t, wantCalls-1, waits, "no wait after the final attempt")
			wantLoads := 1
			if action == RetryAfterRefresh {
				wantLoads = 3
			}
			assert.Equal(t, wantLoads, loads)
		})
	}
}

func TestRetryBudgetsIncludeRouteRPCAndWait(t *testing.T) {
	for _, stage := range []string{"route", "rpc", "wait", "caller"} {
		t.Run(stage, func(t *testing.T) {
			policy := DefaultRetryPolicy()
			policy.MaxElapsed = 20 * time.Millisecond
			router, err := NewRouter(
				func(ctx context.Context, _ int64) (*sequencev1.GetRouteResponse, error) {
					<-ctx.Done()
					return nil, ctx.Err()
				},
				WithRetryPolicy(policy),
			)
			require.NoError(t, err)
			ctx := context.Background()
			if stage == "caller" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, time.Millisecond)
				defer cancel()
			}
			calls := 0
			err = router.runWithRetry(ctx, func(callCtx context.Context) error {
				calls++
				switch stage {
				case "route":
					return router.Refresh(callCtx)
				case "rpc", "caller":
					<-callCtx.Done()
					return callCtx.Err()
				default:
					return retryError(RetryAfterBackoff, "1h")
				}
			})
			assert.ErrorIs(t, err, context.DeadlineExceeded)
			assert.Equal(t, 1, calls)
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	router := mustRouter(t, testRoute(1, "node-a"))
	router.retryWait = func(ctx context.Context, delay time.Duration) error {
		cancel()
		return waitRetry(ctx, delay)
	}
	err := router.runWithRetry(
		ctx,
		func(context.Context) error { return retryError(RetryAfterBackoff, "") },
	)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestRetryRefreshFailureIsBounded(t *testing.T) {
	failure := status.Error(codes.Unavailable, "directory unavailable")
	loads := 0
	router, err := NewRouter(func(context.Context, int64) (*sequencev1.GetRouteResponse, error) {
		loads++
		return nil, failure
	})
	require.NoError(t, err)
	require.NoError(t, router.Update(testRoute(1, "node-a")))
	router.retryWait = func(context.Context, time.Duration) error { return nil }
	calls := 0
	err = router.runWithRetry(context.Background(), func(context.Context) error {
		calls++
		return failure
	})
	assert.Same(t, failure, err)
	assert.Equal(t, 1, calls)
	assert.Equal(t, 7, loads)
}
