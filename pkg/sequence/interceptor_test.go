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
	"strconv"
	"testing"
	"time"

	"github.com/codesjoy/pkg/basic/xerror"
	"github.com/codesjoy/sindri/gen/go/sequence/reason"
	sequencev1 "github.com/codesjoy/sindri/gen/go/sequence/v1"
	"github.com/codesjoy/yggdrasil/v3/rpc/interceptor"
	"github.com/codesjoy/yggdrasil/v3/rpc/metadata"
	"github.com/codesjoy/yggdrasil/v3/rpc/status"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/code"
)

// keyInSlotRange returns a key that hashes into a slot range, so a test can build
// a batch whose keys land in chosen segments.
func keyInSlotRange(t *testing.T, from, to uint32) string {
	t.Helper()
	for i := 0; i < 1_000_000; i++ {
		key := "key-" + strconv.Itoa(i)
		if slot := SlotForKey(key); slot >= from && slot < to {
			return key
		}
	}
	t.Fatalf("no key hashes into slots [%d,%d)", from, to)
	return ""
}

// TestSequenceInterceptorSendsTheLayoutAndEpoch pins appendix A.3 from the caller
// side: every attempt carries the slot layout its keys were hashed under and the
// epoch it believes the target slot is at, which is what lets an owner tell a
// caller that is behind from one that is ahead.
func TestSequenceInterceptorSendsTheLayoutAndEpoch(t *testing.T) {
	router := newSegmentedTestRouter(t, testSegmentedRoute(3, 9,
		testSegment{nodeID: "node-a", epoch: 5, from: 0, to: SlotCount - 1},
	))
	middleware := newUnaryClientInterceptor(router)
	reply := &sequencev1.FetchNextResponse{}
	calls := 0
	err := middleware(
		context.Background(),
		fetchNextFullMethod,
		&sequencev1.FetchNextRequest{Key: "orders"},
		reply,
		func(ctx context.Context, _ string, _, response any) error {
			calls++
			outgoing, ok := metadata.FromOutContext(ctx)
			require.True(t, ok)
			assert.Equal(t, []string{"9"}, outgoing.Get(LayoutVersionMetaKey))
			assert.Equal(t, []string{"5"}, outgoing.Get(SlotEpochMetaKey))
			response.(*sequencev1.FetchNextResponse).Id = 42
			return nil
		},
	)
	require.NoError(t, err)
	assert.Equal(t, 1, calls)
	assert.Equal(t, int64(42), reply.GetId())
}

// TestSequenceInterceptorSendsNoEpochWithoutAnOwnershipView pins the other half:
// a snapshot that carries no segments must not make the caller invent an epoch,
// because an invented one is a claim about ownership the caller cannot make.
func TestSequenceInterceptorSendsNoEpochWithoutAnOwnershipView(t *testing.T) {
	router := newSegmentedTestRouter(t, testRoute(3, "node-a"))
	middleware := newUnaryClientInterceptor(router)
	err := middleware(
		context.Background(),
		fetchNextFullMethod,
		&sequencev1.FetchNextRequest{Key: "orders"},
		&sequencev1.FetchNextResponse{},
		func(ctx context.Context, _ string, _, response any) error {
			outgoing, ok := metadata.FromOutContext(ctx)
			require.True(t, ok)
			assert.Empty(t, outgoing.Get(SlotEpochMetaKey))
			assert.Empty(t, outgoing.Get(LayoutVersionMetaKey))
			response.(*sequencev1.FetchNextResponse).Id = 1
			return nil
		},
	)
	require.NoError(t, err)
}

// TestSequenceInterceptorSendsABatchSpanningEpochs pins that a batch is grouped
// by owner, not by epoch. The server fences every key against its own slot gate,
// so epochs may differ within one owner's group; the anchor epoch is only the
// caller's routing hint.
func TestSequenceInterceptorSendsABatchSpanningEpochs(t *testing.T) {
	third := uint32(SlotCount / 3)
	router := newSegmentedTestRouter(t, testSegmentedRoute(1, 1,
		testSegment{nodeID: "node-a", epoch: 5, from: 0, to: third - 1},
		testSegment{nodeID: "node-b", epoch: 6, from: third, to: 2*third - 1},
		testSegment{nodeID: "node-a", epoch: 7, from: 2 * third, to: SlotCount - 1},
	))
	first := keyInSlotRange(t, 0, third)
	second := keyInSlotRange(t, 2*third, SlotCount)
	request := &sequencev1.FetchNextBatchRequest{Requests: []*sequencev1.FetchNextRequest{
		{Key: first},
		{Key: second},
	}}
	calls := 0
	err := newUnaryClientInterceptor(router)(
		context.Background(),
		fetchNextBatchFullMethod,
		request,
		&sequencev1.FetchNextBatchResponse{},
		func(ctx context.Context, _ string, _, response any) error {
			calls++
			outgoing, ok := metadata.FromOutContext(ctx)
			require.True(t, ok)
			assert.Equal(t, []string{"5"}, outgoing.Get(SlotEpochMetaKey))
			response.(*sequencev1.FetchNextBatchResponse).Results = []*sequencev1.FetchNextBatchResult{
				{Id: 1, Count: 1, Key: first},
				{Id: 2, Count: 1, Key: second},
			}
			return nil
		},
	)
	require.NoError(t, err)
	assert.Equal(t, 1, calls)
}

func TestSequenceInterceptorRefreshesAndRetriesRouteErrorOnce(t *testing.T) {
	var loads int
	router, err := NewRouter(func(
		_ context.Context,
		known int64,
	) (*sequencev1.GetRouteResponse, error) {
		loads++
		return &sequencev1.GetRouteResponse{Route: testRoute(known+1, "node-a")}, nil
	})
	require.NoError(t, err)
	middleware := newUnaryClientInterceptor(router)
	reply := &sequencev1.FetchNextResponse{}
	calls := 0
	err = middleware(
		context.Background(),
		fetchNextFullMethod,
		&sequencev1.FetchNextRequest{Key: "orders"},
		reply,
		func(ctx context.Context, _ string, _, response any) error {
			calls++
			outgoing, ok := metadata.FromOutContext(ctx)
			require.True(t, ok)
			require.Equal(t, []string{strconv.Itoa(calls)}, outgoing.Get(VersionMetaKey))
			slot, ok := SlotFromContext(ctx)
			require.True(t, ok)
			require.Equal(t, SlotForKey("orders"), slot)
			if calls == 1 {
				response.(*sequencev1.FetchNextResponse).Id = 999
				return status.FromError(xerror.NewWithReason(
					reason.Reason_SEQUENCE_ROUTE_EXPIRED,
					"stale",
					nil,
				))
			}
			require.Zero(t, response.(*sequencev1.FetchNextResponse).GetId())
			response.(*sequencev1.FetchNextResponse).Id = 42
			return nil
		},
	)
	require.NoError(t, err)
	assert.Equal(t, int64(42), reply.GetId())
	assert.Equal(t, 2, calls)
	assert.Equal(t, 2, loads)
}

func TestSequenceInterceptorBoundsUnavailableRetries(t *testing.T) {
	var loads int
	router, err := NewRouter(func(
		_ context.Context,
		known int64,
	) (*sequencev1.GetRouteResponse, error) {
		loads++
		return &sequencev1.GetRouteResponse{Route: testRoute(known+1, "node-a")}, nil
	})
	require.NoError(t, err)
	router.retryWait = func(context.Context, time.Duration) error { return nil }
	middleware := newUnaryClientInterceptor(router)
	calls := 0
	err = middleware(
		context.Background(),
		fetchNextFullMethod,
		&sequencev1.FetchNextRequest{Key: "orders"},
		&sequencev1.FetchNextResponse{},
		func(context.Context, string, any, any) error {
			calls++
			return xerror.New(code.Code_UNAVAILABLE, "owner unavailable")
		},
	)
	require.Error(t, err)
	assert.True(t, xerror.IsCode(err, code.Code_UNAVAILABLE))
	assert.Equal(t, DefaultRetryPolicy().MaxAttempts, calls)
	assert.Equal(t, DefaultRetryPolicy().MaxAttempts, loads)
}

func TestSequenceInterceptorReturnsRefreshFailureAfterUnavailable(t *testing.T) {
	wantErr := errors.New("refresh failed")
	loads := 0
	router, err := NewRouter(func(
		context.Context,
		int64,
	) (*sequencev1.GetRouteResponse, error) {
		loads++
		if loads == 1 {
			return &sequencev1.GetRouteResponse{Route: testRoute(1, "node-a")}, nil
		}
		return nil, wantErr
	})
	require.NoError(t, err)
	middleware := newUnaryClientInterceptor(router)
	calls := 0
	err = middleware(
		context.Background(),
		fetchNextFullMethod,
		&sequencev1.FetchNextRequest{Key: "orders"},
		&sequencev1.FetchNextResponse{},
		func(context.Context, string, any, any) error {
			calls++
			return xerror.New(code.Code_UNAVAILABLE, "owner unavailable")
		},
	)
	require.ErrorIs(t, err, wantErr)
	assert.Equal(t, 1, calls)
	assert.Equal(t, 2, loads)
}

func TestSequenceInterceptorDoesNotRetryNonRouteErrors(t *testing.T) {
	router, err := NewRouter(func(context.Context, int64) (*sequencev1.GetRouteResponse, error) {
		return nil, errors.New("unexpected refresh")
	})
	require.NoError(t, err)
	require.NoError(t, router.Update(testRoute(1, "node-a")))
	middleware := newUnaryClientInterceptor(router)
	wantErr := errors.New("connection reset")
	calls := 0
	err = middleware(
		context.Background(),
		fetchNextFullMethod,
		&sequencev1.FetchNextRequest{Key: "orders"},
		&sequencev1.FetchNextResponse{},
		func(context.Context, string, any, any) error {
			calls++
			return wantErr
		},
	)
	require.ErrorIs(t, err, wantErr)
	assert.Equal(t, 1, calls)
}

func TestSequenceInterceptorDoesNotRetryTerminalErrors(t *testing.T) {
	for _, testCase := range []struct {
		name string
		err  error
	}{
		{name: "cancelled", err: xerror.New(code.Code_CANCELLED, "cancelled")},
		{name: "deadline", err: xerror.New(code.Code_DEADLINE_EXCEEDED, "deadline")},
		{name: "validation", err: xerror.New(code.Code_INVALID_ARGUMENT, "invalid")},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			loads := 0
			router, err := NewRouter(func(
				context.Context,
				int64,
			) (*sequencev1.GetRouteResponse, error) {
				loads++
				return &sequencev1.GetRouteResponse{Route: testRoute(1, "node-a")}, nil
			})
			require.NoError(t, err)
			middleware := newUnaryClientInterceptor(router)
			calls := 0
			err = middleware(
				context.Background(),
				fetchNextFullMethod,
				&sequencev1.FetchNextRequest{Key: "orders"},
				&sequencev1.FetchNextResponse{},
				func(context.Context, string, any, any) error {
					calls++
					return testCase.err
				},
			)
			require.ErrorIs(t, err, testCase.err)
			assert.Equal(t, 1, calls)
			assert.Equal(t, 1, loads)
		})
	}
}

func TestSequenceInterceptorPassesOtherMethodsThrough(t *testing.T) {
	router, err := NewRouter(func(context.Context, int64) (*sequencev1.GetRouteResponse, error) {
		return nil, errors.New("unexpected refresh")
	})
	require.NoError(t, err)
	middleware := newUnaryClientInterceptor(router)
	calls := 0
	err = middleware(
		context.Background(),
		getRouteFullMethod,
		&sequencev1.GetRouteRequest{},
		&sequencev1.GetRouteResponse{},
		func(context.Context, string, any, any) error {
			calls++
			return nil
		},
	)
	require.NoError(t, err)
	assert.Equal(t, 1, calls)
}

func TestSequenceInterceptorRejectsReservedMetadata(t *testing.T) {
	router, err := NewRouter(func(context.Context, int64) (*sequencev1.GetRouteResponse, error) {
		return nil, errors.New("unexpected refresh")
	})
	require.NoError(t, err)
	middleware := newUnaryClientInterceptor(router)
	ctx := metadata.WithOutContext(context.Background(), metadata.Pairs(VersionMetaKey, "1"))
	err = middleware(
		ctx,
		fetchNextFullMethod,
		&sequencev1.FetchNextRequest{Key: "orders"},
		&sequencev1.FetchNextResponse{},
		func(context.Context, string, any, any) error { return nil },
	)
	require.ErrorIs(t, err, ErrReservedRouteMetadata)
}

func TestSequenceInterceptorValidatesRequestedCount(t *testing.T) {
	router, err := NewRouter(func(context.Context, int64) (*sequencev1.GetRouteResponse, error) {
		return nil, errors.New("unexpected refresh")
	})
	require.NoError(t, err)
	require.NoError(t, router.Update(testRoute(1, "node-a")))
	middleware := newUnaryClientInterceptor(router)
	count := uint32(3)

	err = middleware(
		context.Background(),
		fetchNextFullMethod,
		&sequencev1.FetchNextRequest{Key: "orders", Count: &count},
		&sequencev1.FetchNextResponse{},
		func(context.Context, string, any, any) error {
			// Simulate an old service that ignored count and returned one ID.
			return nil
		},
	)
	require.ErrorIs(t, err, ErrCountUnsupported)

	err = middleware(
		context.Background(),
		fetchNextFullMethod,
		&sequencev1.FetchNextRequest{Key: "orders"},
		&sequencev1.FetchNextResponse{},
		func(context.Context, string, any, any) error {
			return nil
		},
	)
	require.NoError(t, err, "legacy count=0 response is accepted for a single ID")
}

func TestSequenceInterceptorRoutesHomogeneousBatch(t *testing.T) {
	router, err := NewRouter(func(context.Context, int64) (*sequencev1.GetRouteResponse, error) {
		return nil, errors.New("unexpected refresh")
	})
	require.NoError(t, err)
	require.NoError(t, router.Update(testRoute(1, "node-a")))
	middleware := newUnaryClientInterceptor(router)
	requests := []*sequencev1.FetchNextRequest{{Key: "orders"}, {Key: "invoices"}}
	reply := &sequencev1.FetchNextBatchResponse{}

	err = middleware(
		context.Background(),
		fetchNextBatchFullMethod,
		&sequencev1.FetchNextBatchRequest{Requests: requests},
		reply,
		func(ctx context.Context, _ string, _, response any) error {
			slot, ok := SlotFromContext(ctx)
			require.True(t, ok)
			assert.Equal(t, SlotForKey("orders"), slot)
			outgoing, ok := metadata.FromOutContext(ctx)
			require.True(t, ok)
			assert.Equal(t, []string{"1"}, outgoing.Get(VersionMetaKey))
			response.(*sequencev1.FetchNextBatchResponse).Results = []*sequencev1.FetchNextBatchResult{
				{Key: "orders", Id: 1, Count: 1},
				{Key: "invoices", Id: 1, Count: 1},
			}
			return nil
		},
	)
	require.NoError(t, err)
	require.Len(t, reply.GetResults(), 2)
}

func TestSequenceInterceptorRejectsMixedOwnerBatch(t *testing.T) {
	router, err := NewRouter(func(context.Context, int64) (*sequencev1.GetRouteResponse, error) {
		return nil, errors.New("unexpected refresh")
	})
	require.NoError(t, err)
	require.NoError(t, router.Update(testRoute(1, "node-a", "node-b")))
	middleware := newUnaryClientInterceptor(router)

	nodeA := sameOwnerRequests(t, "node-a", 1)
	nodeB := sameOwnerRequests(t, "node-b", 1)
	requests := []*sequencev1.FetchNextRequest{
		{Key: nodeA[0].Key},
		{Key: nodeB[0].Key},
	}
	calls := 0
	err = middleware(
		context.Background(),
		fetchNextBatchFullMethod,
		&sequencev1.FetchNextBatchRequest{Requests: requests},
		&sequencev1.FetchNextBatchResponse{},
		func(context.Context, string, any, any) error {
			calls++
			return nil
		},
	)
	require.ErrorIs(t, err, ErrBatchRouteChanged)
	assert.Zero(t, calls)
}

func TestSequenceInterceptorRefreshesAndRetriesBatchOnce(t *testing.T) {
	loads := 0
	router, err := NewRouter(func(
		context.Context,
		int64,
	) (*sequencev1.GetRouteResponse, error) {
		loads++
		return &sequencev1.GetRouteResponse{Route: testRoute(int64(loads), "node-a")}, nil
	})
	require.NoError(t, err)
	middleware := newUnaryClientInterceptor(router)
	request := &sequencev1.FetchNextBatchRequest{
		Requests: []*sequencev1.FetchNextRequest{{Key: "orders"}},
	}
	calls := 0
	err = middleware(
		context.Background(),
		fetchNextBatchFullMethod,
		request,
		&sequencev1.FetchNextBatchResponse{},
		func(_ context.Context, _ string, _, response any) error {
			calls++
			out := response.(*sequencev1.FetchNextBatchResponse)
			if calls == 1 {
				out.Results = []*sequencev1.FetchNextBatchResult{
					{Key: "orders", Id: 999, Count: 1},
				}
				return status.FromError(xerror.NewWithReason(
					reason.Reason_SEQUENCE_ROUTE_EXPIRED,
					"stale",
					nil,
				))
			}
			require.Empty(t, out.GetResults())
			out.Results = []*sequencev1.FetchNextBatchResult{
				{Key: "orders", Id: 42, Count: 1},
			}
			return nil
		},
	)
	require.NoError(t, err)
	assert.Equal(t, 2, calls)
}

func TestInterceptorProviderContract(t *testing.T) {
	router, err := NewRouter(func(context.Context, int64) (*sequencev1.GetRouteResponse, error) {
		return nil, errors.New("unused")
	})
	require.NoError(t, err)
	provider := NewUnaryClientInterceptorProvider(router)
	assert.Equal(t, InterceptorName, provider.Name())
	assert.IsType(t, interceptor.UnaryClientInterceptor(nil), provider.New("svc"))
}
