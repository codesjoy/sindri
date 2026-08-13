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

func TestSequenceInterceptorRefreshesAndRetriesUnavailableOnce(t *testing.T) {
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
	assert.Equal(t, 2, calls)
	assert.Equal(t, 2, loads)
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

func TestInterceptorProviderContract(t *testing.T) {
	router, err := NewRouter(func(context.Context, int64) (*sequencev1.GetRouteResponse, error) {
		return nil, errors.New("unused")
	})
	require.NoError(t, err)
	provider := NewUnaryClientInterceptorProvider(router)
	assert.Equal(t, InterceptorName, provider.Name())
	assert.IsType(t, interceptor.UnaryClientInterceptor(nil), provider.New("svc"))
}
