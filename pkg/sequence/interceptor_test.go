package sequence

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/codesjoy/pkg/basic/xerror"
	"github.com/codesjoy/skuld/gen/go/reason"
	sequencev1 "github.com/codesjoy/skuld/gen/go/sequence/v1"
	"github.com/codesjoy/yggdrasil/v3/rpc/interceptor"
	"github.com/codesjoy/yggdrasil/v3/rpc/metadata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
				return xerror.NewWithReason(reason.Reason_SEQUENCE_ROUTE_EXPIRED, "stale", nil)
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
