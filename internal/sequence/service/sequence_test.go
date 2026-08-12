package service

import (
	"context"
	"testing"

	"github.com/codesjoy/pkg/basic/xerror"
	"github.com/codesjoy/skuld/gen/go/reason"
	sequencev1 "github.com/codesjoy/skuld/gen/go/sequence/v1"
	"github.com/codesjoy/skuld/internal/sequence/biz"
	sequencepkg "github.com/codesjoy/skuld/pkg/sequence"
	"github.com/codesjoy/yggdrasil/v3/rpc/metadata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/code"
)

type sequenceStore struct{ max int64 }

func (s *sequenceStore) ReserveRange(
	_ context.Context,
	_ string,
	step int64,
) (biz.SequenceRange, error) {
	start := s.max + 1
	s.max += step
	return biz.SequenceRange{Start: start, End: s.max}, nil
}

func readyService(t *testing.T, key string) (*SequenceService, *biz.Allocator, *biz.RouteCache) {
	t.Helper()
	allocator := biz.NewAllocator(&biz.AllocatorConfig{Step: 10}, &sequenceStore{}, nil)
	allocator.Open(2, 0, []uint32{biz.SlotForKey(key)})
	allocator.ApplyRoute(0)
	route := biz.NewRouteCache()
	route.UpdateRoute(&biz.Route{Version: 2, Nodes: []biz.RouteNode{{
		NodeID: "node-a", Slots: []uint32{biz.SlotForKey(key)},
	}}})
	return NewSequenceService(allocator, route), allocator, route
}

func TestFetchNextSuccess(t *testing.T) {
	svc, _, _ := readyService(t, "orders")
	response, err := svc.FetchNext(
		context.Background(),
		&sequencev1.FetchNextRequest{Key: "orders"},
	)
	require.NoError(t, err)
	assert.Equal(t, int64(1), response.Id)
}

func TestFetchNextReturnsPaused(t *testing.T) {
	svc, allocator, _ := readyService(t, "orders")
	allocator.Pause()
	_, err := svc.FetchNext(context.Background(), &sequencev1.FetchNextRequest{Key: "orders"})
	assert.True(t, xerror.IsReason(err, reason.Reason_SEQUENCE_ALLOCATOR_PAUSED))
}

func TestFetchNextValidatesRouteMetadata(t *testing.T) {
	svc, _, _ := readyService(t, "orders")
	key := "not-owned"
	for biz.SlotForKey(key) == biz.SlotForKey("orders") {
		key += "x"
	}

	tests := []struct {
		name string
		ctx  context.Context
	}{
		{name: "missing", ctx: context.Background()},
		{
			name: "empty",
			ctx: metadata.WithInContext(
				context.Background(),
				metadata.MD{sequencepkg.VersionMetaKey: {}},
			),
		},
		{
			name: "malformed",
			ctx: metadata.WithInContext(
				context.Background(),
				metadata.New(map[string]string{sequencepkg.VersionMetaKey: "bad"}),
			),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := svc.FetchNext(test.ctx, &sequencev1.FetchNextRequest{Key: key})
			assert.True(t, xerror.IsCode(err, code.Code_INVALID_ARGUMENT))
		})
	}

	ctx := metadata.WithInContext(context.Background(), metadata.New(map[string]string{
		sequencepkg.VersionMetaKey: "2",
	}))
	_, err := svc.FetchNext(ctx, &sequencev1.FetchNextRequest{Key: key})
	assert.True(t, xerror.IsReason(err, reason.Reason_SEQUENCE_ROUTE_EXPIRED))
}

func TestFetchNextStopsWaitingWhenContextIsCanceled(t *testing.T) {
	svc, _, _ := readyService(t, "orders")
	key := "not-owned"
	for biz.SlotForKey(key) == biz.SlotForKey("orders") {
		key += "x"
	}
	ctx, cancel := context.WithCancel(metadata.WithInContext(
		context.Background(),
		metadata.New(map[string]string{sequencepkg.VersionMetaKey: "3"}),
	))
	cancel()
	_, err := svc.FetchNext(ctx, &sequencev1.FetchNextRequest{Key: key})
	assert.ErrorIs(t, err, context.Canceled)
}

func TestGetRouteResponses(t *testing.T) {
	empty := NewSequenceService(
		biz.NewAllocator(&biz.AllocatorConfig{Step: 10}, &sequenceStore{}, nil),
		biz.NewRouteCache(),
	)
	_, err := empty.GetRoute(context.Background(), &sequencev1.GetRouteRequest{})
	assert.True(t, xerror.IsReason(err, reason.Reason_SEQUENCE_ROUTE_UNAVAILABLE))

	svc, _, _ := readyService(t, "orders")
	for _, knownVersion := range []int64{2, 3} {
		response, responseErr := svc.GetRoute(
			context.Background(),
			&sequencev1.GetRouteRequest{KnownVersion: knownVersion},
		)
		require.NoError(t, responseErr)
		assert.True(t, response.NotModified)
		assert.Nil(t, response.Route)
	}

	current, err := svc.GetRoute(
		context.Background(),
		&sequencev1.GetRouteRequest{KnownVersion: 1},
	)
	require.NoError(t, err)
	require.NotNil(t, current.Route)
	assert.Equal(t, int64(2), current.Route.Version)
	assert.Equal(t, "node-a", current.Route.Nodes[0].NodeId)
}
