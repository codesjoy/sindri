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

package service

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/codesjoy/pkg/basic/xerror"
	"github.com/codesjoy/sindri/gen/go/sequence/reason"
	sequencev1 "github.com/codesjoy/sindri/gen/go/sequence/v1"
	testkit "github.com/codesjoy/sindri/internal/pkg/tests"
	"github.com/codesjoy/sindri/internal/sequence/biz"
	sequencepkg "github.com/codesjoy/sindri/pkg/sequence"
	"github.com/codesjoy/yggdrasil/v3/rpc/metadata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/code"
)

type sequenceStore struct {
	max int64
	// err, when set, is what a reservation reports instead of granting a range. It
	// lets a test drive the authority failures the service has to translate.
	err error
}

type testMemorySampler struct{}

func (testMemorySampler) MemoryUsage() (uint64, uint64) { return 1, 100 }

// readyAllocator builds the allocator the service tests drive.
//
// It carries a pause bound because the allocator measures every in-memory
// linearisation against one: a zero bound would discard every allocation as a
// stall and fence the slot. Production injects this from the validated HA
// section rather than defaulting it here.
func readyAllocator(store biz.SequenceRepo) *biz.Allocator {
	plane := biz.DataPlaneConfig{
		Allocator: biz.AllocatorConfig{DefaultStep: 10, MaxStep: 100},
		Node:      biz.NodeConfig{ID: "node-a"},
	}
	if err := testkit.DecodeDefaults(&plane); err != nil {
		panic(err)
	}
	return biz.NewAllocator(
		plane,
		store,
		nil,
		testMemorySampler{},
		nil,
	)
}

// segmentedService builds a service whose route carries the ownership view, so
// the epoch and layout checks of appendix A.3 have something to compare against.
func segmentedService(
	t *testing.T,
	key string,
	epoch uint64,
	layout int64,
) (*SequenceService, *biz.Allocator) {
	t.Helper()
	return segmentedServiceWithStore(t, key, epoch, layout, &sequenceStore{})
}

// segmentedServiceWithStore is segmentedService with the backing store supplied,
// so a test can push a specific authority failure through the whole service path
// and check what the caller is actually told about it.
func segmentedServiceWithStore(
	t *testing.T,
	key string,
	epoch uint64,
	layout int64,
	store biz.SequenceRepo,
) (*SequenceService, *biz.Allocator) {
	t.Helper()
	slot := biz.SlotForKey(key)
	allocator := readyAllocator(store)
	allocator.Open(2, 0, []uint32{slot})
	allocator.ApplyRoute(0)
	route := biz.NewRouteCache()
	route.UpdateRoute(&biz.Route{
		Version:       2,
		LayoutVersion: layout,
		Nodes:         []biz.RouteNode{{NodeID: "node-a", Slots: []uint32{slot}}},
		Segments: []biz.RouteSegment{{
			StartSlot:       0,
			EndSlot:         biz.SlotCount - 1,
			OwnerNodeID:     "node-a",
			OwnerInstanceID: "instance-a",
			Epoch:           epoch,
		}},
	})
	return NewSequenceService(allocator, route), allocator
}

func withRequestMetadata(ctx context.Context, pairs map[string]string) context.Context {
	return metadata.WithInContext(ctx, metadata.New(pairs))
}

// TestFetchNextRefusesACallerAheadOfTheSlotEpoch pins the half of A.3 that is not
// symmetric: a caller that knows a newer epoch than this node has seen must be
// sent to refresh rather than answered from a directory it has already passed.
func TestFetchNextRefusesACallerAheadOfTheSlotEpoch(t *testing.T) {
	service, _ := segmentedService(t, "orders", 4, 1)
	ctx := withRequestMetadata(context.Background(), map[string]string{
		sequencepkg.SlotEpochMetaKey: "5",
	})

	_, err := service.FetchNext(ctx, &sequencev1.FetchNextRequest{Key: "orders"})
	require.Error(t, err)
	assert.True(t, xerror.IsReason(err, reason.Reason_SEQUENCE_EPOCH_STALE))

	_, _, meta, ok := xerror.ReasonOf(err)
	require.True(t, ok, "the refusal must carry the A.4 envelope")
	assert.Equal(t, sequencepkg.RetryAfterRefresh, meta[sequencepkg.RetryableMetaKey])
	assert.Equal(t, "4", meta[sequencepkg.SlotEpochMetaKey])
	assert.Equal(t, "node-a", meta[sequencepkg.OwnerHintMetaKey])
}

// TestFetchNextServesACallerBehindTheSlotEpoch is the other direction: a caller
// that is behind is still served. The response carries the epoch that supersedes
// the one it sent, which is the refresh it needs (A.5); that field is pinned by
// TestFetchNextResponseCarriesTheSlotEpoch, and what matters here is that the
// stale hint does not itself cause a refusal.
func TestFetchNextServesACallerBehindTheSlotEpoch(t *testing.T) {
	service, _ := segmentedService(t, "orders", 4, 1)
	ctx := withRequestMetadata(context.Background(), map[string]string{
		sequencepkg.SlotEpochMetaKey: "3",
	})

	response, err := service.FetchNext(ctx, &sequencev1.FetchNextRequest{Key: "orders"})
	require.NoError(t, err)
	assert.Positive(t, response.GetId())
}

// TestFetchNextRefusesALayoutVersionMismatch covers the layout half of A.3: a
// caller hashing under another layout sends slot numbers that mean different
// keys, so answering would be answering for the wrong slot.
func TestFetchNextRefusesALayoutVersionMismatch(t *testing.T) {
	service, _ := segmentedService(t, "orders", 4, 7)
	ctx := withRequestMetadata(context.Background(), map[string]string{
		sequencepkg.LayoutVersionMetaKey: "8",
	})

	_, err := service.FetchNext(ctx, &sequencev1.FetchNextRequest{Key: "orders"})
	require.Error(t, err)
	assert.True(t, xerror.IsReason(err, reason.Reason_SEQUENCE_ROUTE_EXPIRED))
}

// TestFetchNextFailureCarriesTheProtocolEnvelope pins the appendix A.4 fields a
// caller cannot derive from the reason and code alone.
func TestFetchNextFailureCarriesTheProtocolEnvelope(t *testing.T) {
	service, allocator := segmentedService(t, "orders", 4, 1)
	allocator.Pause()

	_, err := service.FetchNext(context.Background(), &sequencev1.FetchNextRequest{Key: "orders"})
	require.Error(t, err)
	assert.True(t, xerror.IsReason(err, reason.Reason_SEQUENCE_ALLOCATOR_PAUSED))

	_, _, meta, ok := xerror.ReasonOf(err)
	require.True(t, ok, "the refusal must carry the A.4 envelope")
	assert.Equal(t, sequencepkg.RetryAfterBackoff, meta[sequencepkg.RetryableMetaKey])
	assert.Equal(t, "4", meta[sequencepkg.SlotEpochMetaKey])
	assert.Equal(t, "node-a", meta[sequencepkg.OwnerHintMetaKey])
}

// TestFetchNextCarriesTheEnvelopeForAnAuthorityFailure pins appendix A.4 for the
// failures that come from the range authority rather than from the allocator's own
// state. They reach the service as bare sentinels unless something maps them, and
// the envelope is what tells the caller how to react: an uncertain commit in
// particular has no safe resolution except to discard the range and retry, so a
// caller that cannot recognise it cannot retry safely at all.
//
// The slot-not-owned case ends as a stale directory rather than as itself, and
// that is the point of mapping it: FetchNext reads that reason as "the caller is
// pointed at the wrong node" and sends it to refresh the route, which is the only
// useful reaction to a node that lost the slot. Pinning it here keeps the two
// reserve-time ownership failures from being collapsed into one reaction.
func TestFetchNextCarriesTheEnvelopeForAnAuthorityFailure(t *testing.T) {
	cases := []struct {
		name   string
		store  error
		reason reason.Reason
		retry  string
	}{
		{
			name:   "uncertain commit",
			store:  fmt.Errorf("%w: connection reset by peer", biz.ErrCommitUncertain),
			reason: reason.Reason_SEQUENCE_COMMIT_UNCERTAIN,
			retry:  sequencepkg.RetryAfterBackoff,
		},
		{
			name:   "lease expired",
			store:  fmt.Errorf("reserve ranges: %w: slot 3", biz.ErrLeaseExpired),
			reason: reason.Reason_SEQUENCE_LEASE_EXPIRED,
			retry:  sequencepkg.RetryAfterRefresh,
		},
		{
			name:   "slot not owned",
			store:  fmt.Errorf("reserve ranges: %w: slot 3", biz.ErrSlotNotOwned),
			reason: reason.Reason_SEQUENCE_ROUTE_EXPIRED,
			retry:  sequencepkg.RetryAfterRefresh,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			service, _ := segmentedServiceWithStore(
				t,
				"orders",
				4,
				1,
				&sequenceStore{err: tc.store},
			)
			// A real client always sends the directory revision with the request,
			// which is what lets the service tell "you are behind" from "refresh".
			ctx := withRequestMetadata(context.Background(), map[string]string{
				sequencepkg.VersionMetaKey: "2",
			})

			_, err := service.FetchNext(ctx, &sequencev1.FetchNextRequest{Key: "orders"})
			require.Error(t, err)
			assert.True(t, xerror.IsReason(err, tc.reason), "want %s, got %v", tc.reason, err)

			_, _, meta, ok := xerror.ReasonOf(err)
			require.True(t, ok, "the refusal must carry the A.4 envelope")
			assert.Equal(t, tc.retry, meta[sequencepkg.RetryableMetaKey])
			assert.Equal(t, "node-a", meta[sequencepkg.OwnerHintMetaKey])
		})
	}
}

// TestFetchNextIgnoresMalformedOptionalMetadata pins that the appendix A.3 hints
// stay optional: a caller that sends nothing, or sends nonsense, is served or
// refused on the request's own merits rather than being answered with a
// permanent failure it could never resolve.
func TestFetchNextIgnoresMalformedOptionalMetadata(t *testing.T) {
	service, _ := segmentedService(t, "orders", 4, 1)
	for _, pairs := range []map[string]string{
		{},
		{sequencepkg.SlotEpochMetaKey: "not-a-number"},
		{sequencepkg.LayoutVersionMetaKey: "-"},
	} {
		ctx := withRequestMetadata(context.Background(), pairs)
		response, err := service.FetchNext(
			ctx,
			&sequencev1.FetchNextRequest{Key: "orders"},
		)
		require.NoError(t, err, "%v", pairs)
		assert.Positive(t, response.GetId())
	}
}

func (s *sequenceStore) ReserveRanges(
	_ context.Context,
	_ biz.ReservationAuthority,
	requests []biz.ReservationRequest,
) ([]biz.SequenceRange, error) {
	if s.err != nil {
		return nil, s.err
	}
	reserved := make([]biz.SequenceRange, len(requests))
	for index, request := range requests {
		start := s.max + 1
		s.max += request.Step
		reserved[index] = biz.SequenceRange{Start: start, End: s.max}
	}
	return reserved, nil
}

func readyService(t *testing.T, key string) (*SequenceService, *biz.Allocator, *biz.RouteCache) {
	t.Helper()
	allocator := readyAllocator(&sequenceStore{})
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
	assert.Equal(t, uint32(1), response.Count)
}

func TestFetchNextReturnsContiguousBlock(t *testing.T) {
	svc, _, _ := readyService(t, "orders")
	count := uint32(3)
	response, err := svc.FetchNext(
		context.Background(),
		&sequencev1.FetchNextRequest{Key: "orders", Count: &count},
	)
	require.NoError(t, err)
	assert.Equal(t, int64(1), response.Id)
	assert.Equal(t, uint32(3), response.Count)

	next, err := svc.FetchNext(
		context.Background(),
		&sequencev1.FetchNextRequest{Key: "orders"},
	)
	require.NoError(t, err)
	assert.Equal(t, int64(4), next.Id)
	assert.Equal(t, uint32(1), next.Count)
}

func TestFetchNextBatchSuccess(t *testing.T) {
	keys := sameSlotKeys(t, "orders", 3)
	svc, _, _ := readyService(t, keys[0])
	response, err := svc.FetchNextBatch(
		context.Background(),
		&sequencev1.FetchNextBatchRequest{Requests: []*sequencev1.FetchNextRequest{
			{Key: keys[0]},
			{Key: keys[1], Count: uint32Pointer(2)},
			{Key: keys[2], Count: uint32Pointer(3)},
		}},
	)
	require.NoError(t, err)
	require.Len(t, response.Results, 3)
	assert.Equal(t, keys[0], response.Results[0].Key)
	assert.Equal(t, int64(1), response.Results[0].Id)
	assert.Equal(t, uint32(1), response.Results[0].Count)
	assert.Equal(t, keys[1], response.Results[1].Key)
	assert.Equal(t, int64(11), response.Results[1].Id)
	assert.Equal(t, uint32(2), response.Results[1].Count)
	assert.Equal(t, keys[2], response.Results[2].Key)
	assert.Equal(t, int64(21), response.Results[2].Id)
	assert.Equal(t, uint32(3), response.Results[2].Count)
}

func TestFetchNextBatchRejectsInvalidRequests(t *testing.T) {
	svc, _, _ := readyService(t, "orders")
	keys := sameSlotKeys(t, "orders", 2)

	tests := []struct {
		name    string
		request *sequencev1.FetchNextBatchRequest
	}{
		{name: "nil", request: nil},
		{name: "empty", request: &sequencev1.FetchNextBatchRequest{}},
		{name: "duplicate", request: &sequencev1.FetchNextBatchRequest{
			Requests: []*sequencev1.FetchNextRequest{
				{Key: keys[0]},
				{Key: keys[0]},
			},
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := svc.FetchNextBatch(context.Background(), test.request)
			assert.True(t, xerror.IsCode(err, code.Code_INVALID_ARGUMENT))
		})
	}
}

func TestFetchNextBatchRejectsExcessiveTotalCount(t *testing.T) {
	keys := sameSlotKeys(t, "orders", 11)
	svc, _, _ := readyService(t, keys[0])
	requests := make([]*sequencev1.FetchNextRequest, len(keys))
	for index, key := range keys {
		requests[index] = &sequencev1.FetchNextRequest{
			Key:   key,
			Count: uint32Pointer(biz.MaxIDsPerKey),
		}
	}
	_, err := svc.FetchNextBatch(
		context.Background(),
		&sequencev1.FetchNextBatchRequest{Requests: requests},
	)
	assert.True(t, xerror.IsCode(err, code.Code_INVALID_ARGUMENT))
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
		readyAllocator(&sequenceStore{}),
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

// TestFetchNextResponseCarriesTheSlotEpoch pins the translation rather than the
// value. The epoch is the client-visible half of the linearization record, so
// dropping it while copying the allocation would quietly remove the only
// evidence a caller can compare across two responses for one key.
func TestFetchNextResponseCarriesTheSlotEpoch(t *testing.T) {
	response := fetchNextResponse(biz.SequenceAllocation{ID: 7, Count: 2, SlotEpoch: 5})
	assert.Equal(t, int64(7), response.Id)
	assert.Equal(t, uint32(2), response.Count)
	assert.Equal(t, uint64(5), response.SlotEpoch)
}

func TestGetRoutePublishesTheOwnershipView(t *testing.T) {
	route := biz.NewRouteCache()
	route.UpdateRoute(&biz.Route{
		Version:       5,
		LayoutVersion: 1,
		Nodes:         []biz.RouteNode{{NodeID: "node-a", Slots: []uint32{0, 1}}},
		Segments: []biz.RouteSegment{{
			StartSlot:       0,
			EndSlot:         1,
			OwnerNodeID:     "node-a",
			OwnerInstanceID: "instance-a",
			Epoch:           9,
		}},
	})

	response, err := NewSequenceService(nil, route).GetRoute(
		context.Background(),
		&sequencev1.GetRouteRequest{},
	)
	require.NoError(t, err)
	require.NotNil(t, response.Route)
	assert.Equal(t, int64(1), response.Route.LayoutVersion)
	require.Len(t, response.Route.Nodes, 1)
	require.Len(t, response.Route.Segments, 1)
	assert.Equal(t, "instance-a", response.Route.Segments[0].OwnerInstanceId)
	assert.Equal(t, uint64(9), response.Route.Segments[0].SlotEpoch)
}

// TestGetRouteOmitsEpochsForALegacySnapshot covers the snapshots that already
// exist in deployed databases: they state ownership through nodes alone, so a
// caller must receive no epoch rather than a zero it would read as a real one.
func TestGetRouteOmitsEpochsForALegacySnapshot(t *testing.T) {
	route := biz.NewRouteCache()
	route.UpdateRoute(&biz.Route{
		Version: 6,
		Nodes:   []biz.RouteNode{{NodeID: "node-a", Slots: []uint32{0}}},
	})

	response, err := NewSequenceService(nil, route).GetRoute(
		context.Background(),
		&sequencev1.GetRouteRequest{},
	)
	require.NoError(t, err)
	require.NotNil(t, response.Route)
	assert.Equal(t, int64(0), response.Route.LayoutVersion)
	assert.Empty(t, response.Route.Segments)
	require.Len(t, response.Route.Nodes, 1)
}

func sameSlotKeys(t *testing.T, base string, count int) []string {
	t.Helper()
	slot := biz.SlotForKey(base)
	keys := []string{base}
	for candidate := 0; len(keys) < count; candidate++ {
		key := base + "-" + strconv.Itoa(candidate)
		if biz.SlotForKey(key) == slot {
			keys = append(keys, key)
		}
	}
	return keys
}

func uint32Pointer(value uint32) *uint32 { return &value }
