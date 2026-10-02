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
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/codesjoy/pkg/basic/xerror"
	"github.com/codesjoy/sindri/gen/go/sequence/reason"
	sequencev1 "github.com/codesjoy/sindri/gen/go/sequence/v1"
	testkit "github.com/codesjoy/sindri/internal/pkg/tests"
	"github.com/codesjoy/sindri/internal/sequence/biz"
	"github.com/codesjoy/sindri/internal/sequence/testutil"
	sequencepkg "github.com/codesjoy/sindri/pkg/sequence"
	"github.com/codesjoy/yggdrasil/v3/rpc/metadata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type sequenceStore struct {
	mu  sync.Mutex
	max map[string]int64
	err error
}

func (s *sequenceStore) ReserveRanges(
	_ context.Context,
	_ biz.ReservationAuthority,
	req []biz.ReservationRequest,
) ([]biz.SequenceRange, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return nil, s.err
	}
	if s.max == nil {
		s.max = map[string]int64{}
	}
	result := make([]biz.SequenceRange, len(req))
	for i, r := range req {
		result[i] = biz.SequenceRange{Start: s.max[r.Key] + 1, End: s.max[r.Key] + r.Step}
		s.max[r.Key] += r.Step
	}
	return result, nil
}

func segmentedService(
	t *testing.T,
	key string,
	epoch uint64,
	layout int64,
) (*SequenceService, *biz.Allocator) {
	t.Helper()
	return segmentedServiceWithStore(t, key, epoch, layout, &sequenceStore{})
}

func segmentedServiceWithStore(
	t *testing.T,
	key string,
	epoch uint64,
	layout int64,
	store biz.SequenceRepo,
) (*SequenceService, *biz.Allocator) {
	t.Helper()
	var cfg biz.DataPlaneConfig
	cfg.Node.ID = "node-a"
	cfg.Allocator.DefaultStep = 10
	cfg.Allocator.MaxStep = 100
	require.NoError(t, testkit.DecodeDefaults(&cfg))
	authority := testutil.NewAuthority()
	authority.Epoch = epoch
	a := biz.NewAllocator(cfg, store, authority, handlerMemorySampler{}, nil)
	a.RenewLeases()
	require.NoError(t, a.Claim(context.Background(), []uint32{biz.SlotForKey(key)}))
	route := biz.NewRouteCache()
	route.UpdateRoute(
		&biz.Route{
			Version:       2,
			LayoutVersion: layout,
			Segments: []biz.RouteSegment{
				{
					StartSlot:       0,
					EndSlot:         biz.SlotCount - 1,
					OwnerNodeID:     "node-a",
					OwnerInstanceID: a.InstanceID(),
					Epoch:           epoch,
				},
			},
		},
	)
	return NewSequenceService(a, route), a
}

func withRequestMetadata(ctx context.Context, pairs map[string]string) context.Context {
	if _, ok := pairs[sequencepkg.VersionMetaKey]; !ok {
		pairs[sequencepkg.VersionMetaKey] = "2"
	}
	return metadata.WithInContext(ctx, metadata.New(pairs))
}

func requestContext() context.Context {
	return withRequestMetadata(context.Background(), map[string]string{})
}

func TestFetchNextAndBlockResponseCountEpoch(t *testing.T) {
	s, _ := segmentedService(t, "orders", 4, 1)
	first, err := s.FetchNext(requestContext(), &sequencev1.FetchNextRequest{Key: "orders"})
	require.NoError(t, err)
	assert.EqualValues(t, 1, first.Id)
	assert.EqualValues(t, 1, first.Count)
	assert.EqualValues(t, 4, first.SlotEpoch)
	count := uint32(3)
	next, err := s.FetchNext(
		requestContext(),
		&sequencev1.FetchNextRequest{Key: "orders", Count: &count},
	)
	require.NoError(t, err)
	assert.EqualValues(t, 2, next.Id)
	assert.EqualValues(t, 3, next.Count)
}

func TestCallerVersionCheckNeverWaitsForMigration(t *testing.T) {
	s, _ := segmentedService(t, "orders", 1, 1)
	for _, tc := range []struct {
		version string
		reason  reason.Reason
	}{{"1", reason.Reason_SEQUENCE_ROUTE_EXPIRED}, {"3", reason.Reason_SEQUENCE_OWNER_RECOVERING}} {
		started := time.Now()
		ctx := withRequestMetadata(
			context.Background(),
			map[string]string{sequencepkg.VersionMetaKey: tc.version},
		)
		_, err := s.FetchNext(ctx, &sequencev1.FetchNextRequest{Key: "orders"})
		require.True(t, xerror.IsReason(err, tc.reason))
		assert.Less(t, time.Since(started), 100*time.Millisecond)
	}
	for _, ctx := range []context.Context{context.Background(), metadata.WithInContext(context.Background(), metadata.Pairs(sequencepkg.VersionMetaKey, "bad")), metadata.WithInContext(context.Background(), metadata.Pairs(sequencepkg.VersionMetaKey, "0"))} {
		_, err := s.FetchNext(ctx, &sequencev1.FetchNextRequest{Key: "orders"})
		require.Error(t, err)
	}
}

func TestCallerLayoutAndEpochChecks(t *testing.T) {
	s, _ := segmentedService(t, "orders", 4, 1)
	for _, tc := range []struct {
		pairs map[string]string
		want  reason.Reason
	}{{map[string]string{sequencepkg.LayoutVersionMetaKey: "2"}, reason.Reason_SEQUENCE_ROUTE_EXPIRED}, {map[string]string{sequencepkg.SlotEpochMetaKey: "5"}, reason.Reason_SEQUENCE_EPOCH_STALE}} {
		_, err := s.FetchNext(
			withRequestMetadata(context.Background(), tc.pairs),
			&sequencev1.FetchNextRequest{Key: "orders"},
		)
		require.True(t, xerror.IsReason(err, tc.want))
	}
	for _, epoch := range []string{"3", "4", "bad"} {
		_, err := s.FetchNext(
			withRequestMetadata(
				context.Background(),
				map[string]string{sequencepkg.SlotEpochMetaKey: epoch},
			),
			&sequencev1.FetchNextRequest{Key: "orders"},
		)
		require.NoError(t, err)
	}
}

func TestAuthorityErrorsCarryRetryEnvelope(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want reason.Reason
	}{{biz.ErrAuthorityChanged, reason.Reason_SEQUENCE_OWNER_RECOVERING}, {biz.ErrSlotNotOwned, reason.Reason_SEQUENCE_SLOT_NOT_OWNER}, {biz.ErrLeaseExpired, reason.Reason_SEQUENCE_LEASE_EXPIRED}, {biz.ErrCommitUncertain, reason.Reason_SEQUENCE_COMMIT_UNCERTAIN}} {
		s, _ := segmentedServiceWithStore(t, "orders", 1, 1, &sequenceStore{err: tc.err})
		_, err := s.FetchNext(requestContext(), &sequencev1.FetchNextRequest{Key: "orders"})
		require.True(t, xerror.IsReason(err, tc.want))
		assert.NotEmpty(t, retryClass(tc.want))
	}
	s, a := segmentedService(t, "orders", 1, 1)
	a.Pause()
	_, err := s.FetchNext(requestContext(), &sequencev1.FetchNextRequest{Key: "orders"})
	require.True(t, xerror.IsReason(err, reason.Reason_SEQUENCE_ALLOCATOR_PAUSED))
}

func TestProtocolValidationAndBatchCountLimit(t *testing.T) {
	s, _ := segmentedService(t, "orders", 1, 1)
	excess := uint32(biz.MaxIDsPerKey + 1)
	for _, r := range []*sequencev1.FetchNextRequest{nil, {}, {Key: strings.Repeat("a", 257)}, {Key: "orders", Count: &excess}} {
		_, err := s.FetchNext(requestContext(), r)
		require.Error(t, err)
	}
	for _, r := range []*sequencev1.FetchNextBatchRequest{nil, {}, {Requests: []*sequencev1.FetchNextRequest{{Key: "orders"}, {Key: "orders"}}}, {Requests: []*sequencev1.FetchNextRequest{nil}}} {
		_, err := s.FetchNextBatch(requestContext(), r)
		require.Error(t, err)
	}
	count := uint32(biz.MaxIDsPerKey)
	var req []*sequencev1.FetchNextRequest
	for i := 0; i < int(biz.MaxIDsPerRequest)/int(count)+1; i++ {
		req = append(req, &sequencev1.FetchNextRequest{Key: strconv.Itoa(i), Count: &count})
	}
	_, err := s.FetchNextBatch(requestContext(), &sequencev1.FetchNextBatchRequest{Requests: req})
	require.Error(t, err)
}

func TestBatchResponsePreservesOrderAndIndividualCount(t *testing.T) {
	s, a := segmentedService(t, "orders", 1, 1)
	keys := []string{"orders", "users", "KEY "}
	slots := []uint32{biz.SlotForKey(keys[1]), biz.SlotForKey(keys[2])}
	require.NoError(t, a.Claim(context.Background(), slots))
	count := uint32(3)
	r, err := s.FetchNextBatch(
		requestContext(),
		&sequencev1.FetchNextBatchRequest{
			Requests: []*sequencev1.FetchNextRequest{
				{Key: keys[0]},
				{Key: keys[1], Count: &count},
				{Key: keys[2]},
			},
		},
	)
	require.NoError(t, err)
	require.Len(t, r.Results, 3)
	for i, item := range r.Results {
		assert.Equal(t, keys[i], item.Key)
		assert.EqualValues(t, 1, item.Id)
		assert.EqualValues(t, 1, item.SlotEpoch)
	}
	assert.EqualValues(t, 3, r.Results[1].Count)
}

func TestGetRouteHasOnlyCompleteSegmentsAndNoFabricatedSnapshot(t *testing.T) {
	s, _ := segmentedService(t, "orders", 4, 1)
	r, err := s.GetRoute(context.Background(), &sequencev1.GetRouteRequest{})
	require.NoError(t, err)
	require.Len(t, r.Route.Segments, 1)
	assert.EqualValues(t, biz.SlotCount-1, r.Route.Segments[0].EndSlot)
	assert.EqualValues(t, 4, r.Route.Segments[0].SlotEpoch)
	r, err = s.GetRoute(context.Background(), &sequencev1.GetRouteRequest{KnownVersion: 2})
	require.NoError(t, err)
	assert.True(t, r.NotModified)
	s.route = biz.NewRouteCache()
	_, err = s.GetRoute(context.Background(), &sequencev1.GetRouteRequest{})
	require.True(t, xerror.IsReason(err, reason.Reason_SEQUENCE_ROUTE_UNAVAILABLE))
}
