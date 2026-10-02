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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sequencev1 "github.com/codesjoy/sindri/gen/go/sequence/v1"
	"github.com/codesjoy/yggdrasil/v3/capabilities"
	"github.com/codesjoy/yggdrasil/v3/rpc/metadata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestClientGroupsKeysByOwner(t *testing.T) {
	router := mustRouter(t, testRoute(1, testNodeIDs(20)...))
	requests := make([]KeyRequest, 500)
	ids := make(map[string]int64, len(requests))
	owners := make(map[string]struct{})
	for index := range requests {
		key := fmt.Sprintf("batch-key-%03d", index)
		requests[index] = KeyRequest{Key: key}
		ids[key] = int64(index + 1)
		owners[testNodeIDs(20)[SlotForKey(key)%20]] = struct{}{}
	}
	client := &recordingSequenceClient{ids: ids}
	batch, err := NewClient(router, client)
	require.NoError(t, err)

	results, err := batch.FetchNextBatch(context.Background(), requests)
	require.NoError(t, err)
	require.Len(t, results, len(requests))
	for index, result := range results {
		assert.Equal(t, requests[index].Key, result.Key)
		assert.Equal(t, ids[requests[index].Key], result.FirstID)
		assert.Equal(t, uint32(1), result.Count)
	}
	assert.Equal(t, len(owners), client.batchCalls())
	assert.Equal(t, len(requests), client.batchItems())
}

func TestClientReturnsWholeBatchError(t *testing.T) {
	router := mustRouter(t, testRoute(1, "node-a", "node-b"))
	requests := sameOwnerRequests(t, "node-a", 2)
	requests = append(requests, sameOwnerRequests(t, "node-b", 2)...)
	client := &recordingSequenceClient{
		ids:       map[string]int64{},
		failOwner: requests[0].Key,
	}
	batch, err := NewClient(router, client)
	require.NoError(t, err)

	results, err := batch.FetchNextBatch(context.Background(), requests)
	require.Error(t, err)
	assert.Nil(t, results)
}

func TestClientRefreshesAndRegroupsOnce(t *testing.T) {
	var loads int
	router, err := NewRouter(func(
		context.Context,
		int64,
	) (*sequencev1.GetRouteResponse, error) {
		loads++
		if loads == 1 {
			return &sequencev1.GetRouteResponse{Route: testRoute(1, "node-a", "node-b")}, nil
		}
		return &sequencev1.GetRouteResponse{Route: testRoute(2, "node-a", "node-b")}, nil
	})
	require.NoError(t, err)
	client := &recordingSequenceClient{
		ids:           map[string]int64{},
		failFirstCall: true,
	}
	batch, err := NewClient(router, client)
	require.NoError(t, err)

	requests := sameOwnerRequests(t, "node-a", 2)
	requests = append(requests, sameOwnerRequests(t, "node-b", 2)...)
	results, err := batch.FetchNextBatch(context.Background(), requests)
	require.NoError(t, err)
	assert.Len(t, results, len(requests))
	assert.GreaterOrEqual(t, loads, 2)
	assert.GreaterOrEqual(t, client.batchCalls(), 2)
}

func TestClientDetectsUnsupportedServer(t *testing.T) {
	router := mustRouter(t, testRoute(1, "node-a"))
	client := &recordingSequenceClient{
		ids:               map[string]int64{},
		batchResponseCode: codes.Unimplemented,
	}
	batch, err := NewClient(router, client)
	require.NoError(t, err)

	_, err = batch.FetchNextBatch(context.Background(), []KeyRequest{{Key: "orders"}})
	require.ErrorIs(t, err, ErrBatchUnsupported)
}

func TestClientRejectsInvalidRequests(t *testing.T) {
	router := mustRouter(t, testRoute(1, "node-a"))
	batch, err := NewClient(router, &recordingSequenceClient{ids: map[string]int64{}})
	require.NoError(t, err)

	tests := []struct {
		name     string
		requests []KeyRequest
	}{
		{name: "empty"},
		{name: "duplicate", requests: []KeyRequest{{Key: "orders"}, {Key: "orders"}}},
		{name: "count", requests: []KeyRequest{{
			Key:   "orders",
			Count: MaxIDsPerKey + 1,
		}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, fetchErr := batch.FetchNextBatch(context.Background(), test.requests)
			require.Error(t, fetchErr)
		})
	}
}

type recordingSequenceClient struct {
	mu                sync.Mutex
	ids               map[string]int64
	failOwner         string
	failFirstCall     bool
	batchResponseCode codes.Code
	calls             int
	items             int
}

func (c *recordingSequenceClient) FetchNext(
	context.Context,
	*sequencev1.FetchNextRequest,
) (*sequencev1.FetchNextResponse, error) {
	return nil, errors.New("unexpected FetchNext call")
}

func (c *recordingSequenceClient) FetchNextBatch(
	_ context.Context,
	request *sequencev1.FetchNextBatchRequest,
) (*sequencev1.FetchNextBatchResponse, error) {
	c.mu.Lock()
	c.calls++
	firstCall := c.calls == 1
	c.items += len(request.GetRequests())
	c.mu.Unlock()

	if c.batchResponseCode != codes.OK {
		return nil, status.Error(c.batchResponseCode, c.batchResponseCode.String())
	}
	if c.failFirstCall && firstCall {
		return nil, ErrBatchRouteChanged
	}
	if c.failOwner != "" {
		for _, item := range request.GetRequests() {
			if item.GetKey() == c.failOwner {
				return nil, errors.New("injected owner failure")
			}
		}
	}
	response := &sequencev1.FetchNextBatchResponse{
		Results: make([]*sequencev1.FetchNextBatchResult, len(request.GetRequests())),
	}
	for index, item := range request.GetRequests() {
		id := c.ids[item.GetKey()]
		if id == 0 {
			id = int64(index + 1)
		}
		count := item.GetCount()
		if count == 0 {
			count = 1
		}
		response.Results[index] = &sequencev1.FetchNextBatchResult{
			Key:   item.GetKey(),
			Id:    id,
			Count: count,
		}
	}
	return response, nil
}

func (c *recordingSequenceClient) GetRoute(
	context.Context,
	*sequencev1.GetRouteRequest,
) (*sequencev1.GetRouteResponse, error) {
	return nil, errors.New("unexpected GetRoute call")
}

func (c *recordingSequenceClient) batchCalls() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

func (c *recordingSequenceClient) batchItems() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.items
}

func mustRouter(t *testing.T, routes ...*sequencev1.RouteSnapshot) *Router {
	t.Helper()
	index := 0
	router, err := NewRouter(func(
		context.Context,
		int64,
	) (*sequencev1.GetRouteResponse, error) {
		if index >= len(routes) {
			index = len(routes) - 1
		}
		route := routes[index]
		index++
		return &sequencev1.GetRouteResponse{Route: route}, nil
	})
	require.NoError(t, err)
	require.NoError(t, router.Update(routes[0]))
	return router
}

func testNodeIDs(count int) []string {
	nodes := make([]string, count)
	for index := range nodes {
		nodes[index] = fmt.Sprintf("node-%02d", index)
	}
	return nodes
}

func sameOwnerRequests(t *testing.T, nodeID string, count int) []KeyRequest {
	t.Helper()
	var requests []KeyRequest
	nodes := []string{"node-a", "node-b"}
	for candidate := 0; len(requests) < count; candidate++ {
		key := fmt.Sprintf("route-key-%d", candidate)
		if nodes[SlotForKey(key)%2] == nodeID {
			requests = append(requests, KeyRequest{Key: key})
		}
	}
	return requests
}

type batchProbe struct {
	sequencev1.SequenceGeneratorClient
	fetch func(context.Context, *sequencev1.FetchNextBatchRequest) (*sequencev1.FetchNextBatchResponse, error)
}

func (c batchProbe) FetchNextBatch(
	ctx context.Context,
	req *sequencev1.FetchNextBatchRequest,
) (*sequencev1.FetchNextBatchResponse, error) {
	return c.fetch(ctx, req)
}

func batchSuccess(req *sequencev1.FetchNextBatchRequest) *sequencev1.FetchNextBatchResponse {
	response := &sequencev1.FetchNextBatchResponse{}
	for _, item := range req.GetRequests() {
		response.Results = append(response.Results, &sequencev1.FetchNextBatchResult{
			Key: item.GetKey(), Id: 100, Count: item.GetCount(),
		})
	}
	return response
}

func TestBatchRetryRetainsSuccessAndRegroupsOnlyPendingKeys(t *testing.T) {
	keyA := keyInSlotRange(t, 0, SlotCount/2)
	keyB := keyInSlotRange(t, SlotCount/2, 3*SlotCount/4)
	keyC := keyInSlotRange(t, 3*SlotCount/4, SlotCount)
	old := testSegmentedRoute(1, 1,
		testSegment{nodeID: "node-a", epoch: 1, from: 0, to: SlotCount/2 - 1},
		testSegment{nodeID: "node-b", epoch: 1, from: SlotCount / 2, to: SlotCount - 1},
	)
	newRoute := testSegmentedRoute(2, 1,
		testSegment{nodeID: "node-a", epoch: 1, from: 0, to: SlotCount/2 - 1},
		testSegment{nodeID: "node-c", epoch: 2, from: SlotCount / 2, to: 3*SlotCount/4 - 1},
		testSegment{nodeID: "node-d", epoch: 2, from: 3 * SlotCount / 4, to: SlotCount - 1},
	)
	loads := 0
	router, err := NewRouter(func(context.Context, int64) (*sequencev1.GetRouteResponse, error) {
		loads++
		return &sequencev1.GetRouteResponse{Route: newRoute}, nil
	})
	require.NoError(t, err)
	require.NoError(t, router.Update(old))
	var mu sync.Mutex
	attempts := map[string]int{}
	groupSizes := []int{}
	firstFailure := make(chan struct{})
	var waits []time.Duration
	router.retryRandom = func() float64 { return 0 }
	router.retryWait = func(_ context.Context, delay time.Duration) error {
		waits = append(waits, delay)
		return nil
	}
	middleware := newUnaryClientInterceptor(router)
	client := batchProbe{
		fetch: func(ctx context.Context, req *sequencev1.FetchNextBatchRequest) (*sequencev1.FetchNextBatchResponse, error) {
			response := &sequencev1.FetchNextBatchResponse{}
			err := middleware(ctx, fetchNextBatchFullMethod, req, response,
				func(callCtx context.Context, _ string, _, reply any) error {
					outgoing, _ := metadata.FromOutContext(callCtx)
					version := outgoing.Get(VersionMetaKey)[0]
					mu.Lock()
					for _, item := range req.GetRequests() {
						attempts[item.GetKey()]++
					}
					groupSizes = append(groupSizes, len(req.GetRequests()))
					mu.Unlock()
					if version == "1" && req.GetRequests()[0].GetKey() != keyA {
						close(firstFailure)
						return retryError(RetryAfterRefresh, "3s")
					}
					if req.GetRequests()[0].GetKey() == keyA {
						<-firstFailure
						// Recoverable sibling failure must not cancel this success.
						assert.NoError(t, callCtx.Err())
					}
					reply.(*sequencev1.FetchNextBatchResponse).Results = batchSuccess(req).Results
					return nil
				})
			return response, err
		},
	}
	batch, err := NewClient(router, client)
	require.NoError(t, err)
	requests := []KeyRequest{{Key: keyC, Count: 3}, {Key: keyA, Count: 1}, {Key: keyB, Count: 2}}
	results, err := batch.FetchNextBatch(context.Background(), requests)
	require.NoError(t, err)
	require.Len(t, results, 3)
	for index, result := range results {
		assert.Equal(t, requests[index].Key, result.Key)
		assert.Equal(t, requests[index].Count, result.Count)
	}
	assert.Equal(t, map[string]int{keyA: 1, keyB: 2, keyC: 2}, attempts)
	assert.ElementsMatch(t, []int{1, 2, 1, 1}, groupSizes)
	assert.Equal(t, 1, loads)
	assert.Equal(t, []time.Duration{3 * time.Second}, waits, "the outer batch alone owns backoff")
}

func TestBatchAttemptBudgetDoesNotMultiplyThroughInterceptor(t *testing.T) {
	router := mustRouter(t, testRoute(1, "node-a"))
	router.retryWait = func(context.Context, time.Duration) error { return nil }
	middleware := newUnaryClientInterceptor(router)
	calls := 0
	failure := retryError(RetryAfterBackoff, "")
	client := batchProbe{
		fetch: func(ctx context.Context, req *sequencev1.FetchNextBatchRequest) (*sequencev1.FetchNextBatchResponse, error) {
			err := middleware(
				ctx,
				fetchNextBatchFullMethod,
				req,
				&sequencev1.FetchNextBatchResponse{},
				func(context.Context, string, any, any) error { calls++; return failure },
			)
			return nil, err
		},
	}
	batch, err := NewClient(router, client)
	require.NoError(t, err)
	results, err := batch.FetchNextBatch(context.Background(), []KeyRequest{{Key: "pending"}})
	require.Same(
		t,
		failure,
		err,
		"exhaustion must expose the original error, not internal group hints",
	)
	assert.Nil(t, results)
	assert.Equal(t, 8, calls, "8 outer rounds, not 8 x 8 nested attempts")
}

func TestBatchTerminalOrMalformedResultCancelsSiblings(t *testing.T) {
	for _, malformed := range []bool{false, true} {
		t.Run(fmt.Sprintf("malformed=%t", malformed), func(t *testing.T) {
			router := mustRouter(t, testRoute(1, "node-a", "node-b"))
			keyA := sameOwnerRequests(t, "node-a", 1)[0].Key
			keyB := sameOwnerRequests(t, "node-b", 1)[0].Key
			started := make(chan struct{})
			cancelled := make(chan struct{})
			fatal := errors.New("terminal group failure")
			client := batchProbe{
				fetch: func(ctx context.Context, req *sequencev1.FetchNextBatchRequest) (*sequencev1.FetchNextBatchResponse, error) {
					if req.GetRequests()[0].GetKey() == keyB {
						close(started)
						<-ctx.Done()
						close(cancelled)
						return nil, ctx.Err()
					}
					<-started
					if malformed {
						return &sequencev1.FetchNextBatchResponse{}, nil
					}
					return nil, fatal
				},
			}
			batch, err := NewClient(router, client)
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			results, err := batch.FetchNextBatch(ctx, []KeyRequest{{Key: keyA}, {Key: keyB}})
			require.Error(t, err)
			assert.NotErrorIs(t, err, context.DeadlineExceeded)
			if !malformed {
				assert.ErrorIs(
					t,
					err,
					fatal,
					"do not replace the primary failure with sibling cancellation",
				)
			}
			assert.Nil(t, results)
			select {
			case <-cancelled:
			default:
				t.Fatal("the outstanding RPC was not cancelled")
			}
		})
	}
}

func TestBatchConcurrencyIsBoundedAndCancellationDrains(t *testing.T) {
	router := mustRouter(t, testRoute(1, testNodeIDs(64)...))
	requests := make([]KeyRequest, 64)
	for index := range requests {
		for candidate := 0; ; candidate++ {
			key := fmt.Sprintf("owner-%d-key-%d", index, candidate)
			if int(SlotForKey(key)%64) == index {
				requests[index] = KeyRequest{Key: key}
				break
			}
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var active atomic.Int32
	var peak atomic.Int32
	client := batchProbe{
		fetch: func(ctx context.Context, _ *sequencev1.FetchNextBatchRequest) (*sequencev1.FetchNextBatchResponse, error) {
			current := active.Add(1)
			defer active.Add(-1)
			for old := peak.Load(); current > old; old = peak.Load() {
				if peak.CompareAndSwap(old, current) {
					break
				}
			}
			if current == MaxBatchConcurrency {
				cancel()
			}
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}
	batch, err := NewClient(router, client)
	require.NoError(t, err)
	results, err := batch.FetchNextBatch(ctx, requests)
	assert.ErrorIs(t, err, context.Canceled)
	assert.Nil(t, results)
	assert.Equal(t, int32(MaxBatchConcurrency), peak.Load())
	assert.Zero(t, active.Load())
}

func TestBatchTimeoutDoesNotExposeRetainedSuccess(t *testing.T) {
	router := mustRouter(t, testRoute(1, "node-a", "node-b"))
	policy := DefaultRetryPolicy()
	policy.MaxElapsed = 20 * time.Millisecond
	router.retryPolicy = policy
	keyA := sameOwnerRequests(t, "node-a", 1)[0].Key
	keyB := sameOwnerRequests(t, "node-b", 1)[0].Key
	client := batchProbe{
		fetch: func(ctx context.Context, req *sequencev1.FetchNextBatchRequest) (*sequencev1.FetchNextBatchResponse, error) {
			if req.GetRequests()[0].GetKey() == keyA {
				return batchSuccess(req), nil
			}
			return nil, retryError(RetryAfterBackoff, "1h")
		},
	}
	batch, err := NewClient(router, client)
	require.NoError(t, err)
	results, err := batch.FetchNextBatch(
		context.Background(),
		[]KeyRequest{{Key: keyA}, {Key: keyB}},
	)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Nil(t, results)
}

func TestModuleCapabilities(t *testing.T) {
	router, err := NewRouter(func(context.Context, int64) (*sequencev1.GetRouteResponse, error) {
		return nil, errors.New("unused")
	})
	require.NoError(t, err)
	module := NewModule(router)
	assert.Equal(t, ModuleName, module.Name())

	provided := module.Capabilities()
	require.Len(t, provided, 2)
	assert.Equal(t, capabilities.BalancerProviderSpec, provided[0].Spec)
	assert.Equal(t, BalancerType, provided[0].Name)
	assert.Equal(t, capabilities.UnaryClientInterceptorSpec, provided[1].Spec)
	assert.Equal(t, InterceptorName, provided[1].Name)
}
