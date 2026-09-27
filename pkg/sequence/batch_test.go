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
	"testing"

	sequencev1 "github.com/codesjoy/sindri/gen/go/sequence/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestBatchClientGroupsKeysByOwner(t *testing.T) {
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
	batch, err := NewBatchClient(router, client)
	require.NoError(t, err)

	results, err := batch.FetchNext(context.Background(), requests)
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

func TestBatchClientReturnsWholeBatchError(t *testing.T) {
	router := mustRouter(t, testRoute(1, "node-a", "node-b"))
	requests := sameOwnerRequests(t, "node-a", 2)
	requests = append(requests, sameOwnerRequests(t, "node-b", 2)...)
	client := &recordingSequenceClient{
		ids:       map[string]int64{},
		failOwner: requests[0].Key,
	}
	batch, err := NewBatchClient(router, client)
	require.NoError(t, err)

	results, err := batch.FetchNext(context.Background(), requests)
	require.Error(t, err)
	assert.Nil(t, results)
}

func TestBatchClientRefreshesAndRegroupsOnce(t *testing.T) {
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
	batch, err := NewBatchClient(router, client)
	require.NoError(t, err)

	requests := sameOwnerRequests(t, "node-a", 2)
	requests = append(requests, sameOwnerRequests(t, "node-b", 2)...)
	results, err := batch.FetchNext(context.Background(), requests)
	require.NoError(t, err)
	assert.Len(t, results, len(requests))
	assert.GreaterOrEqual(t, loads, 2)
	assert.GreaterOrEqual(t, client.batchCalls(), 2)
}

func TestBatchClientDetectsUnsupportedServer(t *testing.T) {
	router := mustRouter(t, testRoute(1, "node-a"))
	client := &recordingSequenceClient{
		ids:               map[string]int64{},
		batchResponseCode: codes.Unimplemented,
	}
	batch, err := NewBatchClient(router, client)
	require.NoError(t, err)

	_, err = batch.FetchNext(context.Background(), []KeyRequest{{Key: "orders"}})
	require.ErrorIs(t, err, ErrBatchUnsupported)
}

func TestBatchClientRejectsInvalidRequests(t *testing.T) {
	router := mustRouter(t, testRoute(1, "node-a"))
	batch, err := NewBatchClient(router, &recordingSequenceClient{ids: map[string]int64{}})
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
			_, fetchErr := batch.FetchNext(context.Background(), test.requests)
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
