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
	"sync"
	"testing"

	sequencev1 "github.com/codesjoy/sindri/gen/go/sequence/v1"
	"github.com/codesjoy/yggdrasil/v3/discovery/resolver"
	"github.com/codesjoy/yggdrasil/v3/rpc/stream"
	remote "github.com/codesjoy/yggdrasil/v3/transport"
	"github.com/codesjoy/yggdrasil/v3/transport/runtime/client/balancer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSequenceBalancerRoutesSlotsAndTracksRouteUpdates(t *testing.T) {
	router, err := NewRouter(func(context.Context, int64) (*sequencev1.GetRouteResponse, error) {
		return nil, errors.New("unused")
	})
	require.NoError(t, err)
	require.NoError(t, router.Update(testRoute(1, "node-a", "node-b")))
	client := newTestBalancerClient()
	b, err := newSequenceBalancer("svc", BalancerType, client, router)
	require.NoError(t, err)
	b.UpdateState(resolver.BaseState{Endpoints: []resolver.Endpoint{
		testEndpoint("a:1", "node-a"),
		testEndpoint("b:1", "node-b"),
	}})

	picker := client.picker()
	result, err := picker.Next(balancer.RPCInfo{
		Ctx:    withSlot(context.Background(), 0),
		Method: fetchNextFullMethod,
	})
	require.NoError(t, err)
	assert.Same(t, client.clients["grpc/a:1"], result.RemoteClient())
	result, err = picker.Next(balancer.RPCInfo{
		Ctx:    withSlot(context.Background(), 0),
		Method: fetchNextBatchFullMethod,
	})
	require.NoError(t, err)
	assert.Same(t, client.clients["grpc/a:1"], result.RemoteClient())

	updates := client.updateCount()
	require.NoError(t, router.Update(testRoute(2, "node-b", "node-a")))
	assert.Greater(t, client.updateCount(), updates)
	result, err = client.picker().Next(balancer.RPCInfo{
		Ctx:    withSlot(context.Background(), 0),
		Method: fetchNextFullMethod,
	})
	require.NoError(t, err)
	assert.Same(t, client.clients["grpc/b:1"], result.RemoteClient())
	require.NoError(t, b.Close())
}

func TestSequenceBalancerControlPlaneAndInvalidOwners(t *testing.T) {
	router, err := NewRouter(func(context.Context, int64) (*sequencev1.GetRouteResponse, error) {
		return nil, errors.New("unused")
	})
	require.NoError(t, err)
	require.NoError(t, router.Update(testRoute(1, "node-a")))
	client := newTestBalancerClient()
	b, err := newSequenceBalancer("svc", BalancerType, client, router)
	require.NoError(t, err)
	b.UpdateState(resolver.BaseState{Endpoints: []resolver.Endpoint{
		testEndpoint("a:1", "node-a"),
		testEndpoint("a:2", "node-a"),
		testEndpoint("unknown:1", ""),
	}})

	_, err = client.picker().Next(balancer.RPCInfo{
		Ctx:    withSlot(context.Background(), 0),
		Method: fetchNextFullMethod,
	})
	require.ErrorIs(t, err, ErrRouteUnavailable)

	seen := map[any]bool{}
	for range 3 {
		result, pickErr := client.picker().Next(balancer.RPCInfo{
			Ctx:    context.Background(),
			Method: getRouteFullMethod,
		})
		require.NoError(t, pickErr)
		seen[result.RemoteClient()] = true
	}
	assert.Len(t, seen, 3)

	first, err := client.picker().Next(balancer.RPCInfo{
		Ctx:    context.Background(),
		Method: getRouteFullMethod,
	})
	require.NoError(t, err)
	b.UpdateState(resolver.BaseState{Endpoints: []resolver.Endpoint{
		testEndpoint("a:1", "node-a"),
		testEndpoint("a:2", "node-a"),
		testEndpoint("unknown:1", ""),
	}})
	second, err := client.picker().Next(balancer.RPCInfo{
		Ctx:    context.Background(),
		Method: getRouteFullMethod,
	})
	require.NoError(t, err)
	assert.NotSame(t, first.RemoteClient(), second.RemoteClient())

	_, err = client.picker().Next(balancer.RPCInfo{
		Ctx:    context.Background(),
		Method: fetchNextFullMethod,
	})
	require.ErrorIs(t, err, ErrMissingSlot)
	_, err = client.picker().Next(balancer.RPCInfo{
		Ctx:    withSlot(context.Background(), SlotCount),
		Method: fetchNextFullMethod,
	})
	require.ErrorIs(t, err, ErrInvalidSlot)
	require.NoError(t, b.Close())
}

type testRemoteClient struct {
	mu        sync.Mutex
	state     remote.State
	closed    bool
	connected bool
}

func (c *testRemoteClient) NewStream(
	context.Context,
	*stream.Desc,
	string,
) (stream.ClientStream, error) {
	return nil, errors.New("unused")
}

func (c *testRemoteClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	return nil
}

func (*testRemoteClient) Protocol() string { return "grpc" }

func (c *testRemoteClient) State() remote.State {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state
}

func (c *testRemoteClient) Connect() {
	c.mu.Lock()
	c.connected = true
	c.mu.Unlock()
}

type testBalancerClient struct {
	mu        sync.Mutex
	state     balancer.State
	updates   int
	clients   map[string]*testRemoteClient
	listeners map[string]func(remote.ClientState)
}

func newTestBalancerClient() *testBalancerClient {
	return &testBalancerClient{
		clients:   make(map[string]*testRemoteClient),
		listeners: make(map[string]func(remote.ClientState)),
	}
}

func (c *testBalancerClient) UpdateState(state balancer.State) {
	c.mu.Lock()
	c.state = state
	c.updates++
	c.mu.Unlock()
}

func (c *testBalancerClient) NewRemoteClient(
	endpoint resolver.Endpoint,
	options balancer.NewRemoteClientOptions,
) (remote.Client, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	client := &testRemoteClient{state: remote.Ready}
	c.clients[endpoint.Name()] = client
	c.listeners[endpoint.Name()] = options.StateListener
	return client, nil
}

func (c *testBalancerClient) picker() balancer.Picker {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state.Picker
}

func (c *testBalancerClient) updateCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.updates
}

func testEndpoint(address, nodeID string) resolver.BaseEndpoint {
	attributes := map[string]any{}
	if nodeID != "" {
		attributes[NodeIDAttribute] = nodeID
	}
	return resolver.BaseEndpoint{
		Address:    address,
		Protocol:   "grpc",
		Attributes: attributes,
	}
}
