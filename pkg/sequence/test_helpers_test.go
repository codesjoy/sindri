package sequence

import (
	"context"
	"errors"
	"sync"

	sequencev1 "github.com/codesjoy/skuld/gen/go/sequence/v1"
	"github.com/codesjoy/yggdrasil/v3/discovery/resolver"
	"github.com/codesjoy/yggdrasil/v3/rpc/stream"
	remote "github.com/codesjoy/yggdrasil/v3/transport"
	"github.com/codesjoy/yggdrasil/v3/transport/runtime/client/balancer"
)

func testRoute(version int64, nodeIDs ...string) *sequencev1.RouteSnapshot {
	nodes := make([]*sequencev1.RouteNode, len(nodeIDs))
	for i, nodeID := range nodeIDs {
		nodes[i] = &sequencev1.RouteNode{NodeId: nodeID}
	}
	for slot := 0; slot < SlotCount; slot++ {
		owner := slot % len(nodes)
		nodes[owner].Slots = append(nodes[owner].Slots, uint32(slot))
	}
	return &sequencev1.RouteSnapshot{Version: version, Nodes: nodes}
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
