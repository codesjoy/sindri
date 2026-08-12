// Package sequence provides client-side route-aware sequence RPC support.
package sequence

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/codesjoy/yggdrasil/v3/discovery/resolver"
	remote "github.com/codesjoy/yggdrasil/v3/transport"
	"github.com/codesjoy/yggdrasil/v3/transport/runtime/client/balancer"
)

const (
	fetchNextFullMethod = "/codesjoy.skuld.sequence.v1.SequenceGenerator/FetchNext"
	getRouteFullMethod  = "/codesjoy.skuld.sequence.v1.SequenceGenerator/GetRoute"
)

var (
	// ErrMissingSlot indicates that a FetchNext call bypassed the sequence interceptor.
	ErrMissingSlot = errors.New("sequence request slot is missing from context")
	// ErrInvalidSlot indicates that a context contains a slot outside the sequence space.
	ErrInvalidSlot = errors.New("sequence request slot is out of range")
)

type remoteEndpoint struct {
	client remote.Client
	state  remote.State
	nodeID string
}

type sequenceBalancer struct {
	cli    balancer.Client
	router *Router

	mu          sync.RWMutex
	remotes     map[string]*remoteEndpoint
	closed      bool
	unsubscribe func()
}

func newSequenceBalancer(
	_ string,
	_ string,
	cli balancer.Client,
	router *Router,
) (balancer.Balancer, error) {
	if cli == nil {
		return nil, errors.New("sequence balancer: client is required")
	}
	if router == nil {
		return nil, errors.New("sequence balancer: router is required")
	}
	b := &sequenceBalancer{
		cli:     cli,
		router:  router,
		remotes: make(map[string]*remoteEndpoint),
	}
	b.unsubscribe = router.subscribe(b.publishState)
	return b, nil
}

func (*sequenceBalancer) Type() string { return BalancerType }

func (b *sequenceBalancer) UpdateState(state resolver.State) {
	wanted := make(map[string]resolver.Endpoint)
	if state != nil {
		for _, endpoint := range state.GetEndpoints() {
			if endpoint != nil {
				wanted[endpoint.Name()] = endpoint
			}
		}
	}

	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return
	}
	var removed []remote.Client
	for name, endpoint := range b.remotes {
		if _, ok := wanted[name]; ok {
			continue
		}
		removed = append(removed, endpoint.client)
		delete(b.remotes, name)
	}
	var connect []remote.Client
	for name, endpoint := range wanted {
		nodeID, _ := endpoint.GetAttributes()[NodeIDAttribute].(string)
		if current, ok := b.remotes[name]; ok {
			current.nodeID = nodeID
			continue
		}
		client, err := b.cli.NewRemoteClient(
			endpoint,
			balancer.NewRemoteClientOptions{StateListener: b.updateRemoteState},
		)
		if err != nil || client == nil {
			continue
		}
		b.remotes[name] = &remoteEndpoint{
			client: client,
			state:  client.State(),
			nodeID: nodeID,
		}
		if client.State() == remote.Idle || client.State() == remote.Connecting {
			connect = append(connect, client)
		}
	}
	b.mu.Unlock()

	for _, client := range connect {
		client.Connect()
	}
	for _, client := range removed {
		_ = client.Close()
	}
	b.publishState()
}

func (b *sequenceBalancer) updateRemoteState(state remote.ClientState) {
	if state.Endpoint == nil {
		return
	}
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return
	}
	endpoint, ok := b.remotes[state.Endpoint.Name()]
	if !ok {
		b.mu.Unlock()
		return
	}
	endpoint.state = state.State
	reconnect := state.State == remote.Idle
	client := endpoint.client
	b.mu.Unlock()
	if reconnect {
		client.Connect()
	}
	b.publishState()
}

func (b *sequenceBalancer) publishState() {
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return
	}
	connectivity, picker := b.buildStateLocked()
	b.mu.RUnlock()
	b.cli.UpdateState(balancer.State{ConnectivityState: connectivity, Picker: picker})
}

func (b *sequenceBalancer) buildStateLocked() (remote.State, balancer.Picker) {
	connectivity := remote.Connecting
	if len(b.remotes) == 0 {
		connectivity = remote.TransientFailure
	}
	nodeCounts := make(map[string]int, len(b.remotes))
	for _, endpoint := range b.remotes {
		if endpoint.nodeID != "" {
			nodeCounts[endpoint.nodeID]++
		}
		if endpoint.state == remote.Ready {
			connectivity = remote.Ready
		} else if connectivity != remote.Ready && endpoint.state == remote.TransientFailure {
			connectivity = remote.TransientFailure
		}
	}

	nodes := make(map[string]remote.Client, len(nodeCounts))
	type namedClient struct {
		name   string
		client remote.Client
	}
	control := make([]namedClient, 0, len(b.remotes))
	for name, endpoint := range b.remotes {
		if endpoint.state != remote.Ready {
			continue
		}
		control = append(control, namedClient{name: name, client: endpoint.client})
		if endpoint.nodeID != "" && nodeCounts[endpoint.nodeID] == 1 {
			nodes[endpoint.nodeID] = endpoint.client
		}
	}
	sort.Slice(control, func(i, j int) bool { return control[i].name < control[j].name })
	controlClients := make([]remote.Client, 0, len(control))
	for _, item := range control {
		controlClients = append(controlClients, item.client)
	}

	owners := b.router.ownerTable()
	slots := make([]remote.Client, SlotCount)
	for slot, nodeID := range owners {
		if client, ok := nodes[nodeID]; ok {
			slots[slot] = client
		}
	}
	return connectivity, &sequencePicker{slots: slots, control: controlClients}
}

func (b *sequenceBalancer) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	clients := make([]remote.Client, 0, len(b.remotes))
	for _, endpoint := range b.remotes {
		clients = append(clients, endpoint.client)
	}
	b.remotes = nil
	unsubscribe := b.unsubscribe
	b.unsubscribe = nil
	b.mu.Unlock()

	if unsubscribe != nil {
		unsubscribe()
	}
	var closeErr error
	for _, client := range clients {
		closeErr = errors.Join(closeErr, client.Close())
	}
	b.cli.UpdateState(balancer.State{
		ConnectivityState: remote.Shutdown,
		Picker:            &sequencePicker{},
	})
	return closeErr
}

type sequencePicker struct {
	slots   []remote.Client
	control []remote.Client
	next    atomic.Uint64
}

func (p *sequencePicker) Next(info balancer.RPCInfo) (balancer.PickResult, error) {
	if info.Method == getRouteFullMethod {
		if len(p.control) == 0 {
			return nil, balancer.ErrNoAvailableInstance
		}
		index := p.next.Add(1) - 1
		return pickedRemote{client: p.control[index%uint64(len(p.control))]}, nil
	}
	if info.Method != fetchNextFullMethod {
		return nil, fmt.Errorf("%w: method %s", ErrMissingSlot, info.Method)
	}
	slot, ok := SlotFromContext(info.Ctx)
	if !ok {
		return nil, ErrMissingSlot
	}
	if slot >= SlotCount {
		return nil, fmt.Errorf("%w: %d", ErrInvalidSlot, slot)
	}
	if int(slot) >= len(p.slots) || p.slots[slot] == nil {
		return nil, fmt.Errorf(
			"slot %d has no ready owner: %w",
			slot,
			balancer.ErrNoAvailableInstance,
		)
	}
	return pickedRemote{client: p.slots[slot]}, nil
}

type pickedRemote struct{ client remote.Client }

func (p pickedRemote) RemoteClient() remote.Client { return p.client }
func (pickedRemote) Report(error)                  {}

// NewBalancerProvider constructs the sequence slot balancer provider.
func NewBalancerProvider(router *Router) balancer.Provider {
	return balancer.NewProvider(
		BalancerType,
		func(serviceName, balancerName string, cli balancer.Client) (balancer.Balancer, error) {
			return newSequenceBalancer(serviceName, balancerName, cli, router)
		},
	)
}
