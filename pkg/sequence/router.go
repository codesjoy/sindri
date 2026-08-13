package sequence

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"sync"

	sequencev1 "github.com/codesjoy/skuld/gen/go/sequence/v1"
	"google.golang.org/protobuf/proto"
)

// SlotCount is the fixed number of routing slots in sequence mode.
const SlotCount = 1 << 14

var (
	// ErrInvalidRoute indicates that a route snapshot violates the routing contract.
	ErrInvalidRoute = errors.New("invalid sequence route")
	// ErrRouteVersionRegression indicates that a route update moved backwards.
	ErrRouteVersionRegression = errors.New("sequence route version regressed")
	// ErrRouteVersionConflict indicates that one version identifies different snapshots.
	ErrRouteVersionConflict = errors.New("sequence route version conflicts with current snapshot")
	// ErrRouteUnavailable indicates that no usable route has been loaded.
	ErrRouteUnavailable = errors.New("sequence route is unavailable")
)

var ieeeCRC32Table = crc32.MakeTable(crc32.IEEE)

// RouteLoader fetches a route newer than knownVersion, or a not-modified response.
type RouteLoader func(
	ctx context.Context,
	knownVersion int64,
) (*sequencev1.GetRouteResponse, error)

type compiledRoute struct {
	snapshot *sequencev1.RouteSnapshot
	owners   [SlotCount]string
}

type refreshCall struct {
	done chan struct{}
	err  error
}

// Router owns the immutable client route snapshot shared by interceptors and balancers.
type Router struct {
	mu sync.RWMutex

	loader  RouteLoader
	current *compiledRoute
	refresh *refreshCall

	nextListener uint64
	listeners    map[uint64]func()
}

// NewRouter constructs an empty router backed by loader.
func NewRouter(loader RouteLoader) (*Router, error) {
	if loader == nil {
		return nil, errors.New("sequence router: route loader is required")
	}
	return &Router{
		loader:    loader,
		listeners: make(map[uint64]func()),
	}, nil
}

// SlotForKey hashes the original UTF-8 key bytes into the fixed slot space.
func SlotForKey(key string) uint32 {
	crc := ^uint32(0)
	for i := 0; i < len(key); i++ {
		crc = ieeeCRC32Table[byte(crc)^key[i]] ^ crc>>8
	}
	return ^crc % SlotCount
}

// Version returns the current route version, or zero before the first route is loaded.
func (r *Router) Version() int64 {
	if r == nil {
		return 0
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.current == nil {
		return 0
	}
	return r.current.snapshot.GetVersion()
}

// Snapshot returns a deep copy of the current route snapshot.
func (r *Router) Snapshot() *sequencev1.RouteSnapshot {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.current == nil {
		return nil
	}
	return proto.Clone(r.current.snapshot).(*sequencev1.RouteSnapshot)
}

// Update validates and publishes a route snapshot.
func (r *Router) Update(snapshot *sequencev1.RouteSnapshot) error {
	if r == nil {
		return errors.New("sequence router: router is required")
	}
	compiled, err := compileRoute(snapshot)
	if err != nil {
		return err
	}

	r.mu.Lock()
	if r.current != nil {
		currentVersion := r.current.snapshot.GetVersion()
		switch {
		case compiled.snapshot.GetVersion() < currentVersion:
			r.mu.Unlock()
			return fmt.Errorf(
				"%w: current=%d update=%d",
				ErrRouteVersionRegression,
				currentVersion,
				compiled.snapshot.GetVersion(),
			)
		case compiled.snapshot.GetVersion() == currentVersion:
			if proto.Equal(r.current.snapshot, compiled.snapshot) {
				r.mu.Unlock()
				return nil
			}
			r.mu.Unlock()
			return fmt.Errorf("%w: version=%d", ErrRouteVersionConflict, currentVersion)
		}
	}
	r.current = compiled
	listeners := make([]func(), 0, len(r.listeners))
	for _, listener := range r.listeners {
		listeners = append(listeners, listener)
	}
	r.mu.Unlock()

	for _, listener := range listeners {
		listener()
	}
	return nil
}

// Refresh loads and publishes a route relative to the current version.
// Concurrent callers share one in-flight load.
func (r *Router) Refresh(ctx context.Context) error {
	return r.refreshRoute(ctx, 0, false)
}

func (r *Router) refreshAfter(ctx context.Context, observedVersion int64) error {
	return r.refreshRoute(ctx, observedVersion, true)
}

func (r *Router) refreshRoute(
	ctx context.Context,
	observedVersion int64,
	skipIfNewer bool,
) error {
	if r == nil {
		return errors.New("sequence router: router is required")
	}
	if ctx == nil {
		return errors.New("sequence router: context is required")
	}

	r.mu.Lock()
	if skipIfNewer && r.current != nil &&
		r.current.snapshot.GetVersion() > observedVersion {
		r.mu.Unlock()
		return nil
	}
	if call := r.refresh; call != nil {
		done := call.done
		r.mu.Unlock()
		select {
		case <-done:
			return call.err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	call := &refreshCall{done: make(chan struct{})}
	r.refresh = call
	knownVersion := int64(0)
	if r.current != nil {
		knownVersion = r.current.snapshot.GetVersion()
	}
	r.mu.Unlock()

	response, err := r.loader(ctx, knownVersion)
	if err == nil {
		err = r.applyRefreshResponse(response)
	}

	r.mu.Lock()
	call.err = err
	r.refresh = nil
	close(call.done)
	r.mu.Unlock()
	return err
}

func (r *Router) applyRefreshResponse(response *sequencev1.GetRouteResponse) error {
	if response == nil {
		return fmt.Errorf("%w: loader returned a nil response", ErrInvalidRoute)
	}
	if response.GetNotModified() {
		if response.GetRoute() != nil {
			return fmt.Errorf("%w: not-modified response includes a route", ErrInvalidRoute)
		}
		if r.Version() == 0 {
			return ErrRouteUnavailable
		}
		return nil
	}
	if response.GetRoute() == nil {
		return fmt.Errorf("%w: loader response does not include a route", ErrInvalidRoute)
	}
	return r.Update(response.GetRoute())
}

func compileRoute(snapshot *sequencev1.RouteSnapshot) (*compiledRoute, error) {
	if snapshot == nil {
		return nil, fmt.Errorf("%w: snapshot is nil", ErrInvalidRoute)
	}
	if snapshot.GetVersion() <= 0 {
		return nil, fmt.Errorf("%w: version must be positive", ErrInvalidRoute)
	}

	compiled := &compiledRoute{
		snapshot: proto.Clone(snapshot).(*sequencev1.RouteSnapshot),
	}
	nodeIDs := make(map[string]struct{}, len(compiled.snapshot.GetNodes()))
	assigned := make([]bool, SlotCount)
	assignedCount := 0
	for _, node := range compiled.snapshot.GetNodes() {
		if node == nil || node.GetNodeId() == "" {
			return nil, fmt.Errorf("%w: node id must not be empty", ErrInvalidRoute)
		}
		if _, exists := nodeIDs[node.GetNodeId()]; exists {
			return nil, fmt.Errorf(
				"%w: duplicate node id %q",
				ErrInvalidRoute,
				node.GetNodeId(),
			)
		}
		nodeIDs[node.GetNodeId()] = struct{}{}
		for _, slot := range node.GetSlots() {
			if slot >= SlotCount {
				return nil, fmt.Errorf("%w: slot %d is out of range", ErrInvalidRoute, slot)
			}
			slotIndex := int(slot)
			if assigned[slotIndex] {
				return nil, fmt.Errorf(
					"%w: slot %d is assigned more than once",
					ErrInvalidRoute,
					slot,
				)
			}
			assigned[slotIndex] = true
			compiled.owners[slotIndex] = node.GetNodeId()
			assignedCount++
		}
	}
	if assignedCount != SlotCount {
		return nil, fmt.Errorf(
			"%w: assigned %d of %d slots",
			ErrInvalidRoute,
			assignedCount,
			SlotCount,
		)
	}
	return compiled, nil
}

func (r *Router) ownerTable() [SlotCount]string {
	if r == nil {
		return [SlotCount]string{}
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.current == nil {
		return [SlotCount]string{}
	}
	return r.current.owners
}

func (r *Router) subscribe(listener func()) func() {
	if r == nil || listener == nil {
		return func() {}
	}
	r.mu.Lock()
	r.nextListener++
	id := r.nextListener
	r.listeners[id] = listener
	r.mu.Unlock()
	return func() {
		r.mu.Lock()
		delete(r.listeners, id)
		r.mu.Unlock()
	}
}
