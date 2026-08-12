package biz

import (
	"context"
	"hash/crc32"
	"sync/atomic"
)

const (
	SlotCount = 1 << 14
)

// SlotForKey hashes the original UTF-8 key bytes into the fixed slot space.
func SlotForKey(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key)) % SlotCount
}

// Route is a complete versioned assignment.
type Route struct {
	Version int64
	Nodes   []RouteNode
}

// RouteNode assigns routing slots to one reachable node.
type RouteNode struct {
	NodeID string
	Slots  []uint32
}

type RouteRepo interface {
	GetNewerRoute(ctx context.Context, version int64) (*Route, error)
}

type RouteCache struct {
	cache atomic.Pointer[Route]

	eventChan chan *Route
}

// NewRouteCache constructs an empty route cache with a coalescing event channel.
func NewRouteCache() *RouteCache {
	r := &RouteCache{eventChan: make(chan *Route, 1)}
	r.cache.Store(&Route{})
	return r
}

func (r *RouteCache) Version() int64 {
	return r.cache.Load().Version
}

func (r *RouteCache) Route() *Route {
	return r.cache.Load()
}

func (r *RouteCache) UpdateRoute(route *Route) {
	r.cache.Store(route)
	select {
	case <-r.eventChan:
	default:
	}
	r.eventChan <- route
}

func (r *RouteCache) EventChan() <-chan *Route {
	return r.eventChan
}
