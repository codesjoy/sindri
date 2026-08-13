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

package biz

import (
	"context"
	"sync/atomic"

	sequencepkg "github.com/codesjoy/sindri/pkg/sequence"
)

const (
	// SlotCount is the fixed number of routing slots.
	SlotCount = sequencepkg.SlotCount
)

// SlotForKey hashes the original UTF-8 key bytes into the fixed slot space.
func SlotForKey(key string) uint32 {
	return sequencepkg.SlotForKey(key)
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

// RouteRepo loads route snapshots newer than a known version.
type RouteRepo interface {
	GetNewerRoute(ctx context.Context, version int64) (*Route, error)
}

// RouteCache stores the latest route and coalesces update notifications.
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

// Version returns the cached route version.
func (r *RouteCache) Version() int64 {
	return r.cache.Load().Version
}

// Route returns the cached route snapshot.
func (r *RouteCache) Route() *Route {
	return r.cache.Load()
}

// UpdateRoute publishes a new route snapshot.
func (r *RouteCache) UpdateRoute(route *Route) {
	r.cache.Store(route)
	select {
	case <-r.eventChan:
	default:
	}
	r.eventChan <- route
}

// EventChan returns the coalescing route update channel.
func (r *RouteCache) EventChan() <-chan *Route {
	return r.eventChan
}
