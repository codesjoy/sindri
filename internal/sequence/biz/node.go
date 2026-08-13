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
	"log/slog"
	"time"
)

// NodeConfig contains node heartbeat and route polling settings.
type NodeConfig struct {
	ID                    string        `mapstructure:"id"`
	HeartbeatTimeoutTicks int64         `mapstructure:"heartbeat_timeout_ticks"`
	RouteQueryTimeout     time.Duration `mapstructure:"route_query_timeout"`
}

// NodeInfo identifies a sequence service node.
type NodeInfo struct {
	ID string
}

// NodeRepo persists sequence node registration state.
type NodeRepo interface {
	RegisterNode(ctx context.Context, node *NodeInfo) error
}

// NodeManager tracks node liveness and applies route assignments.
type NodeManager struct {
	nodeID            string
	tick              int64
	heartbeatElapsed  int64
	heartbeatTimeout  int64
	routeQueryTimeout time.Duration

	allocator *Allocator
	routeRepo RouteRepo
	route     *RouteCache

	logger *slog.Logger
}

// NewNodeManager constructs a node manager with the supplied dependencies.
func NewNodeManager(
	cfg *NodeConfig,
	allocator *Allocator,
	routeRepo RouteRepo,
	route *RouteCache,
	logger *slog.Logger,
) *NodeManager {
	if logger == nil {
		logger = slog.Default()
	}
	return &NodeManager{
		nodeID:            cfg.ID,
		heartbeatTimeout:  cfg.HeartbeatTimeoutTicks,
		routeQueryTimeout: cfg.RouteQueryTimeout,

		allocator: allocator,
		routeRepo: routeRepo,
		route:     route,
		logger:    logger,
	}
}

// Heartbeat refreshes the route and updates the node's allocation state.
func (m *NodeManager) Heartbeat() {
	ctx, cancel := context.WithTimeout(context.Background(), m.routeQueryTimeout)
	defer cancel()
	paused := m.allocator.Paused()
	route, err := m.routeRepo.GetNewerRoute(ctx, m.route.Version())
	if err != nil {
		m.logger.Error(
			"sequence heartbeat failed",
			slog.Any("err", err),
			slog.Int64("elapsed", m.heartbeatElapsed),
			slog.Bool("paused", paused),
		)
		return
	}

	if route == nil && paused && m.route.Version() > 0 {
		route = m.route.Route()
	}
	if route != nil {
		applyTicks := m.tick + m.heartbeatTimeout
		var slots []uint32
		for _, node := range route.Nodes {
			if node.NodeID != m.nodeID {
				continue
			}
			slots = node.Slots
		}
		if paused {
			m.allocator.Open(route.Version, applyTicks, slots)
		} else {
			m.allocator.CommitRoute(route.Version, applyTicks, slots)
		}
		m.route.UpdateRoute(route)
		m.logger.Debug("route change", slog.Any("route", route))
	}
	m.heartbeatElapsed = 0
}

// BaseTick applies the current route or pauses allocation after a timeout.
func (m *NodeManager) BaseTick() {
	if m.heartbeatElapsed >= m.heartbeatTimeout {
		m.allocator.Pause()
	} else {
		m.heartbeatElapsed++
		m.allocator.ApplyRoute(m.tick)
	}
	m.allocator.cleanupIdle()
}

// TickClock advances the node's logical clock by one tick.
func (m *NodeManager) TickClock() {
	m.tick++
}

// CurrentTick returns the node's logical clock.
func (m *NodeManager) CurrentTick() int64 {
	return m.tick
}

// Pause prevents further allocations until the next route is opened.
func (m *NodeManager) Pause() {
	m.allocator.Pause()
}
