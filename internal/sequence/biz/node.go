package biz

import (
	"context"
	"log/slog"
	"time"
)

type NodeConfig struct {
	ID                    string        `mapstructure:"id"`
	HeartbeatTimeoutTicks int64         `mapstructure:"heartbeat_timeout_ticks"`
	RouteQueryTimeout     time.Duration `mapstructure:"route_query_timeout"`
}

type NodeInfo struct {
	ID string
}

type NodeRepo interface {
	RegisterNode(ctx context.Context, node *NodeInfo) error
}

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

func (m *NodeManager) Heartbeat() {
	ctx, cancel := context.WithTimeout(context.Background(), m.routeQueryTimeout)
	defer cancel()
	paused := m.allocator.Paused()
	route, err := m.routeRepo.GetNewerRoute(ctx, m.route.Version())
	if err != nil {
		m.logger.Error("sequence heartbeat failed", slog.Any("err", err))
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

func (m *NodeManager) BaseTick() {
	if m.heartbeatElapsed >= m.heartbeatTimeout {
		m.allocator.Pause()
	} else {
		m.heartbeatElapsed++
		m.allocator.ApplyRoute(m.tick)
	}
}

func (m *NodeManager) TickClock() {
	m.tick++
}

func (m *NodeManager) CurrentTick() int64 {
	return m.tick
}

func (m *NodeManager) Pause() {
	m.allocator.Pause()
}
