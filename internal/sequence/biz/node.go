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
	"errors"
	"log/slog"
	"sync"
	"time"
)

// NodeManager tracks node liveness and applies route assignments.
type NodeManager struct {
	nodeID            string
	tick              int64
	heartbeatElapsed  int64
	heartbeatTimeout  int64
	routeQueryTimeout time.Duration
	nodeTTL           time.Duration

	allocator *Allocator
	routeRepo RouteRepo
	liveness  LivenessRepo
	placement PlacementRepo
	route     *RouteCache
	// clock compares the storage lease clock with the local one. It is optional:
	// without it the node serves on the asserted bounds alone, which is what a
	// deployment gets before it has evidence.
	clock *StorageClockMonitor
	// desiredSlots is the last plan this node computed. It is kept so a failed
	// ownership or liveness read leaves the previous plan in force rather than
	// releasing every slot on a storage blip.
	desiredSlots []uint32

	logger *slog.Logger
}

// NewNodeManager constructs a node manager with the supplied dependencies.
func NewNodeManager(
	cfg DataPlaneConfig,
	allocator *Allocator,
	routeRepo RouteRepo,
	liveness LivenessRepo,
	placement PlacementRepo,
	route *RouteCache,
	clock *StorageClockMonitor,
	logger *slog.Logger,
) *NodeManager {
	if logger == nil {
		logger = slog.Default()
	}
	return &NodeManager{
		nodeID:            cfg.Node.ID,
		heartbeatTimeout:  cfg.Node.HeartbeatTimeoutTicks,
		routeQueryTimeout: cfg.Node.RouteQueryTimeout,
		nodeTTL:           cfg.HA.NodeTTL,

		allocator: allocator,
		routeRepo: routeRepo,
		liveness:  liveness,
		placement: placement,
		route:     route,
		clock:     clock,
		logger:    logger,
	}
}

// Heartbeat refreshes this node's leases, reads the directory, and replans the
// slots the node should hold.
//
// The plan is recomputed on every heartbeat rather than only when a new
// directory appears, because none of the events that move a slot between nodes
// change the directory by themselves: a node dying, an instance being replaced
// under the same node id, or a grant lapsing past the quiet window all show up
// in the ownership and liveness rows, and only a node that reads them can act.
// The published revision is what delays a client's view of the move, not the
// move itself.
func (m *NodeManager) Heartbeat() {
	// The clock comparison comes first: it decides whether any of the bounds the
	// rest of this function relies on are still known to hold.
	if m.clock != nil {
		clockCtx, clockCancel := context.WithTimeout(
			context.Background(),
			m.allocator.storeTimeout(),
		)
		err := m.clock.Observe(clockCtx)
		clockCancel()
		if err != nil &&
			errors.Is(err, ErrStorageClockViolation) {
			// An unreachable store is not a clock violation: the allocation path
			// already fails those closed with their own retriable reason, and
			// fencing on one would turn a storage blip into a dead instance.
			m.allocator.Fence("storage clock exceeded its asserted drift or jump bound")
		}
	}

	// Lease renewal runs off the heartbeat rather than the logical tick: the
	// lease is a wall-clock quantity, so pacing it by a tick would make the
	// safety window depend on the configured interval instead of on the
	// deployment's real pause bound. A heartbeat that stalls lets leases expire
	// early, which is the safe direction.
	m.allocator.RenewLeases()

	ctx, cancel := context.WithTimeout(context.Background(), m.routeQueryTimeout)
	defer cancel()

	// The live node set is renewed here, off the same heartbeat, so the placement
	// authority sees this node without the node having to reach it: the row goes
	// into the storage both of them already use. A failed renewal is logged and
	// the heartbeat continues, because the two failures differ in what they cost:
	// an unrenewed route is a node serving a stale directory, whereas an unrenewed
	// row only makes the planner hand this node's slots elsewhere -- the node
	// still holds its authority until it actually stops renewing leases.
	if err := m.liveness.RenewLiveness(ctx, m.nodeID, m.allocator.InstanceID()); err != nil {
		m.logger.Error("sequence liveness renewal failed", slog.Any("err", err))
	}

	paused := m.allocator.Paused()
	route, routeErr := m.routeRepo.GetNewerRoute(ctx, m.route.Version())
	if routeErr != nil {
		m.logger.Error(
			"sequence heartbeat failed",
			slog.Any("err", routeErr),
			slog.Int64("elapsed", m.heartbeatElapsed),
			slog.Bool("paused", paused),
		)
		return
	}

	if route != nil {
		m.route.UpdateRoute(route)
		m.logger.Debug("route change", slog.Any("route", route))
	}

	// Planning comes after the renewal and after the route read: the renewal is
	// what tells the fleet this instance is the current process for this node
	// id, and the route read is what makes the plan's version the newest one
	// this node knows about.
	desired, planErr := m.planSlots(ctx)
	if planErr != nil {
		m.logger.Error(
			"sequence placement read failed",
			slog.Any("err", planErr),
			slog.Int64("elapsed", m.heartbeatElapsed),
		)
		return
	}
	m.desiredSlots = desired
	m.allocator.Reconcile(m.route.Version(), m.tick+m.heartbeatTimeout, desired, paused)
	m.heartbeatElapsed = 0
}

// planSlots computes the slots this node should hold, from the ownership and
// liveness rows the fleet shares.
//
// The whole view is required. A partial read -- an empty table because a
// migration has not run, or a replica that has not caught up -- would otherwise
// look like a fleet that owns nothing, and the node would release every slot it
// serves on the strength of it. The compact segment read is validated before it
// is planned against, and a view that fails validation leaves the previous plan
// in force: the caller keeps the last good desired set rather than acting on a
// read it cannot trust.
func (m *NodeManager) planSlots(ctx context.Context) ([]uint32, error) {
	if m.placement == nil {
		return m.desiredSlots, nil
	}
	segments, err := m.placement.OwnershipSegments(ctx, m.allocator.QuietWindow())
	if err != nil {
		return nil, err
	}
	live, err := m.placement.LiveNodes(ctx, m.nodeTTL)
	if err != nil {
		return nil, err
	}
	targets, err := PlanTargetsFromSegments(segments, live)
	if err != nil {
		return nil, err
	}
	desired := make([]uint32, 0, len(targets))
	for _, target := range targets {
		if target.TargetNodeID == m.nodeID {
			desired = append(desired, target.SlotID)
		}
	}
	return desired, nil
}

// InstanceID returns the process identity this node claims slots and renews
// liveness under. It is what tells a restarted node from its predecessor in both
// records: the ownership epoch is fenced on it, and the liveness row reports it.
func (m *NodeManager) InstanceID() string {
	return m.allocator.InstanceID()
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

// monotonicClock measures elapsed time from process start.
//
// Section 3.4 derives the local deadline from a reading taken *before* a
// request is sent, so the reading must never move backwards. time.Time carries
// a monotonic component only within a process, and converting it to UnixNano
// drops that component, so the anchor is kept as a time.Time and every reading
// is taken with time.Since. The resulting value is a plain int64 of
// nanoseconds since start, which is safe to publish through an atomic.
type monotonicClock struct {
	anchor time.Time
}

func newMonotonicClock() monotonicClock {
	return monotonicClock{anchor: time.Now()}
}

// now returns nanoseconds elapsed since the process anchor.
func (c monotonicClock) now() int64 {
	return int64(time.Since(c.anchor))
}

// ErrStorageClockViolation reports that the storage clock moved differently from
// the local clock by more than the deployment's bound.
//
// It is the one platform failure this repository can detect at runtime. Neither
// PostgreSQL nor MySQL can bound a forward jump of the storage host clock, so
// J_max and delta are operator assertions; this monitor is what turns them from
// an assumption into evidence, and the only thing that enforces them once the
// fleet is running (appendix E).
var ErrStorageClockViolation = errors.New(
	"sequence: storage clock exceeded the asserted drift or jump bound",
)

// StorageClockMonitor compares successive readings of the storage lease clock
// with the local monotonic clock.
//
// Every safety bound in this protocol is expressed in storage time, but the node
// measures its own lease locally. The difference between the two elapsed times
// is therefore a term in the bound rather than a curiosity: within delta a lag is
// harmless because W covers it, and beyond delta it is not, because the local
// deadline could then outlive the storage lease it is supposed to sit inside.
//
// A violation is sticky. Once the difference has exceeded the bound, the node
// cannot reconstruct which of its earlier decisions were made while the bound
// held, so it stops serving until an operator has looked at the platform: the
// appendix G.2 admission evidence is what it is waiting for.
type StorageClockMonitor struct {
	driftBound time.Duration
	jumpBound  time.Duration

	now  func() time.Time
	read func(context.Context) (time.Time, error)

	mu          sync.Mutex
	armed       bool
	lastStorage time.Time
	lastStart   time.Time
	lastEnd     time.Time
	drift       time.Duration
	forwardJump time.Duration
	violated    bool
	sampleRTT   time.Duration
	uncertain   int64
}

// NewStorageClockMonitor constructs a monitor for the asserted bounds.
func NewStorageClockMonitor(
	driftBound time.Duration,
	jumpBound time.Duration,
	now func() time.Time,
	read func(context.Context) (time.Time, error),
) *StorageClockMonitor {
	return &StorageClockMonitor{
		driftBound: driftBound,
		jumpBound:  jumpBound,
		now:        now,
		read:       read,
	}
}

// Observe reads the storage clock once and compares it with the local clock.
//
// The first observation only arms the monitor: with nothing to compare against,
// a difference cannot be computed, and inventing one from a single reading would
// be measuring the offset between two clocks rather than the drift between two
// elapsed intervals. That offset is exactly what the protocol is designed not to
// depend on.
func (m *StorageClockMonitor) Observe(ctx context.Context) error {
	start := m.now()
	storage, err := m.read(ctx)
	end := m.now()
	if err != nil {
		// An unreachable store is a storage failure, not a clock one. The
		// allocation path already fails those closed with their own reason.
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sampleRTT = end.Sub(start)
	if !m.armed {
		m.armed = true
		m.lastStorage = storage
		m.lastStart = start
		m.lastEnd = end
		return nil
	}
	storageDelta := storage.Sub(m.lastStorage)
	// Each database timestamp was sampled somewhere inside its request interval.
	lower := storageDelta - end.Sub(m.lastStart)
	upper := storageDelta - start.Sub(m.lastEnd)
	m.lastStorage = storage
	m.lastStart = start
	m.lastEnd = end

	drift := lower + (upper-lower)/2
	m.drift = drift
	if lower > m.forwardJump {
		m.forwardJump = lower
	}
	if lower > m.jumpBound || lower > m.driftBound || upper < -m.driftBound {
		m.violated = true
	} else if lower < -m.driftBound || upper > m.driftBound || upper > m.jumpBound {
		m.uncertain++
	}
	if m.violated {
		return ErrStorageClockViolation
	}
	return nil
}

// Violated reports whether the monitor has seen the storage clock exceed its
// bound. It stays true for the life of the process.
func (m *StorageClockMonitor) Violated() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.violated
}

// StorageClockStats is a low-cardinality snapshot of the clock comparison.
type StorageClockStats struct {
	SampleRTT        time.Duration
	UncertainSamples int64
	// Drift is the midpoint of the possible signed elapsed-time differences;
	// only the entire interval outside a bound establishes a violation.
	Drift time.Duration
	// ForwardJump is the largest confirmed lower bound on a forward difference.
	ForwardJump time.Duration
	// Violated reports whether a bound was exceeded.
	Violated bool
}

// Stats returns the current comparison.
func (m *StorageClockMonitor) Stats() StorageClockStats {
	m.mu.Lock()
	defer m.mu.Unlock()
	return StorageClockStats{
		SampleRTT:        m.sampleRTT,
		UncertainSamples: m.uncertain,
		Drift:            m.drift,
		ForwardJump:      m.forwardJump,
		Violated:         m.violated,
	}
}
