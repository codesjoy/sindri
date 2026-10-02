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
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	sequencepkg "github.com/codesjoy/sindri/pkg/sequence"
)

const SlotCount = sequencepkg.SlotCount

// SlotState is the persisted serving state of a slot.
type SlotState string

const (
	SlotUnowned  SlotState = "UNOWNED"
	SlotOwned    SlotState = "OWNED"
	SlotDraining SlotState = "DRAINING"
)

// Ownership includes a derived node identity, never a second persisted owner.
type Ownership struct {
	SlotID          uint32
	OwnerNodeID     string
	OwnerInstanceID string
	Epoch           uint64
	State           SlotState
}

// Holds reports whether the ownership names the instance and epoch exactly.
func (o Ownership) Holds(instanceID string, epoch uint64) bool {
	return o.State == SlotOwned && o.OwnerInstanceID == instanceID && o.Epoch == epoch
}

// OwnershipSegment joins slot authority with its instance's storage-clock lease age.
type OwnershipSegment struct {
	StartSlot          uint32
	EndSlot            uint32
	OwnerNodeID        string
	OwnerInstanceID    string
	Epoch              uint64
	State              SlotState
	GrantAgeKnown      bool
	QuietWindowOverdue bool
	ReleaseReady       bool
}

// Slots returns the number of slots the segment covers.
func (s OwnershipSegment) Slots() uint32 { return s.EndSlot - s.StartSlot + 1 }

// Owned reports whether the segment names a complete owner tuple.
func (s OwnershipSegment) Owned() bool {
	return s.State != SlotUnowned && s.OwnerNodeID != "" && s.OwnerInstanceID != ""
}

// ValidateOwnershipSegments requires complete, contiguous, non-overlapping
// coverage with a fully specified owner for every non-unowned slot.
func ValidateOwnershipSegments(segments []OwnershipSegment) error {
	next := uint32(0)
	for _, s := range segments {
		if s.StartSlot != next || s.EndSlot < s.StartSlot || s.EndSlot >= SlotCount {
			return fmt.Errorf("sequence: invalid ownership segment at slot %d", next)
		}
		switch s.State {
		case SlotUnowned:
			if s.OwnerNodeID != "" || s.OwnerInstanceID != "" {
				return errors.New("sequence: unowned slot names an owner")
			}
		case SlotOwned, SlotDraining:
			if !s.Owned() || s.Epoch == 0 || !s.GrantAgeKnown {
				return errors.New("sequence: incomplete slot authority")
			}
		default:
			return errors.New("sequence: invalid slot state")
		}
		next = s.EndSlot + 1
	}
	if next != SlotCount {
		return errors.New("sequence: incomplete ownership coverage")
	}
	return nil
}

// SlotForKey hashes the original UTF-8 key bytes into the fixed slot space.
func SlotForKey(key string) uint32 { return sequencepkg.SlotForKey(key) }

// RouteRepo loads route snapshots newer than a known version.
type RouteRepo interface {
	GetNewerRoute(context.Context, int64) (*Route, error)
}

// RouteCache stores the latest route snapshot.
type RouteCache struct{ cache atomic.Pointer[Route] }

// NewRouteCache constructs an empty route cache.
func NewRouteCache() *RouteCache { r := &RouteCache{}; r.cache.Store(&Route{}); return r }

// Version returns the cached route version.
func (r *RouteCache) Version() int64 { return r.cache.Load().Version }

// Route returns the cached route snapshot.
func (r *RouteCache) Route() *Route { return r.cache.Load() }

// UpdateRoute publishes a newer route snapshot, ignoring stale versions.
func (r *RouteCache) UpdateRoute(route *Route) {
	for {
		old := r.cache.Load()
		if route.Version <= old.Version {
			return
		}
		if r.cache.CompareAndSwap(old, route) {
			return
		}
	}
}

// LayoutVersion returns the slot layout the cached route was minted under.
func (r *RouteCache) LayoutVersion() int64 { return r.cache.Load().LayoutVersion }

// OwnerOf returns the node owning a slot, or the empty string when unowned.
func (r *RouteCache) OwnerOf(slot uint32) string {
	s, _ := segmentFor(r.Route(), slot)
	return s.OwnerNodeID
}

// EpochOf returns the ownership epoch of a slot when it is known.
func (r *RouteCache) EpochOf(slot uint32) (uint64, bool) {
	s, ok := segmentFor(r.Route(), slot)
	return s.Epoch, ok
}

// Route is a complete versioned snapshot of the slot layout.
type Route struct {
	Version       int64
	LayoutVersion int64
	Segments      []RouteSegment
}

// RouteSegment is one contiguous run of slots sharing an owner instance.
type RouteSegment struct {
	StartSlot       uint32 `json:"start_slot"`
	EndSlot         uint32 `json:"end_slot"`
	OwnerNodeID     string `json:"owner_node_id"`
	OwnerInstanceID string `json:"owner_instance_id"`
	Epoch           uint64 `json:"epoch"`
}

// segmentFor returns the ownership segment covering a slot.
func segmentFor(route *Route, slot uint32) (RouteSegment, bool) {
	if route == nil || slot >= SlotCount {
		return RouteSegment{}, false
	}
	i := sort.Search(
		len(route.Segments),
		func(i int) bool { return route.Segments[i].EndSlot >= slot },
	)
	if i == len(route.Segments) || route.Segments[i].StartSlot > slot {
		return RouteSegment{}, false
	}
	return route.Segments[i], true
}

type routePayload struct {
	LayoutVersion int64          `json:"layout_version"`
	Segments      []RouteSegment `json:"segments"`
}

// EncodeOwnershipSegments builds the stored snapshot payload from the compact
// authority view, canonicalising adjacent runs that share an owner story.
func EncodeOwnershipSegments(segments []OwnershipSegment, layoutVersion int64) ([]byte, error) {
	if layoutVersion <= 0 {
		return nil, errors.New("sequence: invalid layout version")
	}
	if err := ValidateOwnershipSegments(segments); err != nil {
		return nil, err
	}
	payload := routePayload{LayoutVersion: layoutVersion}
	for _, s := range segments {
		current := RouteSegment{
			StartSlot:       s.StartSlot,
			EndSlot:         s.EndSlot,
			OwnerNodeID:     s.OwnerNodeID,
			OwnerInstanceID: s.OwnerInstanceID,
			Epoch:           s.Epoch,
		}
		if len(payload.Segments) > 0 {
			last := &payload.Segments[len(payload.Segments)-1]
			if last.OwnerNodeID == current.OwnerNodeID &&
				last.OwnerInstanceID == current.OwnerInstanceID &&
				last.Epoch == current.Epoch {
				last.EndSlot = current.EndSlot
				continue
			}
		}
		payload.Segments = append(payload.Segments, current)
	}
	return json.Marshal(payload)
}

// SameRoutePayload reports whether two stored payloads describe the same
// directory, comparing decoded values rather than serialised bytes.
func SameRoutePayload(left, right []byte) bool {
	var a, b routePayload
	return json.Unmarshal(left, &a) == nil && json.Unmarshal(right, &b) == nil &&
		reflect.DeepEqual(a, b)
}

// DecodeRoute turns a stored payload into a validated route snapshot.
func DecodeRoute(version int64, payload []byte) (*Route, error) {
	var stored routePayload
	if err := json.Unmarshal(payload, &stored); err != nil {
		return nil, err
	}
	if version <= 0 || stored.LayoutVersion <= 0 {
		return nil, errors.New("sequence: invalid route version")
	}
	next := uint32(0)
	for _, s := range stored.Segments {
		if s.StartSlot != next || s.EndSlot < s.StartSlot || s.EndSlot >= SlotCount {
			return nil, errors.New("sequence: invalid route coverage")
		}
		if (s.OwnerNodeID == "") != (s.OwnerInstanceID == "") ||
			(s.OwnerNodeID != "" && s.Epoch == 0) {
			return nil, errors.New("sequence: incomplete route authority")
		}
		next = s.EndSlot + 1
	}
	if next != SlotCount {
		return nil, errors.New("sequence: ownership segments are required")
	}
	return &Route{
		Version:       version,
		LayoutVersion: stored.LayoutVersion,
		Segments:      stored.Segments,
	}, nil
}

// NodeInfo is one live member as the planner sees it.
type NodeInfo struct {
	ID         string
	InstanceID string
	StableFor  time.Duration
}

// slotTarget is the node a slot is planned onto.
type slotTarget struct {
	SlotID       uint32
	TargetNodeID string
}

// planTargetsFromSegments preserves healthy ownership and fills quota deficits first.
// Draining slots cannot enter ordinary claim recovery.
func planTargetsFromSegments(segments []OwnershipSegment, nodes []NodeInfo) ([]slotTarget, error) {
	if err := ValidateOwnershipSegments(segments); err != nil {
		return nil, err
	}
	nodes = append([]NodeInfo(nil), nodes...)
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })
	counts := make(map[string]int, len(nodes))
	instances := make(map[string]string, len(nodes))
	for _, n := range nodes {
		instances[n.ID] = n.InstanceID
	}
	result := make([]slotTarget, SlotCount)
	var free []uint32
	for _, s := range segments {
		_, live := instances[s.OwnerNodeID]
		healthy := s.State == SlotOwned && live && instances[s.OwnerNodeID] == s.OwnerInstanceID &&
			!s.QuietWindowOverdue
		for slot := s.StartSlot; slot <= s.EndSlot; slot++ {
			result[slot].SlotID = slot
			if healthy || (s.State == SlotDraining && !s.ReleaseReady) {
				result[slot].TargetNodeID = s.OwnerNodeID
				counts[s.OwnerNodeID]++
			} else {
				free = append(free, slot)
			}
		}
	}
	if len(nodes) == 0 {
		return result, nil
	}
	quotas := slotQuotas(nodes)
	for _, slot := range free {
		chosen := nodes[0].ID
		for _, n := range nodes {
			if counts[n.ID] < quotas[n.ID] {
				chosen = n.ID
				break
			}
			if counts[n.ID] < counts[chosen] {
				chosen = n.ID
			}
		}
		result[slot].TargetNodeID = chosen
		counts[chosen]++
	}
	return result, nil
}

func slotQuotas(nodes []NodeInfo) map[string]int {
	quotas := make(map[string]int, len(nodes))
	if len(nodes) == 0 {
		return quotas
	}
	for i, n := range nodes {
		quotas[n.ID] = SlotCount / len(nodes)
		if i < SlotCount%len(nodes) {
			quotas[n.ID]++
		}
	}
	return quotas
}

// PlacementRepo reads the fleet's ownership and membership view.
type PlacementRepo interface {
	OwnershipSegments(context.Context, time.Duration) ([]OwnershipSegment, error)
	LiveNodes(context.Context, time.Duration) ([]NodeInfo, error)
}

// PublishResult reports the version and size of a materialised snapshot.
type PublishResult struct {
	Revision     int64
	PayloadBytes int
}

// PublisherRepo materialises a route from the current ownership view.
type PublisherRepo interface {
	OwnershipSegments(context.Context, time.Duration) ([]OwnershipSegment, error)
	MaterialiseRoute(
		context.Context,
		[]OwnershipSegment,
		int64,
		CoordinatorLease,
	) (PublishResult, error)
}

// CoordinatorRepo elects and fences the control-plane tenure.
type CoordinatorRepo interface {
	AcquireCoordinator(context.Context, string, time.Duration) (CoordinatorLease, error)
}

// CoordinatorLease is the fencing token of a held control-plane tenure.
type CoordinatorLease struct {
	Held       bool
	InstanceID string
	Epoch      uint64
}

// ErrCoordinatorLost reports that a control-plane write lost its tenure.
var ErrCoordinatorLost = errors.New("sequence: coordinator tenure lost")

// ClaimRequest is one bounded authority takeover batch.
type ClaimRequest struct {
	Slots       []uint32
	NodeID      string
	InstanceID  string
	Revision    uint64
	Lease       time.Duration
	QuietWindow time.Duration
}

// ClaimOutcome reports what happened to one requested slot.
type ClaimOutcome struct {
	Ownership Ownership
	Granted   bool
	NotBefore time.Time
}

// ReservationAuthority is the instance revision and epochs a reserve presents.
type ReservationAuthority struct {
	InstanceID string
	Revision   uint64
	Epochs     map[uint32]uint64
	Lease      time.Duration
}

// OwnershipRepo is the durable instance, slot and coordinator authority store.
type OwnershipRepo interface {
	RegisterInstance(context.Context, string, string) error
	RenewInstance(context.Context, string, uint64) (InstanceLease, error)
	InstanceAuthority(context.Context, string) (AuthoritySnapshot, error)
	RetireInstance(context.Context, string) error
	ClaimSlots(context.Context, ClaimRequest) ([]ClaimOutcome, error)
	StorageClock(context.Context) (time.Time, error)
}

var (
	// ErrSlotNotOwned is returned when the caller does not own the slot.
	ErrSlotNotOwned = errors.New("sequence: slot is not owned by this instance")
	// ErrLeaseExpired is returned when the instance lease has passed.
	ErrLeaseExpired = errors.New("sequence: instance lease has expired")
	// ErrAuthorityChanged is returned when the presented revision is stale.
	ErrAuthorityChanged = errors.New("sequence: instance authority changed")
	// ErrCommitUncertain is returned when a write's outcome cannot be known.
	ErrCommitUncertain = errors.New("sequence: write outcome is uncertain")
)

// LivenessRepo maintains the per-node membership heartbeat.
type LivenessRepo interface {
	RenewLiveness(context.Context, string, string) error
	DropLiveness(context.Context, string, string) error
}

// Publisher owns the control-plane pass that materialises and publishes routes.
type Publisher struct {
	cfg               ControlPlaneConfig
	quietWindow       time.Duration
	instanceID        string
	placement         PublisherRepo
	coordinator       CoordinatorRepo
	rebalancer        *Rebalancer
	logger            *slog.Logger
	passes            atomic.Int64
	unownedSlots      atomic.Int64
	coordinatorLost   atomic.Int64
	revision          atomic.Int64
	lastPass          atomic.Int64
	lastPassFailed    atomic.Bool
	viewSegments      atomic.Int64
	routePayloadBytes atomic.Int64
	observer          atomic.Pointer[PublisherObserver]
	active            atomic.Bool
}

// PublisherObserver receives control-plane pass and publication callbacks.
type PublisherObserver struct {
	OwnershipView func(int, time.Duration)
	Published     func(int)
}

// SetObserver installs the pass observer.
func (p *Publisher) SetObserver(observer PublisherObserver) { p.observer.Store(&observer) }

// SetRebalancer attaches the rebalancer that plans migrations.
func (p *Publisher) SetRebalancer(r *Rebalancer) { p.rebalancer = r }

// PublisherReadiness reports whether the control plane is publishing.
type PublisherReadiness struct {
	Ready          bool
	Reason         string
	LastPassFailed bool
}

// PublisherStats reports the control plane's counters.
type PublisherStats struct {
	Passes                int64
	UnownedSlots          int64
	CoordinatorLost       int64
	Revision              int64
	OwnershipViewSegments int64
	RoutePayloadBytes     int64
}

// NewPublisher builds a control-plane publisher over its placement and
// coordinator repositories.
func NewPublisher(
	cfg ControlPlaneConfig,
	quietWindow time.Duration,
	instanceID string,
	placement PublisherRepo,
	coordinator CoordinatorRepo,
	logger *slog.Logger,
) *Publisher {
	if logger == nil {
		logger = slog.Default()
	}
	return &Publisher{
		cfg:         cfg,
		quietWindow: quietWindow,
		instanceID:  instanceID,
		placement:   placement,
		coordinator: coordinator,
		logger:      logger,
	}
}

// Run drives the publisher until the context is cancelled.
func (p *Publisher) Run(ctx context.Context) {
	for ctx.Err() == nil {
		if err := p.Pass(ctx); err != nil {
			p.logger.Error("sequence control pass failed", "error", err)
		}
		interval := p.cfg.ReconcileInterval
		if p.active.Load() {
			interval = p.cfg.ActivePublishInterval
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

// Pass runs one reconcile: acquire tenure, recover handoffs, plan and publish.
func (p *Publisher) Pass(ctx context.Context) (passErr error) {
	defer func() {
		p.passes.Add(1)
		p.lastPass.Store(time.Now().UnixNano())
		p.lastPassFailed.Store(passErr != nil)
	}()
	ctx, cancel := context.WithTimeout(ctx, p.cfg.PassTimeout)
	defer cancel()
	lease, err := p.coordinator.AcquireCoordinator(ctx, p.instanceID, p.cfg.CoordinatorLease)
	if err != nil || !lease.Held {
		return err
	}
	if p.rebalancer != nil {
		active, rebalanceErr := p.rebalancer.Pass(ctx, lease)
		p.active.Store(active)
		if rebalanceErr != nil {
			p.logger.Error("sequence rebalance pass failed", "error", rebalanceErr)
		}
	}
	started := time.Now()
	segments, err := p.placement.OwnershipSegments(ctx, p.quietWindow)
	if err != nil {
		return err
	}
	if err := ValidateOwnershipSegments(segments); err != nil {
		return err
	}
	if o := p.observer.Load(); o != nil && o.OwnershipView != nil {
		o.OwnershipView(len(segments), time.Since(started))
	}
	result, err := p.placement.MaterialiseRoute(ctx, segments, p.cfg.LayoutVersion, lease)
	if errors.Is(err, ErrCoordinatorLost) {
		p.coordinatorLost.Add(1)
	}
	if err != nil {
		return err
	}
	p.revision.Store(result.Revision)
	p.viewSegments.Store(int64(len(segments)))
	p.routePayloadBytes.Store(int64(result.PayloadBytes))
	var unowned int64
	for _, s := range segments {
		if s.State == SlotUnowned {
			unowned += int64(s.Slots())
		}
	}
	p.unownedSlots.Store(unowned)
	if o := p.observer.Load(); o != nil && o.Published != nil {
		o.Published(result.PayloadBytes)
	}
	return nil
}

// Readiness reports whether the last pass is recent enough to serve.
func (p *Publisher) Readiness(now time.Time) PublisherReadiness {
	last := p.lastPass.Load()
	ready := last != 0 && now.Sub(time.Unix(0, last)) <= 3*p.cfg.ReconcileInterval
	reason := "serving"
	if last == 0 {
		reason = "initializing"
	} else if !ready {
		reason = "stalled"
	}
	return PublisherReadiness{Ready: ready, Reason: reason, LastPassFailed: p.lastPassFailed.Load()}
}

// Stats returns a snapshot of the publisher's counters.
func (p *Publisher) Stats() PublisherStats {
	return PublisherStats{
		Passes:                p.passes.Load(),
		UnownedSlots:          p.unownedSlots.Load(),
		CoordinatorLost:       p.coordinatorLost.Load(),
		Revision:              p.revision.Load(),
		OwnershipViewSegments: p.viewSegments.Load(),
		RoutePayloadBytes:     p.routePayloadBytes.Load(),
	}
}

// Handoff is one persisted transfer or release of a slot.
type Handoff struct {
	SlotID           uint32
	ID               string
	Kind             string
	SourceInstanceID string
	SourceEpoch      uint64
	TargetInstanceID string
	Phase            string
	Drained          bool
	TargetReady      bool
	NotBefore        time.Time
}

// Active reports whether the handoff still governs its slot.
func (h Handoff) Active() bool { return h.Phase != "COMPLETED" && h.Phase != "CANCELLED" }

// HandoffCommand is one state-machine action for a handoff.
type HandoffCommand struct {
	Handoff Handoff
	Action  string
}

// HandoffRequest is one bounded handoff execution batch for an instance.
type HandoffRequest struct {
	InstanceID string
	Revision   uint64
	Commands   []HandoffCommand
	HA         HAConfig
	Limits     MigrationConfig
}

// HandoffRepo is the durable handoff state machine.
type HandoffRepo interface {
	Handoffs(context.Context, string, int) ([]Handoff, error)
	PlanHandoffs(context.Context, []Handoff, CoordinatorLease, MigrationConfig, time.Duration) error
	ApplyHandoffs(context.Context, HandoffRequest) error
	RecoverHandoffs(context.Context, CoordinatorLease, []NodeInfo, HAConfig, int) error
	CollectInstances(context.Context, time.Duration, int) error
}

// planMigrations accounts for pending transfers before moving only quota surpluses.
func planMigrations(
	segments []OwnershipSegment,
	nodes []NodeInfo,
	pending []Handoff,
	stability time.Duration,
	limit int,
) []Handoff {
	if ValidateOwnershipSegments(segments) != nil || limit <= 0 {
		return nil
	}
	counts := make(map[string]int)
	byInstance := make(map[string]NodeInfo)
	for _, n := range nodes {
		byInstance[n.InstanceID] = n
	}
	for _, s := range segments {
		if s.State == SlotUnowned || s.QuietWindowOverdue {
			return nil
		}
		if _, ok := byInstance[s.OwnerInstanceID]; !ok {
			return nil
		}
		counts[s.OwnerInstanceID] += int(s.Slots())
	}
	var admitted []NodeInfo
	for _, n := range nodes {
		if counts[n.InstanceID] > 0 || n.StableFor >= stability {
			admitted = append(admitted, n)
		}
	}
	sort.Slice(admitted, func(i, j int) bool { return admitted[i].ID < admitted[j].ID })
	if len(admitted) < 2 {
		return nil
	}
	quotas := slotQuotas(admitted)
	busy := make(map[uint32]bool)
	for _, h := range pending {
		if !h.Active() {
			continue
		}
		busy[h.SlotID] = true
		if h.Phase != "TRANSFERRED" {
			counts[h.SourceInstanceID]--
			if h.TargetInstanceID != "" {
				counts[h.TargetInstanceID]++
			}
		}
	}
	var plans []Handoff
	for _, s := range segments {
		if s.State != SlotOwned {
			continue
		}
		owner := byInstance[s.OwnerInstanceID]
		for slot := s.StartSlot; slot <= s.EndSlot && counts[s.OwnerInstanceID] > quotas[owner.ID]; slot++ {
			if busy[slot] {
				continue
			}
			var target string
			for _, n := range admitted {
				if counts[n.InstanceID] < quotas[n.ID] {
					target = n.InstanceID
					break
				}
			}
			if target == "" {
				return plans
			}
			plans = append(
				plans,
				Handoff{
					SlotID:           slot,
					ID:               uuid.NewString(),
					Kind:             "TRANSFER",
					SourceInstanceID: s.OwnerInstanceID,
					SourceEpoch:      s.Epoch,
					TargetInstanceID: target,
					Phase:            "PLANNED",
				},
			)
			counts[s.OwnerInstanceID]--
			counts[target]++
			if len(plans) == limit {
				return plans
			}
		}
	}
	return plans
}

// Rebalancer plans and recovers the migrations that rebalance the fleet.
type Rebalancer struct {
	cfg            ControlPlaneConfig
	ha             HAConfig
	placement      PlacementRepo
	repo           HandoffRepo
	lastPlan       time.Time
	lastCollection time.Time
}

// NewRebalancer builds a rebalancer over its placement and handoff repositories.
func NewRebalancer(
	cfg ControlPlaneConfig,
	ha HAConfig,
	placement PlacementRepo,
	repo HandoffRepo,
) *Rebalancer {
	return &Rebalancer{cfg: cfg, ha: ha, placement: placement, repo: repo}
}

// Pass recovers orphaned handoffs, collects dead instances and plans new
// migrations while the tenure is held.
func (r *Rebalancer) Pass(ctx context.Context, lease CoordinatorLease) (bool, error) {
	nodes, err := r.placement.LiveNodes(ctx, r.ha.NodeTTL)
	if err != nil {
		return false, err
	}
	if err := r.repo.RecoverHandoffs(
		ctx,
		lease,
		nodes,
		r.ha,
		r.cfg.Migration.BatchSlots,
	); err != nil {
		return false, err
	}
	pending, err := r.repo.Handoffs(ctx, "", r.cfg.Migration.MaxPlanned)
	if err != nil {
		return false, err
	}
	now := time.Now()
	if now.Sub(r.lastCollection) >= time.Minute {
		if err := r.repo.CollectInstances(ctx, r.ha.QuietWindow, 1000); err != nil {
			return len(pending) > 0, err
		}
		r.lastCollection = now
	}
	if now.Sub(r.lastPlan) < r.cfg.ReconcileInterval || len(pending) >= r.cfg.Migration.MaxPlanned {
		return len(pending) > 0, nil
	}
	r.lastPlan = now
	segments, err := r.placement.OwnershipSegments(ctx, r.ha.QuietWindow)
	if err != nil {
		return len(pending) > 0, err
	}
	plans := planMigrations(
		segments,
		nodes,
		pending,
		r.cfg.Migration.JoinStabilityWindow,
		min(r.cfg.Migration.BatchSlots, r.cfg.Migration.MaxPlanned-len(pending)),
	)
	if len(plans) > 0 {
		err = r.repo.PlanHandoffs(ctx, plans, lease, r.cfg.Migration, r.ha.LeaseDuration)
	}
	return len(pending)+len(plans) > 0, err
}

// NodeManager drives one data-plane instance's authority loops.
type NodeManager struct {
	cfg       DataPlaneConfig
	allocator *Allocator
	routeRepo RouteRepo
	liveness  LivenessRepo
	placement PlacementRepo
	route     *RouteCache
	clock     *StorageClockMonitor
	logger    *slog.Logger
}

// NewNodeManager builds the manager over the allocator and its repositories.
func NewNodeManager(
	cfg DataPlaneConfig,
	allocator *Allocator,
	routeRepo RouteRepo,
	liveness LivenessRepo,
	placement PlacementRepo,
	route *RouteCache,
	clock *StorageClockMonitor,
	handoffs HandoffRepo,
	control ControlPlaneConfig,
	logger *slog.Logger,
) *NodeManager {
	if logger == nil {
		logger = slog.Default()
	}
	allocator.SetHandoffs(handoffs, control.Migration)
	return &NodeManager{
		cfg:       cfg,
		allocator: allocator,
		routeRepo: routeRepo,
		liveness:  liveness,
		placement: placement,
		route:     route,
		clock:     clock,
		logger:    logger,
	}
}

// Renew observes the storage clock and renews the instance lease.
func (m *NodeManager) Renew(ctx context.Context) {
	if m.clock != nil {
		clockCtx, cancel := context.WithTimeout(ctx, m.allocator.storeTimeout())
		err := m.clock.Observe(clockCtx)
		cancel()
		if errors.Is(err, errStorageClockViolation) {
			m.allocator.Fence("storage clock exceeded its asserted bound")
			return
		}
	}
	m.allocator.RenewLeases()
}

// Heartbeat renews the node's membership liveness row.
func (m *NodeManager) Heartbeat(ctx context.Context) {
	if !m.allocator.initialized.Load() || m.allocator.stopping.Load() ||
		m.allocator.fenced.Load() != nil {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, m.allocator.storeTimeout())
	defer cancel()
	if err := m.liveness.RenewLiveness(ctx, m.cfg.Node.ID, m.allocator.InstanceID()); err != nil {
		m.logger.Error("sequence membership renewal failed", "error", err)
	}
}

// Refresh loads the newest route and claims the slots the plan hands this node.
func (m *NodeManager) Refresh(ctx context.Context) {
	routeCtx, cancelRoute := context.WithTimeout(ctx, m.cfg.Node.RouteQueryTimeout)
	route, err := m.routeRepo.GetNewerRoute(routeCtx, m.route.Version())
	cancelRoute()
	if err != nil {
		m.logger.Error("sequence route refresh failed", "error", err)
	} else if route != nil {
		m.route.UpdateRoute(route)
	}
	if !m.allocator.initialized.Load() || m.allocator.stopping.Load() ||
		m.allocator.fenced.Load() != nil {
		return
	}
	if m.route.Version() > 0 {
		// A route is in hand, so the instance has something to answer from and
		// may leave the initializing state. Recording it before the claim keeps
		// readiness honest: the request path already reports recovering for a
		// slot that has not been installed yet.
		m.allocator.MarkRouteApplied()
	}
	// The authority pass reads the whole fleet view and then claims the slots
	// the plan hands this node, one storage batch at a time. It runs under a
	// bound that covers those batches rather than the directory read's: sharing
	// the read's bound spends it on the first batch and leaves the node unable
	// to converge whenever the storage is slower than one directory read.
	authorityCtx, cancelAuthority := context.WithTimeout(
		ctx,
		m.allocator.authorityRefreshTimeout(),
	)
	defer cancelAuthority()
	segments, err := m.placement.OwnershipSegments(authorityCtx, m.cfg.HA.QuietWindow)
	if err != nil {
		m.logger.Error("sequence authority view failed", "error", err)
		return
	}
	nodes, err := m.placement.LiveNodes(authorityCtx, m.cfg.HA.NodeTTL)
	if err != nil {
		m.logger.Error("sequence membership view failed", "error", err)
		return
	}
	targets, err := planTargetsFromSegments(segments, nodes)
	if err != nil {
		return
	}
	blocked := make(map[uint32]bool)
	for _, s := range segments {
		if s.State == SlotDraining && !s.ReleaseReady {
			for id := s.StartSlot; id <= s.EndSlot; id++ {
				blocked[id] = true
			}
		}
	}
	var slots []uint32
	for _, target := range targets {
		if target.TargetNodeID == m.cfg.Node.ID && !blocked[target.SlotID] {
			slots = append(slots, target.SlotID)
		}
	}
	if err := m.allocator.Claim(authorityCtx, slots); err != nil {
		m.logger.Error("sequence claim failed", "error", err)
	}
}

// Handoffs executes the pending handoff state machine for this instance.
func (m *NodeManager) Handoffs(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, m.allocator.storeTimeout())
	defer cancel()
	if err := m.allocator.HandoffPass(ctx); err != nil {
		m.logger.Error("sequence handoff pass failed", "error", err)
	}
}

// Maintain runs the allocator's periodic in-memory maintenance.
func (m *NodeManager) Maintain(context.Context) {
	m.allocator.cleanupIdle()
}

// Pause stops the allocator from serving new work.
func (m *NodeManager) Pause() { m.allocator.Pause() }

// InstanceID returns the process's instance identity.
func (m *NodeManager) InstanceID() string { return m.allocator.InstanceID() }

type monotonicClock struct{ anchor time.Time }

func newMonotonicClock() monotonicClock { return monotonicClock{anchor: time.Now()} }
func (c monotonicClock) now() int64     { return int64(time.Since(c.anchor)) }

// errStorageClockViolation reports that the storage clock breached its bound.
var errStorageClockViolation = errors.New(
	"sequence: storage clock exceeded the asserted drift or jump bound",
)

// StorageClockMonitor asserts the storage clock keeps within its bounds.
type StorageClockMonitor struct {
	driftBound  time.Duration
	jumpBound   time.Duration
	now         func() time.Time
	read        func(context.Context) (time.Time, error)
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

// NewStorageClockMonitor builds a monitor over the authority store.
func NewStorageClockMonitor(
	drift, jump time.Duration,
	now func() time.Time,
	read func(context.Context) (time.Time, error),
) *StorageClockMonitor {
	return &StorageClockMonitor{driftBound: drift, jumpBound: jump, now: now, read: read}
}

// Observe reads the storage clock and records a violation if it jumped.
func (m *StorageClockMonitor) Observe(ctx context.Context) error {
	start := m.now()
	storage, err := m.read(ctx)
	end := m.now()
	if err != nil {
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
	delta := storage.Sub(m.lastStorage)
	lower := delta - end.Sub(m.lastStart)
	upper := delta - start.Sub(m.lastEnd)
	m.lastStorage = storage
	m.lastStart = start
	m.lastEnd = end
	m.drift = lower + (upper-lower)/2
	if lower > m.forwardJump {
		m.forwardJump = lower
	}
	if lower > m.jumpBound || lower > m.driftBound || upper < -m.driftBound {
		m.violated = true
	} else if lower < -m.driftBound || upper > m.driftBound || upper > m.jumpBound {
		m.uncertain++
	}
	if m.violated {
		return errStorageClockViolation
	}
	return nil
}

// Violated reports whether a clock breach has been observed.
func (m *StorageClockMonitor) Violated() bool { m.mu.Lock(); defer m.mu.Unlock(); return m.violated }

// StorageClockStats reports the monitor's observations and worst skew.
type StorageClockStats struct {
	SampleRTT        time.Duration
	UncertainSamples int64
	Drift            time.Duration
	ForwardJump      time.Duration
	Violated         bool
}

// Stats returns a snapshot of the monitor's counters.
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
