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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"slices"
	"sort"
	"sync/atomic"
	"time"

	sequencepkg "github.com/codesjoy/sindri/pkg/sequence"
)

// SlotForKey hashes the original UTF-8 key bytes into the fixed slot space.
func SlotForKey(key string) uint32 {
	return sequencepkg.SlotForKey(key)
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

// LayoutVersion returns the slot layout of the cached snapshot, or zero when no
// route has been applied.
func (r *RouteCache) LayoutVersion() int64 {
	return r.cache.Load().LayoutVersion
}

// OwnerOf returns the node the cached snapshot assigns to a slot, or the empty
// string when the slot is unowned or no snapshot is loaded.
//
// It reads the ownership segments first because they are the authoritative view;
// it falls back to the node lists only for a snapshot that predates them. The
// callers are error paths and probes rather than the allocation hot path, so the
// fallback's scan over node lists is acceptable here.
func (r *RouteCache) OwnerOf(slot uint32) string {
	route := r.cache.Load()
	if route == nil {
		return ""
	}
	if segment, ok := SegmentFor(route, slot); ok {
		return segment.OwnerNodeID
	}
	for _, node := range route.Nodes {
		if slices.Contains(node.Slots, slot) {
			return node.NodeID
		}
	}
	return ""
}

// EpochOf returns the ownership epoch the cached snapshot records for a slot.
//
// The second result is false when the snapshot carries no segments, which is
// what a caller needs to know before telling a client its epoch is stale: with
// no epoch of its own, this node has nothing to compare against.
func (r *RouteCache) EpochOf(slot uint32) (uint64, bool) {
	route := r.cache.Load()
	if route == nil {
		return 0, false
	}
	segment, ok := SegmentFor(route, slot)
	if !ok {
		return 0, false
	}
	return segment.Epoch, true
}

// Route is a complete versioned assignment.
//
// The process that decides placement builds one and the process that serves ids
// reads one, so it lives here rather than in either of them: a second definition
// is a second chance for the two views of the same directory to disagree.
type Route struct {
	Version int64
	Nodes   []RouteNode
	// LayoutVersion identifies the slot layout the epochs below were minted
	// under. A change to the slot count or to SlotForKey changes it, because the
	// same slot number then describes a different key.
	LayoutVersion int64
	// Segments is the authority-backed ownership view: contiguous runs of slots
	// sharing one owner instance and epoch, ordered by slot. It is required and
	// covers every slot exactly once; a snapshot without it is refused.
	Segments []RouteSegment
}

// RouteNode assigns routing slots to one reachable node.
type RouteNode struct {
	NodeID string
	Slots  []uint32
}

// RouteSegment is one contiguous run of slots owned by a single instance.
type RouteSegment struct {
	StartSlot uint32
	EndSlot   uint32
	// OwnerNodeID and OwnerInstanceID are empty when the slot is unowned. Such a
	// slot is paused rather than routed to the empty node.
	OwnerNodeID     string
	OwnerInstanceID string
	// Epoch is the ownership generation of the run. It is never reused for a
	// given slot, so a caller can order two observations by comparing it.
	Epoch uint64
}

// SegmentFor finds the ownership segment covering a slot.
//
// Segments are ordered by slot and cover the space exactly once, which the
// decoder guarantees when it builds them, so a binary search is sound and is
// what keeps this off the linear path a probe would otherwise walk.
func SegmentFor(route *Route, slot uint32) (RouteSegment, bool) {
	if route == nil {
		return RouteSegment{}, false
	}
	segments := route.Segments
	index := sort.Search(len(segments), func(i int) bool {
		return segments[i].EndSlot >= slot
	})
	if index == len(segments) || segments[index].StartSlot > slot {
		return RouteSegment{}, false
	}
	return segments[index], true
}

// routePayload is the stored form of a route snapshot.
type routePayload struct {
	// LayoutVersion identifies the slot layout the segments were minted under.
	// It is required: a snapshot that does not state it cannot be checked
	// against the caller's layout.
	LayoutVersion int64 `json:"layout_version"`
	// Nodes is the node-shaped projection of the segments, kept so a reader
	// that only needs "which node serves this slot" can stay simple.
	Nodes    []storedRouteNode    `json:"nodes"`
	Segments []storedRouteSegment `json:"segments"`
}

type storedRouteNode struct {
	NodeID string   `json:"node_id"`
	Slots  []uint32 `json:"slots"`
}

// storedRouteSegment is one contiguous run of slots sharing an owner instance
// and epoch. An empty owner_node_id marks a slot that nobody owns; such a slot
// is paused, not routed, so it must not appear in any node's slot list.
type storedRouteSegment struct {
	StartSlot       uint32 `json:"start_slot"`
	EndSlot         uint32 `json:"end_slot"`
	OwnerNodeID     string `json:"owner_node_id"`
	OwnerInstanceID string `json:"owner_instance_id"`
	Epoch           uint64 `json:"epoch"`
}

// EncodeOwnershipView builds the stored snapshot payload from the per-slot
// authority view.
//
// It is the bridge for callers that hold the diagnostic slot view; the
// publisher encodes the compact segment view directly. The view is refused
// unless it covers every slot exactly once, so a reader cannot publish a
// directory with a hole in it.
func EncodeOwnershipView(view []Ownership, layoutVersion int64) ([]byte, error) {
	segments, err := ownershipSegmentsFromView(view, func(Ownership) bool { return false })
	if err != nil {
		return nil, err
	}
	return EncodeOwnershipSegments(segments, layoutVersion)
}

// EncodeOwnershipSegments builds the stored snapshot payload from the compact
// authority view.
//
// Segments carry every slot exactly once, including the unowned ones, because a
// caller needs a complete ownership picture to check a per-slot epoch. Node
// lists contain only slots that are owned: an unowned slot is paused rather than
// routed, and listing it under a node would claim an owner it does not have.
//
// The encoder re-canonicalises the runs before writing them: only a change of
// owner story (node, instance, epoch) starts a new stored segment, while the
// quiet-window classification that split the read into finer runs is dropped.
// Without that, the payload would change every time a grant aged across the
// window even though nothing a client observes had changed, and the publisher
// would mint a revision for a directory identical to the one already stored.
func EncodeOwnershipSegments(
	segments []OwnershipSegment,
	layoutVersion int64,
) ([]byte, error) {
	if layoutVersion <= 0 {
		return nil, errors.New(
			"sequence placement: layout_version must be positive",
		)
	}
	if err := ValidateOwnershipSegments(segments); err != nil {
		return nil, err
	}
	payload := routePayload{LayoutVersion: layoutVersion}
	slotsByNode := make(map[string][]uint32)
	order := make([]string, 0, 4)
	for _, segment := range segments {
		stored := storedRouteSegment{
			StartSlot:       segment.StartSlot,
			EndSlot:         segment.EndSlot,
			OwnerNodeID:     segment.OwnerNodeID,
			OwnerInstanceID: segment.OwnerInstanceID,
			Epoch:           segment.Epoch,
		}
		if !segment.Owned() {
			// An unowned run carries no authority: it is written without an
			// owner or epoch so a reader pauses those slots rather than routing
			// them to the empty node.
			stored.OwnerNodeID = ""
			stored.OwnerInstanceID = ""
			stored.Epoch = 0
		}
		last := len(payload.Segments) - 1
		if last >= 0 &&
			payload.Segments[last].OwnerNodeID == stored.OwnerNodeID &&
			payload.Segments[last].OwnerInstanceID == stored.OwnerInstanceID &&
			payload.Segments[last].Epoch == stored.Epoch &&
			payload.Segments[last].EndSlot+1 == stored.StartSlot {
			payload.Segments[last].EndSlot = stored.EndSlot
		} else {
			payload.Segments = append(payload.Segments, stored)
		}
		if !segment.Owned() {
			continue
		}
		if _, seen := slotsByNode[segment.OwnerNodeID]; !seen {
			order = append(order, segment.OwnerNodeID)
		}
		for slot := segment.StartSlot; slot <= segment.EndSlot; slot++ {
			slotsByNode[segment.OwnerNodeID] = append(
				slotsByNode[segment.OwnerNodeID],
				slot,
			)
		}
	}
	sort.Strings(order)
	for _, nodeID := range order {
		payload.Nodes = append(payload.Nodes, storedRouteNode{
			NodeID: nodeID,
			Slots:  slotsByNode[nodeID],
		})
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("encode materialised route: %w", err)
	}
	return encoded, nil
}

// SameRoutePayload reports whether two stored payloads describe the same
// directory.
//
// The answer cannot come from the bytes. A route is stored in a jsonb (or MySQL
// json) column, and such a column does not give back what it was handed: the
// document is parsed and re-serialised, so keys come back in the column's own
// order and the separators carry the spaces those types put around them. A
// byte comparison therefore reports every payload as different from itself, and
// a writer that uses it to decide whether to publish would mint a revision and
// push the whole directory to the fleet on every pass over an unchanged fleet.
// What the check has to answer is whether a reader would see a difference, so
// both sides are decoded and compared as values.
//
// Payloads that do not decode are reported as different: nothing can be said
// about what they mean, and republishing over them is the safe answer.
func SameRoutePayload(left, right []byte) bool {
	if bytes.Equal(left, right) {
		return true
	}
	var leftPayload, rightPayload routePayload
	if err := json.Unmarshal(left, &leftPayload); err != nil {
		return false
	}
	if err := json.Unmarshal(right, &rightPayload); err != nil {
		return false
	}
	return reflect.DeepEqual(leftPayload, rightPayload)
}

// DecodeRoute turns a stored payload into a route.
//
// One shape is accepted: a snapshot written from storage ownership. It carries a
// positive layout_version and segments that cover every slot exactly once. The
// node lists are a projection of the segments, not a second authority: they may
// list only some of the owned slots, and they may omit an unowned slot entirely,
// but a slot they do list must agree with the segment that owns it. The
// ownership view is what a caller needs to check a per-slot epoch, so it is
// validated rather than trusted; nodes stay in the payload because that is the
// projection a router uses.
//
// A payload without segments is refused. Routes used to be published by hand
// with only a node list, and such a snapshot says nothing about which epoch a
// slot is served under: accepting it would put a directory in the fleet that no
// node can fence a stale owner against. The publisher writes the first
// ownership-backed revision over any such row, so refusing here turns a
// deployment that forgot the publisher into a readable error rather than a
// fleet quietly routing without epochs.
func DecodeRoute(version int64, payload []byte) (*Route, error) {
	if version <= 0 {
		return nil, fmt.Errorf("decode route: invalid version %d", version)
	}
	var stored routePayload
	if err := json.Unmarshal(payload, &stored); err != nil {
		return nil, fmt.Errorf("decode route version %d: %w", version, err)
	}

	nodes, slotNode, err := decodeNodes(version, stored.Nodes)
	if err != nil {
		return nil, err
	}

	if len(stored.Segments) == 0 {
		return nil, fmt.Errorf(
			"decode route version %d: ownership segments are required; "+
				"a node-only snapshot cannot be checked against slot epochs",
			version,
		)
	}

	if stored.LayoutVersion <= 0 {
		return nil, fmt.Errorf(
			"decode route version %d: layout_version must be positive",
			version,
		)
	}
	segments, err := decodeSegments(version, stored.Segments)
	if err != nil {
		return nil, err
	}
	for _, segment := range segments {
		for slot := segment.StartSlot; slot <= segment.EndSlot; slot++ {
			listed := slotNode[slot]
			if listed == "" {
				// The node projection is allowed to be partial; the segment is
				// the authority, so an omitted slot says nothing.
				continue
			}
			if segment.OwnerNodeID == "" {
				return nil, fmt.Errorf(
					"decode route version %d: node %q lists unowned slot %d",
					version,
					listed,
					slot,
				)
			}
			if listed != segment.OwnerNodeID {
				return nil, fmt.Errorf(
					"decode route version %d: slot %d is owned by %q but node %q lists it",
					version,
					slot,
					segment.OwnerNodeID,
					listed,
				)
			}
		}
	}

	return &Route{
		Version:       version,
		Nodes:         nodes,
		LayoutVersion: stored.LayoutVersion,
		Segments:      segments,
	}, nil
}

// decodeNodes parses the node view and returns it alongside the node that owns
// each slot, with an empty entry for an unassigned slot.
func decodeNodes(version int64, stored []storedRouteNode) ([]RouteNode, []string, error) {
	nodes := make([]RouteNode, 0, len(stored))
	slotNode := make([]string, SlotCount)
	nodeIDs := make(map[string]struct{}, len(stored))
	for _, item := range stored {
		if item.NodeID == "" {
			return nil, nil, fmt.Errorf("decode route version %d: empty node id", version)
		}
		if _, exists := nodeIDs[item.NodeID]; exists {
			return nil, nil, fmt.Errorf(
				"decode route version %d: duplicate node %q",
				version,
				item.NodeID,
			)
		}
		nodeIDs[item.NodeID] = struct{}{}

		slots := append([]uint32(nil), item.Slots...)
		for _, slot := range slots {
			// The bound is taken from the slice being indexed rather than from
			// SlotCount, which is the same number but not one an analyser can tie
			// to this allocation.
			if int(slot) >= len(slotNode) {
				return nil, nil, fmt.Errorf(
					"decode route version %d: slot %d is out of range",
					version,
					slot,
				)
			}
			if slotNode[slot] != "" {
				return nil, nil, fmt.Errorf(
					"decode route version %d: slot %d is assigned more than once",
					version,
					slot,
				)
			}
			slotNode[slot] = item.NodeID
		}
		sort.Slice(slots, func(i, j int) bool { return slots[i] < slots[j] })
		nodes = append(nodes, RouteNode{NodeID: item.NodeID, Slots: slots})
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].NodeID < nodes[j].NodeID })
	return nodes, slotNode, nil
}

// decodeSegments parses the ownership view and requires it to be a complete,
// gap-free, ascending partition of the slot space.
func decodeSegments(version int64, stored []storedRouteSegment) ([]RouteSegment, error) {
	segments := make([]RouteSegment, 0, len(stored))
	next := uint32(0)
	for _, item := range stored {
		if item.StartSlot >= SlotCount || item.EndSlot >= SlotCount {
			return nil, fmt.Errorf(
				"decode route version %d: segment [%d,%d] is out of range",
				version,
				item.StartSlot,
				item.EndSlot,
			)
		}
		if item.EndSlot < item.StartSlot {
			return nil, fmt.Errorf(
				"decode route version %d: segment [%d,%d] is inverted",
				version,
				item.StartSlot,
				item.EndSlot,
			)
		}
		if item.StartSlot != next {
			return nil, fmt.Errorf(
				"decode route version %d: segment [%d,%d] does not continue the coverage at slot %d",
				version,
				item.StartSlot,
				item.EndSlot,
				next,
			)
		}
		// A node id without the instance that holds the claim cannot be checked
		// against an epoch, so the pairing is required rather than defaulted.
		if item.OwnerNodeID == "" && item.OwnerInstanceID != "" {
			return nil, fmt.Errorf(
				"decode route version %d: segment [%d,%d] names an instance but no owner",
				version,
				item.StartSlot,
				item.EndSlot,
			)
		}
		if item.OwnerNodeID != "" && item.OwnerInstanceID == "" {
			return nil, fmt.Errorf(
				"decode route version %d: segment [%d,%d] names owner %q without an instance",
				version,
				item.StartSlot,
				item.EndSlot,
				item.OwnerNodeID,
			)
		}

		next = item.EndSlot + 1
		segments = append(segments, RouteSegment(item))
	}
	if next != SlotCount {
		return nil, fmt.Errorf(
			"decode route version %d: segments cover %d of %d slots",
			version,
			next,
			SlotCount,
		)
	}
	return segments, nil
}

// CurrentVersion returns the version of the active route.
func (obj *Allocator) CurrentVersion() int64 {
	obj.slotsMu.RLock()
	defer obj.slotsMu.RUnlock()
	return obj.version
}

// QuietWindow returns W, the window a candidate waits before it may take a slot
// over. It is what the planner compares an owner's grant age against, so it has
// to be the same value the claim itself enforces.
func (obj *Allocator) QuietWindow() time.Duration {
	return obj.ha.QuietWindow
}

// WaitForVersion waits until the allocator has applied at least version.
func (obj *Allocator) WaitForVersion(ctx context.Context, version int64) error {
	for {
		obj.slotsMu.RLock()
		current := obj.version
		changed := obj.versionCh
		obj.slotsMu.RUnlock()
		if current >= version {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}

// Open applies a directory while reopening allocation.
//
// The slots are the ones this node's own placement pass decided it should hold;
// no central planner hands them down. Opening is what clears a pause, so it is
// the path a node takes when it recovers from a heartbeat timeout.
func (obj *Allocator) Open(version int64, applyTick int64, slots []uint32) {
	obj.Reconcile(version, applyTick, slots, true)
}

// CommitRoute applies a directory without changing whether allocation is open.
func (obj *Allocator) CommitRoute(version int64, applyTick int64, slots []uint32) {
	obj.Reconcile(version, applyTick, slots, false)
}

// Reconcile aligns this instance's slots with the desired set for one route
// version.
//
// It is the data plane's only placement entry point, and it is called on every
// heartbeat rather than only when a new directory appears: which slots this node
// holds is decided by the ownership and liveness rows it just read, so a
// takeover can happen without any revision of the directory changing.
//
// The route version is the client-visible revision, so the rules for it are
// deliberately asymmetric from the slot set:
//
//   - a plan for an older version than the one already applied is dropped;
//   - a plan for a newer version is recorded even when the slot set does not
//     change, because consumers use the allocator version as the handoff
//     barrier and would otherwise wait forever after skipping a snapshot;
//   - a plan for the version already applied may still change the slot set,
//     and does not move the version or the barrier, because the published
//     directory did not change.
//
// open says the caller is resuming from a pause, in which case a newer version
// re-arms every slot with a fresh claim instead of reusing the epochs of the
// suspended instance.
func (obj *Allocator) Reconcile(version int64, applyTick int64, slots []uint32, open bool) {
	desired := normalizedSlots(slots)
	obj.slotsMu.Lock()
	if version < obj.version {
		obj.slotsMu.Unlock()
		return
	}
	var detached []detachedSlot
	if open && version > obj.version {
		// A route this instance never opened is applied under freshly claimed
		// authority: the slots it was serving before the pause were fenced, and
		// reusing their epochs would resume handing out ids from a range whose
		// lease the pause may have outlived.
		detached = obj.detachAllLocked()
	}
	detached = append(detached, obj.reconcileLocked(version, applyTick, desired)...)
	if open {
		obj.state.Store(StateReady)
	}
	obj.slotsMu.Unlock()
	obj.drainAndRelease(detached)
}

// reconcileLocked closes the slots this instance no longer wants, and records
// the claim it still has to make for the ones it does. The dropped slots are
// returned rather than deleted outright so the caller can drain them before
// releasing authority (section 6.3).
//
// The caller holds slotsMu and runs drainAndRelease, so the order the protocol
// requires holds across this call: a dropped slot is closed to new allocations
// first, and only then is its authority released.
func (obj *Allocator) reconcileLocked(
	version int64,
	applyTick int64,
	desired []uint32,
) []detachedSlot {
	wanted := make(map[uint32]struct{}, len(desired))
	for _, slotID := range desired {
		wanted[slotID] = struct{}{}
	}

	var detached []detachedSlot
	for slotID, slot := range obj.slots {
		if _, keep := wanted[slotID]; keep {
			continue
		}
		obj.cachedKeys.Add(-slot.count.Load())
		obj.cancelSlotRetries(slot)
		slot.draining.Store(true)
		detached = append(detached, detachedSlot{slotID: slotID, slot: slot})
		delete(obj.slots, slotID)
	}
	obj.rebuildCleanupSlotsLocked()

	// need is what this instance still has to claim. A slot that is installed
	// and open is left alone: re-claiming one it already holds would be a no-op
	// in the store and a needless statement on every heartbeat. A slot that is
	// still installed but closed -- fenced because a renewal did not confirm the
	// grant -- is claimed again, because the plan still hands it to this node and
	// only a fresh epoch can re-arm it. Without that, a fence this node can
	// recover from on its own would retire the slot for the life of the process.
	need := make([]uint32, 0, len(desired))
	for _, slotID := range desired {
		if slot, installed := obj.slots[slotID]; installed && !slot.draining.Load() {
			continue
		}
		need = append(need, slotID)
	}

	pending := obj.prepareApply
	if pending != nil && pending.Version == version && slices.Equal(pending.Slots, need) {
		// The plan already recorded is the one this pass recomputed. Leaving it
		// in place is what preserves its apply tick and the quiet-window retry
		// backoff: rebuilding it would restart the wait on every heartbeat, and
		// a claim that has to wait out W would never be retried.
		return detached
	}
	if pending == nil && version == obj.version && len(need) == 0 {
		// Stable state: nothing to claim and no new revision to apply.
		return detached
	}

	// A new plan supersedes any quiet-window backoff the previous one left
	// behind: the window is enforced by the claim itself, and this is a real
	// change rather than the same refused attempt repeated.
	obj.claimRetryAfter.Store(0)
	obj.planGeneration++
	obj.prepareApply = &PrepareApply{
		Version:    version,
		ApplyTick:  applyTick,
		Slots:      need,
		Generation: obj.planGeneration,
	}
	obj.logger.Info(
		"slot change",
		append(obj.ownershipLogArgs(),
			slog.Int64("version", version),
			slog.Int("new_slot_count", len(need)),
			slog.Int("detached_slot_count", len(detached)),
		)...,
	)
	return detached
}

// normalizedSlots returns the desired slot set in one canonical order, without
// duplicates. The order matters because the pending plan is compared against the
// next pass's, and the comparison has to answer "same set" rather than "same
// listing".
func normalizedSlots(slots []uint32) []uint32 {
	if len(slots) == 0 {
		return nil
	}
	normalized := slices.Clone(slots)
	slices.Sort(normalized)
	return slices.Compact(normalized)
}

// ApplyRoute claims the authority for a pending route's slots and installs them.
//
// A route whose slots cannot all be claimed stays pending and is retried on the
// next tick: installing a slot before this instance is its authoritative owner
// would let it hand out ids the real owner may also hand out. That is also why
// the allocator version, which is the cross-node handoff barrier, only advances
// once the claim has committed.
func (obj *Allocator) ApplyRoute(tick int64) {
	if !obj.applying.CompareAndSwap(false, true) {
		return
	}
	defer obj.applying.Store(false)
	obj.slotsMu.RLock()
	pending := obj.prepareApply
	obj.slotsMu.RUnlock()
	if pending == nil || pending.ApplyTick > tick {
		return
	}
	// A fenced instance claims nothing. It cannot serve a slot any more, so a
	// claim would only pin one to a node that will never allocate from it until
	// the grant aged out. The pending route is left in place rather than dropped:
	// the fence is sticky, so nothing recoverable is lost by never applying it.
	if obj.stopping.Load() || obj.fenced.Load() != nil {
		return
	}
	if retryAt := obj.claimRetryAfter.Load(); retryAt != 0 &&
		time.Now().UnixNano() < retryAt {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), obj.storeTimeout())
	defer cancel()
	grants, complete := obj.claimSlots(ctx, pending.Slots)
	if !complete {
		obj.claimRetryAfter.Store(
			time.Now().Add(obj.claimRetryInterval()).UnixNano(),
		)
		obj.logger.Warn(
			"slot claim deferred until the quiet window elapses",
			append(obj.ownershipLogArgs(),
				slog.Int64("version", pending.Version),
				slog.Int("slot_count", len(pending.Slots)),
			)...,
		)
	}
	if complete {
		obj.claimRetryAfter.Store(0)
	}

	obj.slotsMu.Lock()
	// The generation is the check, not the version: a local replan may have
	// replaced this plan with a different slot set under the same published
	// revision while the claim was in flight, and installing the superseded
	// plan's slots would put this instance back on a slot it no longer wants.
	if obj.stopping.Load() || obj.fenced.Load() != nil || obj.prepareApply == nil ||
		obj.prepareApply.Generation != pending.Generation {
		obj.slotsMu.Unlock()
		obj.releaseUninstalled(ctx, grants)
		return
	}
	var detached []detachedSlot
	remaining := make([]uint32, 0, len(pending.Slots))
	obsolete := make(map[uint32]localGrant)
	for _, slotID := range pending.Slots {
		grant, ok := grants[slotID]
		if obj.hasAuthority() &&
			(!ok || obj.monoNow() >= grant.anchor+int64(obj.ha.LeaseDeadline())) {
			remaining = append(remaining, slotID)
			if ok {
				obsolete[slotID] = grant
			}
			continue
		}
		if old := obj.slots[slotID]; old != nil {
			old.draining.Store(true)
			obj.cancelSlotRetries(old)
			obj.cachedKeys.Add(-old.count.Load())
			detached = append(detached, detachedSlot{slotID: slotID, slot: old})
		}
		// A new authority never inherits a cached range or an in-flight gate.
		slot := &allocationSlot{}
		obj.installGrant(slot, grant)
		obj.slots[slotID] = slot
	}
	obj.rebuildCleanupSlotsLocked()
	// Only a larger published revision moves the client-visible version and
	// releases the handoff barrier. A plan for the version already applied
	// changed which slots this node holds, not what the fleet was told.
	if len(remaining) == 0 && pending.Version > obj.version {
		obj.version = pending.Version
		close(obj.versionCh)
		obj.versionCh = make(chan struct{})
	}
	if len(remaining) == 0 {
		obj.logger.Info("apply route change", slog.Int64("version", pending.Version))
		obj.prepareApply = nil
	} else {
		next := *pending
		next.Slots = remaining
		obj.prepareApply = &next
	}
	obj.slotsMu.Unlock()
	obj.drainAndReleaseWithin(ctx, detached)
	obj.releaseUninstalled(ctx, obsolete)
}

func (obj *Allocator) releaseUninstalled(ctx context.Context, grants map[uint32]localGrant) {
	targets := make([]SlotAuthority, 0, len(grants))
	for slot, grant := range grants {
		targets = append(targets, SlotAuthority{
			SlotID: slot, InstanceID: obj.instanceID, Epoch: grant.epoch,
		})
	}
	sort.Slice(targets, func(i, j int) bool { return targets[i].SlotID < targets[j].SlotID })
	obj.releaseSlots(ctx, targets)
}
