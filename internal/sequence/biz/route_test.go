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
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/codesjoy/pkg/basic/xerror"
	"github.com/codesjoy/sindri/gen/go/sequence/reason"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/code"
)

func TestRouteCacheStartsEmptyAndCoalescesEvents(t *testing.T) {
	cache := NewRouteCache()
	require.NotNil(t, cache.Route())
	assert.Zero(t, cache.Version())

	cache.UpdateRoute(&Route{Version: 1})
	cache.UpdateRoute(&Route{Version: 2})
	assert.Equal(t, int64(2), cache.Version())
	select {
	case event := <-cache.EventChan():
		assert.Equal(t, int64(2), event.Version)
	default:
		t.Fatal("expected a coalesced route event")
	}
}

// completePayload is a valid ownership-backed snapshot whose node list is
// deliberately out of order, so the decode path is shown to sort it.
func completePayload(t *testing.T) []byte {
	t.Helper()
	payload := halvesPayload(t)
	payload.Nodes = []storedRouteNode{payload.Nodes[1], payload.Nodes[0]}
	return marshalPayload(t, payload)
}

func TestDecodeRouteValidatesAndSortsSnapshot(t *testing.T) {
	route, err := DecodeRoute(7, completePayload(t))
	if err != nil {
		t.Fatal(err)
	}
	if route.Version != 7 || len(route.Nodes) != 2 {
		t.Fatalf("unexpected route: %+v", route)
	}
	if route.Nodes[0].NodeID != "node-a" || route.Nodes[1].NodeID != "node-b" {
		t.Fatalf("nodes were not sorted: %+v", route.Nodes)
	}
	if route.Nodes[0].Slots[0] != 0 || route.Nodes[0].Slots[1] != 1 {
		t.Fatalf("slots were not sorted: %v", route.Nodes[0].Slots[:2])
	}
}

func TestDecodeRouteRejectsInvalidSnapshots(t *testing.T) {
	tests := []struct {
		name    string
		version int64
		payload []byte
	}{
		{name: "version", version: 0, payload: completePayload(t)},
		{name: "json", version: 1, payload: []byte(`{"nodes":`)},
		{
			// The shape this encoder used to write. Accepting it would put a
			// directory in the fleet that carries no epoch, so it is refused.
			name:    "legacy node-only snapshot",
			version: 1,
			payload: marshalPayload(t, routePayload{
				Nodes: []storedRouteNode{
					{NodeID: "node-a", Slots: slotRun(0, SlotCount-1)},
				},
			}),
		},
		{
			name:    "missing slots",
			version: 1,
			payload: marshalPayload(t, routePayload{
				LayoutVersion: 1,
				Nodes:         []storedRouteNode{{NodeID: "node-a", Slots: []uint32{0}}},
				Segments: []storedRouteSegment{{
					StartSlot: 0, EndSlot: SlotCount - 1,
					OwnerNodeID: "node-a", OwnerInstanceID: "instance-a", Epoch: 1,
				}},
			}),
		},
		{
			name:    "duplicate slot",
			version: 1,
			payload: marshalPayload(t, routePayload{
				LayoutVersion: 1,
				Nodes:         []storedRouteNode{{NodeID: "node-a", Slots: []uint32{0, 0}}},
				Segments: []storedRouteSegment{{
					StartSlot: 0, EndSlot: SlotCount - 1,
					OwnerNodeID: "node-a", OwnerInstanceID: "instance-a", Epoch: 1,
				}},
			}),
		},
		{
			name:    "out of range",
			version: 1,
			payload: marshalPayload(t, routePayload{
				LayoutVersion: 1,
				Nodes:         []storedRouteNode{{NodeID: "node-a", Slots: []uint32{SlotCount}}},
				Segments: []storedRouteSegment{{
					StartSlot: 0, EndSlot: SlotCount - 1,
					OwnerNodeID: "node-a", OwnerInstanceID: "instance-a", Epoch: 1,
				}},
			}),
		},
		{
			name:    "empty node",
			version: 1,
			payload: marshalPayload(t, routePayload{
				LayoutVersion: 1,
				Nodes:         []storedRouteNode{{NodeID: "", Slots: []uint32{0}}},
				Segments: []storedRouteSegment{{
					StartSlot: 0, EndSlot: SlotCount - 1,
					OwnerNodeID: "node-a", OwnerInstanceID: "instance-a", Epoch: 1,
				}},
			}),
		},
		{
			name:    "duplicate node",
			version: 1,
			payload: marshalPayload(t, routePayload{
				LayoutVersion: 1,
				Nodes: []storedRouteNode{
					{NodeID: "node-a", Slots: []uint32{0}},
					{NodeID: "node-a", Slots: []uint32{1}},
				},
				Segments: []storedRouteSegment{{
					StartSlot: 0, EndSlot: SlotCount - 1,
					OwnerNodeID: "node-a", OwnerInstanceID: "instance-a", Epoch: 1,
				}},
			}),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := DecodeRoute(test.version, test.payload); err == nil {
				t.Fatal("expected route validation error")
			}
		})
	}
}

func slotRun(start, end uint32) []uint32 {
	slots := make([]uint32, 0, end-start+1)
	for slot := start; slot <= end; slot++ {
		slots = append(slots, slot)
	}
	return slots
}

func marshalPayload(t *testing.T, payload routePayload) []byte {
	t.Helper()
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// halvesPayload is a snapshot whose ownership view splits the slot space in two
// and whose node view agrees with it by construction. Tests below break one
// side or the other.
func halvesPayload(t *testing.T) routePayload {
	t.Helper()
	return routePayload{
		LayoutVersion: 1,
		Nodes: []storedRouteNode{
			{NodeID: "node-a", Slots: slotRun(0, SlotCount/2-1)},
			{NodeID: "node-b", Slots: slotRun(SlotCount/2, SlotCount-1)},
		},
		Segments: []storedRouteSegment{
			{
				StartSlot:       0,
				EndSlot:         SlotCount/2 - 1,
				OwnerNodeID:     "node-a",
				OwnerInstanceID: "instance-a",
				Epoch:           3,
			},
			{
				StartSlot:       SlotCount / 2,
				EndSlot:         SlotCount - 1,
				OwnerNodeID:     "node-b",
				OwnerInstanceID: "instance-b",
				Epoch:           1,
			},
		},
	}
}

func TestDecodeRouteAcceptsAnOwnershipView(t *testing.T) {
	route, err := DecodeRoute(9, marshalPayload(t, halvesPayload(t)))
	if err != nil {
		t.Fatal(err)
	}
	if route.LayoutVersion != 1 {
		t.Fatalf("layout version was dropped: %+v", route)
	}
	if len(route.Segments) != 2 {
		t.Fatalf("unexpected segments: %+v", route.Segments)
	}
	first := route.Segments[0]
	if first.StartSlot != 0 || first.EndSlot != SlotCount/2-1 ||
		first.OwnerNodeID != "node-a" || first.OwnerInstanceID != "instance-a" || first.Epoch != 3 {
		t.Fatalf("unexpected first segment: %+v", first)
	}
	if len(route.Nodes) != 2 {
		t.Fatalf("the node view must survive alongside segments: %+v", route.Nodes)
	}
}

func TestDecodeRouteAcceptsUnownedSlots(t *testing.T) {
	payload := halvesPayload(t)
	// The last slot is owned by nobody, so it is paused rather than routed: its
	// segment stays and no node may list the slot.
	payload.Segments[1].EndSlot = SlotCount - 2
	payload.Nodes[1].Slots = slotRun(SlotCount/2, SlotCount-2)
	payload.Segments = append(payload.Segments, storedRouteSegment{
		StartSlot: SlotCount - 1,
		EndSlot:   SlotCount - 1,
	})

	route, err := DecodeRoute(4, marshalPayload(t, payload))
	if err != nil {
		t.Fatal(err)
	}
	if len(route.Segments) != 3 {
		t.Fatalf("unexpected segments: %+v", route.Segments)
	}
	last := route.Segments[2]
	if last.OwnerNodeID != "" || last.StartSlot != SlotCount-1 {
		t.Fatalf("unowned segment gained an owner: %+v", last)
	}
}

func TestDecodeRouteRejectsInvalidOwnershipViews(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*routePayload)
	}{
		{
			name:   "missing layout version",
			mutate: func(payload *routePayload) { payload.LayoutVersion = 0 },
		},
		{
			name: "gap between segments",
			mutate: func(payload *routePayload) {
				payload.Segments[0].EndSlot = SlotCount/2 - 2
			},
		},
		{
			name: "segments not starting at zero",
			mutate: func(payload *routePayload) {
				payload.Segments = payload.Segments[1:]
			},
		},
		{
			name: "segments short of the slot space",
			mutate: func(payload *routePayload) {
				payload.Segments = payload.Segments[:1]
			},
		},
		{
			name: "inverted segment",
			mutate: func(payload *routePayload) {
				payload.Segments[0].StartSlot = SlotCount/2 - 2
				payload.Segments[0].EndSlot = 0
			},
		},
		{
			name: "segment out of range",
			mutate: func(payload *routePayload) {
				payload.Segments[1].EndSlot = SlotCount
			},
		},
		{
			name: "instance without an owner",
			mutate: func(payload *routePayload) {
				payload.Segments[0].OwnerNodeID = ""
			},
		},
		{
			name: "owner without an instance",
			mutate: func(payload *routePayload) {
				payload.Segments[0].OwnerInstanceID = ""
			},
		},
		{
			name: "node disagrees with the ownership view",
			mutate: func(payload *routePayload) {
				payload.Segments[0].OwnerNodeID = "node-b"
				payload.Segments[0].OwnerInstanceID = "instance-b"
			},
		},
		{
			name: "node lists a slot nobody owns",
			mutate: func(payload *routePayload) {
				payload.Segments[0] = storedRouteSegment{StartSlot: 0, EndSlot: SlotCount/2 - 1}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			payload := halvesPayload(t)
			test.mutate(&payload)
			if _, err := DecodeRoute(1, marshalPayload(t, payload)); err == nil {
				t.Fatal("expected ownership view validation error")
			}
		})
	}
}

// TestSameRoutePayloadComparesMeaningNotBytes pins the comparison a reconciler
// uses to decide whether a directory has changed.
//
// A route is stored in a column that does not give back what it was handed: a
// jsonb (or MySQL json) column parses the document and re-serialises it, so the
// same directory comes back with the separators spaced the way that type spaces
// them. A comparison made on bytes would call that a change, mint a revision and
// push the whole directory to the fleet on every pass over an unchanged fleet.
func TestSameRoutePayloadComparesMeaningNotBytes(t *testing.T) {
	payload := marshalPayload(t, halvesPayload(t))

	var respaced bytes.Buffer
	if err := json.Indent(&respaced, payload, "", "  "); err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(payload, respaced.Bytes()) {
		t.Fatal("the test needs two renderings that differ byte for byte")
	}
	if !SameRoutePayload(payload, respaced.Bytes()) {
		t.Fatal("two renderings of one directory must compare equal")
	}

	other := marshalPayload(t, halvesPayload(t))
	var otherParsed routePayload
	if err := json.Unmarshal(other, &otherParsed); err != nil {
		t.Fatal(err)
	}
	// Move one slot to the other node: the directory a reader sees is different,
	// so the payload must compare different.
	otherParsed.Nodes[0].Slots = otherParsed.Nodes[0].Slots[:len(otherParsed.Nodes[0].Slots)-1]
	otherParsed.Nodes[1].Slots = append([]uint32{SlotCount/2 - 1}, otherParsed.Nodes[1].Slots...)
	if SameRoutePayload(payload, marshalPayload(t, otherParsed)) {
		t.Fatal("different directories must compare different")
	}

	if SameRoutePayload(payload, []byte("not json")) {
		t.Fatal("a payload that cannot be decoded must not compare equal")
	}
}

func TestAllocatorAppliesRouteVersionWithoutNewSlots(t *testing.T) {
	tests := []struct {
		name   string
		routes []struct {
			version int64
			slots   []uint32
			apply   bool
		}
		wantVersion int64
	}{
		{
			name: "unchanged slots",
			routes: []struct {
				version int64
				slots   []uint32
				apply   bool
			}{
				{version: 1, slots: []uint32{1}, apply: true},
				{version: 2, slots: []uint32{1}, apply: true},
			},
			wantVersion: 2,
		},
		{
			name: "empty slots",
			routes: []struct {
				version int64
				slots   []uint32
				apply   bool
			}{
				{version: 1, slots: []uint32{1}, apply: true},
				{version: 2, apply: true},
			},
			wantVersion: 2,
		},
		{
			name: "skipped intermediate snapshot",
			routes: []struct {
				version int64
				slots   []uint32
				apply   bool
			}{
				{version: 1, slots: []uint32{1}, apply: true},
				{version: 2, slots: []uint32{1}},
				{version: 3, slots: []uint32{1}, apply: true},
			},
			wantVersion: 3,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			allocator := newHATestAllocator(
				&AllocatorConfig{DefaultStep: 10, MaxStep: 100},
				&rangeStore{max: make(map[string]int64)},
				nil,
				unlimitedMemorySampler,
				slog.Default(),
			)
			for _, route := range test.routes {
				allocator.CommitRoute(route.version, 0, route.slots)
				if route.apply {
					allocator.ApplyRoute(0)
				}
			}
			if got := allocator.CurrentVersion(); got != test.wantVersion {
				t.Fatalf("route version = %d, want %d", got, test.wantVersion)
			}
		})
	}
}

func TestAllocatorRouteRemovalUpdatesCachedKeyCount(t *testing.T) {
	keyA, keyB := "orders", "invoices"
	for SlotForKey(keyA) == SlotForKey(keyB) {
		keyB += "x"
	}
	allocator := readyAllocatorForKeys(t, keyA, keyB)
	_, err := allocator.FetchNext(context.Background(), keyA)
	require.NoError(t, err)
	_, err = allocator.FetchNext(context.Background(), keyB)
	require.NoError(t, err)
	require.Equal(t, int64(2), allocator.Stats().CachedKeys)

	allocator.CommitRoute(2, 0, []uint32{SlotForKey(keyA)})
	assert.Equal(t, int64(1), allocator.Stats().CachedKeys)
	allocator.ApplyRoute(0)
	assert.Equal(t, int64(1), allocator.Stats().CachedKeys)
}

func TestAllocatorWaitForVersion(t *testing.T) {
	allocator := readyAllocatorForKeys(t, "orders")
	done := make(chan error, 1)
	go func() { done <- allocator.WaitForVersion(context.Background(), 3) }()
	select {
	case err := <-done:
		t.Fatalf("WaitForVersion returned before route application: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	allocator.CommitRoute(3, 0, []uint32{SlotForKey("orders")})
	allocator.ApplyRoute(0)
	require.NoError(t, <-done)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	assert.ErrorIs(t, allocator.WaitForVersion(ctx, 4), context.Canceled)
}

func TestAllocatorMemoryAdmissionOnlyRejectsNewKeys(t *testing.T) {
	allocator := readyAllocatorForKeys(t, "orders", "invoices")
	var pressured atomic.Bool
	allocator.memorySampler = memorySamplerFunc(func() (uint64, uint64) {
		if pressured.Load() {
			return 90, 100
		}
		return 89, 100
	})

	_, err := allocator.FetchNext(context.Background(), "orders")
	require.NoError(t, err)
	pressured.Store(true)
	_, err = allocator.FetchNext(context.Background(), "orders")
	require.NoError(t, err, "existing keys remain available above the watermark")
	_, err = allocator.FetchNext(context.Background(), "invoices")
	assert.True(t, xerror.IsReason(err, reason.Reason_SEQUENCE_CAPACITY_EXHAUSTED))
	assert.True(t, xerror.IsCode(err, code.Code_RESOURCE_EXHAUSTED))
	assert.Equal(t, int64(1), allocator.Stats().CachedKeys)
	assert.Equal(t, int64(1), allocator.Stats().AdmissionRejected)
}
