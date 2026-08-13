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

package gorm

import (
	"encoding/json"
	"testing"

	"github.com/codesjoy/sindri/internal/sequence/biz"
)

func completePayload(t *testing.T) []byte {
	t.Helper()
	even := make([]uint32, 0, biz.SlotCount/2)
	odd := make([]uint32, 0, biz.SlotCount/2)
	for slot := biz.SlotCount - 1; slot >= 0; slot-- {
		if slot%2 == 0 {
			even = append(even, uint32(slot))
		} else {
			odd = append(odd, uint32(slot))
		}
	}
	payload, err := json.Marshal(routePayload{Nodes: []storedRouteNode{
		{NodeID: "node-b", Slots: odd},
		{NodeID: "node-a", Slots: even},
	}})
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func TestDecodeRouteValidatesAndSortsSnapshot(t *testing.T) {
	route, err := decodeRoute(RouteModel{Version: 7, Payload: completePayload(t)})
	if err != nil {
		t.Fatal(err)
	}
	if route.Version != 7 || len(route.Nodes) != 2 {
		t.Fatalf("unexpected route: %+v", route)
	}
	if route.Nodes[0].NodeID != "node-a" || route.Nodes[1].NodeID != "node-b" {
		t.Fatalf("nodes were not sorted: %+v", route.Nodes)
	}
	if route.Nodes[0].Slots[0] != 0 || route.Nodes[0].Slots[1] != 2 {
		t.Fatalf("slots were not sorted: %v", route.Nodes[0].Slots[:2])
	}
}

func TestDecodeRouteRejectsInvalidSnapshots(t *testing.T) {
	tests := []struct {
		name  string
		model RouteModel
	}{
		{name: "version", model: RouteModel{Payload: completePayload(t)}},
		{name: "json", model: RouteModel{Version: 1, Payload: []byte(`{"nodes":`)}},
		{
			name: "missing slots",
			model: RouteModel{
				Version: 1,
				Payload: []byte(`{"nodes":[{"node_id":"node-a","slots":[0]}]}`),
			},
		},
		{
			name: "duplicate slot",
			model: RouteModel{
				Version: 1,
				Payload: []byte(`{"nodes":[{"node_id":"node-a","slots":[0,0]}]}`),
			},
		},
		{
			name: "out of range",
			model: RouteModel{
				Version: 1,
				Payload: []byte(`{"nodes":[{"node_id":"node-a","slots":[16384]}]}`),
			},
		},
		{
			name: "empty node",
			model: RouteModel{
				Version: 1,
				Payload: []byte(`{"nodes":[{"node_id":"","slots":[0]}]}`),
			},
		},
		{
			name: "duplicate node",
			model: RouteModel{
				Version: 1,
				Payload: []byte(
					`{"nodes":[{"node_id":"node-a","slots":[0]},{"node_id":"node-a","slots":[1]}]}`,
				),
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := decodeRoute(test.model); err == nil {
				t.Fatal("expected route validation error")
			}
		})
	}
}
