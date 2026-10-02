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
	"testing"
	"time"

	testkit "github.com/codesjoy/sindri/internal/pkg/tests"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func defaultMigrationConfig() MigrationConfig {
	var c MigrationConfig
	if err := testkit.DecodeDefaults(&c); err != nil {
		panic(err)
	}
	return c
}

type placementFake struct {
	segments  []OwnershipSegment
	nodes     []NodeInfo
	err       error
	published []byte
	version   int64
	tenure    CoordinatorLease
}

func (f *placementFake) OwnershipSegments(
	context.Context,
	time.Duration,
) ([]OwnershipSegment, error) {
	return f.segments, f.err
}

func (f *placementFake) LiveNodes(context.Context, time.Duration) ([]NodeInfo, error) {
	return f.nodes, f.err
}

func (f *placementFake) AcquireCoordinator(
	context.Context,
	string,
	time.Duration,
) (CoordinatorLease, error) {
	return f.tenure, f.err
}

func (f *placementFake) MaterialiseRoute(
	_ context.Context,
	segments []OwnershipSegment,
	layout int64,
	_ CoordinatorLease,
) (PublishResult, error) {
	payload, err := EncodeOwnershipSegments(segments, layout)
	if err != nil {
		return PublishResult{}, err
	}
	if !SameRoutePayload(payload, f.published) {
		f.version++
		f.published = payload
	}
	return PublishResult{Revision: f.version, PayloadBytes: len(payload)}, nil
}

func fullOwnedSegment(node, instance string) []OwnershipSegment {
	return []OwnershipSegment{
		{
			StartSlot:       0,
			EndSlot:         SlotCount - 1,
			OwnerNodeID:     node,
			OwnerInstanceID: instance,
			Epoch:           1,
			State:           SlotOwned,
			GrantAgeKnown:   true,
		},
	}
}

func TestPlanningPrioritizesRecoveryWithoutMovingHealthySlots(t *testing.T) {
	nodes := []NodeInfo{{ID: "b", InstanceID: "ib"}, {ID: "a", InstanceID: "ia"}}
	targets, err := planTargetsFromSegments(
		[]OwnershipSegment{{StartSlot: 0, EndSlot: SlotCount - 1, State: SlotUnowned}},
		nodes,
	)
	require.NoError(t, err)
	counts := map[string]int{}
	for _, s := range targets {
		counts[s.TargetNodeID]++
	}
	assert.Equal(t, SlotCount/2, counts["a"])
	assert.Equal(t, SlotCount/2, counts["b"])
	targets, err = planTargetsFromSegments(fullOwnedSegment("a", "ia"), nodes)
	require.NoError(t, err)
	for _, s := range targets {
		require.Equal(t, "a", s.TargetNodeID)
	}
}

func TestMigrationStableAdmissionMinimalMovementAndPendingAccounting(t *testing.T) {
	nodes := []NodeInfo{{ID: "b", InstanceID: "ib"}, {ID: "a", InstanceID: "ia"}}
	s := fullOwnedSegment("a", "ia")
	assert.Empty(t, planMigrations(s, nodes, nil, 15*time.Second, SlotCount))
	nodes[0].StableFor = 15 * time.Second
	plans := planMigrations(s, nodes, nil, 15*time.Second, SlotCount)
	require.Len(t, plans, SlotCount/2)
	for i, h := range plans {
		assert.Equal(t, uint32(i), h.SlotID)
		assert.Equal(t, "ib", h.TargetInstanceID)
		require.NotEmpty(t, h.ID)
	}
	assert.Empty(t, planMigrations(s, nodes, plans, 15*time.Second, SlotCount))
}

func TestPlanningPinsTransfersButRecoversCompletedReleaseBoundary(t *testing.T) {
	segments := fullOwnedSegment("a", "ia")
	segments[0].State = SlotDraining
	segments[0].QuietWindowOverdue = true
	nodes := []NodeInfo{{ID: "b", InstanceID: "ib"}}
	targets, err := planTargetsFromSegments(segments, nodes)
	require.NoError(t, err)
	assert.Equal(t, "a", targets[0].TargetNodeID)
	segments[0].ReleaseReady = true
	targets, err = planTargetsFromSegments(segments, nodes)
	require.NoError(t, err)
	assert.Equal(t, "b", targets[0].TargetNodeID)
}

func TestPublisherPublishesOnlyAuthorityAndPreservesVersionForSameContent(t *testing.T) {
	var cfg ControlPlaneConfig
	require.NoError(t, testkit.DecodeDefaults(&cfg))
	f := &placementFake{
		segments: fullOwnedSegment("a", "ia"),
		tenure:   CoordinatorLease{Held: true, InstanceID: "control", Epoch: 1},
	}
	p := NewPublisher(cfg, 5*time.Second, "control", f, f, nil)
	require.NoError(t, p.Pass(context.Background()))
	assert.EqualValues(t, 1, p.Stats().Revision)
	require.NoError(t, p.Pass(context.Background()))
	assert.EqualValues(t, 1, p.Stats().Revision)
	assert.True(t, p.Readiness(time.Now()).Ready)
	f.tenure.Held = false
	require.NoError(t, p.Pass(context.Background()))
	assert.EqualValues(t, 1, p.Stats().Revision)
	f.segments[0].Epoch++
	f.tenure.Held = true
	require.NoError(t, p.Pass(context.Background()))
	assert.EqualValues(t, 2, p.Stats().Revision)
}

func TestRouteCacheMonotonicVersions(t *testing.T) {
	c := NewRouteCache()
	assert.Zero(t, c.Version())
	c.UpdateRoute(
		&Route{
			Version:       2,
			LayoutVersion: 1,
			Segments: []RouteSegment{
				{
					StartSlot:       0,
					EndSlot:         SlotCount - 1,
					OwnerNodeID:     "a",
					OwnerInstanceID: "ia",
					Epoch:           3,
				},
			},
		},
	)
	c.UpdateRoute(&Route{Version: 1})
	assert.EqualValues(t, 2, c.Version())
	assert.Equal(t, "a", c.OwnerOf(0))
	epoch, ok := c.EpochOf(0)
	require.True(t, ok)
	assert.EqualValues(t, 3, epoch)
	assert.Empty(t, c.OwnerOf(SlotCount))
}

func TestRoutePayloadIgnoresLeaseClassificationButNotAuthority(t *testing.T) {
	left := []OwnershipSegment{
		{
			StartSlot:       0,
			EndSlot:         9,
			OwnerNodeID:     "a",
			OwnerInstanceID: "ia",
			Epoch:           2,
			State:           SlotOwned,
			GrantAgeKnown:   true,
		},
		{
			StartSlot:          10,
			EndSlot:            SlotCount - 1,
			OwnerNodeID:        "a",
			OwnerInstanceID:    "ia",
			Epoch:              2,
			State:              SlotDraining,
			GrantAgeKnown:      true,
			QuietWindowOverdue: true,
		},
	}
	bytes, err := EncodeOwnershipSegments(left, 1)
	require.NoError(t, err)
	route, err := DecodeRoute(1, bytes)
	require.NoError(t, err)
	require.Len(t, route.Segments, 1)
	assert.EqualValues(t, 2, route.Segments[0].Epoch)
	var pretty map[string]any
	require.NoError(t, json.Unmarshal(bytes, &pretty))
	other, err := json.MarshalIndent(pretty, "", "  ")
	require.NoError(t, err)
	assert.True(t, SameRoutePayload(bytes, other))
	assert.False(t, SameRoutePayload(bytes, []byte("{}")))
}

func TestRouteDecodeRejectsIncompleteSegments(t *testing.T) {
	for _, payload := range []string{`{}`, `{"layout_version":1}`, `{"layout_version":1,"segments":[{"start_slot":1,"end_slot":16383}]}`, `{"layout_version":1,"segments":[{"end_slot":16383,"owner_node_id":"a","owner_instance_id":"ia"}]}`, `{"layout_version":1,"segments":[{"end_slot":16384}]}`} {
		_, err := DecodeRoute(1, []byte(payload))
		require.Error(t, err)
	}
	bytes, err := EncodeOwnershipSegments(
		[]OwnershipSegment{{StartSlot: 0, EndSlot: SlotCount - 1, State: SlotUnowned, Epoch: 8}},
		1,
	)
	require.NoError(t, err)
	r, err := DecodeRoute(1, bytes)
	require.NoError(t, err)
	assert.EqualValues(t, 8, r.Segments[0].Epoch)
}
