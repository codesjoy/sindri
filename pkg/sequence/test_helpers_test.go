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

package sequence

import (
	"context"
	"strconv"
	"testing"

	"github.com/codesjoy/pkg/basic/xerror"
	"github.com/codesjoy/sindri/gen/go/sequence/reason"
	sequencev1 "github.com/codesjoy/sindri/gen/go/sequence/v1"
	"github.com/stretchr/testify/require"
)

func testRoute(version int64, nodeIDs ...string) *sequencev1.RouteSnapshot {
	snapshot := &sequencev1.RouteSnapshot{Version: version, LayoutVersion: 1}
	for slot := 0; slot < SlotCount; slot++ {
		nodeID := nodeIDs[slot%len(nodeIDs)]
		snapshot.Segments = append(
			snapshot.Segments,
			&sequencev1.RouteSegment{
				StartSlot:       uint32(slot),
				EndSlot:         uint32(slot),
				OwnerNodeId:     nodeID,
				OwnerInstanceId: nodeID + "-instance",
				SlotEpoch:       1,
			},
		)
	}
	return snapshot
}

// testSegment is one run of slots in a segmented test route. Splitting the space
// into explicit runs is what lets a test give two slots the same owner under
// different epochs, which is the case a batch has to notice.
type testSegment struct {
	nodeID string
	epoch  uint64
	from   uint32
	to     uint32
}

// testSegmentedRoute builds a snapshot carrying both views, as a snapshot with
// authoritative ownership does: the node lists, which an older caller reads, and
// the ownership segments, which carry the epochs and must agree with them.
func testSegmentedRoute(
	version, layout int64,
	segments ...testSegment,
) *sequencev1.RouteSnapshot {
	snapshot := &sequencev1.RouteSnapshot{Version: version, LayoutVersion: layout}
	for _, segment := range segments {
		snapshot.Segments = append(snapshot.Segments, &sequencev1.RouteSegment{
			StartSlot:       segment.from,
			EndSlot:         segment.to,
			OwnerNodeId:     segment.nodeID,
			OwnerInstanceId: segment.nodeID + "-instance",
			SlotEpoch:       segment.epoch,
		})
	}
	return snapshot
}

func newSegmentedTestRouter(t *testing.T, snapshot *sequencev1.RouteSnapshot) *Router {
	t.Helper()
	router, err := NewRouter(func(context.Context, int64) (*sequencev1.GetRouteResponse, error) {
		return &sequencev1.GetRouteResponse{Route: snapshot}, nil
	})
	require.NoError(t, err)
	require.NoError(t, router.Update(snapshot))
	return router
}

// keyInSlotRange returns a key that hashes into a slot range, so a test can build
// a batch whose keys land in chosen segments.
func keyInSlotRange(t *testing.T, from, to uint32) string {
	t.Helper()
	for i := 0; i < 1_000_000; i++ {
		key := "key-" + strconv.Itoa(i)
		if slot := SlotForKey(key); slot >= from && slot < to {
			return key
		}
	}
	t.Fatalf("no key hashes into slots [%d,%d)", from, to)
	return ""
}

func retryError(action, after string) error {
	return xerror.NewWithReason(reason.Reason_SEQUENCE_OWNER_RECOVERING,
		"owner recovering", map[string]string{RetryableMetaKey: action, RetryAfterMetaKey: after})
}
