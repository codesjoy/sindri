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

//go:build integration

package sequence_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// allocationRecorder is the Appendix F.1 gate: it is the only artifact that can
// turn the existing handoff/restart system tests into strict-ordering tests, so
// it has to be shown to fire before its silence on those tests means anything.

func TestAllocationRecorderRejectsStaleDeliveryAfterCompletion(t *testing.T) {
	recorder := newAllocationRecorder()
	base := time.Now()

	require.NoError(t, recorder.record(allocationObservation{
		Key: "k", ID: 100, Started: base, Received: base.Add(time.Millisecond),
	}))

	err := recorder.record(allocationObservation{
		Key:      "k",
		ID:       99,
		Started:  base.Add(2 * time.Millisecond),
		Received: base.Add(3 * time.Millisecond),
	})
	require.Error(t, err, "a smaller id issued after a larger one had returned must be rejected")

	duplicates, orderViolations := recorder.counters()
	require.Zero(t, duplicates)
	require.Equal(t, 1, orderViolations, "stale_delivery_count must be counted")
}

func TestAllocationRecorderAllowsConcurrentOutOfOrderResponses(t *testing.T) {
	recorder := newAllocationRecorder()
	base := time.Now()

	// A and B overlap in flight, so their responses may legitimately arrive in
	// the opposite order to their linearization (section 1.4). Neither must be
	// reported.
	require.NoError(t, recorder.record(allocationObservation{
		Key: "k", ID: 100,
		Started:  base.Add(time.Millisecond),
		Received: base.Add(2 * time.Millisecond),
	}))
	require.NoError(t, recorder.record(allocationObservation{
		Key: "k", ID: 99,
		Started:  base,
		Received: base.Add(3 * time.Millisecond),
	}))

	_, orderViolations := recorder.counters()
	require.Zero(t, orderViolations, "overlapping requests must not be judged on response order")
}

func TestAllocationRecorderAllowsSequentialIncrease(t *testing.T) {
	recorder := newAllocationRecorder()
	base := time.Now()

	for index := range 50 {
		require.NoError(t, recorder.record(allocationObservation{
			Key:      "k",
			ID:       int64(index + 1),
			Started:  base.Add(time.Duration(index) * time.Millisecond),
			Received: base.Add(time.Duration(index)*time.Millisecond + time.Microsecond),
		}))
	}

	duplicates, orderViolations := recorder.counters()
	require.Zero(t, duplicates)
	require.Zero(t, orderViolations)
	require.Equal(t, int64(50), recorder.maxID("k"))
}

func TestAllocationRecorderRejectsDuplicateDelivery(t *testing.T) {
	recorder := newAllocationRecorder()
	base := time.Now()

	require.NoError(t, recorder.record(allocationObservation{
		Key: "k", ID: 7, Started: base, Received: base.Add(time.Millisecond),
	}))
	err := recorder.record(allocationObservation{
		Key:      "k",
		ID:       7,
		Started:  base.Add(2 * time.Millisecond),
		Received: base.Add(3 * time.Millisecond),
	})
	require.Error(t, err, "the same id delivered twice must be rejected")

	duplicates, _ := recorder.counters()
	require.Equal(t, 1, duplicates, "duplicate_delivery_count must be counted")
}

// TestAllocationRecorderKeepsCheckAcrossCompaction feeds enough sequential
// allocations to fold the order log's oldest safe prefix into its base maximum,
// then asserts a small id issued long afterwards is still rejected. Without the
// base maximum carrying across compaction the gate would silently go vacuous
// under the sustained load of the chaos test.
func TestAllocationRecorderKeepsCheckAcrossCompaction(t *testing.T) {
	recorder := newAllocationRecorder()
	base := time.Now().Add(-time.Hour)

	const records = 4 * orderLogCompactionThreshold
	for index := range records {
		received := base.Add(time.Duration(index) * time.Millisecond)
		require.NoError(t, recorder.record(allocationObservation{
			Key: "k",
			// Even ids only, so the id checked after compaction is genuinely new
			// rather than a duplicate that would trip the other gate.
			ID:       int64(2 * (index + 1)),
			Started:  received,
			Received: received,
		}))
	}

	log := recorder.order["k"]
	require.NotNil(t, log)
	require.Less(
		t,
		len(log.received),
		records,
		"the order log must be compacted under sustained load",
	)
	require.Greater(t, log.baseMax, int64(0), "compaction must have folded a prefix")

	err := recorder.record(allocationObservation{
		Key:      "k",
		ID:       3,
		Started:  base.Add(time.Duration(records) * time.Millisecond),
		Received: base.Add(time.Duration(records+1) * time.Millisecond),
	})
	require.Error(t, err, "compaction must not forget ids delivered before the retained window")

	duplicates, orderViolations := recorder.counters()
	require.Zero(t, duplicates)
	require.Equal(t, 1, orderViolations)
}
