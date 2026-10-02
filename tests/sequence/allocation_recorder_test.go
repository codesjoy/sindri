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

package sequence_test

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/codesjoy/pkg/basic/xerror"
	"github.com/codesjoy/sindri/gen/go/sequence/reason"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/code"
)

// allowedTransient reports whether an error is one the client may see while the
// fleet is moving a key between owners.
func allowedTransient(err error) bool {
	if err == nil {
		return true
	}
	return xerror.IsCode(err, code.Code_UNAVAILABLE) ||
		xerror.IsCode(err, code.Code_DEADLINE_EXCEEDED) ||
		xerror.IsCode(err, code.Code_CANCELLED) ||
		errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, context.Canceled) ||
		xerror.IsReason(err, reason.Reason_SEQUENCE_ALLOCATOR_PAUSED) ||
		xerror.IsReason(err, reason.Reason_SEQUENCE_STORAGE_UNAVAILABLE) ||
		xerror.IsReason(err, reason.Reason_SEQUENCE_ROUTE_EXPIRED) ||
		xerror.IsReason(err, reason.Reason_SEQUENCE_SLOT_NOT_OWNER) ||
		xerror.IsReason(err, reason.Reason_SEQUENCE_ROUTE_UNAVAILABLE)
}

// allocationObservation is one allocation attempt as observed by a client.
//
// The (Started, Received) pair is what makes the strict-ordering contract
// checkable from the client side: §1.3 constrains only requests that were
// initiated after an earlier request had already returned, so overlapping
// requests — whose responses may legitimately arrive out of order (§1.4) — are
// excluded from the ordering check.
type allocationObservation struct {
	Key      string
	ID       int64
	Err      error
	Started  time.Time
	Received time.Time
}

// orderLogCompactionThreshold is the entry count beyond which an
// allocationOrderLog folds its oldest safe prefix into baseMax.
const orderLogCompactionThreshold = 1 << 14

// allocationOrderLog records, per key, the ids of successful allocations in
// completion order together with a prefix maximum, so the largest id that had
// already been delivered when a given request started is answerable in O(log n).
//
// Entries whose completion predates Received-retention can never be the
// already-delivered record for a request that is still to be observed, so they
// are folded into baseMax and dropped. That bounds memory under long load
// without weakening the check.
type allocationOrderLog struct {
	received  []time.Time
	prefixMax []int64
	baseMax   int64
}

// maxDeliveredBefore returns the largest id whose delivery completed strictly
// before t.
func (l *allocationOrderLog) maxDeliveredBefore(t time.Time) int64 {
	index := sort.Search(len(l.received), func(i int) bool { return !l.received[i].Before(t) })
	best := l.baseMax
	if index > 0 && l.prefixMax[index-1] > best {
		best = l.prefixMax[index-1]
	}
	return best
}

func (l *allocationOrderLog) append(received time.Time, id int64) {
	best := id
	if n := len(l.prefixMax); n > 0 && l.prefixMax[n-1] > best {
		best = l.prefixMax[n-1]
	}
	l.received = append(l.received, received)
	l.prefixMax = append(l.prefixMax, best)
}

func (l *allocationOrderLog) compact(cutoff time.Time) {
	drop := 0
	for drop < len(l.received) && l.received[drop].Before(cutoff) {
		drop++
	}
	if drop == 0 {
		return
	}
	if max := l.prefixMax[drop-1]; max > l.baseMax {
		l.baseMax = max
	}
	l.received = append([]time.Time(nil), l.received[drop:]...)
	l.prefixMax = append([]int64(nil), l.prefixMax[drop:]...)
}

// allocationRecorder aggregates every allocation a client observes and enforces
// the Appendix F.1 gate: no duplicated id, no stale and no out-of-order
// delivery.
//
// A client cannot observe the server's linearization order, so what is checked
// here is the observable form of S2 stated in §1.3: if request A has already
// returned and request B is initiated afterwards, then id(B) must be greater
// than id(A). Separating a stale delivery from an ordering violation needs the
// per-slot epoch that Phase 3 adds to responses; until then both collapse into
// orderViolations, which is the stronger client-visible signal.
type allocationRecorder struct {
	mu     sync.Mutex
	ids    map[string]map[int64]struct{}
	max    map[string]int64
	order  map[string]*allocationOrderLog
	errors []error

	duplicateDeliveries int
	orderViolations     int

	// retention bounds how long a request may remain in flight. It must exceed
	// the largest client call timeout in the suite, or compaction could drop an
	// entry that is still the already-delivered record for a live request.
	retention time.Duration
}

func newAllocationRecorder() *allocationRecorder {
	return &allocationRecorder{
		ids:       make(map[string]map[int64]struct{}),
		max:       make(map[string]int64),
		order:     make(map[string]*allocationOrderLog),
		retention: 30 * time.Second,
	}
}

func (r *allocationRecorder) record(observation allocationObservation) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := observation.Key
	if observation.Err != nil {
		r.errors = append(r.errors, observation.Err)
		if !allowedTransient(observation.Err) {
			codeValue, hasCode := xerror.CodeOf(observation.Err)
			reasonValue, domain, metadata, hasReason := xerror.ReasonOf(observation.Err)
			return fmt.Errorf(
				"unexpected allocation error: type=%T code=%s has_code=%t "+
					"reason=%q domain=%q metadata=%v has_reason=%t: %w",
				observation.Err,
				codeValue,
				hasCode,
				reasonValue,
				domain,
				metadata,
				hasReason,
				observation.Err,
			)
		}
		return nil
	}
	if r.ids[key] == nil {
		r.ids[key] = make(map[int64]struct{})
	}
	if _, duplicate := r.ids[key][observation.ID]; duplicate {
		r.duplicateDeliveries++
		return fmt.Errorf("duplicate successful id for %q: %d", key, observation.ID)
	}
	r.ids[key][observation.ID] = struct{}{}
	if observation.ID > r.max[key] {
		r.max[key] = observation.ID
	}

	log := r.order[key]
	if log == nil {
		log = &allocationOrderLog{}
		r.order[key] = log
	}
	deliveredBefore := log.maxDeliveredBefore(observation.Started)
	if observation.ID <= deliveredBefore {
		r.orderViolations++
		return fmt.Errorf(
			"allocation order violation for %q: id %d returned for a request started at %s, "+
				"but id %d had already been delivered before it started",
			key,
			observation.ID,
			observation.Started.Format(time.RFC3339Nano),
			deliveredBefore,
		)
	}
	log.append(observation.Received, observation.ID)
	if len(log.received) > orderLogCompactionThreshold {
		log.compact(observation.Received.Add(-r.retention))
	}
	return nil
}

func (r *allocationRecorder) maxID(key string) int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.max[key]
}

// counters returns the Appendix F.1 gate counters. Every one of them must stay
// at zero: a single duplicated, stale or out-of-order delivery is a P0 defect,
// never a bounded or acceptable degradation (D8).
func (r *allocationRecorder) counters() (duplicates, orderViolations int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.duplicateDeliveries, r.orderViolations
}

// assertNoViolations fails the test when the F.1 gate counters are non-zero.
func (r *allocationRecorder) assertNoViolations(t require.TestingT) {
	duplicates, orderViolations := r.counters()
	require.Zero(t, duplicates, "duplicate_delivery_count must be zero")
	require.Zero(t, orderViolations, "allocation_order_violation must be zero")
}

// The oracle below is the Appendix F.1 gate's own regression test: it is the
// only artifact that can turn the handoff/restart system tests into
// strict-ordering tests, so it has to be shown to fire before its silence on
// those tests means anything. It carries no build tag on purpose, so the fast
// unit gate runs it too.

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

// TestAllocationRecorderCleanRunPassesGate is the oracle's negative control: a
// strictly increasing, duplicate-free run must leave every Appendix F.1 gate
// counter at zero so assertNoViolations stays silent for well-behaved traffic.
func TestAllocationRecorderCleanRunPassesGate(t *testing.T) {
	recorder := newAllocationRecorder()
	base := time.Now()

	for index := range 10 {
		require.NoError(t, recorder.record(allocationObservation{
			Key:      "k",
			ID:       int64(index + 1),
			Started:  base.Add(time.Duration(index) * time.Millisecond),
			Received: base.Add(time.Duration(index)*time.Millisecond + time.Microsecond),
		}))
	}

	recorder.assertNoViolations(t)
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
