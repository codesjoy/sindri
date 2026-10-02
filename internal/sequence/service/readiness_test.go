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

package service

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	testkit "github.com/codesjoy/sindri/internal/pkg/tests"
	"github.com/codesjoy/sindri/internal/sequence/biz"
	"github.com/codesjoy/sindri/internal/sequence/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// handlerSequenceRepo satisfies the reservation side of the allocator without
// reaching storage, because no readiness state is allowed to depend on whether
// storage answers.
type handlerSequenceRepo struct{}

func (handlerSequenceRepo) ReserveRanges(
	context.Context,
	biz.ReservationAuthority,
	[]biz.ReservationRequest,
) ([]biz.SequenceRange, error) {
	return []biz.SequenceRange{{Start: 1, End: 10}}, nil
}

// handlerOwnershipRepo reports every slot as unowned. A readiness verdict that
// changed on this answer would mean the probe is reading ownership, which the
// section D.3 rule forbids.

type handlerMemorySampler struct{}

func (handlerMemorySampler) MemoryUsage() (uint64, uint64) { return 1, 100 }

func testReadinessBounds() ReadinessBounds {
	return ReadinessBounds{
		QuietWindow:   5 * time.Second,
		LeaseDuration: 10 * time.Second,
		MaxPause:      time.Second,
	}
}

// newProbeAllocator builds an allocator that has not yet been given a route, so
// its readiness is whatever the caller drives it to next.
func newProbeAllocator(t *testing.T) *biz.Allocator {
	t.Helper()
	plane := biz.DataPlaneConfig{
		Allocator: biz.AllocatorConfig{
			DefaultStep:     10,
			MaxStep:         100,
			IdleTimeout:     time.Hour,
			CleanupInterval: time.Minute,
		},
		Node: biz.NodeConfig{ID: "node-a"},
	}
	require.NoError(t, testkit.DecodeDefaults(&plane))
	return biz.NewAllocator(
		plane,
		handlerSequenceRepo{},
		testutil.NewAuthority(),
		handlerMemorySampler{},
		slog.Default(),
	)
}

// handlerPublisherRepo is the storage side of a publisher whose passes have not
// run yet, so the probe's control half starts in its initializing state.
type handlerPublisherRepo struct{}

func (handlerPublisherRepo) OwnershipSegments(
	context.Context,
	time.Duration,
) ([]biz.OwnershipSegment, error) {
	return []biz.OwnershipSegment{{
		StartSlot: 0, EndSlot: biz.SlotCount - 1,
		OwnerNodeID: "node-a", OwnerInstanceID: "instance-1",
		State: biz.SlotOwned, Epoch: 1, GrantAgeKnown: true,
	}}, nil
}

func (handlerPublisherRepo) MaterialiseRoute(
	context.Context,
	[]biz.OwnershipSegment,
	int64,
	biz.CoordinatorLease,
) (biz.PublishResult, error) {
	return biz.PublishResult{Revision: 1, PayloadBytes: 64}, nil
}

type handlerCoordinatorRepo struct{}

func (handlerCoordinatorRepo) AcquireCoordinator(
	_ context.Context,
	instanceID string,
	_ time.Duration,
) (biz.CoordinatorLease, error) {
	return biz.CoordinatorLease{Held: true, InstanceID: instanceID, Epoch: 1}, nil
}

// newProbePublisher builds a publisher whose loop has not run yet, so its
// readiness is whatever the caller drives it to next.
func newProbePublisher(t *testing.T) *biz.Publisher {
	t.Helper()
	return biz.NewPublisher(
		biz.ControlPlaneConfig{
			LayoutVersion:     1,
			CoordinatorLease:  10 * time.Second,
			ReconcileInterval: time.Minute,
			PassTimeout:       time.Second,
		},
		time.Second,
		"instance-1",
		handlerPublisherRepo{},
		handlerCoordinatorRepo{},
		slog.Default(),
	)
}

// probe drives the handler the bundle registers and returns the status and the
// decoded body.
//
// The response body is closed here rather than handed to the caller: the
// recorder's body is already fully written, so every caller reads it exactly
// once and none of them has a reason to hold it open.
func probe(
	t *testing.T,
	allocator *biz.Allocator,
	publisher *biz.Publisher,
) (int, readinessBody) {
	t.Helper()
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, ReadinessPath, nil)
	NewReadinessHandler(allocator, publisher, testReadinessBounds())(recorder, request)

	response := recorder.Result()
	defer func() { require.NoError(t, response.Body.Close()) }()
	var body readinessBody
	require.NoError(t, json.NewDecoder(response.Body).Decode(&body))
	return response.StatusCode, body
}

// TestReadinessProbeFailsBeforeTheFirstRoute covers the initializing state: a
// probe that answered 200 here would send traffic to an instance that has no
// route to allocate from, and the request would fail downstream instead of
// being kept away.
func TestReadinessProbeFailsBeforeTheFirstRoute(t *testing.T) {
	status, body := probe(t, newProbeAllocator(t), nil)

	assert.Equal(t, http.StatusServiceUnavailable, status)
	assert.False(t, body.Ready)
	assert.Equal(t, "initializing", body.Reason)
	require.NotNil(t, body.Data)
	assert.Nil(t, body.Control)
	assert.Equal(t, 5.0, body.Data.QuietWindowSeconds)
	assert.Equal(t, 10.0, body.Data.LeaseDurationSeconds)
	assert.Equal(t, 1.0, body.Data.MaxPauseSeconds)
}

func TestReadinessProbeSucceedsOnceARouteIsApplied(t *testing.T) {
	allocator := newProbeAllocator(t)
	allocator.RenewLeases()

	// A live lease is not a route: the instance is registered and renewing, but
	// it has nothing to answer from yet, so it must still fail the probe.
	status, body := probe(t, allocator, nil)
	assert.Equal(t, http.StatusServiceUnavailable, status)
	assert.False(t, body.Ready)
	assert.Equal(t, "initializing", body.Reason)

	// Applying a route is what moves it into service.
	allocator.MarkRouteApplied()
	status, body = probe(t, allocator, nil)

	assert.Equal(t, http.StatusOK, status)
	assert.True(t, body.Ready)
	assert.Equal(t, "serving", body.Reason)
}

// TestReadinessProbeFailsWhileStopping covers the shutdown path. The process
// must fail its probe before it stops accepting, otherwise a probe's success
// would be the last thing it says.
func TestReadinessProbeFailsWhileStopping(t *testing.T) {
	allocator := newProbeAllocator(t)
	allocator.RenewLeases()
	allocator.Shutdown()

	status, body := probe(t, allocator, nil)

	assert.Equal(t, http.StatusServiceUnavailable, status)
	assert.False(t, body.Ready)
	assert.Equal(t, "stopping", body.Reason)
}

// TestReadinessProbeStaysReadyThroughStorageLossAndEmptySlots is the section
// D.17 rule as a test.
//
// Both conditions look alarming and neither is improved by a restart: the
// protocol already refuses the affected requests with a retriable reason, so
// reporting the instance unready would turn a recoverable storage blip into a
// rolling restart of every replica at once.
func TestReadinessProbeStaysReadyThroughStorageLossAndEmptySlots(t *testing.T) {
	allocator := newProbeAllocator(t)
	// A node that applied a route while holding no slots at all: the slot set is
	// empty and every ownership read fails, which is the worst case this rule
	// has to survive.
	allocator.RenewLeases()
	allocator.MarkRouteApplied()
	allocator.Pause()

	status, body := probe(t, allocator, nil)

	assert.Equal(t, http.StatusOK, status)
	assert.True(t, body.Ready)
	assert.Equal(t, "serving", body.Reason)
}

// TestControlReadinessProbeFailsBeforeTheFirstPass covers the control plane's
// initializing state: a replica that has not completed a pass yet cannot say
// whether its loop works, so it must not be reported as serving.
func TestControlReadinessProbeFailsBeforeTheFirstPass(t *testing.T) {
	status, body := probe(t, nil, newProbePublisher(t))

	assert.Equal(t, http.StatusServiceUnavailable, status)
	assert.False(t, body.Ready)
	assert.Equal(t, "initializing", body.Reason)
	require.NotNil(t, body.Control)
	assert.Nil(t, body.Data)
}

func TestControlReadinessProbeSucceedsOnceTheLoopHasPassed(t *testing.T) {
	publisher := newProbePublisher(t)
	require.NoError(t, publisher.Pass(context.Background()))

	status, body := probe(t, nil, publisher)

	assert.Equal(t, http.StatusOK, status)
	assert.True(t, body.Ready)
	assert.Equal(t, "serving", body.Reason)
	require.NotNil(t, body.Control)
	assert.False(t, body.Control.LastPassFailed)
	assert.Equal(t, int64(1), body.Control.Passes)
	assert.Equal(t, int64(1), body.Control.Revision)
}

// TestCombinedReadinessIsTheConjunctionOfBothHalves pins the rule a process
// that runs both halves must follow: a combined process that has stopped
// publishing keeps answering from the last directory, which is exactly the
// failure a probe has to surface.
func TestCombinedReadinessIsTheConjunctionOfBothHalves(t *testing.T) {
	allocator := newProbeAllocator(t)
	allocator.RenewLeases()
	allocator.MarkRouteApplied()

	status, body := probe(t, allocator, newProbePublisher(t))

	assert.Equal(t, http.StatusServiceUnavailable, status)
	assert.False(t, body.Ready)
	assert.Equal(t, "initializing", body.Reason)
	require.NotNil(t, body.Data)
	require.NotNil(t, body.Control)
	assert.True(t, body.Data.Ready)
	assert.False(t, body.Control.Ready)
}

func TestCombinedReadinessReportsBothHalvesServing(t *testing.T) {
	allocator := newProbeAllocator(t)
	allocator.RenewLeases()
	allocator.MarkRouteApplied()
	publisher := newProbePublisher(t)
	require.NoError(t, publisher.Pass(context.Background()))

	status, body := probe(t, allocator, publisher)

	assert.Equal(t, http.StatusOK, status)
	assert.True(t, body.Ready)
	assert.Equal(t, "serving", body.Reason)
	require.NotNil(t, body.Data)
	require.NotNil(t, body.Control)
	assert.True(t, body.Data.Ready)
	assert.True(t, body.Control.Ready)
}
