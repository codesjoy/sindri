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

//go:build integration && chaos

package sequence_test

import (
	"context"
	"fmt"
	"sync"
	"time"

	toxiclient "github.com/Shopify/toxiproxy/v2/client"
	sequencev1 "github.com/codesjoy/sindri/gen/go/sequence/v1"
)

// TestOwnerOutageDuringRouteHandoff covers the two ways a misbehaving owner
// surfaces to a routed client while a key moves away from it: the call hangs
// until its deadline and the call is reset outright. In both cases the client
// must keep making progress on the successor once the route moves, and the
// watermark may never run ahead of an id the client actually received.
func (s *SequenceSystemSuite) TestOwnerOutageDuringRouteHandoff() {
	cases := []struct {
		name  string
		toxic string
		attrs toxiclient.Attributes
	}{
		{name: "timeout", toxic: "timeout", attrs: toxiclient.Attributes{"timeout": 100}},
		{name: "reset", toxic: "reset_peer", attrs: toxiclient.Attributes{"timeout": 0}},
	}
	for _, test := range cases {
		s.Run(test.name, func() {
			key := "chaos-owner-" + test.name
			versionA := s.publishRoute(allSlots("node-a"))
			before := s.waitForOwnership("node-a", key, versionA)
			routed := s.routedClient()
			before = s.waitRoutedAbove(routed, key, before)

			_, err := s.proxies["grpc-a"].AddToxic(
				"owner-outage",
				test.toxic,
				"downstream",
				1,
				test.attrs,
			)
			s.Require().NoError(err)
			versionB := s.publishRoute(allSlots("node-b"))
			s.waitForOwnership("node-b", key, versionB)
			after := s.waitRoutedAbove(routed, key, before)
			removeToxic(s.T(), s.proxies["grpc-a"], "owner-outage")
			s.Greater(after, before)
			s.GreaterOrEqual(s.watermark(key), after)
		})
	}
}

func (s *SequenceSystemSuite) TestDatabaseLatencyPausesAndRecovers() {
	key := "chaos-database-latency"
	version := s.publishRoute(allSlots("node-a"))
	before := s.waitForOwnership("node-a", key, version)

	_, err := s.proxies["db-a"].AddToxic(
		"route-query-latency",
		"latency",
		"upstream",
		1,
		toxiclient.Attributes{"latency": 350, "jitter": 0},
	)
	s.Require().NoError(err)
	// Every statement the node makes now costs more than the route query's own
	// timeout, so its renewals stop confirming and its local lease lapses. The
	// node then refuses to allocate rather than spend a lease it cannot renew;
	// which retriable reason surfaces depends on where the stall is noticed, so
	// the assertion is the shared property, not one reason code.
	s.Require().Eventually(func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
		defer cancel()
		_, fetchErr := s.fetchDirect(ctx, "node-a", key, version)
		return refusedWhileStorageUnavailable(fetchErr)
	}, recoveryDeadline, 50*time.Millisecond)

	removeToxic(s.T(), s.proxies["db-a"], "route-query-latency")

	// The stall outlives the quiet window, and that is the protocol working
	// rather than a fault. Allocation is paused only after three heartbeats have
	// failed, and each of those heartbeats spans most of the window on its own,
	// so by the time the refusal is observable the node has been away for
	// several windows, has left the live set, and node-b has taken the slot
	// over. A lapsed grant is served by whoever can claim it; the fleet never
	// hands it back on its own.
	//
	// Recovering the key onto the node that was stalled therefore takes the same
	// step the handoff tests take: authority is staged for the recovered node
	// once it is back in the live set, and that node has to claim the slot and
	// serve from it again.
	s.waitForLiveNode("node-a")
	recovered := s.publishRoute(allSlots("node-a"))
	after := s.waitForOwnership("node-a", key, recovered)
	s.Greater(after, before)
	s.GreaterOrEqual(s.watermark(key), after)
}

func (s *SequenceSystemSuite) TestDatabaseRestartPreservesWatermarkAndReconnects() {
	key := "chaos-database-restart"
	version := s.publishRoute(allSlots("node-a"))
	s.waitForRoute("node-a", version)

	stopTimeout := 5 * time.Second
	databaseStopped := false
	restoreDatabase := func() error {
		if !databaseStopped {
			return nil
		}
		restoreCtx, restoreCancel := context.WithTimeout(
			context.Background(),
			recoveryDeadline,
		)
		defer restoreCancel()
		if err := s.h.container.Start(restoreCtx); err != nil {
			return err
		}
		dsn, err := s.h.connectionString(restoreCtx)
		if err != nil {
			return err
		}
		s.h.dsn = dsn
		if err := waitForDatabase(restoreCtx, s.h.sqlDriver, dsn); err != nil {
			return err
		}
		databaseStopped = false
		return nil
	}
	defer func() {
		if err := restoreDatabase(); err != nil {
			s.T().Errorf("restore %s database: %v", s.dialect, err)
		}
	}()
	s.Require().NoError(s.h.container.Stop(s.ctx, &stopTimeout))
	databaseStopped = true
	s.Require().Eventually(func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
		defer cancel()
		_, err := s.fetchDirect(ctx, "node-a", key, version)
		return err != nil && allowedTransient(err)
	}, recoveryDeadline, 50*time.Millisecond)
	s.Require().NoError(restoreDatabase())

	after := s.waitForOwnership("node-a", key, version)
	s.Greater(after, int64(0))
	s.GreaterOrEqual(s.watermark(key), after)
}

func (s *SequenceSystemSuite) TestDeterministicHandoffsAndRestartsUnderLoad() {
	key := "chaos-continuous"
	version := s.publishRoute(allSlots("node-a"))
	s.waitForOwnership("node-a", key, version)
	routed := s.routedClient()
	recorder := newAllocationRecorder()

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	errCh := make(chan error, 8)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				callCtx, callCancel := context.WithTimeout(ctx, 400*time.Millisecond)
				started := time.Now()
				response, err := routed.FetchNext(
					callCtx,
					&sequencev1.FetchNextRequest{Key: key},
				)
				received := time.Now()
				callCancel()
				var id int64
				if response != nil {
					id = response.GetId()
				}
				if err := recorder.record(allocationObservation{
					Key:      key,
					ID:       id,
					Err:      err,
					Started:  started,
					Received: received,
				}); err != nil {
					select {
					case errCh <- err:
					default:
					}
					return
				}
			}
		}()
	}
	var stopOnce sync.Once
	stopWorkers := func() {
		stopOnce.Do(func() {
			cancel()
			wg.Wait()
		})
	}
	defer stopWorkers()
	waitForProgress := func(before int64) error {
		deadline := time.NewTimer(recoveryDeadline)
		defer deadline.Stop()
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case err := <-errCh:
				return err
			default:
			}
			if recorder.maxID(key) > before {
				return nil
			}
			select {
			case err := <-errCh:
				return err
			case <-ticker.C:
			case <-deadline.C:
				return fmt.Errorf("allocation did not progress beyond %d", before)
			}
		}
	}

	for index, owner := range []string{"node-b", "node-a", "node-b", "node-a"} {
		s.T().Logf("chaos phase: publish ownership to %s", owner)
		before := recorder.maxID(key)
		version = s.publishRoute(allSlots(owner))
		ownerID := s.waitForOwnership(owner, key, version)
		s.Greater(ownerID, before)
		s.T().Logf("chaos phase: %s converged at route %d", owner, version)
		s.Require().NoError(waitForProgress(before))
		if index == 1 {
			s.T().Log("chaos phase: restart node-b")
			s.stopNode("node-b")
			s.restartNode("node-b")
		}
		if index == 2 {
			s.T().Log("chaos phase: restart node-a")
			s.stopNode("node-a")
			s.restartNode("node-a")
		}
	}
	stopWorkers()
	close(errCh)
	for err := range errCh {
		s.Require().NoError(err)
	}
	recorder.assertNoViolations(s.T())
	maxID := recorder.maxID(key)
	s.Greater(maxID, int64(0))
	s.GreaterOrEqual(s.watermark(key), maxID)
}
