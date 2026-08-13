//go:build integration && chaos

package sequence_test

import (
	"context"
	"fmt"
	"sync"
	"time"

	toxiclient "github.com/Shopify/toxiproxy/v2/client"
	"github.com/codesjoy/pkg/basic/xerror"
	"github.com/codesjoy/sindri/gen/go/sequence/reason"
	sequencev1 "github.com/codesjoy/sindri/gen/go/sequence/v1"
)

func (s *SequenceSystemSuite) TestOwnerTimeoutDuringRouteHandoff() {
	key := "chaos-owner-timeout"
	versionA := s.publishRoute(allSlots("node-a"))
	before := s.waitForOwnership("node-a", key, versionA)
	routed := s.routedClient()
	before = s.waitRoutedAbove(routed, key, before)

	_, err := s.proxies["grpc-a"].AddToxic(
		"owner-timeout",
		"timeout",
		"downstream",
		1,
		toxiclient.Attributes{"timeout": 100},
	)
	s.Require().NoError(err)
	versionB := s.publishRoute(allSlots("node-b"))
	s.waitForOwnership("node-b", key, versionB)
	after := s.waitRoutedAbove(routed, key, before)
	removeToxic(s.T(), s.proxies["grpc-a"], "owner-timeout")
	s.Greater(after, before)
	s.GreaterOrEqual(s.watermark(key), after)
}

func (s *SequenceSystemSuite) TestOwnerResetDuringRouteHandoff() {
	key := "chaos-owner-reset"
	versionA := s.publishRoute(allSlots("node-a"))
	before := s.waitForOwnership("node-a", key, versionA)
	routed := s.routedClient()
	before = s.waitRoutedAbove(routed, key, before)

	_, err := s.proxies["grpc-a"].AddToxic(
		"owner-reset",
		"reset_peer",
		"downstream",
		1,
		toxiclient.Attributes{"timeout": 0},
	)
	s.Require().NoError(err)
	versionB := s.publishRoute(allSlots("node-b"))
	s.waitForOwnership("node-b", key, versionB)
	after := s.waitRoutedAbove(routed, key, before)
	removeToxic(s.T(), s.proxies["grpc-a"], "owner-reset")
	s.Greater(after, before)
	s.GreaterOrEqual(s.watermark(key), after)
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
	s.Require().Eventually(func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
		defer cancel()
		_, fetchErr := s.fetchDirect(ctx, "node-a", key, version)
		return xerror.IsReason(fetchErr, reason.Reason_SEQUENCE_ALLOCATOR_PAUSED)
	}, recoveryDeadline, 50*time.Millisecond)

	removeToxic(s.T(), s.proxies["db-a"], "route-query-latency")
	after := s.waitForOwnership("node-a", key, version)
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
				response, err := routed.FetchNext(
					callCtx,
					&sequencev1.FetchNextRequest{Key: key},
				)
				callCancel()
				var id int64
				if response != nil {
					id = response.GetId()
				}
				if err := recorder.record(key, id, err); err != nil {
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
		recorder.phase("handoff-" + owner)
		s.T().Logf("chaos phase: publish ownership to %s", owner)
		before := recorder.maxID(key)
		version = s.publishRoute(allSlots(owner))
		ownerID := s.waitForOwnership(owner, key, version)
		s.Greater(ownerID, before)
		s.T().Logf("chaos phase: %s converged at route %d", owner, version)
		s.Require().NoError(waitForProgress(before))
		if index == 1 {
			recorder.phase("restart-node-b")
			s.T().Log("chaos phase: restart node-b")
			s.stopNode("node-b")
			s.restartNode("node-b")
		}
		if index == 2 {
			recorder.phase("restart-node-a")
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
	maxID := recorder.maxID(key)
	s.Greater(maxID, int64(0))
	s.GreaterOrEqual(s.watermark(key), maxID)
}
