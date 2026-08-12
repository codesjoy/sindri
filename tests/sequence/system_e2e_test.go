//go:build integration

package sequence_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/codesjoy/pkg/basic/xerror"
	"github.com/codesjoy/skuld/gen/go/reason"
	sequencev1 "github.com/codesjoy/skuld/gen/go/sequence/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"google.golang.org/genproto/googleapis/rpc/code"
)

func TestSequenceSystemPostgres(t *testing.T) {
	suite.Run(t, &SequenceSystemSuite{dialect: "postgres"})
}

func TestSequenceSystemMySQL(t *testing.T) {
	suite.Run(t, &SequenceSystemSuite{dialect: "mysql"})
}

func (s *SequenceSystemSuite) TestEndToEndAllocationAndRestart() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := s.directClient("node-a").GetRoute(ctx, &sequencev1.GetRouteRequest{})
	s.Require().Error(err)
	s.True(xerror.IsReason(err, reason.Reason_SEQUENCE_ROUTE_UNAVAILABLE))
	_, err = s.fetchDirect(ctx, "node-a", "orders", 1)
	s.Require().Error(err)
	s.True(xerror.IsReason(err, reason.Reason_SEQUENCE_ALLOCATOR_PAUSED))
	var rangeCount int64
	db := openGORM(s.T(), s.h)
	s.Require().NoError(db.Table("sequence_ranges").Count(&rangeCount).Error)
	s.Zero(rangeCount, "a node without a route must not reserve a range")

	route := splitSlots()
	version := s.publishRoute(route)
	key := keyForOwner("node-a", route)
	s.waitForOwnership("node-a", key, version)
	s.waitForOwnership("node-b", keyForOwner("node-b", route), version)

	gotRoute, err := s.directClient("node-a").GetRoute(
		context.Background(),
		&sequencev1.GetRouteRequest{KnownVersion: version},
	)
	s.Require().NoError(err)
	s.True(gotRoute.GetNotModified())
	s.Nil(gotRoute.GetRoute())

	routed := s.routedClient()
	_, err = routed.FetchNext(context.Background(), &sequencev1.FetchNextRequest{})
	s.Require().Error(err)
	s.True(xerror.IsCode(err, code.Code_INVALID_ARGUMENT))

	var previous int64
	for range 20 {
		response, fetchErr := routed.FetchNext(
			context.Background(),
			&sequencev1.FetchNextRequest{Key: key},
		)
		s.Require().NoError(fetchErr)
		s.Greater(response.GetId(), previous)
		previous = response.GetId()
	}

	recorder := newAllocationRecorder()
	const workers = 64
	var wg sync.WaitGroup
	errCh := make(chan error, workers)
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			callCtx, callCancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer callCancel()
			response, fetchErr := routed.FetchNext(
				callCtx,
				&sequencev1.FetchNextRequest{Key: key},
			)
			var id int64
			if response != nil {
				id = response.GetId()
			}
			if recordErr := recorder.record(key, id, fetchErr); recordErr != nil {
				errCh <- recordErr
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for recordErr := range errCh {
		s.Require().NoError(recordErr)
	}
	beforeRestart := recorder.maxID(key)
	s.Greater(beforeRestart, previous)

	s.stopNode("node-a")
	s.restartNode("node-a")
	var afterRestart int64
	s.Require().Eventually(func() bool {
		callCtx, callCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer callCancel()
		response, fetchErr := routed.FetchNext(
			callCtx,
			&sequencev1.FetchNextRequest{Key: key},
		)
		if fetchErr != nil {
			s.True(allowedTransient(fetchErr), "unexpected recovery error: %v", fetchErr)
			return false
		}
		afterRestart = response.GetId()
		return afterRestart > beforeRestart
	}, recoveryDeadline, 50*time.Millisecond)
	s.GreaterOrEqual(s.watermark(key), afterRestart)
}

func (s *SequenceSystemSuite) TestLiveRouteHandoff() {
	key := "handoff-orders"
	versionA := s.publishRoute(allSlots("node-a"))
	before := s.waitForOwnership("node-a", key, versionA)

	versionB := s.publishRoute(allSlots("node-b"))
	s.waitForRejection("node-a", key, versionB)
	after := s.waitForOwnership("node-b", key, versionB)
	s.Greater(after, before)
	s.GreaterOrEqual(s.watermark(key), after)
}

func (s *SequenceSystemSuite) TestNodeCrashFailoverAndRecovery() {
	key := "crash-failover"
	versionA := s.publishRoute(allSlots("node-a"))
	s.waitForOwnership("node-a", key, versionA)
	routed := s.routedClient()
	response, err := routed.FetchNext(
		context.Background(),
		&sequencev1.FetchNextRequest{Key: key},
	)
	s.Require().NoError(err)
	maxID := response.GetId()

	s.stopNode("node-a")
	versionB := s.publishRoute(allSlots("node-b"))
	s.waitForOwnership("node-b", key, versionB)
	maxID = s.waitRoutedAbove(routed, key, maxID)

	s.restartNode("node-a")
	versionA2 := s.publishRoute(allSlots("node-a"))
	s.waitForOwnership("node-a", key, versionA2)
	s.waitForRejection("node-b", key, versionA2)
	maxID = s.waitRoutedAbove(routed, key, maxID)
	s.GreaterOrEqual(s.watermark(key), maxID)
}

func (s *SequenceSystemSuite) TestDatabaseDisconnectFailsClosedAndRecovers() {
	key := "chaos-database-disconnect"
	version := s.publishRoute(allSlots("node-a"))
	s.waitForRoute("node-a", version)

	s.Require().NoError(s.proxies["db-a"].Disable())
	defer func() {
		if err := s.proxies["db-a"].Enable(); err != nil {
			s.T().Errorf("enable db-a proxy: %v", err)
		}
	}()
	s.Require().Eventually(func() bool {
		callCtx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
		defer cancel()
		_, err := s.fetchDirect(callCtx, "node-a", key, version)
		return xerror.IsReason(err, reason.Reason_SEQUENCE_ALLOCATOR_PAUSED)
	}, recoveryDeadline, 50*time.Millisecond)

	s.Require().NoError(s.proxies["db-a"].Enable())
	// The deferred enable is intentionally idempotent and protects failures above.
	after := s.waitForOwnership("node-a", key, version)
	s.Greater(after, int64(0))
	s.GreaterOrEqual(s.watermark(key), after)
}

func (s *SequenceSystemSuite) waitRoutedAbove(
	client sequencev1.SequenceGeneratorClient,
	key string,
	minimum int64,
) int64 {
	var id int64
	var lastErr error
	recovered := assert.EventuallyWithT(s.T(), func(t *assert.CollectT) {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		response, err := client.FetchNext(ctx, &sequencev1.FetchNextRequest{Key: key})
		if err != nil {
			lastErr = err
			assert.True(t, allowedTransient(err), "unexpected recovery error: %v", err)
			assert.Fail(t, "allocation has not recovered")
			return
		}
		id = response.GetId()
		lastErr = nil
		assert.Greater(t, id, minimum)
	}, recoveryDeadline, 50*time.Millisecond)
	if !recovered {
		version := int64(0)
		var snapshot *sequencev1.RouteSnapshot
		if s.router != nil {
			version = s.router.Version()
			snapshot = s.router.Snapshot()
		}
		codeValue, hasCode := xerror.CodeOf(lastErr)
		reasonValue, domain, metadata, hasReason := xerror.ReasonOf(lastErr)
		s.T().Logf(
			"routed recovery stopped: router_version=%d snapshot_version=%d "+
				"error_type=%T code=%s has_code=%t reason=%q domain=%q "+
				"metadata=%v has_reason=%t error=%v",
			version,
			snapshot.GetVersion(),
			lastErr,
			codeValue,
			hasCode,
			reasonValue,
			domain,
			metadata,
			hasReason,
			lastErr,
		)
	}
	return id
}
