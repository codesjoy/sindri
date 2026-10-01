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
	"fmt"
	"log/slog"
	"strconv"
	"testing"
	"time"

	sequencev1 "github.com/codesjoy/sindri/gen/go/sequence/v1"
	testkit "github.com/codesjoy/sindri/internal/pkg/tests"
	"github.com/codesjoy/sindri/internal/sequence/biz"
	sequencedata "github.com/codesjoy/sindri/internal/sequence/data"
	"github.com/codesjoy/sindri/internal/sequence/service"
	"github.com/codesjoy/yggdrasil/v3/rpc/stream"
	transportclient "github.com/codesjoy/yggdrasil/v3/transport/runtime/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type inProcessClient struct {
	service *service.SequenceService
}

type testMemorySampler struct{}

func (testMemorySampler) MemoryUsage() (uint64, uint64) { return 1, 100 }

func (c *inProcessClient) Invoke(
	ctx context.Context,
	method string,
	args, reply interface{},
) error {
	switch method {
	case "/codesjoy.sindri.sequence.v1.SequenceGenerator/FetchNext":
		response, err := c.service.FetchNext(ctx, args.(*sequencev1.FetchNextRequest))
		if err == nil {
			proto.Merge(reply.(*sequencev1.FetchNextResponse), response)
		}
		return err
	case "/codesjoy.sindri.sequence.v1.SequenceGenerator/FetchNextBatch":
		response, err := c.service.FetchNextBatch(ctx, args.(*sequencev1.FetchNextBatchRequest))
		if err == nil {
			proto.Merge(reply.(*sequencev1.FetchNextBatchResponse), response)
		}
		return err
	case "/codesjoy.sindri.sequence.v1.SequenceGenerator/GetRoute":
		response, err := c.service.GetRoute(ctx, args.(*sequencev1.GetRouteRequest))
		if err == nil {
			proto.Merge(reply.(*sequencev1.GetRouteResponse), response)
		}
		return err
	default:
		return fmt.Errorf("unexpected method %q", method)
	}
}

func (*inProcessClient) NewStream(
	context.Context,
	*stream.Desc,
	string,
) (stream.ClientStream, error) {
	return nil, fmt.Errorf("streams are unsupported")
}

func (*inProcessClient) Close() error { return nil }

var _ transportclient.Client = (*inProcessClient)(nil)

func TestGeneratedClientDrivesServiceAllocatorAndSQLiteRepo(t *testing.T) {
	db, err := gorm.Open(
		sqlite.Open("file:sequence-component?mode=memory&cache=shared"),
		&gorm.Config{},
	)
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(
		&sequencedata.SequenceModel{},
		&sequencedata.RouteModel{},
		&sequencedata.OwnershipOutboxModel{},
	))
	sqlDB, err := db.DB()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })

	key := "orders"
	prepareSQLiteOwnership(t, db, key)
	repo := sequencedata.NewSequenceData(db)
	allocator := newComponentAllocator(
		componentDataPlane(biz.AllocatorConfig{DefaultStep: 10, MaxStep: 100}),
		repo,
		sequencedata.NewOwnershipData(db),
		testMemorySampler{},
		nil,
	)
	allocator.Open(1, 0, []uint32{biz.SlotForKey(key)})
	allocator.ApplyRoute(0)
	route := biz.NewRouteCache()
	route.UpdateRoute(&biz.Route{Version: 1, Nodes: []biz.RouteNode{{
		NodeID: "node-a", Slots: []uint32{biz.SlotForKey(key)},
	}}})
	client := sequencev1.NewSequenceGeneratorClient(&inProcessClient{
		service: service.NewSequenceService(allocator, route),
	})

	first, err := client.FetchNext(context.Background(), &sequencev1.FetchNextRequest{Key: key})
	require.NoError(t, err)
	assert.Equal(t, int64(1), first.Id)

	restarted := newComponentAllocator(
		componentDataPlane(biz.AllocatorConfig{DefaultStep: 10, MaxStep: 100}),
		sequencedata.NewSequenceData(db),
		sequencedata.NewOwnershipData(db),
		testMemorySampler{},
		nil,
	)
	restarted.Open(1, 0, []uint32{biz.SlotForKey(key)})
	restartedClient := sequencev1.NewSequenceGeneratorClient(&inProcessClient{
		service: service.NewSequenceService(restarted, route),
	})
	// The restarted process takes the slot over from the instance that stopped
	// holding it, and the authority permits that only once the quiet window has
	// elapsed on the storage clock. SQLite stores the grant at whole seconds, so
	// the claim lands on the next storage second rather than on the first retry.
	var afterRestart *sequencev1.FetchNextResponse
	require.Eventually(t, func() bool {
		restarted.ApplyRoute(0)
		response, fetchErr := restartedClient.FetchNext(
			context.Background(),
			&sequencev1.FetchNextRequest{Key: key},
		)
		if fetchErr != nil {
			return false
		}
		afterRestart = response
		return true
	}, 5*time.Second, 20*time.Millisecond, "the restarted process never took the slot over")
	assert.Equal(t, int64(11), afterRestart.Id)

	snapshot, err := restartedClient.GetRoute(context.Background(), &sequencev1.GetRouteRequest{})
	require.NoError(t, err)
	assert.Equal(t, int64(1), snapshot.Route.Version)
}

func TestAllocatorPrefetchesDatabaseRangeBeforeExhaustion(t *testing.T) {
	db, err := gorm.Open(
		sqlite.Open("file:sequence-prefetch-component?mode=memory&cache=shared"),
		&gorm.Config{},
	)
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(
		&sequencedata.SequenceModel{},
		&sequencedata.OwnershipOutboxModel{},
	))
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(1)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })

	key := "orders-prefetch"
	prepareSQLiteOwnership(t, db, key)
	allocator := newComponentAllocator(
		componentDataPlane(biz.AllocatorConfig{
			DefaultStep:    10,
			MaxStep:        20,
			PrefetchRatio:  0.5,
			ReserveTimeout: time.Second,
		}),
		sequencedata.NewSequenceData(db),
		sequencedata.NewOwnershipData(db),
		testMemorySampler{},
		nil,
	)
	allocator.Open(1, 0, []uint32{biz.SlotForKey(key)})
	allocator.ApplyRoute(0)

	for want := int64(1); want <= 5; want++ {
		got, fetchErr := allocator.FetchNext(context.Background(), key)
		require.NoError(t, fetchErr)
		assert.Equal(t, want, got)
	}
	require.Eventually(t, func() bool {
		var model sequencedata.SequenceModel
		if queryErr := db.Where("sequence_key = ?", key).Take(&model).Error; queryErr != nil {
			return false
		}
		return model.ReservedEnd > 10
	}, time.Second, 10*time.Millisecond)

	for want := int64(6); want <= 11; want++ {
		got, fetchErr := allocator.FetchNext(context.Background(), key)
		require.NoError(t, fetchErr)
		assert.Equal(t, want, got)
	}
}

func TestGeneratedClientDrivesBatchAllocation(t *testing.T) {
	db, err := gorm.Open(
		sqlite.Open("file:sequence-batch-component?mode=memory&cache=shared"),
		&gorm.Config{},
	)
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(
		&sequencedata.SequenceModel{},
		&sequencedata.RouteModel{},
		&sequencedata.OwnershipOutboxModel{},
	))
	sqlDB, err := db.DB()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })

	keys := sameSlotComponentKeys(t, "orders", 3)
	prepareSQLiteOwnership(t, db, keys...)
	slots := make([]uint32, len(keys))
	for index, key := range keys {
		slots[index] = biz.SlotForKey(key)
	}
	allocator := newComponentAllocator(
		componentDataPlane(biz.AllocatorConfig{DefaultStep: 10, MaxStep: 100}),
		sequencedata.NewSequenceData(db),
		sequencedata.NewOwnershipData(db),
		testMemorySampler{},
		nil,
	)
	allocator.Open(1, 0, slots)
	allocator.ApplyRoute(0)
	route := biz.NewRouteCache()
	route.UpdateRoute(&biz.Route{Version: 1, Nodes: []biz.RouteNode{{
		NodeID: "node-a",
		Slots:  slots,
	}}})
	client := sequencev1.NewSequenceGeneratorClient(&inProcessClient{
		service: service.NewSequenceService(allocator, route),
	})

	response, err := client.FetchNextBatch(
		context.Background(),
		&sequencev1.FetchNextBatchRequest{Requests: []*sequencev1.FetchNextRequest{
			{Key: keys[0]},
			{Key: keys[1], Count: uint32Pointer(2)},
			{Key: keys[2], Count: uint32Pointer(3)},
		}},
	)
	require.NoError(t, err)
	require.Len(t, response.Results, 3)
	assert.Equal(t, keys[0], response.Results[0].Key)
	assert.Equal(t, uint32(1), response.Results[0].Count)
	assert.Equal(t, uint32(2), response.Results[1].Count)
	assert.Equal(t, uint32(3), response.Results[2].Count)
}

func sameSlotComponentKeys(t *testing.T, base string, count int) []string {
	t.Helper()
	slot := biz.SlotForKey(base)
	keys := []string{base}
	for candidate := 0; len(keys) < count; candidate++ {
		key := base + "-" + strconv.Itoa(candidate)
		if biz.SlotForKey(key) == slot {
			keys = append(keys, key)
		}
	}
	return keys
}

func uint32Pointer(value uint32) *uint32 { return &value }

// componentDataPlane builds the data plane configuration these component tests
// drive. Every in-memory linearisation is measured against the pause bound, and
// every held slot serves from a local lease. A zero for either one would discard
// every allocation rather than weaken the fence, so the block is supplied here.
// Production injects it from the validated HA section; the arithmetic of the
// bounds is pinned by the configuration tests and the F.3 model instead of here.
//
// The quiet window is scaled down alongside the lease: these tests assert the
// allocation path, not the platform's real takeover bound, and SQLite measures
// the age of a grant in whole seconds.
func componentDataPlane(cfg biz.AllocatorConfig) biz.DataPlaneConfig {
	plane := biz.DataPlaneConfig{
		Allocator: cfg,
		Node:      biz.NodeConfig{ID: "node-a"},
	}
	if err := testkit.DecodeDefaults(&plane); err != nil {
		panic(err)
	}
	plane.HA.PauseVerified = true
	plane.HA.ClockDisciplined = true
	plane.HA.QuietWindow = 250 * time.Millisecond
	return plane
}

// newComponentAllocator builds the allocator these component tests drive.
func newComponentAllocator(
	plane biz.DataPlaneConfig,
	store biz.SequenceRepo,
	ownership biz.OwnershipRepo,
	sampler biz.MemorySampler,
	logger *slog.Logger,
) *biz.Allocator {
	return biz.NewAllocator(plane, store, ownership, sampler, logger)
}

// prepareSQLiteOwnership creates the ownership authority table on the SQLite
// component database and seeds the given keys' slots as unowned so the
// allocator can claim them through biz.OwnershipRepo. Production creates
// slot_ownership with the HA migration; the component database is built from
// the GORM models, so the authority table is created here.
func prepareSQLiteOwnership(t *testing.T, db *gorm.DB, keys ...string) {
	t.Helper()
	require.NoError(t, db.Exec(
		"CREATE TABLE IF NOT EXISTS slot_ownership ("+
			"slot_id integer PRIMARY KEY, "+
			"owner_node_id varchar(256), "+
			"owner_instance_id varchar(256), "+
			"epoch integer NOT NULL DEFAULT 0, "+
			"granted_at datetime, "+
			"state varchar(16) NOT NULL DEFAULT 'UNOWNED', "+
			"updated_at datetime NOT NULL DEFAULT CURRENT_TIMESTAMP)",
	).Error)
	seen := make(map[uint32]struct{}, len(keys))
	for _, key := range keys {
		slot := biz.SlotForKey(key)
		if _, ok := seen[slot]; ok {
			continue
		}
		seen[slot] = struct{}{}
		require.NoError(t, db.Exec(
			"INSERT OR IGNORE INTO slot_ownership (slot_id) VALUES (?)", slot,
		).Error)
	}
}
