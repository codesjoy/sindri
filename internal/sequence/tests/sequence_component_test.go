package sequence_test

import (
	"context"
	"fmt"
	"testing"

	sequencev1 "github.com/codesjoy/skuld/gen/go/sequence/v1"
	"github.com/codesjoy/skuld/internal/sequence/biz"
	gormdata "github.com/codesjoy/skuld/internal/sequence/data/gorm"
	"github.com/codesjoy/skuld/internal/sequence/service"
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

func (c *inProcessClient) Invoke(
	ctx context.Context,
	method string,
	args, reply interface{},
) error {
	switch method {
	case "/codesjoy.skuld.sequence.v1.SequenceGenerator/FetchNext":
		response, err := c.service.FetchNext(ctx, args.(*sequencev1.FetchNextRequest))
		if err == nil {
			proto.Merge(reply.(*sequencev1.FetchNextResponse), response)
		}
		return err
	case "/codesjoy.skuld.sequence.v1.SequenceGenerator/GetRoute":
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
	require.NoError(t, db.AutoMigrate(&gormdata.SequenceModel{}, &gormdata.RouteModel{}))
	sqlDB, err := db.DB()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })

	key := "orders"
	repo := gormdata.NewSequenceData(db)
	allocator := biz.NewAllocator(&biz.AllocatorConfig{Step: 10}, repo, nil)
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

	restarted := biz.NewAllocator(&biz.AllocatorConfig{Step: 10}, gormdata.NewSequenceData(db), nil)
	restarted.Open(1, 0, []uint32{biz.SlotForKey(key)})
	restarted.ApplyRoute(0)
	restartedClient := sequencev1.NewSequenceGeneratorClient(&inProcessClient{
		service: service.NewSequenceService(restarted, route),
	})
	afterRestart, err := restartedClient.FetchNext(
		context.Background(),
		&sequencev1.FetchNextRequest{Key: key},
	)
	require.NoError(t, err)
	assert.Equal(t, int64(11), afterRestart.Id)

	snapshot, err := restartedClient.GetRoute(context.Background(), &sequencev1.GetRouteRequest{})
	require.NoError(t, err)
	assert.Equal(t, int64(1), snapshot.Route.Version)
}
