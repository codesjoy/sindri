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
	"errors"
	"testing"

	sequencev1 "github.com/codesjoy/sindri/gen/go/sequence/v1"
	"github.com/codesjoy/yggdrasil/v3/discovery/resolver"
	"github.com/codesjoy/yggdrasil/v3/transport/runtime/client/balancer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSequenceBalancerRoutesSlotsAndTracksRouteUpdates(t *testing.T) {
	router, err := NewRouter(func(context.Context, int64) (*sequencev1.GetRouteResponse, error) {
		return nil, errors.New("unused")
	})
	require.NoError(t, err)
	require.NoError(t, router.Update(testRoute(1, "node-a", "node-b")))
	client := newTestBalancerClient()
	b, err := newSequenceBalancer("svc", BalancerType, client, router)
	require.NoError(t, err)
	b.UpdateState(resolver.BaseState{Endpoints: []resolver.Endpoint{
		testEndpoint("a:1", "node-a"),
		testEndpoint("b:1", "node-b"),
	}})

	picker := client.picker()
	result, err := picker.Next(balancer.RPCInfo{
		Ctx:    WithSlot(context.Background(), 0),
		Method: fetchNextFullMethod,
	})
	require.NoError(t, err)
	assert.Same(t, client.clients["grpc/a:1"], result.RemoteClient())

	updates := client.updateCount()
	require.NoError(t, router.Update(testRoute(2, "node-b", "node-a")))
	assert.Greater(t, client.updateCount(), updates)
	result, err = client.picker().Next(balancer.RPCInfo{
		Ctx:    WithSlot(context.Background(), 0),
		Method: fetchNextFullMethod,
	})
	require.NoError(t, err)
	assert.Same(t, client.clients["grpc/b:1"], result.RemoteClient())
	require.NoError(t, b.Close())
}

func TestSequenceBalancerControlPlaneAndInvalidOwners(t *testing.T) {
	router, err := NewRouter(func(context.Context, int64) (*sequencev1.GetRouteResponse, error) {
		return nil, errors.New("unused")
	})
	require.NoError(t, err)
	require.NoError(t, router.Update(testRoute(1, "node-a")))
	client := newTestBalancerClient()
	b, err := newSequenceBalancer("svc", BalancerType, client, router)
	require.NoError(t, err)
	b.UpdateState(resolver.BaseState{Endpoints: []resolver.Endpoint{
		testEndpoint("a:1", "node-a"),
		testEndpoint("a:2", "node-a"),
		testEndpoint("unknown:1", ""),
	}})

	_, err = client.picker().Next(balancer.RPCInfo{
		Ctx:    WithSlot(context.Background(), 0),
		Method: fetchNextFullMethod,
	})
	require.ErrorIs(t, err, ErrRouteUnavailable)

	seen := map[any]bool{}
	for range 3 {
		result, pickErr := client.picker().Next(balancer.RPCInfo{
			Ctx:    context.Background(),
			Method: getRouteFullMethod,
		})
		require.NoError(t, pickErr)
		seen[result.RemoteClient()] = true
	}
	assert.Len(t, seen, 3)

	first, err := client.picker().Next(balancer.RPCInfo{
		Ctx:    context.Background(),
		Method: getRouteFullMethod,
	})
	require.NoError(t, err)
	b.UpdateState(resolver.BaseState{Endpoints: []resolver.Endpoint{
		testEndpoint("a:1", "node-a"),
		testEndpoint("a:2", "node-a"),
		testEndpoint("unknown:1", ""),
	}})
	second, err := client.picker().Next(balancer.RPCInfo{
		Ctx:    context.Background(),
		Method: getRouteFullMethod,
	})
	require.NoError(t, err)
	assert.NotSame(t, first.RemoteClient(), second.RemoteClient())

	_, err = client.picker().Next(balancer.RPCInfo{
		Ctx:    context.Background(),
		Method: fetchNextFullMethod,
	})
	require.ErrorIs(t, err, ErrMissingSlot)
	_, err = client.picker().Next(balancer.RPCInfo{
		Ctx:    WithSlot(context.Background(), SlotCount),
		Method: fetchNextFullMethod,
	})
	require.ErrorIs(t, err, ErrInvalidSlot)
	require.NoError(t, b.Close())
}
