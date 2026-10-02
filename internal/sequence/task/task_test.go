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

package task

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	testkit "github.com/codesjoy/sindri/internal/pkg/tests"
	"github.com/codesjoy/sindri/internal/sequence/biz"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeNode struct {
	renew, heartbeat, handoff, maintain atomic.Int64
	paused                              atomic.Bool
	blocked                             chan struct{}
}

func (n *fakeNode) Renew(context.Context)     { n.renew.Add(1) }
func (n *fakeNode) Heartbeat(context.Context) { n.heartbeat.Add(1) }
func (n *fakeNode) Refresh(ctx context.Context) {
	if n.blocked != nil {
		select {
		case <-n.blocked:
		case <-ctx.Done():
		}
	}
}
func (n *fakeNode) Handoffs(context.Context) { n.handoff.Add(1) }
func (n *fakeNode) Maintain(context.Context) { n.maintain.Add(1) }
func (n *fakeNode) Pause()                   { n.paused.Store(true) }
func TestIndependentLoopsDoNotBlockRenewalAndStopCancelsIO(t *testing.T) {
	var cfg biz.DataPlaneConfig
	require.NoError(t, testkit.DecodeDefaults(&cfg))
	cfg.HA.RenewInterval = 5 * time.Millisecond
	cfg.Node.HeartbeatInterval = 5 * time.Millisecond
	cfg.Node.RouteRefreshInterval = 5 * time.Millisecond
	cfg.Node.HandoffInterval = 5 * time.Millisecond
	cfg.Allocator.CleanupInterval = 5 * time.Millisecond
	node := &fakeNode{blocked: make(chan struct{})}
	ticker := NewTicker(cfg, node)
	done := make(chan error, 1)
	go func() { done <- ticker.Serve() }()
	require.Eventually(t, func() bool {
		return node.renew.Load() > 3 && node.heartbeat.Load() > 3 && node.handoff.Load() > 3 &&
			node.maintain.Load() > 3
	}, time.Second, time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, ticker.Stop(ctx))
	require.NoError(t, <-done)
	assert.True(t, node.paused.Load())
	require.Error(t, ticker.Serve())
	require.NoError(t, ticker.Stop(ctx))
}

func TestTickerInvalidIntervalsAndStopBeforeServe(t *testing.T) {
	ticker := NewTicker(biz.DataPlaneConfig{}, &fakeNode{})
	require.Error(t, ticker.Serve())
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, ticker.Stop(ctx))
	ticker = NewTicker(biz.DataPlaneConfig{}, &fakeNode{})
	ctx2, cancel2 := context.WithCancel(context.Background())
	cancel2()
	require.Error(t, ticker.Stop(ctx2))
	require.Error(t, ticker.Serve())
}
