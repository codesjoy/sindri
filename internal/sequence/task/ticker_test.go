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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeNode struct {
	mu         sync.Mutex
	tick       int64
	heartbeats int
	baseTicks  int
	pauses     int
}

func (n *fakeNode) Heartbeat() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.heartbeats++
}

func (n *fakeNode) BaseTick() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.baseTicks++
}

func (n *fakeNode) TickClock() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.tick++
}

func (n *fakeNode) CurrentTick() int64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.tick
}

func (n *fakeNode) Pause() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.pauses++
}

func (n *fakeNode) snapshot() (int, int, int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.heartbeats, n.baseTicks, n.pauses
}

func TestTickerRunsImmediateHeartbeatAndStops(t *testing.T) {
	node := &fakeNode{}
	ticker := NewTicker(Config{BaseTickInterval: 5 * time.Millisecond, HeartbeatTicks: 1}, node)
	done := make(chan error, 1)
	go func() { done <- ticker.Serve() }()
	require.Eventually(t, func() bool {
		heartbeats, baseTicks, _ := node.snapshot()
		return heartbeats >= 2 && baseTicks >= 1
	}, time.Second, time.Millisecond)
	require.NoError(t, ticker.Stop(context.Background()))
	require.NoError(t, <-done)
	_, _, pauses := node.snapshot()
	assert.GreaterOrEqual(t, pauses, 1)
	require.NoError(t, ticker.Stop(context.Background()))
}

func TestTickerStopBeforeServeDoesNotBlock(t *testing.T) {
	node := &fakeNode{}
	ticker := NewTicker(Config{BaseTickInterval: time.Second, HeartbeatTicks: 1}, node)
	require.NoError(t, ticker.Stop(context.Background()))
	require.NoError(t, ticker.Serve())
	heartbeats, _, pauses := node.snapshot()
	assert.Zero(t, heartbeats)
	assert.Equal(t, 1, pauses)
}

func TestTickerRejectsSecondServe(t *testing.T) {
	ticker := NewTicker(Config{BaseTickInterval: time.Second, HeartbeatTicks: 1}, &fakeNode{})
	require.NoError(t, ticker.Stop(context.Background()))
	require.NoError(t, ticker.Serve())
	require.Error(t, ticker.Serve())
}
