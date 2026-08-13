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

// Package task runs the sequence node background tick scheduler.
package task

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// Config contains ticker timing configuration.
type Config struct {
	BaseTickInterval time.Duration `mapstructure:"base_tick_interval"`
	HeartbeatTicks   int64         `mapstructure:"heartbeat_ticks"`
}

// TickType identifies a scheduled node task.
type TickType int

const (
	// TickBase schedules the base allocation tick.
	TickBase TickType = 0
	// TickHeartbeat schedules the node heartbeat tick.
	TickHeartbeat TickType = 1
)

type tickSchedule struct {
	runAt    int64
	interval int64
}

// NodeLifecycle is the node manager contract used by Ticker.
type NodeLifecycle interface {
	Heartbeat()
	BaseTick()
	TickClock()
	CurrentTick() int64
	Pause()
}

// Ticker drives node heartbeats and allocation ticks.
type Ticker struct {
	baseTickInterval time.Duration
	schedules        []tickSchedule
	nodeManager      NodeLifecycle

	ctx       context.Context
	cancel    context.CancelFunc
	stoppedCh chan struct{}
	startedCh chan struct{}
	serving   atomic.Bool
	stopOnce  sync.Once
}

// NewTicker constructs a ticker from timing configuration and node manager.
func NewTicker(cfg Config, nodeManager NodeLifecycle) *Ticker {
	ticker := &Ticker{
		baseTickInterval: cfg.BaseTickInterval,
		schedules:        make([]tickSchedule, 2),
		nodeManager:      nodeManager,
		stoppedCh:        make(chan struct{}),
		startedCh:        make(chan struct{}),
	}
	ticker.ctx, ticker.cancel = context.WithCancel(context.Background())
	ticker.schedules[TickBase].interval = 1
	ticker.schedules[TickHeartbeat].interval = cfg.HeartbeatTicks
	return ticker
}

// Serve runs the ticker until it is stopped.
func (t *Ticker) Serve() error {
	if !t.serving.CompareAndSwap(false, true) {
		return errors.New("sequence ticker: Serve may only be called once")
	}
	close(t.startedCh)
	defer close(t.stoppedCh)
	if t.ctx.Err() != nil {
		return nil
	}
	t.nodeManager.Heartbeat()
	for i := range t.schedules {
		t.schedule(TickType(i))
	}
	timer := time.NewTicker(t.baseTickInterval)
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			t.onTick()
		case <-t.ctx.Done():
			t.nodeManager.Pause()
			return nil
		}
	}
}

// Stop requests ticker shutdown and waits for Serve to exit.
func (t *Ticker) Stop(ctx context.Context) error {
	if ctx == nil {
		return errors.New("sequence ticker: stop context is required")
	}
	t.stopOnce.Do(func() {
		t.nodeManager.Pause()
		t.cancel()
	})
	select {
	case <-t.startedCh:
		select {
		case <-t.stoppedCh:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	default:
		return nil
	}
}

func (t *Ticker) isOnTick(tp TickType) bool {
	return t.schedules[tp].runAt == t.nodeManager.CurrentTick()
}

func (t *Ticker) schedule(tp TickType) {
	sched := &t.schedules[tp]
	if sched.interval <= 0 {
		sched.runAt = -1
		return
	}
	sched.runAt = t.nodeManager.CurrentTick() + sched.interval
}

func (t *Ticker) onBaseTick() {
	t.nodeManager.BaseTick()
	t.schedule(TickBase)
}

func (t *Ticker) onHeartbeatTick() {
	t.nodeManager.Heartbeat()
	t.schedule(TickHeartbeat)
}

func (t *Ticker) onTick() {
	t.nodeManager.TickClock()
	if t.isOnTick(TickBase) {
		t.onBaseTick()
	}
	if t.isOnTick(TickHeartbeat) {
		t.onHeartbeatTick()
	}
}
