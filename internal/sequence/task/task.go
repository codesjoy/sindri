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
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/codesjoy/sindri/internal/sequence/biz"
)

// NodeLifecycle is the set of independent loops the ticker drives.
type NodeLifecycle interface {
	Renew(context.Context)
	Heartbeat(context.Context)
	Refresh(context.Context)
	Handoffs(context.Context)
	Maintain(context.Context)
	Pause()
}

// Ticker owns independent single-flight loops so a drain or route read cannot delay renewal.
type Ticker struct {
	cfg       biz.DataPlaneConfig
	node      NodeLifecycle
	ctx       context.Context
	cancel    context.CancelFunc
	startedCh chan struct{}
	stoppedCh chan struct{}
	serving   atomic.Bool
	stopOnce  sync.Once
}

// NewTicker builds the task that drives the node's independent loops.
func NewTicker(cfg biz.DataPlaneConfig, node NodeLifecycle) *Ticker {
	ctx, cancel := context.WithCancel(context.Background())
	return &Ticker{
		cfg:       cfg,
		node:      node,
		ctx:       ctx,
		cancel:    cancel,
		startedCh: make(chan struct{}),
		stoppedCh: make(chan struct{}),
	}
}

// Serve starts every loop and blocks until they all stop or one interval is
// invalid.
func (t *Ticker) Serve() error {
	if !t.serving.CompareAndSwap(false, true) {
		return errors.New("sequence task may only serve once")
	}
	close(t.startedCh)
	defer close(t.stoppedCh)
	var workers sync.WaitGroup
	loops := []struct {
		period time.Duration
		run    func(context.Context)
	}{
		{t.cfg.HA.RenewInterval, t.node.Renew},
		{t.cfg.Node.HeartbeatInterval, t.node.Heartbeat},
		{t.cfg.Node.RouteRefreshInterval, t.node.Refresh},
		{t.cfg.Node.HandoffInterval, t.node.Handoffs},
		{t.cfg.Allocator.CleanupInterval, t.node.Maintain},
	}
	for _, loop := range loops {
		if loop.period <= 0 {
			t.cancel()
			workers.Wait()
			return errors.New("sequence task interval must be positive")
		}
		workers.Add(1)
		go func(period time.Duration, run func(context.Context)) {
			defer workers.Done()
			timer := time.NewTicker(period)
			defer timer.Stop()
			for t.ctx.Err() == nil {
				run(t.ctx)
				select {
				case <-t.ctx.Done():
					return
				case <-timer.C:
				}
			}
		}(loop.period, loop.run)
	}
	workers.Wait()
	return nil
}

// Stop pauses the node, cancels the loops and waits for them to finish.
func (t *Ticker) Stop(ctx context.Context) error {
	t.stopOnce.Do(func() { t.node.Pause(); t.cancel() })
	select {
	case <-t.startedCh:
		select {
		case <-t.stoppedCh:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	case <-ctx.Done():
		return ctx.Err()
	}
}

// PublisherReconciler is the publish loop this task drives.
type PublisherReconciler interface {
	// Run drives passes until ctx is cancelled.
	Run(ctx context.Context)
}

// PublisherTask drives the directory publisher for the lifetime of the process.
//
// The publisher runs inside the sequence process rather than beside it, so a
// data-plane node and a control replica differ only in which tasks they start.
// That is what removes the second deployable without moving any of the work: the
// loop still runs somewhere, and the coordinator lease still keeps it single.
type PublisherTask struct {
	publisher PublisherReconciler

	ctx       context.Context
	cancel    context.CancelFunc
	startedCh chan struct{}
	stoppedCh chan struct{}
	serving   atomic.Bool
	stopOnce  sync.Once
}

// NewPublisherTask constructs the publisher task.
func NewPublisherTask(publisher PublisherReconciler) *PublisherTask {
	task := &PublisherTask{
		publisher: publisher,
		startedCh: make(chan struct{}),
		stoppedCh: make(chan struct{}),
	}
	// The cancel is kept on the task and called by Stop, which is what gives the
	// publisher a context that outlives any single Serve call.
	//
	//nolint:gosec // Cancelled by Stop, not by the constructor.
	task.ctx, task.cancel = context.WithCancel(context.Background())
	return task
}

// Serve runs the publisher until the task is stopped.
func (t *PublisherTask) Serve() error {
	if !t.serving.CompareAndSwap(false, true) {
		return errors.New("publisher task: Serve may only be called once")
	}
	close(t.startedCh)
	defer close(t.stoppedCh)
	if t.ctx.Err() != nil {
		return nil
	}
	t.publisher.Run(t.ctx)
	return nil
}

// Stop requests shutdown and waits for Serve to exit.
func (t *PublisherTask) Stop(ctx context.Context) error {
	if ctx == nil {
		return errors.New("publisher task: stop context is required")
	}
	t.stopOnce.Do(t.cancel)
	select {
	case <-t.startedCh:
		select {
		case <-t.stoppedCh:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	case <-ctx.Done():
		return ctx.Err()
	}
}
