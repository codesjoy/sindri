package task

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

type Config struct {
	BaseTickInterval time.Duration `mapstructure:"base_tick_interval"`
	HeartbeatTicks   int64         `mapstructure:"heartbeat_ticks"`
}

type TickType int

const (
	TickBase      TickType = 0
	TickHeartbeat TickType = 1
)

type tickSchedule struct {
	runAt    int64
	interval int64
}

type NodeLifecycle interface {
	Heartbeat()
	BaseTick()
	TickClock()
	CurrentTick() int64
	Pause()
}

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
