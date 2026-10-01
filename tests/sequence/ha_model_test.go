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
	"math/rand/v2"
	"sort"
	"testing"
	"time"
)

// This file is section F.3 of the HA design: a discrete-event model of the
// allocation protocol that decides safety by walking the interleaving space
// instead of observing one execution.
//
// It exists because the container harness cannot reach the cases that decide the
// question. It cannot inject a forward jump of the storage clock, it cannot
// freeze a host, and it only runs the interleavings a real deployment happens to
// produce. This model is pure Go, so it runs in the default suite, and each scan
// walks a grid of the instants that matter rather than sampling one schedule.
//
// The scenario is the one the silent-takeover window exists for: an owner that
// has lost its lease -- because it is paused, because storage is unreachable, or
// because its renewal loop has not caught up -- while a route change moves its
// slot to another node, and both of them can still hand out from the same key.
//
// Everything reduces to one quantity. Write g for the age, at the instant the old
// owner opens its fences, of the grant those fences are compared against, and W
// for the quiet window a takeover must wait. The new owner may hand out an ID
// once the grant is older than W; the old owner may hand out an ID as long as its
// fence passes and its own exposure is within bounds. A regression therefore
// exists exactly when the old owner can pass a fence on a grant older than W
// minus everything that happens between the fence and the cursor advance. The
// mechanism's job is to refuse service once g reaches that point, and each fence
// below is judged by whether it does.
//
// Two consequences are worth stating because they are easy to get backwards.
//
// The renewal loop is not a safety input. It decides how young a *healthy* node
// can keep its grant, which is an availability property: a node that fails to
// renew stops serving rather than serving wrongly, provided its fences enforce a
// bound. The model takes g as given, which is what makes it complete over
// renewal strategies.
//
// A pause between the fence and the cursor matters only through that bound. It is
// why a fence that reads state describing only the present is not enough on its
// own, and it is why the pause self-check is not a substitute for a lease check:
// it bounds the exposure, not the grant the exposure rests on.
//
// What this model cannot show, and does not claim. First, it samples: the scans
// walk a grid of instants rather than ranging over all of them, so a band
// narrower than the step could hide a regression, and finding none is not a
// proof. What the grid does establish is that each term of a bound is needed --
// a counterexample is a counterexample however it was found -- and the
// resolution is set below the narrowest band the parameters here can produce.
//
// Second, drift is a constant offset here, so it cancels wherever both sides of
// a comparison come from the storage clock. It therefore cannot represent a rate
// difference between the two clocks, and the drift term in the local-lease bound
// covers exactly that. Third, the model has no external fencing, so it can only
// prove the lease protocol sound, never the deployment.

const (
	// haStep is the allocator's default block size. The model only needs it to
	// give the old owner a cached range that outlives the takeover.
	haStep = 100
	// haTakeoverTick is how often a refused claim is retried, matching the tick
	// the placement loop runs on.
	haTakeoverTick = 20 * time.Millisecond
	// haHandoutDelay is the claim, reserve and linearise work the new owner does
	// before it can hand out an ID. An inversion has to fit inside the old
	// owner's exposure minus this delay, so a smaller value is the conservative
	// choice.
	haHandoutDelay = time.Millisecond
	// haNever pins a bound that no mechanism in this file actually enforces.
	haNever = time.Duration(1) << 62
)

// haParams are the section 8.1 configuration parameters under test.
type haParams struct {
	quietWindow time.Duration // W, the silent-takeover window
	lease       time.Duration // L, the local lease
	renewEvery  time.Duration // r, the renewal cadence
	maxPause    time.Duration // P_max, the pause bound
	safety      time.Duration // epsilon, the extra margin
	drift       time.Duration // delta, the storage clock versus the node clock
	jump        time.Duration // J_max, a forward jump of the storage clock
	step        int64
}

// haMechanism is which fences are armed. Each field corresponds to one gate in
// the production code; the scans below flip them one at a time so the
// contribution of each is visible.
type haMechanism struct {
	// localLease is the lease every held slot is armed with: a deadline in the
	// node's monotonic clock, refreshed by each confirmed renewal, that replaces
	// any per-batch read of the authority.
	localLease bool
	// pauseSelfCheck arms the section 3.4 measurement that discards an allocation
	// whose in-memory linearisation outran maxPause.
	pauseSelfCheck bool
}

// haRow is one slot_ownership row: the authority the whole protocol fences on.
type haRow struct {
	instance string
	epoch    uint64
	// granted is the storage clock reading of the last grant or renewal. Every
	// takeover decision is made against it.
	granted time.Duration
	unowned bool
}

// haLinearisation is one cursor advance: the point at which an ID enters the
// order the contract is stated over.
type haLinearisation struct {
	key  string
	id   int64
	node string
	at   time.Duration
}

// haModel is the storage authority plus the clock the model runs on.
type haModel struct {
	params haParams
	rows   map[uint32]haRow
	high   map[string]int64
	jumpAt time.Duration
	order  []haLinearisation
}

func newHaModel(params haParams) *haModel {
	return &haModel{
		params: params,
		rows:   make(map[uint32]haRow),
		high:   make(map[string]int64),
		jumpAt: haNever,
	}
}

// now is the storage clock, the only clock that decides takeovers. drift puts it
// ahead of the node clocks by delta; jump adds the forward jump once it lands.
//
// Only the forward direction is modelled. A storage clock that lags moves every
// takeover decision later, which is the safe direction, so it cannot be the
// source of a counterexample.
func (m *haModel) now(at time.Duration) time.Duration {
	if at >= m.jumpAt {
		return at + m.params.drift + m.params.jump
	}
	return at + m.params.drift
}

// load is the plain read of section 3.1. It returns the row as of the instant it
// runs, which is why the caller must run it at the instant the batch reads its
// fence and not at the instant the batch finishes. A slot with no row is
// unowned, which is how the table is seeded.
func (m *haModel) load(slot uint32) haRow {
	row, ok := m.rows[slot]
	if !ok {
		return haRow{unowned: true}
	}
	return row
}

// claim grants the slot when it is unowned, or once the quiet window has
// elapsed in storage time since the last grant (section 6.4). A refusal changes
// nothing, so the caller may retry it.
func (m *haModel) claim(slot uint32, instance string, at time.Duration) (haRow, bool) {
	row := m.load(slot)
	if !row.unowned && m.now(at) <= row.granted+m.params.quietWindow {
		return row, false
	}
	row = haRow{instance: instance, epoch: row.epoch + 1, granted: m.now(at)}
	m.rows[slot] = row
	return row, true
}

// reserve advances the persisted high watermark for a key, provided the
// authority still holds the slot it presents. It is all-or-nothing, exactly as
// the two-dialect transaction is, and it is the one operation whose serialisation
// the storage itself guarantees.
//
// It takes no instant because the model calls it only at the moment the
// transaction would commit, and the storage holds the ownership row for the whole
// of it. A takeover that lands while a reservation is in flight therefore cannot
// be modelled here and does not need to be: it would have to wait for the row.
func (m *haModel) reserve(
	instance string,
	slot uint32,
	epoch uint64,
	key string,
) (int64, int64, bool) {
	row := m.load(slot)
	if row.unowned || row.instance != instance || row.epoch != epoch {
		return 0, 0, false
	}
	start := m.high[key] + 1
	m.high[key] += m.params.step
	return start, m.high[key], true
}

// record enters an ID into the order the contract is stated over. Only IDs a
// client can observe are entered: the cursor of a discarded allocation has
// already moved, but nobody receives the ID, so it is a hole and not an
// ordering event.
func (m *haModel) record(key string, id int64, node string, at time.Duration) {
	m.order = append(m.order, haLinearisation{key: key, id: id, node: node, at: at})
}

// regression reports the first pair of linearisations for one key whose IDs do
// not increase in linearisation order. That order is the one the cursor advance
// defines, so a pair here is a section F.1 violation of the stated contract and
// not a property of one client's view: a downstream table keyed by ID would see
// the same inversion.
func (m *haModel) regression() (haLinearisation, haLinearisation, bool) {
	last := make(map[string]haLinearisation)
	for _, item := range m.order {
		if previous, ok := last[item.key]; ok && item.id <= previous.id {
			return previous, item, true
		}
		last[item.key] = item
	}
	return haLinearisation{}, haLinearisation{}, false
}

// haSlotGate is the local gate for one slot.
type haSlotGate struct {
	epoch uint64
	// deadline is the local lease deadline in the node's monotonic clock, armed
	// when the slot was claimed and re-armed by each confirmed renewal. A gate
	// built without the lease has no deadline, and then the node has no fence that
	// bounds how old the grant it serves on may be.
	deadline time.Duration
	draining bool
}

// haCachedBlock is the cached block a node hands IDs out of without touching storage.
type haCachedBlock struct {
	next int64
	end  int64
}

// haNode is one instance. Its clock is the model's timeline, so a pause is a gap
// between two of its actions.
type haNode struct {
	id       string
	instance string
	mech     haMechanism
	params   haParams
	slots    map[uint32]*haSlotGate
	keys     map[string]*haCachedBlock

	refused   int
	expired   int
	discarded int
	fenced    int
}

func newHaNode(id string, mech haMechanism, params haParams) *haNode {
	return &haNode{
		id:       id,
		instance: id + "-process",
		mech:     mech,
		params:   params,
		slots:    make(map[uint32]*haSlotGate),
		keys:     make(map[string]*haCachedBlock),
	}
}

// takeRoute claims a slot a new route grants this node and records the epoch its
// gate fences on: the release CASes on it, and the local deadline is armed from
// the same instant so a slot this node can no longer serve stops serving before
// the storage would reassign it.
func (n *haNode) takeRoute(m *haModel, slot uint32, at time.Duration) bool {
	row, granted := m.claim(slot, n.instance, at)
	if !granted {
		return false
	}
	held := &haSlotGate{epoch: row.epoch}
	if n.mech.localLease {
		held.deadline = at + n.params.lease - n.params.safety
	}
	n.slots[slot] = held
	return true
}

// warm gives the node a cached block so it can serve the key without a
// reservation, which is the case the lease has to fence: from here on the node
// hands IDs out of memory and touches nothing that could tell it the slot moved.
func (n *haNode) warm(m *haModel, slot uint32, key string) {
	held := n.slots[slot]
	start, end, ok := m.reserve(n.instance, slot, held.epoch, key)
	if !ok {
		panic("model: a node that just claimed its slot must be able to reserve")
	}
	n.keys[key] = &haCachedBlock{next: start, end: end}
}

// openFences runs the part of a batch that happens before the in-memory
// linearisation, and reports whether the node may continue.
//
// The only fence left here is the local lease, and it constrains an interval
// rather than an instant: a deadline checked at the top of the batch still has to
// be in the future when the cursor moves. That is why it is armed from the grant
// as a monotonic deadline rather than compared against the storage clock, and why
// the pause that follows it is bounded separately.
func (n *haNode) openFences(slot uint32, at time.Duration) bool {
	held := n.slots[slot]
	if held == nil || held.draining {
		n.refused++
		return false
	}
	if n.mech.localLease {
		if held.deadline == 0 || at >= held.deadline {
			n.expired++
			return false
		}
	}
	return true
}

// linearise advances the cursor and applies the post-linearisation check.
//
// started is the reading taken when the batch opened its fences. It is only
// meaningful where the pause self-check is armed, which is the same condition
// the production code takes the reading under.
func (n *haNode) linearise(
	m *haModel,
	slot uint32,
	key string,
	started time.Duration,
	at time.Duration,
) (int64, bool) {
	held := n.slots[slot]
	if held == nil || held.draining {
		n.refused++
		return 0, false
	}
	state := n.keys[key]
	if state == nil || state.next > state.end {
		start, end, ok := m.reserve(n.instance, slot, held.epoch, key)
		if !ok {
			n.refused++
			return 0, false
		}
		state = &haCachedBlock{next: start, end: end}
		n.keys[key] = state
	}
	if n.mech.pauseSelfCheck && at-started > n.params.maxPause {
		// The cursor has already moved, so the ID is a hole rather than a
		// returned one, and the slot is fenced so the next request cannot repeat
		// the mistake. The ID is deliberately not recorded: nobody observes it.
		n.discarded++
		n.fenced++
		held.draining = true
		return 0, false
	}
	id := state.next
	state.next++
	m.record(key, id, n.id, at)
	return id, true
}

// haEvent is one action on the model's timeline.
type haEvent struct {
	at time.Duration
	// order breaks ties between events at the same instant. The old owner's
	// linearisation is given the later order, which is the choice that exposes a
	// regression rather than hiding one.
	order int
	what  func()
}

// haSchedule runs events in wall-clock order. Wall-clock order is what the
// safety argument is about, so the actions cannot simply run in the order they
// are written.
type haSchedule struct {
	events []haEvent
}

const (
	haOrderOpenFence = iota
	haOrderTakeRoute
	haOrderHandout
	haOrderLinearise
)

func (s *haSchedule) at(t time.Duration, order int, what func()) {
	s.events = append(s.events, haEvent{at: t, order: order, what: what})
}

func (s *haSchedule) run() {
	sort.SliceStable(s.events, func(i, j int) bool {
		if s.events[i].at != s.events[j].at {
			return s.events[i].at < s.events[j].at
		}
		return s.events[i].order < s.events[j].order
	})
	for _, event := range s.events {
		event.what()
	}
}

// haScenario is one execution of the two-node handoff. Its three instants are
// the coordinates the scans walk.
type haScenario struct {
	// grantAge is the age, at the instant the old owner opens its fences, of the
	// grant those fences are compared against. It is the whole question: every
	// fence below is judged by what it does as this grows.
	grantAge time.Duration
	// pause is the gap between the fence and the cursor advance. It stands for a
	// host freeze, a SIGSTOP, or simply a scheduling stall.
	pause time.Duration
	// jumpAt is when the storage clock jumps forward by J_max. haNever leaves it
	// out of the scenario entirely.
	jumpAt time.Duration
}

// haRun is one scenario's outcome.
type haRun struct {
	model   *haModel
	old     *haNode
	next    *haNode
	fenceAt time.Duration
	served  bool
	oldID   int64
	nextID  int64
}

const (
	haTestSlot = uint32(7)
	haTestKey  = "orders"
)

// run schedules the handoff and executes it.
//
// The origin is the instant the old owner's grant was last refreshed: a
// successful renewal, or the claim itself. Everything is measured from it, and
// the route change lands there too, which is the worst placement -- it makes the
// takeover admissible as early as the protocol allows.
func (s haScenario) run(params haParams, mech haMechanism) *haRun {
	m := newHaModel(params)
	m.jumpAt = s.jumpAt
	old := newHaNode("node-b", mech, params)
	next := newHaNode("node-a", mech, params)
	run := &haRun{model: m, old: old, next: next, fenceAt: s.grantAge}

	if !old.takeRoute(m, haTestSlot, 0) {
		panic("model: the first claim must be granted")
	}
	old.warm(m, haTestSlot, haTestKey)

	// The new owner retries its claim on the takeover tick until the quiet
	// window has elapsed in storage time.
	claimAt := time.Duration(0)
	for m.now(claimAt) <= m.load(haTestSlot).granted+params.quietWindow {
		claimAt += haTakeoverTick
		if claimAt > haClaimLimit(params) {
			panic("model: the takeover never became admissible")
		}
	}

	writeAt := s.grantAge + s.pause
	schedule := &haSchedule{}
	schedule.at(s.grantAge, haOrderOpenFence, func() {
		run.served = old.openFences(haTestSlot, s.grantAge)
	})
	schedule.at(claimAt, haOrderTakeRoute, func() {
		if !next.takeRoute(m, haTestSlot, claimAt) {
			panic("model: the claim was admissible but refused")
		}
	})
	schedule.at(claimAt+haHandoutDelay, haOrderHandout, func() {
		if !next.openFences(haTestSlot, claimAt+haHandoutDelay) {
			panic("model: a node that just claimed must pass its own fences")
		}
		run.nextID, _ = next.linearise(
			m, haTestSlot, haTestKey, claimAt, claimAt+haHandoutDelay,
		)
	})
	schedule.at(writeAt, haOrderLinearise, func() {
		if !run.served {
			return
		}
		run.oldID, _ = old.linearise(m, haTestSlot, haTestKey, s.grantAge, writeAt)
	})
	schedule.run()
	return run
}

func haClaimLimit(params haParams) time.Duration {
	return 4*params.quietWindow + params.lease + time.Second
}

// haHorizon is how far the scan has to walk the grant's age. It must reach the
// oldest grant the mechanism can still serve on, and it must contain the band
// where a regression is possible, which lies inside the quiet window.
func haHorizon(params haParams, mech haMechanism) time.Duration {
	horizon := 2*params.quietWindow + params.maxPause
	if mech.localLease {
		horizon = max(horizon, params.lease+params.maxPause)
	}
	return horizon
}

// haScan is the search over one mechanism. It walks the grant's age across the
// whole horizon, and across the extremes of the pause and of the clock jump.
type haScan struct {
	params haParams
	mech   haMechanism
}

func (s haScan) run() (*haRun, bool) {
	// The step is the resolution of the oracle. It samples rather than proves, so
	// a band narrower than the step could hide a regression; the bands this
	// protocol can produce are bounded below by the jump term minus the safety
	// margin, which the tests drive down to a few milliseconds on purpose. A
	// millisecond step reaches them, and the cost is a few seconds of scanning.
	step := time.Millisecond
	for grantAge := step; grantAge <= haHorizon(s.params, s.mech); grantAge += step {
		for _, pause := range []time.Duration{s.params.maxPause, 0, haClaimLimit(s.params)} {
			// Where the storage clock jumps decides whether it matters. A jump
			// that lands at or before the fence shifts the takeover and the
			// measured age together, so it cancels; one that lands after the fence
			// advances the takeover while the fence has already passed, so it does
			// not. All three placements are scanned for that reason.
			placements := []time.Duration{haNever, grantAge, grantAge + pause/2}
			for _, jumpAt := range placements {
				run := haScenario{grantAge: grantAge, pause: pause, jumpAt: jumpAt}.
					run(s.params, s.mech)
				if _, _, found := run.model.regression(); found {
					return run, true
				}
			}
		}
	}
	return nil, false
}

// haTestParams are the section 8.1 parameters the scans run with. They are the
// production defaults, except that the lease is shortened so a scan that walks a
// whole lease stays cheap.
func haTestParams(quietWindow time.Duration) haParams {
	return haParams{
		quietWindow: quietWindow,
		lease:       3 * time.Second,
		renewEvery:  time.Second,
		maxPause:    100 * time.Millisecond,
		safety:      50 * time.Millisecond,
		drift:       20 * time.Millisecond,
		jump:        30 * time.Millisecond,
		step:        haStep,
	}
}

// quietWindowFloor is the section 3.3 lower bound: the smallest quiet window on
// which the lease can be served on at all. The local deadline is a monotonic
// quantity compared against a storage quantity, so drift does not cancel here and
// the lease does enter.
func quietWindowFloor(params haParams) time.Duration {
	return params.lease + params.drift + params.jump + params.maxPause + params.safety
}

// TestHALinearisationModelRejectsAFenceThatIgnoresTheGrantAge is the finding
// this model was written to reach.
//
// A node with no lease has only a fence that describes the present instant: it
// knows whether it holds the slot, and nothing about how old the grant behind
// that hold is. A node whose grant has aged -- paused, partitioned, or simply
// lagging on renewals -- still reads itself as the holder, because a lapsed hold
// is only visible in the grant's age or in a claim that has already committed.
//
// So such a node admits a takeover at its own worst, and the takeover's first ID
// can then precede the old owner's cursor. The scan walks every alignment of
// that handoff, and it finds the inversion at every quiet window however large,
// with the pause self-check either off or on: the self-check bounds how long the
// exposure lasts, not the grant the exposure rests on.
func TestHALinearisationModelRejectsAFenceThatIgnoresTheGrantAge(t *testing.T) {
	for _, test := range []struct {
		name string
		mech haMechanism
	}{
		{name: "no fence", mech: haMechanism{}},
		{name: "pause check only", mech: haMechanism{pauseSelfCheck: true}},
	} {
		for _, quietWindow := range []time.Duration{
			150 * time.Millisecond,
			time.Second,
			5 * time.Second,
		} {
			t.Run(test.name+"/"+quietWindow.String(), func(t *testing.T) {
				params := haTestParams(quietWindow)
				run, found := (haScan{params: params, mech: test.mech}).run()
				if !found {
					t.Fatalf(
						"a fence that ignores the grant's age found no regression at "+
							"W=%s, so the scan never reached the interleaving it "+
							"exists to reach", quietWindow,
					)
				}
				previous, item, _ := run.model.regression()
				t.Logf(
					"W=%s: %s handed out %d at %s, then %s handed out %d at %s, "+
						"on a grant %s old",
					quietWindow, previous.node, previous.id, previous.at,
					item.node, item.id, item.at, run.fenceAt,
				)
			})
		}
	}
}

// TestHALinearisationModelKeepsAPauseBeyondTheBoundOutOfTheOrder pins the other
// half of the section 3.4 rule, and pins why it needs both fences.
//
// The pause here lands while the lease still permits service, so the lease is
// the only thing between the old owner and a cursor that has outrun the
// takeover, and it is not enough on its own: the deadline was checked before the
// pause and the cursor advances after it. With the pause self-check armed the
// allocation is discarded and the slot is fenced, so the sequence a caller can
// observe still increases and the cost is a hole. Without it the very same
// schedule inverts.
func TestHALinearisationModelKeepsAPauseBeyondTheBoundOutOfTheOrder(t *testing.T) {
	params := haTestParams(5 * time.Second)
	// A healthy node a moment after its last renewal: young enough that its lease
	// still permits service, so the pause is the only thing left to bound.
	grantAge := params.renewEvery / 2
	if grantAge >= params.lease-params.safety {
		t.Fatalf(
			"the scenario needs a grant the lease still accepts: %s against a "+
				"deadline of %s", grantAge, params.lease-params.safety,
		)
	}
	scenario := haScenario{grantAge: grantAge, pause: 30 * time.Second}

	t.Run("with the pause check", func(t *testing.T) {
		mech := haMechanism{localLease: true, pauseSelfCheck: true}
		run := scenario.run(params, mech)
		requireNoRegression(t, run)
		if run.old.discarded == 0 {
			t.Fatal("a pause of 30s against a 100ms bound must be discarded, not returned")
		}
		if run.old.fenced == 0 {
			t.Fatal("a discarded allocation must leave the slot fenced")
		}
	})

	t.Run("without the pause check", func(t *testing.T) {
		mech := haMechanism{localLease: true}
		run := scenario.run(params, mech)
		if _, _, found := run.model.regression(); !found {
			t.Fatal(
				"the same schedule found no regression without the pause check, " +
					"so this test is not measuring what it claims",
			)
		}
	})
}

// TestHALinearisationModelPinsTheQuietWindowBounds checks the shape against its
// own lower bound: at the bound it must find no regression anywhere in the
// scenario space, and below it it must find one. A bound that is too low
// therefore fails loudly instead of quietly admitting a schedule nobody tested.
func TestHALinearisationModelPinsTheQuietWindowBounds(t *testing.T) {
	mech := haMechanism{localLease: true, pauseSelfCheck: true}
	floor := quietWindowFloor(haTestParams(0))
	params := haTestParams(floor)
	if _, found := (haScan{params: params, mech: mech}).run(); found {
		t.Fatalf("the lease regressed at its own documented bound %s", floor)
	}
	t.Logf("no regression at W=%s, the documented bound", floor)
	params = haTestParams(floor - 2*params.maxPause)
	if _, found := (haScan{params: params, mech: mech}).run(); !found {
		t.Fatalf(
			"the lease found no regression at W=%s, well under the bound "+
				"it claims (%s), so the bound is not the one in force",
			params.quietWindow, floor,
		)
	}
}

// TestHALinearisationModelReportsWhichFenceBoundsTheWindow records, for each
// mechanism, where a regression first stops being reachable, so the contribution
// of each fence is visible in the log rather than only in the discussion that
// produced the change.
func TestHALinearisationModelReportsWhichFenceBoundsTheWindow(t *testing.T) {
	for _, test := range []struct {
		name string
		mech haMechanism
	}{
		{name: "pause check only", mech: haMechanism{pauseSelfCheck: true}},
		{name: "local lease", mech: haMechanism{localLease: true, pauseSelfCheck: true}},
	} {
		t.Run(test.name, func(t *testing.T) {
			params := haTestParams(5 * time.Second)
			run, found := (haScan{params: params, mech: test.mech}).run()
			if !found {
				t.Logf("W=%s: no regression anywhere in the scanned space", params.quietWindow)
				return
			}
			previous, item, _ := run.model.regression()
			t.Logf(
				"W=%s: regresses -- %s handed out %d after %s handed out %d, on a "+
					"grant %s old", params.quietWindow, item.node, item.id,
				previous.node, previous.id, run.fenceAt,
			)
		})
	}
}

func requireNoRegression(t *testing.T, run *haRun) {
	t.Helper()
	if previous, item, found := run.model.regression(); found {
		t.Fatalf(
			"%s handed out %d at %s, then %s handed out %d at %s",
			previous.node, previous.id, previous.at, item.node, item.id, item.at,
		)
	}
}

// TestHALinearisationModelHoldsUnderRandomSchedules is the randomized companion
// to the scans above (appendix F.3).
//
// The scans walk a grid of the instants that matter, which is what makes them
// find the counterexamples the design discussion is about; this walks schedules
// nobody chose, which is what makes it find the ones the grid's alignment hides.
// Both check the same prefix oracle, and a counterexample is a counterexample
// however it was reached.
//
// The parameters are drawn across the ranges the bounds talk about, including
// combinations no single deployment would configure, because a fence that is only
// safe at the settings someone happened to test is not a fence.
func TestHALinearisationModelHoldsUnderRandomSchedules(t *testing.T) {
	leased := haMechanism{localLease: true, pauseSelfCheck: true}
	bare := haMechanism{}

	const rounds = 2000
	var leasedRegressions, bareRegressions int
	for seed := int64(1); seed <= rounds; seed++ {
		// A seeded generator is the point: a counterexample has to be reproducible
		// from its seed, and nothing here defends against an adversary.
		//nolint:gosec // the seed is the artifact, not an entropy source
		random := rand.New(rand.NewPCG(uint64(seed), uint64(seed)))
		params := haTestParams(time.Duration(1+random.IntN(5000)) * time.Millisecond)
		params.maxPause = time.Duration(1+random.IntN(500)) * time.Millisecond
		params.jump = time.Duration(random.IntN(300)) * time.Millisecond
		params.safety = time.Duration(1+random.IntN(300)) * time.Millisecond
		params.renewEvery = time.Duration(1+random.IntN(3000)) * time.Millisecond
		scenario := haScenario{
			grantAge: time.Duration(random.Int64N(int64(2*params.quietWindow) + 1)),
			pause:    time.Duration(random.Int64N(int64(2*params.maxPause) + 1)),
		}
		if random.IntN(2) == 0 {
			scenario.jumpAt = scenario.grantAge
		} else {
			scenario.jumpAt = haNever
		}

		// The lease is only expected to hold at or above its own bound; below it
		// the node cannot serve at all, and the scans above are what pin where that
		// is. The bare mechanism runs at every draw, because it has no bound to be
		// under.
		if params.quietWindow >= quietWindowFloor(params) {
			run := scenario.run(params, leased)
			if _, _, found := run.model.regression(); found {
				leasedRegressions++
				if leasedRegressions == 1 {
					previous, item, _ := run.model.regression()
					t.Logf(
						"seed %d: the lease regressed at W=%s P_max=%s J=%s "+
							"epsilon=%s: %s handed out %d then %s handed out %d",
						seed, params.quietWindow, params.maxPause, params.jump,
						params.safety, previous.node, previous.id, item.node, item.id,
					)
				}
			}
		}

		if _, _, found := scenario.run(params, bare).model.regression(); found {
			bareRegressions++
		}
	}

	if leasedRegressions != 0 {
		t.Fatalf(
			"the lease regressed under %d of %d random schedules at or above its "+
				"bound", leasedRegressions, rounds,
		)
	}
	// The randomized half is also what keeps the counterexample honest: if the
	// bare mechanism stopped inverting, the scan above would be finding something
	// other than the missing lease.
	if bareRegressions == 0 {
		t.Fatalf(
			"a node with no lease was expected to invert under some of %d random "+
				"schedules", rounds,
		)
	}
}
