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
	"fmt"
	"hash/crc32"
	rand "math/rand/v2"
	"sync"
	"time"

	"github.com/codesjoy/pkg/basic/xerror"
	"github.com/codesjoy/sindri/gen/go/sequence/reason"
	sequencev1 "github.com/codesjoy/sindri/gen/go/sequence/v1"
	"github.com/codesjoy/yggdrasil/v3/transport/runtime/client/balancer"
	"google.golang.org/genproto/googleapis/rpc/code"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// SlotCount is the fixed number of routing slots in sequence mode.
const SlotCount = 1 << 14

var (
	// ErrInvalidRoute indicates that a route snapshot violates the routing contract.
	ErrInvalidRoute = errors.New("invalid sequence route")
	// ErrRouteVersionRegression indicates that a route update moved backwards.
	ErrRouteVersionRegression = errors.New("sequence route version regressed")
	// ErrRouteVersionConflict indicates that one version identifies different snapshots.
	ErrRouteVersionConflict = errors.New("sequence route version conflicts with current snapshot")
	// ErrRouteUnavailable indicates that no usable route has been loaded.
	ErrRouteUnavailable = errors.New("sequence route is unavailable")
)

var ieeeCRC32Table = crc32.MakeTable(crc32.IEEE)

// RouteLoader fetches a route newer than knownVersion, or a not-modified response.
type RouteLoader func(
	ctx context.Context,
	knownVersion int64,
) (*sequencev1.GetRouteResponse, error)

type compiledRoute struct {
	snapshot *sequencev1.RouteSnapshot
	owners   [SlotCount]string
	// epochs is the per-slot ownership generation from the snapshot's segments.
	// Zero means the snapshot carried no ownership view, which is what a
	// pre-authority snapshot looks like; a caller then has no epoch to send and
	// the server answers without an epoch comparison.
	epochs        [SlotCount]uint64
	layoutVersion int64
}

type refreshCall struct {
	done chan struct{}
	err  error
}

// Router owns the immutable client route snapshot shared by interceptors and balancers.
type Router struct {
	retryPolicy RetryPolicy
	retryWait   func(context.Context, time.Duration) error
	retryRandom func() float64
	mu          sync.RWMutex

	loader  RouteLoader
	current *compiledRoute
	refresh *refreshCall

	nextListener uint64
	listeners    map[uint64]func()
}

// NewRouter constructs an empty router backed by loader.
func NewRouter(loader RouteLoader, options ...RouterOption) (*Router, error) {
	if loader == nil {
		return nil, errors.New("sequence router: route loader is required")
	}
	router := &Router{
		retryPolicy: DefaultRetryPolicy(),
		retryWait:   waitRetry,
		retryRandom: rand.Float64,
		loader:      loader,
		listeners:   make(map[uint64]func()),
	}
	for _, option := range options {
		if option == nil {
			return nil, errors.New("sequence router: option is required")
		}
		if err := option(router); err != nil {
			return nil, err
		}
	}
	return router, nil
}

// SlotForKey hashes the original UTF-8 key bytes into the fixed slot space.
func SlotForKey(key string) uint32 {
	crc := ^uint32(0)
	for i := 0; i < len(key); i++ {
		crc = ieeeCRC32Table[byte(crc)^key[i]] ^ crc>>8
	}
	return ^crc % SlotCount
}

// Version returns the current route version, or zero before the first route is loaded.
func (r *Router) Version() int64 {
	if r == nil {
		return 0
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.current == nil {
		return 0
	}
	return r.current.snapshot.GetVersion()
}

// Snapshot returns a deep copy of the current route snapshot.
func (r *Router) Snapshot() *sequencev1.RouteSnapshot {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.current == nil {
		return nil
	}
	return proto.Clone(r.current.snapshot).(*sequencev1.RouteSnapshot)
}

// LayoutVersion returns the slot layout the current snapshot was minted under,
// or zero when no snapshot has been loaded.
//
// A caller sends this with its requests so an owner that hashes keys under a
// different layout can refuse, rather than answer for slot numbers that mean
// something else on the caller's side.
func (r *Router) LayoutVersion() int64 {
	if r == nil {
		return 0
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.current == nil {
		return 0
	}
	return r.current.layoutVersion
}

// EpochOf returns the ownership epoch the current snapshot records for a slot.
//
// The second result is false when the snapshot carries no ownership view, which
// is when the server cannot be told an epoch at all; the caller then sends none
// and the server answers without comparing one.
func (r *Router) EpochOf(slot uint32) (uint64, bool) {
	if r == nil {
		return 0, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.current == nil || slot >= SlotCount {
		return 0, false
	}
	epoch := r.current.epochs[slot]
	if epoch == 0 {
		return 0, false
	}
	return epoch, true
}

// Update validates and publishes a route snapshot.
func (r *Router) Update(snapshot *sequencev1.RouteSnapshot) error {
	if r == nil {
		return errors.New("sequence router: router is required")
	}
	compiled, err := compileRoute(snapshot)
	if err != nil {
		return err
	}

	r.mu.Lock()
	if r.current != nil {
		currentVersion := r.current.snapshot.GetVersion()
		switch {
		case compiled.snapshot.GetVersion() < currentVersion:
			r.mu.Unlock()
			return fmt.Errorf(
				"%w: current=%d update=%d",
				ErrRouteVersionRegression,
				currentVersion,
				compiled.snapshot.GetVersion(),
			)
		case compiled.snapshot.GetVersion() == currentVersion:
			if proto.Equal(r.current.snapshot, compiled.snapshot) {
				r.mu.Unlock()
				return nil
			}
			r.mu.Unlock()
			return fmt.Errorf("%w: version=%d", ErrRouteVersionConflict, currentVersion)
		}
	}
	r.current = compiled
	listeners := make([]func(), 0, len(r.listeners))
	for _, listener := range r.listeners {
		listeners = append(listeners, listener)
	}
	r.mu.Unlock()

	for _, listener := range listeners {
		listener()
	}
	return nil
}

// Refresh loads and publishes a route relative to the current version.
// Concurrent callers share one in-flight load.
func (r *Router) Refresh(ctx context.Context) error {
	return r.refreshRoute(ctx, 0, false)
}

func (r *Router) refreshAfter(ctx context.Context, observedVersion int64) error {
	return r.refreshRoute(ctx, observedVersion, true)
}

func (r *Router) refreshRoute(
	ctx context.Context,
	observedVersion int64,
	skipIfNewer bool,
) error {
	if r == nil {
		return errors.New("sequence router: router is required")
	}
	if ctx == nil {
		return errors.New("sequence router: context is required")
	}

	r.mu.Lock()
	if skipIfNewer && r.current != nil &&
		r.current.snapshot.GetVersion() > observedVersion {
		r.mu.Unlock()
		return nil
	}
	if call := r.refresh; call != nil {
		done := call.done
		r.mu.Unlock()
		select {
		case <-done:
			return call.err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	call := &refreshCall{done: make(chan struct{})}
	r.refresh = call
	knownVersion := int64(0)
	if r.current != nil {
		knownVersion = r.current.snapshot.GetVersion()
	}
	r.mu.Unlock()

	response, err := r.loader(ctx, knownVersion)
	if err == nil {
		err = r.applyRefreshResponse(response)
	}

	r.mu.Lock()
	call.err = err
	r.refresh = nil
	close(call.done)
	r.mu.Unlock()
	return err
}

func (r *Router) applyRefreshResponse(response *sequencev1.GetRouteResponse) error {
	if response == nil {
		return fmt.Errorf("%w: loader returned a nil response", ErrInvalidRoute)
	}
	if response.GetNotModified() {
		if response.GetRoute() != nil {
			return fmt.Errorf("%w: not-modified response includes a route", ErrInvalidRoute)
		}
		if r.Version() == 0 {
			return ErrRouteUnavailable
		}
		return nil
	}
	if response.GetRoute() == nil {
		return fmt.Errorf("%w: loader response does not include a route", ErrInvalidRoute)
	}
	return r.Update(response.GetRoute())
}

func compileRoute(snapshot *sequencev1.RouteSnapshot) (*compiledRoute, error) {
	if snapshot == nil {
		return nil, fmt.Errorf("%w: snapshot is nil", ErrInvalidRoute)
	}
	if snapshot.GetVersion() <= 0 || snapshot.GetLayoutVersion() <= 0 {
		return nil, fmt.Errorf("%w: version and layout must be positive", ErrInvalidRoute)
	}

	compiled := &compiledRoute{
		snapshot: proto.Clone(snapshot).(*sequencev1.RouteSnapshot),
	}
	if err := compileSegments(compiled, compiled.snapshot.GetSegments()); err != nil {
		return nil, err
	}
	compiled.layoutVersion = snapshot.GetLayoutVersion()
	return compiled, nil
}

// compileSegments records the per-slot epoch view carried by the snapshot.
//
// The segments are the authority-backed view, so they must cover every slot
// exactly once and in order; a snapshot that does not is refused rather than
// partly applied, because a caller holding a half-filled epoch table would send
// an epoch for some slots and none for others without knowing which. Absent
// segments are accepted: that is a snapshot from before ownership was
// authoritative, and it simply carries no epochs.
func compileSegments(compiled *compiledRoute, segments []*sequencev1.RouteSegment) error {
	if len(segments) == 0 {
		return fmt.Errorf("%w: complete segments are required", ErrInvalidRoute)
	}
	next := uint32(0)
	for _, segment := range segments {
		if segment == nil {
			return fmt.Errorf("%w: route segment is nil", ErrInvalidRoute)
		}
		start, end := segment.GetStartSlot(), segment.GetEndSlot()
		if end >= SlotCount || end < start {
			return fmt.Errorf(
				"%w: route segment [%d,%d] is out of range",
				ErrInvalidRoute,
				start,
				end,
			)
		}
		if start != next {
			return fmt.Errorf(
				"%w: route segment does not continue the coverage at slot %d",
				ErrInvalidRoute,
				next,
			)
		}
		if segment.GetOwnerNodeId() == "" && segment.GetOwnerInstanceId() != "" {
			return fmt.Errorf(
				"%w: route segment [%d,%d] names an instance but no owner",
				ErrInvalidRoute,
				start,
				end,
			)
		}
		if segment.GetOwnerNodeId() != "" && segment.GetOwnerInstanceId() == "" {
			return fmt.Errorf(
				"%w: route segment [%d,%d] names owner %q without an instance",
				ErrInvalidRoute,
				start,
				end,
				segment.GetOwnerNodeId(),
			)
		}
		epoch := segment.GetSlotEpoch()
		if segment.GetOwnerNodeId() != "" && epoch == 0 {
			return fmt.Errorf("%w: owned segment requires positive epoch", ErrInvalidRoute)
		}
		for slot := start; slot <= end; slot++ {
			compiled.owners[slot] = segment.GetOwnerNodeId()
			compiled.epochs[slot] = epoch
		}
		next = end + 1
	}
	if next != SlotCount {
		return fmt.Errorf(
			"%w: route segments cover %d of %d slots",
			ErrInvalidRoute,
			next,
			SlotCount,
		)
	}
	return nil
}

func (r *Router) ownerTable() [SlotCount]string {
	owners, _ := r.ownerSnapshot()
	return owners
}

func (r *Router) ownerSnapshot() ([SlotCount]string, int64) {
	if r == nil {
		return [SlotCount]string{}, 0
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.current == nil {
		return [SlotCount]string{}, 0
	}
	return r.current.owners, r.current.snapshot.GetVersion()
}

func (r *Router) subscribe(listener func()) func() {
	if r == nil || listener == nil {
		return func() {}
	}
	r.mu.Lock()
	r.nextListener++
	id := r.nextListener
	r.listeners[id] = listener
	r.mu.Unlock()
	return func() {
		r.mu.Lock()
		delete(r.listeners, id)
		r.mu.Unlock()
	}
}

// RetryPolicy bounds an allocation, including route loading and backoff waits.
type RetryPolicy struct {
	MaxAttempts    int
	MaxElapsed     time.Duration
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
}

// DefaultRetryPolicy returns bounded recovery defaults, not an exactly-once policy.
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts: 8, MaxElapsed: 10 * time.Second,
		InitialBackoff: 100 * time.Millisecond, MaxBackoff: 2 * time.Second,
	}
}

// Validate rejects incomplete policies; callers can start from DefaultRetryPolicy.
func (p RetryPolicy) Validate() error {
	if p.MaxAttempts < 1 || p.MaxElapsed <= 0 || p.InitialBackoff <= 0 ||
		p.MaxBackoff < p.InitialBackoff {
		return errors.New(
			"sequence retry: attempts and durations must be positive and initial backoff must not exceed its maximum",
		)
	}
	return nil
}

// RouterOption configures immutable routing behavior at construction time.
type RouterOption func(*Router) error

// WithRetryPolicy replaces the default policy shared by interceptors and batches.
func WithRetryPolicy(policy RetryPolicy) RouterOption {
	return func(router *Router) error {
		if err := policy.Validate(); err != nil {
			return err
		}
		router.retryPolicy = policy
		return nil
	}
}

type singleAttemptKey struct{}

type terminalRetryError struct{ err error }

func (e *terminalRetryError) Error() string { return e.err.Error() }
func (e *terminalRetryError) Unwrap() error { return e.err }

type retryDecision struct {
	action string
	after  time.Duration
}

type batchRetryError struct {
	err      error
	decision retryDecision
}

func (e *batchRetryError) Error() string { return e.err.Error() }
func (e *batchRetryError) Unwrap() error { return e.err }

// retryResult keeps internal retry hints out of the public error envelope.
func retryResult(err error) error {
	switch failure := err.(type) {
	case *batchRetryError:
		return failure.err
	case *terminalRetryError:
		return failure.err
	default:
		return err
	}
}

func classifyRetry(err error) retryDecision {
	d := retryDecision{action: RetryNever}
	var terminal *terminalRetryError
	if errors.As(err, &terminal) {
		return d
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		xerror.IsCode(
			err,
			code.Code_CANCELLED,
		) || xerror.IsCode(err, code.Code_DEADLINE_EXCEEDED) ||
		status.Code(err) == codes.Canceled || status.Code(err) == codes.DeadlineExceeded {
		return d
	}
	var batchErr *batchRetryError
	if errors.As(err, &batchErr) {
		return batchErr.decision
	}
	_, _, md, _ := xerror.ReasonOf(err)
	if after, parseErr := time.ParseDuration(md[RetryAfterMetaKey]); parseErr == nil && after > 0 {
		d.after = after
	}
	switch md[RetryableMetaKey] {
	case RetryAfterBackoff, RetryAfterRefresh, RetryAfterThrottle, RetryNever:
		d.action = md[RetryableMetaKey]
		return d
	}
	switch {
	case errors.Is(err, ErrBatchRouteChanged), errors.Is(err, ErrRouteUnavailable),
		xerror.IsReason(err, reason.Reason_SEQUENCE_ROUTE_EXPIRED),
		xerror.IsReason(err, reason.Reason_SEQUENCE_SLOT_NOT_OWNER),
		xerror.IsReason(err, reason.Reason_SEQUENCE_EPOCH_STALE),
		xerror.IsReason(err, reason.Reason_SEQUENCE_LEASE_EXPIRED):
		d.action = RetryAfterRefresh
	case xerror.IsReason(err, reason.Reason_SEQUENCE_OWNER_RECOVERING),
		xerror.IsReason(err, reason.Reason_SEQUENCE_ALLOCATOR_PAUSED),
		xerror.IsReason(err, reason.Reason_SEQUENCE_ROUTE_UNAVAILABLE),
		xerror.IsReason(err, reason.Reason_SEQUENCE_COMMIT_UNCERTAIN),
		xerror.IsReason(err, reason.Reason_SEQUENCE_STORAGE_UNAVAILABLE):
		d.action = RetryAfterBackoff
	case xerror.IsReason(err, reason.Reason_SEQUENCE_CAPACITY_EXHAUSTED):
		d.action = RetryAfterThrottle
	case errors.Is(err, balancer.ErrNoAvailableInstance),
		xerror.IsCode(err, code.Code_UNAVAILABLE), status.Code(err) == codes.Unavailable:
		d.action = RetryAfterRefresh
	}
	return d
}

func waitRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (r *Router) retryDelay(attempt int, minimum time.Duration) time.Duration {
	delay := r.retryPolicy.InitialBackoff
	for index := 1; index < attempt && delay < r.retryPolicy.MaxBackoff; index++ {
		if delay > r.retryPolicy.MaxBackoff/2 {
			delay = r.retryPolicy.MaxBackoff
		} else {
			delay *= 2
		}
	}
	half := delay / 2
	delay = half + time.Duration(float64(delay-half)*r.retryRandom())
	return max(delay, minimum)
}

func (r *Router) runWithRetry(ctx context.Context, invoke func(context.Context) error) error {
	if ctx == nil {
		return errors.New("sequence retry: context is required")
	}
	if r == nil {
		return errors.New("sequence retry: router is required")
	}
	if single, _ := ctx.Value(singleAttemptKey{}).(bool); single {
		return invoke(ctx)
	}
	ctx, cancel := context.WithTimeout(ctx, r.retryPolicy.MaxElapsed)
	defer cancel()
	var last error
	var version int64
	for attempt := 0; attempt < r.retryPolicy.MaxAttempts; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if attempt > 0 {
			d := classifyRetry(last)
			if err := r.retryWait(ctx, r.retryDelay(attempt, d.after)); err != nil {
				return err
			}
			if d.action == RetryAfterRefresh {
				if err := r.refreshAfter(ctx, version); err != nil {
					last = err
					if classifyRetry(err).action == RetryNever {
						return err
					}
					continue
				}
			}
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		version = r.Version()
		last = invoke(ctx)
		if version == 0 {
			version = r.Version()
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if last == nil || classifyRetry(last).action == RetryNever {
			return retryResult(last)
		}
	}
	return retryResult(last)
}
