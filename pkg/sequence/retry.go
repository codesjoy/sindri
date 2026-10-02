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
	"time"

	"github.com/codesjoy/pkg/basic/xerror"
	"github.com/codesjoy/sindri/gen/go/sequence/reason"
	"github.com/codesjoy/yggdrasil/v3/transport/runtime/client/balancer"
	"google.golang.org/genproto/googleapis/rpc/code"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

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
