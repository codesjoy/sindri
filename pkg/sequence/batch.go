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
	"sort"
	"sync"

	sequencev1 "github.com/codesjoy/sindri/gen/go/sequence/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// MaxBatchKeys is the maximum number of keys accepted by one batch request.
	MaxBatchKeys = 1000
	// MaxIDsPerKey is the maximum number of IDs one key may request at once.
	MaxIDsPerKey = 10000
	// MaxIDsPerRequest is the maximum number of IDs accepted by one batch request.
	MaxIDsPerRequest = 100000
	// MaxBatchConcurrency bounds concurrent owner RPCs from BatchClient.
	MaxBatchConcurrency = 32
)

var (
	// ErrBatchUnsupported indicates that the server does not implement FetchNextBatch.
	ErrBatchUnsupported = errors.New("sequence batch RPC is unavailable")
	// ErrBatchRouteChanged indicates that a grouped batch must be rebuilt from a new route.
	ErrBatchRouteChanged = errors.New("sequence batch route changed")
	// ErrCountUnsupported indicates that the server did not honor the requested count.
	ErrCountUnsupported = errors.New("sequence server did not honor the requested count")
)

// KeyRequest describes one key allocation in a batch.
type KeyRequest struct {
	Key   string
	Count uint32
}

// KeyAllocation is a contiguous allocation for one key.
type KeyAllocation struct {
	Key     string
	FirstID int64
	Count   uint32
}

// BatchClient groups unfinished keys by route owner within one retry budget.
type BatchClient struct {
	router *Router
	client sequencev1.SequenceGeneratorClient
}

// NewBatchClient constructs a route-aware batch client.
func NewBatchClient(
	router *Router,
	client sequencev1.SequenceGeneratorClient,
) (*BatchClient, error) {
	if router == nil {
		return nil, errors.New("sequence batch client: router is required")
	}
	if client == nil {
		return nil, errors.New("sequence batch client: generated client is required")
	}
	return &BatchClient{router: router, client: client}, nil
}

// FetchNext allocates every requested block or returns a whole-batch error.
// Results preserve request order. IDs consumed by a failed attempt may become gaps.
func (c *BatchClient) FetchNext(
	ctx context.Context,
	requests []KeyRequest,
) ([]KeyAllocation, error) {
	normalized, err := normalizeKeyRequests(requests)
	if err != nil {
		return nil, err
	}
	if c == nil || c.router == nil || c.client == nil {
		return nil, errors.New("sequence batch client is not initialized")
	}
	if ctx == nil {
		return nil, errors.New("sequence batch client: context is required")
	}

	allocations := make([]KeyAllocation, len(normalized))
	done := make([]bool, len(normalized))
	err = c.router.runWithRetry(ctx, func(attemptCtx context.Context) error {
		if c.router.Version() == 0 {
			if err := c.router.Refresh(attemptCtx); err != nil {
				return err
			}
		}
		pending := make([]KeyRequest, 0, len(normalized))
		indexes := make([]int, 0, len(normalized))
		for index, request := range normalized {
			if !done[index] {
				pending = append(pending, request)
				indexes = append(indexes, index)
			}
		}
		owners, _ := c.router.ownerSnapshot()
		groups, err := groupKeyRequests(pending, owners)
		if err != nil {
			return err
		}
		decoded, groupErrors, fatal := c.fetchGroups(attemptCtx, pending, groups)
		if fatal != nil {
			return normalizeBatchRPCError(fatal)
		}
		var last error
		decision := retryDecision{action: RetryAfterBackoff}
		for index, group := range groups {
			if groupErr := groupErrors[index]; groupErr != nil {
				last = groupErr
				d := classifyRetry(groupErr)
				decision.after = max(decision.after, d.after)
				if d.action == RetryAfterRefresh || decision.action != RetryAfterRefresh {
					decision.action = d.action
				}
				continue
			}
			for _, pendingIndex := range group.indexes {
				original := indexes[pendingIndex]
				allocations[original] = decoded[pendingIndex]
				done[original] = true
			}
		}
		if last != nil {
			return &batchRetryError{err: last, decision: decision}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return allocations, nil
}

func normalizeKeyRequests(requests []KeyRequest) ([]KeyRequest, error) {
	if len(requests) == 0 {
		return nil, errors.New("sequence batch client: requests must not be empty")
	}
	if len(requests) > MaxBatchKeys {
		return nil, fmt.Errorf(
			"sequence batch client: at most %d keys are allowed",
			MaxBatchKeys,
		)
	}

	normalized := make([]KeyRequest, len(requests))
	seen := make(map[string]struct{}, len(requests))
	var total int64
	for index, request := range requests {
		if request.Key == "" || len(request.Key) > 256 {
			return nil, errors.New("sequence batch client: key must contain 1..256 bytes")
		}
		if _, exists := seen[request.Key]; exists {
			return nil, fmt.Errorf("sequence batch client: duplicate key %q", request.Key)
		}
		seen[request.Key] = struct{}{}
		count := request.Count
		if count == 0 {
			count = 1
		}
		if count > MaxIDsPerKey {
			return nil, fmt.Errorf(
				"sequence batch client: count for %q must not exceed %d",
				request.Key,
				MaxIDsPerKey,
			)
		}
		total += int64(count)
		if total > MaxIDsPerRequest {
			return nil, fmt.Errorf(
				"sequence batch client: at most %d IDs are allowed per request",
				MaxIDsPerRequest,
			)
		}
		normalized[index] = KeyRequest{Key: request.Key, Count: count}
	}
	return normalized, nil
}

type ownerGroup struct {
	nodeID     string
	anchorSlot uint32
	indexes    []int
}

func groupKeyRequests(
	requests []KeyRequest,
	owners [SlotCount]string,
) ([]ownerGroup, error) {
	byNode := make(map[string]*ownerGroup)
	for index, request := range requests {
		slot := SlotForKey(request.Key)
		owner := owners[slot]
		if owner == "" {
			return nil, fmt.Errorf(
				"%w: slot %d has no owner",
				ErrBatchRouteChanged,
				slot,
			)
		}
		group := byNode[owner]
		if group == nil {
			group = &ownerGroup{nodeID: owner, anchorSlot: slot}
			byNode[owner] = group
		}
		group.indexes = append(group.indexes, index)
	}

	groups := make([]ownerGroup, 0, len(byNode))
	for _, group := range byNode {
		groups = append(groups, *group)
	}
	sort.Slice(groups, func(i, j int) bool { return groups[i].nodeID < groups[j].nodeID })
	return groups, nil
}

func (c *BatchClient) fetchGroups(
	ctx context.Context,
	requests []KeyRequest,
	groups []ownerGroup,
) ([]KeyAllocation, []error, error) {
	type groupResult struct {
		index    int
		response *sequencev1.FetchNextBatchResponse
		err      error
	}

	callCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	// The outer batch owns the retry budget and regrouping of unfinished keys.
	callCtx = context.WithValue(callCtx, singleAttemptKey{}, true)
	results := make(chan groupResult, len(groups))
	semaphore := make(chan struct{}, MaxBatchConcurrency)
	var wait sync.WaitGroup

	for index, group := range groups {
		wait.Add(1)
		go func() {
			defer wait.Done()
			select {
			case semaphore <- struct{}{}:
				defer func() { <-semaphore }()
			case <-callCtx.Done():
				results <- groupResult{index: index, err: callCtx.Err()}
				return
			}
			response, err := c.client.FetchNextBatch(
				callCtx,
				fetchNextBatchRequest(group, requests),
			)
			results <- groupResult{index: index, response: response, err: err}
		}()
	}

	allocations := make([]KeyAllocation, len(requests))
	groupErrors := make([]error, len(groups))
	var fatal error
	for range groups {
		result := <-results
		if result.err == nil {
			// Validate immediately so a malformed success cancels remaining RPCs.
			result.err = decodeGroupResponse(
				requests,
				groups[result.index],
				result.response,
				allocations,
			)
		}
		if result.err != nil {
			groupErrors[result.index] = result.err
			if fatal == nil && classifyRetry(result.err).action == RetryNever {
				fatal = result.err
				cancel()
			}
			continue
		}
	}
	wait.Wait()
	return allocations, groupErrors, fatal
}

func fetchNextBatchRequest(
	group ownerGroup,
	requests []KeyRequest,
) *sequencev1.FetchNextBatchRequest {
	protoRequests := make([]*sequencev1.FetchNextRequest, len(group.indexes))
	for index, original := range group.indexes {
		count := requests[original].Count
		protoRequests[index] = &sequencev1.FetchNextRequest{
			Key:   requests[original].Key,
			Count: &count,
		}
	}
	return &sequencev1.FetchNextBatchRequest{Requests: protoRequests}
}

func decodeGroupResponse(
	requests []KeyRequest,
	group ownerGroup,
	response *sequencev1.FetchNextBatchResponse,
	allocations []KeyAllocation,
) error {
	if response == nil || len(response.GetResults()) != len(group.indexes) {
		return errors.New("sequence batch client: server result count does not match request")
	}
	for resultIndex, original := range group.indexes {
		request := requests[original]
		result := response.GetResults()[resultIndex]
		if result == nil || result.GetKey() != request.Key {
			return errors.New("sequence batch client: server result key does not match request")
		}
		if result.GetCount() != request.Count || result.GetId() <= 0 {
			return ErrCountUnsupported
		}
		allocations[original] = KeyAllocation{
			Key:     request.Key,
			FirstID: result.GetId(),
			Count:   result.GetCount(),
		}
	}
	return nil
}

func normalizeBatchRPCError(err error) error {
	if status.Code(err) == codes.Unimplemented {
		return fmt.Errorf("%w: %v", ErrBatchUnsupported, err)
	}
	return err
}
