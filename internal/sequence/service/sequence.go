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

// Package service implements the sequence generator RPC service.
package service

import (
	"context"
	"fmt"
	"strconv"

	"github.com/codesjoy/pkg/basic/xerror"
	"github.com/codesjoy/sindri/gen/go/sequence/reason"
	sequencev1 "github.com/codesjoy/sindri/gen/go/sequence/v1"
	"github.com/codesjoy/sindri/internal/sequence/biz"
	"github.com/codesjoy/sindri/pkg/sequence"
	"github.com/codesjoy/yggdrasil/v3/rpc/metadata"
	"google.golang.org/genproto/googleapis/rpc/code"
)

// SequenceService implements the sequence generator RPC contract.
type SequenceService struct {
	sequencev1.UnimplementedSequenceGeneratorServer
	allocator *biz.Allocator
	route     *biz.RouteCache
}

// NewSequenceService constructs a sequence generator service.
func NewSequenceService(allocator *biz.Allocator, route *biz.RouteCache) *SequenceService {
	return &SequenceService{
		allocator: allocator,
		route:     route,
	}
}

// FetchNext allocates an ID and maps domain failures to stable protocol reasons.
func (s *SequenceService) FetchNext(
	ctx context.Context,
	req *sequencev1.FetchNextRequest,
) (*sequencev1.FetchNextResponse, error) {
	request, err := validateFetchNextRequest(req)
	if err != nil {
		return nil, err
	}
	slot := biz.SlotForKey(request.Key)
	if err = s.checkCallerView(ctx, slot); err != nil {
		return nil, err
	}
	allocation, err := s.allocator.FetchNextN(ctx, request.Key, request.Count)
	if err == nil {
		return fetchNextResponse(allocation), nil
	}

	if !xerror.IsReason(err, reason.Reason_SEQUENCE_SLOT_NOT_OWNER) {
		return nil, s.envelope(err, slot)
	}
	if err = s.waitForRouteVersion(ctx); err != nil {
		return nil, s.envelope(err, slot)
	}

	allocation, err = s.allocator.FetchNextN(ctx, request.Key, request.Count)
	if err != nil {
		return nil, s.envelope(err, slot)
	}
	return fetchNextResponse(allocation), nil
}

func fetchNextResponse(allocation biz.SequenceAllocation) *sequencev1.FetchNextResponse {
	return &sequencev1.FetchNextResponse{
		Id:        allocation.ID,
		Count:     allocation.Count,
		SlotEpoch: allocation.SlotEpoch,
	}
}

// FetchNextBatch allocates IDs for a route-homogeneous batch of keys.
func (s *SequenceService) FetchNextBatch(
	ctx context.Context,
	req *sequencev1.FetchNextBatchRequest,
) (*sequencev1.FetchNextBatchResponse, error) {
	requests, err := validateFetchNextBatchRequest(req)
	if err != nil {
		return nil, err
	}
	// The caller groups a batch by owner, so the first key's slot is the one the
	// whole batch was routed by and the one every failure describes (A.6).
	slot := biz.SlotForKey(requests[0].Key)
	if err = s.checkCallerView(ctx, slot); err != nil {
		return nil, err
	}
	allocations, err := s.allocator.FetchNextBatch(ctx, requests)
	if err == nil {
		return fetchNextBatchResponse(requests, allocations), nil
	}

	if !xerror.IsReason(err, reason.Reason_SEQUENCE_SLOT_NOT_OWNER) {
		return nil, s.envelope(err, slot)
	}
	if err = s.waitForRouteVersion(ctx); err != nil {
		return nil, s.envelope(err, slot)
	}

	allocations, err = s.allocator.FetchNextBatch(ctx, requests)
	if err != nil {
		return nil, s.envelope(err, slot)
	}
	return fetchNextBatchResponse(requests, allocations), nil
}

// checkCallerView compares the caller's own view of the slot with this node's,
// which is section A.3.
//
// The two directions are deliberately not symmetric. A caller that is behind is
// served: this node holds the slot, and the response carries the epoch that
// supersedes the caller's, which is the refresh it needs (A.5). A caller that is
// ahead knows about an ownership this node has not seen, so answering would mean
// serving from a directory the caller has already moved past; if this node did
// lose the slot then its own fences would refuse the request anyway, so the
// check costs nothing and turns a confusing refusal into a refresh hint.
//
// A missing or unparseable hint is ignored rather than refused. These keys are
// optional, an older caller sends neither, and a malformed one is a caller bug
// that would otherwise be answered permanently instead of being served by a node
// whose own fences still decide whether it may answer.
func (s *SequenceService) checkCallerView(ctx context.Context, slot uint32) error {
	layout, epoch, hasEpoch := callerView(ctx)
	if layout > 0 {
		if local := s.route.LayoutVersion(); local > 0 && layout != local {
			return s.envelope(xerror.NewWithReason(
				reason.Reason_SEQUENCE_ROUTE_EXPIRED,
				fmt.Sprintf(
					"request layout version %d does not match the layout %d in force",
					layout,
					local,
				),
				nil,
			), slot)
		}
	}
	if !hasEpoch {
		return nil
	}
	local, ok := s.route.EpochOf(slot)
	if !ok {
		// Without an epoch view of its own, this node cannot call the caller's
		// epoch wrong. Ownership is still enforced by its own fences.
		return nil
	}
	if epoch <= local {
		return nil
	}
	return s.envelope(xerror.NewWithReason(
		reason.Reason_SEQUENCE_EPOCH_STALE,
		fmt.Sprintf("request epoch %d is ahead of the epoch %d in force", epoch, local),
		nil,
	), slot)
}

// callerView reads the slot layout and epoch a caller believes are in force.
func callerView(ctx context.Context) (int64, uint64, bool) {
	md, ok := metadata.FromInContext(ctx)
	if !ok {
		return 0, 0, false
	}
	layout := parseMetadataInt(md.Get(sequence.LayoutVersionMetaKey))
	epoch := parseMetadataUint(md.Get(sequence.SlotEpochMetaKey))
	return layout, epoch, epoch != 0
}

// envelope attaches the section A.4 failure envelope to an allocation failure.
//
// The reason and the gRPC code already travel; what this adds is what the caller
// cannot derive: which node to try instead, which epoch is current, whether to
// refresh or back off, and how long to wait. Rebuilding the error preserves its
// message and reason, so a caller that only reads those is unaffected.
func (s *SequenceService) envelope(err error, slot uint32) error {
	if err == nil {
		return nil
	}
	name, _, _, ok := xerror.ReasonOf(err)
	if !ok {
		return err
	}
	value, ok := reason.Reason_value[name]
	if !ok {
		return err
	}
	reasonCode := reason.Reason(value)
	envelope := map[string]string{sequence.RetryableMetaKey: retryClass(reasonCode)}
	if owner := s.route.OwnerOf(slot); owner != "" {
		envelope[sequence.OwnerHintMetaKey] = owner
	}
	if epoch, ok := s.route.EpochOf(slot); ok {
		envelope[sequence.SlotEpochMetaKey] = strconv.FormatUint(epoch, 10)
	}
	if after := s.allocator.RetryAfter(reasonCode); after > 0 {
		envelope[sequence.RetryAfterMetaKey] = after.String()
	}
	return xerror.NewWithReason(reasonCode, err.Error(), envelope)
}

// retryClass maps a reason to the action the appendix A.4 table prescribes for
// it. The table is the source of these groups, not the gRPC code: two reasons
// that share a code can need different reactions from the caller.
func retryClass(r reason.Reason) string {
	switch r {
	case reason.Reason_SEQUENCE_SLOT_NOT_OWNER,
		reason.Reason_SEQUENCE_ROUTE_EXPIRED,
		reason.Reason_SEQUENCE_EPOCH_STALE,
		reason.Reason_SEQUENCE_LEASE_EXPIRED:
		return sequence.RetryAfterRefresh
	case reason.Reason_SEQUENCE_OWNER_RECOVERING,
		reason.Reason_SEQUENCE_ALLOCATOR_PAUSED,
		reason.Reason_SEQUENCE_ROUTE_UNAVAILABLE,
		reason.Reason_SEQUENCE_COMMIT_UNCERTAIN,
		reason.Reason_SEQUENCE_STORAGE_UNAVAILABLE:
		return sequence.RetryAfterBackoff
	case reason.Reason_SEQUENCE_CAPACITY_EXHAUSTED:
		return sequence.RetryAfterThrottle
	default:
		return sequence.RetryNever
	}
}

func parseMetadataInt(values []string) int64 {
	if len(values) == 0 {
		return 0
	}
	parsed, err := strconv.ParseInt(values[0], 10, 64)
	if err != nil {
		return 0
	}
	return parsed
}

func parseMetadataUint(values []string) uint64 {
	if len(values) == 0 {
		return 0
	}
	parsed, err := strconv.ParseUint(values[0], 10, 64)
	if err != nil {
		return 0
	}
	return parsed
}

func (s *SequenceService) waitForRouteVersion(ctx context.Context) error {
	md, ok := metadata.FromInContext(ctx)
	if !ok {
		return xerror.New(code.Code_INVALID_ARGUMENT, "not found metadata")
	}
	v := md.Get(sequence.VersionMetaKey)
	if len(v) == 0 {
		return xerror.New(code.Code_INVALID_ARGUMENT, "version not found")
	}
	rv, err := strconv.ParseInt(v[0], 10, 64)
	if err != nil {
		return xerror.New(code.Code_INVALID_ARGUMENT, "version not found")
	}

	if rv < s.route.Version() {
		return xerror.NewWithReason(reason.Reason_SEQUENCE_ROUTE_EXPIRED, "", nil)
	}

	// A caller at the current published version may still arrive before this
	// node's allocator has applied it. Treat that as a handoff barrier to wait
	// for, not as an expired route: only a version older than the route cache is
	// genuinely stale.
	if err = s.allocator.WaitForVersion(ctx, rv); err != nil {
		return err
	}
	return nil
}

func validateFetchNextRequest(
	req *sequencev1.FetchNextRequest,
) (biz.SequenceRequest, error) {
	if req == nil {
		return biz.SequenceRequest{}, invalidRequest("request is required")
	}
	if req.GetKey() == "" || len(req.GetKey()) > 256 {
		return biz.SequenceRequest{}, invalidRequest("key must contain 1..256 bytes")
	}
	count := req.GetCount()
	if count == 0 {
		count = 1
	}
	if count > biz.MaxIDsPerKey {
		return biz.SequenceRequest{}, invalidRequest("count is out of range")
	}
	return biz.SequenceRequest{Key: req.GetKey(), Count: count}, nil
}

func validateFetchNextBatchRequest(
	req *sequencev1.FetchNextBatchRequest,
) ([]biz.SequenceRequest, error) {
	if req == nil {
		return nil, invalidRequest("request is required")
	}
	if len(req.GetRequests()) == 0 || len(req.GetRequests()) > biz.MaxBatchKeys {
		return nil, invalidRequest("request key count is out of range")
	}

	requests := make([]biz.SequenceRequest, len(req.GetRequests()))
	seen := make(map[string]struct{}, len(requests))
	var total int64
	for index, item := range req.GetRequests() {
		request, err := validateFetchNextRequest(item)
		if err != nil {
			return nil, err
		}
		if _, exists := seen[request.Key]; exists {
			return nil, invalidRequest("duplicate key")
		}
		seen[request.Key] = struct{}{}
		total += int64(request.Count)
		if total > biz.MaxIDsPerRequest {
			return nil, invalidRequest("request ID count is out of range")
		}
		requests[index] = request
	}
	return requests, nil
}

func invalidRequest(message string) error {
	return xerror.New(code.Code_INVALID_ARGUMENT, message)
}

func fetchNextBatchResponse(
	requests []biz.SequenceRequest,
	allocations []biz.SequenceAllocation,
) *sequencev1.FetchNextBatchResponse {
	response := &sequencev1.FetchNextBatchResponse{
		Results: make([]*sequencev1.FetchNextBatchResult, len(allocations)),
	}
	for index, allocation := range allocations {
		response.Results[index] = &sequencev1.FetchNextBatchResult{
			Key:       requests[index].Key,
			Id:        allocation.ID,
			Count:     allocation.Count,
			SlotEpoch: allocation.SlotEpoch,
		}
	}
	return response
}

// GetRoute returns the active route or a not-modified response.
func (s *SequenceService) GetRoute(
	_ context.Context,
	req *sequencev1.GetRouteRequest,
) (*sequencev1.GetRouteResponse, error) {
	route := s.route.Route()
	if route.Version == 0 {
		return nil, xerror.NewWithReason(
			reason.Reason_SEQUENCE_ROUTE_UNAVAILABLE,
			"sequence route is unavailable",
			nil,
		)
	}
	if req.GetKnownVersion() >= route.Version {
		return &sequencev1.GetRouteResponse{NotModified: true}, nil
	}
	out := &sequencev1.RouteSnapshot{
		Version:       route.Version,
		LayoutVersion: route.LayoutVersion,
		Nodes:         make([]*sequencev1.RouteNode, 0, len(route.Nodes)),
		Segments:      make([]*sequencev1.RouteSegment, 0, len(route.Segments)),
	}
	for _, node := range route.Nodes {
		out.Nodes = append(
			out.Nodes,
			&sequencev1.RouteNode{
				NodeId: node.NodeID,
				Slots:  append([]uint32(nil), node.Slots...),
			},
		)
	}
	// Segments carry the per-slot epoch, so they are sent only when the snapshot
	// has them. A route published from storage ownership always does; a legacy
	// one does not, and a caller then has no epoch to check rather than a
	// misleading zero.
	if len(route.Segments) > 0 {
		for _, segment := range route.Segments {
			out.Segments = append(out.Segments, &sequencev1.RouteSegment{
				StartSlot:       segment.StartSlot,
				EndSlot:         segment.EndSlot,
				OwnerNodeId:     segment.OwnerNodeID,
				OwnerInstanceId: segment.OwnerInstanceID,
				SlotEpoch:       segment.Epoch,
			})
		}
	}
	return &sequencev1.GetRouteResponse{Route: out}, nil
}
