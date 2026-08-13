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
	val, err := s.allocator.FetchNext(ctx, req.Key)
	if err == nil {
		return &sequencev1.FetchNextResponse{Id: val}, nil
	}

	if !xerror.IsReason(err, reason.Reason_SEQUENCE_SLOT_NOT_OWNER) {
		return nil, err
	}

	md, ok := metadata.FromInContext(ctx)
	if !ok {
		return nil, xerror.New(code.Code_INVALID_ARGUMENT, "not found metadata")
	}
	v := md.Get(sequence.VersionMetaKey)
	if len(v) == 0 {
		return nil, xerror.New(code.Code_INVALID_ARGUMENT, "version not found")
	}
	rv, err := strconv.ParseInt(v[0], 10, 64)
	if err != nil {
		return nil, xerror.New(code.Code_INVALID_ARGUMENT, "version not found")
	}

	if rv <= s.route.Version() {
		return nil, xerror.NewWithReason(reason.Reason_SEQUENCE_ROUTE_EXPIRED, "", nil)
	}

	if err = s.allocator.WaitForVersion(ctx, rv); err != nil {
		return nil, err
	}

	val, err = s.allocator.FetchNext(ctx, req.Key)
	if err != nil {
		return nil, err
	}
	return &sequencev1.FetchNextResponse{Id: val}, nil
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
		Version: route.Version,
		Nodes:   make([]*sequencev1.RouteNode, 0, len(route.Nodes)),
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
	return &sequencev1.GetRouteResponse{Route: out}, nil
}
