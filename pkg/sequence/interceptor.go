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
	"strconv"

	"github.com/codesjoy/pkg/basic/xerror"
	"github.com/codesjoy/sindri/gen/go/sequence/reason"
	sequencev1 "github.com/codesjoy/sindri/gen/go/sequence/v1"
	"github.com/codesjoy/yggdrasil/v3/rpc/interceptor"
	"github.com/codesjoy/yggdrasil/v3/rpc/metadata"
	"google.golang.org/genproto/googleapis/rpc/code"
	"google.golang.org/protobuf/proto"
)

// ErrReservedRouteMetadata indicates that a caller set middleware-owned route metadata.
var ErrReservedRouteMetadata = errors.New(
	"routerVersion metadata is reserved by sequence middleware",
)

func newUnaryClientInterceptor(router *Router) interceptor.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req, reply any,
		invoker interceptor.UnaryInvoker,
	) error {
		switch request := req.(type) {
		case *sequencev1.FetchNextRequest:
			if method == fetchNextFullMethod {
				return interceptFetchNext(ctx, router, method, request, reply, invoker)
			}
		case *sequencev1.FetchNextBatchRequest:
			if method == fetchNextBatchFullMethod {
				return interceptFetchNextBatch(ctx, router, method, request, reply, invoker)
			}
		}
		return invoker(ctx, method, req, reply)
	}
}

func interceptFetchNext(
	ctx context.Context,
	router *Router,
	method string,
	request *sequencev1.FetchNextRequest,
	reply any,
	invoker interceptor.UnaryInvoker,
) error {
	if err := validateInterceptorRouter(ctx, router); err != nil {
		return err
	}
	slot := SlotForKey(request.GetKey())
	version := router.Version()
	err := invokeRouted(ctx, method, request, reply, invoker, slot, version)
	if err == nil {
		return validateFetchNextCount(request, reply)
	}
	if !isRefreshableRouteError(err) {
		return err
	}
	if err := router.refreshAfter(ctx, version); err != nil {
		return err
	}
	resetReply(reply)
	err = invokeRouted(
		ctx,
		method,
		request,
		reply,
		invoker,
		SlotForKey(request.GetKey()),
		router.Version(),
	)
	if err != nil {
		return err
	}
	return validateFetchNextCount(request, reply)
}

func interceptFetchNextBatch(
	ctx context.Context,
	router *Router,
	method string,
	request *sequencev1.FetchNextBatchRequest,
	reply any,
	invoker interceptor.UnaryInvoker,
) error {
	if err := validateInterceptorRouter(ctx, router); err != nil {
		return err
	}
	slot, err := batchAnchorSlot(request, router)
	if err != nil {
		return err
	}
	version := router.Version()
	err = invokeRouted(ctx, method, request, reply, invoker, slot, version)
	if err == nil {
		return validateFetchNextBatchCounts(request, reply)
	}
	if !isRefreshableRouteError(err) {
		return err
	}
	if err := router.refreshAfter(ctx, version); err != nil {
		return err
	}
	slot, err = batchAnchorSlot(request, router)
	if err != nil {
		return err
	}
	resetReply(reply)
	err = invokeRouted(ctx, method, request, reply, invoker, slot, router.Version())
	if err != nil {
		return err
	}
	return validateFetchNextBatchCounts(request, reply)
}

func validateInterceptorRouter(ctx context.Context, router *Router) error {
	if router == nil {
		return errors.New("sequence interceptor: router is required")
	}
	if outgoing, exists := metadata.FromOutContext(ctx); exists &&
		len(outgoing.Get(VersionMetaKey)) != 0 {
		return ErrReservedRouteMetadata
	}
	if router.Version() == 0 {
		if err := router.Refresh(ctx); err != nil {
			return err
		}
	}
	return nil
}

func batchAnchorSlot(
	request *sequencev1.FetchNextBatchRequest,
	router *Router,
) (uint32, error) {
	if request == nil || len(request.GetRequests()) == 0 {
		return 0, errors.New("sequence interceptor: batch must not be empty")
	}
	owners := router.ownerTable()
	first := request.GetRequests()[0]
	if first == nil {
		return 0, errors.New("sequence interceptor: batch request is nil")
	}
	anchor := SlotForKey(first.GetKey())
	owner := owners[anchor]
	if owner == "" {
		return 0, fmt.Errorf("%w: slot %d has no owner", ErrBatchRouteChanged, anchor)
	}
	for _, item := range request.GetRequests()[1:] {
		if item == nil {
			return 0, errors.New("sequence interceptor: batch request is nil")
		}
		slot := SlotForKey(item.GetKey())
		if owners[slot] != owner {
			return 0, fmt.Errorf(
				"%w: keys span owners %q and %q",
				ErrBatchRouteChanged,
				owner,
				owners[slot],
			)
		}
	}
	return anchor, nil
}

func resetReply(reply any) {
	if message, ok := reply.(proto.Message); ok {
		proto.Reset(message)
	}
}

func invokeRouted(
	ctx context.Context,
	method string,
	request any,
	reply any,
	invoker interceptor.UnaryInvoker,
	slot uint32,
	version int64,
) error {
	ctx = WithSlot(ctx, slot)
	ctx = metadata.WithOutContext(
		ctx,
		metadata.Pairs(VersionMetaKey, strconv.FormatInt(version, 10)),
	)
	return invoker(ctx, method, request, reply)
}

func validateFetchNextCount(
	request *sequencev1.FetchNextRequest,
	reply any,
) error {
	response, ok := reply.(*sequencev1.FetchNextResponse)
	if !ok || response == nil {
		return errors.New("sequence interceptor: invalid FetchNext response")
	}
	want := request.GetCount()
	if want == 0 {
		want = 1
	}
	if response.GetCount() != want &&
		(want != 1 || response.GetCount() != 0) {
		return ErrCountUnsupported
	}
	return nil
}

func validateFetchNextBatchCounts(
	request *sequencev1.FetchNextBatchRequest,
	reply any,
) error {
	response, ok := reply.(*sequencev1.FetchNextBatchResponse)
	if !ok || response == nil {
		return errors.New("sequence interceptor: invalid FetchNextBatch response")
	}
	if len(response.GetResults()) != len(request.GetRequests()) {
		return errors.New("sequence interceptor: batch response count does not match request")
	}
	for index, item := range request.GetRequests() {
		result := response.GetResults()[index]
		if item == nil || result == nil || result.GetKey() != item.GetKey() {
			return errors.New("sequence interceptor: batch response key does not match request")
		}
		want := item.GetCount()
		if want == 0 {
			want = 1
		}
		if result.GetCount() != want {
			return ErrCountUnsupported
		}
	}
	return nil
}

func isRefreshableRouteError(err error) bool {
	return xerror.IsReason(err, reason.Reason_SEQUENCE_ROUTE_EXPIRED) ||
		xerror.IsReason(err, reason.Reason_SEQUENCE_SLOT_NOT_OWNER) ||
		xerror.IsCode(err, code.Code_UNAVAILABLE)
}

// NewUnaryClientInterceptorProvider constructs the sequence routing interceptor provider.
func NewUnaryClientInterceptorProvider(router *Router) interceptor.UnaryClientInterceptorProvider {
	return interceptor.NewUnaryClientInterceptorProvider(
		InterceptorName,
		func(string) interceptor.UnaryClientInterceptor {
			return newUnaryClientInterceptor(router)
		},
	)
}
