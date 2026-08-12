package sequence

import (
	"context"
	"errors"
	"strconv"

	"github.com/codesjoy/pkg/basic/xerror"
	"github.com/codesjoy/skuld/gen/go/reason"
	sequencev1 "github.com/codesjoy/skuld/gen/go/sequence/v1"
	"github.com/codesjoy/yggdrasil/v3/rpc/interceptor"
	"github.com/codesjoy/yggdrasil/v3/rpc/metadata"
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
		request, ok := req.(*sequencev1.FetchNextRequest)
		if method != fetchNextFullMethod || !ok {
			return invoker(ctx, method, req, reply)
		}
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

		version := router.Version()
		err := invokeFetchNext(ctx, method, request, reply, invoker, version)
		if err == nil || !isRefreshableRouteError(err) {
			return err
		}
		if err := router.refreshAfter(ctx, version); err != nil {
			return err
		}
		if message, ok := reply.(proto.Message); ok {
			proto.Reset(message)
		}
		return invokeFetchNext(ctx, method, request, reply, invoker, router.Version())
	}
}

func invokeFetchNext(
	ctx context.Context,
	method string,
	request *sequencev1.FetchNextRequest,
	reply any,
	invoker interceptor.UnaryInvoker,
	version int64,
) error {
	ctx = WithKey(ctx, request.GetKey())
	ctx = metadata.WithOutContext(
		ctx,
		metadata.Pairs(VersionMetaKey, strconv.FormatInt(version, 10)),
	)
	return invoker(ctx, method, request, reply)
}

func isRefreshableRouteError(err error) bool {
	return xerror.IsReason(err, reason.Reason_SEQUENCE_ROUTE_EXPIRED) ||
		xerror.IsReason(err, reason.Reason_SEQUENCE_SLOT_NOT_OWNER)
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
