package sequence

import (
	"github.com/codesjoy/yggdrasil/v3/capabilities"
	"github.com/codesjoy/yggdrasil/v3/module"
)

// RoutingModule registers sequence routing as App-local Yggdrasil capabilities.
type RoutingModule struct {
	router *Router
}

// NewRoutingModule constructs the sequence routing module.
func NewRoutingModule(router *Router) *RoutingModule {
	return &RoutingModule{router: router}
}

// Name returns the stable module identifier.
func (*RoutingModule) Name() string { return ModuleName }

// Capabilities registers the sequence balancer and unary client interceptor.
func (m *RoutingModule) Capabilities() []module.Capability {
	return []module.Capability{
		capabilities.ProvideNamed(
			capabilities.BalancerProviderSpec,
			BalancerType,
			NewBalancerProvider(m.router),
		),
		capabilities.ProvideOrdered(
			capabilities.UnaryClientInterceptorSpec,
			InterceptorName,
			NewUnaryClientInterceptorProvider(m.router),
		),
	}
}
