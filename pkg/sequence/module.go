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
