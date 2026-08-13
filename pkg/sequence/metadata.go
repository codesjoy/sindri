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

const (
	// VersionMetaKey carries the client route version on FetchNext requests.
	VersionMetaKey = "routerVersion"
	// NodeIDAttribute identifies a discovered endpoint in a route snapshot.
	NodeIDAttribute = "node_id"
	// BalancerType is the registered Yggdrasil sequence balancer type.
	BalancerType = "sequence"
	// InterceptorName is the registered Yggdrasil sequence client interceptor.
	InterceptorName = "sequence"
	// ModuleName is the stable Yggdrasil routing module name.
	ModuleName = "skuld.sequence.routing"
)
