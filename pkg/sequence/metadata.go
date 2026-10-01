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
	// LayoutVersionMetaKey carries the slot layout the caller hashed its keys
	// under. A caller whose layout differs sends slot numbers that mean different
	// keys, so the server refuses rather than answering for the wrong slot.
	LayoutVersionMetaKey = "layout_version"
	// SlotEpochMetaKey carries an ownership epoch, and is used in both
	// directions: a caller sends the epoch it believes the target slot is at, and
	// a refused request answers with the epoch the server holds. The name matches
	// appendix A.3 and A.4 so one key covers the request and the envelope.
	SlotEpochMetaKey = "slot_epoch"
	// OwnerHintMetaKey names the node the server believes owns the slot, so a
	// caller can converge on the right owner without a full route refresh.
	OwnerHintMetaKey = "owner_hint"
	// RetryAfterMetaKey carries how long a caller should wait before retrying, in
	// the Go duration format.
	RetryAfterMetaKey = "retry_after"
	// RetryableMetaKey carries what a caller should do with a failure. It is one
	// of the classifications below rather than a boolean, because the appendix A.4
	// table distinguishes a plain retry from a refresh-and-retry from a failure
	// that retrying cannot fix.
	RetryableMetaKey = "retryable"
	// NodeIDAttribute identifies a discovered endpoint in a route snapshot.
	NodeIDAttribute = "node_id"
	// BalancerType is the registered Yggdrasil sequence balancer type.
	BalancerType = "sequence"
	// InterceptorName is the registered Yggdrasil sequence client interceptor.
	InterceptorName = "sequence"
	// ModuleName is the stable Yggdrasil routing module name.
	ModuleName = "skuld.sequence.routing"
)

// Retry classifications carried in RetryableMetaKey. They are the four the
// appendix A.4 table names, in its order.
const (
	// RetryAfterBackoff means the caller should retry the same request after
	// waiting, because the owner is expected to recover on its own.
	RetryAfterBackoff = "retry"
	// RetryAfterRefresh means the caller must refresh its route before retrying,
	// because the failure describes the directory it routed by rather than the
	// request itself.
	RetryAfterRefresh = "refresh"
	// RetryAfterThrottle means the caller may retry but must slow down, because
	// the node is refusing work it could otherwise do.
	RetryAfterThrottle = "throttle"
	// RetryNever means retrying the request unchanged cannot help.
	RetryNever = "never"
)
