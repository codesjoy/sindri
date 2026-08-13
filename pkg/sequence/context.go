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

import "context"

type slotContextKey struct{}

// WithKey stores the routing slot for key in ctx.
func WithKey(ctx context.Context, key string) context.Context {
	return WithSlot(ctx, SlotForKey(key))
}

// WithSlot stores an already computed routing slot in ctx.
func WithSlot(ctx context.Context, slot uint32) context.Context {
	return context.WithValue(ctx, slotContextKey{}, slot)
}

// SlotFromContext returns the routing slot stored in ctx.
func SlotFromContext(ctx context.Context) (uint32, bool) {
	if ctx == nil {
		return 0, false
	}
	slot, ok := ctx.Value(slotContextKey{}).(uint32)
	return slot, ok
}
