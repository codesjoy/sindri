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
