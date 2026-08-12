package biz

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRouteCacheStartsEmptyAndCoalescesEvents(t *testing.T) {
	cache := NewRouteCache()
	require.NotNil(t, cache.Route())
	assert.Zero(t, cache.Version())

	cache.UpdateRoute(&Route{Version: 1})
	cache.UpdateRoute(&Route{Version: 2})
	assert.Equal(t, int64(2), cache.Version())
	select {
	case event := <-cache.EventChan():
		assert.Equal(t, int64(2), event.Version)
	default:
		t.Fatal("expected a coalesced route event")
	}
}
