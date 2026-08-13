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
