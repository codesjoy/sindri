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
	"testing"

	sequencev1 "github.com/codesjoy/sindri/gen/go/sequence/v1"
	"github.com/codesjoy/yggdrasil/v3/capabilities"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRoutingModuleCapabilities(t *testing.T) {
	router, err := NewRouter(func(context.Context, int64) (*sequencev1.GetRouteResponse, error) {
		return nil, errors.New("unused")
	})
	require.NoError(t, err)
	module := NewRoutingModule(router)
	assert.Equal(t, ModuleName, module.Name())

	provided := module.Capabilities()
	require.Len(t, provided, 2)
	assert.Equal(t, capabilities.BalancerProviderSpec, provided[0].Spec)
	assert.Equal(t, BalancerType, provided[0].Name)
	assert.Equal(t, capabilities.UnaryClientInterceptorSpec, provided[1].Spec)
	assert.Equal(t, InterceptorName, provided[1].Name)
}
