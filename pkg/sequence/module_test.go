package sequence

import (
	"context"
	"errors"
	"testing"

	sequencev1 "github.com/codesjoy/skuld/gen/go/sequence/v1"
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
