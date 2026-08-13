package metrics

import (
	"errors"
	"math"
	"runtime/debug"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestManagedMemory(t *testing.T) {
	assert.Equal(t, uint64(70), managedMemory(100, 30))
	assert.Equal(t, uint64(0), managedMemory(30, 30))
	assert.Equal(t, uint64(0), managedMemory(30, 40))
}

func TestRuntimeMemorySampler(t *testing.T) {
	managed, limit := NewRuntimeMemorySampler().MemoryUsage()
	assert.Positive(t, managed)
	assert.Positive(t, limit)
}

func TestValidateMemoryLimit(t *testing.T) {
	require.NoError(t, ValidateMemoryLimit(MinimumMemoryLimit))
	assert.Error(t, ValidateMemoryLimit(0))
	assert.Error(t, ValidateMemoryLimit(MinimumMemoryLimit-1))
	assert.Error(t, ValidateMemoryLimit(math.MaxInt64))
	assert.Error(t, ValidateMemoryLimit(math.MaxUint64))
}

func TestParseMemoryLimit(t *testing.T) {
	tests := []struct {
		value string
		want  uint64
	}{
		{value: "67108864", want: 64 << 20},
		{value: "64MiB", want: 64 << 20},
		{value: "1GiB", want: 1 << 30},
		{value: "2TiB", want: 2 << 40},
		{value: "1024KiB", want: 1 << 20},
		{value: " 64MiB ", want: 64 << 20},
	}
	for _, test := range tests {
		t.Run(test.value, func(t *testing.T) {
			got, err := ParseMemoryLimit(test.value)
			require.NoError(t, err)
			assert.Equal(t, test.want, got)
		})
	}
	for _, value := range []string{"", "auto", "64MB", "1.5GiB", "-1", "18446744073709551615TiB"} {
		t.Run("invalid_"+value, func(t *testing.T) {
			_, err := ParseMemoryLimit(value)
			assert.Error(t, err)
		})
	}
}

func TestDetectMemoryLimit(t *testing.T) {
	provider := func(value uint64, err error) memoryLimitProvider {
		return func() (uint64, error) { return value, err }
	}
	tests := []struct {
		name       string
		cgroup     memoryLimitProvider
		system     memoryLimitProvider
		want       uint64
		wantSource string
		wantError  bool
	}{
		{
			name:   "cgroup is smaller",
			cgroup: provider(1<<30, nil), system: provider(8<<30, nil),
			want: 1 << 30, wantSource: "cgroup",
		},
		{
			name:   "system is smaller",
			cgroup: provider(8<<30, nil), system: provider(4<<30, nil),
			want: 4 << 30, wantSource: "system",
		},
		{
			name:   "cgroup fallback",
			cgroup: provider(1<<30, nil), system: provider(0, errors.New("unavailable")),
			want: 1 << 30, wantSource: "cgroup",
		},
		{
			name:   "system fallback",
			cgroup: provider(0, errors.New("unavailable")), system: provider(2<<30, nil),
			want: 2 << 30, wantSource: "system",
		},
		{
			name:      "both unavailable",
			cgroup:    provider(0, errors.New("cgroup unavailable")),
			system:    provider(0, errors.New("system unavailable")),
			wantError: true,
		},
		{
			name:   "unlimited values",
			cgroup: provider(math.MaxUint64, nil), system: provider(math.MaxInt64, nil),
			wantError: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, source, err := detectMemoryLimit(test.cgroup, test.system)
			if test.wantError {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, test.want, got)
			assert.Equal(t, test.wantSource, source)
		})
	}
}

func TestConfigureMemoryLimit(t *testing.T) {
	provider := func(value uint64, err error) memoryLimitProvider {
		return func() (uint64, error) { return value, err }
	}
	tests := []struct {
		name       string
		configured string
		explicit   bool
		envValue   string
		envSet     bool
		ratio      float64
		cgroup     memoryLimitProvider
		system     memoryLimitProvider
		want       uint64
		wantSource string
		wantError  bool
	}{
		{
			name:       "explicit fixed overrides environment",
			configured: "256MiB", explicit: true, envValue: "512MiB", envSet: true,
			ratio: 0.8, cgroup: provider(1<<30, nil), system: provider(8<<30, nil),
			want: 256 << 20, wantSource: "configuration",
		},
		{
			name:       "explicit auto overrides environment",
			configured: "auto", explicit: true, envValue: "512MiB", envSet: true,
			ratio: 0.8, cgroup: provider(1<<30, nil), system: provider(8<<30, nil),
			want: 858993459, wantSource: "cgroup",
		},
		{
			name:       "environment compatibility",
			configured: "auto", explicit: false, envValue: "384MiB", envSet: true,
			ratio: 0.8, cgroup: provider(1<<30, nil), system: provider(8<<30, nil),
			want: 384 << 20, wantSource: "environment",
		},
		{
			name:       "default auto",
			configured: "auto", explicit: false, ratio: 0.75,
			cgroup: provider(2<<30, nil), system: provider(1<<30, nil),
			want: 768 << 20, wantSource: "system",
		},
		{
			name:       "detection failure",
			configured: "auto", explicit: true, ratio: 0.8,
			cgroup:    provider(0, errors.New("unavailable")),
			system:    provider(0, errors.New("unavailable")),
			wantError: true,
		},
		{
			name:       "below minimum",
			configured: "63MiB", explicit: true, ratio: 0.8,
			cgroup: provider(1<<30, nil), system: provider(1<<30, nil),
			wantError: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var applied int64
			result, err := configureMemoryLimit(
				test.configured,
				test.explicit,
				test.ratio,
				nil,
				memoryLimitDependencies{
					cgroup: test.cgroup,
					system: test.system,
					lookupEnv: func(string) (string, bool) {
						return test.envValue, test.envSet
					},
					setLimit: func(value int64) int64 {
						applied = value
						return math.MaxInt64
					},
				},
			)
			if test.wantError {
				assert.Error(t, err)
				assert.Zero(t, applied)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, test.want, result.LimitBytes)
			assert.Equal(t, test.wantSource, result.Source)
			assert.Equal(t, int64(test.want), applied)
		})
	}
}

func TestConfigureMemoryLimitAppliesRuntimeSetting(t *testing.T) {
	previous := debug.SetMemoryLimit(-1)
	t.Cleanup(func() { debug.SetMemoryLimit(previous) })

	result, err := ConfigureMemoryLimit("128MiB", true, 0.8, nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(128<<20), result.LimitBytes)
	assert.Equal(t, int64(128<<20), debug.SetMemoryLimit(-1))
}
