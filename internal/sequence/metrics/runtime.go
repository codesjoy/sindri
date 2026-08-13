package metrics

import (
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"runtime/debug"
	runtimemetrics "runtime/metrics"
	"strconv"
	"strings"

	"github.com/KimMachineGun/automemlimit/memlimit"
)

const (
	// MinimumMemoryLimit prevents configurations that would force near-continuous GC.
	MinimumMemoryLimit = 64 << 20
	goMemoryLimitEnv   = "GOMEMLIMIT"
)

type memoryLimitProvider func() (uint64, error)

type memoryLimitDependencies struct {
	cgroup    memoryLimitProvider
	system    memoryLimitProvider
	lookupEnv func(string) (string, bool)
	setLimit  func(int64) int64
}

// MemoryLimitResult describes the process memory limit applied at startup.
type MemoryLimitResult struct {
	Source     string
	BaseBytes  uint64
	LimitBytes uint64
	Ratio      float64
}

// RuntimeMemorySampler reads Go-managed memory and the configured Go memory limit.
type RuntimeMemorySampler struct{}

// NewRuntimeMemorySampler creates the process-wide runtime memory sampler.
func NewRuntimeMemorySampler() *RuntimeMemorySampler {
	return &RuntimeMemorySampler{}
}

// MemoryUsage returns runtime-managed bytes excluding released heap memory and GOMEMLIMIT.
func (*RuntimeMemorySampler) MemoryUsage() (managedBytes, limitBytes uint64) {
	samples := [3]runtimemetrics.Sample{
		{Name: "/memory/classes/total:bytes"},
		{Name: "/memory/classes/heap/released:bytes"},
		{Name: "/gc/gomemlimit:bytes"},
	}
	runtimemetrics.Read(samples[:])
	return managedMemory(
		samples[0].Value.Uint64(),
		samples[1].Value.Uint64(),
	), samples[2].Value.Uint64()
}

func managedMemory(total, released uint64) uint64 {
	if total <= released {
		return 0
	}
	return total - released
}

// ParseMemoryLimit parses the byte syntax supported by the Go runtime's GOMEMLIMIT.
func ParseMemoryLimit(value string) (uint64, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, errors.New("must not be empty")
	}

	multiplier := uint64(1)
	number := value
	for _, unit := range []struct {
		suffix string
		scale  uint64
	}{
		{suffix: "TiB", scale: 1 << 40},
		{suffix: "GiB", scale: 1 << 30},
		{suffix: "MiB", scale: 1 << 20},
		{suffix: "KiB", scale: 1 << 10},
		{suffix: "B", scale: 1},
	} {
		if strings.HasSuffix(value, unit.suffix) {
			number = strings.TrimSuffix(value, unit.suffix)
			multiplier = unit.scale
			break
		}
	}
	if number == "" || strings.Trim(number, "0123456789") != "" {
		return 0, fmt.Errorf("invalid byte value %q", value)
	}
	parsed, err := strconv.ParseUint(number, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse byte value %q: %w", value, err)
	}
	if parsed > math.MaxUint64/multiplier {
		return 0, fmt.Errorf("byte value %q overflows uint64", value)
	}
	return parsed * multiplier, nil
}

// ValidateMemoryLimit rejects unsafe or effectively unlimited Go memory budgets.
func ValidateMemoryLimit(limitBytes uint64) error {
	if limitBytes < MinimumMemoryLimit {
		return fmt.Errorf("must be at least %d bytes (64MiB)", MinimumMemoryLimit)
	}
	if limitBytes >= math.MaxInt64 {
		return errors.New("must be less than math.MaxInt64")
	}
	return nil
}

// ConfigureMemoryLimit resolves and applies the sequence process memory limit.
func ConfigureMemoryLimit(
	configuredValue string,
	explicit bool,
	autoRatio float64,
	logger *slog.Logger,
) (MemoryLimitResult, error) {
	return configureMemoryLimit(
		configuredValue,
		explicit,
		autoRatio,
		logger,
		memoryLimitDependencies{
			cgroup:    memlimit.FromCgroup,
			system:    memlimit.FromSystem,
			lookupEnv: os.LookupEnv,
			setLimit:  debug.SetMemoryLimit,
		},
	)
}

func configureMemoryLimit(
	configuredValue string,
	explicit bool,
	autoRatio float64,
	logger *slog.Logger,
	deps memoryLimitDependencies,
) (MemoryLimitResult, error) {
	value := strings.TrimSpace(configuredValue)
	source := "configuration"
	if !explicit {
		if environmentValue, ok := deps.lookupEnv(goMemoryLimitEnv); ok {
			value = strings.TrimSpace(environmentValue)
			source = "environment"
		} else {
			value = "auto"
			source = "auto"
		}
	}

	var result MemoryLimitResult
	if value == "auto" {
		if autoRatio <= 0 || autoRatio >= 1 {
			return result, errors.New("auto memory limit ratio must be within (0,1)")
		}
		base, detectedSource, err := detectMemoryLimit(deps.cgroup, deps.system)
		if err != nil {
			return result, fmt.Errorf(
				"detect automatic memory limit: %w; configure a fixed runtime.memory_limit",
				err,
			)
		}
		result = MemoryLimitResult{
			Source:     detectedSource,
			BaseBytes:  base,
			LimitBytes: uint64(float64(base) * autoRatio),
			Ratio:      autoRatio,
		}
	} else {
		limit, err := ParseMemoryLimit(value)
		if err != nil {
			return result, fmt.Errorf("parse %s memory limit: %w", source, err)
		}
		result = MemoryLimitResult{Source: source, BaseBytes: limit, LimitBytes: limit, Ratio: 1}
	}
	if err := ValidateMemoryLimit(result.LimitBytes); err != nil {
		return MemoryLimitResult{}, fmt.Errorf("validate %s memory limit: %w", source, err)
	}
	deps.setLimit(int64(result.LimitBytes))
	if logger != nil {
		logger.Info(
			"configured Go runtime memory limit",
			"source", result.Source,
			"base_bytes", result.BaseBytes,
			"ratio", result.Ratio,
			"limit_bytes", result.LimitBytes,
		)
	}
	return result, nil
}

func detectMemoryLimit(cgroup, system memoryLimitProvider) (uint64, string, error) {
	cgroupLimit, cgroupErr := cgroup()
	systemLimit, systemErr := system()
	cgroupOK := cgroupErr == nil && cgroupLimit > 0 && cgroupLimit < math.MaxInt64
	systemOK := systemErr == nil && systemLimit > 0 && systemLimit < math.MaxInt64
	if cgroupErr == nil && !cgroupOK {
		cgroupErr = fmt.Errorf("invalid or unlimited value %d", cgroupLimit)
	}
	if systemErr == nil && !systemOK {
		systemErr = fmt.Errorf("invalid or unlimited value %d", systemLimit)
	}

	switch {
	case cgroupOK && systemOK && cgroupLimit <= systemLimit:
		return cgroupLimit, "cgroup", nil
	case cgroupOK && systemOK:
		return systemLimit, "system", nil
	case cgroupOK:
		return cgroupLimit, "cgroup", nil
	case systemOK:
		return systemLimit, "system", nil
	default:
		return 0, "", fmt.Errorf("cgroup provider: %v; system provider: %v", cgroupErr, systemErr)
	}
}
