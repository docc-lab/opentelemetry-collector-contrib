// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package priorityprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/priorityprocessor"

import (
	"errors"
	"time"

	"go.opentelemetry.io/collector/component"
)

var (
	ErrInvalidConfigValue    = errors.New("invalid config value")
	ErrCheckIntervalRequired = errors.New("check_interval must be greater than zero")
	ErrInvalidPercentage     = errors.New("memory limits must be between 1 and 100 percent")
	ErrInvalidLimits         = errors.New("burst_memory_limit_percentage must be greater than memory_limit_percentage")
)

var _ component.Config = (*Config)(nil)

// Config defines the configuration for the processor.
type Config struct {
	// MemoryLimitPercentage is the percentage of total memory at which low-priority spans start being dropped
	MemoryLimitPercentage uint32 `mapstructure:"memory_limit_percentage"`

	// BurstMemoryLimitPercentage is the percentage of total memory at which all spans are dropped
	BurstMemoryLimitPercentage uint32 `mapstructure:"burst_memory_limit_percentage"`

	// CheckInterval is how often to check memory usage
	CheckInterval time.Duration `mapstructure:"check_interval"`
}

// Validate checks whether the input configuration has all of the required fields for the processor.
// An error is returned if there are any invalid inputs.
func (config *Config) Validate() error {
	if config.CheckInterval <= 0 {
		return ErrCheckIntervalRequired
	}

	if config.MemoryLimitPercentage < 1 || config.MemoryLimitPercentage > 100 {
		return ErrInvalidPercentage
	}

	if config.BurstMemoryLimitPercentage < 1 || config.BurstMemoryLimitPercentage > 100 {
		return ErrInvalidPercentage
	}

	if config.BurstMemoryLimitPercentage <= config.MemoryLimitPercentage {
		return ErrInvalidLimits
	}

	return nil
}
