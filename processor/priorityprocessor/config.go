// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package priorityprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/priorityprocessor"

import (
	"errors"
	"time"

	"go.opentelemetry.io/collector/component"
)

var (
	ErrCheckIntervalRequired = errors.New("check_interval must be greater than zero")
	ErrInvalidPercentage     = errors.New("memory limits must be between 1 and 100 percent")
	ErrInvalidThresholdOrder = errors.New("memory thresholds must satisfy ultrasoft_percentage < soft_percentage < hard_percentage")
)

var _ component.Config = (*Config)(nil)

// Config defines the three-zone memory-pressure model used by the
// priority processor. The processor is a pure passthrough — it never
// buffers, never holds queues, never spawns workers. Each incoming
// batch is either forwarded unchanged or refused (gRPC Unavailable)
// based on the current pressure zone and the batch's priority tag from
// the "bridges-priority" gRPC metadata header set by the SB SDK.
//
// Behavior at each zone (alloc = runtime.MemStats.Alloc):
//
//   - NoPressure (alloc < ultrasoft):
//     admit everything; return input traces unchanged
//
//   - Ultrasoft (ultrasoft ≤ alloc < soft):
//     LP refused (gRPC Unavailable); HP admitted unchanged
//
//   - Soft (soft ≤ alloc < hard):
//     refuse all (matches memory_limiter's soft behavior)
//
//   - Hard (alloc ≥ hard):
//     force runtime.GC(), re-read alloc. If still ≥ hard, refuse all
//     until the next check_interval tick re-evaluates
//
// The downstream pipeline is expected to be a standard memlim-style
// setup: priority → batch → otlp_exporter with a generous non-blocking
// sending_queue (queue_size ≫ num_consumers, block_on_overflow=false).
// This keeps the priority processor's feedback loop identical to
// memory_limiter's — alloc rises from spans transiting the pipeline,
// admission tightens, alloc falls — with the only addition being the
// LP-only refusal tier on top.
type Config struct {
	// UltrasoftPercentage is the alloc threshold where LP gets refused
	// but HP still admits. Default 35.
	UltrasoftPercentage uint32 `mapstructure:"ultrasoft_percentage"`

	// SoftPercentage is the alloc threshold where everything gets
	// refused (matches memory_limiter's soft behavior). Default 50.
	SoftPercentage uint32 `mapstructure:"soft_percentage"`

	// HardPercentage is the alloc threshold where the processor forces
	// a GC and re-checks; still-over allocs refuse everything (matches
	// memory_limiter's hard behavior). Default 70.
	HardPercentage uint32 `mapstructure:"hard_percentage"`

	// CheckInterval is how often the memstats probe runs to update the
	// pressure state. Default 100ms (matches memory_limiter).
	CheckInterval time.Duration `mapstructure:"check_interval"`
}

// Validate checks whether the configuration is internally consistent.
func (config *Config) Validate() error {
	if config.CheckInterval <= 0 {
		return ErrCheckIntervalRequired
	}
	for _, p := range []uint32{config.UltrasoftPercentage, config.SoftPercentage, config.HardPercentage} {
		if p < 1 || p > 100 {
			return ErrInvalidPercentage
		}
	}
	if !(config.UltrasoftPercentage < config.SoftPercentage && config.SoftPercentage < config.HardPercentage) {
		return ErrInvalidThresholdOrder
	}
	return nil
}
