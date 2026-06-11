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
	ErrNumWorkersRequired    = errors.New("num_workers must be greater than zero")
	ErrEvictionRatioRequired = errors.New("eviction_ratio must be greater than zero")
	ErrInvalidPercentage     = errors.New("memory limits must be between 1 and 100 percent")
	ErrInvalidThresholdOrder = errors.New("memory thresholds must satisfy soft_percentage < hard_percentage")
)

var _ component.Config = (*Config)(nil)

// Config defines the two-zone memory pressure model the priority
// processor uses to shed load. Buffered architecture: incoming batches
// are placed into per-priority queues (HP/LP) determined by the
// "bridges-priority" gRPC metadata header set by the SB SDK on each
// UploadTraces call. A pool of NumWorkers goroutines drains the queues
// (HP-first) and forwards batches downstream. Backpressure from the
// downstream exporter (set queue_size=NumWorkers, block_on_overflow=true)
// propagates back to the workers, which causes the priority queues to
// grow, which raises the heap, which trips this processor's admission
// control.
//
// Behavior at each zone (alloc = runtime.MemStats.Alloc):
//
//   - NoPressure (alloc < soft):
//     admit all; HP→hpQ, LP→lpQ
//
//   - Soft (soft ≤ alloc < hard):
//     LP: refuse (gRPC Unavailable)
//     HP: if lpQ.len ≥ EvictionRatio, drop EvictionRatio LP batches from
//     the front of lpQ and enqueue the HP. Else refuse HP.
//     (Net effect: under soft pressure the total queue depth never
//     grows; HP can only enter by displacing LP.)
//
//   - Hard (alloc ≥ hard):
//     Force runtime.GC(), re-read alloc. If still ≥ hard, refuse all
//     until the next check_interval tick re-evaluates.
//
// Under any pressure state, the check loop forces a GC at every tick.
//
// In-flight memory is bounded by the in-process priority queues plus
// the downstream otlp exporter's tiny (queue_size=num_workers)
// sending_queue. With block_on_overflow=true on the exporter, the
// total bound is roughly: (priority queue depth in spans) × (per-span
// memory) + (num_workers × send_batch_size × per-span memory). The
// priority queue depth is bounded indirectly by the heap thresholds.
type Config struct {
	// SoftPercentage is the alloc threshold where LP gets refused and
	// HP becomes evict-only. Default 50.
	SoftPercentage uint32 `mapstructure:"soft_percentage"`

	// HardPercentage is the alloc threshold where all incoming batches
	// get refused (after a forced GC re-check). Default 70.
	HardPercentage uint32 `mapstructure:"hard_percentage"`

	// NumWorkers is the size of the worker pool that drains hpQ/lpQ
	// into the next consumer. Should equal the downstream OTLP
	// exporter's sending_queue.num_consumers AND its queue_size — that
	// way each in-flight gRPC call ties up exactly one worker, no
	// buffer slack, and downstream slowdowns propagate to admission.
	// Default 10.
	NumWorkers uint32 `mapstructure:"num_workers"`

	// EvictionRatio is the number of LP batches displaced from lpQ to
	// admit one HP batch when state==soft. Default 2.
	EvictionRatio uint32 `mapstructure:"eviction_ratio"`

	// CheckInterval is how often the memstats probe runs to update the
	// pressure state and (if above NoPressure) force a GC. Default 100ms.
	CheckInterval time.Duration `mapstructure:"check_interval"`
}

// Validate checks whether the configuration is internally consistent.
func (config *Config) Validate() error {
	if config.CheckInterval <= 0 {
		return ErrCheckIntervalRequired
	}
	if config.NumWorkers == 0 {
		return ErrNumWorkersRequired
	}
	if config.EvictionRatio == 0 {
		return ErrEvictionRatioRequired
	}
	for _, p := range []uint32{config.SoftPercentage, config.HardPercentage} {
		if p < 1 || p > 100 {
			return ErrInvalidPercentage
		}
	}
	if !(config.SoftPercentage < config.HardPercentage) {
		return ErrInvalidThresholdOrder
	}
	return nil
}
