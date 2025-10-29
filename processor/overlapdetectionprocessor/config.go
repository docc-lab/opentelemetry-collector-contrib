// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package overlapdetectionprocessor

import (
	"fmt"
)

// Config defines configuration for the overlap detection processor.
type Config struct {
	// OverlapThresholdMs defines the minimum overlap duration in milliseconds
	// to consider two segments as overlapping
	OverlapThresholdMs float64 `mapstructure:"overlap_threshold_ms"`

	// EnableMetrics enables metrics collection for overlap detection
	EnableMetrics bool `mapstructure:"enable_metrics"`
}

// Validate checks if the configuration is valid
func (cfg *Config) Validate() error {
	if cfg.OverlapThresholdMs < 0 {
		return fmt.Errorf("overlap_threshold_ms must be non-negative, got %f", cfg.OverlapThresholdMs)
	}

	// Set default values if not provided
	if cfg.OverlapThresholdMs == 0 {
		cfg.OverlapThresholdMs = 1.0 // Default to 1ms threshold
	}

	return nil
}
