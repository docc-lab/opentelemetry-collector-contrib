// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package segmentationprocessor

import (
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"
)

// Config defines configuration for the segmentation processor.
type Config struct {
	// SegmentDuration is the duration for each time-based segment.
	SegmentDuration time.Duration `mapstructure:"segment_duration"`

	// EnableMetrics enables internal metrics about processor operation.
	EnableMetrics bool `mapstructure:"enable_metrics"`

	// ChangeDetectionPercentile is the percentile threshold for change detection (0.0-1.0).
	ChangeDetectionPercentile float64 `mapstructure:"change_detection_percentile"`

	// MinSamples is the minimum number of samples required before detecting changes.
	MinSamples int64 `mapstructure:"min_samples"`
}

var _ component.Config = (*Config)(nil)

// Validate checks if the processor configuration is valid
func (cfg *Config) Validate() error {
	if cfg.SegmentDuration <= 0 {
		cfg.SegmentDuration = time.Minute // default value
	}

	// Set default percentile if not specified
	if cfg.ChangeDetectionPercentile == 0 {
		cfg.ChangeDetectionPercentile = 0.99 // Default to P99
	}

	// Set default min samples if not specified
	if cfg.MinSamples == 0 {
		cfg.MinSamples = 100 // Default to 100 samples
	}

	// Validate percentile is between 0 and 1
	if cfg.ChangeDetectionPercentile < 0 || cfg.ChangeDetectionPercentile > 1 {
		return fmt.Errorf("change_detection_percentile must be between 0 and 1, got %f", cfg.ChangeDetectionPercentile)
	}

	// Validate min samples is positive
	if cfg.MinSamples < 1 {
		return fmt.Errorf("min_samples must be at least 1, got %d", cfg.MinSamples)
	}

	return nil
}
