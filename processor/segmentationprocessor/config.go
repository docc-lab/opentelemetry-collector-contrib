// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package segmentationprocessor

import (
	"time"

	"go.opentelemetry.io/collector/component"
)

// Config defines configuration for the segmentation processor.
type Config struct {
	// SegmentDuration is the duration for each time-based segment.
	SegmentDuration time.Duration `mapstructure:"segment_duration"`

	// EnableMetrics enables internal metrics about processor operation.
	EnableMetrics bool `mapstructure:"enable_metrics"`
}

var _ component.Config = (*Config)(nil)

// Validate checks if the processor configuration is valid
func (cfg *Config) Validate() error {
	if cfg.SegmentDuration <= 0 {
		cfg.SegmentDuration = time.Minute // default value
	}

	return nil
}
