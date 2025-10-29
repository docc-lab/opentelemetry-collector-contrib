// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package statisticsaggregationprocessor

import (
	"time"

	"go.opentelemetry.io/collector/component"
)

// Config defines the configuration for the statistics aggregation processor
type Config struct {
	// ExportWindow defines the time window for exporting aggregated statistics
	ExportWindow time.Duration `mapstructure:"export_window"`
}

var _ component.Config = (*Config)(nil)

// Validate checks if the processor configuration is valid
func (cfg *Config) Validate() error {
	if cfg.ExportWindow <= 0 {
		cfg.ExportWindow = 60 * time.Second // Default to 60 seconds
	}
	return nil
}
