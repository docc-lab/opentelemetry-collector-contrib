// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package spanlookupprocessor

import (
	"go.opentelemetry.io/collector/component"
)

// Config represents the configuration for the span lookup processor
type Config struct {
	// MaxLookups defines the maximum number of span lookups to perform per batch
	// Default is 1000
	MaxLookups int `mapstructure:"max_lookups"`

	// EnableMetrics enables metrics collection for the processor
	EnableMetrics bool `mapstructure:"enable_metrics"`
}

var _ component.Config = (*Config)(nil)

// Validate checks if the processor configuration is valid
func (cfg *Config) Validate() error {
	// No validation errors for now
	return nil
}
