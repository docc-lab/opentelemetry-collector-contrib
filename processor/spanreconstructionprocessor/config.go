// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package spanreconstructionprocessor

import (
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
)

// Config defines configuration for the span reconstruction processor.
type Config struct {
	// MaxActiveSpans is the maximum number of active spans to keep in memory.
	// When this limit is reached, the oldest span is evicted.
	MaxActiveSpans int `mapstructure:"max_active_spans"`

	// SpanTTL is the time-to-live for active spans before automatic cleanup.
	SpanTTL time.Duration `mapstructure:"span_ttl"`

	// EnableMetrics enables internal metrics about processor operation.
	EnableMetrics bool `mapstructure:"enable_metrics"`
}

var _ component.Config = (*Config)(nil)

// Validate checks if the processor configuration is valid
func (cfg *Config) Validate() error {
	if cfg.MaxActiveSpans <= 0 {
		cfg.MaxActiveSpans = 1000 // default value
	}
	if cfg.SpanTTL <= 0 {
		cfg.SpanTTL = time.Hour // default value
	}
	return nil
}

// Unmarshal implements confmap.Unmarshaler interface.
func (cfg *Config) Unmarshal(component *confmap.Conf) error {
	if component == nil {
		return nil
	}
	return component.Unmarshal(cfg)
}
