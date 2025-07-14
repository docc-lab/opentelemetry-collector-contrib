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

	// Node-and-Edge Configuration
	// EnableNodeAndEdge enables the node-and-edge deconstruction functionality.
	EnableNodeAndEdge bool `mapstructure:"enable_node_and_edge"`

	// StrictParentChildValidation enables strict validation of parent-child relationships.
	StrictParentChildValidation bool `mapstructure:"strict_parent_child_validation"`

	// EmitImmediately controls whether completed spans are emitted immediately
	// when they finish or held for later processing.
	EmitImmediately bool `mapstructure:"emit_immediately"`
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
