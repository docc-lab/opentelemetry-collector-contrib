// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package lookuproutingprocessor

import (
	"go.opentelemetry.io/collector/component"
)

// Config represents the configuration for the lookup routing processor
type Config struct {
	// RoutingRules defines the rules for routing spans based on lookup results
	RoutingRules []RoutingRule `mapstructure:"routing_rules"`

	// DefaultRoute defines the default route when no rules match
	DefaultRoute string `mapstructure:"default_route"`
}

// RoutingRule defines a single routing rule
type RoutingRule struct {
	// Condition defines the condition for this rule
	Condition string `mapstructure:"condition"`

	// Route defines the target route for this rule
	Route string `mapstructure:"route"`
}

var _ component.Config = (*Config)(nil)

// Validate checks if the processor configuration is valid
func (cfg *Config) Validate() error {
	// No validation errors for now
	return nil
}
