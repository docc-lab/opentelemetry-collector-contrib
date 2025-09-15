package agentcontactexporter

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
)

const (
	// The value of "type" key in configuration.
	typeStr = "agentcontact"
	// The stability level of the exporter.
	stability = component.StabilityLevelDevelopment
)

// NewFactory creates a factory for the agent contact exporter.
func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		component.MustNewType(typeStr),
		createDefaultConfig,
		exporter.WithTraces(createTracesExporter, stability),
	)
}

// createDefaultConfig creates the default configuration for the exporter.
func createDefaultConfig() component.Config {
	return &Config{
		DefaultPort:  55679,
		Timeout:      30 * time.Second,
		BatchSize:    100,
		BatchTimeout: 5 * time.Second,
	}
}

// createTracesExporter creates a traces exporter based on this config.
func createTracesExporter(
	ctx context.Context,
	set exporter.Settings,
	cfg component.Config,
) (exporter.Traces, error) {
	config := cfg.(*Config)
	return newAgentContactExporter(config, set)
}
