package agentcontactreceiver

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configgrpc"
	"go.opentelemetry.io/collector/config/confignet"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
)

const (
	// The value of "type" key in configuration.
	typeStr = "agentcontact"
	// The stability level of the receiver.
	stability = component.StabilityLevelDevelopment
	// Default gRPC endpoint
	grpcEndpoint = "localhost:55679"
)

// NewFactory creates a factory for the agent contact receiver.
func NewFactory() receiver.Factory {
	return receiver.NewFactory(
		component.MustNewType(typeStr),
		createDefaultConfig,
		receiver.WithTraces(createTracesReceiver, stability),
	)
}

// createDefaultConfig creates the default configuration for the receiver.
func createDefaultConfig() component.Config {
	return &Config{
		ServerConfig: configgrpc.ServerConfig{
			NetAddr: confignet.AddrConfig{
				Endpoint:  grpcEndpoint,
				Transport: confignet.TransportTypeTCP,
			},
		},
	}
}

// createTracesReceiver creates a traces receiver based on this config.
func createTracesReceiver(
	ctx context.Context,
	set receiver.Settings,
	cfg component.Config,
	nextConsumer consumer.Traces,
) (receiver.Traces, error) {
	config := cfg.(*Config)
	return newAgentContactReceiver(config, set, nextConsumer)
}
