package agentcontactreceiver

import (
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configgrpc"
)

// Config defines configuration for the agent contact receiver.
type Config struct {
	// Configures the receiver server protocol.
	configgrpc.ServerConfig `mapstructure:",squash"` // squash ensures fields are correctly decoded in embedded struct
}

var _ component.Config = (*Config)(nil)

// Validate checks the receiver configuration is valid
func (cfg *Config) Validate() error {
	return nil
}
