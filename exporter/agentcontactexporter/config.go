package agentcontactexporter

import (
	"time"

	"go.opentelemetry.io/collector/component"
)

// Config defines configuration for the agent contact exporter.
type Config struct {
	// DefaultPort is the port to use when constructing dynamic endpoints from IP addresses.
	// If not specified, defaults to 55679.
	DefaultPort int `mapstructure:"default_port"`

	// Timeout is the timeout for gRPC requests.
	Timeout time.Duration `mapstructure:"timeout"`

	// BatchSize is the maximum number of trace IDs to send in a single request.
	BatchSize int `mapstructure:"batch_size"`

	// BatchTimeout is the timeout for batching trace IDs before sending.
	BatchTimeout time.Duration `mapstructure:"batch_timeout"`
}

// Validate checks if the exporter configuration is valid.
func (cfg *Config) Validate() error {
	// Set default port if not specified
	if cfg.DefaultPort == 0 {
		cfg.DefaultPort = 55679
	}

	return nil
}

var _ component.Config = (*Config)(nil)
