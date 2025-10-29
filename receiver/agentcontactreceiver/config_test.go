package agentcontactreceiver

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/confignet"
)

func TestConfig(t *testing.T) {
	cfg := createDefaultConfig()
	assert.NotNil(t, cfg)

	// Test default configuration
	config := cfg.(*Config)
	assert.Equal(t, "localhost:55679", config.NetAddr.Endpoint)
	assert.Equal(t, confignet.TransportTypeTCP, config.NetAddr.Transport)
}

func TestConfigValidation(t *testing.T) {
	cfg := createDefaultConfig()
	config := cfg.(*Config)

	// Test valid configuration
	err := config.Validate()
	require.NoError(t, err)

	// Test custom endpoint
	config.NetAddr.Endpoint = "0.0.0.0:8080"
	err = config.Validate()
	require.NoError(t, err)
}

func TestConfigStruct(t *testing.T) {
	require.NoError(t, componenttest.CheckConfigStruct(createDefaultConfig()))
}
