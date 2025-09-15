package agentcontactexporter

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
)

func TestConfig(t *testing.T) {
	cfg := createDefaultConfig()
	assert.NotNil(t, cfg)

	// Test default configuration
	config := cfg.(*Config)
	assert.Equal(t, 55679, config.DefaultPort)
	assert.Equal(t, 30*time.Second, config.Timeout)
	assert.Equal(t, 100, config.BatchSize)
	assert.Equal(t, 5*time.Second, config.BatchTimeout)
}

func TestConfigValidation(t *testing.T) {
	cfg := createDefaultConfig()
	config := cfg.(*Config)

	// Test valid configuration
	err := config.Validate()
	require.NoError(t, err)

	// Test custom configuration
	config.DefaultPort = 8080
	config.Timeout = 60 * time.Second
	config.BatchSize = 200
	config.BatchTimeout = 10 * time.Second
	err = config.Validate()
	require.NoError(t, err)
}

func TestConfigStruct(t *testing.T) {
	require.NoError(t, componenttest.CheckConfigStruct(createDefaultConfig()))
}
