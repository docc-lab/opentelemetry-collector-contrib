package agentcontactexporter

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/exporter/exportertest"
)

func TestCreateDefaultConfig(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()
	assert.NotNil(t, cfg, "failed to create default config")
	assert.NoError(t, componenttest.CheckConfigStruct(cfg))
}

func TestCreateTraces(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()

	te, err := factory.CreateTraces(context.Background(), exportertest.NewNopSettings(component.MustNewType(typeStr)), cfg)
	assert.NoError(t, err)
	assert.NotNil(t, te)
}

func TestFactory(t *testing.T) {
	factory := NewFactory()
	assert.NotNil(t, factory)

	cfg := factory.CreateDefaultConfig()
	assert.NotNil(t, cfg)
	assert.IsType(t, &Config{}, cfg)

	// Test that the factory can create an exporter
	set := exportertest.NewNopSettings(component.MustNewType(typeStr))
	exp, err := factory.CreateTraces(context.Background(), set, cfg)
	assert.NoError(t, err)
	assert.NotNil(t, exp)

	// Test lifecycle
	err = exp.Start(context.Background(), componenttest.NewNopHost())
	assert.NoError(t, err)

	err = exp.Shutdown(context.Background())
	assert.NoError(t, err)
}
