// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package priorityprocessor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/priorityprocessor/internal/metadata"
)

func TestNewFactory(t *testing.T) {
	factory := NewFactory()
	assert.NotNil(t, factory)
	assert.Equal(t, metadata.Type, factory.Type())
}

func TestCreateDefaultConfig(t *testing.T) {
	cfg := createDefaultConfig()
	assert.NotNil(t, cfg)

	config, ok := cfg.(*Config)
	require.True(t, ok)
	assert.NotNil(t, config)
}

func TestCreateTracesProcessor(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()
	host := component.NewAssertNoErrorHost(t)

	set := processor.CreateSettings{
		TelemetrySettings: component.TelemetrySettings{},
		ID:                component.MustNewID("priority"),
	}

	tracesProcessor, err := factory.CreateTracesProcessor(
		context.Background(),
		set,
		cfg,
		consumertest.NewNop(),
	)

	require.NoError(t, err)
	assert.NotNil(t, tracesProcessor)
}

func TestConsumeTraces(t *testing.T) {
	cfg := &Config{}
	nextConsumer := consumertest.NewNop()
	logger := zap.NewNop()

	processor := newProcessor(cfg, logger, nextConsumer)

	td := ptrace.NewTraces()
	err := processor.ConsumeTraces(context.Background(), td)

	assert.NoError(t, err)
}

func TestStart(t *testing.T) {
	cfg := &Config{}
	nextConsumer := consumertest.NewNop()
	logger := zap.NewNop()

	processor := newProcessor(cfg, logger, nextConsumer)

	host := component.NewAssertNoErrorHost(t)
	err := processor.Start(context.Background(), host)

	assert.NoError(t, err)
}

func TestShutdown(t *testing.T) {
	cfg := &Config{}
	nextConsumer := consumertest.NewNop()
	logger := zap.NewNop()

	processor := newProcessor(cfg, logger, nextConsumer)

	err := processor.Shutdown(context.Background())

	assert.NoError(t, err)
}

func TestCapabilities(t *testing.T) {
	cfg := &Config{}
	nextConsumer := consumertest.NewNop()
	logger := zap.NewNop()

	processor := newProcessor(cfg, logger, nextConsumer)

	caps := processor.Capabilities()
	assert.True(t, caps.MutatesData)
}
