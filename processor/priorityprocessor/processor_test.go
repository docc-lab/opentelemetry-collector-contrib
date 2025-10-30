// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package priorityprocessor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor/processortest"

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

	set := processortest.NewNopSettings(metadata.Type)

	tracesProcessor, err := factory.CreateTraces(
		context.Background(),
		set,
		cfg,
		consumertest.NewNop(),
	)

	require.NoError(t, err)
	assert.NotNil(t, tracesProcessor)

	// Start processor
	host := componenttest.NewNopHost()
	err = tracesProcessor.Start(context.Background(), host)
	require.NoError(t, err)

	// Shutdown processor
	err = tracesProcessor.Shutdown(context.Background())
	require.NoError(t, err)
}

func TestConsumeTraces(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()
	set := processortest.NewNopSettings(metadata.Type)

	tracesProcessor, err := factory.CreateTraces(
		context.Background(),
		set,
		cfg,
		consumertest.NewNop(),
	)
	require.NoError(t, err)

	host := componenttest.NewNopHost()
	err = tracesProcessor.Start(context.Background(), host)
	require.NoError(t, err)

	td := ptrace.NewTraces()
	err = tracesProcessor.ConsumeTraces(context.Background(), td)

	assert.NoError(t, err)

	err = tracesProcessor.Shutdown(context.Background())
	require.NoError(t, err)
}

func TestStart(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()
	set := processortest.NewNopSettings(metadata.Type)

	tracesProcessor, err := factory.CreateTraces(
		context.Background(),
		set,
		cfg,
		consumertest.NewNop(),
	)
	require.NoError(t, err)

	host := componenttest.NewNopHost()
	err = tracesProcessor.Start(context.Background(), host)

	require.NoError(t, err)

	err = tracesProcessor.Shutdown(context.Background())
	require.NoError(t, err)
}

func TestShutdown(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()
	set := processortest.NewNopSettings(metadata.Type)

	tracesProcessor, err := factory.CreateTraces(
		context.Background(),
		set,
		cfg,
		consumertest.NewNop(),
	)
	require.NoError(t, err)

	err = tracesProcessor.Shutdown(context.Background())

	assert.NoError(t, err)
}

func TestCapabilities(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()
	set := processortest.NewNopSettings(metadata.Type)

	tracesProcessor, err := factory.CreateTraces(
		context.Background(),
		set,
		cfg,
		consumertest.NewNop(),
	)
	require.NoError(t, err)

	caps := tracesProcessor.Capabilities()
	assert.True(t, caps.MutatesData)

	err = tracesProcessor.Shutdown(context.Background())
	require.NoError(t, err)
}
