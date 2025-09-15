// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package spanlookupprocessor

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processorhelper"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/spanlookupprocessor/internal/metadata"
)

// NewFactory creates a factory for the span lookup processor
func NewFactory() processor.Factory {
	return processor.NewFactory(
		metadata.Type,
		createDefaultConfig,
		processor.WithTraces(createTracesProcessor, metadata.TracesStability),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		MaxLookups:    1000,
		EnableMetrics: false,
	}
}

func createTracesProcessor(
	ctx context.Context,
	set processor.Settings,
	cfg component.Config,
	nextConsumer consumer.Traces,
) (processor.Traces, error) {
	oCfg := cfg.(*Config)
	sp := newSpanLookupProcessor(set.Logger, oCfg)
	return processorhelper.NewTraces(
		ctx,
		set,
		cfg,
		nextConsumer,
		sp.processTraces,
		processorhelper.WithCapabilities(consumer.Capabilities{MutatesData: false}),
		processorhelper.WithStart(sp.start),
		processorhelper.WithShutdown(sp.shutdown),
	)
}
