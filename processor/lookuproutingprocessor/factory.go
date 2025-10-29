// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package lookuproutingprocessor

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processorhelper"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/lookuproutingprocessor/internal/metadata"
)

// NewFactory creates a factory for the lookup routing processor
func NewFactory() processor.Factory {
	return processor.NewFactory(
		metadata.Type,
		createDefaultConfig,
		processor.WithTraces(createTracesProcessor, metadata.TracesStability),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		RoutingRules: []RoutingRule{},
		DefaultRoute: "default",
	}
}

func createTracesProcessor(
	ctx context.Context,
	set processor.Settings,
	cfg component.Config,
	nextConsumer consumer.Traces,
) (processor.Traces, error) {
	oCfg := cfg.(*Config)
	lrp := newLookupRoutingProcessor(set.Logger, oCfg, nextConsumer)
	return processorhelper.NewTraces(
		ctx,
		set,
		cfg,
		nextConsumer,
		lrp.processTraces,
		processorhelper.WithCapabilities(consumer.Capabilities{MutatesData: false}),
		processorhelper.WithStart(lrp.start),
		processorhelper.WithShutdown(lrp.shutdown),
	)
}
