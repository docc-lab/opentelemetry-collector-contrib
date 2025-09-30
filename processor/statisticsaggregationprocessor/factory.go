// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package statisticsaggregationprocessor

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processorhelper"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/statisticsaggregationprocessor/internal/metadata"
)

// NewFactory creates a factory for the statistics aggregation processor
func NewFactory() processor.Factory {
	return processor.NewFactory(
		metadata.Type,
		createDefaultConfig,
		processor.WithTraces(createTracesProcessor, metadata.TracesStability),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		ExportWindow: 60 * time.Second,
	}
}

func createTracesProcessor(
	ctx context.Context,
	set processor.Settings,
	cfg component.Config,
	nextConsumer consumer.Traces,
) (processor.Traces, error) {
	oCfg := cfg.(*Config)
	sap := newStatisticsAggregationProcessor(set.Logger, oCfg, nextConsumer, ServiceBasedFeatureExtractor)
	return processorhelper.NewTraces(
		ctx,
		set,
		cfg,
		nextConsumer,
		sap.processTraces,
		processorhelper.WithCapabilities(consumer.Capabilities{MutatesData: false}),
		processorhelper.WithStart(sap.start),
		processorhelper.WithShutdown(sap.shutdown),
	)
}
