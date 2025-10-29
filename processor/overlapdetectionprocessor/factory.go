// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package overlapdetectionprocessor

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processorhelper"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/overlapdetectionprocessor/internal/metadata"
)

const (
	// The value of "type" key in configuration.
	typeStr = "overlapdetection"
)

// NewFactory creates a new factory for the overlap detection processor.
func NewFactory() processor.Factory {
	return processor.NewFactory(
		component.MustNewType(typeStr),
		createDefaultConfig,
		processor.WithTraces(createTracesProcessor, metadata.TracesStability),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		OverlapThresholdMs: 1.0,
		EnableMetrics:      false,
	}
}

func createTracesProcessor(
	ctx context.Context,
	set processor.Settings,
	cfg component.Config,
	nextConsumer consumer.Traces,
) (processor.Traces, error) {
	oCfg := cfg.(*Config)
	odp := newOverlapDetectionProcessor(set.Logger, oCfg, nextConsumer)
	return processorhelper.NewTraces(
		ctx,
		set,
		cfg,
		nextConsumer,
		odp.processTraces,
		processorhelper.WithCapabilities(consumer.Capabilities{MutatesData: true}),
		processorhelper.WithStart(odp.start),
		processorhelper.WithShutdown(odp.shutdown),
	)
}
