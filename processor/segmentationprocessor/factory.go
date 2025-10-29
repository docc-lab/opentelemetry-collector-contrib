// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package segmentationprocessor

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processorhelper"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/segmentationprocessor/internal/metadata"
)

const (
	// The value of "type" key in configuration.
	typeStr = "segmentation"
)

// NewFactory creates a factory for the segmentation processor.
func NewFactory() processor.Factory {
	return processor.NewFactory(
		component.MustNewType(typeStr),
		createDefaultConfig,
		processor.WithTraces(createTracesProcessor, metadata.TracesStability),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		SegmentDuration: time.Minute,
		EnableMetrics:   false,
	}
}

func createTracesProcessor(
	ctx context.Context,
	set processor.Settings,
	cfg component.Config,
	nextConsumer consumer.Traces,
) (processor.Traces, error) {
	oCfg := cfg.(*Config)
	sp := newSegmentationProcessor(set.Logger, oCfg, nextConsumer)
	return processorhelper.NewTraces(
		ctx,
		set,
		cfg,
		nextConsumer,
		sp.processTraces,
		processorhelper.WithCapabilities(consumer.Capabilities{MutatesData: true}),
		processorhelper.WithStart(sp.start),
		processorhelper.WithShutdown(sp.shutdown),
	)
}
