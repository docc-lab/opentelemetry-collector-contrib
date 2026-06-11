// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package priorityprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/priorityprocessor"

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processorhelper"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/priorityprocessor/internal/metadata"
)

func NewFactory() processor.Factory {
	return processor.NewFactory(
		metadata.Type,
		createDefaultConfig,
		processor.WithTraces(createTracesProcessor, metadata.TracesStability),
	)
}

// createDefaultConfig returns defaults for the two-zone pressure
// model: soft=50 (LP refused, HP admits only by evicting LP), hard=70
// (force GC and refuse all if still over). NumWorkers defaults to 10
// to match the default sending_queue.num_consumers on the OTLP
// exporter — pair with queue_size=10, block_on_overflow=true downstream
// to get the backpressure-into-priority semantics this processor
// expects. See Config docstring for full behavior.
func createDefaultConfig() component.Config {
	return &Config{
		SoftPercentage: 50,
		HardPercentage: 70,
		NumWorkers:     10,
		EvictionRatio:  2,
		CheckInterval:  100 * time.Millisecond,
	}
}

func createTracesProcessor(
	ctx context.Context,
	set processor.Settings,
	cfg component.Config,
	nextConsumer consumer.Traces,
) (processor.Traces, error) {
	processorConfig, ok := cfg.(*Config)
	if !ok {
		return nil, errors.New("configuration parsing error")
	}

	if err := processorConfig.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	proc, err := newProcessor(processorConfig, set.Logger, nextConsumer)
	if err != nil {
		return nil, err
	}

	return processorhelper.NewTraces(
		ctx,
		set,
		cfg,
		nextConsumer,
		proc.processTraces,
		processorhelper.WithCapabilities(consumer.Capabilities{MutatesData: false}),
		processorhelper.WithStart(proc.start),
		processorhelper.WithShutdown(proc.shutdown),
	)
}
