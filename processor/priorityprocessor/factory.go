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

// createDefaultConfig returns defaults for the analytic LP-shedding
// controller. soft=50 / hard=70 mirror the vanilla memory_limiter
// contract; cp_safety_factor=1.0 means the LP-shed margin is FULLY derived
// (amplitude_env + (1/lp_share)·hp_rise) with no extra cushion — it is an
// optional overall trim on the drift term, not a required gain.
func createDefaultConfig() component.Config {
	return &Config{
		SoftPercentage:      50,
		HardPercentage:      70,
		CPSafetyFactor:      1.0,
		CheckInterval:       100 * time.Millisecond,
		ForceGC:             true,
		GCSoftInterval:      1 * time.Second,
		GCUltrasoftInterval: 2 * time.Second,
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
