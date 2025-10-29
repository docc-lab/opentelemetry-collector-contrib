// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package priorityprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/priorityprocessor"

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"
)

var _ processor.Traces = (*priorityProcessor)(nil)

type priorityProcessor struct {
	config       *Config
	logger       *zap.Logger
	nextConsumer consumer.Traces
}

func newProcessor(config *Config, logger *zap.Logger, nextConsumer consumer.Traces) *priorityProcessor {
	return &priorityProcessor{
		config:       config,
		logger:       logger,
		nextConsumer: nextConsumer,
	}
}

func (p *priorityProcessor) Start(_ context.Context, _ component.Host) error {
	p.logger.Info("Priority processor started")
	return nil
}

func (p *priorityProcessor) Shutdown(_ context.Context) error {
	p.logger.Info("Priority processor shutdown")
	return nil
}

func (p *priorityProcessor) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	// TODO: Implement priority processing logic
	return p.nextConsumer.ConsumeTraces(ctx, td)
}

func (*priorityProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}
