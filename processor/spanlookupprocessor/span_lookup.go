// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package spanlookupprocessor

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/spanreconstructionprocessor"
)

// spanLookupProcessor implements the span lookup logic
type spanLookupProcessor struct {
	logger *zap.Logger
	config *Config
	stopCh chan struct{}
}

// newSpanLookupProcessor creates a new span lookup processor
func newSpanLookupProcessor(logger *zap.Logger, config *Config) *spanLookupProcessor {
	return &spanLookupProcessor{
		logger: logger,
		config: config,
	}
}

// start initializes the processor and launches a goroutine to log spanreconstruction state
func (p *spanLookupProcessor) start(ctx context.Context, host component.Host) error {
	p.stopCh = make(chan struct{})
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				sr := spanreconstructionprocessor.GlobalSpanReconstructionProcessor
				if sr != nil {
					sr.SpansMutex.RLock()
					open := len(sr.OpenSpans)
					closed := len(sr.ClosedSpans)
					sr.SpansMutex.RUnlock()
					p.logger.Info("Span lookup: current spanreconstruction state",
						zap.Int("open_spans", open),
						zap.Int("closed_spans", closed),
					)
				} else {
					p.logger.Info("Span lookup: spanreconstruction processor not available")
				}
			case <-p.stopCh:
				return
			}
		}
	}()
	return nil
}

// shutdown stops the processor and the logging goroutine
func (p *spanLookupProcessor) shutdown(ctx context.Context) error {
	if p.stopCh != nil {
		close(p.stopCh)
	}
	p.logger.Info("Shutting down span lookup processor")
	return nil
}

// processTraces processes trace data and demonstrates accessing global span data
func (p *spanLookupProcessor) processTraces(ctx context.Context, td ptrace.Traces) (ptrace.Traces, error) {
	// No-op for now; all logging is done in the goroutine
	return td, nil
}

// Capabilities returns the capabilities of the processor
func (p *spanLookupProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}
