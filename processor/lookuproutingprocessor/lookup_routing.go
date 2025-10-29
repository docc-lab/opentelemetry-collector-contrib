// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package lookuproutingprocessor

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/agentcontactexporter"
)

// lookupRoutingProcessor implements the lookup routing logic
type lookupRoutingProcessor struct {
	logger       *zap.Logger
	config       *Config
	nextConsumer consumer.Traces
}

// newLookupRoutingProcessor creates a new lookup routing processor
func newLookupRoutingProcessor(logger *zap.Logger, config *Config, nextConsumer consumer.Traces) *lookupRoutingProcessor {
	return &lookupRoutingProcessor{
		logger:       logger,
		config:       config,
		nextConsumer: nextConsumer,
	}
}

// start initializes the processor
func (lrp *lookupRoutingProcessor) start(ctx context.Context, host component.Host) error {
	lrp.logger.Info("Starting lookup routing processor")
	return nil
}

// shutdown stops the processor
func (lrp *lookupRoutingProcessor) shutdown(ctx context.Context) error {
	lrp.logger.Info("Shutting down lookup routing processor")
	return nil
}

// processTraces processes trace data and routes spans based on lookup results
func (lrp *lookupRoutingProcessor) processTraces(ctx context.Context, td ptrace.Traces) (ptrace.Traces, error) {
	lrp.logger.Debug("Processing traces for lookup routing",
		zap.Int("span_count", td.SpanCount()))

	// First, pass through the traces to the next consumer (normal pipeline flow)
	if err := lrp.nextConsumer.ConsumeTraces(ctx, td); err != nil {
		lrp.logger.Error("Failed to pass traces to next consumer", zap.Error(err))
		return td, err
	}

	// Then, extract metadata and send to agent contact exporter
	ace := agentcontactexporter.GlobalAgentContactExporter
	if ace == nil {
		lrp.logger.Debug("Agent contact exporter not available for routing")
		return td, nil
	}

	// Create traces for the agent contact exporter with extracted metadata
	agentContactTraces := lrp.extractMetadataForAgentContact(td)
	if agentContactTraces.SpanCount() > 0 {
		lrp.logger.Info("Sending extracted metadata to agent contact exporter",
			zap.Int("span_count", agentContactTraces.SpanCount()))

		if err := ace.ConsumeTraces(ctx, agentContactTraces); err != nil {
			lrp.logger.Error("Failed to send metadata to agent contact exporter", zap.Error(err))
		}
	}

	return td, nil
}

// extractMetadataForAgentContact extracts trace ID, parent span ID, and upstream IP from spans
func (lrp *lookupRoutingProcessor) extractMetadataForAgentContact(td ptrace.Traces) ptrace.Traces {
	outputTraces := ptrace.NewTraces()
	resourceSpans := outputTraces.ResourceSpans().AppendEmpty()
	scopeSpans := resourceSpans.ScopeSpans().AppendEmpty()

	// Set resource attributes for lookup routing spans
	resourceSpans.Resource().Attributes().PutStr("service.name", "lookup-routing-processor")
	resourceSpans.Resource().Attributes().PutStr("service.version", "1.0.0")

	// Set scope information
	scopeSpans.Scope().SetName("lookup-routing")
	scopeSpans.Scope().SetVersion("1.0.0")

	// Process each span in the incoming traces
	for i := 0; i < td.ResourceSpans().Len(); i++ {
		inputResourceSpans := td.ResourceSpans().At(i)
		for j := 0; j < inputResourceSpans.ScopeSpans().Len(); j++ {
			inputScopeSpans := inputResourceSpans.ScopeSpans().At(j)
			for k := 0; k < inputScopeSpans.Spans().Len(); k++ {
				span := inputScopeSpans.Spans().At(k)

				// Extract trace ID, parent span ID, and upstream IP
				traceID := span.TraceID().String()
				parentSpanID := span.ParentSpanID().String()
				upstreamIP := lrp.extractUpstreamIP(span)

				lrp.logger.Debug("Extracted metadata for agent contact",
					zap.String("trace_id", traceID),
					zap.String("parent_span_id", parentSpanID),
					zap.String("upstream_ip", upstreamIP))

				// Create a new span with the extracted metadata
				outputSpan := scopeSpans.Spans().AppendEmpty()

				// Copy the original span but change the span ID to its parent span ID
				span.CopyTo(outputSpan)
				outputSpan.SetSpanID(span.ParentSpanID())

				// Set upstream IP attribute
				outputSpan.Attributes().PutStr("upstream.ip", upstreamIP)
			}
		}
	}

	return outputTraces
}

// extractUpstreamIP extracts the upstream IP from span attributes
func (lrp *lookupRoutingProcessor) extractUpstreamIP(span ptrace.Span) string {
	if upstreamIP, exists := span.Attributes().AsRaw()["upstream.ip"]; exists {
		if upstreamIPStr, ok := upstreamIP.(string); ok {
			return upstreamIPStr
		}
	}
	return "unknown-upstream"
}

// Capabilities returns the capabilities of the processor
func (lrp *lookupRoutingProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}
