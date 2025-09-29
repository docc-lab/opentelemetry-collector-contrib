// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package spanlookupprocessor

import (
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/spanreconstructionprocessor"
)

// spanLookupProcessor implements the span lookup logic
type spanLookupProcessor struct {
	logger       *zap.Logger
	config       *Config
	nextConsumer consumer.Traces
	stopCh       chan struct{}

	ExportedSpanIDs map[string]bool
	exportMutex     sync.RWMutex

	// Worker control
	stopWorker chan struct{}
	workerWg   sync.WaitGroup
}

// newSpanLookupProcessor creates a new span lookup processor
func newSpanLookupProcessor(logger *zap.Logger, config *Config, nextConsumer consumer.Traces) *spanLookupProcessor {
	return &spanLookupProcessor{
		logger:          logger,
		config:          config,
		nextConsumer:    nextConsumer,
		ExportedSpanIDs: make(map[string]bool),
		stopWorker:      make(chan struct{}),
	}
}

// start initializes the processor and launches goroutines
func (p *spanLookupProcessor) start(ctx context.Context, host component.Host) error {
	p.stopCh = make(chan struct{})

	// Start the state logging goroutine
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

	// Start the span export worker
	p.workerWg.Add(1)
	go p.exportNewSpansWorker()

	return nil
}

// shutdown stops the processor and all goroutines
func (p *spanLookupProcessor) shutdown(ctx context.Context) error {
	// Stop the state logging goroutine
	if p.stopCh != nil {
		close(p.stopCh)
	}

	// Stop the span export worker
	close(p.stopWorker)
	p.workerWg.Wait()

	p.logger.Info("Shutting down span lookup processor")
	return nil
}

// processTraces processes trace data and looks up spans in the span reconstruction processor
func (p *spanLookupProcessor) processTraces(ctx context.Context, td ptrace.Traces) (ptrace.Traces, error) {
	sr := spanreconstructionprocessor.GlobalSpanReconstructionProcessor
	if sr == nil {
		p.logger.Debug("Span reconstruction processor not available for lookup")
		return td, nil
	}

	var foundSpans []ptrace.Span
	var foundResources []ptrace.ResourceSpans
	var foundScopes []ptrace.ScopeSpans

	// Process each span in the incoming traces
	for i := 0; i < td.ResourceSpans().Len(); i++ {
		resourceSpans := td.ResourceSpans().At(i)
		for j := 0; j < resourceSpans.ScopeSpans().Len(); j++ {
			scopeSpans := resourceSpans.ScopeSpans().At(j)
			for k := 0; k < scopeSpans.Spans().Len(); k++ {
				span := scopeSpans.Spans().At(k)

				// Extract trace ID and span ID
				traceID := span.TraceID().String()
				spanID := span.SpanID().String()

				p.logger.Debug("Looking up client-side span in reconstruction processor",
					zap.String("trace_id", traceID),
					zap.String("span_id", spanID))

				// Step 1: Look up the client-side span (sent from overlap detection processor)
				if clientSpan, _, _, exists := sr.GetClosedSpanData(traceID + ":" + spanID); exists {
					p.logger.Info("Found client-side span",
						zap.String("trace_id", traceID),
						zap.String("span_id", spanID),
						zap.String("span_name", clientSpan.Name()))

					// Step 2: Look up the upstream server-side parent span
					serverParentSpanID := clientSpan.ParentSpanID().String()
					if serverParentSpanID != "" {
						p.logger.Debug("Looking up upstream server-side parent span",
							zap.String("trace_id", traceID),
							zap.String("server_parent_span_id", serverParentSpanID))

						if serverSpan, resource, scope, exists := sr.GetClosedSpanData(traceID + ":" + serverParentSpanID); exists {
							p.logger.Info("Found upstream server-side parent span",
								zap.String("trace_id", traceID),
								zap.String("server_span_id", serverParentSpanID),
								zap.String("server_span_name", serverSpan.Name()))

							foundSpans = append(foundSpans, serverSpan)
							foundResources = append(foundResources, resource)
							foundScopes = append(foundScopes, scope)
						} else {
							p.logger.Debug("Upstream server-side parent span not found",
								zap.String("trace_id", traceID),
								zap.String("server_parent_span_id", serverParentSpanID))
						}
					} else {
						p.logger.Debug("Client span has no parent span ID",
							zap.String("trace_id", traceID),
							zap.String("client_span_id", spanID))
					}
				} else {
					p.logger.Debug("Client-side span not found in reconstruction processor",
						zap.String("trace_id", traceID),
						zap.String("span_id", spanID))
				}
			}
		}
	}

	// Log the found upstream server-side spans
	if len(foundSpans) > 0 {
		p.logger.Info("Found upstream server-side spans in reconstruction processor",
			zap.Int("found_count", len(foundSpans)))

		for i, span := range foundSpans {
			p.logger.Info("Found upstream server-side span details",
				zap.Int("index", i),
				zap.String("trace_id", span.TraceID().String()),
				zap.String("span_id", span.SpanID().String()),
				zap.String("span_name", span.Name()),
				zap.String("span_kind", span.Kind().String()),
				zap.String("parent_span_id", span.ParentSpanID().String()))
		}
	} else {
		p.logger.Debug("No upstream server-side spans found in reconstruction processor for lookup request")
	}

	// Return empty traces since we've processed them
	return ptrace.NewTraces(), nil
}

// exportNewSpansWorker continuously exports new spans from ClosedSpans
func (p *spanLookupProcessor) exportNewSpansWorker() {
	defer p.workerWg.Done()

	interval := time.Duration(p.config.ExportIntervalSeconds) * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	p.logger.Info("Starting span export worker",
		zap.Int("export_interval_seconds", p.config.ExportIntervalSeconds))

	for {
		select {
		case <-ticker.C:
			p.exportNewSpans()
		case <-p.stopWorker:
			p.logger.Info("Stopping span export worker")
			return
		}
	}
}

// exportNewSpans exports spans that haven't been exported yet
func (p *spanLookupProcessor) exportNewSpans() {
	sr := spanreconstructionprocessor.GlobalSpanReconstructionProcessor
	if sr == nil {
		p.logger.Debug("Span reconstruction processor not available")
		return
	}

	// Get all closed span keys
	spanKeys := sr.GetAllClosedSpanKeys()

	if len(spanKeys) == 0 {
		p.logger.Debug("No closed spans available for export")
		return
	}

	p.logger.Debug("Checking for new spans to export",
		zap.Int("total_closed_spans", len(spanKeys)))

	exportedCount := 0

	// Iterate through closed spans and export new ones
	for _, spanKey := range spanKeys {

		// Get span data
		span, resource, scope, exists := sr.GetClosedSpanData(spanKey)
		if !exists {
			continue
		}

		// Create traceID:spanID key
		traceID := span.TraceID().String()
		spanID := span.SpanID().String()
		exportKey := traceID + ":" + spanID

		// Check if we've already exported this span
		p.exportMutex.RLock()
		alreadyExported := p.ExportedSpanIDs[exportKey]
		p.exportMutex.RUnlock()

		if alreadyExported {
			continue
		}

		// Export the span
		if err := p.exportSpan(span, resource, scope); err != nil {
			p.logger.Error("Failed to export span",
				zap.String("span_key", spanKey),
				zap.String("trace_id", traceID),
				zap.String("span_id", spanID),
				zap.Error(err))
			continue
		}

		// Mark as exported
		p.exportMutex.Lock()
		p.ExportedSpanIDs[exportKey] = true
		p.exportMutex.Unlock()

		exportedCount++

		p.logger.Debug("Exported new span",
			zap.String("span_key", spanKey),
			zap.String("trace_id", traceID),
			zap.String("span_id", spanID),
			zap.String("span_name", span.Name()))
	}

	if exportedCount > 0 {
		p.logger.Info("Exported new spans",
			zap.Int("exported_count", exportedCount),
			zap.Int("total_exported", len(p.ExportedSpanIDs)))
	}
}

// exportSpan exports a single reconstructed span
func (p *spanLookupProcessor) exportSpan(span ptrace.Span, resource ptrace.ResourceSpans, scope ptrace.ScopeSpans) error {
	if p.nextConsumer == nil {
		return nil // No consumer to export to
	}

	// Create output traces
	outputTraces := ptrace.NewTraces()
	resourceSpans := outputTraces.ResourceSpans().AppendEmpty()
	scopeSpans := resourceSpans.ScopeSpans().AppendEmpty()

	// Copy resource attributes from original span
	resource.Resource().Attributes().CopyTo(resourceSpans.Resource().Attributes())

	// Copy scope information from original span
	scope.Scope().CopyTo(scopeSpans.Scope())

	// Add the span
	outputSpan := scopeSpans.Spans().AppendEmpty()
	span.CopyTo(outputSpan)

	// Send to next consumer
	ctx := context.Background()
	return p.nextConsumer.ConsumeTraces(ctx, outputTraces)
}

// Capabilities returns the capabilities of the processor
func (p *spanLookupProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}
