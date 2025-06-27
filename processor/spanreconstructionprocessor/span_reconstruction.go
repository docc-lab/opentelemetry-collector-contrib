// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package spanreconstructionprocessor

import (
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

// spanReconstructionProcessor implements the span reconstruction logic
type spanReconstructionProcessor struct {
	logger *zap.Logger
	config *Config

	// Active spans storage
	activeSpans map[string]*activeSpan
	spansMutex  sync.RWMutex

	// Cleanup ticker
	cleanupTicker *time.Ticker
	stopCleanup   chan struct{}
}

// activeSpan represents a span that is currently being reconstructed
type activeSpan struct {
	span      ptrace.Span
	resource  ptrace.ResourceSpans
	scope     ptrace.ScopeSpans
	lastSeen  time.Time
	startTime time.Time
}

// newSpanReconstructionProcessor creates a new span reconstruction processor
func newSpanReconstructionProcessor(logger *zap.Logger, config *Config) *spanReconstructionProcessor {
	return &spanReconstructionProcessor{
		logger:      logger,
		config:      config,
		activeSpans: make(map[string]*activeSpan),
		stopCleanup: make(chan struct{}),
	}
}

// start initializes the processor
func (p *spanReconstructionProcessor) start(ctx context.Context, host component.Host) error {
	// Start cleanup goroutine
	p.cleanupTicker = time.NewTicker(p.config.SpanTTL / 4) // Clean up every quarter of TTL
	go p.cleanupRoutine()
	return nil
}

// shutdown stops the processor
func (p *spanReconstructionProcessor) shutdown(ctx context.Context) error {
	if p.cleanupTicker != nil {
		p.cleanupTicker.Stop()
	}
	close(p.stopCleanup)
	return nil
}

// processTraces processes incoming traces and reconstructs spans
func (p *spanReconstructionProcessor) processTraces(ctx context.Context, td ptrace.Traces) (ptrace.Traces, error) {
	p.logger.Info("🟢 INFO: Processing traces batch",
		zap.Int("resource_spans_count", td.ResourceSpans().Len()),
		zap.Int("total_spans", p.countTotalSpans(td)))

	// Create output traces
	outputTraces := ptrace.NewTraces()

	// Process each resource
	for i := 0; i < td.ResourceSpans().Len(); i++ {
		resourceSpans := td.ResourceSpans().At(i)
		outputResource := outputTraces.ResourceSpans().AppendEmpty()
		resourceSpans.Resource().CopyTo(outputResource.Resource())

		p.logger.Debug("🟠 DEBUG: Processing resource",
			zap.Int("resource_index", i),
			zap.String("service_name", p.getServiceName(resourceSpans.Resource())),
			zap.Int("scope_spans_count", resourceSpans.ScopeSpans().Len()))

		// Process each scope
		for j := 0; j < resourceSpans.ScopeSpans().Len(); j++ {
			scopeSpans := resourceSpans.ScopeSpans().At(j)
			outputScope := outputResource.ScopeSpans().AppendEmpty()
			scopeSpans.Scope().CopyTo(outputScope.Scope())

			p.logger.Debug("🟠 DEBUG: Processing scope",
				zap.Int("scope_index", j),
				zap.String("scope_name", scopeSpans.Scope().Name()),
				zap.Int("spans_count", scopeSpans.Spans().Len()))

			// Process each span
			for k := 0; k < scopeSpans.Spans().Len(); k++ {
				span := scopeSpans.Spans().At(k)
				p.processSpan(span, outputScope, outputResource)
			}
		}
	}

	// Add completed spans to output
	completedCount := p.addCompletedSpans(outputTraces)

	p.logger.Info("🟢 INFO: Completed processing traces batch",
		zap.Int("output_spans", p.countTotalSpans(outputTraces)),
		zap.Int("completed_spans_added", completedCount),
		zap.Int("active_spans_count", len(p.activeSpans)))

	return outputTraces, nil
}

// processSpan processes a single span and determines if it's a start, end, or log event
func (p *spanReconstructionProcessor) processSpan(span ptrace.Span, outputScope ptrace.ScopeSpans, outputResource ptrace.ResourceSpans) {
	spanKey := p.getSpanKey(span)

	p.logger.Debug("🟠 DEBUG: Processing individual span",
		zap.String("trace_id", span.TraceID().String()),
		zap.String("span_id", span.SpanID().String()),
		zap.String("span_name", span.Name()),
		zap.Bool("has_start_time", span.StartTimestamp() != 0),
		zap.Bool("has_end_time", span.EndTimestamp() != 0),
		zap.Int("events_count", span.Events().Len()),
		zap.String("span_key", spanKey))

	// Check if this is a complete span (has both start and end times)
	if span.StartTimestamp() != 0 && span.EndTimestamp() != 0 {
		// This is a complete span, add it directly to output
		p.logger.Debug("🟠 DEBUG: Span is complete - passing through directly",
			zap.String("trace_id", span.TraceID().String()),
			zap.String("span_id", span.SpanID().String()))

		outputSpan := outputScope.Spans().AppendEmpty()
		span.CopyTo(outputSpan)
		return
	}

	// Check if this is a span start event
	if span.StartTimestamp() != 0 && span.EndTimestamp() == 0 {
		p.logger.Debug("🟠 DEBUG: Span appears to be a START event",
			zap.String("trace_id", span.TraceID().String()),
			zap.String("span_id", span.SpanID().String()))

		p.handleSpanStart(span, spanKey, outputResource, outputScope)
		return
	}

	// Check if this is a span end event
	if span.StartTimestamp() == 0 && span.EndTimestamp() != 0 {
		p.logger.Debug("🟠 DEBUG: Span appears to be an END event",
			zap.String("trace_id", span.TraceID().String()),
			zap.String("span_id", span.SpanID().String()))

		p.handleSpanEnd(span, spanKey, outputScope)
		return
	}

	// Check if this is a log event (has events but no timestamps)
	if span.Events().Len() > 0 && span.StartTimestamp() == 0 && span.EndTimestamp() == 0 {
		p.logger.Debug("🟠 DEBUG: Span appears to be a LOG event",
			zap.String("trace_id", span.TraceID().String()),
			zap.String("span_id", span.SpanID().String()),
			zap.Int("events_count", span.Events().Len()))

		p.handleLogEvent(span, spanKey, outputScope)
		return
	}

	// Unknown event type, log warning and pass through
	p.logger.Warn("🔴 WARN: Unknown span event type - passing through",
		zap.String("trace_id", span.TraceID().String()),
		zap.String("span_id", span.SpanID().String()),
		zap.Bool("has_start_time", span.StartTimestamp() != 0),
		zap.Bool("has_end_time", span.EndTimestamp() != 0),
		zap.Int("events_count", span.Events().Len()))

	outputSpan := outputScope.Spans().AppendEmpty()
	span.CopyTo(outputSpan)
}

// handleSpanStart processes a span start event
func (p *spanReconstructionProcessor) handleSpanStart(span ptrace.Span, spanKey string, outputResource ptrace.ResourceSpans, outputScope ptrace.ScopeSpans) {
	p.spansMutex.Lock()
	defer p.spansMutex.Unlock()

	p.logger.Info("🟢 INFO: Handling span START event",
		zap.String("trace_id", span.TraceID().String()),
		zap.String("span_id", span.SpanID().String()),
		zap.String("span_name", span.Name()),
		zap.Int("current_active_spans", len(p.activeSpans)),
		zap.Int("max_active_spans", p.config.MaxActiveSpans))

	// Check if we're at capacity
	if len(p.activeSpans) >= p.config.MaxActiveSpans {
		p.logger.Warn("🔴 WARN: Active spans at capacity - evicting oldest span",
			zap.Int("current_count", len(p.activeSpans)),
			zap.Int("max_capacity", p.config.MaxActiveSpans))
		p.evictOldestSpan()
	}

	// Check if span already exists
	if existingSpan, exists := p.activeSpans[spanKey]; exists {
		p.logger.Warn("🔴 WARN: Span START event for already active span - updating",
			zap.String("trace_id", span.TraceID().String()),
			zap.String("span_id", span.SpanID().String()),
			zap.Time("existing_last_seen", existingSpan.lastSeen))
	}

	// Create new active span
	activeSpan := &activeSpan{
		span:      span,
		resource:  outputResource,
		scope:     outputScope,
		lastSeen:  time.Now(),
		startTime: time.Now(),
	}

	p.activeSpans[spanKey] = activeSpan
	p.logger.Info("🟢 INFO: Created new active span",
		zap.String("trace_id", span.TraceID().String()),
		zap.String("span_id", span.SpanID().String()),
		zap.String("span_name", span.Name()),
		zap.Int("total_active_spans", len(p.activeSpans)))
}

// handleSpanEnd processes a span end event
func (p *spanReconstructionProcessor) handleSpanEnd(span ptrace.Span, spanKey string, outputScope ptrace.ScopeSpans) {
	p.spansMutex.Lock()
	defer p.spansMutex.Unlock()

	p.logger.Info("🟢 INFO: Handling span END event",
		zap.String("trace_id", span.TraceID().String()),
		zap.String("span_id", span.SpanID().String()),
		zap.String("span_name", span.Name()))

	activeSpan, exists := p.activeSpans[spanKey]
	if !exists {
		p.logger.Warn("🔴 WARN: Received span END event for unknown span - no matching START found",
			zap.String("trace_id", span.TraceID().String()),
			zap.String("span_id", span.SpanID().String()),
			zap.Int("available_active_spans", len(p.activeSpans)))
		return
	}

	// Complete the span by copying end time
	activeSpan.span.SetEndTimestamp(span.EndTimestamp())
	activeSpan.lastSeen = time.Now()

	p.logger.Info("🟢 INFO: Successfully completed span reconstruction",
		zap.String("trace_id", span.TraceID().String()),
		zap.String("span_id", span.SpanID().String()),
		zap.String("span_name", span.Name()),
		zap.Time("start_time", activeSpan.startTime),
		zap.Time("completion_time", activeSpan.lastSeen),
		zap.Duration("reconstruction_duration", activeSpan.lastSeen.Sub(activeSpan.startTime)))
}

// handleLogEvent processes a log event
func (p *spanReconstructionProcessor) handleLogEvent(span ptrace.Span, spanKey string, outputScope ptrace.ScopeSpans) {
	p.spansMutex.Lock()
	defer p.spansMutex.Unlock()

	p.logger.Info("🟢 INFO: Handling span LOG event",
		zap.String("trace_id", span.TraceID().String()),
		zap.String("span_id", span.SpanID().String()),
		zap.Int("events_count", span.Events().Len()))

	activeSpan, exists := p.activeSpans[spanKey]
	if !exists {
		p.logger.Warn("🔴 WARN: Received LOG event for unknown span - no matching START found",
			zap.String("trace_id", span.TraceID().String()),
			zap.String("span_id", span.SpanID().String()),
			zap.Int("available_active_spans", len(p.activeSpans)))
		return
	}

	// Add events to the active span
	originalEventCount := activeSpan.span.Events().Len()
	for i := 0; i < span.Events().Len(); i++ {
		event := span.Events().At(i)
		outputEvent := activeSpan.span.Events().AppendEmpty()
		event.CopyTo(outputEvent)
	}

	activeSpan.lastSeen = time.Now()

	p.logger.Info("🟢 INFO: Successfully added events to active span",
		zap.String("trace_id", span.TraceID().String()),
		zap.String("span_id", span.SpanID().String()),
		zap.Int("events_added", span.Events().Len()),
		zap.Int("total_events_in_span", activeSpan.span.Events().Len()),
		zap.Int("original_event_count", originalEventCount))
}

// addCompletedSpans adds completed spans to the output
func (p *spanReconstructionProcessor) addCompletedSpans(outputTraces ptrace.Traces) int {
	p.spansMutex.Lock()
	defer p.spansMutex.Unlock()

	// Group completed spans by resource and scope
	completedSpans := make(map[string]map[string][]*activeSpan)

	completedCount := 0

	for _, span := range p.activeSpans {
		// Check if span is complete (has end time)
		if span.span.EndTimestamp() != 0 {
			resourceKey := p.getResourceKey(span.resource)
			scopeKey := p.getScopeKey(span.scope)

			if completedSpans[resourceKey] == nil {
				completedSpans[resourceKey] = make(map[string][]*activeSpan)
			}
			if completedSpans[resourceKey][scopeKey] == nil {
				completedSpans[resourceKey][scopeKey] = make([]*activeSpan, 0)
			}

			completedSpans[resourceKey][scopeKey] = append(completedSpans[resourceKey][scopeKey], span)
		}
	}

	// Add completed spans to output
	for resourceKey, scopeMap := range completedSpans {
		for scopeKey, spans := range scopeMap {
			// Find or create resource in output
			var outputResource ptrace.ResourceSpans
			for i := 0; i < outputTraces.ResourceSpans().Len(); i++ {
				if p.getResourceKey(outputTraces.ResourceSpans().At(i)) == resourceKey {
					outputResource = outputTraces.ResourceSpans().At(i)
					break
				}
			}
			if outputResource == (ptrace.ResourceSpans{}) {
				outputResource = outputTraces.ResourceSpans().AppendEmpty()
			}

			// Find or create scope in output
			var outputScope ptrace.ScopeSpans
			for i := 0; i < outputResource.ScopeSpans().Len(); i++ {
				if p.getScopeKey(outputResource.ScopeSpans().At(i)) == scopeKey {
					outputScope = outputResource.ScopeSpans().At(i)
					break
				}
			}
			if outputScope == (ptrace.ScopeSpans{}) {
				outputScope = outputResource.ScopeSpans().AppendEmpty()
			}

			// Add spans
			for _, span := range spans {
				outputSpan := outputScope.Spans().AppendEmpty()
				span.span.CopyTo(outputSpan)
			}

			completedCount += len(spans)
		}
	}

	// Remove completed spans from active spans
	for key, span := range p.activeSpans {
		if span.span.EndTimestamp() != 0 {
			delete(p.activeSpans, key)
		}
	}

	return completedCount
}

// cleanupRoutine periodically cleans up expired spans
func (p *spanReconstructionProcessor) cleanupRoutine() {
	for {
		select {
		case <-p.cleanupTicker.C:
			p.cleanupExpiredSpans()
		case <-p.stopCleanup:
			return
		}
	}
}

// cleanupExpiredSpans removes spans that have exceeded their TTL
func (p *spanReconstructionProcessor) cleanupExpiredSpans() {
	p.spansMutex.Lock()
	defer p.spansMutex.Unlock()

	now := time.Now()
	expiredCount := 0

	for key, span := range p.activeSpans {
		if now.Sub(span.lastSeen) > p.config.SpanTTL {
			delete(p.activeSpans, key)
			expiredCount++
			p.logger.Debug("🟠 DEBUG: Evicted expired span",
				zap.String("trace_id", span.span.TraceID().String()),
				zap.String("span_id", span.span.SpanID().String()))
		}
	}

	if expiredCount > 0 {
		p.logger.Info("🟢 INFO: Evicted expired spans", zap.Int("count", expiredCount))
	}
}

// evictOldestSpan removes the oldest span when at capacity
func (p *spanReconstructionProcessor) evictOldestSpan() {
	var oldestKey string
	var oldestTime time.Time

	for key, span := range p.activeSpans {
		if oldestKey == "" || span.lastSeen.Before(oldestTime) {
			oldestKey = key
			oldestTime = span.lastSeen
		}
	}

	if oldestKey != "" {
		span := p.activeSpans[oldestKey]
		delete(p.activeSpans, oldestKey)
		p.logger.Debug("🟠 DEBUG: Evicted oldest span due to capacity limit",
			zap.String("trace_id", span.span.TraceID().String()),
			zap.String("span_id", span.span.SpanID().String()))
	}
}

// getSpanKey creates a unique key for a span
func (p *spanReconstructionProcessor) getSpanKey(span ptrace.Span) string {
	return span.TraceID().String() + ":" + span.SpanID().String()
}

// getResourceKey creates a unique key for a resource
func (p *spanReconstructionProcessor) getResourceKey(resource ptrace.ResourceSpans) string {
	// Simple hash of resource attributes
	attrs := resource.Resource().Attributes().AsRaw()
	if serviceName, ok := attrs["service.name"].(string); ok {
		return serviceName
	}
	return "unknown"
}

// getScopeKey generates a unique key for a scope
func (p *spanReconstructionProcessor) getScopeKey(scope ptrace.ScopeSpans) string {
	return scope.Scope().Name() + ":" + scope.Scope().Version()
}

// countTotalSpans counts the total number of spans in a traces object
func (p *spanReconstructionProcessor) countTotalSpans(td ptrace.Traces) int {
	total := 0
	for i := 0; i < td.ResourceSpans().Len(); i++ {
		resourceSpans := td.ResourceSpans().At(i)
		for j := 0; j < resourceSpans.ScopeSpans().Len(); j++ {
			scopeSpans := resourceSpans.ScopeSpans().At(j)
			total += scopeSpans.Spans().Len()
		}
	}
	return total
}

// getServiceName extracts the service name from a resource
func (p *spanReconstructionProcessor) getServiceName(resource pcommon.Resource) string {
	if serviceName, exists := resource.Attributes().Get("service.name"); exists {
		return serviceName.Str()
	}
	return "unknown_service"
}
