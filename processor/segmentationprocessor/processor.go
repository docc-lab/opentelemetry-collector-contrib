// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package segmentationprocessor

import (
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

// ServerSpan represents a server-side span with its associated child spans
type ServerSpan struct {
	// The actual span data with context
	Span SpanWithContext
	// List of child spans that belong to this server span
	ChildSpans []SpanWithContext
	// Timestamp when this server span was first seen
	FirstSeen time.Time
}

// SpanWithContext holds a span with its resource and scope context
type SpanWithContext struct {
	Span     ptrace.Span
	Resource ptrace.ResourceSpans
	Scope    ptrace.ScopeSpans
}

// EventBuffer holds unprocessed events with their context
type EventBuffer struct {
	mu     sync.Mutex
	events []SpanWithContext
}

// segmentationProcessor is the processor that segments spans into time-based segments
type segmentationProcessor struct {
	logger       *zap.Logger
	config       *Config
	nextConsumer consumer.Traces

	// Mutex for thread-safe access to span collections
	// TODO: Will be used when implementing span processing logic
	spansMutex sync.RWMutex

	// Parent-to-children tracking for server-centric eviction
	// Maps server span key -> list of client span keys
	parentToChildren map[string][]string

	// Client-side child spans waiting for their server-side parents
	// These are spans that have arrived but their parent server span hasn't been seen yet
	waitingClientSpans map[string][]SpanWithContext

	// Server spans that have been seen and can have children
	// Maps server span key -> ServerSpan struct
	serverSpans map[string]ServerSpan

	// Timestamps for tracking when spans were added (for eviction)
	spanTimestamps map[string]time.Time

	// Counters for tracking span types
	serverSpanCount int64
	clientSpanCount int64

	// Event buffer for handling spans asynchronously
	eventBuffer *EventBuffer

	// Worker control
	stopWorker chan struct{}
	workerWg   sync.WaitGroup
}

// newSegmentationProcessor creates a new segmentation processor
func newSegmentationProcessor(logger *zap.Logger, config *Config, nextConsumer consumer.Traces) *segmentationProcessor {
	sp := &segmentationProcessor{
		logger:             logger,
		config:             config,
		nextConsumer:       nextConsumer,
		parentToChildren:   make(map[string][]string),
		waitingClientSpans: make(map[string][]SpanWithContext),
		serverSpans:        make(map[string]ServerSpan),
		spanTimestamps:     make(map[string]time.Time),
		serverSpanCount:    0,
		clientSpanCount:    0,
		eventBuffer: &EventBuffer{
			events: make([]SpanWithContext, 0),
		},
		stopWorker: make(chan struct{}),
	}

	return sp
}

// processTraces processes the incoming traces and adds them to the event buffer
func (sp *segmentationProcessor) processTraces(ctx context.Context, td ptrace.Traces) (ptrace.Traces, error) {
	sp.logger.Debug("Processing traces batch",
		zap.Int("resource_spans_count", td.ResourceSpans().Len()),
		zap.Int("total_spans", sp.countTotalSpans(td)))

	// Collect all spans with their context
	var allSpansWithContext []SpanWithContext
	for i := 0; i < td.ResourceSpans().Len(); i++ {
		resourceSpans := td.ResourceSpans().At(i)
		for j := 0; j < resourceSpans.ScopeSpans().Len(); j++ {
			scopeSpans := resourceSpans.ScopeSpans().At(j)
			for k := 0; k < scopeSpans.Spans().Len(); k++ {
				span := scopeSpans.Spans().At(k)
				allSpansWithContext = append(allSpansWithContext, SpanWithContext{
					Span:     span,
					Resource: resourceSpans,
					Scope:    scopeSpans,
				})
			}
		}
	}

	// Add all spans to event buffer in one operation
	sp.addEventsToBuffer(allSpansWithContext)

	sp.logger.Debug("Added spans to event buffer",
		zap.Int("spans_added", len(allSpansWithContext)))

	// Return empty traces - output will be handled independently by worker loop
	// return ptrace.NewTraces(), nil
	return td, nil
}

// start is called when the processor starts
func (sp *segmentationProcessor) start(ctx context.Context, host component.Host) error {
	sp.logger.Info("Starting segmentation processor")

	// Start worker goroutine
	sp.workerWg.Add(1)
	go sp.workerLoop()

	// Start periodic counter logging
	go sp.logCounters()

	return nil
}

// logCounters periodically logs the span counters
func (sp *segmentationProcessor) logCounters() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		// sp.spansMutex.RLock()
		serverCount := sp.serverSpanCount
		clientCount := sp.clientSpanCount
		waitingCount := len(sp.waitingClientSpans)
		serverSpansCount := len(sp.serverSpans)
		// sp.spansMutex.RUnlock()

		sp.logger.Info("Segmentation processor counters",
			zap.Int64("total_server_spans_processed", serverCount),
			zap.Int64("total_client_spans_processed", clientCount),
			zap.Int("waiting_client_spans", waitingCount),
			zap.Int("active_server_spans", serverSpansCount))
	}
}

// shutdown is called when the processor shuts down
func (sp *segmentationProcessor) shutdown(ctx context.Context) error {
	sp.logger.Info("Shutting down segmentation processor")

	// Stop worker
	close(sp.stopWorker)
	sp.workerWg.Wait()

	return nil
}

// workerLoop processes events from the buffer
func (sp *segmentationProcessor) workerLoop() {
	defer sp.workerWg.Done()

	for {
		select {
		case <-sp.stopWorker:
			return
		default:
			sp.processEventBuffer()
		}
	}
}

// processEventBuffer processes all events in the buffer
func (sp *segmentationProcessor) processEventBuffer() {
	// Get current events and clear buffer
	sp.eventBuffer.mu.Lock()
	currentEvents := sp.eventBuffer.events[:]
	sp.eventBuffer.events = make([]SpanWithContext, 0)
	sp.eventBuffer.mu.Unlock()

	if len(currentEvents) == 0 {
		return
	}

	sp.spansMutex.Lock()
	defer sp.spansMutex.Unlock()

	// Process each event
	for _, eventWithContext := range currentEvents {
		sp.processSpan(eventWithContext)
	}
}

// addEventsToBuffer adds multiple span events to the buffer
func (sp *segmentationProcessor) addEventsToBuffer(spansWithContext []SpanWithContext) {
	if len(spansWithContext) == 0 {
		return
	}

	sp.eventBuffer.mu.Lock()
	sp.eventBuffer.events = append(sp.eventBuffer.events, spansWithContext...)
	sp.eventBuffer.mu.Unlock()
}

// countTotalSpans counts the total number of spans in a traces object
func (sp *segmentationProcessor) countTotalSpans(td ptrace.Traces) int {
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

// getSpanKey creates a unique key for a span by combining trace ID and span ID
func (sp *segmentationProcessor) getSpanKey(span ptrace.Span) string {
	return span.TraceID().String() + ":" + span.SpanID().String()
}

// getParentSpanKey creates a unique key for a parent span from trace ID and parent span ID
func (sp *segmentationProcessor) getParentSpanKey(span ptrace.Span) string {
	if span.ParentSpanID().IsEmpty() {
		return ""
	}
	return span.TraceID().String() + ":" + span.ParentSpanID().String()
}

// isServerSpan determines if a span is a server span based on its kind
func (sp *segmentationProcessor) isServerSpan(span ptrace.Span) bool {
	return span.Kind() == ptrace.SpanKindServer
}

// isClientSpan determines if a span is a client span based on its kind
func (sp *segmentationProcessor) isClientSpan(span ptrace.Span) bool {
	return span.Kind() == ptrace.SpanKindClient
}

// getParentSpanID extracts the parent span ID from a span
func (sp *segmentationProcessor) getParentSpanID(span ptrace.Span) string {
	if span.ParentSpanID().IsEmpty() {
		return ""
	}
	return span.ParentSpanID().String()
}

// processSpan handles a single span based on its type
func (sp *segmentationProcessor) processSpan(spanWithContext SpanWithContext) {
	span := spanWithContext.Span
	spanKey := sp.getSpanKey(span)

	if sp.isServerSpan(span) {
		sp.handleServerSpan(spanWithContext, spanKey)
	} else if sp.isClientSpan(span) {
		sp.handleClientSpan(spanWithContext, spanKey)
	}
}

// handleServerSpan processes server-side spans
func (sp *segmentationProcessor) handleServerSpan(spanWithContext SpanWithContext, spanKey string) {
	span := spanWithContext.Span
	// Increment server span counter
	sp.serverSpanCount++

	// Create new ServerSpan entry
	serverSpan := ServerSpan{
		Span:       spanWithContext,
		ChildSpans: make([]SpanWithContext, 0),
		FirstSeen:  time.Now(),
	}

	// Check if there are any waiting client spans for this server span
	if waitingSpans, exists := sp.waitingClientSpans[spanKey]; exists {
		// Transfer waiting client spans to the server span's ChildSpans
		serverSpan.ChildSpans = waitingSpans

		// Clear the waiting client spans
		delete(sp.waitingClientSpans, spanKey)

		sp.logger.Info("Transferred waiting client spans to server span",
			zap.String("server_span_key", spanKey),
			zap.Int("client_spans_transferred", len(waitingSpans)))
	}

	// Store the server span
	sp.serverSpans[spanKey] = serverSpan
	sp.spanTimestamps[spanKey] = time.Now()

	sp.logger.Debug("Added server span",
		zap.String("span_key", spanKey),
		zap.String("span_name", span.Name()),
		zap.Int("child_spans_count", len(serverSpan.ChildSpans)),
		zap.Int64("total_server_spans", sp.serverSpanCount))
}

// handleClientSpan processes client-side spans
func (sp *segmentationProcessor) handleClientSpan(spanWithContext SpanWithContext, spanKey string) {
	span := spanWithContext.Span
	// Increment client span counter
	sp.clientSpanCount++

	parentSpanKey := sp.getParentSpanKey(span)

	if parentSpanKey == "" {
		sp.logger.Warn("Client span has no parent span ID",
			zap.String("client_span_key", spanKey),
			zap.String("trace_id", span.TraceID().String()))
		return
	}

	// Check if the parent server span exists
	if parentServerSpan, exists := sp.serverSpans[parentSpanKey]; exists {
		// Parent exists, add this client span to its ChildSpans
		parentServerSpan.ChildSpans = append(parentServerSpan.ChildSpans, spanWithContext)
		sp.serverSpans[parentSpanKey] = parentServerSpan // Update the map

		sp.logger.Debug("Added client span to existing parent",
			zap.String("client_span_key", spanKey),
			zap.String("parent_span_key", parentSpanKey),
			zap.String("parent_span_name", parentServerSpan.Span.Span.Name()),
			zap.Int64("total_client_spans", sp.clientSpanCount))
	} else {
		// Parent doesn't exist yet, add to waiting list
		if sp.waitingClientSpans[parentSpanKey] == nil {
			sp.waitingClientSpans[parentSpanKey] = []SpanWithContext{spanWithContext}
		} else {
			sp.waitingClientSpans[parentSpanKey] = append(sp.waitingClientSpans[parentSpanKey], spanWithContext)
		}

		sp.spanTimestamps[spanKey] = time.Now()

		sp.logger.Debug("Added client span to waiting list",
			zap.String("client_span_key", spanKey),
			zap.String("parent_span_key", parentSpanKey),
			zap.String("span_name", span.Name()),
			zap.Int64("total_client_spans", sp.clientSpanCount))
	}
}
