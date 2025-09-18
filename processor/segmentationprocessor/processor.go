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

// Segment represents a time-based segment of work within a server span
type Segment struct {
	// Service name + span operation name (e.g., "user-service:process-payment")
	ServiceOperation string
	// Starting endpoint (e.g., "process-payment:START" or "process-payment:END")
	StartEndpoint string
	// Ending endpoint (e.g., "process-payment:START" or "process-payment:END")
	EndEndpoint string
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

	// Segmentation worker control
	stopSegmentationWorker chan struct{}
	segmentationWorkerWg   sync.WaitGroup
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
		stopWorker:             make(chan struct{}),
		stopSegmentationWorker: make(chan struct{}),
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

	// Start segmentation worker goroutine
	sp.segmentationWorkerWg.Add(1)
	go sp.segmentationWorkerLoop()

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

		sp.logger.Info("Segmentation processor counters:",
			zap.Int64("total_server_spans_processed", serverCount),
			zap.Int64("total_client_spans_processed", clientCount),
			zap.Int("waiting_client_spans", waitingCount),
			zap.Int("active_server_spans", serverSpansCount))
	}
}

// shutdown is called when the processor shuts down
func (sp *segmentationProcessor) shutdown(ctx context.Context) error {
	sp.logger.Info("Shutting down segmentation processor")

	// Stop workers
	close(sp.stopWorker)
	sp.workerWg.Wait()

	close(sp.stopSegmentationWorker)
	sp.segmentationWorkerWg.Wait()

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

	sp.logger.Info("Processing event buffer", zap.Int("event_count", len(currentEvents)))

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

	// Log span details for debugging
	sp.logger.Info("Processing span",
		zap.String("span_name", span.Name()),
		zap.String("span_kind", span.Kind().String()),
		zap.String("trace_id", span.TraceID().String()),
		zap.String("span_id", span.SpanID().String()),
		zap.Bool("is_server", sp.isServerSpan(span)),
		zap.Bool("is_client", sp.isClientSpan(span)))

	if sp.isServerSpan(span) {
		sp.handleServerSpan(spanWithContext, spanKey)
	} else if sp.isClientSpan(span) {
		sp.handleClientSpan(spanWithContext, spanKey)
	} else {
		sp.logger.Info("Span not classified as server or client",
			zap.String("span_name", span.Name()),
			zap.String("span_kind", span.Kind().String()))
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
	sp.spansMutex.Lock()
	sp.serverSpans[spanKey] = serverSpan
	sp.spanTimestamps[spanKey] = time.Now()
	sp.spansMutex.Unlock()

	sp.logger.Info("Added server span",
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

// segmentationWorkerLoop processes completed server spans for segmentation
func (sp *segmentationProcessor) segmentationWorkerLoop() {
	defer sp.segmentationWorkerWg.Done()

	sp.logger.Info("Starting segmentation worker loop")
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-sp.stopSegmentationWorker:
			sp.logger.Info("Stopping segmentation worker loop")
			return
		case <-ticker.C:
			sp.segmentSpans()
		}
	}
}

// segmentSpans processes completed server spans and creates segments
func (sp *segmentationProcessor) segmentSpans() {
	// Get current timestamp
	currentTime := time.Now()

	// Lock around the serverSpans data structure
	sp.spansMutex.Lock()

	// Create temporary structure for spans to segment
	toSegment := make([]ServerSpan, 0)

	// Iterate through all server spans
	for spanKey, serverSpan := range sp.serverSpans {
		// Check if the server span's last time seen is more than 100ms before current timestamp
		lastSeenTime := sp.spanTimestamps[spanKey]
		if currentTime.Sub(lastSeenTime) >= 100*time.Millisecond {
			// Add to segmentation queue
			toSegment = append(toSegment, serverSpan)

			// Remove from serverSpans data structure
			delete(sp.serverSpans, spanKey)
			delete(sp.spanTimestamps, spanKey)
		}
	}

	// Unlock around the serverSpans structure
	sp.spansMutex.Unlock()

	// Log if we have spans to segment
	if len(toSegment) > 0 {
		sp.logger.Info("Processing spans for segmentation",
			zap.Int("spans_to_segment", len(toSegment)))
	}

	// Process each entry in toSegment
	for _, serverSpan := range toSegment {
		segments := createSegments(serverSpan)

		// Log the segments for debugging
		sp.logger.Info("Created segments for server span",
			zap.String("span_name", serverSpan.Span.Span.Name()),
			zap.Int("segment_count", len(segments)))

		// TODO: Process or emit the segments as needed
		_ = segments // Placeholder to avoid unused variable warning
	}
}

// createSegments analyzes a completed ServerSpan and creates segments representing
// on-service and off-service work periods
func createSegments(serverSpan ServerSpan) []Segment {
	segments := make([]Segment, 0)

	// Get server span details
	serverSpanData := serverSpan.Span.Span
	serverSpanName := serverSpanData.Name()

	// Extract service name from resource attributes
	attrs := serverSpan.Span.Resource.Resource().Attributes().AsRaw()
	serviceName := "unknown-service"
	if serviceNameValue, ok := attrs["service.name"].(string); ok {
		serviceName = serviceNameValue
	}

	// Create service operation identifier
	serviceOperation := serviceName + ":" + serverSpanName

	// Get server span timing
	serverStartTime := serverSpanData.StartTimestamp().AsTime()
	serverEndTime := serverSpanData.EndTimestamp().AsTime()

	// If no client spans, the entire server span is one on-service segment
	if len(serverSpan.ChildSpans) == 0 {
		segment := Segment{
			ServiceOperation: serviceOperation,
			StartEndpoint:    serverSpanName + ":START",
			EndEndpoint:      serverSpanName + ":END",
		}
		segments = append(segments, segment)
		return segments
	}

	// Create timeline events for server span and all client spans
	type TimelineEvent struct {
		Time     time.Time
		IsStart  bool
		IsServer bool
		Name     string
	}

	events := make([]TimelineEvent, 0)

	// Add server span events
	events = append(events, TimelineEvent{
		Time:     serverStartTime,
		IsStart:  true,
		IsServer: true,
		Name:     serverSpanName,
	})
	events = append(events, TimelineEvent{
		Time:     serverEndTime,
		IsStart:  false,
		IsServer: true,
		Name:     serverSpanName,
	})

	// Add client span events
	for _, childSpan := range serverSpan.ChildSpans {
		clientSpan := childSpan.Span
		clientStartTime := clientSpan.StartTimestamp().AsTime()
		clientEndTime := clientSpan.EndTimestamp().AsTime()

		events = append(events, TimelineEvent{
			Time:     clientStartTime,
			IsStart:  true,
			IsServer: false,
			Name:     clientSpan.Name(),
		})
		events = append(events, TimelineEvent{
			Time:     clientEndTime,
			IsStart:  false,
			IsServer: false,
			Name:     clientSpan.Name(),
		})
	}

	// Sort events by time
	for i := 0; i < len(events)-1; i++ {
		for j := i + 1; j < len(events); j++ {
			if events[i].Time.After(events[j].Time) {
				events[i], events[j] = events[j], events[i]
			}
		}
	}

	// Track active client spans
	activeClientSpans := 0
	currentSegmentStart := serverStartTime
	isOnService := true

	// Process events to create segments
	for _, event := range events {
		// Skip events outside server span bounds
		if event.Time.Before(serverStartTime) || event.Time.After(serverEndTime) {
			continue
		}

		// Update active client span count
		if !event.IsServer {
			if event.IsStart {
				activeClientSpans++
			} else {
				activeClientSpans--
			}
		}

		// Determine if we're transitioning between on-service and off-service
		wasOnService := isOnService
		isOnService = (activeClientSpans == 0)

		// If we're transitioning or this is the server end event, create a segment
		if wasOnService != isOnService || (!event.IsStart && event.IsServer) {
			segmentType := "ON-SERVICE"
			if !wasOnService {
				segmentType = "OFF-SERVICE"
			}

			startEndpoint := serverSpanName + ":START"
			if currentSegmentStart != serverStartTime {
				startEndpoint = serverSpanName + ":END"
			}

			endEndpoint := serverSpanName + ":END"
			if event.Time != serverEndTime {
				endEndpoint = serverSpanName + ":START"
			}

			segment := Segment{
				ServiceOperation: serviceOperation + ":" + segmentType,
				StartEndpoint:    startEndpoint,
				EndEndpoint:      endEndpoint,
			}
			segments = append(segments, segment)

			currentSegmentStart = event.Time
		}
	}

	return segments
}
