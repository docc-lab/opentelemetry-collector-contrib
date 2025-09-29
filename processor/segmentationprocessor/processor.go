// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package segmentationprocessor

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
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
	// Start time of this segment
	StartTime time.Time
	// End time of this segment
	EndTime time.Time
}

// SegmentDistribution tracks latency distribution for a segment using HDR Histogram
type SegmentDistribution struct {
	histogram  *hdrhistogram.Histogram
	count      int64
	sum        float64
	lastUpdate time.Time
}

// PointBasedDetector implements change detection based on percentiles
type PointBasedDetector struct {
	distributions map[string]*SegmentDistribution
	mu            sync.RWMutex
	percentile    float64
	minSamples    int64
}

// ChangeInfo contains information about detected changes
type ChangeInfo struct {
	IsChange   bool
	Confidence float64
	ChangeType string
	Details    map[string]interface{}
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
	serverSpanCount    int64
	clientSpanCount    int64
	segmentedSpanCount int64
	segmentedSpanNames []string

	// Event buffer for handling spans asynchronously
	eventBuffer *EventBuffer

	// Worker control
	stopWorker chan struct{}
	workerWg   sync.WaitGroup

	// Segmentation worker control
	stopSegmentationWorker chan struct{}
	segmentationWorkerWg   sync.WaitGroup

	// Change detection
	changeDetector *PointBasedDetector
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
		segmentedSpanCount: 0,
		segmentedSpanNames: make([]string, 0),
		eventBuffer: &EventBuffer{
			events: make([]SpanWithContext, 0),
		},
		stopWorker:             make(chan struct{}),
		stopSegmentationWorker: make(chan struct{}),
		changeDetector:         NewPointBasedDetector(config.ChangeDetectionPercentile, config.MinSamples),
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

	// Return empty traces - segment spans will be output independently by worker loop
	return ptrace.NewTraces(), nil
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

	// Start periodic histogram stats logging
	go sp.logHistogramStats()

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

		segmentedSpanNames := strings.Join(sp.segmentedSpanNames, ", ")

		sp.logger.Info("Segmentation processor counters:",
			zap.Int64("total_server_spans_processed", serverCount),
			zap.Int64("total_client_spans_processed", clientCount),
			zap.Int("waiting_client_spans", waitingCount),
			zap.Int("active_server_spans", serverSpansCount),
			zap.Int64("total_segmented_spans", sp.segmentedSpanCount),
			zap.String("segment_span_names", segmentedSpanNames),
		)
	}
}

// logHistogramStats periodically logs histogram statistics
func (sp *segmentationProcessor) logHistogramStats() {
	ticker := time.NewTicker(10 * time.Second) // Log every 10 seconds
	defer ticker.Stop()

	for range ticker.C {
		stats := sp.changeDetector.DumpStats()
		sp.logger.Info("Histogram statistics:",
			zap.Any("stats", stats))
	}
}

// DumpHistogramStats manually dumps histogram statistics (for debugging)
func (sp *segmentationProcessor) DumpHistogramStats() map[string]interface{} {
	return sp.changeDetector.DumpStats()
}

// DumpSegmentStats dumps histogram statistics for a specific segment
func (sp *segmentationProcessor) DumpSegmentStats(segmentID string) map[string]interface{} {
	sp.changeDetector.mu.RLock()
	defer sp.changeDetector.mu.RUnlock()

	if dist, exists := sp.changeDetector.distributions[segmentID]; exists {
		return dist.GetStats()
	}

	return map[string]interface{}{
		"error":      "segment not found",
		"segment_id": segmentID,
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
		sp.logger.Info("Routing to handleServerSpan",
			zap.String("span_name", span.Name()),
			zap.String("trace_id", span.TraceID().String()),
			zap.String("span_id", span.SpanID().String()))
		sp.handleServerSpan(spanWithContext, spanKey)
	} else if sp.isClientSpan(span) {
		sp.logger.Info("Routing to handleClientSpan", zap.String("span_name", span.Name()))
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

	sp.logger.Info("Entering handleServerSpan",
		zap.String("span_name", span.Name()),
		zap.String("span_key", spanKey),
		zap.String("trace_id", span.TraceID().String()),
		zap.String("span_id", span.SpanID().String()))

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
	sp.spansMutex.Lock()
	if parentServerSpan, exists := sp.serverSpans[parentSpanKey]; exists {
		// Parent exists, add this client span to its ChildSpans
		parentServerSpan.ChildSpans = append(parentServerSpan.ChildSpans, spanWithContext)
		sp.serverSpans[parentSpanKey] = parentServerSpan // Update the map
		sp.spansMutex.Unlock()

		sp.logger.Debug("Added client span to existing parent",
			zap.String("client_span_key", spanKey),
			zap.String("parent_span_key", parentSpanKey),
			zap.String("parent_span_name", parentServerSpan.Span.Span.Name()),
			zap.String("span_name", span.Name()),
			zap.Int64("total_client_spans", sp.clientSpanCount))
	} else {
		// Parent doesn't exist yet, add to waiting list
		if sp.waitingClientSpans[parentSpanKey] == nil {
			sp.waitingClientSpans[parentSpanKey] = []SpanWithContext{spanWithContext}
		} else {
			sp.waitingClientSpans[parentSpanKey] = append(sp.waitingClientSpans[parentSpanKey], spanWithContext)
		}

		sp.spanTimestamps[spanKey] = time.Now()
		sp.spansMutex.Unlock()

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
		// sp.segmentedSpanCount++
		sp.logger.Info("Creating segments for server span",
			zap.String("span_name", serverSpan.Span.Span.Name()),
			zap.String("trace_id", serverSpan.Span.Span.TraceID().String()),
			zap.String("span_id", serverSpan.Span.Span.SpanID().String()))

		segments := createSegments(serverSpan)

		sp.logger.Info("Generated segments",
			zap.String("span_name", serverSpan.Span.Span.Name()),
			zap.Int("segment_count", len(segments)))

		// Log the segments for debugging
		sp.logger.Info("Created segments for server span",
			zap.String("span_name", serverSpan.Span.Span.Name()),
			zap.Int("segment_count", len(segments)))

		// Create and output segment spans
		segmentSpans := make([]ptrace.Span, 0, len(segments))
		for _, segment := range segments {
			latency := segment.LatencyMs()

			// Check if segment is "bad"
			isBad := sp.isSegmentBad(segment, latency)

			sp.logger.Info("Segment analysis",
				zap.String("segment_id", segment.String()),
				zap.Float64("latency_ms", latency),
				zap.Bool("is_bad", isBad))

			// Create segment span
			segmentSpan := sp.createSegmentSpan(segment, serverSpan, isBad, latency)
			segmentSpans = append(segmentSpans, segmentSpan)
		}

		// Output segment spans to next consumer
		if len(segmentSpans) > 0 {
			sp.outputSegmentSpans(segmentSpans, serverSpan)
		}

		sp.segmentedSpanCount++
		sp.segmentedSpanNames = append(sp.segmentedSpanNames, serverSpan.Span.Span.Name())
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
	serviceOperation := serviceName + "__" + serverSpanName

	// Get server span timing
	serverStartTime := serverSpanData.StartTimestamp().AsTime()
	serverEndTime := serverSpanData.EndTimestamp().AsTime()

	// If no client spans, the entire server span is one on-service segment
	if len(serverSpan.ChildSpans) == 0 {
		segment := Segment{
			ServiceOperation: serviceOperation,
			StartEndpoint:    serverSpanName + "_START",
			EndEndpoint:      serverSpanName + "_END",
			StartTime:        serverStartTime,
			EndTime:          serverEndTime,
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

	// Add server start event first (will be at index 0)
	events = append(events, TimelineEvent{
		Time:     serverStartTime,
		IsStart:  true,
		IsServer: true,
		Name:     serverSpanName + "_START",
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
			Name:     clientSpan.Name() + "_START",
		})
		events = append(events, TimelineEvent{
			Time:     clientEndTime,
			IsStart:  false,
			IsServer: false,
			Name:     clientSpan.Name() + "_END",
		})
	}

	// Sort only the client events (events[1:] to skip server start event)
	sort.Slice(events[1:], func(i, j int) bool {
		return events[i+1].Time.Before(events[j+1].Time)
	})

	// Append server end event (ensures it comes last)
	events = append(events, TimelineEvent{
		Time:     serverEndTime,
		IsStart:  false,
		IsServer: true,
		Name:     serverSpanName + "_END",
	})

	// Track active client spans and segment creation
	activeClientSpans := 0
	currentSegmentStart := serverStartTime
	isOnService := true
	previousEvent := events[0] // Start with server start event

	// Process events to create segments
	for _, event := range events[1:] {
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
			// Only create segments for on-service periods
			if wasOnService {
				// Determine start and end endpoints
				var startEndpoint, endEndpoint string

				if currentSegmentStart.Equal(serverStartTime) {
					// First segment starts with server start event
					startEndpoint = events[0].Name
				} else {
					// Use the previous event as the start endpoint
					startEndpoint = previousEvent.Name
				}

				// End endpoint is always the current event name
				endEndpoint = event.Name

				segment := Segment{
					ServiceOperation: serviceOperation,
					StartEndpoint:    startEndpoint,
					EndEndpoint:      endEndpoint,
					StartTime:        currentSegmentStart,
					EndTime:          event.Time,
				}
				segments = append(segments, segment)
			}

			currentSegmentStart = event.Time
		}

		// Update previous event for next iteration
		previousEvent = event
	}

	return segments
}

// NewSegmentDistribution creates a new segment distribution with HDR histogram
func NewSegmentDistribution() *SegmentDistribution {
	// HDR histogram: 1 microsecond to 1 hour, 3 significant digits
	histogram := hdrhistogram.New(1, 3600000000, 3) // 1μs to 1h in microseconds
	return &SegmentDistribution{
		histogram:  histogram,
		count:      0,
		sum:        0,
		lastUpdate: time.Now(),
	}
}

// Add adds a latency value to the distribution (latency in milliseconds)
func (sd *SegmentDistribution) Add(latencyMs float64) {
	// Convert milliseconds to microseconds for HDR histogram
	latencyMicros := int64(latencyMs * 1000)
	sd.histogram.RecordValue(latencyMicros)
	sd.count++
	sd.sum += latencyMs
	sd.lastUpdate = time.Now()
}

// Percentile returns the percentile value in milliseconds
func (sd *SegmentDistribution) Percentile(percentile float64) float64 {
	if sd.count == 0 {
		return 0
	}
	// Get percentile from HDR histogram (returns microseconds)
	valueMicros := sd.histogram.ValueAtQuantile(percentile * 100)
	// Convert back to milliseconds
	return float64(valueMicros) / 1000.0
}

// GetStats returns histogram statistics for debugging
func (sd *SegmentDistribution) GetStats() map[string]interface{} {
	if sd.count == 0 {
		return map[string]interface{}{
			"count": 0,
		}
	}

	return map[string]interface{}{
		"count":       sd.count,
		"sum_ms":      sd.sum,
		"mean_ms":     sd.sum / float64(sd.count),
		"min_ms":      float64(sd.histogram.Min()) / 1000.0,
		"max_ms":      float64(sd.histogram.Max()) / 1000.0,
		"p50_ms":      float64(sd.histogram.ValueAtQuantile(50)) / 1000.0,
		"p95_ms":      float64(sd.histogram.ValueAtQuantile(95)) / 1000.0,
		"p99_ms":      float64(sd.histogram.ValueAtQuantile(99)) / 1000.0,
		"p99_9_ms":    float64(sd.histogram.ValueAtQuantile(99.9)) / 1000.0,
		"std_dev_ms":  float64(sd.histogram.StdDev()) / 1000.0,
		"last_update": sd.lastUpdate,
	}
}

// NewPointBasedDetector creates a new point-based change detector
func NewPointBasedDetector(percentile float64, minSamples int64) *PointBasedDetector {
	return &PointBasedDetector{
		distributions: make(map[string]*SegmentDistribution),
		percentile:    percentile,
		minSamples:    minSamples,
	}
}

// DetectChange checks if a latency value represents a change
func (d *PointBasedDetector) DetectChange(segmentID string, latency float64) (bool, ChangeInfo) {
	d.mu.RLock()
	dist, exists := d.distributions[segmentID]
	percentile := d.percentile
	d.mu.RUnlock()

	if !exists || dist.count < d.minSamples {
		return false, ChangeInfo{IsChange: false}
	}

	// Get historical percentile
	historicalPercentile := dist.Percentile(percentile)

	// Check if current latency exceeds historical percentile
	isChange := latency > historicalPercentile

	return isChange, ChangeInfo{
		IsChange:   isChange,
		Confidence: 0.8,
		ChangeType: "percentile",
		Details: map[string]interface{}{
			"current_latency":       latency,
			"historical_percentile": historicalPercentile,
			"percentile":            percentile,
		},
	}
}

// UpdateDistribution adds a latency value to the distribution
func (d *PointBasedDetector) UpdateDistribution(segmentID string, latency float64) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if dist, exists := d.distributions[segmentID]; exists {
		dist.Add(latency)
	} else {
		dist = NewSegmentDistribution()
		dist.Add(latency)
		d.distributions[segmentID] = dist
	}
}

// DumpStats returns statistics for all segment distributions
func (d *PointBasedDetector) DumpStats() map[string]interface{} {
	d.mu.RLock()
	defer d.mu.RUnlock()

	allStats := make(map[string]interface{})
	for segmentID, dist := range d.distributions {
		allStats[segmentID] = dist.GetStats()
	}

	return map[string]interface{}{
		"total_segments": len(d.distributions),
		"percentile":     d.percentile,
		"min_samples":    d.minSamples,
		"segments":       allStats,
	}
}

// isSegmentBad checks if a segment latency is "bad" based on change detection
func (sp *segmentationProcessor) isSegmentBad(segment Segment, latency float64) bool {
	segmentID := segment.String()

	// Check for change
	isChange, info := sp.changeDetector.DetectChange(segmentID, latency)

	if isChange {
		sp.logger.Warn("Segment latency change detected",
			zap.String("segment_id", segmentID),
			zap.Float64("latency", latency),
			zap.Any("change_info", info))
	}

	// Update distribution
	sp.changeDetector.UpdateDistribution(segmentID, latency)

	return isChange
}

func (seg Segment) String() string {
	return fmt.Sprintf(
		"%s:%s:%s",
		seg.ServiceOperation,
		seg.StartEndpoint,
		seg.EndEndpoint,
	)
}

// LatencyMs calculates the latency of this segment in milliseconds
func (seg Segment) LatencyMs() float64 {
	return float64(seg.EndTime.Sub(seg.StartTime).Nanoseconds()) / 1e6
}

// createSegmentSpan creates a ptrace.Span from a segment
func (sp *segmentationProcessor) createSegmentSpan(segment Segment, serverSpan ServerSpan, isBad bool, latency float64) ptrace.Span {
	// Create new span
	segmentSpan := ptrace.NewSpan()

	// Set basic span properties - inherit trace ID and span ID from server span
	segmentSpan.SetTraceID(serverSpan.Span.Span.TraceID())
	segmentSpan.SetSpanID(serverSpan.Span.Span.SpanID())             // Inherit server span ID
	segmentSpan.SetParentSpanID(serverSpan.Span.Span.ParentSpanID()) // Inherit server span's parent
	segmentSpan.SetName(segment.String())

	// Use actual segment start and end times
	segmentSpan.SetStartTimestamp(pcommon.NewTimestampFromTime(segment.StartTime))
	segmentSpan.SetEndTimestamp(pcommon.NewTimestampFromTime(segment.EndTime))

	// Generate random 4-byte identifier for this segment
	randomBytes := make([]byte, 4)
	_, err := rand.Read(randomBytes)
	if err != nil {
		sp.logger.Warn("Failed to generate random bytes for segment", zap.Error(err))
		// Fallback to zero bytes if random generation fails
		randomBytes = []byte{0, 0, 0, 0}
	}
	segmentID := hex.EncodeToString(randomBytes)

	// Add segment attributes
	segmentSpan.Attributes().PutStr("service_operation", segment.ServiceOperation)
	segmentSpan.Attributes().PutStr("start_endpoint", segment.StartEndpoint)
	segmentSpan.Attributes().PutStr("end_endpoint", segment.EndEndpoint)
	segmentSpan.Attributes().PutBool("is_bad", isBad)
	segmentSpan.Attributes().PutDouble("latency_ms", latency)
	segmentSpan.Attributes().PutStr("segment_id", segmentID)

	// Copy upstream.ip attribute from the original server span if it exists
	if upstreamIP, exists := serverSpan.Span.Span.Attributes().AsRaw()["upstream.ip"]; exists {
		if upstreamIPStr, ok := upstreamIP.(string); ok {
			segmentSpan.Attributes().PutStr("upstream.ip", upstreamIPStr)
		}
	}

	return segmentSpan
}

// outputSegmentSpans outputs segment spans to the next consumer
func (sp *segmentationProcessor) outputSegmentSpans(segmentSpans []ptrace.Span, serverSpan ServerSpan) {
	if sp.nextConsumer == nil {
		return
	}

	// Create output traces with segment spans
	outputTraces := ptrace.NewTraces()
	resourceSpans := outputTraces.ResourceSpans().AppendEmpty()
	scopeSpans := resourceSpans.ScopeSpans().AppendEmpty()

	// Copy resource attributes from original server span
	serverSpan.Span.Resource.Resource().Attributes().CopyTo(resourceSpans.Resource().Attributes())

	// Copy scope information from original server span
	serverSpan.Span.Scope.Scope().CopyTo(scopeSpans.Scope())

	// Add segment spans
	for _, segmentSpan := range segmentSpans {
		outputSpan := scopeSpans.Spans().AppendEmpty()
		segmentSpan.CopyTo(outputSpan)
	}

	// Send to next consumer
	ctx := context.Background()
	if err := sp.nextConsumer.ConsumeTraces(ctx, outputTraces); err != nil {
		sp.logger.Error("Failed to output segment spans",
			zap.Error(err),
			zap.Int("segment_count", len(segmentSpans)))
	} else {
		sp.logger.Debug("Successfully output segment spans",
			zap.Int("segment_count", len(segmentSpans)))
	}
}
