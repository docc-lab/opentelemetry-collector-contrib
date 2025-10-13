// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package overlapdetectionprocessor

import (
	"context"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

// SpanBuffer holds unprocessed spans
type SpanBuffer struct {
	mu    sync.Mutex
	spans []ptrace.Span
}

// SegmentSpanWithTime holds a segment span with its timestamp
type SegmentSpanWithTime struct {
	Span    ptrace.Span
	AddedAt time.Time
}

// BadSegmentOverlapInfo tracks a bad segment and its overlapping segments
type BadSegmentOverlapInfo struct {
	BadSegmentTraceID       string
	OverlappingTraceIDs     []string
	BadSegmentUpstreamIP    string
	OverlappingUpstreamIPs  []string
	BadSegmentServiceName   string
	OverlappingServiceNames []string
	DetectedAt              time.Time
}

// overlapDetectionProcessor is a processor that detects overlaps between segment spans.
type overlapDetectionProcessor struct {
	logger       *zap.Logger
	config       *Config
	nextConsumer consumer.Traces

	// Event buffer for handling spans asynchronously
	spanBuffer *SpanBuffer

	// Map from spanID+segmentID -> SegmentSpanWithTime for segment spans
	segmentSpans map[string]SegmentSpanWithTime

	// Map from segmentID -> timestamp for bad segments only
	badSegments map[string]time.Time

	// Mutex for thread-safe access to segmentSpans and badSegments
	segmentsMutex sync.RWMutex

	// Counters for tracking
	badSegmentCount  int64
	overlappingCount int64
	countersMutex    sync.RWMutex

	// List of bad segment overlap information for enhanced logging
	badSegmentOverlaps []BadSegmentOverlapInfo
	overlapsMutex      sync.RWMutex

	// Worker control
	stopWorker           chan struct{}
	stopBadSegmentWorker chan struct{}
	stopCounterWorker    chan struct{}
	workerWg             sync.WaitGroup
	badSegmentWorkerWg   sync.WaitGroup
	counterWorkerWg      sync.WaitGroup
}

func newOverlapDetectionProcessor(logger *zap.Logger, config *Config, nextConsumer consumer.Traces) *overlapDetectionProcessor {
	return &overlapDetectionProcessor{
		logger:       logger,
		config:       config,
		nextConsumer: nextConsumer,
		spanBuffer: &SpanBuffer{
			spans: make([]ptrace.Span, 0),
		},
		segmentSpans:         make(map[string]SegmentSpanWithTime),
		badSegments:          make(map[string]time.Time),
		badSegmentCount:      0,
		overlappingCount:     0,
		badSegmentOverlaps:   make([]BadSegmentOverlapInfo, 0),
		stopWorker:           make(chan struct{}),
		stopBadSegmentWorker: make(chan struct{}),
		stopCounterWorker:    make(chan struct{}),
	}
}

func (o *overlapDetectionProcessor) processTraces(ctx context.Context, td ptrace.Traces) (ptrace.Traces, error) {
	o.logger.Debug("Processing traces for overlap detection", zap.Int("resource_spans_count", td.ResourceSpans().Len()))

	// Collect all spans
	var allSpans []ptrace.Span
	for i := 0; i < td.ResourceSpans().Len(); i++ {
		resourceSpans := td.ResourceSpans().At(i)
		for j := 0; j < resourceSpans.ScopeSpans().Len(); j++ {
			scopeSpans := resourceSpans.ScopeSpans().At(j)
			for k := 0; k < scopeSpans.Spans().Len(); k++ {
				span := scopeSpans.Spans().At(k)
				allSpans = append(allSpans, span)
			}
		}
	}

	// Add all spans to span buffer in one operation
	o.addSpansToBuffer(allSpans)

	o.logger.Debug("Added spans to span buffer", zap.Int("spans_added", len(allSpans)))

	// Return empty traces - processed spans will be output independently by worker loop
	return ptrace.NewTraces(), nil
}

// workerLoop processes events from the buffer
func (o *overlapDetectionProcessor) workerLoop() {
	defer o.workerWg.Done()

	for {
		select {
		case <-o.stopWorker:
			return
		default:
			o.processSpanBuffer()
			// Add small delay between worker loop iterations to reduce CPU usage
			time.Sleep(1 * time.Millisecond)
		}
	}
}

// processSpanBuffer processes all spans in the buffer
func (o *overlapDetectionProcessor) processSpanBuffer() {
	// Get current spans and clear buffer
	o.spanBuffer.mu.Lock()
	currentSpans := o.spanBuffer.spans[:]
	o.spanBuffer.spans = make([]ptrace.Span, 0)
	o.spanBuffer.mu.Unlock()

	if len(currentSpans) == 0 {
		return
	}

	o.logger.Info("Processing span buffer", zap.Int("span_count", len(currentSpans)))

	curTime := time.Now()

	// Process each span and add to segmentSpans map
	o.segmentsMutex.Lock()
	for _, span := range currentSpans {
		// Check if this is a segment span by looking for segment_id attribute
		segmentID, exists := span.Attributes().AsRaw()["segment_id"]
		if !exists {
			// Not a segment span, skip
			continue
		}

		segmentIDStr, ok := segmentID.(string)
		if !ok || segmentIDStr == "" {
			// Invalid segment_id, skip
			continue
		}

		// Create key: spanID + segmentID
		spanID := span.SpanID().String()
		key := spanID + ":" + segmentIDStr

		// Store in segmentSpans map with timestamp
		o.segmentSpans[key] = SegmentSpanWithTime{
			Span:    span,
			AddedAt: curTime,
		}

		// Check if this is a bad segment and add to badSegments map
		if isBad, exists := span.Attributes().AsRaw()["is_bad"]; exists {
			if isBadBool, ok := isBad.(bool); ok && isBadBool {
				o.badSegments[segmentIDStr] = curTime

				// Increment bad segment counter
				o.countersMutex.Lock()
				o.badSegmentCount++
				o.countersMutex.Unlock()

				o.logger.Debug("Added bad segment to badSegments map",
					zap.String("segment_id", segmentIDStr),
					zap.String("span_name", span.Name()))
			}
		}

		o.logger.Debug("Added segment span to map",
			zap.String("key", key),
			zap.String("span_name", span.Name()),
			zap.String("trace_id", span.TraceID().String()),
			zap.Time("added_at", curTime))
	}
	o.segmentsMutex.Unlock()
}

// addSpansToBuffer adds multiple spans to the buffer
func (o *overlapDetectionProcessor) addSpansToBuffer(spans []ptrace.Span) {
	if len(spans) == 0 {
		return
	}

	o.spanBuffer.mu.Lock()
	o.spanBuffer.spans = append(o.spanBuffer.spans, spans...)
	o.spanBuffer.mu.Unlock()
}

// badSegmentWorkerLoop processes bad segments after they've been in the map for a threshold time
func (o *overlapDetectionProcessor) badSegmentWorkerLoop() {
	defer o.badSegmentWorkerWg.Done()

	ticker := time.NewTicker(100 * time.Millisecond) // Check every 100ms
	defer ticker.Stop()

	for {
		select {
		case <-o.stopBadSegmentWorker:
			return
		case <-ticker.C:
			o.processBadSegments()
		}
	}
}

// processBadSegments processes bad segments that have been in the map for a threshold time
func (o *overlapDetectionProcessor) processBadSegments() {
	o.segmentsMutex.Lock()
	defer o.segmentsMutex.Unlock()

	currentTime := time.Now()
	threshold := 200 * time.Millisecond // Process segments after 200ms

	var segmentsToProcess []string

	// Find bad segments that are old enough to process
	for segmentID, addedTime := range o.badSegments {
		if currentTime.Sub(addedTime) >= threshold {
			segmentsToProcess = append(segmentsToProcess, segmentID)
		}
	}

	if len(segmentsToProcess) == 0 {
		return
	}

	o.logger.Info("Processing bad segments", zap.Int("count", len(segmentsToProcess)))

	// Collect all overlapping segments from all bad segments
	var allOverlappingSegments []ptrace.Span

	// Process each bad segment
	for _, segmentID := range segmentsToProcess {
		overlappingSegments := o.processBadSegment(segmentID)
		if len(overlappingSegments) > 0 {
			allOverlappingSegments = append(allOverlappingSegments, overlappingSegments...)
		}
		// Remove from badSegments map after processing
		delete(o.badSegments, segmentID)
	}

	// Emit all overlapping segments in one batch
	if len(allOverlappingSegments) > 0 {
		o.emitOverlappingSegments(allOverlappingSegments)
	}
}

// processBadSegment finds overlapping segments for a given bad segment
func (o *overlapDetectionProcessor) processBadSegment(badSegmentID string) []ptrace.Span {
	// Find the bad segment in segmentSpans
	var badSegment ptrace.Span
	var badSegmentKey string
	found := false

	for key, segmentWithTime := range o.segmentSpans {
		if segmentID, exists := segmentWithTime.Span.Attributes().AsRaw()["segment_id"]; exists {
			if segmentIDStr, ok := segmentID.(string); ok && segmentIDStr == badSegmentID {
				badSegment = segmentWithTime.Span
				badSegmentKey = key
				found = true
				break
			}
		}
	}

	if !found {
		o.logger.Warn("Bad segment not found in segmentSpans", zap.String("segment_id", badSegmentID))
		return nil
	}

	// Get service name and upstream IP for filtering
	badSegmentService := o.getServiceName(badSegment)
	badSegmentUpstreamIP := o.extractUpstreamIP(badSegment)
	badSegmentStart := badSegment.StartTimestamp().AsTime()
	badSegmentEnd := badSegment.EndTimestamp().AsTime()

	o.logger.Info("Processing bad segment for overlaps",
		zap.String("segment_id", badSegmentID),
		zap.String("service", badSegmentService),
		zap.String("upstream_ip", badSegmentUpstreamIP),
		zap.Time("start", badSegmentStart),
		zap.Time("end", badSegmentEnd))

	// Find overlapping segments within the same service
	var overlappingSegments []ptrace.Span
	var overlappingServiceNames []string
	for key, segmentWithTime := range o.segmentSpans {
		if key == badSegmentKey {
			continue // Skip the bad segment itself
		}

		segment := segmentWithTime.Span
		segmentService := o.getServiceName(segment)

		// Only check segments from the same service
		if segmentService != badSegmentService {
			continue
		}

		segmentStart := segment.StartTimestamp().AsTime()
		segmentEnd := segment.EndTimestamp().AsTime()

		// Check for temporal overlap
		if o.segmentsOverlap(badSegmentStart, badSegmentEnd, segmentStart, segmentEnd) {
			overlappingSegments = append(overlappingSegments, segment)
			overlappingServiceNames = append(overlappingServiceNames, segmentService)
			o.logger.Debug("Found overlapping segment",
				zap.String("bad_segment_id", badSegmentID),
				zap.String("overlapping_segment_id", segment.Name()),
				zap.String("overlapping_service", segmentService),
				zap.Time("overlap_start", max(badSegmentStart, segmentStart)),
				zap.Time("overlap_end", min(badSegmentEnd, segmentEnd)))
		}
	}

	// Emit overlapping segments
	if len(overlappingSegments) > 0 {
		// Increment overlapping segments counter
		o.countersMutex.Lock()
		o.overlappingCount += int64(len(overlappingSegments))
		o.countersMutex.Unlock()

		// Track overlap information for enhanced logging
		overlappingTraceIDs := make([]string, 0, len(overlappingSegments))
		overlappingUpstreamIPs := make([]string, 0, len(overlappingSegments))
		for _, segment := range overlappingSegments {
			overlappingTraceIDs = append(overlappingTraceIDs, segment.TraceID().String())
			overlappingUpstreamIPs = append(overlappingUpstreamIPs, o.extractUpstreamIP(segment))
		}

		overlapInfo := BadSegmentOverlapInfo{
			BadSegmentTraceID:       badSegment.TraceID().String(),
			OverlappingTraceIDs:     overlappingTraceIDs,
			BadSegmentUpstreamIP:    badSegmentUpstreamIP,
			OverlappingUpstreamIPs:  overlappingUpstreamIPs,
			BadSegmentServiceName:   badSegmentService,
			OverlappingServiceNames: overlappingServiceNames,
			DetectedAt:              time.Now(),
		}

		// Add to tracking list
		o.overlapsMutex.Lock()
		o.badSegmentOverlaps = append(o.badSegmentOverlaps, overlapInfo)
		o.overlapsMutex.Unlock()

		return overlappingSegments
	}

	return nil
}

// segmentsOverlap checks if two time ranges overlap
func (o *overlapDetectionProcessor) segmentsOverlap(start1, end1, start2, end2 time.Time) bool {
	return start1.Before(end2) && start2.Before(end1)
}

// emitOverlappingSegments emits overlapping segments to the next consumer
func (o *overlapDetectionProcessor) emitOverlappingSegments(overlappingSegments []ptrace.Span) {
	o.logger.Info("Emitting overlapping segments", zap.Int("count", len(overlappingSegments)))

	if len(overlappingSegments) == 0 {
		return
	}

	// Create output traces
	outputTraces := ptrace.NewTraces()
	resourceSpans := outputTraces.ResourceSpans().AppendEmpty()
	scopeSpans := resourceSpans.ScopeSpans().AppendEmpty()

	// Set resource attributes for overlap detection spans
	resourceSpans.Resource().Attributes().PutStr("service.name", "overlap-detection-processor")
	resourceSpans.Resource().Attributes().PutStr("service.version", "1.0.0")

	// Set scope information
	scopeSpans.Scope().SetName("overlap-detection")
	scopeSpans.Scope().SetVersion("1.0.0")

	// Add all overlapping segments to output, but use their parent span IDs
	for _, segment := range overlappingSegments {
		outputSpan := scopeSpans.Spans().AppendEmpty()

		// Copy the segment span but change the span ID to its parent span ID
		segment.CopyTo(outputSpan)
		parentSpanID := segment.ParentSpanID()
		outputSpan.SetSpanID(parentSpanID)

		// Log each segment being emitted with parent span ID
		upstreamIP := o.extractUpstreamIP(segment)
		o.logger.Debug("Emitting overlapping segment with parent span ID",
			zap.String("segment_name", segment.Name()),
			zap.String("trace_id", segment.TraceID().String()),
			zap.String("original_span_id", segment.SpanID().String()),
			zap.String("parent_span_id", parentSpanID.String()),
			zap.String("upstream_ip", upstreamIP))
	}

	// Send to next consumer
	ctx := context.Background()
	o.logger.Info("Emitting overlapping segments", zap.Int("count", len(overlappingSegments)))
	if err := o.nextConsumer.ConsumeTraces(ctx, outputTraces); err != nil {
		o.logger.Error("Failed to emit overlapping segments", zap.Error(err))
	} else {
		o.logger.Info("Successfully emitted overlapping segments", zap.Int("count", len(overlappingSegments)))
	}
}

// Helper functions for time comparison
func max(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

func min(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

// counterWorkerLoop logs counters every 1 second
func (o *overlapDetectionProcessor) counterWorkerLoop() {
	defer o.counterWorkerWg.Done()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-o.stopCounterWorker:
			return
		case <-ticker.C:
			o.logCounters()
		}
	}
}

// logCounters logs the current counter values
func (o *overlapDetectionProcessor) logCounters() {
	o.countersMutex.RLock()
	badCount := o.badSegmentCount
	overlapCount := o.overlappingCount
	o.countersMutex.RUnlock()

	// Get recent overlap information for enhanced logging
	o.overlapsMutex.RLock()
	recentOverlaps := make([]BadSegmentOverlapInfo, 0)
	if len(o.badSegmentOverlaps) > 0 {
		// Get the last 10 overlap detections for detailed logging
		startIdx := 0
		if len(o.badSegmentOverlaps) > 10 {
			startIdx = len(o.badSegmentOverlaps) - 10
		}
		recentOverlaps = o.badSegmentOverlaps[startIdx:]
	}
	o.overlapsMutex.RUnlock()

	o.logger.Info("Overlap Detection Processor counters:",
		zap.Int64("total_bad_segments_detected", badCount),
		zap.Int64("total_overlapping_segments_detected", overlapCount),
		zap.Int("total_overlap_events_tracked", len(o.badSegmentOverlaps)))

	// Log detailed overlap information for recent events
	if len(recentOverlaps) > 0 {
		for i, overlap := range recentOverlaps {
			o.logger.Info("Bad segment overlap details:",
				zap.Int("overlap_event_index", i),
				zap.String("bad_segment_trace_id", overlap.BadSegmentTraceID),
				zap.String("bad_segment_upstream_ip", overlap.BadSegmentUpstreamIP),
				zap.String("bad_segment_service", overlap.BadSegmentServiceName),
				zap.Strings("overlapping_trace_ids", overlap.OverlappingTraceIDs),
				zap.Strings("overlapping_upstream_ips", overlap.OverlappingUpstreamIPs),
				zap.Strings("overlapping_services", overlap.OverlappingServiceNames),
				zap.Time("detected_at", overlap.DetectedAt),
				zap.Int("overlapping_count", len(overlap.OverlappingTraceIDs)))
		}
	}
}

// GetBadSegmentOverlaps returns a copy of the current bad segment overlap information
func (o *overlapDetectionProcessor) GetBadSegmentOverlaps() []BadSegmentOverlapInfo {
	o.overlapsMutex.RLock()
	defer o.overlapsMutex.RUnlock()

	// Return a copy to avoid race conditions
	result := make([]BadSegmentOverlapInfo, len(o.badSegmentOverlaps))
	copy(result, o.badSegmentOverlaps)
	return result
}

// ClearBadSegmentOverlaps clears the overlap tracking list (useful for memory management)
func (o *overlapDetectionProcessor) ClearBadSegmentOverlaps() {
	o.overlapsMutex.Lock()
	defer o.overlapsMutex.Unlock()
	o.badSegmentOverlaps = make([]BadSegmentOverlapInfo, 0)
}

// getServiceName extracts the service name from service_operation attribute
func (o *overlapDetectionProcessor) getServiceName(span ptrace.Span) string {
	if serviceOp, exists := span.Attributes().AsRaw()["service_operation"]; exists {
		if serviceOpStr, ok := serviceOp.(string); ok {
			// Split on "__" and take the first part
			parts := strings.Split(serviceOpStr, "__")
			if len(parts) > 0 {
				return parts[0]
			}
		}
	}
	return "unknown-service"
}

// extractUpstreamIP extracts the upstream IP address from span attributes
func (o *overlapDetectionProcessor) extractUpstreamIP(span ptrace.Span) string {
	// Check span attributes for upstream IP using AsRaw method
	if upstreamIP, exists := span.Attributes().AsRaw()["upstream.ip"]; exists {
		if upstreamIPStr, ok := upstreamIP.(string); ok && upstreamIPStr != "" {
			return upstreamIPStr
		}
	}

	return "unknown-upstream"
}

// start is called when the processor starts
func (o *overlapDetectionProcessor) start(ctx context.Context, host component.Host) error {
	o.logger.Info("Starting overlap detection processor")

	// Start worker goroutine
	o.workerWg.Add(1)
	go o.workerLoop()

	// Start bad segment worker goroutine
	o.badSegmentWorkerWg.Add(1)
	go o.badSegmentWorkerLoop()

	// Start counter logging worker goroutine
	o.counterWorkerWg.Add(1)
	go o.counterWorkerLoop()

	return nil
}

// shutdown is called when the processor shuts down
func (o *overlapDetectionProcessor) shutdown(ctx context.Context) error {
	o.logger.Info("Shutting down overlap detection processor")

	// Stop workers
	close(o.stopWorker)
	o.workerWg.Wait()

	close(o.stopBadSegmentWorker)
	o.badSegmentWorkerWg.Wait()

	close(o.stopCounterWorker)
	o.counterWorkerWg.Wait()

	return nil
}
