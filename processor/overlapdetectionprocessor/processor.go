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

	// Process each bad segment
	for _, segmentID := range segmentsToProcess {
		o.processBadSegment(segmentID)
		// Remove from badSegments map after processing
		delete(o.badSegments, segmentID)
	}
}

// processBadSegment finds overlapping segments for a given bad segment
func (o *overlapDetectionProcessor) processBadSegment(badSegmentID string) {
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
		return
	}

	// Get service name for filtering
	badSegmentService := o.getServiceName(badSegment)
	badSegmentStart := badSegment.StartTimestamp().AsTime()
	badSegmentEnd := badSegment.EndTimestamp().AsTime()

	o.logger.Info("Processing bad segment for overlaps",
		zap.String("segment_id", badSegmentID),
		zap.String("service", badSegmentService),
		zap.Time("start", badSegmentStart),
		zap.Time("end", badSegmentEnd))

	// Find overlapping segments within the same service
	var overlappingSegments []ptrace.Span
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
			o.logger.Debug("Found overlapping segment",
				zap.String("bad_segment_id", badSegmentID),
				zap.String("overlapping_segment_id", segment.Name()),
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

		o.emitOverlappingSegments(badSegment, overlappingSegments)
	}
}

// segmentsOverlap checks if two time ranges overlap
func (o *overlapDetectionProcessor) segmentsOverlap(start1, end1, start2, end2 time.Time) bool {
	return start1.Before(end2) && start2.Before(end1)
}

// emitOverlappingSegments emits the bad segment and its overlapping segments
func (o *overlapDetectionProcessor) emitOverlappingSegments(badSegment ptrace.Span, overlappingSegments []ptrace.Span) {
	o.logger.Info("Emitting overlapping segments",
		zap.String("bad_segment", badSegment.Name()),
		zap.Int("overlapping_count", len(overlappingSegments)))

	// TODO: Implement actual emission logic
	// For now, just log the segments that would be emitted
	for _, segment := range overlappingSegments {
		o.logger.Info("Would emit overlapping segment",
			zap.String("bad_segment", badSegment.Name()),
			zap.String("overlapping_segment", segment.Name()))
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

	o.logger.Info("Overlap Detection Processor counters:",
		zap.Int64("total_bad_segments_detected", badCount),
		zap.Int64("total_overlapping_segments_detected", overlapCount))
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
