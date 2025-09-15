// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package spanreconstructionprocessor

import (
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor/batchprocessor"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/spanprocessor"
)

/*
// Tracepoint represents a state change moment in service execution
type Tracepoint struct {
	ID        string // e.g., "span_123_START" or "span_456_END"
	Timestamp time.Time
	Name      string // e.g., "FrontendService_GetCart_START" or "CartService_AddItem_END"
	// Service      string // Service name where this tracepoint occurred
}

// Segment represents a work unit (on-service or off-service execution)
type Segment struct {
	From             string // Tracepoint ID where segment starts
	To               string // Tracepoint ID where segment ends
	StartTime        time.Time
	EndTime          time.Time
	Service          string   // Service name
	OnService        bool     // true for on-service, false for off-service
	ClientOperations []string // e.g., ["CartService_AddItem", "UserService_GetUser"]
	TraceID          string
	ServerSpanID     string
}

type tracepointChainState struct {
	lastTp             *Tracepoint
	traceID            pcommon.TraceID
	serverSpanID       pcommon.SpanID
	serviceName        string
	onService          bool
	openClientSpans    map[string]bool
	numOpenClientSpans int
	curSegment         *Segment
}
*/

var (
	_ = batchprocessor.Config{}
	_ = spanprocessor.Config{}
)

// GlobalSpanReconstructionProcessor is the global instance that other processors can access
var GlobalSpanReconstructionProcessor *SpanReconstructionProcessor

// traceSegment represents a segment of a trace between two tracepoints
type traceSegment struct {
	from        string
	to          string
	fromIsStart bool
	toIsStart   bool
	onService   bool
	tags        map[string]string
}

// reconstructedSpan represents a span that is currently being reconstructed
type reconstructedSpan struct {
	span     ptrace.Span
	resource ptrace.ResourceSpans
	scope    ptrace.ScopeSpans
	lastSeen time.Time
	// Below: segments within the span, denoting on-service and off-service work
	segments              []traceSegment
	lastTracepointSpan    pcommon.SpanID
	lastTracepointIsStart bool
	openClientSpans       int
}

// SpanWithContext holds a span with its resource and scope context
type SpanWithContext struct {
	Span     ptrace.Span
	Resource ptrace.ResourceSpans
	Scope    ptrace.ScopeSpans
	FlowID   int // Sequential ID to track span flow through the system
}

// EventBuffer holds unprocessed events with their context
type EventBuffer struct {
	mu     sync.Mutex
	events []SpanWithContext
}

// SpanReconstructionProcessor implements the span reconstruction logic
type SpanReconstructionProcessor struct {
	logger *zap.Logger
	config *Config

	// Next consumer for outputting completed spans
	nextConsumer consumer.Traces

	// Event buffer for handling out-of-order events
	eventBuffer *EventBuffer

	// Span storage - separate open and closed spans
	OpenSpans       map[string]*reconstructedSpan
	ClosedSpans     map[string]*reconstructedSpan
	ExportableSpans map[string]*reconstructedSpan
	TotalSpans      int
	SpansEnqueued   int
	SpansDuplicated int
	SpansCompleted  int
	SpansCollided   int
	SpansEmitted    int
	SpansMutex      sync.RWMutex

	EnqueuedSpanIDs map[string]bool

	// Cleanup ticker
	cleanupTicker *time.Ticker
	stopCleanup   chan struct{}

	// Worker control
	stopWorker chan struct{}
	workerWg   sync.WaitGroup

	// Parent-to-children map
	ParentToChildren map[string][]string // server span key -> list of client span keys

	// Flow tracking
	NextFlowID  int        // Global counter for tracking span flow
	FlowIDMutex sync.Mutex // Mutex to protect NextFlowID counter

	// Tracepoints and segments
	// tracepoints      map[string]*Tracepoint
	// segments         map[string]*Segment
	// tracepointChains map[string]*tracepointChainState

	testingLock sync.Mutex

	// Debugging
	FlowIDsEnqueued map[int]bool
	FlowIDsSeen     map[int]bool
	FlowIDsHandled  map[int]bool
}

// newSpanReconstructionProcessor creates a new span reconstruction processor
func newSpanReconstructionProcessor(logger *zap.Logger, config *Config, nextConsumer consumer.Traces) *SpanReconstructionProcessor {
	processor := &SpanReconstructionProcessor{
		logger:       logger,
		config:       config,
		nextConsumer: nextConsumer,
		eventBuffer: &EventBuffer{
			events: make([]SpanWithContext, 0),
		},
		OpenSpans:       make(map[string]*reconstructedSpan),
		ClosedSpans:     make(map[string]*reconstructedSpan),
		ExportableSpans: make(map[string]*reconstructedSpan),
		EnqueuedSpanIDs: make(map[string]bool),
		// tracepoints:      make(map[string]*Tracepoint),
		// segments:         make(map[string]*Segment),
		stopCleanup:      make(chan struct{}),
		stopWorker:       make(chan struct{}),
		ParentToChildren: make(map[string][]string),
		// tracepointChains: make(map[string]*tracepointChainState),
	}

	// Set the global instance so other processors can access it
	GlobalSpanReconstructionProcessor = processor

	return processor
}

// start initializes the processor
func (p *SpanReconstructionProcessor) start(ctx context.Context, host component.Host) error {
	// Start cleanup goroutine
	p.cleanupTicker = time.NewTicker(p.config.SpanTTL / 4) // Clean up every quarter of TTL
	go p.cleanupRoutine()

	// Start worker goroutine
	p.workerWg.Add(1)
	go p.workerLoop()

	// Start printing spans enqueued goroutine
	go p.PrintSpansEnqueued()

	return nil
}

func (p *SpanReconstructionProcessor) PrintSpansEnqueued() {
	for {
		p.logger.Info("🟢 INFO: Total spans enqueued/skipped/emitted/etc",
			zap.Int("spans_enqueued", p.SpansEnqueued),
			zap.Int("spans_duplicated", p.SpansDuplicated),
			zap.Int("spans_completed", p.SpansCompleted),
			zap.Int("spans_collided", p.SpansCollided),
			zap.Int("spans_emitted", p.SpansEmitted),
			zap.Int("spans_still_in_buffer", len(p.eventBuffer.events)))
		time.Sleep(1 * time.Second)
	}
}

// shutdown stops the processor
func (p *SpanReconstructionProcessor) shutdown(ctx context.Context) error {
	if p.cleanupTicker != nil {
		p.cleanupTicker.Stop()
	}
	close(p.stopCleanup)

	// Stop worker
	close(p.stopWorker)
	p.workerWg.Wait()

	return nil
}

// processTraces processes incoming traces and adds them to the event buffer
func (p *SpanReconstructionProcessor) processTraces(ctx context.Context, td ptrace.Traces) (ptrace.Traces, error) {
	p.logger.Info("🟢 INFO: Processing traces batch",
		zap.Int("resource_spans_count", td.ResourceSpans().Len()),
		zap.Int("total_spans", p.countTotalSpans(td)))

	// duplicatedSpanIDs := 0

	// Collect all spans with their context
	var allSpansWithContext []SpanWithContext
	for i := 0; i < td.ResourceSpans().Len(); i++ {
		resourceSpans := td.ResourceSpans().At(i)
		for j := 0; j < resourceSpans.ScopeSpans().Len(); j++ {
			scopeSpans := resourceSpans.ScopeSpans().At(j)
			for k := 0; k < scopeSpans.Spans().Len(); k++ {
				span := scopeSpans.Spans().At(k)
				// p.FlowIDMutex.Lock()
				// p.NextFlowID++
				// flowID := p.NextFlowID
				// p.FlowIDMutex.Unlock()

				allSpansWithContext = append(allSpansWithContext, SpanWithContext{
					Span:     span,
					Resource: resourceSpans,
					Scope:    scopeSpans,
					// FlowID:   flowID,
				})

				// spanKey := p.getSpanKey(span)
				// if p.isStartSpan(span) {
				// 	spanKey += "_START"
				// } else if p.isEndSpan(span) {
				// 	spanKey += "_END"
				// } else if p.isLogSpan(span) {
				// 	spanKey += "_LOG"
				// }

				// p.SpansMutex.Lock()
				// if _, exists := p.EnqueuedSpanIDs[spanKey]; exists {
				// 	duplicatedSpanIDs++
				// } else {
				// 	p.EnqueuedSpanIDs[spanKey] = true
				// }
				// p.SpansMutex.Unlock()
			}
		}
	}

	// Add all spans with context to event buffer in one operation
	p.addEventsToBufferWithContext(allSpansWithContext)

	p.eventBuffer.mu.Lock()
	p.SpansEnqueued += len(allSpansWithContext)
	// p.SpansDuplicated += duplicatedSpanIDs
	p.eventBuffer.mu.Unlock()

	p.logger.Info("🟢 INFO: Added spans to event buffer",
		zap.Int("spans_added", len(allSpansWithContext)),
		zap.Int("total_spans", p.TotalSpans))

	// Return empty traces - output will be handled independently by worker loop
	return ptrace.NewTraces(), nil
}

func (p *SpanReconstructionProcessor) isStartSpan(span ptrace.Span) bool {
	return span.StartTimestamp() != 0 && span.EndTimestamp() == 0
}

func (p *SpanReconstructionProcessor) isEndSpan(span ptrace.Span) bool {
	return span.StartTimestamp() == 0 && span.EndTimestamp() != 0
}

func (p *SpanReconstructionProcessor) isLogSpan(span ptrace.Span) bool {
	return span.Events().Len() > 0 && span.StartTimestamp() == 0 && span.EndTimestamp() == 0
}

// outputCompletedSpans outputs completed spans to the next consumer
func (p *SpanReconstructionProcessor) outputCompletedSpans() {
	if p.nextConsumer == nil {
		return
	}

	// Create output traces with completed spans
	outputTraces := ptrace.NewTraces()
	completedCount := p.addCompletedSpans(outputTraces)

	if completedCount > 0 {
		p.logger.Info("🟢 INFO: Outputting completed spans",
			zap.Int("completed_spans", completedCount),
			zap.Int("output_spans", p.countTotalSpans(outputTraces)))

		// Send to next consumer
		ctx := context.Background()
		if err := p.nextConsumer.ConsumeTraces(ctx, outputTraces); err != nil {
			p.logger.Error("🔴 ERROR: Failed to output completed spans",
				zap.Error(err),
				zap.Int("completed_spans", completedCount))
		}
	}
}

// addEventsToBuffer adds multiple span events to the buffer in one operation
func (p *SpanReconstructionProcessor) addEventsToBuffer(spans []ptrace.Span) {
	if len(spans) == 0 {
		return
	}

	// Convert spans to SpanWithContext with empty context
	spansWithContext := make([]SpanWithContext, len(spans))
	for i, span := range spans {
		p.FlowIDMutex.Lock()
		p.NextFlowID++
		flowID := p.NextFlowID
		p.FlowIDMutex.Unlock()

		spansWithContext[i] = SpanWithContext{
			Span:     span,
			Resource: ptrace.ResourceSpans{},
			Scope:    ptrace.ScopeSpans{},
			FlowID:   flowID,
		}
	}

	p.eventBuffer.mu.Lock()
	p.eventBuffer.events = append(p.eventBuffer.events, spansWithContext...)
	p.eventBuffer.mu.Unlock()
}

// addEventsToBufferWithContext adds multiple span events to the buffer with their context
func (p *SpanReconstructionProcessor) addEventsToBufferWithContext(spansWithContext []SpanWithContext) {
	if len(spansWithContext) == 0 {
		return
	}

	p.eventBuffer.mu.Lock()
	p.eventBuffer.events = append(p.eventBuffer.events, spansWithContext...)
	p.eventBuffer.mu.Unlock()
}

// workerLoop processes events from the buffer
func (p *SpanReconstructionProcessor) workerLoop() {
	defer p.workerWg.Done()

	for {
		// time.Sleep(180 * time.Second)
		select {
		case <-p.stopWorker:
			return
		default:
			p.processEventBuffer()
			// Output completed spans independently
			p.outputCompletedSpans()
		}
	}
}

// processEventBuffer processes all events in the buffer
func (p *SpanReconstructionProcessor) processEventBuffer() {
	// Get current events and clear buffer
	p.eventBuffer.mu.Lock()
	currentEvents := p.eventBuffer.events[:]
	p.eventBuffer.events = make([]SpanWithContext, 0)
	p.eventBuffer.mu.Unlock()

	// ceSum := 0
	// for _, event := range currentEvents {
	// 	ceSum += event.FlowID
	// }

	if len(currentEvents) == 0 {
		return
	}

	unhandledEvents := make([]SpanWithContext, 0)

	// duplicatedSpanIDs := 0

	processedSum := 0
	unprocessedSum := 0

	// Process each event
	for _, eventWithContext := range currentEvents {
		if p.canHandleEvent(eventWithContext.Span) {
			p.handleEventWithContext(eventWithContext)
			// processedSum += eventWithContext.FlowID
			// spanKey := p.getSpanKey(eventWithContext.Span)

			// p.SpansMutex.Lock()
			// if _, exists := p.EnqueuedSpanIDs[spanKey]; exists {
			// 	duplicatedSpanIDs++
			// 	p.logger.Warn("🔴 DUPLICATE: FlowID", zap.Int("flow_id", eventWithContext.FlowID), zap.String("span_key", spanKey))
			// } else {
			// 	p.EnqueuedSpanIDs[spanKey] = true
			// 	p.logger.Info("🟢 PROCESSED: FlowID", zap.Int("flow_id", eventWithContext.FlowID), zap.String("span_key", spanKey))
			// }
			// p.SpansMutex.Unlock()
		} else {
			unhandledEvents = append(unhandledEvents, eventWithContext)
			// p.logger.Info("🟡 REQUEUED: FlowID", zap.Int("flow_id", eventWithContext.FlowID), zap.String("span_key", p.getSpanKey(eventWithContext.Span)))
		}
	}

	// for _, event := range unhandledEvents {
	// 	unprocessedSum += event.FlowID
	// }

	p.logger.Info("🟢 INFO: Processed/Unprocessed/Total",
		zap.Int("processed_sum", processedSum),
		zap.Int("unprocessed_sum", unprocessedSum),
		zap.Int("processed_sum + unprocessed_sum", processedSum+unprocessedSum))

	// p.SpansDuplicated += duplicatedSpanIDs

	// Add unhandled events back to buffer
	if len(unhandledEvents) > 0 {
		p.eventBuffer.mu.Lock()
		p.eventBuffer.events = append(p.eventBuffer.events, unhandledEvents...)
		p.eventBuffer.mu.Unlock()
	}
}

// canHandleEvent determines if an event can be processed
func (p *SpanReconstructionProcessor) canHandleEvent(span ptrace.Span) bool {
	// Check if this is a complete span (pass through)
	if span.StartTimestamp() != 0 && span.EndTimestamp() != 0 {
		return true
	}

	// Check if this is an END event
	if span.StartTimestamp() == 0 && span.EndTimestamp() != 0 {
		spanKey := p.getSpanKey(span)
		// Can handle if corresponding START event exists in OpenSpans
		p.SpansMutex.RLock()
		_, exists := p.OpenSpans[spanKey]
		p.SpansMutex.RUnlock()
		return exists
	}

	// Check if this is a client START event
	if span.StartTimestamp() != 0 && span.EndTimestamp() == 0 && isClientSpan(span) {
		// return p.establishParentChildRelationship(span, p.getSpanKey(span))
		return true
	}

	// Check if this is a server START event or LOG event
	if (span.StartTimestamp() != 0 && span.EndTimestamp() == 0 && isServerSpan(span)) ||
		(span.Events().Len() > 0 && span.StartTimestamp() == 0 && span.EndTimestamp() == 0) {
		return true
	}

	return false
}

// handleEvent processes a single event
func (p *SpanReconstructionProcessor) handleEvent(span ptrace.Span) {
	spanKey := p.getSpanKey(span)

	// Check if this is a complete span (pass through)
	if span.StartTimestamp() != 0 && span.EndTimestamp() != 0 {
		// This is a complete span, no processing needed
		return
	}

	// Check if this is a span start event
	if span.StartTimestamp() != 0 && span.EndTimestamp() == 0 {
		p.handleSpanStart(span, spanKey, ptrace.ResourceSpans{}, ptrace.ScopeSpans{})
		return
	}

	// Check if this is a span end event
	if span.StartTimestamp() == 0 && span.EndTimestamp() != 0 {
		p.handleSpanEnd(span, spanKey, ptrace.ScopeSpans{})
		return
	}

	// Check if this is a log event
	if span.Events().Len() > 0 && span.StartTimestamp() == 0 && span.EndTimestamp() == 0 {
		p.handleLogEvent(span, spanKey, ptrace.ScopeSpans{})
		return
	}
}

// handleEventWithContext processes a single event with its resource and scope context
func (p *SpanReconstructionProcessor) handleEventWithContext(eventWithContext SpanWithContext) {
	span := eventWithContext.Span
	spanKey := p.getSpanKey(span)

	// Check if this is a complete span (pass through)
	if span.StartTimestamp() != 0 && span.EndTimestamp() != 0 {
		// This is a complete span, no processing needed
		return
	}

	// Check if this is a span start event
	if span.StartTimestamp() != 0 && span.EndTimestamp() == 0 {
		p.handleSpanStart(span, spanKey, eventWithContext.Resource, eventWithContext.Scope)
		return
	}

	// Check if this is a span end event
	if span.StartTimestamp() == 0 && span.EndTimestamp() != 0 {
		p.handleSpanEnd(span, spanKey, eventWithContext.Scope)
		return
	}

	// Check if this is a log event
	if span.Events().Len() > 0 && span.StartTimestamp() == 0 && span.EndTimestamp() == 0 {
		p.handleLogEvent(span, spanKey, eventWithContext.Scope)
		return
	}

	// Unknown event type
	p.logger.Warn("🟡 WARN: Unknown event type",
		zap.String("trace_id", span.TraceID().String()),
		zap.String("span_id", span.SpanID().String()),
		zap.String("start_timestamp", span.StartTimestamp().String()),
		zap.String("end_timestamp", span.EndTimestamp().String()),
		zap.Int("events_count", span.Events().Len()))
}

// processSpan processes a single span and determines if it's a start, end, or log event
func (p *SpanReconstructionProcessor) processSpan(span ptrace.Span, outputScope ptrace.ScopeSpans, outputResource ptrace.ResourceSpans) {
	spanKey := p.getSpanKey(span)

	p.logger.Debug("🟠 DEBUG: Processing individual span",
		zap.String("trace_id", span.TraceID().String()),
		zap.String("span_id", span.SpanID().String()),
		zap.String("span_name", span.Name()),
		zap.String("span_kind", span.Kind().String()),
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
func (p *SpanReconstructionProcessor) handleSpanStart(span ptrace.Span, spanKey string, outputResource ptrace.ResourceSpans, outputScope ptrace.ScopeSpans) {
	p.SpansMutex.Lock()
	defer p.SpansMutex.Unlock()

	p.logger.Info("🟢 INFO: Handling span START event",
		zap.String("trace_id", span.TraceID().String()),
		zap.String("span_id", span.SpanID().String()),
		zap.String("span_name", span.Name()),
		zap.String("span_kind", span.Kind().String()),
		zap.Int("current_open_spans", len(p.OpenSpans)),
		zap.Int("current_closed_spans", len(p.ClosedSpans)),
		zap.Int("max_active_spans", p.config.MaxActiveSpans))

	// Check if span already exists
	if existingSpan, exists := p.OpenSpans[spanKey]; exists {
		p.logger.Warn("🔴 WARN: Span START event for already open span - discarding duplicate",
			zap.String("span_key", spanKey),
			zap.String("trace_id", span.TraceID().String()),
			zap.String("span_id", span.SpanID().String()),
			zap.String("span_name", span.Name()),
			zap.Time("existing_last_seen", existingSpan.lastSeen))

		p.SpansCollided++
		return // Discard the duplicate, keep the original
	}

	// For client spans, check parent-child relationship BEFORE adding to OpenSpans
	/*
		if isClientSpan(span) {
			hasParent := p.establishParentChildRelationship(span, spanKey)
			if !hasParent {
				// Discard client span that has no server parent
				p.logger.Info("🟢 INFO: Discarding client span with no server parent",
					zap.String("trace_id", span.TraceID().String()),
					zap.String("span_id", span.SpanID().String()),
					zap.String("span_name", span.Name()))
				return
			}
		}
	*/

	// Create new open span with independent copies
	openSpan := &reconstructedSpan{
		span:     ptrace.NewSpan(),          // Create new span
		resource: ptrace.NewResourceSpans(), // Create new resource
		scope:    ptrace.NewScopeSpans(),    // Create new scope
		lastSeen: time.Now(),
	}

	// Copy the span data to ensure independence
	span.CopyTo(openSpan.span)

	// Copy the resource data to ensure independence
	if outputResource != (ptrace.ResourceSpans{}) {
		outputResource.CopyTo(openSpan.resource)
	}

	// Copy the scope data to ensure independence
	if outputScope != (ptrace.ScopeSpans{}) {
		outputScope.CopyTo(openSpan.scope)
	}

	p.OpenSpans[spanKey] = openSpan
	p.TotalSpans++

	// // Create tracepoint for span start event
	// tracepoint := createTracepointFromSpanEvent(span, "START", outputResource.Resource())
	// p.tracepoints[tracepoint.ID] = tracepoint

	if isServerSpan(span) {
		openSpan.segments = []traceSegment{{
			from:        span.SpanID().String(),
			to:          "",
			fromIsStart: true,
			toIsStart:   false,
			onService:   true,
		}}
	} else {
		openSpan.segments = nil
		parentSpanKey := p.getSpanKeyFromIDs(span.TraceID(), span.ParentSpanID())

		parentSpan, exists := p.OpenSpans[parentSpanKey]
		if !exists {
			p.logger.Warn("🔴 WARN: Client span has no parent span - discarding",
				zap.String("client_span_key", spanKey),
				zap.String("trace_id", span.TraceID().String()))
			return
		}

		if parentSpan.openClientSpans == 0 {
			parentSpan.segments = append(
				parentSpan.segments,
				traceSegment{
					from:        parentSpan.lastTracepointSpan.String(),
					to:          openSpan.span.SpanID().String(),
					fromIsStart: parentSpan.lastTracepointIsStart,
					toIsStart:   true,
					onService:   true,
				},
			)
		}
	}

	p.logger.Info("🟢 INFO: Created new open span",
		zap.String("trace_id", span.TraceID().String()),
		zap.String("span_id", span.SpanID().String()),
		zap.String("span_name", span.Name()),
		zap.Int("total_open_spans", len(p.OpenSpans)),
		zap.Int("total_closed_spans", len(p.ClosedSpans)),
		zap.Int("total_spans", p.TotalSpans))

	// Check if we need to evict spans due to memory pressure (asynchronously)
	go p.evictUntilUnderLimit(true)
}

// Helper: establishParentChildRelationship establishes parent-child relationship for client spans
// Returns true if a parent was found and relationship was established, false otherwise
func (p *SpanReconstructionProcessor) establishParentChildRelationship(clientSpan ptrace.Span, clientSpanKey string) bool {
	parentSpanID := clientSpan.ParentSpanID()
	traceID := clientSpan.TraceID()

	if parentSpanID == pcommon.SpanID([8]byte{}) {
		p.logger.Warn("🔴 WARN: Client span has no parent span ID - discarding",
			zap.String("client_span_key", clientSpanKey),
			zap.String("trace_id", traceID.String()))
		return false
	}

	// Look for the specific parent server span using parent span ID
	parentServerKey := p.getSpanKeyFromIDs(traceID, parentSpanID)

	// CRITICAL: Add mutex lock to prevent race conditions
	p.SpansMutex.Lock()
	defer p.SpansMutex.Unlock()

	// Check open spans for the parent server span
	if parentSpan, exists := p.OpenSpans[parentServerKey]; exists {
		if isServerSpan(parentSpan.span) {
			// Establish the relationship
			if p.ParentToChildren[parentServerKey] == nil {
				p.ParentToChildren[parentServerKey] = make([]string, 0)
			}
			p.ParentToChildren[parentServerKey] = append(p.ParentToChildren[parentServerKey], clientSpanKey)

			p.logger.Info("🟢 INFO: Established parent-child relationship (open span)",
				zap.String("parent_server_span", parentServerKey),
				zap.String("child_client_span", clientSpanKey),
				zap.String("trace_id", traceID.String()),
				zap.String("parent_span_id", parentSpanID.String()))
			return true
		}
	}

	// Check closed spans for the parent server span
	if parentSpan, exists := p.ClosedSpans[parentServerKey]; exists {
		if isServerSpan(parentSpan.span) {
			// Establish the relationship
			if p.ParentToChildren[parentServerKey] == nil {
				p.ParentToChildren[parentServerKey] = make([]string, 0)
			}
			p.ParentToChildren[parentServerKey] = append(p.ParentToChildren[parentServerKey], clientSpanKey)

			p.logger.Info("🟢 INFO: Established parent-child relationship (closed span)",
				zap.String("parent_server_span", parentServerKey),
				zap.String("child_client_span", clientSpanKey),
				zap.String("trace_id", traceID.String()),
				zap.String("parent_span_id", parentSpanID.String()))
			return true
		}
	}

	p.logger.Warn("🔴 WARN: Client span's parent server span not found - discarding",
		zap.String("client_span_key", clientSpanKey),
		zap.String("trace_id", traceID.String()),
		zap.String("parent_span_id", parentSpanID.String()))
	return false
}

// handleSpanEnd processes a span end event
func (p *SpanReconstructionProcessor) handleSpanEnd(span ptrace.Span, spanKey string, _ ptrace.ScopeSpans) {
	p.logger.Debug("🟠 DEBUG: handleSpanEnd - entering", zap.String("trace_id", span.TraceID().String()), zap.String("span_id", span.SpanID().String()))
	p.SpansMutex.Lock()
	defer p.SpansMutex.Unlock()

	p.logger.Info("🟢 INFO: Handling span END event",
		zap.String("trace_id", span.TraceID().String()),
		zap.String("span_id", span.SpanID().String()),
		zap.String("span_name", span.Name()),
		zap.String("span_kind", span.Kind().String()))

	openSpan, exists := p.OpenSpans[spanKey]
	if !exists {
		p.logger.Warn("🔴 WARN: Received span END event for unknown span - no matching START found",
			zap.String("trace_id", span.TraceID().String()),
			zap.String("span_id", span.SpanID().String()),
			zap.Int("available_open_spans", len(p.OpenSpans)))
		p.logger.Debug("🟠 DEBUG: handleSpanEnd - exiting (span not found)")
		return
	}

	// Complete the span by copying end time
	openSpan.span.SetEndTimestamp(span.EndTimestamp())
	openSpan.lastSeen = time.Now()

	// Move span from open to closed
	if p.ExportableSpans[spanKey] != nil {
		p.SpansCollided++
	}
	p.SpansCompleted++

	p.ClosedSpans[spanKey] = openSpan
	p.ExportableSpans[spanKey] = openSpan
	delete(p.OpenSpans, spanKey)
	// TotalSpans stays the same since we're just moving from one collection to another

	// // Create tracepoint for span end event
	// tracepoint := createTracepointFromSpanEvent(span, "END", openSpan.resource.Resource())
	// p.tracepoints[tracepoint.ID] = tracepoint

	// p.logger.Info("🟢 COMPLETED: FlowID", zap.Int("flow_id", 0), zap.String("span_key", spanKey))
	p.logger.Info("🟢 INFO: Successfully completed span reconstruction",
		zap.String("trace_id", span.TraceID().String()),
		zap.String("span_id", span.SpanID().String()),
		zap.String("span_name", span.Name()),
		zap.Time("start_time", openSpan.span.StartTimestamp().AsTime()),
		zap.Time("completion_time", openSpan.lastSeen),
		zap.Duration("reconstruction_duration", openSpan.lastSeen.Sub(openSpan.span.StartTimestamp().AsTime())))
	p.logger.Debug("🟠 DEBUG: handleSpanEnd - exiting", zap.String("trace_id", span.TraceID().String()), zap.String("span_id", span.SpanID().String()))
}

// handleLogEvent processes a log event
func (p *SpanReconstructionProcessor) handleLogEvent(span ptrace.Span, spanKey string, _ ptrace.ScopeSpans) {
	p.SpansMutex.Lock()
	defer p.SpansMutex.Unlock()

	p.logger.Info("🟢 INFO: Handling span LOG event",
		zap.String("trace_id", span.TraceID().String()),
		zap.String("span_id", span.SpanID().String()),
		zap.Int("events_count", span.Events().Len()))

	openSpan, exists := p.OpenSpans[spanKey]
	if !exists {
		p.logger.Warn("🔴 WARN: Received LOG event for unknown span - no matching START found",
			zap.String("trace_id", span.TraceID().String()),
			zap.String("span_id", span.SpanID().String()),
			zap.Int("available_open_spans", len(p.OpenSpans)))
		return
	}

	// Add events to the open span
	originalEventCount := openSpan.span.Events().Len()
	for i := 0; i < span.Events().Len(); i++ {
		event := span.Events().At(i)
		outputEvent := openSpan.span.Events().AppendEmpty()
		event.CopyTo(outputEvent)
	}

	openSpan.lastSeen = time.Now()

	p.logger.Info("🟢 INFO: Successfully added events to open span",
		zap.String("trace_id", span.TraceID().String()),
		zap.String("span_id", span.SpanID().String()),
		zap.Int("events_added", span.Events().Len()),
		zap.Int("total_events_in_span", openSpan.span.Events().Len()),
		zap.Int("original_event_count", originalEventCount))
}

// addCompletedSpans adds completed spans to the output
func (p *SpanReconstructionProcessor) addCompletedSpans(outputTraces ptrace.Traces) int {
	p.SpansMutex.Lock()
	defer p.SpansMutex.Unlock()

	// Group completed spans by resource and scope
	completedSpans := make(map[string]map[string][]*reconstructedSpan)

	completedCount := 0

	// Process spans from ExportableSpans (these are already complete)
	keysSeen := make([]string, len(p.ExportableSpans))
	for key, span := range p.ExportableSpans {
		// for _, span := range p.ExportableSpans {
		resourceKey := p.getResourceKey(span.resource)
		scopeKey := p.getScopeKey(span.scope)

		if completedSpans[resourceKey] == nil {
			completedSpans[resourceKey] = make(map[string][]*reconstructedSpan)
		}
		if completedSpans[resourceKey][scopeKey] == nil {
			completedSpans[resourceKey][scopeKey] = make([]*reconstructedSpan, 0)
		}

		completedSpans[resourceKey][scopeKey] = append(completedSpans[resourceKey][scopeKey], span)
		keysSeen = append(keysSeen, key)
	}

	// Clear closed spans
	for _, key := range keysSeen {
		delete(p.ExportableSpans, key)
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
				// Copy resource attributes from the first span's original resource
				if len(spans) > 0 && spans[0].resource != (ptrace.ResourceSpans{}) {
					spans[0].resource.Resource().Attributes().CopyTo(outputResource.Resource().Attributes())
				}
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
				// Copy scope information from the first span's original scope
				if len(spans) > 0 && spans[0].scope != (ptrace.ScopeSpans{}) {
					spans[0].scope.Scope().CopyTo(outputScope.Scope())
				}
			}

			// Add spans
			for _, span := range spans {
				// if p.ExportedSpans[span.span.TraceID().String()+":"+span.span.SpanID().String()] {
				// 	// p.logger.Info("🔴 WARN DURING EXPORT: Span already seen: ",
				// 	// 	zap.String("trace_id", span.span.TraceID().String()),
				// 	// 	zap.String("span_id", span.span.SpanID().String()),
				// 	// 	zap.String("span_name", span.span.Name()))
				// 	continue
				// }
				// p.ExportedSpans[span.span.TraceID().String()+":"+span.span.SpanID().String()] = true

				outputSpan := outputScope.Spans().AppendEmpty()
				span.span.CopyTo(outputSpan)
				p.SpansEmitted++
			}

			completedCount += len(spans)
		}
	}

	// Note: We don't remove spans from ClosedSpans after emitting them
	// They stay in the cache until they expire via TTL or are evicted due to memory pressure

	return completedCount
}

// cleanupRoutine periodically cleans up expired spans
func (p *SpanReconstructionProcessor) cleanupRoutine() {
	for {
		select {
		case <-p.cleanupTicker.C:
			p.cleanupExpiredSpans()
		case <-p.stopCleanup:
			return
		}
	}
}

// cleanupExpiredSpans removes expired server spans and their client children
func (p *SpanReconstructionProcessor) cleanupExpiredSpans() {
	p.SpansMutex.Lock()
	defer p.SpansMutex.Unlock()

	now := time.Now()
	expiredServerKeys := []string{}

	// Find expired server spans
	for key, span := range p.ClosedSpans {
		if isServerSpan(span.span) && now.Sub(span.lastSeen) > p.config.SpanTTL {
			expiredServerKeys = append(expiredServerKeys, key)
		}
	}

	// For each expired server span, remove it and all its client children
	totalRemoved := 0
	for _, serverKey := range expiredServerKeys {
		// Remove the server span
		delete(p.ClosedSpans, serverKey)
		totalRemoved++

		// Remove all direct client children
		for _, childKey := range p.ParentToChildren[serverKey] {
			if _, exists := p.ClosedSpans[childKey]; exists {
				delete(p.ClosedSpans, childKey)
				totalRemoved++
			}
		}

		// Clean up the parent-child relationship
		delete(p.ParentToChildren, serverKey)
	}

	p.TotalSpans -= totalRemoved
	if len(expiredServerKeys) > 0 {
		p.logger.Info("🟢 INFO: Cleaned up expired server spans and their children",
			zap.Int("expired_server_spans", len(expiredServerKeys)),
			zap.Int("total_spans_removed", totalRemoved))
	}

	// Check if we need additional eviction after TTL cleanup
	p.evictUntilUnderLimit(false)
}

// evictUntilUnderLimit evicts spans until TotalSpans is under the configured limit
// Prioritizes evicting oldest closed server spans first, then oldest open server spans
func (p *SpanReconstructionProcessor) evictUntilUnderLimit(toLock bool) {
	if toLock {
		p.SpansMutex.Lock()
		defer p.SpansMutex.Unlock()
	}

	if p.TotalSpans <= p.config.MaxActiveSpans {
		return
	}

	p.logger.Warn("🔴 WARN: Total spans exceeds limit - starting eviction",
		zap.Int("current_total", p.TotalSpans),
		zap.Int("max_limit", p.config.MaxActiveSpans),
	)

	// First, try to evict from closed spans
	p.logger.Info("🟢 INFO: Attempting to evict from closed server spans")
	evictedFromClosed := p.evictOldestClosedServerSpans()

	// If still over limit, evict from open spans
	if p.TotalSpans > p.config.MaxActiveSpans {
		p.logger.Info("🟢 INFO: Attempting to evict from open server spans")
		evictedFromOpen := p.evictOldestOpenServerSpans()

		p.logger.Info("🟢 INFO: Eviction completed",
			zap.Int("evicted_from_closed", evictedFromClosed),
			zap.Int("evicted_from_open", evictedFromOpen),
			zap.Int("final_total", p.TotalSpans),
		)
	} else {
		p.logger.Info("🟢 INFO: Eviction completed from closed spans only",
			zap.Int("evicted_from_closed", evictedFromClosed),
			zap.Int("final_total", p.TotalSpans),
		)
	}
}

// evictOldestClosedServerSpans evicts the oldest closed server spans and their children
// Returns the number of spans evicted
func (p *SpanReconstructionProcessor) evictOldestClosedServerSpans() int {
	// Find all closed server spans and sort by lastSeen (oldest first)
	type serverSpanInfo struct {
		key      string
		lastSeen time.Time
	}

	var closedServerSpans []serverSpanInfo
	for key, span := range p.ClosedSpans {
		if isServerSpan(span.span) {
			closedServerSpans = append(closedServerSpans, serverSpanInfo{
				key:      key,
				lastSeen: span.lastSeen,
			})
		}
	}

	// Sort by lastSeen (oldest first)
	for i := 0; i < len(closedServerSpans)-1; i++ {
		for j := i + 1; j < len(closedServerSpans); j++ {
			if closedServerSpans[i].lastSeen.After(closedServerSpans[j].lastSeen) {
				closedServerSpans[i], closedServerSpans[j] = closedServerSpans[j], closedServerSpans[i]
			}
		}
	}

	// Evict server spans until under limit
	totalEvicted := 0
	for _, serverInfo := range closedServerSpans {
		if p.TotalSpans <= p.config.MaxActiveSpans {
			break
		}

		evicted := p.evictServerSpanAndChildren(serverInfo.key, true)
		totalEvicted += evicted
	}

	return totalEvicted
}

// evictOldestOpenServerSpans evicts the oldest open server spans and their children
// Returns the number of spans evicted
func (p *SpanReconstructionProcessor) evictOldestOpenServerSpans() int {
	// Find all open server spans and sort by lastSeen (oldest first)
	type serverSpanInfo struct {
		key      string
		lastSeen time.Time
	}

	var openServerSpans []serverSpanInfo
	for key, span := range p.OpenSpans {
		if isServerSpan(span.span) {
			openServerSpans = append(openServerSpans, serverSpanInfo{
				key:      key,
				lastSeen: span.lastSeen,
			})
		}
	}

	// Sort by lastSeen (oldest first)
	for i := 0; i < len(openServerSpans)-1; i++ {
		for j := i + 1; j < len(openServerSpans); j++ {
			if openServerSpans[i].lastSeen.After(openServerSpans[j].lastSeen) {
				openServerSpans[i], openServerSpans[j] = openServerSpans[j], openServerSpans[i]
			}
		}
	}

	// Evict server spans until under limit
	totalEvicted := 0
	for _, serverInfo := range openServerSpans {
		if p.TotalSpans <= p.config.MaxActiveSpans {
			break
		}

		evicted := p.evictServerSpanAndChildren(serverInfo.key, false)
		totalEvicted += evicted
	}

	return totalEvicted
}

// evictServerSpanAndChildren evicts a server span and all its children
// fromClosed indicates whether the server span is in ClosedSpans (true) or OpenSpans (false)
// Returns the number of spans evicted
func (p *SpanReconstructionProcessor) evictServerSpanAndChildren(serverKey string, fromClosed bool) int {
	// CRITICAL: Add mutex lock to prevent race conditions
	p.SpansMutex.Lock()
	defer p.SpansMutex.Unlock()

	evictedCount := 0

	// Remove the server span from its collection
	if fromClosed {
		if _, exists := p.ClosedSpans[serverKey]; exists {
			delete(p.ClosedSpans, serverKey)
			evictedCount++
		}
	} else {
		if _, exists := p.OpenSpans[serverKey]; exists {
			delete(p.OpenSpans, serverKey)
			evictedCount++
		}
	}

	// Gather child keys for logging
	childKeys := p.ParentToChildren[serverKey]
	childEvicted := []string{}

	// Remove all children from both collections
	for _, childKey := range childKeys {
		// Check ClosedSpans
		if _, exists := p.ClosedSpans[childKey]; exists {
			delete(p.ClosedSpans, childKey)
			evictedCount++
			childEvicted = append(childEvicted, childKey+" (closed)")
		}
		// Check OpenSpans
		if _, exists := p.OpenSpans[childKey]; exists {
			delete(p.OpenSpans, childKey)
			evictedCount++
			childEvicted = append(childEvicted, childKey+" (open)")
		}
	}

	// Clean up the parent-child relationship
	delete(p.ParentToChildren, serverKey)

	// Update total spans counter
	p.TotalSpans -= evictedCount

	p.logger.Warn("🔴 EVICTION: Server span and children evicted",
		zap.String("server_key", serverKey),
		zap.Bool("from_closed", fromClosed),
		zap.Strings("child_keys", childKeys),
		zap.Strings("child_evicted", childEvicted),
		zap.Int("spans_evicted", evictedCount),
		zap.Int("total_spans_after", p.TotalSpans),
	)

	return evictedCount
}

// getSpanKey creates a unique key for a span
func (p *SpanReconstructionProcessor) getSpanKey(span ptrace.Span) string {
	return span.TraceID().String() + ":" + span.SpanID().String()
}

// getSpanKeyFromIDs creates a unique key for a span from trace ID and span ID
func (p *SpanReconstructionProcessor) getSpanKeyFromIDs(traceID pcommon.TraceID, spanID pcommon.SpanID) string {
	return traceID.String() + ":" + spanID.String()
}

// getResourceKey creates a unique key for a resource
func (p *SpanReconstructionProcessor) getResourceKey(resource ptrace.ResourceSpans) string {
	// Simple hash of resource attributes
	attrs := resource.Resource().Attributes().AsRaw()
	if serviceName, ok := attrs["service.name"].(string); ok {
		return serviceName
	}
	return "unknown"
}

// getScopeKey generates a unique key for a scope
func (p *SpanReconstructionProcessor) getScopeKey(scope ptrace.ScopeSpans) string {
	return scope.Scope().Name() + ":" + scope.Scope().Version()
}

// countTotalSpans counts the total number of spans in a traces object
func (p *SpanReconstructionProcessor) countTotalSpans(td ptrace.Traces) int {
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

// Helper: isServerSpan
func isServerSpan(span ptrace.Span) bool {
	return span.Kind() == ptrace.SpanKindServer
}

// Helper: isClientSpan
func isClientSpan(span ptrace.Span) bool {
	return span.Kind() == ptrace.SpanKindClient
}

// Helper: getServiceName extracts the service name from a resource
func getServiceName(resource pcommon.Resource) string {
	if serviceName, exists := resource.Attributes().Get("service.name"); exists {
		return serviceName.Str()
	}
	return "unknown_service"
}

/*
// createTracepointFromSpanEvent creates a tracepoint from a span start or end event
func createTracepointFromSpanEvent(span ptrace.Span, eventType string, resource pcommon.Resource) *Tracepoint {
	spanID := span.SpanID().String()
	// traceID := span.TraceID().String()

	// Determine the tracepoint ID
	tracepointID := spanID + "_" + eventType

	// Create operation name
	serviceName := getServiceName(resource)
	operationName := serviceName + "_" + span.Name() + "_" + eventType

	// // Determine server span ID
	// var serverSpanID string
	// if isServerSpan(span) {
	// 	serverSpanID = spanID
	// } else if isClientSpan(span) {
	// 	// For client spans, use the parent span ID as the server span ID
	// 	serverSpanID = span.ParentSpanID().String()
	// } else {
	// 	// For other span kinds, use the span ID itself
	// 	serverSpanID = spanID
	// }

	// Determine timestamp based on event type
	var timestamp time.Time
	switch eventType {
	case "START":
		timestamp = span.StartTimestamp().AsTime()
	case "END":
		timestamp = span.EndTimestamp().AsTime()
	default:
		// Fallback to current time if event type is unknown
		timestamp = time.Now()
	}

	return &Tracepoint{
		ID:        tracepointID,
		Timestamp: timestamp,
		Name:      operationName,
		// TraceID:      traceID,
		// ServerSpanID: serverSpanID,
		// Service:      serviceName,
	}
}

// newTracepointChainState creates a new tracepointChainState from a server-side START event tracepoint
func newTracepointChainState(startSpan ptrace.Span, resource pcommon.Resource) *tracepointChainState {
	startTp := createTracepointFromSpanEvent(startSpan, "START", resource)
	serviceName := getServiceName(resource)

	return &tracepointChainState{
		lastTp:             startTp,
		traceID:            startSpan.TraceID(),
		serverSpanID:       startSpan.SpanID(),
		serviceName:        serviceName,
		onService:          true, // At START, server is on-service (no client spans yet)
		openClientSpans:    make(map[string]bool),
		numOpenClientSpans: 0,
		curSegment: &Segment{
			From:             startTp.ID,
			To:               "",
			StartTime:        startTp.Timestamp,
			EndTime:          time.Time{},
			Service:          serviceName,
			OnService:        true,
			ClientOperations: []string{},
			TraceID:          startSpan.TraceID().String(),
			ServerSpanID:     startSpan.SpanID().String(),
		}, // No segment started yet
	}
}

// processSpanStart processes a start event and returns a segments if one should be created
func (tcs *tracepointChainState) processSpanStart(
	span ptrace.Span,
	resource pcommon.Resource,
) []*Segment {
	toReturn := []*Segment{}

	if tcs.onService {
		newTracepoint := createTracepointFromSpanEvent(span, "START", resource)

		tcs.curSegment.To = newTracepoint.ID
		tcs.curSegment.EndTime = newTracepoint.Timestamp

		toReturn = append(toReturn, tcs.curSegment)

		tcs.lastTp = newTracepoint
		tcs.curSegment = &Segment{
			From:      newTracepoint.ID,
			To:        "",
			StartTime: newTracepoint.Timestamp,
			EndTime:   time.Time{},
			Service:   tcs.serviceName,
			OnService: false,
			ClientOperations: []string{
				span.Name(),
			},
			TraceID:      tcs.traceID.String(),
			ServerSpanID: tcs.serverSpanID.String(),
		}

		// tcs.openClientSpans[span.SpanID().String()] = true
		tcs.numOpenClientSpans++

		tcs.onService = false
	} else {
		// tcs.openClientSpans[span.SpanID().String()] = true
		tcs.numOpenClientSpans++

		tcs.curSegment.ClientOperations = append(
			tcs.curSegment.ClientOperations,
			span.Name(),
		)
	}

	return toReturn
}

// processSpanEnd processes a end event and returns a segments if one should be created
func (tcs *tracepointChainState) processSpanEnd(
	span ptrace.Span,
	resource pcommon.Resource,
) []*Segment {
	toReturn := []*Segment{}

	if tcs.onService {
		newTracepoint := createTracepointFromSpanEvent(span, "END", resource)
		tcs.curSegment.To = newTracepoint.ID
		tcs.curSegment.EndTime = newTracepoint.Timestamp

		toReturn = append(toReturn, tcs.curSegment)
		toReturn = append(toReturn, &Segment{})
		// We can stop here as this is the last segment so we do not need
		// to set up any future segment; extra segment appended to denote
		// that tcs should be destroyed
	} else {
		tcs.numOpenClientSpans--

		if tcs.numOpenClientSpans == 0 {
			// Flush the current segment
			newTracepoint := createTracepointFromSpanEvent(span, "END", resource)
			tcs.curSegment.To = newTracepoint.ID
			tcs.curSegment.EndTime = newTracepoint.Timestamp

			toReturn = append(toReturn, tcs.curSegment)

			tcs.lastTp = newTracepoint
			tcs.curSegment = &Segment{
				From:             newTracepoint.ID,
				To:               "",
				StartTime:        newTracepoint.Timestamp,
				EndTime:          time.Time{},
				Service:          tcs.serviceName,
				OnService:        true,
				ClientOperations: []string{},
				TraceID:          tcs.traceID.String(),
				ServerSpanID:     tcs.serverSpanID.String(),
			}
			tcs.onService = true
		}
	}

	return toReturn
}
*/
