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

// Tracepoint represents a state change moment in service execution
type Tracepoint struct {
	ID           string // e.g., "span_123_START" or "span_456_END"
	Timestamp    time.Time
	Name         string // e.g., "FrontendService_GetCart_START" or "CartService_AddItem_END"
	TraceID      string
	ServerSpanID string
}

// Segment represents a work unit (on-service or off-service execution)
type Segment struct {
	From             string   // Tracepoint ID where segment starts
	To               string   // Tracepoint ID where segment ends
	Service          string   // Service name
	OnService        bool     // true for on-service, false for off-service
	ClientOperations []string // e.g., ["CartService_AddItem", "UserService_GetUser"]
}

// reconstructedSpan represents a span that is currently being reconstructed
type reconstructedSpan struct {
	span     ptrace.Span
	resource ptrace.ResourceSpans
	scope    ptrace.ScopeSpans
	lastSeen time.Time
}

// spanReconstructionProcessor implements the span reconstruction logic
type spanReconstructionProcessor struct {
	logger *zap.Logger
	config *Config

	// Span storage - separate open and closed spans
	openSpans   map[string]*reconstructedSpan
	closedSpans map[string]*reconstructedSpan
	totalSpans  int
	spansMutex  sync.RWMutex

	// Cleanup ticker
	cleanupTicker *time.Ticker
	stopCleanup   chan struct{}

	// Parent-to-children map
	parentToChildren map[string][]string // server span key -> list of client span keys

	// Tracepoints and segments
	tracepoints map[string]*Tracepoint
	segments    map[string]*Segment
}

// newSpanReconstructionProcessor creates a new span reconstruction processor
func newSpanReconstructionProcessor(logger *zap.Logger, config *Config) *spanReconstructionProcessor {
	return &spanReconstructionProcessor{
		logger:           logger,
		config:           config,
		openSpans:        make(map[string]*reconstructedSpan),
		closedSpans:      make(map[string]*reconstructedSpan),
		tracepoints:      make(map[string]*Tracepoint),
		segments:         make(map[string]*Segment),
		stopCleanup:      make(chan struct{}),
		parentToChildren: make(map[string][]string),
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
			zap.String("service_name", getServiceName(resourceSpans.Resource())),
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
		zap.Int("open_spans_count", len(p.openSpans)),
		zap.Int("closed_spans_count", len(p.closedSpans)),
		zap.Int("total_spans", p.totalSpans))

	return outputTraces, nil
}

// processSpan processes a single span and determines if it's a start, end, or log event
func (p *spanReconstructionProcessor) processSpan(span ptrace.Span, outputScope ptrace.ScopeSpans, outputResource ptrace.ResourceSpans) {
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
func (p *spanReconstructionProcessor) handleSpanStart(span ptrace.Span, spanKey string, outputResource ptrace.ResourceSpans, outputScope ptrace.ScopeSpans) {
	p.spansMutex.Lock()
	defer p.spansMutex.Unlock()

	p.logger.Info("🟢 INFO: Handling span START event",
		zap.String("trace_id", span.TraceID().String()),
		zap.String("span_id", span.SpanID().String()),
		zap.String("span_name", span.Name()),
		zap.String("span_kind", span.Kind().String()),
		zap.Int("current_open_spans", len(p.openSpans)),
		zap.Int("current_closed_spans", len(p.closedSpans)),
		zap.Int("max_active_spans", p.config.MaxActiveSpans))

	// Check if span already exists
	if existingSpan, exists := p.openSpans[spanKey]; exists {
		p.logger.Warn("🔴 WARN: Span START event for already open span - updating",
			zap.String("trace_id", span.TraceID().String()),
			zap.String("span_id", span.SpanID().String()),
			zap.Time("existing_last_seen", existingSpan.lastSeen))
	}

	// For client spans, check parent-child relationship BEFORE adding to openSpans
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

	// Create new open span
	openSpan := &reconstructedSpan{
		span:     span,
		resource: outputResource,
		scope:    outputScope,
		lastSeen: time.Now(),
	}

	p.openSpans[spanKey] = openSpan
	p.totalSpans++

	// Create tracepoint for span start event
	tracepoint := createTracepointFromSpanEvent(span, "START", outputResource.Resource())
	p.tracepoints[tracepoint.ID] = tracepoint

	p.logger.Info("🟢 INFO: Created new open span",
		zap.String("trace_id", span.TraceID().String()),
		zap.String("span_id", span.SpanID().String()),
		zap.String("span_name", span.Name()),
		zap.Int("total_open_spans", len(p.openSpans)),
		zap.Int("total_closed_spans", len(p.closedSpans)),
		zap.Int("total_spans", p.totalSpans))

	// Check if we need to evict spans due to memory pressure (asynchronously)
	go p.evictUntilUnderLimit(true)
}

// establishParentChildRelationship establishes parent-child relationship for client spans
// Returns true if a parent was found and relationship was established, false otherwise
func (p *spanReconstructionProcessor) establishParentChildRelationship(clientSpan ptrace.Span, clientSpanKey string) bool {
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

	// Check open spans for the parent server span
	if _, exists := p.openSpans[parentServerKey]; exists {
		parentSpan := p.openSpans[parentServerKey]
		if isServerSpan(parentSpan.span) {
			// Establish the relationship
			if p.parentToChildren[parentServerKey] == nil {
				p.parentToChildren[parentServerKey] = make([]string, 0)
			}
			p.parentToChildren[parentServerKey] = append(p.parentToChildren[parentServerKey], clientSpanKey)

			p.logger.Info("🟢 INFO: Established parent-child relationship",
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
func (p *spanReconstructionProcessor) handleSpanEnd(span ptrace.Span, spanKey string, _ ptrace.ScopeSpans) {
	p.logger.Debug("🟠 DEBUG: handleSpanEnd - entering", zap.String("trace_id", span.TraceID().String()), zap.String("span_id", span.SpanID().String()))
	p.spansMutex.Lock()
	defer p.spansMutex.Unlock()

	p.logger.Info("🟢 INFO: Handling span END event",
		zap.String("trace_id", span.TraceID().String()),
		zap.String("span_id", span.SpanID().String()),
		zap.String("span_name", span.Name()),
		zap.String("span_kind", span.Kind().String()))

	openSpan, exists := p.openSpans[spanKey]
	if !exists {
		p.logger.Warn("🔴 WARN: Received span END event for unknown span - no matching START found",
			zap.String("trace_id", span.TraceID().String()),
			zap.String("span_id", span.SpanID().String()),
			zap.Int("available_open_spans", len(p.openSpans)))
		p.logger.Debug("🟠 DEBUG: handleSpanEnd - exiting (span not found)")
		return
	}

	// Complete the span by copying end time
	openSpan.span.SetEndTimestamp(span.EndTimestamp())
	openSpan.lastSeen = time.Now()

	// Move span from open to closed
	p.closedSpans[spanKey] = openSpan
	delete(p.openSpans, spanKey)
	// totalSpans stays the same since we're just moving from one collection to another

	// Create tracepoint for span end event
	tracepoint := createTracepointFromSpanEvent(span, "END", openSpan.resource.Resource())
	p.tracepoints[tracepoint.ID] = tracepoint

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
func (p *spanReconstructionProcessor) handleLogEvent(span ptrace.Span, spanKey string, _ ptrace.ScopeSpans) {
	p.spansMutex.Lock()
	defer p.spansMutex.Unlock()

	p.logger.Info("🟢 INFO: Handling span LOG event",
		zap.String("trace_id", span.TraceID().String()),
		zap.String("span_id", span.SpanID().String()),
		zap.Int("events_count", span.Events().Len()))

	openSpan, exists := p.openSpans[spanKey]
	if !exists {
		p.logger.Warn("🔴 WARN: Received LOG event for unknown span - no matching START found",
			zap.String("trace_id", span.TraceID().String()),
			zap.String("span_id", span.SpanID().String()),
			zap.Int("available_open_spans", len(p.openSpans)))
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
func (p *spanReconstructionProcessor) addCompletedSpans(outputTraces ptrace.Traces) int {
	p.spansMutex.Lock()
	defer p.spansMutex.Unlock()

	// Group completed spans by resource and scope
	completedSpans := make(map[string]map[string][]*reconstructedSpan)

	completedCount := 0

	// Process spans from closedSpans (these are already complete)
	for _, span := range p.closedSpans {
		resourceKey := p.getResourceKey(span.resource)
		scopeKey := p.getScopeKey(span.scope)

		if completedSpans[resourceKey] == nil {
			completedSpans[resourceKey] = make(map[string][]*reconstructedSpan)
		}
		if completedSpans[resourceKey][scopeKey] == nil {
			completedSpans[resourceKey][scopeKey] = make([]*reconstructedSpan, 0)
		}

		completedSpans[resourceKey][scopeKey] = append(completedSpans[resourceKey][scopeKey], span)
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

	// Note: We don't remove spans from closedSpans after emitting them
	// They stay in the cache until they expire via TTL or are evicted due to memory pressure

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

// cleanupExpiredSpans removes expired server spans and their client children
func (p *spanReconstructionProcessor) cleanupExpiredSpans() {
	p.spansMutex.Lock()
	defer p.spansMutex.Unlock()

	now := time.Now()
	expiredServerKeys := []string{}

	// Find expired server spans
	for key, span := range p.closedSpans {
		if isServerSpan(span.span) && now.Sub(span.lastSeen) > p.config.SpanTTL {
			expiredServerKeys = append(expiredServerKeys, key)
		}
	}

	// For each expired server span, remove it and all its client children
	totalRemoved := 0
	for _, serverKey := range expiredServerKeys {
		// Remove the server span
		delete(p.closedSpans, serverKey)
		totalRemoved++

		// Remove all direct client children
		for _, childKey := range p.parentToChildren[serverKey] {
			if _, exists := p.closedSpans[childKey]; exists {
				delete(p.closedSpans, childKey)
				totalRemoved++
			}
		}

		// Clean up the parent-child relationship
		delete(p.parentToChildren, serverKey)
	}

	p.totalSpans -= totalRemoved
	if len(expiredServerKeys) > 0 {
		p.logger.Info("🟢 INFO: Cleaned up expired server spans and their children",
			zap.Int("expired_server_spans", len(expiredServerKeys)),
			zap.Int("total_spans_removed", totalRemoved))
	}

	// Check if we need additional eviction after TTL cleanup
	p.evictUntilUnderLimit(false)
}

// evictUntilUnderLimit evicts spans until totalSpans is under the configured limit
// Prioritizes evicting oldest closed server spans first, then oldest open server spans
func (p *spanReconstructionProcessor) evictUntilUnderLimit(toLock bool) {
	if toLock {
		p.spansMutex.Lock()
		defer p.spansMutex.Unlock()
	}

	if p.totalSpans <= p.config.MaxActiveSpans {
		return
	}

	p.logger.Warn("🔴 WARN: Total spans exceeds limit - starting eviction",
		zap.Int("current_total", p.totalSpans),
		zap.Int("max_limit", p.config.MaxActiveSpans),
	)

	// First, try to evict from closed spans
	p.logger.Info("🟢 INFO: Attempting to evict from closed server spans")
	evictedFromClosed := p.evictOldestClosedServerSpans()

	// If still over limit, evict from open spans
	if p.totalSpans > p.config.MaxActiveSpans {
		p.logger.Info("🟢 INFO: Attempting to evict from open server spans")
		evictedFromOpen := p.evictOldestOpenServerSpans()

		p.logger.Info("🟢 INFO: Eviction completed",
			zap.Int("evicted_from_closed", evictedFromClosed),
			zap.Int("evicted_from_open", evictedFromOpen),
			zap.Int("final_total", p.totalSpans),
		)
	} else {
		p.logger.Info("🟢 INFO: Eviction completed from closed spans only",
			zap.Int("evicted_from_closed", evictedFromClosed),
			zap.Int("final_total", p.totalSpans),
		)
	}
}

// evictOldestClosedServerSpans evicts the oldest closed server spans and their children
// Returns the number of spans evicted
func (p *spanReconstructionProcessor) evictOldestClosedServerSpans() int {
	// Find all closed server spans and sort by lastSeen (oldest first)
	type serverSpanInfo struct {
		key      string
		lastSeen time.Time
	}

	var closedServerSpans []serverSpanInfo
	for key, span := range p.closedSpans {
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
		if p.totalSpans <= p.config.MaxActiveSpans {
			break
		}

		evicted := p.evictServerSpanAndChildren(serverInfo.key, true)
		totalEvicted += evicted
	}

	return totalEvicted
}

// evictOldestOpenServerSpans evicts the oldest open server spans and their children
// Returns the number of spans evicted
func (p *spanReconstructionProcessor) evictOldestOpenServerSpans() int {
	// Find all open server spans and sort by lastSeen (oldest first)
	type serverSpanInfo struct {
		key      string
		lastSeen time.Time
	}

	var openServerSpans []serverSpanInfo
	for key, span := range p.openSpans {
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
		if p.totalSpans <= p.config.MaxActiveSpans {
			break
		}

		evicted := p.evictServerSpanAndChildren(serverInfo.key, false)
		totalEvicted += evicted
	}

	return totalEvicted
}

// evictServerSpanAndChildren evicts a server span and all its children
// fromClosed indicates whether the server span is in closedSpans (true) or openSpans (false)
// Returns the number of spans evicted
func (p *spanReconstructionProcessor) evictServerSpanAndChildren(serverKey string, fromClosed bool) int {
	evictedCount := 0

	// Remove the server span from its collection
	if fromClosed {
		if _, exists := p.closedSpans[serverKey]; exists {
			delete(p.closedSpans, serverKey)
			evictedCount++
		}
	} else {
		if _, exists := p.openSpans[serverKey]; exists {
			delete(p.openSpans, serverKey)
			evictedCount++
		}
	}

	// Gather child keys for logging
	childKeys := p.parentToChildren[serverKey]
	childEvicted := []string{}

	// Remove all children from both collections
	for _, childKey := range childKeys {
		// Check closedSpans
		if _, exists := p.closedSpans[childKey]; exists {
			delete(p.closedSpans, childKey)
			evictedCount++
			childEvicted = append(childEvicted, childKey+" (closed)")
		}
		// Check openSpans
		if _, exists := p.openSpans[childKey]; exists {
			delete(p.openSpans, childKey)
			evictedCount++
			childEvicted = append(childEvicted, childKey+" (open)")
		}
	}

	// Clean up the parent-child relationship
	delete(p.parentToChildren, serverKey)

	// Update total spans counter
	p.totalSpans -= evictedCount

	p.logger.Warn("🔴 EVICTION: Server span and children evicted",
		zap.String("server_key", serverKey),
		zap.Bool("from_closed", fromClosed),
		zap.Strings("child_keys", childKeys),
		zap.Strings("child_evicted", childEvicted),
		zap.Int("spans_evicted", evictedCount),
		zap.Int("total_spans_after", p.totalSpans),
	)

	return evictedCount
}

// getSpanKey creates a unique key for a span
func (p *spanReconstructionProcessor) getSpanKey(span ptrace.Span) string {
	return span.TraceID().String() + ":" + span.SpanID().String()
}

// getSpanKeyFromIDs creates a unique key for a span from trace ID and span ID
func (p *spanReconstructionProcessor) getSpanKeyFromIDs(traceID pcommon.TraceID, spanID pcommon.SpanID) string {
	return traceID.String() + ":" + spanID.String()
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

// Helper: isServerSpan
func isServerSpan(span ptrace.Span) bool {
	return span.Kind() == ptrace.SpanKindServer
}

// Helper: isClientSpan
func isClientSpan(span ptrace.Span) bool {
	return span.Kind() == ptrace.SpanKindClient
}

// createTracepointFromSpanEvent creates a tracepoint from a span start or end event
func createTracepointFromSpanEvent(span ptrace.Span, eventType string, resource pcommon.Resource) *Tracepoint {
	spanID := span.SpanID().String()
	traceID := span.TraceID().String()

	// Determine the tracepoint ID
	tracepointID := spanID + "_" + eventType

	// Create operation name
	serviceName := getServiceName(resource)
	operationName := serviceName + "_" + span.Name() + "_" + eventType

	// Determine server span ID
	var serverSpanID string
	if isServerSpan(span) {
		serverSpanID = spanID
	} else if isClientSpan(span) {
		// For client spans, use the parent span ID as the server span ID
		serverSpanID = span.ParentSpanID().String()
	} else {
		// For other span kinds, use the span ID itself
		serverSpanID = spanID
	}

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
		ID:           tracepointID,
		Timestamp:    timestamp,
		Name:         operationName,
		TraceID:      traceID,
		ServerSpanID: serverSpanID,
	}
}

// getServiceName extracts the service name from a resource
func getServiceName(resource pcommon.Resource) string {
	if serviceName, exists := resource.Attributes().Get("service.name"); exists {
		return serviceName.Str()
	}
	return "unknown_service"
}
