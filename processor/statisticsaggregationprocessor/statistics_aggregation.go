// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package statisticsaggregationprocessor

import (
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

// SpanWithResource holds a span and its associated resource
type SpanWithResource struct {
	Span     ptrace.Span
	Resource ptrace.ResourceSpans
}

// SpanBuffer holds unprocessed spans with their resources
type SpanBuffer struct {
	mu    sync.Mutex
	spans []SpanWithResource
}

// FeatureCounters tracks numeric counters for a specific feature
type FeatureCounters struct {
	Victims   int64 // Counter for "Victims" category
	Survivors int64 // Counter for "Survivors" category
}

// StatisticsMap holds counters for all features by feature ID
type StatisticsMap struct {
	mu         sync.RWMutex
	counters   map[string]*FeatureCounters // featureID -> counters
	totalCount int64                       // Total count across all features
}

// FeatureExtractionResult represents the result of feature extraction from a span
type FeatureExtractionResult struct {
	FeatureID string // The extracted feature ID
	IsVictim  bool   // true if this span represents a "victim", false if "survivor"
}

// FeatureExtractor is a function type that extracts feature information from a span and its resource
type FeatureExtractor func(span ptrace.Span, resource ptrace.ResourceSpans) []FeatureExtractionResult

// statisticsAggregationProcessor processes traces and aggregates statistics
type statisticsAggregationProcessor struct {
	logger       *zap.Logger
	config       *Config
	nextConsumer consumer.Traces

	// Event buffer for handling spans asynchronously
	spanBuffer *SpanBuffer

	// Statistics tracking
	statistics *StatisticsMap
	spanCount  int64
	startTime  time.Time
	lastExport time.Time

	// Feature extraction
	featureExtractor FeatureExtractor

	// Worker control
	stopWorker chan struct{}
	workerWg   sync.WaitGroup
}

// newStatisticsAggregationProcessor creates a new statistics aggregation processor
func newStatisticsAggregationProcessor(
	logger *zap.Logger,
	config *Config,
	nextConsumer consumer.Traces,
	featureExtractor FeatureExtractor,
) *statisticsAggregationProcessor {
	return &statisticsAggregationProcessor{
		logger:       logger,
		config:       config,
		nextConsumer: nextConsumer,
		spanBuffer: &SpanBuffer{
			spans: make([]SpanWithResource, 0),
		},
		statistics: &StatisticsMap{
			counters: make(map[string]*FeatureCounters),
		},
		featureExtractor: featureExtractor,
		startTime:        time.Now(),
		lastExport:       time.Now(),
		stopWorker:       make(chan struct{}),
	}
}

// start initializes the processor
func (sap *statisticsAggregationProcessor) start(ctx context.Context, host component.Host) error {
	sap.logger.Info("Starting statistics aggregation processor",
		zap.Duration("export_window", sap.config.ExportWindow))

	// Start worker goroutine
	sap.workerWg.Add(1)
	go sap.workerLoop()

	// Start per-second status logging goroutine
	sap.workerWg.Add(1)
	go sap.statusLoggingLoop()

	return nil
}

// shutdown cleans up the processor
func (sap *statisticsAggregationProcessor) shutdown(ctx context.Context) error {
	sap.logger.Info("Shutting down statistics aggregation processor")

	// Stop worker
	close(sap.stopWorker)
	sap.workerWg.Wait()

	return nil
}

// processTraces processes incoming traces and adds them to the event buffer
func (sap *statisticsAggregationProcessor) processTraces(ctx context.Context, td ptrace.Traces) (ptrace.Traces, error) {
	sap.logger.Debug("Processing traces for statistics aggregation",
		zap.Int("resource_spans_count", td.ResourceSpans().Len()))

	// Collect all spans with their resources
	var allSpansWithResources []SpanWithResource
	for i := 0; i < td.ResourceSpans().Len(); i++ {
		resourceSpans := td.ResourceSpans().At(i)
		for j := 0; j < resourceSpans.ScopeSpans().Len(); j++ {
			scopeSpans := resourceSpans.ScopeSpans().At(j)
			for k := 0; k < scopeSpans.Spans().Len(); k++ {
				span := scopeSpans.Spans().At(k)
				allSpansWithResources = append(allSpansWithResources, SpanWithResource{
					Span:     span,
					Resource: resourceSpans,
				})
			}
		}
	}

	// Add spans to buffer
	sap.addSpansToBuffer(allSpansWithResources)

	sap.logger.Debug("Added spans to buffer", zap.Int("spans_added", len(allSpansWithResources)))

	// Pass traces to next consumer unchanged
	return td, sap.nextConsumer.ConsumeTraces(ctx, td)
}

// addSpansToBuffer adds spans with resources to the buffer
func (sap *statisticsAggregationProcessor) addSpansToBuffer(spansWithResources []SpanWithResource) {
	sap.spanBuffer.mu.Lock()
	defer sap.spanBuffer.mu.Unlock()
	sap.spanBuffer.spans = append(sap.spanBuffer.spans, spansWithResources...)
}

// workerLoop processes events from the buffer
func (sap *statisticsAggregationProcessor) workerLoop() {
	defer sap.workerWg.Done()

	for {
		select {
		case <-sap.stopWorker:
			return
		default:
			sap.processSpanBuffer()
		}
	}
}

// processSpanBuffer processes all spans in the buffer
func (sap *statisticsAggregationProcessor) processSpanBuffer() {
	// Get current spans and clear buffer
	sap.spanBuffer.mu.Lock()
	currentSpansWithResources := sap.spanBuffer.spans[:]
	sap.spanBuffer.spans = make([]SpanWithResource, 0)
	sap.spanBuffer.mu.Unlock()

	if len(currentSpansWithResources) == 0 {
		return
	}

	sap.logger.Debug("Processing span buffer", zap.Int("span_count", len(currentSpansWithResources)))

	// Process each span with its resource
	for _, spanWithResource := range currentSpansWithResources {
		sap.processSpan(spanWithResource.Span, spanWithResource.Resource)
	}

	// Check if it's time to export statistics
	now := time.Now()
	if now.Sub(sap.lastExport) >= sap.config.ExportWindow {
		sap.exportStatistics()
		sap.lastExport = now
	}
}

// statusLoggingLoop logs current statistics every second
func (sap *statisticsAggregationProcessor) statusLoggingLoop() {
	defer sap.workerWg.Done()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			sap.logCurrentStatus()
		case <-sap.stopWorker:
			return
		}
	}
}

// logCurrentStatus logs the current statistics without resetting counters
func (sap *statisticsAggregationProcessor) logCurrentStatus() {
	uptime := time.Since(sap.startTime)
	spansPerSecond := float64(sap.spanCount) / uptime.Seconds()

	// Get snapshot of current statistics
	statisticsSnapshot := sap.getStatisticsSnapshot()

	sap.logger.Info("Statistics aggregation status",
		zap.Int64("total_spans_processed", sap.spanCount),
		zap.Duration("uptime", uptime),
		zap.Float64("spans_per_second", spansPerSecond),
		zap.Int("services_tracked", len(statisticsSnapshot)))

	// Log current counts for each service
	for serviceName, counters := range statisticsSnapshot {
		sap.logger.Info("Service status",
			zap.String("service_name", serviceName),
			zap.Int64("current_victim_count", counters.Victims))
	}
}

// processSpan processes a single span for statistics aggregation
func (sap *statisticsAggregationProcessor) processSpan(span ptrace.Span, resource ptrace.ResourceSpans) {
	sap.spanCount++

	// Extract features from the span and resource
	if sap.featureExtractor != nil {
		features := sap.featureExtractor(span, resource)

		// Process each extracted feature - only count victims
		for _, feature := range features {
			if feature.IsVictim {
				sap.incrementVictimCounter(feature.FeatureID)
				sap.logger.Debug("Incremented victim counter",
					zap.String("service_name", feature.FeatureID),
					zap.String("span_name", span.Name()),
					zap.String("trace_id", span.TraceID().String()))
			}
			// Ignore survivors for now
		}
	}
}

// incrementVictimCounter increments the victim counter for a specific feature
func (sap *statisticsAggregationProcessor) incrementVictimCounter(featureID string) {
	sap.statistics.mu.Lock()
	defer sap.statistics.mu.Unlock()

	if sap.statistics.counters[featureID] == nil {
		sap.statistics.counters[featureID] = &FeatureCounters{}
	}
	sap.statistics.counters[featureID].Victims++
	sap.statistics.totalCount++
}

// incrementSurvivorCounter increments the survivor counter for a specific feature
func (sap *statisticsAggregationProcessor) incrementSurvivorCounter(featureID string) {
	sap.statistics.mu.Lock()
	defer sap.statistics.mu.Unlock()

	if sap.statistics.counters[featureID] == nil {
		sap.statistics.counters[featureID] = &FeatureCounters{}
	}
	sap.statistics.counters[featureID].Survivors++
	sap.statistics.totalCount++
}

// getStatisticsSnapshot returns a snapshot of current statistics
func (sap *statisticsAggregationProcessor) getStatisticsSnapshot() map[string]*FeatureCounters {
	sap.statistics.mu.RLock()
	defer sap.statistics.mu.RUnlock()

	snapshot := make(map[string]*FeatureCounters)
	for featureID, counters := range sap.statistics.counters {
		snapshot[featureID] = &FeatureCounters{
			Victims:   counters.Victims,
			Survivors: counters.Survivors,
		}
	}
	return snapshot
}

// exportStatistics exports the aggregated statistics
func (sap *statisticsAggregationProcessor) exportStatistics() {
	uptime := time.Since(sap.startTime)
	spansPerSecond := float64(sap.spanCount) / uptime.Seconds()

	// Get snapshot of current statistics
	statisticsSnapshot := sap.getStatisticsSnapshot()

	sap.logger.Info("Statistics aggregation completed",
		zap.Int64("total_spans_processed", sap.spanCount),
		zap.Duration("uptime", uptime),
		zap.Float64("spans_per_second", spansPerSecond),
		zap.Duration("export_window", sap.config.ExportWindow),
		zap.Int("services_tracked", len(statisticsSnapshot)))

	// Log detailed statistics for each service (feature)
	for serviceName, counters := range statisticsSnapshot {
		sap.logger.Info("Service statistics",
			zap.String("service_name", serviceName),
			zap.Int64("victim_count", counters.Victims))
	}

	// TODO: Implement actual statistics export logic here
}

// DefaultFeatureExtractor is a simple feature extractor for testing
func DefaultFeatureExtractor(span ptrace.Span, resource ptrace.ResourceSpans) []FeatureExtractionResult {
	var results []FeatureExtractionResult

	// Simple example: extract based on span name
	spanName := span.Name()
	if spanName != "" {
		// Example logic: spans with "error" in name are victims, others are survivors
		if contains(spanName, "error") || contains(spanName, "fail") {
			results = append(results, FeatureExtractionResult{
				FeatureID: "span_name_analysis",
				IsVictim:  true,
			})
		} else {
			results = append(results, FeatureExtractionResult{
				FeatureID: "span_name_analysis",
				IsVictim:  false,
			})
		}
	}

	return results
}

// ServiceBasedFeatureExtractor extracts features based on service name from resource attributes
func ServiceBasedFeatureExtractor(span ptrace.Span, resource ptrace.ResourceSpans) []FeatureExtractionResult {
	var results []FeatureExtractionResult

	// Extract service name from resource attributes
	resourceAttrs := resource.Resource().Attributes()
	if serviceName, exists := resourceAttrs.AsRaw()["service.name"]; exists {
		if serviceNameStr, ok := serviceName.(string); ok {
			// Use service name as feature key and count as victim
			results = append(results, FeatureExtractionResult{
				FeatureID: serviceNameStr, // Service name is the feature key
				IsVictim:  true,           // Always count as victim
			})
		}
	}

	return results
}

// UpstreamIPBasedFeatureExtractor extracts features based on upstream IP
func UpstreamIPBasedFeatureExtractor(span ptrace.Span, resource ptrace.ResourceSpans) []FeatureExtractionResult {
	var results []FeatureExtractionResult

	// Extract upstream IP from span attributes (this is set by the lookup routing processor)
	spanAttrs := span.Attributes()
	if upstreamIP, exists := spanAttrs.AsRaw()["upstream.ip"]; exists {
		if upstreamIPStr, ok := upstreamIP.(string); ok {
			// Example logic: unknown upstream IPs are victims, known ones are survivors
			if upstreamIPStr == "unknown-upstream" {
				results = append(results, FeatureExtractionResult{
					FeatureID: "upstream_ip_analysis",
					IsVictim:  true,
				})
			} else {
				results = append(results, FeatureExtractionResult{
					FeatureID: "upstream_ip_analysis",
					IsVictim:  false,
				})
			}
		}
	}

	return results
}

// TraceIDBasedFeatureExtractor extracts features based on trace ID patterns
func TraceIDBasedFeatureExtractor(span ptrace.Span, resource ptrace.ResourceSpans) []FeatureExtractionResult {
	var results []FeatureExtractionResult

	// Extract trace ID and analyze patterns
	traceID := span.TraceID().String()
	if traceID != "" {
		// Example logic: trace IDs ending in certain patterns might indicate issues
		// This is just an example - you could implement more sophisticated logic
		if len(traceID) >= 4 {
			lastFour := traceID[len(traceID)-4:]
			// Simple heuristic: if last 4 chars contain many zeros, might be problematic
			zeroCount := 0
			for _, char := range lastFour {
				if char == '0' {
					zeroCount++
				}
			}

			if zeroCount >= 2 {
				results = append(results, FeatureExtractionResult{
					FeatureID: "trace_id_pattern_analysis",
					IsVictim:  true,
				})
			} else {
				results = append(results, FeatureExtractionResult{
					FeatureID: "trace_id_pattern_analysis",
					IsVictim:  false,
				})
			}
		}
	}

	return results
}

// contains is a helper function to check if a string contains a substring
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		(len(s) > len(substr) && (s[:len(substr)] == substr ||
			s[len(s)-len(substr):] == substr ||
			containsSubstring(s, substr))))
}

// containsSubstring checks if substr is contained in s
func containsSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
