// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package priorityprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/priorityprocessor"

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

// SpanWithContext holds a span with its resource and scope context
type SpanWithContext struct {
	Span     ptrace.Span
	Resource ptrace.ResourceSpans
	Scope    ptrace.ScopeSpans
}

// PriorityBuffer holds unprocessed spans for a priority level
type PriorityBuffer struct {
	mu    sync.Mutex
	spans []SpanWithContext
}

type priorityProcessor struct {
	config       *Config
	logger       *zap.Logger
	nextConsumer consumer.Traces

	// Memory tracking
	totalMemory   uint64
	normalLimit   uint64
	burstLimit    uint64
	lastCheck     time.Time
	memoryState   sync.RWMutex
	currentMemory uint64

	// Background monitoring
	ticker *time.Ticker
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Priority buffers
	highPriorityBuffer *PriorityBuffer
	lowPriorityBuffer  *PriorityBuffer

	// Worker control
	stopHighPriorityWorker chan struct{}
	stopLowPriorityWorker  chan struct{}
	highPriorityWorkerWg   sync.WaitGroup
	lowPriorityWorkerWg    sync.WaitGroup
}

func newProcessor(config *Config, logger *zap.Logger, nextConsumer consumer.Traces) (*priorityProcessor, error) {
	// Get total memory
	totalMemory, err := getTotalMemory()
	if err != nil {
		return nil, fmt.Errorf("failed to get total memory: %w", err)
	}

	// Calculate limits as percentages of total memory
	normalLimit := uint64(config.MemoryLimitPercentage) * totalMemory / 100
	burstLimit := uint64(config.BurstMemoryLimitPercentage) * totalMemory / 100

	logger.Info("Priority processor initialized",
		zap.Uint64("total_memory", totalMemory),
		zap.Uint64("normal_limit", normalLimit),
		zap.Uint64("burst_limit", burstLimit),
		zap.Duration("check_interval", config.CheckInterval))

	return &priorityProcessor{
		config:       config,
		logger:       logger,
		nextConsumer: nextConsumer,
		totalMemory:  totalMemory,
		normalLimit:  normalLimit,
		burstLimit:   burstLimit,
		ticker:       time.NewTicker(config.CheckInterval),
		highPriorityBuffer: &PriorityBuffer{
			spans: make([]SpanWithContext, 0),
		},
		lowPriorityBuffer: &PriorityBuffer{
			spans: make([]SpanWithContext, 0),
		},
		stopHighPriorityWorker: make(chan struct{}),
		stopLowPriorityWorker:  make(chan struct{}),
	}, nil
}

func (p *priorityProcessor) start(ctx context.Context, _ component.Host) error {
	ctx, cancel := context.WithCancel(ctx)
	p.cancel = cancel

	p.wg.Add(1)
	go p.monitorMemory(ctx)

	// Start worker goroutines
	p.highPriorityWorkerWg.Add(1)
	go p.highPriorityWorkerLoop(ctx)

	p.lowPriorityWorkerWg.Add(1)
	go p.lowPriorityWorkerLoop(ctx)

	p.logger.Info("Priority processor started")
	return nil
}

func (p *priorityProcessor) shutdown(_ context.Context) error {
	p.logger.Info("Priority processor shutdown")

	// Stop workers
	close(p.stopHighPriorityWorker)
	p.highPriorityWorkerWg.Wait()

	close(p.stopLowPriorityWorker)
	p.lowPriorityWorkerWg.Wait()

	if p.ticker != nil {
		p.ticker.Stop()
	}
	if p.cancel != nil {
		p.cancel()
	}
	p.wg.Wait()
	return nil
}

func (p *priorityProcessor) monitorMemory(ctx context.Context) {
	defer p.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case <-p.ticker.C:
			p.checkMemory()
		}
	}
}

func (p *priorityProcessor) checkMemory() {
	// Try to get actual container memory usage from cgroups (most accurate)
	var currentMemory uint64
	var err error

	isV2, err := isCGroupV2()
	if err == nil {
		if isV2 {
			currentMemory, err = readCGroupV2MemoryCurrent()
		} else {
			currentMemory, err = readCGroupV1MemoryUsage()
		}
	}

	// Fallback to runtime.MemStats.Sys if cgroup reading fails
	// Sys includes heap, stack, and other runtime allocations (better than Alloc)
	if err != nil || currentMemory == 0 {
		ms := &runtime.MemStats{}
		runtime.ReadMemStats(ms)
		currentMemory = ms.Sys
	}

	p.memoryState.Lock()
	p.currentMemory = currentMemory
	p.memoryState.Unlock()

	p.lastCheck = time.Now()
}

func (p *priorityProcessor) shouldAcceptLowPriority() bool {
	p.memoryState.RLock()
	defer p.memoryState.RUnlock()
	return p.currentMemory < p.normalLimit
}

func (p *priorityProcessor) shouldAcceptHighPriority() bool {
	p.memoryState.RLock()
	defer p.memoryState.RUnlock()
	return p.currentMemory < p.burstLimit
}

func (p *priorityProcessor) processTraces(ctx context.Context, td ptrace.Traces) (ptrace.Traces, error) {
	// Extract all spans with their context
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

	// Check memory bounds
	acceptLow := p.shouldAcceptLowPriority()
	acceptHigh := p.shouldAcceptHighPriority()

	// Route spans to appropriate buffer based on priority and memory
	highPrioritySpans := make([]SpanWithContext, 0)
	lowPrioritySpans := make([]SpanWithContext, 0)
	dropped := 0

	for _, spanWithContext := range allSpansWithContext {
		isHigh := isHighPrioritySpan(spanWithContext.Span)

		if isHigh {
			if acceptHigh {
				highPrioritySpans = append(highPrioritySpans, spanWithContext)
			} else {
				dropped++
			}
		} else {
			if acceptLow {
				lowPrioritySpans = append(lowPrioritySpans, spanWithContext)
			} else {
				dropped++
			}
		}
	}

	// Add spans to buffers
	if len(highPrioritySpans) > 0 {
		p.addToHighPriorityBuffer(highPrioritySpans)
	}
	if len(lowPrioritySpans) > 0 {
		p.addToLowPriorityBuffer(lowPrioritySpans)
	}

	if dropped > 0 {
		p.logger.Debug("Dropped spans",
			zap.Int("dropped", dropped),
			zap.Int("high_priority_accepted", len(highPrioritySpans)),
			zap.Int("low_priority_accepted", len(lowPrioritySpans)))
	}

	// Return empty traces - buffered spans will be output by worker loops
	return ptrace.NewTraces(), nil
}

func isHighPrioritySpan(span ptrace.Span) bool {
	// Check for "prio" attribute
	if val, exists := span.Attributes().Get("prio"); exists && val.Type().String() == "Str" {
		return val.AsString() == "high"
	}
	return false
}

// addToHighPriorityBuffer adds spans to the high priority buffer
func (p *priorityProcessor) addToHighPriorityBuffer(spans []SpanWithContext) {
	p.highPriorityBuffer.mu.Lock()
	p.highPriorityBuffer.spans = append(p.highPriorityBuffer.spans, spans...)
	p.highPriorityBuffer.mu.Unlock()
}

// addToLowPriorityBuffer adds spans to the low priority buffer
func (p *priorityProcessor) addToLowPriorityBuffer(spans []SpanWithContext) {
	p.lowPriorityBuffer.mu.Lock()
	p.lowPriorityBuffer.spans = append(p.lowPriorityBuffer.spans, spans...)
	p.lowPriorityBuffer.mu.Unlock()
}

// highPriorityWorkerLoop processes spans from the high priority buffer
func (p *priorityProcessor) highPriorityWorkerLoop(ctx context.Context) {
	defer p.highPriorityWorkerWg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case <-p.stopHighPriorityWorker:
			return
		default:
			p.processHighPriorityBuffer(ctx)
			time.Sleep(1 * time.Millisecond) // CPU throttling
		}
	}
}

// lowPriorityWorkerLoop processes spans from the low priority buffer
func (p *priorityProcessor) lowPriorityWorkerLoop(ctx context.Context) {
	defer p.lowPriorityWorkerWg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case <-p.stopLowPriorityWorker:
			return
		default:
			p.processLowPriorityBuffer(ctx)
			time.Sleep(1 * time.Millisecond) // CPU throttling
		}
	}
}

// processHighPriorityBuffer processes all spans from the high priority buffer
func (p *priorityProcessor) processHighPriorityBuffer(ctx context.Context) {
	// Get current spans and clear buffer
	p.highPriorityBuffer.mu.Lock()
	currentSpans := p.highPriorityBuffer.spans[:]
	p.highPriorityBuffer.spans = make([]SpanWithContext, 0)
	p.highPriorityBuffer.mu.Unlock()

	if len(currentSpans) == 0 {
		return
	}

	// Convert spans to traces and forward to next consumer
	td := p.buildTracesFromSpans(currentSpans)
	if td.SpanCount() > 0 {
		if err := p.nextConsumer.ConsumeTraces(ctx, td); err != nil {
			p.logger.Error("Failed to consume high priority traces", zap.Error(err))
		}
	}
}

// processLowPriorityBuffer processes all spans from the low priority buffer
func (p *priorityProcessor) processLowPriorityBuffer(ctx context.Context) {
	// Get current spans and clear buffer
	p.lowPriorityBuffer.mu.Lock()
	currentSpans := p.lowPriorityBuffer.spans[:]
	p.lowPriorityBuffer.spans = make([]SpanWithContext, 0)
	p.lowPriorityBuffer.mu.Unlock()

	if len(currentSpans) == 0 {
		return
	}

	// Convert spans to traces and forward to next consumer
	td := p.buildTracesFromSpans(currentSpans)
	if td.SpanCount() > 0 {
		if err := p.nextConsumer.ConsumeTraces(ctx, td); err != nil {
			p.logger.Error("Failed to consume low priority traces", zap.Error(err))
		}
	}
}

// buildTracesFromSpans converts SpanWithContext slices into ptrace.Traces
func (p *priorityProcessor) buildTracesFromSpans(spans []SpanWithContext) ptrace.Traces {
	td := ptrace.NewTraces()
	if len(spans) == 0 {
		return td
	}

	// Group spans by resource to maintain structure
	resourceMap := make(map[string]ptrace.ResourceSpans)

	for _, spanWithContext := range spans {
		// Create a key based on resource attributes
		resourceKey := p.getResourceKey(spanWithContext.Resource)

		rs, exists := resourceMap[resourceKey]
		if !exists {
			rs = td.ResourceSpans().AppendEmpty()
			spanWithContext.Resource.Resource().CopyTo(rs.Resource())
			rs.SetSchemaUrl(spanWithContext.Resource.SchemaUrl())
			resourceMap[resourceKey] = rs
		}

		// Find or create scope spans
		var ss ptrace.ScopeSpans
		found := false
		for i := 0; i < rs.ScopeSpans().Len(); i++ {
			existingSs := rs.ScopeSpans().At(i)
			if existingSs.Scope().Name() == spanWithContext.Scope.Scope().Name() &&
				existingSs.Scope().Version() == spanWithContext.Scope.Scope().Version() {
				ss = existingSs
				found = true
				break
			}
		}

		if !found {
			ss = rs.ScopeSpans().AppendEmpty()
			spanWithContext.Scope.Scope().CopyTo(ss.Scope())
			ss.SetSchemaUrl(spanWithContext.Scope.SchemaUrl())
		}

		// Copy span
		newSpan := ss.Spans().AppendEmpty()
		spanWithContext.Span.CopyTo(newSpan)
	}

	return td
}

// getResourceKey generates a unique key for a resource spans
func (p *priorityProcessor) getResourceKey(rs ptrace.ResourceSpans) string {
	// Simple key based on resource attributes hash
	attrs := rs.Resource().Attributes()
	var keys []string
	attrs.Range(func(k string, v pcommon.Value) bool {
		keys = append(keys, k+":"+v.AsString())
		return true
	})
	sort.Strings(keys)
	return strings.Join(keys, "|")
}

// getTotalMemory reads the total available memory, prioritizing container/cgroup limits
func getTotalMemory() (uint64, error) {
	// Determine which cgroup version is active
	isV2, err := isCGroupV2()
	if err == nil {
		// Check the active cgroup version
		if isV2 {
			if mem, err := readCGroupV2MemoryLimit(); err == nil && mem > 0 {
				return mem, nil
			}
		} else {
			if mem, err := readCGroupV1MemoryLimit(); err == nil && mem > 0 {
				return mem, nil
			}
		}
	}

	// Fallback to /proc/meminfo (host system memory)
	if data, err := os.ReadFile("/proc/meminfo"); err == nil {
		lines := strings.Split(string(data), "\n")
		for _, line := range lines {
			if strings.HasPrefix(line, "MemTotal:") {
				fields := strings.Fields(line)
				if len(fields) >= 2 {
					memKB, err := strconv.ParseUint(fields[1], 10, 64)
					if err == nil {
						return memKB * 1024, nil // Convert KB to bytes
					}
				}
			}
		}
	}

	// If we can't detect memory via cgroups or /proc/meminfo, we can't proceed
	return 0, fmt.Errorf("unable to determine total memory: cgroup limits not found and /proc/meminfo unavailable")
}

// isCGroupV2 checks if the system is using cgroup v2
// In v2, there's a unified hierarchy at /sys/fs/cgroup
func isCGroupV2() (bool, error) {
	// Check if /sys/fs/cgroup/cgroup.controllers exists (indicates v2)
	if _, err := os.Stat("/sys/fs/cgroup/cgroup.controllers"); err == nil {
		return true, nil
	}
	// If /sys/fs/cgroup/memory exists, it's likely v1
	if _, err := os.Stat("/sys/fs/cgroup/memory"); err == nil {
		return false, nil
	}
	// Can't determine - might not have cgroups mounted
	return false, fmt.Errorf("unable to determine cgroup version")
}

// readCGroupV2MemoryLimit reads memory limit from cgroup v2
func readCGroupV2MemoryLimit() (uint64, error) {
	data, err := os.ReadFile("/sys/fs/cgroup/memory.max")
	if err != nil {
		return 0, err
	}

	limitStr := strings.TrimSpace(string(data))
	if limitStr == "max" || limitStr == "" {
		return 0, fmt.Errorf("no limit set")
	}

	limit, err := strconv.ParseUint(limitStr, 10, 64)
	if err != nil {
		return 0, err
	}

	return limit, nil
}

// readCGroupV1MemoryLimit reads memory limit from cgroup v1
func readCGroupV1MemoryLimit() (uint64, error) {
	data, err := os.ReadFile("/sys/fs/cgroup/memory/memory.limit_in_bytes")
	if err != nil {
		return 0, err
	}

	limitStr := strings.TrimSpace(string(data))
	limit, err := strconv.ParseInt(limitStr, 10, 64)
	if err != nil {
		return 0, err
	}

	// 9223372036854771712 is the value for unlimited in cgroup v1
	if limit < 0 || limit == 9223372036854771712 {
		return 0, fmt.Errorf("no limit set")
	}

	return uint64(limit), nil
}

// readCGroupV2MemoryCurrent reads current memory usage from cgroup v2
func readCGroupV2MemoryCurrent() (uint64, error) {
	data, err := os.ReadFile("/sys/fs/cgroup/memory.current")
	if err != nil {
		return 0, err
	}

	usageStr := strings.TrimSpace(string(data))
	usage, err := strconv.ParseUint(usageStr, 10, 64)
	if err != nil {
		return 0, err
	}

	return usage, nil
}

// readCGroupV1MemoryUsage reads current memory usage from cgroup v1
func readCGroupV1MemoryUsage() (uint64, error) {
	data, err := os.ReadFile("/sys/fs/cgroup/memory/memory.usage_in_bytes")
	if err != nil {
		return 0, err
	}

	usageStr := strings.TrimSpace(string(data))
	usage, err := strconv.ParseUint(usageStr, 10, 64)
	if err != nil {
		return 0, err
	}

	return usage, nil
}
