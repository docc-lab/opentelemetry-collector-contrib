package agentcontactexporter

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	conventions "go.opentelemetry.io/otel/semconv/v1.7.0"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/common/agentcontact/protocol"
)

// GlobalSpanReconstructionProcessor is the global instance that other processors can access
var GlobalAgentContactExporter *AgentContactExporter

// ipBatchManager manages batching for a specific upstream IP
type ipBatchManager struct {
	ip             string
	endpoint       string
	traceSpanPairs []TraceSpanPair
	batchSize      int
	batchTimeout   time.Duration
	timer          *time.Timer
	timerChan      chan struct{}
	client         protocol.AgentContactClient
	logger         *zap.Logger
	timeout        time.Duration

	// Channels for communication
	flushChan chan struct{}
	stopChan  chan struct{}
	wg        sync.WaitGroup
}

// newIPBatchManager creates a new batch manager for a specific IP
func newIPBatchManager(ip, endpoint string, batchSize int, batchTimeout, timeout time.Duration, client protocol.AgentContactClient, logger *zap.Logger) *ipBatchManager {
	bm := &ipBatchManager{
		ip:             ip,
		endpoint:       endpoint,
		traceSpanPairs: make([]TraceSpanPair, 0, batchSize),
		batchSize:      batchSize,
		batchTimeout:   batchTimeout,
		timeout:        timeout,
		client:         client,
		logger:         logger,
		flushChan:      make(chan struct{}, 1),
		stopChan:       make(chan struct{}),
		timerChan:      make(chan struct{}, 1),
	}

	// Start the batch management goroutine
	bm.wg.Add(1)
	go bm.run()

	return bm
}

// run is the main goroutine for this IP's batch management
func (bm *ipBatchManager) run() {
	defer bm.wg.Done()

	for {
		select {
		case <-bm.stopChan:
			// Shutdown requested
			bm.flushBatch()
			return
		case <-bm.flushChan:
			// Manual flush requested
			bm.flushBatch()
		case <-bm.timerChan:
			// Timeout reached
			bm.flushBatch()
		}
	}
}

// addTraceID adds a trace ID to the batch and potentially triggers a flush
func (bm *ipBatchManager) addTraceSpanPair(pair TraceSpanPair) {
	bm.traceSpanPairs = append(bm.traceSpanPairs, pair)

	bm.logger.Debug("🟠 Batch Manager: Added trace+span ID pair to batch",
		zap.String("ip", bm.ip),
		zap.String("trace_id", pair.TraceID),
		zap.String("span_id", pair.SpanID),
		zap.Int("current_batch_size", len(bm.traceSpanPairs)),
		zap.Int("batch_size_limit", bm.batchSize))

	// If batch is full, trigger flush
	if len(bm.traceSpanPairs) >= bm.batchSize {
		bm.logger.Info("🟢 Batch Manager: Batch size limit reached, triggering flush",
			zap.String("ip", bm.ip),
			zap.Int("batch_size", len(bm.traceSpanPairs)))
		select {
		case bm.flushChan <- struct{}{}:
		default:
			// Channel is full, flush is already pending
			bm.logger.Debug("🟠 Batch Manager: Flush already pending",
				zap.String("ip", bm.ip))
		}
	} else if bm.timer == nil {
		// Start timer for this batch
		bm.logger.Debug("🟠 Batch Manager: Starting batch timeout timer",
			zap.String("ip", bm.ip),
			zap.Duration("timeout", bm.batchTimeout))
		bm.timer = time.AfterFunc(bm.batchTimeout, func() {
			bm.logger.Debug("🟠 Batch Manager: Batch timeout reached",
				zap.String("ip", bm.ip))
			select {
			case bm.timerChan <- struct{}{}:
			default:
				// Channel is full, timeout is already pending
				bm.logger.Debug("🟠 Batch Manager: Timeout already pending",
					zap.String("ip", bm.ip))
			}
		})
	}
}

// flushBatch sends the current batch and resets the timer
func (bm *ipBatchManager) flushBatch() {
	if len(bm.traceSpanPairs) == 0 {
		bm.logger.Debug("🟠 Batch Manager: No trace IDs to flush",
			zap.String("ip", bm.ip))
		return
	}

	bm.logger.Info("🟢 Batch Manager: Flushing batch",
		zap.String("ip", bm.ip),
		zap.String("endpoint", bm.endpoint),
		zap.Int("pair_count", len(bm.traceSpanPairs)))

	// Stop the current timer
	if bm.timer != nil {
		bm.timer.Stop()
		bm.timer = nil
		bm.logger.Debug("🟠 Batch Manager: Stopped batch timer",
			zap.String("ip", bm.ip))
	}

	// Send the batch
	if err := bm.sendBatch(); err != nil {
		bm.logger.Error("🟡 Batch Manager: Failed to send batch",
			zap.String("ip", bm.ip),
			zap.String("endpoint", bm.endpoint),
			zap.Error(err))
	} else {
		bm.logger.Info("🟢 Batch Manager: Successfully sent batch",
			zap.String("ip", bm.ip),
			zap.String("endpoint", bm.endpoint),
			zap.Int("pair_count", len(bm.traceSpanPairs)))
	}

	// Clear the batch
	bm.traceSpanPairs = bm.traceSpanPairs[:0]
	bm.logger.Debug("🟠 Batch Manager: Cleared batch buffer",
		zap.String("ip", bm.ip))
}

// sendBatch sends the current batch of trace+span ID pairs
func (bm *ipBatchManager) sendBatch() error {
	if len(bm.traceSpanPairs) == 0 {
		bm.logger.Debug("🟠 Batch Manager: No trace+span ID pairs to send",
			zap.String("ip", bm.ip))
		return nil
	}

	bm.logger.Info("🟢 Batch Manager: Sending trace+span ID pairs via gRPC",
		zap.String("ip", bm.ip),
		zap.String("endpoint", bm.endpoint),
		zap.Int("count", len(bm.traceSpanPairs)),
		zap.Duration("timeout", bm.timeout))

	// Extract trace IDs and span IDs from pairs
	traceIDs := make([]string, len(bm.traceSpanPairs))
	spanIDs := make([]string, len(bm.traceSpanPairs))
	for i, pair := range bm.traceSpanPairs {
		traceIDs[i] = pair.TraceID
		spanIDs[i] = pair.SpanID
	}

	// Create request
	req := &protocol.TraceIDRequest{
		TraceIds: traceIDs,
		SpanIds:  spanIDs,
	}

	// Send request with timeout
	ctx, cancel := context.WithTimeout(context.Background(), bm.timeout)
	defer cancel()

	resp, err := bm.client.SendTraceIDs(ctx, req)
	if err != nil {
		bm.logger.Error("🟡 Batch Manager: gRPC call failed",
			zap.String("ip", bm.ip),
			zap.String("endpoint", bm.endpoint),
			zap.Error(err))
		return fmt.Errorf("failed to send trace IDs to %s: %w", bm.endpoint, err)
	}

	if !resp.Ack {
		bm.logger.Error("🟡 Batch Manager: Remote collector did not acknowledge request",
			zap.String("ip", bm.ip),
			zap.String("endpoint", bm.endpoint))
		return fmt.Errorf("remote collector at %s did not acknowledge trace ID request", bm.endpoint)
	}

	bm.logger.Info("🟢 Batch Manager: Successfully sent trace IDs and received ACK",
		zap.String("ip", bm.ip),
		zap.String("endpoint", bm.endpoint),
		zap.Int("count", len(traceIDs)),
		zap.Strings("trace_ids", traceIDs))

	return nil
}

// shutdown gracefully shuts down the batch manager
func (bm *ipBatchManager) shutdown() {
	close(bm.stopChan)
	bm.wg.Wait()
}

// agentContactExporter implements the exporter interface for sending trace ID requests.
type AgentContactExporter struct {
	config   *Config
	logger   *zap.Logger
	settings exporter.Settings

	// Dynamic endpoint management
	connections map[string]*grpc.ClientConn
	clients     map[string]protocol.AgentContactClient
	connMutex   sync.RWMutex

	// Per-IP batch managers
	batchManagers map[string]*ipBatchManager
	bmMutex       sync.RWMutex
}

// newAgentContactExporter creates a new agent contact exporter.
func newAgentContactExporter(config *Config, settings exporter.Settings) (*AgentContactExporter, error) {
	ace := &AgentContactExporter{
		config:        config,
		logger:        settings.Logger,
		settings:      settings,
		connections:   make(map[string]*grpc.ClientConn),
		clients:       make(map[string]protocol.AgentContactClient),
		batchManagers: make(map[string]*ipBatchManager),
	}

	GlobalAgentContactExporter = ace

	return ace, nil
}

// Start starts the exporter.
func (e *AgentContactExporter) Start(ctx context.Context, host component.Host) error {
	e.logger.Info("Agent contact exporter started with dynamic endpoints",
		zap.Int("default_port", e.config.DefaultPort))
	return nil
}

// Shutdown shuts down the exporter.
func (e *AgentContactExporter) Shutdown(ctx context.Context) error {
	// Shutdown all batch managers
	e.bmMutex.Lock()
	for _, bm := range e.batchManagers {
		bm.shutdown()
	}
	e.bmMutex.Unlock()

	// Close all connections
	e.connMutex.Lock()
	defer e.connMutex.Unlock()

	for endpoint, conn := range e.connections {
		if err := conn.Close(); err != nil {
			e.logger.Error("Failed to close connection",
				zap.String("endpoint", endpoint), zap.Error(err))
		}
	}

	return nil
}

// ConsumeTraces processes traces and extracts trace IDs for lookup requests.
func (e *AgentContactExporter) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	e.logger.Info("🟢 Agent Contact Exporter: Processing traces",
		zap.Int("resource_spans_count", td.ResourceSpans().Len()),
		zap.Int("total_spans", e.countTotalSpans(td)))

	// Extract trace IDs and upstream IPs from the traces
	traceData := extractTraceIDsAndUpstreamIPs(td)

	e.logger.Info("🟢 Agent Contact Exporter: Extracted trace data",
		zap.Int("upstream_ips_count", len(traceData)))

	// Group trace+span ID pairs by upstream IP and add to appropriate batch managers
	for upstreamIP, traceSpanPairs := range traceData {
		endpoint := fmt.Sprintf("%s:%d", upstreamIP, e.config.DefaultPort)

		e.logger.Info("🟢 Agent Contact Exporter: Processing upstream IP",
			zap.String("upstream_ip", upstreamIP),
			zap.String("endpoint", endpoint),
			zap.Int("trace_span_pairs_count", len(traceSpanPairs)))

		// Get or create batch manager for this upstream IP
		bm := e.getOrCreateBatchManager(upstreamIP, endpoint)

		// Add trace+span ID pairs to the batch manager
		for _, pair := range traceSpanPairs {
			e.logger.Debug("🟠 Agent Contact Exporter: Enqueuing trace+span ID pair",
				zap.String("upstream_ip", upstreamIP),
				zap.String("trace_id", pair.TraceID),
				zap.String("span_id", pair.SpanID))
			bm.addTraceSpanPair(pair)
		}
	}

	e.logger.Info("🟢 Agent Contact Exporter: Completed processing traces")
	return nil
}

// countTotalSpans counts the total number of spans in a traces object
func (e *AgentContactExporter) countTotalSpans(td ptrace.Traces) int {
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

// getOrCreateBatchManager gets or creates a batch manager for a specific IP
func (e *AgentContactExporter) getOrCreateBatchManager(ip, endpoint string) *ipBatchManager {
	e.bmMutex.RLock()
	bm, exists := e.batchManagers[ip]
	e.bmMutex.RUnlock()

	if exists {
		e.logger.Debug("🟠 Agent Contact Exporter: Using existing batch manager",
			zap.String("ip", ip),
			zap.String("endpoint", endpoint))
		return bm
	}

	e.logger.Info("🟢 Agent Contact Exporter: Creating new batch manager",
		zap.String("ip", ip),
		zap.String("endpoint", endpoint))

	// Create new batch manager
	e.bmMutex.Lock()
	defer e.bmMutex.Unlock()

	// Double-check after acquiring write lock
	if bm, exists := e.batchManagers[ip]; exists {
		e.logger.Debug("🟠 Agent Contact Exporter: Batch manager created by another goroutine",
			zap.String("ip", ip))
		return bm
	}

	// Get or create client for this endpoint
	client, err := e.getClientForEndpoint(endpoint)
	if err != nil {
		e.logger.Error("🟡 Agent Contact Exporter: Failed to get client for endpoint",
			zap.String("endpoint", endpoint), zap.Error(err))
		// Return a nil client - the batch manager will handle this gracefully
		client = nil
	}

	// Create new batch manager
	bm = newIPBatchManager(
		ip,
		endpoint,
		e.config.BatchSize,
		e.config.BatchTimeout,
		e.config.Timeout,
		client,
		e.logger,
	)

	e.batchManagers[ip] = bm
	e.logger.Info("🟢 Agent Contact Exporter: Successfully created new batch manager",
		zap.String("ip", ip),
		zap.String("endpoint", endpoint),
		zap.Int("batch_size", e.config.BatchSize),
		zap.Duration("batch_timeout", e.config.BatchTimeout))

	return bm
}

// getClientForEndpoint gets or creates a gRPC client for the given endpoint.
func (e *AgentContactExporter) getClientForEndpoint(endpoint string) (protocol.AgentContactClient, error) {
	e.connMutex.RLock()
	client, exists := e.clients[endpoint]
	e.connMutex.RUnlock()

	if exists {
		e.logger.Debug("🟠 Agent Contact Exporter: Using existing gRPC connection",
			zap.String("endpoint", endpoint))
		return client, nil
	}

	e.logger.Info("🟢 Agent Contact Exporter: Creating new gRPC connection",
		zap.String("endpoint", endpoint))

	// Create new connection
	e.connMutex.Lock()
	defer e.connMutex.Unlock()

	// Double-check after acquiring write lock
	if client, exists := e.clients[endpoint]; exists {
		e.logger.Debug("🟠 Agent Contact Exporter: Connection created by another goroutine",
			zap.String("endpoint", endpoint))
		return client, nil
	}

	// Create connection using the base config but with the dynamic endpoint
	conn, err := grpc.Dial(endpoint, grpc.WithInsecure())
	if err != nil {
		e.logger.Error("🟡 Agent Contact Exporter: Failed to create gRPC connection",
			zap.String("endpoint", endpoint), zap.Error(err))
		return nil, fmt.Errorf("failed to create gRPC connection to %s: %w", endpoint, err)
	}

	e.connections[endpoint] = conn
	e.clients[endpoint] = protocol.NewAgentContactClient(conn)

	e.logger.Info("🟢 Agent Contact Exporter: Successfully created new gRPC connection",
		zap.String("endpoint", endpoint))

	return e.clients[endpoint], nil
}

// extractTraceIDsAndIPs extracts trace IDs grouped by IP addresses from traces.
func extractTraceIDsAndIPs(td ptrace.Traces) map[string][]string {
	// Map IP address -> trace IDs
	ipToTraceIDs := make(map[string]map[string]struct{})

	for i := 0; i < td.ResourceSpans().Len(); i++ {
		rs := td.ResourceSpans().At(i)
		for j := 0; j < rs.ScopeSpans().Len(); j++ {
			ss := rs.ScopeSpans().At(j)
			for k := 0; k < ss.Spans().Len(); k++ {
				span := ss.Spans().At(k)
				traceID := span.TraceID().String()

				// Extract IP addresses from span attributes
				ips := extractIPAddresses(span)

				if len(ips) == 0 {
					// If no IP found, use a default endpoint
					ips = []string{"localhost"}
				}

				// Add trace ID to each IP
				for _, ip := range ips {
					if ipToTraceIDs[ip] == nil {
						ipToTraceIDs[ip] = make(map[string]struct{})
					}
					ipToTraceIDs[ip][traceID] = struct{}{}
				}
			}
		}
	}

	// Convert to slice format
	result := make(map[string][]string)
	for ip, traceIDSet := range ipToTraceIDs {
		traceIDs := make([]string, 0, len(traceIDSet))
		for traceID := range traceIDSet {
			traceIDs = append(traceIDs, traceID)
		}
		result[ip] = traceIDs
	}

	return result
}

// TraceSpanPair represents a trace ID and span ID combination
type TraceSpanPair struct {
	TraceID string
	SpanID  string
}

// extractTraceIDsAndUpstreamIPs extracts trace ID and span ID pairs grouped by upstream IP addresses from traces.
func extractTraceIDsAndUpstreamIPs(td ptrace.Traces) map[string][]TraceSpanPair {
	// Map upstream IP -> trace+span ID pairs
	upstreamIPToTraceSpanPairs := make(map[string][]TraceSpanPair)
	processedCount := 0
	skippedCount := 0

	for i := 0; i < td.ResourceSpans().Len(); i++ {
		rs := td.ResourceSpans().At(i)
		for j := 0; j < rs.ScopeSpans().Len(); j++ {
			ss := rs.ScopeSpans().At(j)
			for k := 0; k < ss.Spans().Len(); k++ {
				span := ss.Spans().At(k)
				traceID := span.TraceID().String()
				spanID := span.SpanID().String()

				// Extract upstream IP from span attributes
				upstreamIP := extractUpstreamIP(span)

				// Skip spans with unknown or invalid upstream IPs
				if upstreamIP == "" || upstreamIP == "unknown-upstream" {
					skippedCount++
					continue // Discard this span
				}

				// Add trace+span ID pair to the upstream IP
				upstreamIPToTraceSpanPairs[upstreamIP] = append(upstreamIPToTraceSpanPairs[upstreamIP], TraceSpanPair{
					TraceID: traceID,
					SpanID:  spanID,
				})
				processedCount++
			}
		}
	}

	// Log extraction results (this will be called from ConsumeTraces which has access to logger)
	// Note: We can't access logger here since this is a standalone function

	return upstreamIPToTraceSpanPairs
}

// extractIPAddresses extracts IP addresses from span attributes.
func extractIPAddresses(span ptrace.Span) []string {
	var ips []string
	seen := make(map[string]struct{})

	// Check span attributes for IP addresses
	span.Attributes().Range(func(k string, v pcommon.Value) bool {
		switch k {
		case string(conventions.NetPeerIPKey), string(conventions.NetHostIPKey):
			if v.Type() == pcommon.ValueTypeStr {
				ip := v.Str()
				if isValidIP(ip) && !isLocalhost(ip) {
					if _, exists := seen[ip]; !exists {
						ips = append(ips, ip)
						seen[ip] = struct{}{}
					}
				}
			}
		}
		return true
	})

	return ips
}

// extractUpstreamIP extracts the upstream IP address from span attributes.
func extractUpstreamIP(span ptrace.Span) string {
	var upstreamIP string

	// Check span attributes for upstream IP
	span.Attributes().Range(func(k string, v pcommon.Value) bool {
		if k == "upstream.ip" && v.Type() == pcommon.ValueTypeStr {
			upstreamIP = v.Str()
			return false // Found the upstream IP, stop iterating
		}
		return true
	})

	return upstreamIP
}

// isValidIP checks if a string is a valid IP address.
func isValidIP(ip string) bool {
	return net.ParseIP(ip) != nil
}

// isLocalhost checks if an IP address is localhost.
func isLocalhost(ip string) bool {
	return ip == "127.0.0.1" || ip == "localhost" || ip == "::1"
}

// extractTraceIDs extracts unique trace IDs from traces (original function).
func extractTraceIDs(td ptrace.Traces) []string {
	traceIDSet := make(map[string]struct{})

	for i := 0; i < td.ResourceSpans().Len(); i++ {
		rs := td.ResourceSpans().At(i)
		for j := 0; j < rs.ScopeSpans().Len(); j++ {
			ss := rs.ScopeSpans().At(j)
			for k := 0; k < ss.Spans().Len(); k++ {
				span := ss.Spans().At(k)
				traceID := span.TraceID().String()
				traceIDSet[traceID] = struct{}{}
			}
		}
	}

	traceIDs := make([]string, 0, len(traceIDSet))
	for traceID := range traceIDSet {
		traceIDs = append(traceIDs, traceID)
	}

	return traceIDs
}

// Capabilities returns the capabilities of the exporter.
func (e *AgentContactExporter) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}
