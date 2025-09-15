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

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/agentcontactexporter/internal/protocol"
)

// ipBatchManager manages batching for a specific upstream IP
type ipBatchManager struct {
	ip           string
	endpoint     string
	traceIDs     []string
	batchSize    int
	batchTimeout time.Duration
	timer        *time.Timer
	timerChan    chan struct{}
	client       protocol.AgentContactClient
	logger       *zap.Logger
	timeout      time.Duration

	// Channels for communication
	flushChan chan struct{}
	stopChan  chan struct{}
	wg        sync.WaitGroup
}

// newIPBatchManager creates a new batch manager for a specific IP
func newIPBatchManager(ip string, endpoint string, batchSize int, batchTimeout time.Duration, timeout time.Duration, client protocol.AgentContactClient, logger *zap.Logger) *ipBatchManager {
	bm := &ipBatchManager{
		ip:           ip,
		endpoint:     endpoint,
		traceIDs:     make([]string, 0, batchSize),
		batchSize:    batchSize,
		batchTimeout: batchTimeout,
		timeout:      timeout,
		client:       client,
		logger:       logger,
		flushChan:    make(chan struct{}, 1),
		stopChan:     make(chan struct{}),
		timerChan:    make(chan struct{}, 1),
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
func (bm *ipBatchManager) addTraceID(traceID string) {
	bm.traceIDs = append(bm.traceIDs, traceID)

	// If batch is full, trigger flush
	if len(bm.traceIDs) >= bm.batchSize {
		select {
		case bm.flushChan <- struct{}{}:
		default:
			// Channel is full, flush is already pending
		}
	} else if bm.timer == nil {
		// Start timer for this batch
		bm.timer = time.AfterFunc(bm.batchTimeout, func() {
			select {
			case bm.timerChan <- struct{}{}:
			default:
				// Channel is full, timeout is already pending
			}
		})
	}
}

// flushBatch sends the current batch and resets the timer
func (bm *ipBatchManager) flushBatch() {
	if len(bm.traceIDs) == 0 {
		return
	}

	// Stop the current timer
	if bm.timer != nil {
		bm.timer.Stop()
		bm.timer = nil
	}

	// Send the batch
	if err := bm.sendBatch(); err != nil {
		bm.logger.Error("Failed to send batch",
			zap.String("ip", bm.ip),
			zap.String("endpoint", bm.endpoint),
			zap.Error(err))
	}

	// Clear the batch
	bm.traceIDs = bm.traceIDs[:0]
}

// sendBatch sends the current batch of trace IDs
func (bm *ipBatchManager) sendBatch() error {
	if len(bm.traceIDs) == 0 {
		return nil
	}

	// Create request
	req := &protocol.TraceIDRequest{
		TraceIds: bm.traceIDs,
	}

	// Send request with timeout
	ctx, cancel := context.WithTimeout(context.Background(), bm.timeout)
	defer cancel()

	resp, err := bm.client.SendTraceIDs(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to send trace IDs to %s: %w", bm.endpoint, err)
	}

	if !resp.Ack {
		return fmt.Errorf("remote collector at %s did not acknowledge trace ID request", bm.endpoint)
	}

	bm.logger.Debug("Successfully sent trace IDs",
		zap.String("ip", bm.ip),
		zap.String("endpoint", bm.endpoint),
		zap.Int("count", len(bm.traceIDs)),
		zap.Strings("trace_ids", bm.traceIDs))

	return nil
}

// shutdown gracefully shuts down the batch manager
func (bm *ipBatchManager) shutdown() {
	close(bm.stopChan)
	bm.wg.Wait()
}

// agentContactExporter implements the exporter interface for sending trace ID requests.
type agentContactExporter struct {
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
func newAgentContactExporter(config *Config, settings exporter.Settings) (*agentContactExporter, error) {
	return &agentContactExporter{
		config:        config,
		logger:        settings.Logger,
		settings:      settings,
		connections:   make(map[string]*grpc.ClientConn),
		clients:       make(map[string]protocol.AgentContactClient),
		batchManagers: make(map[string]*ipBatchManager),
	}, nil
}

// Start starts the exporter.
func (e *agentContactExporter) Start(ctx context.Context, host component.Host) error {
	e.logger.Info("Agent contact exporter started with dynamic endpoints",
		zap.Int("default_port", e.config.DefaultPort))
	return nil
}

// Shutdown shuts down the exporter.
func (e *agentContactExporter) Shutdown(ctx context.Context) error {
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
func (e *agentContactExporter) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	// Extract trace IDs and upstream IPs from the traces
	traceData := extractTraceIDsAndUpstreamIPs(td)

	// Group trace IDs by upstream IP and add to appropriate batch managers
	for upstreamIP, traceIDs := range traceData {
		endpoint := fmt.Sprintf("%s:%d", upstreamIP, e.config.DefaultPort)

		// Get or create batch manager for this upstream IP
		bm := e.getOrCreateBatchManager(upstreamIP, endpoint)

		// Add trace IDs to the batch manager
		for _, traceID := range traceIDs {
			bm.addTraceID(traceID)
		}
	}

	return nil
}

// getOrCreateBatchManager gets or creates a batch manager for a specific IP
func (e *agentContactExporter) getOrCreateBatchManager(ip, endpoint string) *ipBatchManager {
	e.bmMutex.RLock()
	bm, exists := e.batchManagers[ip]
	e.bmMutex.RUnlock()

	if exists {
		return bm
	}

	// Create new batch manager
	e.bmMutex.Lock()
	defer e.bmMutex.Unlock()

	// Double-check after acquiring write lock
	if bm, exists := e.batchManagers[ip]; exists {
		return bm
	}

	// Get or create client for this endpoint
	client, err := e.getClientForEndpoint(endpoint)
	if err != nil {
		e.logger.Error("Failed to get client for endpoint",
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
	e.logger.Info("Created new batch manager",
		zap.String("ip", ip),
		zap.String("endpoint", endpoint))

	return bm
}

// getClientForEndpoint gets or creates a gRPC client for the given endpoint.
func (e *agentContactExporter) getClientForEndpoint(endpoint string) (protocol.AgentContactClient, error) {
	e.connMutex.RLock()
	client, exists := e.clients[endpoint]
	e.connMutex.RUnlock()

	if exists {
		return client, nil
	}

	// Create new connection
	e.connMutex.Lock()
	defer e.connMutex.Unlock()

	// Double-check after acquiring write lock
	if client, exists := e.clients[endpoint]; exists {
		return client, nil
	}

	// Create connection using the base config but with the dynamic endpoint
	conn, err := grpc.Dial(endpoint, grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("failed to create gRPC connection to %s: %w", endpoint, err)
	}

	e.connections[endpoint] = conn
	e.clients[endpoint] = protocol.NewAgentContactClient(conn)

	e.logger.Info("Created new connection", zap.String("endpoint", endpoint))

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

// extractTraceIDsAndUpstreamIPs extracts trace IDs grouped by upstream IP addresses from traces.
func extractTraceIDsAndUpstreamIPs(td ptrace.Traces) map[string][]string {
	// Map upstream IP -> trace IDs
	upstreamIPToTraceIDs := make(map[string][]string)

	for i := 0; i < td.ResourceSpans().Len(); i++ {
		rs := td.ResourceSpans().At(i)
		for j := 0; j < rs.ScopeSpans().Len(); j++ {
			ss := rs.ScopeSpans().At(j)
			for k := 0; k < ss.Spans().Len(); k++ {
				span := ss.Spans().At(k)
				traceID := span.TraceID().String()

				// Extract upstream IP from span attributes
				upstreamIP := extractUpstreamIP(span)

				if upstreamIP == "" {
					// If no upstream IP found, use a default endpoint
					upstreamIP = "localhost"
				}

				// Add trace ID to the upstream IP
				upstreamIPToTraceIDs[upstreamIP] = append(upstreamIPToTraceIDs[upstreamIP], traceID)
			}
		}
	}

	return upstreamIPToTraceIDs
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
func (e *agentContactExporter) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}
