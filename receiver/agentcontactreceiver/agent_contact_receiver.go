package agentcontactreceiver

import (
	"context"
	"encoding/hex"
	"fmt"
	"net"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/common/agentcontact/protocol"
)

// TraceSpanPair represents a trace ID and span ID combination
type TraceSpanPair struct {
	TraceID string
	SpanID  string
}

// agentContactReceiver implements the receiver interface for accepting trace ID requests.
type agentContactReceiver struct {
	config   *Config
	logger   *zap.Logger
	settings receiver.Settings

	// gRPC server
	grpcServer *grpc.Server
	listener   net.Listener

	// Consumer for passing trace IDs to the span lookup processor
	traceConsumer consumer.Traces

	// Server implementation
	server *agentContactServer

	// Processing queue
	traceSpanQueue []TraceSpanPair
	queueMutex     sync.RWMutex
	stopQueue      chan struct{}
	queueWg        sync.WaitGroup

	// Shutdown synchronization
	stopWG sync.WaitGroup
}

// agentContactServer implements the gRPC service
type agentContactServer struct {
	protocol.UnimplementedAgentContactServer
	receiver *agentContactReceiver
}

// newAgentContactReceiver creates a new agent contact receiver.
func newAgentContactReceiver(config *Config, settings receiver.Settings, traceConsumer consumer.Traces) (*agentContactReceiver, error) {
	if traceConsumer == nil {
		return nil, fmt.Errorf("trace consumer is required")
	}

	return &agentContactReceiver{
		config:         config,
		logger:         settings.Logger,
		settings:       settings,
		traceConsumer:  traceConsumer,
		traceSpanQueue: make([]TraceSpanPair, 0),
		stopQueue:      make(chan struct{}),
	}, nil
}

// Start starts the receiver.
func (r *agentContactReceiver) Start(ctx context.Context, host component.Host) error {
	// Create gRPC server using the ServerConfig's ToServer method
	grpcServer, err := r.config.ServerConfig.ToServer(ctx, host, r.settings.TelemetrySettings)
	if err != nil {
		return fmt.Errorf("failed to create gRPC server: %w", err)
	}

	r.grpcServer = grpcServer

	// Create server implementation
	r.server = &agentContactServer{
		receiver: r,
	}

	// Register the service
	protocol.RegisterAgentContactServer(grpcServer, r.server)

	// Start listening using the ServerConfig's NetAddr
	listener, err := net.Listen(string(r.config.NetAddr.Transport), r.config.NetAddr.Endpoint)
	if err != nil {
		return fmt.Errorf("failed to bind to address %q: %w", r.config.NetAddr.Endpoint, err)
	}

	r.listener = listener

	// Start the gRPC server in a goroutine
	r.stopWG.Add(1)
	go func() {
		defer r.stopWG.Done()
		if err := grpcServer.Serve(listener); err != nil {
			r.logger.Error("gRPC server error", zap.Error(err))
		}
	}()

	// Start the processing queue worker
	r.startQueueWorker()

	r.logger.Info("Agent contact receiver started", zap.String("endpoint", r.config.NetAddr.Endpoint))
	return nil
}

// Shutdown shuts down the receiver.
func (r *agentContactReceiver) Shutdown(ctx context.Context) error {
	// Stop the processing queue
	close(r.stopQueue)
	r.queueWg.Wait()

	if r.grpcServer != nil {
		r.grpcServer.GracefulStop()
	}

	if r.listener != nil {
		r.listener.Close()
	}

	// Wait for the server to stop
	r.stopWG.Wait()

	r.logger.Info("Agent contact receiver stopped")
	return nil
}

// SendTraceIDs implements the gRPC service method.
func (s *agentContactServer) SendTraceIDs(ctx context.Context, req *protocol.TraceIDRequest) (*protocol.TraceIDResponse, error) {
	s.receiver.logger.Debug("Received trace ID request",
		zap.Int("count", len(req.TraceIds)),
		zap.Strings("trace_ids", req.TraceIds))

	// Extract and enqueue trace ID and span ID pairs for processing
	s.receiver.queueMutex.Lock()

	// Pre-allocate slice for efficiency
	pairCount := len(req.TraceIds)
	if len(req.SpanIds) < pairCount {
		pairCount = len(req.SpanIds)
	}

	newPairs := make([]TraceSpanPair, pairCount)
	for i := 0; i < pairCount; i++ {
		newPairs[i] = TraceSpanPair{
			TraceID: req.TraceIds[i],
			SpanID:  req.SpanIds[i],
		}
	}

	s.receiver.traceSpanQueue = append(s.receiver.traceSpanQueue, newPairs...)
	queueLen := len(s.receiver.traceSpanQueue)
	s.receiver.queueMutex.Unlock()

	s.receiver.logger.Debug("Enqueued trace+span ID pairs for processing",
		zap.Int("pair_count", len(req.TraceIds)),
		zap.Int("queue_length", queueLen))

	return &protocol.TraceIDResponse{
		Ack: true,
	}, nil
}

// startQueueWorker starts the processing queue worker
func (r *agentContactReceiver) startQueueWorker() {
	r.queueWg.Add(1)
	go r.queueWorker()
	r.logger.Info("Started processing queue worker")
}

// queueWorker processes trace ID requests from the queue
func (r *agentContactReceiver) queueWorker() {
	defer r.queueWg.Done()

	r.logger.Debug("Queue worker started")

	for {
		select {
		case <-r.stopQueue:
			r.logger.Debug("Queue worker stopping")
			return
		default:
			r.processQueueBatch()
			time.Sleep(10 * time.Millisecond) // Brief pause to avoid busy waiting
		}
	}
}

// processQueueBatch processes all current items in the queue
func (r *agentContactReceiver) processQueueBatch() {
	// Get current pairs and clear queue
	r.queueMutex.Lock()
	currentPairs := r.traceSpanQueue[:]
	r.traceSpanQueue = make([]TraceSpanPair, 0)
	r.queueMutex.Unlock()

	if len(currentPairs) == 0 {
		return
	}

	r.logger.Debug("Processing batch of trace+span pairs",
		zap.Int("batch_size", len(currentPairs)))

	// Process all pairs without holding the lock
	for _, pair := range currentPairs {
		r.processTraceSpanPair(pair)
	}
}

// processTraceSpanPair processes a single trace ID and span ID pair
func (r *agentContactReceiver) processTraceSpanPair(pair TraceSpanPair) {
	startTime := time.Now()

	r.logger.Info("Processing trace+span ID pair",
		zap.String("trace_id", pair.TraceID),
		zap.String("span_id", pair.SpanID))

	// Create a fresh span for this trace+span ID pair
	traces := ptrace.NewTraces()
	resourceSpans := traces.ResourceSpans().AppendEmpty()
	scopeSpans := resourceSpans.ScopeSpans().AppendEmpty()
	span := scopeSpans.Spans().AppendEmpty()

	// Set basic span attributes
	span.SetTraceID(parseTraceID(pair.TraceID))
	span.SetSpanID(parseSpanID(pair.SpanID))
	span.SetName("agent-contact-requested-span")
	span.SetKind(ptrace.SpanKindInternal)
	span.SetStartTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	span.SetEndTimestamp(pcommon.NewTimestampFromTime(time.Now().Add(1 * time.Millisecond)))
	span.Status().SetCode(ptrace.StatusCodeOk)

	// Add attributes
	span.Attributes().PutStr("agent.contact.requested", "true")
	span.Attributes().PutStr("agent.contact.trace_id", pair.TraceID)
	span.Attributes().PutStr("agent.contact.span_id", pair.SpanID)

	// Send to next consumer
	if err := r.traceConsumer.ConsumeTraces(context.Background(), traces); err != nil {
		r.logger.Error("Failed to send trace to consumer",
			zap.String("trace_id", pair.TraceID),
			zap.String("span_id", pair.SpanID),
			zap.Error(err))
		return
	}

	processingTime := time.Since(startTime)
	r.logger.Info("Completed trace+span ID pair processing",
		zap.String("trace_id", pair.TraceID),
		zap.String("span_id", pair.SpanID),
		zap.Duration("processing_time", processingTime))
}

// parseTraceID converts a hex string trace ID to pcommon.TraceID
func parseTraceID(traceIDStr string) pcommon.TraceID {
	var traceID pcommon.TraceID
	decoded, err := hex.DecodeString(traceIDStr)
	if err != nil {
		// If hex decoding fails, return zero trace ID
		return traceID
	}
	copy(traceID[:], decoded)
	return traceID
}

// parseSpanID converts a hex string span ID to pcommon.SpanID
func parseSpanID(spanIDStr string) pcommon.SpanID {
	var spanID pcommon.SpanID
	decoded, err := hex.DecodeString(spanIDStr)
	if err != nil {
		// If hex decoding fails, return zero span ID
		return spanID
	}
	copy(spanID[:], decoded)
	return spanID
}
