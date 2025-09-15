package agentcontactreceiver

import (
	"context"
	"fmt"
	"net"
	"sync"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/agentcontactreceiver/internal/protocol"
)

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
		config:        config,
		logger:        settings.Logger,
		settings:      settings,
		traceConsumer: traceConsumer,
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

	r.logger.Info("Agent contact receiver started", zap.String("endpoint", r.config.NetAddr.Endpoint))
	return nil
}

// Shutdown shuts down the receiver.
func (r *agentContactReceiver) Shutdown(ctx context.Context) error {
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

	// TODO: Pass trace IDs to the span lookup processor
	// For now, we just acknowledge the request
	// In the future, this will trigger the span lookup processor to emit spans

	s.receiver.logger.Info("Processed trace ID request",
		zap.Int("trace_count", len(req.TraceIds)))

	return &protocol.TraceIDResponse{
		Ack: true,
	}, nil
}
