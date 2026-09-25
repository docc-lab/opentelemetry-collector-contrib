// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Package priorityotlpreceiver is an OTLP/gRPC trace receiver that consults the priority
// processor's admission gate BEFORE a request is read or decoded.
//
// Tomislav-RetCtx: the bridges SDK sends priority-homogeneous export requests tagged with a
// "bridges-priority: hp|lp" gRPC header. The priority processor (the controller) publishes
// its state every tick; this receiver's gRPC tap reads only the method and that header and
// refuses, with the processor's own codes, any request the processor would refuse anyway:
// LP while shedding LP, everything in refuse-all. Admitted requests are decoded and handed
// to the pipeline exactly as the stock OTLP receiver does (same accounting, same error
// mapping), where the priority processor still enforces its decision per batch. The point is
// CPU: a refused LP request costs no receive/decode, so shedding actually frees CPU.
package priorityotlpreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/priorityprocessor/priorityotlpreceiver"

import (
	"context"
	"errors"
	"net"
	"sync"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configgrpc"
	"go.opentelemetry.io/collector/config/confignet"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/receiverhelper"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/tap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/priorityprocessor"
)

const (
	typeStr        = "priorityotlp"
	exportMethod   = "/opentelemetry.proto.collector.trace.v1.TraceService/Export"
	dataFormatOTLP = "protobuf"
)

// Config is the gRPC server config of the stock OTLP receiver.
type Config struct {
	GRPC configgrpc.ServerConfig `mapstructure:"grpc"`
}

// NewFactory registers the "priorityotlp" receiver.
func NewFactory() receiver.Factory {
	return receiver.NewFactory(component.MustNewType(typeStr), createDefaultConfig,
		receiver.WithTraces(createTraces, component.StabilityLevelDevelopment))
}

func createDefaultConfig() component.Config {
	cfg := configgrpc.NewDefaultServerConfig()
	cfg.NetAddr = confignet.AddrConfig{Endpoint: "0.0.0.0:4317", Transport: confignet.TransportTypeTCP}
	cfg.IncludeMetadata = true // the priority processor reads bridges-priority from client metadata
	return &Config{GRPC: cfg}
}

func createTraces(_ context.Context, set receiver.Settings, cfg component.Config, next consumer.Traces) (receiver.Traces, error) {
	obs, err := receiverhelper.NewObsReport(receiverhelper.ObsReportSettings{
		ReceiverID: set.ID, Transport: "grpc", ReceiverCreateSettings: set})
	if err != nil {
		return nil, err
	}
	return &priorityReceiver{cfg: cfg.(*Config), set: set, next: next, obs: obs}, nil
}

type priorityReceiver struct {
	ptraceotlp.UnimplementedGRPCServer
	cfg    *Config
	set    receiver.Settings
	next   consumer.Traces
	obs    *receiverhelper.ObsReport
	server *grpc.Server
	wg     sync.WaitGroup
}

// tapDecision is the pre-decode check: method + bridges-priority header only.
func tapDecision(info *tap.Info) error {
	if info.FullMethodName != exportMethod {
		return nil
	}
	// Tomislav-RetCtx: an upstream hop with checkpoints waiting marks its sends; the mark makes
	// this hop refuse LP until that backlog clears (priorityprocessor.HPWaitingKey).
	priorityprocessor.MarkUpstreamHPWaiting(info.Header.Get(priorityprocessor.HPWaitingKey))
	decision, code := priorityprocessor.Decide(priorityprocessor.IsLPHeader(info.Header.Get(priorityprocessor.MetadataPriorityKey)))
	switch decision {
	case priorityprocessor.GateRefuseLP:
		return status.Error(code, "priorityotlp: shedding LP (refused before decode)")
	case priorityprocessor.GateRefuseAll:
		return status.Error(code, "priorityotlp: refusing all (refused before decode)")
	}
	return nil
}

func (r *priorityReceiver) Start(ctx context.Context, host component.Host) error {
	server, err := r.cfg.GRPC.ToServer(ctx, host, r.set.TelemetrySettings,
		configgrpc.WithGrpcServerOption(grpc.InTapHandle(func(ctx context.Context, info *tap.Info) (context.Context, error) {
			return ctx, tapDecision(info)
		})))
	if err != nil {
		return err
	}
	r.server = server
	ptraceotlp.RegisterGRPCServer(server, r)
	ln, err := r.cfg.GRPC.NetAddr.Listen(ctx)
	if err != nil {
		return err
	}
	r.set.Logger.Info("priorityotlp receiver listening (pre-decode admission via the priority processor's gate)",
		zap.String("endpoint", ln.Addr().String()))
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		if err := server.Serve(ln); err != nil && !errors.Is(err, grpc.ErrServerStopped) && !errors.Is(err, net.ErrClosed) {
			r.set.Logger.Error("priorityotlp server stopped", zap.Error(err))
		}
	}()
	return nil
}

func (r *priorityReceiver) Shutdown(context.Context) error {
	if r.server != nil {
		r.server.GracefulStop()
	}
	r.wg.Wait()
	return nil
}

// Export mirrors the stock OTLP receiver's trace handler.
func (r *priorityReceiver) Export(ctx context.Context, req ptraceotlp.ExportRequest) (ptraceotlp.ExportResponse, error) {
	td := req.Traces()
	numSpans := td.SpanCount()
	if numSpans == 0 {
		return ptraceotlp.NewExportResponse(), nil
	}
	ctx = r.obs.StartTracesOp(ctx)
	err := r.next.ConsumeTraces(ctx, td)
	r.obs.EndTracesOp(ctx, dataFormatOTLP, numSpans, err)
	if err != nil {
		if _, ok := status.FromError(err); ok {
			return ptraceotlp.NewExportResponse(), err
		}
		return ptraceotlp.NewExportResponse(), status.Error(codes.Unavailable, err.Error())
	}
	return ptraceotlp.NewExportResponse(), nil
}
