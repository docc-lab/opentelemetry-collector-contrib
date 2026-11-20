// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Package priorityarenareceiver is an OTLP/gRPC trace receiver that holds every buffered
// request in a fixed arena outside the Go heap, admits and evicts by exact byte counts, and
// sends checkpoints first.
//
// Tomislav-RetCtx: requests are priority-homogeneous and tagged by the bridges SDK with the
// "bridges-priority: hp|lp" gRPC header. The receiver never builds pdata on the forwarding
// path: requests are counted with a wire scan, copied into the arena and sent on as bytes.
// See README.md.
package priorityarenareceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/priorityprocessor/priorityarenareceiver"

import (
	"context"
	"errors"
	"net"
	"runtime/metrics"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configgrpc"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/receiverhelper"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/tap"
)

const (
	typeStr       = "priorityarena"
	exportMethod  = "/opentelemetry.proto.collector.trace.v1.TraceService/Export"
	priorityKey   = "bridges-priority"
	formatProto   = "protobuf"
	shutdownGrace = 5 * time.Second
)

// NewFactory registers the "priorityarena" receiver.
func NewFactory() receiver.Factory {
	return receiver.NewFactory(component.MustNewType(typeStr), createDefaultConfig,
		receiver.WithTraces(createTraces, component.StabilityLevelDevelopment))
}

func createTraces(_ context.Context, set receiver.Settings, cfg component.Config, next consumer.Traces) (receiver.Traces, error) {
	obs, err := receiverhelper.NewObsReport(receiverhelper.ObsReportSettings{
		ReceiverID: set.ID, Transport: "grpc", ReceiverCreateSettings: set})
	if err != nil {
		return nil, err
	}
	c := cfg.(*Config)
	lp := codes.ResourceExhausted
	if c.LPRefusalCode == LPRefusalUnavailable {
		lp = codes.Unavailable
	}
	return &arenaReceiver{cfg: c, set: set, next: next, obs: obs, lpCode: lp}, nil
}

type arenaReceiver struct {
	cfg    *Config
	set    receiver.Settings
	next   consumer.Traces
	obs    *receiverhelper.ObsReport
	lpCode codes.Code

	store   *store
	adapter *lpAdapter // nil when the LP limit is fixed
	gauges  metric.Registration
	addr    net.Addr
	server  *grpc.Server
	cc      *grpc.ClientConn
	cancel  context.CancelFunc
	serve   sync.WaitGroup
	work    sync.WaitGroup
}

var serviceDesc = grpc.ServiceDesc{
	ServiceName: "opentelemetry.proto.collector.trace.v1.TraceService",
	HandlerType: (*any)(nil),
	Methods:     []grpc.MethodDesc{{MethodName: "Export", Handler: exportHandler}},
	Metadata:    "opentelemetry/proto/collector/trace/v1/trace_service.proto",
}

func (r *arenaReceiver) Start(ctx context.Context, host component.Host) error {
	sz, err := r.cfg.sizing(memoryLimit, r.set.Logger)
	if err != nil {
		return err
	}
	a, err := newArena(sz.bytes, r.cfg.BlockKiB<<10, r.cfg.Mmap)
	if err != nil {
		return err
	}
	r.store = newStore(a, sz.lpPct)
	if sz.bandPct > 0 {
		r.adapter = newLPAdapter(r.store, float64(a.nblocks)*sz.bandPct/100, int(r.cfg.SizeWindow/adaptStep), r.set.Logger)
	}

	server, err := r.cfg.GRPC.ToServer(ctx, host, r.set.TelemetrySettings,
		configgrpc.WithGrpcServerOption(grpc.ForceServerCodecV2(arenaCodec{})),
		configgrpc.WithGrpcServerOption(grpc.InTapHandle(r.tap)))
	if err != nil {
		return err
	}
	r.server = server
	server.RegisterService(&serviceDesc, r)
	ln, err := r.cfg.GRPC.NetAddr.Listen(ctx)
	if err != nil {
		return err
	}
	r.addr = ln.Addr()

	wctx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	lim := takeLimits{maxSpans: r.cfg.MaxBatchSpans, maxBytes: r.cfg.MaxBatchKiB << 10, linger: r.cfg.Linger}
	if r.cfg.Forward.Endpoint != "" {
		r.cc, err = grpc.NewClient(r.cfg.Forward.Endpoint,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(r.cfg.MaxBatchKiB<<10+r.cfg.BlockKiB<<10)),
			grpc.WithUserAgent("priorityarena"))
		if err != nil {
			cancel()
			return err
		}
		f := &forwarder{s: r.store, cc: r.cc, cfg: r.cfg.Forward, lim: lim, logger: r.set.Logger}
		f.hpBack.init, f.hpBack.max = r.cfg.Forward.BackoffInitial, r.cfg.Forward.BackoffMax
		f.lpBack.init, f.lpBack.max = r.cfg.Forward.BackoffInitial, r.cfg.Forward.BackoffMax
		r.spawn(r.cfg.Workers, func() { f.run(wctx) })
	} else {
		lim.mixed = true
		p := &pipeliner{s: r.store, next: r.next, lim: lim, logger: r.set.Logger}
		p.back.init, p.back.max = 50*time.Millisecond, time.Second
		r.spawn(r.cfg.Workers, func() { p.run(wctx) })
	}
	r.spawn(1, func() { r.metricsLoop(wctx) })
	if r.adapter != nil {
		r.spawn(1, func() { r.adapter.run(wctx) })
	}
	if err := r.registerGauges(); err != nil {
		r.set.Logger.Warn("priorityarena: gauges not registered", zap.Error(err))
	}

	r.set.Logger.Info("priorityarena receiver listening",
		zap.String("endpoint", ln.Addr().String()),
		zap.Int("arena_blocks", int(a.nblocks)), zap.Int("block_bytes", a.blockSize), zap.Bool("mmap", a.mapped),
		zap.Int32("lp_limit_blocks", r.store.lpLimit.Load()), zap.Bool("lp_limit_adaptive", r.adapter != nil),
		zap.String("forward", r.cfg.Forward.Endpoint),
		zap.Int("workers", r.cfg.Workers))
	r.serve.Add(1)
	go func() {
		defer r.serve.Done()
		if err := server.Serve(ln); err != nil && !errors.Is(err, grpc.ErrServerStopped) && !errors.Is(err, net.ErrClosed) {
			r.set.Logger.Error("priorityarena server stopped", zap.Error(err))
		}
	}()
	return nil
}

func (r *arenaReceiver) spawn(n int, f func()) {
	for i := 0; i < n; i++ {
		r.work.Add(1)
		go func() { defer r.work.Done(); f() }()
	}
}

func (r *arenaReceiver) Shutdown(context.Context) error {
	if r.server != nil {
		done := make(chan struct{})
		go func() { r.server.GracefulStop(); close(done) }()
		select {
		case <-done:
		case <-time.After(shutdownGrace):
			r.server.Stop()
		}
		r.serve.Wait()
	}
	if r.cancel != nil {
		r.cancel()
		r.work.Wait()
	}
	if r.cc != nil {
		_ = r.cc.Close()
	}
	if r.gauges != nil {
		_ = r.gauges.Unregister()
	}
	if r.store == nil {
		return nil
	}
	r.logMetrics("priority_arena_shutdown_metrics")
	// gRPC may still hold blocks of an aborted send; the mapping is then left to the process exit.
	for i := range r.store.lent {
		if r.store.lent[i].Load() != 0 {
			return nil
		}
	}
	return r.store.a.close()
}

// tap refuses a request from its headers alone, before gRPC reads it, when the arena is
// already past what its class may use.
func (r *arenaReceiver) tap(ctx context.Context, info *tap.Info) (context.Context, error) {
	if info.FullMethodName != exportMethod {
		return ctx, nil
	}
	if isLP(info.Header.Get(priorityKey)) {
		if r.store.refuseLPEarly() {
			r.store.c.lpRefusedReq.Add(1)
			return ctx, status.Error(r.lpCode, "priorityarena: LP limit reached (refused before reading)")
		}
	} else if r.store.refuseHPEarly() {
		r.store.c.hpRefusedReq.Add(1)
		return ctx, status.Error(codes.Unavailable, "priorityarena: full of checkpoints (refused before reading)")
	}
	return ctx, nil
}

func isLP(values []string) bool {
	for _, v := range values {
		if len(v) == 2 && (v[0] == 'l' || v[0] == 'L') && (v[1] == 'p' || v[1] == 'P') {
			return true
		}
	}
	return false
}

func exportHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	r := srv.(*arenaReceiver)
	md, _ := metadata.FromIncomingContext(ctx)
	in := &inbound{s: r.store, hp: !isLP(md.Get(priorityKey)), lp: r.lpCode}
	ctx = r.obs.StartTracesOp(ctx)
	if err := dec(in); err != nil {
		r.obs.EndTracesOp(ctx, formatProto, 0, err)
		return nil, err
	}
	handle := func(ctx context.Context, _ any) (any, error) {
		var err error
		if in.code != codes.OK {
			err = status.Error(in.code, in.msg)
		}
		r.obs.EndTracesOp(ctx, formatProto, in.spans, err)
		if err != nil {
			return nil, err
		}
		return &exportResponse{}, nil
	}
	if interceptor == nil {
		return handle(ctx, in)
	}
	return interceptor(ctx, in, &grpc.UnaryServerInfo{Server: srv, FullMethod: exportMethod}, handle)
}

// metrics

// registerGauges publishes the arena's state with the collector's own metrics (port 8888):
// blocks in use (queued, in flight or being filled), spans queued, and the LP limit in force.
// A drained receiver has no blocks in use.
func (r *arenaReceiver) registerGauges() error {
	m := r.set.TelemetrySettings.MeterProvider.Meter("github.com/open-telemetry/opentelemetry-collector-contrib/processor/priorityprocessor/priorityarenareceiver")
	used, err := m.Int64ObservableGauge("otelcol_priorityarena_blocks_used",
		metric.WithDescription("Arena blocks in use: queued, in flight or being filled"))
	if err != nil {
		return err
	}
	queued, err := m.Int64ObservableGauge("otelcol_priorityarena_queued_spans",
		metric.WithDescription("Spans queued in the arena"))
	if err != nil {
		return err
	}
	lpLimit, err := m.Int64ObservableGauge("otelcol_priorityarena_lp_limit_blocks",
		metric.WithDescription("Arena blocks in use up to which LP is admitted"))
	if err != nil {
		return err
	}
	s := r.store
	r.gauges, err = m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		o.ObserveInt64(used, int64(s.used.Load()))
		o.ObserveInt64(queued, s.c.hpQueuedSpans.Load()+s.c.lpQueuedSpans.Load())
		o.ObserveInt64(lpLimit, int64(s.lpLimit.Load()))
		return nil
	}, used, queued, lpLimit)
	return err
}

var runtimeSamples = []string{
	"/gc/cycles/total:gc-cycles",
	"/cpu/classes/gc/total:cpu-seconds",
	"/gc/heap/live:bytes",
	"/memory/classes/heap/objects:bytes",
	"/gc/heap/allocs:bytes",
}

func (r *arenaReceiver) metricsLoop(ctx context.Context) {
	t := time.NewTicker(r.cfg.MetricsInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.logMetrics("priority_arena_metrics")
		}
	}
}

func (r *arenaReceiver) logMetrics(name string) {
	s, c := r.store, &r.store.c
	samples := make([]metrics.Sample, len(runtimeSamples))
	for i, n := range runtimeSamples {
		samples[i].Name = n
	}
	metrics.Read(samples)
	rt := func(i int) zap.Field {
		v := samples[i].Value
		switch v.Kind() {
		case metrics.KindUint64:
			return zap.Uint64(runtimeField[i], v.Uint64())
		case metrics.KindFloat64:
			return zap.Float64(runtimeField[i], v.Float64())
		}
		return zap.Skip()
	}
	ratio := zap.Skip()
	if r.adapter != nil {
		ratio = zap.Float64("size_ratio", r.adapter.ratioNow()) // NaN until the first window
	}
	r.set.Logger.Info(name,
		zap.Int64("hp_admitted", c.hpAdmitted.Load()),
		zap.Int64("lp_admitted", c.lpAdmitted.Load()),
		zap.Int64("hp_admitted_bytes", c.hpAdmittedB.Load()),
		zap.Int64("lp_admitted_bytes", c.lpAdmittedB.Load()),
		zap.Int64("hp_admitted_blocks", c.hpAdmittedBlk.Load()),
		zap.Int64("lp_admitted_blocks", c.lpAdmittedBlk.Load()),
		zap.Int64("hp_refused", c.hpRefused.Load()),
		zap.Int64("lp_refused", c.lpRefused.Load()),
		zap.Int64("hp_refused_requests_predecode", c.hpRefusedReq.Load()),
		zap.Int64("lp_refused_requests_predecode", c.lpRefusedReq.Load()),
		zap.Int64("hp_admitted_by_eviction", c.hpByEviction.Load()),
		zap.Int64("lp_evicted", c.lpEvicted.Load()),
		zap.Int64("lp_evicted_requests", c.lpEvictedReq.Load()),
		zap.Int64("hp_sent", c.hpSent.Load()),
		zap.Int64("lp_sent", c.lpSent.Load()),
		zap.Int64("hp_requeued", c.hpRequeued.Load()),
		zap.Int64("lp_requeued", c.lpRequeued.Load()),
		zap.Int64("hp_failed", c.hpFailed.Load()),
		zap.Int64("lp_failed", c.lpFailed.Load()),
		zap.Int64("malformed_requests", c.malformed.Load()),
		zap.Int64("sends", c.sends.Load()),
		zap.Int64("send_errors", c.sendErrors.Load()),
		zap.Int64("hp_queued_spans", c.hpQueuedSpans.Load()),
		zap.Int64("lp_queued_spans", c.lpQueuedSpans.Load()),
		zap.Int64("hp_queued_blocks", c.hpQueuedBlocks.Load()),
		zap.Int64("lp_queued_blocks", c.lpQueuedBlocks.Load()),
		zap.Int32("blocks_used", s.used.Load()),
		zap.Int32("blocks_used_max", s.usedMax.Load()),
		zap.Int32("blocks_total", s.a.nblocks),
		zap.Int32("lp_used_blocks", s.lpUsedA.Load()),
		zap.Int32("lp_limit_blocks", s.lpLimit.Load()),
		ratio,
		zap.Int("block_bytes", s.a.blockSize),
		rt(0), rt(1), rt(2), rt(3), rt(4))
}

var runtimeField = []string{"gc_cycles", "gc_cpu_seconds", "heap_live_bytes", "heap_objects_bytes", "heap_allocs_bytes_total"}
