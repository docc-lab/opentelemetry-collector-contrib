// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Command sink is a stand-in for the gateway: an OTLP/gRPC server that counts the spans it
// receives per bridges-priority class, at a capacity that follows a schedule. A request waits
// for its share of the capacity (spans/s), so a slow sink pushes back on the collector; once
// the wait would exceed -max-queue the request is refused with Unavailable, and at capacity 0
// every request is. One JSON line per second.
//
// Tomislav-RetCtx: A/B harness for the priority pipelines (see ../README.md).
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/mem"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type rawCodec struct{}

func (rawCodec) Name() string { return "proto" }
func (rawCodec) Marshal(any) (mem.BufferSlice, error) {
	return nil, nil
}

func (rawCodec) Unmarshal(data mem.BufferSlice, v any) error {
	*(v.(*int)) = countSpans(data.Materialize())
	return nil
}

// countSpans counts ExportTraceServiceRequest.resource_spans[].scope_spans[].spans[].
func countSpans(b []byte) int {
	n := 0
	walk(b, 0, &n)
	return n
}

func walk(b []byte, depth int, n *int) {
	for len(b) > 0 {
		tag, k := uvarint(b)
		if k <= 0 {
			return
		}
		b = b[k:]
		switch tag & 7 {
		case 0:
			_, k = uvarint(b)
			if k <= 0 {
				return
			}
			b = b[k:]
		case 1:
			b = b[min(8, len(b)):]
		case 5:
			b = b[min(4, len(b)):]
		case 2:
			l, k := uvarint(b)
			if k <= 0 || l > uint64(len(b)-k) {
				return
			}
			body := b[k : k+int(l)]
			b = b[k+int(l):]
			field := tag >> 3
			switch {
			case depth < 2 && (depth == 0 && field == 1 || depth == 1 && field == 2):
				walk(body, depth+1, n)
			case depth == 2 && field == 2:
				*n++
			}
		default:
			return
		}
	}
}

func uvarint(b []byte) (uint64, int) {
	var v uint64
	for i := 0; i < len(b) && i < 10; i++ {
		v |= uint64(b[i]&0x7f) << (7 * i)
		if b[i] < 0x80 {
			return v, i + 1
		}
	}
	return 0, 0
}

type step struct {
	d   time.Duration
	cap float64 // spans/s; +Inf unlimited, 0 refuse all
}

func parseSteps(s string) ([]step, error) {
	var out []step
	for _, part := range strings.Split(s, ",") {
		d, c, ok := strings.Cut(strings.TrimSpace(part), ":")
		if !ok {
			return nil, fmt.Errorf("step %q is not duration:capacity", part)
		}
		dur, err := time.ParseDuration(d)
		if err != nil {
			return nil, err
		}
		cp := math.Inf(1)
		if c != "inf" {
			if cp, err = strconv.ParseFloat(c, 64); err != nil {
				return nil, err
			}
		}
		out = append(out, step{dur, cp})
	}
	return out, nil
}

type sink struct {
	mu       sync.Mutex
	capacity float64
	nextFree time.Time
	maxQueue time.Duration

	hpSpans, lpSpans, hpReq, lpReq   atomic.Int64
	hpRefused, lpRefused, refusedReq atomic.Int64
}

func (s *sink) export(ctx context.Context, n int, hp bool) error {
	s.mu.Lock()
	now := time.Now()
	var wait time.Duration
	switch {
	case s.capacity == 0:
		s.mu.Unlock()
		return s.refuse(n, hp, "sink stalled")
	case !math.IsInf(s.capacity, 1):
		start := s.nextFree
		if start.Before(now) {
			start = now
		}
		done := start.Add(time.Duration(float64(n) / s.capacity * float64(time.Second)))
		if done.Sub(now) > s.maxQueue {
			s.mu.Unlock()
			return s.refuse(n, hp, "sink queue full")
		}
		s.nextFree = done
		wait = done.Sub(now)
	}
	s.mu.Unlock()
	if wait > 0 {
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if hp {
		s.hpSpans.Add(int64(n))
		s.hpReq.Add(1)
	} else {
		s.lpSpans.Add(int64(n))
		s.lpReq.Add(1)
	}
	return nil
}

func (s *sink) refuse(n int, hp bool, why string) error {
	s.refusedReq.Add(1)
	if hp {
		s.hpRefused.Add(int64(n))
	} else {
		s.lpRefused.Add(int64(n))
	}
	return status.Error(codes.Unavailable, why)
}

func main() {
	addr := flag.String("listen", "0.0.0.0:4317", "listen address")
	sched := flag.String("capacity", "1h:inf", "comma-separated duration:spans-per-second steps (inf, or 0 to refuse all); the last holds")
	maxQueue := flag.Duration("max-queue", time.Second, "refuse a request that would wait longer than this")
	flag.Parse()
	steps, err := parseSteps(*sched)
	if err != nil {
		log.Fatal(err)
	}
	s := &sink{capacity: steps[0].cap, maxQueue: *maxQueue}
	srv := grpc.NewServer(grpc.ForceServerCodecV2(rawCodec{}))
	srv.RegisterService(&grpc.ServiceDesc{
		ServiceName: "opentelemetry.proto.collector.trace.v1.TraceService",
		HandlerType: (*any)(nil),
		Methods: []grpc.MethodDesc{{MethodName: "Export", Handler: func(_ any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
			var n int
			if err := dec(&n); err != nil {
				return nil, err
			}
			md, _ := metadata.FromIncomingContext(ctx)
			hp := true
			for _, v := range md.Get("bridges-priority") {
				hp = hp && !strings.EqualFold(v, "lp")
			}
			if err := s.export(ctx, n, hp); err != nil {
				return nil, err
			}
			return &n, nil
		}}},
	}, nil)
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatal(err)
	}
	go func() { log.Fatal(srv.Serve(ln)) }()

	start := time.Now()
	go func() {
		for _, st := range steps {
			s.mu.Lock()
			s.capacity = st.cap
			s.mu.Unlock()
			time.Sleep(st.d)
		}
	}()
	enc := json.NewEncoder(os.Stdout)
	for range time.Tick(time.Second) {
		s.mu.Lock()
		c := s.capacity
		s.mu.Unlock()
		if math.IsInf(c, 1) {
			c = -1
		}
		_ = enc.Encode(map[string]any{"t": time.Since(start).Seconds(), "capacity": c,
			"hp_spans": s.hpSpans.Load(), "lp_spans": s.lpSpans.Load(), "hp_requests": s.hpReq.Load(), "lp_requests": s.lpReq.Load(),
			"hp_refused_spans": s.hpRefused.Load(), "lp_refused_spans": s.lpRefused.Load(), "refused_requests": s.refusedReq.Load()})
	}
}
