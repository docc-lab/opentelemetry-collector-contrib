// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Command load offers span load to an OTLP/gRPC collector the way the bridges SDK exports:
// every 100 ms each client sends its spans in requests of at most 512, checkpoints and the
// rest in separate requests tagged "bridges-priority: hp|lp". The load is open-loop: a client
// with too many requests in flight drops new ones and counts them. One JSON line per second.
//
// Tomislav-RetCtx: A/B harness for the priority pipelines (see ../README.md).
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/mem"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	exportMethod = "/opentelemetry.proto.collector.trace.v1.TraceService/Export"
	tick         = 100 * time.Millisecond
	maxRequest   = 512
)

type rawCodec struct{}

func (rawCodec) Name() string { return "proto" }
func (rawCodec) Marshal(v any) (mem.BufferSlice, error) {
	return mem.BufferSlice{mem.SliceBuffer(v.([]byte))}, nil
}
func (rawCodec) Unmarshal(mem.BufferSlice, any) error { return nil }

type step struct {
	d    time.Duration
	rate float64
}

func parseSteps(s string) ([]step, error) {
	var out []step
	for _, part := range strings.Split(s, ",") {
		d, r, ok := strings.Cut(strings.TrimSpace(part), ":")
		if !ok {
			return nil, fmt.Errorf("step %q is not duration:rate", part)
		}
		dur, err := time.ParseDuration(d)
		if err != nil {
			return nil, err
		}
		rate, err := strconv.ParseFloat(r, 64)
		if err != nil {
			return nil, err
		}
		out = append(out, step{dur, rate})
	}
	return out, nil
}

// payloads pre-encodes one request per (class, span count); span IDs repeat, which no
// collector here looks at.
type payloads struct {
	attrs int
	mu    sync.Mutex
	cache map[[2]int][]byte
}

func (p *payloads) get(hp bool, n int) []byte {
	key := [2]int{n, 0}
	if hp {
		key[1] = 1
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if b, ok := p.cache[key]; ok {
		return b
	}
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "compose-post-service")
	ss := rs.ScopeSpans().AppendEmpty()
	ss.Scope().SetName("blueprint")
	now := time.Now()
	for i := 0; i < n; i++ {
		sp := ss.Spans().AppendEmpty()
		sp.SetTraceID(pcommon.TraceID{byte(i), byte(i >> 8), 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14})
		sp.SetSpanID(pcommon.SpanID{byte(i), byte(i >> 8), 1, 2, 3, 4, 5, 6})
		sp.SetParentSpanID(pcommon.SpanID{9, byte(i), 1, 2, 3, 4, 5, 6})
		sp.SetName("ComposePostService.ComposePost")
		sp.SetKind(ptrace.SpanKindServer)
		sp.SetStartTimestamp(pcommon.NewTimestampFromTime(now))
		sp.SetEndTimestamp(pcommon.NewTimestampFromTime(now.Add(time.Millisecond)))
		for a := 0; a < p.attrs; a++ {
			sp.Attributes().PutStr("attr."+strconv.Itoa(a), "value-"+strconv.Itoa(a))
		}
		if hp {
			sp.Attributes().PutEmptyBytes("_br").FromRaw(make([]byte, 24))
		} else {
			sp.Attributes().PutEmptyBytes("_d").FromRaw([]byte{6})
		}
	}
	b, err := (&ptrace.ProtoMarshaler{}).MarshalTraces(td)
	if err != nil {
		log.Fatal(err)
	}
	p.cache[key] = b
	return b
}

type class struct {
	offered, acked, dropped atomic.Int64
	refused                 sync.Map // code -> *atomic.Int64
	latMu                   sync.Mutex
	lat                     []time.Duration
}

func (c *class) refuse(code codes.Code, n int64) {
	v, _ := c.refused.LoadOrStore(code.String(), new(atomic.Int64))
	v.(*atomic.Int64).Add(n)
}

type snapshot struct {
	Offered int64            `json:"offered"`
	Acked   int64            `json:"acked"`
	Dropped int64            `json:"dropped"`
	Refused map[string]int64 `json:"refused"`
	P50ms   float64          `json:"p50_ms"`
	P99ms   float64          `json:"p99_ms"`
	MaxMs   float64          `json:"max_ms"`
}

func (c *class) snap() snapshot {
	s := snapshot{Offered: c.offered.Load(), Acked: c.acked.Load(), Dropped: c.dropped.Load(), Refused: map[string]int64{}}
	c.refused.Range(func(k, v any) bool { s.Refused[k.(string)] = v.(*atomic.Int64).Load(); return true })
	c.latMu.Lock()
	lat := c.lat
	c.lat = nil
	c.latMu.Unlock()
	if len(lat) > 0 {
		sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
		ms := func(q float64) float64 { return float64(lat[int(q*float64(len(lat)-1))]) / 1e6 }
		s.P50ms, s.P99ms, s.MaxMs = ms(0.5), ms(0.99), ms(1)
	}
	return s
}

func main() {
	endpoint := flag.String("endpoint", "127.0.0.1:4317", "collector OTLP/gRPC endpoint")
	stepsFlag := flag.String("steps", "30s:10000", "comma-separated duration:spans-per-second steps")
	clients := flag.Int("clients", 16, "SDK clients, one connection each")
	hpFrac := flag.Float64("hp-fraction", 0.5, "share of spans that are checkpoints")
	attrs := flag.Int("attrs", 2, "string attributes per span besides the bridge attribute")
	inflight := flag.Int("inflight", 8, "requests in flight per client before new ones are dropped")
	timeout := flag.Duration("timeout", 5*time.Second, "per-request timeout")
	flag.Parse()
	steps, err := parseSteps(*stepsFlag)
	if err != nil {
		log.Fatal(err)
	}
	p := &payloads{attrs: *attrs, cache: map[[2]int][]byte{}}
	hpB, lpB := len(p.get(true, 100))/100, len(p.get(false, 100))/100
	log.Printf("bytes per span: hp %d lp %d", hpB, lpB)

	var hp, lp class
	ctx, cancel := context.WithCancel(context.Background())
	var rate atomic.Uint64 // float64 bits
	var wg sync.WaitGroup
	for c := 0; c < *clients; c++ {
		cc, err := grpc.NewClient(*endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatal(err)
		}
		defer cc.Close()
		sem := make(chan struct{}, *inflight)
		send := func(isHP bool, n int) {
			cl, prio := &lp, "lp"
			if isHP {
				cl, prio = &hp, "hp"
			}
			cl.offered.Add(int64(n))
			select {
			case sem <- struct{}{}:
			default:
				cl.dropped.Add(int64(n))
				return
			}
			go func() {
				defer func() { <-sem }()
				rctx, rcancel := context.WithTimeout(metadata.AppendToOutgoingContext(ctx, "bridges-priority", prio), *timeout)
				t0 := time.Now()
				var resp []byte
				err := cc.Invoke(rctx, exportMethod, p.get(isHP, n), &resp, grpc.ForceCodecV2(rawCodec{}))
				rcancel()
				d := time.Since(t0)
				if err == nil {
					cl.acked.Add(int64(n))
					cl.latMu.Lock()
					cl.lat = append(cl.lat, d)
					cl.latMu.Unlock()
				} else {
					cl.refuse(status.Code(err), int64(n))
				}
			}()
		}
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			t := time.NewTicker(tick)
			defer t.Stop()
			var owed, hpOwed float64
			// stagger clients across the tick, as independent services are
			time.Sleep(time.Duration(c) * tick / time.Duration(*clients))
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
				}
				owed += mathFloat(rate.Load()) / float64(*clients) * tick.Seconds()
				n := int(owed)
				owed -= float64(n)
				hpOwed += float64(n) * *hpFrac
				h := int(hpOwed)
				hpOwed -= float64(h)
				for k := h; k > 0; k -= maxRequest {
					send(true, min(k, maxRequest))
				}
				for k := n - h; k > 0; k -= maxRequest {
					send(false, min(k, maxRequest))
				}
			}
		}(c)
	}

	enc := json.NewEncoder(os.Stdout)
	start := time.Now()
	done := make(chan struct{})
	go func() {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				_ = enc.Encode(map[string]any{"t": time.Since(start).Seconds(), "rate": mathFloat(rate.Load()), "hp": hp.snap(), "lp": lp.snap()})
			}
		}
	}()
	for _, s := range steps {
		rate.Store(floatBits(s.rate))
		time.Sleep(s.d)
	}
	rate.Store(0)
	time.Sleep(*timeout + time.Second) // let requests in flight finish
	cancel()
	wg.Wait()
	close(done)
	_ = enc.Encode(map[string]any{"final": true, "t": time.Since(start).Seconds(), "hp": hp.snap(), "lp": lp.snap()})
}
