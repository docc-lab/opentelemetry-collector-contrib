// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package priorityarenareceiver

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/receiver/receivertest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/mem"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// rawCodec sends request bytes as they are, like the bridges SDK.
type rawCodec struct{}

func (rawCodec) Name() string { return "proto" }
func (rawCodec) Marshal(v any) (mem.BufferSlice, error) {
	return mem.BufferSlice{mem.SliceBuffer(v.([]byte))}, nil
}
func (rawCodec) Unmarshal(mem.BufferSlice, any) error { return nil }

func start(t *testing.T, mutate func(*Config), next consumer.Traces) *arenaReceiver {
	f := NewFactory()
	cfg := f.CreateDefaultConfig().(*Config)
	cfg.GRPC.NetAddr.Endpoint = "127.0.0.1:0"
	cfg.MetricsInterval = time.Hour
	mutate(cfg)
	require.NoError(t, cfg.Validate())
	r, err := f.CreateTraces(context.Background(), receivertest.NewNopSettings(f.Type()), cfg, next)
	require.NoError(t, err)
	require.NoError(t, r.Start(context.Background(), componenttest.NewNopHost()))
	return r.(*arenaReceiver)
}

type client struct{ cc *grpc.ClientConn }

func dial(t *testing.T, r *arenaReceiver) *client {
	cc, err := grpc.NewClient(r.addr.String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { cc.Close() })
	return &client{cc}
}

func (c *client) export(hp bool, req []byte) error {
	class := "lp"
	if hp {
		class = "hp"
	}
	ctx, cancel := context.WithTimeout(metadata.AppendToOutgoingContext(context.Background(), priorityKey, class), 5*time.Second)
	defer cancel()
	var resp []byte
	return c.cc.Invoke(ctx, exportMethod, req, &resp, grpc.ForceCodecV2(rawCodec{}))
}

// request builds a request of n spans whose resource is tagged with its class.
func request(t testing.TB, r *rand.Rand, hp bool, n int) []byte {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutBool("hp", hp)
	ss := rs.ScopeSpans().AppendEmpty()
	for i := 0; i < n; i++ {
		sp := ss.Spans().AppendEmpty()
		sp.SetName(fmt.Sprintf("op-%d", r.Intn(100)))
		sp.Attributes().PutInt("i", int64(i))
		sp.Attributes().PutEmptyBytes("_br").FromRaw(make([]byte, 24))
	}
	return marshal(t, td)
}

// spansByClass counts the spans a sink got, by the class their resource carries.
func spansByClass(sink *consumertest.TracesSink) (hp, lp int) {
	for _, td := range sink.AllTraces() {
		for i := 0; i < td.ResourceSpans().Len(); i++ {
			rs := td.ResourceSpans().At(i)
			v, _ := rs.Resource().Attributes().Get("hp")
			n := 0
			for j := 0; j < rs.ScopeSpans().Len(); j++ {
				n += rs.ScopeSpans().At(j).Spans().Len()
			}
			if v.Bool() {
				hp += n
			} else {
				lp += n
			}
		}
	}
	return
}

func TestForwardThenPipeline(t *testing.T) {
	sink := new(consumertest.TracesSink)
	gw := start(t, func(c *Config) { c.Workers = 2; c.MaxBatchSpans = 300 }, sink)
	agent := start(t, func(c *Config) { c.Forward.Endpoint = gw.addr.String(); c.Workers = 4 }, consumertest.NewNop())
	c := dial(t, agent)
	r := rand.New(rand.NewSource(6))
	wantHP, wantLP := 0, 0
	for i := 0; i < 200; i++ {
		hp, n := i%3 == 0, r.Intn(100)+1
		require.NoError(t, c.export(hp, request(t, r, hp, n)))
		if hp {
			wantHP += n
		} else {
			wantLP += n
		}
	}
	require.Eventually(t, func() bool { return sink.SpanCount() == wantHP+wantLP }, 10*time.Second, 10*time.Millisecond)
	hp, lp := spansByClass(sink)
	require.Equal(t, wantHP, hp)
	require.Equal(t, wantLP, lp)
	require.NoError(t, agent.Shutdown(context.Background()))
	require.NoError(t, gw.Shutdown(context.Background()))
	for _, rc := range []*arenaReceiver{agent, gw} {
		require.NoError(t, rc.store.check())
		require.Equal(t, rc.store.a.nblocks, rc.store.a.free, "every block back")
	}
}

func TestMalformedRequestRefused(t *testing.T) {
	gw := start(t, func(*Config) {}, consumertest.NewNop())
	defer gw.Shutdown(context.Background())
	err := dial(t, gw).export(true, []byte{0x0a, 0x05, 0x12})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

// blocker is a pipeline that holds every call until released, like a stalled store.
type blocker struct {
	consumertest.TracesSink
	release chan struct{}
}

func (b *blocker) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	select {
	case <-b.release:
	case <-ctx.Done():
		return ctx.Err()
	}
	return b.TracesSink.ConsumeTraces(ctx, td)
}

// TestOverloadKeepsCheckpoints stalls the gateway's pipeline, fills both arenas, and checks
// the policy end to end: LP is refused at the LP limit, checkpoints evict LP, checkpoints are
// refused only when no LP is left, and after the stall every acknowledged checkpoint arrives.
func TestOverloadKeepsCheckpoints(t *testing.T) {
	sink := &blocker{release: make(chan struct{})}
	gw := start(t, func(c *Config) { c.MemoryMiB = 1; c.Workers = 1; c.MaxBatchSpans = 100 }, sink)
	agent := start(t, func(c *Config) {
		c.MemoryMiB = 1
		c.LPLimitPercentage = 50
		c.Forward.Endpoint = gw.addr.String()
		c.Forward.BackoffInitial, c.Forward.BackoffMax = 5*time.Millisecond, 20*time.Millisecond
	}, consumertest.NewNop())
	c := dial(t, agent)
	r := rand.New(rand.NewSource(7))

	ackHP, ackLP := 0, 0
	var lpRefusal, hpRefusal codes.Code
	for i := 0; i < 2000 && lpRefusal == codes.OK; i++ {
		switch err := c.export(false, request(t, r, false, 50)); status.Code(err) {
		case codes.OK:
			ackLP += 50
		default:
			lpRefusal = status.Code(err)
		}
	}
	require.Equal(t, codes.ResourceExhausted, lpRefusal, "LP refused once both arenas hold their LP share")
	time.Sleep(100 * time.Millisecond) // let the agent settle what it can forward

	for i := 0; i < 2000 && hpRefusal == codes.OK; i++ {
		switch err := c.export(true, request(t, r, true, 50)); status.Code(err) {
		case codes.OK:
			ackHP += 50
		default:
			hpRefusal = status.Code(err)
		}
	}
	require.Equal(t, codes.Unavailable, hpRefusal)
	agent.store.mu.Lock()
	lpLeft := agent.store.lp.entries
	agent.store.mu.Unlock()
	require.Zero(t, lpLeft, "checkpoints refused while LP was still queued")
	require.Positive(t, agent.store.c.lpEvicted.Load())

	close(sink.release)
	require.Eventually(t, func() bool {
		hp, _ := spansByClass(&sink.TracesSink)
		return hp == ackHP
	}, 20*time.Second, 20*time.Millisecond, "every acknowledged checkpoint arrives")
	time.Sleep(200 * time.Millisecond)
	hp, lp := spansByClass(&sink.TracesSink)
	require.Equal(t, ackHP, hp)
	evicted := agent.store.c.lpEvicted.Load() + gw.store.c.lpEvicted.Load()
	require.Equal(t, int64(ackLP), int64(lp)+evicted, "acknowledged LP is delivered or evicted, nothing else")

	var wg sync.WaitGroup
	for _, rc := range []*arenaReceiver{agent, gw} {
		wg.Add(1)
		go func() { defer wg.Done(); require.NoError(t, rc.Shutdown(context.Background())) }()
	}
	wg.Wait()
	for _, rc := range []*arenaReceiver{agent, gw} {
		require.NoError(t, rc.store.check())
		c := &rc.store.c
		t.Logf("lp admitted %d refused %d evicted %d sent %d | hp admitted %d byEviction %d refused %d sent %d requeued %d | sends %d errors %d predecode lp %d hp %d",
			c.lpAdmitted.Load(), c.lpRefused.Load(), c.lpEvicted.Load(), c.lpSent.Load(), c.hpAdmitted.Load(), c.hpByEviction.Load(),
			c.hpRefused.Load(), c.hpSent.Load(), c.hpRequeued.Load()+c.lpRequeued.Load(), c.sends.Load(), c.sendErrors.Load(),
			c.lpRefusedReq.Load(), c.hpRefusedReq.Load())
	}
}
