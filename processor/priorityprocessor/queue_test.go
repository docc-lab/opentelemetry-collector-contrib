// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package priorityprocessor

// Tomislav-RetCtx: tests for the strict-priority queue stage (queue.go) and its coupling to the
// admission stage and the pre-decode gate.

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor/processortest"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/priorityprocessor/internal/metadata"
)

// gatedSink records (priority, span count) in arrival order; each send blocks until released.
type gatedSink struct {
	mu      sync.Mutex
	got     []string
	release chan struct{}
	started chan struct{}
}

func newGatedSink() *gatedSink {
	return &gatedSink{release: make(chan struct{}, 1000), started: make(chan struct{}, 1000)}
}

func (s *gatedSink) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	tag := "hp"
	if isLPBatch(ctx) {
		tag = "lp"
	}
	s.started <- struct{}{}
	select {
	case <-s.release:
	case <-ctx.Done():
		return ctx.Err()
	}
	s.mu.Lock()
	s.got = append(s.got, tag+":"+string(rune('0'+td.SpanCount())))
	s.mu.Unlock()
	return nil
}

func (s *gatedSink) order() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.got...)
}

func startQueue(t *testing.T, workers int, sink *gatedSink) *priorityQueue {
	t.Helper()
	c := createDefaultConfig().(*Config)
	c.Mode, c.DispatchWorkers = ModeQueue, workers
	require.NoError(t, c.Validate())
	q := newPriorityQueue(c, zap.NewNop(), &sinkConsumer{sink})
	require.NoError(t, q.Start(context.Background(), componenttest.NewNopHost()))
	t.Cleanup(func() {
		for i := 0; i < 100; i++ {
			sink.release <- struct{}{}
		}
		_ = q.Shutdown(context.Background())
	})
	return q
}

// sinkConsumer adapts gatedSink to consumer.Traces.
type sinkConsumer struct{ s *gatedSink }

func (c *sinkConsumer) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	return c.s.ConsumeTraces(ctx, td)
}
func (c *sinkConsumer) Capabilities() consumer.Capabilities { return consumer.Capabilities{} }

func waitFor(t *testing.T, ch chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out")
	}
}

// Checkpoints queued behind LP are dispatched first.
func TestQueueDispatchesHPFirst(t *testing.T) {
	sink := newGatedSink()
	q := startQueue(t, 1, sink)
	require.NoError(t, q.ConsumeTraces(lpContext(), traces(1))) // taken by the only worker
	waitFor(t, sink.started)
	require.NoError(t, q.ConsumeTraces(lpContext(), traces(2)))
	require.NoError(t, q.ConsumeTraces(lpContext(), traces(3)))
	require.NoError(t, q.ConsumeTraces(context.Background(), traces(4)))
	require.NoError(t, q.ConsumeTraces(context.Background(), traces(5)))
	for i := 0; i < 5; i++ {
		sink.release <- struct{}{}
		if i < 4 {
			waitFor(t, sink.started)
		}
	}
	require.Eventually(t, func() bool { return len(sink.order()) == 5 }, 5*time.Second, time.Millisecond)
	assert.Equal(t, []string{"lp:1", "hp:4", "hp:5", "lp:2", "lp:3"}, sink.order())
	assert.EqualValues(t, 9, q.hpSent.Load())
	assert.EqualValues(t, 6, q.lpSent.Load())
}

// The exporter's headers_setter reads bridges-priority from the context: it must survive the queue.
func TestQueueForwardsPriorityMetadata(t *testing.T) {
	var got []string
	var mu sync.Mutex
	c := createDefaultConfig().(*Config)
	c.Mode = ModeQueue
	q := newPriorityQueue(c, zap.NewNop(), funcConsumer(func(ctx context.Context, _ ptrace.Traces) error {
		mu.Lock()
		got = append(got, client.FromContext(ctx).Metadata.Get(MetadataPriorityKey)...)
		mu.Unlock()
		return nil
	}))
	require.NoError(t, q.Start(context.Background(), componenttest.NewNopHost()))
	require.NoError(t, q.ConsumeTraces(lpContext(), traces(2)))
	require.Eventually(t, func() bool { mu.Lock(); defer mu.Unlock(); return len(got) == 1 }, 5*time.Second, time.Millisecond)
	require.NoError(t, q.Shutdown(context.Background()))
	assert.Equal(t, []string{"lp"}, got)
}

// Eviction frees the oldest LP until the checkpoint's size is covered; surplus is credit;
// with no LP left the checkpoint must be refused.
func TestQueueEvictFor(t *testing.T) {
	sink := newGatedSink()
	q := startQueue(t, 1, sink)
	require.NoError(t, q.ConsumeTraces(context.Background(), traces(1))) // occupies the worker
	waitFor(t, sink.started)
	require.NoError(t, q.ConsumeTraces(lpContext(), traces(6)))
	require.NoError(t, q.ConsumeTraces(lpContext(), traces(6)))
	assert.True(t, q.hasLP())
	assert.True(t, q.evictFor(4, 1.0)) // evicts the first 6-span LP batch, credit 2
	assert.EqualValues(t, 6, q.lpEvicted.Load())
	assert.True(t, q.evictFor(2, 1.0)) // covered by credit, nothing evicted
	assert.EqualValues(t, 6, q.lpEvicted.Load())
	assert.True(t, q.evictFor(3, 2.0)) // needs 6 LP spans: evicts the second batch
	assert.EqualValues(t, 12, q.lpEvicted.Load())
	assert.False(t, q.hasLP())
	assert.False(t, q.evictFor(1, 1.0)) // nothing left: refuse
	assert.EqualValues(t, 9, q.hpAdmittedByEviction.Load())
	assert.EqualValues(t, 0, q.lpQueued.Load())
}

// At soft/hard the admission stage admits a checkpoint by evicting queued LP, and refuses it
// only when no LP is queued. LP is still refused.
func TestAdmissionEvictsQueuedLPInsteadOfRefusingHP(t *testing.T) {
	p := testProcessor(t, MarginModeBacklog, LPRefusalResourceExhausted)
	sink := newGatedSink()
	q := startQueue(t, 1, sink)
	require.NoError(t, q.ConsumeTraces(context.Background(), traces(1))) // occupies the worker
	waitFor(t, sink.started)
	require.NoError(t, q.ConsumeTraces(lpContext(), traces(8)))
	for _, st := range []pressureState{stateSoft, stateHard} {
		p.state.Store(int32(st))
		_, err := p.processTraces(lpContext(), traces(2))
		assert.Equal(t, codes.ResourceExhausted, status.Code(err), "LP refused in %s", stateName(st))
		_, err = p.processTraces(context.Background(), traces(3))
		assert.NoError(t, err, "HP admitted by eviction in %s", stateName(st))
	}
	assert.EqualValues(t, 6, p.hpAdmittedByEviction)
	_, err := p.processTraces(context.Background(), traces(3)) // 2 credit left < 3, no LP queued
	assert.Equal(t, codes.Unavailable, status.Code(err))
	assert.EqualValues(t, 3, p.hpRefused)
}

// The pre-decode gate lets a checkpoint through to decode at soft only while LP is queued.
func TestGateDecodesHPAtSoftWhileLPQueued(t *testing.T) {
	SetGateForTesting("soft", codes.ResourceExhausted)
	defer SetGateForTesting("off", codes.Unavailable)
	sink := newGatedSink()
	q := startQueue(t, 1, sink)
	d, _ := Decide(false)
	assert.Equal(t, GateRefuseAll, d, "nothing queued")
	require.NoError(t, q.ConsumeTraces(context.Background(), traces(1)))
	waitFor(t, sink.started)
	require.NoError(t, q.ConsumeTraces(lpContext(), traces(4)))
	d, _ = Decide(false)
	assert.Equal(t, GateAdmit, d, "LP queued: decode the checkpoint")
	d, code := Decide(true)
	assert.Equal(t, GateRefuseAll, d)
	assert.Equal(t, codes.ResourceExhausted, code)
}

// Shutdown returns even when a send is stuck downstream (e.g. exporter retrying forever).
func TestQueueShutdownAbortsStuckSends(t *testing.T) {
	sink := newGatedSink()
	c := createDefaultConfig().(*Config)
	c.Mode, c.DispatchWorkers = ModeQueue, 2
	q := newPriorityQueue(c, zap.NewNop(), &sinkConsumer{sink})
	require.NoError(t, q.Start(context.Background(), componenttest.NewNopHost()))
	require.NoError(t, q.ConsumeTraces(context.Background(), traces(1)))
	waitFor(t, sink.started)
	done := make(chan error, 1)
	go func() { done <- q.Shutdown(context.Background()) }()
	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown hung")
	}
	assert.Nil(t, activeQueue.Load())
	assert.Error(t, q.ConsumeTraces(context.Background(), traces(1)))
}

// The factory builds the queue stage for mode: queue.
func TestFactoryQueueMode(t *testing.T) {
	f := NewFactory()
	c := f.CreateDefaultConfig().(*Config)
	c.Mode = ModeQueue
	tp, err := f.CreateTraces(context.Background(), processortest.NewNopSettings(metadata.Type), c, funcConsumer(func(context.Context, ptrace.Traces) error { return nil }))
	require.NoError(t, err)
	_, ok := tp.(*priorityQueue)
	assert.True(t, ok)
	c.DispatchWorkers = 0
	assert.ErrorIs(t, c.Validate(), ErrInvalidDispatchWorkers)
	c.Mode = "bogus"
	assert.ErrorIs(t, c.Validate(), ErrInvalidMode)
}

// funcConsumer adapts a function to consumer.Traces.
type funcConsumer func(context.Context, ptrace.Traces) error

func (f funcConsumer) ConsumeTraces(ctx context.Context, td ptrace.Traces) error { return f(ctx, td) }
func (f funcConsumer) Capabilities() consumer.Capabilities                       { return consumer.Capabilities{} }

// With LP already waiting in the queue stage, new LP is refused (pre-decode and in the processor)
// even with no memory pressure; checkpoints are unaffected; LP is admitted again once none waits.
func TestLPRefusedWhileLPWaits(t *testing.T) {
	SetGateForTesting("none", codes.ResourceExhausted)
	defer SetGateForTesting("off", codes.Unavailable)
	p := testProcessor(t, MarginModeBacklog, LPRefusalResourceExhausted)
	p.state.Store(int32(stateNoPressure))
	sink := newGatedSink()
	q := startQueue(t, 1, sink)
	d, _ := Decide(true)
	assert.Equal(t, GateAdmit, d, "empty queue: LP admitted")
	_, err := p.processTraces(lpContext(), traces(2))
	assert.NoError(t, err)
	require.NoError(t, q.ConsumeTraces(lpContext(), traces(1))) // taken by the only worker
	waitFor(t, sink.started)
	require.NoError(t, q.ConsumeTraces(lpContext(), traces(3))) // waits
	d, code := Decide(true)
	assert.Equal(t, GateRefuseLP, d)
	assert.Equal(t, codes.ResourceExhausted, code)
	_, err = p.processTraces(lpContext(), traces(2))
	assert.Equal(t, codes.ResourceExhausted, status.Code(err))
	assert.EqualValues(t, 2, p.lpRefusedBacklog)
	d, _ = Decide(false)
	assert.Equal(t, GateAdmit, d, "checkpoints unaffected")
	_, err = p.processTraces(context.Background(), traces(2))
	assert.NoError(t, err)
	sink.release <- struct{}{} // worker finishes lp:1, takes lp:3: nothing waits any more
	require.Eventually(t, func() bool { return !q.hasLP() }, 5*time.Second, time.Millisecond)
	d, _ = Decide(true)
	assert.Equal(t, GateAdmit, d, "LP admitted again")
}

// At and above the ultrasoft line the admission stage drops the LP waiting in the queue stage;
// below it nothing is evicted; checkpoints and in-flight batches are untouched.
func TestUltrasoftEvictsQueuedLP(t *testing.T) {
	p := testProcessor(t, MarginModeBacklog, LPRefusalResourceExhausted)
	sink := newGatedSink()
	q := startQueue(t, 1, sink)
	require.NoError(t, q.ConsumeTraces(lpContext(), traces(1))) // in flight
	waitFor(t, sink.started)
	require.NoError(t, q.ConsumeTraces(lpContext(), traces(5)))
	require.NoError(t, q.ConsumeTraces(context.Background(), traces(4)))
	p.evictQueuedLP(stateNoPressure)
	assert.True(t, q.hasLP(), "below ultrasoft: nothing evicted")
	p.evictQueuedLP(stateShedLP)
	assert.False(t, q.hasLP())
	assert.EqualValues(t, 5, q.lpEvictedUS.Load())
	assert.EqualValues(t, 4, q.hpQueued.Load(), "checkpoints stay queued")
	for i := 0; i < 2; i++ {
		sink.release <- struct{}{}
	}
	require.Eventually(t, func() bool { return q.hpSent.Load() == 4 && q.lpSent.Load() == 1 }, 5*time.Second, time.Millisecond)
}

// A waiting checkpoint also stops LP intake (any batch waiting = this hop is not keeping up).
func TestLPRefusedWhileCheckpointWaits(t *testing.T) {
	SetGateForTesting("none", codes.ResourceExhausted)
	defer SetGateForTesting("off", codes.Unavailable)
	sink := newGatedSink()
	q := startQueue(t, 1, sink)
	require.NoError(t, q.ConsumeTraces(context.Background(), traces(1))) // in flight
	waitFor(t, sink.started)
	d, _ := Decide(true)
	assert.Equal(t, GateAdmit, d, "nothing waiting")
	require.NoError(t, q.ConsumeTraces(context.Background(), traces(2))) // waits
	d, _ = Decide(true)
	assert.Equal(t, GateRefuseLP, d, "checkpoint waiting: LP refused")
}

// While checkpoints wait, sends carry HPWaitingKey=1 (plus the batch's own metadata); once none
// wait, sends are unmarked.
func TestQueueMarksSendsWhileCheckpointsWait(t *testing.T) {
	var mu sync.Mutex
	var marks, prios []string
	release := make(chan struct{}, 10)
	c := createDefaultConfig().(*Config)
	c.Mode, c.DispatchWorkers = ModeQueue, 1
	q := newPriorityQueue(c, zap.NewNop(), funcConsumer(func(ctx context.Context, _ ptrace.Traces) error {
		md := client.FromContext(ctx).Metadata
		mu.Lock()
		marks = append(marks, strings.Join(md.Get(HPWaitingKey), ","))
		prios = append(prios, strings.Join(md.Get(MetadataPriorityKey), ","))
		mu.Unlock()
		<-release
		return nil
	}))
	require.NoError(t, q.Start(context.Background(), componenttest.NewNopHost()))
	defer func() { _ = q.Shutdown(context.Background()) }()
	require.NoError(t, q.ConsumeTraces(lpContext(), traces(1))) // sent unmarked, blocks
	require.Eventually(t, func() bool { mu.Lock(); defer mu.Unlock(); return len(marks) == 1 }, 5*time.Second, time.Millisecond)
	require.NoError(t, q.ConsumeTraces(context.Background(), traces(2)))
	require.NoError(t, q.ConsumeTraces(context.Background(), traces(3)))
	release <- struct{}{} // hp:2 is sent with hp:3 still waiting -> marked
	require.Eventually(t, func() bool { mu.Lock(); defer mu.Unlock(); return len(marks) == 2 }, 5*time.Second, time.Millisecond)
	release <- struct{}{} // hp:3 is sent with nothing waiting -> unmarked
	require.Eventually(t, func() bool { mu.Lock(); defer mu.Unlock(); return len(marks) == 3 }, 5*time.Second, time.Millisecond)
	release <- struct{}{}
	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []string{"", "1", ""}, marks)
	assert.Equal(t, []string{"lp", "", ""}, prios)
	assert.EqualValues(t, 1, q.hpWaitingMarked.Load())
}

// The gateway refuses LP while an upstream mark was seen in the current or previous tick.
func TestGateRefusesLPOnUpstreamMark(t *testing.T) {
	SetGateForTesting("none", codes.ResourceExhausted)
	defer SetGateForTesting("off", codes.Unavailable)
	gate.upstreamMark.Store(false)
	gate.upstreamActive.Store(false)
	d, _ := Decide(true)
	assert.Equal(t, GateAdmit, d)
	MarkUpstreamHPWaiting([]string{""})
	d, _ = Decide(true)
	assert.Equal(t, GateAdmit, d, "empty mark is no mark")
	MarkUpstreamHPWaiting([]string{"1"})
	d, code := Decide(true)
	assert.Equal(t, GateRefuseLP, d)
	assert.Equal(t, codes.ResourceExhausted, code)
	d, _ = Decide(false)
	assert.Equal(t, GateAdmit, d, "checkpoints unaffected")
	gate.publish(stateNoPressure, codes.ResourceExhausted) // tick 1: mark carried over
	d, _ = Decide(true)
	assert.Equal(t, GateRefuseLP, d)
	gate.publish(stateNoPressure, codes.ResourceExhausted) // tick 2: no new mark -> cleared
	d, _ = Decide(true)
	assert.Equal(t, GateAdmit, d)
}
