// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package priorityprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/priorityprocessor"

// Tomislav-RetCtx: strict-priority queue stage (Config.Mode = "queue").
//
// Pipeline: priority (admission) -> batch -> priority/queue -> exporter with sending_queue
// disabled. The exporter then holds only the requests in flight, so the backlog of a collector
// whose downstream is slower than its intake builds up HERE instead of in the exporter's FIFO,
// where checkpoints waited behind LP admitted earlier. Two rules, no thresholds of its own:
//
//  1. Dispatch: DispatchWorkers workers send batches downstream; each takes the oldest HP batch
//     if any is queued, else the oldest LP batch. LP is sent only when no checkpoint waits.
//  2. LP intake: while any batch waits here (this hop is not keeping up), the admission stage
//     and the pre-decode gate refuse new LP -- receiving it would only take CPU from checkpoints.
//     While checkpoints wait here, every send carries HPWaitingKey=1, and the next hop refuses
//     all LP while it sees that mark (gate.go), so a backlog upstream also sheds LP downstream.
//  3. Ultrasoft eviction: at and above the ultrasoft line (where new LP is refused anyway) the
//     admission stage drops all LP waiting here each tick, freeing its memory before the
//     refuse-all line instead of when it is reached.
//  4. Eviction for checkpoints: the admission stage (same process, same memory states) calls evictFor when it
//     would refuse a checkpoint at soft/hard. The oldest queued LP batches are dropped until at
//     least the checkpoint's size is freed; the checkpoint is then admitted. It is refused only
//     when no LP is left to evict. So no checkpoint is refused while LP is buffered here.
//
// Memory is bounded by the admission stage's heap states exactly as before (LP refused from
// US, HP admitted at soft/hard only by evicting LP of at least its size).

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

// activeQueue is the running queue stage of this process (nil when none). One traces pipeline
// per collector in our deployments, so one queue stage.
var activeQueue atomic.Pointer[priorityQueue]

type queueItem struct {
	td    ptrace.Traces
	info  client.Info // carries bridges-priority for the exporter's headers_setter
	spans int64
}

// fifo is a slice-backed FIFO; popped slots are cleared so their batches can be collected.
type fifo struct {
	items []queueItem
	head  int
}

func (f *fifo) len() int { return len(f.items) - f.head }

func (f *fifo) push(it queueItem) { f.items = append(f.items, it) }

func (f *fifo) pop() queueItem {
	it := f.items[f.head]
	f.items[f.head] = queueItem{}
	f.head++
	if f.head == len(f.items) {
		f.items, f.head = f.items[:0], 0
	} else if f.head >= 1024 && f.head*2 >= len(f.items) {
		n := copy(f.items, f.items[f.head:])
		f.items, f.head = f.items[:n], 0
	}
	return it
}

type priorityQueue struct {
	logger  *zap.Logger
	next    consumer.Traces
	workers int

	mu      sync.Mutex
	cond    *sync.Cond
	hp, lp  fifo
	hpSpans int64
	lpSpans int64
	credit  float64 // LP spans evicted but not yet charged to an admitted checkpoint
	closed  bool

	ctx    context.Context // cancelled at shutdown: aborts sends (and exporter retries) in flight
	cancel context.CancelFunc
	wg     sync.WaitGroup
	stopM  chan struct{}

	// cumulative counters (spans unless noted) and gauges for the metrics line
	hpEnqueued, lpEnqueued     atomic.Int64
	hpSent, lpSent             atomic.Int64
	hpSendFailed, lpSendFailed atomic.Int64
	lpEvicted, lpEvictedBatch  atomic.Int64
	lpEvictedUS                atomic.Int64 // of lpEvicted: evicted at the ultrasoft line
	hpAdmittedByEviction       atomic.Int64
	hpQueued, lpQueued         atomic.Int64 // current spans queued
	hpQueuedMax, lpQueuedMax   atomic.Int64
	lpBatchesQueued            atomic.Int64
	hpBatchesQueued            atomic.Int64
	hpWaitingMarked            atomic.Int64 // sends that carried the checkpoints-waiting mark
}

func newPriorityQueue(cfg *Config, logger *zap.Logger, next consumer.Traces) *priorityQueue {
	q := &priorityQueue{logger: logger, next: next, workers: cfg.DispatchWorkers}
	if q.workers < 1 {
		q.workers = 1
	}
	q.cond = sync.NewCond(&q.mu)
	return q
}

func (q *priorityQueue) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (q *priorityQueue) Start(_ context.Context, _ component.Host) error {
	q.ctx, q.cancel = context.WithCancel(context.Background())
	q.stopM = make(chan struct{})
	for i := 0; i < q.workers; i++ {
		q.wg.Add(1)
		go q.worker()
	}
	go q.metricsLoop()
	activeQueue.Store(q)
	q.logger.Info("Priority queue stage started (strict HP-first dispatch, LP eviction for checkpoints)",
		zap.Int("dispatch_workers", q.workers))
	return nil
}

func (q *priorityQueue) Shutdown(ctx context.Context) error {
	activeQueue.CompareAndSwap(q, nil)
	q.mu.Lock()
	if q.closed || q.cancel == nil {
		q.mu.Unlock()
		return nil
	}
	q.closed = true
	q.mu.Unlock()
	q.cond.Broadcast()
	q.cancel()
	close(q.stopM)
	done := make(chan struct{})
	go func() { q.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
	}
	q.logMetrics("priority_queue_shutdown_metrics")
	return nil
}

// ConsumeTraces queues a batch (never refuses: admission already decided) and returns.
func (q *priorityQueue) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	n := int64(td.SpanCount())
	if n == 0 {
		return nil
	}
	isLP := isLPBatch(ctx)
	it := queueItem{td: td, info: client.FromContext(ctx), spans: n}
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return errors.New("priority queue: shutting down")
	}
	if isLP {
		q.lp.push(it)
		q.lpSpans += n
		q.lpEnqueued.Add(n)
		q.lpQueued.Store(q.lpSpans)
		q.lpBatchesQueued.Store(int64(q.lp.len()))
		bumpMax(&q.lpQueuedMax, q.lpSpans)
	} else {
		q.hp.push(it)
		q.hpSpans += n
		q.hpEnqueued.Add(n)
		q.hpQueued.Store(q.hpSpans)
		q.hpBatchesQueued.Store(int64(q.hp.len()))
		bumpMax(&q.hpQueuedMax, q.hpSpans)
	}
	q.mu.Unlock()
	q.cond.Signal()
	return nil
}

// take pops the oldest HP batch, else the oldest LP batch; blocks while both are empty.
// ok is false once the stage is closed.
func (q *priorityQueue) take() (it queueItem, isLP bool, ok bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for q.hp.len() == 0 && q.lp.len() == 0 && !q.closed {
		q.cond.Wait()
	}
	if q.closed {
		return queueItem{}, false, false
	}
	if q.hp.len() > 0 {
		it = q.hp.pop()
		q.hpSpans -= it.spans
		q.hpQueued.Store(q.hpSpans)
		q.hpBatchesQueued.Store(int64(q.hp.len()))
		return it, false, true
	}
	it = q.lp.pop()
	q.lpSpans -= it.spans
	q.lpQueued.Store(q.lpSpans)
	q.lpBatchesQueued.Store(int64(q.lp.len()))
	return it, true, true
}

func (q *priorityQueue) worker() {
	defer q.wg.Done()
	for {
		it, isLP, ok := q.take()
		if !ok {
			return
		}
		err := q.next.ConsumeTraces(client.NewContext(q.ctx, q.dispatchInfo(it.info)), it.td)
		switch {
		case err == nil && isLP:
			q.lpSent.Add(it.spans)
		case err == nil:
			q.hpSent.Add(it.spans)
		case isLP:
			q.lpSendFailed.Add(it.spans)
		default:
			q.hpSendFailed.Add(it.spans)
			q.logger.Warn("priority queue: checkpoint batch failed downstream", zap.Int64("spans", it.spans), zap.Error(err))
		}
	}
}

// hasLP reports whether any LP batch is waiting (at soft/hard a checkpoint is then decoded and
// admitted by eviction).
func (q *priorityQueue) hasLP() bool { return q.lpBatchesQueued.Load() > 0 }

// hasWaiting reports whether any batch (checkpoint or LP) waits for a dispatch worker, i.e. this
// hop is not keeping up: new LP is then refused before decode.
func (q *priorityQueue) hasWaiting() bool {
	return q.hpBatchesQueued.Load() > 0 || q.lpBatchesQueued.Load() > 0
}

// dispatchInfo returns the client info for a send: the batch's own metadata (bridges-priority,
// re-emitted by the exporter's headers_setter) plus HPWaitingKey=1 while checkpoints wait behind
// this send, so the next hop refuses LP until this hop's checkpoints get through.
func (q *priorityQueue) dispatchInfo(info client.Info) client.Info {
	if q.hpBatchesQueued.Load() == 0 {
		return info
	}
	md := map[string][]string{HPWaitingKey: {"1"}}
	for k := range info.Metadata.Keys() {
		if k != HPWaitingKey {
			md[k] = info.Metadata.Get(k)
		}
	}
	info.Metadata = client.NewMetadata(md)
	q.hpWaitingMarked.Add(1)
	return info
}

// evictFor makes room for an incoming checkpoint of hpSpans spans, whose bytes equal
// hpSpans*sizeRatio LP spans (sizeRatio = bytes per HP span / bytes per LP span). It evicts
// the oldest queued LP batches until at least that much LP has been freed and reports true;
// false (nothing more to evict) means the checkpoint must be refused. LP freed beyond one
// checkpoint's need is kept as credit for the next one.
func (q *priorityQueue) evictFor(hpSpans int64, sizeRatio float64) bool {
	if sizeRatio <= 0 {
		sizeRatio = 1
	}
	need := float64(hpSpans) * sizeRatio
	q.mu.Lock()
	defer q.mu.Unlock()
	defer func() {
		q.lpQueued.Store(q.lpSpans)
		q.lpBatchesQueued.Store(int64(q.lp.len()))
	}()
	for q.credit < need {
		if q.lp.len() == 0 {
			return false
		}
		it := q.lp.pop()
		q.lpSpans -= it.spans
		q.credit += float64(it.spans)
		q.lpEvicted.Add(it.spans)
		q.lpEvictedBatch.Add(1)
	}
	q.credit -= need
	q.hpAdmittedByEviction.Add(hpSpans)
	return true
}

// evictAllLP drops every LP batch waiting in the queue (not those in flight) and returns the
// spans dropped. Called by the admission stage at and above the ultrasoft line.
func (q *priorityQueue) evictAllLP() int64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	var n int64
	for q.lp.len() > 0 {
		it := q.lp.pop()
		n += it.spans
		q.lpEvictedBatch.Add(1)
	}
	if n > 0 {
		q.lpSpans -= n
		q.lpEvicted.Add(n)
		q.lpEvictedUS.Add(n)
		q.lpQueued.Store(q.lpSpans)
		q.lpBatchesQueued.Store(0)
	}
	return n
}

func (q *priorityQueue) metricsLoop() {
	t := time.NewTicker(MetricsInterval)
	defer t.Stop()
	for {
		select {
		case <-q.stopM:
			return
		case <-t.C:
			q.logMetrics("priority_queue_metrics")
		}
	}
}

func (q *priorityQueue) logMetrics(name string) {
	q.logger.Info(name,
		zap.Int64("hp_queued_spans", q.hpQueued.Load()),
		zap.Int64("lp_queued_spans", q.lpQueued.Load()),
		zap.Int64("hp_queued_spans_max", q.hpQueuedMax.Load()),
		zap.Int64("lp_queued_spans_max", q.lpQueuedMax.Load()),
		zap.Int64("hp_enqueued", q.hpEnqueued.Load()),
		zap.Int64("lp_enqueued", q.lpEnqueued.Load()),
		zap.Int64("hp_sent", q.hpSent.Load()),
		zap.Int64("lp_sent", q.lpSent.Load()),
		zap.Int64("hp_send_failed", q.hpSendFailed.Load()),
		zap.Int64("lp_send_failed", q.lpSendFailed.Load()),
		zap.Int64("lp_evicted_spans", q.lpEvicted.Load()),
		zap.Int64("lp_evicted_batches", q.lpEvictedBatch.Load()),
		zap.Int64("lp_evicted_ultrasoft_spans", q.lpEvictedUS.Load()),
		zap.Int64("hp_admitted_by_eviction", q.hpAdmittedByEviction.Load()),
		zap.Int64("hp_waiting_marked_sends", q.hpWaitingMarked.Load()))
}

func bumpMax(v *atomic.Int64, x int64) {
	for {
		cur := v.Load()
		if x <= cur || v.CompareAndSwap(cur, x) {
			return
		}
	}
}
