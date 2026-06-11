// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package priorityprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/priorityprocessor"

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor/processorhelper"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// MetadataPriorityKey is the gRPC metadata key the SB SDK stamps onto
// every UploadTraces call: "hp" for checkpoint batches, "lp" for
// non-checkpoint batches. The receiver propagates gRPC metadata into
// ctx when `include_metadata: true` is set; this processor reads it
// via client.FromContext to make an O(1) admission decision per batch.
//
// Untagged batches (no header set) are treated as "hp" — that way the
// priority processor degenerates to memory_limiter-like behavior for
// pipelines that don't have a priority-aware SDK.
const MetadataPriorityKey = "bridges-priority"

// pressureState is the three-zone model used by the priority processor.
// Set atomically by the check goroutine, read by processTraces on every
// incoming batch. See Config docstring for the behavior at each state.
type pressureState int32

const (
	stateNoPressure pressureState = 0
	stateSoft       pressureState = 1
	stateHard       pressureState = 2
)

// MetricsInterval bounds how often we log the counter snapshot.
const MetricsInterval = 1 * time.Second

type priorityProcessor struct {
	config       *Config
	logger       *zap.Logger
	nextConsumer consumer.Traces

	// Thresholds in BYTES against runtime.MemStats.Alloc — the SAME
	// source memory_limiter uses, so anyone reading these numbers can
	// compare apples-to-apples against memory_limiter's instrumentation.
	totalMemory uint64
	softLimit   uint64
	hardLimit   uint64

	// Atomic pressure state; written by checkLoop, read by processTraces.
	state atomic.Int32

	// Priority queues, mutex-protected. Bounded indirectly by the soft/
	// hard heap thresholds — the more spans we hold, the more heap we
	// use, the sooner admission control tightens. `cond` signals workers
	// when new data arrives.
	mu      sync.Mutex
	cond    *sync.Cond
	hpQ     []ptrace.Traces
	lpQ     []ptrace.Traces
	stopped bool

	// Background tick control.
	checkTicker   *time.Ticker
	metricsTicker *time.Ticker
	cancel        context.CancelFunc
	wg            sync.WaitGroup

	gcCount int64

	// Per-priority counters. "Admitted" means enqueued (workers will
	// drain async); "refused" means returned gRPC Unavailable upstream
	// so the SDK records the loss; "evicted" means dropped from lpQ to
	// make room for an HP under soft pressure.
	hpAdmitted int64
	lpAdmitted int64
	hpRefused  int64
	lpRefused  int64
	lpEvicted  int64
}

func newProcessor(config *Config, logger *zap.Logger, nextConsumer consumer.Traces) (*priorityProcessor, error) {
	totalMemory, err := getTotalMemory()
	if err != nil {
		return nil, fmt.Errorf("failed to determine total memory: %w", err)
	}
	softLimit := uint64(config.SoftPercentage) * totalMemory / 100
	hardLimit := uint64(config.HardPercentage) * totalMemory / 100

	logger.Info("Priority processor initialized (buffered+workers mode)",
		zap.Uint64("total_memory_bytes", totalMemory),
		zap.Uint64("soft_limit_bytes", softLimit),
		zap.Uint64("hard_limit_bytes", hardLimit),
		zap.Uint32("soft_percentage", config.SoftPercentage),
		zap.Uint32("hard_percentage", config.HardPercentage),
		zap.Uint32("num_workers", config.NumWorkers),
		zap.Uint32("eviction_ratio", config.EvictionRatio),
		zap.Duration("check_interval", config.CheckInterval))

	p := &priorityProcessor{
		config:       config,
		logger:       logger,
		nextConsumer: nextConsumer,
		totalMemory:  totalMemory,
		softLimit:    softLimit,
		hardLimit:    hardLimit,
	}
	p.cond = sync.NewCond(&p.mu)
	return p, nil
}

func (p *priorityProcessor) start(ctx context.Context, _ component.Host) error {
	ctx, cancel := context.WithCancel(ctx)
	p.cancel = cancel

	p.checkTicker = time.NewTicker(p.config.CheckInterval)
	p.metricsTicker = time.NewTicker(MetricsInterval)

	p.wg.Add(2)
	go p.checkLoop(ctx)
	go p.metricsLoop(ctx)

	// Spawn NumWorkers dequeue-and-forward goroutines.
	for i := uint32(0); i < p.config.NumWorkers; i++ {
		p.wg.Add(1)
		go p.worker(ctx, i)
	}

	return nil
}

func (p *priorityProcessor) shutdown(_ context.Context) error {
	p.mu.Lock()
	p.stopped = true
	p.cond.Broadcast()
	p.mu.Unlock()

	if p.cancel != nil {
		p.cancel()
	}
	p.wg.Wait()
	if p.checkTicker != nil {
		p.checkTicker.Stop()
	}
	if p.metricsTicker != nil {
		p.metricsTicker.Stop()
	}
	p.logMetricsSnapshot("priority_processor_shutdown_metrics")
	return nil
}

// ---------------------------------------------------------------------
// Memory pressure check loop
// ---------------------------------------------------------------------

func (p *priorityProcessor) checkLoop(ctx context.Context) {
	defer p.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case <-p.checkTicker.C:
			p.sampleAndUpdateState()
		}
	}
}

// sampleAndUpdateState reads ms.Alloc, force-GCs if above Soft, then
// re-reads ms.Alloc and uses the POST-GC value to set the pressure
// state. The post-GC re-check matches memory_limiter's semantics: if
// GC was enough to bring alloc back below a threshold, we don't latch
// the higher state.
func (p *priorityProcessor) sampleAndUpdateState() {
	cur := p.readCurrentMemory()
	classify := func(alloc uint64) pressureState {
		switch {
		case alloc >= p.hardLimit:
			return stateHard
		case alloc >= p.softLimit:
			return stateSoft
		}
		return stateNoPressure
	}
	preGCState := classify(cur)

	if preGCState != stateNoPressure {
		preGCAlloc := cur
		runtime.GC()
		atomic.AddInt64(&p.gcCount, 1)
		cur = p.readCurrentMemory()
		p.logger.Debug("Forced GC under pressure",
			zap.String("pre_gc_state", stateName(preGCState)),
			zap.Uint64("pre_gc_alloc_bytes", preGCAlloc),
			zap.Uint64("post_gc_alloc_bytes", cur))
	}
	next := classify(cur)
	prev := pressureState(p.state.Swap(int32(next)))

	if prev != next {
		p.logger.Info("Memory pressure state transition",
			zap.String("from", stateName(prev)),
			zap.String("to", stateName(next)),
			zap.Uint64("alloc_bytes", cur),
			zap.Uint64("soft_limit_bytes", p.softLimit),
			zap.Uint64("hard_limit_bytes", p.hardLimit))
	}
}

// readCurrentMemory returns the live Go heap allocation in bytes.
// Matches memorylimiter@v0.139.0 memorylimiter.go::aboveSoftLimit which
// compares against ms.Alloc, so anyone reading these numbers can
// compare apples-to-apples against memory_limiter's instrumentation.
func (p *priorityProcessor) readCurrentMemory() uint64 {
	ms := &runtime.MemStats{}
	runtime.ReadMemStats(ms)
	return ms.Alloc
}

func stateName(s pressureState) string {
	switch s {
	case stateNoPressure:
		return "none"
	case stateSoft:
		return "soft"
	case stateHard:
		return "hard"
	default:
		return "unknown"
	}
}

// ---------------------------------------------------------------------
// Worker pool — dequeues from hpQ/lpQ and forwards to nextConsumer
// ---------------------------------------------------------------------

// worker dequeues one batch at a time, HP-first, and synchronously
// forwards it via p.nextConsumer.ConsumeTraces. With the downstream
// otlp exporter configured as queue_size=NumWorkers,
// block_on_overflow=true, that call blocks when downstream is at
// capacity — naturally throttling the dequeue rate and propagating
// backpressure back into the priority queues.
func (p *priorityProcessor) worker(ctx context.Context, id uint32) {
	defer p.wg.Done()
	for {
		p.mu.Lock()
		for len(p.hpQ) == 0 && len(p.lpQ) == 0 && !p.stopped {
			p.cond.Wait()
		}
		if p.stopped {
			p.mu.Unlock()
			return
		}
		var batch ptrace.Traces
		if len(p.hpQ) > 0 {
			batch = p.hpQ[0]
			// shift-pop: avoid retaining backing array of evicted slot
			p.hpQ[0] = ptrace.Traces{}
			p.hpQ = p.hpQ[1:]
		} else {
			batch = p.lpQ[0]
			p.lpQ[0] = ptrace.Traces{}
			p.lpQ = p.lpQ[1:]
		}
		p.mu.Unlock()

		// Blocks when downstream sending_queue is full (with
		// block_on_overflow=true). That stall is the WHOLE POINT of
		// this architecture — backpressure has to land somewhere, and
		// here it lands in a place that triggers priority-aware
		// admission control.
		if err := p.nextConsumer.ConsumeTraces(ctx, batch); err != nil {
			p.logger.Debug("downstream consumer error",
				zap.Uint32("worker", id),
				zap.Error(err))
		}
	}
}

// ---------------------------------------------------------------------
// Inbound (processTraces) — priority-aware admission into hpQ/lpQ
// ---------------------------------------------------------------------

// processTraces decides admission based on the per-batch priority
// (extracted from the "bridges-priority" gRPC metadata header) and the
// current pressure state. Admitted batches go into hpQ/lpQ; refused
// batches return gRPC Unavailable. The empty ptrace.Traces returned on
// admission prevents processorhelper from forwarding the same batch
// downstream a second time — our worker pool handles forwarding.
//
// Admission matrix:
//
//	NoPressure → enqueue (HP→hpQ, LP→lpQ)
//	Soft       → LP refused; HP admitted iff lpQ has ≥EvictionRatio
//	             batches to evict (drop oldest EvictionRatio LP, enqueue
//	             HP). Else HP refused.
//	Hard       → refuse all
//
// Untagged batches default to HP — same behavior as memory_limiter for
// pipelines that don't have a priority-aware SDK.
func (p *priorityProcessor) processTraces(ctx context.Context, td ptrace.Traces) (ptrace.Traces, error) {
	isLP := isLPBatch(ctx)
	spanCount := int64(td.SpanCount())
	st := pressureState(p.state.Load())

	p.mu.Lock()
	defer p.mu.Unlock()

	switch st {
	case stateNoPressure:
		if isLP {
			p.lpQ = append(p.lpQ, td)
			atomic.AddInt64(&p.lpAdmitted, spanCount)
		} else {
			p.hpQ = append(p.hpQ, td)
			atomic.AddInt64(&p.hpAdmitted, spanCount)
		}
		p.cond.Signal()
		// ErrSkipProcessingData tells processorhelper NOT to forward the
		// returned (empty) Traces to nextConsumer on the receiver path.
		// The worker pool handles forwarding asynchronously, so the
		// receiver hot path stays decoupled from downstream backpressure.
		return ptrace.NewTraces(), processorhelper.ErrSkipProcessingData

	case stateSoft:
		if isLP {
			atomic.AddInt64(&p.lpRefused, spanCount)
			return ptrace.NewTraces(), status.Error(codes.Unavailable,
				"priority processor: refusing LP under soft pressure")
		}
		// HP: admit iff we can evict EvictionRatio LP batches.
		ratio := int(p.config.EvictionRatio)
		if len(p.lpQ) >= ratio {
			var evictedSpans int64
			for i := 0; i < ratio; i++ {
				evictedSpans += int64(p.lpQ[i].SpanCount())
				p.lpQ[i] = ptrace.Traces{}
			}
			p.lpQ = p.lpQ[ratio:]
			atomic.AddInt64(&p.lpEvicted, evictedSpans)
			p.hpQ = append(p.hpQ, td)
			atomic.AddInt64(&p.hpAdmitted, spanCount)
			p.cond.Signal()
			return ptrace.NewTraces(), processorhelper.ErrSkipProcessingData
		}
		atomic.AddInt64(&p.hpRefused, spanCount)
		return ptrace.NewTraces(), status.Error(codes.Unavailable,
			"priority processor: no LP to evict, HP refused under soft pressure")

	case stateHard:
		if isLP {
			atomic.AddInt64(&p.lpRefused, spanCount)
		} else {
			atomic.AddInt64(&p.hpRefused, spanCount)
		}
		return ptrace.NewTraces(), status.Errorf(codes.Unavailable,
			"priority processor: refusing all under hard pressure")
	}

	return ptrace.NewTraces(), nil
}

// isLPBatch returns true iff the incoming gRPC call carries the
// "bridges-priority: lp" metadata header. Anything else (header
// missing, set to "hp", set to garbage) is treated as HP.
func isLPBatch(ctx context.Context) bool {
	info := client.FromContext(ctx)
	values := info.Metadata.Get(MetadataPriorityKey)
	for _, v := range values {
		if strings.EqualFold(v, "lp") {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------

func (p *priorityProcessor) metricsLoop(ctx context.Context) {
	defer p.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case <-p.metricsTicker.C:
			p.logMetricsSnapshot("priority_processor_metrics")
		}
	}
}

func (p *priorityProcessor) logMetricsSnapshot(name string) {
	cur := p.readCurrentMemory()
	st := pressureState(p.state.Load())
	p.mu.Lock()
	hpDepth := len(p.hpQ)
	lpDepth := len(p.lpQ)
	p.mu.Unlock()
	p.logger.Info(name,
		zap.String("state", stateName(st)),
		zap.Uint64("alloc_bytes", cur),
		zap.Uint64("soft_limit_bytes", p.softLimit),
		zap.Uint64("hard_limit_bytes", p.hardLimit),
		zap.Int("hp_queue_depth", hpDepth),
		zap.Int("lp_queue_depth", lpDepth),
		zap.Int64("hp_admitted", atomic.LoadInt64(&p.hpAdmitted)),
		zap.Int64("lp_admitted", atomic.LoadInt64(&p.lpAdmitted)),
		zap.Int64("hp_refused", atomic.LoadInt64(&p.hpRefused)),
		zap.Int64("lp_refused", atomic.LoadInt64(&p.lpRefused)),
		zap.Int64("lp_evicted", atomic.LoadInt64(&p.lpEvicted)),
		zap.Int64("gc_count", atomic.LoadInt64(&p.gcCount)),
	)
}

// ---------------------------------------------------------------------
// Total memory probe (used at startup to compute hard/soft thresholds)
// ---------------------------------------------------------------------

// getTotalMemory reads the container's memory budget. Prefers the
// cgroup limit (v2: memory.max, v1: memory.limit_in_bytes), falls back
// to /proc/meminfo.
func getTotalMemory() (uint64, error) {
	isV2, err := isCGroupV2()
	if err == nil {
		if isV2 {
			if mem, err := readCGroupV2MemoryLimit(); err == nil && mem > 0 {
				return mem, nil
			}
		} else {
			if mem, err := readCGroupV1MemoryLimit(); err == nil && mem > 0 {
				return mem, nil
			}
		}
	}
	if data, err := os.ReadFile("/proc/meminfo"); err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			if strings.HasPrefix(line, "MemTotal:") {
				fields := strings.Fields(line)
				if len(fields) >= 2 {
					kb, err := strconv.ParseUint(fields[1], 10, 64)
					if err == nil {
						return kb * 1024, nil
					}
				}
			}
		}
	}
	return 0, fmt.Errorf("unable to determine total memory: cgroup limits not found and /proc/meminfo unavailable")
}

func isCGroupV2() (bool, error) {
	if _, err := os.Stat("/sys/fs/cgroup/cgroup.controllers"); err == nil {
		return true, nil
	}
	if _, err := os.Stat("/sys/fs/cgroup/memory"); err == nil {
		return false, nil
	}
	return false, fmt.Errorf("unable to determine cgroup version")
}

func readCGroupV2MemoryLimit() (uint64, error) {
	data, err := os.ReadFile("/sys/fs/cgroup/memory.max")
	if err != nil {
		return 0, err
	}
	s := strings.TrimSpace(string(data))
	if s == "max" || s == "" {
		return 0, fmt.Errorf("no limit set")
	}
	return strconv.ParseUint(s, 10, 64)
}

func readCGroupV1MemoryLimit() (uint64, error) {
	data, err := os.ReadFile("/sys/fs/cgroup/memory/memory.limit_in_bytes")
	if err != nil {
		return 0, err
	}
	s := strings.TrimSpace(string(data))
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, err
	}
	if v < 0 || v == 9223372036854771712 {
		return 0, fmt.Errorf("no limit set")
	}
	return uint64(v), nil
}
