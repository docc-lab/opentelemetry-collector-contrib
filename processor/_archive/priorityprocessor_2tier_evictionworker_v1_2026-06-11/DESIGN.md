# Priority Processor — Design Notes

This document describes the v2 architecture of the priority processor.
It supersedes the original "filter + 100ms drain" shape, which produced
proportional drops indistinguishable from `memory_limiter` because the
processor's internal buffers never held any in-flight load.

## Motivation: why v1 didn't work

The original shape was:

```
otlp recv → priority { filter, 100ms drain }
         → batch { 8192-span accumulator }
         → otlp { sending_queue (1000 batches ≈ 8M spans) + retry_on_failure }
         → wire
```

The priority processor classified each span as CP (`_br` breadcrumb
present) or LP (absent) and routed into `hpBuf` / `lpBuf`, then a 100ms
ticker drained both buffers wholesale into the next consumer.

Empirically observed under a 1500→3500 rps ramp on DSB-SN:

- `hp_buf_depth = 0`, `lp_buf_depth = 0` at every sample tick — buffers
  drained faster than they filled, so there was no LP backlog ever.
- Memory pressure entered the soft zone for one or two ticks then
  jumped to hard (allocation grew faster than `check_interval=1s` could
  observe).
- Under hard pressure the policy was symmetric (drop both CP and LP),
  so cp_drop% and lp_drop% came out **exactly equal** (e.g. 75.2% /
  75.2%), and `lp_evicted = 0` everywhere. No priority distinction.
- 8 of 9 collector pods OOM-killed despite the hard threshold, because
  the bytes triggering pressure were downstream (batch + sending_queue),
  not in our own buffers — shedding incoming load did not reduce them.

Conclusion: v1 was a label, not a mechanism. The actual queue lived in
the otlp exporter's `sending_queue`, which had no notion of priority,
and our local buffers had neither the capacity nor the policy to do
priority-aware shedding.

## v2 architecture

```
otlp recv ──fast non-blocking──> priority { hp/lp queues,
                                            send workers (×N),
                                            admission policy,
                                            eviction policy }
                                ──worker N blocks on────> otlp { retry_on_failure
                                                                  → gRPC }
                                                       ──> wire
```

The priority processor **absorbs the role of the export queue**. There
is no `batch` processor between it and the exporter, and the otlp
exporter's `sending_queue` is disabled (`enabled: false`). The otlp
exporter keeps `retry_on_failure` because that is wire-level resilience,
not buffering. All in-flight bytes between accept and send live in
priority's hp/lp queues — which means (a) `runtime.MemStats.Alloc`
reflects what we can actually shed, and (b) eviction policy operates on
the bytes that are actually causing pressure.

### Components

**Inbound (`processTraces`)**
- Runs on the receiver's goroutine.
- NEVER blocks. Classifies each span by `_br` presence, then applies
  the admission policy under `bufLock`, then signals workers and
  returns an empty Traces.
- Backpressure to the receiver is implicit: under sustained overload
  the admission policy drops incoming spans (drop LP first, then CP if
  no LP available to evict). The receiver itself is never made to wait.

**Send worker pool (×N, configurable; default 10)**
- N goroutines. Each waits on `sync.Cond` for `len(hpBuf)+len(lpBuf) >
  0`, then pulls up to `BatchSize` spans (HP first, then LP, FIFO
  within each lane), releases the lock, and calls `nextConsumer.
  ConsumeTraces` — which blocks through `retry_on_failure` until the
  wire accepts the batch.
- Worker blocking is intentional. When the wire slows, workers stall in
  retry, hp/lp accumulate, `ms.Alloc` rises, and the memory state
  machine engages eviction. **The backpressure mechanism IS the
  priority mechanism.**
- `BatchSize` is intentionally small (default 256) so the in-flight
  bytes held *outside* hp/lp by stalled workers are bounded:
  `inflight = BatchSize × N`. This stays small relative to the queue,
  so eviction policy on hp/lp remains the dominant pressure lever.

**Memory check (`checkLoop`)**
- Reads `runtime.MemStats.Alloc` every `check_interval` (default 100ms
  in v2, was 1s in v1). MemStats is **the same source `memory_limiter`
  uses**, so this processor and memory_limiter compute pressure against
  the same number.
- We deliberately do NOT read the cgroup `memory.current` reading any
  more: that number includes pages Go has freed but not yet returned to
  the OS, so it lags shedding and is not responsive to our actions.
- Sets atomic `state ∈ {NoPressure, SoftPressure, HardPressure}` for
  inbound to read on every batch.
- `softLimit = limit - spike` and `hardLimit = limit`, computed from
  the configured percentages against the cgroup memory limit (same
  total-memory source as `memory_limiter`).

### Admission and eviction policy

For each incoming batch (one call to `processTraces`):

| state          | incoming CP                              | incoming LP            |
|----------------|------------------------------------------|------------------------|
| **NoPressure** | append to hpBuf                          | append to lpBuf        |
| **SoftPressure**, lpBuf non-empty | LIFO-evict 1 LP, append CP to hpBuf | drop |
| **SoftPressure**, lpBuf empty     | drop                            | drop |
| **HardPressure** | drop                                   | drop                   |

LIFO eviction: the **newest** LP entry is sacrificed first. Rationale —
under a burst, the newest LP is part of the spike that's causing
pressure; the older LP may be part of in-progress traces whose
checkpoint cousins are already further along in the pipeline.

There is no separate `lpBuf` size cap. The cap is implicit via memory
pressure: when buffers grow large enough to push `ms.Alloc` above hard,
admission stops, workers continue to drain.

### Buffer drain ordering

Workers always prefer HP. Within HP, FIFO. Once HP is empty, they pull
LP, also FIFO.

This means under no-pressure with healthy wire, HP and LP both drain
roughly in arrival order. Under stress where wire stalls and we
accumulate, HP gets sent first when the wire recovers — checkpoints
catch up before non-checkpoints.

### Worker signaling

Inbound after a successful admission calls `bufCond.Signal()` (or
`Broadcast()` if it admitted enough to feed multiple workers).
Workers `Wait()` on `bufCond` whenever they find both buffers empty.
This avoids a polling-style flush loop entirely — workers are woken
only when there's actual work to do.

Shutdown sets a `done` flag and `Broadcast()`s the cond; workers
observe `done` after wake and exit.

### Counters / metrics

Same set as v1 (cp_admitted, lp_admitted, cp_dropped, lp_dropped,
lp_evicted, hp_buf_depth, lp_buf_depth, plus per-flush spans/batches
sent/failed), logged once per second.

## Configuration

```yaml
processors:
  priority:
    limit_percentage:       80     # hard limit, % of container memory (same as memory_limiter)
    spike_limit_percentage: 20     # soft = limit - spike (same as memory_limiter)
    check_interval:         100ms  # memory poll cadence (faster than v1's 1s)
    num_consumers:          10     # send worker goroutines (replaces sending_queue.num_consumers)
    batch_size:             256    # spans per worker pull (replaces batch processor)
```

The `limit_percentage` / `spike_limit_percentage` / `check_interval`
trio is **identical** to `memory_limiter`'s config interface. The two
new fields (`num_consumers`, `batch_size`) correspond to roles this
processor has absorbed from the batch processor and the otlp exporter's
`sending_queue`.

## Pipeline configuration

The v2 priority processor REPLACES both `batch` and the otlp exporter's
`sending_queue`. A valid pipeline:

```yaml
processors:
  priority:
    limit_percentage: 80
    spike_limit_percentage: 20
    check_interval: 100ms
    num_consumers: 10
    batch_size: 256

exporters:
  otlp:
    endpoint: ...
    sending_queue:
      enabled: false        # disabled — priority IS the queue
    retry_on_failure:
      enabled: true         # KEEP — wire-level resilience, not buffering
      initial_interval: 5s
      max_interval: 30s
      max_elapsed_time: 0

service:
  pipelines:
    traces:
      receivers:  [otlp]
      processors: [priority]   # NO batch — priority does its own batching
      exporters:  [otlp]
```

DO NOT run `priority` together with `batch` or `memory_limiter`. They
overlap on the queueing/admission role and will compete.

## Failure modes and open questions

- **All workers stalled in retry simultaneously.** Possible during a
  real wire outage. hp keeps growing as well as lp. Once ms.Alloc
  crosses hard, all incoming drops. Once a worker frees, it drains hp
  fastest. Open question: should we LIFO-evict from hp under hard too,
  to free memory faster? For now: no. Hard just stops admissions and
  lets workers catch up. Revisit if we observe outage-driven OOMs.

- **Sustained overload at multi-GB/s, beyond worker drain.** Steady-
  state: all original LP evicted, lpBuf at zero, incoming LP dropped,
  incoming CP dropped (no LP to evict). Drop rate = (intake - drain) /
  intake. CP and LP drop at *different* rates because CP gets admitted
  whenever drain frees buffer headroom (and we evict a fresh LP that
  arrived in the gap), while LP drops on arrival. This is the expected
  steady-state benefit over `memory_limiter`.

- **MemStats.Alloc vs container OOM.** ms.Alloc only counts live Go
  heap. The full process RSS (cgroup view) can sit much higher because
  Go holds onto pages. We deliberately accept this: ms.Alloc is what's
  responsive to shedding, and the cgroup limit should be set with
  enough headroom (say 1.5× the configured hard limit) that the kernel
  doesn't kill us during normal post-shedding heap-retention windows.
  If we observe OOMs at the new design, we may need to add an explicit
  scavenger nudge (`debug.FreeOSMemory()`) on hard-pressure entry.

- **Retry behavior during shutdown.** If workers are mid-retry on
  shutdown, we either wait them out (graceful) or cancel context
  (drops the in-flight batch). For now: cancel + log dropped count.
