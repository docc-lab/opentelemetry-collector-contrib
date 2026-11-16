# Priority arena receiver

Tomislav-RetCtx: an OTLP/gRPC trace receiver that buffers every request in a fixed memory
arena outside the Go heap, decides admission by exact byte counts, and sends checkpoints
first. It replaces the `priorityotlp` → `priority` → `batch` → `priority/queue` pipeline.

## Why

The `priority` processor decides from `runtime.MemStats.Alloc`, which includes garbage the GC
has not collected yet. With `GOGC=100` the heap climbs to about twice the live heap before every
collection, so the processor sheds LP, and at the soft line refuses checkpoints, on garbage
rather than on data it holds. It then forces GCs to bring the reading down. Every admitted
request is also decoded into pdata, a graph of small objects per span that becomes garbage once
the request is sent.

This receiver never decodes requests on the forwarding path and holds them as bytes in memory
it manages itself. The Go heap then holds little more than gRPC's per-request state.

## How it works

* **Arena.** One region of `memory_mib`, mapped with `mmap` (`MAP_POPULATE`, so it is charged to
  the container once, at start) and split into `block_kib` blocks. A request occupies a chain of
  whole blocks. Bookkeeping is int32 arrays, so the GC neither scans nor counts any of it.
* **Admission**, per request, from the `bridges-priority: hp|lp` header the bridges SDK sets:
  * LP is admitted while the blocks in use stay within `lp_limit_percentage` of the arena.
  * A checkpoint (HP) is admitted if there are free blocks. Otherwise the **newest** queued LP
    requests are evicted until there are. If queued LP is not enough, LP being sent is evicted
    too, newest send first: the send is cancelled, its requests are dropped instead of
    requeued, and the checkpoint waits up to 500 ms for gRPC to hand back the blocks. A
    checkpoint is refused only when evicting all LP, queued and in flight, would still not
    make room.

  Evicted blocks are free at once, so nothing is evicted beyond what a checkpoint needs. Newest
  first keeps the oldest LP, the part of traces furthest along, intact.
* **Before reading.** A gRPC tap refuses a request from its headers alone when its class is
  already over its limit, so a refused request is never read or copied.
* **Counting.** A wire scan counts the request's spans and checks its structure (lengths, wire
  types, nesting down to the spans) without allocating. Malformed requests are refused with
  `InvalidArgument`.
* **Dispatch.** `workers` goroutines take queued requests, checkpoints first, concatenating
  them up to `max_batch_spans` / `max_batch_kib` (concatenated OTLP requests are one valid
  request). LP is not sent while a checkpoint waits.
  * **Forward mode** (`forward.endpoint` set) sends the bytes to the next OTLP/gRPC server
    as they are, one class per send, with the `bridges-priority` header. gRPC gets the arena
    blocks themselves; they are released when gRPC hands them back. The pipeline's consumer
    is not used.
  * **Pipeline mode** decodes each batch (checkpoints, then LP) and calls the pipeline. Nothing
    after the receiver should queue: no `batch` processor, exporter `sending_queue` disabled.
    A batch that does not decode is retried request by request, and only the bad ones are dropped.
* **Failures.** A batch stays in the arena until it is delivered. On a retryable error it goes
  back to the front of its queue, and sends pause for a backoff that doubles from
  `forward.backoff_initial` to `forward.backoff_max`. A refused checkpoint pauses every send,
  a refused LP request only LP. While back in the queue it can be evicted like any other LP.

Refusal codes: LP gets `lp_refusal_code` (default `ResourceExhausted`, which the OTLP exporter
does not retry), checkpoints `Unavailable` (retried).

## Configuration

| Field | Default | |
|---|---|---|
| `grpc` | `0.0.0.0:4317` | gRPC server settings, as for the OTLP receiver |
| `memory_mib` | 128 | Arena size. It is not Go heap: leave it out of `GOMEMLIMIT` |
| `memory_percentage` | 0 | Size the arena as this share of the container's memory limit (cgroup) instead. Checkpoints are refused above it, once no LP is left |
| `lp_percentage` | 0 | With `memory_percentage`: the share of the container's limit the arena may fill before LP is refused. Replaces `lp_limit_percentage` |
| `block_kib` | 4 | Allocation unit, at least 4. A request wastes half a block on average |
| `lp_limit_percentage` | 90 | Share of the arena LP may fill; the rest is kept for checkpoints |
| `lp_refusal_code` | `resource_exhausted` | Or `unavailable` |
| `mmap` | true | false allocates the arena as a Go slice (tests) |
| `workers` | 10 | Concurrent sends or pipeline calls |
| `max_batch_spans`, `max_batch_kib` | 8192, 4096 | Bounds of one send |
| `linger` | 0 | Wait up to this long for a batch to reach `max_batch_spans`; 0 sends at once |
| `forward.endpoint` | none | Forward mode when set |
| `forward.timeout` | 5s | Per send |
| `forward.backoff_initial`, `forward.backoff_max` | 50ms, 1s | Pause after a failed send |
| `metrics_interval` | 1s | Period of the metrics log line |

Agent (forward mode), with the thresholds of a `memory_limiter` at 50 % soft and 70 % hard:
LP is refused once the arena holds 50 % of the container's limit, and checkpoints once it
holds 70 % and no LP is left to evict:

```yaml
receivers:
  priorityarena:
    memory_percentage: 70
    lp_percentage: 50
    forward:
      endpoint: otelgw:4317
exporters:
  nop:
service:
  pipelines:
    traces:
      receivers: [priorityarena]
      exporters: [nop]
```

In a 256 MiB container that is a 179 MiB arena; set `GOMEMLIMIT` to what is left after it
and room for the binary and stacks (about 64MiB). The receiver warns at start when
`GOMEMLIMIT` is larger. Nothing else needs a threshold: admission is checked against exact
free space before a request is copied, so the arena cannot overshoot, and evicting frees memory
at once, with no GC involved. The memory outside the arena is mostly requests gRPC is reading;
`grpc.max_concurrent_streams` and `grpc.max_recv_msg_size_mib` bound it.

Gateway (pipeline mode), with the exporter's queue off:

```yaml
receivers:
  priorityarena:
    memory_mib: 1024
    workers: 4
    max_batch_spans: 20000
    linger: 1s
exporters:
  clickhouse:
    sending_queue:
      enabled: false
service:
  pipelines:
    traces:
      receivers: [priorityarena]
      exporters: [clickhouse]
```

## Metrics

One log line, `priority_arena_metrics`, every `metrics_interval`. All counts are spans unless
named requests.

| Field | |
|---|---|
| `hp_admitted`, `lp_admitted` | Admitted |
| `hp_refused`, `lp_refused` | Refused after reading |
| `hp_refused_requests_predecode`, `lp_refused_requests_predecode` | Requests refused from their headers |
| `hp_admitted_by_eviction`, `lp_evicted`, `lp_evicted_requests` | Checkpoints admitted by evicting LP, and the LP evicted |
| `hp_sent`, `lp_sent`, `hp_failed`, `lp_failed` | Delivered, and dropped after a permanent error |
| `hp_requeued`, `lp_requeued`, `sends`, `send_errors` | Retries |
| `hp_queued_spans`, `lp_queued_spans`, `hp_queued_blocks`, `lp_queued_blocks` | Queued now |
| `blocks_used`, `blocks_used_max`, `blocks_total`, `block_bytes` | Arena use |
| `gc_cycles`, `gc_cpu_seconds`, `heap_live_bytes`, `heap_objects_bytes`, `heap_allocs_bytes_total` | Go runtime, from `runtime/metrics` |

## Limitations

* Requests must be priority-homogeneous, as the bridges SDK sends them. Without the header a
  request counts as a checkpoint.
* Forward mode has no TLS, compression or authentication on the outgoing connection.
* Spans refused before reading are counted as requests; their spans are unknown.
* The wire scan checks structure, not span contents; pipeline mode drops requests pdata cannot
  decode, one by one.

## Tests and tools

`go test ./priorityarenareceiver/` covers the arena accounting (including a randomized test
that checks every block after each step), admission and eviction, the span scan against pdata
(with a fuzz target, `FuzzCountSpans`), and forward-to-pipeline delivery and overload over real
gRPC.

`testbed/load` offers SDK-shaped load (requests of at most 512 spans every 100 ms per client,
HP and LP separate and tagged) and reports acked, refused and dropped spans and latency per
class. `testbed/sink` stands in for the gateway: it counts spans per class at a capacity that
follows a schedule, pushing back by delaying responses, and refusing once its queue is full.
`cmd/prioritycol` is a small collector with the components of both pipelines, for comparing
them on identical binaries.
