# Config Discovery Receiver

This receiver serves SDK configuration over HTTP through `/getFullConfig` and
`/getConfig?property=...`. `/getFullConfig` returns `{"config": {...}}`, containing
the configured `config_map`. Register the receiver in a logs pipeline; it does
not ingest application logs.

Tomislav-RetCtx: randomized checkpoint distance in the Blueprint PB, CGPB, and SB SDKs:

```yaml
receivers:
  configdiscovery:
    endpoint: ":8080"
    config_map:
      cpd_min: 2
      cpd_max: 6

exporters:
  debug: {}

service:
  pipelines:
    logs/config:
      receivers: [configdiscovery]
      exporters: [debug]
```

Both bounds are required. Validation requires integer distances satisfying
`1 <= cpd_min <= cpd_max <= 256`. Equal bounds are allowed. These settings are
independent of the trace pipeline's priority/shedding processor.

The SDK fetches the bounds at startup. Each root or scheduled checkpoint draws
a distance uniformly from this inclusive range at span start, then propagates
`distance - 1` as a one-byte prefix on forward bridge baggage. A child receiving
nonzero TTL decrements it; a child receiving zero schedules a checkpoint and
draws its next distance. Checkpoints are exported at span end.

Tomislav-RetCtx: with SDK leaf rejection enabled and the default
`reverse_policy: ttl`, unscheduled leaves draw a
fresh reverse TTL from these same bounds for each rejected truss. Original
checkpoints absorb all returned trusses; other spans emit only expired trusses
and forward the remainder. This decision stays inside the SDK; the collector
needs no additional configuration key for the default TTL mode.

Tomislav-RetCtx: `config_map.reverse_policy` also accepts `probability`,
`inverse_depth`, and `depth_linear`. Fixed probability requires
`reverse_probability` in `[0,1]`; the other modes reject that extra value.
The SDK applies fixed p, `1/n`, or `2(d+1)/(n(n+1))` independently to each
TTL-free returned truss, where n is its leaf's absolute span depth and d is the
receiver's depth. Original checkpoints always absorb returns. These keys do not
enable rejection themselves; application SDKs still need `REVERSE_TRUSS=on`
and a positive `RT_LEAF_REJECT`. The collector validates and serves the policy;
it does not make these per-truss decisions. Restart SDK processes after changing it.

Legacy `config_map: {cpd: 2}` retains fixed-distance behavior. Explicit bounds
take precedence over `cpd` in supporting SDKs. All SDKs along a call path must
be upgraded and configured consistently before using the additional baggage
byte; this receiver does not negotiate protocol versions or push live updates.
