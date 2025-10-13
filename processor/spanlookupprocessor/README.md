# Span Lookup Processor

The Span Lookup Processor is a custom OpenTelemetry Collector component that performs on-demand span lookups in the distributed tracing system. It receives trace ID requests from the Agent Contact Receiver and queries the Span Reconstruction Processor to find corresponding upstream server-side spans.

## Overview

This processor is designed for scenarios where you need to:
- Look up specific spans based on trace ID and span ID pairs
- Find upstream server-side spans from client-side span references
- Forward looked-up spans to downstream processors for further analysis
- Maintain state logging for monitoring span reconstruction processor status

## Configuration

```yaml
processors:
  spanlookup:
    max_lookups: 1000              # Maximum span lookups per batch
    enable_metrics: false          # Enable internal metrics
    export_interval_seconds: 5     # Export check interval (disabled)
```

### Configuration Parameters

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `max_lookups` | int | 1000 | Maximum number of span lookups to perform per batch |
| `enable_metrics` | bool | false | Enable metrics collection for the processor |
| `export_interval_seconds` | int | 5 | How often to check for new spans to export (currently disabled) |

## Usage

### Basic Pipeline

```yaml
receivers:
  agentcontact:
    endpoint: "0.0.0.0:55679"

processors:
  spanlookup:
    max_lookups: 1000
    enable_metrics: true

exporters:
  otlp:
    endpoint: "http://localhost:4317"

service:
  pipelines:
    traces/lookup:
      receivers: [agentcontact]
      processors: [spanlookup]
      exporters: [otlp]
```

### With Lookup Routing

```yaml
processors:
  spanlookup:
    max_lookups: 1000
  lookuprouting:
    # routing configuration

service:
  pipelines:
    traces/lookup:
      receivers: [agentcontact]
      processors: [spanlookup, lookuprouting]
      exporters: [otlp]
```

## Behavior

### Lookup Process

1. **Request Reception**: Receives trace ID/span ID pairs from Agent Contact Receiver
2. **Client Span Lookup**: Queries Span Reconstruction Processor for client-side spans
3. **Parent Span Resolution**: Extracts parent span ID from found client spans
4. **Server Span Lookup**: Queries for upstream server-side parent spans
5. **Span Forwarding**: Constructs new traces with found server spans and forwards them

### State Monitoring

- **Periodic Logging**: Logs span reconstruction processor state every second
- **Span Counts**: Reports open and closed span counts
- **Availability Check**: Monitors Span Reconstruction Processor availability

### Export Worker (Disabled)

The automatic span export worker is currently disabled to prevent incorrect statistics:
- **Purpose**: Was designed to continuously export new spans from closed spans
- **Status**: Disabled to ensure only on-demand lookups trigger span forwarding
- **Configuration**: `export_interval_seconds` setting is preserved but unused

## Performance Considerations

- **On-Demand Processing**: Only processes spans when explicitly requested
- **Memory Efficient**: No internal span storage, queries external processor
- **Low Latency**: Direct lookup operations with minimal overhead
- **Thread Safety**: Thread-safe operations with proper mutex protection

## Limitations

- **Dependency**: Requires Span Reconstruction Processor to be available
- **Lookup Scope**: Only looks up spans in closed spans collection
- **Parent-Child Relationships**: Relies on proper parent-child span relationships
- **Single Lookup**: Processes one trace ID/span ID pair at a time

## Troubleshooting

### Common Issues

1. **"Span reconstruction processor not available"**: Ensure Span Reconstruction Processor is running
2. **"Client-side span not found"**: Span may not be in closed spans collection
3. **"Upstream server-side parent span not found"**: Parent-child relationship may be missing

### Debug Logging

Enable debug logging to see detailed lookup operations:

```yaml
service:
  telemetry:
    logs:
      level: debug
```

---

## Technical Documentation

### Component Architecture

The Span Lookup Processor implements a query-based span retrieval system with external dependency management and state monitoring. Key architectural components:

- **Lookup Engine**: Core span lookup logic with parent-child resolution
- **State Monitor**: Periodic monitoring of Span Reconstruction Processor state
- **Export Tracking**: Thread-safe tracking of exported spans (currently disabled)
- **External Integration**: Dependency on global Span Reconstruction Processor instance

### Lookup Algorithm

The processor implements a two-step lookup process:

1. **Client Span Resolution**:
   - Queries `GlobalSpanReconstructionProcessor.GetClosedSpanData(traceID:spanID)`
   - Extracts parent span ID from found client span
   - Validates span existence and parent relationship

2. **Server Span Resolution**:
   - Queries for server-side parent span using `traceID:parentSpanID`
   - Retrieves complete span data with resource and scope context
   - Constructs output traces with found server spans

### State Monitoring System

- **Periodic Monitoring**: 1-second ticker for continuous state reporting
- **Span Counts**: Reports open and closed span counts from reconstruction processor
- **Availability Tracking**: Monitors global processor instance availability
- **Graceful Shutdown**: Proper cleanup of monitoring goroutines

### Thread Safety Implementation

- **Export Tracking**: `sync.RWMutex` protects `ExportedSpanIDs` map
- **Concurrent Access**: Read locks for lookup operations, write locks for export tracking
- **Goroutine Management**: Proper wait group management for worker goroutines
- **Channel Communication**: Structured shutdown using channels

### Integration Points

- **Span Reconstruction Processor**: Primary data source via global singleton
- **Agent Contact Receiver**: Request source for trace ID/span ID pairs
- **Lookup Routing Processor**: Downstream consumer for looked-up spans
- **OpenTelemetry Pipeline**: Standard processor interface with trace consumer

### Performance Characteristics

- **Query-Based Processing**: No internal data storage, external queries only
- **Memory Efficient**: Minimal memory footprint with external data access
- **Low Latency**: Direct lookup operations with O(1) complexity
- **Scalable Design**: Configurable lookup limits and batch processing

### Error Handling & Resilience

- **Missing Dependencies**: Graceful handling when reconstruction processor unavailable
- **Lookup Failures**: Comprehensive logging for failed span lookups
- **Invalid Data**: Robust handling of missing parent-child relationships
- **Resource Management**: Proper cleanup of goroutines and resources

### Configuration Details

- **`max_lookups`**: Batch size limit for lookup operations (default: 1000)
- **`enable_metrics`**: Optional metrics collection (default: false)
- **`export_interval_seconds`**: Export worker interval (currently disabled)

### Monitoring & Observability

- **Comprehensive Logging**: Debug-level logging for lookup operations
- **State Reporting**: Periodic reporting of reconstruction processor state
- **Performance Tracking**: Lookup success/failure monitoring
- **Error Reporting**: Detailed error logging with context
