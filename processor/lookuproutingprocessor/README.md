# Lookup Routing Processor

The Lookup Routing Processor is a custom OpenTelemetry Collector component that routes looked-up spans to downstream systems and extracts metadata for the Agent Contact Exporter. It serves as a bridge between the Span Lookup Processor and downstream exporters, while also facilitating inter-agent communication.

## Overview

This processor is designed for scenarios where you need to:
- Route looked-up spans to downstream systems (e.g., Jaeger)
- Extract metadata from looked-up spans for inter-agent communication
- Send trace ID and upstream IP information to the Agent Contact Exporter
- Maintain dual pipeline flow (normal traces + metadata extraction)

## Configuration

```yaml
processors:
  lookuprouting:
    routing_rules: []           # Routing rules (currently unused)
    default_route: "default"    # Default route (currently unused)
```

### Configuration Parameters

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `routing_rules` | array | `[]` | Routing rules for span routing (currently unused) |
| `default_route` | string | `"default"` | Default route when no rules match (currently unused) |

## Usage

### Basic Pipeline

```yaml
receivers:
  agentcontact:
    endpoint: "0.0.0.0:55679"

processors:
  spanlookup:
    max_lookups: 1000
  lookuprouting:
    # Configuration options
  statisticsaggregation:
    export_window: 60s

exporters:
  otlp:
    endpoint: "http://localhost:4317"

service:
  pipelines:
    traces/lookup:
      receivers: [agentcontact]
      processors: [spanlookup, lookuprouting, statisticsaggregation]
      exporters: [otlp]
```

### With Agent Contact Exporter

```yaml
exporters:
  agentcontact:
    default_port: 55679
    timeout: 30s

service:
  pipelines:
    traces/lookup:
      receivers: [agentcontact]
      processors: [spanlookup, lookuprouting, statisticsaggregation]
      exporters: [otlp]
```

## Behavior

### Dual Pipeline Processing

1. **Normal Flow**: Passes looked-up spans to the next consumer (Statistics Aggregation Processor)
2. **Metadata Extraction**: Extracts trace ID, parent span ID, and upstream IP from spans
3. **Agent Contact**: Sends extracted metadata to the Agent Contact Exporter
4. **Return**: Returns original traces unchanged

### Metadata Extraction

For each looked-up span, the processor extracts:
- **Trace ID**: Original trace identifier
- **Parent Span ID**: Parent span identifier (used as span ID in metadata)
- **Upstream IP**: IP address of the upstream service

### Agent Contact Integration

- **Global Access**: Uses `agentcontactexporter.GlobalAgentContactExporter`
- **Metadata Spans**: Creates synthetic spans with extracted metadata
- **Service Attribution**: Marks metadata spans with "lookup-routing-processor" service
- **Error Handling**: Graceful handling when Agent Contact Exporter unavailable

## Performance Considerations

- **Synchronous Processing**: Processes spans synchronously without queuing
- **Minimal Overhead**: Simple pass-through with metadata extraction
- **Memory Efficient**: No internal state or buffering
- **Low Latency**: Direct processing with minimal processing time

## Limitations

- **Routing Rules**: Configuration exists but routing rules are not implemented
- **Single Consumer**: Only supports single downstream consumer
- **Metadata Only**: Only extracts basic metadata (trace ID, parent span ID, upstream IP)
- **No Filtering**: Processes all incoming spans without filtering

## Troubleshooting

### Common Issues

1. **"Agent contact exporter not available"**: Ensure Agent Contact Exporter is running
2. **"Failed to send metadata to agent contact exporter"**: Check exporter configuration
3. **"Failed to pass traces to next consumer"**: Check downstream processor configuration

### Debug Logging

Enable debug logging to see detailed processing:

```yaml
service:
  telemetry:
    logs:
      level: debug
```

---

## Technical Documentation

### Component Architecture

The Lookup Routing Processor implements a simple pass-through processor with metadata extraction capabilities. Key architectural components:

- **Pass-Through Processing**: Direct forwarding of traces to downstream consumer
- **Metadata Extraction**: Extraction of trace ID, parent span ID, and upstream IP
- **Agent Contact Integration**: Integration with global Agent Contact Exporter instance
- **Synthetic Span Creation**: Creation of metadata spans for inter-agent communication

### Processing Flow

The processor implements a dual-path processing approach:

1. **Primary Path**: Direct pass-through of incoming traces to next consumer
2. **Metadata Path**: Extraction and forwarding of metadata to Agent Contact Exporter
3. **Return Path**: Returns original traces unchanged to maintain pipeline integrity

### Metadata Extraction Process

**Span Processing**:
- Iterates through all spans in incoming traces
- Extracts trace ID, parent span ID, and upstream IP from each span
- Creates synthetic spans with extracted metadata
- Sets service attribution to "lookup-routing-processor"

**Upstream IP Extraction**:
- Looks for `upstream.ip` attribute in span attributes
- Falls back to "unknown-upstream" if attribute not found
- Handles type conversion from interface{} to string

### Agent Contact Integration

**Global Exporter Access**:
- Uses `agentcontactexporter.GlobalAgentContactExporter` singleton
- Graceful handling when exporter not available
- Direct trace consumption for metadata forwarding

**Synthetic Span Creation**:
- Creates new traces with extracted metadata
- Sets resource attributes for service identification
- Sets scope information for component identification
- Copies original span data with modified span ID

### Thread Safety Implementation

- **Stateless Design**: No shared state requiring synchronization
- **Synchronous Processing**: No concurrent access to shared resources
- **Immutable Operations**: Read-only operations on incoming traces
- **Error Isolation**: Errors in metadata path don't affect primary path

### Performance Characteristics

- **Minimal Overhead**: Simple pass-through with basic metadata extraction
- **Memory Efficient**: No internal buffering or state management
- **Low Latency**: Direct processing without queuing or batching
- **Scalable Design**: Stateless design supports high throughput

### Integration Points

- **Span Lookup Processor**: Receives looked-up spans for routing
- **Statistics Aggregation Processor**: Primary downstream consumer
- **Agent Contact Exporter**: Metadata destination for inter-agent communication
- **OpenTelemetry Pipeline**: Standard processor interface with trace consumer

### Configuration Details

- **`routing_rules`**: Array of routing rules (currently unused)
- **`default_route`**: Default route for unmatched spans (currently unused)
- **Future Extensibility**: Configuration structure prepared for future routing features

### Error Handling & Resilience

- **Graceful Degradation**: Continues processing when Agent Contact Exporter unavailable
- **Error Isolation**: Metadata extraction errors don't affect primary trace flow
- **Consumer Errors**: Proper error handling for downstream consumer failures
- **Attribute Handling**: Robust handling of missing or invalid attributes

### Monitoring & Observability

- **Comprehensive Logging**: Debug-level logging for processing operations
- **Metadata Tracking**: Detailed logging of extracted metadata
- **Performance Monitoring**: Processing time and span count tracking
- **Error Reporting**: Detailed error logging with context

### Key Data Structures

**Config**:
- `RoutingRules`: Array of routing rules (unused)
- `DefaultRoute`: Default route string (unused)

**RoutingRule**:
- `Condition`: Rule condition string (unused)
- `Route`: Target route string (unused)

### Future Extensibility

The processor is designed with future routing capabilities in mind:
- **Routing Rules**: Configuration structure for conditional routing
- **Multiple Consumers**: Architecture supports multiple downstream consumers
- **Filtering**: Framework for span filtering and selection
- **Transformation**: Base for span transformation capabilities
