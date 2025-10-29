# Agent Contact Receiver

The Agent Contact Receiver is a gRPC-based receiver that accepts trace ID requests from other collector instances and passes them to the span lookup processor for processing.

## Configuration

The receiver can be configured with the following options:

```yaml
receivers:
  agentcontact:
    # Network address configuration
    endpoint: "localhost:55679"  # Default: "localhost:55679"
    transport: "tcp"             # Default: "tcp"
    
    # TLS configuration (optional)
    tls:
      cert_file: "/path/to/cert.pem"
      key_file: "/path/to/key.pem"
      ca_file: "/path/to/ca.pem"
      insecure: false
    
    # Additional gRPC server options
    read_buffer_size: 512000      # Default: 512KB
    write_buffer_size: 512000     # Default: 512KB
    max_concurrent_streams: 250   # Default: 250
    max_connection_idle: 15s      # Default: 15s
    max_connection_age: 30s       # Default: 30s
    max_connection_age_grace: 5s  # Default: 5s
    time: 2h                      # Default: 2h
    timeout: 20s                  # Default: 20s
    permit_without_stream: true   # Default: true
```

### Configuration Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `endpoint` | string | `"localhost:55679"` | The network address to bind to (host:port) |
| `transport` | string | `"tcp"` | The transport protocol to use |
| `tls` | object | - | TLS configuration for secure connections |
| `read_buffer_size` | int | `524288` | Size of the read buffer in bytes |
| `write_buffer_size` | int | `524288` | Size of the write buffer in bytes |
| `max_concurrent_streams` | int | `250` | Maximum number of concurrent gRPC streams |
| `max_connection_idle` | duration | `15s` | Maximum time a connection can be idle |
| `max_connection_age` | duration | `30s` | Maximum age of a connection |
| `max_connection_age_grace` | duration | `5s` | Grace period for connection age |
| `time` | duration | `2h` | Keepalive time |
| `timeout` | duration | `20s` | Keepalive timeout |
| `permit_without_stream` | bool | `true` | Allow keepalive pings without active streams |

## Usage

The receiver is designed to work with the span lookup processor in a pipeline:

```yaml
receivers:
  agentcontact:
    endpoint: "0.0.0.0:55679"  # Listen on all interfaces, port 55679

processors:
  spanlookup:
    # span lookup processor configuration

exporters:
  otlp:
    endpoint: "http://localhost:4317"

service:
  pipelines:
    traces:
      receivers: [agentcontact]
      processors: [spanlookup]
      exporters: [otlp]
```

## Protocol

The receiver implements the Agent Contact protocol via gRPC:

- **Service**: `AgentContact`
- **Method**: `SendTraceIDs`
- **Request**: List of trace IDs to lookup
- **Response**: Acknowledgment of receipt

The receiver accepts trace ID requests, sends back an acknowledgment, and then passes the trace IDs to the span lookup processor for processing.

---

## Technical Documentation

### Component Architecture

The Agent Contact Receiver implements a gRPC-based communication protocol for distributed trace lookup requests. It consists of:

- **gRPC Server**: Handles incoming trace ID requests from upstream collectors
- **Processing Queue**: Asynchronous queue system for processing trace ID requests
- **Queue Worker**: Background worker that processes queued requests
- **Trace Consumer**: Interface to pass processed requests to downstream processors

### Protocol Implementation

The receiver implements a custom gRPC protocol defined in `agent_contact_receiver.proto`:

- **Service**: `AgentContact`
- **Method**: `SendTraceIDs`
- **Request**: `TraceIDRequest` containing arrays of trace IDs and span IDs
- **Response**: `TraceIDResponse` with acknowledgment status

### Request Processing Flow

1. **gRPC Request Reception**: Incoming `SendTraceIDs` requests are received by the gRPC server
2. **Queue Enqueue**: Trace ID and span ID pairs are extracted and added to the processing queue
3. **Acknowledgment**: Immediate acknowledgment is sent back to the requesting agent
4. **Asynchronous Processing**: Queue worker processes requests in batches
5. **Trace Generation**: Each trace ID/span ID pair is converted to a synthetic trace
6. **Downstream Forwarding**: Generated traces are sent to the configured consumer (typically span lookup processor)

### Queue Management

- **Thread-Safe Queue**: Uses `sync.RWMutex` for concurrent access to the trace/span ID queue
- **Batch Processing**: Queue worker processes all queued items in batches to improve efficiency
- **CPU Throttling**: Includes 10ms sleep between processing cycles to prevent busy waiting
- **Graceful Shutdown**: Properly drains the queue during shutdown

### Trace Generation

For each trace ID/span ID pair, the receiver generates a synthetic trace with:

- **Trace ID**: Parsed from hex string to `pcommon.TraceID`
- **Span ID**: Parsed from hex string to `pcommon.SpanID`
- **Span Name**: `"agent-contact-requested-span"`
- **Span Kind**: `SpanKindInternal`
- **Timestamps**: Current time for start, +1ms for end
- **Status**: `StatusCodeOk`
- **Attributes**:
  - `agent.contact.requested`: `"true"`
  - `agent.contact.trace_id`: Original trace ID string
  - `agent.contact.span_id`: Original span ID string

### Error Handling

- **Hex Decoding**: Gracefully handles invalid hex strings by returning zero IDs
- **Consumer Errors**: Logs errors when trace consumption fails
- **gRPC Errors**: Properly handles gRPC server errors and shutdown
- **Queue Errors**: Thread-safe queue operations with proper error handling

### Performance Characteristics

- **Asynchronous Processing**: Non-blocking request handling with immediate acknowledgments
- **Batch Processing**: Efficient processing of multiple requests in batches
- **Memory Management**: Pre-allocated slices for efficient memory usage
- **CPU Optimization**: Built-in throttling to prevent excessive CPU usage
- **Connection Management**: Standard gRPC connection pooling and keepalive settings

### Integration Points

- **Span Lookup Processor**: Primary consumer for processing lookup requests
- **Agent Contact Exporter**: Upstream component that sends trace ID requests
- **gRPC Protocol**: Standard gRPC server with configurable network settings
- **OpenTelemetry Pipeline**: Integrates with standard OpenTelemetry trace processing pipeline

### Configuration Details

The receiver uses standard OpenTelemetry gRPC server configuration:

- **Network Address**: Configurable endpoint and transport protocol
- **TLS Support**: Optional TLS configuration for secure communications
- **Buffer Sizes**: Configurable read/write buffer sizes
- **Connection Limits**: Configurable concurrent streams and connection timeouts
- **Keepalive Settings**: Configurable keepalive parameters for connection management