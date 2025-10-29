# Agent Contact Exporter

The Agent Contact Exporter is a gRPC-based exporter that sends trace ID requests to upstream collector instances based on IP addresses found in span attributes. It uses sophisticated per-IP batching with dedicated goroutines for each upstream IP.

## Configuration

The exporter can be configured with the following options:

```yaml
exporters:
  agentcontact:
    # Batching configuration
    timeout: 30s                 # Default: 30s
    batch_size: 100              # Default: 100
    batch_timeout: 5s            # Default: 5s
    
    # Dynamic endpoint configuration
    default_port: 55679          # Default: 55679
```

### Configuration Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `timeout` | duration | `30s` | Timeout for gRPC requests |
| `batch_size` | int | `100` | Maximum number of trace IDs to send in a single request |
| `batch_timeout` | duration | `5s` | Timeout for batching trace IDs before sending |
| `default_port` | int | `55679` | Port to use when constructing dynamic endpoints from IP addresses |

## Upstream IP Detection

The exporter looks for the `upstream.ip` attribute in spans to determine which upstream collector to contact:

```yaml
# Example span with upstream IP
spans:
  - trace_id: "1234567890abcdef"
    span_id: "abcdef1234567890"
    attributes:
      upstream.ip: "192.168.1.100"  # This IP will be contacted
```

## Per-IP Batching Architecture

The exporter implements sophisticated per-IP batching with dedicated goroutines:

### **Key Features:**

1. **Dedicated Goroutines**: Each upstream IP gets its own goroutine for batch management
2. **Independent Batching**: Each IP's batches are managed independently
3. **Timeout-Based Flushing**: Batches are flushed when either:
   - Batch size limit is reached
   - Batch timeout expires (prevents upstream agents from not being contacted)
4. **Thread-Safe Operations**: All batch operations are thread-safe with proper mutex protection

### **Batch Manager Lifecycle:**

1. **Creation**: When a new upstream IP is encountered, a dedicated `ipBatchManager` is created
2. **Goroutine Spawn**: Each batch manager runs in its own goroutine
3. **Batch Collection**: Trace IDs are added to the IP-specific batch
4. **Flush Triggers**: Batches are sent when size limit OR timeout is reached
5. **Graceful Shutdown**: All batch managers are properly shut down on exporter shutdown

### **Batch Flush Conditions:**

- **Size-Based**: When `batch_size` trace IDs are collected
- **Timeout-Based**: When `batch_timeout` expires (even if batch is not full)
- **Shutdown-Based**: When the exporter is shutting down

## Connection Management

### **Connection Creation:**
- New connections are only created when a span contains a new `upstream.ip` that hasn't been seen before
- Connections persist for the lifetime of the exporter
- No connection limits are enforced

### **Endpoint Construction:**
- Endpoints are constructed as `{upstream.ip}:{default_port}`
- Example: `192.168.1.100:55679`

## Usage

### Basic Configuration

```yaml
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: "0.0.0.0:4317"

processors:
  spanreconstruction:
    # span reconstruction processor configuration

exporters:
  agentcontact:
    default_port: 55679
    timeout: 30s
    batch_size: 100
    batch_timeout: 5s

service:
  pipelines:
    traces:
      receivers: [otlp]
      processors: [spanreconstruction]
      exporters: [agentcontact]
```

### Example Behavior

If spans contain `upstream.ip` values like `192.168.1.100` and `10.0.0.50`:

1. **Two separate batch managers** are created
2. **Each IP gets its own goroutine** and timer
3. **Independent batching**: 
   - `192.168.1.100` batch flushes when it reaches 100 trace IDs OR after 5 seconds
   - `10.0.0.50` batch flushes when it reaches 100 trace IDs OR after 5 seconds
4. **No cross-contamination**: Each IP's batches are completely independent

## Protocol

The exporter implements the Agent Contact protocol via gRPC:

- **Service**: `AgentContact`
- **Method**: `SendTraceIDs`
- **Request**: List of trace IDs to lookup
- **Response**: Acknowledgment of receipt

The exporter extracts trace IDs from incoming traces, batches them per upstream IP address, and sends them to remote collectors for span lookup processing. Each upstream IP has its own independent batching and timing.

---

## Technical Documentation

### Component Architecture

The Agent Contact Exporter implements a sophisticated per-IP batching system with dedicated goroutines and event-driven processing. Key architectural components:

- **Per-IP Batch Managers**: Dedicated `ipBatchManager` instances for each upstream IP
- **Event-Driven Processing**: Uses channels and select statements for non-blocking operations
- **Connection Pooling**: Maintains persistent gRPC connections to upstream collectors
- **Global Access**: Singleton pattern for cross-processor communication
- **Graceful Shutdown**: Proper cleanup of all batch managers and connections

### Per-IP Batching System

**Batch Manager Lifecycle**:
1. **Creation**: New `ipBatchManager` created when new upstream IP encountered
2. **Goroutine Spawn**: Each manager runs in dedicated goroutine with `run()` method
3. **Event Loop**: Uses `select` with channels for flush triggers and shutdown
4. **Batch Management**: Independent batching per IP with size and timeout limits
5. **Connection Management**: Persistent gRPC connections per IP

**Batch Flush Triggers**:
- **Size-Based**: When `batch_size` trace IDs collected
- **Timeout-Based**: When `batch_timeout` expires (prevents starvation)
- **Manual Flush**: Via `flushChan` for immediate processing
- **Shutdown Flush**: Final flush during graceful shutdown

### Event-Driven Architecture

**Channel Communication**:
- `flushChan`: Manual flush requests
- `stopChan`: Shutdown signal
- `timerChan`: Timeout-based flush triggers
- `timer`: Go timer for timeout management

**Non-Blocking Operations**:
- Uses `select` statements for event handling
- No busy waiting or tight loops
- Event-driven processing with channel communication
- Proper goroutine synchronization with wait groups

### Connection Management

**Dynamic Endpoint Construction**:
- Endpoints: `{upstream.ip}:{default_port}`
- Example: `192.168.1.100:55679`
- New connections only for new upstream IPs
- Persistent connections for lifetime of exporter

**gRPC Client Management**:
- One client per upstream IP
- Connection reuse for efficiency
- Proper connection cleanup on shutdown
- Timeout configuration per request

### Thread Safety Implementation

- **Batch Manager Mutex**: Each manager has independent state
- **Global Mutex**: `batchManagersMutex` protects batch manager map
- **Channel Communication**: Thread-safe channel operations
- **Wait Groups**: Proper goroutine synchronization
- **Atomic Operations**: Safe counter updates

### Performance Optimizations

- **Per-IP Isolation**: Independent batching prevents cross-contamination
- **Event-Driven Processing**: No CPU busy waiting
- **Connection Reuse**: Persistent gRPC connections
- **Batch Efficiency**: Configurable batch sizes and timeouts
- **Memory Management**: Pre-allocated slices with capacity

### Integration Points

- **Lookup Routing Processor**: Primary source of trace ID requests
- **Agent Contact Receiver**: Destination for trace ID requests
- **Global Access**: `GlobalAgentContactExporter` singleton
- **OpenTelemetry Pipeline**: Standard exporter interface

### Configuration Options

- **`default_port`**: Port for dynamic endpoint construction (default: 55679)
- **`timeout`**: gRPC request timeout (default: 30s)
- **`batch_size`**: Maximum trace IDs per batch (default: 100)
- **`batch_timeout`**: Batch timeout for flushing (default: 5s)

### Error Handling & Resilience

- **Connection Failures**: Graceful handling of gRPC connection errors
- **Batch Management**: Robust batch state management
- **Shutdown Handling**: Proper cleanup of all resources
- **Timeout Management**: Prevents upstream agents from being starved

### Monitoring & Observability

- **Comprehensive Logging**: Debug-level logging for batch operations
- **Batch Tracking**: Detailed logging of batch sizes and flush triggers
- **Connection Monitoring**: Logging of connection creation and management
- **Performance Metrics**: Batch processing time and success rates

### Key Data Structures

**TraceSpanPair**:
- `TraceID`: Trace identifier string
- `SpanID`: Span identifier string

**ipBatchManager**:
- `ip`/`endpoint`: Upstream IP and constructed endpoint
- `traceSpanPairs`: Array of trace/span ID pairs
- `batchSize`/`batchTimeout`: Batch configuration
- `client`: gRPC client for upstream communication
- `flushChan`/`stopChan`/`timerChan`: Event channels

**AgentContactExporter**:
- `batchManagers`: Map of IP -> batch manager
- `batchManagersMutex`: Mutex for batch manager map
- `config`: Exporter configuration
- `logger`: Logger instance

### Protocol Implementation

**gRPC Protocol**:
- Service: `AgentContact`
- Method: `SendTraceIDs`
- Request: `TraceIDRequest` with trace ID array
- Response: `TraceIDResponse` with acknowledgment

**Request Processing**:
1. Extract trace IDs from incoming traces
2. Group by upstream IP from span attributes
3. Add to appropriate batch manager
4. Trigger flush based on size or timeout
5. Send gRPC request to upstream collector

### Advanced Features

**Dynamic IP Discovery**:
- Automatically discovers new upstream IPs
- Creates batch managers on-demand
- No pre-configuration required
- Scales with number of upstream agents

**Timeout Management**:
- Prevents upstream agents from being starved
- Ensures timely delivery of trace ID requests
- Configurable timeout values
- Graceful handling of timeout scenarios 