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