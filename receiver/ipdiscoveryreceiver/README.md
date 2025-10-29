# IP Discovery Receiver

The IP Discovery Receiver is a custom OpenTelemetry Collector component that provides HTTP endpoints for discovering and exposing the agent's own IP address. This is particularly useful in distributed environments like Kubernetes where agents need to communicate their network location to other components.

## Overview

The IP Discovery Receiver runs an HTTP server that exposes a single endpoint (`/getIP`) which returns the agent's IP address in JSON format. This enables other components in the distributed tracing system to discover and communicate with this agent.

## Configuration

```yaml
receivers:
  ipdiscovery:
    endpoint: ":8080"  # HTTP server endpoint
```

### Configuration Parameters

- `endpoint` (string, required): The HTTP server endpoint to listen on. Uses standard OpenTelemetry HTTP server configuration.

## HTTP API

### GET /getIP

Returns the agent's IP address in JSON format.

**Response:**
```json
{
  "ip": "192.168.1.100"
}
```

**Status Codes:**
- `200 OK`: Successfully returned IP address
- `500 Internal Server Error`: Failed to encode response

## IP Discovery Methods

The receiver uses multiple methods to discover the agent's IP address, in order of preference:

1. **Environment Variables** (Kubernetes):
   - `POD_IP`: Pod IP address
   - `HOST_IP`: Host IP address

2. **Network Interface Detection**:
   - Scans all network interfaces
   - Filters out loopback addresses (`127.x.x.x`)
   - Filters out link-local addresses (`169.254.x.x`)
   - Returns the first valid IPv4 address found

3. **Fallback**:
   - Returns `"localhost"` if no other method succeeds

## Usage in Pipelines

The IP Discovery Receiver is typically used in a logs pipeline for service discovery:

```yaml
service:
  pipelines:
    logs/ipdiscovery:
      receivers: [ipdiscovery]
      processors: []
      exporters: [debug]
```

## Implementation Details

- **HTTP Server**: Uses `httprouter` for routing and standard Go HTTP server
- **Graceful Shutdown**: Properly handles server shutdown with wait groups
- **Error Handling**: Reports fatal errors to the component host
- **Logging**: Provides debug logging for IP discovery process
- **Thread Safety**: Uses sync.WaitGroup for proper goroutine management

## Use Cases

- **Service Discovery**: Other agents can discover this agent's IP for communication
- **Load Balancing**: Load balancers can query agent IPs for health checks
- **Monitoring**: External monitoring systems can discover agent locations
- **Debugging**: Developers can easily check agent network configuration

---

## Technical Documentation

### Component Architecture

The IP Discovery Receiver implements the standard OpenTelemetry receiver interface but focuses on HTTP endpoint provision rather than telemetry data consumption. It consists of:

- **HTTP Server**: Handles incoming requests and serves IP discovery endpoints
- **IP Discovery Logic**: Multiple fallback methods for determining the agent's IP address
- **Configuration Management**: Standard OpenTelemetry configuration with HTTP server settings
- **Lifecycle Management**: Proper startup and shutdown handling

### IP Discovery Algorithm

1. **Environment Variable Check**: First checks for Kubernetes-specific environment variables (`POD_IP`, `HOST_IP`)
2. **Network Interface Scan**: Iterates through all network interfaces using `net.InterfaceAddrs()`
3. **Address Filtering**: Filters out loopback (`127.x.x.x`) and link-local (`169.254.x.x`) addresses
4. **IPv4 Preference**: Prioritizes IPv4 addresses over IPv6
5. **Fallback Strategy**: Returns `"localhost"` if no valid address is found

### HTTP Server Configuration

- **Router**: Uses `julienschmidt/httprouter` for efficient HTTP routing
- **Content Type**: Returns JSON responses with proper `Content-Type: application/json` headers
- **Error Handling**: Proper HTTP status codes and error responses
- **Logging**: Debug-level logging for request handling and IP discovery

### Integration Points

- **Component Host**: Integrates with OpenTelemetry component lifecycle management
- **Telemetry Settings**: Uses standard OpenTelemetry telemetry configuration
- **Logging**: Integrates with OpenTelemetry logging system using zap logger
- **Configuration**: Uses standard OpenTelemetry HTTP server configuration

### Performance Characteristics

- **Low Latency**: Simple HTTP endpoint with minimal processing overhead
- **Memory Efficient**: No data buffering or complex state management
- **Network Efficient**: Single endpoint with lightweight JSON responses
- **Resource Light**: Minimal CPU and memory footprint
