# Span Reconstruction Processor Summary

## Overview
The span reconstruction processor is a custom OpenTelemetry Collector processor designed to handle real-time span events and reconstruct incomplete spans from distributed tracing data. It's particularly useful for scenarios where spans arrive out of order or are split across multiple events.

## Core Functionality

### Purpose
- **Reconstructs incomplete spans** from partial span data (START/END/LOG events)
- **Maintains active span state** in memory for reconstruction
- **Handles real-time event processing** for immediate decision-making
- **Supports client/server tagging** for distributed tracing scenarios

### Key Components

#### 1. **Active Span Management**
- Uses an in-memory map to track incomplete spans by `trace_id:span_id` key
- Implements LRU (Least Recently Used) eviction policy with configurable capacity
- TTL (Time To Live) expiration for stale spans
- Thread-safe operations for concurrent access

#### 2. **Event Processing Logic**
The processor handles three types of span events:
- **START events**: Begin a new span, stored in active spans map
- **END events**: Complete a span, trigger reconstruction and output
- **LOG events**: Add events/attributes to existing spans

#### 3. **Signal Types and Required Attributes**

The processor categorizes incoming spans into four signal types based on their attributes:

**Complete Span** (Pass-through)
- **Required**: `start_timestamp` ≠ 0 AND `end_timestamp` ≠ 0
- **Behavior**: Passed through directly without processing
- **Use case**: Already complete spans from standard tracing

**START Event**
- **Required**: `start_timestamp` ≠ 0 AND `end_timestamp` = 0
- **Optional**: `trace_id`, `span_id`, `name`, `attributes`, `events`
- **Behavior**: Creates new active span entry in memory
- **Use case**: Span initiation events

**END Event**
- **Required**: `start_timestamp` = 0 AND `end_timestamp` ≠ 0
- **Required**: `trace_id` AND `span_id` (must match existing START event)
- **Optional**: `attributes`, `events`
- **Behavior**: Completes matching active span, outputs reconstructed span
- **Use case**: Span completion events

**LOG Event**
- **Required**: `start_timestamp` = 0 AND `end_timestamp` = 0 AND `events` count > 0
- **Required**: `trace_id` AND `span_id` (must match existing START event)
- **Optional**: `attributes`
- **Behavior**: Adds events to existing active span
- **Use case**: Intermediate logging/annotation events

**Unknown Event** (Warning + Pass-through)
- **Trigger**: Any combination not matching above patterns
- **Behavior**: Logged as warning and passed through unchanged
- **Use case**: Malformed or unexpected span data

#### 4. **Span Reconstruction Algorithm**
```
For each incoming span:
1. Check if span has both start_time and end_time
   - If YES: Pass through directly (already complete)
   - If NO: Process as START/END/LOG event
2. For incomplete spans:
   - START: Create new active span entry
   - END: Find matching active span, reconstruct, output, remove from active
   - LOG: Add events to existing active span
3. Apply eviction policies (LRU/TTL) when capacity limits reached
```

### Configuration Options
- **Capacity**: Maximum number of active spans in memory (default: 1000)
- **TTL**: Time-to-live for active spans (default: 1 hour)
- **Eviction Policy**: LRU-based eviction when capacity exceeded

### Logging Features
Enhanced with colorful emoji prefixes for easy visual scanning:
- 🟢 **INFO**: High-level processing events (batch start/completion)
- 🟠 **DEBUG**: Detailed span processing information
- 🟡 **WARN**: Warning conditions (unknown event types, evictions)

### Use Cases
1. **Real-time monitoring**: Immediate visibility into span lifecycle events
2. **Distributed debugging**: Track spans across service boundaries
3. **Performance analysis**: Monitor span reconstruction efficiency
4. **Event-driven architectures**: Handle out-of-order span delivery

### Integration
- Plugs into OpenTelemetry Collector pipeline as a processor
- Compatible with OTLP receivers/exporters
- Supports standard OpenTelemetry span data model
- Works with existing tracing infrastructure (Jaeger, etc.)

### Performance Characteristics
- **Memory usage**: Bounded by capacity setting
- **Latency**: Minimal overhead for complete spans, reconstruction time for incomplete
- **Throughput**: Designed for high-volume trace processing
- **Scalability**: Thread-safe for concurrent processing

This processor essentially acts as a "span buffer" that can handle the complexity of distributed tracing where spans may arrive fragmented or out of order, providing a clean interface for downstream consumers while maintaining the integrity of the trace data. 