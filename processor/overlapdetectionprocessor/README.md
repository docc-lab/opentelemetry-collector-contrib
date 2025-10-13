# Overlap Detection Processor

The Overlap Detection Processor analyzes segment spans to detect temporal overlaps between segments from the same or different server spans.

## Overview

This processor is designed to work with segment spans produced by the [Segmentation Processor](../segmentationprocessor/README.md). It groups segments by their span ID and detects overlaps based on configurable thresholds.

## Configuration

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `overlap_threshold_ms` | float64 | 1.0 | Minimum overlap duration in milliseconds to consider two segments as overlapping |
| `enable_metrics` | bool | false | Enables metrics collection for overlap detection |

## Example Configuration

```yaml
processors:
  overlapdetection:
    overlap_threshold_ms: 1.0
    enable_metrics: true
```

## How It Works

1. **Segment Grouping**: Segments are grouped by their span ID (inherited from the parent server span)
2. **Overlap Detection**: Within each group, segments are compared pairwise to detect temporal overlaps
3. **Threshold Filtering**: Only overlaps exceeding the configured threshold are reported
4. **Pass-through**: Original traces are passed through to the next consumer

## Overlap Detection Algorithm

For each pair of segments within the same span group:

1. Calculate the overlap time window:
   - `overlap_start = max(segment1.start_time, segment2.start_time)`
   - `overlap_end = min(segment1.end_time, segment2.end_time)`

2. If `overlap_start < overlap_end`, there is an overlap

3. Calculate overlap duration: `overlap_duration = overlap_end - overlap_start`

4. If `overlap_duration >= overlap_threshold_ms`, report the overlap

## Output

The processor outputs:
- Original traces (passed through unchanged)
- Overlap detection logs with details about detected overlaps
- Optional metrics (if enabled)

## Dependencies

- Requires segment spans from the Segmentation Processor
- Expects segment spans to have `service_operation`, `start_endpoint`, `end_endpoint`, and `is_bad` attributes

---

## Technical Documentation

### Component Architecture

The Overlap Detection Processor implements a sophisticated temporal overlap detection system with multi-worker architecture and comprehensive overlap tracking. Key architectural components:

- **Event Buffer**: Thread-safe queue for incoming span events with asynchronous processing
- **Segment Tracking**: Maps segment spans by spanID+segmentID for overlap analysis
- **Bad Segment Detection**: Specialized tracking for segments marked as "bad" by segmentation processor
- **Multi-Worker System**: Separate workers for span processing, bad segment analysis, and counter management
- **Overlap Information**: Detailed tracking of bad segment overlaps with metadata

### Overlap Detection Algorithm

The processor implements a comprehensive overlap detection process:

1. **Segment Collection**: Collects all segment spans from incoming traces
2. **Segment Grouping**: Groups segments by span ID for pairwise comparison
3. **Temporal Analysis**: Calculates overlap windows between segment pairs
4. **Threshold Filtering**: Filters overlaps based on configurable threshold
5. **Bad Segment Tracking**: Special handling for segments marked as "bad"
6. **Metadata Extraction**: Extracts service names, upstream IPs, and trace IDs

### Multi-Worker Architecture

**Main Worker Loop**:
- Processes incoming spans asynchronously
- Adds spans to event buffer for batch processing
- CPU throttling with 1ms sleep to prevent busy waiting
- Handles span collection and overlap detection

**Bad Segment Worker**:
- Specialized worker for analyzing bad segment overlaps
- Processes bad segments separately for enhanced detection
- Tracks overlapping segments with detailed metadata
- Maintains `BadSegmentOverlapInfo` structures

**Counter Worker**:
- Manages overlap counters and statistics
- Provides periodic counter updates
- Tracks bad segment counts and overlap counts
- Thread-safe counter management

### Thread Safety Implementation

- **Multiple Mutexes**: Separate mutexes for different data structures
- **Read-Write Locks**: `segmentsMutex` for segment collections
- **Counter Protection**: `countersMutex` for statistical counters
- **Overlap Protection**: `overlapsMutex` for overlap information
- **Buffer Protection**: `SpanBuffer` mutex for event queue

### Performance Optimizations

- **CPU Throttling**: 1ms sleep in worker loop prevents busy waiting
- **Batch Processing**: Processes all queued spans in single iteration
- **Asynchronous Processing**: Non-blocking span processing with workers
- **Memory Efficiency**: Efficient data structures for overlap tracking

### Integration Points

- **Segmentation Processor**: Receives segment spans for overlap analysis
- **Statistics Aggregation**: Feeds overlap detection results for statistics
- **Agent Contact Exporter**: Triggers trace ID requests for overlapping spans
- **OpenTelemetry Pipeline**: Standard processor interface with trace consumer

### Configuration Options

- **`overlap_threshold_ms`**: Minimum overlap duration threshold (default: 1.0ms)
- **`enable_metrics`**: Internal metrics collection (default: false)

### Error Handling & Resilience

- **Invalid Data**: Robust handling of missing segment attributes
- **Threshold Validation**: Configurable threshold validation
- **Memory Management**: Efficient cleanup of processed spans
- **Worker Management**: Proper shutdown of all worker goroutines

### Monitoring & Observability

- **Comprehensive Logging**: Debug-level logging for overlap detection
- **Overlap Tracking**: Detailed logging of detected overlaps
- **Counter Management**: Periodic counter updates and statistics
- **Performance Monitoring**: Processing time and overlap count tracking

### Key Data Structures

**SegmentSpanWithTime**:
- `Span`: Original segment span with attributes
- `AddedAt`: Timestamp when segment was added

**BadSegmentOverlapInfo**:
- `BadSegmentTraceID`: Trace ID of the bad segment
- `OverlappingTraceIDs`: Array of overlapping trace IDs
- `BadSegmentUpstreamIP`/`OverlappingUpstreamIPs`: IP address tracking
- `BadSegmentServiceName`/`OverlappingServiceNames`: Service name tracking
- `DetectedAt`: Timestamp of overlap detection

**SpanBuffer**:
- `mu`: Mutex for thread-safe access
- `spans`: Array of unprocessed spans

### Overlap Detection Logic

**Temporal Overlap Calculation**:
1. **Overlap Start**: `max(segment1.start_time, segment2.start_time)`
2. **Overlap End**: `min(segment1.end_time, segment2.end_time)`
3. **Overlap Duration**: `overlap_end - overlap_start`
4. **Threshold Check**: `overlap_duration >= overlap_threshold_ms`

**Bad Segment Processing**:
- Special handling for segments marked with `is_bad: true`
- Enhanced metadata extraction for bad segments
- Detailed overlap tracking with service and IP information
- Separate worker for bad segment analysis
