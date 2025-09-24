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
