// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:generate mdatagen metadata.yaml

// Package overlapdetectionprocessor analyzes segment spans to detect temporal overlaps
// between segments from the same or different server spans.
package overlapdetectionprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/overlapdetectionprocessor"
