// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:generate mdatagen metadata.yaml

// Package spanreconstructionprocessor contains the logic to reconstruct complete spans
// from event-based inputs. It processes span start events, span end events, and log events
// to build and maintain a running set of active spans and completed spans.
package spanreconstructionprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/spanreconstructionprocessor"
