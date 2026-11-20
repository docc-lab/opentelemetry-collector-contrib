// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:build !linux

package priorityarenareceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/priorityprocessor/priorityarenareceiver"

// mapArena falls back to an ordinary Go slice: the receiver works, but the arena is on the Go heap
// and counts towards GOMEMLIMIT.
func mapArena(size int) ([]byte, bool, error) { return make([]byte, size), false, nil }

func unmapArena([]byte) error { return nil }
