// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:build linux

package priorityarenareceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/priorityprocessor/priorityarenareceiver"

import (
	"fmt"
	"syscall"
)

// mapArena maps size bytes outside the Go heap. MAP_POPULATE faults every page in now, so the
// container is charged for the arena once, at start, and never page-faults while receiving.
func mapArena(size int) ([]byte, bool, error) {
	mem, err := syscall.Mmap(-1, 0, size, syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_ANON|syscall.MAP_PRIVATE|syscall.MAP_POPULATE)
	if err != nil {
		return nil, false, fmt.Errorf("mmap %d bytes: %w", size, err)
	}
	return mem, true, nil
}

func unmapArena(mem []byte) error { return syscall.Munmap(mem) }
