// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package priorityarenareceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/priorityprocessor/priorityarenareceiver"

// Tomislav-RetCtx: arena size from memory_mib, or from memory_percentage of the container's
// memory limit, read from the cgroup as the memory_limiter does.

import (
	"errors"
	"fmt"
	"math"
	"os"
	"runtime/debug"
	"strconv"
	"strings"

	"go.uber.org/zap"
)

// memoryLimit reads the container's memory limit; tests replace it.
var memoryLimit = containerMemoryLimit

// arenaSizing is the arena's size and how much of it LP may use, in percent of it.
type arenaSizing struct {
	bytes   int
	lpPct   float64 // the LP limit: blocks in use, of either class, up to which LP is admitted (adaptive: at start)
	bandPct float64 // adaptive: the checkpoint band at a size ratio of 1 or less (the reserve); 0 for a fixed limit
}

// sizing returns the arena's size, the LP limit and the checkpoint reserve.
func (c *Config) sizing(limit func() (uint64, error), logger *zap.Logger) (arenaSizing, error) {
	if c.MemoryPercentage == 0 {
		return arenaSizing{bytes: c.MemoryMiB << 20, lpPct: float64(c.LPLimitPercentage)}, nil
	}
	total, err := limit()
	if err != nil {
		return arenaSizing{}, fmt.Errorf("memory_percentage needs the container's memory limit: %w", err)
	}
	size := int(total * uint64(c.MemoryPercentage) / 100)
	pct := func(p int) float64 { return 100 * float64(p) / float64(c.MemoryPercentage) }
	sz := arenaSizing{bytes: size, lpPct: 100}
	if c.CheckpointReservePercentage != 0 {
		sz.bandPct = pct(c.CheckpointReservePercentage)
		sz.lpPct = 100 - sz.bandPct
	}
	if c.LPPercentage != 0 {
		sz.lpPct = pct(c.LPPercentage)
	}
	rest := int64(total) - int64(size)
	if gml := debug.SetMemoryLimit(-1); gml != math.MaxInt64 && gml > rest {
		logger.Warn("GOMEMLIMIT leaves no room for the arena: the Go heap and the arena together may exceed the container's limit",
			zap.Int64("gomemlimit_bytes", gml), zap.Int64("container_limit_bytes", int64(total)), zap.Int("arena_bytes", size))
	}
	logger.Info("priorityarena sized from the container's memory limit",
		zap.Uint64("container_limit_bytes", total), zap.Int("memory_percentage", c.MemoryPercentage),
		zap.Int("lp_percentage", c.LPPercentage), zap.Int("checkpoint_reserve_percentage", c.CheckpointReservePercentage),
		zap.Int("arena_bytes", size))
	return sz, nil
}

// containerMemoryLimit is the cgroup (v2, then v1) memory limit of this process.
func containerMemoryLimit() (uint64, error) {
	for _, path := range []string{"/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory/memory.limit_in_bytes"} {
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		s := strings.TrimSpace(string(data))
		if s == "max" {
			return 0, errors.New("the container has no memory limit")
		}
		v, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("%s: %w", path, err)
		}
		if v >= 1<<62 { // cgroup v1 reports "no limit" as a huge number
			return 0, errors.New("the container has no memory limit")
		}
		return v, nil
	}
	return 0, errors.New("no cgroup memory limit found")
}
