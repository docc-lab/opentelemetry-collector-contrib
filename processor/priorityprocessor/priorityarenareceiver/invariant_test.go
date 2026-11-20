// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package priorityarenareceiver

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
)

func TestBlocksForNeverOverflows(t *testing.T) {
	s := testStore(t, 10, 90)
	require.Equal(t, int32(10), s.a.blocksFor(10*testBlock))
	for _, size := range []int{10*testBlock + 1, math.MaxInt, -1} {
		require.Equal(t, int32(11), s.a.blocksFor(size), size)
	}
	_, _, code, _ := s.reserve(true, 10*testBlock+1, 1)
	require.Equal(t, codes.ResourceExhausted, code, "a request larger than the arena is refused, even a checkpoint")
	require.Equal(t, int64(1), s.c.hpRefused.Load())
	require.NoError(t, s.check())
}

func TestBrokenAccountingPanics(t *testing.T) {
	t.Run("reserve without spans", func(t *testing.T) {
		s := testStore(t, 10, 90)
		require.Panics(t, func() { s.reserve(false, testBlock, 0) })
	})
	t.Run("commit twice", func(t *testing.T) {
		s := testStore(t, 10, 90)
		id, _ := put(t, s, false, 1)
		require.Panics(t, func() { s.commit(id) })
	})
	t.Run("free twice", func(t *testing.T) {
		s := testStore(t, 10, 90)
		id, _ := put(t, s, false, 1)
		var b batch
		ok, _ := s.take(&b, anyBatch)
		require.True(t, ok)
		s.finish(&b, sent)
		require.Panics(t, func() { s.free(id) })
	})
	t.Run("block returned more often than lent", func(t *testing.T) {
		s := testStore(t, 10, 90)
		put(t, s, false, 1)
		var b batch
		s.take(&b, anyBatch)
		bufs := s.lend(&b)
		require.Len(t, bufs, 1)
		head := s.entries[b.ids[0]].head
		s.Put(&s.a.slices[head])
		require.Panics(t, func() { s.Put(&s.a.slices[head]) })
	})
	t.Run("release with the wrong count", func(t *testing.T) {
		a, err := newArena(10*testBlock, testBlock, false)
		require.NoError(t, err)
		head := a.alloc(3)
		require.Panics(t, func() { a.release(head, 2) }, "chain longer than the count")
		require.Panics(t, func() { a.release(head, 4) }, "more than is in use")
		require.Panics(t, func() { a.alloc(8) }, "more than is free")
	})
	t.Run("adapter band outside the arena", func(t *testing.T) {
		s := testStore(t, 10, 90)
		require.Panics(t, func() { newLPAdapter(s, 11, 1, zap.NewNop()) })
		require.Panics(t, func() { newLPAdapter(s, 1, 0, zap.NewNop()) })
	})
}

func TestAdapterIgnoresWindowWithoutLPBytes(t *testing.T) {
	s := testStore(t, 100, 50)
	a := newLPAdapter(s, 10, 1, zap.NewNop())
	admitted(s, 10, 1500, 20, 0)
	a.step()
	require.Equal(t, int32(50), s.lpLimit.Load())
}

func TestConfigBounds(t *testing.T) {
	for name, mutate := range map[string]func(*Config){
		"block over 1 MiB":             func(c *Config) { c.BlockKiB = 2048 },
		"arena of the whole container": func(c *Config) { c.MemoryPercentage = 100 },
		"more blocks than int32 holds": func(c *Config) { c.MemoryMiB = 1 << 23 },
		"size window over an hour": func(c *Config) {
			c.MemoryPercentage, c.CheckpointReservePercentage, c.SizeWindow = 70, 10, 2*time.Hour
		},
	} {
		c := createDefaultConfig().(*Config)
		mutate(c)
		require.Error(t, c.Validate(), name)
	}
	require.NoError(t, createDefaultConfig().(*Config).Validate())
}
