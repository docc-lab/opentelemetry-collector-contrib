// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package priorityarenareceiver

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/mem"
)

const testBlock = 4096

func testStore(t *testing.T, blocks, lpPct int) *store {
	a, err := newArena(blocks*testBlock, testBlock, false)
	require.NoError(t, err)
	s := newStore(a, float64(lpPct))
	s.roomWait = 0 // tests that wait for in-flight LP set it
	return s
}

// put admits a request of the given number of blocks (full ones) and returns its entry.
func put(t *testing.T, s *store, hp bool, blocks int) (int32, codes.Code) {
	size := blocks * testBlock
	id, head, code, _ := s.reserve(hp, size, blocks)
	if code != codes.OK {
		return noEntry, code
	}
	s.a.fill(head, [][]byte{make([]byte, size)})
	s.commit(id)
	require.NoError(t, s.check())
	return id, code
}

func queued(s *store, f *fifo) []int32 {
	s.mu.Lock()
	defer s.mu.Unlock()
	var ids []int32
	for id := f.head; id != noEntry; id = s.entries[id].next {
		ids = append(ids, id)
	}
	return ids
}

var anyBatch = takeLimits{maxSpans: 1 << 30, maxBytes: 1 << 30}

func TestLPLimit(t *testing.T) {
	s := testStore(t, 10, 50)
	for i := 0; i < 5; i++ {
		_, code := put(t, s, false, 1)
		require.Equal(t, codes.OK, code)
	}
	_, code := put(t, s, false, 1)
	require.Equal(t, codes.ResourceExhausted, code)
	require.True(t, s.refuseLPEarly())
	require.False(t, s.refuseHPEarly())
	// checkpoints still fit in the reserved half
	_, code = put(t, s, true, 5)
	require.Equal(t, codes.OK, code)
	require.Equal(t, int64(1), s.c.lpRefused.Load())
}

func TestCheckpointEvictsNewestLP(t *testing.T) {
	s := testStore(t, 10, 50)
	var lp []int32
	for i := 0; i < 5; i++ {
		id, _ := put(t, s, false, 1)
		lp = append(lp, id)
	}
	_, code := put(t, s, true, 5)
	require.Equal(t, codes.OK, code)
	_, code = put(t, s, true, 2)
	require.Equal(t, codes.OK, code)
	require.Equal(t, lp[:3], queued(s, &s.lp), "the two newest LP requests go first")
	require.Equal(t, int64(2), s.c.lpEvicted.Load())
	require.Equal(t, int64(2), s.c.hpByEviction.Load())
	require.Equal(t, int32(0), s.a.free)
}

func TestCheckpointRefusedOnlyWhenNoLPLeft(t *testing.T) {
	s := testStore(t, 4, 100)
	put(t, s, false, 1)
	put(t, s, true, 3)
	_, code := put(t, s, true, 2) // one LP block to evict is not enough
	require.Equal(t, codes.Unavailable, code)
	require.Len(t, queued(s, &s.lp), 1, "LP is not evicted for a checkpoint that cannot fit anyway")
	_, code = put(t, s, true, 1) // evicting it makes room for a one-block checkpoint
	require.Equal(t, codes.OK, code)
	require.Empty(t, queued(s, &s.lp))
	require.True(t, s.refuseHPEarly())
	require.True(t, s.refuseLPEarly())
}

func TestInflightLPEvictedByCancel(t *testing.T) {
	s := testStore(t, 4, 100)
	s.roomWait = 5 * time.Second
	put(t, s, false, 2)
	put(t, s, false, 2)
	var b batch
	ok, _ := s.take(&b, takeLimits{maxSpans: 2, maxBytes: 1 << 30})
	require.True(t, ok)
	require.Len(t, b.ids, 1)
	require.False(t, s.refuseHPEarly(), "in-flight LP counts as room for checkpoints")

	// the send: cancelled when a checkpoint evicts it, then it returns and is settled
	ctx, cancel := context.WithCancel(context.Background())
	s.arm(&b, cancel)
	go func() {
		<-ctx.Done()
		s.finish(&b, requeue)
	}()
	_, code := put(t, s, true, 4) // queued LP (2 blocks) and the flight (2 blocks)
	require.Equal(t, codes.OK, code)
	require.Error(t, ctx.Err(), "the LP send was cancelled")
	require.Empty(t, queued(s, &s.lp), "the evicted flight is dropped, not requeued")
	require.Equal(t, int64(4), s.c.lpEvicted.Load(), "the queued request and the one in flight")
	require.Zero(t, s.c.lpRequeued.Load())
	require.NoError(t, s.check())
}

func TestEvictedFlightDeliveredStillCounts(t *testing.T) {
	// A flight evicted after its send already succeeded: the LP was delivered, not evicted.
	s := testStore(t, 4, 100)
	put(t, s, false, 2)
	var b batch
	s.take(&b, anyBatch)
	_, code := put(t, s, true, 4) // nothing returns within roomWait 0: refused, flight evicted
	require.Equal(t, codes.Unavailable, code)
	s.finish(&b, sent)
	require.Equal(t, int64(2), s.c.lpSent.Load())
	require.Zero(t, s.c.lpEvicted.Load())
	require.NoError(t, s.check())
	require.Equal(t, s.a.nblocks, s.a.free)
}

func TestTakeOrder(t *testing.T) {
	s := testStore(t, 20, 100)
	l1, _ := put(t, s, false, 1)
	h1, _ := put(t, s, true, 1)
	l2, _ := put(t, s, false, 1)
	h2, _ := put(t, s, true, 1)
	var b batch
	ok, _ := s.take(&b, anyBatch)
	require.True(t, ok)
	require.True(t, b.hp)
	require.Equal(t, []int32{h1, h2}, b.ids, "checkpoints first, one class per batch")
	s.finish(&b, requeue)
	ok, _ = s.take(&b, anyBatch)
	require.True(t, ok)
	require.Equal(t, []int32{h1, h2}, b.ids, "a requeued batch keeps its place and order")
	s.finish(&b, sent)
	ok, _ = s.take(&b, anyBatch)
	require.True(t, ok)
	require.False(t, b.hp)
	require.Equal(t, []int32{l1, l2}, b.ids)
	s.finish(&b, sent)
	require.NoError(t, s.check())
	require.Equal(t, s.a.nblocks, s.a.free)

	// pipeline mode mixes: checkpoints first, then LP
	l3, _ := put(t, s, false, 1)
	h3, _ := put(t, s, true, 1)
	ok, _ = s.take(&b, takeLimits{maxSpans: 1 << 30, maxBytes: 1 << 30, mixed: true})
	require.True(t, ok)
	require.Equal(t, []int32{h3, l3}, b.ids)
	s.finish(&b, sent)
}

func TestTakeLimitsAndLinger(t *testing.T) {
	s := testStore(t, 20, 100)
	for i := 0; i < 3; i++ {
		put(t, s, true, 1) // 1 span each
	}
	var b batch
	lim := takeLimits{maxSpans: 2, maxBytes: 1 << 30}
	ok, _ := s.take(&b, lim)
	require.True(t, ok)
	require.Len(t, b.ids, 2)
	s.finish(&b, sent)

	lim = takeLimits{maxSpans: 5, maxBytes: 1 << 30, linger: time.Hour}
	ok, wait := s.take(&b, lim)
	require.False(t, ok, "one span queued, below the batch size, young")
	require.Greater(t, wait, 59*time.Minute)
	for i := 0; i < 4; i++ {
		put(t, s, true, 1)
	}
	ok, _ = s.take(&b, lim)
	require.True(t, ok, "batch size reached")
	require.Equal(t, 5, b.spans)
	s.finish(&b, sent)
}

func TestLentBlocksFreedWhenReturned(t *testing.T) {
	s := testStore(t, 8, 100)
	put(t, s, true, 3)
	var b batch
	ok, _ := s.take(&b, anyBatch)
	require.True(t, ok)
	bufs := s.lend(&b)
	require.Len(t, bufs, 3)
	require.Equal(t, 3*testBlock, bufs.Len())
	s.finish(&b, sent) // delivered, but gRPC still holds the blocks
	require.NoError(t, s.check())
	require.Equal(t, int32(5), s.a.free)
	bufs[0].Free()
	bufs[1].Free()
	require.Equal(t, int32(5), s.a.free)
	bufs[2].Free()
	require.Equal(t, int32(8), s.a.free)
	require.NoError(t, s.check())
}

func TestRequeuedLentEntryEvicted(t *testing.T) {
	// A send that failed while gRPC still holds its blocks; the entry is then evicted. Its
	// blocks are freed only when gRPC returns them.
	s := testStore(t, 4, 100)
	put(t, s, false, 2)
	var b batch
	s.take(&b, anyBatch)
	bufs := s.lend(&b)
	s.finish(&b, requeue)
	_, code := put(t, s, true, 4)
	require.Equal(t, codes.Unavailable, code, "evicted, but its blocks are not back yet")
	require.Equal(t, int32(2), s.a.free)
	bufs.Free()
	require.Equal(t, int32(4), s.a.free)
	_, code = put(t, s, true, 4)
	require.Equal(t, codes.OK, code)
}

// TestStoreRandom runs random admissions, sends and returns, checking the accounting after
// each step, and that a checkpoint is refused only when no queued LP was left.
func TestStoreRandom(t *testing.T) {
	r := rand.New(rand.NewSource(5))
	s := testStore(t, 64, 75)
	var (
		inflight []batch
		held     []mem.BufferSlice
	)
	for step := 0; step < 20000; step++ {
		switch op := r.Intn(10); {
		case op < 5:
			hp := r.Intn(3) == 0
			blocks := r.Intn(6) + 1
			// what a checkpoint may take: free blocks plus queued LP that gRPC does not hold
			s.mu.Lock()
			room := s.a.free
			for id := s.lp.head; id != noEntry; id = s.entries[id].next {
				if s.lent[id].Load() == 0 {
					room += s.entries[id].nblocks
				}
			}
			s.mu.Unlock()
			_, code := put(t, s, hp, blocks)
			if hp && code != codes.OK {
				require.Less(t, room, int32(blocks), "checkpoint refused though free and queued LP blocks covered it")
			}
		case op < 7:
			var b batch
			if ok, _ := s.take(&b, takeLimits{maxSpans: r.Intn(8) + 1, maxBytes: 1 << 30}); ok {
				if r.Intn(2) == 0 {
					held = append(held, s.lend(&b))
				}
				inflight = append(inflight, b)
			}
		case op < 9 && len(inflight) > 0:
			i := r.Intn(len(inflight))
			s.finish(&inflight[i], outcome(r.Intn(3)))
			inflight = append(inflight[:i], inflight[i+1:]...)
		case len(held) > 0:
			i := r.Intn(len(held))
			held[i].Free()
			held = append(held[:i], held[i+1:]...)
		}
		require.NoError(t, s.check(), "step %d", step)
	}
	for i := range inflight {
		s.finish(&inflight[i], sent)
	}
	for _, h := range held {
		h.Free()
	}
	var b batch
	for {
		ok, _ := s.take(&b, anyBatch)
		if !ok {
			break
		}
		s.finish(&b, sent)
	}
	require.NoError(t, s.check())
	require.Equal(t, s.a.nblocks, s.a.free, "every block back")
}

func TestArenaMmap(t *testing.T) {
	a, err := newArena(1<<20, testBlock, true)
	require.NoError(t, err)
	require.True(t, a.mapped)
	head := a.alloc(3)
	a.fill(head, [][]byte{make([]byte, 2*testBlock+10)})
	var got [][]byte
	got = a.chunks(head, got)
	require.Len(t, got, 3)
	require.Len(t, got[2], 10)
	a.release(head, 3)
	require.Equal(t, a.nblocks, a.free)
	require.NoError(t, a.close())
}

func TestSizingFromContainerLimit(t *testing.T) {
	c := createDefaultConfig().(*Config)
	c.MemoryPercentage, c.LPPercentage = 70, 50
	require.NoError(t, c.Validate())
	size, lpPct, err := c.sizing(func() (uint64, error) { return 256 << 20, nil }, zap.NewNop())
	require.NoError(t, err)
	require.Equal(t, int(256<<20)*70/100, size)
	require.InDelta(t, 100*50.0/70, lpPct, 1e-9)
	a, err := newArena(size, testBlock, false)
	require.NoError(t, err)
	s := newStore(a, lpPct)
	// LP stops at 50 % of the container, checkpoints at 70 %
	require.InDelta(t, float64(256<<20)/2, float64(s.lpLimit)*testBlock, testBlock)
	require.InDelta(t, float64(256<<20)*0.7, float64(s.a.nblocks)*testBlock, testBlock)

	c.LPPercentage = 80
	require.Error(t, c.Validate(), "LP above the arena")
	c.MemoryPercentage, c.LPPercentage = 0, 50
	require.Error(t, c.Validate(), "lp_percentage without memory_percentage")
}
