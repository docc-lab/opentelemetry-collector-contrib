// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package priorityarenareceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/priorityprocessor/priorityarenareceiver"

// Tomislav-RetCtx: the arena holds every buffered request's bytes. It is one region mapped
// outside the Go heap and split into equal blocks; it never holds Go pointers, so the garbage
// collector neither scans it nor counts it towards the heap. All bookkeeping is int32 indices.

import (
	"fmt"
	"syscall"
)

const noBlock = -1

type arena struct {
	mem       []byte
	blockSize int
	nblocks   int32

	// next links the blocks of one entry, or the free list, in order; noBlock ends a chain.
	next     []int32
	freeHead int32
	free     int32

	// slices[i] views block i, cut to the bytes it holds; gRPC gets &slices[i] when a block is
	// sent, and hands the same pointer back when it is done with it.
	slices [][]byte

	mapped bool
}

// newArena maps size bytes (rounded down to whole blocks). With useMmap false the region is an
// ordinary Go slice, for tests.
func newArena(size, blockSize int, useMmap bool) (*arena, error) {
	if blockSize <= 0 || size < blockSize {
		return nil, fmt.Errorf("arena of %d bytes cannot hold a %d-byte block", size, blockSize)
	}
	n := size / blockSize
	if n > 1<<30 {
		return nil, fmt.Errorf("arena of %d blocks is too large", n)
	}
	size = n * blockSize
	a := &arena{blockSize: blockSize, nblocks: int32(n), next: make([]int32, n), slices: make([][]byte, n)}
	if useMmap {
		// MAP_POPULATE faults every page in now, so the container is charged for the arena once,
		// at start, and never page-faults while receiving.
		mem, err := syscall.Mmap(-1, 0, size, syscall.PROT_READ|syscall.PROT_WRITE,
			syscall.MAP_ANON|syscall.MAP_PRIVATE|syscall.MAP_POPULATE)
		if err != nil {
			return nil, fmt.Errorf("mmap %d bytes: %w", size, err)
		}
		a.mem, a.mapped = mem, true
	} else {
		a.mem = make([]byte, size)
	}
	for i := range a.next {
		a.next[i] = int32(i + 1)
	}
	a.next[n-1] = noBlock
	a.freeHead, a.free = 0, int32(n)
	return a, nil
}

func (a *arena) close() error {
	if !a.mapped {
		return nil
	}
	a.mapped = false
	return syscall.Munmap(a.mem)
}

// blocksFor is the number of blocks size bytes need.
func (a *arena) blocksFor(size int) int32 {
	return int32((size + a.blockSize - 1) / a.blockSize)
}

// alloc takes n blocks off the free list and returns the first; the caller has checked a.free.
func (a *arena) alloc(n int32) int32 {
	if n <= 0 || n > a.free {
		panic(fmt.Sprintf("arena: alloc %d blocks with %d free", n, a.free))
	}
	head := a.freeHead
	last := head
	for i := int32(1); i < n; i++ {
		last = a.next[last]
	}
	a.freeHead = a.next[last]
	a.next[last] = noBlock
	a.free -= n
	return head
}

// release puts a chain of n blocks back on the free list.
func (a *arena) release(head, n int32) {
	last := head
	for i := int32(1); i < n; i++ {
		last = a.next[last]
	}
	if a.next[last] != noBlock {
		panic("arena: released chain is longer than its block count")
	}
	a.next[last] = a.freeHead
	a.freeHead = head
	a.free += n
}

// block returns the whole of block i.
func (a *arena) block(i int32) []byte {
	off := int(i) * a.blockSize
	return a.mem[off : off+a.blockSize : off+a.blockSize]
}

// fill copies src into the chain starting at head and records how much of each block is used.
func (a *arena) fill(head int32, src [][]byte) {
	blk, dst := head, a.block(head)
	used := 0
	for _, s := range src {
		for len(s) > 0 {
			if used == a.blockSize {
				a.slices[blk] = dst[:used]
				blk, dst, used = a.next[blk], a.block(a.next[blk]), 0
			}
			n := copy(dst[used:], s)
			used += n
			s = s[n:]
		}
	}
	a.slices[blk] = dst[:used]
}

// chunks appends the used part of every block in the chain to out.
func (a *arena) chunks(head int32, out [][]byte) [][]byte {
	for blk := head; blk != noBlock; blk = a.next[blk] {
		out = append(out, a.slices[blk])
	}
	return out
}
