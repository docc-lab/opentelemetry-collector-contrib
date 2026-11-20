// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package priorityarenareceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/priorityprocessor/priorityarenareceiver"

// Tomislav-RetCtx: the arena holds every buffered request's bytes. It is one region mapped
// outside the Go heap and split into equal blocks; it never holds Go pointers, so the garbage
// collector neither scans it nor counts it towards the heap. All bookkeeping is int32 indices.

import "fmt"

const noBlock = -1

// maxBlocks bounds the arena so that block and entry indices, counts and their sums stay int32.
const maxBlocks = 1 << 30

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
	if n > maxBlocks {
		return nil, fmt.Errorf("arena of %d blocks is too large", n)
	}
	size = n * blockSize
	a := &arena{blockSize: blockSize, nblocks: int32(n), next: make([]int32, n), slices: make([][]byte, n)}
	if useMmap {
		mem, mapped, err := mapArena(size)
		if err != nil {
			return nil, err
		}
		a.mem, a.mapped = mem, mapped
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
	return unmapArena(a.mem)
}

// blocksFor is the number of blocks size bytes need, or nblocks+1 when they cannot fit the
// arena at all (so the int32 count never overflows).
func (a *arena) blocksFor(size int) int32 {
	if size < 0 || size > int(a.nblocks)*a.blockSize {
		return a.nblocks + 1
	}
	return int32((size + a.blockSize - 1) / a.blockSize)
}

// alloc takes n blocks off the free list and returns the first; the caller has checked a.free.
func (a *arena) alloc(n int32) int32 {
	if n <= 0 || n > a.free {
		broken("alloc of %d blocks with %d free", n, a.free)
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
	if n <= 0 || n > a.nblocks-a.free || head < 0 || head >= a.nblocks {
		broken("release of %d blocks from block %d with %d of %d free", n, head, a.free, a.nblocks)
	}
	last := head
	for i := int32(1); i < n; i++ {
		if last = a.next[last]; last == noBlock {
			broken("released chain is shorter than its %d blocks", n)
		}
	}
	if a.next[last] != noBlock {
		broken("released chain is longer than its %d blocks", n)
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
				if a.next[blk] == noBlock {
					broken("request overran the blocks reserved for it, at block %d", blk)
				}
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
