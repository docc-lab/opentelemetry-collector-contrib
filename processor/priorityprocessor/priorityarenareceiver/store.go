// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package priorityarenareceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/priorityprocessor/priorityarenareceiver"

// Tomislav-RetCtx: the store queues admitted requests in the arena, one entry per request, in
// two FIFOs: checkpoints (HP) and the rest (LP). Admission is decided per request on exact
// block counts:
//
//   - LP is admitted while the blocks in use stay within the LP limit.
//   - HP is admitted if there are free blocks; otherwise the newest queued LP is evicted until
//     there are. HP is refused only when no queued LP is left to evict.
//
// Evicted blocks are free at once, so nothing is evicted beyond what a checkpoint needs.
// When queued LP is not enough, LP being sent is evicted too: its send is cancelled, its
// requests are dropped instead of requeued, and the checkpoint waits (up to roomWait) for
// gRPC to hand back the blocks. Blocks gRPC still holds after a send are freed when gRPC
// returns the last of them.

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/mem"
)

const noEntry = -1

type entryState uint8

const (
	entryFree entryState = iota
	entryFilling
	entryQueued
	entryInflight
	entryDead // dropped while gRPC still holds some of its blocks
)

type entry struct {
	head    int32
	nblocks int32
	size    int32
	spans   int32
	hp      bool
	state   entryState
	prev    int32
	next    int32
	queued  int64 // nanoseconds since the store started, when first queued
}

type fifo struct {
	head, tail int32
	entries    int32
	blocks     int32
	spans      int64
}

type store struct {
	mu      sync.Mutex
	a       *arena
	entries []entry
	lent    []atomic.Int32 // per entry: blocks gRPC holds
	owner   []int32        // per block: the entry it belongs to
	slots   []int32        // free entry slots
	hp, lp  fifo
	lpLimit int32 // blocks in use up to which LP is admitted
	start   time.Time

	flights     []*flight  // sends holding LP, oldest first
	lpFlightBlk int32      // LP blocks in flights not yet evicted
	room        *sync.Cond // broadcast when blocks are freed
	roomWait    time.Duration

	wake chan struct{}

	// read without the lock by the pre-decode check and the metrics line
	used        atomic.Int32
	lpQueuedBlk atomic.Int32
	lpFlightA   atomic.Int32 // lpFlightBlk
	usedMax     atomic.Int32
	hpPause     atomic.Int64 // forward mode: no sends until this time (ns since start)
	lpPause     atomic.Int64 // forward mode: no LP sends until this time

	c counters
}

type counters struct {
	hpAdmitted, lpAdmitted         atomic.Int64 // spans
	hpRefused, lpRefused           atomic.Int64 // spans refused after reading the request
	hpRefusedReq, lpRefusedReq     atomic.Int64 // requests refused before reading them
	hpByEviction                   atomic.Int64 // spans admitted by evicting LP
	lpEvicted, lpEvictedReq        atomic.Int64
	hpSent, lpSent                 atomic.Int64
	hpRequeued, lpRequeued         atomic.Int64
	hpFailed, lpFailed             atomic.Int64 // dropped after a permanent downstream error
	malformed                      atomic.Int64 // requests
	sends, sendErrors              atomic.Int64 // forward RPCs / pipeline batches
	hpQueuedSpans, lpQueuedSpans   atomic.Int64
	hpQueuedBlocks, lpQueuedBlocks atomic.Int64
}

func newStore(a *arena, lpLimitPct float64) *store {
	n := a.nblocks
	s := &store{
		a: a, entries: make([]entry, n), lent: make([]atomic.Int32, n), owner: make([]int32, n),
		slots: make([]int32, n), lpLimit: int32(float64(n) * lpLimitPct / 100),
		start: time.Now(), wake: make(chan struct{}, 1),
	}
	for i := range s.slots {
		s.slots[i] = n - 1 - int32(i)
	}
	s.hp = fifo{head: noEntry, tail: noEntry}
	s.lp = fifo{head: noEntry, tail: noEntry}
	s.room = sync.NewCond(&s.mu)
	s.roomWait = 500 * time.Millisecond
	return s
}

// flight is a send that holds LP. A checkpoint short of room can evict it: the send is
// cancelled and its LP dropped when it returns, unless it was delivered meanwhile.
type flight struct {
	lpBlocks int32
	cancel   context.CancelFunc
	evicted  bool
}

func (s *store) now() int64 { return int64(time.Since(s.start)) }

func (s *store) notify() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// refuseLPEarly and refuseHPEarly are the pre-decode checks, from the gauges alone.
func (s *store) refuseLPEarly() bool { return s.used.Load() >= s.lpLimit }

func (s *store) refuseHPEarly() bool {
	return s.used.Load() >= s.a.nblocks && s.lpQueuedBlk.Load() == 0 && s.lpFlightA.Load() == 0
}

// reserve admits a request of size bytes and spans spans, or says why not. On admission it
// returns an entry in the filling state, holding enough blocks for the request.
func (s *store) reserve(hp bool, size, spans int) (id, head int32, code codes.Code, msg string) {
	need := s.a.blocksFor(size)
	if need > s.a.nblocks {
		s.refused(hp, spans)
		return noEntry, noBlock, codes.ResourceExhausted, fmt.Sprintf("request of %d bytes is larger than the arena", size)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	used := s.a.nblocks - s.a.free
	if !hp {
		if used+need > s.lpLimit {
			s.refused(false, spans)
			return noEntry, noBlock, codes.ResourceExhausted, "priorityarena: LP limit reached"
		}
	} else if s.a.free < need {
		if s.a.free+s.lp.blocks+s.lpFlightBlk < need { // evicting all LP would not make room: keep it
			s.refused(true, spans)
			return noEntry, noBlock, codes.Unavailable, "priorityarena: full of checkpoints"
		}
		if !s.evictFor(need) {
			s.refused(true, spans)
			return noEntry, noBlock, codes.Unavailable, "priorityarena: full of checkpoints"
		}
		s.c.hpByEviction.Add(int64(spans))
	}
	id = s.slots[len(s.slots)-1]
	s.slots = s.slots[:len(s.slots)-1]
	head = s.a.alloc(need)
	for blk := head; blk != noBlock; blk = s.a.next[blk] {
		s.owner[blk] = id
	}
	s.entries[id] = entry{head: head, nblocks: need, size: int32(size), spans: int32(spans), hp: hp,
		state: entryFilling, prev: noEntry, next: noEntry}
	s.setUsed()
	return id, head, codes.OK, ""
}

func (s *store) refused(hp bool, spans int) {
	if hp {
		s.c.hpRefused.Add(int64(spans))
	} else {
		s.c.lpRefused.Add(int64(spans))
	}
}

// commit queues a filled entry.
func (s *store) commit(id int32) {
	s.mu.Lock()
	e := &s.entries[id]
	e.state, e.queued = entryQueued, s.now()
	if e.hp {
		s.pushBack(&s.hp, id)
		s.c.hpAdmitted.Add(int64(e.spans))
	} else {
		s.pushBack(&s.lp, id)
		s.c.lpAdmitted.Add(int64(e.spans))
	}
	s.mu.Unlock()
	s.notify()
}

// evictFor evicts LP, newest first, until need blocks are free: queued LP, then LP in
// flight, whose blocks come back once its cancelled send returns. It waits up to roomWait
// for them and reports whether the room is there. Caller holds the lock; the wait releases it.
func (s *store) evictFor(need int32) bool {
	pending := int32(0) // evicted blocks gRPC still holds
	for s.a.free < need && s.lp.tail != noEntry {
		id := s.lp.tail
		if s.lent[id].Load() > 0 {
			pending += s.entries[id].nblocks
		}
		s.evictNewestLP()
	}
	var cancels []context.CancelFunc
	for i := len(s.flights) - 1; i >= 0 && s.a.free+pending < need; i-- {
		f := s.flights[i]
		if f.evicted {
			continue
		}
		f.evicted = true
		s.lpFlightBlk -= f.lpBlocks
		pending += f.lpBlocks
		if f.cancel != nil {
			cancels = append(cancels, f.cancel)
		}
	}
	s.setUsed()
	if s.a.free >= need {
		return true
	}
	s.mu.Unlock() // cancelling may run gRPC callbacks that return blocks
	for _, c := range cancels {
		c()
	}
	s.mu.Lock()
	if s.roomWait <= 0 {
		return s.a.free >= need
	}
	deadline := time.Now().Add(s.roomWait)
	t := time.AfterFunc(s.roomWait, func() { s.mu.Lock(); s.room.Broadcast(); s.mu.Unlock() })
	defer t.Stop()
	for s.a.free < need && time.Now().Before(deadline) {
		s.room.Wait()
	}
	return s.a.free >= need
}

// evictNewestLP drops the newest queued LP entry. Caller holds the lock.
func (s *store) evictNewestLP() {
	id := s.lp.tail
	s.unlink(&s.lp, id)
	s.c.lpEvicted.Add(int64(s.entries[id].spans))
	s.c.lpEvictedReq.Add(1)
	s.drop(id)
}

// drop frees an entry that is out of the queues, or leaves it to the last block gRPC returns.
// Caller holds the lock.
func (s *store) drop(id int32) {
	if s.lent[id].Load() > 0 {
		s.entries[id].state = entryDead
		return
	}
	s.free(id)
}

func (s *store) free(id int32) {
	e := &s.entries[id]
	s.a.release(e.head, e.nblocks)
	*e = entry{prev: noEntry, next: noEntry}
	s.slots = append(s.slots, id)
	s.setUsed()
	s.room.Broadcast()
}

func (s *store) setUsed() {
	used := s.a.nblocks - s.a.free
	s.used.Store(used)
	if used > s.usedMax.Load() {
		s.usedMax.Store(used)
	}
	s.lpQueuedBlk.Store(s.lp.blocks)
	s.lpFlightA.Store(s.lpFlightBlk)
	s.c.hpQueuedSpans.Store(s.hp.spans)
	s.c.lpQueuedSpans.Store(s.lp.spans)
	s.c.hpQueuedBlocks.Store(int64(s.hp.blocks))
	s.c.lpQueuedBlocks.Store(int64(s.lp.blocks))
}

func (s *store) pushBack(f *fifo, id int32) {
	e := &s.entries[id]
	e.prev, e.next = f.tail, noEntry
	if f.tail != noEntry {
		s.entries[f.tail].next = id
	} else {
		f.head = id
	}
	f.tail = id
	s.counted(f, id, 1)
}

func (s *store) pushFront(f *fifo, id int32) {
	e := &s.entries[id]
	e.prev, e.next = noEntry, f.head
	if f.head != noEntry {
		s.entries[f.head].prev = id
	} else {
		f.tail = id
	}
	f.head = id
	s.counted(f, id, 1)
}

func (s *store) unlink(f *fifo, id int32) {
	e := &s.entries[id]
	if e.prev != noEntry {
		s.entries[e.prev].next = e.next
	} else {
		f.head = e.next
	}
	if e.next != noEntry {
		s.entries[e.next].prev = e.prev
	} else {
		f.tail = e.prev
	}
	e.prev, e.next = noEntry, noEntry
	s.counted(f, id, -1)
}

func (s *store) counted(f *fifo, id int32, sign int32) {
	e := &s.entries[id]
	f.entries += sign
	f.blocks += sign * e.nblocks
	f.spans += int64(sign) * int64(e.spans)
	s.setUsed()
}

// batch is a set of entries taken for one send.
type batch struct {
	ids   []int32
	hp    bool // forward mode: the class of every entry; pipeline mode: it holds some HP
	spans int
	size  int
	f     *flight // when the batch holds LP
}

type takeLimits struct {
	maxSpans int
	maxBytes int
	linger   time.Duration
	mixed    bool // pipeline mode: HP first, then LP, in one batch
}

// take fills b with queued entries, HP first. It returns false and how long to wait when
// nothing is ready: the queues are empty, sends are paused, or the oldest entry is younger
// than linger and the queue is below the batch limits.
func (s *store) take(b *batch, lim takeLimits) (bool, time.Duration) {
	b.ids, b.spans, b.size, b.f = b.ids[:0], 0, 0, nil
	s.mu.Lock()
	defer s.mu.Unlock()
	defer s.fly(b)
	now := s.now()
	const idle = time.Second
	if p := s.hpPause.Load(); now < p {
		return false, time.Duration(p - now)
	}
	if lim.mixed {
		if s.hp.head == noEntry && s.lp.head == noEntry {
			return false, idle
		}
		if wait := s.lingerWait(now, lim, s.hp.spans+s.lp.spans, s.oldest()); wait > 0 {
			return false, wait
		}
		b.hp = s.hp.head != noEntry
		s.fill(b, &s.hp, lim)
		s.fill(b, &s.lp, lim)
		return true, 0
	}
	if s.hp.head != noEntry { // LP is never sent while a checkpoint waits
		if wait := s.lingerWait(now, lim, s.hp.spans, s.entries[s.hp.head].queued); wait > 0 {
			return false, wait
		}
		b.hp = true
		s.fill(b, &s.hp, lim)
		return true, 0
	}
	if s.lp.head == noEntry {
		return false, idle
	}
	if p := s.lpPause.Load(); now < p {
		return false, time.Duration(p - now)
	}
	if wait := s.lingerWait(now, lim, s.lp.spans, s.entries[s.lp.head].queued); wait > 0 {
		return false, wait
	}
	b.hp = false
	s.fill(b, &s.lp, lim)
	return true, 0
}

// fly records a taken batch's LP as in flight. Caller holds the lock.
func (s *store) fly(b *batch) {
	n := int32(0)
	for _, id := range b.ids {
		if !s.entries[id].hp {
			n += s.entries[id].nblocks
		}
	}
	if n == 0 {
		return
	}
	b.f = &flight{lpBlocks: n}
	s.flights = append(s.flights, b.f)
	s.lpFlightBlk += n
	s.setUsed()
}

// arm gives a batch's flight the cancel of its send; if the flight was evicted already, the
// send is cancelled at once.
func (s *store) arm(b *batch, cancel context.CancelFunc) {
	if b.f == nil {
		return
	}
	s.mu.Lock()
	evicted := b.f.evicted
	b.f.cancel = cancel
	s.mu.Unlock()
	if evicted {
		cancel()
	}
}

func (s *store) oldest() int64 {
	switch {
	case s.hp.head == noEntry:
		return s.entries[s.lp.head].queued
	case s.lp.head == noEntry:
		return s.entries[s.hp.head].queued
	default:
		return min(s.entries[s.hp.head].queued, s.entries[s.lp.head].queued)
	}
}

func (s *store) lingerWait(now int64, lim takeLimits, spans int64, oldest int64) time.Duration {
	if lim.linger <= 0 || spans >= int64(lim.maxSpans) {
		return 0
	}
	return lim.linger - time.Duration(now-oldest)
}

// fill moves entries from the front of f into b while the batch stays within the limits; the
// first entry always goes in.
func (s *store) fill(b *batch, f *fifo, lim takeLimits) {
	for f.head != noEntry {
		e := &s.entries[f.head]
		if len(b.ids) > 0 && (b.spans+int(e.spans) > lim.maxSpans || b.size+int(e.size) > lim.maxBytes) {
			return
		}
		id := f.head
		s.unlink(f, id)
		e.state = entryInflight
		b.ids = append(b.ids, id)
		b.spans += int(e.spans)
		b.size += int(e.size)
	}
}

type outcome int

const (
	sent    outcome = iota // delivered: free the entries
	requeue                // not delivered, try again: back to the front of their queues
	failed                 // not delivered, never will be: drop
)

// finish settles a batch taken by take.
func (s *store) finish(b *batch, o outcome) {
	s.mu.Lock()
	evicted := false
	if f := b.f; f != nil {
		for i, g := range s.flights {
			if g == f {
				s.flights = append(s.flights[:i], s.flights[i+1:]...)
				break
			}
		}
		if !f.evicted {
			s.lpFlightBlk -= f.lpBlocks
		}
		evicted, b.f = f.evicted, nil
		s.setUsed()
	}
	for i := len(b.ids) - 1; i >= 0; i-- { // backwards, so a requeued batch keeps its order
		id := b.ids[i]
		e := &s.entries[id]
		if evicted && !e.hp && o != sent { // evicted for a checkpoint while in flight
			s.c.lpEvicted.Add(int64(e.spans))
			s.c.lpEvictedReq.Add(1)
			s.drop(id)
			continue
		}
		switch o {
		case requeue:
			e.state = entryQueued
			if e.hp {
				s.pushFront(&s.hp, id)
				s.c.hpRequeued.Add(int64(e.spans))
			} else {
				s.pushFront(&s.lp, id)
				s.c.lpRequeued.Add(int64(e.spans))
			}
			continue
		case sent:
			if e.hp {
				s.c.hpSent.Add(int64(e.spans))
			} else {
				s.c.lpSent.Add(int64(e.spans))
			}
		case failed:
			if e.hp {
				s.c.hpFailed.Add(int64(e.spans))
			} else {
				s.c.lpFailed.Add(int64(e.spans))
			}
		}
		s.drop(id)
	}
	s.mu.Unlock()
	if o == requeue {
		s.notify()
	}
}

// lend returns the batch's blocks as gRPC buffers, without copying. Each block counts as held
// until gRPC hands it back through Put. The entries are in flight, so nothing else touches them.
func (s *store) lend(b *batch) mem.BufferSlice {
	n := 0
	for _, id := range b.ids {
		n += int(s.entries[id].nblocks)
	}
	out := make(mem.BufferSlice, 0, n)
	for _, id := range b.ids {
		e := &s.entries[id]
		s.lent[id].Add(e.nblocks)
		for blk := e.head; blk != noBlock; blk = s.a.next[blk] {
			out = append(out, mem.NewBuffer(&s.a.slices[blk], s))
		}
	}
	return out
}

// Get is never called: gRPC asks a pool for buffers only when it allocates them itself.
func (s *store) Get(n int) *[]byte {
	b := make([]byte, n)
	return &b
}

// Put takes back a block gRPC held. When an entry's last block comes back and the entry was
// dropped meanwhile, its blocks are freed.
func (s *store) Put(p *[]byte) {
	blk := int32((uintptr(unsafe.Pointer(p)) - uintptr(unsafe.Pointer(&s.a.slices[0]))) / unsafe.Sizeof(s.a.slices[0]))
	if blk < 0 || blk >= s.a.nblocks || p != &s.a.slices[blk] {
		panic("priorityarena: gRPC returned a buffer that is not an arena block")
	}
	id := s.owner[blk]
	if s.lent[id].Add(-1) > 0 {
		return
	}
	s.mu.Lock()
	if s.entries[id].state == entryDead && s.lent[id].Load() == 0 {
		s.free(id)
	}
	s.mu.Unlock()
}

// chunks appends the views of the batch's bytes, in order, to out.
func (s *store) chunks(b *batch, out [][]byte) [][]byte {
	for _, id := range b.ids {
		out = s.a.chunks(s.entries[id].head, out)
	}
	return out
}

// check verifies the accounting: every block is free or belongs to exactly one live entry, and
// the queues' totals match their entries. Tests call it; it takes the lock.
func (s *store) check() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	seen := make([]bool, s.a.nblocks)
	nfree := int32(0)
	for blk := s.a.freeHead; blk != noBlock; blk = s.a.next[blk] {
		if seen[blk] {
			return fmt.Errorf("block %d twice on the free list", blk)
		}
		seen[blk] = true
		nfree++
	}
	if nfree != s.a.free {
		return fmt.Errorf("free list holds %d blocks, counter says %d", nfree, s.a.free)
	}
	live := int32(0)
	for id := range s.entries {
		e := &s.entries[id]
		if e.state == entryFree {
			continue
		}
		live++
		n := int32(0)
		for blk := e.head; blk != noBlock; blk = s.a.next[blk] {
			if seen[blk] {
				return fmt.Errorf("block %d of entry %d is also free or in another entry", blk, id)
			}
			if s.owner[blk] != int32(id) {
				return fmt.Errorf("block %d owned by %d, found in entry %d", blk, s.owner[blk], id)
			}
			seen[blk] = true
			n++
		}
		if n != e.nblocks || int(e.nblocks) != int(s.a.blocksFor(int(e.size))) {
			return fmt.Errorf("entry %d: chain of %d blocks, %d recorded, %d bytes", id, n, e.nblocks, e.size)
		}
	}
	for blk, ok := range seen {
		if !ok {
			return fmt.Errorf("block %d leaked", blk)
		}
	}
	if int(live)+len(s.slots) != len(s.entries) {
		return fmt.Errorf("%d live entries and %d free slots, %d slots in all", live, len(s.slots), len(s.entries))
	}
	flying := int32(0)
	for _, f := range s.flights {
		if !f.evicted {
			flying += f.lpBlocks
		}
	}
	if flying != s.lpFlightBlk {
		return fmt.Errorf("flights hold %d LP blocks, counter says %d", flying, s.lpFlightBlk)
	}
	for _, q := range []struct {
		name string
		f    *fifo
		hp   bool
	}{{"hp", &s.hp, true}, {"lp", &s.lp, false}} {
		var n, blocks int32
		var spans int64
		prev := int32(noEntry)
		for id := q.f.head; id != noEntry; id = s.entries[id].next {
			e := &s.entries[id]
			if e.state != entryQueued || e.hp != q.hp || e.prev != prev {
				return fmt.Errorf("%s queue: entry %d state %d hp %v prev %d (want %d)", q.name, id, e.state, e.hp, e.prev, prev)
			}
			prev = id
			n++
			blocks += e.nblocks
			spans += int64(e.spans)
		}
		if prev != q.f.tail || n != q.f.entries || blocks != q.f.blocks || spans != q.f.spans {
			return fmt.Errorf("%s queue totals: %d entries %d blocks %d spans, recorded %d %d %d",
				q.name, n, blocks, spans, q.f.entries, q.f.blocks, q.f.spans)
		}
	}
	return nil
}
