// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package priorityarenareceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/priorityprocessor/priorityarenareceiver"

// Tomislav-RetCtx: the two ways out of the arena. The forwarder sends queued requests on as
// bytes, one class per send, checkpoints first. The pipeliner decodes a batch (checkpoints
// first, then LP) and calls the pipeline, which should not queue it again. Either way a batch
// stays in the arena until it is delivered, and goes back to the front of its queue if it is
// not.

import (
	"context"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

var (
	hpHeader = metadata.Pairs(priorityKey, "hp")
	lpHeader = metadata.Pairs(priorityKey, "lp")
)

// backoff is a pause that doubles on each failure and resets on success.
type backoff struct {
	init, max time.Duration
	cur       atomic.Int64
}

func (b *backoff) next() time.Duration {
	d := time.Duration(b.cur.Load())
	if d == 0 {
		d = b.init
	} else {
		d = min(2*d, b.max)
	}
	b.cur.Store(int64(d))
	return d
}

func (b *backoff) reset() { b.cur.Store(0) }

// waitFor sleeps for d or until the store has new work; false once ctx is done.
func waitFor(ctx context.Context, s *store, t *time.Timer, d time.Duration) bool {
	t.Reset(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-s.wake:
		return true
	case <-t.C:
		return true
	}
}

type forwarder struct {
	s      *store
	cc     *grpc.ClientConn
	cfg    ForwardConfig
	lim    takeLimits
	logger *zap.Logger
	hpBack backoff
	lpBack backoff
}

func (f *forwarder) run(ctx context.Context) {
	t := time.NewTimer(time.Hour)
	var b batch
	for ctx.Err() == nil {
		ok, wait := f.s.take(&b, f.lim)
		if !ok {
			if !waitFor(ctx, f.s, t, wait) {
				return
			}
			continue
		}
		f.s.notify() // another worker may find more
		f.s.finish(&b, f.send(ctx, &b))
	}
}

func (f *forwarder) send(ctx context.Context, b *batch) outcome {
	md := lpHeader
	if b.hp {
		md = hpHeader
	}
	cctx, cancel := context.WithTimeout(metadata.NewOutgoingContext(ctx, md), f.cfg.Timeout)
	f.s.arm(b, cancel)
	err := f.cc.Invoke(cctx, exportMethod, &outbound{s: f.s, b: b}, &exportResponse{}, grpc.ForceCodecV2(arenaCodec{}))
	cancel()
	f.s.c.sends.Add(1)
	back, pause := &f.lpBack, &f.s.lpPause
	if b.hp {
		back, pause = &f.hpBack, &f.s.hpPause
	}
	if err == nil {
		back.reset()
		return sent
	}
	f.s.c.sendErrors.Add(1)
	switch status.Code(err) {
	case codes.Unavailable, codes.ResourceExhausted, codes.DeadlineExceeded, codes.Aborted, codes.Canceled, codes.Internal, codes.Unknown:
		pause.Store(f.s.now() + int64(back.next()))
		return requeue
	}
	f.logger.Warn("priorityarena: send failed permanently, dropping",
		zap.Bool("hp", b.hp), zap.Int("spans", b.spans), zap.Error(err))
	return failed
}

// gather copies the batch's requests into one buffer; concatenated requests are one request.
func (p *pipeliner) gather(b *batch, scratch []byte, chunks *[][]byte) []byte {
	*chunks = p.s.chunks(b, (*chunks)[:0])
	for _, c := range *chunks {
		scratch = append(scratch, c...)
	}
	return scratch
}

// dropUndecodable moves every request of b that does not decode on its own into bad, and
// drops them.
func (p *pipeliner) dropUndecodable(b, bad *batch, chunks *[][]byte) {
	var u ptrace.ProtoUnmarshaler
	keep := b.ids[:0]
	bad.ids, bad.spans, bad.size = bad.ids[:0], 0, 0
	for _, id := range b.ids {
		one := batch{ids: []int32{id}}
		if _, err := u.UnmarshalTraces(p.gather(&one, nil, chunks)); err != nil {
			e := &p.s.entries[id]
			bad.ids = append(bad.ids, id)
			bad.spans += int(e.spans)
			bad.size += int(e.size)
			p.logger.Warn("priorityarena: request does not decode, dropping", zap.Int("spans", int(e.spans)), zap.Error(err))
			continue
		}
		keep = append(keep, id)
	}
	b.ids = keep
	b.spans -= bad.spans
	b.size -= bad.size
	if len(bad.ids) > 0 {
		p.s.c.malformed.Add(int64(len(bad.ids)))
		p.s.finish(bad, failed)
	}
}

type pipeliner struct {
	s      *store
	next   consumer.Traces
	lim    takeLimits
	logger *zap.Logger
	back   backoff
}

func (p *pipeliner) run(ctx context.Context) {
	t := time.NewTimer(time.Hour)
	var (
		b, bad  batch
		chunks  [][]byte
		scratch []byte
		u       ptrace.ProtoUnmarshaler
	)
	for ctx.Err() == nil {
		ok, wait := p.s.take(&b, p.lim)
		if !ok {
			if !waitFor(ctx, p.s, t, wait) {
				return
			}
			continue
		}
		p.s.notify()
		scratch = p.gather(&b, scratch[:0], &chunks)
		td, err := u.UnmarshalTraces(scratch)
		if err != nil {
			// The scan at admission checks structure, not span contents. Find the requests that
			// do not decode, drop them alone, and send the rest.
			p.dropUndecodable(&b, &bad, &chunks)
			if len(b.ids) == 0 {
				continue
			}
			scratch = p.gather(&b, scratch[:0], &chunks)
			if td, err = u.UnmarshalTraces(scratch); err != nil {
				p.s.finish(&b, failed)
				continue
			}
		}
		cctx, cancel := context.WithCancel(ctx)
		p.s.arm(&b, cancel)
		err = p.next.ConsumeTraces(cctx, td)
		cancel()
		p.s.c.sends.Add(1)
		switch {
		case err == nil:
			p.back.reset()
			p.s.finish(&b, sent)
		case consumererror.IsPermanent(err):
			p.s.c.sendErrors.Add(1)
			p.logger.Warn("priorityarena: pipeline refused batch permanently, dropping", zap.Int("spans", b.spans), zap.Error(err))
			p.s.finish(&b, failed)
		default:
			p.s.c.sendErrors.Add(1)
			p.s.hpPause.Store(p.s.now() + int64(p.back.next()))
			p.s.finish(&b, requeue)
		}
	}
}
