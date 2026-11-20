// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package priorityarenareceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/priorityprocessor/priorityarenareceiver"

// Tomislav-RetCtx: the adaptive LP limit (checkpoint_reserve_percentage; user 2026-11-20: sized by the ratio between
// the sizes of checkpoint and LP spans). The top of the arena, the band, is for checkpoints alone: LP is refused once
// the blocks in use, of either class, reach its lower edge. The band is the configured reserve scaled by the size
// ratio r = checkpoint bytes per span / LP bytes per span, and never smaller than the reserve: reserve x max(1, r).
// It then holds about as many checkpoint spans as the reserve would hold LP spans, so LP refused at the limit and
// checkpoints admitted into the band trade about span for span. Once the band is full, checkpoints evict LP as with
// a fixed limit.
//
// Spans and bytes are those of the requests admitted over the last size_window seconds that admitted something.
// Refusals do not bias a size per span, so seconds with refusals count too. Moving the limit evicts nothing: LP
// above a lowered limit drains, and only new LP is refused.

import (
	"context"
	"math"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

// adaptStep is the period at which the adapter reads the counters; tests shorten it.
var adaptStep = time.Second

type lpAdapter struct {
	s      *store
	band   float64 // the reserve, in blocks: the band at a size ratio of 1 or less
	logger *zap.Logger

	// spans and bytes admitted per step and class, over the last len(win) steps that admitted something
	win    []sizeStep
	next   int
	filled int
	sum    sizeStep
	last   sizeStep

	ratio atomic.Uint64 // float64 bits of the size ratio in force; NaN until known
}

type sizeStep struct{ hpSpans, hpBytes, lpSpans, lpBytes int64 }

func (a sizeStep) minus(b sizeStep) sizeStep {
	return sizeStep{a.hpSpans - b.hpSpans, a.hpBytes - b.hpBytes, a.lpSpans - b.lpSpans, a.lpBytes - b.lpBytes}
}

func (a sizeStep) plus(b sizeStep) sizeStep {
	return sizeStep{a.hpSpans + b.hpSpans, a.hpBytes + b.hpBytes, a.lpSpans + b.lpSpans, a.lpBytes + b.lpBytes}
}

func (s *store) sizeReading() sizeStep {
	c := &s.c
	return sizeStep{c.hpAdmitted.Load(), c.hpAdmittedB.Load(), c.lpAdmitted.Load(), c.lpAdmittedB.Load()}
}

// newLPAdapter keeps band blocks (the reserve) for checkpoints at a size ratio of 1 and averages the sizes over window
// steps. The store's LP limit stays as it is until window steps have admitted spans of both classes.
func newLPAdapter(s *store, band float64, window int, logger *zap.Logger) *lpAdapter {
	if !(band >= 0 && band <= float64(s.a.nblocks)) || window < 1 {
		broken("adapter with a band of %v of %d blocks over %d steps", band, s.a.nblocks, window)
	}
	a := &lpAdapter{s: s, band: band, logger: logger, win: make([]sizeStep, window), last: s.sizeReading()}
	a.ratio.Store(math.Float64bits(math.NaN()))
	return a
}

func (a *lpAdapter) run(ctx context.Context) {
	t := time.NewTicker(adaptStep)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			a.step()
		}
	}
}

// step takes in the spans and bytes admitted since the last step and sets the LP limit.
func (a *lpAdapter) step() {
	now := a.s.sizeReading()
	d := now.minus(a.last)
	a.last = now
	if d.hpSpans+d.lpSpans == 0 {
		return
	}
	a.sum = a.sum.plus(d).minus(a.win[a.next])
	a.win[a.next] = d
	a.next = (a.next + 1) % len(a.win)
	if a.filled < len(a.win) {
		a.filled++
	}
	if a.filled < len(a.win) || a.sum.hpSpans == 0 || a.sum.lpSpans == 0 {
		return
	}
	r := (float64(a.sum.hpBytes) / float64(a.sum.hpSpans)) / (float64(a.sum.lpBytes) / float64(a.sum.lpSpans))
	if !(r > 0) || math.IsInf(r, 0) { // only with no LP bytes in the window; keep the limit
		return
	}
	limit := int32(max(0, float64(a.s.a.nblocks)-a.band*max(1, r)))
	if math.IsNaN(a.ratioNow()) {
		a.logger.Info("priorityarena LP limit now follows the checkpoint/LP span size ratio",
			zap.Float64("size_ratio", r), zap.Int32("lp_limit_blocks", limit),
			zap.Int32("lp_limit_blocks_before", a.s.lpLimit.Load()))
	}
	a.ratio.Store(math.Float64bits(r))
	a.s.lpLimit.Store(limit)
}

// ratioNow is the size ratio the LP limit was last set from, or NaN before the first window.
func (a *lpAdapter) ratioNow() float64 { return math.Float64frombits(a.ratio.Load()) }
