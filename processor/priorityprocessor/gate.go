// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package priorityprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/priorityprocessor"

// Tomislav-RetCtx: process-wide admission gate, published by the priority processor every
// tick and consulted by the priorityotlp receiver BEFORE a request is read or decoded, from
// its gRPC headers alone. A request the processor would refuse anyway is then refused
// without paying to receive and decode it -- which is what returns CPU to checkpoints on a
// CPU-bound collector. One pipeline per collector in our deployments, so one gate.

import (
	"sync/atomic"

	"google.golang.org/grpc/codes"
)

// GateDecision is the admission decision for one request.
type GateDecision int

const (
	GateAdmit     GateDecision = iota // decode and hand to the pipeline
	GateRefuseLP                      // refuse: LP request while shedding LP
	GateRefuseAll                     // refuse: refuse-all state (soft/hard)
)

type admissionGate struct {
	state     atomic.Int32 // pressureState
	lpCode    atomic.Int32 // codes.Code for LP refusals
	active    atomic.Bool  // a priority processor has published at least once
	refusedLP atomic.Int64 // requests refused pre-decode, by class
	refusedHP atomic.Int64
	// Tomislav-RetCtx: LP requests refused because a batch was waiting in the queue stage
	refusedLPBacklog atomic.Int64
	// Tomislav-RetCtx: checkpoints-waiting mark from upstream (HPWaitingKey): set by the receiver
	// for every marked request; each controller tick moves it to upstreamActive, so the mark holds
	// for the current and the previous tick.
	upstreamMark      atomic.Bool
	upstreamActive    atomic.Bool
	refusedLPUpstream atomic.Int64

	// Tomislav-RetCtx: graded LP admission (Config.CPUTarget). lpAdmitMicro is the admitted LP
	// fraction x 1e6; credit spreads admissions evenly (deterministic, no RNG).
	graded       atomic.Bool
	lpAdmitMicro atomic.Int64
	credit       atomic.Int64
}

var gate admissionGate

func init() { gate.lpCode.Store(int32(codes.Unavailable)) }

func (g *admissionGate) publish(st pressureState, lpCode codes.Code) {
	g.upstreamActive.Store(g.upstreamMark.Swap(false))
	g.state.Store(int32(st))
	g.lpCode.Store(int32(lpCode))
	g.active.Store(true)
}

// Decide returns the pre-decode decision for a request whose bridges-priority header marks
// it LP (isLP) or not. Before any priority processor has published, everything is admitted.
func Decide(isLP bool) (GateDecision, codes.Code) {
	if !gate.active.Load() {
		return GateAdmit, codes.OK
	}
	switch pressureState(gate.state.Load()) {
	case stateSoft, stateHard:
		if isLP {
			gate.refusedLP.Add(1)
			return GateRefuseAll, codes.Code(gate.lpCode.Load())
		}
		// Tomislav-RetCtx: a checkpoint goes on to decode (and to the processor, which evicts
		// queued LP to admit it) while the queue stage holds LP; refused here otherwise.
		if q := activeQueue.Load(); q != nil && q.hasLP() {
			return GateAdmit, codes.OK
		}
		gate.refusedHP.Add(1)
		return GateRefuseAll, codes.Unavailable
	case stateShedLP:
		if isLP {
			gate.refusedLP.Add(1)
			return GateRefuseLP, codes.Code(gate.lpCode.Load())
		}
	}
	// Tomislav-RetCtx: LP is refused before decode while this hop is not keeping up (a batch
	// waits in its queue stage) or while an upstream hop reports checkpoints waiting.
	if isLP {
		if q := activeQueue.Load(); q != nil && q.hasWaiting() {
			gate.refusedLP.Add(1)
			gate.refusedLPBacklog.Add(1)
			return GateRefuseLP, codes.Code(gate.lpCode.Load())
		}
		if upstreamHPWaiting() {
			gate.refusedLP.Add(1)
			gate.refusedLPUpstream.Add(1)
			return GateRefuseLP, codes.Code(gate.lpCode.Load())
		}
	}
	// Graded LP admission: admit exactly the published fraction of LP requests.
	if isLP && gate.graded.Load() {
		f := gate.lpAdmitMicro.Load()
		if f < fracScale {
			if c := gate.credit.Add(f); c >= fracScale {
				gate.credit.Add(-fracScale)
			} else {
				gate.refusedLP.Add(1)
				return GateRefuseLP, codes.Code(gate.lpCode.Load())
			}
		}
	}
	return GateAdmit, codes.OK
}

// HPWaitingKey is the header an upstream hop sets ("1") on its sends while checkpoints wait in
// its queue stage. Tomislav-RetCtx.
const HPWaitingKey = "bridges-hp-waiting"

// MarkUpstreamHPWaiting records that a request carrying HPWaitingKey=1 arrived (called by the
// priorityotlp receiver before the admission decision).
func MarkUpstreamHPWaiting(values []string) {
	for _, v := range values {
		if v == "1" {
			gate.upstreamMark.Store(true)
			return
		}
	}
}

func upstreamHPWaiting() bool { return gate.upstreamMark.Load() || gate.upstreamActive.Load() }

// IsLPHeader reports whether a bridges-priority header value marks an LP request.
func IsLPHeader(values []string) bool {
	for _, v := range values {
		if len(v) == 2 && (v[0] == 'l' || v[0] == 'L') && (v[1] == 'p' || v[1] == 'P') {
			return true
		}
	}
	return false
}
