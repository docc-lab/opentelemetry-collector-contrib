// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Tomislav-RetCtx: tests for the backlog US-margin mode and the configurable
// LP refusal code.

package priorityprocessor

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const mib = 1 << 20

func TestComputeMarginModes(t *testing.T) {
	// arrival reproduces v18 exactly: phaseAmp + cp*sizeGain*aggression*commit*horizon.
	assert.InDelta(t, 10.0+1*1.5*2.5*100*1.0,
		computeMargin(MarginModeArrival, 10, 1, 1.5, 2.5, 100, 999, 1.0, 0.5), 1e-9)
	// backlog with a flat heap reserves only the swing and the reaction window.
	assert.InDelta(t, 10.0+100*0.5,
		computeMargin(MarginModeBacklog, 10, 1, 1.5, 2.5, 100, 0, 1.0, 0.5), 1e-9)
	// real growth adds the lever-weighted drift; negative growth is ignored.
	assert.InDelta(t, 10.0+1*1.5*2.5*40*1.0+100*0.5,
		computeMargin(MarginModeBacklog, 10, 1, 1.5, 2.5, 100, 40, 1.0, 0.5), 1e-9)
	assert.InDelta(t, 10.0+100*0.5,
		computeMargin(MarginModeBacklog, 10, 1, 1.5, 2.5, 100, -40, 1.0, 0.5), 1e-9)
}

func TestConfigValidateNewOptions(t *testing.T) {
	c := createDefaultConfig().(*Config)
	require.NoError(t, c.Validate())
	c.USMarginMode = MarginModeBacklog
	c.LPRefusalCode = LPRefusalResourceExhausted
	require.NoError(t, c.Validate())
	c.USMarginMode = "net"
	assert.ErrorIs(t, c.Validate(), ErrInvalidMarginMode)
	c.USMarginMode = MarginModeBacklog
	c.LPRefusalCode = "aborted"
	assert.ErrorIs(t, c.Validate(), ErrInvalidLPRefusalCode)
}

func testProcessor(t *testing.T, mode, lpCode string) *priorityProcessor {
	t.Helper()
	c := createDefaultConfig().(*Config)
	c.SoftPercentage, c.HardPercentage = 55, 80
	c.ForceGC = false
	c.USMarginMode, c.LPRefusalCode = mode, lpCode
	require.NoError(t, c.Validate())
	p, err := newProcessor(c, zap.NewNop(), nil)
	require.NoError(t, err)
	// the node-1 agent budget of the hotel runs: 256 MiB cgroup, 55/80
	p.totalMemory = 256 * mib
	p.softLimit = 55 * p.totalMemory / 100
	p.hardLimit = 80 * p.totalMemory / 100
	return p
}

// drive runs the controller for n ticks against a scripted heap. alloc(i)
// is the heap at tick i; each tick hpPerTick/lpPerTick spans arrive.
// Returns the per-tick states.
func drive(p *priorityProcessor, n int, hpPerTick, lpPerTick int64, alloc func(i int) uint64) []pressureState {
	clock := time.Unix(1_000_000, 0)
	p.now = func() time.Time { return clock }
	p.bytesPerHPSpan, p.bytesPerLPSpan = 166, 113 // measured on the hotel frontend agent
	var cur atomic.Uint64
	p.readMem = cur.Load
	states := make([]pressureState, n)
	for i := 0; i < n; i++ {
		cur.Store(alloc(i))
		atomic.AddInt64(&p.hpAdmitted, hpPerTick)
		atomic.AddInt64(&p.lpAdmitted, lpPerTick)
		p.tick()
		states[i] = pressureState(p.state.Load())
		clock = clock.Add(p.config.CheckInterval)
	}
	return states
}

// sawtooth: climbs from trough by step per tick for climb ticks, then GCs back
// to trough + drift*cycle (drift = trough growth per cycle).
func sawtooth(trough, step uint64, climb int, driftPerCycle uint64) func(int) uint64 {
	return func(i int) uint64 {
		cycle, phase := i/(climb+1), i%(climb+1)
		base := trough + uint64(cycle)*driftPerCycle
		if phase == climb { // GC tick
			return base + driftPerCycle
		}
		return base + uint64(phase)*step
	}
}

func count(states []pressureState, s pressureState) int {
	n := 0
	for _, x := range states {
		if x == s {
			n++
		}
	}
	return n
}

// A healthy pipeline at the PB-rev frontend-agent load (about 20k HP + 16k LP
// spans/s, heap swinging 40-70 MiB with flat troughs, soft at 141 MiB): v18's
// arrival margin sheds LP although the heap never approaches soft; backlog mode
// does not shed at all.
func TestBacklogModeDoesNotShedHealthyPipeline(t *testing.T) {
	heap := sawtooth(40*mib, 3*mib, 10, 0)
	arrival := drive(testProcessor(t, MarginModeArrival, LPRefusalUnavailable), 600, 2000, 1600, heap)
	backlog := drive(testProcessor(t, MarginModeBacklog, LPRefusalUnavailable), 600, 2000, 1600, heap)
	steady := func(s []pressureState) []pressureState { return s[200:] } // after warm-up of the envelopes
	t.Logf("shed-LP ticks over the last 400: arrival %d, backlog %d", count(steady(arrival), stateShedLP), count(steady(backlog), stateShedLP))
	assert.Greater(t, count(steady(arrival), stateShedLP), 0, "v18 arrival margin should shed LP here (the observed early shedding)")
	assert.Equal(t, 0, count(steady(backlog), stateShedLP), "backlog margin must not shed LP while troughs are flat")
	assert.Equal(t, 0, count(backlog, stateSoft)+count(backlog, stateHard))
}

// A building backlog (troughs rising 6 MiB per GC cycle, as when the gateway
// stops draining): backlog mode must start shedding LP before the heap reaches
// soft, i.e. all LP goes before any CP.
func TestBacklogModeShedsBeforeSoftWhenBacklogGrows(t *testing.T) {
	p := testProcessor(t, MarginModeBacklog, LPRefusalUnavailable)
	heap := sawtooth(40*mib, 3*mib, 10, 6*mib)
	states := drive(p, 400, 2000, 1600, heap)
	firstShed, firstSoft := -1, -1
	for i, s := range states {
		if s == stateShedLP && firstShed < 0 {
			firstShed = i
		}
		if (s == stateSoft || s == stateHard) && firstSoft < 0 {
			firstSoft = i
		}
	}
	t.Logf("first shed-LP tick %d (heap %.0f MiB), first refuse-all tick %d (heap %.0f MiB), shed-LP ticks before soft %d",
		firstShed, float64(heap(firstShed))/mib, firstSoft, float64(heap(firstSoft))/mib, count(states[:max(firstSoft, 0)], stateShedLP))
	require.GreaterOrEqual(t, firstShed, 0, "backlog growth must trigger LP shedding")
	require.GreaterOrEqual(t, firstSoft, 0, "the scripted heap eventually crosses soft")
	assert.Less(t, firstShed, firstSoft, "LP shedding must begin before refuse-all")
	assert.Greater(t, p.troughSlopeEMA, 0.0)
}

func lpContext() context.Context {
	return client.NewContext(context.Background(), client.Info{
		Metadata: client.NewMetadata(map[string][]string{MetadataPriorityKey: {"lp"}}),
	})
}

func traces(n int) ptrace.Traces {
	td := ptrace.NewTraces()
	ss := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty()
	for i := 0; i < n; i++ {
		ss.Spans().AppendEmpty()
	}
	return td
}

func TestLPRefusalCodes(t *testing.T) {
	for _, tc := range []struct {
		cfg  string
		want codes.Code
	}{{LPRefusalUnavailable, codes.Unavailable}, {LPRefusalResourceExhausted, codes.ResourceExhausted}} {
		p := testProcessor(t, MarginModeArrival, tc.cfg)
		for _, st := range []pressureState{stateShedLP, stateSoft, stateHard} {
			p.state.Store(int32(st))
			_, err := p.processTraces(lpContext(), traces(3))
			assert.Equal(t, tc.want, status.Code(err), "LP refusal code in %s", stateName(st))
		}
		// HP: admitted while shedding LP, always retryable when refused.
		p.state.Store(int32(stateShedLP))
		_, err := p.processTraces(context.Background(), traces(3))
		assert.NoError(t, err)
		p.state.Store(int32(stateSoft))
		_, err = p.processTraces(context.Background(), traces(3))
		assert.Equal(t, codes.Unavailable, status.Code(err))
	}
}

// The CPU signal sheds LP when CPU use over the 1s window reaches the threshold of the quota,
// with no memory pressure at all (flat 40 MiB heap), and releases when CPU use falls.
func TestCPUShedSignal(t *testing.T) {
	p := testProcessor(t, MarginModeBacklog, LPRefusalResourceExhausted)
	p.config.CPUShedThreshold = 0.9
	p.cpuQuota = 2.0 // a 2-CPU gateway
	var cpu float64
	var rate atomic.Value
	rate.Store(1.95) // cores in use
	p.readCPU = func() float64 { return cpu }
	flat := func(int) uint64 { return 40 * mib }
	p.now = func() time.Time { return time.Unix(0, 0) }
	var cur atomic.Uint64
	p.readMem = cur.Load
	shed := 0
	for i := 0; i < 60; i++ {
		cur.Store(flat(i))
		cpu += rate.Load().(float64) * p.config.CheckInterval.Seconds()
		atomic.AddInt64(&p.hpAdmitted, 2000)
		atomic.AddInt64(&p.lpAdmitted, 1600)
		p.tick()
		if i >= 20 && pressureState(p.state.Load()) == stateShedLP {
			shed++
		}
	}
	assert.Equal(t, 40, shed, "at 97.5%% of the quota every tick after warm-up must shed LP")
	d, code := Decide(true)
	assert.Equal(t, GateRefuseLP, d)
	assert.Equal(t, codes.ResourceExhausted, code)
	d, _ = Decide(false)
	assert.Equal(t, GateAdmit, d, "HP is admitted while CPU-shedding")
	rate.Store(1.0) // 50% of the quota
	for i := 0; i < 20; i++ {
		cpu += 1.0 * p.config.CheckInterval.Seconds()
		atomic.AddInt64(&p.hpAdmitted, 2000)
		p.tick()
	}
	assert.Equal(t, stateNoPressure, pressureState(p.state.Load()), "the signal must release once CPU use drops")
	SetGateForTesting("off", codes.Unavailable)
}

// Graded LP admission converges to the fraction that holds CPU at the target and the gate
// admits that share of LP requests. Model: CPU = 0.5 + 0.6*f cores on a 1-core quota, so the
// 0.95 target needs f = 0.75. Then load drops (CPU = 0.3 + 0.6*f) and f must recover to 1.
func TestGradedCPUAdmission(t *testing.T) {
	p := testProcessor(t, MarginModeBacklog, LPRefusalResourceExhausted)
	p.config.CPUTarget = 0.95
	p.cpuQuota = 1.0
	var cpu float64
	base := 0.5
	p.readCPU = func() float64 { return cpu }
	p.now = func() time.Time { return time.Unix(0, 0) }
	var cur atomic.Uint64
	cur.Store(40 * mib)
	p.readMem = cur.Load
	for i := 0; i < 400; i++ {
		cpu += (base + 0.6*p.lpAdmit) * p.config.CheckInterval.Seconds()
		atomic.AddInt64(&p.hpAdmitted, 2000)
		atomic.AddInt64(&p.lpAdmitted, 1600)
		p.tick()
	}
	assert.InDelta(t, 0.75, p.lpAdmit, 0.08, "admitted LP fraction must settle where CPU equals the target")
	assert.Equal(t, stateNoPressure, pressureState(p.state.Load()), "graded admission is not the all-or-nothing shed state")
	admitted := 0
	for i := 0; i < 1000; i++ {
		if d, _ := Decide(true); d == GateAdmit {
			admitted++
		}
	}
	assert.InDelta(t, 1000*p.lpAdmit, admitted, 20, "the gate admits the published share of LP requests")
	d, _ := Decide(false)
	assert.Equal(t, GateAdmit, d, "HP is never refused by graded admission")
	base = 0.3
	for i := 0; i < 300; i++ {
		cpu += (base + 0.6*p.lpAdmit) * p.config.CheckInterval.Seconds()
		atomic.AddInt64(&p.hpAdmitted, 2000)
		p.tick()
	}
	assert.InDelta(t, 1.0, p.lpAdmit, 1e-9, "with CPU to spare every LP request is admitted again")
	gate.graded.Store(false)
	SetGateForTesting("off", codes.Unavailable)
}
