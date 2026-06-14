// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package priorityprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/priorityprocessor"

import (
	"context"
	"fmt"
	"math"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// MetadataPriorityKey is the gRPC metadata key the SB SDK stamps onto
// every UploadTraces call: "lp" for non-checkpoint batches, anything
// else (incl. missing) is treated as HP. Requires include_metadata:true
// on the receiver.
const MetadataPriorityKey = "bridges-priority"

// ---------------------------------------------------------------------
// Internal constants. These are NOT tuning gains — they are physical
// conversions, time constants tied to the GC period / pipeline drain
// time, or noise filters. The controller's behavior is set by the
// analytic law plus the soft/hard thresholds; these just convert units
// and filter noise, and none needs per-deployment tuning. (Behavioral
// knobs are soft_percentage and hard_percentage in Config.)
// ---------------------------------------------------------------------
const (
	// memoryOverheadFactor converts proto wire bytes → live heap
	// footprint (pipeline copies, queue buffers, Go object overhead).
	// The control loop re-evaluates every tick and self-corrects for
	// modest errors here, so its exact value is not critical.
	memoryOverheadFactor = 4.0

	// horizonTicks is the response/prediction horizon: alloc is projected
	// velocity×horizon ahead, and the overshoot is spread over the same
	// horizon when sizing the shed. ~1s at the 100ms default cadence,
	// matched to typical pipeline drain time.
	horizonTicks = 10

	// peakWindowTicks is the windowed-max-alloc horizon used as the
	// feedback reference — long enough to span one GC sawtooth (~2s) so a
	// post-GC trough doesn't reopen LP.
	peakWindowTicks = 20

	// arrivalWindowTicks is the sliding window for HP/LP arrival rates.
	arrivalWindowTicks = 10

	// velocityEMAAlpha / byteEMAAlpha smooth the velocity and per-priority
	// bytes/span estimates.
	velocityEMAAlpha = 0.3
	byteEMAAlpha     = 0.2

	// byteSampleEveryN samples 1-in-N batches for proto-size measurement
	// (full marshal is O(n) CPU).
	byteSampleEveryN = 32

	// idleRateThreshold (spans/sec) is the windowed arrival rate below
	// which the controller is considered idle and FREEZES (no envelope
	// decay, no f recovery) — so a no-traffic gap can't drain state. Real
	// load is thousands of spans/sec; a benchmark pause is ~0, so this
	// cleanly separates the two.
	idleRateThreshold = 100.0

	// envDecay is the per-tick decay of the hp_commit / lp_commit envelopes.
	// US is derived from these (recent worst-case held, decayed slowly)
	// rather than instantaneous values, so a transient stall — arrivals
	// momentarily 0 — can't collapse the margin and snap US to soft.
	// 0.97/tick ≈ 2.3s half-life: holds through a ~1s stall, releases over a
	// few seconds of genuine low load.
	envDecay = 0.97

	// ampDecay is the (much slower) per-tick decay of the GC-drop amplitude
	// MAX. The amplitude feeds both the US floor and the gate trigger, where
	// a STABLE, conservative swing estimate matters more than responsiveness:
	// a momentary low reading must NOT delay the gate (that was v22's late-
	// engagement bug). So we hold the recent MAX GC drop and forget it only
	// slowly. 0.998/tick ≈ 35s half-life — stable over a ramp, eventually
	// relaxes a one-off spike. Frozen when idle (updateGCAmplitude returns
	// early), so a pause never decays it.
	ampDecay = 0.998
)

// pressureState is the refuse-all gate on real alloc vs soft/hard.
type pressureState int32

const (
	stateNoPressure pressureState = 0
	stateShedLP     pressureState = 1
	stateSoft       pressureState = 2
	stateHard       pressureState = 3
)

const MetricsInterval = 1 * time.Second

// fracScale is the fixed-point scale for the atomic LP-admit fraction.
const fracScale = 1_000_000

var protoMarshaler ptrace.ProtoMarshaler

type priorityProcessor struct {
	config       *Config
	logger       *zap.Logger
	nextConsumer consumer.Traces

	totalMemory uint64
	softLimit   uint64
	hardLimit   uint64
	horizonSec  float64

	// LP-admit fraction (1=admit, 0=shed) × fracScale: a binary mirror of
	// the controller state for processTraces and metrics. Written by the
	// controller, read by processTraces per LP batch.
	admitFracMicro atomic.Int64

	state atomic.Int32

	checkTicker   *time.Ticker
	metricsTicker *time.Ticker
	cancel        context.CancelFunc
	wg            chan struct{}

	gcCount         int64
	lastSoftGC      time.Time // throttle for soft-band forced GC (controller-goroutine only)
	lastUltrasoftGC time.Time // throttle for ultrasoft-band (US≤alloc<soft) forced GC

	hpAdmitted int64
	lpAdmitted int64
	hpRefused  int64
	lpRefused  int64

	// Sampled proto-byte accumulators (reset each tick).
	batchSeq    atomic.Int64
	sampHPBytes atomic.Int64
	sampHPSpans atomic.Int64
	sampLPBytes atomic.Int64
	sampLPSpans atomic.Int64

	// Arrival ring (spans/tick) for HP/LP rates.
	hpArrivalsLastSnap int64
	lpArrivalsLastSnap int64
	hpArrivalsPerTick  []int64
	lpArrivalsPerTick  []int64
	windowIdx          int

	bytesPerHPSpan float64
	bytesPerLPSpan float64

	velocityInit         bool
	prevAlloc            uint64
	velocityBytesPerTick atomic.Int64

	// Decaying-max envelopes for the US derivation (controller-goroutine
	// only). Hold the recent worst-case so a transient stall can't snap US.
	// ampEnv is the working GC-drop amplitude: decays toward the midpoint of
	// the hi/lo swing band (never below it), bumped by fresh drops. See
	// updateGCAmplitude. Feeds the US floor and the gate trigger.
	hpCommitEnv float64
	lpCommitEnv float64
	ampEnv      float64

	// GC-drop amplitude tracker (controller-goroutine only): measures the
	// sawtooth from alloc down-steps, climb-immune. ampHi/ampLo are the slow
	// high/low envelopes of per-cycle swing; their midpoint floors ampEnv so
	// the shed-induced swing shrink can't collapse the cushion.
	gcInit      bool
	gcPrevAlloc uint64
	gcPrevFall  bool
	gcCycleHigh uint64
	gcCycleLow  uint64
	ampHi       float64
	ampLo       float64

	// Peak (windowed-max) alloc ring (diagnostics/metrics only).
	peakRing []uint64
	peakIdx  int

	// Metrics mirrors.
	lastLPShareMilli atomic.Int64
	lastUSBytes      atomic.Int64
	lastPeakBytes    atomic.Int64
}

func newProcessor(config *Config, logger *zap.Logger, nextConsumer consumer.Traces) (*priorityProcessor, error) {
	totalMemory, err := getTotalMemory()
	if err != nil {
		return nil, fmt.Errorf("failed to determine total memory: %w", err)
	}
	softLimit := uint64(config.SoftPercentage) * totalMemory / 100
	hardLimit := uint64(config.HardPercentage) * totalMemory / 100

	logger.Info("Priority processor initialized (analytic LP-shedding controller v18)",
		zap.Uint64("total_memory_bytes", totalMemory),
		zap.Uint64("soft_limit_bytes", softLimit),
		zap.Uint64("hard_limit_bytes", hardLimit),
		zap.Uint32("soft_percentage", config.SoftPercentage),
		zap.Uint32("hard_percentage", config.HardPercentage),
		zap.Float64("cp_safety_factor", config.CPSafetyFactor),
		zap.Duration("check_interval", config.CheckInterval),
		zap.Float64("memory_overhead_factor", memoryOverheadFactor),
		zap.Int("horizon_ticks", horizonTicks))

	p := &priorityProcessor{
		config:            config,
		logger:            logger,
		nextConsumer:      nextConsumer,
		totalMemory:       totalMemory,
		softLimit:         softLimit,
		hardLimit:         hardLimit,
		horizonSec:        float64(horizonTicks) * config.CheckInterval.Seconds(),
		hpArrivalsPerTick: make([]int64, arrivalWindowTicks),
		lpArrivalsPerTick: make([]int64, arrivalWindowTicks),
		peakRing:          make([]uint64, peakWindowTicks),
	}
	p.admitFracMicro.Store(fracScale) // start admitting all LP
	return p, nil
}

func (p *priorityProcessor) start(ctx context.Context, _ component.Host) error {
	ctx, cancel := context.WithCancel(ctx)
	p.cancel = cancel
	p.checkTicker = time.NewTicker(p.config.CheckInterval)
	p.metricsTicker = time.NewTicker(MetricsInterval)
	p.wg = make(chan struct{}, 2)
	go func() { p.checkLoop(ctx); p.wg <- struct{}{} }()
	go func() { p.metricsLoop(ctx); p.wg <- struct{}{} }()
	return nil
}

func (p *priorityProcessor) shutdown(_ context.Context) error {
	if p.cancel != nil {
		p.cancel()
	}
	if p.wg != nil {
		<-p.wg
		<-p.wg
	}
	if p.checkTicker != nil {
		p.checkTicker.Stop()
	}
	if p.metricsTicker != nil {
		p.metricsTicker.Stop()
	}
	p.logMetricsSnapshot("priority_processor_shutdown_metrics")
	return nil
}

// ---------------------------------------------------------------------
// Controller
// ---------------------------------------------------------------------

func (p *priorityProcessor) checkLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-p.checkTicker.C:
			p.tick()
		}
	}
}

// tick recomputes the LP-admit fraction f from the analytic law. No
// tuning gains: US is derived from the unsheddable HP commit, and the
// shed fraction is the analytic amount of LP commit needed to remove the
// projected overshoot over the horizon.
func (p *priorityProcessor) tick() {
	alloc := p.readCurrentMemory()

	// Arrival-rate detector runs every tick — it's how we know we're idle.
	hpRate, lpRate := p.updateArrivalRates()
	p.updateByteEstimates()
	idle := (hpRate + lpRate) < idleRateThreshold

	// Velocity freezes when idle (hold the EMA; don't let the no-traffic
	// alloc drain pollute it).
	velocity := p.updateVelocity(alloc, idle)

	var st pressureState
	switch {
	case alloc >= p.hardLimit:
		if p.config.ForceGC {
			preGC := alloc
			runtime.GC()
			atomic.AddInt64(&p.gcCount, 1)
			alloc = p.readCurrentMemory()
			p.logger.Debug("Forced GC under hard pressure",
				zap.Uint64("pre_gc_alloc_bytes", preGC),
				zap.Uint64("post_gc_alloc_bytes", alloc))
		}
		if alloc >= p.hardLimit {
			st = stateHard
		} else if alloc >= p.softLimit {
			st = stateSoft
		}
	case alloc >= p.softLimit:
		st = stateSoft
		// Throttled forced GC in the SOFT band (mirrors stock memory_limiter's
		// min_gc_interval_when_soft_limited): above soft we don't just refuse —
		// we actively claw alloc back, so the heap doesn't sit pegged over soft
		// in sustained refuse-all. Throttled (not every tick like the hard
		// backstop) to bound STW/CPU cost. Re-read alloc post-GC and
		// re-classify so the rest of the tick (US, shed decision) sees the
		// reclaimed heap — a successful GC can drop us back below soft and
		// re-admit CP instead of refusing it.
		if p.config.ForceGC && p.config.GCSoftInterval > 0 && time.Since(p.lastSoftGC) >= p.config.GCSoftInterval {
			preGC := alloc
			runtime.GC()
			atomic.AddInt64(&p.gcCount, 1)
			p.lastSoftGC = time.Now()
			alloc = p.readCurrentMemory()
			p.logger.Debug("Forced GC under soft pressure",
				zap.Uint64("pre_gc_alloc_bytes", preGC),
				zap.Uint64("post_gc_alloc_bytes", alloc))
			switch {
			case alloc >= p.hardLimit:
				st = stateHard
			case alloc >= p.softLimit:
				st = stateSoft
			default:
				st = stateNoPressure
			}
		}
	default:
		st = stateNoPressure
	}

	// Peak ring freezes when idle (hold the windowed max; a no-traffic
	// gap must not age the recent swing-top out of the window). When idle
	// the whole controller is held — only the arrival detector advances —
	// so recovery is a function of time-UNDER-LOAD, not wall-clock, and
	// the benchmark's inter-step snapshot pauses can't cold-start each
	// step un-shed. Continuous real load is never idle: no-op there.
	peak, trough := p.updatePeak(alloc, idle)

	// Commit rates in heap bytes/sec. hp_rate is the ABSOLUTE arrival
	// rate, so a pure load surge (ratio unchanged) raises hp_commit and
	// lowers US immediately — load, not just mix, drives the buffer.
	hpCommit := hpRate * p.bytesPerHPSpan * memoryOverheadFactor
	lpCommit := lpRate * p.bytesPerLPSpan * memoryOverheadFactor

	// Measured GC sawtooth amplitude, derived from the GC DROPS (down-steps
	// in alloc), NOT peak−trough over a window. A windowed max−min conflates
	// the SECULAR CLIMB during a load ramp with the GC swing — it over-reads
	// 2–3× at step transitions (a measured sd ≈ 50% of mean, range 1%→30%).
	// The per-cycle drop is climb-immune (rises ignored) and low-variance.
	// updateGCAmplitude updates p.ampEnv (decaying-max of drops) and returns
	// this tick's drop for diagnostics.
	amplitude := p.updateGCAmplitude(alloc, idle)

	// Decaying-max envelopes for the commit rates (ampEnv handled above).
	// Hold the recent worst-case and decay slowly so a transient stall
	// (arrivals momentarily 0) can't collapse the margin and snap US to soft.
	// Frozen when idle — don't let a pause decay the margin.
	if !idle {
		p.hpCommitEnv = math.Max(hpCommit, p.hpCommitEnv*envDecay)
		p.lpCommitEnv = math.Max(lpCommit, p.lpCommitEnv*envDecay)
	}

	// Byte-weighted LP share from the envelopes (robust to stalls).
	var lpShare float64
	if p.hpCommitEnv+p.lpCommitEnv > 0 {
		lpShare = p.lpCommitEnv / (p.hpCommitEnv + p.lpCommitEnv)
	} else {
		lpShare = 1.0
	}

	// Derived US = soft − margin. The margin is the simple ALWAYS-ON sum of
	// the two things that drive alloc toward soft (no gate, no weight):
	//
	//   margin  = median_amp + cp_safety_factor × (1/lp_share) × hp_rise
	//   hp_rise = hp_commit_env × horizon
	//
	//   - SWING (median_amp, coeff 1): the GC sawtooth, the midpoint of the
	//     slow hi/lo band of GC drops (updateGCAmplitude — climb-immune, and
	//     the midpoint floor keeps it from collapsing during shedding). The
	//     permanent floor; DOMINATES for LP-heavy loads (high lp_share) where
	//     the swing is the whole threat — this is what cpd=2 needs and v18
	//     lacked.
	//   - DRIFT ((1/lp_share) × hp_rise): the unsheddable HP climb over the
	//     horizon, scaled by LEVER WEAKNESS. 1/lp_share = total_commit /
	//     lp_commit: a weak LP lever (low lp_share, HP-heavy) barely dents the
	//     fill rate when shed, so the cushion must be deep and we must shed
	//     early; a strong lever relieves instantly so amplitude carries it.
	//     DOMINATES for HP-heavy loads (cpd=3): always-on means we pre-shed on
	//     the approach and the heap is pre-drained entering the wall — which is
	//     exactly what every GATED variant (v21/22/24) failed to do (they
	//     deferred shedding and entered the critical step under-shed → CP leak).
	//
	// cp_safety_factor defaults to 1 — NO tuning multiplier; the margin is
	// fully derived from measured amplitude, rate and ratio. hp_rise → 0 at
	// idle, so the drift vanishes and US → soft − median_amp (clean low load).
	// Always-on (no gate) is deliberate: for HP-heavy, CP protection at the
	// onset REQUIRES early pre-draining, and the approach LP shed is its
	// necessary price (LP is reconstructable). Safe under v18's binary shed +
	// INSTANTANEOUS-alloc comparison.
	aggression := 1.0 / math.Max(lpShare, 0.01)
	// DRIFT uses TOTAL arrival commit (hp+lp), not hp-only: the total rate is
	// smoother (more events → lower relative variance than the spiky HP-only
	// rate, which the 100ms-window decaying-max otherwise latches into margin
	// jitter) and larger, so US descends earlier/deeper under load. Still scaled
	// by lever-weakness (1/lp_share) so HP-heavy mixes reserve more.
	totalRise := (p.hpCommitEnv + p.lpCommitEnv) * p.horizonSec
	// PHASE-AWARE SWING: reserve the climb REMAINING to the next sawtooth peak,
	// not the full amplitude. trough = recent post-GC low, (alloc-trough) = climb
	// already done this cycle, ampEnv = expected full climb. Near the trough we
	// reserve ~full ampEnv (the peak is still ahead); near the peak we reserve
	// ~0 (a GC drop is imminent). This makes the shed decision track the
	// projected PEAK (≈ trough+ampEnv) regardless of where in the sawtooth we
	// sample — killing the phase-driven over-shed (sampled high, about to drop)
	// and under-shed (sampled low) that a flat ampEnv causes.
	climbDone := 0.0
	if alloc > trough {
		climbDone = float64(alloc - trough)
	}
	phaseAmp := p.ampEnv - climbDone
	if phaseAmp < 0 {
		phaseAmp = 0
	}
	// SIZE-AWARE AUTO-GAIN: replace the hand-picked cp_safety constant's role
	// with the MEASURED HP/LP span-size ratio (bytesPerHPSpan/bytesPerLPSpan).
	// Rationale: the reserve buys CP headroom by shedding LP, but each
	// unsheddable HP span costs bytesPerHPSpan of heap while each shed LP span
	// only frees bytesPerLPSpan — so the LP you must shed to cover a unit of HP
	// commit scales with the HP/LP size ratio. Larger HP (ratio>1) ⇒ reserve
	// more / shed LP earlier; equal sizes (ratio→1) ⇒ no extra gain. Self-tunes
	// (~1.16 at cpd=3 for esrev2's 132/114B spans) instead of a magic 1-or-2.
	// cp_safety_factor remains an OUTER trim (default 1 = pure auto-gain; raise
	// to scale the whole reserve). Guard the pre-warmup window where no LP span
	// has been sized yet (bytesPerLPSpan==0) by falling back to unity gain.
	sizeGain := 1.0
	if p.bytesPerLPSpan > 0 {
		sizeGain = p.bytesPerHPSpan / p.bytesPerLPSpan
	}
	margin := phaseAmp + p.config.CPSafetyFactor*sizeGain*aggression*totalRise
	us := float64(p.softLimit) - margin
	if us < 0 {
		us = 0
	}
	if us > float64(p.softLimit) {
		us = float64(p.softLimit)
	}

	// Binary LP shed (no f): if not already in soft/hard refuse-all, shed
	// ALL LP once alloc has reached US, admit all LP below it. There is no
	// admit fraction and no integral — the GRADED shed emerges as the duty
	// cycle of the GC sawtooth crossing US (alloc above US some fraction of
	// ticks ⇒ that fraction of LP refused). US (dynamic) is the single
	// control variable; the alloc level itself discriminates idle (alloc <
	// US ⇒ admit) from the cliff (alloc ≥ US ⇒ shed all). This directly
	// implements "all LP gone before any CP goes": US sits below soft, so
	// alloc crosses US (100% LP shed) before it can reach the refuse-all
	// line.
	// Ultrasoft GC: the gentlest of the three GC tiers (ultrasoft → soft →
	// hard, escalating). As soon as alloc reaches US (the shed line) we start
	// clawing the heap back — begun in the shed band so alloc is less likely
	// to climb all the way to soft (refuse-all). Only fires when still below
	// soft (st==noPressure here); the soft/hard tiers GC above. Re-read alloc
	// post-GC so the shed decision below uses the reclaimed heap — a good GC
	// can drop us back under US and avoid shedding LP at all.
	if p.config.ForceGC && p.config.GCUltrasoftInterval > 0 && st == stateNoPressure && float64(alloc) >= us &&
		time.Since(p.lastUltrasoftGC) >= p.config.GCUltrasoftInterval {
		preGC := alloc
		runtime.GC()
		atomic.AddInt64(&p.gcCount, 1)
		p.lastUltrasoftGC = time.Now()
		alloc = p.readCurrentMemory()
		p.logger.Debug("Forced GC under ultrasoft pressure",
			zap.Uint64("pre_gc_alloc_bytes", preGC),
			zap.Uint64("post_gc_alloc_bytes", alloc))
	}

	if st == stateNoPressure && float64(alloc) >= us {
		st = stateShedLP
	}

	// admit_fraction mirrors the binary decision (1 = admitting LP, 0 =
	// shedding) so the metrics/duty-cycle averaging still reads cleanly.
	if st == stateNoPressure {
		p.admitFracMicro.Store(fracScale)
	} else {
		p.admitFracMicro.Store(0)
	}
	p.state.Store(int32(st))
	p.lastUSBytes.Store(int64(us))
	p.lastPeakBytes.Store(int64(peak))
	p.lastLPShareMilli.Store(int64(lpShare * 1000))

	if ce := p.logger.Check(zap.DebugLevel, "priority_decision"); ce != nil {
		admitFrac := 0.0
		if st == stateNoPressure {
			admitFrac = 1.0
		}
		ce.Write(
			zap.String("state", stateName(st)),
			zap.Float64("admit_fraction", admitFrac),
			zap.Uint64("alloc_bytes", alloc),
			zap.Uint64("peak_bytes", peak),
			zap.Uint64("trough_bytes", trough),
			zap.Float64("amplitude_bytes", amplitude),
			zap.Float64("amplitude_env_bytes", p.ampEnv),
			zap.Float64("hp_commit_env_bps", p.hpCommitEnv),
			zap.Float64("us_bytes", us),
			zap.Uint64("soft_limit_bytes", p.softLimit),
			zap.Int64("velocity_bytes_per_tick", velocity),
			zap.Float64("hp_commit_bps", hpCommit),
			zap.Float64("lp_commit_bps", lpCommit),
			zap.Float64("hp_rate_sps", hpRate),
			zap.Float64("lp_rate_sps", lpRate),
			zap.Float64("bytes_per_hp_span", p.bytesPerHPSpan),
			zap.Float64("bytes_per_lp_span", p.bytesPerLPSpan),
			zap.Float64("size_gain", sizeGain),
			zap.Float64("lp_share", lpShare))
	}
}

// updateGCAmplitude estimates the GC sawtooth amplitude from alloc DOWN-steps
// (GC frees). This is immune to the secular climb during a load ramp — a
// windowed peak−trough is not, and over-reads at step transitions. Each
// non-idle tick it decays ampEnv; when alloc is falling it latches the peak
// at fall onset (prevAlloc) and captures the running peak→current drop into
// the decaying-max ampEnv. Returns this tick's drop (0 when rising) for
// diagnostics. Frozen when idle (no decay, no update) so a pause can't drain
// the envelope.
func (p *priorityProcessor) updateGCAmplitude(alloc uint64, idle bool) float64 {
	if idle {
		return 0
	}
	if !p.gcInit {
		p.gcInit = true
		p.gcPrevAlloc = alloc
		p.gcCycleHigh = alloc
		p.gcCycleLow = alloc
		return 0
	}
	var drop float64
	falling := alloc < p.gcPrevAlloc
	if falling {
		if !p.gcPrevFall {
			// fall onset: latch peak at the value just before the drop, reset
			// the trough tracker.
			p.gcCycleHigh = p.gcPrevAlloc
			p.gcCycleLow = alloc
		}
		if alloc < p.gcCycleLow {
			p.gcCycleLow = alloc
		}
		if p.gcCycleHigh > alloc {
			drop = float64(p.gcCycleHigh - alloc)
		}
	} else if p.gcPrevFall {
		// fall just ended → a full GC cycle completed. Feed its peak→trough
		// amplitude into the slow hi/lo band: ampHi decays-max (forgets a
		// spike slowly), ampLo captures new lows instantly and relaxes UP
		// toward ampHi (forgets a one-off small swing slowly).
		cycle := float64(p.gcCycleHigh - p.gcCycleLow)
		if cycle > 0 {
			p.ampHi = math.Max(cycle, p.ampHi*ampDecay)
			if p.ampLo == 0 || cycle < p.ampLo {
				p.ampLo = cycle
			}
		}
	}
	// ampLo relaxes upward toward ampHi each tick (slow), so a single tiny
	// cycle doesn't pin the band low forever.
	p.ampLo += (p.ampHi - p.ampLo) * (1 - ampDecay)
	// Working amplitude decays toward the MIDPOINT of the band (not toward 0),
	// floored there, and is bumped by the live drop. The midpoint floor is
	// what keeps the cushion sized to the UNSHED swing after shedding shrinks
	// the observed GC drops (the shed-feedback shrink).
	mid := 0.5 * (p.ampHi + p.ampLo)
	p.ampEnv = math.Max(p.ampEnv*ampDecay, mid)
	if drop > p.ampEnv {
		p.ampEnv = drop
	}
	p.gcPrevFall = falling
	p.gcPrevAlloc = alloc
	return drop
}

func (p *priorityProcessor) updateVelocity(alloc uint64, idle bool) int64 {
	if !p.velocityInit {
		p.velocityInit = true
		p.prevAlloc = alloc
		return 0
	}
	if idle {
		// Hold the EMA; advance prevAlloc so the first post-idle tick
		// computes a normal one-tick delta rather than spanning the gap.
		p.prevAlloc = alloc
		return p.velocityBytesPerTick.Load()
	}
	delta := int64(alloc) - int64(p.prevAlloc)
	p.prevAlloc = alloc
	prev := p.velocityBytesPerTick.Load()
	nv := int64(velocityEMAAlpha*float64(delta) + (1.0-velocityEMAAlpha)*float64(prev))
	p.velocityBytesPerTick.Store(nv)
	return nv
}

// updatePeak rolls alloc into the ring and returns the max and min over
// the window. The min ignores unfilled (zero) slots so the amplitude is
// not inflated during the first window after startup.
func (p *priorityProcessor) updatePeak(alloc uint64, idle bool) (peak, trough uint64) {
	// When not idle, push the new sample; when idle, hold the ring so a
	// no-traffic gap can't age the recent swing-top out of the window.
	if !idle {
		p.peakRing[p.peakIdx] = alloc
		p.peakIdx = (p.peakIdx + 1) % len(p.peakRing)
	}
	for _, v := range p.peakRing {
		if v > peak {
			peak = v
		}
		if v > 0 && (trough == 0 || v < trough) {
			trough = v
		}
	}
	if trough == 0 {
		trough = alloc
	}
	return peak, trough
}

func (p *priorityProcessor) updateArrivalRates() (hpRate, lpRate float64) {
	curHp := atomic.LoadInt64(&p.hpAdmitted) + atomic.LoadInt64(&p.hpRefused)
	curLp := atomic.LoadInt64(&p.lpAdmitted) + atomic.LoadInt64(&p.lpRefused)
	dHp := curHp - p.hpArrivalsLastSnap
	dLp := curLp - p.lpArrivalsLastSnap
	if dHp < 0 {
		dHp = 0
	}
	if dLp < 0 {
		dLp = 0
	}
	p.hpArrivalsLastSnap = curHp
	p.lpArrivalsLastSnap = curLp
	p.hpArrivalsPerTick[p.windowIdx] = dHp
	p.lpArrivalsPerTick[p.windowIdx] = dLp
	p.windowIdx = (p.windowIdx + 1) % len(p.hpArrivalsPerTick)

	var sumHp, sumLp int64
	for i := range p.hpArrivalsPerTick {
		sumHp += p.hpArrivalsPerTick[i]
		sumLp += p.lpArrivalsPerTick[i]
	}
	windowSec := float64(len(p.hpArrivalsPerTick)) * p.config.CheckInterval.Seconds()
	if windowSec <= 0 {
		windowSec = 1
	}
	return float64(sumHp) / windowSec, float64(sumLp) / windowSec
}

func (p *priorityProcessor) updateByteEstimates() {
	hb := p.sampHPBytes.Swap(0)
	hs := p.sampHPSpans.Swap(0)
	lb := p.sampLPBytes.Swap(0)
	ls := p.sampLPSpans.Swap(0)
	if hs > 0 {
		v := float64(hb) / float64(hs)
		if p.bytesPerHPSpan == 0 {
			p.bytesPerHPSpan = v
		} else {
			p.bytesPerHPSpan = byteEMAAlpha*v + (1-byteEMAAlpha)*p.bytesPerHPSpan
		}
	}
	if ls > 0 {
		v := float64(lb) / float64(ls)
		if p.bytesPerLPSpan == 0 {
			p.bytesPerLPSpan = v
		} else {
			p.bytesPerLPSpan = byteEMAAlpha*v + (1-byteEMAAlpha)*p.bytesPerLPSpan
		}
	}
}

func (p *priorityProcessor) readCurrentMemory() uint64 {
	ms := &runtime.MemStats{}
	runtime.ReadMemStats(ms)
	return ms.Alloc
}

func stateName(s pressureState) string {
	switch s {
	case stateNoPressure:
		return "none"
	case stateShedLP:
		return "shedlp"
	case stateSoft:
		return "soft"
	case stateHard:
		return "hard"
	default:
		return "unknown"
	}
}

// ---------------------------------------------------------------------
// processTraces — passthrough admission with graded LP shedding.
// ---------------------------------------------------------------------

func (p *priorityProcessor) processTraces(ctx context.Context, td ptrace.Traces) (ptrace.Traces, error) {
	isLP := isLPBatch(ctx)
	spanCount := int64(td.SpanCount())

	if spanCount > 0 && p.batchSeq.Add(1)%byteSampleEveryN == 0 {
		sz := int64(protoMarshaler.TracesSize(td))
		if isLP {
			p.sampLPBytes.Add(sz)
			p.sampLPSpans.Add(spanCount)
		} else {
			p.sampHPBytes.Add(sz)
			p.sampHPSpans.Add(spanCount)
		}
	}

	st := pressureState(p.state.Load())
	switch st {
	case stateSoft, stateHard:
		// Refuse all (memory_limiter contract).
		if isLP {
			atomic.AddInt64(&p.lpRefused, spanCount)
		} else {
			atomic.AddInt64(&p.hpRefused, spanCount)
		}
		return td, status.Error(codes.Unavailable, "priority processor: refusing all under "+stateName(st)+" pressure")
	case stateShedLP:
		// Shed LP, admit HP.
		if isLP {
			atomic.AddInt64(&p.lpRefused, spanCount)
			return td, status.Error(codes.Unavailable, "priority processor: shedding LP (alloc ≥ US)")
		}
		atomic.AddInt64(&p.hpAdmitted, spanCount)
		return td, nil
	default: // stateNoPressure — admit everything
		if isLP {
			atomic.AddInt64(&p.lpAdmitted, spanCount)
		} else {
			atomic.AddInt64(&p.hpAdmitted, spanCount)
		}
		return td, nil
	}
}

func isLPBatch(ctx context.Context) bool {
	info := client.FromContext(ctx)
	for _, v := range info.Metadata.Get(MetadataPriorityKey) {
		if strings.EqualFold(v, "lp") {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------

func (p *priorityProcessor) metricsLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-p.metricsTicker.C:
			p.logMetricsSnapshot("priority_processor_metrics")
		}
	}
}

func (p *priorityProcessor) logMetricsSnapshot(name string) {
	alloc := p.readCurrentMemory()
	st := pressureState(p.state.Load())
	f := float64(p.admitFracMicro.Load()) / float64(fracScale)
	lpShare := float64(p.lastLPShareMilli.Load()) / 1000.0
	p.logger.Info(name,
		zap.String("state", stateName(st)),
		zap.Uint64("alloc_bytes", alloc),
		zap.Float64("admit_fraction", f),
		zap.Int64("velocity_bytes_per_tick", p.velocityBytesPerTick.Load()),
		zap.Int64("peak_bytes", p.lastPeakBytes.Load()),
		zap.Int64("us_bytes", p.lastUSBytes.Load()),
		zap.Uint64("soft_limit_bytes", p.softLimit),
		zap.Uint64("hard_limit_bytes", p.hardLimit),
		zap.Float64("lp_share", lpShare),
		zap.Float64("bytes_per_hp_span", p.bytesPerHPSpan),
		zap.Float64("bytes_per_lp_span", p.bytesPerLPSpan),
		zap.Int64("hp_admitted", atomic.LoadInt64(&p.hpAdmitted)),
		zap.Int64("lp_admitted", atomic.LoadInt64(&p.lpAdmitted)),
		zap.Int64("hp_refused", atomic.LoadInt64(&p.hpRefused)),
		zap.Int64("lp_refused", atomic.LoadInt64(&p.lpRefused)),
		zap.Int64("gc_count", atomic.LoadInt64(&p.gcCount)))
}

// ---------------------------------------------------------------------
// Total memory probe
// ---------------------------------------------------------------------

func getTotalMemory() (uint64, error) {
	isV2, err := isCGroupV2()
	if err == nil {
		if isV2 {
			if mem, err := readCGroupV2MemoryLimit(); err == nil && mem > 0 {
				return mem, nil
			}
		} else {
			if mem, err := readCGroupV1MemoryLimit(); err == nil && mem > 0 {
				return mem, nil
			}
		}
	}
	if data, err := os.ReadFile("/proc/meminfo"); err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			if strings.HasPrefix(line, "MemTotal:") {
				fields := strings.Fields(line)
				if len(fields) >= 2 {
					kb, err := strconv.ParseUint(fields[1], 10, 64)
					if err == nil {
						return kb * 1024, nil
					}
				}
			}
		}
	}
	return 0, fmt.Errorf("unable to determine total memory: cgroup limits not found and /proc/meminfo unavailable")
}

func isCGroupV2() (bool, error) {
	if _, err := os.Stat("/sys/fs/cgroup/cgroup.controllers"); err == nil {
		return true, nil
	}
	if _, err := os.Stat("/sys/fs/cgroup/memory"); err == nil {
		return false, nil
	}
	return false, fmt.Errorf("unable to determine cgroup version")
}

func readCGroupV2MemoryLimit() (uint64, error) {
	data, err := os.ReadFile("/sys/fs/cgroup/memory.max")
	if err != nil {
		return 0, err
	}
	s := strings.TrimSpace(string(data))
	if s == "max" || s == "" {
		return 0, fmt.Errorf("no limit set")
	}
	return strconv.ParseUint(s, 10, 64)
}

func readCGroupV1MemoryLimit() (uint64, error) {
	data, err := os.ReadFile("/sys/fs/cgroup/memory/memory.limit_in_bytes")
	if err != nil {
		return 0, err
	}
	s := strings.TrimSpace(string(data))
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, err
	}
	if v < 0 || v == 9223372036854771712 {
		return 0, fmt.Errorf("no limit set")
	}
	return uint64(v), nil
}
