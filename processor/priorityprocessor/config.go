// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package priorityprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/priorityprocessor"

import (
	"errors"
	"time"

	"go.opentelemetry.io/collector/component"
)

var (
	ErrCheckIntervalRequired   = errors.New("check_interval must be greater than zero")
	ErrInvalidPercentage       = errors.New("memory limits must be between 1 and 100 percent")
	ErrInvalidThresholdOrder   = errors.New("soft_percentage < hard_percentage required")
	ErrInvalidSafetyFactor     = errors.New("cp_safety_factor must be greater than zero")
	ErrInvalidMarginMode       = errors.New(`us_margin_mode must be "arrival" or "backlog"`)
	ErrInvalidLPRefusalCode    = errors.New(`lp_refusal_code must be "unavailable" or "resource_exhausted"`)
	ErrInvalidCPUShedThreshold = errors.New("cpu_shed_threshold must be between 0 (off) and 1")
	ErrInvalidMode             = errors.New(`mode must be "admission" or "queue"`)
	ErrInvalidDispatchWorkers  = errors.New("dispatch_workers must be at least 1 in queue mode")
)

// Tomislav-RetCtx: US-margin modes and LP refusal codes (see Config).
const (
	MarginModeArrival = "arrival"
	MarginModeBacklog = "backlog"

	LPRefusalUnavailable       = "unavailable"
	LPRefusalResourceExhausted = "resource_exhausted"

	// Tomislav-RetCtx: processor stages (Config.Mode).
	ModeAdmission = "admission"
	ModeQueue     = "queue"
)

var _ component.Config = (*Config)(nil)

// Config is the analytic LP-shedding controller (v18). It is a pure
// passthrough: each batch is forwarded or refused (gRPC Unavailable) by
// priority tag ("bridges-priority", "lp" vs HP) and controller state.
//
// Admission each request (alloc = runtime.MemStats.Alloc):
//   - alloc ≥ hard:  GC, then if still ≥ hard refuse ALL
//   - alloc ≥ soft:  refuse ALL (memory_limiter contract)
//   - alloc ≥ US:    refuse LP, admit HP   (the LP-shedding state)
//   - alloc < US:    admit ALL
//
// LP shedding is BINARY (shed all LP above US, none below); the graded
// behavior observed across the ramp is the duty cycle of the GC sawtooth
// crossing US, not a per-batch probability. There is no f, no integral,
// no RNG.
//
// US (the level where LP shedding begins) is DERIVED every tick, not a
// setpoint:
//
//	hp_commit = hp_rate × bytes_per_hp_span × overhead   (heap B/s, unsheddable)
//	lp_commit = lp_rate × bytes_per_lp_span × overhead   (heap B/s, the LP lever)
//	lp_share  = lp_commit_env / (hp_commit_env + lp_commit_env)
//	margin    = hp_commit_env × horizon × (1/lp_share) × cp_safety_factor
//	US        = clamp(soft − margin, 0, soft)
//
// The buffer below soft equals the HP-driven alloc rise we cannot shed
// over the response horizon, scaled up when LP is a weak lever (small
// lp_share) and by cp_safety_factor. hp_rate is the ABSOLUTE arrival
// rate, so a pure load surge lowers US immediately; HP-heavy or
// fast-rising load begins shedding LP earlier, carving the cushion that
// lets 100% of LP drop before any CP does.
//
// THREE behavioral knobs: soft (S) and hard (H) — the memory_limiter
// contract — and cp_safety_factor, the CP-vs-LP tradeoff dial. Higher
// sheds LP earlier / protects CP harder; ~1.0 sheds only as late as the
// measured HP rise demands. check_interval is sampling cadence, not a
// behavioral knob. Everything else (overhead factor, horizon, EMA
// smoothing, sampling rate, idle threshold) is a fixed internal constant
// — a physical conversion or time constant, see the const block in
// priority.go. None is a gain; none needs per-deployment tuning.
type Config struct {
	// SoftPercentage (S): refuse-all threshold (memory_limiter contract).
	SoftPercentage uint32 `mapstructure:"soft_percentage"`
	// HardPercentage (H): force-GC threshold.
	HardPercentage uint32 `mapstructure:"hard_percentage"`
	// CPSafetyFactor: multiplies the derived (rate × ratio) LP-shed
	// margin below soft — the one CP-vs-LP policy dial. Higher sheds LP
	// earlier and protects CP harder; ~1.0 sheds only as late as the
	// measured HP rise requires. Default 2.0.
	CPSafetyFactor float64 `mapstructure:"cp_safety_factor"`
	// CheckInterval: controller sampling cadence. Default 100ms
	// (matches memory_limiter). Sampling cadence, not a behavioral knob.
	CheckInterval time.Duration `mapstructure:"check_interval"`

	// ForceGC is the master switch for ALL manual (forced) GC — the hard
	// backstop AND the throttled soft/ultrasoft clawback. Default true.
	// Set false to do NO manual GC and rely purely on the Go runtime
	// (GOGC + GOMEMLIMIT) — useful to isolate the controller's shedding
	// from forced-GC CPU cost on a CPU-limited collector.
	ForceGC bool `mapstructure:"force_gc"`
	// GCSoftInterval / GCUltrasoftInterval throttle the forced GC in the
	// soft (soft ≤ alloc < hard) and ultrasoft (US ≤ alloc < soft) bands
	// when ForceGC is true. Defaults 1s / 2s. The hard backstop (alloc ≥
	// hard) is unthrottled. Only consulted when ForceGC is true.
	GCSoftInterval      time.Duration `mapstructure:"gc_soft_interval"`
	GCUltrasoftInterval time.Duration `mapstructure:"gc_ultrasoft_interval"`

	// Tomislav-RetCtx: USMarginMode selects what the drift term of the US margin
	// is built from.
	//   - "arrival" (default, v18 behaviour): the GROSS arrival commit
	//     (hp+lp rate x bytes/span x overhead) over the horizon. At high
	//     throughput this reserves more than the whole soft headroom even while
	//     the pipeline keeps up and the heap is flat, so LP is shed with no
	//     memory pressure at all.
	//   - "backlog": the measured NET heap growth (slope of the post-GC
	//     troughs, i.e. live-heap backlog) over the horizon, plus a fixed
	//     reaction reserve of reactionTicks of gross arrivals so a sudden
	//     downstream stall is still covered until the growth is observed.
	//     A pipeline that keeps up does not shed LP.
	USMarginMode string `mapstructure:"us_margin_mode"`
	// Tomislav-RetCtx: LPRefusalCode is the gRPC status returned when an LP
	// batch is refused (any state). "unavailable" (default) is retryable: an
	// upstream collector re-queues the shed LP and retries it, which turns this
	// collector's LP shedding into upstream memory pressure. "resource_exhausted"
	// (without RetryInfo) is non-retryable for the OTLP exporter, so upstream
	// drops shed LP immediately. HP refusals are always "unavailable".
	LPRefusalCode string `mapstructure:"lp_refusal_code"`
	// Tomislav-RetCtx: CPUShedThreshold (0 = off, the default) sheds LP when this process's
	// CPU use over the last second reaches this fraction of its cgroup CPU quota, even with
	// no memory pressure. A CPU-bound collector (e.g. a gateway pinned at its quota) never
	// reaches its memory thresholds -- the backlog waits upstream -- so memory alone never
	// frees CPU for checkpoints. Pairs with the priorityotlp receiver, which refuses LP before
	// decoding it, so shedding actually returns CPU.
	CPUShedThreshold float64 `mapstructure:"cpu_shed_threshold"`
	// Tomislav-RetCtx: CPUTarget (0 = off, the default) is GRADED LP admission: LP may use
	// this process's CPU up to this fraction of its cgroup quota. Once per second the admitted
	// LP fraction is rescaled by target / measured-CPU, so exactly as much LP is refused (before
	// decode, by the priorityotlp receiver) as keeps the collector at the target -- instead of
	// cpu_shed_threshold's all-or-nothing shedding. Memory pressure (US / soft / hard) still
	// overrides. Requires the priorityotlp receiver (the gate is where the fraction is applied).
	CPUTarget float64 `mapstructure:"cpu_target"`
	// Tomislav-RetCtx: Mode selects the stage. "admission" (default, "" too) is the controller
	// described above. "queue" is the strict-priority queue stage (queue.go): placed after the
	// batch processor, with the exporter's sending_queue disabled, it holds the backlog the
	// exporter queue used to hold, in separate HP and LP FIFOs, and always sends HP first. While
	// a queue stage runs, the admission stage admits a checkpoint at soft/hard by evicting the
	// oldest queued LP of at least its size, and refuses it only when no LP is queued. The
	// memory fields are ignored in queue mode (the admission stage owns the state machine).
	Mode string `mapstructure:"mode"`
	// DispatchWorkers is the number of concurrent sends from the queue stage (the role of the
	// exporter's sending_queue.num_consumers, which is disabled in this setup). Default 10.
	DispatchWorkers int `mapstructure:"dispatch_workers"`
}

func (config *Config) Validate() error {
	if config.CheckInterval <= 0 {
		return ErrCheckIntervalRequired
	}
	for _, p := range []uint32{config.SoftPercentage, config.HardPercentage} {
		if p < 1 || p > 100 {
			return ErrInvalidPercentage
		}
	}
	if config.SoftPercentage >= config.HardPercentage {
		return ErrInvalidThresholdOrder
	}
	if config.CPSafetyFactor <= 0 {
		return ErrInvalidSafetyFactor
	}
	switch config.USMarginMode {
	case "", MarginModeArrival, MarginModeBacklog:
	default:
		return ErrInvalidMarginMode
	}
	if config.CPUShedThreshold < 0 || config.CPUShedThreshold > 1 {
		return ErrInvalidCPUShedThreshold
	}
	if config.CPUTarget < 0 || config.CPUTarget > 1 {
		return ErrInvalidCPUShedThreshold
	}
	switch config.LPRefusalCode {
	case "", LPRefusalUnavailable, LPRefusalResourceExhausted:
	default:
		return ErrInvalidLPRefusalCode
	}
	switch config.Mode {
	case "", ModeAdmission:
	case ModeQueue:
		if config.DispatchWorkers < 1 {
			return ErrInvalidDispatchWorkers
		}
	default:
		return ErrInvalidMode
	}
	return nil
}
