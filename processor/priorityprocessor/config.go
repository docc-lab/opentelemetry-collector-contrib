// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package priorityprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/priorityprocessor"

import (
	"errors"
	"time"

	"go.opentelemetry.io/collector/component"
)

var (
	ErrCheckIntervalRequired = errors.New("check_interval must be greater than zero")
	ErrInvalidPercentage     = errors.New("memory limits must be between 1 and 100 percent")
	ErrInvalidThresholdOrder = errors.New("soft_percentage < hard_percentage required")
	ErrInvalidSafetyFactor   = errors.New("cp_safety_factor must be greater than zero")
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
	return nil
}
