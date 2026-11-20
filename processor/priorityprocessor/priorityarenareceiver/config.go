// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package priorityarenareceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/priorityprocessor/priorityarenareceiver"

import (
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configgrpc"
	"go.opentelemetry.io/collector/config/confignet"
)

// Tomislav-RetCtx: LP refusal codes.
const (
	LPRefusalResourceExhausted = "resource_exhausted"
	LPRefusalUnavailable       = "unavailable"
)

// Config of the priorityarena receiver. All buffered spans live in its arena; nothing after
// it in the pipeline should queue (no batch processor, exporter sending_queue disabled).
type Config struct {
	GRPC configgrpc.ServerConfig `mapstructure:"grpc"`

	// MemoryMiB is the arena size. It is outside the Go heap: GOMEMLIMIT should leave it out.
	MemoryMiB int `mapstructure:"memory_mib"`
	// MemoryPercentage, when set, sizes the arena as this share of the container's memory
	// limit instead: the line above which checkpoints are refused, once no LP is left.
	MemoryPercentage int `mapstructure:"memory_percentage"`
	// LPPercentage, with MemoryPercentage, is the share of the container's memory limit the
	// arena may fill before LP is refused. It replaces LPLimitPercentage.
	LPPercentage int `mapstructure:"lp_percentage"`
	// CheckpointReservePercentage, with MemoryPercentage, makes the LP limit adaptive: the top of
	// the arena, this share of the container's memory limit scaled by the checkpoint/LP span size
	// ratio (never less than the reserve), is for checkpoints alone, and LP is refused below it.
	// Until the ratio is known, the limit is LPPercentage, or the reserve's lower edge if
	// LPPercentage is not set.
	CheckpointReservePercentage int `mapstructure:"checkpoint_reserve_percentage"`
	// SizeWindow is how much traffic the span sizes are averaged over: seconds that admitted
	// something.
	SizeWindow time.Duration `mapstructure:"size_window"`
	// BlockKiB is the arena's allocation unit; each request uses whole blocks. At least 4.
	BlockKiB int `mapstructure:"block_kib"`
	// LPLimitPercentage is the share of blocks LP may fill; the rest is kept for checkpoints,
	// which evict LP (newest first) once the arena is full.
	LPLimitPercentage int `mapstructure:"lp_limit_percentage"`
	// LPRefusalCode is the gRPC code LP is refused with: "resource_exhausted" (default) or
	// "unavailable". Checkpoints are always refused with Unavailable.
	LPRefusalCode string `mapstructure:"lp_refusal_code"`
	// Mmap maps the arena outside the Go heap (default); false allocates it as a Go slice.
	Mmap bool `mapstructure:"mmap"`

	// Workers is the number of concurrent sends (forward mode) or pipeline calls.
	Workers int `mapstructure:"workers"`
	// MaxBatchSpans and MaxBatchKiB bound one send; queued requests are concatenated up to them.
	MaxBatchSpans int `mapstructure:"max_batch_spans"`
	MaxBatchKiB   int `mapstructure:"max_batch_kib"`
	// Linger lets a send wait for up to this long for the queue to reach MaxBatchSpans.
	// 0 (default) sends whatever is queued at once.
	Linger time.Duration `mapstructure:"linger"`

	// Forward, when its endpoint is set, sends requests on to another OTLP/gRPC server as
	// they are, without decoding them; the pipeline's consumer is then not used. Without
	// it the receiver decodes batches as it sends them into the pipeline.
	Forward ForwardConfig `mapstructure:"forward"`

	MetricsInterval time.Duration `mapstructure:"metrics_interval"`
}

type ForwardConfig struct {
	Endpoint string        `mapstructure:"endpoint"`
	Timeout  time.Duration `mapstructure:"timeout"`
	// A refused or failed send pauses its class (a checkpoint pauses all sends) for a backoff
	// that doubles from BackoffInitial to BackoffMax and resets on success.
	BackoffInitial time.Duration `mapstructure:"backoff_initial"`
	BackoffMax     time.Duration `mapstructure:"backoff_max"`
}

var _ component.Config = (*Config)(nil)

func createDefaultConfig() component.Config {
	grpc := configgrpc.NewDefaultServerConfig()
	grpc.NetAddr = confignet.AddrConfig{Endpoint: "0.0.0.0:4317", Transport: confignet.TransportTypeTCP}
	return &Config{
		GRPC:              grpc,
		MemoryMiB:         128,
		BlockKiB:          4,
		LPLimitPercentage: 90,
		LPRefusalCode:     LPRefusalResourceExhausted,
		Mmap:              true,
		Workers:           10,
		MaxBatchSpans:     8192,
		MaxBatchKiB:       4096,
		MetricsInterval:   time.Second,
		SizeWindow:        30 * time.Second,
		Forward: ForwardConfig{
			Timeout: 5 * time.Second, BackoffInitial: 50 * time.Millisecond, BackoffMax: time.Second,
		},
	}
}

func (c *Config) Validate() error {
	switch {
	case c.BlockKiB < 4 || c.BlockKiB > 1024:
		return errors.New("block_kib must be between 4 and 1024")
	case c.MemoryPercentage == 0 && (c.MemoryMiB <= 0 || c.MemoryMiB<<20 < c.BlockKiB<<10):
		return errors.New("memory_mib must hold at least one block")
	case c.MemoryPercentage == 0 && int64(c.MemoryMiB)<<10/int64(c.BlockKiB) > maxBlocks:
		return fmt.Errorf("memory_mib must not exceed %d blocks", maxBlocks)
	case c.LPLimitPercentage < 0 || c.LPLimitPercentage > 100:
		return errors.New("lp_limit_percentage must be between 0 and 100")
	case c.MemoryPercentage < 0 || c.MemoryPercentage >= 100:
		return errors.New("memory_percentage must be between 0 (use memory_mib) and 99: the rest of the process needs memory too")
	case c.LPPercentage != 0 && (c.MemoryPercentage == 0 || c.LPPercentage < 0 || c.LPPercentage > c.MemoryPercentage):
		return errors.New("lp_percentage needs memory_percentage, and must not exceed it")
	case c.CheckpointReservePercentage < 0 || c.CheckpointReservePercentage > 0 &&
		(c.MemoryPercentage == 0 || c.CheckpointReservePercentage >= c.MemoryPercentage):
		return errors.New("checkpoint_reserve_percentage needs memory_percentage, and must be below it")
	case c.CheckpointReservePercentage > 0 && c.LPPercentage > c.MemoryPercentage-c.CheckpointReservePercentage:
		return errors.New("lp_percentage must not reach into checkpoint_reserve_percentage")
	case c.CheckpointReservePercentage > 0 && (c.SizeWindow < adaptStep || c.SizeWindow > time.Hour):
		return fmt.Errorf("size_window must be between %v and 1h", adaptStep)
	case c.LPRefusalCode != LPRefusalResourceExhausted && c.LPRefusalCode != LPRefusalUnavailable:
		return errors.New(`lp_refusal_code must be "resource_exhausted" or "unavailable"`)
	case c.Workers < 1:
		return errors.New("workers must be at least 1")
	case c.MaxBatchSpans < 1 || c.MaxBatchKiB < 1:
		return errors.New("max_batch_spans and max_batch_kib must be positive")
	case c.Linger < 0 || c.MetricsInterval <= 0:
		return errors.New("linger must not be negative and metrics_interval must be positive")
	case c.Forward.Endpoint != "" && (c.Forward.Timeout <= 0 || c.Forward.BackoffInitial <= 0 || c.Forward.BackoffMax < c.Forward.BackoffInitial):
		return errors.New("forward needs a positive timeout and backoff_initial <= backoff_max")
	}
	return nil
}
