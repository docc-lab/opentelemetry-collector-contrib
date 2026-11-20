// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package priorityarenareceiver

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
)

// admitted counts spans and bytes as admitted, as commit does.
func admitted(s *store, hpSpans, hpBytes, lpSpans, lpBytes int64) {
	s.c.hpAdmitted.Add(hpSpans)
	s.c.hpAdmittedB.Add(hpBytes)
	s.c.lpAdmitted.Add(lpSpans)
	s.c.lpAdmittedB.Add(lpBytes)
}

func TestLPLimitFollowsSizeRatio(t *testing.T) {
	s := testStore(t, 100, 50) // LP at 50 blocks until the ratio is known
	a := newLPAdapter(s, 10, 3, zap.NewNop())

	a.step() // nothing admitted: nothing learnt
	require.Equal(t, int32(50), s.lpLimit.Load())
	require.True(t, math.IsNaN(a.ratioNow()))
	for i := 0; i < 2; i++ {
		admitted(s, 10, 1500, 20, 2000) // checkpoints 150 bytes per span, LP 100
		a.step()
	}
	require.Equal(t, int32(50), s.lpLimit.Load(), "the window is not full yet")
	admitted(s, 10, 1500, 20, 2000)
	a.step()
	require.Equal(t, 1.5, a.ratioNow())
	require.Equal(t, int32(85), s.lpLimit.Load(), "a band of 10 x 1.5 blocks")

	// Refusals do not stop it: sizes per span are not biased by them.
	s.c.lpRefusedReq.Add(5)
	for i := 0; i < 3; i++ {
		admitted(s, 10, 2000, 20, 2000)
		a.step()
	}
	require.Equal(t, 2.0, a.ratioNow())
	require.Equal(t, int32(80), s.lpLimit.Load())

	// Checkpoints smaller than LP: the band stays at the reserve.
	for i := 0; i < 3; i++ {
		admitted(s, 10, 800, 20, 2000)
		a.step()
	}
	require.InDelta(t, 0.8, a.ratioNow(), 1e-12)
	require.Equal(t, int32(90), s.lpLimit.Load())

	// A window without checkpoints has no ratio: the limit stays.
	for i := 0; i < 3; i++ {
		admitted(s, 0, 0, 20, 2000)
		a.step()
	}
	require.Equal(t, int32(90), s.lpLimit.Load())
}

func TestLoweredLPLimitEvictsNothing(t *testing.T) {
	s := testStore(t, 10, 90)
	for i := 0; i < 6; i++ {
		_, code := put(t, s, false, 1)
		require.Equal(t, codes.OK, code)
	}
	s.lpLimit.Store(3)
	require.Len(t, queued(s, &s.lp), 6, "LP above the new limit stays")
	_, code := put(t, s, false, 1)
	require.Equal(t, codes.ResourceExhausted, code, "new LP is refused")
	require.True(t, s.refuseLPEarly())
	_, code = put(t, s, true, 4)
	require.Equal(t, codes.OK, code)
	require.Zero(t, s.c.hpByEviction.Load())
	require.Zero(t, s.c.lpEvicted.Load())
}

// sizedStore is a 100-block store whose adapter has learnt a size ratio of 1.5 with a reserve of 10 blocks: LP is
// refused from 85 blocks in use.
func sizedStore(t *testing.T) *store {
	s := testStore(t, 100, 50)
	a := newLPAdapter(s, 10, 1, zap.NewNop())
	admitted(s, 10, 1500, 20, 2000)
	a.step()
	require.Equal(t, int32(85), s.lpLimit.Load())
	return s
}

// A downstream that still takes checkpoints leaves only LP queued: LP gets all of the arena below the band.
func TestLPAloneFillsToTheBand(t *testing.T) {
	s := sizedStore(t)
	lp := 0
	for {
		if _, code := put(t, s, false, 1); code != codes.OK {
			break
		}
		lp++
	}
	require.Equal(t, 85, lp)
}

// A downstream that takes nothing: LP stops at the band, the band takes checkpoints with no eviction, and only then do
// checkpoints evict LP.
func TestBandTakesCheckpointsAfterLPStops(t *testing.T) {
	s := sizedStore(t)
	hp, lp := 0, 0
	for refused := false; !refused; {
		_, code := put(t, s, true, 1)
		require.Equal(t, codes.OK, code)
		hp++
		for i := 0; i < 3 && !refused; i++ {
			if _, code := put(t, s, false, 1); code != codes.OK {
				refused = true
			} else {
				lp++
			}
		}
	}
	require.Equal(t, 63, lp)
	require.Equal(t, 22, hp)
	require.Equal(t, int32(15), s.a.free, "the band is untouched")
	for i := 0; i < 15; i++ {
		_, code := put(t, s, true, 1)
		require.Equal(t, codes.OK, code)
	}
	require.Zero(t, s.c.hpByEviction.Load(), "the band took them")
	_, code := put(t, s, true, 1)
	require.Equal(t, codes.OK, code)
	require.Equal(t, int64(1), s.c.hpByEviction.Load(), "then checkpoints evict LP, as before")
}

func TestSizingAdaptive(t *testing.T) {
	limit := func() (uint64, error) { return 256 << 20, nil }
	c := createDefaultConfig().(*Config)
	c.MemoryPercentage, c.CheckpointReservePercentage = 70, 10
	require.NoError(t, c.Validate())
	sz, err := c.sizing(limit, zap.NewNop())
	require.NoError(t, err)
	require.InDelta(t, 100*10.0/70, sz.bandPct, 1e-9)
	require.InDelta(t, 100*60.0/70, sz.lpPct, 1e-9, "no lp_percentage: start at the reserve's edge")

	c.LPPercentage = 40
	require.NoError(t, c.Validate())
	sz, err = c.sizing(limit, zap.NewNop())
	require.NoError(t, err)
	require.InDelta(t, 100*40.0/70, sz.lpPct, 1e-9)

	for name, mutate := range map[string]func(*Config){
		"lp_percentage in the reserve": func(c *Config) { c.LPPercentage = 65 },
		"reserve the whole arena":      func(c *Config) { c.LPPercentage, c.CheckpointReservePercentage = 0, 70 },
		"no memory_percentage":         func(c *Config) { c.LPPercentage, c.MemoryPercentage = 0, 0 },
		"negative reserve":             func(c *Config) { c.LPPercentage, c.CheckpointReservePercentage = 0, -1 },
		"no window":                    func(c *Config) { c.LPPercentage, c.SizeWindow = 0, 0 },
	} {
		c := createDefaultConfig().(*Config)
		c.MemoryPercentage, c.CheckpointReservePercentage, c.LPPercentage = 70, 10, 40
		mutate(c)
		require.Error(t, c.Validate(), name)
	}
}

// sizedRequest is a request of n spans whose bridge attribute has br bytes.
func sizedRequest(t *testing.T, hp bool, n, br int) []byte {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutBool("hp", hp)
	ss := rs.ScopeSpans().AppendEmpty()
	for i := 0; i < n; i++ {
		sp := ss.Spans().AppendEmpty()
		sp.SetName("op")
		sp.Attributes().PutEmptyBytes("_br").FromRaw(make([]byte, br))
	}
	return marshal(t, td)
}

// Over gRPC: checkpoints with larger spans than LP, one checkpoint request for three LP ones. The band follows the size
// ratio per span, whatever the share of checkpoints.
func TestAdaptiveLPLimitOverGRPC(t *testing.T) {
	oldLimit, oldStep := memoryLimit, adaptStep
	memoryLimit = func() (uint64, error) { return 16 << 20, nil }
	adaptStep = 10 * time.Millisecond
	t.Cleanup(func() { memoryLimit, adaptStep = oldLimit, oldStep })

	sink := new(consumertest.TracesSink)
	gw := start(t, func(c *Config) {
		c.MemoryPercentage, c.CheckpointReservePercentage, c.LPPercentage = 70, 10, 30
		c.SizeWindow = 50 * time.Millisecond
		c.Mmap = false
	}, sink)
	defer gw.Shutdown(context.Background())
	s := gw.store
	require.NotNil(t, gw.adapter)
	require.Equal(t, int32(float64(s.a.nblocks)*30/70), s.lpLimit.Load(), "lp_percentage until the ratio is known")

	c := dial(t, gw)
	reqs := [2][]byte{sizedRequest(t, false, 10, 24), sizedRequest(t, true, 10, 200)}
	want := float64(len(reqs[1])) / float64(len(reqs[0]))
	sent := 0
	for deadline := time.Now().Add(300 * time.Millisecond); time.Now().Before(deadline); sent++ {
		hp, i := sent%4 == 0, 0
		if hp {
			i = 1
		}
		require.NoError(t, c.export(hp, reqs[i]))
	}
	require.Eventually(t, func() bool { return sink.SpanCount() == 10*sent }, 5*time.Second, 10*time.Millisecond)
	time.Sleep(5 * adaptStep) // idle steps change nothing
	require.InDelta(t, want, gw.adapter.ratioNow(), 1e-9)
	band := float64(s.a.nblocks) * 10 / 70 * want
	require.InDelta(t, float64(s.a.nblocks)-band, float64(s.lpLimit.Load()), 1)
}
