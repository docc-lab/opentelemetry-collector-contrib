// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Tomislav-RetCtx: end-to-end test of pre-decode admission over a real gRPC connection.

package priorityotlpreceiver

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"go.opentelemetry.io/collector/receiver/receivertest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/priorityprocessor"
)

func freePort(t *testing.T) string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())
	return addr
}

func TestPreDecodeAdmission(t *testing.T) {
	var spans atomic.Int64
	var sawLPHeader atomic.Bool
	sink, err := consumer.NewTraces(func(ctx context.Context, td ptrace.Traces) error {
		spans.Add(int64(td.SpanCount()))
		if v := client.FromContext(ctx).Metadata.Get(priorityprocessor.MetadataPriorityKey); len(v) == 1 && v[0] == "lp" {
			sawLPHeader.Store(true)
		}
		return nil
	})
	require.NoError(t, err)

	f := NewFactory()
	cfg := f.CreateDefaultConfig().(*Config)
	addr := freePort(t)
	cfg.GRPC.NetAddr.Endpoint = addr
	r, err := f.CreateTraces(context.Background(), receivertest.NewNopSettings(f.Type()), cfg, sink)
	require.NoError(t, err)
	require.NoError(t, r.Start(context.Background(), componenttest.NewNopHost()))
	defer func() { require.NoError(t, r.Shutdown(context.Background())) }()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	cl := ptraceotlp.NewGRPCClient(conn)
	send := func(priority string) error {
		td := ptrace.NewTraces()
		ss := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty()
		for i := 0; i < 5; i++ {
			ss.Spans().AppendEmpty()
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		ctx = metadata.AppendToOutgoingContext(ctx, priorityprocessor.MetadataPriorityKey, priority)
		_, err := cl.Export(ctx, ptraceotlp.NewExportRequestFromTraces(td))
		return err
	}

	// No controller yet: everything is admitted.
	priorityprocessor.SetGateForTesting("off", codes.Unavailable)
	require.NoError(t, send("lp"))
	assert.Equal(t, int64(5), spans.Load())
	assert.True(t, sawLPHeader.Load(), "the priority header must reach the pipeline's client metadata")

	// Shedding LP with a non-retryable LP code: LP refused before decode, HP admitted.
	priorityprocessor.SetGateForTesting("shedlp", codes.ResourceExhausted)
	err = send("lp")
	assert.Equal(t, codes.ResourceExhausted, status.Code(err), "%v", err)
	assert.Equal(t, int64(5), spans.Load(), "a refused LP request must never reach the pipeline")
	require.NoError(t, send("hp"))
	assert.Equal(t, int64(10), spans.Load())

	// Refuse-all: HP refused (retryable), LP refused with the LP code.
	priorityprocessor.SetGateForTesting("soft", codes.ResourceExhausted)
	assert.Equal(t, codes.Unavailable, status.Code(send("hp")))
	assert.Equal(t, codes.ResourceExhausted, status.Code(send("lp")))
	assert.Equal(t, int64(10), spans.Load())

	// Back to normal.
	priorityprocessor.SetGateForTesting("none", codes.ResourceExhausted)
	require.NoError(t, send("lp"))
	assert.Equal(t, int64(15), spans.Load())
}
