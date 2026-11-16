// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package priorityarenareceiver

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// randomTraces builds a request with a random shape: resource spans, scope spans, spans,
// attributes, events and links, so every skipped field type appears.
func randomTraces(r *rand.Rand, maxSpans int) ptrace.Traces {
	td := ptrace.NewTraces()
	for i := r.Intn(3) + 1; i > 0; i-- {
		rs := td.ResourceSpans().AppendEmpty()
		rs.Resource().Attributes().PutStr("service.name", "svc")
		rs.SetSchemaUrl("https://example.com/schema")
		for j := r.Intn(3); j >= 0; j-- {
			ss := rs.ScopeSpans().AppendEmpty()
			ss.Scope().SetName("scope")
			for k := r.Intn(maxSpans + 1); k > 0; k-- {
				sp := ss.Spans().AppendEmpty()
				sp.SetName("op")
				sp.SetTraceID(pcommon.TraceID{byte(r.Intn(255) + 1)})
				sp.SetSpanID(pcommon.SpanID{byte(r.Intn(255) + 1)})
				sp.SetStartTimestamp(pcommon.Timestamp(r.Int63()))
				sp.SetKind(ptrace.SpanKindServer)
				sp.Attributes().PutInt("n", r.Int63())
				sp.Attributes().PutDouble("d", r.Float64())
				sp.Attributes().PutEmptyBytes("_br").FromRaw(make([]byte, r.Intn(40)))
				if r.Intn(4) == 0 {
					sp.Events().AppendEmpty().SetName("ev")
					sp.Links().AppendEmpty().SetSpanID(pcommon.SpanID{1})
				}
			}
		}
	}
	return td
}

func marshal(t testing.TB, td ptrace.Traces) []byte {
	b, err := (&ptrace.ProtoMarshaler{}).MarshalTraces(td)
	require.NoError(t, err)
	return b
}

// split cuts b into pieces of random length, as gRPC buffers and arena blocks do.
func split(r *rand.Rand, b []byte) [][]byte {
	var out [][]byte
	for len(b) > 0 {
		n := min(r.Intn(17)+1, len(b))
		out = append(out, b[:n])
		b = b[n:]
	}
	return out
}

func TestCountSpans(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	for i := 0; i < 500; i++ {
		td := randomTraces(r, 20)
		b := marshal(t, td)
		n, err := countSpans(split(r, b), len(b))
		require.NoError(t, err)
		require.Equal(t, td.SpanCount(), n)
	}
}

func TestCountSpansConcatenated(t *testing.T) {
	// Concatenated requests are one request with the spans of both; the forwarder relies on it.
	r := rand.New(rand.NewSource(2))
	a, b := randomTraces(r, 10), randomTraces(r, 10)
	ab := append(marshal(t, a), marshal(t, b)...)
	n, err := countSpans(split(r, ab), len(ab))
	require.NoError(t, err)
	require.Equal(t, a.SpanCount()+b.SpanCount(), n)
	td, err := (&ptrace.ProtoUnmarshaler{}).UnmarshalTraces(ab)
	require.NoError(t, err)
	require.Equal(t, n, td.SpanCount())
}

func TestCountSpansMalformed(t *testing.T) {
	r := rand.New(rand.NewSource(3))
	b := marshal(t, randomTraces(r, 5))
	for _, bad := range [][]byte{
		b[:len(b)-1],                    // truncated inside the last message
		{0x0a, 0x05, 0x12},              // length beyond the end
		{0x0b},                          // group wire type
		{0x00, 0x01},                    // field number 0
		{0x0a, 0x80, 0x80},              // unterminated length varint
		append(b[:len(b):len(b)], 0x08), // varint field with no value
	} {
		_, err := countSpans([][]byte{bad}, len(bad))
		require.ErrorIs(t, err, errMalformed, "%x", bad)
	}
}

// FuzzCountSpans: when both the scan and pdata accept a request, they count the same spans.
// (pdata also accepts some malformed groups, which no OTLP sender produces; the scan refuses
// them. What the scan accepts and pdata does not is dropped per request by the pipeliner.)
func FuzzCountSpans(f *testing.F) {
	r := rand.New(rand.NewSource(4))
	for i := 0; i < 8; i++ {
		f.Add(marshal(f, randomTraces(r, 4)))
	}
	f.Add([]byte("C2\f000000000000"))
	f.Fuzz(func(t *testing.T, b []byte) {
		td, err := (&ptrace.ProtoUnmarshaler{}).UnmarshalTraces(b)
		n, scanErr := countSpans(split(rand.New(rand.NewSource(int64(len(b)))), b), len(b))
		if err == nil && scanErr == nil {
			require.Equal(t, td.SpanCount(), n)
		}
	})
}
