// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package priorityarenareceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/priorityprocessor/priorityarenareceiver"

// Tomislav-RetCtx: the gRPC codec. Requests are never decoded into pdata: an incoming request
// is checked, counted and copied from gRPC's receive buffers into the arena, and an outgoing
// one is handed to gRPC as the arena blocks themselves. It is registered as "proto", so the
// wire format and content type are those of any OTLP client and server.

import (
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/mem"
)

// inbound is what the Export handler decodes into; decoding it admits the request.
type inbound struct {
	s     *store
	hp    bool
	spans int
	code  codes.Code
	msg   string
	lp    codes.Code // refusal code for LP over the limit
}

// outbound is a batch being forwarded.
type outbound struct {
	s *store
	b *batch
}

// exportResponse stands for ExportTraceServiceResponse; it is sent empty and read as such
// (partial success is not used by this pipeline).
type exportResponse struct{}

type arenaCodec struct{}

func (arenaCodec) Name() string { return "proto" }

func (arenaCodec) Marshal(v any) (mem.BufferSlice, error) {
	switch m := v.(type) {
	case *exportResponse:
		return nil, nil
	case *outbound:
		return m.s.lend(m.b), nil
	}
	return nil, fmt.Errorf("priorityarena codec: cannot marshal %T", v)
}

func (arenaCodec) Unmarshal(data mem.BufferSlice, v any) error {
	switch m := v.(type) {
	case *inbound:
		m.admit(data)
		return nil
	case *exportResponse:
		return nil
	}
	return fmt.Errorf("priorityarena codec: cannot unmarshal into %T", v)
}

// admit counts the request's spans, decides admission and copies it into the arena. gRPC frees
// data when this returns.
func (in *inbound) admit(data mem.BufferSlice) {
	size := data.Len()
	if size == 0 {
		return
	}
	var arr [16][]byte
	chunks := arr[:0]
	for _, b := range data {
		chunks = append(chunks, b.ReadOnlyData())
	}
	spans, err := countSpans(chunks, size)
	if err != nil {
		in.s.c.malformed.Add(1)
		in.code, in.msg = codes.InvalidArgument, err.Error()
		return
	}
	in.spans = spans
	if spans == 0 {
		return
	}
	id, head, code, msg := in.s.reserve(in.hp, size, spans)
	if code != codes.OK {
		if code == codes.ResourceExhausted && !in.hp {
			code = in.lp
		}
		in.code, in.msg = code, msg
		return
	}
	in.s.a.fill(head, chunks)
	in.s.commit(id)
}
