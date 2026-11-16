// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package priorityarenareceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/priorityprocessor/priorityarenareceiver"

// Tomislav-RetCtx: counts the spans in an encoded ExportTraceServiceRequest without decoding it.
// The request may be split across arena blocks. Only the message structure is checked:
// lengths, wire types and nesting down to ScopeSpans; span bodies are skipped.

import "errors"

var errMalformed = errors.New("malformed ExportTraceServiceRequest")

// OTLP field numbers on the path to a span.
const (
	fieldResourceSpans = 1 // ExportTraceServiceRequest.resource_spans
	fieldScopeSpans    = 2 // ResourceSpans.scope_spans
	fieldSpans         = 2 // ScopeSpans.spans
)

type wireReader struct {
	chunks [][]byte
	cur    []byte
	next   int
}

func (r *wireReader) reset(chunks [][]byte) {
	r.chunks, r.cur, r.next = chunks, nil, 0
}

func (r *wireReader) byte() (byte, bool) {
	for len(r.cur) == 0 {
		if r.next == len(r.chunks) {
			return 0, false
		}
		r.cur = r.chunks[r.next]
		r.next++
	}
	b := r.cur[0]
	r.cur = r.cur[1:]
	return b, true
}

// varint reads a base-128 varint and returns it with its length in bytes.
func (r *wireReader) varint() (uint64, int, bool) {
	var v uint64
	for i := 0; i < 10; i++ {
		b, ok := r.byte()
		if !ok {
			return 0, 0, false
		}
		v |= uint64(b&0x7f) << (7 * i)
		if b < 0x80 {
			return v, i + 1, true
		}
	}
	return 0, 0, false
}

func (r *wireReader) skip(n uint64) bool {
	for n > 0 {
		if len(r.cur) == 0 {
			if r.next == len(r.chunks) {
				return false
			}
			r.cur = r.chunks[r.next]
			r.next++
			continue
		}
		k := uint64(len(r.cur))
		if k > n {
			k = n
		}
		r.cur = r.cur[k:]
		n -= k
	}
	return true
}

// countSpans returns the number of spans in a request of size bytes held in chunks.
func countSpans(chunks [][]byte, size int) (int, error) {
	var r wireReader
	r.reset(chunks)
	spans := 0
	err := r.message(uint64(size), 0, &spans)
	return spans, err
}

// message walks size bytes of one message at depth 0 (request), 1 (ResourceSpans) or
// 2 (ScopeSpans), counting the spans of every ScopeSpans it reaches.
func (r *wireReader) message(size uint64, depth int, spans *int) error {
	for size > 0 {
		tag, n, ok := r.varint()
		if !ok || uint64(n) > size {
			return errMalformed
		}
		size -= uint64(n)
		field, wire := tag>>3, tag&7
		if field == 0 {
			return errMalformed
		}
		var skip uint64
		switch wire {
		case 0:
			_, n, ok := r.varint()
			if !ok || uint64(n) > size {
				return errMalformed
			}
			size -= uint64(n)
			continue
		case 1:
			skip = 8
		case 5:
			skip = 4
		case 2:
			l, n, ok := r.varint()
			if !ok || uint64(n) > size || l > size-uint64(n) {
				return errMalformed
			}
			size -= uint64(n) + l
			switch {
			case depth == 0 && field == fieldResourceSpans, depth == 1 && field == fieldScopeSpans:
				if err := r.message(l, depth+1, spans); err != nil {
					return err
				}
				continue
			case depth == 2 && field == fieldSpans:
				*spans++
			}
			if !r.skip(l) {
				return errMalformed
			}
			continue
		default: // groups (3, 4) are not used by OTLP
			return errMalformed
		}
		if skip > size || !r.skip(skip) {
			return errMalformed
		}
		size -= skip
	}
	return nil
}
