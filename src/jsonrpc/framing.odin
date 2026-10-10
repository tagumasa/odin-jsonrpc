// Message framing for the JSON-RPC faces: Content-Length headers (the
// LSP-style stream framing, header block strictly \r\n-terminated) and
// newline-delimited JSON (the MCP stdio face — the MCP specification
// delimits stdio messages by newlines and forbids embedded ones).
// Reader and Writer are function-pointer abstractions so real fds,
// in-memory pipes, and test fakes all use the same path.
package jsonrpc

import "core:fmt"
import "core:mem"
import "core:strings"

Read_Err :: enum {
	None,
	Eof,       // orderly end of stream
	Closed,    // local close
	Framing,   // malformed header block
	Too_Large, // Content-Length above the connection-class cap
	Io,
}

DEFAULT_MAX_FRAME :: 64 * 1024 * 1024 // the connection-class default; hosts pick their own cap

// The header block's own bound: a header section is a few short lines by
// nature, and a stream that never produces the separator (a wedged peer
// streaming garbage) must not grow the receive buffer without bound —
// the frame cap governs bodies, not the pre-header wait.
HEADER_MAX_BYTES :: int(8 * 1024)

// Bytes requested per fill/buffered-body read: big enough that a full
// pipe write crosses in one call, small enough that a dribbling source
// does not over-reserve.
READ_CHUNK_BYTES :: 512

// Rx_Buf is an explicit byte accumulator (length <= capacity, data owned
// by the Reader).
Rx_Buf :: struct {
	data: []u8,
	n:    int,
}

// rx_reserve grows through the caller-supplied allocator: the Reader runs
// on host threads whose context allocator differs from the one that owns
// the buffer.
rx_reserve :: proc(b: ^Rx_Buf, want: int, a: mem.Allocator) {
	if want <= len(b.data) {
		return
	}
	grown := make([]u8, max(want, len(b.data) * 2), a)
	if b.n > 0 {
		mem.copy_non_overlapping(&grown[0], &b.data[0], b.n)
	}
	if b.data != nil {
		delete(b.data, a)
	}
	b.data = grown
}

rx_reset :: proc(b: ^Rx_Buf) {
	b.n = 0
}

rx_consume :: proc(b: ^Rx_Buf, count: int) {
	rest := b.n - count
	if rest > 0 {
		// Source and destination overlap inside the same buffer; copy (not
		// copy_non_overlapping) has the move semantics this needs.
		mem.copy(&b.data[0], &b.data[count], rest)
	}
	b.n = rest
}

Framing :: enum {
	Header,  // Content-Length headers: internal RPC, LSP
	Newline, // newline-delimited JSON: MCP stdio
}

Reader :: struct {
	read_fn:         proc(data: rawptr, buf: []u8) -> (int, Read_Err),
	data:            rawptr,
	max_frame_bytes: int,
	framing:         Framing,
	buf:             Rx_Buf,
	// Newline framing only: the buffered prefix below this offset holds
	// no '\n' (scanned by earlier fills of the same frame), so a refill
	// resumes the scan here instead of rescanning from the start — a
	// dribbling source costs one scan per byte, not one per byte per
	// fill. Reset at each frame's read start.
	nl_scan_from:    int,
	allocator:       mem.Allocator,
}

reader_init :: proc(r: ^Reader, read_fn: proc(data: rawptr, buf: []u8) -> (int, Read_Err), data: rawptr, max_frame_bytes: int, a := context.allocator) {
	r^ = {
		read_fn         = read_fn,
		data            = data,
		max_frame_bytes = max_frame_bytes,
		framing         = .Header,
		allocator       = a,
	}
}

// reader_init_ndjson prepares the MCP stdio framing: one message per
// line (a trailing CR is tolerated; embedded newlines are the sender's
// spec violation and end the frame early).
reader_init_ndjson :: proc(r: ^Reader, read_fn: proc(data: rawptr, buf: []u8) -> (int, Read_Err), data: rawptr, max_frame_bytes: int, a := context.allocator) {
	reader_init(r, read_fn, data, max_frame_bytes, a)
	r.framing = .Newline
}

reader_destroy :: proc(r: ^Reader) {
	if r.buf.data != nil {
		delete(r.buf.data, r.allocator)
	}
	r^ = {}
}

// fill reads at least one more byte into the buffer.
fill :: proc(r: ^Reader) -> Read_Err {
	rx_reserve(&r.buf, r.buf.n + READ_CHUNK_BYTES, r.allocator)
	n, err := r.read_fn(r.data, r.buf.data[r.buf.n:])
	if n > 0 {
		r.buf.n += n
		return .None
	}
	// A read_fn reports zero bytes only together with an error; a
	// progress-less success would spin every read loop forever, so it
	// folds into .Io — the same guard write_all applies to the write side.
	if err == .None {
		return .Io
	}
	return err
}

// read_frame returns the next complete body, allocated from `a`. Whatever
// follows the frame stays buffered for the next call.
read_frame :: proc(r: ^Reader, a: mem.Allocator) -> (body: []u8, err: Read_Err) {
	if r.framing == .Newline {
		return read_line_frame(r, a)
	}
	header_end := 0
	content_length := -1

	for {
		he, cl, ok := try_parse_header(r.buf.data[:r.buf.n])
		if ok == 1 {
			header_end = he
			content_length = cl
			break
		}
		if ok == -1 {
			rx_reset(&r.buf)
			return nil, .Framing
		}
		if r.buf.n >= HEADER_MAX_BYTES {
			// No separator within a generously sized header block: a
			// broken or hostile peer, fatal to the read like any other
			// framing violation.
			rx_reset(&r.buf)
			return nil, .Framing
		}
		if ferr := fill(r); ferr != .None {
			if ferr == .Eof && r.buf.n == 0 {
				return nil, .Eof
			}
			return nil, ferr
		}
	}

	if content_length > r.max_frame_bytes {
		rx_reset(&r.buf)
		return nil, .Too_Large
	}
	total := header_end + content_length
	for r.buf.n < total {
		rx_reserve(&r.buf, max(r.buf.n + READ_CHUNK_BYTES, total), r.allocator)
		n, rerr := r.read_fn(r.data, r.buf.data[r.buf.n:total])
		if n > 0 {
			r.buf.n += n
			continue
		}
		if rerr != .None {
			return nil, rerr
		}
		// Same progress-less guard as fill: zero bytes with no error
		// would spin this loop forever.
		return nil, .Io
	}

	body = make([]u8, content_length, a)
	if content_length > 0 {
		mem.copy_non_overlapping(&body[0], &r.buf.data[header_end], content_length)
	}
	rx_consume(&r.buf, total)
	return body, .None
}

// read_line_frame is the newline-delimited read: the body is one line
// without its terminator; a trailing CR is stripped.
read_line_frame :: proc(r: ^Reader, a: mem.Allocator) -> (body: []u8, err: Read_Err) {
	r.nl_scan_from = 0
	for {
		nl := strings.index_byte(string(r.buf.data[r.nl_scan_from:r.buf.n]), '\n')
		if nl >= 0 {
			nl += r.nl_scan_from
			end := nl
			if end > 0 && r.buf.data[end - 1] == '\r' {
				end -= 1
			}
			if end > r.max_frame_bytes {
				rx_reset(&r.buf)
				return nil, .Too_Large
			}
			body = make([]u8, end, a)
			if end > 0 {
				mem.copy_non_overlapping(&body[0], &r.buf.data[0], end)
			}
			rx_consume(&r.buf, nl + 1)
			return body, .None
		}
		// No newline buffered: the next fill's scan resumes where this
		// one stopped (the scanned prefix stays valid — fill only
		// appends).
		r.nl_scan_from = r.buf.n
		if r.buf.n > r.max_frame_bytes {
			rx_reset(&r.buf)
			return nil, .Too_Large
		}
		if ferr := fill(r); ferr != .None {
			if ferr == .Eof && r.buf.n == 0 {
				return nil, .Eof
			}
			// A partial line at EOF is not a message.
			return nil, ferr
		}
	}
}

// try_parse_header returns (header_end, content_length, 1) when a full
// header block with a valid Content-Length is buffered, (0, 0, 0) when more
// bytes are needed, and (_, _, -1) on a malformed header block.
try_parse_header :: proc(buf: []u8) -> (header_end: int, content_length: int, ok: int) {
	sep := strings.index(string(buf), HEADER_SEP)
	if sep < 0 {
		return 0, 0, 0
	}
	header_end = sep + len(HEADER_SEP)

	headers := buf[:sep]
	content_length = -1
	line_start := 0
	for i := 0; i <= len(headers); i += 1 {
		if i == len(headers) || headers[i] == '\n' {
			line := headers[line_start:i]
			if len(line) > 0 && line[len(line) - 1] == '\r' {
				line = line[:len(line) - 1]
			}
			if len(line) > 0 {
				colon := strings.index(string(line), ":")
				if colon <= 0 {
					return 0, 0, -1
				}
				value := strings.trim_space(string(line[colon + 1:]))
				if ascii_equal_ci(strings.trim_space(string(line[:colon])), "content-length") {
					n, valid := parse_decimal(value)
					if !valid {
						return 0, 0, -1
					}
					if content_length != -1 {
						return 0, 0, -1 // duplicate header
					}
					content_length = n
				}
			}
			line_start = i + 1
		}
	}
	if content_length < 0 {
		return 0, 0, -1
	}
	return header_end, content_length, 1
}

HEADER_SEP :: string("\r\n\r\n")

// ascii_equal_ci compares without allocating: header parsing runs per frame
// and must not grow any allocator.
ascii_equal_ci :: proc(a: string, b: string) -> bool {
	if len(a) != len(b) {
		return false
	}
	for i in 0..<len(a) {
		ca := a[i]
		cb := b[i]
		if ca >= 'A' && ca <= 'Z' {
			ca = ca + ('a' - 'A')
		}
		if cb >= 'A' && cb <= 'Z' {
			cb = cb + ('a' - 'A')
		}
		if ca != cb {
			return false
		}
	}
	return true
}

// parse_decimal accepts digit-only decimal text (a leading sign is a
// spec violation) and bounds the accumulator before it can overflow.
parse_decimal :: proc(s: string) -> (int, bool) {
	if len(s) == 0 {
		return 0, false
	}
	n := 0
	for c in s {
		if c < '0' || c > '9' {
			return 0, false
		}
		// Bound before the multiply: 20-digit inputs would overflow i64,
		// wrap negative, and slip past the post-multiply check.
		if n > (1 << 40) / 10 {
			return 0, false
		}
		n = n * 10 + int(c - '0')
		if n > 1 << 40 {
			return 0, false
		}
	}
	return n, true
}

// ---------------------------------------------------------------------------

Writer :: struct {
	write_fn: proc(data: rawptr, buf: []u8) -> (int, Read_Err),
	data:     rawptr,
	framing:  Framing,
}

writer_init :: proc(w: ^Writer, write_fn: proc(data: rawptr, buf: []u8) -> (int, Read_Err), data: rawptr) {
	w^ = {
		write_fn = write_fn,
		data     = data,
		framing  = .Header,
	}
}

// writer_init_ndjson prepares the MCP stdio writer: the body followed by
// one newline (the serializer emits single-line JSON).
writer_init_ndjson :: proc(w: ^Writer, write_fn: proc(data: rawptr, buf: []u8) -> (int, Read_Err), data: rawptr) {
	writer_init(w, write_fn, data)
	w.framing = .Newline
}

write_frame :: proc(w: ^Writer, body: []u8) -> Read_Err {
	if w.framing == .Newline {
		if err := write_all(w, body); err != .None {
			return err
		}
		nl := []u8{'\n'}
		return write_all(w, nl)
	}
	// The label (16) plus a 19-digit i64 plus the separator (4) fit forty
	// bytes — the header needs no allocator at all.
	hdr_buf: [40]u8
	header := fmt.bprintf(hdr_buf[:], "Content-Length: %d\r\n\r\n", len(body))
	if err := write_all(w, transmute([]u8)header); err != .None {
		return err
	}
	if len(body) > 0 {
		return write_all(w, body)
	}
	return .None
}

write_all :: proc(w: ^Writer, data: []u8) -> Read_Err {
	sent := 0
	for sent < len(data) {
		n, err := w.write_fn(w.data, data[sent:])
		if n > 0 {
			sent += n
		}
		if err != .None {
			return err
		}
		if n <= 0 {
			return .Io
		}
	}
	return .None
}
