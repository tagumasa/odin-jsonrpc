// The wire-path bench: the inbound face (framing, sanity scan, parse,
// dispatch) over in-memory feeds, measured wall-clock (best-of-5) plus
// deterministic callgrind instruction counts (one scenario per run, so
// the count is the scenario's alone). A host tool in conformance's
// shape: not part of `just test`, no assertions — it prints numbers.
package bench

import "core:fmt"
import "core:mem"
import "core:os"
import "core:time"

import "jsonrpc:jsonrpc"

// Feed serves a fixed byte blob through the Reader's read_fn in fixed
// chunks (0 = whatever the buffer takes, the whole-remaining shape).
Feed :: struct {
	data:  []u8,
	pos:   int,
	chunk: int,
}

feed_read :: proc(data: rawptr, buf: []u8) -> (int, jsonrpc.Read_Err) {
	f := cast(^Feed)data
	rest := len(f.data) - f.pos
	if rest <= 0 {
		return 0, .Eof
	}
	n := rest
	if f.chunk > 0 && n > f.chunk {
		n = f.chunk
	}
	if n > len(buf) {
		n = len(buf)
	}
	mem.copy_non_overlapping(&buf[0], &f.data[f.pos], n)
	f.pos += n
	return n, .None
}

// sink_write accepts every byte: replies to nowhere (the conn's handler
// table stays empty; unknown notifications are dropped without a reply).
sink_write :: proc(data: rawptr, buf: []u8) -> (int, jsonrpc.Read_Err) {
	return len(buf), .None
}

// run_inbound feeds `blob` (one NDJSON message per line) through the
// public read_frame + conn_handle_body pair, one arena per message —
// conn_read_loop's exact shape minus the thread. Returns the message
// count (so a broken feed cannot read as a fast one).
run_inbound :: proc(blob: []u8, chunk: int, a := context.allocator) -> int {
	frames := 0
	r: jsonrpc.Reader
	jsonrpc.reader_init_ndjson(&r, feed_read, nil, jsonrpc.DEFAULT_MAX_FRAME, a)
	f := Feed{data = blob, chunk = chunk}
	r.data = &f
	w: jsonrpc.Writer
	jsonrpc.writer_init_ndjson(&w, sink_write, nil)
	c := new(jsonrpc.Conn, a)
	jsonrpc.conn_init(c, r, w, a)
	defer jsonrpc.conn_destroy(c)
	defer free(c, a)

	for {
		arena: mem.Dynamic_Arena
		mem.dynamic_arena_init(&arena, a, block_size = jsonrpc.MESSAGE_ARENA_BLOCK_SIZE)
		body, err := jsonrpc.read_frame(&c.reader, mem.dynamic_arena_allocator(&arena))
		if err != .None {
			mem.dynamic_arena_destroy(&arena)
			break
		}
		keep := jsonrpc.conn_handle_body(c, body, mem.dynamic_arena_allocator(&arena))
		mem.dynamic_arena_destroy(&arena)
		if !keep {
			break
		}
		frames += 1
	}
	return frames
}

// run_framing feeds one long line and stops at the frame boundary —
// read_frame alone, no parse: the framing-and-copy cost per feed shape.
run_framing :: proc(blob: []u8, chunk: int, a := context.allocator) -> int {
	r: jsonrpc.Reader
	jsonrpc.reader_init_ndjson(&r, feed_read, nil, jsonrpc.DEFAULT_MAX_FRAME, a)
	f := Feed{data = blob, chunk = chunk}
	r.data = &f
	a2: mem.Dynamic_Arena
	mem.dynamic_arena_init(&a2, a)
	body, err := jsonrpc.read_frame(&r, mem.dynamic_arena_allocator(&a2))
	n := err == .None ? len(body) : 0
	mem.dynamic_arena_destroy(&a2)
	return n
}

// ---------------------------------------------------------------------------
// Payload builders — deterministic (a fixed LCG, a fixed seed): every run
// of the same scenario feeds byte-identical input. The blobs are built
// into the scenario arena, which dies with the repetition.
// ---------------------------------------------------------------------------

Buf :: struct {
	d: [dynamic]u8,
}

buf_init :: proc(b: ^Buf, cap: int, a: mem.Allocator) {
	b.d = make([dynamic]u8, 0, cap, a)
}

buf_str :: proc(b: ^Buf, s: string) {
	append(&b.d, ..transmute([]u8)s)
}

buf_byte :: proc(b: ^Buf, c: u8) {
	append(&b.d, c)
}

buf_bytes :: proc(b: ^Buf) -> []u8 {
	return b.d[:]
}

Lcg :: struct {
	state: u32,
}

lcg_pick :: proc(l: ^Lcg, n: int) -> int {
	l.state = l.state * 1103515245 + 12345
	return int((l.state >> 16) & 0x7FFF) % n
}

WORDS :: []string{
	"protocol", "session", "framing", "envelope", "dispatch", "correlation",
	"monotonic", "deadline", "cancellation", "negotiation", "capability",
	"transport", "stream", "payload", "registry", "backpressure",
}

append_ascii_text :: proc(out: ^Buf, l: ^Lcg, bytes: int) {
	words := WORDS // constant data is indexed through a materialized local
	written := 0
	for written < bytes {
		w := words[lcg_pick(l, len(words))]
		if written > 0 {
			buf_byte(out, ' ')
			written += 1
		}
		buf_str(out, w)
		written += len(w)
	}
}

// CJK sample sentences: three-byte UTF-8 sequences dominate, the shape a
// Japanese tool result carries.
CJK :: []string{
	"形態素解析は辞書とコストの連鎖で文を区切る。",
	"接続の候補の中から最小コストの経路を選ぶ。",
	"辞書の読みはバイト列のまま比較されてよい。",
	"逐語訳ではなく、文全体の対訳を返すこと。",
	"改行は正規化してから送出する。",
}

append_cjk_text :: proc(out: ^Buf, l: ^Lcg, bytes: int) {
	sentences := CJK // constant data is indexed through a materialized local
	written := 0
	for written < bytes {
		s := sentences[lcg_pick(l, len(sentences))]
		buf_str(out, s)
		written += len(s)
	}
}

// Escaped text: a quote, backslash, or newline escape every few words.
append_escaped_text :: proc(out: ^Buf, l: ^Lcg, bytes: int) {
	words := WORDS // constant data is indexed through a materialized local
	written := 0
	for written < bytes {
		w := words[lcg_pick(l, len(words))]
		buf_str(out, w)
		written += len(w)
		switch lcg_pick(l, 3) {
		case 0:
			buf_str(out, "\\\"")
		case 1:
			buf_str(out, "\\\\")
		case:
			buf_str(out, "\\n")
		}
		written += 2
	}
}

Text_Kind :: enum {
	Ascii,
	Cjk,
	Escaped,
}

append_text :: proc(out: ^Buf, l: ^Lcg, bytes: int, kind: Text_Kind) {
	switch kind {
	case .Ascii:
		append_ascii_text(out, l, bytes)
	case .Cjk:
		append_cjk_text(out, l, bytes)
	case .Escaped:
		append_escaped_text(out, l, bytes)
	}
}

itoa_into :: proc(out: ^Buf, v: int) {
	buf: [24]u8
	n := 0
	x := v
	if x == 0 {
		buf_byte(out, '0')
		return
	}
	for x > 0 {
		buf[n] = u8('0' + x % 10)
		n += 1
		x /= 10
	}
	for i := n - 1; i >= 0; i -= 1 {
		buf_byte(out, buf[i])
	}
}

build_notification_blob :: proc(a: mem.Allocator, count: int, body_bytes: int, kind: Text_Kind) -> []u8 {
	l := Lcg{state = 20261008}
	b: Buf
	buf_init(&b, count * (body_bytes + 128), a)
	for i in 0..<count {
		buf_str(&b, `{"jsonrpc":"2.0","method":"bench/feed","params":{"seq":`)
		itoa_into(&b, i)
		buf_str(&b, `,"text":"`)
		append_text(&b, &l, body_bytes, kind)
		buf_str(&b, `"}}
`)
	}
	return buf_bytes(&b)
}

build_single_line_blob :: proc(a: mem.Allocator, body_bytes: int) -> []u8 {
	l := Lcg{state = 99}
	b: Buf
	buf_init(&b, body_bytes + 128, a)
	buf_str(&b, `{"jsonrpc":"2.0","method":"bench/one","params":{"text":"`)
	append_ascii_text(&b, &l, body_bytes)
	buf_str(&b, `"}}
`)
	return buf_bytes(&b)
}

// ---------------------------------------------------------------------------
// Scenarios
// ---------------------------------------------------------------------------

Scenario :: struct {
	name:  string,
	setup: proc(a: mem.Allocator) -> (blob: []u8, chunk: int, inbound: bool),
}

sc_ascii_4k :: proc(a: mem.Allocator) -> ([]u8, int, bool) {
	return build_notification_blob(a, 2000, 4096, .Ascii), 0, true
}

sc_cjk_4k :: proc(a: mem.Allocator) -> ([]u8, int, bool) {
	return build_notification_blob(a, 2000, 4096, .Cjk), 0, true
}

sc_escaped_4k :: proc(a: mem.Allocator) -> ([]u8, int, bool) {
	return build_notification_blob(a, 2000, 4096, .Escaped), 0, true
}

sc_flood :: proc(a: mem.Allocator) -> ([]u8, int, bool) {
	return build_notification_blob(a, 20000, 8, .Ascii), 0, true
}

sc_dribble_1m :: proc(a: mem.Allocator) -> ([]u8, int, bool) {
	return build_single_line_blob(a, 1_000_000), 64, false
}

sc_chunk_1m :: proc(a: mem.Allocator) -> ([]u8, int, bool) {
	return build_single_line_blob(a, 1_000_000), 512, false
}

SCENARIOS :: []Scenario{
	{name = "inbound-ascii-4k",   setup = sc_ascii_4k},
	{name = "inbound-cjk-4k",     setup = sc_cjk_4k},
	{name = "inbound-escaped-4k", setup = sc_escaped_4k},
	{name = "inbound-flood",      setup = sc_flood},
	{name = "framing-dribble-1m", setup = sc_dribble_1m},
	{name = "framing-chunk-1m",   setup = sc_chunk_1m},
}

main :: proc() {
	// One scenario per process (argv[1]) keeps a callgrind run's
	// instruction count that scenario's alone; no argument runs the
	// whole wall table.
	only := ""
	if len(os.args) > 1 {
		only = os.args[1]
	}
	for s in SCENARIOS {
		if only != "" && s.name != only {
			continue
		}
		best: time.Duration = 0
		units := 0
		for rep in 0..<5 {
			arena: mem.Dynamic_Arena
			mem.dynamic_arena_init(&arena, context.allocator)
			a := mem.dynamic_arena_allocator(&arena)
			blob, chunk, inbound := s.setup(a)
			t0 := time.tick_now()
			if inbound {
				units = run_inbound(blob, chunk, context.allocator)
			} else {
				units = run_framing(blob, chunk, context.allocator)
			}
			el := time.tick_since(t0)
			if rep == 0 || el < best {
				best = el
			}
			mem.dynamic_arena_destroy(&arena)
		}
		fmt.printfln("%-22s %.3f ms  units=%d", s.name, f64(best) / 1e6, units)
	}
}
