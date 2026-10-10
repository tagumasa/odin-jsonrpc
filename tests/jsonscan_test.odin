// The frame sanity scan guards the core json parser's two blind spots:
// unbounded nesting (a stack-exhaustion crash in the parser) and string
// mangling (invalid UTF-8 normalized to U+FFFD, raw control chars
// truncating values with no error). Well-formed frames — including
// astral-plane text and escapes — must sail through to decode_envelope.
package tests

import "core:mem"
import "core:strings"
import "core:testing"
import "jsonrpc:jsonrpc"

jsonscan_decode :: proc(t: ^testing.T, body: []u8, want_accepted: bool) {
	arena: mem.Dynamic_Arena
	mem.dynamic_arena_init(&arena, context.allocator)
	env, code := jsonrpc.decode_envelope(body, mem.dynamic_arena_allocator(&arena))
	mem.dynamic_arena_destroy(&arena)
	if want_accepted {
		testing.expectf(t, env != nil, "expected acceptance, got code %v", code)
	} else {
		testing.expect_value(t, env == nil, true)
		testing.expect_value(t, code == .Parse_Error, true)
	}
}

@(test)
frame_scan_accepts_wellformed_frames :: proc(t: ^testing.T) {
	frames: []string = {
		`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"x","arguments":{"deep":{"deeper":[1,2,3]}}}}`,
		`{"jsonrpc":"2.0","method":"notify","params":{"text":"月 🌙 àöü ✓"}}`,
		`{"jsonrpc":"2.0","method":"e","params":{"s":"line\nbreak \u00e9 \\ \"quoted\""}}`,
	}
	for f in frames {
		jsonscan_decode(t, transmute([]u8)f, true)
	}
}

deep_nesting_body :: proc(depth: int, a: mem.Allocator) -> []u8 {
	buf := make([dynamic]u8, 0, depth * 2 + 8, a)
	for _ in 0..<depth {
		append(&buf, u8('['))
	}
	append(&buf, u8('0'))
	for _ in 0..<depth {
		append(&buf, u8(']'))
	}
	// The backing is freed by the caller's delete(slice, a) — freeing the
	// dynamic here as well would be a double free.
	return buf[:]
}

@(test)
frame_scan_bounds_nesting_depth :: proc(t: ^testing.T) {
	at_cap := deep_nesting_body(jsonrpc.MAX_JSON_DEPTH, context.allocator)
	defer delete(at_cap, context.allocator)
	testing.expect_value(t, jsonrpc.frame_sanity_ok(at_cap), true)

	over_cap := deep_nesting_body(jsonrpc.MAX_JSON_DEPTH + 1, context.allocator)
	defer delete(over_cap, context.allocator)
	testing.expect_value(t, jsonrpc.frame_sanity_ok(over_cap), false)
	jsonscan_decode(t, over_cap, false)

	// The crash shape the scan exists for: deep enough that the core
	// parser's per-level recursion would exhaust the stack — rejected
	// before parsing, not crashed inside it.
	runaway := deep_nesting_body(100_000, context.allocator)
	defer delete(runaway, context.allocator)
	jsonscan_decode(t, runaway, false)
}

patched_body :: proc(base: string, marker: string, byte: u8, a: mem.Allocator) -> []u8 {
	out := make([]u8, len(base), a)
	for i in 0..<len(base) {
		out[i] = base[i]
	}
	idx := strings.index(base, marker)
	if idx >= 0 {
		out[idx] = byte
	}
	return out
}

@(test)
frame_scan_rejects_bad_utf8_and_control_chars :: proc(t: ^testing.T) {
	// An invalid lead byte (0xFF) inside a string value.
	bad_utf8 := patched_body(`{"jsonrpc":"2.0","method":"xabc"}`, "abc", 0xFF, context.allocator)
	defer delete(bad_utf8, context.allocator)
	testing.expect_value(t, jsonrpc.frame_sanity_ok(bad_utf8), false)
	jsonscan_decode(t, bad_utf8, false)

	// A truncated multi-byte sequence: 0xE6 demands two continuation
	// bytes, and the 'a' that follows is not one.
	truncated := patched_body(`{"jsonrpc":"2.0","method":"xabc"}`, "x", 0xE6, context.allocator)
	defer delete(truncated, context.allocator)
	testing.expect_value(t, jsonrpc.frame_sanity_ok(truncated), false)
	jsonscan_decode(t, truncated, false)

	// A raw newline inside a string value (the escaped two-character \n
	// stays legal — see the accepted-frames test).
	raw_nl := patched_body(`{"jsonrpc":"2.0","method":"x_y"}`, "_", 0x0A, context.allocator)
	defer delete(raw_nl, context.allocator)
	testing.expect_value(t, jsonrpc.frame_sanity_ok(raw_nl), false)
	jsonscan_decode(t, raw_nl, false)
}
