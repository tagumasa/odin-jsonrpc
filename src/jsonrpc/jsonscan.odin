// Structural sanity for inbound JSON frames, run before the core parser:
// the core parser recurses per nesting level with no depth cap (a deep
// body is a stack-exhaustion crash, not a parse error), silently
// normalizes invalid UTF-8 inside strings to U+FFFD, and truncates values
// at raw control characters with no error. The scan is a single
// allocation-free pass; everything it rejects the parser would have
// either crashed on or mangled. decode_envelope runs it on every frame
// body before parse.
package jsonrpc

MAX_JSON_DEPTH :: 128

// Every index in the scan is a body index guarded by construction: i
// advances only through bytes the loop re-checks against len(body), and
// the multi-byte skip rides json_utf8_sequence_len's own end checks.
frame_sanity_ok :: proc(body: []u8) -> bool #no_bounds_check {
	depth := 0
	in_string := false
	escaped := false
	i := 0
	for i < len(body) {
		c := body[i]
		if in_string {
			if escaped {
				escaped = false
			} else if c == '\\' {
				escaped = true
			} else if c == '"' {
				in_string = false
			} else if c < 0x20 {
				return false
			} else if c >= 0x80 {
				n := json_utf8_sequence_len(body, i)
				if n == 0 {
					return false
				}
				i += n
				continue
			} else {
				// Ordinary string byte: the run continues while bytes
				// stay printable ASCII that is neither quote nor
				// escape — none of the per-byte states above can
				// engage inside such a run, and the string payload is
				// where a frame's bytes mostly live.
				j := i + 1
				for j < len(body) {
					d := body[j]
					if d < 0x20 || d >= 0x80 || d == '"' || d == '\\' {
						break
					}
					j += 1
				}
				i = j
				continue
			}
		} else {
			switch c {
			case '{', '[':
				depth += 1
				if depth > MAX_JSON_DEPTH {
					return false
				}
			case '}', ']':
				depth -= 1
				if depth < 0 {
					return false
				}
			case '"':
				in_string = true
			case:
			}
		}
		i += 1
	}
	return true
}

json_utf8_cont :: proc(b: u8) -> bool {
	return b >= 0x80 && b <= 0xBF
}

// json_utf8_sequence_len validates the well-formed UTF-8 sequence at
// body[i] — rejecting overlong encodings (C0/C1, E0 80..9F, F0 80..8F),
// UTF-16 surrogates (ED A0..BF), and code points beyond U+10FFFF
// (F5..FF) — and returns its byte length, or 0 when the sequence is
// invalid or truncated at the end of the body.
json_utf8_sequence_len :: proc(body: []u8, i: int) -> int {
	c := body[i]
	if i + 1 >= len(body) {
		return 0
	}
	n1 := body[i + 1]
	switch {
	case c >= 0xC2 && c <= 0xDF:
		if !json_utf8_cont(n1) {
			return 0
		}
		return 2
	case c == 0xE0:
		if i + 2 >= len(body) || n1 < 0xA0 || n1 > 0xBF || !json_utf8_cont(body[i + 2]) {
			return 0
		}
		return 3
	case (c >= 0xE1 && c <= 0xEC) || c == 0xEE || c == 0xEF:
		if i + 2 >= len(body) || !json_utf8_cont(n1) || !json_utf8_cont(body[i + 2]) {
			return 0
		}
		return 3
	case c == 0xED:
		if i + 2 >= len(body) || n1 < 0x80 || n1 > 0x9F || !json_utf8_cont(body[i + 2]) {
			return 0
		}
		return 3
	case c == 0xF0:
		if i + 3 >= len(body) || n1 < 0x90 || n1 > 0xBF || !json_utf8_cont(body[i + 2]) || !json_utf8_cont(body[i + 3]) {
			return 0
		}
		return 4
	case c >= 0xF1 && c <= 0xF3:
		if i + 3 >= len(body) || !json_utf8_cont(n1) || !json_utf8_cont(body[i + 2]) || !json_utf8_cont(body[i + 3]) {
			return 0
		}
		return 4
	case c == 0xF4:
		if i + 3 >= len(body) || n1 < 0x80 || n1 > 0x8F || !json_utf8_cont(body[i + 2]) || !json_utf8_cont(body[i + 3]) {
			return 0
		}
		return 4
	}
	return 0
}
