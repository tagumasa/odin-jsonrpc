// jsonrpc component tests: framing (defects, split delivery, oversize,
// huge messages), ID normalization, batch rejection, and full Conn
// roundtrips over an in-memory loopback pipe (server dispatch + client
// pending table, timeout, and close semantics).
package tests

import "core:encoding/json"
import "core:mem"
import "core:strings"
import "core:sync"
import "core:testing"
import "core:thread"
import "core:time"
import "jsonrpc:jsonrpc"
import "jsonrpc:jsonutil"
import "jsonrpc:platform"

// conn_reader_entry is the reader-thread body: one conn_read_loop on its
// own thread.
conn_reader_entry :: proc(c: ^jsonrpc.Conn) {
	jsonrpc.conn_read_loop(c)
}

obj_value :: proc(m: map[string]json.Value) -> json.Value {
	return json.Value(json.Object(m))
}

// --- in-memory loopback pipe (writes feed the same pipe's reads) ---------

Pipe :: struct {
	buf:              [dynamic]u8,
	mu:               sync.Mutex,
	cond:             sync.Cond,
	closed:           bool,
	allocator:        mem.Allocator,
	// Optional per-read deadline in ms (0 = wait forever). A read that
	// would block longer returns .Eof instead of parking the reading
	// thread — for harnesses whose producer thread may die before writing
	// its frame, so the suite fails in bounded time instead of hanging.
	read_deadline_ms: i64,
}

pipe_init :: proc(p: ^Pipe, a := context.allocator) {
	p^ = {allocator = a}
}

// The pipe procs run on several threads (test, caller, reader); each pins
// the buffer to the pipe's own allocator so growth and free always match,
// whatever context the calling thread carries.
pipe_write :: proc(data: rawptr, buf: []u8) -> (int, jsonrpc.Read_Err) {
	p := cast(^Pipe)data
	context.allocator = p.allocator
	sync.mutex_lock(&p.mu)
	if p.closed {
		sync.mutex_unlock(&p.mu)
		return 0, .Eof
	}
	append(&p.buf, ..buf)
	sync.cond_broadcast(&p.cond)
	sync.mutex_unlock(&p.mu)
	return len(buf), .None
}

pipe_read :: proc(data: rawptr, buf: []u8) -> (int, jsonrpc.Read_Err) {
	p := cast(^Pipe)data
	context.allocator = p.allocator
	sync.mutex_lock(&p.mu)
	if p.read_deadline_ms > 0 {
		// Timed wait: cond_wait_with_timeout returns on the deadline even
		// without a signal, and the loop re-checks the buffer each wake.
		deadline := platform.mono_ms() + p.read_deadline_ms
		for len(p.buf) == 0 && !p.closed {
			remaining := deadline - platform.mono_ms()
			if remaining <= 0 {
				break
			}
			sync.cond_wait_with_timeout(&p.cond, &p.mu, time.Duration(remaining * 1_000_000))
		}
	} else {
		for len(p.buf) == 0 && !p.closed {
			sync.cond_wait(&p.cond, &p.mu)
		}
	}
	if len(p.buf) == 0 {
		sync.mutex_unlock(&p.mu)
		return 0, .Eof
	}
	n := min(len(p.buf), len(buf))
	for i := 0; i < n; i += 1 {
		buf[i] = p.buf[i]
	}
	rest := len(p.buf) - n
	for i := 0; i < rest; i += 1 {
		p.buf[i] = p.buf[i + n]
	}
	resize(&p.buf, rest)
	sync.mutex_unlock(&p.mu)
	return n, .None
}

pipe_close :: proc(p: ^Pipe) {
	context.allocator = p.allocator
	sync.mutex_lock(&p.mu)
	p.closed = true
	// Always delete: a fully drained buffer (len 0) still owns its backing.
	delete(p.buf)
	sync.cond_broadcast(&p.cond)
	sync.mutex_unlock(&p.mu)
}

// json_bytes copies a JSON literal into test scratch (backtick literals
// are untyped and cannot be transmuted directly).
json_bytes :: proc(s: string) -> []u8 {
	b := make([]u8, len(s), context.temp_allocator)
	for i in 0..<len(s) {
		b[i] = s[i]
	}
	return b
}

// --- framing --------------------------------------------------------------

@(test)
frame_roundtrip :: proc(t: ^testing.T) {
	p: Pipe
	pipe_init(&p)
	defer pipe_close(&p)
	w: jsonrpc.Writer
	jsonrpc.writer_init(&w, pipe_write, &p)
	body := "hello framing world"
	testing.expect(t, jsonrpc.write_frame(&w, transmute([]u8)body) == .None)

	r: jsonrpc.Reader
	jsonrpc.reader_init(&r, pipe_read, &p, 1024)
	defer jsonrpc.reader_destroy(&r)
	got, rerr := jsonrpc.read_frame(&r, context.temp_allocator)
	testing.expect(t, rerr == .None)
	testing.expect_value(t, string(got), body)
}

// Source serves a fixed byte slice; max_per_call limits each read so the
// frame assembler must accumulate across many reads (max_per_call=1 turns
// it into a byte-dribble).
Source :: struct {
	src:          []u8,
	pos:          int,
	max_per_call: int,
}

source_read :: proc(data: rawptr, buf: []u8) -> (int, jsonrpc.Read_Err) {
	s := cast(^Source)data
	if s.pos >= len(s.src) {
		return 0, .Eof
	}
	take := min(len(buf), len(s.src) - s.pos)
	if s.max_per_call > 0 {
		take = min(take, s.max_per_call)
	}
	for i := 0; i < take; i += 1 {
		buf[i] = s.src[s.pos + i]
	}
	s.pos += take
	return take, .None
}

@(test)
frame_split_delivery :: proc(t: ^testing.T) {
	frame := "Content-Length: 5\r\n\r\nabcde"
	d := Source{src = transmute([]u8)frame, max_per_call = 1}
	r: jsonrpc.Reader
	jsonrpc.reader_init(&r, source_read, &d, 1024)
	defer jsonrpc.reader_destroy(&r)
	got, err := jsonrpc.read_frame(&r, context.temp_allocator)
	testing.expect(t, err == .None)
	testing.expect_value(t, string(got), "abcde")
}

@(test)
frame_missing_content_length :: proc(t: ^testing.T) {
	bogus := "X-Bogus: 1\r\n\r\nbody"
	src := Source{src = transmute([]u8)bogus}
	r: jsonrpc.Reader
	jsonrpc.reader_init(&r, source_read, &src, 1024)
	defer jsonrpc.reader_destroy(&r)
	_, err := jsonrpc.read_frame(&r, context.temp_allocator)
	testing.expect_value(t, err, jsonrpc.Read_Err.Framing)
}

@(test)
frame_oversize_rejected :: proc(t: ^testing.T) {
	huge := "Content-Length: 999999999\r\n\r\n"
	src := Source{src = transmute([]u8)huge}
	r: jsonrpc.Reader
	jsonrpc.reader_init(&r, source_read, &src, 1024)
	defer jsonrpc.reader_destroy(&r)
	_, err := jsonrpc.read_frame(&r, context.temp_allocator)
	testing.expect_value(t, err, jsonrpc.Read_Err.Too_Large)
}

// A read_fn that returns zero bytes with no error is a contract violation
// (every real source answers "0 bytes => error"); the frame read folds it
// into .Io instead of spinning forever.
zero_progress_read :: proc(data: rawptr, buf: []u8) -> (int, jsonrpc.Read_Err) {
	_ = data
	_ = buf
	return 0, .None
}

@(test)
frame_read_rejects_progress_less_source :: proc(t: ^testing.T) {
	r: jsonrpc.Reader
	jsonrpc.reader_init_ndjson(&r, zero_progress_read, nil, 1024)
	defer jsonrpc.reader_destroy(&r)
	_, err := jsonrpc.read_frame(&r, context.temp_allocator)
	testing.expect_value(t, err, jsonrpc.Read_Err.Io)
}

@(test)
frame_huge_message_roundtrip :: proc(t: ^testing.T) {
	p: Pipe
	pipe_init(&p)
	defer pipe_close(&p)
	w: jsonrpc.Writer
	jsonrpc.writer_init(&w, pipe_write, &p)
	payload := make([]u8, 1024 * 1024, context.temp_allocator)
	for i in 0..<len(payload) {
		payload[i] = u8(i % 251)
	}
	testing.expect(t, jsonrpc.write_frame(&w, payload) == .None)

	r: jsonrpc.Reader
	jsonrpc.reader_init(&r, pipe_read, &p, 2 * 1024 * 1024)
	defer jsonrpc.reader_destroy(&r)
	got, err := jsonrpc.read_frame(&r, context.temp_allocator)
	testing.expect(t, err == .None)
	testing.expect_value(t, len(got), len(payload))
	match := true
	for i in 0..<len(payload) {
		if got[i] != payload[i] {
			match = false
			break
		}
	}
	testing.expect(t, match, "1 MiB payload corrupted")
}

// --- envelope / ID normalization ------------------------------------------

@(test)
id_normalization :: proc(t: ^testing.T) {
	env, _ := jsonrpc.decode_envelope(json_bytes(`{"jsonrpc":"2.0","id":42,"method":"m"}`), context.temp_allocator)
	testing.expect(t, env != nil)
	testing.expect(t, env.id_set)
#partial switch v in env.id {
	case i64:
		testing.expect_value(t, v, i64(42))
	case:
		testing.expectf(t, false, "numeric id not normalized to i64")
	}

	env, _ = jsonrpc.decode_envelope(json_bytes(`{"jsonrpc":"2.0","id":"42","method":"m"}`), context.temp_allocator)
#partial switch v in env.id {
	case string:
		testing.expect_value(t, v, "42") // the JSON type is preserved: the reply must echo the same value
	case:
		testing.expectf(t, false, "numeric string id collapsed into a number")
	}

	env, _ = jsonrpc.decode_envelope(json_bytes(`{"jsonrpc":"2.0","id":"abc-42","method":"m"}`), context.temp_allocator)
#partial switch v in env.id {
	case string:
		testing.expect_value(t, v, "abc-42")
	case:
		testing.expectf(t, false, "non-numeric string id must stay a string")
	}

	env, _ = jsonrpc.decode_envelope(json_bytes(`{"jsonrpc":"2.0","method":"n"}`), context.temp_allocator)
	testing.expect_value(t, env.kind, jsonrpc.Msg_Kind.Notification)
	testing.expect(t, !env.id_set)
}

@(test)
batch_rejected :: proc(t: ^testing.T) {
	env, code := jsonrpc.decode_envelope(json_bytes(`[{"jsonrpc":"2.0","id":1,"method":"m"}]`), context.temp_allocator)
	testing.expect(t, env == nil)
	testing.expect_value(t, code, jsonrpc.Err_Code.Invalid_Request)
}

@(test)
envelope_requires_jsonrpc_member :: proc(t: ^testing.T) {
	// JSON-RPC 2.0 demands the member on every message; foreign input that
	// merely looks request-shaped is rejected at the boundary.
	env, code := jsonrpc.decode_envelope(json_bytes(`{"id":1,"method":"m"}`), context.temp_allocator)
	testing.expect(t, env == nil)
	testing.expect_value(t, code, jsonrpc.Err_Code.Invalid_Request)

	env, code = jsonrpc.decode_envelope(json_bytes(`{"jsonrpc":"1.0","id":1,"method":"m"}`), context.temp_allocator)
	testing.expect(t, env == nil)
	testing.expect_value(t, code, jsonrpc.Err_Code.Invalid_Request)

	env, code = jsonrpc.decode_envelope(json_bytes(`{"jsonrpc":2.0,"id":1,"method":"m"}`), context.temp_allocator)
	testing.expect(t, env == nil)
	testing.expect_value(t, code, jsonrpc.Err_Code.Invalid_Request)

	env, _ = jsonrpc.decode_envelope(json_bytes(`{"jsonrpc":"2.0","id":1,"method":"m"}`), context.temp_allocator)
	testing.expect(t, env != nil)
}

@(test)
notification_body_omits_null_params :: proc(t: ^testing.T) {
	// Notification params are optional-object on the wire, never null —
	// a literal "params":null frame is rejected by strict client
	// validators, so the member disappears when there is no payload.
	empty := jsonrpc.build_notification_body("notifications/tools/list_changed", nil, context.temp_allocator)
	testing.expect_value(t, empty, `{"jsonrpc":"2.0","method":"notifications/tools/list_changed"}`)

	params := jsonutil.json_object(1, context.temp_allocator)
	jsonutil.obj_set(&params, "n", jsonutil.json_int(7))
	loaded := jsonrpc.build_notification_body("n", json.Value(json.Object(params)), context.temp_allocator)
	testing.expect_value(t, loaded, `{"jsonrpc":"2.0","method":"n","params":{"n":7}}`)
}

// --- Conn roundtrips --------------------------------------------------------

echo_handler :: proc(conn: ^jsonrpc.Conn, env: ^jsonrpc.Envelope, arena: mem.Allocator) -> (jsonrpc.Reply, jsonrpc.Action) {
	out := jsonutil.json_object(1, arena)
	jsonutil.obj_set(&out, "echo", jsonutil.json_string(env.method))
	reply: jsonrpc.Reply = {result = json.Value(json.Object(out))}
	return reply, .Respond
}

Loopback :: struct {
	conn: ^jsonrpc.Conn,
	pipe: ^Pipe,
}

loopback_serve :: proc(lb: ^Loopback) {
	jsonrpc.conn_read_loop(lb.conn)
}

@(test)
conn_request_response_roundtrip :: proc(t: ^testing.T) {
	p: Pipe
	pipe_init(&p)

	c := new(jsonrpc.Conn, context.allocator)
	defer free(c, context.allocator)
	r: jsonrpc.Reader
	jsonrpc.reader_init(&r, pipe_read, &p, 1024)
	w: jsonrpc.Writer
	jsonrpc.writer_init(&w, pipe_write, &p)
	jsonrpc.conn_init(c, r, w, context.allocator)
	jsonrpc.conn_register(c, "svc.test/echo", echo_handler)

	lb := new(Loopback, context.allocator)
	lb^ = {conn = c, pipe = &p}
	thr := thread.create_and_start_with_poly_data(lb, loopback_serve, self_cleanup = false)

	arena: mem.Dynamic_Arena
	mem.dynamic_arena_init(&arena, context.allocator)
	defer mem.dynamic_arena_destroy(&arena)
	result, _, _, cerr := jsonrpc.conn_call(c, "svc.test/echo", nil, mem.dynamic_arena_allocator(&arena), platform.mono_ms() + 2000)
	testing.expect_value(t, cerr, jsonrpc.Call_Err.None)
	echoed := ""
	if v, ok := jsonutil.obj_get(result, "echo"); ok {
		#partial switch x in v {
		case json.String:
			echoed = string(x)
		case:
		}
	}
	testing.expect_value(t, echoed, "svc.test/echo")

	// Unknown method surfaces as a typed JSON-RPC error.
	_, nf_code, _, nf_cerr := jsonrpc.conn_call(c, "svc.nowhere", nil, mem.dynamic_arena_allocator(&arena), platform.mono_ms() + 2000)
	testing.expect_value(t, nf_cerr, jsonrpc.Call_Err.Error_Response)
	testing.expect_value(t, nf_code, jsonrpc.Err_Code.Method_Not_Found)

	// Shutdown order: stop the conn, wake the loopback reader through the
	// pipe, join it, then let the defers free the rest.
	jsonrpc.conn_close(c)
	pipe_close(&p)
	thread.join(thr)
	free(thr, context.allocator)
	free(lb, context.allocator)
	jsonrpc.conn_destroy(c)
}

@(test)
conn_call_timeout_and_close :: proc(t: ^testing.T) {
	p: Pipe
	pipe_init(&p)

	c := new(jsonrpc.Conn, context.allocator)
	defer free(c, context.allocator)
	r: jsonrpc.Reader
	jsonrpc.reader_init(&r, pipe_read, &p, 1024)
	w: jsonrpc.Writer
	jsonrpc.writer_init(&w, pipe_write, &p)
	jsonrpc.conn_init(c, r, w, context.allocator)
	// No handler, no reader: nobody will ever reply.

	deadline := platform.mono_ms() + 120
	_, _, _, cerr := jsonrpc.conn_call(c, "svc.slow", nil, context.temp_allocator, deadline)
	testing.expect_value(t, cerr, jsonrpc.Call_Err.Timeout)

	// A blocked call must be released with .Closed when the conn closes.
	// The deadline is generous rather than 0 (wait forever): a regression
	// that stops releasing waiters then fails here as .Timeout instead of
	// hanging the join below forever.
	Blocked :: struct {
		conn:   ^jsonrpc.Conn,
		result: jsonrpc.Call_Err,
		done:   bool,
		mu:     sync.Mutex,
		cond:   sync.Cond,
	}
	blocked := new(Blocked, context.allocator)
	defer free(blocked, context.allocator)
	blocked^ = {conn = c}

	blocked_caller :: proc(b: ^Blocked) {
		_, _, _, err := jsonrpc.conn_call(b.conn, "svc.blocked", nil, context.temp_allocator, platform.mono_ms() + 10_000)
		sync.mutex_lock(&b.mu)
		b.result = err
		b.done = true
		sync.cond_broadcast(&b.cond)
		sync.mutex_unlock(&b.mu)
	}
	bthr := thread.create_and_start_with_poly_data(blocked, blocked_caller, self_cleanup = false)

	deadline2 := platform.mono_ms() + 500
	for {
		sync.mutex_lock(&p.mu)
		pending := len(p.buf)
		sync.mutex_unlock(&p.mu)
		if pending > 0 || platform.mono_ms() >= deadline2 {
			break
		}
		time_sleep_ms(1)
	}
	jsonrpc.conn_close(c)
	pipe_close(&p)

	sync.mutex_lock(&blocked.mu)
	for !blocked.done {
		sync.cond_wait(&blocked.cond, &blocked.mu)
	}
	got := blocked.result
	sync.mutex_unlock(&blocked.mu)
	thread.join(bthr)
	free(bthr, context.allocator)
	jsonrpc.conn_destroy(c)
	testing.expect_value(t, got, jsonrpc.Call_Err.Closed)
}

// A nil conn is a typed refusal, not a deref: a host re-reading its
// connection pointer can race a teardown that publishes nil, and the call
// face must answer .Closed instead of crashing.
@(test)
conn_call_nil_conn_refused :: proc(t: ^testing.T) {
	_, _, msg, cerr := jsonrpc.conn_call(
		nil, "svc.test/echo", nil, context.temp_allocator, platform.mono_ms() + 50,
	)
	testing.expect_value(t, cerr, jsonrpc.Call_Err.Closed)
	testing.expect(t, msg == "no connection", msg)
}

// A reader-stream EOF must release pending waiters immediately: the peer
// process is gone, no reply can ever arrive, so calls report .Closed long
// before their deadlines — and later calls fail fast at the pending gate.
// Two one-way pipes model a silent server (requests pile up unread; the
// reply stream ends).
@(test)
conn_reader_eof_fails_pending :: proc(t: ^testing.T) {
	up := new(Pipe, context.allocator) // client -> server (nobody reads)
	pipe_init(up)
	down := new(Pipe, context.allocator) // server -> client (never answered)
	pipe_init(down)

	c := new(jsonrpc.Conn, context.allocator)
	r: jsonrpc.Reader
	jsonrpc.reader_init(&r, pipe_read, down, 1024, context.allocator)
	w: jsonrpc.Writer
	jsonrpc.writer_init(&w, pipe_write, up)
	jsonrpc.conn_init(c, r, w, context.allocator)
	thr := thread.create_and_start_with_poly_data(c, conn_reader_entry, self_cleanup = false, name = "jsonrpc-eof-reader")

	Eof_Result :: struct {
		conn: ^jsonrpc.Conn,
		err:  jsonrpc.Call_Err,
		done: bool,
		mu:   sync.Mutex,
		cond: sync.Cond,
	}
	res := new(Eof_Result, context.allocator)
	res^ = {conn = c}
	eof_caller :: proc(e: ^Eof_Result) {
		_, _, _, err := jsonrpc.conn_call(e.conn, "svc.hang", nil, context.temp_allocator, platform.mono_ms() + 5000)
		sync.mutex_lock(&e.mu)
		e.err = err
		e.done = true
		sync.cond_broadcast(&e.cond)
		sync.mutex_unlock(&e.mu)
	}
	caller := thread.create_and_start_with_poly_data(res, eof_caller, self_cleanup = false)

	gate := platform.mono_ms() + 500
	for {
		sync.mutex_lock(&up.mu)
		pending := len(up.buf)
		sync.mutex_unlock(&up.mu)
		if pending > 0 || platform.mono_ms() >= gate {
			break
		}
		time_sleep_ms(1)
	}

	before := platform.mono_ms()
	pipe_close(down) // the server's reply stream ends: EOF
	sync.mutex_lock(&res.mu)
	for !res.done {
		sync.cond_wait(&res.cond, &res.mu)
	}
	got := res.err
	sync.mutex_unlock(&res.mu)
	elapsed := platform.mono_ms() - before
	testing.expect_value(t, got, jsonrpc.Call_Err.Closed)
	testing.expect(t, elapsed < 2500, "EOF must release the waiter long before the deadline")

	// The latched pending table refuses new calls outright.
	_, _, _, cerr2 := jsonrpc.conn_call(c, "svc.next", nil, context.temp_allocator, platform.mono_ms() + 100)
	testing.expect_value(t, cerr2, jsonrpc.Call_Err.Closed)

	thread.join(caller)
	free(caller, context.allocator)
	free(res, context.allocator)
	thread.join(thr)
	free(thr, context.allocator)
	jsonrpc.conn_destroy(c)
	free(c, context.allocator)
	pipe_close(up)
	free(up, context.allocator)
	free(down, context.allocator)
}

// conn_call with a cancel token: a fired token must end the wait with
// .Cancelled, forward the cancel through the conn's notify port with the
// request id, and free the slot (no reply will ever be matched).
cancel_notify_id: i64 // single-threaded recording: the test runs on the
// calling thread, so the module-level recorder needs no lock

recording_cancel_notify :: proc(c: ^jsonrpc.Conn, id: i64) {
	cancel_notify_id = id
}

@(test)
conn_call_returns_early_on_cancel :: proc(t: ^testing.T) {
	p: Pipe
	pipe_init(&p)

	c := new(jsonrpc.Conn, context.allocator)
	defer free(c, context.allocator)
	r: jsonrpc.Reader
	jsonrpc.reader_init(&r, pipe_read, &p, 1024)
	w: jsonrpc.Writer
	jsonrpc.writer_init(&w, pipe_write, &p)
	jsonrpc.conn_init(c, r, w, context.allocator)
	c.cancel_notify = recording_cancel_notify
	// No handler, no reader: nobody will ever reply.

	root := new(platform.Cancel_Token, context.allocator)
	platform.token_init_root(root)
	defer platform.token_destroy(root, context.allocator)
	task := platform.token_derive(root, 0, context.allocator)
	defer platform.token_destroy(task, context.allocator)
	platform.token_fire(root, .Cancelled)

	cancel_notify_id = 0
	_, _, _, cerr := jsonrpc.conn_call(c, "svc.slow", nil, context.temp_allocator, 0, task)
	testing.expect_value(t, cerr, jsonrpc.Call_Err.Cancelled)
	testing.expect_value(t, cancel_notify_id, 1)

	jsonrpc.conn_close(c)
	pipe_close(&p)
	jsonrpc.conn_destroy(c)
}

@(test)
jsonrpc_parse_decimal_bounds :: proc(t: ^testing.T) {
	n, ok := jsonrpc.parse_decimal("0")
	testing.expect(t, ok && n == 0)

	// The cap itself is accepted, one past it is not.
	n, ok = jsonrpc.parse_decimal("1099511627776") // 1 << 40
	testing.expect(t, ok)
	testing.expect_value(t, n, 1 << 40)
	_, ok = jsonrpc.parse_decimal("1099511627777")
	testing.expect(t, !ok)

	// 19+ digits would overflow i64 and wrap negative past a post-multiply
	// check; the bound must reject them before the multiply.
	_, ok = jsonrpc.parse_decimal("9999999999999999999")
	testing.expect(t, !ok)
	_, ok = jsonrpc.parse_decimal("99999999999999999999")
	testing.expect(t, !ok)

	_, ok = jsonrpc.parse_decimal("12x4")
	testing.expect(t, !ok)
	_, ok = jsonrpc.parse_decimal("")
	testing.expect(t, !ok)
}

// --- newline-delimited framing (MCP stdio) -------------------------------------

@(test)
ndjson_frame_roundtrip :: proc(t: ^testing.T) {
	p: Pipe
	pipe_init(&p)
	defer pipe_close(&p)
	w: jsonrpc.Writer
	jsonrpc.writer_init_ndjson(&w, pipe_write, &p)
	body := `{"jsonrpc":"2.0","id":1,"result":{}}`
	testing.expect(t, jsonrpc.write_frame(&w, json_bytes(body)) == .None)

	r: jsonrpc.Reader
	jsonrpc.reader_init_ndjson(&r, pipe_read, &p, 1024)
	defer jsonrpc.reader_destroy(&r)
	got, rerr := jsonrpc.read_frame(&r, context.temp_allocator)
	testing.expect(t, rerr == .None)
	testing.expect_value(t, string(got), body)

	// The second message on the same line stream arrives whole: two
	// frames written back-to-back read as two bodies, terminator eaten.
	testing.expect(t, jsonrpc.write_frame(&w, json_bytes(`{"jsonrpc":"2.0","id":2}`)) == .None)
	got2, rerr2 := jsonrpc.read_frame(&r, context.temp_allocator)
	testing.expect(t, rerr2 == .None)
	testing.expect(t, string(got2) == `{"jsonrpc":"2.0","id":2}`)
}

@(test)
ndjson_frame_split_delivery_and_crlf :: proc(t: ^testing.T) {
	// Byte-dribbled delivery with a CRLF terminator: the CR is stripped.
	frame := "{\"a\":1}\r\n{\"b\":2}\n"
	d := Source{src = json_bytes(frame), max_per_call = 1}
	r: jsonrpc.Reader
	jsonrpc.reader_init_ndjson(&r, source_read, &d, 1024)
	defer jsonrpc.reader_destroy(&r)
	got, err := jsonrpc.read_frame(&r, context.temp_allocator)
	testing.expect(t, err == .None)
	testing.expect(t, string(got) == `{"a":1}`)
	got2, err2 := jsonrpc.read_frame(&r, context.temp_allocator)
	testing.expect(t, err2 == .None)
	testing.expect(t, string(got2) == `{"b":2}`)

	// A trailing partial line at EOF is not a message.
	d2 := Source{src = json_bytes("{\"c\":3"), max_per_call = 1}
	r2: jsonrpc.Reader
	jsonrpc.reader_init_ndjson(&r2, source_read, &d2, 1024)
	defer jsonrpc.reader_destroy(&r2)
	_, err3 := jsonrpc.read_frame(&r2, context.temp_allocator)
	testing.expect_value(t, err3, jsonrpc.Read_Err.Eof)
}

@(test)
ndjson_frame_oversize_rejected :: proc(t: ^testing.T) {
	xs := make([dynamic]u8, 0, 40, context.temp_allocator)
	for i := 0; i < 40; i += 1 {
		append(&xs, 'x')
	}
	long := strings.concatenate({`{"a":"`, string(xs[:]), `"}`}, context.temp_allocator)
	d := Source{src = json_bytes(strings.concatenate({long, "\n"}, context.temp_allocator))}
	r: jsonrpc.Reader
	jsonrpc.reader_init_ndjson(&r, source_read, &d, 16)
	defer jsonrpc.reader_destroy(&r)
	_, err := jsonrpc.read_frame(&r, context.temp_allocator)
	testing.expect_value(t, err, jsonrpc.Read_Err.Too_Large)
}

@(test)
jsonrpc_envelope_clone_string_id :: proc(t: ^testing.T) {
	// A request queued onto the request queue must survive the reader's
	// per-message arena being torn down right after the post — including a
	// string id (numeric ids copy with the struct; a string id would not
	// survive without the clone).
	src: mem.Dynamic_Arena
	mem.dynamic_arena_init(&src, context.allocator)
	sa := mem.dynamic_arena_allocator(&src)

	env := new(jsonrpc.Envelope, sa)
	env^ = {
		kind    = .Request,
		id      = strings.clone("server-side-7", sa),
		id_set  = true,
		method  = strings.clone("workspace/configuration", sa),
	}
	clone := jsonrpc.envelope_clone(env, context.allocator)
	mem.dynamic_arena_destroy(&src)

	testing.expect(t, clone.id_set)
	switch v in clone.id {
	case string:
		testing.expect_value(t, v, "server-side-7")
	case i64:
		testing.expectf(t, false, "id must stay the string variant")
	}
	testing.expect(t, clone.method == "workspace/configuration", clone.method)

	delete(clone.method, context.allocator)
	#partial switch v in clone.id {
	case string:
		delete(v, context.allocator)
	}
	free(clone, context.allocator)
}

@(test)
jsonrpc_build_error_body_rides_destination_scope :: proc(t: ^testing.T) {
	// The wire builders serialize every sub-part on the destination
	// allocator together with the final body: a caller on a long-lived
	// thread must not depend on temp resets, so the send's whole
	// footprint must die with the caller's scope. The arena over the
	// tracking allocator is the oracle — the destroy empties the map.
	ta: mem.Tracking_Allocator
	mem.tracking_allocator_init(&ta, context.allocator)
	track := mem.tracking_allocator(&ta)

	arena: mem.Dynamic_Arena
	mem.dynamic_arena_init(&arena, track)
	body := jsonrpc.build_error_body(7, true, .Internal_Error, "boom \x1b", mem.dynamic_arena_allocator(&arena))
	testing.expect(t, strings.contains(body, `"boom \u001b"`), body)
	testing.expect(t, strings.contains(body, "-32603"), body)
	testing.expect(t, len(ta.allocation_map) > 0, "the builder allocated on the destination")

	mem.dynamic_arena_destroy(&arena)
	testing.expect_value(t, len(ta.allocation_map), 0)
	mem.tracking_allocator_destroy(&ta)
}

@(test)
jsonrpc_request_queue_stop_then_post :: proc(t: ^testing.T) {
	// The queue-stop contract: after queue_stop, a poster reading
	// c.request_queue must observe nil and fail its post cleanly — never
	// touch the freed queue or its chan. The detach is idempotent.
	p: Pipe
	pipe_init(&p)
	defer delete(p.buf)

	c := new(jsonrpc.Conn, context.allocator)
	defer free(c, context.allocator)
	r: jsonrpc.Reader
	jsonrpc.reader_init(&r, pipe_read, &p, 1024)
	w: jsonrpc.Writer
	jsonrpc.writer_init(&w, pipe_write, &p)
	jsonrpc.conn_init(c, r, w, context.allocator)

	testing.expect(t, jsonrpc.conn_start_request_queue(c), "queue starts")
	env: jsonrpc.Envelope
	env.kind = .Request
	env.method = "svc.test/nothing"
	testing.expect(t, jsonrpc.queue_try_post(c, &env), "post reaches the live queue")
	jsonrpc.queue_stop(c)
	testing.expect(t, !jsonrpc.queue_try_post(c, &env), "post after stop refuses")
	jsonrpc.queue_stop(c) // the second stop is a no-op, not a double free
}

@(test)
jsonrpc_request_queue_stop_drains_buffered_entries :: proc(t: ^testing.T) {
	// The queue_stop drain contract: every entry still buffered at close
	// is destroyed, not abandoned. The worker's recv drains a closed
	// buffered chan to empty before reporting closed (core:sync/chan
	// semantics), destroying each entry's arena on the way; the tracking
	// allocator is the oracle — were the entries abandoned, every posted
	// arena would surface as a leak WARN.
	p: Pipe
	pipe_init(&p)
	defer delete(p.buf)

	c := new(jsonrpc.Conn, context.allocator)
	defer free(c, context.allocator)
	r: jsonrpc.Reader
	jsonrpc.reader_init(&r, pipe_read, &p, 1024)
	w: jsonrpc.Writer
	jsonrpc.writer_init(&w, pipe_write, &p)
	jsonrpc.conn_init(c, r, w, context.allocator)

	testing.expect(t, jsonrpc.conn_start_request_queue(c), "queue starts")
	env: jsonrpc.Envelope
	env.kind = .Request
	env.method = "svc.test/nothing"
	for _ in 0..<6 {
		testing.expect(t, jsonrpc.queue_try_post(c, &env), "post while the queue is live")
	}
	// Whether the worker consumes these before or after the close, every
	// entry's arena is freed on its path out — join covers both orders.
	jsonrpc.queue_stop(c)
	testing.expect(t, !jsonrpc.queue_try_post(c, &env), "post after stop refuses")
}

// --- header-block defects (each is a Framing death, never a misread) ------

@(test)
frame_header_defects :: proc(t: ^testing.T) {
	dup := Source{src = json_bytes("Content-Length: 2\r\nContent-Length: 3\r\n\r\nok")}
	r: jsonrpc.Reader
	jsonrpc.reader_init(&r, source_read, &dup, 1024)
	defer jsonrpc.reader_destroy(&r)
	_, err := jsonrpc.read_frame(&r, context.temp_allocator)
	testing.expect_value(t, err, jsonrpc.Read_Err.Framing) // duplicate Content-Length is ambiguous

	colonless := Source{src = json_bytes("Content-Length\r\n\r\nok")}
	r2: jsonrpc.Reader
	jsonrpc.reader_init(&r2, source_read, &colonless, 1024)
	defer jsonrpc.reader_destroy(&r2)
	_, err2 := jsonrpc.read_frame(&r2, context.temp_allocator)
	testing.expect_value(t, err2, jsonrpc.Read_Err.Framing) // a header line without a colon is malformed

	zero := Source{src = json_bytes("Content-Length: 0\r\n\r\n")}
	r3: jsonrpc.Reader
	jsonrpc.reader_init(&r3, source_read, &zero, 1024)
	defer jsonrpc.reader_destroy(&r3)
	body, err3 := jsonrpc.read_frame(&r3, context.temp_allocator)
	testing.expect_value(t, err3, jsonrpc.Read_Err.None)
	testing.expect_value(t, len(body), 0) // a legal empty body

	// A header block that never separates inside its own cap: the receive
	// buffer must not grow without bound waiting for the separator.
	runaway := make([]u8, jsonrpc.HEADER_MAX_BYTES + 512, context.temp_allocator)
	for i in 0..<len(runaway) {
		runaway[i] = 'x'
	}
	r4: jsonrpc.Reader
	no_sep := Source{src = runaway}
	jsonrpc.reader_init(&r4, source_read, &no_sep, 1 << 20)
	defer jsonrpc.reader_destroy(&r4)
	_, err4 := jsonrpc.read_frame(&r4, context.temp_allocator)
	testing.expect_value(t, err4, jsonrpc.Read_Err.Framing)
}

// --- short writes: write_all retries per byte until the frame crosses ------

Trickle_Sink :: struct {
	buf: [dynamic]u8,
}

trickle_write :: proc(data: rawptr, buf: []u8) -> (int, jsonrpc.Read_Err) {
	s := cast(^Trickle_Sink)data
	if len(buf) == 0 {
		return 0, .None
	}
	append(&s.buf, buf[0]) // exactly one byte per call, always succeeding
	return 1, .None
}

@(test)
frame_short_write_retried :: proc(t: ^testing.T) {
	sink := new(Trickle_Sink, context.allocator)
	defer free(sink, context.allocator)
	w: jsonrpc.Writer
	jsonrpc.writer_init(&w, trickle_write, sink)
	testing.expect(t, jsonrpc.write_frame(&w, json_bytes("hello")) == .None)
	testing.expect_value(t, string(sink.buf[:]), "Content-Length: 5\r\n\r\nhello")
	delete(sink.buf)
}

// --- envelope shapes the conn tests never reach directly --------------------

@(test)
envelope_response_and_error_shapes :: proc(t: ^testing.T) {
	env, code := jsonrpc.decode_envelope(json_bytes(`{"jsonrpc":"2.0","id":7,"result":{"n":1}}`), context.temp_allocator)
	testing.expect(t, env != nil)
	testing.expect_value(t, code, jsonrpc.Err_Code.None)
	testing.expect_value(t, env.kind, jsonrpc.Msg_Kind.Response)
	ro, is_obj := jsonutil.as_object(env.result)
	testing.expect(t, is_obj && len(ro) == 1)

	// The closed code vocabulary carries the wire code; unknown codes and
	// non-object error values fold to Internal_Error.
	env, _ = jsonrpc.decode_envelope(json_bytes(`{"jsonrpc":"2.0","id":8,"error":{"code":-32601,"message":"nope"}}`), context.temp_allocator)
	testing.expect_value(t, env.kind, jsonrpc.Msg_Kind.Error_Response)
	testing.expect_value(t, env.err_code, jsonrpc.Err_Code.Method_Not_Found)
	testing.expect_value(t, env.err_message, "nope")

	env, _ = jsonrpc.decode_envelope(json_bytes(`{"jsonrpc":"2.0","id":9,"error":{"code":-32999,"message":"odd"}}`), context.temp_allocator)
	testing.expect_value(t, env.err_code, jsonrpc.Err_Code.Internal_Error)

	env, _ = jsonrpc.decode_envelope(json_bytes(`{"jsonrpc":"2.0","id":10,"error":"broken"}`), context.temp_allocator)
	testing.expect_value(t, env.kind, jsonrpc.Msg_Kind.Error_Response)
	testing.expect_value(t, env.err_code, jsonrpc.Err_Code.Internal_Error)

	// Shape violations are typed rejections, never crashes: an invalid id
	// type, a non-string method, a response with neither result nor error.
	env, code = jsonrpc.decode_envelope(json_bytes(`{"jsonrpc":"2.0","id":true,"method":"m"}`), context.temp_allocator)
	testing.expect(t, env == nil)
	testing.expect_value(t, code, jsonrpc.Err_Code.Invalid_Request)

	env, code = jsonrpc.decode_envelope(json_bytes(`{"jsonrpc":"2.0","id":1,"method":42}`), context.temp_allocator)
	testing.expect(t, env == nil)
	testing.expect_value(t, code, jsonrpc.Err_Code.Invalid_Request)

	env, code = jsonrpc.decode_envelope(json_bytes(`{"jsonrpc":"2.0","id":1}`), context.temp_allocator)
	testing.expect(t, env == nil)
	testing.expect_value(t, code, jsonrpc.Err_Code.Invalid_Request)
}

// --- queue-driven dispatch: the worker runs handlers; a full queue answers --

gated_echo_handler :: proc(conn: ^jsonrpc.Conn, env: ^jsonrpc.Envelope, arena: mem.Allocator) -> (jsonrpc.Reply, jsonrpc.Action) {
	g := cast(^Wedge_Gate)conn.host
	sync.mutex_lock(&g.guard)
	g.entered += 1
	sync.mutex_unlock(&g.guard)
	sync.mutex_lock(&g.hold) // the test holds this while it fills the queue
	sync.mutex_unlock(&g.hold)
	out := jsonutil.json_object(1, arena)
	jsonutil.obj_set(&out, "echo", jsonutil.json_string(env.method))
	reply: jsonrpc.Reply = {result = json.Value(json.Object(out))}
	return reply, .Respond
}

@(test)
conn_request_queue_dispatch_and_full_reply :: proc(t: ^testing.T) {
	p: Pipe
	pipe_init(&p)
	defer pipe_close(&p)
	p.read_deadline_ms = 10_000

	c := new(jsonrpc.Conn, context.allocator)
	defer free(c, context.allocator)
	r: jsonrpc.Reader
	jsonrpc.reader_init(&r, pipe_read, &p, 1024)
	w: jsonrpc.Writer
	jsonrpc.writer_init(&w, pipe_write, &p)
	jsonrpc.conn_init(c, r, w, context.allocator)
	gate: Wedge_Gate
	sync.mutex_lock(&gate.hold) // hold the handler until the queue is provably full
	c.host = &gate
	jsonrpc.conn_register(c, "svc.test/echo", gated_echo_handler)
	testing.expect(t, jsonrpc.conn_start_request_queue(c, 1))

	// The first request is popped by the worker and wedges inside the
	// handler; the second fills the queue; the third is answered
	// -32803 on the posting thread — the reader never blocks on handlers.
	testing.expect(t, jsonrpc.conn_handle_body(c, json_bytes(`{"jsonrpc":"2.0","id":1,"method":"svc.test/echo"}`), context.temp_allocator))
	testing.expect(t, wedge_gate_entered(&gate, 1, 2_000), "the worker entered the handler")
	testing.expect(t, jsonrpc.conn_handle_body(c, json_bytes(`{"jsonrpc":"2.0","id":2,"method":"svc.test/echo"}`), context.temp_allocator))
	testing.expect(t, jsonrpc.conn_handle_body(c, json_bytes(`{"jsonrpc":"2.0","id":3,"method":"svc.test/echo"}`), context.temp_allocator))

	cr: jsonrpc.Reader
	jsonrpc.reader_init(&cr, pipe_read, &p, 1024)
	defer jsonrpc.reader_destroy(&cr)
	rejected, rerr := jsonrpc.read_frame(&cr, context.temp_allocator)
	testing.expect(t, rerr == .None)
	testing.expectf(t, strings.contains(string(rejected), "-32803"), "the full queue answers RequestFailed, got %s", string(rejected))
	testing.expectf(t, strings.contains(string(rejected), `"id":3`), "the refusal names the refused request, got %s", string(rejected))

	// Release: the queued work drains in order, both replies cross.
	sync.mutex_unlock(&gate.hold)
	first, e1 := jsonrpc.read_frame(&cr, context.temp_allocator)
	testing.expect(t, e1 == .None)
	testing.expectf(t, strings.contains(string(first), `"id":1`), "the first queued reply crossed, got %s", string(first))
	second, e2 := jsonrpc.read_frame(&cr, context.temp_allocator)
	testing.expect(t, e2 == .None)
	testing.expectf(t, strings.contains(string(second), `"id":2`), "the second queued reply crossed, got %s", string(second))

	jsonrpc.queue_stop(c)
	jsonrpc.conn_destroy(c)
}

// --- the trace hook observes both directions -------------------------------

Trace_Record :: struct {
	outbound: bool,
	body:     string,
}

Trace_Log :: struct {
	mu:        sync.Mutex,
	allocator: mem.Allocator,
	records:   [dynamic]Trace_Record,
}

trace_recorder :: proc(user: rawptr, outbound: bool, body: string) {
	log := cast(^Trace_Log)user
	// Runs on the reader thread for inbound frames: pin to the log's own
	// allocator — the borrowed body dies with the message arena.
	sync.mutex_lock(&log.mu)
	saved := context.allocator
	context.allocator = log.allocator
	append(&log.records, Trace_Record{outbound = outbound, body = strings.clone(body, log.allocator)})
	context.allocator = saved
	sync.mutex_unlock(&log.mu)
}

@(test)
conn_trace_observes_both_directions :: proc(t: ^testing.T) {
	p: Pipe
	pipe_init(&p)

	log := new(Trace_Log, context.allocator)
	log^ = {allocator = context.allocator}

	c := new(jsonrpc.Conn, context.allocator)
	defer free(c, context.allocator)
	r: jsonrpc.Reader
	jsonrpc.reader_init(&r, pipe_read, &p, 1024)
	w: jsonrpc.Writer
	jsonrpc.writer_init(&w, pipe_write, &p)
	jsonrpc.conn_init(c, r, w, context.allocator)
	jsonrpc.conn_register(c, "svc.test/echo", echo_handler)
	// The trace contract: installed at setup, before the reader starts
	// and before any send.
	c.trace = trace_recorder
	c.trace_user = log

	lb := new(Loopback, context.allocator)
	lb^ = {conn = c, pipe = &p}
	thr := thread.create_and_start_with_poly_data(lb, loopback_serve, self_cleanup = false)

	arena: mem.Dynamic_Arena
	mem.dynamic_arena_init(&arena, context.allocator)
	defer mem.dynamic_arena_destroy(&arena)
	_, _, _, cerr := jsonrpc.conn_call(c, "svc.test/echo", nil, mem.dynamic_arena_allocator(&arena), platform.mono_ms() + 2000)
	testing.expect_value(t, cerr, jsonrpc.Call_Err.None)

	sync.mutex_lock(&log.mu)
	saw_request := false
	saw_reply := false
	for rec in log.records {
		if rec.outbound && strings.contains(rec.body, `"method":"svc.test/echo"`) {
			saw_request = true
		}
		if !rec.outbound && strings.contains(rec.body, `"echo"`) {
			saw_reply = true
		}
	}
	sync.mutex_unlock(&log.mu)
	testing.expect(t, saw_request, "the outbound request was traced")
	testing.expect(t, saw_reply, "the inbound reply was traced")

	jsonrpc.conn_close(c)
	pipe_close(&p)
	thread.join(thr)
	free(thr, context.allocator)
	free(lb, context.allocator)
	jsonrpc.conn_destroy(c)

	sync.mutex_lock(&log.mu)
	for rec in log.records {
		delete(rec.body, log.allocator)
	}
	sync.mutex_unlock(&log.mu)
	delete(log.records)
	free(log, context.allocator)
}

// --- a string-id response is absorbed, never matched ------------------------

Absorb_Result :: struct {
	conn: ^jsonrpc.Conn,
	err:  jsonrpc.Call_Err,
	ok:   bool, // the result's marker, read inside the caller thread
	done: bool,
	mu:   sync.Mutex,
	cond: sync.Cond,
}

absorb_caller :: proc(e: ^Absorb_Result) {
	res, _, _, err := jsonrpc.conn_call(e.conn, "svc.any", nil, context.temp_allocator, platform.mono_ms() + 5000)
	// res borrows this thread's temp arena: read the marker here, never
	// after the join.
	e.ok = jsonutil.obj_get_bool(res, "ok")
	sync.mutex_lock(&e.mu)
	e.err = err
	e.done = true
	sync.cond_broadcast(&e.cond)
	sync.mutex_unlock(&e.mu)
}

@(test)
conn_string_id_response_absorbed :: proc(t: ^testing.T) {
	up := new(Pipe, context.allocator) // client -> server (nobody reads)
	pipe_init(up)
	down := new(Pipe, context.allocator) // server -> client
	pipe_init(down)

	c := new(jsonrpc.Conn, context.allocator)
	r: jsonrpc.Reader
	jsonrpc.reader_init(&r, pipe_read, down, 1024, context.allocator)
	w: jsonrpc.Writer
	jsonrpc.writer_init(&w, pipe_write, up)
	jsonrpc.conn_init(c, r, w, context.allocator)
	thr := thread.create_and_start_with_poly_data(c, conn_reader_entry, self_cleanup = false, name = "jsonrpc-absorb-reader")

	res := new(Absorb_Result, context.allocator)
	res^ = {conn = c}
	caller := thread.create_and_start_with_poly_data(res, absorb_caller, self_cleanup = false)

	gate := platform.mono_ms() + 500
	for {
		sync.mutex_lock(&up.mu)
		pending := len(up.buf)
		sync.mutex_unlock(&up.mu)
		if pending > 0 || platform.mono_ms() >= gate {
			break
		}
		time_sleep_ms(1)
	}

	// A string-id response cannot match any slot (the client numbers its
	// own requests): the deliver path must absorb it silently and free the
	// cloned value — the leak gate is the oracle — and the real numeric
	// reply behind it still completes the call.
	sw: jsonrpc.Writer
	jsonrpc.writer_init(&sw, pipe_write, down)
	stray_obj := jsonutil.json_object(1, context.temp_allocator)
	jsonutil.obj_set(&stray_obj, "junk", jsonutil.json_bool(true))
	stray := jsonrpc.build_result_body("weird", true, obj_value(stray_obj), context.temp_allocator)
	testing.expect(t, jsonrpc.write_frame(&sw, transmute([]u8)stray) == .None)
	real_obj := jsonutil.json_object(1, context.temp_allocator)
	jsonutil.obj_set(&real_obj, "ok", jsonutil.json_bool(true))
	real := jsonrpc.build_result_body(1, true, obj_value(real_obj), context.temp_allocator)
	testing.expect(t, jsonrpc.write_frame(&sw, transmute([]u8)real) == .None)

	sync.mutex_lock(&res.mu)
	for !res.done {
		sync.cond_wait(&res.cond, &res.mu)
	}
	got := res.err
	ok := res.ok
	sync.mutex_unlock(&res.mu)
	testing.expect_value(t, got, jsonrpc.Call_Err.None)
	testing.expect_value(t, ok, true)

	thread.join(caller)
	free(caller, context.allocator)
	free(res, context.allocator)
	jsonrpc.conn_close(c)
	pipe_close(down)
	thread.join(thr)
	free(thr, context.allocator)
	jsonrpc.conn_destroy(c)
	free(c, context.allocator)
	pipe_close(up)
	free(up, context.allocator)
	free(down, context.allocator)
}
