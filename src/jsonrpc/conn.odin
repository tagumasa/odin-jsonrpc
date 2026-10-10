// Conn: the connection object shared by servers (dispatch table) and
// clients (correlation table). The connection owns no threads; hosts run
// their own reader loops and call conn_handle_body / conn_read_loop.
package jsonrpc

import "core:encoding/json"
import "core:fmt"
import "core:mem"
import "core:strings"
import "core:sync"
import "core:time"

import "jsonutil:jsonutil"
import "jsonrpc:platform"

// Reply carries a handler's immediate answer. result must be allocated
// from the message arena; it is serialized before the arena is freed.
Reply :: struct {
	is_error:    bool,
	err_code:    Err_Code,
	err_message: string,
	result:      json.Value,
}

Action :: enum {
	Respond,
	Defer, // the host sends the reply later (e.g. from a worker pool)
}

Handler :: proc(conn: ^Conn, env: ^Envelope, arena: mem.Allocator) -> (Reply, Action)
Notifier :: proc(conn: ^Conn, env: ^Envelope, arena: mem.Allocator)

// Cancel_Notify is the optional port a host installs so a cancelled call
// can tell the peer to stop the matched request (the peer's own cancel
// semantics apply). Best effort: the local abandon happens either way.
Cancel_Notify :: proc(c: ^Conn, id: i64)

// Trace_Fn observes every frame crossing the connection (outbound =
// sent, otherwise received). The body is a borrowed view valid only for
// the duration of the call.
Trace_Fn :: proc(user: rawptr, outbound: bool, body: string)

Conn :: struct {
	reader:        Reader,
	writer:        Writer,
	write_mu:      sync.Mutex,
	allocator:     mem.Allocator, // conn-owned allocations (pending slots, tables)
	host:          rawptr,        // opaque host state passed back via handlers

	handlers:      map[string]Handler,
	notifiers:     map[string]Notifier,

	cancel_notify: Cancel_Notify, // nil = cancelled calls abandon silently

	// Optional frame tracing. Set once at connection setup, before the
	// reader starts and before any send — never mutated afterwards, so
	// no lock guards it.
	trace:         Trace_Fn,
	trace_user:    rawptr,

	// The bounded request queue (server->client requests), plus the mutex
	// that makes detaching it race-free: a reader mid-post holds the queue
	// pointer across an envelope clone, so queue_stop must not free the
	// queue while a poster may still be walking it (conn_close can run on
	// another thread than the reader).
	request_queue: ^Request_Queue,
	queue_mu:      sync.Mutex,

	// Optional dedicated writer (see outbound.odin): non-nil = every send
	// queues to one writer thread, so a peer that stops reading never
	// blocks a caller. nil = synchronous sends under write_mu.
	outbound:      ^Outbound,

	pending:       Pending,
	is_closed:     bool,
	closed_mu:     sync.Mutex,
}

// conn_init takes ownership of the passed Reader/Writer state: the Conn keeps
// its own copies and conn_close destroys them. Callers must not reuse or
// destroy the originals afterwards.
conn_init :: proc(c: ^Conn, r: Reader, w: Writer, a := context.allocator) {
	c^ = {
		reader    = r,
		writer    = w,
		allocator = a,
		pending   = {allocator = a, next_id = 1},
	}
}

conn_destroy :: proc(c: ^Conn) {
	queue_stop(c)
	// Mark-then-join: the writer idles on the queue cond, so a destroy
	// without a prior conn_close must still tell it to exit. Callers
	// guarantee the peer process is dead by now (teardown kills it
	// first), so a write wedged in a full pipe has already returned EPIPE.
	outbound_stop(c)
	outbound_join(c)
	pending_fail_all(&c.pending, c.allocator)
	reader_destroy(&c.reader)
	// The method keys are owned clones (see conn_register): collect then
	// free — deleting the current key mid-iteration is map-mutation. The
	// maps and the scratch buffer free through the allocators they store
	// (set at make), whatever this thread's context happens to be.
	names := make([dynamic]string, 0, len(c.handlers) + len(c.notifiers), c.allocator)
	for k, _ in c.handlers {
		append(&names, k)
	}
	for k, _ in c.notifiers {
		append(&names, k)
	}
	for k in names {
		delete(k, c.allocator)
	}
	delete(names)
	delete(c.handlers)
	delete(c.notifiers)
	c^ = {}
}

conn_register :: proc(c: ^Conn, method: string, h: Handler) {
	if c.handlers == nil {
		c.handlers = make(map[string]Handler, 16, c.allocator)
	}
	// The table outlives its registrar, so the key is cloned on first
	// insert: a caller's arena-scoped method string must not rot behind
	// the map's back.
	if _, exists := c.handlers[method]; exists {
		c.handlers[method] = h
	} else {
		c.handlers[strings.clone(method, c.allocator)] = h
	}
}

conn_register_notification :: proc(c: ^Conn, method: string, n: Notifier) {
	if c.notifiers == nil {
		c.notifiers = make(map[string]Notifier, 8, c.allocator)
	}
	if _, exists := c.notifiers[method]; exists {
		c.notifiers[method] = n
	} else {
		c.notifiers[strings.clone(method, c.allocator)] = n
	}
}

conn_is_closed :: proc(c: ^Conn) -> bool {
	sync.mutex_lock(&c.closed_mu)
	v := c.is_closed
	sync.mutex_unlock(&c.closed_mu)
	return v
}

conn_close :: proc(c: ^Conn) {
	// write_mu first: a sender checks `closed` while holding it, so a
	// close can never interleave between the check and the write itself.
	// Lock order write_mu -> closed_mu matches conn_send_body.
	sync.mutex_lock(&c.write_mu)
	sync.mutex_lock(&c.closed_mu)
	c.is_closed = true
	sync.mutex_unlock(&c.closed_mu)
	sync.mutex_unlock(&c.write_mu)
	pending_fail_all(&c.pending, c.allocator)
	// After the waiters are released: stop the request queue (joins its
	// worker — outside every conn lock, so an in-flight handler can still
	// finish its reply attempt against the closed conn), then the writer
	// thread (it drops what is queued; the join happens at destroy).
	queue_stop(c)
	outbound_stop(c)
}

// conn_handle_body decodes one framed body and routes it via
// conn_dispatch. Returns false when a framing-level rejection was answered
// and the caller should stop reading.
conn_handle_body :: proc(c: ^Conn, body: []u8, arena: mem.Allocator) -> bool {
	env, bad_code := decode_envelope(body, arena)
	if env == nil {
		conn_send_error(c, 0, false, bad_code, code_message(bad_code), arena)
		return false
	}
	return conn_dispatch(c, env, arena)
}

// conn_dispatch routes an already-decoded envelope: requests to handlers
// (or Method_Not_Found), notifications to notifiers (unknown ones are
// ignored), responses into the pending table.
conn_dispatch :: proc(c: ^Conn, env: ^Envelope, arena: mem.Allocator) -> bool {
	switch env.kind {
	case .Request:
		if c.request_queue != nil {
			// Bounded dispatch: the reader stays free; a full queue is
			// answered immediately with RequestFailed so the peer learns
			// the outcome instead of waiting on a handler that may never
			// run.
			if queue_try_post(c, env) {
				return true
			}
			conn_send_error(c, env.id, env.id_set, .Request_Failed, "server request queue full", arena)
			return true
		}
		h, found := c.handlers[env.method]
		if !found {
			conn_send_error(
				c, env.id, env.id_set, .Method_Not_Found,
				fmt.aprintf("method not found: %s", env.method, allocator = arena),
				arena,
			)
			return true
		}
		reply, action := h(c, env, arena)
		if action == .Respond {
			conn_send_reply(c, env.id, env.id_set, reply, arena)
		}
	case .Notification:
		n, found := c.notifiers[env.method]
		if found {
			n(c, env, arena)
		}
	case .Response:
		pending_deliver_ok(&c.pending, env)
	case .Error_Response:
		pending_deliver_err(&c.pending, env)
	}
	return true
}

// Per-message arenas: the default allocator serves a block allocation
// through calloc, so a block of the arena's default size
// (mem.DYNAMIC_ARENA_BLOCK_SIZE_DEFAULT) costs that whole block in libc
// zeroing per message; a smaller block cuts the zeroing by the same
// ratio. The size must stay above the out-band threshold
// (mem.DYNAMIC_ARENA_OUT_OF_BAND_SIZE_DEFAULT): a bump request larger
// than the block is Invalid_Argument, so block_size > out_band_size
// keeps every larger allocation out-of-band instead.
MESSAGE_ARENA_BLOCK_SIZE :: 8 * 1024

// conn_read_loop reads frames until the stream ends or close; each message
// gets its own arena which is freed after dispatch. Runs on whatever
// thread the host calls it from; queue-based hosts use read_frame and
// conn_handle_body directly.
conn_read_loop :: proc(c: ^Conn) -> Read_Err {
	for !conn_is_closed(c) {
		// destroy, not free_all: free_all retains the arena's tracking
		// allocation, which would leak per message once the arena leaves scope.
		a: mem.Dynamic_Arena
		mem.dynamic_arena_init(&a, c.allocator, block_size = MESSAGE_ARENA_BLOCK_SIZE)
		body, err := read_frame(&c.reader, mem.dynamic_arena_allocator(&a))
		if err != .None {
			mem.dynamic_arena_destroy(&a)
			// The stream is gone: no reply can ever arrive. Release every
			// waiter now (.Closed) instead of leaving each to burn its own
			// deadline, and latch the table so new calls fail fast.
			pending_fail_all(&c.pending, c.allocator)
			return err
		}
		if c.trace != nil {
			c.trace(c.trace_user, false, string(body))
		}
		keep := conn_handle_body(c, body, mem.dynamic_arena_allocator(&a))
		mem.dynamic_arena_destroy(&a)
		// Frame-loop temp reset: the reader thread's default temp arena
		// would otherwise grow for the life of the connection. Library
		// sends do not touch temp; host handlers scratch on it — the
		// loop owns this thread's temp and resets it per frame.
		free_all(context.temp_allocator)
		if !keep {
			// Reading stops here (framing-level rejection answered): same
			// reasoning — a table no reader serves can never complete.
			pending_fail_all(&c.pending, c.allocator)
			return .Framing
		}
	}
	return .Closed
}

// ---------------------------------------------------------------------------
// Sending. Bodies are serialized in the caller's arena (or temp). A conn
// with the outbound writer installed queues each frame (the writer thread
// is the sole wire writer), and a writer-less conn — the in-memory fakes —
// writes synchronously under the write mutex. Either way the caller's
// allocator must be request/temp-scoped: the outbound queue takes its own
// clone and the synchronous writer only borrows the bytes, so nothing
// frees the original — a long-lived allocator would collect one body per
// send.
// ---------------------------------------------------------------------------

conn_send_reply :: proc(c: ^Conn, id: Id, id_set: bool, reply: Reply, arena: mem.Allocator) {
	if reply.is_error {
		conn_send_error(c, id, id_set, reply.err_code, reply.err_message, arena)
		return
	}
	body := build_result_body(id, id_set, reply.result, arena)
	conn_send_body(c, body, platform.mono_ms() + OUTBOUND_REPLY_TIMEOUT_MS)
}

conn_send_error :: proc(c: ^Conn, id: Id, id_set: bool, code: Err_Code, message: string, a: mem.Allocator) -> bool {
	body := build_error_body(id, id_set, code, message, a)
	return conn_send_body(c, body, platform.mono_ms() + OUTBOUND_REPLY_TIMEOUT_MS)
}

conn_notify :: proc(c: ^Conn, method: string, params: json.Value, arena: mem.Allocator) -> bool {
	body := build_notification_body(method, params, arena)
	return conn_send_body(c, body)
}

// conn_send_body sends (or, on conns with an outbound writer, queues) one
// serialized frame. deadline_ms bounds the outbound queue wait as an
// absolute monotonic timestamp; 0 never waits — a full queue drops the
// frame immediately. The deadline is ignored on synchronous connections:
// their writers are trusted in-process peers, not pipes to a mortal
// child.
conn_send_body :: proc(c: ^Conn, body: string, deadline_ms: i64 = 0) -> bool {
	if c.outbound != nil {
		return outbound_post(c, body, deadline_ms)
	}
	// The closed check runs under write_mu (same order as conn_close):
	// outside the lock it would race a concurrent close and slip a write
	// into a half-torn-down connection.
	sync.mutex_lock(&c.write_mu)
	if conn_is_closed(c) {
		sync.mutex_unlock(&c.write_mu)
		return false
	}
	if c.trace != nil {
		c.trace(c.trace_user, true, body)
	}
	err := write_frame(&c.writer, transmute([]u8)body)
	sync.mutex_unlock(&c.write_mu)
	return err == .None
}

// The body builders serialize every sub-part (quoted strings, marshaled
// values, id text) on the destination allocator together with the final
// body. A caller on a long-lived host thread — the client face's every
// call — must not depend on anyone resetting context.temp_allocator
// underneath it, so a send's whole footprint rides the request-scoped
// allocator the caller passed and dies with that scope.

build_result_body :: proc(id: Id, id_set: bool, result: json.Value, arena: mem.Allocator) -> string {
	result_json := "null"
	if result != nil {
		result_json = jsonutil.marshal_value_unsorted(result, arena)
	}
	return strings.concatenate(
		{
			`{"jsonrpc":"2.0","result":`,
			result_json,
			`,"id":`,
			id_json(id, id_set, arena),
			"}",
		},
		arena,
	)
}

build_error_body :: proc(id: Id, id_set: bool, code: Err_Code, message: string, a: mem.Allocator) -> string {
	return strings.concatenate(
		{
			`{"jsonrpc":"2.0","error":{"code":`,
			fmt.aprintf("%d", i32(code), allocator = a),
			`,"message":`,
			jsonutil.json_quote(message, a),
			`},"id":`,
			id_json(id, id_set, a),
			"}",
		},
		a,
	)
}

build_notification_body :: proc(method: string, params: json.Value, arena: mem.Allocator) -> string {
	// The params member is omitted when nil: the protocol shapes
	// notification params as optional-object, never null — strict client
	// validators reject a literal "params":null frame outright.
	if params == nil {
		return strings.concatenate(
			{`{"jsonrpc":"2.0","method":`, jsonutil.json_quote(method, arena), "}"}, arena,
		)
	}
	return strings.concatenate(
		{
			`{"jsonrpc":"2.0","method":`,
			jsonutil.json_quote(method, arena),
			`,"params":`,
			jsonutil.marshal_value_unsorted(params, arena),
			"}",
		},
		arena,
	)
}

build_request_body :: proc(id: i64, method: string, params: json.Value, a: mem.Allocator) -> string {
	// Same contract as build_notification_body: the params member is
	// omitted when nil — a literal "params":null is not an absent params,
	// and strict peers reject it for methods whose params are optional.
	if params == nil {
		return strings.concatenate(
			{
				`{"jsonrpc":"2.0","id":`,
				fmt.aprintf("%d", id, allocator = a),
				`,"method":`,
				jsonutil.json_quote(method, a),
				"}",
			},
			a,
		)
	}
	return strings.concatenate(
		{
			`{"jsonrpc":"2.0","id":`,
			fmt.aprintf("%d", id, allocator = a),
			`,"method":`,
			jsonutil.json_quote(method, a),
			`,"params":`,
			jsonutil.marshal_value_unsorted(params, a),
			"}",
		},
		a,
	)
}

id_json :: proc(id: Id, id_set: bool, a: mem.Allocator) -> string {
	if !id_set {
		return "null"
	}
	switch v in id {
	case i64:
		return fmt.aprintf("%d", v, allocator = a)
	case string:
		return jsonutil.json_quote(v, a)
	}
	return "null"
}

code_message :: proc(code: Err_Code) -> string {
	switch code {
	case .None:              return "no error"
	case .Parse_Error:       return "parse error"
	case .Invalid_Request:   return "invalid request"
	case .Method_Not_Found:  return "method not found"
	case .Invalid_Params:    return "invalid params"
	case .Internal_Error:    return "internal error"
	case .Server_Retryable: return "retryable server error"
	case .Request_Cancelled: return "request cancelled"
	case .Request_Failed:    return "request failed"
	}
	return "internal error"
}


// ---------------------------------------------------------------------------
// Pending: the client-role correlation table. Replies are copy-in: the
// slot clones what it needs into the connection allocator, so the reader
// thread's message arena can be freed while a waiter still reads the slot.
// ---------------------------------------------------------------------------

// Call_Err is the closed client-role outcome vocabulary. err_code and
// err_message of conn_call are authoritative only for .Error_Response; on
// every other outcome they are .None / a best-effort hint, and callers
// must branch on call_err (never on the code).
Call_Err :: enum {
	None,
	Timeout,
	Cancelled,      // the caller's cancel token fired before the reply
	Closed,
	Transport,      // the exchange failed below the protocol level
	Error_Response, // the peer answered with a JSON-RPC error object
}

// Pending tracks in-flight requests by id. The client numbers every
// request it sends itself, so numeric ids are the only match keys — a
// peer echoing a string id cannot correspond to any slot.
Pending :: struct {
	mu:        sync.Mutex,
	num:       map[i64]^Slot,
	next_id:   i64,
	is_closed: bool,
	allocator: mem.Allocator,
}

Slot :: struct {
	mu:          sync.Mutex,
	cond:        sync.Cond,
	is_done:     bool,
	is_failed:    bool,   // connection died before the reply arrived
	is_abandoned: bool,   // waiter timed out; the deliver path frees the slot
	value:       json.Value, // deep-cloned result (owned by the pending table); nil for error replies
	is_error:    bool,
	err_code:    Err_Code,
	err_message: string, // owned copy
}

// conn_call sends a request and blocks until the reply, the deadline
// (monotonic ms, 0 = none), the optional cancel token, or connection
// failure. The result is deep-copied into `arena`. See Call_Err for
// the outcome contract.
conn_call :: proc(
	c: ^Conn,
	method: string,
	params: json.Value,
	arena: mem.Allocator,
	deadline_ms: i64,
	token: ^platform.Cancel_Token = nil,
) -> (result: json.Value, err_code: Err_Code, err_message: string, call_err: Call_Err) {
	// A nil conn is the no-connection spelling — a host re-reading its
	// connection pointer can race a teardown that publishes nil. The
	// caller gets a typed refusal, never a deref.
	if c == nil {
		return nil, .None, "no connection", .Closed
	}
	sync.mutex_lock(&c.pending.mu)
	if c.pending.is_closed {
		sync.mutex_unlock(&c.pending.mu)
		return nil, .None, "connection closed", .Closed
	}
	id := c.pending.next_id
	c.pending.next_id += 1
	slot := new(Slot, c.allocator)
	slot^ = {}
	if c.pending.num == nil {
		c.pending.num = make(map[i64]^Slot, 16, c.allocator)
	}
	c.pending.num[id] = slot
	sync.mutex_unlock(&c.pending.mu)

	body := build_request_body(id, method, params, arena)
	if !conn_send_body(c, body, deadline_ms) {
		if abandon_slot(&c.pending, id, slot, c.allocator) {
			return nil, .None, "transport failure", .Transport
		}
		// abandon lost the race with a concurrent fail_all: the slot is
		// done and this caller owns it — fall through to the processing
		// tail so it is read and freed instead of leaking (the outcome
		// reports as .Closed, which is what raced the send).
	} else {
		delivered, cancelled := slot_wait(slot, deadline_ms, token)
		if cancelled {
			// Best effort: tell the peer to stop the matched request,
			// then stop waiting locally regardless of the answer.
			if c.cancel_notify != nil {
				c.cancel_notify(c, id)
			}
			if abandon_slot(&c.pending, id, slot, c.allocator) {
				return nil, .None, "cancelled", .Cancelled
			}
			// The reply landed in the race window: process it below.
		} else if !delivered && abandon_slot(&c.pending, id, slot, c.allocator) {
			return nil, .None, "timeout", .Timeout
		}
	}

	sync.mutex_lock(&slot.mu)
	failed := slot.is_failed
	is_error := slot.is_error
	value := slot.value
	slot_code := slot.err_code
	slot_msg := slot.err_message
	sync.mutex_unlock(&slot.mu)

	if failed {
		free(slot, c.allocator)
		return nil, .None, "connection closed", .Closed
	}
	if is_error {
		// The returned message must outlive the slot-owned clone.
		msg := slot_msg
		if len(slot_msg) > 0 {
			msg = strings.clone(slot_msg, arena)
			delete(slot_msg, c.allocator)
		}
		free(slot, c.allocator)
		return nil, slot_code, msg, .Error_Response
	}
	// The result crosses as a deep copy, not a marshal/parse round trip:
	// the slot's clone is duplicated into the caller's arena, then the
	// table-owned copy is released along with the slot itself.
	result = jsonutil.clone_value(value, arena)
	jsonutil.free_value(value, c.allocator)
	free(slot, c.allocator)
	return result, .None, "", .None
}

CANCEL_POLL_SLICE_MS :: i64(50) // cancel-token checkpoint cadence in slot_wait

// slot_wait blocks until the reply lands, the deadline passes (0 = none),
// or the optional token fires. cancelled=true means the token fired
// before the reply; the caller then owns the abandon decision.
slot_wait :: proc(s: ^Slot, deadline_ms: i64, token: ^platform.Cancel_Token = nil) -> (delivered: bool, cancelled: bool) {
	sync.mutex_lock(&s.mu)
	for !s.is_done {
		if token != nil {
			if _, fired := platform.token_check(token); fired {
				sync.mutex_unlock(&s.mu)
				return false, true
			}
		}
		if deadline_ms == 0 && token == nil {
			sync.cond_wait(&s.cond, &s.mu)
			continue
		}
		wait_ms := CANCEL_POLL_SLICE_MS
		if deadline_ms != 0 {
			now := platform.mono_ms()
			if now >= deadline_ms {
				sync.mutex_unlock(&s.mu)
				return false, false
			}
			remaining := deadline_ms - now
			if remaining < wait_ms {
				wait_ms = remaining
			}
		}
		// Real-time slice by design: this foundation
		// layer parks on wall time; the deadline it counts down reads the
		// process monotonic clock directly.
		sync.cond_wait_with_timeout(&s.cond, &s.mu, time.Duration(wait_ms * 1_000_000))
	}
	sync.mutex_unlock(&s.mu)
	return true, false
}

pending_deliver_ok :: proc(p: ^Pending, env: ^Envelope) {
	// The result is deep-copied straight into the pending table's own
	// allocator — no marshal/clone/re-parse round trip; the waiter clones
	// out of the slot into its arena.
	value := json.Value(nil)
	if env.result_set && env.result != nil {
		value = jsonutil.clone_value(env.result, p.allocator)
	}
	pending_deliver(p, env.id, env.id_set, value, false, .Internal_Error, "")
}

pending_deliver_err :: proc(p: ^Pending, env: ^Envelope) {
	// No clone here: env.err_message lives in the caller's message arena and
	// pending_deliver copy-in's it into the table's own allocator; cloning
	// twice burns the arena for nothing.
	msg := env.err_message
	if msg == "" {
		msg = code_message(env.err_code)
	}
	pending_deliver(p, env.id, env.id_set, nil, true, env.err_code, msg)
}

// pending_deliver finds the slot for a normalized id, copy-in's the reply,
// wakes the waiter, and removes the entry. The waiter frees the slot —
// unless it timed out and set is_abandoned first, in which case the
// abandoned branch below frees it here. `value` must already be owned by
// the pending table's allocator
// (ok replies arrive pre-cloned; error replies pass nil); the error
// message is cloned here. The caller's per-message arena may be freed as
// soon as the waiter wakes.
pending_deliver :: proc(p: ^Pending, id: Id, id_set: bool, value: json.Value, is_error: bool, code: Err_Code, message: string) {
	key_i: i64
	numeric := false
	if id_set {
		#partial switch v in id {
		case i64:
			key_i = v
			numeric = true
		}
	}

	slot: ^Slot
	sync.mutex_lock(&p.mu)
	if numeric && p.num != nil {
		if s, found := p.num[key_i]; found {
			slot = s
			delete_key(&p.num, key_i)
		}
	}
	sync.mutex_unlock(&p.mu)

	if slot == nil {
		jsonutil.free_value(value, p.allocator)
		return
	}
	msg_copy := message
	if len(message) > 0 {
		msg_copy = strings.clone(message, p.allocator)
	}
	sync.mutex_lock(&slot.mu)
	if slot.is_abandoned {
		// The waiter timed out and handed the slot to us.
		sync.mutex_unlock(&slot.mu)
		jsonutil.free_value(value, p.allocator)
		if len(msg_copy) > 0 {
			delete(msg_copy, p.allocator)
		}
		free(slot, p.allocator)
		return
	}
	slot.value = value
	slot.is_error = is_error
	slot.err_code = code
	slot.err_message = msg_copy
	slot.is_done = true
	sync.cond_broadcast(&slot.cond)
	sync.mutex_unlock(&slot.mu)
}

// abandon_slot handles a waiter whose call failed or timed out. It marks the
// slot abandoned under its mutex, then removes it from the table. Returns
// false when the reply landed in the meantime (done is set — process it
// normally); true when the call is over, in which case the slot has been
// freed here unless an in-flight deliver took ownership via the abandoned
// flag and will free it there.
abandon_slot :: proc(p: ^Pending, id: i64, s: ^Slot, a: mem.Allocator) -> bool {
	sync.mutex_lock(&s.mu)
	if s.is_done {
		sync.mutex_unlock(&s.mu)
		return false
	}
	s.is_abandoned = true
	sync.mutex_unlock(&s.mu)

	owned := false
	sync.mutex_lock(&p.mu)
	if p.num != nil {
		if _, found := p.num[id]; found {
			delete_key(&p.num, id)
			owned = true
		}
	}
	sync.mutex_unlock(&p.mu)
	if owned {
		free(s, a)
	}
	return true
}

pending_fail_all :: proc(p: ^Pending, a: mem.Allocator) {
	sync.mutex_lock(&p.mu)
	p.is_closed = true
	num := p.num
	p.num = nil
	sync.mutex_unlock(&p.mu)

	for _, slot in num {
		sync.mutex_lock(&slot.mu)
		slot.is_failed = true
		slot.is_done = true
		abandoned := slot.is_abandoned
		sync.cond_broadcast(&slot.cond)
		sync.mutex_unlock(&slot.mu)
		if abandoned {
			// The waiter timed out and handed the slot over; its table
			// lookup found nothing (the map was already detached here), so
			// this side is the last observer and frees the slot.
			free(slot, a)
		}
	}
	if num != nil {
		// The map frees through the allocator it stored at make.
		delete(num)
	}
}
