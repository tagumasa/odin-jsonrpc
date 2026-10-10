// Wedge coverage lives here, on the Wedge_Gate fake: a real-pipe wedge
// needs a peer that stays alive while reading nothing, and a scripted
// fake is the only peer that provides one deterministically.
package tests

import "core:mem"
import "core:sync"
import "core:testing"
import "core:time"

import "jsonrpc:jsonrpc"
import "jsonrpc:platform"

// The conns here hand their allocations to the writer thread (frame
// clones die there), so every conn rides a mutex allocator over the
// per-test tracking allocator — cross-thread alloc/free through the raw
// tracking allocator is not safe.
Outbound_Conn :: struct {
	ma:        mem.Mutex_Allocator,
	allocator: mem.Allocator,
	conn:      ^jsonrpc.Conn,
}

outbound_conn_init :: proc(
	oc: ^Outbound_Conn,
	write_proc: proc(data: rawptr, buf: []u8) -> (int, jsonrpc.Read_Err),
	user: rawptr,
) {
	mem.mutex_allocator_init(&oc.ma, context.allocator)
	oc.allocator = mem.mutex_allocator(&oc.ma)
	oc.conn = new(jsonrpc.Conn, oc.allocator)
	w: jsonrpc.Writer
	jsonrpc.writer_init(&w, write_proc, user)
	jsonrpc.conn_init(oc.conn, {}, w, oc.allocator)
}

outbound_conn_destroy :: proc(oc: ^Outbound_Conn) {
	jsonrpc.conn_destroy(oc.conn)
	free(oc.conn, oc.allocator)
}

// --- blocking writer plumbing -------------------------------------------------

Wedge_Gate :: struct {
	hold:    sync.Mutex, // the test holds this while the writer must stay blocked
	guard:   sync.Mutex, // serializes the byte counter and the entry counter
	entered: int,       // write calls that reached the wedge (the pop before them is done)
	wrote:   int,
}

wedged_write :: proc(data: rawptr, buf: []u8) -> (int, jsonrpc.Read_Err) {
	g := cast(^Wedge_Gate)data
	sync.mutex_lock(&g.guard)
	g.entered += 1
	sync.mutex_unlock(&g.guard)
	sync.mutex_lock(&g.hold) // blocks while the test holds the gate
	sync.mutex_unlock(&g.hold)
	sync.mutex_lock(&g.guard)
	g.wrote += len(buf)
	sync.mutex_unlock(&g.guard)
	return len(buf), .None
}

failing_write :: proc(data: rawptr, buf: []u8) -> (int, jsonrpc.Read_Err) {
	return 0, .Io
}

// outbound_queue_drained polls until the writer consumed every queued
// frame (or the bound passes — the test then fails on the byte counter).
outbound_queue_drained :: proc(c: ^jsonrpc.Conn, bound_ms: i64) -> bool {
	deadline := platform.mono_ms() + bound_ms
	for {
		out := c.outbound
		if out == nil {
			return true
		}
		sync.mutex_lock(&out.mu)
		n := len(out.queue)
		sync.mutex_unlock(&out.mu)
		if n == 0 {
			return true
		}
		if platform.mono_ms() >= deadline {
			return false
		}
		// Bounded real-time poll: the writer drains on its own thread.
		time_sleep_ms(10)
	}
}

time_sleep_ms :: proc(ms: i64) {
	time.sleep(time.Duration(ms) * time.Millisecond)
}

// wedge_gate_entered polls until the writer entered its write at least
// `want` times (or the bound passes — the test then fails on its caller).
wedge_gate_entered :: proc(g: ^Wedge_Gate, want: int, bound_ms: i64) -> bool {
	deadline := platform.mono_ms() + bound_ms
	for {
		sync.mutex_lock(&g.guard)
		n := g.entered
		sync.mutex_unlock(&g.guard)
		if n >= want {
			return true
		}
		if platform.mono_ms() >= deadline {
			return false
		}
		time_sleep_ms(10)
	}
}

// --- tests --------------------------------------------------------------------

@(test)
outbound_post_never_blocks_on_wedged_writer :: proc(t: ^testing.T) {
	gate: Wedge_Gate
	sync.mutex_lock(&gate.hold) // the writer will block in write

	oc: Outbound_Conn
	outbound_conn_init(&oc, wedged_write, &gate)
	defer outbound_conn_destroy(&oc)
	testing.expect(t, jsonrpc.conn_start_outbound(oc.conn, 2, 1024))

	// The first frames queue without touching the pipe; the poster never
	// blocks behind the wedged write.
	t0 := platform.mono_ms()
	testing.expect(t, jsonrpc.conn_send_body(oc.conn, "frame-one"))
	testing.expect(t, jsonrpc.conn_send_body(oc.conn, "frame-two"))
	dt := platform.mono_ms() - t0
	testing.expectf(t, dt < 2_000, "posts must not block on the pipe (took %d ms)", dt)

	// The writer pops a frame BEFORE writing it, so a wedged writer holds
	// its frame outside the queue and the pop freed one slot — two posts do
	// not make the queue full. Wait until the writer provably entered the
	// wedged write (it cannot pop again until the gate opens), then refill
	// the freed slot so the queue is deterministically full.
	if !wedge_gate_entered(&gate, 1, 2_000) {
		testing.expectf(t, false, "the writer never entered its wedged write")
		return
	}
	testing.expect(t, jsonrpc.conn_send_body(oc.conn, "frame-three"))
	// Queue full with deadline 0: drop immediately.
	testing.expect(t, !jsonrpc.conn_send_body(oc.conn, "frame-four"))

	// Release: the writer drains every frame through the pipe.
	sync.mutex_unlock(&gate.hold)
	testing.expect(t, outbound_queue_drained(oc.conn, 2_000))
	sync.mutex_lock(&gate.guard)
	wrote := gate.wrote
	sync.mutex_unlock(&gate.guard)
	// write_frame wraps each body with its framing header, so the byte
	// count is body bytes plus headers — assert the bodies all crossed.
	testing.expect(
		t, wrote >= len("frame-one") + len("frame-two") + len("frame-three"),
		"all three frames must cross the wire",
	)
}

@(test)
outbound_write_failure_breaks_conn :: proc(t: ^testing.T) {
	oc: Outbound_Conn
	outbound_conn_init(&oc, failing_write, nil)
	defer outbound_conn_destroy(&oc)
	testing.expect(t, jsonrpc.conn_start_outbound(oc.conn, 4, 1024))

	testing.expect(t, jsonrpc.conn_send_body(oc.conn, "doomed"))
	deadline := platform.mono_ms() + 2_000
	for !jsonrpc.conn_is_closed(oc.conn) {
		if platform.mono_ms() >= deadline {
			break
		}
		time_sleep_ms(5)
	}
	testing.expect(t, jsonrpc.conn_is_closed(oc.conn), "a failed write must close the conn")
	testing.expect(t, !jsonrpc.conn_send_body(oc.conn, "after"), "posts after the break fail fast")
}

@(test)
outbound_request_post_honors_deadline :: proc(t: ^testing.T) {
	gate: Wedge_Gate
	sync.mutex_lock(&gate.hold)

	oc: Outbound_Conn
	outbound_conn_init(&oc, wedged_write, &gate)
	defer outbound_conn_destroy(&oc)
	testing.expect(t, jsonrpc.conn_start_outbound(oc.conn, 2, 1024))

	// Occupy the queue, then send a request whose post must give up at
	// its own deadline (conn_call reports the failure as transport).
	testing.expect(t, jsonrpc.conn_send_body(oc.conn, "filler-one"))
	testing.expect(t, jsonrpc.conn_send_body(oc.conn, "filler-two"))
	call_t0 := platform.mono_ms()
	_, _, _, cerr := jsonrpc.conn_call(
		oc.conn, "poll/while-wedged", nil, context.temp_allocator, platform.mono_ms() + 300,
	)
	call_dt := platform.mono_ms() - call_t0
	// The writer may already have popped a filler and blocked, so the
	// request either waits out its deadline on the slot (.Timeout) or the
	// full queue rejects its post (.Transport) — either way the call is
	// bounded by the deadline, never by the wedged pipe.
	testing.expectf(
		t, cerr == .Timeout || cerr == .Transport,
		"the wedged call must fail at its deadline (got %d)", int(cerr),
	)
	testing.expectf(t, call_dt < 5_000, "the wedged call must not block on the pipe (took %d ms)", call_dt)

	sync.mutex_unlock(&gate.hold)
}
