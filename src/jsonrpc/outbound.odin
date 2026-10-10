// Outbound: the optional dedicated writer thread for a Conn. When
// installed, every frame crosses the wire from this one thread, fed by a
// bounded queue — a peer that stops reading (a hung server with a full
// stdin pipe) then blocks exactly one thread instead of every sender:
// posts fail once the queue is full past their deadline, so no
// caller-side lock ever queues behind a pipe write. A failed write marks
// the connection broken (callers observe a closed conn); teardown unwedges
// the thread because the owner kills the peer process first (the kill
// turns the blocked write into EPIPE), and conn_destroy joins it.
package jsonrpc

import "core:mem"
import "core:strings"
import "core:sync"
import "core:thread"
import "core:time"

import "jsonrpc:platform"

OUTBOUND_FRAMES_CAP :: 64
OUTBOUND_BYTES_CAP :: 8 * 1024 * 1024
// Replies to peer-initiated requests wait briefly for queue space before
// dropping (the peer re-asks or times out on its own side).
OUTBOUND_REPLY_TIMEOUT_MS :: i64(500)
OUTBOUND_POST_SLICE_MS :: i64(25)

Outbound :: struct {
	mu:           sync.Mutex,
	cond:         sync.Cond,
	queue:        [dynamic]string, // FIFO of owned clones (alloc)
	queued_bytes: int,
	frames_cap:   int,
	bytes_cap:    int,
	is_stopping:  bool, // conn_close/teardown: drop everything and exit
	is_broken:    bool, // a write failed: the connection is dead
	thread:       ^thread.Thread,
	allocator:    mem.Allocator,
	conn:         ^Conn,
}

// conn_start_outbound installs the writer thread. Call once, after
// conn_init and before the first send. False (and nothing installed) when
// the thread cannot start — callers fail closed.
conn_start_outbound :: proc(c: ^Conn, frames_cap: int, bytes_cap: int) -> bool {
	frames := frames_cap
	if frames < 1 {
		frames = 1
	}
	cap_bytes := bytes_cap
	if cap_bytes < 1 {
		cap_bytes = 1
	}
	out := new(Outbound, c.allocator)
	out^ = {
		frames_cap = frames,
		bytes_cap  = cap_bytes,
		allocator  = c.allocator,
		conn       = c,
	}
	out.queue = make([dynamic]string, 0, frames, c.allocator)
	// The core allocates the ^Thread handle from the installing thread's
	// ambient context.allocator while outbound_join frees it through
	// out.allocator: pin the ambient for the spawn so both sides name the
	// same owner — an installer running on an arena or temp allocator
	// would otherwise hand teardown an unfreeable handle.
	thread_alloc := context.allocator
	context.allocator = out.allocator
	out.thread = thread.create_and_start_with_poly_data(
		out, outbound_thread_main, self_cleanup = false, name = "jsonrpc-outbound",
	)
	context.allocator = thread_alloc
	if out.thread == nil {
		delete(out.queue)
		free(out, c.allocator)
		return false
	}
	c.outbound = out
	return true
}

// outbound_post queues one frame for the writer. deadline_ms is an
// absolute monotonic timestamp; 0 never waits — a full queue drops the
// frame immediately (the notification policy: the next change retries).
// The single-oversize rule admits a frame larger than bytes_cap when the
// queue is empty, so a large didOpen still flows under a small budget.
outbound_post :: proc(c: ^Conn, body: string, deadline_ms: i64) -> bool {
	out := c.outbound
	if out == nil {
		return false
	}
	sync.mutex_lock(&out.mu)
	for {
		if out.is_stopping || out.is_broken {
			sync.mutex_unlock(&out.mu)
			return false
		}
		fits := len(out.queue) < out.frames_cap &&
			(len(out.queue) == 0 || out.queued_bytes + len(body) <= out.bytes_cap)
		if fits {
			frame := strings.clone(body, out.allocator)
			append(&out.queue, frame)
			out.queued_bytes += len(frame)
			sync.cond_broadcast(&out.cond)
			sync.mutex_unlock(&out.mu)
			if c.trace != nil {
				c.trace(c.trace_user, true, body)
			}
			return true
		}
		if deadline_ms == 0 {
			sync.mutex_unlock(&out.mu)
			return false
		}
		now := platform.mono_ms()
		if now >= deadline_ms {
			sync.mutex_unlock(&out.mu)
			return false
		}
		wait := OUTBOUND_POST_SLICE_MS
		if deadline_ms - now < wait {
			wait = deadline_ms - now
		}
		// Real-time slice by design (same as slot_wait): posters poll the
		// drained queue; only the deadline bounds them.
		sync.cond_wait_with_timeout(&out.cond, &out.mu, time.Duration(wait * 1_000_000))
	}
}

outbound_thread_main :: proc(out: ^Outbound) {
	c := out.conn
	for {
		sync.mutex_lock(&out.mu)
		for len(out.queue) == 0 && !out.is_stopping && !out.is_broken {
			sync.cond_wait(&out.cond, &out.mu)
		}
		if out.is_stopping || out.is_broken {
			outbound_drop_locked(out)
			sync.mutex_unlock(&out.mu)
			return
		}
		frame := out.queue[0]
		ordered_remove(&out.queue, 0)
		out.queued_bytes -= len(frame)
		sync.mutex_unlock(&out.mu)

		// The sole writer for this conn: no write_mu — frame order is
		// the queue order.
		err := write_frame(&c.writer, transmute([]u8)frame)
		delete(frame, out.allocator)
		// Per-frame temp reset (the frame loop's idiom): the library send
		// path is temp-free, but the host write_fn under write_frame may
		// scratch on this thread's temp, and the writer has no enclosing
		// loop to reset it.
		free_all(context.temp_allocator)
		if err != .None {
			// The pipe is gone (dead or killed peer, closed stdin): the
			// connection is dead. Mark it, release every waiter, and let
			// teardown join us.
			sync.mutex_lock(&out.mu)
			out.is_broken = true
			outbound_drop_locked(out)
			sync.cond_broadcast(&out.cond)
			sync.mutex_unlock(&out.mu)
			outbound_fail_conn(c)
			return
		}
	}
}

// outbound_fail_conn marks the connection closed and releases every
// pending waiter after the writer died. Deliberately not conn_close: its
// queue_stop joins the request-queue worker, and the owning thread owns
// that teardown.
outbound_fail_conn :: proc(c: ^Conn) {
	sync.mutex_lock(&c.closed_mu)
	c.is_closed = true
	sync.mutex_unlock(&c.closed_mu)
	pending_fail_all(&c.pending, c.allocator)
}

// outbound_stop asks the writer to exit (dropping queued frames — every
// waiter was already released by then). Called from conn_close on the
// owning thread. The writer may be blocked mid-write in a wedged pipe;
// joining happens in conn_destroy, by which time the owner has killed the
// peer.
outbound_stop :: proc(c: ^Conn) {
	out := c.outbound
	if out == nil {
		return
	}
	sync.mutex_lock(&out.mu)
	out.is_stopping = true
	sync.cond_broadcast(&out.cond)
	sync.mutex_unlock(&out.mu)
}

// outbound_join tears the writer down: join (the caller guarantees the
// peer is dead, so a wedged write has turned into EPIPE), then free the
// queue and state.
outbound_join :: proc(c: ^Conn) {
	out := c.outbound
	if out == nil {
		return
	}
	thread.join(out.thread)
	free(out.thread, out.allocator)
	for f in out.queue {
		delete(f, out.allocator)
	}
	delete(out.queue)
	free(out, out.allocator)
	c.outbound = nil
}

outbound_drop_locked :: proc(out: ^Outbound) {
	for f in out.queue {
		delete(f, out.allocator)
	}
	resize(&out.queue, 0)
	out.queued_bytes = 0
}
