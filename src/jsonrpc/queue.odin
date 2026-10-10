// Bounded dispatch for server->client requests. The default Conn dispatches
// incoming requests inline on the reader thread; a connection with a
// started request queue transfers them instead into a bounded chan drained
// by ONE worker. A full queue is answered immediately with RequestFailed
// (-32803): the reader never blocks on handlers and no thread is ever
// created per request. The reader keeps reading past a full queue —
// stopping it would also stall every pending response and notification
// on the same loop, so the rejection reply is the backpressure the
// protocol carries.
package jsonrpc

import "core:fmt"
import "core:mem"
import "core:strings"
import "core:sync"
import "core:sync/chan"
import "core:thread"

import "jsonutil:jsonutil"

REQUEST_QUEUE_CAP :: 16

Queue_Entry :: struct {
	env:   ^Envelope,
	arena: ^mem.Dynamic_Arena, // owns env and everything reachable from it
}

Request_Queue :: struct {
	conn:      ^Conn, // back-pointer: the worker reads handlers/closed state
	ch:        chan.Chan(Queue_Entry, .Send),
	recv:      chan.Chan(Queue_Entry, .Recv),
	worker:    ^thread.Thread,
	allocator: mem.Allocator,
}

// conn_start_request_queue switches incoming-request dispatch from inline
// to the bounded worker queue. Idempotent guard: a conn has at most one.
conn_start_request_queue :: proc(c: ^Conn, queue_cap: int = REQUEST_QUEUE_CAP, a := context.allocator) -> bool {
	if queue_cap <= 0 || c.request_queue != nil {
		return false
	}
	raw, err := chan.create_buffered(chan.Chan(Queue_Entry, .Both), queue_cap, a)
	if err != nil {
		return false
	}
	q := new(Request_Queue, a)
	// Fully initialized BEFORE the thread starts: the worker receives q
	// itself and must never race the c.request_queue publication below.
	q^ = {
		conn  = c,
		ch    = chan.as_send(raw),
		recv  = chan.as_recv(raw),
		allocator = a,
	}
	// The core allocates the ^Thread handle from the installing thread's
	// ambient context.allocator while queue_stop frees it through
	// q.allocator: pin the ambient for the spawn so both sides name the
	// same owner — an installer running on an arena or temp allocator
	// would otherwise hand teardown an unfreeable handle.
	thread_alloc := context.allocator
	context.allocator = a
	q.worker = thread.create_and_start_with_poly_data(q, queue_worker_entry, self_cleanup = false, name = "odin-jsonrpc-req-queue")
	context.allocator = thread_alloc
	if q.worker == nil {
		chan.destroy(q.recv)
		free(q, a)
		return false
	}
	// Published under the queue mutex like every other request_queue
	// access, so the stop/post protocol holds from the first frame.
	sync.mutex_lock(&c.queue_mu)
	c.request_queue = q
	sync.mutex_unlock(&c.queue_mu)
	return true
}

// queue_stop closes the queue, joins the worker, and releases its memory.
// Entries still queued at close are destroyed unopened: the connection is
// going away, so their replies would have no reader. The detach from
// c.request_queue happens under c.queue_mu so a reader posting through
// queue_try_post either finishes its send or observes nil — the queue is
// never freed while a poster may still hold the pointer. Must be called
// from the owning thread (conn_close/conn_destroy), never from the worker
// — it joins the worker.
queue_stop :: proc(c: ^Conn) {
	sync.mutex_lock(&c.queue_mu)
	q := c.request_queue
	if q == nil {
		sync.mutex_unlock(&c.queue_mu)
		return
	}
	c.request_queue = nil
	sync.mutex_unlock(&c.queue_mu)
	chan.close(q.ch)
	thread.join(q.worker)
	free(q.worker, q.allocator)
	chan.destroy(q.recv)
	free(q, q.allocator)
}

queue_worker_entry :: proc(q: ^Request_Queue) {
	c := q.conn
	for {
		entry, ok := chan.recv(q.recv)
		if !ok {
			break
		}
		if !conn_is_closed(c) {
			h, found := c.handlers[entry.env.method]
			if !found {
				conn_send_error(
					c, entry.env.id, entry.env.id_set, .Method_Not_Found,
					fmt.aprintf("method not found: %s", entry.env.method, allocator = mem.dynamic_arena_allocator(entry.arena)),
					mem.dynamic_arena_allocator(entry.arena),
				)
			} else {
				reply, action := h(c, entry.env, mem.dynamic_arena_allocator(entry.arena))
				if action == .Respond {
					conn_send_reply(c, entry.env.id, entry.env.id_set, reply, mem.dynamic_arena_allocator(entry.arena))
				}
			}
		}
		mem.dynamic_arena_destroy(entry.arena)
		free(entry.arena, q.allocator)
		// Per-reply temp reset (the frame loop's idiom): the library send
		// path is temp-free (write_frame formats its header into a stack
		// buffer), but host handlers scratch on this thread's temp, and
		// the worker has no enclosing frame loop to reset it.
		free_all(context.temp_allocator)
	}
}

// queue_try_post clones the envelope into a queue-owned arena and hands it
// to the worker. false = the queue is full (or closing); the caller
// answers the peer with RequestFailed on the reader thread — the reader
// keeps reading (stopping it would also stall every pending response),
// so a flooding peer costs a rejection reply per frame, never a wedged
// conn. The queue pointer is read and the send made under c.queue_mu so
// queue_stop cannot free the queue between the two (try_send never
// blocks, so the hold is bounded). The capacity pre-check keeps the
// full case from cloning an envelope it is about to drop.
queue_try_post :: proc(c: ^Conn, env: ^Envelope) -> bool {
	sync.mutex_lock(&c.queue_mu)
	q := c.request_queue
	if q == nil {
		sync.mutex_unlock(&c.queue_mu)
		return false
	}
	if chan.len(q.ch) >= chan.cap(q.ch) {
		sync.mutex_unlock(&c.queue_mu)
		return false
	}
	arena := new(mem.Dynamic_Arena, q.allocator)
	mem.dynamic_arena_init(arena, q.allocator, block_size = MESSAGE_ARENA_BLOCK_SIZE)
	entry := Queue_Entry{
		env   = envelope_clone(env, mem.dynamic_arena_allocator(arena)),
		arena = arena,
	}
	if !chan.try_send(q.ch, entry) {
		mem.dynamic_arena_destroy(arena)
		free(arena, q.allocator)
		sync.mutex_unlock(&c.queue_mu)
		return false
	}
	sync.mutex_unlock(&c.queue_mu)
	return true
}

// envelope_clone copies an envelope with every string and JSON value it
// reaches into `a`, so the clone outlives the reader's per-message arena.
envelope_clone :: proc(env: ^Envelope, a: mem.Allocator) -> ^Envelope {
	clone := new(Envelope, a)
	clone^ = env^
	clone.method = strings.clone(env.method, a)
	clone.err_message = strings.clone(env.err_message, a)
	switch v in env.id {
	case i64:
	case string:
		// The string variant's bytes live in the reader's per-message
		// arena; without this clone the queued copy dangles once the
		// reader frees it. Numeric ids copy by value with the struct.
		clone.id = strings.clone(v, a)
	}
	if env.params_set {
		clone.params = jsonutil.clone_value(env.params, a)
	}
	if env.result_set {
		clone.result = jsonutil.clone_value(env.result, a)
	}
	return clone
}
