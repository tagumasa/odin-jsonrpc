// Cancellation tokens and the shared error vocabulary; the monotonic
// clock lives in mono.odin. platform is the pure-Odin foundation layer
// the jsonrpc package builds on.
package platform

import "core:mem"
import "core:strings"
import "core:sync"

// ---------------------------------------------------------------------------
// Error vocabulary (design: closed unions per boundary; boundaries convert
// into these kinds explicitly at their edge).
// ---------------------------------------------------------------------------

Err_Kind :: enum {
	Cancelled,   // caller withdrew the request (cancel, shutdown, session gone)
	Retryable,   // transient failure; only read-only tools may retry
	Denied,      // policy rejection (safety guard, permissions)
	NotFound,    // lookup miss (never a panic)
	Timeout,     // deadline exceeded
	Terminated,  // dependency process died mid-request
	Invalid,     // malformed input (tool arguments etc.)
	Internal,    // invariant or unexpected failure
}

// Wrapped is a failure with context. The optional cause chain keeps the
// failure being wrapped instead of flattening it away: wrapping must
// retain the cause. A chain link lives in the same lifetime scope (the
// request arena or the constructing scope's allocator) as its wrapper and
// never crosses an RPC boundary — boundaries render it to text through
// err_message. The message field is the terse `msg` because this is an
// internal type; wire mirrors of the same concept across the boundary
// spell it `err_message` (jsonrpc.Reply, jsonrpc.Envelope) — the two
// names mark the layer they live in.
Wrapped :: struct {
	kind:  Err_Kind,
	msg:   string,
	cause: ^Wrapped, // nil = primary failure
}

Err :: union {Err_Kind, Wrapped}

err_kind :: proc(e: Err) -> Err_Kind {
	switch v in e {
	case Err_Kind:
		return v
	case Wrapped:
		return v.kind
	}
	return .Internal
}

err_cause :: proc(e: Err) -> (cause: ^Wrapped) {
	switch v in e {
	case Err_Kind:
		return nil
	case Wrapped:
		return v.cause
	}
	return nil
}

// err_clone deep-copies an error into `a`, cause chain included: moving an
// error across a lifetime boundary (a scratch arena dying at return) keeps
// the chain instead of flattening it to rendered text. The copy keeps the
// chain-links-share-the-wrapper's-scope property whole — every link lives
// in the same allocator as its wrapper.
err_clone :: proc(e: Err, a := context.allocator) -> Err {
	switch v in e {
	case Err_Kind:
		return v
	case Wrapped:
		return Wrapped{
			kind  = v.kind,
			msg   = strings.clone(v.msg, a),
			cause = wrapped_clone(v.cause, a),
		}
	}
	return .Internal
}

wrapped_clone :: proc(w: ^Wrapped, a: mem.Allocator) -> ^Wrapped {
	if w == nil {
		return nil
	}
	out := new(Wrapped, a)
	out^ = {
		kind  = w.kind,
		msg   = strings.clone(w.msg, a),
		cause = wrapped_clone(w.cause, a),
	}
	return out
}

err_message :: proc(e: Err, allocator := context.temp_allocator) -> string {
	switch v in e {
	case Err_Kind:
		return kind_name(v)
	case Wrapped:
		if v.cause == nil {
			if v.msg != "" {
				return v.msg
			}
			return kind_name(v.kind)
		}
		// Chain render — the same flattening Go's err.Error() gives a %w
		// chain: head message, then each cause joined with ": ". Depth-
		// capped: nothing constructs cycles, but rendering a message must
		// not be able to run away either.
		w := v
		cur := &w
		parts := make([dynamic]string, 0, 4, allocator)
		for depth := 0; depth < ERR_CHAIN_MAX_DEPTH && cur != nil; depth += 1 {
			if cur.msg != "" {
				append(&parts, cur.msg)
			} else {
				append(&parts, kind_name(cur.kind))
			}
			cur = cur.cause
		}
		out, _ := strings.join(parts[:], ": ", allocator)
		delete(parts) // frees through the allocator the array stored at make
		return out
	}
	return kind_name(.Internal)
}

kind_name :: proc(k: Err_Kind) -> string {
	switch k {
	case .Cancelled:   return "cancelled"
	case .Retryable:   return "retryable failure"
	case .Denied:      return "denied"
	case .NotFound:    return "not found"
	case .Timeout:     return "timeout"
	case .Terminated:  return "terminated"
	case .Invalid:     return "invalid input"
	case .Internal:    return "internal error"
	}
	return "internal error"
}

// ---------------------------------------------------------------------------
// Cancel_Token tree. New tokens are created only via token_derive (or one
// root per process). fire() propagates downward. Two wait forms share the
// tree: the library's own waiters poll through token_check's timed slice,
// and hosts that need a blocking wait park on the token's one-shot event
// through token_wait — neither form holds the mutex while waiting, so the
// ownership contract stated at each site covers both.
// ---------------------------------------------------------------------------

Cancel_Reason :: enum {
	Cancelled,
	Deadline,
	Shutdown,
	Terminated,
	Session_Gone,
}

// The cause-chain render bound (see err_message): a message render must
// not be able to run away, cycles or not.
ERR_CHAIN_MAX_DEPTH :: 8

Cancel_Token :: struct {
	parent:   ^Cancel_Token,          // nil only for roots
	children: [dynamic]^Cancel_Token, // guarded by mutex
	reason:   Cancel_Reason,          // set when fired
	deadline: i64,                    // monotonic ms; 0 = none
	mu:       sync.Mutex,
	is_fired: bool,
	event:    sync.One_Shot_Event, // signalled under the mutex at fire; token_wait parks on it
}

token_init_root :: proc(t: ^Cancel_Token) {
	t^ = {}
	t.event = {}
}

// Derive a child token. deadline_ms uses the monotonic clock (0 = inherit
// without own deadline). The effective deadline of a token is the minimum
// of its own and all ancestors'; ancestors propagate their fire downward.
token_derive :: proc(parent: ^Cancel_Token, deadline_ms: i64, a: mem.Allocator) -> ^Cancel_Token {
	t := new(Cancel_Token, a)
	t^ = {
		parent   = parent,
		deadline = deadline_ms,
	}
	sync.mutex_lock(&parent.mu)
	if parent.is_fired {
		// Derived from an already-fired parent: born fired with the same
		// reason so cancel still holds at the token's first checkpoint.
		// Not registered as a child — the fire already happened.
		t.is_fired = true
		t.reason = parent.reason
	} else {
		// The children list is made on the tree allocator at first use and
		// carries it from then on: a dynamic array stores the allocator it
		// was made on, so growth here and the delete in token_destroy go
		// through that one, whatever the calling thread's context happens
		// to be.
		if cap(parent.children) == 0 {
			parent.children = make([dynamic]^Cancel_Token, 0, 4, a)
		}
		append(&parent.children, t)
	}
	sync.mutex_unlock(&parent.mu)
	if t.is_fired {
		// Born fired: the event must present the fire too, or a token_wait
		// on this token parks forever on a state that already happened.
		sync.one_shot_event_signal(&t.event)
	}
	return t
}

// Remove t from its parent's children list; callers invoke this in the
// token's teardown path so a later parent fire cannot touch freed memory.
// Destroy children before their parent: destroying a token frees it and
// drops its children list, so a child destroyed afterwards would touch the
// freed parent's mutex through its back-pointer.
token_destroy :: proc(t: ^Cancel_Token, a: mem.Allocator) {
	if t.parent != nil {
		sync.mutex_lock(&t.parent.mu)
		for i := 0; i < len(t.parent.children); i += 1 {
			if t.parent.children[i] == t {
				ordered_remove(&t.parent.children, i)
				break
			}
		}
		sync.mutex_unlock(&t.parent.mu)
	}
	// Serialize with any in-flight fire on this token before freeing it:
	// fire holds t.mu through its recursion, so by the time the lock is
	// granted no caller can still be walking t (a parent fire would have
	// needed the parent's mutex, held while iterating its children).
	// Waiters are NOT covered by this serialization — a waiter either
	// polls in a timed slice or parks on the event, in both forms without
	// the mutex — so the ownership contract applies: a token is destroyed
	// only after every waiter has returned from its wait, and waiters read
	// state through the accessors (whose mutex survives as long as the
	// token does), never the raw fields.
	sync.mutex_lock(&t.mu)
	// The children list was made on the tree allocator at its first derive
	// (see token_derive); the delete frees it through the allocator the
	// dynamic array stores.
	delete(t.children)
	sync.mutex_unlock(&t.mu)
	free(t, a)
}

token_fire :: proc(t: ^Cancel_Token, reason: Cancel_Reason) {
	sync.mutex_lock(&t.mu)
	if t.is_fired {
		sync.mutex_unlock(&t.mu)
		return
	}
	t.is_fired = true
	t.reason = reason
	sync.one_shot_event_signal(&t.event)
	// Children fire while the mutex is held: derive (append) and destroy
	// (remove + free) take the same mutex, so the list cannot be reallocated
	// or freed mid-iteration (a plain slice alias would race both). Lock
	// order is strictly parent -> child, so the recursion is deadlock-free.
	for c in t.children {
		token_fire(c, reason)
	}
	sync.mutex_unlock(&t.mu)
}

// token_wait blocks until the token fires. The event parks without the
// mutex, so the ownership contract applies: the caller must not destroy
// the token until this call has returned on every waiting thread (destroy
// frees the token; a waiter waking past that point touches freed memory),
// and after the wait the state is read through the accessor procs — a raw
// field read races a concurrent destroy, the accessors' mutex does not.
token_wait :: proc(t: ^Cancel_Token) {
	sync.one_shot_event_wait(&t.event)
}

// token_is_fired is the mutex-guarded fired read for loops that poll the
// token without converting it into the error vocabulary.
token_is_fired :: proc(t: ^Cancel_Token) -> bool {
	sync.mutex_lock(&t.mu)
	f := t.is_fired
	sync.mutex_unlock(&t.mu)
	return f
}

// token_reason reports the fired state and, when fired, the reason. The
// waiter contract of the tree: a token is destroyed only after every
// waiter has returned from its wait (destroy frees the token; a waiter
// waking past that point touches freed memory), and after any wait the
// state is read through token_reason / token_check — a raw field read
// races a concurrent destroy, these accessors' mutex does not.
token_reason :: proc(t: ^Cancel_Token) -> (fired: bool, reason: Cancel_Reason) {
	sync.mutex_lock(&t.mu)
	f := t.is_fired
	r := t.reason
	sync.mutex_unlock(&t.mu)
	return f, r
}

// Convert the current fired state into the error vocabulary; ok=false when
// the token has not fired.
token_check :: proc(t: ^Cancel_Token) -> (e: Err, fired: bool) {
	is_fired, reason := token_reason(t)
	if !is_fired {
		return nil, false
	}
	switch reason {
	case .Cancelled, .Shutdown, .Session_Gone:
		return Err_Kind.Cancelled, true
	case .Deadline:
		return Err_Kind.Timeout, true
	case .Terminated:
		return Err_Kind.Terminated, true
	}
	return Err_Kind.Cancelled, true
}
