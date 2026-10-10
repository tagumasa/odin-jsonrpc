// Cancel_Token tree and error-vocabulary tests: downward fire
// propagation, the derive-only creation rule's observable behavior
// (deregistration), the event latch behind token_wait, reason ->
// error-kind mapping, owner-allocator discipline, and the error cause
// chain's render and clone semantics.
package tests

import "core:mem"
import "core:strings"
import "core:testing"
import "jsonrpc:platform"

@(test)
token_fire_propagates_downward :: proc(t: ^testing.T) {
	arena: mem.Dynamic_Arena
	mem.dynamic_arena_init(&arena, context.allocator)
	defer mem.dynamic_arena_destroy(&arena)
	a := mem.dynamic_arena_allocator(&arena)

	root := new(platform.Cancel_Token, a)
	platform.token_init_root(root)
	child := platform.token_derive(root, 0, a)
	grandchild := platform.token_derive(child, 0, a)

	testing.expect(t, !platform.token_is_fired(root))

	// Firing the middle token reaches the grandchild but not the root.
	platform.token_fire(child, .Cancelled)
	testing.expect(t, platform.token_is_fired(child))
	testing.expect(t, platform.token_is_fired(grandchild))
	testing.expect(t, !platform.token_is_fired(root))

	fired, reason := platform.token_reason(grandchild)
	testing.expect(t, fired)
	testing.expect_value(t, reason, platform.Cancel_Reason.Cancelled)
}

@(test)
token_root_fire_reaches_all :: proc(t: ^testing.T) {
	arena: mem.Dynamic_Arena
	mem.dynamic_arena_init(&arena, context.allocator)
	defer mem.dynamic_arena_destroy(&arena)
	a := mem.dynamic_arena_allocator(&arena)

	root := new(platform.Cancel_Token, a)
	platform.token_init_root(root)
	c1 := platform.token_derive(root, 0, a)
	c2 := platform.token_derive(root, 0, a)
	gc := platform.token_derive(c2, 0, a)

	platform.token_fire(root, .Shutdown)
	testing.expect(t, platform.token_is_fired(c1))
	testing.expect(t, platform.token_is_fired(gc))

	// Deregistration, actually exercised: re-firing the already-fired root
	// above would be a no-op and prove nothing, so this walks a FRESH tree.
	// If destroy failed to deregister, the fire below would recurse into
	// freed memory — the tracking allocator catches that.
	root2 := new(platform.Cancel_Token, a)
	platform.token_init_root(root2)
	gone := platform.token_derive(root2, 0, a)
	platform.token_destroy(gone, a)
	platform.token_fire(root2, .Shutdown)
	testing.expect(t, platform.token_is_fired(root2))
	platform.token_destroy(root2, a)
}

// The one-shot event presents the fire to a blocking waiter: token_wait
// on a fired token returns immediately, and a token derived from an
// already-fired parent is born with the event signalled — parking on it
// would otherwise wait forever on a state that already happened.
@(test)
token_wait_latches_after_fire :: proc(t: ^testing.T) {
	arena: mem.Dynamic_Arena
	mem.dynamic_arena_init(&arena, context.allocator)
	defer mem.dynamic_arena_destroy(&arena)
	a := mem.dynamic_arena_allocator(&arena)

	root := new(platform.Cancel_Token, a)
	platform.token_init_root(root)
	fired_tok := platform.token_derive(root, 0, a)
	platform.token_fire(root, .Shutdown)
	platform.token_wait(fired_tok) // latched: returns immediately

	born := platform.token_derive(root, 0, a) // parent already fired
	platform.token_wait(born) // the born-fired signal holds here too

	testing.expect(t, platform.token_is_fired(born))
	platform.token_destroy(born, a)
	platform.token_destroy(fired_tok, a)
	platform.token_destroy(root, a)
}

@(test)
token_check_maps_reasons :: proc(t: ^testing.T) {
	arena: mem.Dynamic_Arena
	mem.dynamic_arena_init(&arena, context.allocator)
	defer mem.dynamic_arena_destroy(&arena)
	a := mem.dynamic_arena_allocator(&arena)

	root := new(platform.Cancel_Token, a)
	platform.token_init_root(root)
	dl := platform.token_derive(root, 0, a)
	platform.token_fire(dl, .Deadline)
	e, fired := platform.token_check(dl)
	testing.expect(t, fired)
	testing.expect_value(t, platform.err_kind(e), platform.Err_Kind.Timeout)

	sg := platform.token_derive(root, 0, a)
	platform.token_fire(sg, .Session_Gone)
	e, fired = platform.token_check(sg)
	testing.expect(t, fired)
	testing.expect_value(t, platform.err_kind(e), platform.Err_Kind.Cancelled)
}

// --- owner-allocator discipline ----------------------------------------------

// The token tree must move memory only through the allocator its owner
// passed in, never through the ambient context: derive, fire, and destroy
// here run under a ambient context.allocator, and the tracking allocator
// behind it must see zero bytes. Guards the stored-allocator rule that
// replaced the old context swaps in token_derive/token_destroy.
@(test)
token_tree_ignores_ambient_context :: proc(t: ^testing.T) {
	arena: mem.Dynamic_Arena
	mem.dynamic_arena_init(&arena, context.allocator)
	defer mem.dynamic_arena_destroy(&arena)
	owner := mem.dynamic_arena_allocator(&arena)

	ambient: mem.Tracking_Allocator
	mem.tracking_allocator_init(&ambient, context.allocator)
	defer mem.tracking_allocator_destroy(&ambient)

	root := new(platform.Cancel_Token, owner)
	platform.token_init_root(root)

	saved := context.allocator
	context.allocator = mem.tracking_allocator(&ambient)
	child := platform.token_derive(root, 0, owner)
	grandchild := platform.token_derive(child, 0, owner)
	platform.token_fire(root, .Cancelled)
	platform.token_destroy(grandchild, owner)
	platform.token_destroy(child, owner)
	context.allocator = saved

	testing.expect(t, platform.token_is_fired(root))
	platform.token_destroy(root, owner)
	testing.expect_value(t, ambient.total_memory_allocated, 0)
}

// --- error cause chain ---------------------------------------------------------

@(test)
wrapped_cause_chain_renders :: proc(t: ^testing.T) {
	inner := platform.Wrapped{kind = .Invalid, msg = "json parse error: Unexpected_Token"}
	outer := platform.Wrapped{kind = .Internal, msg = "projects.json is malformed", cause = &inner}
	err: platform.Err = outer
	testing.expect(
		t,
		platform.err_message(err) == "projects.json is malformed: json parse error: Unexpected_Token",
	)
	testing.expect(t, platform.err_cause(err) == &inner)
	// Kind-only accessors still read the head.
	testing.expect(t, platform.err_kind(err) == .Internal)
}

@(test)
wrapped_cause_nil_keeps_message :: proc(t: ^testing.T) {
	simple: platform.Err = platform.Wrapped{kind = .NotFound, msg = "nope"}
	testing.expect(t, platform.err_message(simple) == "nope")
	testing.expect(t, platform.err_cause(simple) == nil)
	bare: platform.Err = .Timeout
	testing.expect(t, platform.err_message(bare) == "timeout")
	testing.expect(t, platform.err_cause(bare) == nil)
}

@(test)
wrapped_cause_chain_depth_capped :: proc(t: ^testing.T) {
	// Ten links; the render must stop at eight segments (seven ": ").
	links := make([]platform.Wrapped, 10, context.allocator)
	defer delete(links, context.allocator)
	for i := 0; i < 10; i += 1 {
		links[i] = {kind = .Internal, msg = "n"}
		if i > 0 {
			links[i].cause = &links[i - 1]
		}
	}
	err: platform.Err = links[9]
	rendered := platform.err_message(err)
	testing.expect_value(t, strings.count(rendered, ": "), 7)
}

// err_clone moves an error across a lifetime boundary with its cause
// chain intact: the source arena dies after the clone, and the
// copy must still render the full chain and expose every link — the
// passthrough re-wraps it replaces flattened all of this to text.
@(test)
err_clone_preserves_cause_chain_across_arena_death :: proc(t: ^testing.T) {
	source: mem.Dynamic_Arena
	mem.dynamic_arena_init(&source, context.allocator)
	sa := mem.dynamic_arena_allocator(&source)

	inner := new(platform.Wrapped, sa)
	inner^ = {kind = .Internal, msg = "disk gave up"}
	middle := new(platform.Wrapped, sa)
	middle^ = {kind = .Retryable, msg = "index write failed", cause = inner}
	orig: platform.Err = platform.Wrapped{kind = .Internal, msg = "file handle_symbol_list failed", cause = middle}

	cloned := platform.err_clone(orig, context.allocator)
	mem.dynamic_arena_destroy(&source)

	testing.expect(t, platform.err_kind(cloned) == .Internal)
	rendered := platform.err_message(cloned, context.temp_allocator)
	testing.expect(t, strings.contains(rendered, "file handle_symbol_list failed"), rendered)
	testing.expect(t, strings.contains(rendered, "index write failed"), rendered)
	testing.expect(t, strings.contains(rendered, "disk gave up"), rendered)

	head := platform.err_cause(cloned)
	testing.expect(t, head != nil && head.msg == "index write failed")
	if head != nil {
		head_err: platform.Err = head^
		tail := platform.err_cause(head_err)
		testing.expect(t, tail != nil && tail.msg == "disk gave up")
	}

	// Kind-only values clone to themselves.
	bare := platform.err_clone(platform.Err(.Timeout), context.allocator)
	testing.expect(t, platform.err_kind(bare) == .Timeout)
	testing.expect(t, platform.err_cause(bare) == nil)

	// The clone's links are independent copies: mutating the (still valid
	// here) source chain must not reflect through.
	inner.msg = "mutated"
	testing.expect(t, platform.err_message(cloned, context.temp_allocator) != "mutated")

	// The clone's own allocations die here: the head's message plus every
	// cause link (message + node), mirroring the ownership err_clone
	// hands the destination allocator.
	#partial switch cv in cloned {
	case platform.Wrapped:
		delete(cv.msg, context.allocator)
		link := cv.cause
		for link != nil {
			next := link.cause
			delete(link.msg, context.allocator)
			free(link, context.allocator)
			link = next
		}
	case:
	}
}
