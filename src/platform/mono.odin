// Monotonic time: the real per-OS clock plus the injectable override the
// deterministic tests drive. Liveness, deadlines, and TTLs only ever see
// this monotonic domain (never wall time).
package platform

// Real monotonic milliseconds since an arbitrary epoch (process boot order
// is fine; only differences are meaningful). Reads mono_override while one
// is installed (the test seam), the real clock otherwise.
mono_ms :: proc() -> i64 {
	if o := &mono_override; o.fn != nil {
		return o.fn(o.data)
	}
	return mono_ns() / 1_000_000
}

// The package's one module-level variable, and it is a reserved seam: a
// non-nil fn stands in for the real clock everywhere in the process.
// Install before the threads under test start and clear after they are
// joined, so the variable itself never races; the fake clock behind it
// must be mutex-guarded because the code under test reads deadlines from
// its own threads. No suite wires it — the suites settle with
// bounded real-time polls around an observed condition; the seam stays
// for deadline-only logic that needs a virtual clock.
mono_override: struct {
	fn:   proc(data: rawptr) -> i64,
	data: rawptr,
}

// mono_override_set installs a virtual clock; mono_override_clear
// restores the real one.
mono_override_set :: proc(fn: proc(data: rawptr) -> i64, data: rawptr) {
	mono_override = {fn = fn, data = data}
}

mono_override_clear :: proc() {
	mono_override = {}
}
