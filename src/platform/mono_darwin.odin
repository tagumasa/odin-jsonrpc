#+build darwin

// Monotonic nanoseconds on darwin via libSystem's clock_gettime (the core
// library exposes no monotonic primitive; core:time's now() is wall time,
// and core:sys/unix on darwin has no clock wrapper).
package platform

foreign import libc "system:System"

@(default_calling_convention = "c")
foreign libc {
	clock_gettime :: proc(clk_id: i32, tp: ^Timespec) -> i32 ---
}

// Darwin's clockid numbering is not Linux's: CLOCK_MONOTONIC is 6 here
// (1 is an invalid clock id on darwin — clock_gettime returns EINVAL and
// every deadline computed from this clock would never fire).
CLOCK_MONOTONIC_ID :: i32(6)

Timespec :: struct {
	sec:  i64,
	nsec: i64,
}

mono_ns :: proc() -> i64 {
	ts: Timespec
	res := clock_gettime(CLOCK_MONOTONIC_ID, &ts)
	if res != 0 {
		return 0
	}
	return ts.sec * 1_000_000_000 + ts.nsec
}
