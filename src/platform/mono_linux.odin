#+build linux

// Monotonic nanoseconds via core:sys/linux's clock_gettime wrapper. The
// core library exposes no monotonic primitive of its own (core:time's
// now() is wall time), so this is the thinnest monotonic source available.
package platform

import "core:sys/linux"

mono_ns :: proc() -> i64 {
	ts, err := linux.clock_gettime(linux.Clock_Id.MONOTONIC)
	if err != linux.Errno(0) {
		// Unreachable on a healthy kernel; 0 keeps a flat baseline.
		return 0
	}
	return i64(ts.time_sec) * 1_000_000_000 + i64(ts.time_nsec)
}
