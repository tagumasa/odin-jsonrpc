#+build windows

// Monotonic nanoseconds on Windows via QueryPerformanceCounter, already
// declared by core:sys/windows (the core library exposes no monotonic
// primitive; core:time's now() is wall time).
package platform

import "core:sys/windows"

mono_ns :: proc() -> i64 {
	freq_li: windows.LARGE_INTEGER
	counter_li: windows.LARGE_INTEGER
	if !windows.QueryPerformanceFrequency(&freq_li) || !windows.QueryPerformanceCounter(&counter_li) {
		return 0
	}
	freq := (cast(^i64)(&freq_li))^
	counter := (cast(^i64)(&counter_li))^
	if freq == 0 {
		return 0
	}
	// Split the scaling: counter * 1e9 overflows i64 after ~15 minutes at a
	// typical 10 MHz QPC. remainder < freq keeps the second product bounded.
	secs := counter / freq
	rem := counter % freq
	return secs * 1_000_000_000 + (rem * 1_000_000_000) / freq
}
