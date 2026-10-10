# odin-jsonrpc build tasks. The packages live under src/ (the collection
# is `jsonrpc:`); the suites are a separate package in tests/ importing
# them through the collection.

# Odin links .exe on Windows; the tool binaries carry the suffix in
# their names so one recipe line builds and invokes them on every OS.
exe := if os_family() == "windows" { ".exe" } else { "" }

# Show available tasks
default:
	@just --list

# Type-check every package with vet and strict style (the wire layer and
# the two vendored support packages).
check:
	odin check src/jsonrpc -collection:jsonrpc=src -vet -strict-style -no-entry-point
	odin check src/jsonutil -collection:jsonrpc=src -vet -strict-style -no-entry-point
	odin check src/platform -collection:jsonrpc=src -vet -strict-style -no-entry-point

# Run the suites single-threaded under the log gate: the runner's own
# exit status is honored first (a compile failure must not read as a
# green run through an empty log), then the verdict is the grep over
# tmp/test.log (zero errors, zero warnings, zero leak lines). grep -a
# because a failing render test can spill NUL bytes and silently match
# nothing.
test:
	#!/bin/sh
	set -e
	mkdir -p tmp
	set +e
	odin test tests -collection:jsonrpc=src -define:ODIN_TEST_THREADS=1 > tmp/test.log 2>&1
	status=$?
	set -e
	cat tmp/test.log
	if [ "$status" -ne 0 ]; then
		echo "test runner failed (exit $status; see tmp/test.log)" >&2
		exit "$status"
	fi
	if grep -a -E '\[ERROR\]|\[FATAL\]|\[WARN \]|\[WARN\]|\+\+\+ leak|bad free|out of range' tmp/test.log; then
		echo "test log gate failed (see tmp/test.log)" >&2
		exit 1
	fi

# Bench the wire hot paths: wall best-of-5 over the scenario table
# (tools/bench). `just bench-callgrind <scenario>` adds the
# deterministic instruction count for one scenario — counts compare
# only between runs of the same scenario in the same host state.
bench:
	@mkdir -p bin
	odin build tools/bench -collection:jsonrpc=src -vet -strict-style -o:speed -out:bin/wire-bench{{exe}}
	./bin/wire-bench{{exe}}

# Instruction counts need valgrind — a Linux host tool; the recipe
# skips loudly where it is not installed.
bench-callgrind scenario:
	#!/bin/sh
	set -e
	if ! command -v valgrind >/dev/null 2>&1; then
		echo "SKIP: valgrind not found (bench-callgrind is a Linux host tool)" >&2
		exit 0
	fi
	mkdir -p bin
	odin build tools/bench -collection:jsonrpc=src -vet -strict-style -o:speed -out:bin/wire-bench{{exe}}
	mkdir -p tmp/cg
	valgrind --tool=callgrind --callgrind-out-file=tmp/cg/{{scenario}}.out ./bin/wire-bench{{exe}} {{scenario}} > /dev/null 2>&1
	callgrind_annotate tmp/cg/{{scenario}}.out | sed -n '20,32p'
