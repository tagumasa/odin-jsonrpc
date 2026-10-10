# AGENTS.md — odin-jsonrpc

Rules and policy for coding agents on the odin-jsonrpc codebase — not
session records or measurements. User-facing documentation lives in
README.md and docs/.

# Project description

## Overview

odin-jsonrpc is a JSON-RPC 2.0 wire layer for Odin (pure Odin,
`core:*` only, no C dependencies): `src/jsonrpc` — both framings
(Content-Length headers and newline-delimited JSON), the envelope
with ID normalization, and the `Conn` that dispatches requests and
correlates responses, plus the bounded outbound writer and request
queue — built on the vendored odin-jsonutil value layer
(`vendor/odin-jsonutil`, the `jsonutil:` collection). `src/platform`
is the vendored internal support package (the cancel-token tree, the
shared error vocabulary, the monotonic clock). The submodule is
never edited in this tree: value-layer changes are made in the
odin-jsonutil repository, and this tree takes them by a pin bump.
The support package and the value collection are internal surface,
not the library contract, with one exception: the cancel-token tree
is host-facing API, because `conn_call`'s token parameter carries
the `platform.Cancel_Token` type.

Hosts own transport injection (both directions read and write
through function pointers, never fds), thread policy above the
library's own threads, process spawning, reconnect backoff,
registries, permission, and logging (no module-global logger: the
library's surfaces report through their return values).

### Package layout and dependency direction

`src/` is a single collection (imports use the `jsonrpc:` prefix);
the value layer is mounted as the `jsonutil:` collection. Dependencies
flow one way only:

```
vendor/odin-jsonutil (pinned submodule, jsonutil: collection)
src/platform (cancel-token tree, error vocabulary, monotonic clock;
             pure Odin; the one foreign import is darwin's libSystem
             clock_gettime)
  ↑ jsonrpc (both framings, the envelope, the Conn: dispatch and
             correlation, the outbound writer, the request queue)
```

- Lower layers must not import upper layers.
- No process-global variables: state rides the owning struct
  (`jsonrpc.Conn`); `platform.mono_override` is the one sanctioned
  seam (a virtual-clock test hook).
- Consumers mount this tree as the `jsonrpc:` collection
  (`-collection:jsonrpc=<path-to-src>`); the collection name must
  not collide with a host's own collection names.

# Project principles

- **Correctness, processing performance, and resource behaviour are the
  product.** A change that trades any of them for convenience is a
  defect. Enforced in: Design rules; Build, test, and measure.
- **The specification governs the implementation.** Where code and
  specification disagree, one of them is defective — determine which,
  and fix that side. Enforced in: Specification.
- **Structure is load-bearing.** Dependencies flow one way, and every
  rule has one home. Enforced in: Package layout and dependency
  direction; Design rules.
- **Every resource has one owner and one bound.** There is no GC safety
  net: lifetimes are explicit, queues are bounded, and ownership
  transfers are visible at the call site. Enforced in: Design rules.
- **Verdicts come from artifacts.** Suite outcomes are read from the
  suite log, never from the exit status alone; measurements carry
  their method and toolchain. Enforced in: Build, test, and measure;
  docs/benchmarks.md.
- **Write for the next reader.** Comments state constraints the code
  cannot show; no text cites material the reader does not have.
  Enforced in: Comment self-containment.

## Comment self-containment

Committed code, docs, and error strings must be self-contained: no
citations of private notebooks, design-record numbers, or local
machine paths — every such citation dangles for readers without the
referenced material. State the constraint in prose where it matters.

Exact numbers that move while the code evolves (suite sizes, default
values, timings) rot between edits; where a magnitude carries a rule,
name the identifier that owns the exact value.

# Development guide

## Build, test, and measure

```sh
just check        # odin check -vet -strict-style -no-entry-point, per package
just test         # odin test tests (ODIN_TEST_THREADS=1), log-gated
just bench        # wire-path wall benchmarks (best of 5)
```

- The Odin compiler is pinned to `dev-2026-10-nightly:84bc3fc` (the
  frozen `dev-2026-10` release; CI asserts the hash). After a toolchain
  move, re-run every gate and re-measure docs/benchmarks.md.
- Clone with `--recurse-submodules`; the value layer is the
  `vendor/odin-jsonutil` submodule.
- `just test` writes `tmp/test.log`: the runner's exit status is
  honored first (a compile failure is never a green run), then the
  log gate fails on any match of
  `\[ERROR\]|\[FATAL\]|\[WARN \]|\[WARN\]|\+\+\+ leak|bad free|out of range`.
  The `[WARN ]` alternative is load-bearing (the framework prints its
  per-test leak blocks under a padded header); always `grep -a` (a
  failing test can spill NUL bytes and make grep treat the log as
  binary). The leak discipline is zero leak lines.
- Do NOT use `-define:ODIN_TEST_NAMES` single-test builds — they change
  the binary's layout; the filter exists only as a `-define:` (an
  `ODIN_TEST_NAMES` environment variable does not filter at all).
- Wire rule both request builders in `src/jsonrpc/message.odin`
  enforce: request bodies omit the `params` member when it is nil — a
  literal `"params":null` is not an absent params, and strict peers
  reject it. Keep the builders in step.
- `tmp/` is recipe scratch and `bin/` holds the built tool binaries —
  both gitignored.
- Commits are conventional style (`feat:`, `fix:`, …); only when the
  user asks.

# Development rules in detail

## Design rules

- **Naming**: multiword types `Word_Word`; procs snake_case; mutex
  fields `mu`; allocator fields `allocator` (short `a` for procedure
  parameters).
- **Error vocabularies are closed per boundary**: `_Err` for boundary
  enums/unions (`Read_Err`, `Call_Err`, `Err_Code`), `_Error` for
  circumstance-explaining structs; failures propagate through explicit
  checks, and callers branch on the member, never on accompanying
  prose.
- **Every queue is bounded**: the outbound writer (frame and byte
  caps) and the request queue (channel capacity); a full queue is
  answered immediately or drops past its deadline — load is shed at
  the source, never buffered.
- **Memory lifetime is rule-bound**: long-lived maps clone their keys
  on first insert; a value crossing a lifetime boundary is copied into
  the destination allocator; an arena-allocated pointer never escapes
  its scope.
- **Cancellation lives in the cancel-token tree only**
  (`platform.Cancel_Token`): tokens derive from a parent, fire
  propagates downward under a strictly parent-to-child lock order, a
  token derived from an already-fired parent is born fired with its
  event signalled. Two wait forms share the tree — the library's own
  waiters poll through `token_check`'s timed slice; hosts that need a
  blocking wait park on the one-shot event through `token_wait`. The
  ownership contract: a token is destroyed only after every waiter
  has returned from its wait.
- **Monotonic clocks only** for liveness, deadlines, and TTLs
  (`platform.mono_ms`); wall time is display-only.
- **Thread entries go through the typed facade**: every spawn site uses
  `thread.create_and_start_with_poly_data` (`proc(data: $T)`,
  `self_cleanup = false`, the handle freed by the owner) — never the
  rawptr `create_and_start_with_data`. Timer and transport callbacks
  keep the rawptr shape; they are not thread entries.
- **Pure Odin, `core:*` only**: no vendored C and no foreign imports
  but darwin's libSystem `clock_gettime`
  (`src/platform/mono_darwin.odin` — core ships no monotonic wrapper
  there).

## Testing conventions

- Wire deadlines and settle bounds read the monotonic source only.
  Tests settle with bounded real-time polls (1–10 ms sleeps around an
  observed condition: a pipe byte count, an atomic flag, a hook
  counter) — never open-ended spins or unbounded waits.
- `platform.mono_override_set` is the reserved seam for driving
  deadlines from a virtual clock: install before the threads under
  test start, clear after they are joined; no suite wires it.

## Cross-platform

- The justfile's `exe` variable carries the Windows suffix — tool
  binaries build and invoke as `bin/<name>{{exe}}` on every OS.
- The `sh`-scripted recipes (`test`, `bench-callgrind`) need Git
  Bash's `sh` on PATH on Windows; the CI runner image carries it.

# Reference

## Odin language and stdlib

- `matrix` and `distinct` are keywords — a field or local by either
  name is a parse error.
- A named return cannot be shadowed by a multi-value local ("Direct
  shadowing of the named return value").
- `mem.Allocator` is the two-field `{procedure, data}` struct.
- A map carries its creating allocator through growth (the growth
  allocation is `.Alloc_Non_Zeroed` on that allocator), and an insert
  whose growth allocation fails is silently dropped — `len` unmoved,
  no error.

## Specification

- JSON-RPC 2.0 (jsonrpc.org) is the wire specification; the LSP 3.17
  base protocol defines the Content-Length framing and MCP's stdio
  transport the newline framing. Before touching anything
  framing-related, re-check the current revisions of the protocols
  that ride this layer — the baselines move.
