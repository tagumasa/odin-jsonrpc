# odin-jsonrpc

A JSON-RPC 2.0 wire layer for Odin: both framings — LSP-style
`Content-Length` headers and newline-delimited JSON — the envelope with
ID normalization, the `Conn` that dispatches requests and correlates
responses, the bounded outbound writer, and the bounded request queue,
over any byte transport the host injects. Pure Odin on `core:*`, no
vendored C — the one foreign import is darwin's libSystem
`clock_gettime` (`src/platform/mono_darwin.odin`), where core ships no
monotonic wrapper.

The MCP SDK ([odin-mcp](https://github.com/tagumasa/odin-mcp)) builds
its two protocol faces on this wire layer; any JSON-RPC or LSP-style
host can take it directly.

The specification — layer rules, framing and the read path, the
connection object, memory and API discipline — is
[docs/design.md](docs/design.md).

## Layout

```
src/jsonrpc/   the wire layer: both framings, the envelope, the Conn
               (dispatch and correlation), the bounded outbound writer,
               the bounded request queue, the structural sanity scan
src/jsonutil/  vendored internal support (JSON value helpers)
src/platform/  vendored internal support (the cancel-token tree, the
               shared error vocabulary, the monotonic clock)
tests/         the component suites for the three packages
tools/
  bench/       the wire-path benchmark (just bench; the committed
               tables are docs/benchmarks.md)
docs/          design.md, benchmarks.md, and the documentation index
justfile       the gates: check, test, bench
.github/       pinned-toolchain CI (Linux, macOS, Windows)
AGENTS.md      rules and policy for coding agents
LICENCE.md     MIT
```

## The connection object

The host owns the wires: it reads and writes through function pointers
(injected as a `jsonrpc.Reader`/`Writer` pair), so real fds, in-memory
pipes, and test fakes all use the same path. The minimal host shape:

```odin
conn := new(jsonrpc.Conn)
reader: jsonrpc.Reader
jsonrpc.reader_init_ndjson(&reader, read_fn, nil, jsonrpc.DEFAULT_MAX_FRAME)
writer: jsonrpc.Writer
jsonrpc.writer_init_ndjson(&writer, write_fn, nil)
jsonrpc.conn_init(conn, reader, writer, context.allocator)
// The outbound writer is mandatory on any host whose peer might stop
// reading: replies then park in a bounded queue instead of the
// dispatch thread.
jsonrpc.conn_start_outbound(conn, jsonrpc.OUTBOUND_FRAMES_CAP, jsonrpc.OUTBOUND_BYTES_CAP)

jsonrpc.conn_register(conn, "initialize", host_initialize)
jsonrpc.conn_register_notification(conn, "notifications/initialized", host_initialized)

jsonrpc.conn_read_loop(conn) // dispatch on this thread; stream EOF ends it
```

Queue-based hosts run `read_frame` + `conn_handle_body` instead and
hand requests to their own workers through
`conn_start_request_queue` — a full queue answers `Request_Failed`
(-32803) immediately; the reader never blocks on handlers.

## The client-role exchange

`conn_call` sends a request, parks on a slot, and wakes on reply,
deadline, cancel token, or connection failure:

```odin
result, err_code, err_message, call_err :=
    jsonrpc.conn_call(conn, "tools/list", params, arena, deadline_ms, token)
```

Callers branch on `call_err` (the closed outcome vocabulary), never on
accompanying prose. A fired token abandons the slot locally and rides
the conn's `cancel_notify` hook to tell the peer — best effort by
contract.

## Cancellation

`conn_call` takes a `^platform.Cancel_Token` from this library's
platform package: tokens derive downward (`token_derive`), fire
propagates parent → child, the library's own waiters poll through
`token_check`, and a host that needs a blocking wait parks on the
token's one-shot event (`token_wait`). The ownership contract: a token
is destroyed only after every waiter has returned from its wait.

## Quick start

```sh
git clone https://github.com/tagumasa/odin-jsonrpc
cd odin-jsonrpc
just check         # odin check -vet -strict-style over every package
just test          # serial suite; verdicts from the log, zero leak lines
just bench         # wire-path wall numbers (best of 5)
```

`just check` and `just test` are self-contained: the Odin compiler and
`just` are the whole setup. The bench recipes are host tools:
`just bench` for wall numbers, `just bench-callgrind <scenario>` for
instruction counts (Linux, valgrind).

## Requirements

- The Odin compiler, nightly `dev-2026-10-nightly:84bc3fc` (the
  frozen `dev-2026-10` release; the gates are re-run and the measured
  tables re-baselined after toolchain moves)
- `just` (any recent version); on Windows the `sh`-scripted recipes
  (`test`, `bench-callgrind`) need Git Bash's `sh` on PATH — the CI
  runner image carries it
- valgrind, only for `just bench-callgrind`

## Licence

MIT — see [LICENCE.md](LICENCE.md).
