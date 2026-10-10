# odin-jsonrpc design

The specification the implementation governs against: the layer
rules, the wire paths, the connection object, and the memory,
threading, and API discipline every piece follows. The code is the
arbiter — where this document and the code disagree, one of them is
defective.

## Layers

```
src/jsonutil   JSON value helpers (vendored internal support)
src/platform   the cancel-token tree, the shared error vocabulary,
               the monotonic clock (vendored internal support)
src/jsonrpc    the wire layer: both framings, the envelope, the Conn
               (dispatch and correlation), the outbound writer, the
               bounded request queue
```

`src/jsonutil` and `src/platform` are internal surface, not the
library contract, with one exception shaped like a contract: the
cancel-token tree — `conn_call`'s token parameter carries the
`platform.Cancel_Token` type, so hosts construct and fire this
library's tokens. Everything is pure Odin on `core:*` with one
foreign-import exception — darwin's libSystem `clock_gettime`
(`src/platform/mono_darwin.odin`), because core ships no monotonic
wrapper there.

The split of ownership is a rule, not a convenience:

- **The library owns the wire**: framing, envelope decoding, dispatch
  and correlation, the wire half of cancellation.
- **The host owns the process**: transport injection (both directions
  read and write through function pointers, never fds), thread policy
  above the library's own threads, process spawning, reconnect
  backoff, registries, permission, and logging (there is no
  module-global logger; the library's own surfaces report through
  their return values).

## Framing and the read path

Two framings share one `Reader`/`Writer` pair of function-pointer
abstractions, so real fds, in-memory pipes, and test fakes all use the
same path:

- **Header framing** (`Content-Length`, LSP-style; the header block
  strictly `\r\n`-terminated) — the LSP base protocol and internal
  RPCs.
- **Newline-delimited JSON** (`reader_init_ndjson`) — MCP's stdio
  transport; one message per line, a trailing CR tolerated, embedded
  newlines are the sender's violation and end the frame early.

Bounds and their reasons:

| constant | value | governs |
|---|---|---|
| `DEFAULT_MAX_FRAME` | 64 MiB | the connection-class body cap; hosts pick their own |
| `HEADER_MAX_BYTES` | 8 KiB | the header block itself — a stream that never produces the separator must not grow the receive buffer without bound |
| `READ_CHUNK_BYTES` | 512 | bytes per fill — a full pipe write crosses in one call, a dribbling source does not over-reserve |

The newline scan resumes where the previous fill stopped
(`nl_scan_from`): a dribbling source costs one scan per byte, not
one per byte per fill. Header parsing is allocation-free
(case-insensitive compare, digit-only decimal with the accumulator
bounded before it can overflow, duplicate `Content-Length` and any
leading sign rejected as framing violations).

Read failures speak one closed vocabulary (`Read_Err`:
`Eof`/`Closed`/`Framing`/`Too_Large`/`Io`). A `read_fn` reporting
zero bytes with no error folds into `.Io` — a progress-less success
would spin every read loop forever, so both the fill path and the
write-all path guard it.

### The sanity scan

`decode_envelope` runs `frame_sanity_ok` on every frame body before
the core parser, because the core parser (a) recurses per nesting
level with no depth cap — a deep body is a stack-exhaustion crash,
not a parse error — (b) silently normalizes invalid UTF-8 inside
strings to U+FFFD, and (c) truncates values at raw control
characters. The scan is a single allocation-free pass that rejects
nesting beyond `MAX_JSON_DEPTH` (128), negative or overflowing
bracket depth, raw control characters inside strings, and invalid
UTF-8 sequences (overlong forms, surrogates, beyond U+10FFFF,
truncation at the body's end). Everything it rejects is a body the
parser would have crashed on or mangled; the reply is
`-32700 Parse_Error`.

### The per-message arena

`conn_read_loop` gives each message its own `Dynamic_Arena`,
destroyed after dispatch (`destroy`, not `free_all` — the tracking
allocation would leak per message otherwise). The arenas take
8 KiB blocks (`MESSAGE_ARENA_BLOCK_SIZE`): the default allocator
serves a block allocation through calloc, so a block of the arena's
default size (`mem.DYNAMIC_ARENA_BLOCK_SIZE_DEFAULT`) costs that
whole block in libc zeroing per message — on small-frame floods
that zeroing dominated the whole inbound path (measured in
[benchmarks.md](benchmarks.md)). The size must stay above the
out-of-band threshold (`mem.DYNAMIC_ARENA_OUT_OF_BAND_SIZE_DEFAULT`):
a bump request larger than the block is `Invalid_Argument`, so
`block_size > out_band_size` keeps every larger allocation
out-of-band instead. The same 8 KiB blocks serve the request-queue
entries.

The frame loop resets the thread's temp allocator per frame: the
library's own send path is temp-free, but host handlers scratch on
it, and the loop owns the thread.

## The envelope

`Envelope` normalizes exactly once, at decode:

- **`Id`** keeps the JSON type it arrived with — integers become
  `i64`, strings stay strings. JSON-RPC requires the reply to echo
  the same value; a numeric-looking string id must not collapse
  into a number, and every internal consumer (reply matching, the
  cancelled-notification's `requestId` lookup) keys off this value.
- **`Err_Code`** is the closed wire-error table: the JSON-RPC five
  (`-32700`…`-32603`), `Server_Retryable` (`-32000`, the
  implementation-defined band — a transient failure a read-only
  caller may reapply; callers fold it into their own failure
  prose), `Request_Cancelled` (`-32800`), `Request_Failed`
  (`-32803`). Unknown codes decode to `Internal_Error`.
- Batch envelopes (a top-level array) are rejected — JSON-RPC
  batching is absent from the current protocol revisions that ride
  this layer.
- `"jsonrpc":"2.0"` is demanded on every message; every face of
  this codebase emits it, so the demand only rejects foreign
  non-JSON-RPC input early.

One wire rule both request builders enforce: **the `params` member
is omitted when it is nil** — a literal `"params":null` is not an
absent params, and strict peers reject it outright.

## The Conn

`Conn` is the connection object shared by servers (a dispatch
table: `handlers` for requests, `notifiers` for notifications) and
clients (a correlation table: `pending`). The Conn owns no threads;
hosts run their own loops — `conn_read_loop` on any host thread, or
`read_frame` + `conn_handle_body` for queue-based hosts.

Dispatch routes requests to handlers (unknown method →
`-32601 Method_Not_Found`), notifications to notifiers (unknown
ones are ignored — notifications carry no reply), and responses
into the pending table. Handler replies answer immediately
(`Action.Respond`) or defer (`Action.Defer`) — the deferred path
lets a host answer from its own worker without the library owning
threads.

### Sending

Two send paths, one entry point (`conn_send_body`):

- **The outbound writer** (`conn_start_outbound`): every frame
  crosses the wire from one dedicated thread fed by a bounded queue
  (`OUTBOUND_FRAMES_CAP` 64 frames, `OUTBOUND_BYTES_CAP` 8 MiB). A
  peer that stops reading then blocks exactly that thread instead
  of every sender: posts fail once the queue is full past their
  deadline (replies wait `OUTBOUND_REPLY_TIMEOUT_MS` 500 ms;
  notifications default to never — the next change retries), and no
  caller-side lock ever queues behind a pipe write. One oversize
  frame is admitted into an empty queue so a large payload still
  flows under a small budget. A failed write marks the connection
  broken and releases every pending waiter. Teardown premise: the
  owner kills the peer process first, which turns a wedged write
  into EPIPE; `conn_destroy` joins the thread.
- **Synchronous writes** under `write_mu` — the writer-less form
  for in-process fakes and trusted peers. `conn_close` takes
  `write_mu` before `closed_mu` so a close can never interleave
  between a sender's closed-check and its write.

Serialized bodies are built on the caller's request/temp-scoped
allocator, never on a long-lived one: the outbound queue clones
what it queues and the synchronous writer only borrows the bytes,
so nothing downstream frees the original — a long-lived allocator
would collect one body per send.

### The bounded request queue

`conn_start_request_queue` switches incoming-request dispatch from
inline (on the reader thread) to a bounded chan drained by one
worker — no thread is ever created per request. A full queue is
answered immediately with `Request_Failed` (-32803): the reader
never blocks on handlers, and stopping it would also stall every
pending response on the same loop — the rejection reply is the
backpressure the protocol carries. Entries are cloned into
queue-owned arenas (`envelope_clone`: strings, string ids, params,
results — everything that must outlive the reader's per-message
arena).

### The pending table and conn_call

`conn_call` is the client-role exchange: send, park on a slot,
wake on reply / deadline / cancel token / connection failure. The
table is copy-in: the slot deep-clones what it needs into the
connection allocator, so the reader's message arena can be freed
while a waiter still reads the slot; the waiter clones the result
out into its own arena. Numeric ids are the only match keys — the
client numbers every request it sends itself, so a peer echoing a
string id cannot correspond to any slot.

`Call_Err` is the closed outcome vocabulary
(`Timeout`/`Cancelled`/`Closed`/`Transport`/`Error_Response`);
`err_code`/`err_message` are authoritative only for
`.Error_Response` — on every other outcome they are a best-effort
hint, and callers branch on `call_err`, never on the code. A nil
conn is the no-connection spelling and returns a typed `.Closed`
refusal, never a deref.

When the stream dies (read error, framing rejection, close), the
reader releases every waiter at once (`.Closed`) and latches the
table so new calls fail fast — a table no reader serves can never
complete, and each waiter burning its own deadline buys nothing.

### Cancellation

A caller's `platform.Cancel_Token` fires → the waiter abandons its
slot → the conn's `cancel_notify` hook sends the protocol's
cancelled notification with the request id. Best effort by
contract: the local abandon happens whether or not the peer
answers. The token tree lives in `src/platform`: tokens derive
downward, `fire` propagates parent → child under a strictly
parent-to-child lock order (a child's mutex is never held while
taking the parent's, so the recursion cannot deadlock), a token
derived from an already-fired parent is born fired with its event
already signalled, and the ownership contract is explicit —
children are destroyed before their parent, and a token is
destroyed only after every waiter has returned from its wait.

Two wait forms share the tree, and neither holds the mutex while
waiting: the library's own waiters (the `conn_call` slot) poll
through `token_check`'s timed slice (`CANCEL_POLL_SLICE_MS`), and
hosts that need a blocking wait park on the token's one-shot event
through `token_wait` — the event is signalled under the mutex at
fire, so a waiter either observes the latch or wakes from the
signal; it never polls the raw fields (a raw read races a
concurrent destroy; the accessors' mutex does not).

## Memory and lifetime rules

- **Arena per message**, request-scoped arenas for callers, and
  nothing borrowed across a lifetime boundary: whatever must
  outlive a source arena is cloned (`envelope_clone`, the pending
  slot's copy-in, `clone_value`), including map keys — Odin maps
  store key headers, so key bytes are the inserter's ownership.
- **Bounded by construction**: frame caps, the header cap, the
  outbound queue's frames-and-bytes caps, the request queue's
  channel capacity, the error-chain render depth (8). Anything a
  peer can grow without bound is a defect.
- **Monotonic clocks only**: every deadline, TTL, and settle bound
  reads `platform.mono_ms`. The package's one module-level variable
  (`mono_override`) is a reserved test seam — no suite wires it.
- **No process globals**: per-connection state lives in the `Conn`
  the host owns.

## API discipline

Naming and vocabulary: multiword types `Word_Word`, procs
snake_case, mutex fields `mu`, allocator fields `allocator` (short
`a` for procedure parameters).
Failure vocabularies are closed per boundary — `_Err` for boundary
enums/unions (`Read_Err`, `Call_Err`, `Err_Code`), `_Error` for
circumstance-explaining structs — and boundaries convert into the
next vocabulary explicitly at their edge; callers branch on the
member, never on accompanying prose. The structural rules (bounded
queues, cloned keys, the cancel-token tree, monotonic clocks, no
process globals, per-boundary error propagation with explicit
checks) govern throughout.

## Testing and measurement

- `tests/` — the component suites: framing and envelope defects,
  the sanity scan, the outbound writer (including the wedged-write
  shape on a scripted fake), and the cancel tree with its error
  vocabulary. `just test` runs them single-threaded under a
  zero-leak log gate; verdicts come from the log, never the exit
  status alone.
- `tools/bench` — the wire-path benchmark; the committed numbers
  and their attribution live in [benchmarks.md](benchmarks.md).
- The toolchain is pinned (dev-2026-10-nightly:84bc3fc, the frozen
  dev-2026-10 release); CI asserts the pin hard and re-baselines
  the measured tables on every deliberate move.

## Specification

- JSON-RPC 2.0 is the wire specification (jsonrpc.org).
- The LSP 3.17 base protocol defines the `Content-Length` header
  framing; MCP's stdio transport defines the newline framing. Before
  touching anything framing-related, re-check the current revisions
  of the protocols that ride this layer — the baselines move.
