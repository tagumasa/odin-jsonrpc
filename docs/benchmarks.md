# Wire-path benchmarks

The inbound face end to end — framing, sanity scan, parse, dispatch —
plus framing alone under dribbled feeds, measured by `just bench`
(tools/bench). The tables are this tree on the tracked toolchain;
re-measure them on every toolchain move. Instruction counts compare
only between runs of the same scenario in the same host state.

## Environment and method

- Odin dev-2026-10-nightly:84bc3fc, `-o:speed`, single process.
- Measured 2026-10-08 on AMD Ryzen 7 5700U (16 threads); wall run at
  load average 1.4/2.4/2.2, 8.9 GB available of 15 GB.
- Wall: best of 5 repetitions, each rebuilding its (deterministic)
  payload in a fresh arena — `LCG(20261008)`; identical bytes every
  run.
- Instructions: callgrind (`--tool=callgrind`), the scenario run in
  its own process, `summary:` total of 5 repetitions plus payload
  build. A repeat of the ascii row reproduced within 5 instructions.
- Attribution: `callgrind_annotate` self costs summed per group; the
  `rest` column is the remainder to the row total. In the memset
  column callgrind counts roughly one instruction per zeroed byte:
  the per-call cost is the byte count plus ~40 (8,234 instructions
  for an 8,192-byte block, 65,573 for a 65,536-byte block).
- Inbound scenarios drive `read_frame` + `conn_handle_body` over an
  in-memory NDJSON feed with the reader served whole-remaining (the
  chunk-0 shape) — `conn_read_loop`'s exact shape minus the thread;
  framing scenarios drive `read_frame` alone over one 1 MB line fed
  in fixed chunks.

## Current numbers

| scenario            | feed                     | instructions  | wall    |
|---------------------|--------------------------|---------------|---------|
| inbound-ascii-4k    | 2000 × 4 kB ascii text   | 5,288,709,288 | 51.6 ms |
| inbound-cjk-4k      | 2000 × 4 kB CJK text     | 4,277,578,111 | 41.5 ms |
| inbound-escaped-4k  | 2000 × 4 kB escaped text | 7,305,698,160 | 90.9 ms |
| inbound-flood       | 20,000 × 8 B text        | 2,698,092,418 | 29.6 ms |
| framing-dribble-1m  | 1 MB line, 64 B chunks   | 83,092,968    | 0.39 ms |
| framing-chunk-1m    | 1 MB line, 512 B chunks  | 73,890,813    | 0.28 ms |

## Where the instructions go

Inbound rows, millions of instructions (share of the row total):

| row          | json parse¹ | libc memset | libc memmove | run_inbound² | bench build³ | rest  |
|--------------|-------------|-------------|--------------|--------------|--------------|-------|
| ascii 4 kB   | 4,026 (76%) | 252 (4.8%)  | 75 (1.4%)    | 507 (9.6%)   | 341 (6.4%)   | 88    |
| cjk 4 kB     | 3,152 (74%) | 252 (5.9%)  | 28 (0.7%)    | 717 (17%)    | 51 (1.2%)    | 78    |
| escaped 4 kB | 5,707 (78%) | 252 (3.4%)  | 134 (1.8%)   | 625 (8.6%)   | 492 (6.7%)   | 96    |
| flood 8 B    | 1,266 (47%) | 844 (31%)   | 33 (1.2%)    | 167 (6.2%)   | 51 (1.9%)    | 337   |

¹ every `encoding_json` and `utf8` function plus the map runtime it
  drives (`map_get`/`map_set`/`hasher`/`map_reserve_dynamic`).
² the bench loop holding the inlined sanity scan
  (`frame_sanity_ok`) and the per-frame arena cycle — the library's
  own inbound surface.
³ the payload builders (`build_notification_blob`, `append_*_text`)
  and the runtime append they drive (`runtime::_append_elems`).

- **Inbound is parse-bound.** The parse column is 74–78% of every
  4 kB row; the library's own column never exceeds 17%. Wall per
  4 kB frame: 25.8 / 20.7 / 45.5 µs (ascii / cjk / escaped).
- **The scan rides the text shape.** The `run_inbound` column on the
  same 4 kB byte budget: 507 M ascii, 717 M CJK (every three-byte
  sequence passes `json_utf8_sequence_len`), 625 M escaped (a
  backslash ends every run).
- **A small frame costs its arena block, zeroed.** The flood row's
  memset is 844 M, of which 823.5 M is one call site — `calloc`,
  100,009 calls (once per frame), 8,234 instructions a call: the
  per-message arena's 8 KiB first block
  (`MESSAGE_ARENA_BLOCK_SIZE`, src/jsonrpc/conn.odin), zeroed by the default
  allocator. Wall: 29.6 ms for 20,000 frames, 1.48 µs a frame. The
  4 kB rows carry 252 M — the same block callocs plus the body
  buffer's `make` zeroing (core zeroes `.Alloc` results;
  4.1 kB a frame).
- **Framing is one pass in every feed shape.** The two framing rows
  agree instruction-for-instruction on the build lines
  (`runtime::_append_elems` 23,438,150, `append_ascii_text` 17,833,290,
  memset 11,709,952 in both), so the 9.2 M difference is all feed
  machinery: `read_frame` 4.77 M vs 1.96 M, `fill`+`feed_read`
  5.70 M vs 0.71 M, over 15,626 fills at 64 B vs 1,954 at 512 B.

## Not tuned

- The scan's out-of-string path stays per-byte: JSON structural
  bytes sit one or two apart (`,`, `:`, digits), so runs are shorter
  than the run loop's entry cost — a degenerate run loop pays more
  than the per-byte switch it would replace.
- `read_frame` keeps `make` for the body buffer: the zeroing it
  performs is overwritten by the frame copy one line later (0.8% of
  the ascii row), and skipping it means a non-zeroed allocation with
  an error branch against the panic-on-OOM convention.
- `json_quote` stays unbatched: on the wire paths its inputs are
  method names and error messages (short); large text payloads ride
  `marshal_value_unsorted`, which is core:encoding/json's `unparse`.
  A cold path is not tuned.
- The core parser is the material win — 74–78% of every 4 kB row —
  but replacing core:encoding/json is new surface, not a tuning
  step; the wire path does not take it.
