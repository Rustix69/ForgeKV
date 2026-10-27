# ForgeKV Feature Completion Audit

Source: the original plan, recovered with `git show 1128417:PLAN.md` (Phases 0–8 and the scope section), checked against the source by inspection. Updated 2026-10-27 for commit `68f3993`, which closed the three gaps the first pass found.

## Summary
- **Total planned items:** 92 (Phases 0–8 plus the cross-cutting durability and locking items)
- **Implemented:** 92
- **Not implemented:** 0
- **Unclear:** 0

The per-phase test plans aren't scored. The exceptions are Phase 8 items where the deliverable *is* a test harness; those are marked as test code.

## Phase 0 — Scaffolding
- [x] Library plus binary layout, `src/main.rs` removed: `src/lib.rs`, `src/bin/forgekv-server.rs`.
- [x] Error type with every planned variant: `src/error.rs` `Error::{Io, Corruption, InvalidArgument, Poisoned, UnsupportedVersion, Closed, Locked}`, plus a test-only `Injected`.
- [x] `Options` with every planned field and a `validate()` method: `src/config.rs`. It also has `recovery_mode` and `manifest_max_bytes`.
- [x] `SyncMode::Always` (default) and `Never`: `src/config.rs`.
- [x] Check script: `scripts/check.sh` runs fmt, clippy `-D warnings` with and without features, and tests with and without failpoints.
- [x] `.gitignore` covers `/target` and `/bench-results/tmp`.
- [x] Branch renamed to `main`.

## Phase 1 — WAL, Memtable and Recovery
- [x] Checksummed record log, format v2 (16-byte header; frame `len | header_crc | payload_crc | payload`): `src/logfile.rs` `encode_header`, `encode_frame`, `LogWriter`, `LogReader`.
- [x] CRC-32/IEEE via `crc32fast`: `header_crc`, `frame_crc`.
- [x] Full-write append loop; a write or sync error poisons the writer, and fsync is never retried: `LogWriter::append` / `sync`.
- [x] WAL Put/Delete encoding and decoding: `src/wal.rs` `encode_put`, `encode_delete`, `decode`.
- [x] Acknowledged only after the WAL write, plus `sync_data` under `Always`: `Wal::append_put` / `append_delete` → `sync_if`; `Inner::write`.
- [x] Memtable: `BTreeMap<Vec<u8>, Entry>` with `approx_bytes`; `Entry::{Value, Tombstone}` (`src/memtable.rs`).
- [x] `Db::open`, `put`, `get`, `delete`, `close`: `src/engine.rs`.
- [x] Delete writes a tombstone even when the key is absent: `Db::delete` → `Memtable::delete`.
- [x] Key and value size limits enforced: `Db::check_key`, the value check in `Db::put`.
- [x] Recovery:
  - An incomplete final record of the newest WAL is discarded.
  - A complete record with a bad CRC is `Corruption`, and so is an incomplete record in an older, sealed WAL.
  - `replay_wal`, `ReplayEnd`.
  - Deviation: the record is discarded and the file retired, never truncated.
- [x] Opt-in `RecoveryMode::SalvagePrefix`: `src/config.rs`, `wal::replay`.
- [x] Directory `LOCK`: `fs::DirLock::acquire` (`try_lock`).
- [x] Engine poisoned after a WAL write or sync failure: `Inner::fail`, `check_writable` → `Error::Poisoned`.

## Phase 2 — SSTables
- [x] `SstBuilder::new`, `add`, `finish`, with strictly increasing keys enforced: `src/sstable/builder.rs`.
- [x] Empty table refused: `finish` returns `InvalidArgument("cannot finish an empty sstable")`.
- [x] The builder fsyncs the file: `sync_all` in `finish`.
- [x] Format v1:
  - Data blocks with a block CRC.
  - An index with its own CRC.
  - A 56-byte footer with a CRC, magic and version.
  - `src/sstable/mod.rs`.
- [x] `SstReader::open` (validates footer and index), `get` (`Some(Tombstone)` is distinct from `None`), `iter`, `verify`: `src/sstable/reader.rs`.
- [x] `TableMeta { size, smallest, largest, entries, blocks }`.
- [x] Bounded allocation caps `MAX_BLOCK_LEN` (16 MiB) and `MAX_INDEX_LEN` (1 GiB).
- [x] Bloom fields reserved and required to be zero: the builder writes 0 and the reader validates it.

## Phase 3 — Manifest, Flush and Multi-layer Reads
- [x] MANIFEST as a record log of `VersionEdit::{SetNextFileNumber, SetLogNumber, AddFile, DeleteFile}`: `src/manifest.rs`.
- [x] Replay with validation (incomplete final record ignored, bad CRC or inconsistent edit → `Corruption`): `manifest::replay`, `check_l1_disjoint`.
- [x] Snapshot rewrite (tmp file, fsync, rename, directory fsync) on every open and when the file exceeds `manifest_max_bytes`: `Manifest::create`, `needs_rewrite`, `Inner::maybe_rewrite_manifest`.
- [x] `Version { l0 newest-first, l1 sorted }`: `src/version.rs`.
- [x] Flush to L0:
  - The SST and directory are fsynced, then the MANIFEST commits.
  - The snapshot is installed, then the old WAL deleted.
  - `Inner::flush_imm` (runs in the background since Phase 5).
- [x] WAL rotation: `Inner::switch_memtable` syncs the old WAL and creates a new one.
- [x] Multi-layer reads, in order: memtable, immutable memtable, L0 newest-first, then the one covering L1 table (`Db::get`, `Version::get`).
- [x] Orphan cleanup on open (unreferenced `.sst`, WAL below `log_number`, `*.tmp`): `Db::open_inner`.
- [x] Recovery flushes the replayed WALs to L0 and creates the new WAL only after the MANIFEST commit: `open_inner`.
- [x] Explicit `Db::flush`.
- [x] A missing MANIFEST alongside data files is `Corruption`: `open_inner`.

## Phase 4 — Compaction
- [x] k-way merge, newest source wins: `src/merge.rs` `MergeIter` (`BinaryHeap`).
- [x] Picker takes all of L0 plus the overlapping L1 tables: `compaction::pick`.
- [x] Outputs split at `sst_target_bytes`: `compaction::run`.
- [x] Tombstones dropped at L1: `compaction::run` (`tombstones_dropped`).
- [x] Crash-safe replacement: outputs and directory fsynced, then one MANIFEST record that adds the outputs and deletes the inputs (`Inner::compact`).
- [x] Deferred unlink: `Table::mark_obsolete` plus `impl Drop for Table`.
- [x] Automatic trigger (`l0_compaction_trigger`), explicit `Db::compact() -> Option<CompactionStats>`, and compaction at open.
- [x] Invariant checker: `Version::check_invariants`, `Db::check_invariants`.

## Phase 5 — Concurrency and Backpressure
- [x] `Db` is `Send + Sync`: it is shared as `Arc<Db>` into `spawn_blocking` in `src/server.rs`.
- [x] Writer lock keeps WAL order equal to memtable order: `write: FairMutex<Writer>`.
- [x] Read snapshot `RwLock<Arc<Snapshot>>` holding the memtable, immutable memtable and version; readers only clone the `Arc` (`Inner::snapshot`, `install`).
- [x] Memtable has its own internal `RwLock`.
- [x] One background worker thread with a `Condvar`: `Inner::worker` / `next_job` (thread `forgekv-bg`).
- [x] Backpressure bounded at two memtables: `Inner::make_room`, `stall_until_flushed`.
- [x] Lock order documented in the `src/engine.rs` module docs.
- [x] `shutdown`/`close`: stop accepting writes, wake blocked writers, join the worker, sync the WAL.
- [x] Best-effort close on drop: `impl Drop for Db`.
- [x] Drop logs failures: `impl Drop for Db` reports a failed shutdown on stderr with a fallible `writeln!`, so it cannot panic, and stays silent if `close`/`shutdown` already returned the error (`tests/drop_log.rs`).
- [x] A worker failure or panic poisons writes and wakes waiters: `Inner::fail`, `WorkerPanicGuard`.

## Phase 6 — Protocol and TCP Server
- [x] Big-endian length framing; ops PING, GET, PUT, DELETE; statuses 0–5: `src/protocol.rs`.
- [x] Pure encode and decode, no I/O: `encode_request`, `decode_request`, `encode_response`, `decode_response`.
- [x] Frame length checked before allocation; the buffer grows only as bytes arrive and never past the frame length:
  - `check_frame_len`, `server::read_frame`.
  - `max_frame` is derived from the key and value limits instead of the planned fixed 2 MiB.
- [x] Malformed input handling (`server::connection`):
  - BAD_REQUEST keeps the connection open.
  - TOO_LARGE closes it.
  - EOF in the middle of a frame drops it.
- [x] In-order responses and pipelining (one request at a time per connection).
- [x] Storage calls run under `spawn_blocking`, bounded by the `max_inflight_ops` semaphore, with BUSY after `busy_timeout`: `server::execute`.
- [x] Connection limit: excess connections get BUSY and are closed (`serve`, `reject`).
- [x] Idle, request and write timeouts, configurable: `ServerConfig`.
- [x] Graceful shutdown (`serve`):
  - Stop accepting, signal connections through a `watch` channel.
  - Drain with a grace period, then abort what's left.
  - Shut the engine down with `Db::shutdown`.
- [x] Server binary (`src/bin/forgekv-server.rs`):
  - Hand-parsed flags.
  - SIGINT/SIGTERM handling.
  - A second signal exits immediately (`exit(130)`).
- [x] Async client: `src/client.rs` `Client::{connect, send, recv, call, ping, get, put, delete}`.
- [x] Engine errors mapped to statuses: `map_engine_error`.
- [x] Logging of dropped partial frames: `server::connection` logs the peer and the reason (never request contents); a mid-frame disconnect is counted in `ServerStats::partial_frames`, a mid-frame timeout in `timeouts`. Deviation: plain stderr output, no logging framework.

## Phase 7 — Benchmarks
- [x] Custom `forgekv-bench` binary: `src/bin/forgekv-bench/`.
- [x] Exact percentiles from per-operation `Instant` timings stored as nanoseconds in a `Vec<u64>`: `run.rs` `Phase::note`, `stats.rs` `percentile`.
- [x] Modes `engine` and `server` (plus `recovery` and `fsync`, beyond the plan).
- [x] Workloads `fillseq`, `fillrandom`, `readrandom`, `readwrite --read-pct`, `deleteheavy`: `workload.rs`.
- [x] Parameter flags (keys, sizes, threads or clients, warm-up, duration, sync, seed, dir): `main.rs` `parse`.
- [x] tmpfs warning: `env.rs` `ram_fs`.
- [x] Procedure: fresh directory, untimed preload, unrecorded warm-up, timed runs (5 by default), seeded generator.
- [x] JSON output:
  - Contents: the full environment (git head and source fingerprint, rustc, build profile, CPU, RAM, OS, filesystem), all parameters, ops/s, p50/p95/p99/max, errors, sync mode, SST counts and directory size, plus a human-readable table.
  - Deviation: one file per configuration rather than per run.
- [x] Median, min and max across runs: `load_summary_json`.

## Phase 8 — Hardening, Crash Testing and Documentation
- [x] Failpoint mechanism and matrix:
  - 12 named points covering open, write, WAL rotation, flush, compaction, MANIFEST rewrite and reads.
  - Fail, pause and panic actions; a separate registry per `Db`.
  - `src/failpoint.rs` `ALL`, plus the `fail_point!` call sites.
- [x] `kill -9` loop: a child writer acknowledges over a pipe, gets a random SIGKILL, and is checked against a model (`tests/kill.rs`, test code).
- [x] Simulated loss of unsynced WAL data: `tests/recovery.rs` (test code, feature `failpoints`).
- [x] Engine exposes its synced offset under the `failpoints` feature: `Db::wal_offsets() -> WalOffsets { wal_number, written, synced }`, backed by `LogWriter::synced_end`, which advances only on a successful sync.
- [x] A zero- or garbage-filled tail is `Corruption`: enforced by the strict `LogReader`.
- [x] Corruption sweep over all file types:
  - `tests/corruption.rs` (test code).
  - Backed by reader validation, including the reserved header bytes checked in `LogReader::open`.
- [x] `#![forbid(unsafe_code)]`; no `unwrap`/`expect` outside test modules in library or binary code (by inspection).
- [x] Clippy pedantic spot-check: run manually on the follow-up changes, with the same warning count (274) as before them, so they added none. The pedantic lints are not enforced by `scripts/check.sh`.
- [x] Docs: README (architecture, formats, durability, limitations, usage, benchmarks), `docs/FORMATS.md`, `docs/BENCHMARKS.md`.

## Cross-cutting: Durability Modes and Locking
- [x] `Always` and `Never` semantics in the write path; under `Never` the WAL is still synced when a memtable is sealed and at shutdown (`Inner::write`, `switch_memtable`, `Db::shutdown`).
- [x] Directory lock released explicitly on drop with `flock(LOCK_UN)`: `fs::DirLock` `Drop` (post-Phase 8 fix).
- [x] FIFO writer lock: `src/fairlock.rs` `FairMutex` (added in Phase 8).

## Optional / Out-of-Scope Features
**Optional extensions from the plan:**
- [ ] **Bloom filters** — footer fields reserved and always zero.
- [ ] **Range scans** — no `Db::scan`; `SstReader::iter` and `MergeIter` exist only internally.
- [ ] **Group commit** — each `Always` write pays its own fsync.
- [ ] **Atomic write batches** — none.
- [ ] **Block cache** — reads rely on the OS page cache.
- [ ] **Prefix or block compression** — none.
- [ ] **Multi-level leveled compaction** — two levels only.
- [ ] **Sequence numbers and user-visible snapshots** — internal read snapshots only; no MVCC API.
- [ ] **`loom` / `miri` / `cargo-fuzz`** — no dependency or fuzz directory.
- [ ] **Open-loop benchmark mode** — none.
- [ ] **Zipfian key distribution** (`--key-dist`) — none.
- [ ] **`dm-flakey` power-loss testing** — none.
- [ ] **Metrics endpoint** — only in-process `Db::stats()` and `ServerStats`.

**Explicitly out of scope, all intentionally absent:**
- [ ] Replication
- [ ] Consensus
- [ ] SQL or any query language
- [ ] Transactions
- [ ] TTL
- [ ] Encryption
- [ ] Windows support

**Implemented beyond the plan, not counted above:**
- Stall counters in `Stats`.
- The worker panic guard.
- Validation of the reserved log-header bytes.
- The `tests/data/golden-db/` format fixture.
- The `recovery` and `fsync` benchmark modes.
- CSV export and chart scripts.

## Final Assessment
By source inspection, the core plan for Phases 0–8 is implemented. The three small gaps found in the first pass (none affecting data integrity) were closed in commit `68f3993`:
1. **`Drop` logs shutdown failures** (Phase 5); `close()` remains the way to handle them.
2. **Dropped partial frames are logged and counted** (Phase 6).
3. **The engine exposes its synced WAL offset for tests** (Phase 8), and the unsynced-loss test uses it.

The clippy pedantic spot-check was run manually; the lints are not enforced in the repository.

The optional extensions are all unimplemented, and the out-of-scope features are absent by design. This is an educational engine, not production software; power-loss durability in particular remains untested.
