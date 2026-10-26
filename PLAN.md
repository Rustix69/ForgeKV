# ForgeKV — Engineering Implementation Plan

Status: **APPROVED WITH CHANGES (revision 2). No application code has been written yet.**

### Revision 3 (post-Phase 1): record format v2
WAL/manifest frame header gains a `header_crc` over `len` (format version bumped 1 → 2). See §3.1 and §3.3. Phase 1 code and tests were updated accordingly; the "corrupted length looks like torn tail" limitation is removed.

### Revision 2 changelog (decisions approved by the project owner)
1. Two-level L0/L1 compaction stays. 2. No sequence numbers/snapshots in v1. 3. `crc32fast`; every format and document says **CRC-32/IEEE** (never CRC-32C). 4. `SyncMode::Always` stays the default. 5. `src/main.rs` is replaced by lib + `src/bin/forgekv-server.rs`. 6. `master` is renamed to `main` before the first commit. 7. Mid-log WAL corruption is a hard error.
8. **WAL recovery correction**: only an *incomplete* final frame is a torn tail and may be truncated. A *complete* frame with a bad checksum is `Error::Corruption` **even if it is the last record**; it is never silently discarded by default (§3.3).
9. **No reliance on single-`write` atomicity**: the writer handles short writes and mid-append failures; the reader handles short reads and incomplete frames (§2 rule 1, §3.3).
Contradictions with these decisions elsewhere in this document were searched for and resolved (§2, §3, §4, Phases 0/1/3/8, §8, §9, §11, §12).

## 0. Current repository state

| Item | State |
|---|---|
| Git | Branch `main`, **no commits yet**; the first commit is being prepared. |
| Crate | `forgekv` 0.1.0, edition 2024: library (`src/lib.rs`) plus binaries `forgekv-server` and `forgekv-bench`. `#![forbid(unsafe_code)]`. |
| Dependencies | `crc32fast`, `tokio` (features `rt-multi-thread, net, io-util, time, sync, signal, macros`); dev-only `tempfile`, `rustix` (`process`, for a test). Feature `failpoints` (test-only fault injection). |
| Status | Phases 0–8 implemented, plus the post-Phase 8 directory-lock fix (§5). `scripts/check.sh` passes with and without `failpoints`. |
| Docs | `README.md`, `docs/FORMATS.md`, `docs/BENCHMARKS.md`, this plan, `CLAUDE.md` (project rules). |
| Benchmarks | Two recorded sessions in `bench-results/` (Phase 7 and Phase 8 source, identified by source fingerprint); see `docs/BENCHMARKS.md`. |
| License | None selected yet. |
| Toolchain | rustc/cargo 1.98.1. |
| Dev machine | x86_64, i5-12500H, 16 threads, ~14 GiB RAM, ext4 on NVMe, Linux 7.0 (relevant for benchmark reporting). |

History: planning started from a `cargo init` binary crate (branch `master`, zero dependencies, hello-world `src/main.rs`); Phase 0 replaced it with the library + binary layout and renamed the branch to `main`.

---

## 1. Architecture and data flow

### 1.1 Layout (single crate, library + binaries)

```
src/
  lib.rs            public API re-exports; #![forbid(unsafe_code)]
  error.rs          Error enum (hand-written, no thiserror), Result alias
  config.rs         Options, SyncMode, RecoveryMode (+ validation)
  codec.rs          bounds-checked little-endian reader shared by the parsers
  logfile.rs        checksummed record log: LogWriter / LogReader (WAL and MANIFEST)
  wal.rs            WAL record encode/decode on top of logfile, replay
  memtable.rs       BTreeMap memtable with tombstones + size accounting
  sstable/
    mod.rs          format constants, footer, format spec
    builder.rs      SstBuilder (sorted writes -> file)
    reader.rs       SstReader (open, point get, iterator, verify)
  manifest.rs       VersionEdit, Manifest log, replay into ManifestState
  version.rs        Version (immutable L0/L1 table set), reference-counted Table
  merge.rs          k-way merge iterator (newest source wins)
  compaction.rs     picker + executor (L0 + overlapping L1 -> L1)
  engine.rs         Db: open/recovery, write and read paths, background worker
  fairlock.rs       FIFO writer lock                                       [Phase 8]
  fs.rs             file naming, sync_dir, DirLock (directory lock)        [post-Phase 8]
  failpoint.rs      test-only fault injection (feature `failpoints`)
  test_io.rs        I/O fault-injection helpers for unit tests (cfg(test))
  protocol.rs       frame + request/response encode/decode (pure, no I/O)  [Phase 6]
  server.rs         Tokio TCP server                                       [Phase 6]
  client.rs         minimal async client (tests + bench)                   [Phase 6]
  bin/forgekv-server.rs
  bin/forgekv-bench/  main, run, stats, workload, env, json               [Phase 7]
tests/
  common/mod.rs     independent SSTable encoder for crafted files
  engine.rs lsm.rs recovery.rs sstable.rs sstable_alloc.rs
  crash.rs          failpoint crash matrix (feature `failpoints`)
  concurrency.rs server.rs
  kill.rs corruption.rs golden.rs                                          [Phase 8]
  lock.rs                                                                  [post-Phase 8]
  data/golden-db/   on-disk format fixture
docs/               FORMATS.md, BENCHMARKS.md
scripts/            check.sh, bench.sh
bench-results/      recorded benchmark sessions (tmp/ is git-ignored)
```


### 1.2 Write path

```
put(k,v) ──► validate sizes ──► [write mutex]
               1. encode WAL frame into one buffer; append with a write-all loop
                  (short writes handled; NOT assumed atomic; failure => engine poisoned)
               2. if SyncMode::Always: sync_data() the WAL      <── ack boundary
               3. insert into active memtable
             [release] ──► ack
             if memtable >= threshold: freeze -> imm, rotate WAL, schedule flush
```

Order matters: **WAL first, memtable second, ack last.** A reader can never observe a value that is not in the WAL.

### 1.3 Read path

```
get(k): active memtable -> immutable memtable (if any) -> L0 files (newest→oldest)
        -> L1 files (binary search the one file whose range contains k)
First hit wins. A tombstone hit means "not found" and STOPS the search.
```

### 1.4 Flush and compaction

```
Flush:  imm memtable ─► NNNNNN.sst (write, fsync) ─► fsync dir
        ─► manifest edit {AddFile L0, SetLogNumber} (append, fsync)
        ─► delete obsolete WAL

Compaction (L0→L1): merge all L0 files + overlapping L1 files ─► new L1 SSTs (fsync, fsync dir)
        ─► ONE manifest edit {AddFile*, DeleteFile*} (atomic unit, fsync)
        ─► unlink old SSTs (after last reader drops them)
```

### 1.5 On-disk directory

```
<db>/
  LOCK                  exclusive advisory lock (prevents two processes opening the same dir)
  MANIFEST              append-only log of VersionEdits (rewritten as snapshot via tmp+rename when large)
  MANIFEST.tmp          transient
  wal-000007.log        WAL numbered by file number
  000012.sst            SSTables numbered by file number
  *.tmp                 transient; deleted on open
```

All files share one monotonically increasing `next_file_number`, persisted in the manifest.

---

## 2. Durability guarantees and limitations (explicit)

Ordinary `write(2)` is **not** durable. It only copies into the OS page cache. We define three levels:

| Failure | `SyncMode::Always` (default) | `SyncMode::Never` |
|---|---|---|
| Process crash / `kill -9` of ForgeKV after ack | **No acknowledged write lost.** (Data is in the page cache; kernel survives.) | Same: no loss, provided we do not buffer in user space before ack. |
| OS crash / power loss after ack | **No acknowledged write lost, assuming** the drive honors flush/FUA and the filesystem honors `fdatasync` + `fsync` on the directory. | **Recent writes may be lost.** Database must still open to a *consistent prefix*. |
| Incomplete final WAL frame (file ends inside header or payload) | Detected by EOF-before-declared-length; discarded on recovery (everything before it is replayed); never acked (ack follows sync). | Same. |
| Complete frame with bad CRC, **anywhere including the last record** (bit rot, or a power-loss artifact such as a zero/garbage-filled extended tail) | **`Error::Corruption`; open refuses.** Not silently dropped. Operator may opt in to `RecoveryMode::SalvagePrefix`. | Same. |
| Bit rot / media corruption in SSTs, manifest, or WAL | **Detected** (CRC-32/IEEE) and surfaced as `Error::Corruption`. Never silently skipped. **Not repaired.** | Same. |

Rules we will implement:
1. No `BufWriter` between the WAL and the ack. The frame is assembled in one buffer and written with a loop (`Write::write_all` semantics: retries `Interrupted`, continues after short writes), then `sync_data`. **We never assume a record reaches the file in one OS call or atomically.** Consequences: (a) a crash or error mid-append can leave an incomplete frame, which recovery handles (§3.3); (b) if an append returns an error after possibly writing a partial frame, the writer does **not** continue appending after it (that would bury garbage mid-log): the engine is poisoned and must be reopened, where recovery truncates the incomplete frame; (c) `LogWriter`/`LogReader` are generic over `Write`/`Read` so tests can inject 1-byte writes, short reads, and mid-frame errors. The writer tracks `good_end` (offset after the last fully written frame).
2. Every file **creation, rename, or deletion that matters** is followed by `fsync` of the parent directory (`fs::sync_dir`).
3. New WAL/SST/manifest files: `fsync` the file, then `fsync` the directory, **then** reference them from the manifest. File length growth on WAL appends uses `sync_data`, which on ext4 does flush the size change needed to read the data back.
4. **fsync failure is fatal.** After a failed `sync_data`/`sync_all` the page-cache state is unknowable (the "fsyncgate" problem). The engine enters a *poisoned* state: every later write returns `Error::Poisoned`; the operator must reopen (which re-runs recovery from disk). We do not retry fsync.
5. Atomic replacement = write temp → fsync temp → `rename` → fsync dir.

Limitations we state in the README rather than hide:
- **Known consequence of the strict WAL policy**: after a power loss, some filesystems can expose a file whose length was extended but whose tail contains zeros or stale bytes. That produces a *complete-looking* frame with a bad CRC, which we report as `Corruption` rather than guess. With `Always` such a frame was never acknowledged, but the database will still refuse to open until the operator chooses `RecoveryMode::SalvagePrefix` (truncate at the first invalid record, report bytes discarded, log loudly). This is deliberate: we prefer a loud failure over silently discarding what might be acknowledged data.
- We cannot detect drives that lie about flushes, and no test here proves power-loss durability on real hardware. Our evidence is: (a) `kill -9` tests (real), (b) failpoint/truncation tests that *simulate* unsynced-tail loss, (c) code review of sync ordering. **We will word claims as "designed for power-loss durability; process-crash durability verified"** unless a stronger test (e.g., dm-log-writes/`dm-flakey`, optional) is run.
- Single process, single node, Unix only (`FileExt::read_at`, `flock`). No snapshots, transactions, or replication.
- `SyncMode::Never` is for benchmarking/ephemeral use only.

---

## 3. Formats (specified up front, versioned)

All integers little-endian. Every checksum is **CRC-32/IEEE** (reflected polynomial `0xEDB88320`, init `0xFFFFFFFF`, xorout `0xFFFFFFFF`; the zlib/Ethernet CRC), computed with `crc32fast::Hasher`. Known-answer test: CRC-32/IEEE of ASCII `"123456789"` = `0xCBF43926`. (We do not use CRC-32C/Castagnoli anywhere.) Limits (configurable defaults): key ≤ 4 KiB, value ≤ 1 MiB, empty key rejected.

### 3.1 Record log framing (`logfile.rs`; used by WAL and MANIFEST)

```
file header (16 B): magic[8] = "FKVLOG\0\1" | kind u8 (1=WAL,2=MANIFEST) | version u8 = 2 | reserved[6] = 0 (checked: non-zero is Corruption)
frame header (12 B): len u32 | header_crc u32 | payload_crc u32
record:              frame header | payload[len]
         header_crc  (CRC-32/IEEE) covers: the 4 bytes of len
         payload_crc (CRC-32/IEEE) covers: len (4 bytes LE) || payload
         len must be 1..=max_record_size (every payload has at least a type byte)
```
**Format version 2 (revision 3).** Version 1 (`crc32 | len | payload`, never released) had no header checksum: a `len` corrupted upward past EOF was indistinguishable from a torn write and was silently truncated. Version 2 adds `header_crc` so `len` is verified *before* it is trusted. Version-1 files are rejected with `UnsupportedVersion` (no migration: v1 never shipped). Golden bytes for `payload = [1,2,3]`: `len=03 00 00 00`, `header_crc=f2 70 f1 33`, `payload_crc=ec 9c 79 06`, payload `01 02 03`.
Header is mandatory: a log shorter than 16 bytes at open is treated as "creation was interrupted" → recreate (only legal for the newest WAL); wrong magic/version → `Error::Corruption`/`UnsupportedVersion`.

### 3.2 WAL payload

```
type u8 (1=Put, 2=Delete) | key_len u32 | key | [val_len u32 | value]   (value fields only for Put)
```
Reserve type 3 = Batch for a possible atomic-batch extension. Unknown type with valid CRC → `Corruption` (never skip).

### 3.3 Recovery policy for a record log (WAL and manifest; deterministic)

Reading uses a loop that tolerates short reads (`read_exact`-style; `Interrupted` retried). "EOF" means a read returned 0 bytes. Read frames sequentially from offset 16; `good_end` = end of the last valid frame.

| Situation at frame start `p` | Classification | Action |
|---|---|---|
| `p == file_len` | clean end | done |
| EOF after 1..7 bytes of the frame header (`len`/`header_crc` not yet both present, so nothing is verifiable) | **incomplete frame (torn tail)** | discard from `good_end` (see below) |
| 8..11 header bytes present (`len` + `header_crc`), `header_crc` valid, EOF before the rest of the header | **incomplete frame (torn tail)** | discard from `good_end` |
| `len` + `header_crc` present but `header_crc` does **not** match `crc32(len)` (checked first, regardless of what follows, including a short file) | **Corruption** (a torn write leaves only a *prefix of the correct bytes*, so wrong bytes are damage) | `Error::Corruption { file, offset }`; file not modified |
| Full header, `header_crc` valid, `1 <= len <= max_record_size`, EOF before `len` payload bytes arrive | **incomplete frame (torn tail)** (length is now trusted) | discard from `good_end` |
| Header with valid `header_crc` but `len == 0` or `len > max_record_size` | **Corruption** (a writer never emits it; no allocation based on `len`) | `Error::Corruption { file, offset }`; no allocation based on `len` |
| Full header and full payload, `payload_crc` mismatch | **Corruption — including when this is the final record in the file** | `Error::Corruption { file, offset }`; open fails; file is **not modified** |
| Full frame, CRC ok, unknown record type or undecodable payload | **Corruption** | error, never skip |

- **How "discard" is implemented (since Phase 3):** no file is truncated. Recovery replays the records before `good_end`, writes them to a new L0 table, commits a new MANIFEST naming a new WAL, and only then deletes the replayed WALs; the incomplete frame is never read again. This implements decision 7 ("may be truncated") without modifying the damaged file. For the MANIFEST, the fresh snapshot written on every open drops the incomplete record.
- Only an *incomplete* frame at the end of the **newest** log may be discarded. An incomplete frame in any older (sealed) WAL is `Corruption`, because a sealed WAL was fully written and synced before its successor was created.
- A complete frame with a bad checksum is never discarded by default. `RecoveryMode::SalvagePrefix` (opt-in, off by default) truncates at the first invalid frame of any kind, returns a report (file, offset, bytes dropped), and logs loudly.
- Recovery never modifies a log file. `Db::open` does all reading and classification first (manifest, every table, every live WAL), so a `Corruption` error is raised before any file in the directory is changed; only after that does it delete orphans and write new files. A replayed WAL is never appended to again (writes go to a new WAL).
- **Resolved in format v2 (revision 3)**: the Phase 1 implementation exposed that, with a single checksum, a corrupted `len` enlarged past EOF looked like a torn write. `header_crc` closes that gap: every bit flip in `len` or `header_crc` of any frame, including the last, is `Corruption`. **Remaining, unavoidable ambiguity:** if fewer than 8 bytes of a final frame header are present (nothing verifiable), the stub is treated as torn and truncated; likewise 1–7 zero bytes appended to a log. 8 or more zero bytes are `Corruption` (`crc32(0) != 0`).
- Trade-off, stated honestly: the strict rule can make an unacknowledged, damaged final record block startup (see the §2 limitation). We accept that to avoid silently discarding data that may have been acknowledged.
- The WAL is only opened for append after recovery succeeded.
- **Writer side (no atomic-write assumption)**: frames are written with a write-all loop; any append error poisons the writer (§2 rule 1). A mid-append failure therefore never leaves garbage followed by more frames.

### 3.4 SSTable (as implemented in Phase 2; format version 1)

All integers little-endian; every checksum is CRC-32/IEEE. The authoritative spec is the module doc of `src/sstable/mod.rs`; golden bytes are in `tests/sstable.rs`.

```
[data block 0] ... [data block n-1] [index block] [footer]

data block:   entry* | block_crc u32            (block_crc over the entries)
entry:        kind u8 (1=Value, 2=Tombstone) | key_len u32 | val_len u32 | key | value
              (Tombstone: val_len = 0; keys non-empty, strictly increasing within and across blocks)
index block:  count u32 | smallest_key_len u32 | smallest_key |
              { last_key_len u32 | last_key | block_offset u64 | block_len u32 | block_entries u32 }* |
              index_crc u32                      (index_crc over everything before it)
footer (56 B): index_offset u64 | index_len u64 | bloom_offset u64 | bloom_len u64 |
               entry_count u64 | version u32 = 1 | magic[8] = "FKVSST\0\1" | footer_crc u32
               (footer_crc over the preceding 52 bytes; bloom_* reserved, must be 0: no Bloom filter in v1)
```
Differences from the original sketch, made deliberately while implementing: the index carries the table's `smallest_key` and a per-block entry count (so structure can be validated at open without reading data blocks); `block_len` includes the block crc; magic is 8 raw bytes; bloom fields are reserved and must be zero.

- A block ends once it reaches `block_bytes` (so one large entry can exceed the target); hard cap `MAX_BLOCK_LEN` = 16 MiB; `Options::block_bytes` ≤ 8 MiB. Index cap `MAX_INDEX_LEN` = 1 GiB.
- No prefix compression, no compression codec, no Bloom filter yet.
- **Reader validation at open** (nothing in the footer is trusted before its checksum; the footer is validated before the index is read or allocated): magic → footer crc → version → bloom fields zero → `index_offset + index_len == file_len - 56` (checked arithmetic) → index crc → data blocks tile `[0, index_offset)` exactly, block lengths within `[14, MAX_BLOCK_LEN]`, entry counts plausible and summing to `entry_count`, index keys non-empty and strictly increasing, `smallest <= first last_key`, table non-empty.
- **At block read**: crc, every length bounds-checked against the block, strict key order, kind/length rules (a tombstone with a value is corruption), entry count equals the index's, last key equals the index's, block ranges do not overlap.
- **Bounded allocation**: no allocation is sized from an unchecked field; sizes are validated against hard caps, the real file length, and what the enclosing buffer can physically hold *before* allocating. Demonstrated by `tests/sstable_alloc.rs` (counting global allocator over crafted files with valid checksums).
- Lookups: binary search the index for the first block with `last_key >= key`, read it, decode (validating) and binary search its entries. `get` returns `None` (absent), `Some(Tombstone)` (deleted here; caller stops searching), or `Some(Value)` (possibly empty).
- Smallest/largest key are stored in the table (and `TableMeta`) for the Phase 3 manifest.
- The builder fsyncs the file but **not the directory**; the caller must `sync_dir` before the manifest references it. An unfinished builder deletes its partial file on drop (best effort; Phase 3 also removes unreferenced files on open).
- An `.sst` is only valid if the footer verifies. Files not in the manifest are never trusted.
- **Limitations**: linear scan within a block; whole index in memory; Unix-only `read_at`.

### 3.5 Manifest (as implemented in Phase 3)

Record log (kind=MANIFEST, frame format v2, CRC-32/IEEE) of `VersionEdit` payloads; the authoritative spec is the module doc of `src/manifest.rs`:
```
record = edit_count u32 | edit*          (one record = one atomic version change)
edit   = 1 SetNextFileNumber u64 | 2 SetLogNumber u64 |
         3 AddFile  level u8, file_no u64, size u64, smallest_len u32, smallest, largest_len u32, largest |
         4 DeleteFile level u8, file_no u64
```
- `Version` = immutable value: `l0: Vec<Arc<Table>>` (newest first = decreasing file number), `l1: Vec<Arc<Table>>` (sorted, non-overlapping).
- **Every `Db::open` writes a fresh one-record snapshot** (`MANIFEST.tmp` → fsync → rename → fsync dir), so the file appended to at run time is always one this process wrote. The same rewrite happens at run time once the file exceeds `Options::manifest_max_bytes` (default 1 MiB).
- **Replay**: an *incomplete* final frame is ignored (it was never acknowledged; the snapshot drops it). A complete frame with a bad CRC, an undecodable record, or an inconsistent edit (duplicate add, delete of an unknown file or at the wrong level, log number going backwards, file number ≥ next, overlapping L1) is `Corruption`. `SalvagePrefix` does **not** apply to the manifest (dropping a record could resurrect deleted files).
- Every table named by the manifest must exist, open cleanly, and match its recorded size and key range; otherwise `Corruption`.
- `log_number` = lowest WAL number still needed. WALs below it are obsolete.
- A directory with WAL/SST files but no MANIFEST is `Corruption` (never silently treated as empty).

### 3.6 No sequence numbers in v1
We have no snapshots/MVCC, so "newest source wins" is determined by structure (memtable > imm > L0 newest-first > L1). This is a deliberate simplification; sequence numbers are an optional extension (§12).

---

## 4. Dependencies (justified; nothing added until the phase that needs it)

| Crate | Phase | Purpose | Alternative considered |
|---|---|---|---|
| `crc32fast` | 1 | CRC-32 for record/block/footer checksums. Hardware accelerated, tiny, widely audited. It computes CRC-32/IEEE, which is what every format and document in this project specifies (known-answer test `0xCBF43926` for `"123456789"` guards against accidental algorithm drift). | Hand-roll table-driven CRC (~20 lines). Good educational option; I recommend the crate to avoid bugs in the integrity primitive, and will switch if you prefer to write it. |
| `tokio` (features: `rt-multi-thread`, `net`, `io-util`, `time`, `sync`, `signal`, `macros`) | 6 | Required by project spec: async TCP server. | — |
| `tempfile` (dev-dependency) | 1 | Isolated temp dirs for tests. | `std::env::temp_dir` + manual cleanup (error-prone). |
| Dev-only: none for property testing | — | Randomized tests use a tiny in-repo seeded xorshift PRNG for determinism. | `proptest` (optional extension). |

Explicitly **not** added: `thiserror`/`anyhow` (hand-written error enum is short), `clap` (hand-parsed flags), `rand` (own PRNG), `bytes`, `criterion`/`hdrhistogram` (see §8), `tracing` (use `eprintln!`-style minimal logging; optional later), `libc`/`fs2` (use `std::fs::File::lock` — stable since Rust 1.89 — for `LOCK`).

---

## 5. Phased roadmap

Dependency chain: **0 → 1 → 2 → 3 → 4 → 5 → 6 → 7 → 8**. Phases 1–5 are the engine and need no async. Per CLAUDE.md, each phase ends with: `fmt`, `test`, `clippy`, summary, and **a stop for your approval**.

Legend: **[M]** mandatory for first complete version, **[O]** optional extension.

---

### Phase 0 — Project skeleton [M]
- **Objective**: lib/bin layout, error type, CI-equivalent script, test scaffolding.
- **Files**: new `src/lib.rs`, `src/error.rs`, `src/config.rs`, `src/bin/forgekv-server.rs` (stub); delete `src/main.rs`; add `scripts/check.sh`; `README.md` stub; `.gitignore` adds `/bench-results/tmp`.
- **Interfaces**: `enum Error { Io{op,path,source}, Corruption{file,offset,detail}, InvalidArgument(..), Poisoned, UnsupportedVersion, Closed, Locked{path} }`; `Options` (`sync_mode: Always|Never`, `max_key_len`, `max_value_len`, `memtable_bytes`, `l0_compaction_trigger`, `sst_target_bytes`, `block_bytes`).
- **Sequence**: `git branch -m master main` (before the first commit) → remove `src/main.rs`, add lib + `src/bin/forgekv-server.rs` stub → add `scripts/check.sh` → no dependencies yet (`crc32fast`/`tempfile` arrive in Phase 1).
- **Tests**: `Error` Display/`source` chain; Options defaults validate.
- **Acceptance**: `scripts/check.sh` passes on an empty-ish crate; no warnings.
- **Depends on**: nothing. (Requires your OK to remove `main.rs`.)

### Phase 1 — Record log, WAL, memtable, minimal durable engine [M]
- **Objective**: smallest *working* engine: `put/get/delete` durable via WAL, recovered on reopen. Memtable grows unbounded in this phase (documented limitation).
- **Concepts**: framing + CRC-32/IEEE, ack-after-sync, incomplete-frame (torn-tail) recovery vs strict corruption errors, partial-write handling, tombstones in memtable, directory `LOCK`.
- **Files**: `logfile.rs`, `wal.rs`, `memtable.rs`, `engine.rs`, `fs.rs` (`sync_dir`, lock).
- **Interfaces**:
  ```rust
  Db::open(path: &Path, opts: Options) -> Result<Db>
  Db::put(&self, key: &[u8], value: &[u8]) -> Result<()>
  Db::get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>
  Db::delete(&self, key: &[u8]) -> Result<()>      // writes tombstone even if key absent
  Db::close(self) -> Result<()>
  enum Entry { Value(Vec<u8>), Tombstone }
  Memtable { map: BTreeMap<Vec<u8>, Entry>, approx_bytes: usize }
  LogWriter::append(&mut self, payload:&[u8]) -> Result<()>; ::sync(); LogReader::next() -> Result<Option<Vec<u8>>> /* with RecoveryOutcome */
  ```
- **Sequence**: (1) logfile writer/reader + header; (2) WAL encode/decode; (3) memtable + size accounting; (4) engine open→replay→truncate→append; (5) LOCK; (6) poisoning on sync failure.
- **Tests**:
  - Unit: encode/decode round-trip; empty value vs tombstone distinct; limits rejected (empty key, oversize); memtable overwrite/delete/size accounting.
  - Integration: put/get/delete; overwrite; reopen preserves state; delete survives reopen; second `open` of same dir fails (LOCK).
  - CRC known-answer: `crc32("123456789") == 0xCBF43926`.
  - **Failure/recovery**: truncate WAL at *every* byte offset inside the last frame (header and payload) → reopen yields exactly the pre-last-record state and the file is truncated to the boundary; flip one bit in a mid-log record → `Corruption`; **flip **every bit of every byte** of the final complete record (`len`, `header_crc`, `payload_crc`, payload) → `Corruption`, file unmodified; truncation at every offset inside the 12-byte header (unverifiable < 8 bytes → torn; ≥ 8 bytes intact → torn; ≥ 8 bytes with damaged `header_crc` → `Corruption`); verified `len` beyond EOF → torn, but the same bytes with a damaged `header_crc` → `Corruption`; version-1 file → `UnsupportedVersion`, file left unmodified, not silently dropped** (with `SalvagePrefix` opt-in → dropped and reported); final complete frame followed by a zero-filled/garbage extension → `Corruption`; garbage length field (`0xFFFFFFFF`) and `len == 0` → `Corruption` with no large allocation; header-only/short/zero-length WAL; wrong magic; unknown record type; truncation inside a non-newest WAL → `Corruption`.
  - **Partial I/O**: writer wrapper that accepts 1–3 bytes per `write` call and injects `Interrupted` → frames still byte-identical to a normal write; writer wrapper that fails mid-frame → writer poisoned, further appends rejected, reopen truncates the incomplete frame; reader wrapper that returns 1 byte per `read` → identical replay result to a normal read; reader hitting EOF at every possible offset classified per the §3.3 table.
  - Randomized (seeded): apply N random ops to engine and a `HashMap` model, reopen at random points, compare.
  - Regression test per bug found.
- **Acceptance**: all above pass; `kill -9` smoke test (Phase 8 formalizes) shows no acked loss; README documents that durability = `Always` semantics.
- **Depends on**: Phase 0.

### Phase 2 — SSTable writer/reader [M]
- **Objective**: immutable sorted on-disk tables, independently testable (not yet wired to engine).
- **Files**: `sstable/{mod,builder,reader}.rs`.
- **Interfaces**:
  ```rust
  SstBuilder::new(path, &Options) ; add(&mut self, key:&[u8], entry:&Entry) -> Result<()> /* enforces order */ ; finish(self) -> Result<TableMeta> /* fsync file */
  SstReader::open(path) -> Result<SstReader>
  SstReader::get(&self, key:&[u8]) -> Result<Option<Entry>>   // Some(Tombstone) distinct from None
  SstReader::iter(&self) -> SstIter  (Iterator<Item=Result<(Vec<u8>,Entry)>>)
  TableMeta { size, smallest, largest, entries, blocks }   // file_no is added by the Phase 3 manifest
  SstReader::verify(&self) -> Result<u64>                   // reads and validates every block
  ```
- **Status**: implemented in Phase 2 (see §3.4 for the actual format). New shared `codec.rs` (bounds-checked cursor, also now used by the WAL decoder).
- **Sequence**: block encode/decode → builder → footer/index → reader get → iterator.
- **Tests**:
  - Unit: round-trip many entries, 1 entry, empty-table policy (builder refuses 0 entries), key at block boundaries, entries larger than a block, tombstone round-trip, out-of-order add rejected.
  - **Corruption**: flip each region (data block, index, footer, magic) → `Corruption`, never wrong data/panic; truncate file at every footer-relevant offset → open fails cleanly; fuzz-style: 1k seeded random single-byte mutations never panic and never return a value that differs from the original unless error.
  - `get` for absent keys before first/after last/between blocks.
- **Acceptance**: every mutation test yields `Err` or the correct answer; iteration output equals a sorted `BTreeMap` model.
- **Depends on**: Phase 1 (`Entry`, errors, CRC, `fs`).

### Phase 3 — Manifest, flush, multi-layer reads [M]
- **Status: implemented.** Deviations from the text below, made deliberately:
  - *Recovery never truncates or re-appends to an old WAL.* `open` replays every live WAL (only the newest may end in an incomplete record or be salvaged; an incomplete record in an older, sealed WAL is `Corruption`), deletes orphans, flushes the recovered memtable to L0, commits a new manifest snapshot naming a new WAL number, creates that WAL, then deletes the replayed WALs. Creating the new WAL **after** the manifest commit guarantees a torn WAL can never become a "sealed" WAL on a later open (a crash test reproduces that bug when the order is swapped).
  - *No immutable memtable yet.* The flush runs inline, **before** the write that needs room is logged, so an `Err` from `put`/`delete` means the write was not applied. Memory is bounded by `memtable_bytes` (no "+ one imm" until Phase 5).
  - *Failure policy:* any error during a write, flush, compaction or manifest update poisons the engine; reopening re-runs recovery. In-memory state changes only after the manifest commit, so reads keep returning committed data.
  - *New option* `manifest_max_bytes`; new `Error::Injected` (test-only) and `Db::{flush, level_summary, check_invariants}`.
  - *Failpoints* are a Cargo feature (`failpoints`) with **thread-local** arming, which is valid while flush/compaction run on the caller's thread; Phase 5 must revisit this when a background worker exists.
- **Objective**: bounded memory. Memtable flushes to L0; engine reads across memtable + SSTs; recovery uses the manifest.
- **Concepts**: version/manifest, WAL rotation, flush ordering, orphan cleanup, tombstone shadowing, recovery = replay live WALs → flush to L0 → fresh WAL (avoids appending to a possibly-damaged old WAL and handles "crash with imm + active" without a memtable spanning two WALs).
- **Files**: `manifest.rs`, `engine.rs` (major change), `fs.rs` (file naming), `sstable` small additions.
- **Interfaces**: `VersionEdit`, `Version { l0, l1 }`, `Manifest::open/append(edit)/snapshot_if_large`, `Db::flush()` (explicit, for tests). Flush is **synchronous** inside the writer in this phase (simple; background in Phase 5).
- **Implementation sequence** (flush): rotate WAL (new file, fsync, dir fsync) → write SST from frozen memtable → fsync → fsync dir → manifest edit `{AddFile L0, SetLogNumber}` fsync → install new `Version` → delete old WAL → (crash before this step just leaves a WAL < log_number that is deleted on open).
- **Tests**:
  - Put across flush boundaries; get from each layer; overwrite in newer layer hides older; **delete in memtable hides value in SST**; **delete in L0 hides value in older L0**; tombstone flushed then reopened still hides.
  - Reopen: with data only in WAL, only in SST, and both; manifest torn tail; manifest corruption mid-log → `Corruption`.
  - **Crash-point tests** (failpoints, §9): crash after SST written but before manifest edit (→ orphan SST deleted, data recovered from WAL); crash after manifest edit before WAL delete; crash after new WAL created but before first write; each followed by reopen + model comparison.
  - Orphan cleanup: stray `.tmp`, unreferenced `.sst`, WAL below `log_number` removed on open.
  - Manifest snapshot rewrite crash: `MANIFEST.tmp` leftover ignored.
- **Acceptance**: memory bounded by `memtable_bytes` (+ one imm); all layer-shadowing tests pass; every failpoint reopen equals model.
- **Depends on**: Phases 1, 2.

### Phase 4 — Compaction [M]
- **Status: implemented** (`merge.rs`, `compaction.rs`, `Db::compact() -> Option<CompactionStats>`). Deviations:
  - *No deferred unlink.* Input tables are deleted right after the manifest commit. That is safe in this phase because every reader holds the engine mutex and Unix keeps unlinked files readable through open handles. Phase 5 (readers outside the lock) must add reference-counted deletion.
  - Compaction also runs at the end of `open` when L0 is at or over the trigger (otherwise frequent restarts would grow L0 without bound).
  - Tombstones are dropped in every compaction; the argument is in `src/compaction.rs` (all L0 + every overlapping L1 table are inputs, L1 is the bottom level).
  - Compaction runs inline in the write that triggered it (no background thread until Phase 5).
- **Objective**: bound read amplification and reclaim space; physically drop tombstones/obsolete values safely.
- **Design (deliberately simple)**: two levels. **L0**: flush outputs, may overlap, ordered by recency. **L1**: sorted, non-overlapping files of ≈ `sst_target_bytes`. When `L0.len() >= l0_compaction_trigger`, merge *all* L0 files with all *overlapping* L1 files into new L1 files. Because L1 is the bottom level and *every* older file overlapping the key range participates, **tombstones can be dropped during this compaction** (nothing older can resurrect). Multi-level leveled compaction is optional [O].
- **Files**: `merge.rs` (k-way merge; on equal keys the source with higher priority wins, duplicates skipped), `compaction.rs`, `manifest.rs` (DeleteFile), `engine.rs` (trigger, `Db::compact()` explicit).
- **Implementation sequence**: merge iterator + tests → picker → output writer splitting at size → atomic manifest edit {add outputs, delete inputs} → install version → deferred unlink (`Arc<Table>` drop with `obsolete` flag) → startup orphan cleanup already exists.
- **Crash-safe replacement protocol**: outputs fully written + fsynced + dir-fsynced **before** the single manifest record naming them. Crash earlier → old files are authoritative, outputs are orphans. Crash after the manifest record → new files authoritative, old files are orphans. There is no intermediate state in which a key is in neither.
- **Tests**:
  - Merge iterator vs sorted-model across 2..8 sources, duplicates, tombstones, empty sources.
  - After compaction: all keys' `get` equals pre-compaction values; tombstoned keys absent; file count/size reduced; L1 files non-overlapping and sorted (invariant checker `Version::check_invariants()` run after every compaction in tests).
  - **Resurrection test**: put k=1 → flush → delete k → flush → compact → reopen → `get(k) == None`.
  - Overwrite chains across 3 flushes → only newest survives.
  - **Failpoints**: crash after writing some outputs; after all outputs before manifest; after manifest before unlink; each → reopen equals model, no orphan files remain.
  - Repeated randomized workload (seeded) with interleaved flush/compact/reopen vs model, ≥ several thousand ops.
  - Read handles during compaction (Phase 5 re-runs this concurrently).
- **Acceptance**: invariant checker holds; resurrection and crash tests pass; disk usage after delete-heavy workload + compaction is lower than before (measured, test asserts relative drop, no absolute target).
- **Depends on**: Phase 3.

### Phase 5 — Concurrency model and backpressure [M]
- **Status: implemented and verified** (see `src/engine.rs` module docs, `tests/concurrency.rs`). Deviations from the text below:
  - *Locks:* as planned, `write` mutex + `RwLock<Arc<Snapshot>>` (active memtable, immutable memtable, `Arc<Version>`); additionally `bg` (mutex + condvar for jobs, backpressure, failure, shutdown) and `vset` (manifest appender). Order: `write -> bg -> snap`, `vset -> snap`; `snap` is never held while taking another lock; the worker never takes `write`.
  - *WAL rotation* moved from the flush to the **memtable switch** on the writer thread (the new WAL must exist before writes go to the new memtable). Failpoint `flush:after_new_wal` became `switch:after_new_wal`.
  - *No L0 slowdown/stop triggers.* The single worker runs a compaction whenever L0 ≥ `l0_compaction_trigger` *before* flushing again, so L0 never exceeds the trigger, and writers block once two memtables are full. (Phase 4's inline compaction kept L0 strictly below the trigger; `tests/lsm.rs::automatic_compaction_bounds_l0` now asserts `<=` for that reason.)
  - *Deferred unlink* is implemented by reference counting: compaction inputs are marked obsolete after the commit and their file is deleted when the last `Arc<Table>` drops. A failed deletion is counted in `Db::stats()` (cannot be returned from `Drop`); the file becomes an orphan removed on the next open.
  - *Failures:* an error in the writer or the worker poisons writes; the call that hit or waited on it gets the original error (`Injected(name)` in tests) and later writes get `Poisoned`; reads keep working; `Db::stats().background_error` holds the message. A **panic** on the worker thread is treated the same way (a drop guard poisons the engine and wakes every waiter; added by the post-Phase-6 audit).
  - *Failed writes:* an `Err` from `put`/`delete` returned before the record was logged means "not applied". If the WAL write or sync itself fails, the outcome is **unknown**: the record may be in the log, is not visible in this process (the engine is poisoned), and may be replayed by recovery (`tests/crash.rs::failure_after_the_wal_write_has_an_unknown_outcome`). The earlier wording ("an `Err` means not applied") was wrong for this case.
  - *Shutdown:* `Db::shutdown(&self)` (also `close`, and best-effort in `Drop`) wakes blocked writers with `Closed`, lets the worker finish its current job, joins it, then syncs the WAL. An unflushed immutable memtable stays in its WAL and is recovered on the next open.
  - *Failpoints* are now a per-`Db` registry (`Db::open_with_failpoints`, `Db::failpoints()`) that fires on any thread, with **fail** and **pause** actions; pause-points drive the deterministic read-gap, refcount, backpressure and shutdown tests.
  - Not done: `loom`/`miri` (remain optional).
- **Objective**: make `Db` `Send + Sync`, safe under concurrent get/put/delete with background flush/compaction.
- **Locking design (explicit)**:
  - `write_mutex: Mutex<WriteState>` { active WAL, active memtable handle } — serializes writers (WAL order = memtable order).
  - `state: RwLock<Arc<DbState>>`-style snapshot: `{ mem: Arc<Memtable>, imm: Option<Arc<Memtable>>, version: Arc<Version> }`. Readers take the lock **only to clone Arcs**, then read without holding it. Memtable uses an internal `RwLock<BTreeMap>`.
  - One background thread (std::thread + `Condvar`) runs flush then compaction. Single background worker ⇒ no concurrent version edits; manifest appended only from that thread (and recovery).
  - **Backpressure**: if `imm` exists and active memtable is full again, writers **block** on a condvar (bounded memory = at most 2 memtables). Optionally a soft-slowdown when L0 count is ≥ trigger + k; a hard stall at a higher count. Thresholds configurable; no claim of optimality.
  - Lock order documented: `write_mutex` → `state` (never reverse). Never hold either across file I/O except the WAL append inside `write_mutex` (that is the serialization point by design).
  - `close()`: stop accepting writes, flush/sync WAL, signal + join background thread. `Drop` does a best-effort close and logs failures.
- **Files**: `engine.rs` (restructure), `config.rs`, small `bg.rs` if engine grows.
- **Tests**:
  - Multi-thread: N writers on disjoint keys, M readers; afterwards all keys present; readers never see torn values (values self-validating: embed key + checksum).
  - Concurrent writers on the *same* key: final value is one of the written values, and equals the value after reopen.
  - Readers during flush and during compaction never return `None` for a key that has been acked and not deleted (**no read gap** test).
  - Backpressure: tiny memtable + slowed flush (failpoint sleep) → memory stays bounded, writers eventually proceed, no deadlock (test with timeout).
  - Shutdown while writers run: every `Ok` write is present after reopen; others return `Closed`.
  - Run suite also with `RUST_TEST_THREADS=1` and repeated loops (`for i in $(seq 50)`) to expose races. Optional: `cargo +nightly miri`/`loom` on the small sync core [O].
- **Acceptance**: stress tests pass repeatedly (e.g., 50 iterations) with no deadlock/lost write; documented lock order.
- **Depends on**: Phases 3, 4.

### Phase 6 — Protocol and async TCP server [M]
- **Status: implemented and verified** (`src/protocol.rs`, `src/server.rs`, `src/client.rs`, `src/bin/forgekv-server.rs`, `tests/server.rs`). Deviations / specifics:
  - *Byte order:* every protocol integer (frame length and field lengths) is **big-endian**; the on-disk formats stay little-endian.
  - *Status mapping:* engine `InvalidArgument` → BAD_REQUEST; engine `Closed` (shutting down) → BUSY; any other engine error → INTERNAL with a generic message (details go to stderr). PING is answered without taking an engine permit.
  - *Frame rules:* length 0 → BAD_REQUEST, connection kept (the frame is still delimited); length > `max_frame` (`1+4+max_key+4+max_value`) → TOO_LARGE and close; a key/value over the limit inside a frame → TOO_LARGE and close, as planned.
  - *Allocation:* the payload buffer grows as bytes arrive (doubling, capped at the declared length), so a client declaring a large frame and stalling costs nothing, and the buffer never exceeds `max_frame`. While a request is decoded the frame and the decoded key/value coexist (≈ `2 × max_frame` per connection), then the frame is freed. There is no global memory budget: the server-wide worst case is `max_connections × ≈2 × max_frame` (≈ 2 GiB with defaults). (The audit found the first version used `read_to_end`, which rounded capacity up to 2× the frame.)
  - *Shutdown:* listener dropped first (new connects refused), idle connections closed, an in-flight request completes and is answered, then `shutdown_grace`, then `Db::shutdown` on the blocking pool. The binary handles SIGINT/SIGTERM; a second signal exits immediately. The signal handlers are installed **before** `listening on` is printed (the audit found that a SIGTERM right after readiness could otherwise kill the process).
  - *Counters:* `ServerStats` (accepted/rejected connections, requests, bad/too-large/busy/internal, timeouts) instead of a logging framework.
  - *Dependency added:* `tokio` 1.x (resolved 1.54.1) with features `rt-multi-thread, net, io-util, time, sync, signal, macros`, as justified in §4.
- **Objective**: network access with bounded resources.
- **Protocol (binary, explicit)**
  ```
  frame:    len u32 BE | payload[len]          len ≤ max_frame (default 2 MiB; > max key+value+header)
  request:  op u8 | body
             1 PING   : (empty)
             2 GET    : key_len u32 | key
             3 PUT    : key_len u32 | key | val_len u32 | value
             4 DELETE : key_len u32 | key
  response: status u8 | body
             0 OK (PUT/DELETE/PING: empty; GET: val_len u32 | value)
             1 NOT_FOUND
             2 BAD_REQUEST  (body: utf8 message)   -- connection stays open
             3 TOO_LARGE    (frame/key/value limit) -- connection closed
             4 BUSY         (server overloaded)     -- connection stays open
             5 INTERNAL     (engine error; body generic message, details logged server-side)
  ```
  Requests on one connection are answered **in order**; pipelining allowed but bounded. A version/hello byte is deliberately omitted in v1 (documented); protocol evolution = new opcodes.
- **Malformed-input rules**: oversize `len` → respond TOO_LARGE and close (cannot resync a stream); valid frame with unknown opcode / short body / trailing bytes / `key_len` exceeding body → BAD_REQUEST, continue; partial frame then EOF → drop connection silently (log at debug); never allocate based on an unchecked length (check against `max_frame` first, read incrementally into a bounded buffer).
- **Concurrency**: `Db` is blocking (fsync). Each request calls the engine via `tokio::task::spawn_blocking`, gated by a `Semaphore` (`max_inflight_engine_ops`) so blocking pool use is bounded; if the semaphore wait would exceed a short limit → BUSY. Connection count bounded by `Semaphore` (`max_connections`); excess connections get closed immediately (or BUSY then close).
- **Timeouts**: idle read timeout (no request begun), request-body read timeout once a frame header arrived (slowloris defense), write timeout. All configurable.
- **Graceful shutdown**: `ctrl_c`/SIGTERM → stop accept loop → `watch` flag tells connections to finish the in-flight request and close → wait for connection tasks up to a drain deadline → then `Db::close()` (flush WAL, join bg thread). Second signal = immediate exit (acked data still durable by WAL).
- **Files**: `protocol.rs` (pure encode/decode, fully unit-testable without sockets), `server.rs`, `client.rs`, `bin/forgekv-server.rs` (flags `--dir --addr --sync always|never --max-conns ...`, hand-parsed), `Cargo.toml` (+tokio).
- **Tests**:
  - Unit: protocol round-trips; truncated payloads at every length; unknown opcode; length overflow; max-size boundary ±1; fuzz-style random bytes never panic.
  - Integration (ephemeral port `127.0.0.1:0`): put/get/delete over TCP; NOT_FOUND; pipelined requests ordered; malformed frame handling per rules above (raw socket writes); oversize length rejected without large allocation; slow client times out; idle timeout; connection limit enforced (N+1th refused/closed); **many concurrent clients** (e.g., 64 tasks) disjoint and overlapping keys; client disconnect mid-request doesn't affect others or crash server.
  - Shutdown: in-flight requests complete, new connections refused, engine closed, restart sees all acked writes.
  - **Server crash test**: spawn server binary, clients write, `kill -9`, restart, verify every acked response's data present.
- **Acceptance**: all tests pass; server RSS bounded under connection-flood test (assert limits are enforced via counters, not absolute memory); `cargo clippy` clean.
- **Depends on**: Phase 5.

### Phase 7 — Reproducible benchmarks [M]
- **Status: implemented and run** (`src/bin/forgekv-bench/`, `scripts/bench.sh`, `docs/BENCHMARKS.md`, raw results `bench-results/20261026T160449Z/`: 25 configurations × 5 runs, 0 errors). Deviations / specifics:
  - *Layout:* the harness is a multi-file binary (`src/bin/forgekv-bench/{main,run,stats,workload,env,json}.rs`) instead of one file; no library code was added for it.
  - *Extra modes:* `--mode recovery` (time of `Db::open` after `keys` writes left in the WAL; requested for Phase 7) and `--mode fsync` (device baseline: append + `fdatasync`, no ForgeKV code), added after the device's fsync latency turned out to be bimodal (≈0.6 ms or ≈3–4 ms).
  - *Output:* one JSON per **configuration** containing every run's raw statistics plus median/min/max, instead of one JSON per run; `scripts/bench.sh` groups a session in `bench-results/<UTC timestamp>/` with `log.txt` and `summary.md`. Data directories go to `bench-results/tmp/` (git-ignored); the harness refuses to reuse an existing run directory and never overwrites a result file.
  - *Provenance:* there is no git commit yet, so each file records `git_head: none`, `git_dirty`, and a CRC-32 `source_fingerprint` of the crate sources in addition to the planned metadata (CPU, governor, RAM, OS, kernel, filesystem, mount, device model, rustc, profile, command line).
  - *Preload:* keys in order with `SyncMode::Never`, then flush + compact (dataset in L1), then reopen with the configured mode.
  - *Not implemented:* `--key-dist zipf-lite` (optional), open-loop mode (optional), batch levels (the engine has no batch API).
  - *Acceptance:* generator determinism and percentiles are unit-tested; a smoke test runs every mode. Run-to-run variance is reported, not hidden: `SyncMode::Always` results vary with the device's bimodal fsync latency. The Phase 7 session `bench-results/20261026T160449Z/` measured the Phase 7 source (`source_fingerprint` `crc32:0f6f6c76 over 30 files`). Phase 8 changed the source, so README and `docs/BENCHMARKS.md` now cite the Phase 8 session `bench-results/20261026T173115Z/` (`crc32:bbce3c06 over 31 files`) and keep the Phase 7 session only for a labelled comparison. Neither is committed yet (nothing in the repository is).
  - *Findings recorded as limitations, not fixed in this phase:* no group commit (concurrency does not raise `Always` write throughput); `std::sync::Mutex` writer lock is unfair (individual writes waited up to 10 s with 4 `Always` writers); two-level compaction rewrites nearly all of L1 under random writes (1000-byte values: ~24k PUT/s, max ~0.7 s).
- **Objective**: measured, repeatable throughput and latency. **No targets are set in advance; no number appears in docs without the raw result file and command that produced it.**
- **Harness**: custom binary `forgekv-bench` (justification: need p50/p95/p99 and workload control; `criterion` is for microbenchmarks and its default statistics don't give tail-latency percentiles for concurrent workloads). Latencies recorded per-operation with `Instant` into per-thread `Vec<u64>` (nanoseconds), merged and sorted at the end → exact percentiles (no histogram dependency; memory ≈ 8 B × ops, bounded by duration/op cap).
- **Parameters (all flags, all recorded in the output)**: `--mode engine|server`, `--workload fillseq|fillrandom|readrandom|readwrite --read-pct N|delete-heavy`, `--keys`, `--key-size`, `--value-size`, `--threads/--clients`, `--warmup-secs`, `--duration-secs`, `--sync always|never`, `--seed`, `--key-dist uniform|zipf-lite [O]`, `--dir` (default under a dedicated path on the target filesystem; **warn if tmpfs**).
- **Procedure**: fresh dir → preload dataset (not timed, reported) → warm-up (not recorded) → timed run → report. Each configuration run ≥ 5 times; report median and min/max of throughput and p50/p95/p99. Seeded PRNG ⇒ identical key sequence.
- **Output**: one JSON (and a human table) per run in `bench-results/<timestamp>-<name>.json` containing: git commit, `rustc -V`, build profile (`--release`), CPU model, cores, RAM, OS/kernel, filesystem type + mount, all parameters, ops, ops/s, p50/p95/p99/max, error counts, WAL sync mode, number of SST files at end, final DB size.
- **Methodology caveats documented**: closed-loop clients suffer coordinated omission (latency understated under saturation) — we report that explicitly and may add an open-loop fixed-rate mode [O]; `SyncMode::Always` is bounded by device fsync latency (compare only against runs with the same sync mode); engine-mode vs server-mode results are not comparable; no comparison to other databases unless identical workloads/durability settings are run by us.
- **Tests (correctness of the harness, kept separate from perf)**: percentile function against known arrays; workload generator determinism for a fixed seed; a smoke run with tiny parameters (`--duration-secs 1`) under `cargo test` verifying the output schema and zero errors — *not* asserting any performance value.
- **Acceptance**: two runs with the same seed/config produce the same op sequence and results within reported variance; README "Performance" section contains only numbers traceable to committed result files (or states "not yet measured").
- **Depends on**: Phase 6 (engine mode only needs Phase 5).

### Phase 8 — Hardening, crash testing, documentation [M]
- **Status: implemented and verified.** Deviations / specifics:
  - *Crash tests added:* runtime MANIFEST rewrite after a flush and after a compaction at both rewrite failpoints (`tests/crash.rs::every_runtime_manifest_rewrite_step`; open's rewrite was already covered). The failpoint matrix now covers every point in `failpoint::ALL`.
  - *`kill -9` loop* (`tests/kill.rs`): the test binary re-executes itself as the writer child (no extra binary); it acknowledges each operation on stdout ("ack over pipe"), the parent SIGKILLs it at a seeded random moment, reopens, and checks every acknowledged operation (only the single in-flight one may go either way) plus invariants; 20 rounds alternating `Always`/`Never` on one directory, with flushes and compactions running. A planted bug (old WAL deleted before the MANIFEST commit) fails it 3/3. Server variant: SIGKILL of the binary with 4 clients' writes in flight (`tests/server.rs`).
  - *Simulated unsynced loss* (`tests/recovery.rs::losing_unsynced_wal_bytes_recovers_a_consistent_prefix`): instead of exposing a synced offset from the engine, the test uses the documented sync points (a WAL header is synced at creation; under `Never` nothing after it is synced until rotation/close), truncates the live WAL at 69 offsets between the synced and written ends (the two ends and their neighbours, the first five record boundaries, 60 seeded random points), and checks recovery to an exact prefix. The companion "zero/garbage extension is `Corruption`" test already existed (`zero_filled_or_garbage_tail_is_corruption`).
  - *Corruption sweep over all file types* (`tests/corruption.rs`): a database with MANIFEST, L1 and L0 SSTables and a WAL; one bit flipped per byte (stride ≤ 2, ≤ 1500 cases). **Found a real gap:** the 6 reserved bytes of the record-log header (WAL, MANIFEST) were not validated and not checksummed, so damage there was silently accepted. Fixed: non-zero reserved bytes are `Corruption` (existing files are unaffected: writers always wrote zeros). Regression tests: `logfile::tests::non_zero_reserved_header_bytes_are_corruption`, and the sweep now requires that no flip goes unnoticed.
  - *Format compatibility* (PLAN §9 risk 8): `tests/golden.rs` with a stored database `tests/data/golden-db/` — it must open with the expected contents, and the current code must rebuild it byte for byte.
  - *Writer starvation (Phase 7 finding): fixed.* Direct evidence first: 4 `Always` writers on ext4 for 4 s completed 104/545/191/320 writes, worst wait 3.3 s (`std::sync::Mutex` lets the releasing thread re-acquire before woken waiters run). The writer lock is now a FIFO ticket lock in safe Rust (`src/fairlock.rs`, unit-tested for arrival order, no barging, poisoning, mutual exclusion). After: 1505/1505/1505/1505 writes, worst wait 10–23 ms. Cost, measured: contended `Never` writes are slower (each hand-off wakes a sleeping thread); see `docs/BENCHMARKS.md`. Group commit would remove the cost but is an optional extension (§10).
  - *Compaction-stall instrumentation* (small, in `Db::stats()`): `write_stalls`, `write_stall_time`, `max_write_stall`, `write_stalls_during_compaction` (stalls that began while the worker was compacting) and the gauge `writers_stalled_now`; tested deterministically with pause points; recorded per benchmark run. The Phase 7 attribution of tail latency to compaction was an inference; Phase 8 benchmarks measure it.
  - *Benchmark accounting correction:* Phase 7 `end_state` counters (flushes, compactions) were cumulative since open, so they **included the warm-up**; `docs/BENCHMARKS.md` called them "in 10 s". Phase 8 runs report measured-phase deltas.
  - *Protocol/operations:* connection-flood test (200 connections over a limit of 8: all answered BUSY and counted, admitted clients unaffected, slots reusable). No new defect found in malformed-input, limit or timeout handling.
  - *Race-prone tests fixed:* two Phase 6 server tests asserted that connecting to a closed server's port fails; once the listener is closed the OS may reuse the port (parallel tests) or self-connect, so they now assert that *this* server's accept counters do not move. The new stall tests first used fixed sleeps and failed on ext4 (slow fsync); they now wait for the `writers_stalled_now` gauge.
  - *`flock` and child processes (finding):* a process that spawns children while closing and reopening a database could see a spurious `Error::Locked` (probe: 2699 of 3000 reopens while another thread spawned processes; 0 of 3000 without), which caused a 1-in-60 flake in the server tests. Phase 8 worked around it with a retry in the test harness; **superseded by the post-Phase 8 fix below** (retry removed).
  - *Audit:* clippy restriction lints (`unwrap_used`, `expect_used`, `panic`, `unreachable`, `todo`, `unimplemented`) over the library and binaries report only the test-only failpoint panic action; `#![forbid(unsafe_code)]`. Two `unreachable!` introduced in `fairlock.rs` were removed by relying on field drop order.
  - *Docs:* README rewritten (architecture, durability statement, formats, how to run, performance, limitations); new `docs/FORMATS.md`; stale statements corrected (recovery discards an incomplete frame instead of truncating the file; SSTable "not integrated"/"until Phase 8" remarks; `Err` semantics).
  - *Benchmarks re-run* because the source changed: full matrix, 25 configurations × 5 runs, 0 errors, session `bench-results/20261026T173115Z/` (`crc32:bbce3c06 over 31 files`, re-checked against the final tree). Phase 7 → Phase 8 comparison for all configurations is in `docs/BENCHMARKS.md`.
  - *Verification:* `scripts/check.sh` (both feature sets), 20× repeats of the concurrency/crash/recovery/kill/corruption/server/golden suites, a single-threaded run, a full run on ext4, 80× the server suite after the flake fixes (then with the lock retry; repeated 80× without any retry after the post-Phase 8 lock fix), and a one-off stride-1 corruption sweep (2,078 bytes, every flip detected).
  - *Not done (optional or out of scope):* power-loss testing (`dm-flakey`), `loom`/`miri`/fuzzing, group commit, Bloom filter, `scan`.
- Failpoint matrix finalized (§9); `kill -9` loop test (spawn writer child, ack over pipe, SIGKILL at random time, reopen, verify acked ⊆ present and no unacked-but-corrupt state) run N iterations.
- Simulated unsynced-loss: test helper that, after a "crash", truncates the WAL to a random length between the last-synced offset and the written offset (requires engine to expose synced offset under `cfg(test)`/failpoints feature); verify recovery lands on a consistent prefix (truncation yields an *incomplete* frame, which is the only case recovery repairs). A companion test extends the WAL with zero bytes / garbage and asserts `Corruption`, documenting the §2 limitation rather than hiding it.
- Deterministic corruption sweep over all file types.
- Clippy pedantic spot-check on `unwrap`/`expect`/`unsafe` (`#![forbid(unsafe_code)]` in the lib).
- Docs: README (architecture, formats, durability statement, limitations, how to run, benchmark section), `docs/FORMATS.md`. Optional: Bloom filter [O], `scan` [O].
- **Acceptance**: see §11.
- **Depends on**: all.


### Post-Phase 8 — Directory lock released explicitly (bug fix)
- **Symptom:** after a `Db` was dropped, an immediate reopen in the same process failed with `Error::Locked` while another thread was spawning processes (2,699 of 3,000 reopens in the Phase 8 probe; 0 without spawning).
- **Root cause (verified):** `LOCK` is locked with `flock`, which belongs to the *open file description* and is released implicitly only when every descriptor referring to it is closed. `std::process::Command` creates the child with `clone3(CLONE_VM | CLONE_VFORK)` without `CLONE_FILES` (seen with `strace`), so the child starts with a copy of the parent's descriptor table — another reference to the lock's description — until its `execve` closes the descriptor (`O_CLOEXEC`, flags `02100001` in `/proc/self/fdinfo`). `CLONE_VFORK` suspends only the spawning thread, so another thread could drop the `Db` (closing its descriptor; the lock survived through the child's copy) and reopen (a new description, which conflicts) inside that window. The descriptor is **not** inherited across `exec`: an exec'd child does not have it.
- **Fix (safe Rust, no new dependency):** `fs::DirLock` calls `File::unlock` (`flock(LOCK_UN)`) in `Drop`, which releases the lock for every descriptor of the description, so the release happens exactly when the `Db` lets go (`DirLock` is the last field of the engine state, so it is released after everything else). Exclusivity is unchanged: the lock is held for the whole life of the `Db`, and any other open — in this process or another — uses its own description, which conflicts. Not chosen: a retry (masks the bug, delays genuine conflicts), `fcntl` record locks (per-process, so they would no longer exclude a second `Db` in the same process, and they need FFI or a new dependency), unsafe FFI.
- **Regression tests (deterministic):** `fs::tests::a_duplicated_descriptor_does_not_keep_the_lock_after_its_owner_drops` (an extra reference made with `try_clone`, exactly what the child holds) and `a_child_process_holding_the_description_does_not_keep_the_lock` (a real `sleep` process holding the description as its stdin across `exec`). Process tests in `tests/lock.rs` (the test binary re-executes itself as the other process): exclusivity in both directions, release on close and normal exit, release on SIGKILL, a child spawned while open does not keep the lock, and 300 close/reopen cycles while another thread spawns processes with **zero** `Locked` and no retry. With the explicit unlock removed, both unit tests fail and the 300-cycle test fails 5/5 (256–279 `Locked`).
- **Also fixed:** the retry added to the server test harness in Phase 8 is removed; `tests/kill.rs` parsed its first `ACK` line incorrectly (libtest prints `test child_writer ... ` before it without a newline; harmless because later ACKs were counted, now fixed).
- **Verification:** `scripts/check.sh` (208 tests without failpoints, 233 with); `tests/lock.rs` 50×, server suite 80× (no retry), `tests/kill.rs` 10×, all passing; single-threaded run of the lock, kill and server suites; full suite on ext4 (233/233).
- **Remaining risk:** a *crashed* process cannot run `Drop`; then the lock is released when every process holding a copy of the descriptor has exited or exec'd (normally immediately). A child that inherits the descriptor *across* `exec` (only possible if someone passes it explicitly, as the regression test does) would keep the lock after its parent's crash until it exits.

---

## 6. Implementation discipline per phase (from CLAUDE.md)

Each phase will be delivered as: goal/design recap → file list → invariants/edge cases → test plan → **your approval** → implementation → `scripts/check.sh` output → summary (changed files, results, limitations, next steps). Small commits, only when you ask.

## 7. Commands

`scripts/check.sh` (created in Phase 0):
```bash
set -euo pipefail
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets
```

| Purpose | Command |
|---|---|
| Format (apply) | `cargo fmt` |
| Format check | `cargo fmt --check` |
| Lint | `cargo clippy --all-targets --all-features -- -D warnings` |
| Unit + integration tests | `cargo test` |
| One test, with output | `cargo test --test recovery wal_torn_tail -- --nocapture` |
| With failpoints (Phases 3+) | `cargo test --features failpoints` (crash tests in `tests/crash.rs`; `scripts/check.sh` runs both with and without the feature) |
| Tests on a real disk (tmpfs ignores fsync) | `mkdir -p target/ext4-tmp && TMPDIR=$PWD/target/ext4-tmp cargo test --all-features` |
| Race-hunting loop (Phase 5+) | `for i in $(seq 50); do cargo test --release --test concurrency || break; done` |
| Single-threaded tests | `RUST_TEST_THREADS=1 cargo test` |
| Run server (Phase 6) | `cargo run --release --bin forgekv-server -- --dir ./data --addr 127.0.0.1:7878 --sync always` (also `--max-connections N --max-inflight N --idle-timeout-ms N --request-timeout-ms N`) |
| Benchmark (Phase 7) | `cargo run --release --bin forgekv-bench -- --mode engine --workload readwrite --read-pct 50 --keys 1000000 --key-size 16 --value-size 100 --threads 8 --warmup-secs 10 --duration-secs 60 --sync always --seed 42 --dir /path/on/real/disk` |
| Docs | `cargo doc --no-deps` |
| Optional later | `cargo +nightly miri test -p forgekv --lib memtable` ; `cargo audit` |

The `failpoints` feature is a Cargo feature (no extra dependency): a `fail_point!("name")` macro that, when the named point is armed via a test-only static, returns `Err(Error::Injected)`; compiled to nothing otherwise.

## 8. Key correctness invariants

1. **Ack ⇒ durable (per sync mode)**: a write is acked only after its WAL record is fully written, and (in `Always`) `sync_data` returned `Ok`.
2. **WAL before memtable**: every memtable entry has a corresponding WAL record (or lives in an SST).
3. **Recovery determinism**: same on-disk bytes ⇒ same recovered state. Only an *incomplete* final frame of the newest log may be discarded (§3.3: ignored on replay, never truncated in place). A complete frame with a bad CRC — even the last one — is an error unless the operator opts into `SalvagePrefix`.
4. **Reference-before-publish**: a file is fully written, fsynced, and its directory entry fsynced **before** the manifest names it.
5. **Manifest is the source of truth**: files not in the manifest are garbage; files in the manifest must exist and verify, else `Corruption`.
6. **Atomic version change**: add/delete sets in a compaction/flush are one manifest record.
7. **Newest source wins**: lookup order memtable > imm > L0 (newest first) > L1; a tombstone stops the search.
8. **Tombstones dropped only at the bottom level when all potentially-older data for that key range is in the compaction input.**
9. **SST order**: keys strictly increasing within a file; L1 files non-overlapping and sorted; L0 ordered by file number (recency).
10. **Never silent**: every checksum failure or I/O error is returned (or poisons the engine); none is logged-and-ignored.
11. **Bounded memory**: ≤ active + one immutable memtable, bounded frames, bounded connections, bounded blocking-pool use.
12. **Lock order**: `write_mutex` → `state`; no lock held across network I/O; poisoned engine rejects writes.
13. **No data-dependent allocation**: lengths read from disk/network are validated against limits before allocating.

## 9. Risks and mitigations

| # | Risk (hardest first) | Why it is hard | Tests that catch it |
|---|---|---|---|
| 1 | **Crash-ordering bugs** in flush/compaction/manifest (missing fsync, wrong order) | Failures only appear at specific crash points; can pass all happy-path tests | Failpoint at *every* step of flush, compaction, manifest rewrite, WAL rotation, each followed by reopen + model check; `kill -9` loop; code-review checklist of fsync/rename/dir-fsync |
| 2 | **Tombstone resurrection / premature tombstone drop** | Correct only if bottom-level condition holds | Resurrection tests (delete in L0 over value in L1 / older L0), randomized model test with interleaved flush/compact/reopen, invariant checker |
| 3 | **Torn vs corrupt WAL ambiguity** | Strict policy can refuse to open after a damaged-but-unacked tail; lax policy could drop acked data | Truncate-at-every-offset test, bit-flip tests for mid *and final* records (must be `Corruption`), zero-fill-tail test, partial read/write wrappers; policy documented, `SalvagePrefix` opt-in |
| 4 | **Concurrency: lost update, read gap during version swap, deadlock** | Rare interleavings | Stress loops, "no read gap" test, backpressure-with-timeout test, documented lock order, optional `loom` |
| 5 | **fsync semantics** (failed fsync, directory fsync forgotten, false sense of durability) | OS/hardware dependent, untestable by unit tests | Poison-on-error test (injected sync failure), explicit README limitations, wording discipline on claims |
| 6 | **Async/blocking mismatch** (fsync inside Tokio worker starving the runtime) | Easy to write, shows as latency spikes | All engine calls via bounded `spawn_blocking`; test that PING latency stays reasonable while writers fsync (assert liveness, not a number) |
| 7 | **Resource exhaustion via malformed/slow clients** | Many edge cases | Oversize-length, slowloris, connection-flood, disconnect-mid-frame tests |
| 8 | **Format evolution / accidental incompatibility** | Hidden until a later change | Magic+version in every file; golden-file tests committing small known-good files that must still open |
| 9 | **Benchmark misinterpretation** (page cache effects, tmpfs, coordinated omission, thermal/CPU frequency) | Easy to produce misleading numbers | Metadata capture, repeated runs with variance, tmpfs warning, documented caveats |
| 10 | **Scope creep** | Interview project becomes unfinishable | Mandatory/optional split below; phase gates |

## 10. Scope: mandatory vs optional

**Mandatory (v1)**: Phases 0–8 as specified: WAL + CRC-32/IEEE + incomplete-frame recovery with strict corruption errors, memtable, SSTables with block index, manifest, flush, two-level compaction with tombstone GC, background worker with backpressure, binary TCP protocol with limits/timeouts/graceful shutdown, custom benchmark harness, crash/failpoint tests, README with honest durability section.

**Optional extensions (only after v1, each needs separate approval)**: Bloom filters; range `SCAN` (merge iterator already exists); group commit (batch fsync across concurrent writers — big win for `Always`); atomic `WriteBatch`; block cache; prefix key compression / block compression; multi-level leveled compaction; sequence numbers + snapshots; `loom`/`miri`/`cargo-fuzz`; open-loop benchmark mode; Zipfian key distribution; `dm-flakey` power-loss testing; metrics endpoint.

**Explicitly out of scope**: replication, consensus, SQL/query language, transactions, TTL, encryption, Windows support.

## 11. Acceptance checklist for the complete project

Status after Phase 8 (evidence in brackets; "verified" means a test or a recorded run, not review).

- [x] `scripts/check.sh` passes (fmt, clippy `-D warnings`, all tests with and without failpoints) — in the working tree; there is no commit yet, so "clean checkout" has not been tried.
- [x] No `unsafe` (`#![forbid(unsafe_code)]`); no `unwrap`/`expect` on fallible production paths [`cargo clippy --lib --bins --all-features -- -W clippy::unwrap_used -W clippy::expect_used -W clippy::panic -W clippy::unreachable` reports only the test-only failpoint panic action].
- [x] put/get/delete correct, including empty values, max-size keys/values, and rejection of invalid sizes [`tests/engine.rs`].
- [x] Reopen test passes with data in WAL only, SST only, and both [`tests/lsm.rs::reopen_with_data_only_in_wal_only_in_sst_and_both`].
- [x] WAL incomplete-frame test at every truncation offset passes (incomplete frame discarded, state equals pre-record state) [`tests/recovery.rs`; engine-level unsynced-loss simulation `losing_unsynced_wal_bytes_recovers_a_consistent_prefix`].
- [x] Any complete WAL/manifest frame with a bad CRC — mid-log **or final** — returns `Corruption` and leaves the file unmodified; only the opt-in `SalvagePrefix` discards it [`tests/recovery.rs`, `tests/lsm.rs`].
- [x] Partial-write and partial-read injection tests pass; a failed mid-frame append poisons the writer [`src/logfile.rs`, `src/wal.rs` unit tests].
- [x] Every format and document says CRC-32/IEEE; known-answer test present; `grep -ri castagnoli\|crc-32c` finds only "we do not use" notes.
- [x] SSTable and manifest corruption sweeps never panic and never return wrong data [`tests/sstable.rs`, `tests/lsm.rs`, whole-database sweep `tests/corruption.rs`].
- [x] Deletes never resurrect across memtable/L0/L1, flush, compaction, or restart [`tests/lsm.rs`].
- [x] Compaction invariants hold after every compaction in randomized model tests [`tests/lsm.rs`, `tests/crash.rs::randomized_crashes_match_model`].
- [x] Failpoint crash matrix covers every failpoint in `failpoint::ALL` (WAL rotation, flush, compaction, MANIFEST rewrite at open and at run time, recovery); all recover to model state with no orphan files [`tests/crash.rs`].
- [x] `kill -9` test (engine and server) shows zero acknowledged-write loss over N iterations [`tests/kill.rs`: 20 random SIGKILLs during writes/flushes/compactions; `tests/server.rs`: 6 SIGKILLs with requests in flight + 3 between batches]. Process crash only — not power loss.
- [x] Injected fsync failure poisons the engine [`src/logfile.rs`/`src/wal.rs` unit tests; `tests/crash.rs::failure_after_the_wal_write_has_an_unknown_outcome`].
- [x] Concurrency stress tests pass over repeated runs; no deadlock; memory bounded by configured limits [`tests/concurrency.rs`; 20× repeated runs, single-threaded run, ext4 run — see the Phase 8 status].
- [x] Protocol: malformed/oversize/truncated frames handled per spec; fuzz-style test never panics [`src/protocol.rs`, `tests/server.rs`].
- [x] Server enforces connection limit, in-flight limit, idle/read/write timeouts; graceful shutdown drains and closes the engine [`tests/server.rs`, including a 200-connection flood].
- [x] Benchmark harness produces JSON with full environment + parameters; ≥ 5 runs per reported config; percentiles verified by unit tests [`src/bin/forgekv-bench/`].
- [ ] README states durability guarantees and limitations, formats, how to run tests/benchmarks — **done** — and **only** benchmark numbers backed by committed result files — **not met yet**: the numbers trace to files in `bench-results/`, which are not committed (nothing is, by instruction).
- [x] Every discovered bug has a regression test (see the per-phase status notes).

## 12. Decisions (approved, revision 2)

1. **Two-level compaction** (L0 + L1) for v1. Multi-level is optional later.
2. **No sequence numbers/snapshots** in v1.
3. **`crc32fast` crate, CRC-32/IEEE** in every format and document.
4. **Default `SyncMode::Always`**; `Never` for benchmarks/ephemeral use.
5. **`src/main.rs` replaced** by lib + `src/bin/forgekv-server.rs` in Phase 0.
6. **Branch renamed `master` → `main`** before the first commit.
7. **WAL/manifest recovery**: only an incomplete final frame is truncated; any complete frame with a bad CRC (including the last) is `Corruption`; mid-log corruption is a hard error; `SalvagePrefix` is an explicit opt-in.
8. **Partial writes/reads are handled explicitly**; append errors poison the writer.
