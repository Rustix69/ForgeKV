# ForgeKV benchmarks

## Which results describe which code

| Session (raw files) | Source it measured | Status |
|---|---|---|
| `bench-results/20261026T173115Z/` | `source_fingerprint` **`crc32:bbce3c06 over 31 files`** — the Phase 8 source | **current results**: every number on this page outside the comparison section |
| `bench-results/20261026T160449Z/` | `crc32:0f6f6c76 over 30 files` — the Phase 7 source (before the FIFO writer lock, the reserved-header check and the stall counters) | kept for provenance; describes **old code**, used only in the comparison section |

**The code has changed once since that session** (fingerprint now
`crc32:3e1430fe over 31 files`), and it has **not** been re-benchmarked.
The only change is the post-Phase 8 directory-lock fix: `src/fs.rs`
(`DirLock`, which calls `flock(LOCK_UN)` when a database is dropped),
`src/engine.rs` (the lock field's type and comment) and `src/error.rs` (a
doc comment). Lock acquisition is the same `try_lock` call as before, and
the release runs when a `Db` is dropped, which no benchmark times (it
happens after the measured phase; recovery times `Db::open` only). So the
numbers below are expected to carry over, but strictly they describe
`crc32:bbce3c06`.

**Redaction.** Before the first commit, the `environment.bench_dir` field
of all 50 JSON files was changed from the local absolute path of the
benchmark directory to `<repository>/bench-results/tmp`. Nothing else was
changed: every file is otherwise identical to what the harness wrote
(checked by comparing the parsed JSON), and `log.txt` and `summary.md` are
untouched. The harness still records the absolute path, so review new
result files before publishing them.

There are no git commits yet, so each result file identifies its code by a
CRC-32 over the crate's `src/`, `Cargo.toml` and `Cargo.lock`
(`environment.source_fingerprint`); it was re-checked against the current
tree after the Phase 8 session. Each session directory holds one JSON file
per configuration (every run's raw statistics, the environment, the exact
command line), `log.txt` (complete output) and `summary.md`. Nothing was
re-run selectively or dropped: all 25 configurations × 5 runs of each
session are reported. The tables below are copied from the `summary.md`
files by script, not retyped.

These numbers describe **this machine, this build, these workloads**. They
are not a comparison with any other database.

## How to reproduce

```bash
scripts/bench.sh                       # full matrix, ~25 minutes; writes bench-results/<UTC timestamp>/
# one configuration, e.g.:
cargo run --release --bin forgekv-bench -- --mode engine --workload readwrite --read-pct 50 \
    --keys 200000 --key-size 16 --value-size 100 --threads 4 --sync always \
    --runs 5 --warmup-secs 3 --duration-secs 10 --seed 42 --dir bench-results/tmp --out bench-results/mine
```

The exact command of every configuration is the `+ ...` line in `log.txt`
and `environment.command_line` in its JSON file. `forgekv-bench --help`
lists all flags.

## Environment (recorded in every JSON file)

| | |
|---|---|
| CPU | 12th Gen Intel Core i5-12500H, 16 logical CPUs, governor `powersave` |
| RAM | 15,499,704 kB (`MemTotal`) |
| OS | Ubuntu 26.04 LTS, kernel 7.0.0-38-generic |
| Filesystem | ext4 (`rw,relatime`) on `/dev/nvme0n1p5`, mounted at `/` |
| Storage | NVMe SSD, SK hynix PC801 HFS512GEJ9X101N |
| Toolchain | rustc 1.98.1 (48a229cea 2026-09-01), `--release` (default profile) |
| Current session | 2026-10-26 17:31:15–17:56:20 UTC; 1-minute load average 4.12 at start (the Phase 7 session started at 1.80; the difference is probably the test runs that had just finished); laptop with a desktop session, not an isolated server |
| Engine options | defaults except `sync_mode` and, for recovery, `memtable_bytes`: memtable 4 MiB, L0 trigger 4, SST target 2 MiB, block 4 KiB |

## Method

- **Harness:** `src/bin/forgekv-bench/` (custom; no Criterion or
  HdrHistogram). Every operation's latency is recorded with `Instant`
  around the call, kept in full, sorted and indexed: **exact nearest-rank
  percentiles**. Percentiles and the workload generator are unit-tested; a
  smoke test runs every mode with tiny parameters and checks the output
  schema and zero errors (it asserts nothing about speed).
- **One repetition:** fresh directory → preload (read/mixed workloads only:
  keys `0..keys` in order with `SyncMode::Never`, then flush + compact so the
  dataset starts in L1; reported as `preload_secs`) → open with the
  configured sync mode → **3 s warm-up, not recorded** → **10 s measured** →
  engine state captured → close → directory deleted. Five repetitions per
  configuration.
- **Summary statistics:** per run, ops/s and p50/p95/p99/p99.9/max per
  operation kind; across runs, the **median, min and max** of each. Tables
  show the median of each run's percentile.
- **Engine counters per run** (Phase 8 session): flushes, compactions,
  **write stalls** (a writer waited for memtable room because both
  memtables were full), how many of those **began while the background
  worker was compacting**, and total stall time — all as deltas over the
  measured phase. These come from `Db::stats()`. Writers queue on the FIFO
  writer lock, and only the writer holding it waits for room, so total
  stall time is time during which *no* write could proceed. (The Phase 7
  session's flush/compaction counts were cumulative since open and so
  included the warm-up; its documentation called them "in 10 s", which was
  wrong.)
- **Workloads:** uniform random keys, 16-byte keys, 100-byte incompressible
  values (one configuration 1000 B), 200,000-key dataset (one configuration
  1,000,000). Seed 42 for every run. `deleteheavy` = 20% GET, 40% PUT,
  40% DELETE. GETs in read workloads always hit.
- **Modes:** `engine` = OS threads calling `Db` in-process; `server` = the
  Tokio TCP server in the same process on 127.0.0.1, one closed-loop client
  connection per client; `recovery` = time of `Db::open` after a writer
  stopped (below); `fsync` = **device baseline**: append 100 B to a file +
  `fdatasync`, one thread, no ForgeKV code.
- **Not available:** batched writes (no batch API), open-loop load, skewed
  keys.

## Results: throughput and latency (current code)

Throughput is the median across 5 runs (min–max). Latency cells are the
median across runs of each run's p50 / p95 / p99. "worst max latency" is
the largest single operation in any run. Write stalls: median count per
run (in parentheses: those that began during a compaction), and median
total stall time per 10 s measured phase. All runs: 0 errors, 0 BUSY.

| name | mode | workload | sync | threads/clients | runs | ops/s median (min–max) | GET p50 / p95 / p99 | PUT p50 / p95 / p99 | DELETE p50 / p95 / p99 | worst max latency | write stalls (during compaction), stall time | errors / busy |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| fsync-probe-start | fsync | append 100 B + fdatasync | always | 1 | 5 | 335 (285–426) | - | 3.08 ms / 4.08 ms / 4.67 ms | - | 9.63 ms | 0 (0), 0.00 s | 0 / 0 |
| engine-fillseq-never-c1 | engine | fillseq | never | 1 | 5 | 416920 (414468–425740) | - | 864 ns / 2.0 µs / 3.2 µs | - | 143.96 ms | 41 (41), 3.40 s | 0 / 0 |
| engine-fillrandom-never-c1 | engine | fillrandom | never | 1 | 5 | 383643 (381304–407399) | - | 932 ns / 1.8 µs / 2.9 µs | - | 152.03 ms | 36 (35), 3.97 s | 0 / 0 |
| engine-fillrandom-never-c4 | engine | fillrandom | never | 4 | 5 | 236323 (185663–242003) | - | 14.2 µs / 19.2 µs / 23.9 µs | - | 50.90 ms | 0 (0), 0.00 s | 0 / 0 |
| engine-fillrandom-always-c1 | engine | fillrandom | always | 1 | 5 | 306 (287–744) | - | 3.43 ms / 3.88 ms / 4.20 ms | - | 8.93 ms | 0 (0), 0.00 s | 0 / 0 |
| engine-fillrandom-always-c4 | engine | fillrandom | always | 4 | 5 | 287 (249–306) | - | 13.69 ms / 16.50 ms / 18.35 ms | - | 28.22 ms | 0 (0), 0.00 s | 0 / 0 |
| engine-fillrandom-never-c4-v1000 | engine | fillrandom | never | 4 | 5 | 23673 (22829–23821) | - | 21.5 µs / 35.2 µs / 66.3 µs | - | 687.05 ms | 16 (15), 7.95 s | 0 / 0 |
| engine-readrandom-always-c1 | engine | readrandom | always | 1 | 5 | 669668 (640137–695049) | 1.4 µs / 1.8 µs / 2.3 µs | - | - | 1.03 ms | 0 (0), 0.00 s | 0 / 0 |
| engine-readrandom-always-c4 | engine | readrandom | always | 4 | 5 | 2255094 (1948064–2325036) | 1.7 µs / 2.1 µs / 2.6 µs | - | - | 1.04 ms | 0 (0), 0.00 s | 0 / 0 |
| engine-readrandom-always-c8 | engine | readrandom | always | 8 | 5 | 2822684 (2592780–3149390) | 2.6 µs / 3.9 µs / 4.5 µs | - | - | 6.96 ms | 0 (0), 0.00 s | 0 / 0 |
| engine-readrandom-always-c4-k1M | engine | readrandom | always | 4 | 5 | 1764758 (1691864–1849222) | 2.1 µs / 2.9 µs / 3.6 µs | - | - | 4.05 ms | 0 (0), 0.00 s | 0 / 0 |
| engine-readwrite90-always-c4 | engine | readwrite 90% get | always | 4 | 5 | 2580 (2495–3748) | 11.0 µs / 31.2 µs / 49.1 µs | 15.26 ms / 17.05 ms / 18.33 ms | - | 27.29 ms | 0 (0), 0.00 s | 0 / 0 |
| engine-readwrite90-never-c4 | engine | readwrite 90% get | never | 4 | 5 | 998745 (944297–1040210) | 3.6 µs / 7.7 µs / 10.6 µs | 1.6 µs / 6.0 µs / 10.1 µs | - | 42.80 ms | 0 (0), 0.00 s | 0 / 0 |
| engine-readwrite50-always-c4 | engine | readwrite 50% get | always | 4 | 5 | 529 (506–840) | 15.0 µs / 46.9 µs / 67.5 µs | 14.72 ms / 17.16 ms / 18.97 ms | - | 26.93 ms | 0 (0), 0.00 s | 0 / 0 |
| engine-readwrite50-never-c4 | engine | readwrite 50% get | never | 4 | 5 | 493845 (471098–521487) | 5.4 µs / 11.6 µs / 14.8 µs | 7.8 µs / 15.0 µs / 19.5 µs | - | 60.33 ms | 6 (1), 0.12 s | 0 / 0 |
| engine-deleteheavy-never-c4 | engine | deleteheavy | never | 4 | 5 | 305238 (286445–336770) | 5.5 µs / 13.3 µs / 17.8 µs | 13.3 µs / 20.2 µs / 25.6 µs | 13.2 µs / 20.0 µs / 25.2 µs | 18.20 ms | 0 (0), 0.00 s | 0 / 0 |
| server-readwrite50-never-c1 | server | readwrite 50% get | never | 1 | 5 | 47129 (46169–47808) | 20.4 µs / 37.1 µs / 48.0 µs | 17.3 µs / 33.6 µs / 41.6 µs | - | 19.37 ms | 0 (0), 0.00 s | 0 / 0 |
| server-readwrite50-never-c8 | server | readwrite 50% get | never | 8 | 5 | 160928 (159887–177884) | 41.1 µs / 97.2 µs / 153.0 µs | 39.6 µs / 94.1 µs / 156.2 µs | - | 26.13 ms | 0 (0), 0.00 s | 0 / 0 |
| server-readwrite50-never-c32 | server | readwrite 50% get | never | 32 | 5 | 125746 (122713–135508) | 83.5 µs / 351.1 µs / 540.1 µs | 353.2 µs / 658.5 µs / 1.14 ms | - | 30.38 ms | 0 (0), 0.00 s | 0 / 0 |
| server-readwrite50-always-c8 | server | readwrite 50% get | always | 8 | 5 | 582 (496–1939) | 411.4 µs / 689.1 µs / 814.6 µs | 25.81 ms / 32.68 ms / 33.68 ms | - | 40.78 ms | 0 (0), 0.00 s | 0 / 0 |
| server-readrandom-always-c8 | server | readrandom | always | 8 | 5 | 220159 (218282–221040) | 34.3 µs / 50.6 µs / 94.6 µs | - | - | 2.57 ms | 0 (0), 0.00 s | 0 / 0 |
| fsync-probe-end | fsync | append 100 B + fdatasync | always | 1 | 5 | 296 (261–326) | - | 3.65 ms / 4.15 ms / 4.39 ms | - | 8.68 ms | 0 (0), 0.00 s | 0 / 0 |

## Results: recovery time (current code)

`recovery` writes `keys` random PUTs with `SyncMode::Never`, calls `close`
(which syncs the WAL but does not flush the memtable), then times
`Db::open`: replay every live WAL, write the recovered memtable to an L0
table (fsync), write a new MANIFEST (fsync + rename + directory fsync),
create a new WAL. On disk this is the state a process crash leaves after
the last WAL sync. The page cache is warm, so this measures replay and
rewrite cost, not cold reads.

| name | records written | sync | memtable MiB | WAL MiB before open | SSTs before open | runs | open ms median (min–max) | records replayed | flushed on open |
|---|---|---|---|---|---|---|---|---|---|
| recovery-k100000-m67108864 | 100000 | always | 64.0 | 13.1 | 0 | 5 | 92.2 (90.8–94.6) | 100000 | true |
| recovery-k300000-m67108864 | 300000 | always | 64.0 | 39.2 | 0 | 5 | 312.9 (297.6–317.5) | 300000 | true |
| recovery-k1000000-m4194304 | 1000000 | always | 4.0 | 2.0 | 39 | 5 | 33.3 (27.9–36.7) | 15362 | true |

- With a 64 MiB memtable all writes stay in one WAL: 100,000 records
  (13.1 MiB) open in 92.2 ms and 300,000 records (39.2 MiB) in 312.9 ms,
  roughly linear in WAL size.
- With the default 4 MiB memtable, 1,000,000 writes leave only the last
  memtable (15,362 records, 2.0 MiB of WAL) to replay plus 39 SSTs to open:
  33.3 ms. The memtable size bounds recovery work.

## What the results show

**Durability mode dominates write cost.** With `SyncMode::Always` each PUT
or DELETE returns only after `fdatasync` of the WAL. The device baseline
(`fsync-probe-*`, no ForgeKV code) gives 296–335 syncs/s median, and
single-thread PUT throughput with `Always` is at that bound: 306 ops/s, PUT
p50 3.43 ms. `SyncMode::Never` acknowledges after `write(2)`: 383,643 PUT/s
on one thread (PUT p50 932 ns), about 1,250× more.

**No group commit: more writers do not raise `Always` throughput.** 4
threads: 287 ops/s, the same as 1 thread. Each writer pays its own sync in
turn, so with 4 writers a PUT waits for about 4 syncs: PUT p50 13.69 ms,
p99 18.35 ms.

**Writer waits are now bounded (FIFO writer lock).** The worst single
operation with 4 `Always` writers is 28.22 ms (`fillrandom`), 26.93 ms and
27.29 ms (mixed). Under the Phase 7 code the same configurations had
single writes waiting up to 10.01 s, 2.47 s and 174 ms (comparison below).
The median PUT latency is higher than in Phase 7 (13.69 ms vs 3.47 ms)
because every writer now queues in arrival order instead of a few writers
taking most turns while others starve. Throughput of the mixed `Always`
configurations moved −20% (90% reads) and −10% (50% reads) against Phase
7, but their 5-run ranges overlap (2,495–3,748 vs 2,917–3,614 and 506–840
vs 520–715) and the device's sync time varies between runs, so this is
**not attributed** to the lock change.

**The fairness costs throughput when several writers contend without
fsync.** Each hand-off of the writer lock wakes a sleeping thread. With
`Never` and 4 writer threads: `fillrandom` 236,323 ops/s (Phase 7:
334,418), 50% reads 493,845 (612,788), `deleteheavy` 305,238 (536,164);
through the server with 32 clients 125,746 (196,150). Single-writer
throughput is unchanged within run-to-run spread (383,643 vs 394,289), and
mostly-read mixes are unaffected (90% reads: 998,745 vs 955,921).

**Compaction stalls, now measured.** With one thread writing at full speed
(`Never`), writers waited for memtable room 36–41 times per 10 s and
nearly every wait began during a compaction (`fillrandom`: 36 stalls, 35
during compaction, 3.97 s in total; `fillseq`: 41, all 41, 3.40 s), so no
write could proceed for 34–40% of the measured time. With 1000-byte values
it is 16 stalls (15 during compaction) and 7.95 s of the 10 s, with
single-operation maxima of 687 ms: each compaction rewrites nearly all of
L1 (random keys overlap every L1 file), and the single background worker
cannot flush while it compacts. This is the measured cost of two-level
compaction with one worker. With 4 `Never` writer threads (slower, see
above) the worker kept up: 0 stalls.

**Device fsync latency varies, so `Always` results vary between runs.**
The device's sync time switches between roughly 0.6 ms and 3–4 ms between
runs (Phase 7 found this; the cause, likely device or filesystem power
management, was not identified). In this session the probes stayed mostly
in the slow mode (285–426 and 261–326 syncs/s), while
`engine-fillrandom-always-c1` ranged 287–744 ops/s and
`server-readwrite50-always-c8` 496–1939. **Compare `Always` results only as
ranges.**

**Reads scale with threads.** `readrandom` (dataset in L1 and in the OS
page cache): 669,668 ops/s on 1 thread, 2,255,094 on 4, 2,822,684 on 8, p99
≤ 4.5 µs; with 1,000,000 keys 1,764,758 on 4 threads.

**Server.** One client over loopback: 47,129 ops/s (p50 ≈ 20 µs per round
trip). 8 clients: 160,928; 32 clients: 125,746, where 32 blocking-pool
threads contend for the writer lock (see the fairness cost above) and
share 16 logical CPUs with the clients.

## Phase 7 → Phase 8 comparison (all load configurations)

Same machine, workloads and harness; different code (fingerprints above)
and a different session, so differences include run-to-run and
session-to-session noise (Phase 7's own min–max ranges were 5–20% for most
`Never` configurations and wider for `Always`). The change column is
median vs median.

| name | Phase 7 ops/s median (min–max) | Phase 8 ops/s median (min–max) | change | Phase 7 worst max latency | Phase 8 worst max latency |
|---|---|---|---|---|---|
| fsync-probe-start | 354 (348–829) | 335 (285–426) | -5% | 18.47 ms | 9.63 ms |
| engine-fillseq-never-c1 | 420515 (403649–449909) | 416920 (414468–425740) | -1% | 150.18 ms | 143.96 ms |
| engine-fillrandom-never-c1 | 394289 (381967–430678) | 383643 (381304–407399) | -3% | 179.45 ms | 152.03 ms |
| engine-fillrandom-never-c4 | 334418 (319056–342945) | 236323 (185663–242003) | -29% | 109.46 ms | 50.90 ms |
| engine-fillrandom-always-c1 | 282 (273–317) | 306 (287–744) | +9% | 19.10 ms | 8.93 ms |
| engine-fillrandom-always-c4 | 283 (273–324) | 287 (249–306) | +1% | 10.01 s | 28.22 ms |
| engine-fillrandom-never-c4-v1000 | 23953 (22804–25071) | 23673 (22829–23821) | -1% | 719.18 ms | 687.05 ms |
| engine-readrandom-always-c1 | 648724 (571604–675593) | 669668 (640137–695049) | +3% | 276.5 µs | 1.03 ms |
| engine-readrandom-always-c4 | 2236751 (2145333–2292108) | 2255094 (1948064–2325036) | +1% | 1.77 ms | 1.04 ms |
| engine-readrandom-always-c8 | 2966063 (2872771–3081835) | 2822684 (2592780–3149390) | -5% | 2.71 ms | 6.96 ms |
| engine-readrandom-always-c4-k1M | 1733647 (1531631–1818341) | 1764758 (1691864–1849222) | +2% | 3.03 ms | 4.05 ms |
| engine-readwrite90-always-c4 | 3232 (2917–3614) | 2580 (2495–3748) | -20% | 174.20 ms | 27.29 ms |
| engine-readwrite90-never-c4 | 955921 (927882–981122) | 998745 (944297–1040210) | +4% | 26.32 ms | 42.80 ms |
| engine-readwrite50-always-c4 | 590 (520–715) | 529 (506–840) | -10% | 2.47 s | 26.93 ms |
| engine-readwrite50-never-c4 | 612788 (588507–622581) | 493845 (471098–521487) | -19% | 100.29 ms | 60.33 ms |
| engine-deleteheavy-never-c4 | 536164 (521784–580554) | 305238 (286445–336770) | -43% | 44.80 ms | 18.20 ms |
| server-readwrite50-never-c1 | 36353 (30144–48811) | 47129 (46169–47808) | +30% | 18.91 ms | 19.37 ms |
| server-readwrite50-never-c8 | 178034 (163529–183846) | 160928 (159887–177884) | -10% | 21.69 ms | 26.13 ms |
| server-readwrite50-never-c32 | 196150 (192961–207835) | 125746 (122713–135508) | -36% | 23.68 ms | 30.38 ms |
| server-readwrite50-always-c8 | 512 (498–917) | 582 (496–1939) | +14% | 56.81 ms | 40.78 ms |
| server-readrandom-always-c8 | 221258 (217540–221349) | 220159 (218282–221040) | -0% | 2.86 ms | 2.57 ms |
| fsync-probe-end | 262 (259–537) | 296 (261–326) | +13% | 9.69 ms | 8.68 ms |

## Durability trade-off: `Always` vs `Never`

| | `SyncMode::Always` (default) | `SyncMode::Never` |
|---|---|---|
| Acknowledged after | WAL record written **and `fdatasync` succeeded** | WAL record written to the OS (`write(2)`), no sync |
| Process crash / `kill -9` | No acknowledged write lost (tested: `tests/kill.rs`, `tests/server.rs`, failpoint crash tests) | No acknowledged write lost: the data is in the kernel page cache (same tests) |
| OS crash / power loss | Designed to lose no acknowledged write if the device honours flushes; **not tested** | **Recent acknowledged writes can be lost** (whatever the kernel had not yet written back); the WAL is synced when a memtable is sealed and on `close` |
| Measured here (engine, 100-byte values, current code) | 249–744 PUT/s across all fill runs (device baseline 261–426 syncs/s) | 185,663–425,740 PUT/s across all fill runs |

`Never` is for benchmarks and data that can be recreated.

## Limitations that affect interpretation

- **No power-loss testing.** Durability under `Always` against OS crash or
  power loss is a design property (sync ordering, PLAN.md §2), not a
  measured one.
- **Single background worker and two-level compaction** — measured above:
  up to 80% of the time blocked with 1000-byte values. Write amplification
  grows with dataset size; the 200,000-key dataset (~23 MB of user data) is
  small.
- **No group commit** (`Always` throughput is one sync per write) and **a
  FIFO writer lock** (bounded waits, lower contended `Never` throughput).
- **Reads come from the OS page cache.** Every dataset fits in RAM; there is
  no block cache or Bloom filter. Recovery is measured with a warm cache.
- **Closed-loop clients** understate tail latency under saturation
  (coordinated omission). No open-loop mode.
- **Server mode** runs clients and server in one process on loopback.
- **Machine noise:** a laptop with a desktop session, CPU governor
  `powersave`, no CPU pinning, the bimodal fsync latency, and a higher load
  average at the start of the current session than of the Phase 7 one.
- **Two sessions on one machine.** No cross-machine reproducibility was
  measured.
