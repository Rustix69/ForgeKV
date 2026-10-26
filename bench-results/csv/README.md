# Benchmark results as CSV

CSV exports of the two recorded benchmark sessions, generated from their
JSON result files by [`scripts/bench_csv.py`](../../scripts/bench_csv.py):

```bash
scripts/bench_csv.py          # rewrites every file in this folder
scripts/bench_charts.py --png # redraws docs/images/ from these CSVs (PNGs need Chrome)
```

The JSON files in `bench-results/<session>/` remain the primary record;
these CSVs copy their values without rounding and add nothing. Method,
environment and caveats: [`docs/BENCHMARKS.md`](../../docs/BENCHMARKS.md).

| Session | Code measured |
|---|---|
| `20261026T160449Z` | Phase 7 source, `crc32:0f6f6c76` (historical) |
| `20261026T173115Z` | Phase 8 source, `crc32:bbce3c06` (current results) |

## Files

| File | One row per | Content |
|---|---|---|
| `<session>-load-summary.csv` | engine/server/fsync configuration | the summary table: ops/s median, min and max across runs; per operation kind (`all`, `get`, `put`, `delete`) the median across runs of each run's p50, p95, p99 and max; the largest single latency in any run (`worst_max_ns`); median write-stall statistics; total errors and BUSY responses |
| `<session>-load-runs.csv` | run (5 per configuration) | every raw per-run statistic: ops, ops/s, elapsed time, latency count/mean/min/p50/p95/p99/p999/max per kind, engine state at the end of the run |
| `<session>-recovery-summary.csv` | recovery configuration | records written, memtable size, WAL and SST state before open, `Db::open` time median/min/max, records replayed |
| `<session>-recovery-runs.csv` | recovery run | every raw per-run recovery statistic |
| `comparison-20261026T160449Z-vs-20261026T173115Z.csv` | configuration present in both sessions | ops/s median/min/max in each session, `median_change_pct` (exact medians, not rounded), worst single latency in each |

## Units and conventions

- `*_ns` columns are nanoseconds; `*_secs` columns are seconds;
  `ops_per_sec` is successful operations per second; `*_bytes` are bytes.
- Percentiles are exact nearest-rank percentiles of every recorded
  operation in a run; summary columns are medians across the 5 runs.
- An empty cell means the value does not exist: no operations of that
  kind in the workload, or a counter the harness did not record yet.
- **Phase 7 session:** its harness had no write-stall counters, so those
  columns are empty, and its `end_flushes`/`end_compactions` count from
  database open, including the 3 s warm-up. The Phase 8 session counts
  the measured phase only.
- `source_fingerprint` identifies the code that was measured; the current
  source has not been re-benchmarked (see `docs/BENCHMARKS.md`).
