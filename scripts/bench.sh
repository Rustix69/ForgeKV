#!/usr/bin/env bash
# Phase 7 benchmark matrix (see docs/BENCHMARKS.md).
#
# Builds forgekv-bench in release mode and runs every configuration below,
# each `--runs 5` times in fresh directories under bench-results/tmp. Raw
# results go to bench-results/<UTC timestamp>/ (one JSON per configuration),
# with log.txt (full output) and summary.md (one row per configuration,
# including FAILED rows). Nothing is skipped or filtered.
#
# Usage: scripts/bench.sh [extra forgekv-bench flags, e.g. --runs 1 --duration-secs 2]
set -uo pipefail
cd "$(dirname "$0")/.."

cargo build --release --bin forgekv-bench || exit 1
B=target/release/forgekv-bench
SESSION="bench-results/$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$SESSION" bench-results/tmp
LOG="$SESSION/log.txt"
COMMON=(--runs 5 --warmup-secs 3 --duration-secs 10 --seed 42 --key-size 16 --value-size 100
        --dir bench-results/tmp --out "$SESSION")

LOAD_ROWS="$SESSION/.load-rows"
REC_ROWS="$SESSION/.recovery-rows"
: > "$LOAD_ROWS"
: > "$REC_ROWS"

# run <rows-file> <flags...>
run() {
    local rows=$1
    shift
    echo "+ $B ${COMMON[*]} $*" | tee -a "$LOG"
    local out
    if out=$("$B" "${COMMON[@]}" "$@" 2>>"$LOG"); then
        printf '%s\n' "$out" >> "$LOG"
        printf '%s\n' "$out" | grep '^| ' >> "$rows"
    else
        printf '%s\n' "$out" >> "$LOG"
        echo "| FAILED: $* (see log.txt) |" >> "$rows"
    fi
}

K=200000
E=(--mode engine --keys $K)
S=(--mode server --keys $K)

# Device baseline (no ForgeKV code): append 100 B + fdatasync. Run first and
# last so drift in device fsync latency during the session is visible.
run "$LOAD_ROWS" --mode fsync --threads 1 --name fsync-probe-start "$@"
# Engine: write-only.
run "$LOAD_ROWS" "${E[@]}" --workload fillseq    --sync never  --threads 1 "$@"
run "$LOAD_ROWS" "${E[@]}" --workload fillrandom --sync never  --threads 1 "$@"
run "$LOAD_ROWS" "${E[@]}" --workload fillrandom --sync never  --threads 4 "$@"
run "$LOAD_ROWS" "${E[@]}" --workload fillrandom --sync always --threads 1 "$@"
run "$LOAD_ROWS" "${E[@]}" --workload fillrandom --sync always --threads 4 "$@"
run "$LOAD_ROWS" "${E[@]}" --workload fillrandom --sync never  --threads 4 --value-size 1000 --name engine-fillrandom-never-c4-v1000 "$@"
# Engine: read-only (sync mode does not affect reads; `always` is the default).
run "$LOAD_ROWS" "${E[@]}" --workload readrandom --sync always --threads 1 "$@"
run "$LOAD_ROWS" "${E[@]}" --workload readrandom --sync always --threads 4 "$@"
run "$LOAD_ROWS" "${E[@]}" --workload readrandom --sync always --threads 8 "$@"
run "$LOAD_ROWS" --mode engine --keys 1000000 --workload readrandom --sync always --threads 4 --name engine-readrandom-always-c4-k1M "$@"
# Engine: mixed.
run "$LOAD_ROWS" "${E[@]}" --workload readwrite --read-pct 90 --sync always --threads 4 "$@"
run "$LOAD_ROWS" "${E[@]}" --workload readwrite --read-pct 90 --sync never  --threads 4 "$@"
run "$LOAD_ROWS" "${E[@]}" --workload readwrite --read-pct 50 --sync always --threads 4 "$@"
run "$LOAD_ROWS" "${E[@]}" --workload readwrite --read-pct 50 --sync never  --threads 4 "$@"
run "$LOAD_ROWS" "${E[@]}" --workload deleteheavy --sync never --threads 4 "$@"
# Server (in-process TCP server on 127.0.0.1, closed-loop clients).
run "$LOAD_ROWS" "${S[@]}" --workload readwrite --read-pct 50 --sync never  --clients 1 "$@"
run "$LOAD_ROWS" "${S[@]}" --workload readwrite --read-pct 50 --sync never  --clients 8 "$@"
run "$LOAD_ROWS" "${S[@]}" --workload readwrite --read-pct 50 --sync never  --clients 32 "$@"
run "$LOAD_ROWS" "${S[@]}" --workload readwrite --read-pct 50 --sync always --clients 8 "$@"
run "$LOAD_ROWS" "${S[@]}" --workload readrandom --sync always --clients 8 "$@"
# Recovery: WAL-only (64 MiB memtable) at two sizes, and an SST-heavy database.
run "$REC_ROWS" --mode recovery --keys 100000  --memtable-bytes 67108864 --sync always "$@"
run "$REC_ROWS" --mode recovery --keys 300000  --memtable-bytes 67108864 --sync always "$@"
run "$REC_ROWS" --mode recovery --keys 1000000 --sync always "$@"
run "$LOAD_ROWS" --mode fsync --threads 1 --name fsync-probe-end "$@"

{
    echo "# Benchmark session $(basename "$SESSION")"
    echo
    echo "Raw results: \`$SESSION/*.json\`; full output: \`$LOG\`."
    echo "Latency cells are the median across runs of each run's p50 / p95 / p99."
    echo "Throughput is the median across runs (min–max in parentheses)."
    echo "Write stalls (waits for memtable room; in parentheses those that began during a"
    echo "compaction) and total stall time are medians across runs of the measured phase."
    echo
    echo "| name | mode | workload | sync | threads/clients | runs | ops/s median (min–max) | GET p50 / p95 / p99 | PUT p50 / p95 / p99 | DELETE p50 / p95 / p99 | worst max latency | write stalls (during compaction), stall time | errors / busy |"
    echo "|---|---|---|---|---|---|---|---|---|---|---|---|---|"
    cat "$LOAD_ROWS"
    echo
    echo "| name | records written | sync | memtable MiB | WAL MiB before open | SSTs before open | runs | open ms median (min–max) | records replayed | flushed on open |"
    echo "|---|---|---|---|---|---|---|---|---|---|"
    cat "$REC_ROWS"
} > "$SESSION/summary.md"
rm -f "$LOAD_ROWS" "$REC_ROWS"
echo "session: $SESSION"
