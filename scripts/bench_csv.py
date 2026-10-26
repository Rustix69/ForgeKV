#!/usr/bin/env python3
"""Export recorded ForgeKV benchmark sessions to CSV.

Reads the per-configuration JSON files written by `forgekv-bench` in each
session directory (bench-results/<UTC timestamp>/) and writes CSV tables to
bench-results/csv/. Values are copied from the JSON as recorded: latencies
in nanoseconds, throughput in operations per second, no rounding. Nothing
in the session directories is modified.

Usage: scripts/bench_csv.py [SESSION_DIR ...]
       (default: every bench-results/2*/ directory)

Standard library only.
"""

import csv
import glob
import json
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT = os.path.join(ROOT, "bench-results", "csv")
KINDS = ["all", "get", "put", "delete"]
STATS = ["p50", "p95", "p99", "max"]


def load_session(path):
    """All result documents of a session, in file-name (= run) order."""
    docs = []
    for f in sorted(glob.glob(os.path.join(path, "*.json"))):
        with open(f, encoding="utf-8") as fh:
            docs.append(json.load(fh))
    if not docs:
        sys.exit(f"no result files in {path}")
    return docs


def median(xs):
    v = sorted(xs)
    n = len(v)
    return v[n // 2] if n % 2 else (v[n // 2 - 1] + v[n // 2]) / 2


def memtable_bytes(params):
    m = re.search(r"memtable_bytes: (\d+)", params["engine_options"])
    return int(m.group(1)) if m else ""


def write_csv(name, header, rows):
    path = os.path.join(OUT, name)
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh, lineterminator="\n")
        w.writerow(header)
        w.writerows(rows)
    return path, len(rows)


def base_columns(session, doc):
    p = doc["params"]
    return [session, doc["environment"]["source_fingerprint"], p["name"], p["mode"],
            p["workload"], p["read_pct"], p["sync"], p["threads_or_clients"], p["keys"],
            p["key_size"], p["value_size"], p["runs"]]


BASE_HEADER = ["session", "source_fingerprint", "name", "mode", "workload", "read_pct",
               "sync", "threads_or_clients", "keys", "key_size", "value_size", "runs"]


def load_summary(session, docs):
    header = BASE_HEADER + ["ops_per_sec_median", "ops_per_sec_min", "ops_per_sec_max"]
    for k in KINDS:
        header += [f"{k}_{s}_ns_median" for s in STATS]
    header += ["worst_max_ns", "write_stalls_median", "write_stalls_during_compaction_median",
               "write_stall_secs_median", "errors_total", "busy_total"]
    rows = []
    for d in docs:
        if d["params"]["mode"] == "recovery":
            continue
        s = d["summary"]
        row = base_columns(session, d)
        row += [s["ops_per_sec"]["median"], s["ops_per_sec"]["min"], s["ops_per_sec"]["max"]]
        for k in KINDS:
            lat = s["latency_ns"].get(k)
            row += [lat[st]["median"] if lat else "" for st in STATS]
        row.append(max(r["latency_ns"]["all"]["max"] for r in d["runs"] if r["latency_ns"]["all"]))
        ends = [r["end_state"] for r in d["runs"]]
        if all("write_stalls" in e for e in ends):
            row += [median([e["write_stalls"] for e in ends]),
                    median([e["write_stalls_during_compaction"] for e in ends]),
                    median([e["write_stall_secs"] for e in ends])]
        else:  # Phase 7 harness: no stall counters
            row += ["", "", ""]
        row += [s["errors_total"], s["busy_total"]]
        rows.append(row)
    return header, rows


def load_runs(session, docs):
    header = BASE_HEADER + ["run", "preload_secs", "open_secs", "elapsed_secs", "ops_ok",
                            "ops_per_sec", "get_ops", "put_ops", "delete_ops", "get_misses",
                            "errors", "busy"]
    for k in KINDS:
        header += [f"{k}_{s}_ns" for s in ["count", "mean", "min", "p50", "p95", "p99", "p999", "max"]]
    end_keys = ["l0_files", "l1_files", "flushes", "compactions", "write_stalls",
                "write_stalls_during_compaction", "write_stall_secs",
                "max_write_stall_since_open_secs", "dir_bytes"]
    header += [f"end_{k}" for k in end_keys]
    rows = []
    for d in docs:
        if d["params"]["mode"] == "recovery":
            continue
        for r in d["runs"]:
            row = base_columns(session, d)
            row += [r["run"], r["preload_secs"], r["open_secs"], r["elapsed_secs"], r["ops_ok"],
                    r["ops_per_sec"], r["ops"]["get"], r["ops"]["put"], r["ops"]["delete"],
                    r["get_misses"], r["errors"], r["busy"]]
            for k in KINDS:
                lat = r["latency_ns"].get(k)
                row += [lat[f] if lat else "" for f in
                        ["count", "mean", "min", "p50", "p95", "p99", "p999", "max"]]
            row += [r["end_state"].get(k, "") for k in end_keys]
            rows.append(row)
    return header, rows


def recovery_summary(session, docs):
    header = BASE_HEADER + ["memtable_bytes", "wal_files_before", "wal_bytes_before",
                            "sst_files_before", "open_secs_median", "open_secs_min",
                            "open_secs_max", "records_replayed", "flushed_on_open"]
    rows = []
    for d in docs:
        if d["params"]["mode"] != "recovery":
            continue
        first = d["runs"][0]
        s = d["summary"]["open_secs"]
        rows.append(base_columns(session, d) + [
            memtable_bytes(d["params"]), first["wal_files_before"], first["wal_bytes_before"],
            first["sst_files_before"], s["median"], s["min"], s["max"],
            first["records_replayed"], first["flushed_on_open"]])
    return header, rows


def recovery_runs(session, docs):
    keys = ["run", "prepare_secs", "records_written", "wal_files_before", "wal_bytes_before",
            "sst_files_before", "dir_bytes_before", "open_secs", "wal_files_replayed",
            "records_replayed", "flushed_on_open", "l0_files_after", "l1_files_after"]
    header = BASE_HEADER + ["memtable_bytes"] + keys
    rows = []
    for d in docs:
        if d["params"]["mode"] != "recovery":
            continue
        for r in d["runs"]:
            rows.append(base_columns(session, d) + [memtable_bytes(d["params"])] +
                        [r[k] for k in keys])
    return header, rows


def comparison(sessions):
    """Same configurations in two sessions: the earlier vs the later one."""
    (old_id, old), (new_id, new) = sessions[0], sessions[-1]
    old_by = {d["params"]["name"]: d for d in old if d["params"]["mode"] != "recovery"}
    header = ["name", "old_session", "old_source_fingerprint", "old_ops_per_sec_median",
              "old_ops_per_sec_min", "old_ops_per_sec_max", "new_session",
              "new_source_fingerprint", "new_ops_per_sec_median", "new_ops_per_sec_min",
              "new_ops_per_sec_max", "median_change_pct", "old_worst_max_ns", "new_worst_max_ns"]
    rows = []
    for d in new:
        name = d["params"]["name"]
        if d["params"]["mode"] == "recovery" or name not in old_by:
            continue
        o = old_by[name]
        so, sn = o["summary"]["ops_per_sec"], d["summary"]["ops_per_sec"]
        worst = lambda doc: max(r["latency_ns"]["all"]["max"] for r in doc["runs"])
        rows.append([name, old_id, o["environment"]["source_fingerprint"], so["median"], so["min"],
                     so["max"], new_id, d["environment"]["source_fingerprint"], sn["median"],
                     sn["min"], sn["max"], (sn["median"] / so["median"] - 1) * 100,
                     worst(o), worst(d)])
    return f"comparison-{old_id}-vs-{new_id}.csv", header, rows


def main():
    dirs = sys.argv[1:] or sorted(glob.glob(os.path.join(ROOT, "bench-results", "2*")))
    os.makedirs(OUT, exist_ok=True)
    sessions = []
    for d in dirs:
        sid = os.path.basename(os.path.normpath(d))
        docs = load_session(d)
        sessions.append((sid, docs))
        for suffix, fn in [("load-summary", load_summary), ("load-runs", load_runs),
                           ("recovery-summary", recovery_summary),
                           ("recovery-runs", recovery_runs)]:
            path, n = write_csv(f"{sid}-{suffix}.csv", *fn(sid, docs))
            print(f"{os.path.relpath(path, ROOT)}: {n} rows")
    if len(sessions) >= 2:
        name, header, rows = comparison(sessions)
        path, n = write_csv(name, header, rows)
        print(f"{os.path.relpath(path, ROOT)}: {n} rows")


if __name__ == "__main__":
    main()
