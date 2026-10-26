//! ForgeKV benchmark harness.
//!
//! ```text
//! forgekv-bench --mode engine|server|recovery|fsync
//!   [--workload fillseq|fillrandom|readrandom|readwrite|deleteheavy] [--read-pct N]
//!   [--keys N] [--key-size N] [--value-size N] [--threads N | --clients N]
//!   [--warmup-secs F] [--duration-secs F] [--runs N] [--sync always|never]
//!   [--seed N] [--memtable-bytes N] [--max-ops N]
//!   [--dir PATH] [--out DIR] [--name NAME] [--keep]
//! ```
//! Each invocation runs one configuration `--runs` times, each repetition in
//! a fresh directory under `--dir`, and writes every raw per-run result plus
//! median/min/max summaries to `<out>/<UTC timestamp>-<name>.json`. It prints
//! a per-run table and, last, one Markdown summary row (used by
//! `scripts/bench.sh`). Methodology and caveats: `docs/BENCHMARKS.md`.

mod env;
mod json;
mod run;
mod stats;
mod workload;

use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use forgekv::{Options, SyncMode};

use json::J;
use run::{LoadRun, RecoveryRun};
use stats::{Lat, Spread, spread};
use workload::{KINDS, Workload, min_key_size};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Engine,
    Server,
    Recovery,
    /// Device baseline: append + `sync_data` loop, no ForgeKV code.
    Fsync,
}

impl Mode {
    fn name(self) -> &'static str {
        match self {
            Mode::Engine => "engine",
            Mode::Server => "server",
            Mode::Recovery => "recovery",
            Mode::Fsync => "fsync",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Params {
    pub name: String,
    pub mode: Mode,
    pub workload: Workload,
    pub read_pct: u64,
    pub keys: u64,
    pub key_size: usize,
    pub value_size: usize,
    /// Engine threads, or client connections in server mode.
    pub threads: usize,
    pub warmup_secs: f64,
    pub duration_secs: f64,
    pub runs: usize,
    pub sync: SyncMode,
    pub seed: u64,
    pub memtable_bytes: usize,
    /// Upper bound on operations per measured phase (bounds sample memory).
    pub max_ops: u64,
    pub dir: PathBuf,
    pub out: PathBuf,
    pub keep: bool,
}

fn usage() -> String {
    "usage: forgekv-bench --mode engine|server|recovery|fsync [--workload fillseq|fillrandom|readrandom|readwrite|deleteheavy] \
     [--read-pct N] [--keys N] [--key-size N] [--value-size N] [--threads N|--clients N] [--warmup-secs F] \
     [--duration-secs F] [--runs N] [--sync always|never] [--seed N] [--memtable-bytes N] [--max-ops N] \
     [--dir PATH] [--out DIR] [--name NAME] [--keep]"
        .into()
}

fn parse(mut it: impl Iterator<Item = String>) -> Result<Params, String> {
    let defaults = Options::default();
    let mut mode = None;
    let mut workload = Workload::ReadWrite;
    let mut name = None;
    let mut p = Params {
        name: String::new(),
        mode: Mode::Engine,
        workload,
        read_pct: 50,
        keys: 100_000,
        key_size: 16,
        value_size: 100,
        threads: 1,
        warmup_secs: 2.0,
        duration_secs: 10.0,
        runs: 5,
        sync: SyncMode::Always,
        seed: 42,
        memtable_bytes: defaults.memtable_bytes,
        max_ops: 50_000_000,
        dir: PathBuf::from("bench-results/tmp"),
        out: PathBuf::from("bench-results"),
        keep: false,
    };
    while let Some(flag) = it.next() {
        let mut value = || it.next().ok_or_else(|| format!("{flag} needs a value"));
        let int = |v: String| {
            v.parse::<u64>()
                .map_err(|_| format!("{flag}: not an integer: {v}"))
        };
        let secs = |v: String| match v.parse::<f64>() {
            Ok(x) if x.is_finite() && x >= 0.0 => Ok(x),
            _ => Err(format!("{flag}: not a non-negative number: {v}")),
        };
        match flag.as_str() {
            "--mode" => {
                mode = Some(match value()?.as_str() {
                    "engine" => Mode::Engine,
                    "server" => Mode::Server,
                    "recovery" => Mode::Recovery,
                    "fsync" => Mode::Fsync,
                    other => return Err(format!("--mode: unknown {other}")),
                })
            }
            "--workload" => {
                let v = value()?;
                workload = Workload::parse(&v).ok_or_else(|| format!("--workload: unknown {v}"))?;
            }
            "--read-pct" => p.read_pct = int(value()?)?,
            "--keys" => p.keys = int(value()?)?,
            "--key-size" => p.key_size = int(value()?)? as usize,
            "--value-size" => p.value_size = int(value()?)? as usize,
            "--threads" | "--clients" => p.threads = int(value()?)? as usize,
            "--warmup-secs" => p.warmup_secs = secs(value()?)?,
            "--duration-secs" => p.duration_secs = secs(value()?)?,
            "--runs" => p.runs = int(value()?)? as usize,
            "--sync" => {
                p.sync = match value()?.as_str() {
                    "always" => SyncMode::Always,
                    "never" => SyncMode::Never,
                    other => return Err(format!("--sync: expected always|never, got {other}")),
                }
            }
            "--seed" => p.seed = int(value()?)?,
            "--memtable-bytes" => p.memtable_bytes = int(value()?)? as usize,
            "--max-ops" => p.max_ops = int(value()?)?,
            "--dir" => p.dir = PathBuf::from(value()?),
            "--out" => p.out = PathBuf::from(value()?),
            "--name" => name = Some(value()?),
            "--keep" => p.keep = true,
            "--help" | "-h" => return Err(usage()),
            other => return Err(format!("unknown argument {other}\n{}", usage())),
        }
    }
    p.mode = mode.ok_or_else(usage)?;
    p.workload = workload;
    p.name = name.unwrap_or_else(|| default_name(&p));
    validate(&p, &defaults)?;
    Ok(p)
}

fn sync_name(s: SyncMode) -> &'static str {
    match s {
        SyncMode::Always => "always",
        SyncMode::Never => "never",
    }
}

fn default_name(p: &Params) -> String {
    match p.mode {
        Mode::Recovery => format!("recovery-k{}-m{}", p.keys, p.memtable_bytes),
        Mode::Fsync => format!("fsync-probe-v{}", p.value_size),
        m => {
            let mix = match p.workload {
                Workload::ReadWrite => format!("readwrite{}", p.read_pct),
                w => w.name().to_string(),
            };
            format!("{}-{mix}-{}-c{}", m.name(), sync_name(p.sync), p.threads)
        }
    }
}

fn validate(p: &Params, d: &Options) -> Result<(), String> {
    let bad = |m: String| Err(m);
    if p.keys == 0 || p.threads == 0 || p.runs == 0 || p.max_ops == 0 {
        return bad("--keys, --threads, --runs and --max-ops must be > 0".into());
    }
    if p.mode == Mode::Fsync && p.threads != 1 {
        return bad("--mode fsync is single-threaded; use --threads 1".into());
    }
    if p.duration_secs <= 0.0 {
        return bad("--duration-secs must be > 0".into());
    }
    if p.read_pct > 100 {
        return bad("--read-pct must be 0..=100".into());
    }
    if p.key_size < min_key_size(p.keys) || p.key_size > d.max_key_len {
        return bad(format!(
            "--key-size must be in {}..={} for {} keys",
            min_key_size(p.keys),
            d.max_key_len,
            p.keys
        ));
    }
    if p.value_size > d.max_value_len {
        return bad(format!("--value-size must be <= {}", d.max_value_len));
    }
    let o = Options {
        memtable_bytes: p.memtable_bytes,
        ..d.clone()
    };
    o.validate().map_err(|e| format!("--memtable-bytes: {e}"))?;
    if p.name.is_empty() || p.name.contains(['/', '\\']) {
        return bad("--name must be non-empty and contain no path separators".into());
    }
    Ok(())
}

// ------------------------------------------------------------------ output

fn fmt_ns(ns: f64) -> String {
    if ns < 1e3 {
        format!("{ns:.0} ns")
    } else if ns < 1e6 {
        format!("{:.1} µs", ns / 1e3)
    } else if ns < 1e9 {
        format!("{:.2} ms", ns / 1e6)
    } else {
        format!("{:.2} s", ns / 1e9)
    }
}

fn lat_json(l: &Lat) -> J {
    J::obj([
        ("count", J::Int(l.count)),
        ("mean", J::Num(l.mean)),
        ("min", J::Int(l.min)),
        ("p50", J::Int(l.p50)),
        ("p95", J::Int(l.p95)),
        ("p99", J::Int(l.p99)),
        ("p999", J::Int(l.p999)),
        ("max", J::Int(l.max)),
    ])
}

fn spread_json(s: Option<Spread>) -> J {
    J::opt(s, |s| {
        J::obj([
            ("median", J::Num(s.median)),
            ("min", J::Num(s.min)),
            ("max", J::Num(s.max)),
        ])
    })
}

fn load_run_json(i: usize, r: &LoadRun) -> J {
    let mut lat = vec![("all".to_string(), J::opt(r.all.as_ref(), lat_json))];
    for (k, name) in KINDS.iter().enumerate() {
        lat.push((name.to_string(), J::opt(r.lat[k].as_ref(), lat_json)));
    }
    J::obj([
        ("run", J::Int(i as u64)),
        ("preload_secs", J::Num(r.preload_secs)),
        ("open_secs", J::Num(r.open_secs)),
        ("elapsed_secs", J::Num(r.elapsed_secs)),
        ("ops_ok", J::Int(r.ok_total())),
        ("ops_per_sec", J::Num(r.ops_per_sec())),
        (
            "ops",
            J::obj(
                KINDS
                    .iter()
                    .enumerate()
                    .map(|(k, n)| (*n, J::Int(r.ok_ops[k]))),
            ),
        ),
        ("get_misses", J::Int(r.get_misses)),
        ("errors", J::Int(r.errors)),
        ("busy", J::Int(r.busy)),
        ("first_error", J::opt(r.first_error.clone(), J::Str)),
        ("latency_ns", J::Obj(lat)),
        (
            "end_state",
            J::obj([
                ("l0_files", J::Int(r.end.l0_files as u64)),
                ("l1_files", J::Int(r.end.l1_files as u64)),
                ("flushes", J::Int(r.end.flushes)),
                ("compactions", J::Int(r.end.compactions)),
                ("write_stalls", J::Int(r.end.write_stalls)),
                (
                    "write_stalls_during_compaction",
                    J::Int(r.end.write_stalls_during_compaction),
                ),
                ("write_stall_secs", J::Num(r.end.write_stall_secs)),
                (
                    "max_write_stall_since_open_secs",
                    J::Num(r.end.max_write_stall_since_open_secs),
                ),
                ("dir_bytes", J::Int(r.end.dir_bytes)),
            ]),
        ),
    ])
}

fn recovery_run_json(i: usize, r: &RecoveryRun) -> J {
    J::obj([
        ("run", J::Int(i as u64)),
        ("prepare_secs", J::Num(r.prepare_secs)),
        ("records_written", J::Int(r.records_written)),
        ("wal_files_before", J::Int(r.wal_files_before)),
        ("wal_bytes_before", J::Int(r.wal_bytes_before)),
        ("sst_files_before", J::Int(r.sst_files_before)),
        ("dir_bytes_before", J::Int(r.dir_bytes_before)),
        ("open_secs", J::Num(r.open_secs)),
        ("wal_files_replayed", J::Int(r.wal_files_replayed)),
        ("records_replayed", J::Int(r.records_replayed)),
        ("flushed_on_open", J::Bool(r.flushed_on_open)),
        ("l0_files_after", J::Int(r.l0_files_after as u64)),
        ("l1_files_after", J::Int(r.l1_files_after as u64)),
    ])
}

/// Per-run values of one latency field for one op kind (`None` = all ops).
fn lat_field(runs: &[LoadRun], kind: Option<usize>, f: LatField) -> Vec<f64> {
    runs.iter()
        .filter_map(|r| match kind {
            Some(k) => r.lat[k].as_ref(),
            None => r.all.as_ref(),
        })
        .map(|l| f(l) as f64)
        .collect()
}

/// Reads one statistic out of a latency summary.
type LatField = fn(&Lat) -> u64;

const FIELDS: [(&str, LatField); 4] = [
    ("p50", |l| l.p50),
    ("p95", |l| l.p95),
    ("p99", |l| l.p99),
    ("max", |l| l.max),
];

fn load_summary_json(runs: &[LoadRun]) -> J {
    let ops: Vec<f64> = runs.iter().map(LoadRun::ops_per_sec).collect();
    let kinds =
        std::iter::once(("all", None)).chain(KINDS.iter().enumerate().map(|(k, n)| (*n, Some(k))));
    let lat = kinds
        .filter(|(_, k)| !lat_field(runs, *k, |l| l.p50).is_empty())
        .map(|(name, k)| {
            let fields = FIELDS
                .iter()
                .map(|(f, get)| (*f, spread_json(spread(&lat_field(runs, k, *get)))));
            (name, J::obj(fields))
        });
    J::obj([
        (
            "note",
            J::str("median/min/max across runs of each per-run statistic"),
        ),
        ("ops_per_sec", spread_json(spread(&ops))),
        ("latency_ns", J::obj(lat)),
        ("errors_total", J::Int(runs.iter().map(|r| r.errors).sum())),
        ("busy_total", J::Int(runs.iter().map(|r| r.busy).sum())),
    ])
}

fn params_json(p: &Params) -> J {
    J::obj([
        ("name", J::str(p.name.clone())),
        ("mode", J::str(p.mode.name())),
        ("workload", J::str(p.workload.name())),
        ("read_pct", J::Int(p.read_pct)),
        ("keys", J::Int(p.keys)),
        ("key_size", J::Int(p.key_size as u64)),
        ("value_size", J::Int(p.value_size as u64)),
        ("threads_or_clients", J::Int(p.threads as u64)),
        ("warmup_secs", J::Num(p.warmup_secs)),
        ("duration_secs", J::Num(p.duration_secs)),
        ("runs", J::Int(p.runs as u64)),
        ("sync", J::str(sync_name(p.sync))),
        ("seed", J::Int(p.seed)),
        ("max_ops", J::Int(p.max_ops)),
        ("dir", J::str(p.dir.display().to_string())),
        (
            "engine_options",
            J::str(format!(
                "{:?}",
                Options {
                    sync_mode: p.sync,
                    memtable_bytes: p.memtable_bytes,
                    ..Options::default()
                }
            )),
        ),
    ])
}

/// "p50 / p95 / p99" medians across runs, or "-".
fn lat_cell(runs: &[LoadRun], kind: Option<usize>) -> String {
    let med = |f: LatField| spread(&lat_field(runs, kind, f)).map(|s| fmt_ns(s.median));
    match (med(|l| l.p50), med(|l| l.p95), med(|l| l.p99)) {
        (Some(a), Some(b), Some(c)) => format!("{a} / {b} / {c}"),
        _ => "-".into(),
    }
}

fn load_md_row(p: &Params, runs: &[LoadRun]) -> String {
    let ops: Vec<f64> = runs.iter().map(LoadRun::ops_per_sec).collect();
    let o = spread(&ops).unwrap_or(Spread {
        median: 0.0,
        min: 0.0,
        max: 0.0,
    });
    let worst = runs
        .iter()
        .filter_map(|r| r.all.as_ref().map(|l| l.max))
        .max()
        .map_or("-".into(), |m| fmt_ns(m as f64));
    let med = |f: fn(&LoadRun) -> f64| {
        spread(&runs.iter().map(f).collect::<Vec<_>>()).map_or(0.0, |s| s.median)
    };
    let stalls = format!(
        "{:.0} ({:.0}), {:.2} s",
        med(|r| r.end.write_stalls as f64),
        med(|r| r.end.write_stalls_during_compaction as f64),
        med(|r| r.end.write_stall_secs)
    );
    let mix = match (p.mode, p.workload) {
        (Mode::Fsync, _) => format!("append {} B + fdatasync", p.value_size),
        (_, Workload::ReadWrite) => format!("readwrite {}% get", p.read_pct),
        (_, w) => w.name().to_string(),
    };
    format!(
        "| {} | {} | {mix} | {} | {} | {} | {:.0} ({:.0}–{:.0}) | {} | {} | {} | {worst} | {stalls} | {} / {} |",
        p.name,
        p.mode.name(),
        sync_name(p.sync),
        p.threads,
        runs.len(),
        o.median,
        o.min,
        o.max,
        lat_cell(runs, Some(0)),
        lat_cell(runs, Some(1)),
        lat_cell(runs, Some(2)),
        runs.iter().map(|r| r.errors).sum::<u64>(),
        runs.iter().map(|r| r.busy).sum::<u64>(),
    )
}

fn recovery_md_row(p: &Params, runs: &[RecoveryRun]) -> String {
    let open: Vec<f64> = runs.iter().map(|r| r.open_secs * 1e3).collect();
    let o = spread(&open).unwrap_or(Spread {
        median: 0.0,
        min: 0.0,
        max: 0.0,
    });
    let mib = |b: u64| b as f64 / (1024.0 * 1024.0);
    let first = &runs[0];
    format!(
        "| {} | {} | {} | {:.1} | {:.1} | {} | {} | {:.1} ({:.1}–{:.1}) | {} | {} |",
        p.name,
        p.keys,
        sync_name(p.sync),
        mib(p.memtable_bytes as u64),
        mib(first.wal_bytes_before),
        first.sst_files_before,
        runs.len(),
        o.median,
        o.min,
        o.max,
        first.records_replayed,
        first.flushed_on_open,
    )
}

// -------------------------------------------------------------------- main

#[derive(Debug)]
pub enum Outcome {
    Load(Vec<LoadRun>),
    Recovery(Vec<RecoveryRun>),
}

/// Run every repetition, write the JSON file, print the tables; returns the
/// file path and the raw results.
fn bench(p: &Params) -> Result<(PathBuf, Outcome), String> {
    fs::create_dir_all(&p.dir).map_err(|e| format!("create {}: {e}", p.dir.display()))?;
    fs::create_dir_all(&p.out).map_err(|e| format!("create {}: {e}", p.out.display()))?;
    let env = env::collect(&p.dir);
    let mut warnings = vec![];
    if env.ram_fs {
        warnings.push(format!(
            "{} is on a RAM-backed filesystem: fsync is nearly free and results do not reflect a disk",
            p.dir.display()
        ));
    }
    if cfg!(debug_assertions) {
        warnings.push("debug build: use `cargo run --release`".to_string());
    }
    for w in &warnings {
        eprintln!("warning: {w}");
    }
    let (started_at, stamp) = env::utc_now();
    println!("== {} ({} runs)", p.name, p.runs);

    let outcome = match p.mode {
        Mode::Engine | Mode::Server | Mode::Fsync => {
            let mut runs = vec![];
            for i in 0..p.runs {
                let r = match p.mode {
                    Mode::Engine => run::engine(p, i)?,
                    Mode::Server => run::server(p, i)?,
                    _ => run::fsync_probe(p, i)?,
                };
                print_load_run(i, &r);
                runs.push(r);
            }
            Outcome::Load(runs)
        }
        Mode::Recovery => {
            let mut runs = vec![];
            for i in 0..p.runs {
                let r = run::recovery(p, i)?;
                println!(
                    "run {i}: open {:.1} ms, replayed {} records from {} WAL file(s) ({:.1} MiB), {} SST before",
                    r.open_secs * 1e3,
                    r.records_replayed,
                    r.wal_files_replayed,
                    r.wal_bytes_before as f64 / 1048576.0,
                    r.sst_files_before
                );
                runs.push(r);
            }
            Outcome::Recovery(runs)
        }
    };

    let (finished_at, _) = env::utc_now();
    let (runs_json, summary, row) = match &outcome {
        Outcome::Load(runs) => (
            runs.iter()
                .enumerate()
                .map(|(i, r)| load_run_json(i, r))
                .collect(),
            load_summary_json(runs),
            load_md_row(p, runs),
        ),
        Outcome::Recovery(runs) => {
            let open: Vec<f64> = runs.iter().map(|r| r.open_secs).collect();
            (
                runs.iter()
                    .enumerate()
                    .map(|(i, r)| recovery_run_json(i, r))
                    .collect(),
                J::obj([("open_secs", spread_json(spread(&open)))]),
                recovery_md_row(p, runs),
            )
        }
    };
    let doc = J::obj([
        ("schema", J::str("forgekv-bench/1")),
        ("started_at", J::str(started_at)),
        ("finished_at", J::str(finished_at)),
        ("environment", J::Obj(env.fields)),
        ("params", params_json(p)),
        (
            "warnings",
            J::Arr(warnings.into_iter().map(J::Str).collect()),
        ),
        ("runs", J::Arr(runs_json)),
        ("summary", summary),
    ]);
    let path = p.out.join(format!("{stamp}-{}.json", p.name));
    write_new(&path, &doc.render())?;
    println!("{row}");
    Ok((path, outcome))
}

fn print_load_run(i: usize, r: &LoadRun) {
    let mut line = format!(
        "run {i}: {:.0} ops/s ({} ok in {:.2} s)",
        r.ops_per_sec(),
        r.ok_total(),
        r.elapsed_secs
    );
    for (k, name) in KINDS.iter().enumerate() {
        if let Some(l) = &r.lat[k] {
            line += &format!(
                "  {name} p50 {} p95 {} p99 {} max {}",
                fmt_ns(l.p50 as f64),
                fmt_ns(l.p95 as f64),
                fmt_ns(l.p99 as f64),
                fmt_ns(l.max as f64)
            );
        }
    }
    if r.errors + r.busy > 0 {
        line += &format!(
            "  errors {} busy {} ({:?})",
            r.errors, r.busy, r.first_error
        );
    }
    println!("{line}");
}

/// Never overwrite an existing result file.
fn write_new(path: &Path, text: &str) -> Result<(), String> {
    use std::io::Write;
    let mut f = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|e| format!("create {}: {e}", path.display()))?;
    f.write_all(text.as_bytes())
        .and_then(|()| f.sync_all())
        .map_err(|e| format!("write {}: {e}", path.display()))
}

fn main() -> ExitCode {
    let p = match parse(std::env::args().skip(1)) {
        Ok(p) => p,
        Err(msg) => {
            eprintln!("{msg}");
            return ExitCode::from(2);
        }
    };
    match bench(&p) {
        Ok((path, _)) => {
            eprintln!("wrote {}", path.display());
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("forgekv-bench: {e}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(s: &str) -> Result<Params, String> {
        parse(s.split_whitespace().map(String::from))
    }

    #[test]
    fn parses_and_validates_flags() {
        let p = args("--mode server --workload readwrite --read-pct 90 --keys 1000 --clients 8 --sync never --duration-secs 0.5").unwrap();
        assert_eq!(
            (p.mode, p.workload, p.read_pct, p.threads),
            (Mode::Server, Workload::ReadWrite, 90, 8)
        );
        assert_eq!(p.sync, SyncMode::Never);
        assert_eq!(p.duration_secs, 0.5);
        assert_eq!(p.name, "server-readwrite90-never-c8");
        for bad in [
            "",
            "--mode bogus",
            "--mode engine --read-pct 101",
            "--mode engine --keys 1000000 --key-size 3",
            "--mode engine --threads 0",
            "--mode engine --duration-secs 0",
            "--mode engine --memtable-bytes 10",
            "--mode engine --name a/b",
            "--mode engine --workload nope",
            "--mode fsync --threads 2",
        ] {
            assert!(args(bad).is_err(), "{bad:?}");
        }
    }

    /// Smoke run of every mode with tiny parameters: checks the output file
    /// and that no operation failed. Asserts nothing about performance.
    #[test]
    fn smoke_run_every_mode() {
        let tmp = tempfile::TempDir::new().unwrap();
        for (mode, workload) in [
            ("engine", "readwrite"),
            ("engine", "fillseq"),
            ("engine", "deleteheavy"),
            ("server", "readwrite"),
            ("recovery", "readwrite"),
            ("fsync", "readwrite"),
        ] {
            let threads = if mode == "fsync" { 1 } else { 2 };
            let p = args(&format!(
                "--mode {mode} --workload {workload} --keys 300 --threads {threads} --runs 2 \
                 --warmup-secs 0.05 --duration-secs 0.2 --sync never --dir {} --out {}",
                tmp.path().join("data").display(),
                tmp.path().join("out").display()
            ))
            .unwrap();
            let (path, outcome) = bench(&p).unwrap();
            let text = fs::read_to_string(&path).unwrap();
            for field in [
                "\"schema\": \"forgekv-bench/1\"",
                "\"environment\"",
                "\"params\"",
                "\"runs\"",
                "\"summary\"",
            ] {
                assert!(text.contains(field), "{mode}/{workload}: missing {field}");
            }
            match outcome {
                Outcome::Load(runs) => {
                    assert_eq!(runs.len(), 2);
                    for r in &runs {
                        assert_eq!(
                            (r.errors, r.busy),
                            (0, 0),
                            "{mode}/{workload}: {:?}",
                            r.first_error
                        );
                        assert!(r.ok_total() > 0);
                        assert!(r.all.is_some());
                    }
                }
                Outcome::Recovery(runs) => {
                    for r in &runs {
                        assert_eq!(r.records_replayed, 300);
                        assert!(r.flushed_on_open);
                    }
                }
            }
            // Every repetition's data directory was removed.
            assert_eq!(fs::read_dir(tmp.path().join("data")).unwrap().count(), 0);
        }
    }

    #[test]
    fn existing_run_directories_are_never_reused() {
        let tmp = tempfile::TempDir::new().unwrap();
        let p = args(&format!(
            "--mode engine --keys 10 --runs 1 --duration-secs 0.05 --warmup-secs 0 --name x --dir {} --out {}",
            tmp.path().join("data").display(),
            tmp.path().join("out").display()
        ))
        .unwrap();
        fs::create_dir_all(tmp.path().join("data/x-run0")).unwrap();
        fs::write(tmp.path().join("data/x-run0/keep.txt"), b"mine").unwrap();
        assert!(bench(&p).unwrap_err().contains("already exists"));
        assert!(tmp.path().join("data/x-run0/keep.txt").exists());
    }
}
