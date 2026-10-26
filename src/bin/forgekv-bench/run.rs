//! The benchmark modes. Each function performs one complete, independent
//! repetition in a fresh directory: optional untimed preload, open, warm-up
//! (not recorded), measured phase, end-state capture, cleanup.
//!
//! Clients are *closed-loop*: each issues its next operation only after the
//! previous one completed. Under saturation this understates tail latency
//! (coordinated omission); see `docs/BENCHMARKS.md`.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use forgekv::client::{Client, ClientError};
use forgekv::protocol::Response;
use forgekv::server::{ServerConfig, ServerStats, serve};
use forgekv::{Db, Options, Stats, SyncMode};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

use crate::Params;
use crate::stats::{Lat, latency};
use crate::workload::{Gen, Op, Values, Workload, write_key};

/// Engine state when the measured phase ended. Counters are deltas over
/// the measured phase (warm-up excluded), except the longest stall, which
/// the engine only tracks since open.
#[derive(Debug, Clone, Default)]
pub struct EndState {
    pub l0_files: usize,
    pub l1_files: usize,
    pub flushes: u64,
    pub compactions: u64,
    pub write_stalls: u64,
    pub write_stalls_during_compaction: u64,
    pub write_stall_secs: f64,
    pub max_write_stall_since_open_secs: f64,
    pub dir_bytes: u64,
}

/// One repetition of a load (engine or server) benchmark.
#[derive(Debug)]
pub struct LoadRun {
    pub preload_secs: f64,
    pub open_secs: f64,
    pub elapsed_secs: f64,
    /// Successful operations per kind (GET, PUT, DELETE); GET includes misses.
    pub ok_ops: [u64; 3],
    pub get_misses: u64,
    pub errors: u64,
    pub busy: u64,
    pub first_error: Option<String>,
    pub lat: [Option<Lat>; 3],
    pub all: Option<Lat>,
    pub end: EndState,
}

impl LoadRun {
    pub fn ok_total(&self) -> u64 {
        self.ok_ops.iter().sum()
    }

    pub fn ops_per_sec(&self) -> f64 {
        self.ok_total() as f64 / self.elapsed_secs
    }
}

/// One repetition of the recovery benchmark.
#[derive(Debug)]
pub struct RecoveryRun {
    pub prepare_secs: f64,
    pub records_written: u64,
    pub wal_files_before: u64,
    pub wal_bytes_before: u64,
    pub sst_files_before: u64,
    pub dir_bytes_before: u64,
    pub open_secs: f64,
    pub wal_files_replayed: u64,
    pub records_replayed: u64,
    pub flushed_on_open: bool,
    pub l0_files_after: usize,
    pub l1_files_after: usize,
}

// ---------------------------------------------------------------- helpers

fn err(what: &str) -> impl FnOnce(forgekv::Error) -> String + '_ {
    move |e| format!("{what}: {e}")
}

fn db_opts(p: &Params, sync: SyncMode) -> Options {
    Options {
        sync_mode: sync,
        memtable_bytes: p.memtable_bytes,
        ..Options::default()
    }
}

/// A fresh directory for one repetition. Never reuses (or deletes) a
/// directory this run did not create.
fn run_dir(p: &Params, run: usize) -> Result<PathBuf, String> {
    let dir = p.dir.join(format!("{}-run{run}", p.name));
    if dir.exists() {
        return Err(format!(
            "{} already exists; remove it or choose another --dir/--name",
            dir.display()
        ));
    }
    fs::create_dir_all(&dir).map_err(|e| format!("create {}: {e}", dir.display()))?;
    Ok(dir)
}

fn cleanup(p: &Params, dir: &Path) -> Result<(), String> {
    if p.keep {
        return Ok(());
    }
    fs::remove_dir_all(dir).map_err(|e| format!("remove {}: {e}", dir.display()))
}

/// (count, bytes) of the directory's files whose name satisfies `f`.
fn files(dir: &Path, f: impl Fn(&str) -> bool) -> Result<(u64, u64), String> {
    let mut n = 0;
    let mut bytes = 0;
    let entries = fs::read_dir(dir).map_err(|e| format!("list {}: {e}", dir.display()))?;
    for e in entries {
        let e = e.map_err(|e| format!("list {}: {e}", dir.display()))?;
        if f(&e.file_name().to_string_lossy()) {
            n += 1;
            bytes += e.metadata().map_err(|e| format!("stat: {e}"))?.len();
        }
    }
    Ok((n, bytes))
}

fn end_state(db: &Db, dir: &Path, before: &Stats) -> Result<EndState, String> {
    let levels = db.level_summary().map_err(err("level summary"))?;
    let s = db.stats().map_err(err("stats"))?;
    Ok(EndState {
        l0_files: levels.l0.len(),
        l1_files: levels.l1.len(),
        flushes: s.flushes - before.flushes,
        compactions: s.compactions - before.compactions,
        write_stalls: s.write_stalls - before.write_stalls,
        write_stalls_during_compaction: s.write_stalls_during_compaction
            - before.write_stalls_during_compaction,
        write_stall_secs: (s.write_stall_time - before.write_stall_time).as_secs_f64(),
        max_write_stall_since_open_secs: s.max_write_stall.as_secs_f64(),
        dir_bytes: files(dir, |_| true)?.1,
    })
}

fn gens(p: &Params) -> Vec<Gen> {
    (0..p.threads as u64)
        .map(|c| Gen::new(p.workload, p.read_pct, p.keys, p.seed, c, p.threads as u64))
        .collect()
}

/// Load keys `0..keys` in order (untimed for the measurement, but reported),
/// then flush and compact so the measured phase starts from a dataset fully
/// in L1. Uses `SyncMode::Never`; the final close syncs the WAL.
fn preload(p: &Params, dir: &Path) -> Result<f64, String> {
    let t = Instant::now();
    let db = Db::open(dir, db_opts(p, SyncMode::Never)).map_err(err("open for preload"))?;
    let values = Values::new(p.value_size, p.seed);
    let mut key = vec![0u8; p.key_size];
    for i in 0..p.keys {
        write_key(&mut key, i);
        db.put(&key, values.get(i)).map_err(err("preload put"))?;
    }
    db.flush().map_err(err("preload flush"))?;
    db.compact().map_err(err("preload compact"))?;
    db.close().map_err(err("close after preload"))?;
    Ok(t.elapsed().as_secs_f64())
}

// ------------------------------------------------------- per-op accounting

enum Outcome {
    Ok,
    Miss,
    Busy,
    Failed(String),
    /// The connection is gone; the client stops.
    Fatal(String),
}

#[derive(Default)]
struct Phase {
    lat: [Vec<u64>; 3],
    ok: [u64; 3],
    misses: u64,
    errors: u64,
    busy: u64,
    first_error: Option<String>,
}

impl Phase {
    fn attempts(&self) -> u64 {
        self.ok.iter().sum::<u64>() + self.errors + self.busy
    }

    fn note(&mut self, kind: usize, elapsed: Duration, record: bool, o: Outcome) {
        match o {
            Outcome::Ok | Outcome::Miss => {
                self.ok[kind] += 1;
                self.misses += u64::from(matches!(o, Outcome::Miss));
                if record {
                    self.lat[kind].push(elapsed.as_nanos().min(u128::from(u64::MAX)) as u64);
                }
            }
            Outcome::Busy => self.busy += 1,
            Outcome::Failed(m) | Outcome::Fatal(m) => {
                self.errors += 1;
                self.first_error.get_or_insert(m);
            }
        }
    }

    fn merge(&mut self, o: Phase) {
        for k in 0..3 {
            self.lat[k].extend(o.lat[k].iter());
            self.ok[k] += o.ok[k];
        }
        self.misses += o.misses;
        self.errors += o.errors;
        self.busy += o.busy;
        if self.first_error.is_none() {
            self.first_error = o.first_error;
        }
    }

    fn failed(msg: String) -> Phase {
        Phase {
            errors: 1,
            first_error: Some(msg),
            ..Phase::default()
        }
    }

    fn into_run(
        self,
        preload_secs: f64,
        open_secs: f64,
        elapsed_secs: f64,
        end: EndState,
    ) -> LoadRun {
        let all = latency(self.lat.iter().flatten().copied().collect());
        let [g, pu, d] = self.lat;
        LoadRun {
            preload_secs,
            open_secs,
            elapsed_secs,
            ok_ops: self.ok,
            get_misses: self.misses,
            errors: self.errors,
            busy: self.busy,
            first_error: self.first_error,
            lat: [latency(g), latency(pu), latency(d)],
            all,
            end,
        }
    }
}

struct Window {
    deadline: Instant,
    /// Per-client operation cap (bounds latency-sample memory).
    cap: u64,
    record: bool,
}

fn window(p: &Params, secs: f64, record: bool) -> Window {
    Window {
        deadline: Instant::now() + Duration::from_secs_f64(secs),
        cap: (p.max_ops / p.threads as u64).max(1),
        record,
    }
}

// ------------------------------------------------------------- engine mode

fn engine_worker(db: &Db, g: &mut Gen, values: &Values, key_size: usize, w: &Window) -> Phase {
    let mut ph = Phase::default();
    let mut key = vec![0u8; key_size];
    while ph.attempts() < w.cap {
        let op = g.next_op();
        write_key(&mut key, op.key());
        let t0 = Instant::now();
        let outcome = match op {
            Op::Get(_) => match db.get(&key) {
                Ok(Some(_)) => Outcome::Ok,
                Ok(None) => Outcome::Miss,
                Err(e) => Outcome::Failed(e.to_string()),
            },
            Op::Put(k) => db
                .put(&key, values.get(k))
                .map_or_else(|e| Outcome::Failed(e.to_string()), |()| Outcome::Ok),
            Op::Delete(_) => db
                .delete(&key)
                .map_or_else(|e| Outcome::Failed(e.to_string()), |()| Outcome::Ok),
        };
        let t1 = Instant::now();
        ph.note(op.kind(), t1 - t0, w.record, outcome);
        if t1 >= w.deadline {
            break;
        }
    }
    ph
}

/// Run every generator on its own OS thread until the window closes.
fn engine_phase(
    db: &Db,
    gens: &mut [Gen],
    values: &Values,
    p: &Params,
    w: &Window,
) -> (Phase, f64) {
    let start = Instant::now();
    let parts: Vec<Phase> = std::thread::scope(|s| {
        let handles: Vec<_> = gens
            .iter_mut()
            .map(|g| s.spawn(move || engine_worker(db, g, values, p.key_size, w)))
            .collect();
        handles
            .into_iter()
            .map(|h| {
                h.join()
                    .unwrap_or_else(|_| Phase::failed("benchmark thread panicked".into()))
            })
            .collect()
    });
    let elapsed = start.elapsed().as_secs_f64();
    let mut ph = Phase::default();
    for part in parts {
        ph.merge(part);
    }
    (ph, elapsed)
}

/// In-process engine benchmark: `threads` OS threads call `Db` directly.
pub fn engine(p: &Params, run: usize) -> Result<LoadRun, String> {
    let dir = run_dir(p, run)?;
    let preload_secs = if p.workload.needs_preload() {
        preload(p, &dir)?
    } else {
        0.0
    };
    let t = Instant::now();
    let db = Db::open(&dir, db_opts(p, p.sync)).map_err(err("open"))?;
    let open_secs = t.elapsed().as_secs_f64();
    let values = Values::new(p.value_size, p.seed);
    let mut gens = gens(p);
    engine_phase(&db, &mut gens, &values, p, &window(p, p.warmup_secs, false));
    let before = db.stats().map_err(err("stats"))?;
    let (ph, elapsed) = engine_phase(
        &db,
        &mut gens,
        &values,
        p,
        &window(p, p.duration_secs, true),
    );
    let end = end_state(&db, &dir, &before)?;
    db.close().map_err(err("close"))?;
    cleanup(p, &dir)?;
    Ok(ph.into_run(preload_secs, open_secs, elapsed, end))
}

// ------------------------------------------------------------- server mode

fn classify(e: ClientError) -> Outcome {
    match e {
        ClientError::Status(Response::Busy(_)) => Outcome::Busy,
        ClientError::Io(e) => Outcome::Fatal(format!("connection: {e}")),
        other => Outcome::Failed(other.to_string()),
    }
}

async fn client_worker(
    mut c: Client,
    mut g: Gen,
    values: Arc<Values>,
    key_size: usize,
    w: Arc<Window>,
) -> (Client, Gen, Phase) {
    let mut ph = Phase::default();
    let mut key = vec![0u8; key_size];
    while ph.attempts() < w.cap {
        let op = g.next_op();
        write_key(&mut key, op.key());
        let t0 = Instant::now();
        let outcome = match op {
            Op::Get(_) => match c.get(&key).await {
                Ok(Some(_)) => Outcome::Ok,
                Ok(None) => Outcome::Miss,
                Err(e) => classify(e),
            },
            Op::Put(k) => c
                .put(&key, values.get(k))
                .await
                .map_or_else(classify, |()| Outcome::Ok),
            Op::Delete(_) => c.delete(&key).await.map_or_else(classify, |()| Outcome::Ok),
        };
        let t1 = Instant::now();
        let fatal = matches!(outcome, Outcome::Fatal(_));
        ph.note(op.kind(), t1 - t0, w.record, outcome);
        if fatal || t1 >= w.deadline {
            break;
        }
    }
    (c, g, ph)
}

type Conn = (Client, Gen);

async fn client_phase(
    clients: Vec<Conn>,
    values: &Arc<Values>,
    p: &Params,
    w: Window,
) -> (Vec<Conn>, Phase, f64) {
    let w = Arc::new(w);
    let start = Instant::now();
    let tasks: Vec<_> = clients
        .into_iter()
        .map(|(c, g)| {
            tokio::spawn(client_worker(
                c,
                g,
                Arc::clone(values),
                p.key_size,
                Arc::clone(&w),
            ))
        })
        .collect();
    let mut out = Vec::with_capacity(tasks.len());
    let mut ph = Phase::default();
    for t in tasks {
        match t.await {
            Ok((c, g, part)) => {
                ph.merge(part);
                out.push((c, g));
            }
            Err(e) => ph.merge(Phase::failed(format!("client task failed: {e}"))),
        }
    }
    (out, ph, start.elapsed().as_secs_f64())
}

/// End-to-end benchmark through the TCP server, which runs in the same
/// process on 127.0.0.1 (clients and server share the CPU). `threads` is
/// the number of client connections, each closed-loop.
pub fn server(p: &Params, run: usize) -> Result<LoadRun, String> {
    let dir = run_dir(p, run)?;
    let preload_secs = if p.workload.needs_preload() {
        preload(p, &dir)?
    } else {
        0.0
    };
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("tokio runtime: {e}"))?;
    let result = rt.block_on(server_async(p, &dir, preload_secs))?;
    cleanup(p, &dir)?;
    Ok(result)
}

async fn server_async(p: &Params, dir: &Path, preload_secs: f64) -> Result<LoadRun, String> {
    let t = Instant::now();
    let (d, opts) = (dir.to_path_buf(), db_opts(p, p.sync));
    let db = tokio::task::spawn_blocking(move || Db::open(d, opts))
        .await
        .map_err(|e| format!("open task: {e}"))?
        .map_err(err("open"))?;
    let open_secs = t.elapsed().as_secs_f64();
    let db = Arc::new(db);
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|e| format!("bind: {e}"))?;
    let addr = listener
        .local_addr()
        .map_err(|e| format!("local addr: {e}"))?;
    // Admission limits sized so they never reject this benchmark's clients:
    // the run measures the engine and the protocol, not BUSY handling.
    let cfg = ServerConfig {
        max_connections: p.threads + 16,
        max_inflight_ops: ServerConfig::default().max_inflight_ops.max(p.threads),
        ..ServerConfig::default()
    };
    let (stop_tx, stop_rx) = oneshot::channel::<()>();
    let server = tokio::spawn(serve(
        listener,
        Arc::clone(&db),
        cfg,
        Arc::new(ServerStats::default()),
        async {
            let _ = stop_rx.await;
        },
    ));

    let values = Arc::new(Values::new(p.value_size, p.seed));
    let mut clients = Vec::with_capacity(p.threads);
    for g in gens(p) {
        let mut c = Client::connect(addr)
            .await
            .map_err(|e| format!("connect: {e}"))?;
        c.set_max_value(p.value_size);
        clients.push((c, g));
    }
    let (clients, _, _) = client_phase(clients, &values, p, window(p, p.warmup_secs, false)).await;
    let before = db.stats().map_err(err("stats"))?;
    let (clients, ph, elapsed) =
        client_phase(clients, &values, p, window(p, p.duration_secs, true)).await;
    drop(clients);

    let (db2, d) = (Arc::clone(&db), dir.to_path_buf());
    let end = tokio::task::spawn_blocking(move || end_state(&db2, &d, &before))
        .await
        .map_err(|e| format!("end state task: {e}"))??;
    let _ = stop_tx.send(());
    server
        .await
        .map_err(|e| format!("server task: {e}"))?
        .map_err(err("server shutdown"))?;
    Ok(ph.into_run(preload_secs, open_secs, elapsed, end))
}

// ----------------------------------------------------------- recovery mode

/// Time `Db::open` on a directory left by a writer that stopped without
/// flushing: `keys` random PUTs (`SyncMode::Never`), then `close`, which
/// syncs the WAL but leaves memtable contents only in the WAL -- the same
/// on-disk state as a process crash after the last sync. The page cache is
/// warm (the files were just written).
pub fn recovery(p: &Params, run: usize) -> Result<RecoveryRun, String> {
    let dir = run_dir(p, run)?;
    let t = Instant::now();
    {
        let db = Db::open(&dir, db_opts(p, SyncMode::Never)).map_err(err("open for prepare"))?;
        let values = Values::new(p.value_size, p.seed);
        let mut g = Gen::new(Workload::FillRandom, 0, p.keys, p.seed, 0, 1);
        let mut key = vec![0u8; p.key_size];
        for _ in 0..p.keys {
            let k = g.next_op().key();
            write_key(&mut key, k);
            db.put(&key, values.get(k)).map_err(err("prepare put"))?;
        }
        db.close().map_err(err("close after prepare"))?;
    }
    let prepare_secs = t.elapsed().as_secs_f64();
    let is_wal = |n: &str| n.starts_with("wal-") && n.ends_with(".log");
    let (wal_files_before, wal_bytes_before) = files(&dir, is_wal)?;
    let (sst_files_before, _) = files(&dir, |n| n.ends_with(".sst"))?;
    let (_, dir_bytes_before) = files(&dir, |_| true)?;

    let t = Instant::now();
    let db = Db::open(&dir, db_opts(p, p.sync)).map_err(err("recovery open"))?;
    let open_secs = t.elapsed().as_secs_f64();
    let report = db.recovery_report().clone();
    let levels = db.level_summary().map_err(err("level summary"))?;
    db.close().map_err(err("close"))?;
    cleanup(p, &dir)?;
    Ok(RecoveryRun {
        prepare_secs,
        records_written: p.keys,
        wal_files_before,
        wal_bytes_before,
        sst_files_before,
        dir_bytes_before,
        open_secs,
        wal_files_replayed: report.wal_files_replayed,
        records_replayed: report.records_replayed,
        flushed_on_open: report.flushed_on_open,
        l0_files_after: levels.l0.len(),
        l1_files_after: levels.l1.len(),
    })
}

// -------------------------------------------------------------- fsync probe

/// Device baseline with no ForgeKV code: append `value_size` bytes to a file
/// and `sync_data` it, in a loop on one thread. A `SyncMode::Always` write
/// pays at least this. Reported as PUT latency.
pub fn fsync_probe(p: &Params, run: usize) -> Result<LoadRun, String> {
    let dir = run_dir(p, run)?;
    let path = dir.join("probe.log");
    let io = |e: std::io::Error| format!("{}: {e}", path.display());
    let mut f = fs::File::create(&path).map_err(io)?;
    let buf = Values::new(p.value_size, p.seed).get(0).to_vec();
    let mut measured = (Phase::default(), 0.0);
    for (secs, record) in [(p.warmup_secs, false), (p.duration_secs, true)] {
        let w = window(p, secs, record);
        let mut ph = Phase::default();
        let start = Instant::now();
        while ph.attempts() < w.cap {
            let t0 = Instant::now();
            f.write_all(&buf).and_then(|()| f.sync_data()).map_err(io)?;
            let t1 = Instant::now();
            ph.note(1, t1 - t0, record, Outcome::Ok);
            if t1 >= w.deadline {
                break;
            }
        }
        measured = (ph, start.elapsed().as_secs_f64());
    }
    let dir_bytes = files(&dir, |_| true)?.1;
    drop(f);
    cleanup(p, &dir)?;
    let end = EndState {
        dir_bytes,
        ..EndState::default()
    };
    Ok(measured.0.into_run(0.0, 0.0, measured.1, end))
}
