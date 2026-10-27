//! The public key-value API and the coordination of WAL, memtables,
//! SSTables, manifest and the background worker.
//!
//! # Guarantee of a successful write
//! `put`/`delete` return `Ok` only after the WAL record has been fully
//! written and, under `SyncMode::Always` (default), `sync_data` succeeded.
//! The write is applied to the memtable after the WAL, so a reader never
//! sees a value that is not in the log.
//!
//! An `Err` means the write was not acknowledged. If it was returned before
//! the record was logged (invalid argument, `Closed`, `Poisoned`, or a
//! failure while waiting for memtable room) the write was not applied. If
//! the WAL write or sync itself failed, the outcome is *unknown*: the record
//! may be partly or wholly in the log, so it is not visible now (the engine
//! is poisoned) but recovery may replay it.
//!
//! # State and locking (Phase 5)
//! - `snap: RwLock<Arc<Snapshot>>` -- the readable state: active memtable,
//!   optional immutable memtable (`imm`), and the current `Version` of
//!   SSTables. Readers take the read lock only to clone the `Arc`, then read
//!   without any engine lock. Every state change replaces the whole snapshot
//!   at once, so a reader sees either the old or the new state, never a mix
//!   (no "read gap" while data moves from memtable to L0 or from L0 to L1).
//! - `write: FairMutex<Writer>` -- serializes writers: WAL order equals
//!   memtable order. Memtable switches happen under this lock. It is a FIFO
//!   ticket lock (`fairlock.rs`): writers enter in arrival order. With
//!   `std::sync::Mutex` a writer looping on fsync could re-take the lock
//!   ahead of waiting writers and starve them for seconds (measured in
//!   Phase 7, fixed in Phase 8).
//! - `bg: Mutex<Bg>` + `cv` -- background job bookkeeping, failure and
//!   shutdown flags; writers wait here for room (backpressure).
//! - `vset: Mutex<VersionSet>` -- the MANIFEST appender; used by the single
//!   background worker (and by `open` before the worker starts).
//!
//! Lock order (a lock may only be taken while holding those to its left):
//! `write` -> `bg` -> `snap` and `vset` -> `snap`. `snap` is never held
//! while taking another lock, and the worker never takes `write`.
//!
//! # Write path and backpressure
//! Under `write`: if adding the entry would overflow the active memtable,
//! either *switch* (seal and sync the current WAL, create a new WAL, make the
//! active memtable immutable, start an empty one) or -- if an immutable
//! memtable is still being flushed -- wait for the worker. Memory is
//! therefore bounded by two memtables (`2 * memtable_bytes`). Then the record
//! is appended to the WAL (and synced) and inserted into the memtable.
//!
//! # Background worker
//! One thread runs, in priority order: a compaction when L0 has at least
//! `l0_compaction_trigger` tables (so L0 never exceeds the trigger), a flush
//! of the immutable memtable, and explicitly requested compactions. A flush
//! writes the immutable memtable to an L0 table (fsync file + directory),
//! appends `{AddFile, SetLogNumber, SetNextFileNumber}` to the MANIFEST
//! (fsync: the commit point), swaps in a snapshot without the immutable
//! memtable, and deletes its WAL. Compaction is as described in
//! `compaction.rs`; its inputs are marked obsolete after the commit and
//! deleted when the last snapshot using them is dropped.
//!
//! # Recovery (`Db::open`)
//! Read-only analysis first: replay the MANIFEST, open and check every live
//! SSTable, list the directory, replay every live WAL (number >=
//! `log_number`) in order. Only the newest WAL may end in an incomplete
//! record; older (sealed) WALs must be complete. Then: delete orphans; flush
//! the recovered memtable to L0; write a fresh MANIFEST snapshot naming a new
//! WAL number; create that WAL; delete the replayed WALs; compact if L0 is at
//! the trigger; start the worker.
//!
//! # Failures and shutdown
//! Any error while writing the WAL, switching memtables, or in a background
//! job *poisons* the engine: writes return `Error::Poisoned` (the call that
//! hit or waited on the failure gets the original error) until the database
//! is reopened. Reads keep working on the last installed snapshot.
//! `shutdown`/`close` stop accepting operations, wake blocked writers
//! (`Error::Closed`), let the worker finish its current job, join it, and
//! sync the WAL. An unflushed immutable memtable stays in its WAL and is
//! recovered on the next open.

use std::fs::File;
use std::io::{BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, RwLock};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::compaction::{self, CompactionStats};
use crate::config::{Options, RecoveryMode};
use crate::error::{Error, Result};
use crate::failpoint::FailPoints;
use crate::fairlock::FairMutex;
use crate::fs::{
    DirLock, FileKind, MANIFEST_FILE, list_dir, remove_file, sst_path, sync_dir, wal_path,
};
use crate::manifest::{self, FileMeta, Manifest, ManifestState, VersionEdit};
use crate::memtable::{Entry, Memtable};
use crate::sstable::SstBuilder;
use crate::version::{DeleteErrors, Table, Version};
use crate::wal::{self, ReplayEnd, Wal, WalRecord};

/// What recovery found and did when the database was opened.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RecoveryReport {
    pub wal_files_replayed: u64,
    pub records_replayed: u64,
    /// Bytes ignored at the end of the newest WAL (incomplete record, or the
    /// region dropped by `SalvagePrefix`).
    pub discarded_bytes: u64,
    /// Bytes of an incomplete final MANIFEST record that were ignored.
    pub manifest_discarded_bytes: u64,
    /// `SalvagePrefix` dropped an invalid record; describes why.
    pub salvaged: Option<String>,
    /// The replayed WAL data was written to a new L0 table during open.
    pub flushed_on_open: bool,
    /// Unreferenced files deleted during open.
    pub orphans_removed: u64,
}

/// Files per level, for tests and introspection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LevelSummary {
    /// Newest first.
    pub l0: Vec<FileMeta>,
    /// Sorted by key range.
    pub l1: Vec<FileMeta>,
    /// Current `approx_bytes` of the active memtable.
    pub memtable_bytes: usize,
    /// `approx_bytes` of the immutable memtable waiting to be flushed.
    pub immutable_memtable_bytes: Option<usize>,
}

/// Write and sync position of the active WAL (feature `failpoints`, for
/// tests that simulate losing unsynced data).
#[cfg(feature = "failpoints")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalOffsets {
    /// File number of the active WAL (`wal-<number>.log`).
    pub wal_number: u64,
    /// End of the last fully written record.
    pub written: u64,
    /// End of the data covered by the last successful sync. Bytes between
    /// `synced` and `written` are not guaranteed to survive a power loss.
    pub synced: u64,
}

/// Counters and errors that cannot be returned from the call that caused them.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Stats {
    pub flushes: u64,
    pub compactions: u64,
    /// Obsolete table files whose deletion failed (they become orphans and
    /// are removed on the next open).
    pub obsolete_delete_failures: u64,
    pub last_delete_error: Option<String>,
    /// The error that poisoned the engine, if any.
    pub background_error: Option<String>,
    /// Times a writer had to wait for memtable room (both memtables full).
    pub write_stalls: u64,
    /// Total and longest time writers spent in those waits.
    pub write_stall_time: Duration,
    pub max_write_stall: Duration,
    /// Stalls that began while the background worker was running a
    /// compaction (rather than flushing or idle).
    pub write_stalls_during_compaction: u64,
    /// Writers waiting for memtable room right now (a gauge).
    pub writers_stalled_now: u64,
}

struct Imm {
    mem: Arc<Memtable>,
    /// WAL holding exactly this memtable's records.
    wal_number: u64,
    /// The WAL started when this memtable was sealed: the new `log_number`
    /// once this memtable is flushed.
    next_wal_number: u64,
    seq: u64,
}

struct Snapshot {
    mem: Arc<Memtable>,
    imm: Option<Arc<Imm>>,
    version: Arc<Version>,
}

struct Writer {
    wal: Wal<File>,
    wal_number: u64,
}

struct VersionSet {
    manifest: Manifest,
    log_number: u64,
}

#[derive(Default)]
struct Bg {
    shutdown: bool,
    failed: bool,
    failure_message: Option<String>,
    failure_injected: Option<&'static str>,
    busy: bool,
    /// The current job is a compaction.
    compacting: bool,
    flush_seq_assigned: u64,
    flush_seq_done: u64,
    compact_requested: u64,
    compact_done: u64,
    last_compaction: Option<CompactionStats>,
    flushes: u64,
    compactions: u64,
    write_stalls: u64,
    write_stall_time: Duration,
    max_write_stall: Duration,
    write_stalls_during_compaction: u64,
    writers_stalled_now: u64,
}

impl Bg {
    fn imm_pending(&self) -> bool {
        self.flush_seq_assigned > self.flush_seq_done
    }

    /// Error for a caller that was waiting on the failed operation.
    fn failure(&self) -> Error {
        match self.failure_injected {
            Some(name) => Error::Injected(name),
            None => Error::Poisoned,
        }
    }
}

enum Job {
    Flush,
    Compact { ticket: Option<u64> },
    Exit,
}

enum JobDone {
    Flushed(u64),
    Compacted(Option<u64>, Option<CompactionStats>),
}

struct Inner {
    dir: PathBuf,
    opts: Options,
    fp: FailPoints,
    write: FairMutex<Writer>,
    snap: RwLock<Arc<Snapshot>>,
    vset: Mutex<VersionSet>,
    bg: Mutex<Bg>,
    cv: Condvar,
    next_file: AtomicU64,
    closed: AtomicBool,
    failed: AtomicBool,
    delete_errors: Arc<DeleteErrors>,
    /// Held for the lifetime of the database; dropping it releases the lock.
    /// Must stay the **last** field: fields drop in declaration order, so the
    /// directory is unlocked only after every other resource is released.
    _lock: DirLock,
}

pub struct Db {
    inner: Arc<Inner>,
    worker: Mutex<Option<JoinHandle<()>>>,
    report: RecoveryReport,
}

fn lock<T>(m: &Mutex<T>) -> Result<MutexGuard<'_, T>> {
    // A poisoned std mutex means a thread panicked inside a critical
    // section; treat the engine as poisoned rather than trusting its state.
    m.lock().map_err(|_| Error::Poisoned)
}

impl Db {
    /// Open (creating if necessary) the database in `dir` and recover it.
    pub fn open(dir: impl AsRef<Path>, opts: Options) -> Result<Db> {
        Self::open_inner(dir.as_ref(), opts, FailPoints::new())
    }

    /// Like [`Db::open`], with a failpoint registry armed by the caller.
    #[cfg(feature = "failpoints")]
    pub fn open_with_failpoints(
        dir: impl AsRef<Path>,
        opts: Options,
        fp: FailPoints,
    ) -> Result<Db> {
        Self::open_inner(dir.as_ref(), opts, fp)
    }

    /// This database's failpoint registry.
    #[cfg(feature = "failpoints")]
    pub fn failpoints(&self) -> &FailPoints {
        &self.inner.fp
    }

    /// The active WAL's written and synced offsets (feature `failpoints`).
    ///
    /// # Errors
    /// `Error::Poisoned` if a writer panicked while holding the writer lock.
    #[cfg(feature = "failpoints")]
    pub fn wal_offsets(&self) -> Result<WalOffsets> {
        let w = self.inner.write.lock()?;
        Ok(WalOffsets {
            wal_number: w.wal_number,
            written: w.wal.good_end(),
            synced: w.wal.synced_end(),
        })
    }

    fn open_inner(dir: &Path, opts: Options, fp: FailPoints) -> Result<Db> {
        opts.validate()?;
        if !dir.exists() {
            std::fs::create_dir_all(dir).map_err(|e| Error::io("create db dir", dir, e))?;
            if let Some(parent) = dir.parent().filter(|p| !p.as_os_str().is_empty()) {
                sync_dir(parent)?;
            }
        }
        let lock_file = DirLock::acquire(dir)?;
        let mut report = RecoveryReport::default();
        let delete_errors = Arc::new(DeleteErrors::default());

        // ---- read-only analysis -------------------------------------------
        let manifest_path = dir.join(MANIFEST_FILE);
        let exists = manifest_path
            .try_exists()
            .map_err(|e| Error::io("stat manifest", &manifest_path, e))?;
        let mstate = if exists {
            let r = manifest::replay(&manifest_path)?;
            report.manifest_discarded_bytes = r.discarded_bytes;
            r.state
        } else {
            let data_files = list_dir(dir)?
                .iter()
                .any(|(k, _)| matches!(k, FileKind::Sst(_) | FileKind::Wal(_)));
            if data_files {
                return Err(Error::corruption(
                    &manifest_path,
                    0,
                    "MANIFEST is missing but the directory contains data files",
                ));
            }
            // Mark the directory as a database before creating anything else.
            let st = ManifestState::new_db();
            Manifest::create(dir, &st.snapshot(), opts.manifest_max_bytes, &fp)?;
            st
        };
        let version = Version::open(dir, &mstate, &delete_errors)?;
        let listing = list_dir(dir)?;
        let mut live_wals: Vec<(u64, PathBuf)> = listing
            .iter()
            .filter_map(|(k, p)| match k {
                FileKind::Wal(n) if *n >= mstate.log_number => Some((*n, p.clone())),
                _ => None,
            })
            .collect();
        live_wals.sort();
        let mem = Memtable::new();
        for (i, (_, path)) in live_wals.iter().enumerate() {
            let newest = i + 1 == live_wals.len();
            replay_wal(path, newest, &opts, &mem, &mut report)?;
        }

        // ---- mutations ------------------------------------------------------
        let max_seen = listing
            .iter()
            .filter_map(|(k, _)| match k {
                FileKind::Sst(n) | FileKind::Wal(n) => Some(*n),
                _ => None,
            })
            .max()
            .unwrap_or(0);
        let mut next = mstate.next_file_number.max(max_seen.saturating_add(1));
        for (kind, path) in &listing {
            let orphan = match kind {
                FileKind::Sst(n) => !version.contains(*n),
                FileKind::Wal(n) => *n < mstate.log_number,
                FileKind::Temp => true,
                _ => false,
            };
            if orphan {
                remove_file(path)?;
                report.orphans_removed += 1;
            }
        }

        let mut version = version;
        if !mem.is_empty() {
            let file_no = next;
            next += 1;
            let table = write_memtable(dir, file_no, &mem, &opts, &delete_errors)?;
            sync_dir(dir)?;
            fail_point!(fp, "open:after_flush_sst");
            version = version.with_new_l0(table);
            report.flushed_on_open = true;
        }
        let wal_number = next;
        next += 1;
        let manifest = Manifest::create(
            dir,
            &snapshot_edits(&version, wal_number, next),
            opts.manifest_max_bytes,
            &fp,
        )?;
        // Committed. The new WAL is created only now, so a torn WAL replayed
        // above can never end up "sealed" behind a newer WAL.
        let wal = Wal::create(&wal_path(dir, wal_number), &opts)?;
        fail_point!(fp, "open:after_new_wal");
        for (_, path) in &live_wals {
            remove_file(path)?;
        }

        let inner = Arc::new(Inner {
            dir: dir.to_path_buf(),
            opts,
            fp,
            write: FairMutex::new(Writer { wal, wal_number }),
            snap: RwLock::new(Arc::new(Snapshot {
                mem: Arc::new(Memtable::new()),
                imm: None,
                version: Arc::new(version),
            })),
            vset: Mutex::new(VersionSet {
                manifest,
                log_number: wal_number,
            }),
            bg: Mutex::new(Bg::default()),
            cv: Condvar::new(),
            next_file: AtomicU64::new(next),
            closed: AtomicBool::new(false),
            failed: AtomicBool::new(false),
            delete_errors,
            _lock: lock_file,
        });
        // Compact synchronously before the worker exists, so a database that
        // was closed with a full L0 is healthy as soon as `open` returns.
        if inner.snapshot()?.version.l0.len() >= inner.opts.l0_compaction_trigger {
            inner.compact()?;
        }
        let worker_inner = Arc::clone(&inner);
        let handle = std::thread::Builder::new()
            .name("forgekv-bg".into())
            .spawn(move || worker_inner.worker())
            .map_err(|e| Error::io("spawn background worker", dir, e))?;
        Ok(Db {
            inner,
            worker: Mutex::new(Some(handle)),
            report,
        })
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.check_key(key)?;
        if value.len() > self.inner.opts.max_value_len {
            return Err(Error::InvalidArgument(format!(
                "value length {} exceeds max {}",
                value.len(),
                self.inner.opts.max_value_len
            )));
        }
        self.inner.write(key, Some(value))
    }

    /// Delete `key`. A tombstone is recorded even if the key does not exist.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.check_key(key)?;
        self.inner.write(key, None)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.check_key(key)?;
        self.inner.check_open()?;
        let snap = self.inner.snapshot()?;
        fail_point!(self.inner.fp, "read:after_snapshot");
        let mut found = snap.mem.get(key);
        if found.is_none()
            && let Some(imm) = &snap.imm
        {
            found = imm.mem.get(key);
        }
        if found.is_none() {
            found = snap.version.get(key)?;
        }
        Ok(match found {
            Some(Entry::Value(v)) => Some(v),
            Some(Entry::Tombstone) | None => None,
        })
    }

    /// Flush the memtable to a new L0 table and wait until that is committed.
    /// No-op if the memtable is empty (beyond waiting for a pending flush).
    pub fn flush(&self) -> Result<()> {
        self.inner.flush()
    }

    /// Ask the worker to compact all of L0 (plus overlapping L1) into L1 and
    /// wait for it. Returns `None` if L0 was empty. Does not flush first.
    pub fn compact(&self) -> Result<Option<CompactionStats>> {
        self.inner.check_writable()?;
        let ticket = {
            let mut bg = lock(&self.inner.bg)?;
            bg.compact_requested += 1;
            bg.compact_requested
        };
        self.inner.cv.notify_all();
        let bg = self.inner.wait_bg(|bg| bg.compact_done >= ticket)?;
        Ok(bg.last_compaction.clone())
    }

    /// Stop accepting operations, wake blocked writers, wait for the
    /// background worker to finish its current job, and sync the WAL (even
    /// under `SyncMode::Never`). Later calls return `Error::Closed`. Safe to
    /// call more than once and from any thread holding a reference.
    pub fn shutdown(&self) -> Result<()> {
        self.inner.closed.store(true, Ordering::SeqCst);
        {
            let mut bg = lock(&self.inner.bg)?;
            bg.shutdown = true;
        }
        self.inner.cv.notify_all();
        let handle = lock(&self.worker)?.take();
        // A panicked worker has already poisoned the engine (see
        // `WorkerPanicGuard`); still sync the WAL below.
        let panicked = handle.is_some_and(|h| h.join().is_err());
        let synced = self.inner.write.lock()?.wal.sync();
        if panicked || self.inner.failed.load(Ordering::SeqCst) {
            return Err(Error::Poisoned);
        }
        synced
    }

    /// [`Db::shutdown`], consuming the handle. Use this (not drop) to learn
    /// about a final sync error.
    pub fn close(self) -> Result<()> {
        self.shutdown()
    }

    pub fn options(&self) -> &Options {
        &self.inner.opts
    }

    pub fn recovery_report(&self) -> &RecoveryReport {
        &self.report
    }

    pub fn stats(&self) -> Result<Stats> {
        let bg = lock(&self.inner.bg)?;
        Ok(Stats {
            flushes: bg.flushes,
            compactions: bg.compactions,
            obsolete_delete_failures: self.inner.delete_errors.count(),
            last_delete_error: self.inner.delete_errors.last(),
            background_error: bg.failure_message.clone(),
            write_stalls: bg.write_stalls,
            write_stall_time: bg.write_stall_time,
            max_write_stall: bg.max_write_stall,
            write_stalls_during_compaction: bg.write_stalls_during_compaction,
            writers_stalled_now: bg.writers_stalled_now,
        })
    }

    pub fn level_summary(&self) -> Result<LevelSummary> {
        let snap = self.inner.snapshot()?;
        Ok(LevelSummary {
            l0: snap.version.l0.iter().map(|t| t.meta.clone()).collect(),
            l1: snap.version.l1.iter().map(|t| t.meta.clone()).collect(),
            memtable_bytes: snap.mem.approx_bytes(),
            immutable_memtable_bytes: snap.imm.as_ref().map(|i| i.mem.approx_bytes()),
        })
    }

    /// Expensive self-check for tests, meant for quiescent moments (no
    /// concurrent writers): waits for the background worker to go idle, then
    /// checks version invariants, every block of every table, and that the
    /// directory holds no unreferenced data files.
    pub fn check_invariants(&self) -> Result<()> {
        let inner = &self.inner;
        let trigger = inner.opts.l0_compaction_trigger;
        drop(inner.wait_bg(|bg| {
            !bg.busy
                && !bg.imm_pending()
                && bg.compact_done >= bg.compact_requested
                && inner.snapshot().is_ok_and(|s| s.version.l0.len() < trigger)
        })?);
        let snap = inner.snapshot()?;
        let bad = |d: String| Error::corruption(&inner.dir, 0, d);
        snap.version.check_invariants().map_err(bad)?;
        for (_, t) in snap.version.tables() {
            t.reader.verify()?;
        }
        let wal_number = inner.write.lock()?.wal_number;
        for (kind, path) in list_dir(&inner.dir)? {
            let stray = match kind {
                FileKind::Sst(n) => !snap.version.contains(n),
                FileKind::Wal(n) => n != wal_number,
                FileKind::Temp => true,
                _ => false,
            };
            if stray {
                return Err(bad(format!("unexpected file {}", path.display())));
            }
        }
        Ok(())
    }

    fn check_key(&self, key: &[u8]) -> Result<()> {
        if key.is_empty() || key.len() > self.inner.opts.max_key_len {
            return Err(Error::InvalidArgument(format!(
                "key length {} outside 1..={}",
                key.len(),
                self.inner.opts.max_key_len
            )));
        }
        Ok(())
    }
}

impl Drop for Db {
    /// Best-effort shutdown so the worker thread and directory lock are
    /// released. `drop` cannot return an error, so a failure is reported on
    /// stderr; call `close` to handle it instead. If `shutdown` or `close`
    /// already ran, its caller received any error, so nothing is printed.
    /// Acknowledged writes do not depend on this: they are already in the WAL.
    fn drop(&mut self) {
        let already_shut_down = self.inner.closed.load(Ordering::SeqCst);
        if let Err(e) = self.shutdown()
            && !already_shut_down
        {
            // Not `eprintln!`, which panics if stderr fails (e.g. a closed
            // pipe): a diagnostic must never panic out of `drop`.
            let _ = writeln!(
                std::io::stderr(),
                "forgekv: shutdown during drop of the database at {} failed: {e} \
                 (call Db::close() to handle this error)",
                self.inner.dir.display()
            );
        }
    }
}

impl Inner {
    fn snapshot(&self) -> Result<Arc<Snapshot>> {
        self.snap
            .read()
            .map(|g| Arc::clone(&g))
            .map_err(|_| Error::Poisoned)
    }

    /// Replace the snapshot with `f(current)`. Callers must not hold `snap`.
    fn install(&self, f: impl FnOnce(&Snapshot) -> Snapshot) -> Result<()> {
        let mut g = self.snap.write().map_err(|_| Error::Poisoned)?;
        let next = f(&g);
        *g = Arc::new(next);
        Ok(())
    }

    fn alloc_file_number(&self) -> u64 {
        self.next_file.fetch_add(1, Ordering::SeqCst)
    }

    fn check_open(&self) -> Result<()> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(Error::Closed);
        }
        Ok(())
    }

    fn check_writable(&self) -> Result<()> {
        self.check_open()?;
        if self.failed.load(Ordering::SeqCst) {
            return Err(Error::Poisoned);
        }
        Ok(())
    }

    /// Record `e` as the error that poisoned the engine, wake every waiter,
    /// and hand `e` back for the caller to return.
    fn fail(&self, e: Error) -> Error {
        let injected = match e {
            Error::Injected(name) => Some(name),
            _ => None,
        };
        self.record_failure(e.to_string(), injected);
        e
    }

    fn record_failure(&self, message: String, injected: Option<&'static str>) {
        self.failed.store(true, Ordering::SeqCst);
        // If `bg` is poisoned, waiters see that themselves when they wake.
        if let Ok(mut bg) = self.bg.lock() {
            if !bg.failed {
                bg.failure_message = Some(message);
                bg.failure_injected = injected;
            }
            bg.failed = true;
        }
        self.cv.notify_all();
    }

    /// Block until `done(bg)`, or fail with the background error / `Closed`.
    fn wait_bg(&self, mut done: impl FnMut(&Bg) -> bool) -> Result<MutexGuard<'_, Bg>> {
        let mut bg = lock(&self.bg)?;
        loop {
            if done(&bg) {
                return Ok(bg);
            }
            if bg.failed {
                return Err(bg.failure());
            }
            if bg.shutdown {
                return Err(Error::Closed);
            }
            bg = self.cv.wait(bg).map_err(|_| Error::Poisoned)?;
        }
    }

    fn write(&self, key: &[u8], value: Option<&[u8]>) -> Result<()> {
        let mut w = self.write.lock()?;
        self.check_writable()?;
        let cost = Memtable::entry_cost(key.len(), value.map_or(0, <[u8]>::len));
        self.make_room(&mut w, cost)?;
        let mode = self.opts.sync_mode;
        let logged = match value {
            Some(v) => w.wal.append_put(key, v, mode),
            None => w.wal.append_delete(key, mode),
        };
        if let Err(e) = logged.and_then(|()| self.after_wal_hook()) {
            return Err(self.fail(e));
        }
        // `write` is held, so this is the memtable the record belongs to.
        let snap = self.snapshot()?;
        match value {
            Some(v) => snap.mem.put(key, v),
            None => snap.mem.delete(key),
        }
        Ok(())
    }

    /// Failpoint `write:after_wal`: the record is fully logged but the call
    /// fails, as when `sync_data` fails after a complete write.
    fn after_wal_hook(&self) -> Result<()> {
        fail_point!(self.fp, "write:after_wal");
        Ok(())
    }

    /// Ensure the active memtable can take `cost` more bytes: switch to a
    /// fresh memtable, or wait for the worker to flush the immutable one.
    fn make_room(&self, w: &mut Writer, cost: usize) -> Result<()> {
        loop {
            let snap = self.snapshot()?;
            if snap.mem.is_empty() || snap.mem.approx_bytes() + cost <= self.opts.memtable_bytes {
                return Ok(());
            }
            if snap.imm.is_none() {
                drop(snap);
                return self
                    .switch_memtable(w)
                    .map(|_| ())
                    .map_err(|e| self.fail(e));
            }
            drop(snap);
            // Backpressure: two memtables are full.
            self.stall_until_flushed()?;
        }
    }

    /// Wait for the immutable memtable to be flushed, recording the wait in
    /// the stall statistics.
    fn stall_until_flushed(&self) -> Result<()> {
        let during_compaction = {
            let mut bg = lock(&self.bg)?;
            if !bg.imm_pending() {
                return Ok(());
            }
            bg.writers_stalled_now += 1;
            bg.compacting
        };
        let started = Instant::now();
        let waited = self.wait_bg(|bg| !bg.imm_pending());
        let mut bg = match waited {
            Ok(bg) => bg,
            Err(e) => {
                if let Ok(mut bg) = lock(&self.bg) {
                    bg.writers_stalled_now -= 1;
                }
                return Err(e);
            }
        };
        let waited = started.elapsed();
        bg.writers_stalled_now -= 1;
        bg.write_stalls += 1;
        bg.write_stall_time += waited;
        bg.max_write_stall = bg.max_write_stall.max(waited);
        bg.write_stalls_during_compaction += u64::from(during_compaction);
        Ok(())
    }

    /// Seal the current WAL and memtable and start new ones. Requires that
    /// there is no immutable memtable. Returns the flush sequence number.
    fn switch_memtable(&self, w: &mut Writer) -> Result<u64> {
        w.wal.sync()?;
        let new_no = self.alloc_file_number();
        let new_wal = Wal::create(&wal_path(&self.dir, new_no), &self.opts)?;
        fail_point!(self.fp, "switch:after_new_wal");
        let old_no = std::mem::replace(&mut w.wal_number, new_no);
        w.wal = new_wal;
        let seq = {
            let mut bg = lock(&self.bg)?;
            let seq = bg.flush_seq_assigned + 1;
            self.install(|cur| Snapshot {
                mem: Arc::new(Memtable::new()),
                imm: Some(Arc::new(Imm {
                    mem: Arc::clone(&cur.mem),
                    wal_number: old_no,
                    next_wal_number: new_no,
                    seq,
                })),
                version: Arc::clone(&cur.version),
            })?;
            bg.flush_seq_assigned = seq;
            seq
        };
        self.cv.notify_all();
        Ok(seq)
    }

    fn flush(&self) -> Result<()> {
        let target = {
            let mut w = self.write.lock()?;
            self.check_writable()?;
            loop {
                let snap = self.snapshot()?;
                if snap.mem.is_empty() {
                    break lock(&self.bg)?.flush_seq_assigned;
                }
                if snap.imm.is_none() {
                    drop(snap);
                    break self.switch_memtable(&mut w).map_err(|e| self.fail(e))?;
                }
                drop(snap);
                drop(self.wait_bg(|bg| !bg.imm_pending())?);
            }
        };
        drop(self.wait_bg(|bg| bg.flush_seq_done >= target)?);
        Ok(())
    }

    // ---- background worker ------------------------------------------------

    fn worker(&self) {
        let _guard = WorkerPanicGuard(self);
        loop {
            let job = match self.next_job() {
                Ok(j) => j,
                Err(e) => {
                    self.fail(e);
                    return;
                }
            };
            let result = match job {
                Job::Exit => return,
                Job::Flush => self.flush_imm().map(JobDone::Flushed),
                Job::Compact { ticket } => self.compact().map(|s| JobDone::Compacted(ticket, s)),
            };
            // `result` holds no table references, so obsolete inputs of a
            // finished compaction are already unlinked unless a reader still
            // uses them.
            let mut bg = match lock(&self.bg) {
                Ok(bg) => bg,
                Err(e) => {
                    self.fail(e);
                    return;
                }
            };
            bg.busy = false;
            bg.compacting = false;
            match result {
                Ok(JobDone::Flushed(seq)) => {
                    bg.flush_seq_done = seq;
                    bg.flushes += 1;
                }
                Ok(JobDone::Compacted(ticket, stats)) => {
                    if let Some(t) = ticket {
                        bg.compact_done = t;
                        bg.last_compaction = stats.clone();
                    }
                    if stats.is_some() {
                        bg.compactions += 1;
                    }
                }
                Err(e) => {
                    drop(bg);
                    self.fail(e);
                    return;
                }
            }
            drop(bg);
            self.cv.notify_all();
        }
    }

    fn next_job(&self) -> Result<Job> {
        let mut bg = lock(&self.bg)?;
        loop {
            if bg.failed || bg.shutdown {
                return Ok(Job::Exit);
            }
            let l0 = self.snapshot()?.version.l0.len();
            let job = if l0 >= self.opts.l0_compaction_trigger {
                // Compaction first keeps L0 at or below the trigger.
                Some(Job::Compact { ticket: None })
            } else if bg.imm_pending() {
                Some(Job::Flush)
            } else if bg.compact_requested > bg.compact_done {
                Some(Job::Compact {
                    ticket: Some(bg.compact_requested),
                })
            } else {
                None
            };
            if let Some(job) = job {
                bg.busy = true;
                bg.compacting = matches!(job, Job::Compact { .. });
                return Ok(job);
            }
            bg = self.cv.wait(bg).map_err(|_| Error::Poisoned)?;
        }
    }

    /// Flush the immutable memtable; returns its sequence number.
    fn flush_imm(&self) -> Result<u64> {
        let Some(imm) = self.snapshot()?.imm.clone() else {
            return Err(Error::corruption(
                &self.dir,
                0,
                "internal: flush scheduled without an immutable memtable",
            ));
        };
        let sst_no = self.alloc_file_number();
        let table = write_memtable(&self.dir, sst_no, &imm.mem, &self.opts, &self.delete_errors)?;
        sync_dir(&self.dir)?;
        fail_point!(self.fp, "flush:before_manifest");
        {
            let mut vs = lock(&self.vset)?;
            vs.manifest.append(&[
                VersionEdit::AddFile {
                    level: 0,
                    meta: table.meta.clone(),
                },
                VersionEdit::SetLogNumber(imm.next_wal_number),
                VersionEdit::SetNextFileNumber(self.next_file.load(Ordering::SeqCst)),
            ])?;
            vs.log_number = imm.next_wal_number;
        }
        fail_point!(self.fp, "flush:after_manifest");
        self.install(|cur| Snapshot {
            mem: Arc::clone(&cur.mem),
            imm: None,
            version: Arc::new(cur.version.with_new_l0(table)),
        })?;
        remove_file(&wal_path(&self.dir, imm.wal_number))?;
        self.maybe_rewrite_manifest()?;
        Ok(imm.seq)
    }

    /// Compact L0 (+ overlapping L1); see `compaction.rs` and the module docs.
    /// Only the worker (or `open`, before the worker starts) calls this, so
    /// the version cannot change underneath it.
    fn compact(&self) -> Result<Option<CompactionStats>> {
        let Some(picked) = compaction::pick(&self.snapshot()?.version) else {
            return Ok(None);
        };
        let (outputs, stats) = compaction::run(
            &self.dir,
            &self.opts,
            &picked,
            || self.alloc_file_number(),
            &self.fp,
            &self.delete_errors,
        )?;
        sync_dir(&self.dir)?;
        fail_point!(self.fp, "compaction:before_manifest");

        let mut edits: Vec<VersionEdit> = outputs
            .iter()
            .map(|t| VersionEdit::AddFile {
                level: 1,
                meta: t.meta.clone(),
            })
            .collect();
        for (level, tables) in [(0u8, &picked.l0), (1u8, &picked.l1)] {
            for t in tables {
                edits.push(VersionEdit::DeleteFile {
                    level,
                    file_no: t.meta.file_no,
                });
            }
        }
        edits.push(VersionEdit::SetNextFileNumber(
            self.next_file.load(Ordering::SeqCst),
        ));
        lock(&self.vset)?.manifest.append(&edits)?;
        fail_point!(self.fp, "compaction:after_manifest");

        let removed = picked.input_file_numbers();
        self.install(|cur| Snapshot {
            mem: Arc::clone(&cur.mem),
            imm: cur.imm.clone(),
            version: Arc::new(cur.version.with_compaction(&removed, outputs)),
        })?;
        // Deleted when the last snapshot that still lists them is dropped.
        for t in picked.l0.iter().chain(&picked.l1) {
            t.mark_obsolete();
        }
        drop(picked);
        self.maybe_rewrite_manifest()?;
        Ok(Some(stats))
    }

    fn maybe_rewrite_manifest(&self) -> Result<()> {
        let mut vs = lock(&self.vset)?;
        if vs.manifest.needs_rewrite() {
            let snap = self.snapshot()?;
            vs.manifest = Manifest::create(
                &self.dir,
                &snapshot_edits(
                    &snap.version,
                    vs.log_number,
                    self.next_file.load(Ordering::SeqCst),
                ),
                self.opts.manifest_max_bytes,
                &self.fp,
            )?;
        }
        Ok(())
    }
}

/// Poisons the engine if the worker thread unwinds, so no caller waits
/// forever for a job that will never finish.
struct WorkerPanicGuard<'a>(&'a Inner);

impl Drop for WorkerPanicGuard<'_> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.0
                .record_failure("background worker panicked".into(), None);
        }
    }
}

fn snapshot_edits(version: &Version, log_number: u64, next_file_number: u64) -> Vec<VersionEdit> {
    let mut edits = vec![
        VersionEdit::SetNextFileNumber(next_file_number),
        VersionEdit::SetLogNumber(log_number),
    ];
    for (level, t) in version.tables() {
        edits.push(VersionEdit::AddFile {
            level,
            meta: t.meta.clone(),
        });
    }
    edits
}

/// Write `mem` (tombstones included) to `<file_no>.sst`; fsyncs the file.
fn write_memtable(
    dir: &Path,
    file_no: u64,
    mem: &Memtable,
    opts: &Options,
    delete_errors: &Arc<DeleteErrors>,
) -> Result<Arc<Table>> {
    let mut builder = SstBuilder::new(&sst_path(dir, file_no), opts)?;
    mem.try_for_each(|k, e| builder.add(k, e))?;
    Table::from_builder(dir, file_no, builder, delete_errors)
}

/// Replay one WAL into `mem`. Only the newest WAL may end in an incomplete
/// record or be salvaged; older WALs were sealed (fully written and synced)
/// before their successor was created, so any defect there is corruption.
fn replay_wal(
    path: &Path,
    newest: bool,
    opts: &Options,
    mem: &Memtable,
    report: &mut RecoveryReport,
) -> Result<()> {
    let file = File::open(path).map_err(|e| Error::io("open wal", path, e))?;
    let len = file
        .metadata()
        .map_err(|e| Error::io("stat wal", path, e))?
        .len();
    let strict;
    let o = if newest {
        opts
    } else {
        strict = Options {
            recovery_mode: RecoveryMode::Strict,
            ..opts.clone()
        };
        &strict
    };
    let rep = wal::replay(
        BufReader::with_capacity(64 * 1024, file),
        path,
        o,
        |rec| match rec {
            WalRecord::Put { key, value } => mem.put(&key, &value),
            WalRecord::Delete { key } => mem.delete(&key),
        },
    )?;
    report.wal_files_replayed += 1;
    report.records_replayed += rep.records;
    match rep.end {
        ReplayEnd::Clean => {}
        ReplayEnd::IncompleteTail | ReplayEnd::HeaderIncomplete if newest => {
            report.discarded_bytes += len - rep.good_end;
        }
        ReplayEnd::IncompleteTail | ReplayEnd::HeaderIncomplete => {
            return Err(Error::corruption(
                path,
                rep.good_end,
                "incomplete record in a sealed (non-newest) WAL",
            ));
        }
        ReplayEnd::Salvaged(detail) => {
            report.salvaged = Some(detail);
            report.discarded_bytes += len - rep.good_end;
        }
    }
    Ok(())
}
