//! Crash-point tests (feature `failpoints`): inject a failure at every step
//! of flush, open, compaction, WAL rotation and manifest rewrite, drop the database as a process crash
//! would, reopen, and compare against a model of acknowledged writes.
//!
//! Failpoints are per database (`Db::failpoints`, `Db::open_with_failpoints`)
//! and fire on whichever thread reaches them, including the background
//! worker. A failpoint returns `Err(Injected)` from inside the operation. Nothing is
//! buffered in user space, so the files are then exactly what a crash at
//! that instant would leave (minus page-cache loss, which is out of scope:
//! see the README's "Durability semantics").
#![cfg(feature = "failpoints")]

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use forgekv::{Db, Error, FailPoints, Options, failpoint};
use tempfile::TempDir;

type Model = BTreeMap<Vec<u8>, Vec<u8>>;

fn small() -> Options {
    Options {
        max_key_len: 64,
        max_value_len: 512,
        memtable_bytes: 4096,
        block_bytes: 256,
        sst_target_bytes: 1024,
        l0_compaction_trigger: 1_000_000,
        ..Options::default()
    }
}

fn k(i: usize) -> Vec<u8> {
    format!("key-{i:04}").into_bytes()
}

fn v(i: usize, round: usize) -> Vec<u8> {
    format!("v{i}r{round}").into_bytes()
}

/// Apply writes for keys `range` (deleting every 7th) to db and model.
fn write(db: &Db, model: &mut Model, range: std::ops::Range<usize>, round: usize) {
    for i in range {
        if i % 7 == 3 {
            db.delete(&k(i)).unwrap();
            model.remove(&k(i));
        } else {
            db.put(&k(i), &v(i, round)).unwrap();
            model.insert(k(i), v(i, round));
        }
    }
}

fn assert_model(db: &Db, model: &Model, keys: usize, ctx: &str) {
    for i in 0..keys {
        assert_eq!(
            db.get(&k(i)).unwrap().as_ref(),
            model.get(&k(i)),
            "{ctx}: key {i}"
        );
    }
}

/// Reopen after a crash and check data, invariants, and that the database
/// keeps working (write, flush, reopen).
fn recover_and_check(dir: &Path, opts: &Options, model: &mut Model, keys: usize, ctx: &str) {
    let db = Db::open(dir, opts.clone()).unwrap_or_else(|e| panic!("{ctx}: reopen failed: {e}"));
    assert_model(&db, model, keys, ctx);
    db.check_invariants()
        .unwrap_or_else(|e| panic!("{ctx}: invariants: {e}"));
    write(&db, model, 0..keys, 99);
    db.flush().unwrap();
    drop(db);
    let db = Db::open(dir, opts.clone()).unwrap();
    assert_model(&db, model, keys, ctx);
    db.check_invariants().unwrap();
}

/// `Db::open` with `name` armed; returns the result without the handle.
fn open_armed(dir: &Path, opts: &Options, name: &'static str) -> Result<(), Error> {
    let fp = FailPoints::new();
    fp.arm(name);
    Db::open_with_failpoints(dir, opts.clone(), fp).map(|_| ())
}

fn expect_injected<T: std::fmt::Debug>(r: Result<T, Error>, name: &str) {
    match r {
        Err(Error::Injected(n)) if n == name => {}
        other => panic!("expected injected failure at {name}, got {other:?}"),
    }
}

#[test]
fn every_flush_step() {
    for name in [
        "switch:after_new_wal",
        "flush:before_manifest",
        "flush:after_manifest",
    ] {
        let dir = TempDir::new().unwrap();
        let opts = small();
        let mut model = Model::new();
        {
            let db = Db::open(dir.path(), opts.clone()).unwrap();
            write(&db, &mut model, 0..40, 0);
            db.flush().unwrap();
            write(&db, &mut model, 20..60, 1);
            db.failpoints().arm(name);
            expect_injected(db.flush(), name);
            // The engine is poisoned for writes, but reads still see committed data.
            assert!(matches!(db.put(b"x", b"y"), Err(Error::Poisoned)));
            assert_model(&db, &model, 60, name);
        }
        recover_and_check(dir.path(), &opts, &mut model, 60, name);
    }
}

#[test]
fn flush_failure_inside_put_does_not_apply_that_write() {
    let dir = TempDir::new().unwrap();
    let opts = small();
    let mut model = Model::new();
    let failed_key;
    {
        let db = Db::open(dir.path(), opts.clone()).unwrap();
        db.failpoints().arm("flush:before_manifest");
        let mut i = 0;
        loop {
            match db.put(&k(i), &v(i, 0)) {
                Ok(()) => {
                    model.insert(k(i), v(i, 0));
                    i += 1;
                }
                // The flush fails on the worker thread; the write that
                // observes it gets the injected error (if it waited on that
                // flush) or `Poisoned`. Either way it was not applied.
                Err(Error::Injected(_) | Error::Poisoned) => break,
                Err(e) => panic!("{e}"),
            }
        }
        failed_key = i;
    }
    assert!(
        failed_key > 5,
        "the memtable should hold several writes first"
    );
    let db = Db::open(dir.path(), opts.clone()).unwrap();
    assert_eq!(
        db.get(&k(failed_key)).unwrap(),
        None,
        "failed put must not appear"
    );
    assert_model(&db, &model, failed_key + 1, "auto flush");
    db.check_invariants().unwrap();
}

/// Regression (audit): the docs claimed an `Err` from `put` means the write
/// was not applied. When the WAL write/sync fails after the record reached
/// the file, the outcome is unknown: invisible now, but replayed on recovery.
#[test]
fn failure_after_the_wal_write_has_an_unknown_outcome() {
    let dir = TempDir::new().unwrap();
    let opts = small();
    {
        let db = Db::open(dir.path(), opts.clone()).unwrap();
        db.put(b"a", b"1").unwrap();
        db.failpoints().arm("write:after_wal");
        expect_injected(db.put(b"b", b"2"), "write:after_wal");
        assert_eq!(db.get(b"b").unwrap(), None, "not visible before recovery");
        assert!(matches!(db.put(b"c", b"3"), Err(Error::Poisoned)));
    }
    let db = Db::open(dir.path(), opts).unwrap();
    assert_eq!(db.get(b"a").unwrap(), Some(b"1".to_vec()));
    assert_eq!(
        db.get(b"b").unwrap(),
        Some(b"2".to_vec()),
        "logged, so replayed"
    );
    assert_eq!(db.get(b"c").unwrap(), None);
}

/// Phase 8: the MANIFEST rewrite that follows a flush or a compaction once
/// the manifest outgrows `manifest_max_bytes` (open's rewrite is covered by
/// `every_open_step`). The edit itself is already committed when the
/// rewrite fails, so recovery must see the flushed/compacted state.
#[test]
fn every_runtime_manifest_rewrite_step() {
    let opts = Options {
        // Every append makes the manifest "too large": rewrite after each job.
        manifest_max_bytes: 1,
        ..small()
    };
    for job in ["flush", "compaction"] {
        for name in ["manifest:after_tmp_written", "manifest:after_rename"] {
            let ctx = format!("{job} / {name}");
            let dir = TempDir::new().unwrap();
            let mut model = Model::new();
            {
                let db = Db::open(dir.path(), opts.clone()).unwrap();
                write(&db, &mut model, 0..40, 0);
                db.flush().unwrap();
                write(&db, &mut model, 20..60, 1);
                if job == "compaction" {
                    db.flush().unwrap();
                }
                db.failpoints().arm(name);
                let r = if job == "flush" {
                    db.flush()
                } else {
                    db.compact().map(|_| ())
                };
                expect_injected(r, name);
                assert!(matches!(db.put(b"x", b"y"), Err(Error::Poisoned)), "{ctx}");
                assert_model(&db, &model, 60, &ctx);
            }
            recover_and_check(dir.path(), &opts, &mut model, 60, &ctx);
        }
    }
}

/// A database with flushed tables *and* unflushed WAL data, closed abruptly.
fn prepared(dir: &Path, opts: &Options, model: &mut Model) {
    let db = Db::open(dir, opts.clone()).unwrap();
    write(&db, model, 0..50, 0);
    db.flush().unwrap();
    write(&db, model, 25..70, 1);
}

#[test]
fn every_open_step() {
    for name in [
        "open:after_flush_sst",
        "manifest:after_tmp_written",
        "manifest:after_rename",
        "open:after_new_wal",
    ] {
        let dir = TempDir::new().unwrap();
        let opts = small();
        let mut model = Model::new();
        prepared(dir.path(), &opts, &mut model);
        expect_injected(open_armed(dir.path(), &opts, name), name);
        recover_and_check(dir.path(), &opts, &mut model, 70, name);
    }
}

/// The newest WAL ends in a torn record *and* recovery itself crashes. The
/// torn WAL must never end up "sealed" behind a newer WAL (which would turn
/// a legal torn tail into corruption on the next open).
#[test]
fn torn_wal_plus_crash_during_recovery_still_recovers() {
    for name in [
        "open:after_flush_sst",
        "manifest:after_tmp_written",
        "manifest:after_rename",
        "open:after_new_wal",
    ] {
        let dir = TempDir::new().unwrap();
        let opts = small();
        let mut model = Model::new();
        prepared(dir.path(), &opts, &mut model);
        let wal = fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().path())
            .find(|p| p.file_name().unwrap().to_str().unwrap().starts_with("wal-"))
            .unwrap();
        let mut bytes = fs::read(&wal).unwrap();
        bytes.extend_from_slice(&[7, 0, 0]); // torn frame header
        fs::write(&wal, bytes).unwrap();

        expect_injected(open_armed(dir.path(), &opts, name), name);
        // Crash again at the same point, then recover for real.
        let second = open_armed(dir.path(), &opts, name);
        assert!(
            matches!(second, Err(Error::Injected(_)) | Ok(())),
            "{name}: {second:?}"
        );
        recover_and_check(dir.path(), &opts, &mut model, 70, name);
    }
}

#[test]
fn crash_while_creating_a_new_database() {
    for name in ["manifest:after_tmp_written", "manifest:after_rename"] {
        let dir = TempDir::new().unwrap();
        expect_injected(open_armed(dir.path(), &small(), name), name);
        let db = Db::open(dir.path(), small()).unwrap();
        assert_eq!(db.get(b"k").unwrap(), None);
        db.put(b"k", b"v").unwrap();
        db.check_invariants().unwrap();
        drop(db);
        let db = Db::open(dir.path(), small()).unwrap();
        assert_eq!(db.get(b"k").unwrap(), Some(b"v".to_vec()));
    }
}

/// After a crash between WAL rotation and the manifest commit there are two
/// live WALs. The older one is sealed: an incomplete record in it is
/// corruption, not a torn tail.
#[test]
fn incomplete_record_in_sealed_wal_is_corruption() {
    let dir = TempDir::new().unwrap();
    let opts = small();
    let mut model = Model::new();
    {
        let db = Db::open(dir.path(), opts.clone()).unwrap();
        write(&db, &mut model, 0..30, 0);
        db.failpoints().arm("switch:after_new_wal");
        expect_injected(db.flush(), "switch:after_new_wal");
    }
    let mut wals: Vec<_> = fs::read_dir(dir.path())
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.file_name().unwrap().to_str().unwrap().starts_with("wal-"))
        .collect();
    wals.sort();
    assert_eq!(wals.len(), 2, "{wals:?}");
    let older = &wals[0];
    let mut bytes = fs::read(older).unwrap();
    bytes.truncate(bytes.len() - 2);
    fs::write(older, &bytes).unwrap();
    let err = Db::open(dir.path(), opts).err().unwrap();
    assert!(
        matches!(&err, Error::Corruption { detail, .. } if detail.contains("sealed")),
        "{err}"
    );
}

// ---------------------------------------------------------------- Phase 4

/// L1 tables from an earlier compaction plus several overlapping L0 tables.
fn prepared_for_compaction(dir: &Path, opts: &Options, model: &mut Model) -> Db {
    let db = Db::open(dir, opts.clone()).unwrap();
    write(&db, model, 0..80, 0);
    db.flush().unwrap();
    db.compact().unwrap();
    assert!(!db.level_summary().unwrap().l1.is_empty());
    for round in 1..4 {
        write(&db, model, (round * 10)..(round * 10 + 50), round);
        db.flush().unwrap();
    }
    db
}

#[test]
fn every_compaction_step() {
    for name in [
        "compaction:after_output",
        "compaction:before_manifest",
        "compaction:after_manifest",
    ] {
        let dir = TempDir::new().unwrap();
        let opts = small();
        let mut model = Model::new();
        {
            let db = prepared_for_compaction(dir.path(), &opts, &mut model);
            db.failpoints().arm(name);
            expect_injected(db.compact(), name);
            assert!(matches!(db.put(b"x", b"y"), Err(Error::Poisoned)));
            assert_model(&db, &model, 80, name);
        }
        recover_and_check(dir.path(), &opts, &mut model, 80, name);
        // And the interrupted compaction can be completed afterwards.
        let db = Db::open(dir.path(), opts.clone()).unwrap();
        db.compact().unwrap();
        assert_model(&db, &model, 80, name);
        db.check_invariants().unwrap();
    }
}

#[test]
fn compaction_failure_inside_put_does_not_apply_that_write() {
    let dir = TempDir::new().unwrap();
    let opts = Options {
        l0_compaction_trigger: 2,
        ..small()
    };
    let mut model = Model::new();
    let failed;
    {
        let db = Db::open(dir.path(), opts.clone()).unwrap();
        db.failpoints().arm("compaction:before_manifest");
        let mut i = 0;
        loop {
            match db.put(&k(i % 300), &v(i, 0)) {
                Ok(()) => {
                    model.insert(k(i % 300), v(i, 0));
                    i += 1;
                }
                // Background compaction failure: see the flush variant above.
                Err(Error::Injected(_) | Error::Poisoned) => break,
                Err(e) => panic!("{e}"),
            }
        }
        failed = i;
    }
    let db = Db::open(dir.path(), opts).unwrap();
    assert_model(&db, &model, 300, "auto compaction");
    assert!(failed < 300, "test expects the failing key to be new");
    assert_eq!(db.get(&k(failed)).unwrap(), None);
    db.check_invariants().unwrap();
}

/// Random operations; before some flushes/compactions/opens a random
/// failpoint is armed. After every injected failure the database is
/// reopened and must equal the model of acknowledged writes.
#[test]
fn randomized_crashes_match_model() {
    let mut seed = 0xC0FF_EE00_1234_5678u64;
    let mut rnd = move |n: u64| {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed % n
    };
    let opts = Options {
        l0_compaction_trigger: 3,
        sst_target_bytes: 1024,
        ..small()
    };
    for round in 0..6 {
        let dir = TempDir::new().unwrap();
        let mut model = Model::new();
        let mut db = Db::open(dir.path(), opts.clone()).unwrap();
        let mut crashes = 0;
        for step in 0..1500 {
            let i = rnd(120) as usize;
            let op = rnd(100);
            let result = if op < 55 {
                let val = v(i, step);
                db.put(&k(i), &val).map(|()| {
                    model.insert(k(i), val);
                })
            } else if op < 75 {
                db.delete(&k(i)).map(|()| {
                    model.remove(&k(i));
                })
            } else if op < 90 {
                assert_eq!(db.get(&k(i)).unwrap().as_ref(), model.get(&k(i)));
                Ok(())
            } else {
                let name = failpoint::ALL[rnd(failpoint::ALL.len() as u64) as usize];
                let r = match op {
                    90..=94 => {
                        db.failpoints().arm(name);
                        db.flush()
                    }
                    95..=97 => {
                        db.failpoints().arm(name);
                        db.compact().map(|_| ())
                    }
                    _ => {
                        drop(db);
                        let fp = FailPoints::new();
                        fp.arm(name);
                        match Db::open_with_failpoints(dir.path(), opts.clone(), fp) {
                            Ok(new) => {
                                db = new;
                                Ok(())
                            }
                            Err(e) => {
                                db = Db::open(dir.path(), opts.clone()).unwrap();
                                Err(e)
                            }
                        }
                    }
                };
                db.failpoints().clear();
                r
            };
            match result {
                Ok(()) => {}
                // `Poisoned`: an armed point fired in a background job that
                // ran before it was cleared. Same treatment as a crash.
                Err(Error::Injected(_) | Error::Poisoned) => {
                    crashes += 1;
                    drop(db);
                    db = Db::open(dir.path(), opts.clone()).unwrap();
                    db.check_invariants().unwrap();
                    assert_model(&db, &model, 120, &format!("round {round} step {step}"));
                }
                Err(e) => panic!("round {round} step {step}: {e}"),
            }
        }
        assert!(
            crashes > 5,
            "round {round}: only {crashes} crashes injected"
        );
        drop(db);
        let db = Db::open(dir.path(), opts.clone()).unwrap();
        assert_model(&db, &model, 120, "final");
        db.check_invariants().unwrap();
    }
}
