//! On-disk format compatibility.
//!
//! `tests/data/golden-db/` is a small database (record log format v2 for the
//! WAL and MANIFEST, SSTable format v1) with data in L1, L0 and the WAL.
//! Two checks:
//! - it opens (from a copy) with exactly the expected contents, so files
//!   written by this format version stay readable;
//! - building it again with the current code produces byte-identical files,
//!   so an accidental format change fails here.
//!
//! After an *intentional* format change (with a version bump), regenerate
//! with `FORGEKV_WRITE_GOLDEN=1 cargo test --test golden` and review the diff.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use forgekv::{Db, Options};
use tempfile::TempDir;

const GOLDEN: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/data/golden-db");

fn opts() -> Options {
    Options {
        max_key_len: 64,
        max_value_len: 512,
        memtable_bytes: 64 * 1024,
        block_bytes: 128,
        sst_target_bytes: 512,
        l0_compaction_trigger: 1_000,
        ..Options::default()
    }
}

fn key(i: usize) -> Vec<u8> {
    format!("golden-{i:02}").into_bytes()
}

/// Deterministic construction; returns the expected contents.
fn build(dir: &Path) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let mut model = BTreeMap::new();
    let db = Db::open(dir, opts()).unwrap();
    for i in 0..30 {
        let v = format!("value-{i}-in-l1").into_bytes();
        db.put(&key(i), &v).unwrap();
        model.insert(key(i), v);
    }
    db.flush().unwrap();
    db.compact().unwrap();
    for i in (0..30).step_by(4) {
        let v = format!("value-{i}-in-l0").into_bytes();
        db.put(&key(i), &v).unwrap();
        model.insert(key(i), v);
    }
    db.delete(&key(5)).unwrap();
    model.remove(&key(5));
    db.flush().unwrap();
    db.put(&key(7), b"").unwrap();
    model.insert(key(7), vec![]);
    db.delete(&key(9)).unwrap();
    model.remove(&key(9));
    db.put(&key(30), b"only-in-the-wal").unwrap();
    model.insert(key(30), b"only-in-the-wal".to_vec());
    db.close().unwrap();
    model
}

/// Data files (everything but the lock file), sorted by name.
fn files(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.file_name().unwrap() != "LOCK")
        .collect();
    v.sort();
    v
}

fn names(files: &[PathBuf]) -> Vec<String> {
    files
        .iter()
        .map(|p| p.file_name().unwrap().to_string_lossy().into_owned())
        .collect()
}

#[test]
fn the_current_writer_reproduces_the_golden_files_byte_for_byte() {
    let fresh = TempDir::new().unwrap();
    build(fresh.path());
    if std::env::var_os("FORGEKV_WRITE_GOLDEN").is_some() {
        let _ = fs::remove_dir_all(GOLDEN);
        fs::create_dir_all(GOLDEN).unwrap();
        for f in files(fresh.path()) {
            fs::copy(&f, Path::new(GOLDEN).join(f.file_name().unwrap())).unwrap();
        }
        eprintln!("wrote {GOLDEN}");
        return;
    }
    let (want, got) = (files(Path::new(GOLDEN)), files(fresh.path()));
    assert_eq!(names(&want), names(&got), "different set of files");
    for (w, g) in want.iter().zip(&got) {
        assert!(
            fs::read(w).unwrap() == fs::read(g).unwrap(),
            "{} differs from the golden file: the on-disk format changed",
            g.file_name().unwrap().to_string_lossy()
        );
    }
}

#[test]
fn the_golden_database_opens_with_the_expected_contents() {
    let expected = build(TempDir::new().unwrap().path());
    let copy = TempDir::new().unwrap();
    let golden = files(Path::new(GOLDEN));
    let n = names(&golden);
    assert!(n.contains(&"MANIFEST".to_string()), "{n:?}");
    assert!(
        n.iter().filter(|f| f.ends_with(".sst")).count() >= 2,
        "{n:?}"
    );
    assert!(n.iter().any(|f| f.starts_with("wal-")), "{n:?}");
    for f in &golden {
        fs::copy(f, copy.path().join(f.file_name().unwrap())).unwrap();
    }
    let db = Db::open(copy.path(), opts()).unwrap();
    assert_eq!(db.recovery_report().records_replayed, 3);
    for i in 0..32 {
        assert_eq!(
            db.get(&key(i)).unwrap().as_ref(),
            expected.get(&key(i)),
            "key {i}"
        );
    }
    db.check_invariants().unwrap();
}
