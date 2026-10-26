//! Phase 8: deterministic corruption sweep over every file type of a real
//! database (MANIFEST, L0 and L1 SSTables, WAL). One bit is flipped at a
//! time; the database must then either refuse to open with `Corruption` /
//! `UnsupportedVersion`, or open and answer every read with the right value
//! or `Corruption`. It must never panic and never return wrong data, and
//! (since every byte is checksummed or validated) no flip may go unnoticed.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use forgekv::{Db, Error, Options};
use tempfile::TempDir;

fn opts() -> Options {
    Options {
        max_key_len: 64,
        max_value_len: 512,
        memtable_bytes: 64 * 1024,
        block_bytes: 128,
        sst_target_bytes: 1024,
        l0_compaction_trigger: 1_000,
        ..Options::default()
    }
}

type Model = BTreeMap<Vec<u8>, Vec<u8>>;

const KEYS: usize = 40;

fn key(i: usize) -> Vec<u8> {
    format!("key-{i:03}").into_bytes()
}

/// A database with data in L1, in L0 and in the WAL, closed cleanly.
fn build(dir: &Path) -> Model {
    let mut model = Model::new();
    let db = Db::open(dir, opts()).unwrap();
    for i in 0..KEYS {
        let v = format!("l1-{i}").into_bytes();
        db.put(&key(i), &v).unwrap();
        model.insert(key(i), v);
    }
    db.flush().unwrap();
    db.compact().unwrap();
    for i in (0..KEYS).step_by(3) {
        let v = format!("l0-{i}").into_bytes();
        db.put(&key(i), &v).unwrap();
        model.insert(key(i), v);
    }
    db.flush().unwrap();
    for i in (1..KEYS).step_by(4) {
        db.delete(&key(i)).unwrap();
        model.remove(&key(i));
    }
    db.put(&key(2), b"wal").unwrap();
    model.insert(key(2), b"wal".to_vec());
    db.close().unwrap();
    model
}

fn data_files(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.file_name().unwrap() != "LOCK")
        .collect();
    v.sort();
    v
}

fn acceptable_open_error(e: &Error) -> bool {
    matches!(
        e,
        Error::Corruption { .. } | Error::UnsupportedVersion { .. }
    )
}

#[test]
fn single_bit_flips_in_any_file_are_detected_or_harmless() {
    let golden = TempDir::new().unwrap();
    let model = build(golden.path());
    let files = data_files(golden.path());
    let names: Vec<String> = files
        .iter()
        .map(|p| p.file_name().unwrap().to_string_lossy().into_owned())
        .collect();
    assert!(names.iter().any(|n| n == "MANIFEST"), "{names:?}");
    assert!(
        names.iter().filter(|n| n.ends_with(".sst")).count() >= 2,
        "{names:?}"
    );
    assert!(names.iter().any(|n| n.starts_with("wal-")), "{names:?}");

    let total: usize = files
        .iter()
        .map(|f| fs::metadata(f).unwrap().len() as usize)
        .sum();
    let stride = total.div_ceil(1500).max(1);
    let (mut refused, mut detected_on_read, mut harmless) = (0, 0, 0);
    for (fi, file) in files.iter().enumerate() {
        let original = fs::read(file).unwrap();
        for pos in (fi % stride..original.len()).step_by(stride) {
            let bit = 1u8 << (pos % 8);
            let ctx = format!("{} byte {pos} bit {bit:#04x}", names[fi]);
            let dir = TempDir::new().unwrap();
            for f in &files {
                fs::copy(f, dir.path().join(f.file_name().unwrap())).unwrap();
            }
            let mut bad = original.clone();
            bad[pos] ^= bit;
            fs::write(dir.path().join(&names[fi]), &bad).unwrap();

            let db = match Db::open(dir.path(), opts()) {
                Ok(db) => db,
                Err(e) => {
                    assert!(acceptable_open_error(&e), "{ctx}: open failed with {e}");
                    refused += 1;
                    continue;
                }
            };
            let mut saw_corruption = false;
            for i in 0..KEYS {
                match db.get(&key(i)) {
                    Ok(got) => assert_eq!(
                        got.as_ref(),
                        model.get(&key(i)),
                        "{ctx}: wrong data for key {i}"
                    ),
                    Err(Error::Corruption { .. }) => saw_corruption = true,
                    Err(e) => panic!("{ctx}: key {i}: unexpected error {e}"),
                }
            }
            if saw_corruption {
                detected_on_read += 1;
            } else {
                harmless += 1;
            }
        }
    }
    eprintln!(
        "{total} bytes, stride {stride}: refused to open {refused}, corruption on read {detected_on_read}, opened with correct data {harmless}"
    );
    assert!(refused > 0 && detected_on_read > 0);
    // Every byte of every file is covered by a checksum or validated, so no
    // flip may go unnoticed. (Regression: the reserved bytes of the record
    // log header were not checked before Phase 8.)
    assert_eq!(harmless, 0, "a flipped bit was accepted silently");
}
