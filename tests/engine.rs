//! Integration tests for the Phase 1 engine: CRUD semantics, restart, locking.

use std::collections::BTreeMap;
use std::sync::Arc;

use forgekv::{Db, Error, Options, SyncMode};
use tempfile::TempDir;

fn open(dir: &TempDir) -> Db {
    Db::open(dir.path(), Options::default()).unwrap()
}

#[test]
fn put_get_delete_basic() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);
    assert_eq!(db.get(b"k").unwrap(), None);
    db.put(b"k", b"v").unwrap();
    assert_eq!(db.get(b"k").unwrap(), Some(b"v".to_vec()));
    db.delete(b"k").unwrap();
    assert_eq!(db.get(b"k").unwrap(), None);
    db.put(b"k", b"again").unwrap();
    assert_eq!(db.get(b"k").unwrap(), Some(b"again".to_vec()));
}

#[test]
fn overwrite_returns_latest() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);
    for i in 0..10 {
        db.put(b"k", format!("v{i}").as_bytes()).unwrap();
    }
    assert_eq!(db.get(b"k").unwrap(), Some(b"v9".to_vec()));
}

#[test]
fn empty_value_is_distinct_from_deleted() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);
    db.put(b"k", b"").unwrap();
    assert_eq!(db.get(b"k").unwrap(), Some(Vec::new()));
    db.delete(b"k").unwrap();
    assert_eq!(db.get(b"k").unwrap(), None);
}

#[test]
fn deleting_absent_key_is_ok() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);
    db.delete(b"never-written").unwrap();
    assert_eq!(db.get(b"never-written").unwrap(), None);
}

#[test]
fn binary_keys_and_values() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);
    let key = [0u8, 255, 0, 1, 10, 13];
    let val: Vec<u8> = (0..=255).collect();
    db.put(&key, &val).unwrap();
    assert_eq!(db.get(&key).unwrap(), Some(val));
}

#[test]
fn invalid_arguments_are_rejected_and_write_nothing() {
    let dir = TempDir::new().unwrap();
    let opts = Options::default();
    let db = Db::open(dir.path(), opts.clone()).unwrap();
    let too_big_key = vec![b'k'; opts.max_key_len + 1];
    let too_big_val = vec![0u8; opts.max_value_len + 1];

    for r in [
        db.put(b"", b"v"),
        db.put(&too_big_key, b"v"),
        db.put(b"k", &too_big_val),
        db.delete(b""),
        db.delete(&too_big_key),
    ] {
        assert!(matches!(r, Err(Error::InvalidArgument(_))), "{r:?}");
    }
    assert!(matches!(db.get(b""), Err(Error::InvalidArgument(_))));
    assert!(matches!(
        db.get(&too_big_key),
        Err(Error::InvalidArgument(_))
    ));
    // Rejected writes must not poison the engine or reach the log.
    db.put(b"k", b"v").unwrap();
    drop(db);
    let db = open(&dir);
    assert_eq!(db.recovery_report().records_replayed, 1);
}

#[test]
fn maximum_size_key_and_value_work_across_restart() {
    let dir = TempDir::new().unwrap();
    let opts = Options::default();
    let key = vec![b'k'; opts.max_key_len];
    let val = vec![7u8; opts.max_value_len];
    {
        let db = Db::open(dir.path(), opts.clone()).unwrap();
        db.put(&key, &val).unwrap();
    }
    let db = Db::open(dir.path(), opts).unwrap();
    assert_eq!(db.get(&key).unwrap(), Some(val));
}

#[test]
fn invalid_options_rejected_on_open() {
    let dir = TempDir::new().unwrap();
    let opts = Options {
        max_key_len: 0,
        ..Options::default()
    };
    assert!(matches!(
        Db::open(dir.path(), opts),
        Err(Error::InvalidArgument(_))
    ));
}

#[test]
fn creates_missing_directory() {
    let dir = TempDir::new().unwrap();
    let nested = dir.path().join("a").join("b");
    let db = Db::open(&nested, Options::default()).unwrap();
    db.put(b"k", b"v").unwrap();
    assert!(nested.join("wal-000001.log").exists());
}

#[test]
fn reopen_preserves_state_after_drop_and_after_close() {
    let dir = TempDir::new().unwrap();
    {
        let db = open(&dir);
        db.put(b"a", b"1").unwrap();
        db.put(b"b", b"2").unwrap();
        // Dropped without close: acknowledged writes must still be there.
    }
    {
        let db = open(&dir);
        assert_eq!(db.get(b"a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(db.recovery_report().records_replayed, 2);
        assert_eq!(db.recovery_report().discarded_bytes, 0);
        db.put(b"c", b"3").unwrap();
        db.close().unwrap();
    }
    let db = open(&dir);
    assert_eq!(db.get(b"a").unwrap(), Some(b"1".to_vec()));
    assert_eq!(db.get(b"b").unwrap(), Some(b"2".to_vec()));
    assert_eq!(db.get(b"c").unwrap(), Some(b"3".to_vec()));
}

#[test]
fn deletes_and_overwrites_survive_restart() {
    let dir = TempDir::new().unwrap();
    {
        let db = open(&dir);
        db.put(b"gone", b"x").unwrap();
        db.put(b"over", b"old").unwrap();
        db.delete(b"gone").unwrap();
        db.put(b"over", b"new").unwrap();
        db.delete(b"never-existed").unwrap();
        db.put(b"back", b"1").unwrap();
        db.delete(b"back").unwrap();
        db.put(b"back", b"2").unwrap();
    }
    let db = open(&dir);
    assert_eq!(db.get(b"gone").unwrap(), None);
    assert_eq!(db.get(b"over").unwrap(), Some(b"new".to_vec()));
    assert_eq!(db.get(b"never-existed").unwrap(), None);
    assert_eq!(db.get(b"back").unwrap(), Some(b"2".to_vec()));
}

#[test]
fn second_open_of_same_directory_fails_until_first_is_dropped() {
    let dir = TempDir::new().unwrap();
    let first = open(&dir);
    assert!(matches!(
        Db::open(dir.path(), Options::default()),
        Err(Error::Locked { .. })
    ));
    drop(first);
    open(&dir);
}

#[test]
fn never_mode_still_persists_across_process_level_drop_and_on_close() {
    let dir = TempDir::new().unwrap();
    let opts = Options {
        sync_mode: SyncMode::Never,
        ..Options::default()
    };
    let db = Db::open(dir.path(), opts.clone()).unwrap();
    db.put(b"k", b"v").unwrap();
    db.close().unwrap();
    let db = Db::open(dir.path(), opts).unwrap();
    assert_eq!(db.get(b"k").unwrap(), Some(b"v".to_vec()));
}

#[test]
fn concurrent_writers_and_readers() {
    let dir = TempDir::new().unwrap();
    let opts = Options {
        sync_mode: SyncMode::Never,
        ..Options::default()
    };
    let db = Arc::new(Db::open(dir.path(), opts.clone()).unwrap());
    let mut handles = vec![];
    for t in 0..4u32 {
        let db = Arc::clone(&db);
        handles.push(std::thread::spawn(move || {
            for i in 0..200u32 {
                let key = format!("t{t}-k{i}");
                db.put(key.as_bytes(), key.as_bytes()).unwrap();
                // A key this thread just wrote is always visible to it.
                assert_eq!(db.get(key.as_bytes()).unwrap(), Some(key.into_bytes()));
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    let db = Arc::try_unwrap(db).ok().unwrap();
    db.close().unwrap();
    let db = Db::open(dir.path(), opts).unwrap();
    assert_eq!(db.recovery_report().records_replayed, 800);
    for t in 0..4u32 {
        for i in 0..200u32 {
            let key = format!("t{t}-k{i}");
            assert_eq!(db.get(key.as_bytes()).unwrap(), Some(key.into_bytes()));
        }
    }
}

/// Tiny deterministic PRNG so the test is reproducible without a dependency.
struct XorShift(u64);
impl XorShift {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

#[test]
fn randomized_ops_with_restarts_match_a_model() {
    for seed in [1u64, 2, 3, 0xDEAD_BEEF] {
        let dir = TempDir::new().unwrap();
        let opts = Options {
            sync_mode: SyncMode::Never,
            ..Options::default()
        };
        let mut db = Db::open(dir.path(), opts.clone()).unwrap();
        let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        let mut rng = XorShift(seed);
        for step in 0..3000 {
            let key = format!("key-{:02}", rng.below(40)).into_bytes();
            match rng.below(100) {
                0..=54 => {
                    let len = rng.below(50) as usize; // includes empty values
                    let val: Vec<u8> = (0..len).map(|_| rng.next() as u8).collect();
                    db.put(&key, &val).unwrap();
                    model.insert(key, val);
                }
                55..=74 => {
                    db.delete(&key).unwrap();
                    model.remove(&key);
                }
                75..=94 => {
                    assert_eq!(
                        db.get(&key).unwrap().as_ref(),
                        model.get(&key),
                        "seed={seed} step={step}"
                    );
                }
                _ => {
                    // Alternate clean close and abrupt drop.
                    if step % 2 == 0 {
                        db.close().unwrap();
                    } else {
                        drop(db);
                    }
                    db = Db::open(dir.path(), opts.clone()).unwrap();
                }
            }
        }
        for i in 0..40 {
            let key = format!("key-{i:02}").into_bytes();
            assert_eq!(
                db.get(&key).unwrap().as_ref(),
                model.get(&key),
                "seed={seed}"
            );
        }
    }
}
