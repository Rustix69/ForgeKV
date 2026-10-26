//! Phase 3/4 engine tests: reads across memtable and SSTable layers, flush,
//! manifest recovery and corruption, orphan cleanup.

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use forgekv::{Db, Error, Options};
use tempfile::TempDir;

/// Small limits so flushes happen after a few dozen writes. Compaction is
/// effectively disabled (tests that want it lower the trigger).
fn small() -> Options {
    Options {
        max_key_len: 64,
        max_value_len: 512,
        memtable_bytes: 4096,
        block_bytes: 256,
        sst_target_bytes: 2048,
        l0_compaction_trigger: 1_000_000,
        ..Options::default()
    }
}

fn open(dir: &Path) -> Db {
    Db::open(dir, small()).unwrap()
}

fn key(i: usize) -> Vec<u8> {
    format!("key-{i:05}").into_bytes()
}

fn val(i: usize, round: usize) -> Vec<u8> {
    format!("value-{i}-{round}-").repeat(3).into_bytes()
}

fn file_names(dir: &Path) -> Vec<String> {
    let mut v: Vec<String> = fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    v.sort();
    v
}

#[test]
fn writes_across_many_flushes_are_readable_from_every_layer() {
    let dir = TempDir::new().unwrap();
    let opts = small();
    {
        let db = open(dir.path());
        for i in 0..600 {
            db.put(&key(i), &val(i, 0)).unwrap();
            let s = db.level_summary().unwrap();
            assert!(
                s.memtable_bytes <= opts.memtable_bytes,
                "memtable over budget"
            );
        }
        let s = db.level_summary().unwrap();
        assert!(
            s.l0.len() >= 10,
            "expected many L0 files, got {}",
            s.l0.len()
        );
        for i in 0..600 {
            assert_eq!(db.get(&key(i)).unwrap(), Some(val(i, 0)), "{i}");
        }
        db.check_invariants().unwrap();
    }
    let db = open(dir.path());
    for i in 0..600 {
        assert_eq!(db.get(&key(i)).unwrap(), Some(val(i, 0)), "{i}");
    }
    assert_eq!(db.get(b"absent").unwrap(), None);
    db.check_invariants().unwrap();
}

#[test]
fn newer_layers_shadow_older_ones() {
    let dir = TempDir::new().unwrap();
    let db = open(dir.path());
    db.put(b"k", b"v1").unwrap();
    db.flush().unwrap();
    db.put(b"k", b"v2").unwrap();
    db.flush().unwrap();
    assert_eq!(db.level_summary().unwrap().l0.len(), 2);
    assert_eq!(
        db.get(b"k").unwrap(),
        Some(b"v2".to_vec()),
        "newest L0 wins"
    );
    db.put(b"k", b"v3").unwrap();
    assert_eq!(db.get(b"k").unwrap(), Some(b"v3".to_vec()), "memtable wins");
    drop(db);
    let db = open(dir.path());
    assert_eq!(db.get(b"k").unwrap(), Some(b"v3".to_vec()));
}

#[test]
fn delete_in_memtable_hides_value_in_sstable() {
    let dir = TempDir::new().unwrap();
    let db = open(dir.path());
    db.put(b"k", b"v").unwrap();
    db.flush().unwrap();
    db.delete(b"k").unwrap();
    assert_eq!(db.get(b"k").unwrap(), None);
    drop(db);
    // The tombstone is replayed and flushed into a newer L0 table on open.
    let db = open(dir.path());
    assert_eq!(db.get(b"k").unwrap(), None);
    assert_eq!(db.level_summary().unwrap().l0.len(), 2);
}

#[test]
fn delete_in_newer_l0_hides_value_in_older_l0_across_restart() {
    let dir = TempDir::new().unwrap();
    {
        let db = open(dir.path());
        db.put(b"k", b"v").unwrap();
        db.put(b"other", b"x").unwrap();
        db.flush().unwrap();
        db.delete(b"k").unwrap();
        db.flush().unwrap();
        assert_eq!(db.level_summary().unwrap().l0.len(), 2);
        assert_eq!(db.get(b"k").unwrap(), None);
    }
    let db = open(dir.path());
    assert_eq!(db.get(b"k").unwrap(), None);
    assert_eq!(db.get(b"other").unwrap(), Some(b"x".to_vec()));
    // Writing again after a delete works and wins.
    db.put(b"k", b"again").unwrap();
    db.flush().unwrap();
    assert_eq!(db.get(b"k").unwrap(), Some(b"again".to_vec()));
}

#[test]
fn reopen_with_data_only_in_wal_only_in_sst_and_both() {
    // Only WAL.
    let dir = TempDir::new().unwrap();
    {
        let db = open(dir.path());
        db.put(b"w", b"1").unwrap();
        assert!(db.level_summary().unwrap().l0.is_empty());
    }
    let db = open(dir.path());
    assert_eq!(db.get(b"w").unwrap(), Some(b"1".to_vec()));
    assert!(db.recovery_report().flushed_on_open);
    drop(db);

    // Only SST.
    let dir = TempDir::new().unwrap();
    {
        let db = open(dir.path());
        db.put(b"s", b"1").unwrap();
        db.flush().unwrap();
    }
    let db = open(dir.path());
    assert_eq!(db.get(b"s").unwrap(), Some(b"1".to_vec()));
    assert_eq!(db.recovery_report().records_replayed, 0);
    assert!(!db.recovery_report().flushed_on_open);
    drop(db);

    // Both, with overlap.
    let dir = TempDir::new().unwrap();
    {
        let db = open(dir.path());
        db.put(b"a", b"sst").unwrap();
        db.put(b"b", b"sst").unwrap();
        db.flush().unwrap();
        db.put(b"b", b"wal").unwrap();
        db.put(b"c", b"wal").unwrap();
    }
    let db = open(dir.path());
    assert_eq!(db.get(b"a").unwrap(), Some(b"sst".to_vec()));
    assert_eq!(db.get(b"b").unwrap(), Some(b"wal".to_vec()));
    assert_eq!(db.get(b"c").unwrap(), Some(b"wal".to_vec()));
    db.check_invariants().unwrap();
}

#[test]
fn flushing_an_empty_memtable_is_a_no_op() {
    let dir = TempDir::new().unwrap();
    let db = open(dir.path());
    db.flush().unwrap();
    assert!(db.level_summary().unwrap().l0.is_empty());
    db.put(b"k", b"v").unwrap();
    db.flush().unwrap();
    db.flush().unwrap();
    assert_eq!(db.level_summary().unwrap().l0.len(), 1);
}

#[test]
fn only_live_files_remain_after_flushes_and_reopens() {
    let dir = TempDir::new().unwrap();
    for round in 0..3 {
        let db = open(dir.path());
        for i in 0..200 {
            db.put(&key(i), &val(i, round)).unwrap();
        }
        db.check_invariants().unwrap();
        let names = file_names(dir.path());
        let wals = names.iter().filter(|n| n.starts_with("wal-")).count();
        assert_eq!(wals, 1, "{names:?}");
        assert!(names.contains(&"MANIFEST".to_string()));
        assert!(!names.iter().any(|n| n.ends_with(".tmp")));
    }
    let db = open(dir.path());
    for i in 0..200 {
        assert_eq!(db.get(&key(i)).unwrap(), Some(val(i, 2)));
    }
}

#[test]
fn orphans_are_removed_and_unknown_files_are_left_alone() {
    let dir = TempDir::new().unwrap();
    let live_sst;
    {
        let db = open(dir.path());
        db.put(b"k", b"v").unwrap();
        db.flush().unwrap();
        live_sst = db.level_summary().unwrap().l0[0].file_no;
    }
    let d = dir.path();
    // A valid but unreferenced table, a garbage table, temp files, an old WAL.
    fs::copy(d.join(format!("{live_sst:06}.sst")), d.join("000777.sst")).unwrap();
    fs::write(d.join("000999.sst"), b"garbage").unwrap();
    fs::write(d.join("MANIFEST.tmp"), b"half-written").unwrap();
    fs::write(d.join("scratch.tmp"), b"x").unwrap();
    fs::write(d.join("wal-000000.log"), b"obsolete").unwrap();
    fs::write(d.join("notes.txt"), b"not ours").unwrap();

    let db = open(d);
    assert_eq!(db.recovery_report().orphans_removed, 5);
    assert_eq!(db.get(b"k").unwrap(), Some(b"v".to_vec()));
    db.check_invariants().unwrap();
    let names = file_names(d);
    assert!(names.contains(&"notes.txt".to_string()));
    for gone in [
        "000777.sst",
        "000999.sst",
        "MANIFEST.tmp",
        "scratch.tmp",
        "wal-000000.log",
    ] {
        assert!(!names.contains(&gone.to_string()), "{gone} survived");
    }
    // New files are numbered past the highest number ever seen on disk.
    db.put(b"k2", b"v2").unwrap();
    db.flush().unwrap();
    assert!(db.level_summary().unwrap().l0[0].file_no > 999);
}

#[test]
fn missing_manifest_with_data_files_is_an_error_and_touches_nothing() {
    let dir = TempDir::new().unwrap();
    {
        let db = open(dir.path());
        db.put(b"k", b"v").unwrap();
        db.flush().unwrap();
    }
    fs::remove_file(dir.path().join("MANIFEST")).unwrap();
    let before = file_names(dir.path());
    let err = Db::open(dir.path(), small()).err().unwrap();
    assert!(matches!(err, Error::Corruption { .. }), "{err}");
    assert_eq!(file_names(dir.path()), before);
}

/// A db with three manifest records: the open snapshot plus two flushes.
fn db_with_manifest_records(dir: &Path) {
    let db = open(dir);
    db.put(b"a", b"1").unwrap();
    db.flush().unwrap();
    db.put(b"b", b"2").unwrap();
    db.flush().unwrap();
}

#[test]
fn manifest_incomplete_tail_is_ignored() {
    let dir = TempDir::new().unwrap();
    db_with_manifest_records(dir.path());
    let m = dir.path().join("MANIFEST");
    let mut bytes = fs::read(&m).unwrap();
    bytes.extend_from_slice(&[0x10, 0, 0]); // 3 bytes of a frame header
    fs::write(&m, &bytes).unwrap();
    let db = open(dir.path());
    assert_eq!(db.recovery_report().manifest_discarded_bytes, 3);
    assert_eq!(db.get(b"a").unwrap(), Some(b"1".to_vec()));
    assert_eq!(db.get(b"b").unwrap(), Some(b"2".to_vec()));
}

#[test]
fn manifest_checksum_damage_is_corruption_in_any_record() {
    let dir = TempDir::new().unwrap();
    db_with_manifest_records(dir.path());
    let m = dir.path().join("MANIFEST");
    let good = fs::read(&m).unwrap();
    // Every byte after the 16-byte file header belongs to some record.
    for pos in 16..good.len() {
        let mut bad = good.clone();
        bad[pos] ^= 0x01;
        fs::write(&m, &bad).unwrap();
        let err = Db::open(dir.path(), small()).err();
        assert!(
            matches!(err, Some(Error::Corruption { .. })),
            "byte {pos}: {err:?}"
        );
        assert_eq!(fs::read(&m).unwrap(), bad, "manifest modified (byte {pos})");
    }
    fs::write(&m, &good).unwrap();
    open(dir.path());
}

#[test]
fn manifest_without_records_or_header_is_corruption() {
    for content in [Vec::new(), b"FKVLOG".to_vec()] {
        let dir = TempDir::new().unwrap();
        drop(open(dir.path()));
        let m = dir.path().join("MANIFEST");
        let header = fs::read(&m).unwrap()[..16].to_vec();
        for bytes in [content.clone(), header] {
            fs::write(&m, &bytes).unwrap();
            assert!(matches!(
                Db::open(dir.path(), small()).err(),
                Some(Error::Corruption { .. })
            ));
        }
    }
}

#[test]
fn referenced_sstable_missing_damaged_or_resized_is_corruption() {
    for damage in ["missing", "footer", "appended"] {
        let dir = TempDir::new().unwrap();
        let file_no;
        {
            let db = open(dir.path());
            db.put(b"k", b"v").unwrap();
            db.flush().unwrap();
            file_no = db.level_summary().unwrap().l0[0].file_no;
        }
        let p = dir.path().join(format!("{file_no:06}.sst"));
        match damage {
            "missing" => fs::remove_file(&p).unwrap(),
            "footer" => {
                let mut b = fs::read(&p).unwrap();
                let n = b.len();
                b[n - 10] ^= 0xFF;
                fs::write(&p, b).unwrap();
            }
            _ => {
                let mut b = fs::read(&p).unwrap();
                b.push(0);
                fs::write(&p, b).unwrap();
            }
        }
        let err = Db::open(dir.path(), small()).err();
        assert!(
            matches!(err, Some(Error::Corruption { .. })),
            "{damage}: {err:?}"
        );
    }
}

#[test]
fn manifest_is_rewritten_when_it_grows() {
    let dir = TempDir::new().unwrap();
    let opts = Options {
        manifest_max_bytes: 300,
        ..small()
    };
    {
        let db = Db::open(dir.path(), opts.clone()).unwrap();
        for i in 0..40 {
            db.put(&key(i), &val(i, 0)).unwrap();
            db.flush().unwrap();
            let len = fs::metadata(dir.path().join("MANIFEST")).unwrap().len();
            // One snapshot plus at most one appended flush record past the limit.
            assert!(
                len < 300 + 400 + 64 * (i as u64 + 2),
                "manifest at {len} bytes"
            );
        }
        db.check_invariants().unwrap();
    }
    let db = Db::open(dir.path(), opts).unwrap();
    assert_eq!(db.level_summary().unwrap().l0.len(), 40);
    for i in 0..40 {
        assert_eq!(db.get(&key(i)).unwrap(), Some(val(i, 0)));
    }
}

struct Rng(u64);
impl Rng {
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

/// Random puts/deletes/gets with explicit flushes and restarts, checked
/// against a `BTreeMap` model. `compact_pct` (per mille) adds explicit
/// compactions; a low `l0_compaction_trigger` in `opts` adds automatic ones.
fn model_test(seed: u64, steps: usize, opts: &Options, compact_pct: u64) {
    let dir = TempDir::new().unwrap();
    let mut db = Db::open(dir.path(), opts.clone()).unwrap();
    let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
    for step in 0..steps {
        let k = format!("k{:03}", rng.below(150)).into_bytes();
        let r = rng.below(1000);
        if r < 550 {
            let len = rng.below(120) as usize;
            let v: Vec<u8> = (0..len).map(|_| rng.next() as u8).collect();
            db.put(&k, &v).unwrap();
            model.insert(k, v);
        } else if r < 750 {
            db.delete(&k).unwrap();
            model.remove(&k);
        } else if r < 950 {
            assert_eq!(
                db.get(&k).unwrap().as_ref(),
                model.get(&k),
                "seed={seed} step={step}"
            );
        } else if r < 970 {
            db.flush().unwrap();
        } else if r < 970 + compact_pct {
            db.compact().unwrap();
            db.check_invariants().unwrap();
        } else if r < 985 + compact_pct {
            drop(db);
            db = Db::open(dir.path(), opts.clone()).unwrap();
        }
    }
    db.check_invariants().unwrap();
    for i in 0..150 {
        let k = format!("k{i:03}").into_bytes();
        assert_eq!(
            db.get(&k).unwrap().as_ref(),
            model.get(&k),
            "seed={seed} final"
        );
    }
    drop(db);
    let db = Db::open(dir.path(), opts.clone()).unwrap();
    for (k, v) in &model {
        assert_eq!(
            db.get(k).unwrap().as_ref(),
            Some(v),
            "seed={seed} after reopen"
        );
    }
}

#[test]
fn randomized_flush_and_restart_model() {
    for seed in 1..=4 {
        model_test(seed, 4000, &small(), 0);
    }
}

// ---------------------------------------------------------------- Phase 4

use forgekv::memtable::Entry;
use forgekv::sstable::SstReader;

fn sst_file(dir: &Path, n: u64) -> std::path::PathBuf {
    dir.join(format!("{n:06}.sst"))
}

/// Every entry stored in L1, read straight from the table files.
fn l1_entries(db: &Db, dir: &Path) -> Vec<(Vec<u8>, Entry)> {
    let mut out = vec![];
    for m in db.level_summary().unwrap().l1 {
        let r = SstReader::open(&sst_file(dir, m.file_no)).unwrap();
        out.extend(r.iter().map(|x| x.unwrap()));
    }
    out
}

fn sst_bytes(dir: &Path) -> u64 {
    fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap())
        .filter(|e| e.file_name().to_str().unwrap().ends_with(".sst"))
        .map(|e| e.metadata().unwrap().len())
        .sum()
}

#[test]
fn compaction_preserves_values_and_drops_tombstones() {
    let dir = TempDir::new().unwrap();
    let db = open(dir.path());
    let mut model = BTreeMap::new();
    for round in 0..4 {
        for i in (round..300).step_by(2) {
            if i % 5 == 0 {
                db.delete(&key(i)).unwrap();
                model.remove(&key(i));
            } else {
                db.put(&key(i), &val(i, round)).unwrap();
                model.insert(key(i), val(i, round));
            }
        }
        db.flush().unwrap();
    }
    assert!(db.level_summary().unwrap().l0.len() >= 4);
    let stats = db.compact().unwrap().unwrap();
    assert!(stats.tombstones_dropped > 0, "{stats:?}");
    assert_eq!(stats.entries_written, model.len() as u64);
    let s = db.level_summary().unwrap();
    assert!(s.l0.is_empty());
    assert!(!s.l1.is_empty());
    db.check_invariants().unwrap();
    for i in 0..300 {
        assert_eq!(db.get(&key(i)).unwrap().as_ref(), model.get(&key(i)), "{i}");
    }
    // L1 holds exactly the live data: no tombstones, no shadowed versions.
    let stored = l1_entries(&db, dir.path());
    assert!(stored.iter().all(|(_, e)| *e != Entry::Tombstone));
    let expected: Vec<(Vec<u8>, Entry)> = model
        .iter()
        .map(|(k, v)| (k.clone(), Entry::Value(v.clone())))
        .collect();
    assert_eq!(stored, expected);
    drop(db);
    let db = open(dir.path());
    for i in 0..300 {
        assert_eq!(db.get(&key(i)).unwrap().as_ref(), model.get(&key(i)), "{i}");
    }
}

#[test]
fn deleted_keys_never_resurrect() {
    // Value and tombstone in two L0 files.
    let dir = TempDir::new().unwrap();
    {
        let db = open(dir.path());
        db.put(b"k", b"1").unwrap();
        db.flush().unwrap();
        db.delete(b"k").unwrap();
        db.flush().unwrap();
        db.compact().unwrap();
        assert_eq!(db.get(b"k").unwrap(), None);
    }
    let db = open(dir.path());
    assert_eq!(db.get(b"k").unwrap(), None);
    drop(db);

    // Value already compacted into L1, tombstone arrives later via L0.
    let dir = TempDir::new().unwrap();
    {
        let db = open(dir.path());
        db.put(b"k", b"1").unwrap();
        db.put(b"z", b"keep").unwrap();
        db.flush().unwrap();
        db.compact().unwrap();
        assert_eq!(db.level_summary().unwrap().l1.len(), 1);
        db.delete(b"k").unwrap();
        db.flush().unwrap();
        assert_eq!(db.get(b"k").unwrap(), None, "L0 tombstone hides L1 value");
        db.compact().unwrap();
        assert_eq!(db.get(b"k").unwrap(), None);
        assert_eq!(db.get(b"z").unwrap(), Some(b"keep".to_vec()));
    }
    let db = open(dir.path());
    assert_eq!(db.get(b"k").unwrap(), None);

    // Tombstone still only in the WAL when the process stops.
    let dir = TempDir::new().unwrap();
    {
        let db = open(dir.path());
        db.put(b"k", b"1").unwrap();
        db.flush().unwrap();
        db.compact().unwrap();
        db.delete(b"k").unwrap();
    }
    let db = open(dir.path());
    assert_eq!(db.get(b"k").unwrap(), None);
    db.compact().unwrap();
    assert_eq!(db.get(b"k").unwrap(), None);
}

#[test]
fn overwrite_chain_across_flushes_keeps_only_the_newest_version() {
    let dir = TempDir::new().unwrap();
    let db = open(dir.path());
    for v in [b"v1", b"v2", b"v3"] {
        db.put(b"k", v).unwrap();
        db.flush().unwrap();
    }
    db.compact().unwrap();
    assert_eq!(db.get(b"k").unwrap(), Some(b"v3".to_vec()));
    assert_eq!(
        l1_entries(&db, dir.path()),
        vec![(b"k".to_vec(), Entry::Value(b"v3".to_vec()))]
    );
}

#[test]
fn outputs_are_split_and_non_overlapping() {
    let dir = TempDir::new().unwrap();
    let opts = Options {
        sst_target_bytes: 1024,
        ..small()
    };
    let db = Db::open(dir.path(), opts).unwrap();
    for i in 0..400 {
        db.put(&key(i), &val(i, 0)).unwrap();
    }
    db.flush().unwrap();
    let stats = db.compact().unwrap().unwrap();
    let l1 = db.level_summary().unwrap().l1;
    assert!(l1.len() >= 5, "expected several outputs, got {}", l1.len());
    assert_eq!(stats.output_files, l1.len());
    for w in l1.windows(2) {
        assert!(w[0].largest < w[1].smallest);
    }
    db.check_invariants().unwrap();
    for i in 0..400 {
        assert_eq!(db.get(&key(i)).unwrap(), Some(val(i, 0)));
    }
}

#[test]
fn only_overlapping_l1_files_are_rewritten() {
    let dir = TempDir::new().unwrap();
    let opts = Options {
        sst_target_bytes: 1024,
        ..small()
    };
    let db = Db::open(dir.path(), opts).unwrap();
    for i in 0..400 {
        db.put(&key(i), &val(i, 0)).unwrap();
    }
    db.flush().unwrap();
    db.compact().unwrap();
    let before = db.level_summary().unwrap().l1;
    assert!(before.len() >= 5);
    // Touch a narrow range in the middle.
    db.put(&key(200), b"new").unwrap();
    db.put(&key(201), b"new").unwrap();
    db.flush().unwrap();
    let stats = db.compact().unwrap().unwrap();
    assert!(stats.input_files <= 3, "{stats:?}");
    let after = db.level_summary().unwrap().l1;
    let kept = before
        .iter()
        .filter(|m| after.iter().any(|a| a.file_no == m.file_no))
        .count();
    assert!(kept >= before.len() - 2, "untouched L1 files must be kept");
    assert_eq!(db.get(&key(200)).unwrap(), Some(b"new".to_vec()));
    assert_eq!(db.get(&key(199)).unwrap(), Some(val(199, 0)));
    db.check_invariants().unwrap();
}

#[test]
fn compacting_only_tombstones_leaves_no_tables() {
    let dir = TempDir::new().unwrap();
    let db = open(dir.path());
    for i in 0..50 {
        db.put(&key(i), b"v").unwrap();
    }
    db.flush().unwrap();
    db.compact().unwrap();
    for i in 0..50 {
        db.delete(&key(i)).unwrap();
    }
    db.flush().unwrap();
    let stats = db.compact().unwrap().unwrap();
    assert_eq!(stats.output_files, 0);
    let s = db.level_summary().unwrap();
    assert!(s.l0.is_empty() && s.l1.is_empty());
    assert_eq!(sst_bytes(dir.path()), 0);
    db.check_invariants().unwrap();
    assert_eq!(db.compact().unwrap(), None, "nothing to compact");
}

#[test]
fn delete_heavy_workload_reclaims_space() {
    let dir = TempDir::new().unwrap();
    let db = open(dir.path());
    for i in 0..1000 {
        db.put(&key(i), &val(i, 0)).unwrap();
    }
    db.flush().unwrap();
    db.compact().unwrap();
    for i in 0..1000 {
        if i % 10 != 0 {
            db.delete(&key(i)).unwrap();
        }
    }
    db.flush().unwrap();
    let before = sst_bytes(dir.path());
    db.compact().unwrap();
    let after = sst_bytes(dir.path());
    assert!(after < before, "{after} >= {before}");
    assert!(
        after * 4 < before,
        "expected most space back: {after} vs {before}"
    );
    for i in 0..1000 {
        let want = (i % 10 == 0).then(|| val(i, 0));
        assert_eq!(db.get(&key(i)).unwrap(), want);
    }
}

#[test]
fn automatic_compaction_bounds_l0() {
    let dir = TempDir::new().unwrap();
    let opts = Options {
        l0_compaction_trigger: 4,
        ..small()
    };
    let db = Db::open(dir.path(), opts.clone()).unwrap();
    for i in 0..1500 {
        db.put(&key(i % 500), &val(i, i / 500)).unwrap();
        // Since Phase 5 compaction runs in the background: the flush that
        // brings L0 *to* the trigger is visible before the worker compacts,
        // and the worker always compacts before the next flush, so L0 never
        // exceeds the trigger. (Phase 4, compacting inline, kept it below.)
        assert!(db.level_summary().unwrap().l0.len() <= opts.l0_compaction_trigger);
    }
    assert!(!db.level_summary().unwrap().l1.is_empty());
    db.check_invariants().unwrap();
    for i in 1000..1500 {
        assert_eq!(db.get(&key(i % 500)).unwrap(), Some(val(i, 2)));
    }
}

#[test]
fn open_compacts_when_l0_is_over_the_trigger() {
    let dir = TempDir::new().unwrap();
    {
        let db = open(dir.path()); // trigger effectively off
        for r in 0..6 {
            db.put(&key(r), b"v").unwrap();
            db.flush().unwrap();
        }
        assert_eq!(db.level_summary().unwrap().l0.len(), 6);
    }
    let opts = Options {
        l0_compaction_trigger: 4,
        ..small()
    };
    let db = Db::open(dir.path(), opts).unwrap();
    let s = db.level_summary().unwrap();
    assert!(s.l0.is_empty() && !s.l1.is_empty());
    for r in 0..6 {
        assert_eq!(db.get(&key(r)).unwrap(), Some(b"v".to_vec()));
    }
    db.check_invariants().unwrap();
}

#[test]
fn randomized_model_with_explicit_compaction() {
    for seed in 10..=13 {
        model_test(seed, 4000, &small(), 15);
    }
}

#[test]
fn randomized_model_with_automatic_compaction() {
    let opts = Options {
        l0_compaction_trigger: 3,
        sst_target_bytes: 1024,
        ..small()
    };
    for seed in 20..=23 {
        model_test(seed, 5000, &opts, 0);
    }
}
