//! SSTable integration tests: round trips, builder rules, truncation and
//! corruption sweeps, and hand-crafted malformed files.

mod common;

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use forgekv::memtable::Entry;
use forgekv::sstable::{SstBuilder, SstReader, TableMeta};
use forgekv::{Error, Options};
use tempfile::TempDir;

fn small_blocks() -> Options {
    Options {
        block_bytes: 128,
        ..Options::default()
    }
}

fn build(path: &Path, opts: &Options, entries: &BTreeMap<Vec<u8>, Entry>) -> TableMeta {
    let mut b = SstBuilder::new(path, opts).unwrap();
    for (k, e) in entries {
        b.add(k, e).unwrap();
    }
    b.finish().unwrap()
}

fn sample(n: usize) -> BTreeMap<Vec<u8>, Entry> {
    let mut m = BTreeMap::new();
    for i in 0..n {
        let key = format!("key-{i:05}").into_bytes();
        let entry = match i % 5 {
            0 => Entry::Tombstone,
            1 => Entry::Value(Vec::new()),
            _ => Entry::Value(format!("value-{i}").repeat(1 + i % 7).into_bytes()),
        };
        m.insert(key, entry);
    }
    m
}

fn collect(r: &SstReader) -> Vec<(Vec<u8>, Entry)> {
    r.iter().map(|x| x.unwrap()).collect()
}

fn is_corruption<T: std::fmt::Debug>(r: &Result<T, Error>) -> bool {
    matches!(r, Err(Error::Corruption { .. }))
}

#[test]
fn golden_bytes_match_the_specification() {
    // Computed independently with Python/zlib from the spec in sstable/mod.rs.
    const GOLDEN: &str = "010100000001000000613102010000000000000062739cf0ff01000000010000006101000000620000000000000000190000000200000032087e7f1900000000000000220000000000000000000000000000000000000000000000020000000000000001000000464b565353540001cc37e3fe";
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("g.sst");
    let mut m = BTreeMap::new();
    m.insert(b"a".to_vec(), Entry::Value(b"1".to_vec()));
    m.insert(b"b".to_vec(), Entry::Tombstone);
    let meta = build(&path, &Options::default(), &m);
    let hex: String = fs::read(&path)
        .unwrap()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    assert_eq!(hex, GOLDEN);
    assert_eq!(meta.size, 115);
    assert_eq!(meta.blocks, 1);
}

#[test]
fn independent_encoder_produces_a_readable_table() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("t.sst");
    fs::write(&path, common::baseline()).unwrap();
    let r = SstReader::open(&path).unwrap();
    assert_eq!(r.get(b"a").unwrap(), Some(Entry::Value(b"1".to_vec())));
    assert_eq!(r.get(b"b").unwrap(), Some(Entry::Value(b"2".to_vec())));
    assert_eq!(r.verify().unwrap(), 2);
}

#[test]
fn round_trip_get_iter_and_meta() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("t.sst");
    let m = sample(500);
    let meta = build(&path, &small_blocks(), &m);
    assert_eq!(meta.entries, 500);
    assert!(
        meta.blocks > 10,
        "expected many blocks, got {}",
        meta.blocks
    );
    assert_eq!(meta.smallest, b"key-00000");
    assert_eq!(meta.largest, b"key-00499");
    assert_eq!(meta.size, fs::metadata(&path).unwrap().len());

    let r = SstReader::open(&path).unwrap();
    assert_eq!(r.entry_count(), 500);
    assert_eq!(r.block_count(), meta.blocks);
    assert_eq!(r.smallest(), b"key-00000");
    assert_eq!(r.largest(), b"key-00499");
    for (k, e) in &m {
        assert_eq!(
            r.get(k).unwrap().as_ref(),
            Some(e),
            "{:?}",
            String::from_utf8_lossy(k)
        );
    }
    let expected: Vec<_> = m.iter().map(|(k, e)| (k.clone(), e.clone())).collect();
    assert_eq!(collect(&r), expected, "iteration is sorted and complete");
    assert_eq!(r.verify().unwrap(), 500);
}

#[test]
fn missing_tombstone_empty_and_value_are_distinct() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("t.sst");
    let mut m = BTreeMap::new();
    m.insert(b"b-value".to_vec(), Entry::Value(b"v".to_vec()));
    m.insert(b"c-empty".to_vec(), Entry::Value(Vec::new()));
    m.insert(b"d-deleted".to_vec(), Entry::Tombstone);
    build(&path, &Options::default(), &m);
    let r = SstReader::open(&path).unwrap();
    assert_eq!(
        r.get(b"b-value").unwrap(),
        Some(Entry::Value(b"v".to_vec()))
    );
    assert_eq!(r.get(b"c-empty").unwrap(), Some(Entry::Value(Vec::new())));
    assert_eq!(r.get(b"d-deleted").unwrap(), Some(Entry::Tombstone));
    assert_eq!(r.get(b"c-missing").unwrap(), None);
}

#[test]
fn absent_keys_before_between_after_and_at_block_boundaries() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("t.sst");
    // Even numbers present; odd numbers absent.
    let m: BTreeMap<Vec<u8>, Entry> = (0..300)
        .map(|i| {
            (
                format!("k{:04}", i * 2).into_bytes(),
                Entry::Value(vec![1; 10]),
            )
        })
        .collect();
    build(&path, &small_blocks(), &m);
    let r = SstReader::open(&path).unwrap();
    assert_eq!(r.get(b"").unwrap(), None);
    assert_eq!(r.get(b"a").unwrap(), None, "before first");
    assert_eq!(r.get(b"z").unwrap(), None, "after last");
    assert_eq!(r.get(b"k0598x").unwrap(), None, "just after last");
    for i in 0..300 {
        let absent = format!("k{:04}", i * 2 + 1);
        assert_eq!(r.get(absent.as_bytes()).unwrap(), None, "{absent}");
        let prefix_of_present = format!("k{:04}0", i * 2); // longer than a present key
        assert_eq!(r.get(prefix_of_present.as_bytes()).unwrap(), None);
    }
}

#[test]
fn single_entry_tombstone_only_and_oversized_entries() {
    let dir = TempDir::new().unwrap();
    let opts = small_blocks();

    let p = dir.path().join("one.sst");
    let mut m = BTreeMap::new();
    m.insert(b"only".to_vec(), Entry::Value(b"x".to_vec()));
    build(&p, &opts, &m);
    let r = SstReader::open(&p).unwrap();
    assert_eq!((r.smallest(), r.largest()), (&b"only"[..], &b"only"[..]));
    assert_eq!(r.get(b"only").unwrap(), Some(Entry::Value(b"x".to_vec())));

    let p = dir.path().join("tombs.sst");
    let m: BTreeMap<_, _> = (0..50)
        .map(|i| (format!("t{i:02}").into_bytes(), Entry::Tombstone))
        .collect();
    build(&p, &opts, &m);
    let r = SstReader::open(&p).unwrap();
    assert!(r.iter().all(|x| x.unwrap().1 == Entry::Tombstone));
    assert_eq!(r.verify().unwrap(), 50);

    // Entries far larger than the 128-byte block target, plus max-size key/value.
    let p = dir.path().join("big.sst");
    let o = Options::default();
    let mut m = BTreeMap::new();
    m.insert(b"a".to_vec(), Entry::Value(vec![1; 10_000]));
    m.insert(b"b".to_vec(), Entry::Value(vec![2; 3]));
    m.insert(
        vec![b'c'; o.max_key_len],
        Entry::Value(vec![3; o.max_value_len]),
    );
    m.insert(b"d".to_vec(), Entry::Value(vec![4; 200_000]));
    build(&p, &opts_with_small_blocks(&o), &m);
    let r = SstReader::open(&p).unwrap();
    for (k, e) in &m {
        assert_eq!(r.get(k).unwrap().as_ref(), Some(e));
    }
    assert_eq!(r.verify().unwrap(), 4);
}

fn opts_with_small_blocks(o: &Options) -> Options {
    Options {
        block_bytes: 128,
        ..o.clone()
    }
}

#[test]
fn builder_rejects_bad_input_and_stays_usable() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("t.sst");
    let o = Options::default();
    let mut b = SstBuilder::new(&path, &o).unwrap();
    let v = Entry::Value(b"v".to_vec());
    b.add(b"b", &v).unwrap();
    for (what, r) in [
        ("duplicate", b.add(b"b", &v)),
        ("descending", b.add(b"a", &v)),
        ("empty key", b.add(b"", &v)),
        ("long key", b.add(&vec![b'k'; o.max_key_len + 1], &v)),
        (
            "long value",
            b.add(b"c", &Entry::Value(vec![0; o.max_value_len + 1])),
        ),
    ] {
        assert!(matches!(r, Err(Error::InvalidArgument(_))), "{what}: {r:?}");
    }
    b.add(b"c", &Entry::Tombstone).unwrap();
    let meta = b.finish().unwrap();
    assert_eq!(meta.entries, 2, "rejected adds wrote nothing");
    let r = SstReader::open(&path).unwrap();
    assert_eq!(r.verify().unwrap(), 2);
    assert_eq!(r.get(b"a").unwrap(), None);
}

#[test]
fn empty_table_is_refused_and_leaves_no_file() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("t.sst");
    let b = SstBuilder::new(&path, &Options::default()).unwrap();
    assert!(matches!(b.finish(), Err(Error::InvalidArgument(_))));
    assert!(!path.exists(), "partial file must be removed");
}

#[test]
fn dropping_an_unfinished_builder_removes_its_file() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("t.sst");
    {
        let mut b = SstBuilder::new(&path, &small_blocks()).unwrap();
        for (k, e) in &sample(100) {
            b.add(k, e).unwrap();
        }
    }
    assert!(!path.exists());
}

#[test]
fn builder_will_not_overwrite_an_existing_file() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("t.sst");
    fs::write(&path, b"precious").unwrap();
    assert!(SstBuilder::new(&path, &Options::default()).is_err());
    assert_eq!(fs::read(&path).unwrap(), b"precious");
}

#[test]
fn reader_rejects_empty_and_tiny_files_and_empty_index() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("t.sst");
    for len in [0usize, 1, 55] {
        fs::write(&path, vec![0u8; len]).unwrap();
        assert!(
            is_corruption(&SstReader::open(&path).map(|_| ())),
            "len={len}"
        );
    }
    // A structurally valid footer over an index with zero entries.
    let idx = common::index_raw(0, 1, b"a", &[]);
    fs::write(&path, common::assemble(&[], &idx, 0)).unwrap();
    assert!(is_corruption(&SstReader::open(&path).map(|_| ())));
    assert!(SstReader::open(&dir.path().join("missing.sst")).is_err());
}

fn build_sweep_table(dir: &Path) -> (Vec<u8>, BTreeMap<Vec<u8>, Entry>) {
    let path = dir.join("sweep.sst");
    let m = sample(100);
    build(&path, &small_blocks(), &m);
    (fs::read(&path).unwrap(), m)
}

#[test]
fn truncation_at_every_length_is_corruption() {
    let dir = TempDir::new().unwrap();
    let (bytes, _) = build_sweep_table(dir.path());
    let path = dir.path().join("cut.sst");
    for len in 0..bytes.len() {
        fs::write(&path, &bytes[..len]).unwrap();
        let r = SstReader::open(&path).map(|_| ());
        assert!(is_corruption(&r), "len={len}: {r:?}");
    }
}

#[test]
fn appended_garbage_is_corruption() {
    let dir = TempDir::new().unwrap();
    let (bytes, _) = build_sweep_table(dir.path());
    let path = dir.path().join("long.sst");
    for extra in [1usize, 7, 56, 200] {
        let mut b = bytes.clone();
        b.extend(std::iter::repeat_n(0u8, extra));
        fs::write(&path, &b).unwrap();
        let r = SstReader::open(&path).map(|_| ());
        assert!(is_corruption(&r), "extra={extra}: {r:?}");
    }
}

/// Every byte of the file is covered by a checksum (data blocks by their crc,
/// the index by its crc, the footer by its crc), so any single-bit flip must be
/// detected by `open` + `verify`, and `get` must never return wrong data.
#[test]
fn single_bit_flip_in_any_byte_is_detected_and_never_returns_wrong_data() {
    let dir = TempDir::new().unwrap();
    let (bytes, model) = build_sweep_table(dir.path());
    let path = dir.path().join("flip.sst");
    // Bytes at or after `index_start` are the index block and footer.
    let index_start = bytes.len() - 56 - index_len(&bytes);
    let keys: Vec<&Vec<u8>> = model.keys().step_by(7).collect();

    for pos in 0..bytes.len() {
        // All 8 bits for the index and footer; one varying bit for data blocks.
        let bits: Vec<u8> = if pos >= index_start {
            (0..8).collect()
        } else {
            vec![(pos % 8) as u8]
        };
        for bit in bits {
            let mut bad = bytes.clone();
            bad[pos] ^= 1 << bit;
            fs::write(&path, &bad).unwrap();

            let opened = SstReader::open(&path);
            let verdict = opened
                .as_ref()
                .map_err(|_| ())
                .and_then(|r| r.verify().map_err(|_| ()));
            assert!(verdict.is_err(), "undetected flip at byte {pos} bit {bit}");
            if let Ok(r) = &opened {
                for k in &keys {
                    match r.get(k) {
                        Err(_) => {}
                        Ok(got) => assert_eq!(
                            got.as_ref(),
                            model.get(*k),
                            "wrong data returned at byte {pos} bit {bit}"
                        ),
                    }
                }
            }
            if let Err(e) = &opened {
                assert!(
                    matches!(e, Error::Corruption { .. }),
                    "byte {pos} bit {bit}: {e}"
                );
            }
        }
    }
}

/// Size of the index block, read from the (valid) footer.
fn index_len(bytes: &[u8]) -> usize {
    let f = &bytes[bytes.len() - 56..];
    u64::from_le_bytes(f[8..16].try_into().unwrap()) as usize
}

#[test]
fn crafted_malformed_files_are_rejected_with_the_expected_error() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("evil.sst");
    for case in common::malicious_cases() {
        fs::write(&path, &case.bytes).unwrap();
        let err = SstReader::open(&path)
            .and_then(|r| r.verify())
            .expect_err(case.name);
        assert!(
            matches!(err, Error::Corruption { .. }),
            "{}: wrong error kind: {err}",
            case.name
        );
        assert!(
            err.to_string().contains(case.expect),
            "{}: expected {:?} in {:?}",
            case.name,
            case.expect,
            err.to_string()
        );
    }
}

#[test]
fn unsupported_footer_version_is_reported_as_such() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("v.sst");
    let blk = common::baseline_block();
    let idx = common::index_ok(b"a", &[(1, b"b", 0, blk.len() as u32, 2)]);
    for version in [0u32, 2, 99] {
        let mut f = blk.clone();
        f.extend(&idx);
        f.extend(common::footer_raw(
            blk.len() as u64,
            idx.len() as u64,
            0,
            0,
            2,
            version,
            common::MAGIC,
        ));
        fs::write(&path, &f).unwrap();
        assert!(matches!(
            SstReader::open(&path).map(|_| ()),
            Err(Error::UnsupportedVersion { version: v, .. }) if v == version
        ));
    }
}

#[test]
fn randomized_round_trips_match_a_model() {
    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
    }
    let dir = TempDir::new().unwrap();
    for (seed, block_bytes) in [(1u64, 64usize), (2, 200), (3, 4096), (4, 1)] {
        let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15));
        let mut m = BTreeMap::new();
        for _ in 0..400 {
            let klen = 1 + (rng.next() % 12) as usize;
            let key: Vec<u8> = (0..klen).map(|_| (rng.next() % 4) as u8).collect();
            let e = match rng.next() % 4 {
                0 => Entry::Tombstone,
                1 => Entry::Value(Vec::new()),
                _ => Entry::Value((0..rng.next() % 300).map(|_| rng.next() as u8).collect()),
            };
            m.insert(key, e);
        }
        let path = dir.path().join(format!("r{seed}.sst"));
        let o = Options {
            block_bytes,
            ..Options::default()
        };
        build(&path, &o, &m);
        let r = SstReader::open(&path).unwrap();
        assert_eq!(r.verify().unwrap(), m.len() as u64);
        for (k, e) in &m {
            assert_eq!(r.get(k).unwrap().as_ref(), Some(e), "seed={seed}");
        }
        let expected: Vec<_> = m.iter().map(|(k, e)| (k.clone(), e.clone())).collect();
        assert_eq!(collect(&r), expected, "seed={seed}");
        // Keys not in the model are absent.
        for _ in 0..200 {
            let klen = 1 + (rng.next() % 14) as usize;
            let key: Vec<u8> = (0..klen).map(|_| (rng.next() % 5) as u8).collect();
            assert_eq!(r.get(&key).unwrap().as_ref(), m.get(&key), "seed={seed}");
        }
    }
}
