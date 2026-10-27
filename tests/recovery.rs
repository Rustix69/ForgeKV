//! On-disk recovery tests: incomplete tails, checksum corruption, bad headers.
//! Each test builds a WAL with the real engine, damages the bytes, installs
//! them as the live WAL of a fresh database, and reopens.
//!
//! Since Phase 3, a successful open replays the WAL into a new L0 table and
//! retires the WAL (it is never truncated and appended to again), so "the
//! tail was repaired" is checked as "the data is right and the old WAL is
//! gone", and a second open replays nothing.

use std::fs;
use std::path::{Path, PathBuf};

use forgekv::logfile::{FRAME_HEADER_LEN, HEADER_LEN, encode_frame, header_crc};
use forgekv::wal::{encode_put, max_record_size};
use forgekv::{Db, Error, Options, RecoveryMode};
use tempfile::TempDir;

fn wal_path(dir: &Path) -> PathBuf {
    dir.join("wal-000001.log")
}

/// Write `n` records "k0=v0".. and return the WAL bytes.
fn build_wal(n: usize) -> Vec<u8> {
    let dir = TempDir::new().unwrap();
    let db = Db::open(dir.path(), Options::default()).unwrap();
    for i in 0..n {
        db.put(format!("k{i}").as_bytes(), format!("v{i}").as_bytes())
            .unwrap();
    }
    drop(db);
    fs::read(wal_path(dir.path())).unwrap()
}

/// A fresh database (MANIFEST with log number 1) whose live WAL 1 holds `bytes`.
fn dir_with_wal(bytes: &[u8]) -> TempDir {
    let dir = TempDir::new().unwrap();
    drop(Db::open(dir.path(), Options::default()).unwrap());
    assert!(wal_path(dir.path()).exists());
    fs::write(wal_path(dir.path()), bytes).unwrap();
    dir
}

fn salvage() -> Options {
    Options {
        recovery_mode: RecoveryMode::SalvagePrefix,
        ..Options::default()
    }
}

fn assert_keys(db: &Db, present: usize, absent_from: usize) {
    for i in 0..present {
        assert_eq!(
            db.get(format!("k{i}").as_bytes()).unwrap(),
            Some(format!("v{i}").into_bytes()),
            "k{i}"
        );
    }
    for i in present..absent_from {
        assert_eq!(db.get(format!("k{i}").as_bytes()).unwrap(), None, "k{i}");
    }
}

/// Size of the frame holding record `k{i}=v{i}` (single-digit i).
fn frame_len() -> usize {
    FRAME_HEADER_LEN + encode_put(b"k0", b"v0").len()
}

#[test]
fn truncation_at_every_offset_of_last_record_recovers_prior_state() {
    let full = build_wal(5);
    let boundary = full.len() - frame_len();
    for cut in 0..frame_len() {
        let dir = dir_with_wal(&full[..boundary + cut]);
        let db = Db::open(dir.path(), Options::default()).unwrap();
        assert_keys(&db, 4, 5);
        let rep = db.recovery_report();
        assert_eq!(rep.records_replayed, 4, "cut={cut}");
        assert_eq!(rep.discarded_bytes, cut as u64, "cut={cut}");
        assert!(rep.flushed_on_open);
        drop(db);
        // The damaged WAL was retired; a second open sees the same state.
        assert!(!wal_path(dir.path()).exists(), "cut={cut}");
        let db = Db::open(dir.path(), Options::default()).unwrap();
        assert_keys(&db, 4, 5);
        assert_eq!(db.recovery_report().records_replayed, 0);
    }
}

#[test]
fn writes_after_torn_tail_recovery_are_durable_and_replayable() {
    let full = build_wal(3);
    let dir = dir_with_wal(&full[..full.len() - 3]);
    {
        let db = Db::open(dir.path(), Options::default()).unwrap();
        assert_keys(&db, 2, 3);
        db.put(b"after", b"crash").unwrap();
    }
    let db = Db::open(dir.path(), Options::default()).unwrap();
    assert_keys(&db, 2, 3);
    assert_eq!(db.get(b"after").unwrap(), Some(b"crash".to_vec()));
    assert_eq!(db.recovery_report().discarded_bytes, 0);
}

#[test]
fn truncation_at_every_offset_across_all_records_is_a_prefix() {
    let full = build_wal(6);
    for cut in HEADER_LEN as usize..=full.len() {
        let dir = dir_with_wal(&full[..cut]);
        let db = Db::open(dir.path(), Options::default()).unwrap();
        let complete = (cut - HEADER_LEN as usize) / frame_len();
        assert_keys(&db, complete, 6);
        assert_eq!(db.recovery_report().records_replayed, complete as u64);
    }
}

#[test]
fn incomplete_header_means_interrupted_creation_and_is_discarded() {
    let full = build_wal(1);
    for cut in 0..HEADER_LEN as usize {
        let dir = dir_with_wal(&full[..cut]);
        {
            let db = Db::open(dir.path(), Options::default()).unwrap();
            assert_eq!(
                db.recovery_report().discarded_bytes,
                cut as u64,
                "cut={cut}"
            );
            assert_eq!(db.recovery_report().records_replayed, 0);
            assert_eq!(db.get(b"k0").unwrap(), None);
            db.put(b"new", b"1").unwrap();
        }
        let db = Db::open(dir.path(), Options::default()).unwrap();
        assert_eq!(db.get(b"new").unwrap(), Some(b"1".to_vec()), "cut={cut}");
    }
}

#[test]
fn mid_log_bit_flip_is_corruption_and_file_is_untouched() {
    let full = build_wal(5);
    let mut bad = full.clone();
    let pos = HEADER_LEN as usize + frame_len() * 2 + FRAME_HEADER_LEN + 3;
    bad[pos] ^= 0x04;
    let dir = dir_with_wal(&bad);
    let err = Db::open(dir.path(), Options::default()).err().unwrap();
    match err {
        Error::Corruption { offset, .. } => {
            assert_eq!(offset, HEADER_LEN + 2 * frame_len() as u64)
        }
        other => panic!("expected corruption, got {other}"),
    }
    assert_eq!(fs::read(wal_path(dir.path())).unwrap(), bad);
}

#[test]
fn any_damage_in_final_complete_record_is_corruption_not_a_torn_tail() {
    let full = build_wal(5);
    let last = full.len() - frame_len();
    // Every bit of every byte of the final record: header fields and payload.
    for pos in last..full.len() {
        for bit in 0..8 {
            let mut bad = full.clone();
            bad[pos] ^= 1 << bit;
            let dir = dir_with_wal(&bad);
            let err = Db::open(dir.path(), Options::default()).err().unwrap();
            assert!(
                matches!(err, Error::Corruption { .. }),
                "pos={pos} bit={bit}: {err}"
            );
            assert_eq!(
                fs::read(wal_path(dir.path())).unwrap(),
                bad,
                "strict mode must not modify the file (pos={pos} bit={bit})"
            );
        }
    }
}

#[test]
fn salvage_prefix_drops_damaged_final_record_and_reports_it() {
    let full = build_wal(5);
    let mut bad = full.clone();
    let last = bad.len() - 1;
    bad[last] ^= 0xFF;
    let dir = dir_with_wal(&bad);
    {
        let db = Db::open(dir.path(), salvage()).unwrap();
        assert_keys(&db, 4, 5);
        let rep = db.recovery_report();
        assert!(rep.salvaged.is_some());
        assert_eq!(rep.discarded_bytes, frame_len() as u64);
        db.put(b"x", b"y").unwrap();
    }
    // After salvage the log is clean and opens in strict mode.
    let db = Db::open(dir.path(), Options::default()).unwrap();
    assert_keys(&db, 4, 5);
    assert_eq!(db.get(b"x").unwrap(), Some(b"y".to_vec()));
    assert!(db.recovery_report().salvaged.is_none());
}

#[test]
fn salvage_prefix_drops_everything_from_mid_log_corruption() {
    let full = build_wal(5);
    let mut bad = full.clone();
    bad[HEADER_LEN as usize + frame_len() * 2 + FRAME_HEADER_LEN] ^= 1;
    let dir = dir_with_wal(&bad);
    let db = Db::open(dir.path(), salvage()).unwrap();
    assert_keys(&db, 2, 5);
    assert_eq!(db.recovery_report().discarded_bytes, 3 * frame_len() as u64);
}

#[test]
fn zero_filled_or_garbage_tail_is_corruption() {
    let full = build_wal(3);
    for tail in [vec![0u8; 64], vec![0xAB; 64]] {
        let mut bad = full.clone();
        bad.extend_from_slice(&tail);
        let dir = dir_with_wal(&bad);
        let err = Db::open(dir.path(), Options::default()).err().unwrap();
        assert!(matches!(err, Error::Corruption { .. }), "{err}");
        assert_eq!(fs::read(wal_path(dir.path())).unwrap(), bad);
    }
}

/// Append a frame header with the given `len` and a good or damaged header crc.
fn append_header(bytes: &mut Vec<u8>, len: u32, good_header_crc: bool) {
    let len_le = len.to_le_bytes();
    let crc = if good_header_crc {
        header_crc(len_le)
    } else {
        header_crc(len_le) ^ 1
    };
    bytes.extend_from_slice(&len_le);
    bytes.extend_from_slice(&crc.to_le_bytes());
    bytes.extend_from_slice(&0u32.to_le_bytes());
}

#[test]
fn huge_length_field_is_corruption_without_allocation() {
    for good_crc in [true, false] {
        for len in [0u32, u32::MAX, 2 * 1024 * 1024] {
            let mut bad = build_wal(2);
            append_header(&mut bad, len, good_crc);
            bad.extend_from_slice(b"xxxx");
            let dir = dir_with_wal(&bad);
            let err = Db::open(dir.path(), Options::default()).err().unwrap();
            assert!(
                matches!(err, Error::Corruption { .. }),
                "len={len} good_crc={good_crc}: {err}"
            );
        }
    }
}

#[test]
fn verified_length_beyond_eof_is_a_torn_tail_but_damaged_length_is_corruption() {
    // A verified in-range length with a short payload is a legitimate torn
    // write. The same shape with a damaged header checksum (v1's blind spot,
    // e.g. a bit-flipped length) is now Corruption.
    let mut torn = build_wal(2);
    let good_len = torn.len();
    append_header(&mut torn, 1000, true);
    torn.extend_from_slice(b"short");
    let dir = dir_with_wal(&torn);
    let db = Db::open(dir.path(), Options::default()).unwrap();
    assert_keys(&db, 2, 2);
    assert_eq!(
        db.recovery_report().discarded_bytes,
        (torn.len() - good_len) as u64
    );
    drop(db);

    let mut damaged = build_wal(2);
    append_header(&mut damaged, 1000, false);
    damaged.extend_from_slice(b"short");
    let dir = dir_with_wal(&damaged);
    assert!(matches!(
        Db::open(dir.path(), Options::default()).err().unwrap(),
        Error::Corruption { .. }
    ));
}

#[test]
fn truncated_final_frame_header_cases() {
    // 1..=7 header bytes: unverifiable, torn. 8..=11 with intact bytes: torn.
    // 8..=11 with a damaged header_crc: Corruption.
    let full = build_wal(3);
    let boundary = full.len() - frame_len();
    for cut in 1..FRAME_HEADER_LEN {
        let dir = dir_with_wal(&full[..boundary + cut]);
        let db = Db::open(dir.path(), Options::default()).unwrap();
        assert_keys(&db, 2, 3);
        assert_eq!(db.recovery_report().discarded_bytes, cut as u64);
        drop(db);
        if cut >= 8 {
            let mut bad = full[..boundary + cut].to_vec();
            bad[boundary + 5] ^= 0x01;
            let dir = dir_with_wal(&bad);
            assert!(matches!(
                Db::open(dir.path(), Options::default()).err().unwrap(),
                Error::Corruption { .. }
            ));
        }
    }
}

#[test]
fn format_version_1_files_are_rejected_as_unsupported() {
    let mut old = build_wal(1);
    old[9] = 1;
    let dir = dir_with_wal(&old);
    assert!(matches!(
        Db::open(dir.path(), Options::default()).err().unwrap(),
        Error::UnsupportedVersion { version: 1, .. }
    ));
}

#[test]
fn bad_magic_kind_and_version_are_errors() {
    let full = build_wal(1);
    let mut magic = full.clone();
    magic[0] = b'X';
    let dir = dir_with_wal(&magic);
    assert!(matches!(
        Db::open(dir.path(), Options::default()).err().unwrap(),
        Error::Corruption { .. }
    ));
    // Salvage must not paper over a damaged header.
    assert!(matches!(
        Db::open(dir.path(), salvage()).err().unwrap(),
        Error::Corruption { .. }
    ));

    let mut kind = full.clone();
    kind[8] = 2; // manifest
    let dir = dir_with_wal(&kind);
    assert!(matches!(
        Db::open(dir.path(), Options::default()).err().unwrap(),
        Error::Corruption { .. }
    ));

    let mut version = full.clone();
    version[9] = 99;
    let dir = dir_with_wal(&version);
    assert!(matches!(
        Db::open(dir.path(), Options::default()).err().unwrap(),
        Error::UnsupportedVersion { version: 99, .. }
    ));
}

#[test]
fn checksummed_but_undecodable_records_are_corruption() {
    let opts = Options::default();
    let max = max_record_size(&opts);
    let payloads: Vec<Vec<u8>> = vec![
        vec![9, 1, 0, 0, 0, b'k'],                  // unknown type
        vec![1, 0, 0, 0, 0, 0, 0, 0, 0],            // empty key
        vec![2, 5, 0, 0, 0, b'k'],                  // key longer than payload
        [encode_put(b"k", b"v"), vec![0]].concat(), // trailing byte
    ];
    for payload in payloads {
        let mut bytes = build_wal(1);
        bytes.extend(encode_frame(&payload, max).unwrap());
        let dir = dir_with_wal(&bytes);
        let err = Db::open(dir.path(), opts.clone()).err().unwrap();
        assert!(
            matches!(err, Error::Corruption { .. }),
            "{payload:?}: {err}"
        );
    }
}

#[test]
fn empty_wal_file_is_discarded() {
    let dir = dir_with_wal(&[]);
    let db = Db::open(dir.path(), Options::default()).unwrap();
    assert_eq!(db.recovery_report().records_replayed, 0);
    assert!(!db.recovery_report().flushed_on_open);
}

#[test]
fn recovery_is_deterministic_and_idempotent() {
    let full = build_wal(4);
    let dir = dir_with_wal(&full[..full.len() - 5]);
    let twin = dir_with_wal(&full[..full.len() - 5]);
    let first = {
        let db = Db::open(dir.path(), Options::default()).unwrap();
        assert_keys(&db, 3, 4);
        db.recovery_report().clone()
    };
    assert_eq!(first.records_replayed, 3);
    // Same bytes, same outcome.
    let twin_report = Db::open(twin.path(), Options::default())
        .unwrap()
        .recovery_report()
        .clone();
    assert_eq!(twin_report, first);
    // Reopening after recovery is a no-op for the data.
    let db = Db::open(dir.path(), Options::default()).unwrap();
    assert_keys(&db, 3, 4);
    assert_eq!(db.recovery_report().records_replayed, 0);
    assert_eq!(db.recovery_report().discarded_bytes, 0);
}

/// Copy every regular file of `from` into the (existing) directory `to`.
#[cfg(feature = "failpoints")]
fn copy_files(from: &Path, to: &Path) {
    for e in fs::read_dir(from).unwrap() {
        let e = e.unwrap();
        if e.file_type().unwrap().is_file() && e.file_name() != "LOCK" {
            fs::copy(e.path(), to.join(e.file_name())).unwrap();
        }
    }
}

#[cfg(feature = "failpoints")]
fn the_wal(dir: &Path) -> PathBuf {
    let wals: Vec<PathBuf> = fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.file_name().unwrap().to_str().unwrap().starts_with("wal-"))
        .collect();
    assert_eq!(wals.len(), 1, "{wals:?}");
    wals.into_iter().next().unwrap()
}

/// Phase 8: simulated loss of unsynced WAL data (what a power loss could
/// leave under `SyncMode::Never`). Batch A is flushed to an SST (synced) and
/// the flush rotates to a new WAL. Batch B is then written without a sync.
/// The engine reports the live WAL's synced and written offsets
/// (`Db::wal_offsets`, feature `failpoints`); truncating the WAL anywhere
/// between the two must recover A plus exactly the B operations whose
/// records are complete: a consistent prefix, never an error and never a
/// partial record.
#[cfg(feature = "failpoints")]
#[test]
fn losing_unsynced_wal_bytes_recovers_a_consistent_prefix() {
    use std::collections::BTreeMap;
    let opts = Options {
        sync_mode: forgekv::SyncMode::Never,
        ..Options::default()
    };
    let live = TempDir::new().unwrap();
    let db = Db::open(live.path(), opts.clone()).unwrap();
    let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    for i in 0..200 {
        let (k, v) = (
            format!("k{i:03}").into_bytes(),
            format!("a{i}").into_bytes(),
        );
        db.put(&k, &v).unwrap();
        model.insert(k, v);
    }
    db.flush().unwrap();
    let wal = the_wal(live.path());
    let at_rotation = db.wal_offsets().unwrap();
    assert_eq!(
        (at_rotation.written, at_rotation.synced),
        (HEADER_LEN, HEADER_LEN),
        "a new WAL holds only its synced header"
    );
    // Batch B; `states[j]` is the model after j operations, `ends[j]` the
    // written offset after them (checked against the file length).
    let mut states = vec![model.clone()];
    let mut ends = vec![at_rotation.written];
    let mut x = 0x2545_F491_4F6C_DD1Du64;
    for j in 0..300u64 {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        let k = format!("k{:03}", x % 250).into_bytes();
        if x.is_multiple_of(5) {
            db.delete(&k).unwrap();
            model.remove(&k);
        } else {
            let v = format!("b{j}-{}", "x".repeat((x % 40) as usize)).into_bytes();
            db.put(&k, &v).unwrap();
            model.insert(k, v);
        }
        states.push(model.clone());
        let off = db.wal_offsets().unwrap();
        assert_eq!(
            off.wal_number, at_rotation.wal_number,
            "no rotation during B"
        );
        assert_eq!(off.written, fs::metadata(&wal).unwrap().len());
        assert_eq!(
            off.synced, at_rotation.synced,
            "SyncMode::Never: nothing synced"
        );
        ends.push(off.written);
    }
    let synced = at_rotation.synced;
    // The state on disk at this instant (page cache included).
    let snapshot = TempDir::new().unwrap();
    copy_files(live.path(), snapshot.path());
    drop(db);
    let written = *ends.last().unwrap();

    let mut cuts = vec![synced, written, synced + 1, written - 1];
    cuts.extend(ends[1..6].iter().copied());
    let mut y = 7u64;
    for _ in 0..60 {
        y = y
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        cuts.push(synced + (y >> 33) % (written - synced + 1));
    }
    for cut in cuts {
        let dir = TempDir::new().unwrap();
        copy_files(snapshot.path(), dir.path());
        let w = the_wal(dir.path());
        let f = fs::OpenOptions::new().write(true).open(&w).unwrap();
        f.set_len(cut).unwrap();
        drop(f);
        let db = Db::open(dir.path(), Options::default())
            .unwrap_or_else(|e| panic!("cut at {cut}: recovery failed: {e}"));
        let j = ends.iter().rposition(|&e| e <= cut).unwrap();
        assert_eq!(
            db.recovery_report().discarded_bytes,
            cut - ends[j],
            "cut at {cut}"
        );
        for i in 0..250 {
            let k = format!("k{i:03}").into_bytes();
            assert_eq!(
                db.get(&k).unwrap().as_ref(),
                states[j].get(&k),
                "cut at {cut} (prefix of {j} operations): key {i}"
            );
        }
        db.check_invariants().unwrap();
    }
}

/// The engine's synced offset follows the documented sync points: every
/// write under `SyncMode::Always`; under `SyncMode::Never` only the header
/// of a new WAL, the sealed WAL at rotation, and `shutdown`.
#[cfg(feature = "failpoints")]
#[test]
fn wal_offsets_track_the_sync_points() {
    use forgekv::SyncMode;
    let wal_len = |dir: &Path| fs::metadata(the_wal(dir)).unwrap().len();

    let dir = TempDir::new().unwrap();
    let opts = Options {
        sync_mode: SyncMode::Always,
        ..Options::default()
    };
    let db = Db::open(dir.path(), opts).unwrap();
    let o = db.wal_offsets().unwrap();
    assert_eq!((o.written, o.synced), (HEADER_LEN, HEADER_LEN));
    db.put(b"k", b"v").unwrap();
    let o = db.wal_offsets().unwrap();
    assert!(o.written > HEADER_LEN);
    assert_eq!(o.synced, o.written, "Always: synced before the ack");
    assert_eq!(o.written, wal_len(dir.path()));
    drop(db);

    let dir = TempDir::new().unwrap();
    let opts = Options {
        sync_mode: SyncMode::Never,
        ..Options::default()
    };
    let db = Db::open(dir.path(), opts).unwrap();
    db.put(b"k", b"v").unwrap();
    let before = db.wal_offsets().unwrap();
    assert_eq!(
        before.synced, HEADER_LEN,
        "Never: only the header is synced"
    );
    assert!(before.written > before.synced);
    db.flush().unwrap(); // seals and syncs the old WAL, starts a new one
    let rotated = db.wal_offsets().unwrap();
    assert_ne!(rotated.wal_number, before.wal_number);
    assert_eq!((rotated.written, rotated.synced), (HEADER_LEN, HEADER_LEN));
    db.put(b"k2", b"v2").unwrap();
    assert_eq!(db.wal_offsets().unwrap().synced, HEADER_LEN);
    db.shutdown().unwrap(); // syncs the WAL even under Never
    let o = db.wal_offsets().unwrap();
    assert_eq!(o.synced, o.written);
    assert_eq!(o.written, wal_len(dir.path()));
}
