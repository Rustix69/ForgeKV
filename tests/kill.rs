//! Phase 8: abrupt process termination (SIGKILL) at random moments while a
//! child process writes, with flushes and compactions running in the
//! background, followed by recovery and a check against the acknowledged
//! operations.
//!
//! This is a **process-crash** test. The kernel survives a SIGKILL and keeps
//! the page cache, so data written but not yet synced is still there. It
//! says nothing about power loss or an OS crash (see the README's
//! "Durability semantics").
//!
//! The child is this test binary re-executed with `CHILD_DIR` set, running
//! only `child_writer`. It prints `ACK <n>` after operation `n` returned
//! `Ok`. Operations are a pure function of `(seed, n)`, so the parent can
//! replay exactly what was acknowledged.

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use forgekv::{Db, Options, SyncMode};
use tempfile::TempDir;

const CHILD_DIR: &str = "FORGEKV_KILL_CHILD_DIR";
const CHILD_SYNC: &str = "FORGEKV_KILL_CHILD_SYNC";
const CHILD_FIRST_OP: &str = "FORGEKV_KILL_CHILD_FIRST_OP";
const SEED: u64 = 0x5EED_0000_0008;
const KEYS: u64 = 300;

fn opts(sync: SyncMode) -> Options {
    Options {
        sync_mode: sync,
        max_key_len: 64,
        max_value_len: 512,
        memtable_bytes: 4096,
        block_bytes: 256,
        sst_target_bytes: 2048,
        l0_compaction_trigger: 3,
        ..Options::default()
    }
}

fn mix(mut z: u64) -> u64 {
    z = z.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Operation `n`: a PUT (`Some(value)`) or a DELETE (`None`) of `key`.
fn op(n: u64) -> (Vec<u8>, Option<Vec<u8>>) {
    let r = mix(SEED ^ n);
    let key = format!("key-{:04}", r % KEYS).into_bytes();
    if r % 100 < 20 {
        (key, None)
    } else {
        let len = 20 + (r >> 32) % 120;
        let mut v = format!("op{n}:").into_bytes();
        v.resize(len as usize, b'.');
        (key, Some(v))
    }
}

/// Runs only as the child (see module docs); a no-op in a normal test run.
#[test]
fn child_writer() {
    let Ok(dir) = std::env::var(CHILD_DIR) else {
        return;
    };
    let sync = match std::env::var(CHILD_SYNC).as_deref() {
        Ok("always") => SyncMode::Always,
        _ => SyncMode::Never,
    };
    let first: u64 = std::env::var(CHILD_FIRST_OP).unwrap().parse().unwrap();
    let db = Db::open(&dir, opts(sync)).unwrap();
    let mut out = std::io::stdout().lock();
    // Runs until the parent kills this process.
    for n in first..=u64::MAX {
        let (key, value) = op(n);
        match &value {
            Some(v) => db.put(&key, v).unwrap(),
            None => db.delete(&key).unwrap(),
        }
        writeln!(out, "ACK {n}").unwrap();
        out.flush().unwrap();
    }
}

/// Start a child writer at operation `first`; SIGKILL it `delay` after its
/// first acknowledgement; return the last acknowledged operation, if any.
fn run_child_and_kill(dir: &Path, sync: &str, first: u64, delay: Duration) -> Option<u64> {
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "child_writer", "--nocapture", "--test-threads=1"])
        .env(CHILD_DIR, dir)
        .env(CHILD_SYNC, sync)
        .env(CHILD_FIRST_OP, first.to_string())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let (tx, rx) = mpsc::channel();
    let reader = thread::spawn(move || {
        let mut last = None;
        for line in BufReader::new(stdout).lines() {
            let Ok(line) = line else { break };
            // libtest prints `test child_writer ... ` without a newline
            // before the first output, so the marker may follow it.
            if let Some(n) = line.split("ACK ").nth(1) {
                let n: u64 = n.parse().unwrap();
                if last.is_none() {
                    let _ = tx.send(());
                }
                last = Some(n);
            }
        }
        last
    });
    let started = rx.recv_timeout(Duration::from_secs(30)).is_ok();
    if started {
        thread::sleep(delay);
    }
    child.kill().unwrap(); // SIGKILL
    child.wait().unwrap();
    let last = reader.join().unwrap();
    assert!(started, "child never acknowledged a write");
    last
}

type Model = BTreeMap<Vec<u8>, Vec<u8>>;

fn apply(model: &mut Model, n: u64) {
    match op(n) {
        (k, Some(v)) => {
            model.insert(k, v);
        }
        (k, None) => {
            model.remove(&k);
        }
    }
}

#[test]
fn sigkill_during_writes_flushes_and_compactions_loses_no_acknowledged_write() {
    let dir = TempDir::new().unwrap();
    let mut model = Model::new();
    let mut next = 0u64;
    let mut rng = SEED;
    for round in 0..20 {
        let sync = if round % 2 == 0 { "never" } else { "always" };
        rng = mix(rng);
        let delay = Duration::from_millis(20 + rng % 300);
        let last = run_child_and_kill(dir.path(), sync, next, delay)
            .unwrap_or_else(|| panic!("round {round}: nothing acknowledged"));
        for n in next..=last {
            apply(&mut model, n);
        }
        // Operation `last + 1` may have been in flight: either outcome is
        // allowed for its key, and only for its key.
        let in_flight = last + 1;
        let mut with_in_flight = model.clone();
        apply(&mut with_in_flight, in_flight);
        let (in_flight_key, _) = op(in_flight);

        let db = Db::open(dir.path(), opts(SyncMode::Always))
            .unwrap_or_else(|e| panic!("round {round} ({sync}): recovery failed: {e}"));
        db.check_invariants()
            .unwrap_or_else(|e| panic!("round {round}: invariants: {e}"));
        let mut in_flight_applied = false;
        for i in 0..KEYS {
            let k = format!("key-{i:04}").into_bytes();
            let got = db.get(&k).unwrap();
            if k == in_flight_key && got.as_ref() != model.get(&k) {
                assert_eq!(
                    got.as_ref(),
                    with_in_flight.get(&k),
                    "round {round}: key {i}"
                );
                in_flight_applied = true;
            } else {
                assert_eq!(
                    got.as_ref(),
                    model.get(&k),
                    "round {round} ({sync}): key {i} after ack {last}"
                );
            }
        }
        drop(db);
        if in_flight_applied {
            model = with_in_flight;
        }
        // Applied or not, the next child resumes after the in-flight
        // operation, so it is never re-run.
        next = in_flight + 1;
    }
    assert!(next > 40, "too few operations acknowledged: {next}");
    // The kills must have hit a database with flushed and compacted data.
    let db = Db::open(dir.path(), opts(SyncMode::Always)).unwrap();
    let levels = db.level_summary().unwrap();
    assert!(!levels.l1.is_empty(), "no compaction happened: {levels:?}");
    eprintln!(
        "{next} operations over 20 kills; L0 {} L1 {}",
        levels.l0.len(),
        levels.l1.len()
    );
}
