//! Phase 5: concurrent reads and writes with background flush/compaction,
//! backpressure, shutdown, and reference-counted table lifetimes.
//!
//! Every multi-threaded test runs under a watchdog (`with_timeout`) so a
//! deadlock fails the test instead of hanging the suite. The deterministic
//! tests at the bottom (feature `failpoints`) freeze a flush, compaction or
//! read at an exact step with a pause-point.

use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::Duration;

use forgekv::{Db, Error, Options};
use tempfile::TempDir;

fn small() -> Options {
    Options {
        max_key_len: 64,
        max_value_len: 512,
        memtable_bytes: 4096,
        block_bytes: 256,
        sst_target_bytes: 1024,
        l0_compaction_trigger: 3,
        ..Options::default()
    }
}

/// Run `f` on a thread; fail if it does not finish within `secs`.
fn with_timeout<T: Send + 'static>(
    secs: u64,
    name: &str,
    f: impl FnOnce() -> T + Send + 'static,
) -> T {
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let _ = tx.send(f());
    });
    match rx.recv_timeout(Duration::from_secs(secs)) {
        Ok(v) => v,
        Err(mpsc::RecvTimeoutError::Timeout) => panic!("{name}: timed out (deadlock?)"),
        Err(mpsc::RecvTimeoutError::Disconnected) => panic!("{name}: worker thread panicked"),
    }
}

fn key(t: usize, i: usize) -> Vec<u8> {
    format!("t{t}-k{i:05}").into_bytes()
}

/// Self-validating value: embeds the key and a version, so a torn or
/// misattributed read is detectable.
fn value(k: &[u8], version: usize) -> Vec<u8> {
    let mut v = k.to_vec();
    v.extend_from_slice(format!("#v{version}#").as_bytes());
    v.extend(std::iter::repeat_n(b'.', version % 40));
    v
}

fn check_value(k: &[u8], v: &[u8]) {
    assert!(
        v.starts_with(k),
        "value for {:?} does not embed its key",
        String::from_utf8_lossy(k)
    );
    assert_eq!(v[k.len()], b'#', "torn value");
}

const _: () = {
    const fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<Db>();
};

#[test]
fn concurrent_writers_and_readers_with_background_flush_and_compaction() {
    with_timeout(120, "stress", || {
        let dir = TempDir::new().unwrap();
        let db = Arc::new(Db::open(dir.path(), small()).unwrap());
        let done = Arc::new(AtomicBool::new(false));
        const WRITERS: usize = 4;
        const KEYS: usize = 300;
        const ROUNDS: usize = 3;

        let writers: Vec<_> = (0..WRITERS)
            .map(|t| {
                let db = Arc::clone(&db);
                thread::spawn(move || {
                    for round in 0..ROUNDS {
                        for i in 0..KEYS {
                            let k = key(t, i);
                            db.put(&k, &value(&k, round)).unwrap();
                        }
                    }
                })
            })
            .collect();
        let readers: Vec<_> = (0..3)
            .map(|r| {
                let db = Arc::clone(&db);
                let done = Arc::clone(&done);
                thread::spawn(move || {
                    let mut n = 0usize;
                    while !done.load(Ordering::SeqCst) {
                        let k = key(n % WRITERS, (n * 7 + r) % KEYS);
                        if let Some(v) = db.get(&k).unwrap() {
                            check_value(&k, &v);
                        }
                        n += 1;
                    }
                    n
                })
            })
            .collect();
        for w in writers {
            w.join().unwrap();
        }
        done.store(true, Ordering::SeqCst);
        let reads: usize = readers.into_iter().map(|r| r.join().unwrap()).sum();
        assert!(reads > 0);

        let stats = db.stats().unwrap();
        assert!(stats.flushes > 5 && stats.compactions > 0, "{stats:?}");
        for t in 0..WRITERS {
            for i in 0..KEYS {
                let k = key(t, i);
                assert_eq!(
                    db.get(&k).unwrap(),
                    Some(value(&k, ROUNDS - 1)),
                    "lost write"
                );
            }
        }
        db.check_invariants().unwrap();
        drop(db);
        let db = Db::open(dir.path(), small()).unwrap();
        for t in 0..WRITERS {
            for i in 0..KEYS {
                let k = key(t, i);
                assert_eq!(db.get(&k).unwrap(), Some(value(&k, ROUNDS - 1)));
            }
        }
        assert_eq!(db.stats().unwrap().obsolete_delete_failures, 0);
    });
}

/// An acknowledged key must be readable at every instant, including while
/// it moves memtable -> immutable memtable -> L0 -> L1.
#[test]
fn acknowledged_writes_never_disappear_during_flush_or_compaction() {
    with_timeout(120, "read gap", || {
        let dir = TempDir::new().unwrap();
        let db = Arc::new(Db::open(dir.path(), small()).unwrap());
        let acked = Arc::new(AtomicUsize::new(0));
        let done = Arc::new(AtomicBool::new(false));
        const N: usize = 4000;

        let writer = {
            let (db, acked) = (Arc::clone(&db), Arc::clone(&acked));
            thread::spawn(move || {
                for i in 0..N {
                    let k = key(0, i);
                    db.put(&k, &value(&k, 0)).unwrap();
                    acked.store(i + 1, Ordering::SeqCst);
                }
            })
        };
        let readers: Vec<_> = (0..4)
            .map(|r| {
                let (db, acked, done) = (Arc::clone(&db), Arc::clone(&acked), Arc::clone(&done));
                thread::spawn(move || {
                    let mut checks = 0usize;
                    let mut x = 0x9E37_79B9u64 + r as u64;
                    while !done.load(Ordering::SeqCst) {
                        let a = acked.load(Ordering::SeqCst);
                        if a == 0 {
                            continue;
                        }
                        x ^= x << 13;
                        x ^= x >> 7;
                        x ^= x << 17;
                        // The newest acked key (most often mid-transition) and a random older one.
                        for i in [a - 1, (x as usize) % a] {
                            let k = key(0, i);
                            let got = db.get(&k).unwrap();
                            assert_eq!(got, Some(value(&k, 0)), "read gap for key {i} (acked {a})");
                            checks += 1;
                        }
                    }
                    checks
                })
            })
            .collect();
        writer.join().unwrap();
        done.store(true, Ordering::SeqCst);
        let checks: usize = readers.into_iter().map(|r| r.join().unwrap()).sum();
        assert!(checks > 1000, "only {checks} checks");
        let stats = db.stats().unwrap();
        assert!(stats.flushes > 10 && stats.compactions > 2, "{stats:?}");
    });
}

#[test]
fn concurrent_writers_on_one_key_agree_before_and_after_restart() {
    with_timeout(60, "same key", || {
        let dir = TempDir::new().unwrap();
        let db = Arc::new(Db::open(dir.path(), small()).unwrap());
        let written: Vec<HashSet<Vec<u8>>> = (0..4)
            .map(|t| {
                let db = Arc::clone(&db);
                thread::spawn(move || {
                    let mut mine = HashSet::new();
                    for i in 0..300 {
                        let v = format!("t{t}-{i}-").repeat(1 + i % 5).into_bytes();
                        db.put(b"hot", &v).unwrap();
                        db.put(&key(t, i), b"filler-to-force-flushes-filler")
                            .unwrap();
                        mine.insert(v);
                    }
                    mine
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|h| h.join().unwrap())
            .collect();
        let final_value = db.get(b"hot").unwrap().unwrap();
        assert!(written.iter().any(|s| s.contains(&final_value)));
        drop(db);
        let db = Db::open(dir.path(), small()).unwrap();
        assert_eq!(db.get(b"hot").unwrap(), Some(final_value));
    });
}

#[test]
fn memory_stays_within_two_memtables() {
    with_timeout(60, "memory bound", || {
        let dir = TempDir::new().unwrap();
        let opts = small();
        let db = Db::open(dir.path(), opts.clone()).unwrap();
        for i in 0..3000 {
            let k = key(0, i);
            db.put(&k, &value(&k, i)).unwrap();
            let s = db.level_summary().unwrap();
            assert!(s.memtable_bytes <= opts.memtable_bytes);
            assert!(s.immutable_memtable_bytes.unwrap_or(0) <= opts.memtable_bytes);
            assert!(s.l0.len() <= opts.l0_compaction_trigger);
        }
    });
}

#[test]
fn shutdown_while_writers_are_active_keeps_every_acknowledged_write() {
    with_timeout(60, "shutdown", || {
        let dir = TempDir::new().unwrap();
        let db = Arc::new(Db::open(dir.path(), small()).unwrap());
        let writers: Vec<_> = (0..4)
            .map(|t| {
                let db = Arc::clone(&db);
                thread::spawn(move || {
                    let mut acked = BTreeMap::new();
                    for i in 0.. {
                        let k = key(t, i);
                        let v = value(&k, i);
                        match db.put(&k, &v) {
                            Ok(()) => {
                                acked.insert(k, v);
                            }
                            Err(Error::Closed) => break,
                            Err(e) => panic!("unexpected error {e}"),
                        }
                    }
                    acked
                })
            })
            .collect();
        thread::sleep(Duration::from_millis(100));
        db.shutdown().unwrap();
        let acked: Vec<BTreeMap<Vec<u8>, Vec<u8>>> =
            writers.into_iter().map(|w| w.join().unwrap()).collect();
        assert!(acked.iter().all(|m| !m.is_empty()));
        drop(db);
        let db = Db::open(dir.path(), small()).unwrap();
        for m in &acked {
            for (k, v) in m {
                assert_eq!(db.get(k).unwrap().as_ref(), Some(v), "acked write lost");
            }
        }
    });
}

#[test]
fn every_operation_after_shutdown_returns_closed() {
    let dir = TempDir::new().unwrap();
    let db = Db::open(dir.path(), small()).unwrap();
    db.put(b"k", b"v").unwrap();
    db.shutdown().unwrap();
    db.shutdown().unwrap(); // idempotent
    assert!(matches!(db.put(b"k", b"v"), Err(Error::Closed)));
    assert!(matches!(db.delete(b"k"), Err(Error::Closed)));
    assert!(matches!(db.get(b"k"), Err(Error::Closed)));
    assert!(matches!(db.flush(), Err(Error::Closed)));
    assert!(matches!(db.compact(), Err(Error::Closed)));
    drop(db);
    let db = Db::open(dir.path(), small()).unwrap();
    assert_eq!(db.get(b"k").unwrap(), Some(b"v".to_vec()));
}

#[test]
fn dropping_the_db_stops_the_worker_and_releases_the_lock() {
    with_timeout(30, "drop", || {
        let dir = TempDir::new().unwrap();
        for round in 0..20 {
            let db = Db::open(dir.path(), small()).unwrap();
            for i in 0..50 {
                db.put(&key(round, i), b"some value to fill the memtable")
                    .unwrap();
            }
            // No close(): Drop must join the worker so the next open is not Locked.
        }
        let db = Db::open(dir.path(), small()).unwrap();
        assert!(db.get(&key(19, 49)).unwrap().is_some());
    });
}

#[test]
fn explicit_flush_and_compact_from_many_threads() {
    with_timeout(60, "flush/compact contention", || {
        let dir = TempDir::new().unwrap();
        let db = Arc::new(Db::open(dir.path(), small()).unwrap());
        let hs: Vec<_> = (0..4)
            .map(|t| {
                let db = Arc::clone(&db);
                thread::spawn(move || {
                    for i in 0..150 {
                        let k = key(t, i);
                        db.put(&k, &value(&k, 1)).unwrap();
                        match i % 30 {
                            0 => db.flush().unwrap(),
                            15 => {
                                db.compact().unwrap();
                            }
                            _ => {}
                        }
                    }
                })
            })
            .collect();
        for h in hs {
            h.join().unwrap();
        }
        db.check_invariants().unwrap();
        for t in 0..4 {
            for i in 0..150 {
                let k = key(t, i);
                assert_eq!(db.get(&k).unwrap(), Some(value(&k, 1)));
            }
        }
    });
}

// --------------------------------------------- deterministic (pause points)

#[cfg(feature = "failpoints")]
mod paused {
    use super::*;
    use forgekv::FailPoints;
    use std::path::Path;

    const WAIT: Duration = Duration::from_secs(10);

    fn sst_files(dir: &Path) -> HashSet<String> {
        std::fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .filter(|n| n.ends_with(".sst"))
            .collect()
    }

    #[test]
    fn reads_and_writes_continue_while_a_flush_is_paused() {
        with_timeout(60, "paused flush", || {
            let dir = TempDir::new().unwrap();
            let db = Arc::new(Db::open(dir.path(), small()).unwrap());
            for i in 0..20 {
                let k = key(0, i);
                db.put(&k, &value(&k, 0)).unwrap();
            }
            db.failpoints().pause("flush:before_manifest");
            let flusher = {
                let db = Arc::clone(&db);
                thread::spawn(move || db.flush())
            };
            assert!(db.failpoints().wait_paused("flush:before_manifest", WAIT));
            // The flushed data lives only in the immutable memtable now.
            let s = db.level_summary().unwrap();
            assert!(s.immutable_memtable_bytes.is_some() && s.l0.is_empty());
            for i in 0..20 {
                let k = key(0, i);
                assert_eq!(db.get(&k).unwrap(), Some(value(&k, 0)));
            }
            // New writes go to the fresh memtable while the flush is stuck.
            db.put(b"during", b"flush").unwrap();
            assert_eq!(db.get(b"during").unwrap(), Some(b"flush".to_vec()));

            db.failpoints().release("flush:before_manifest");
            flusher.join().unwrap().unwrap();
            let s = db.level_summary().unwrap();
            assert!(s.immutable_memtable_bytes.is_none() && s.l0.len() == 1);
            for i in 0..20 {
                let k = key(0, i);
                assert_eq!(db.get(&k).unwrap(), Some(value(&k, 0)));
            }
            assert_eq!(db.get(b"during").unwrap(), Some(b"flush".to_vec()));
        });
    }

    #[test]
    fn reads_continue_at_every_paused_compaction_step() {
        for point in [
            "compaction:after_output",
            "compaction:before_manifest",
            "compaction:after_manifest",
        ] {
            with_timeout(60, point, move || {
                let dir = TempDir::new().unwrap();
                let opts = Options {
                    l0_compaction_trigger: 1_000,
                    ..small()
                };
                let db = Arc::new(Db::open(dir.path(), opts).unwrap());
                for round in 0..3 {
                    for i in 0..60 {
                        let k = key(0, i);
                        db.put(&k, &value(&k, round)).unwrap();
                    }
                    db.flush().unwrap();
                }
                db.failpoints().pause(point);
                let compactor = {
                    let db = Arc::clone(&db);
                    thread::spawn(move || db.compact())
                };
                assert!(db.failpoints().wait_paused(point, WAIT), "{point}");
                for i in 0..60 {
                    let k = key(0, i);
                    assert_eq!(db.get(&k).unwrap(), Some(value(&k, 2)), "{point}");
                }
                db.failpoints().release(point);
                compactor.join().unwrap().unwrap();
                for i in 0..60 {
                    let k = key(0, i);
                    assert_eq!(db.get(&k).unwrap(), Some(value(&k, 2)), "{point}");
                }
                db.check_invariants().unwrap();
            });
        }
    }

    /// A read that took its snapshot before a compaction committed must
    /// still find the (now obsolete) input files; they are deleted only when
    /// that read drops its snapshot.
    #[test]
    fn obsolete_tables_live_until_the_last_reader_drops_them() {
        with_timeout(60, "refcount", || {
            let dir = TempDir::new().unwrap();
            let opts = Options {
                l0_compaction_trigger: 1_000,
                ..small()
            };
            let db = Arc::new(Db::open(dir.path(), opts).unwrap());
            db.put(b"old", b"value").unwrap();
            db.flush().unwrap();
            db.put(b"other", b"x").unwrap();
            db.flush().unwrap();
            let inputs = sst_files(dir.path());
            assert_eq!(inputs.len(), 2);

            db.failpoints().pause("read:after_snapshot");
            let reader = {
                let db = Arc::clone(&db);
                thread::spawn(move || db.get(b"old"))
            };
            assert!(db.failpoints().wait_paused("read:after_snapshot", WAIT));
            db.compact().unwrap();
            assert!(
                inputs.is_subset(&sst_files(dir.path())),
                "inputs deleted while a reader still holds them"
            );
            db.failpoints().release("read:after_snapshot");
            assert_eq!(reader.join().unwrap().unwrap(), Some(b"value".to_vec()));
            let now = sst_files(dir.path());
            assert!(
                inputs.is_disjoint(&now),
                "obsolete inputs not deleted: {now:?}"
            );
            assert_eq!(db.stats().unwrap().obsolete_delete_failures, 0);
            db.check_invariants().unwrap();
        });
    }

    /// With the flush frozen, writers fill the second memtable and then block
    /// (bounded memory). Releasing the flush lets them finish.
    #[test]
    fn writers_block_when_both_memtables_are_full() {
        with_timeout(60, "backpressure", || {
            let dir = TempDir::new().unwrap();
            let opts = small();
            let db = Arc::new(Db::open(dir.path(), opts.clone()).unwrap());
            db.failpoints().pause("flush:before_manifest");
            let acked = Arc::new(AtomicUsize::new(0));
            const N: usize = 400;
            let writer = {
                let (db, acked) = (Arc::clone(&db), Arc::clone(&acked));
                thread::spawn(move || {
                    for i in 0..N {
                        let k = key(0, i);
                        db.put(&k, &value(&k, 0)).unwrap();
                        acked.store(i + 1, Ordering::SeqCst);
                    }
                })
            };
            assert!(db.failpoints().wait_paused("flush:before_manifest", WAIT));
            // Wait for the writer to stall.
            let mut last = usize::MAX;
            loop {
                thread::sleep(Duration::from_millis(100));
                let now = acked.load(Ordering::SeqCst);
                if now == last {
                    break;
                }
                last = now;
            }
            assert!(last < N, "writer never blocked");
            assert!(!writer.is_finished());
            let s = db.level_summary().unwrap();
            assert!(s.immutable_memtable_bytes.is_some());
            assert!(s.memtable_bytes <= opts.memtable_bytes);
            // Reads are not blocked by backpressure.
            assert!(db.get(&key(0, 0)).unwrap().is_some());

            db.failpoints().release("flush:before_manifest");
            writer.join().unwrap();
            for i in 0..N {
                let k = key(0, i);
                assert_eq!(db.get(&k).unwrap(), Some(value(&k, 0)));
            }
        });
    }

    /// Wait until a writer is blocked for memtable room (deterministic: no
    /// assumption about how fast writes or fsyncs are).
    fn wait_for_stalled_writer(db: &Db) {
        for _ in 0..3000 {
            if db.stats().unwrap().writers_stalled_now > 0 {
                return;
            }
            thread::sleep(Duration::from_millis(10));
        }
        panic!("no writer stalled within 30 s");
    }

    /// Spawn a writer that writes distinct keys until `n` are acknowledged.
    fn writer(db: &Arc<Db>, n: usize) -> thread::JoinHandle<()> {
        let db = Arc::clone(db);
        thread::spawn(move || {
            for i in 0..n {
                let k = key(9, i);
                db.put(&k, &value(&k, 0)).unwrap();
            }
        })
    }

    /// Stall counters: a writer blocked behind a frozen flush is counted
    /// once the flush lets it go, and not attributed to compaction.
    #[test]
    fn write_stalls_behind_a_flush_are_counted() {
        with_timeout(60, "stall stats (flush)", || {
            let dir = TempDir::new().unwrap();
            // No automatic compaction, so no stall can be due to one.
            let opts = Options {
                l0_compaction_trigger: 1_000,
                ..small()
            };
            let db = Arc::new(Db::open(dir.path(), opts).unwrap());
            assert_eq!(db.stats().unwrap().write_stalls, 0);
            db.failpoints().pause("flush:before_manifest");
            let w = writer(&db, 400);
            assert!(db.failpoints().wait_paused("flush:before_manifest", WAIT));
            wait_for_stalled_writer(&db);
            thread::sleep(Duration::from_millis(50)); // make the stall measurable
            db.failpoints().release("flush:before_manifest");
            w.join().unwrap();
            let s = db.stats().unwrap();
            assert!(s.write_stalls >= 1, "{s:?}");
            assert_eq!(s.writers_stalled_now, 0, "{s:?}");
            assert!(s.max_write_stall >= Duration::from_millis(50), "{s:?}");
            assert!(s.write_stall_time >= s.max_write_stall);
            assert_eq!(s.write_stalls_during_compaction, 0, "{s:?}");
        });
    }

    /// A writer that runs out of room while the worker is compacting (so it
    /// cannot flush) is counted as a stall during compaction.
    #[test]
    fn write_stalls_during_a_compaction_are_attributed_to_it() {
        with_timeout(60, "stall stats (compaction)", || {
            let dir = TempDir::new().unwrap();
            let db = Arc::new(Db::open(dir.path(), small()).unwrap());
            db.failpoints().pause("compaction:after_output");
            // Writing fills L0 to the trigger; the worker starts compacting
            // and freezes; the writer then fills both memtables and blocks.
            let w = writer(&db, 2000);
            assert!(db.failpoints().wait_paused("compaction:after_output", WAIT));
            wait_for_stalled_writer(&db);
            db.failpoints().release("compaction:after_output");
            w.join().unwrap();
            let s = db.stats().unwrap();
            assert!(s.write_stalls_during_compaction >= 1, "{s:?}");
            assert!(s.write_stalls >= s.write_stalls_during_compaction);
        });
    }

    /// Shutdown must wake a writer blocked on backpressure (no deadlock), and
    /// complete once the worker finishes its job.
    #[test]
    fn shutdown_wakes_a_blocked_writer() {
        with_timeout(60, "shutdown vs backpressure", || {
            let dir = TempDir::new().unwrap();
            let db = Arc::new(Db::open(dir.path(), small()).unwrap());
            db.failpoints().pause("flush:before_manifest");
            let writer = {
                let db = Arc::clone(&db);
                thread::spawn(move || {
                    let mut acked = vec![];
                    for i in 0.. {
                        let k = key(0, i);
                        match db.put(&k, &value(&k, 0)) {
                            Ok(()) => acked.push(k),
                            Err(e) => return (acked, e),
                        }
                    }
                    unreachable!()
                })
            };
            assert!(db.failpoints().wait_paused("flush:before_manifest", WAIT));
            thread::sleep(Duration::from_millis(200)); // let the writer block
            let closer = {
                let db = Arc::clone(&db);
                thread::spawn(move || db.shutdown())
            };
            let (acked, err) = writer.join().unwrap();
            assert!(matches!(err, Error::Closed), "{err}");
            // The worker is still frozen mid-flush; shutdown waits for it.
            assert!(!closer.is_finished());
            db.failpoints().release("flush:before_manifest");
            closer.join().unwrap().unwrap();
            drop(db);
            let db = Db::open(dir.path(), small()).unwrap();
            for k in &acked {
                assert_eq!(db.get(k).unwrap(), Some(value(k, 0)));
            }
        });
    }

    #[test]
    fn a_background_failure_is_reported_and_poisons_writes_only() {
        with_timeout(60, "background failure", || {
            let dir = TempDir::new().unwrap();
            let db = Db::open(dir.path(), small()).unwrap();
            db.failpoints().arm("flush:before_manifest");
            let mut acked = vec![];
            for i in 0.. {
                let k = key(0, i);
                match db.put(&k, &value(&k, 0)) {
                    Ok(()) => acked.push(k),
                    Err(Error::Poisoned | Error::Injected(_)) => break,
                    Err(e) => panic!("{e}"),
                }
            }
            let stats = db.stats().unwrap();
            assert!(
                stats
                    .background_error
                    .as_deref()
                    .is_some_and(|m| m.contains("flush:before_manifest")),
                "{stats:?}"
            );
            assert!(matches!(db.put(b"x", b"y"), Err(Error::Poisoned)));
            for k in &acked {
                assert_eq!(db.get(k).unwrap(), Some(value(k, 0)), "reads still work");
            }
            drop(db);
            let db = Db::open(dir.path(), small()).unwrap();
            for k in &acked {
                assert_eq!(db.get(k).unwrap(), Some(value(k, 0)));
            }
        });
    }

    /// Regression (audit): a panic on the background worker used to leave
    /// every caller waiting on it (`flush`, `compact`, writers blocked on
    /// backpressure) blocked forever, and made `shutdown` skip the WAL sync.
    #[test]
    fn a_worker_panic_wakes_waiters_and_poisons_writes() {
        with_timeout(30, "worker panic", || {
            let dir = TempDir::new().unwrap();
            let db = Db::open(dir.path(), small()).unwrap();
            db.put(b"k", b"v").unwrap();
            db.failpoints().panic("flush:before_manifest");
            assert!(matches!(db.flush(), Err(Error::Poisoned)));
            assert!(matches!(db.put(b"x", b"y"), Err(Error::Poisoned)));
            assert!(matches!(db.compact(), Err(Error::Poisoned)));
            assert_eq!(db.get(b"k").unwrap(), Some(b"v".to_vec()));
            let stats = db.stats().unwrap();
            assert!(
                stats
                    .background_error
                    .as_deref()
                    .is_some_and(|m| m.contains("panicked")),
                "{stats:?}"
            );
            assert!(matches!(db.close(), Err(Error::Poisoned)));
            let db = Db::open(dir.path(), small()).unwrap();
            assert_eq!(db.get(b"k").unwrap(), Some(b"v".to_vec()));
        });
    }

    /// A writer already blocked on backpressure when the worker panics is
    /// woken with an error instead of waiting forever.
    #[test]
    fn a_worker_panic_wakes_a_writer_blocked_on_backpressure() {
        with_timeout(30, "worker panic vs backpressure", || {
            let dir = TempDir::new().unwrap();
            let db = Arc::new(Db::open(dir.path(), small()).unwrap());
            db.failpoints().pause("flush:before_manifest");
            let writer = {
                let db = Arc::clone(&db);
                thread::spawn(move || {
                    let mut acked = vec![];
                    for i in 0.. {
                        let k = key(0, i);
                        match db.put(&k, &value(&k, 0)) {
                            Ok(()) => acked.push(k),
                            Err(e) => return (acked, e),
                        }
                    }
                    unreachable!()
                })
            };
            assert!(db.failpoints().wait_paused("flush:before_manifest", WAIT));
            thread::sleep(Duration::from_millis(200)); // let the writer block
            // The frozen flush continues straight into a panic.
            db.failpoints().panic("flush:after_manifest");
            db.failpoints().release("flush:before_manifest");
            let (acked, err) = writer.join().unwrap();
            assert!(matches!(err, Error::Poisoned), "{err}");
            for k in &acked {
                assert_eq!(db.get(k).unwrap(), Some(value(k, 0)));
            }
            drop(db);
            let db = Db::open(dir.path(), small()).unwrap();
            for k in &acked {
                assert_eq!(db.get(k).unwrap(), Some(value(k, 0)));
            }
        });
    }

    /// Regression for the Phase 3/4 thread-local design: arming one database
    /// must not affect another, and points fire on the worker thread.
    #[test]
    fn failpoints_are_per_database_and_cross_threads() {
        let (a, b) = (TempDir::new().unwrap(), TempDir::new().unwrap());
        let fp = FailPoints::new();
        fp.arm("flush:before_manifest");
        let db_a = Db::open_with_failpoints(a.path(), small(), fp).unwrap();
        let db_b = Db::open(b.path(), small()).unwrap();
        db_a.put(b"k", b"v").unwrap();
        db_b.put(b"k", b"v").unwrap();
        db_b.flush().unwrap();
        assert!(matches!(
            db_a.flush(),
            Err(Error::Injected("flush:before_manifest"))
        ));
    }
}
