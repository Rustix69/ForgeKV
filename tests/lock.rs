//! The directory lock across processes: exclusivity, release on close, exit
//! and crash, and no spurious `Error::Locked` when the process spawns
//! children.
//!
//! Children are this test binary re-executed with `CHILD_DIR` set, running
//! only `child`. A child opens the database and prints `OPENED` or
//! `LOCKED`; with `CHILD_MODE=hold` it then keeps the database open until
//! its stdin reaches EOF (closing it then prints `CLOSED`); with
//! `CHILD_MODE=idle` it opens nothing and just waits for EOF.
//! No test retries anything.

use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::Duration;

use forgekv::{Db, Error, Options};
use tempfile::TempDir;

const CHILD_DIR: &str = "FORGEKV_LOCK_CHILD_DIR";
const CHILD_MODE: &str = "FORGEKV_LOCK_CHILD_MODE";

fn opts() -> Options {
    Options::default()
}

/// Runs only as a child (see module docs); a no-op in a normal test run.
#[test]
fn child() {
    let Ok(dir) = std::env::var(CHILD_DIR) else {
        return;
    };
    let mode = std::env::var(CHILD_MODE).unwrap();
    let mut out = std::io::stdout().lock();
    let wait_for_eof = || {
        let mut sink = String::new();
        while std::io::stdin().read_line(&mut sink).unwrap() > 0 {}
    };
    if mode == "idle" {
        writeln!(out, "IDLE").unwrap();
        out.flush().unwrap();
        wait_for_eof();
        return;
    }
    match Db::open(&dir, opts()) {
        Ok(db) => {
            writeln!(out, "OPENED").unwrap();
            out.flush().unwrap();
            if mode == "hold" {
                wait_for_eof();
            }
            db.close().unwrap();
            writeln!(out, "CLOSED").unwrap();
        }
        Err(Error::Locked { .. }) => writeln!(out, "LOCKED").unwrap(),
        Err(e) => writeln!(out, "ERROR {e}").unwrap(),
    }
    out.flush().unwrap();
}

struct Proc {
    child: Child,
    stdin: Option<ChildStdin>,
    /// The child's status lines; disconnects when its stdout closes.
    statuses: mpsc::Receiver<String>,
}

const STATUS_WAIT: Duration = Duration::from_secs(30);

/// The status on a child's output line, if any. libtest prints
/// `test child ... ` without a newline before the test's first output, so
/// the first status arrives as `test child ... OPENED`.
fn status_of(line: &str) -> Option<String> {
    let tail = line.rsplit("... ").next().unwrap_or(line);
    let word = tail.split_whitespace().next()?;
    matches!(word, "OPENED" | "LOCKED" | "CLOSED" | "IDLE" | "ERROR").then(|| tail.to_string())
}

impl Proc {
    fn spawn(dir: &Path, mode: &str) -> Proc {
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "child", "--nocapture", "--test-threads=1"])
            .env(CHILD_DIR, dir)
            .env(CHILD_MODE, mode)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let stdin = child.stdin.take();
        let stdout = child.stdout.take().unwrap();
        let (tx, statuses) = mpsc::channel();
        thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let Ok(line) = line else { break };
                if let Some(s) = status_of(&line)
                    && tx.send(s).is_err()
                {
                    break;
                }
            }
        });
        Proc {
            child,
            stdin,
            statuses,
        }
    }

    /// The next status (`OPENED`, `LOCKED`, `CLOSED`, `IDLE`, `ERROR ..`);
    /// fails instead of hanging if none arrives.
    fn next(&mut self) -> String {
        match self.statuses.recv_timeout(STATUS_WAIT) {
            Ok(s) => s,
            Err(e) => {
                let _ = self.child.kill();
                panic!("no status line from the child: {e:?}");
            }
        }
    }

    /// Close stdin (the child closes the database and exits), reap it, and
    /// return its remaining statuses.
    fn finish(mut self) -> Vec<String> {
        drop(self.stdin.take());
        let mut rest = vec![];
        loop {
            match self.statuses.recv_timeout(STATUS_WAIT) {
                Ok(s) => rest.push(s),
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
                Err(e) => {
                    let _ = self.child.kill();
                    panic!("child did not exit: {e:?}");
                }
            }
        }
        assert!(self.child.wait().unwrap().success());
        rest
    }
}

/// One-shot: a separate process tries to open `dir`; returns its verdict.
fn other_process_tries(dir: &Path) -> String {
    let mut p = Proc::spawn(dir, "try");
    let verdict = p.next();
    p.finish();
    verdict
}

#[test]
fn another_process_cannot_open_while_this_one_holds_the_database() {
    let dir = TempDir::new().unwrap();
    let db = Db::open(dir.path(), opts()).unwrap();
    assert_eq!(other_process_tries(dir.path()), "LOCKED");
    db.close().unwrap();
    assert_eq!(other_process_tries(dir.path()), "OPENED");
}

#[test]
fn this_process_cannot_open_while_another_holds_the_database() {
    let dir = TempDir::new().unwrap();
    let mut holder = Proc::spawn(dir.path(), "hold");
    assert_eq!(holder.next(), "OPENED");
    assert!(matches!(
        Db::open(dir.path(), opts()),
        Err(Error::Locked { .. })
    ));
    // Reopening after the owner closes (normal close, then normal exit).
    let rest = holder.finish();
    assert!(rest.iter().any(|l| l == "CLOSED"), "{rest:?}");
    Db::open(dir.path(), opts()).unwrap();
}

#[test]
fn the_lock_is_released_when_the_owner_crashes() {
    let dir = TempDir::new().unwrap();
    let mut holder = Proc::spawn(dir.path(), "hold");
    assert_eq!(holder.next(), "OPENED");
    assert!(matches!(
        Db::open(dir.path(), opts()),
        Err(Error::Locked { .. })
    ));
    holder.child.kill().unwrap(); // SIGKILL: no destructor runs
    holder.child.wait().unwrap();
    Db::open(dir.path(), opts()).unwrap();
}

/// A child spawned while the database is open must not keep the lock after
/// the database is closed (the descriptor is close-on-exec).
#[test]
fn a_child_spawned_while_the_database_was_open_does_not_keep_the_lock() {
    let dir = TempDir::new().unwrap();
    let db = Db::open(dir.path(), opts()).unwrap();
    let mut idle = Proc::spawn(dir.path(), "idle");
    assert_eq!(idle.next(), "IDLE");
    drop(db);
    // The idle child is still running.
    assert!(idle.child.try_wait().unwrap().is_none());
    assert_eq!(other_process_tries(dir.path()), "OPENED");
    Db::open(dir.path(), opts()).unwrap().close().unwrap();
    idle.finish();
}

/// Regression for the reported failure (2,699 of 3,000 reopens returned
/// `Locked` while another thread spawned processes): close and immediately
/// reopen while a thread spawns processes continuously. With the lock
/// released explicitly on drop, no reopen may fail; there is no retry.
#[test]
fn reopening_while_another_thread_spawns_processes_never_reports_locked() {
    let dir = TempDir::new().unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let spawned = Arc::new(AtomicU64::new(0));
    let spawner = {
        let (stop, spawned) = (Arc::clone(&stop), Arc::clone(&spawned));
        thread::spawn(move || {
            while !stop.load(Ordering::SeqCst) {
                Command::new("true").status().unwrap();
                spawned.fetch_add(1, Ordering::SeqCst);
            }
        })
    };
    // Make sure spawning is under way before measuring.
    while spawned.load(Ordering::SeqCst) < 5 {
        thread::yield_now();
    }
    let mut locked = 0;
    for _ in 0..300 {
        match Db::open(dir.path(), opts()) {
            Ok(db) => drop(db),
            Err(Error::Locked { .. }) => locked += 1,
            Err(e) => panic!("{e}"),
        }
    }
    stop.store(true, Ordering::SeqCst);
    spawner.join().unwrap();
    assert!(spawned.load(Ordering::SeqCst) >= 5);
    assert_eq!(
        locked, 0,
        "reopen reported Locked while processes were being spawned"
    );
}
