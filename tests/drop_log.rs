//! `Drop for Db` reports a failed best-effort shutdown on stderr, and stays
//! silent when shutdown succeeded or when an explicit `close`/`shutdown`
//! already returned the error to its caller.
//!
//! The diagnostic goes to the process's stderr, so each case runs in a
//! child: this test binary re-executed with `CHILD_CASE` set, running only
//! `child`. The engine is made to fail by poisoning it with the test-only
//! failpoint `flush:before_manifest`.
#![cfg(feature = "failpoints")]

use std::process::{Command, Stdio};

use forgekv::{Db, Options};
use tempfile::TempDir;

const CHILD_CASE: &str = "FORGEKV_DROP_LOG_CASE";
const CHILD_DIR: &str = "FORGEKV_DROP_LOG_DIR";
const MESSAGE: &str = "forgekv: shutdown during drop of the database at";

/// Runs only as a child (see module docs); a no-op in a normal test run.
#[test]
fn child() {
    let (Ok(case), Ok(dir)) = (std::env::var(CHILD_CASE), std::env::var(CHILD_DIR)) else {
        return;
    };
    let db = Db::open(&dir, Options::default()).unwrap();
    db.put(b"k", b"v").unwrap();
    if case != "healthy-drop" {
        db.failpoints().arm("flush:before_manifest");
        assert!(
            db.flush().is_err(),
            "the injected failure must poison the engine"
        );
    }
    match case.as_str() {
        "poisoned-drop" | "healthy-drop" => drop(db),
        "poisoned-close" => assert!(db.close().is_err(), "close must return the error"),
        other => panic!("unknown case {other}"),
    }
}

/// Run one case in a child process; return its stderr.
fn run_case(case: &str) -> String {
    let dir = TempDir::new().unwrap();
    let out = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "child", "--nocapture", "--test-threads=1"])
        .env(CHILD_CASE, case)
        .env(CHILD_DIR, dir.path())
        .stdout(Stdio::null())
        .output()
        .unwrap();
    assert!(out.status.success(), "{case}: child failed: {out:?}");
    String::from_utf8_lossy(&out.stderr).into_owned()
}

#[test]
fn a_failed_shutdown_during_drop_is_reported_on_stderr() {
    let stderr = run_case("poisoned-drop");
    assert_eq!(stderr.matches(MESSAGE).count(), 1, "stderr: {stderr}");
    assert!(stderr.contains("Db::close()"), "stderr: {stderr}");
}

#[test]
fn an_error_already_returned_by_close_is_not_reported_again() {
    let stderr = run_case("poisoned-close");
    assert!(!stderr.contains(MESSAGE), "stderr: {stderr}");
}

#[test]
fn a_successful_shutdown_during_drop_prints_nothing() {
    let stderr = run_case("healthy-drop");
    assert!(!stderr.contains(MESSAGE), "stderr: {stderr}");
}
