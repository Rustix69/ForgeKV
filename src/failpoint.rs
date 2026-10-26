//! Test-only fault injection, compiled in with the `failpoints` Cargo feature.
//!
//! Each `Db` owns a [`FailPoints`] registry (pass one to
//! `Db::open_with_failpoints`, or use `Db::failpoints()`), so parallel tests
//! never interfere and points fire on whichever thread reaches them --
//! including the background worker. A point can be armed to:
//! - **fail** (one shot): `fail_point!(fp, "name")` returns
//!   `Err(Error::Injected("name"))` from the enclosing function. Because
//!   nothing is buffered in user space, the files are then exactly what a
//!   process crash at that instant would leave.
//! - **pause** (one shot): the thread that reaches the point blocks until
//!   `FailPoints::release`; `FailPoints::wait_paused` lets a test wait
//!   until it is there. Used to freeze a flush, compaction or read at a
//!   precise step and observe the rest of the system.
//! - **panic** (one shot): the thread that reaches the point panics. Used to
//!   check that a panicking background worker cannot strand waiters.
//!
//! Without the feature `FailPoints` is an empty type and the macro compiles
//! to nothing.

macro_rules! fail_point {
    ($fp:expr, $name:literal) => {
        #[cfg(feature = "failpoints")]
        {
            if $fp.hit($name) {
                return Err($crate::error::Error::Injected($name));
            }
        }
        #[cfg(not(feature = "failpoints"))]
        {
            let _ = &$fp;
        }
    };
}

/// Every failpoint in the crate, in the order they occur within each operation.
pub const ALL: &[&str] = &[
    // Db::open
    "open:after_flush_sst",
    "manifest:after_tmp_written",
    "manifest:after_rename",
    "open:after_new_wal",
    // write path, after the WAL record is written (and synced)
    "write:after_wal",
    // memtable switch (writer thread)
    "switch:after_new_wal",
    // flush of the immutable memtable (background worker)
    "flush:before_manifest",
    "flush:after_manifest",
    // compaction (background worker, or open)
    "compaction:after_output",
    "compaction:before_manifest",
    "compaction:after_manifest",
    // reads, after taking the state snapshot
    "read:after_snapshot",
];

#[cfg(feature = "failpoints")]
mod imp {
    use std::collections::{HashMap, HashSet};
    use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
    use std::time::Duration;

    #[derive(Clone, Copy, PartialEq, Eq)]
    enum Action {
        Fail,
        Pause,
        Panic,
    }

    #[derive(Default)]
    struct State {
        armed: HashMap<&'static str, Action>,
        paused: HashSet<&'static str>,
    }

    #[derive(Default)]
    struct Inner {
        state: Mutex<State>,
        cv: Condvar,
    }

    #[derive(Clone, Default)]
    pub struct FailPoints {
        inner: Arc<Inner>,
    }

    impl FailPoints {
        pub fn new() -> Self {
            Self::default()
        }

        fn lock(&self) -> MutexGuard<'_, State> {
            // Test-only code: a panicking test thread must not wedge others.
            self.inner
                .state
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
        }

        fn set(&self, name: &'static str, action: Action) {
            assert!(super::ALL.contains(&name), "unknown failpoint {name}");
            self.lock().armed.insert(name, action);
        }

        /// The next thread to reach `name` gets `Err(Injected(name))`.
        pub fn arm(&self, name: &'static str) {
            self.set(name, Action::Fail);
        }

        /// The next thread to reach `name` blocks until `release(name)`.
        pub fn pause(&self, name: &'static str) {
            self.set(name, Action::Pause);
        }

        /// The next thread to reach `name` panics.
        pub fn panic(&self, name: &'static str) {
            self.set(name, Action::Panic);
        }

        /// Unblock a paused thread (or cancel a pause not yet reached).
        pub fn release(&self, name: &'static str) {
            let mut st = self.lock();
            st.paused.remove(name);
            st.armed.remove(name);
            self.inner.cv.notify_all();
        }

        /// Wait until some thread is blocked at `name`.
        pub fn wait_paused(&self, name: &'static str, timeout: Duration) -> bool {
            let st = self.lock();
            let (st, _) = self
                .inner
                .cv
                .wait_timeout_while(st, timeout, |s| !s.paused.contains(name))
                .unwrap_or_else(PoisonError::into_inner);
            st.paused.contains(name)
        }

        /// Disarm everything and release every paused thread.
        pub fn clear(&self) {
            let mut st = self.lock();
            st.armed.clear();
            st.paused.clear();
            self.inner.cv.notify_all();
        }

        pub(crate) fn hit(&self, name: &'static str) -> bool {
            let mut st = self.lock();
            match st.armed.get(name).copied() {
                Some(Action::Fail) => {
                    st.armed.remove(name);
                    true
                }
                Some(Action::Pause) => {
                    st.armed.remove(name);
                    st.paused.insert(name);
                    self.inner.cv.notify_all();
                    while st.paused.contains(name) {
                        st = self
                            .inner
                            .cv
                            .wait(st)
                            .unwrap_or_else(PoisonError::into_inner);
                    }
                    false
                }
                Some(Action::Panic) => {
                    st.armed.remove(name);
                    // Release the registry before unwinding so it is not poisoned.
                    drop(st);
                    panic!("failpoint {name}: injected panic");
                }
                None => false,
            }
        }
    }
}

#[cfg(feature = "failpoints")]
pub use imp::FailPoints;

/// Without the `failpoints` feature this is an empty placeholder.
#[cfg(not(feature = "failpoints"))]
#[derive(Clone, Default)]
pub struct FailPoints;

#[cfg(not(feature = "failpoints"))]
impl FailPoints {
    pub fn new() -> Self {
        FailPoints
    }
}

impl std::fmt::Debug for FailPoints {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("FailPoints")
    }
}
