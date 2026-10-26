//! A FIFO ("ticket") mutex for the engine's writer lock.
//!
//! `std::sync::Mutex` makes no fairness promise: a thread that releases it
//! can take it again before a woken waiter runs. Under `SyncMode::Always`
//! the writer lock is held across an fsync, so a looping writer could keep
//! the lock for seconds while others waited (Phase 7 benchmark: single
//! writes waiting up to 10 s with 4 writers). Here each caller takes a
//! ticket and enters strictly in ticket order, so a writer waits at most
//! for the writers that arrived before it.
//!
//! Safe Rust only: the protected value lives in an inner `Mutex` that is
//! only ever locked by the current ticket holder (so it is uncontended),
//! and the queue is a `Mutex` + `Condvar`.

use std::sync::{Condvar, Mutex, MutexGuard, PoisonError};

use crate::error::{Error, Result};

#[derive(Default)]
struct Queue {
    next_ticket: u64,
    serving: u64,
    waiting: usize,
}

pub struct FairMutex<T> {
    queue: Mutex<Queue>,
    turn: Condvar,
    data: Mutex<T>,
}

/// Fields drop in declaration order: the data is unlocked first, then
/// `_turn` passes the turn to the next ticket.
pub struct FairGuard<'a, T> {
    data: MutexGuard<'a, T>,
    _turn: Turn<'a, T>,
}

/// Passes the turn when dropped (also on unwind).
struct Turn<'a, T>(&'a FairMutex<T>);

impl<T> Drop for Turn<'_, T> {
    fn drop(&mut self) {
        self.0.pass_turn();
    }
}

impl<T> FairMutex<T> {
    pub fn new(value: T) -> Self {
        FairMutex {
            queue: Mutex::new(Queue::default()),
            turn: Condvar::new(),
            data: Mutex::new(value),
        }
    }

    fn queue(&self) -> MutexGuard<'_, Queue> {
        // Queue critical sections are a few integer updates and cannot
        // panic, so a poisoned queue lock still holds consistent counters.
        self.queue.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Block until every earlier caller has released the lock, then lock.
    /// `Error::Poisoned` if a previous holder panicked inside the critical
    /// section (the turn is passed on, so later callers see the same error
    /// instead of blocking).
    pub fn lock(&self) -> Result<FairGuard<'_, T>> {
        let mut q = self.queue();
        let ticket = q.next_ticket;
        q.next_ticket += 1;
        if q.serving != ticket {
            q.waiting += 1;
            while q.serving != ticket {
                q = self.turn.wait(q).unwrap_or_else(PoisonError::into_inner);
            }
            q.waiting -= 1;
        }
        drop(q);
        let turn = Turn(self);
        match self.data.lock() {
            Ok(data) => Ok(FairGuard { data, _turn: turn }),
            // `turn` drops here and passes the turn on.
            Err(_) => Err(Error::Poisoned),
        }
    }

    fn pass_turn(&self) {
        let mut q = self.queue();
        q.serving += 1;
        let waiting = q.waiting > 0;
        drop(q);
        if waiting {
            self.turn.notify_all();
        }
    }
}

impl<T> std::ops::Deref for FairGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.data
    }
}

impl<T> std::ops::DerefMut for FairGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.data
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    /// Waiters enter in arrival order: each thread queues while the lock is
    /// held, and the order in which they get it is the order they queued.
    #[test]
    fn waiters_acquire_in_arrival_order() {
        let m = Arc::new(FairMutex::new(Vec::new()));
        let first = m.lock().unwrap();
        let mut handles = vec![];
        for i in 0..8 {
            let m2 = Arc::clone(&m);
            handles.push(thread::spawn(move || m2.lock().unwrap().push(i)));
            // Wait until thread i is queued before starting thread i + 1.
            while m.queue().waiting < i + 1 {
                thread::yield_now();
            }
        }
        drop(first);
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(*m.lock().unwrap(), (0..8).collect::<Vec<_>>());
    }

    /// A thread that releases and immediately re-locks must not overtake a
    /// thread that was already waiting (the barging that `std` allows).
    #[test]
    fn a_releasing_thread_cannot_overtake_a_waiter() {
        let m = Arc::new(FairMutex::new(Vec::<&str>::new()));
        let g = m.lock().unwrap();
        let (tx, rx) = mpsc::channel();
        let waiter = {
            let m = Arc::clone(&m);
            thread::spawn(move || {
                tx.send(()).unwrap();
                m.lock().unwrap().push("waiter");
            })
        };
        rx.recv().unwrap();
        while m.queue().waiting == 0 {
            thread::yield_now();
        }
        drop(g);
        m.lock().unwrap().push("releaser");
        waiter.join().unwrap();
        assert_eq!(*m.lock().unwrap(), ["waiter", "releaser"]);
    }

    #[test]
    fn a_panic_poisons_later_lockers_without_blocking_them() {
        let m = Arc::new(FairMutex::new(0));
        let m2 = Arc::clone(&m);
        let r = thread::spawn(move || {
            let _g = m2.lock().unwrap();
            panic!("inside the critical section");
        })
        .join();
        assert!(r.is_err());
        let (tx, rx) = mpsc::channel();
        for _ in 0..3 {
            let (m, tx) = (Arc::clone(&m), tx.clone());
            thread::spawn(move || tx.send(matches!(m.lock(), Err(Error::Poisoned))).unwrap());
        }
        for _ in 0..3 {
            assert!(rx.recv_timeout(Duration::from_secs(10)).unwrap());
        }
    }

    #[test]
    fn many_threads_mutual_exclusion() {
        let m = Arc::new(FairMutex::new(0u64));
        let hs: Vec<_> = (0..8)
            .map(|_| {
                let m = Arc::clone(&m);
                thread::spawn(move || {
                    for _ in 0..2000 {
                        let mut g = m.lock().unwrap();
                        let v = *g;
                        thread::yield_now();
                        *g = v + 1;
                    }
                })
            })
            .collect();
        for h in hs {
            h.join().unwrap();
        }
        assert_eq!(*m.lock().unwrap(), 16_000);
    }
}
