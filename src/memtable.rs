//! In-memory sorted index of the newest writes.
//!
//! Invariants:
//! - A key maps to exactly one [`Entry`]; a later write replaces the earlier one.
//! - A delete stores a [`Entry::Tombstone`], even for a key never written, so it
//!   can later shadow older data in SSTables. A tombstone is distinct from an
//!   empty value.
//! - `approx_bytes` always equals the sum of `entry_cost` over all entries.
//!
//! # Concurrency
//! The map sits behind an `RwLock`: one writer (the engine serializes writes)
//! and any number of concurrent readers. An immutable memtable is simply one
//! that no longer receives writes. A poisoned lock is recovered from with
//! `into_inner`: every critical section is a single `BTreeMap` operation plus
//! a size update, which leaves the map consistent even if a thread panics
//! mid-way (it cannot panic between the two except on allocation failure,
//! which aborts).

use std::collections::BTreeMap;
use std::sync::{PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Entry {
    Value(Vec<u8>),
    Tombstone,
}

/// Per-entry bookkeeping overhead added to key+value bytes. An estimate for
/// flush thresholds, not an exact allocator measurement.
const ENTRY_OVERHEAD: usize = 48;

fn entry_cost(key_len: usize, entry: &Entry) -> usize {
    let value_len = match entry {
        Entry::Value(v) => v.len(),
        Entry::Tombstone => 0,
    };
    key_len + value_len + ENTRY_OVERHEAD
}

#[derive(Debug, Default)]
struct Inner {
    map: BTreeMap<Vec<u8>, Entry>,
    approx_bytes: usize,
}

#[derive(Debug, Default)]
pub struct Memtable {
    inner: RwLock<Inner>,
}

impl Memtable {
    pub fn new() -> Self {
        Self::default()
    }

    fn read(&self) -> RwLockReadGuard<'_, Inner> {
        self.inner.read().unwrap_or_else(PoisonError::into_inner)
    }

    fn write(&self) -> RwLockWriteGuard<'_, Inner> {
        self.inner.write().unwrap_or_else(PoisonError::into_inner)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) {
        self.insert(key, Entry::Value(value.to_vec()));
    }

    pub fn delete(&self, key: &[u8]) {
        self.insert(key, Entry::Tombstone);
    }

    fn insert(&self, key: &[u8], entry: Entry) {
        let new_cost = entry_cost(key.len(), &entry);
        let mut g = self.write();
        let inner = &mut *g;
        match inner.map.get_mut(key) {
            Some(slot) => {
                inner.approx_bytes -= entry_cost(key.len(), slot);
                *slot = entry;
            }
            None => {
                inner.map.insert(key.to_vec(), entry);
            }
        }
        inner.approx_bytes += new_cost;
    }

    /// What inserting an entry with these lengths adds to `approx_bytes`
    /// (an upper bound when it replaces an existing key).
    pub fn entry_cost(key_len: usize, value_len: usize) -> usize {
        key_len + value_len + ENTRY_OVERHEAD
    }

    pub fn get(&self, key: &[u8]) -> Option<Entry> {
        self.read().map.get(key).cloned()
    }

    /// Number of distinct keys, tombstones included.
    pub fn len(&self) -> usize {
        self.read().map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.read().map.is_empty()
    }

    pub fn approx_bytes(&self) -> usize {
        self.read().approx_bytes
    }

    /// Visit entries in ascending key order under the read lock, stopping at
    /// the first error.
    pub fn try_for_each<E>(
        &self,
        mut f: impl FnMut(&[u8], &Entry) -> Result<(), E>,
    ) -> Result<(), E> {
        let g = self.read();
        for (k, e) in &g.map {
            f(k, e)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::Infallible;
    use std::sync::Arc;

    fn entries(m: &Memtable) -> Vec<(Vec<u8>, Entry)> {
        let mut v = vec![];
        m.try_for_each(|k, e| {
            v.push((k.to_vec(), e.clone()));
            Ok::<(), Infallible>(())
        })
        .unwrap();
        v
    }

    #[test]
    fn put_get_overwrite() {
        let m = Memtable::new();
        assert!(m.get(b"k").is_none());
        m.put(b"k", b"v1");
        assert_eq!(m.get(b"k"), Some(Entry::Value(b"v1".to_vec())));
        m.put(b"k", b"v2");
        assert_eq!(m.get(b"k"), Some(Entry::Value(b"v2".to_vec())));
        assert_eq!(m.len(), 1);
    }

    #[test]
    fn delete_stores_tombstone_even_for_absent_key() {
        let m = Memtable::new();
        m.delete(b"ghost");
        assert_eq!(m.get(b"ghost"), Some(Entry::Tombstone));
        m.put(b"a", b"1");
        m.delete(b"a");
        assert_eq!(m.get(b"a"), Some(Entry::Tombstone));
        m.put(b"a", b"2");
        assert_eq!(m.get(b"a"), Some(Entry::Value(b"2".to_vec())));
    }

    #[test]
    fn empty_value_is_not_a_tombstone() {
        let m = Memtable::new();
        m.put(b"k", b"");
        assert_eq!(m.get(b"k"), Some(Entry::Value(Vec::new())));
        assert_ne!(m.get(b"k"), Some(Entry::Tombstone));
    }

    #[test]
    fn size_accounting_matches_recomputation() {
        let m = Memtable::new();
        let check = |m: &Memtable| {
            let expected: usize = entries(m).iter().map(|(k, e)| entry_cost(k.len(), e)).sum();
            assert_eq!(m.approx_bytes(), expected);
        };
        check(&m);
        m.put(b"a", b"short");
        check(&m);
        m.put(b"a", &[7u8; 1000]);
        check(&m);
        m.delete(b"a");
        check(&m);
        m.delete(b"b");
        check(&m);
        m.put(b"b", b"");
        check(&m);
        assert!(m.approx_bytes() > 0);
    }

    #[test]
    fn iterates_in_key_order_and_stops_on_error() {
        let m = Memtable::new();
        for k in [&b"c"[..], b"a", b"b"] {
            m.put(k, b"x");
        }
        let keys: Vec<Vec<u8>> = entries(&m).into_iter().map(|(k, _)| k).collect();
        assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
        let mut seen = 0;
        let r = m.try_for_each(|_, _| {
            seen += 1;
            if seen == 2 { Err("stop") } else { Ok(()) }
        });
        assert_eq!((r, seen), (Err("stop"), 2));
    }

    #[test]
    fn concurrent_readers_see_complete_entries() {
        let m = Arc::new(Memtable::new());
        let writer = {
            let m = Arc::clone(&m);
            std::thread::spawn(move || {
                for i in 0..2000u32 {
                    let k = (i % 50).to_le_bytes();
                    m.put(&k, &i.to_le_bytes().repeat(8));
                }
            })
        };
        let readers: Vec<_> = (0..3)
            .map(|_| {
                let m = Arc::clone(&m);
                std::thread::spawn(move || {
                    for i in 0..2000u32 {
                        if let Some(Entry::Value(v)) = m.get(&(i % 50).to_le_bytes()) {
                            // Every value is one u32 repeated 8 times.
                            assert_eq!(v.len(), 32);
                            assert!(v.chunks(4).all(|c| c == &v[..4]));
                        }
                    }
                })
            })
            .collect();
        writer.join().unwrap();
        for r in readers {
            r.join().unwrap();
        }
        assert_eq!(m.len(), 50);
    }
}
