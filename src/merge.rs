//! k-way merge of sorted entry streams.
//!
//! Sources are given newest first: on equal keys the entry from the source
//! with the *lowest index* wins and the older duplicates are skipped. Every
//! source must yield strictly increasing keys; the merge checks that its own
//! output is strictly increasing and reports a violation as corruption. The
//! first error from any source ends the merge.

use std::cmp::Reverse;
use std::collections::BinaryHeap;

use crate::error::{Error, Result};
use crate::memtable::Entry;

pub type Source<'a> = Box<dyn Iterator<Item = Result<(Vec<u8>, Entry)>> + 'a>;

pub struct MergeIter<'a> {
    sources: Vec<Source<'a>>,
    /// Current entry of each source; `Some` exactly when the source has a
    /// `(key, index)` item in the heap.
    heads: Vec<Option<Entry>>,
    /// Min-heap on (key, source index).
    heap: BinaryHeap<Reverse<(Vec<u8>, usize)>>,
    error: Option<Error>,
    last_key: Option<Vec<u8>>,
    done: bool,
}

impl<'a> MergeIter<'a> {
    pub fn new(sources: Vec<Source<'a>>) -> Self {
        let n = sources.len();
        let mut m = MergeIter {
            sources,
            heads: (0..n).map(|_| None).collect(),
            heap: BinaryHeap::with_capacity(n),
            error: None,
            last_key: None,
            done: false,
        };
        for i in 0..n {
            m.advance(i);
        }
        m
    }

    fn advance(&mut self, i: usize) {
        if self.error.is_some() {
            return;
        }
        match self.sources[i].next() {
            Some(Ok((k, e))) => {
                self.heads[i] = Some(e);
                self.heap.push(Reverse((k, i)));
            }
            Some(Err(e)) => self.error = Some(e),
            None => {}
        }
    }

    fn fail(&mut self, e: Error) -> Option<Result<(Vec<u8>, Entry)>> {
        self.done = true;
        Some(Err(e))
    }
}

impl Iterator for MergeIter<'_> {
    type Item = Result<(Vec<u8>, Entry)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        if let Some(e) = self.error.take() {
            return self.fail(e);
        }
        let Some(Reverse((key, i))) = self.heap.pop() else {
            self.done = true;
            return None;
        };
        let Some(entry) = self.heads[i].take() else {
            return self.fail(Error::corruption("", 0, "merge: heap and heads disagree"));
        };
        self.advance(i);
        // Skip older versions of the same key.
        while let Some(Reverse((k, j))) = self.heap.peek() {
            if *k != key {
                break;
            }
            let j = *j;
            self.heap.pop();
            self.heads[j] = None;
            self.advance(j);
        }
        if let Some(e) = self.error.take() {
            return self.fail(e);
        }
        if self.last_key.as_ref().is_some_and(|last| *last >= key) {
            return self.fail(Error::corruption("", 0, "merge input is not sorted"));
        }
        self.last_key = Some(key.clone());
        Some(Ok((key, entry)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    type Items = Vec<(Vec<u8>, Entry)>;

    fn src(items: Items) -> Source<'static> {
        Box::new(items.into_iter().map(Ok))
    }

    fn val(s: &str) -> Entry {
        Entry::Value(s.as_bytes().to_vec())
    }

    fn collect(m: MergeIter<'_>) -> Result<Items> {
        m.collect()
    }

    #[test]
    fn newest_source_wins_and_duplicates_are_dropped() {
        let newest = vec![
            (b"b".to_vec(), Entry::Tombstone),
            (b"d".to_vec(), val("d0")),
        ];
        let middle = vec![(b"a".to_vec(), val("a1")), (b"b".to_vec(), val("b1"))];
        let oldest = vec![
            (b"a".to_vec(), val("a2")),
            (b"b".to_vec(), val("b2")),
            (b"c".to_vec(), val("c2")),
            (b"d".to_vec(), val("d2")),
        ];
        let out = collect(MergeIter::new(vec![src(newest), src(middle), src(oldest)])).unwrap();
        assert_eq!(
            out,
            vec![
                (b"a".to_vec(), val("a1")),
                (b"b".to_vec(), Entry::Tombstone),
                (b"c".to_vec(), val("c2")),
                (b"d".to_vec(), val("d0")),
            ]
        );
    }

    #[test]
    fn empty_inputs() {
        assert!(collect(MergeIter::new(vec![])).unwrap().is_empty());
        assert!(
            collect(MergeIter::new(vec![src(vec![]), src(vec![])]))
                .unwrap()
                .is_empty()
        );
        let one = vec![(b"x".to_vec(), val("1"))];
        assert_eq!(
            collect(MergeIter::new(vec![src(vec![]), src(one.clone())])).unwrap(),
            one
        );
    }

    #[test]
    fn errors_are_propagated_once_and_end_the_merge() {
        let failing: Source<'static> = Box::new(
            vec![
                Ok((b"a".to_vec(), val("1"))),
                Err(Error::corruption("t", 7, "boom")),
            ]
            .into_iter(),
        );
        let mut m = MergeIter::new(vec![failing, src(vec![(b"b".to_vec(), val("2"))])]);
        assert!(matches!(m.next(), Some(Err(Error::Corruption { .. }))));
        assert!(m.next().is_none());
    }

    #[test]
    fn unsorted_source_is_detected() {
        let bad = vec![(b"b".to_vec(), val("1")), (b"a".to_vec(), val("2"))];
        assert!(collect(MergeIter::new(vec![src(bad)])).is_err());
    }

    /// Random sources vs. a model where source 0 is applied last (newest).
    #[test]
    fn randomized_against_model() {
        let mut seed = 0x1234_5678_9abc_def1u64;
        let mut rnd = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        for round in 0..200 {
            let n_sources = 2 + (rnd() % 7) as usize;
            let mut sources: Vec<BTreeMap<Vec<u8>, Entry>> = vec![];
            for _ in 0..n_sources {
                let mut m = BTreeMap::new();
                for _ in 0..(rnd() % 30) {
                    let k = vec![(rnd() % 40) as u8];
                    let e = if rnd() % 4 == 0 {
                        Entry::Tombstone
                    } else {
                        Entry::Value(vec![(rnd() % 255) as u8])
                    };
                    m.insert(k, e);
                }
                sources.push(m);
            }
            let mut model = BTreeMap::new();
            for s in sources.iter().rev() {
                for (k, e) in s {
                    model.insert(k.clone(), e.clone());
                }
            }
            let iters = sources
                .iter()
                .map(|s| src(s.iter().map(|(k, e)| (k.clone(), e.clone())).collect()))
                .collect();
            let out = collect(MergeIter::new(iters)).unwrap();
            let expected: Items = model.into_iter().collect();
            assert_eq!(out, expected, "round {round}");
        }
    }
}
