//! The set of live SSTables (a `Version`) and point lookups across them.
//!
//! Invariants (checked by [`Version::check_invariants`]):
//! - L0 is ordered newest first, i.e. by strictly decreasing file number
//!   (flushes allocate increasing numbers). L0 files may overlap.
//! - L1 is sorted by smallest key and its files are pairwise non-overlapping.
//! - Every table's footer, index and metadata matched its manifest entry
//!   when it was opened.
//!
//! A `Version` is immutable; changes build a new one. Tables are shared
//! between versions (and in-flight reads) through `Arc`.
//!
//! # Obsolete files
//! When a compaction commits, its input tables are marked obsolete. The file
//! is deleted when the *last* `Arc<Table>` is dropped -- i.e. once no state
//! snapshot held by a reader or the worker still refers to it -- so a read
//! that started on an older version can always finish. A failed deletion
//! cannot be returned from `Drop`; it is counted in [`DeleteErrors`]
//! (surfaced by `Db::stats`), and the file is removed as an orphan on the
//! next open.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use crate::error::{Error, Result};
use crate::fs::sst_path;
use crate::manifest::{FileMeta, ManifestState, check_l1_disjoint};
use crate::memtable::Entry;
use crate::sstable::{SstBuilder, SstReader};

/// Failures to delete obsolete table files.
#[derive(Debug, Default)]
pub struct DeleteErrors {
    count: AtomicU64,
    last: Mutex<Option<String>>,
}

impl DeleteErrors {
    fn record(&self, path: &Path, e: std::io::Error) {
        self.count.fetch_add(1, Ordering::SeqCst);
        *self.last.lock().unwrap_or_else(PoisonError::into_inner) =
            Some(format!("{}: {e}", path.display()));
    }

    pub fn count(&self) -> u64 {
        self.count.load(Ordering::SeqCst)
    }

    pub fn last(&self) -> Option<String> {
        self.last
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

pub struct Table {
    pub meta: FileMeta,
    pub reader: SstReader,
    path: PathBuf,
    obsolete: AtomicBool,
    delete_errors: Arc<DeleteErrors>,
}

impl Drop for Table {
    fn drop(&mut self) {
        if self.obsolete.load(Ordering::SeqCst) {
            match std::fs::remove_file(&self.path) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => self.delete_errors.record(&self.path, e),
            }
        }
    }
}

impl Table {
    /// Open the table described by `meta` and check it matches.
    pub fn open(dir: &Path, meta: FileMeta, delete_errors: &Arc<DeleteErrors>) -> Result<Table> {
        let path = sst_path(dir, meta.file_no);
        let reader = SstReader::open(&path).map_err(|e| match e {
            Error::Io { source, .. } if source.kind() == std::io::ErrorKind::NotFound => {
                Error::corruption(&path, 0, "sstable referenced by the manifest is missing")
            }
            other => other,
        })?;
        if reader.file_len() != meta.size
            || reader.smallest() != meta.smallest.as_slice()
            || reader.largest() != meta.largest.as_slice()
        {
            return Err(Error::corruption(
                &path,
                0,
                "sstable size or key range does not match the manifest",
            ));
        }
        Ok(Table {
            meta,
            reader,
            path,
            obsolete: AtomicBool::new(false),
            delete_errors: Arc::clone(delete_errors),
        })
    }

    /// Delete the file once the last reference is dropped.
    pub fn mark_obsolete(&self) {
        self.obsolete.store(true, Ordering::SeqCst);
    }

    /// Finish `builder` (which writes `sst_path(dir, file_no)`) and open the result.
    pub fn from_builder(
        dir: &Path,
        file_no: u64,
        builder: SstBuilder,
        delete_errors: &Arc<DeleteErrors>,
    ) -> Result<Arc<Table>> {
        let m = builder.finish()?;
        let meta = FileMeta {
            file_no,
            size: m.size,
            smallest: m.smallest,
            largest: m.largest,
        };
        Ok(Arc::new(Table::open(dir, meta, delete_errors)?))
    }

    fn may_contain(&self, key: &[u8]) -> bool {
        self.meta.smallest.as_slice() <= key && key <= self.meta.largest.as_slice()
    }
}

#[derive(Clone, Default)]
pub struct Version {
    /// Newest first.
    pub l0: Vec<Arc<Table>>,
    /// Sorted by smallest key; non-overlapping.
    pub l1: Vec<Arc<Table>>,
}

impl Version {
    /// Open every table listed in `state`.
    pub fn open(
        dir: &Path,
        state: &ManifestState,
        delete_errors: &Arc<DeleteErrors>,
    ) -> Result<Version> {
        let mut v = Version::default();
        for (level, meta) in state.files.values() {
            let t = Arc::new(Table::open(dir, meta.clone(), delete_errors)?);
            match level {
                0 => v.l0.push(t),
                _ => v.l1.push(t),
            }
        }
        v.sort();
        Ok(v)
    }

    fn sort(&mut self) {
        self.l0.sort_by_key(|t| std::cmp::Reverse(t.meta.file_no));
        self.l1
            .sort_by(|a, b| a.meta.smallest.cmp(&b.meta.smallest));
    }

    /// The newest entry for `key` across all tables: `Some(Tombstone)` means
    /// deleted. Older tables are not consulted once an entry is found.
    pub fn get(&self, key: &[u8]) -> Result<Option<Entry>> {
        for t in &self.l0 {
            if t.may_contain(key)
                && let Some(e) = t.reader.get(key)?
            {
                return Ok(Some(e));
            }
        }
        let i = self.l1.partition_point(|t| t.meta.largest.as_slice() < key);
        match self.l1.get(i) {
            Some(t) if t.may_contain(key) => t.reader.get(key),
            _ => Ok(None),
        }
    }

    /// A new version with `table` as the newest L0 file.
    pub fn with_new_l0(&self, table: Arc<Table>) -> Version {
        let mut v = self.clone();
        v.l0.insert(0, table);
        v
    }

    /// A new version with `removed` file numbers dropped and `added` placed in L1.
    pub fn with_compaction(&self, removed: &[u64], added: Vec<Arc<Table>>) -> Version {
        let keep = |t: &&Arc<Table>| !removed.contains(&t.meta.file_no);
        let mut v = Version {
            l0: self.l0.iter().filter(keep).cloned().collect(),
            l1: self.l1.iter().filter(keep).cloned().collect(),
        };
        v.l1.extend(added);
        v.sort();
        v
    }

    pub fn contains(&self, file_no: u64) -> bool {
        self.tables().any(|(_, t)| t.meta.file_no == file_no)
    }

    /// All tables with their level.
    pub fn tables(&self) -> impl Iterator<Item = (u8, &Arc<Table>)> {
        self.l0
            .iter()
            .map(|t| (0u8, t))
            .chain(self.l1.iter().map(|t| (1u8, t)))
    }

    pub fn check_invariants(&self) -> std::result::Result<(), String> {
        for w in self.l0.windows(2) {
            if w[0].meta.file_no <= w[1].meta.file_no {
                return Err("L0 is not ordered newest first".into());
            }
        }
        for w in self.l1.windows(2) {
            if w[0].meta.smallest > w[1].meta.smallest {
                return Err("L1 is not sorted by smallest key".into());
            }
        }
        check_l1_disjoint(self.l1.iter().map(|t| &t.meta))?;
        let mut numbers: Vec<u64> = self.tables().map(|(_, t)| t.meta.file_no).collect();
        numbers.sort_unstable();
        if numbers.windows(2).any(|w| w[0] == w[1]) {
            return Err("a file appears twice".into());
        }
        for (_, t) in self.tables() {
            if t.meta.smallest > t.meta.largest {
                return Err(format!("file {} has an inverted key range", t.meta.file_no));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Options;
    use crate::fs::sst_path;
    use crate::memtable::Entry;

    fn table(dir: &Path, n: u64, errors: &Arc<DeleteErrors>) -> Arc<Table> {
        let mut b = SstBuilder::new(&sst_path(dir, n), &Options::default()).unwrap();
        b.add(b"k", &Entry::Value(b"v".to_vec())).unwrap();
        Table::from_builder(dir, n, b, errors).unwrap()
    }

    #[test]
    fn obsolete_file_is_deleted_only_after_the_last_reference() {
        let dir = tempfile::TempDir::new().unwrap();
        let errors = Arc::new(DeleteErrors::default());
        let t = table(dir.path(), 1, &errors);
        let path = sst_path(dir.path(), 1);
        let reader_ref = Arc::clone(&t);
        t.mark_obsolete();
        drop(t);
        assert!(path.exists(), "still referenced");
        assert_eq!(
            reader_ref.reader.get(b"k").unwrap(),
            Some(Entry::Value(b"v".to_vec()))
        );
        drop(reader_ref);
        assert!(!path.exists());
        assert_eq!(errors.count(), 0);
    }

    #[test]
    fn live_tables_are_never_deleted_on_drop() {
        let dir = tempfile::TempDir::new().unwrap();
        let errors = Arc::new(DeleteErrors::default());
        drop(table(dir.path(), 1, &errors));
        assert!(sst_path(dir.path(), 1).exists());
    }

    #[test]
    fn failed_deletion_is_recorded_not_ignored() {
        let dir = tempfile::TempDir::new().unwrap();
        let errors = Arc::new(DeleteErrors::default());
        let t = table(dir.path(), 1, &errors);
        // Replace the file with a non-empty directory so unlink fails.
        let path = sst_path(dir.path(), 1);
        std::fs::remove_file(&path).unwrap();
        std::fs::create_dir(&path).unwrap();
        std::fs::write(path.join("x"), b"x").unwrap();
        t.mark_obsolete();
        drop(t);
        assert_eq!(errors.count(), 1);
        assert!(errors.last().unwrap().contains("000001.sst"));
    }
}
