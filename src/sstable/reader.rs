//! SSTable reader: validated open, point lookups, ordered iteration.

use std::fs::File;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

use super::{FOOTER_LEN, Footer, IndexEntry, RawEntry, decode_block, parse_index};
use crate::error::{Error, Result};
use crate::memtable::Entry;

pub struct SstReader {
    file: File,
    path: PathBuf,
    file_len: u64,
    smallest: Vec<u8>,
    index: Vec<IndexEntry>,
    entries: u64,
}

impl SstReader {
    /// Open and validate the footer and index. Data blocks are verified
    /// lazily when read (or all at once with [`SstReader::verify`]).
    pub fn open(path: &Path) -> Result<SstReader> {
        let file = File::open(path).map_err(|e| Error::io("open sstable", path, e))?;
        let file_len = file
            .metadata()
            .map_err(|e| Error::io("stat sstable", path, e))?
            .len();
        if file_len < FOOTER_LEN as u64 {
            return Err(Error::corruption(
                path,
                0,
                format!("file of {file_len} bytes is shorter than a footer"),
            ));
        }
        let mut raw = [0u8; FOOTER_LEN];
        file.read_exact_at(&mut raw, file_len - FOOTER_LEN as u64)
            .map_err(|e| Error::io("read sstable footer", path, e))?;
        let footer = Footer::decode(&raw, path, file_len)?;

        // `Footer::decode` guarantees index_offset + index_len == file_len - FOOTER_LEN
        // and index_len <= MAX_INDEX_LEN, so this buffer is bounded by the real file.
        let mut index_raw = vec![0u8; footer.index_len as usize];
        file.read_exact_at(&mut index_raw, footer.index_offset)
            .map_err(|e| Error::io("read sstable index", path, e))?;
        let (smallest, index) = parse_index(&index_raw, path, footer.index_offset, &footer)?;
        Ok(SstReader {
            file,
            path: path.to_path_buf(),
            file_len,
            smallest,
            index,
            entries: footer.entry_count,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn file_len(&self) -> u64 {
        self.file_len
    }

    pub fn entry_count(&self) -> u64 {
        self.entries
    }

    pub fn block_count(&self) -> usize {
        self.index.len()
    }

    pub fn smallest(&self) -> &[u8] {
        &self.smallest
    }

    pub fn largest(&self) -> &[u8] {
        // The index is validated non-empty at open.
        self.index.last().map_or(&[], |e| e.last_key.as_slice())
    }

    /// Look up `key`. `Ok(None)` means the key is not in this table;
    /// `Ok(Some(Entry::Tombstone))` means it was deleted here (and the caller
    /// must not look in older tables); `Ok(Some(Entry::Value(_)))` is a value,
    /// possibly empty.
    pub fn get(&self, key: &[u8]) -> Result<Option<Entry>> {
        if key.is_empty() || key < self.smallest.as_slice() || key > self.largest() {
            return Ok(None);
        }
        let i = self.index.partition_point(|e| e.last_key.as_slice() < key);
        if i == self.index.len() {
            return Ok(None);
        }
        self.with_block(i, |entries| {
            entries
                .binary_search_by(|e| e.key.cmp(key))
                .ok()
                .map(|pos| entries[pos].to_entry())
        })
    }

    pub fn iter(&self) -> SstIter<'_> {
        SstIter {
            reader: self,
            next_block: 0,
            buffered: Vec::new().into_iter(),
            done: false,
        }
    }

    /// Read and verify every block; returns the number of entries seen.
    /// Checks the cross-structure invariants that `open` cannot see.
    pub fn verify(&self) -> Result<u64> {
        let mut n = 0u64;
        for item in self.iter() {
            item?;
            n += 1;
        }
        if n != self.entries {
            return Err(Error::corruption(
                &self.path,
                0,
                format!("iterated {n} entries, footer says {}", self.entries),
            ));
        }
        Ok(n)
    }

    /// Read block `i`, verify it, check it against the index, and hand its
    /// decoded entries to `f`.
    fn with_block<T>(&self, i: usize, f: impl FnOnce(&[RawEntry<'_>]) -> T) -> Result<T> {
        let e = &self.index[i];
        // block_len was validated against MAX_BLOCK_LEN and the file at open.
        let mut buf = vec![0u8; e.block_len as usize];
        self.file
            .read_exact_at(&mut buf, e.block_offset)
            .map_err(|err| Error::io("read sstable block", &self.path, err))?;
        let entries = decode_block(&buf, e.entries, &self.path, e.block_offset)?;
        // Decoding succeeded, so the block is non-empty.
        let (Some(first), Some(last)) = (entries.first(), entries.last()) else {
            return Err(Error::corruption(&self.path, e.block_offset, "empty block"));
        };
        if last.key != e.last_key.as_slice() {
            return Err(Error::corruption(
                &self.path,
                e.block_offset,
                "block's last key differs from the index",
            ));
        }
        let lower: &[u8] = if i == 0 {
            // First key of the table must be the recorded smallest key.
            if first.key != self.smallest.as_slice() {
                return Err(Error::corruption(
                    &self.path,
                    e.block_offset,
                    "first key differs from the recorded smallest key",
                ));
            }
            &[]
        } else {
            self.index[i - 1].last_key.as_slice()
        };
        if i > 0 && first.key <= lower {
            return Err(Error::corruption(
                &self.path,
                e.block_offset,
                "block overlaps the previous block's key range",
            ));
        }
        Ok(f(&entries))
    }
}

/// Ordered iterator over every entry (tombstones included). After the first
/// error it yields that error once and then ends.
pub struct SstIter<'a> {
    reader: &'a SstReader,
    next_block: usize,
    buffered: std::vec::IntoIter<(Vec<u8>, Entry)>,
    done: bool,
}

impl Iterator for SstIter<'_> {
    type Item = Result<(Vec<u8>, Entry)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        loop {
            if let Some(item) = self.buffered.next() {
                return Some(Ok(item));
            }
            if self.next_block >= self.reader.index.len() {
                self.done = true;
                return None;
            }
            let i = self.next_block;
            self.next_block += 1;
            match self.reader.with_block(i, |entries| {
                entries
                    .iter()
                    .map(|e| (e.key.to_vec(), e.to_entry()))
                    .collect::<Vec<_>>()
            }) {
                Ok(v) => self.buffered = v.into_iter(),
                Err(e) => {
                    self.done = true;
                    return Some(Err(e));
                }
            }
        }
    }
}
