//! SSTable writer. Entries must be added in strictly increasing key order.

use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use super::{
    BLOCK_CRC_LEN, ENTRY_HEADER_LEN, Footer, IndexEntry, MAX_BLOCK_LEN, MAX_INDEX_LEN, TableMeta,
    encode_entry, encode_index,
};
use crate::config::Options;
use crate::error::{Error, Result};
use crate::memtable::Entry;

pub struct SstBuilder {
    out: Option<BufWriter<File>>,
    path: PathBuf,
    max_key_len: usize,
    max_value_len: usize,
    block_bytes: usize,
    /// File offset of the next byte to be written.
    offset: u64,
    block: Vec<u8>,
    block_entries: u32,
    index: Vec<IndexEntry>,
    smallest: Option<Vec<u8>>,
    last_key: Option<Vec<u8>>,
    entries: u64,
    /// Set after an I/O error: the file may be partial, so refuse further use.
    failed: bool,
    finished: bool,
}

impl SstBuilder {
    /// Create a new table file at `path`; fails if the file already exists.
    pub fn new(path: &Path, opts: &Options) -> Result<Self> {
        opts.validate()?;
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(|e| Error::io("create sstable", path, e))?;
        Ok(SstBuilder {
            out: Some(BufWriter::new(file)),
            path: path.to_path_buf(),
            max_key_len: opts.max_key_len,
            max_value_len: opts.max_value_len,
            block_bytes: opts.block_bytes,
            offset: 0,
            block: Vec::new(),
            block_entries: 0,
            index: Vec::new(),
            smallest: None,
            last_key: None,
            entries: 0,
            failed: false,
            finished: false,
        })
    }

    /// Append one entry. Invalid input (empty/oversize key or value, key not
    /// greater than the previous one, duplicate) is rejected without touching
    /// the file and the builder remains usable.
    pub fn add(&mut self, key: &[u8], entry: &Entry) -> Result<()> {
        if self.failed {
            return Err(Error::Poisoned);
        }
        if key.is_empty() || key.len() > self.max_key_len {
            return Err(Error::InvalidArgument(format!(
                "key length {} outside 1..={}",
                key.len(),
                self.max_key_len
            )));
        }
        let value_len = match entry {
            Entry::Value(v) => v.len(),
            Entry::Tombstone => 0,
        };
        if value_len > self.max_value_len {
            return Err(Error::InvalidArgument(format!(
                "value length {value_len} exceeds max {}",
                self.max_value_len
            )));
        }
        if let Some(last) = &self.last_key
            && key <= last.as_slice()
        {
            return Err(Error::InvalidArgument(if key == last.as_slice() {
                "duplicate key".to_string()
            } else {
                "keys must be added in strictly increasing order".to_string()
            }));
        }
        let entry_len = ENTRY_HEADER_LEN + key.len() + value_len;
        if entry_len + BLOCK_CRC_LEN > MAX_BLOCK_LEN {
            return Err(Error::InvalidArgument(format!(
                "entry of {entry_len} bytes cannot fit in a block"
            )));
        }

        if !self.block.is_empty() && self.block.len() + entry_len + BLOCK_CRC_LEN > MAX_BLOCK_LEN {
            self.flush_block()?;
        }
        encode_entry(&mut self.block, key, entry);
        self.block_entries += 1;
        self.entries += 1;
        if self.smallest.is_none() {
            self.smallest = Some(key.to_vec());
        }
        self.last_key = Some(key.to_vec());
        if self.block.len() >= self.block_bytes {
            self.flush_block()?;
        }
        Ok(())
    }

    /// Bytes written so far plus the pending block: the approximate size the
    /// table would have if finished now (excluding index and footer).
    pub fn estimated_size(&self) -> u64 {
        self.offset + self.block.len() as u64
    }

    /// Write the index and footer, fsync the file, and return its summary.
    /// The directory is *not* synced; the caller does that before publishing
    /// the table. An empty table is rejected (and its file removed).
    pub fn finish(mut self) -> Result<TableMeta> {
        if self.failed {
            return Err(Error::Poisoned);
        }
        let (Some(smallest), Some(largest)) = (self.smallest.clone(), self.last_key.clone()) else {
            return Err(Error::InvalidArgument(
                "cannot finish an empty sstable".into(),
            ));
        };
        if !self.block.is_empty() {
            self.flush_block()?;
        }
        let index_bytes = encode_index(&smallest, &self.index);
        if index_bytes.len() > MAX_INDEX_LEN {
            return Err(Error::InvalidArgument("index block too large".into()));
        }
        let footer = Footer {
            index_offset: self.offset,
            index_len: index_bytes.len() as u64,
            bloom_offset: 0,
            bloom_len: 0,
            entry_count: self.entries,
        };
        self.write(&index_bytes)?;
        self.write(&footer.encode())?;
        let size = self.offset;

        let path = self.path.clone();
        let Some(out) = self.out.take() else {
            return Err(Error::Poisoned);
        };
        let file = out.into_inner().map_err(|e| {
            self.failed = true;
            Error::io("flush sstable", &path, e.into_error())
        })?;
        file.sync_all().map_err(|e| {
            self.failed = true;
            Error::io("sync sstable", &path, e)
        })?;
        self.finished = true;
        Ok(TableMeta {
            size,
            smallest,
            largest,
            entries: self.entries,
            blocks: self.index.len(),
        })
    }

    fn flush_block(&mut self) -> Result<()> {
        let crc = crc32fast::hash(&self.block);
        self.block.extend_from_slice(&crc.to_le_bytes());
        let block = std::mem::take(&mut self.block);
        let block_offset = self.offset;
        self.write(&block)?;
        let Some(last_key) = self.last_key.clone() else {
            return Err(Error::Poisoned);
        };
        self.index.push(IndexEntry {
            last_key,
            block_offset,
            block_len: block.len() as u32,
            entries: self.block_entries,
        });
        self.block_entries = 0;
        Ok(())
    }

    fn write(&mut self, bytes: &[u8]) -> Result<()> {
        let Some(out) = self.out.as_mut() else {
            return Err(Error::Poisoned);
        };
        if let Err(e) = out.write_all(bytes) {
            self.failed = true;
            return Err(Error::io("write sstable", &self.path, e));
        }
        self.offset += bytes.len() as u64;
        Ok(())
    }
}

impl Drop for SstBuilder {
    /// An unfinished builder removes its partial file (best effort; Phase 3
    /// also deletes unreferenced files on open, so a failure here is benign).
    fn drop(&mut self) {
        if !self.finished {
            self.out = None;
            let _ = fs::remove_file(&self.path);
        }
    }
}
