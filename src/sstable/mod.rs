//! Sorted, immutable on-disk tables (SSTables).
//!
//! # File format, version 1 (all integers little-endian)
//! ```text
//! [data block 0] ... [data block n-1] [index block] [footer]
//!
//! data block   = entry* | block_crc u32
//!   block_crc  = CRC-32/IEEE over the entries (everything before the crc)
//!   entry      = kind u8 | key_len u32 | val_len u32 | key | value
//!   kind       = 1 Value, 2 Tombstone (a tombstone has val_len = 0)
//!   keys are non-empty and strictly increasing within and across blocks
//!   a block holds >= 1 entry; it ends once it reaches the target size, so a
//!   single large entry may make a block bigger than the target
//!
//! index block  = count u32 | smallest_key_len u32 | smallest_key | index_entry{count} | index_crc u32
//!   index_entry = last_key_len u32 | last_key | block_offset u64 | block_len u32 | block_entries u32
//!   index_crc  = CRC-32/IEEE over everything in the index block before it
//!   `block_len` includes the block's trailing crc
//!
//! footer (56 bytes) =
//!   index_offset u64 | index_len u64 | bloom_offset u64 | bloom_len u64 |
//!   entry_count u64 | version u32 = 1 | magic[8] = "FKVSST\0\1" | footer_crc u32
//!   footer_crc = CRC-32/IEEE over the preceding 52 footer bytes
//!   bloom_offset and bloom_len are reserved and must be 0 in version 1
//! ```
//!
//! # Structural invariants enforced by the reader at open
//! - the footer checksum, magic and version are valid, and the index block
//!   ends exactly where the footer begins;
//! - data blocks tile `[0, index_offset)` exactly, in order, with no gaps;
//! - every block length is within `[MIN_BLOCK_LEN, MAX_BLOCK_LEN]` and the
//!   block entry counts sum to the footer's `entry_count`;
//! - index last-keys are non-empty and strictly increasing, and
//!   `smallest_key <= first last_key`;
//! - the table is non-empty.
//!
//! Each data block is additionally checked when it is read: checksum, entry
//! framing (every length is bounds-checked against the block), strict key
//! order, kind/length rules, entry count, and agreement with the index.
//!
//! # Bounded allocation
//! No allocation is sized from an unchecked on-disk field. The index buffer is
//! limited by `MAX_INDEX_LEN` and by the real file length; a block buffer by
//! `MAX_BLOCK_LEN` and by the index tiling; per-block entry vectors by
//! `block_len / MIN_ENTRY_LEN`. Fields are validated *before* they are used
//! as a size.
//!
//! # Limitations
//! - No compression, no prefix compression, no Bloom filter (reserved).
//! - Blocks are scanned linearly after an index binary search.
//! - The builder fsyncs the file but not its directory; the caller must
//!   `sync_dir` before referencing the table (the engine does, before the
//!   MANIFEST commit).
//! - Linux/Unix only (`read_at`). The whole index is held in memory.

mod builder;
mod reader;

pub use builder::SstBuilder;
pub use reader::{SstIter, SstReader};

use std::path::Path;

use crate::codec::Cur;
use crate::error::{Error, Result};
use crate::memtable::Entry;

pub const MAGIC: [u8; 8] = *b"FKVSST\0\x01";
pub const FORMAT_VERSION: u32 = 1;
pub const FOOTER_LEN: usize = 56;
pub const BLOCK_CRC_LEN: usize = 4;
pub const KIND_VALUE: u8 = 1;
pub const KIND_TOMBSTONE: u8 = 2;
/// kind (1) + key_len (4) + val_len (4); an entry also has a >= 1 byte key.
pub const ENTRY_HEADER_LEN: usize = 9;
/// Smallest possible entry: header + 1-byte key.
pub const MIN_ENTRY_LEN: usize = ENTRY_HEADER_LEN + 1;
/// Smallest possible block: one minimal entry + crc.
pub const MIN_BLOCK_LEN: usize = MIN_ENTRY_LEN + BLOCK_CRC_LEN;
/// Hard cap on one block, independent of configuration.
pub const MAX_BLOCK_LEN: usize = 16 * 1024 * 1024;
/// Hard cap on the index block, independent of configuration.
pub const MAX_INDEX_LEN: usize = 1 << 30;
/// count + smallest_key_len + crc, with a 1-byte smallest key, plus one index entry.
const MIN_INDEX_ENTRY_LEN: usize = 4 + 1 + 8 + 4 + 4;

/// Summary of a finished table, for the (Phase 3) manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableMeta {
    pub size: u64,
    pub smallest: Vec<u8>,
    pub largest: Vec<u8>,
    pub entries: u64,
    pub blocks: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexEntry {
    pub last_key: Vec<u8>,
    pub block_offset: u64,
    /// Includes the trailing block crc.
    pub block_len: u32,
    pub entries: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Footer {
    pub index_offset: u64,
    pub index_len: u64,
    pub bloom_offset: u64,
    pub bloom_len: u64,
    pub entry_count: u64,
}

fn corrupt(path: &Path, offset: u64, detail: impl Into<String>) -> Error {
    Error::corruption(path, offset, detail)
}

impl Footer {
    pub fn encode(&self) -> [u8; FOOTER_LEN] {
        let mut b = [0u8; FOOTER_LEN];
        b[0..8].copy_from_slice(&self.index_offset.to_le_bytes());
        b[8..16].copy_from_slice(&self.index_len.to_le_bytes());
        b[16..24].copy_from_slice(&self.bloom_offset.to_le_bytes());
        b[24..32].copy_from_slice(&self.bloom_len.to_le_bytes());
        b[32..40].copy_from_slice(&self.entry_count.to_le_bytes());
        b[40..44].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
        b[44..52].copy_from_slice(&MAGIC);
        let crc = crc32fast::hash(&b[..52]);
        b[52..56].copy_from_slice(&crc.to_le_bytes());
        b
    }

    /// Validate magic, checksum and version, then the layout against `file_len`.
    /// Nothing in the footer is trusted until its checksum has been verified.
    pub fn decode(b: &[u8; FOOTER_LEN], path: &Path, file_len: u64) -> Result<Footer> {
        let footer_at = file_len.saturating_sub(FOOTER_LEN as u64);
        if b[44..52] != MAGIC {
            return Err(corrupt(path, footer_at, "bad sstable magic"));
        }
        let stored = u32::from_le_bytes([b[52], b[53], b[54], b[55]]);
        if crc32fast::hash(&b[..52]) != stored {
            return Err(corrupt(path, footer_at, "footer checksum mismatch"));
        }
        let version = u32::from_le_bytes([b[40], b[41], b[42], b[43]]);
        if version != FORMAT_VERSION {
            return Err(Error::UnsupportedVersion {
                file: path.to_path_buf(),
                version,
            });
        }
        let mut c = Cur::new(&b[..40]);
        // 40 bytes were just sliced, so these reads cannot fail.
        let mut next = || c.u64().map_err(|d| corrupt(path, footer_at, d));
        let f = Footer {
            index_offset: next()?,
            index_len: next()?,
            bloom_offset: next()?,
            bloom_len: next()?,
            entry_count: next()?,
        };
        if f.bloom_offset != 0 || f.bloom_len != 0 {
            return Err(corrupt(
                path,
                footer_at,
                "reserved bloom fields are non-zero",
            ));
        }
        if f.index_len < (4 + 4 + 1 + MIN_INDEX_ENTRY_LEN + BLOCK_CRC_LEN) as u64
            || f.index_len > MAX_INDEX_LEN as u64
        {
            return Err(corrupt(
                path,
                footer_at,
                format!("invalid index length {}", f.index_len),
            ));
        }
        match f.index_offset.checked_add(f.index_len) {
            Some(end) if end == footer_at => {}
            _ => {
                return Err(corrupt(
                    path,
                    footer_at,
                    "index block does not end where the footer begins",
                ));
            }
        }
        if f.entry_count == 0 {
            return Err(corrupt(path, footer_at, "table has no entries"));
        }
        Ok(f)
    }
}

pub fn encode_index(smallest: &[u8], entries: &[IndexEntry]) -> Vec<u8> {
    let mut b = Vec::new();
    b.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    b.extend_from_slice(&(smallest.len() as u32).to_le_bytes());
    b.extend_from_slice(smallest);
    for e in entries {
        b.extend_from_slice(&(e.last_key.len() as u32).to_le_bytes());
        b.extend_from_slice(&e.last_key);
        b.extend_from_slice(&e.block_offset.to_le_bytes());
        b.extend_from_slice(&e.block_len.to_le_bytes());
        b.extend_from_slice(&e.entries.to_le_bytes());
    }
    let crc = crc32fast::hash(&b);
    b.extend_from_slice(&crc.to_le_bytes());
    b
}

/// Parse and fully validate an index block (see the module docs for the
/// invariants). `index_offset` is where the block sits in the file.
pub fn parse_index(
    raw: &[u8],
    path: &Path,
    index_offset: u64,
    footer: &Footer,
) -> Result<(Vec<u8>, Vec<IndexEntry>)> {
    let bad = |detail: String| corrupt(path, index_offset, detail);
    let (body, crc_bytes) = raw.split_at(raw.len() - BLOCK_CRC_LEN);
    let stored = u32::from_le_bytes([crc_bytes[0], crc_bytes[1], crc_bytes[2], crc_bytes[3]]);
    if crc32fast::hash(body) != stored {
        return Err(bad("index checksum mismatch".into()));
    }
    let mut c = Cur::new(body);
    let count = c.u32().map_err(&bad)? as usize;
    // Bound the allocation by what the block could physically hold.
    if count == 0 || count > body.len() / MIN_INDEX_ENTRY_LEN {
        return Err(bad(format!("implausible index entry count {count}")));
    }
    let smallest_len = c.u32().map_err(&bad)? as usize;
    if smallest_len == 0 || smallest_len > c.remaining() {
        return Err(bad(format!("invalid smallest key length {smallest_len}")));
    }
    let smallest = c.take(smallest_len).map_err(&bad)?.to_vec();

    let mut entries: Vec<IndexEntry> = Vec::with_capacity(count);
    let mut next_offset = 0u64;
    let mut total_entries = 0u64;
    for i in 0..count {
        let klen = c.u32().map_err(&bad)? as usize;
        if klen == 0 || klen > c.remaining() {
            return Err(bad(format!("index entry {i}: invalid key length {klen}")));
        }
        let last_key = c.take(klen).map_err(&bad)?.to_vec();
        let block_offset = c.u64().map_err(&bad)?;
        let block_len = c.u32().map_err(&bad)?;
        let n = c.u32().map_err(&bad)?;

        if block_offset != next_offset {
            return Err(bad(format!(
                "index entry {i}: block offset {block_offset} != expected {next_offset}"
            )));
        }
        let len = block_len as usize;
        if !(MIN_BLOCK_LEN..=MAX_BLOCK_LEN).contains(&len) {
            return Err(bad(format!("index entry {i}: invalid block length {len}")));
        }
        next_offset = match next_offset.checked_add(u64::from(block_len)) {
            Some(end) if end <= index_offset => end,
            _ => {
                return Err(bad(format!(
                    "index entry {i}: block extends past the data area"
                )));
            }
        };
        if n == 0 || n as usize > (len - BLOCK_CRC_LEN) / MIN_ENTRY_LEN {
            return Err(bad(format!("index entry {i}: invalid entry count {n}")));
        }
        total_entries += u64::from(n);
        if let Some(prev) = entries.last() {
            if prev.last_key >= last_key {
                return Err(bad(format!(
                    "index entry {i}: keys not strictly increasing"
                )));
            }
        } else if smallest > last_key {
            return Err(bad(
                "smallest key is greater than the first block's last key".into(),
            ));
        }
        entries.push(IndexEntry {
            last_key,
            block_offset,
            block_len,
            entries: n,
        });
    }
    if c.remaining() != 0 {
        return Err(bad(format!("{} trailing bytes in index", c.remaining())));
    }
    if next_offset != index_offset {
        return Err(bad(
            "data blocks do not fill the area before the index".into()
        ));
    }
    if total_entries != footer.entry_count {
        return Err(bad(format!(
            "block entry counts sum to {total_entries}, footer says {}",
            footer.entry_count
        )));
    }
    Ok((smallest, entries))
}

pub fn encode_entry(buf: &mut Vec<u8>, key: &[u8], entry: &Entry) {
    let (kind, value): (u8, &[u8]) = match entry {
        Entry::Value(v) => (KIND_VALUE, v),
        Entry::Tombstone => (KIND_TOMBSTONE, &[]),
    };
    buf.push(kind);
    buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
    buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
    buf.extend_from_slice(key);
    buf.extend_from_slice(value);
}

/// One decoded entry, borrowing from the block buffer. `value` is `None` for
/// a tombstone (distinct from `Some(&[])`, an empty value).
#[derive(Debug, Clone, Copy)]
pub struct RawEntry<'a> {
    pub key: &'a [u8],
    pub value: Option<&'a [u8]>,
}

impl RawEntry<'_> {
    pub fn to_entry(&self) -> Entry {
        match self.value {
            Some(v) => Entry::Value(v.to_vec()),
            None => Entry::Tombstone,
        }
    }
}

/// Verify and decode one data block (`block` includes its trailing crc).
/// `expected_entries` comes from the (already validated) index.
pub fn decode_block<'a>(
    block: &'a [u8],
    expected_entries: u32,
    path: &Path,
    block_offset: u64,
) -> Result<Vec<RawEntry<'a>>> {
    let bad = |at: usize, detail: String| corrupt(path, block_offset + at as u64, detail);
    if block.len() < MIN_BLOCK_LEN {
        return Err(bad(
            0,
            format!("block of {} bytes is too short", block.len()),
        ));
    }
    let (body, crc_bytes) = block.split_at(block.len() - BLOCK_CRC_LEN);
    let stored = u32::from_le_bytes([crc_bytes[0], crc_bytes[1], crc_bytes[2], crc_bytes[3]]);
    if crc32fast::hash(body) != stored {
        return Err(bad(0, "block checksum mismatch".into()));
    }
    let expected = expected_entries as usize;
    if expected == 0 || expected > body.len() / MIN_ENTRY_LEN {
        return Err(bad(0, format!("implausible entry count {expected}")));
    }
    let mut out: Vec<RawEntry<'a>> = Vec::with_capacity(expected);
    let mut c = Cur::new(body);
    while c.remaining() > 0 {
        let at = c.pos();
        let kind = c.u8().map_err(|d| bad(at, d))?;
        let klen = c.u32().map_err(|d| bad(at, d))? as usize;
        let vlen = c.u32().map_err(|d| bad(at, d))? as usize;
        if klen == 0 || klen > c.remaining() {
            return Err(bad(at, format!("invalid key length {klen}")));
        }
        let key = c.take(klen).map_err(|d| bad(at, d))?;
        if vlen > c.remaining() {
            return Err(bad(at, format!("value length {vlen} exceeds block")));
        }
        let raw = c.take(vlen).map_err(|d| bad(at, d))?;
        let value = match kind {
            KIND_VALUE => Some(raw),
            KIND_TOMBSTONE if vlen == 0 => None,
            KIND_TOMBSTONE => return Err(bad(at, "tombstone carries a value".into())),
            other => return Err(bad(at, format!("unknown entry kind {other}"))),
        };
        if out.len() == expected {
            return Err(bad(at, "more entries than the index declares".into()));
        }
        if let Some(prev) = out.last()
            && prev.key >= key
        {
            return Err(bad(at, "keys not strictly increasing".into()));
        }
        out.push(RawEntry { key, value });
    }
    if out.len() != expected {
        return Err(bad(
            0,
            format!("block has {} entries, index declares {expected}", out.len()),
        ));
    }
    Ok(out)
}
