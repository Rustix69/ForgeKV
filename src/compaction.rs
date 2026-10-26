//! L0 -> L1 compaction (PLAN.md Phase 4).
//!
//! # Picking
//! All L0 files, plus every L1 file whose key range overlaps the combined
//! range `[min L0 smallest, max L0 largest]`. L1 files outside that range
//! are untouched.
//!
//! # Why tombstones can be dropped
//! L1 is the bottom level. For any key `k` produced by the merge, every older
//! version of `k` must live in (a) an older L0 file -- all L0 files are
//! inputs -- or (b) the single L1 file whose range contains `k`; since `k`
//! lies inside the L0 range, that file overlaps it and is an input too. So
//! once the newest version of `k` is a tombstone, no older version survives
//! the compaction and the tombstone itself can be discarded. (The memtable is
//! newer than every table and is not affected.)
//!
//! # Outputs
//! Live entries are written in key order into new L1 tables, starting a new
//! table once the current one reaches `sst_target_bytes`. Keys are unique in
//! the merged stream, so consecutive outputs never overlap. Each output is
//! fsynced by the builder. Installing the result (one manifest record that
//! adds every output and deletes every input) is the caller's job.

use std::path::Path;
use std::sync::Arc;

use crate::config::Options;
use crate::error::Result;
use crate::failpoint::FailPoints;
use crate::fs::sst_path;
use crate::memtable::Entry;
use crate::merge::{MergeIter, Source};
use crate::sstable::SstBuilder;
use crate::version::{DeleteErrors, Table, Version};

pub struct Picked {
    /// Newest first.
    pub l0: Vec<Arc<Table>>,
    pub l1: Vec<Arc<Table>>,
}

impl Picked {
    pub fn input_file_numbers(&self) -> Vec<u64> {
        self.l0
            .iter()
            .chain(&self.l1)
            .map(|t| t.meta.file_no)
            .collect()
    }
}

/// Counters describing one compaction.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CompactionStats {
    pub input_files: usize,
    pub output_files: usize,
    pub input_bytes: u64,
    pub output_bytes: u64,
    pub entries_written: u64,
    pub tombstones_dropped: u64,
}

pub fn pick(v: &Version) -> Option<Picked> {
    let smallest = v.l0.iter().map(|t| &t.meta.smallest).min()?;
    let largest = v.l0.iter().map(|t| &t.meta.largest).max()?;
    let l1 =
        v.l1.iter()
            .filter(|t| t.meta.smallest <= *largest && t.meta.largest >= *smallest)
            .cloned()
            .collect();
    Some(Picked {
        l0: v.l0.clone(),
        l1,
    })
}

/// Merge the inputs into new L1 tables numbered by `alloc`. Returns the
/// opened output tables in key order.
pub fn run(
    dir: &Path,
    opts: &Options,
    picked: &Picked,
    mut alloc: impl FnMut() -> u64,
    fp: &FailPoints,
    delete_errors: &Arc<DeleteErrors>,
) -> Result<(Vec<Arc<Table>>, CompactionStats)> {
    let mut stats = CompactionStats {
        input_files: picked.l0.len() + picked.l1.len(),
        input_bytes: picked
            .l0
            .iter()
            .chain(&picked.l1)
            .map(|t| t.meta.size)
            .sum(),
        ..CompactionStats::default()
    };
    // L0 newest first, then L1 (oldest). L1 inputs never overlap each other.
    let sources: Vec<Source<'_>> = picked
        .l0
        .iter()
        .chain(&picked.l1)
        .map(|t| Box::new(t.reader.iter()) as Source<'_>)
        .collect();

    let mut outputs: Vec<Arc<Table>> = Vec::new();
    let mut current: Option<(u64, SstBuilder)> = None;
    for item in MergeIter::new(sources) {
        let (key, entry) = item?;
        if entry == Entry::Tombstone {
            stats.tombstones_dropped += 1;
            continue;
        }
        let (_, builder) = match &mut current {
            Some(c) => c,
            None => {
                let n = alloc();
                current.insert((n, SstBuilder::new(&sst_path(dir, n), opts)?))
            }
        };
        builder.add(&key, &entry)?;
        stats.entries_written += 1;
        if builder.estimated_size() >= opts.sst_target_bytes as u64
            && let Some((n, b)) = current.take()
        {
            outputs.push(Table::from_builder(dir, n, b, delete_errors)?);
            fail_point!(fp, "compaction:after_output");
        }
    }
    if let Some((n, b)) = current.take() {
        outputs.push(Table::from_builder(dir, n, b, delete_errors)?);
        fail_point!(fp, "compaction:after_output");
    }
    stats.output_files = outputs.len();
    stats.output_bytes = outputs.iter().map(|t| t.meta.size).sum();
    Ok((outputs, stats))
}
