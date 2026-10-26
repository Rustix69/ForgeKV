//! Engine configuration. Defaults are conservative starting points, not
//! performance targets; benchmarks (Phase 7) report the values actually used.

use crate::error::{Error, Result};

/// When the WAL is synced to stable storage relative to acknowledging a write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncMode {
    /// `sync_data` the WAL before every acknowledgement (default).
    Always,
    /// Never sync on the write path. Recent writes may be lost on OS crash or
    /// power loss. For benchmarking and ephemeral data only.
    Never,
}

/// How WAL/manifest recovery treats a damaged record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryMode {
    /// Only an incomplete final frame is discarded; a complete frame with a
    /// bad checksum is `Error::Corruption` (default). See PLAN.md §3.3.
    Strict,
    /// Opt-in: drop everything from the first invalid frame of the newest WAL
    /// and report what was dropped.
    SalvagePrefix,
}

/// Largest accepted `block_bytes`; keeps SSTable blocks well under the
/// format's hard cap (`sstable::MAX_BLOCK_LEN`).
pub const MAX_BLOCK_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Options {
    pub sync_mode: SyncMode,
    pub recovery_mode: RecoveryMode,
    /// Maximum key length in bytes (keys must also be non-empty).
    pub max_key_len: usize,
    /// Maximum value length in bytes.
    pub max_value_len: usize,
    /// Approximate memtable size that triggers a flush.
    pub memtable_bytes: usize,
    /// Number of L0 files that triggers an L0->L1 compaction.
    pub l0_compaction_trigger: usize,
    /// Target size of each L1 SSTable.
    pub sst_target_bytes: usize,
    /// Target size of an SSTable data block.
    pub block_bytes: usize,
    /// Rewrite the MANIFEST as a single snapshot once it grows past this size.
    pub manifest_max_bytes: u64,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            sync_mode: SyncMode::Always,
            recovery_mode: RecoveryMode::Strict,
            max_key_len: 4 * 1024,
            max_value_len: 1024 * 1024,
            memtable_bytes: 4 * 1024 * 1024,
            l0_compaction_trigger: 4,
            sst_target_bytes: 2 * 1024 * 1024,
            block_bytes: 4 * 1024,
            manifest_max_bytes: 1024 * 1024,
        }
    }
}

impl Options {
    /// Check internal consistency. Called by `Db::open` (Phase 1+).
    pub fn validate(&self) -> Result<()> {
        let bad = |msg: &str| Err(Error::InvalidArgument(msg.to_string()));
        if self.max_key_len == 0 {
            return bad("max_key_len must be > 0");
        }
        if self.max_value_len == 0 {
            return bad("max_value_len must be > 0");
        }
        if self.block_bytes == 0 {
            return bad("block_bytes must be > 0");
        }
        if self.block_bytes > MAX_BLOCK_BYTES {
            return bad("block_bytes must be <= 8 MiB");
        }
        if self.manifest_max_bytes == 0 {
            return bad("manifest_max_bytes must be > 0");
        }
        if self.l0_compaction_trigger == 0 {
            return bad("l0_compaction_trigger must be > 0");
        }
        // Record lengths are stored as u32 in the WAL frame header.
        if self.max_key_len.saturating_add(self.max_value_len) > u32::MAX as usize - 16 {
            return bad("max_key_len + max_value_len must fit in a u32 record length");
        }
        // A memtable must be able to hold at least one maximal entry, or a
        // single legal write could never fit.
        if self.memtable_bytes < self.max_key_len.saturating_add(self.max_value_len) {
            return bad("memtable_bytes must be >= max_key_len + max_value_len");
        }
        if self.sst_target_bytes < self.block_bytes {
            return bad("sst_target_bytes must be >= block_bytes");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_valid_and_safe() {
        let o = Options::default();
        assert!(o.validate().is_ok());
        assert_eq!(o.sync_mode, SyncMode::Always);
        assert_eq!(o.recovery_mode, RecoveryMode::Strict);
    }

    #[test]
    fn invalid_options_are_rejected() {
        let cases: [fn(&mut Options); 8] = [
            |o| o.max_key_len = 0,
            |o| o.max_value_len = 0,
            |o| o.block_bytes = 0,
            |o| o.block_bytes = MAX_BLOCK_BYTES + 1,
            |o| o.l0_compaction_trigger = 0,
            |o| o.manifest_max_bytes = 0,
            |o| o.memtable_bytes = 10,
            |o| o.sst_target_bytes = o.block_bytes - 1,
        ];
        for (i, mutate) in cases.iter().enumerate() {
            let mut o = Options::default();
            mutate(&mut o);
            assert!(
                matches!(o.validate(), Err(Error::InvalidArgument(_))),
                "case {i} should be rejected"
            );
        }
    }

    #[test]
    fn huge_limits_do_not_overflow() {
        let o = Options {
            max_key_len: usize::MAX,
            max_value_len: usize::MAX,
            ..Options::default()
        };
        assert!(o.validate().is_err());
    }

    #[test]
    fn record_length_must_fit_u32() {
        let o = Options {
            max_key_len: 4 * 1024,
            max_value_len: u32::MAX as usize,
            memtable_bytes: usize::MAX,
            sst_target_bytes: usize::MAX,
            ..Options::default()
        };
        assert!(matches!(o.validate(), Err(Error::InvalidArgument(_))));
    }
}
