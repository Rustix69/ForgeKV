//! ForgeKV: an educational, persistent key-value storage engine.
//!
//! See `README.md` for the architecture, durability semantics and usage, and
//! `docs/FORMATS.md` for the on-disk and wire formats. Main entry points:
//! [`Db`] (the storage engine), [`server::serve`] (the TCP server) and
//! [`client::Client`] (the async client).

#![forbid(unsafe_code)]

#[macro_use]
pub mod failpoint;

pub mod client;
mod codec;
pub mod compaction;
pub mod config;
pub mod engine;
pub mod error;
mod fairlock;
pub mod fs;
pub mod logfile;
pub mod manifest;
pub mod memtable;
pub mod merge;
pub mod protocol;
pub mod server;
pub mod sstable;
pub mod version;
pub mod wal;

#[cfg(test)]
mod test_io;

pub use compaction::CompactionStats;
pub use config::{Options, RecoveryMode, SyncMode};
pub use engine::{Db, LevelSummary, RecoveryReport, Stats};
pub use error::{Error, Result};
pub use failpoint::FailPoints;
