//! ForgeKV: an educational, persistent key-value storage engine.
//!
//! See `PLAN.md` for the phased design. Phase 1: checksummed record log,
//! WAL, memtable, and a durable `put`/`get`/`delete` engine with recovery.
//! Phase 2: immutable SSTable writer and reader.
//! Phase 3: manifest, WAL rotation, memtable flush to L0, multi-layer reads.
//! Phase 4: L0 -> L1 compaction with tombstone garbage collection.
//! Phase 5: concurrent reads/writes, background flush/compaction worker,
//! backpressure, reference-counted table lifetimes, graceful shutdown.
//! Phase 6: binary protocol, Tokio TCP server and client.

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
