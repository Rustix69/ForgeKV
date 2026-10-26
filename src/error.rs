//! Error type shared by every ForgeKV module.

use std::fmt;
use std::io;
use std::path::PathBuf;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    /// An I/O operation failed. `op` names what we were doing (e.g. "sync wal").
    Io {
        op: &'static str,
        path: PathBuf,
        source: io::Error,
    },
    /// On-disk data failed validation (bad checksum, impossible length, ...).
    /// Never skipped silently; see `docs/FORMATS.md`, "Record log".
    Corruption {
        file: PathBuf,
        offset: u64,
        detail: String,
    },
    /// The caller supplied an invalid key, value, or option.
    InvalidArgument(String),
    /// A durability-relevant I/O step failed earlier; the engine rejects
    /// further writes until it is reopened and recovered.
    Poisoned,
    /// A file's magic matched but its format version is not supported.
    UnsupportedVersion { file: PathBuf, version: u32 },
    /// The database has been closed.
    Closed,
    /// Another handle or process holds the database directory lock
    /// (`flock` on `LOCK`, see `fs::DirLock`). The lock is released when the
    /// holding `Db` is dropped, even if a child process spawned meanwhile
    /// still has a copy of the descriptor.
    Locked { path: PathBuf },
    /// A test-only failpoint fired (feature `failpoints`).
    Injected(&'static str),
}

impl Error {
    pub fn io(op: &'static str, path: impl Into<PathBuf>, source: io::Error) -> Self {
        Error::Io {
            op,
            path: path.into(),
            source,
        }
    }

    pub fn corruption(file: impl Into<PathBuf>, offset: u64, detail: impl Into<String>) -> Self {
        Error::Corruption {
            file: file.into(),
            offset,
            detail: detail.into(),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io { op, path, source } => {
                write!(f, "I/O error during {op} on {}: {source}", path.display())
            }
            Error::Corruption {
                file,
                offset,
                detail,
            } => write!(
                f,
                "corruption in {} at offset {offset}: {detail}",
                file.display()
            ),
            Error::InvalidArgument(msg) => write!(f, "invalid argument: {msg}"),
            Error::Poisoned => write!(f, "engine poisoned by an earlier I/O failure; reopen it"),
            Error::UnsupportedVersion { file, version } => write!(
                f,
                "unsupported format version {version} in {}",
                file.display()
            ),
            Error::Closed => write!(f, "database is closed"),
            Error::Locked { path } => write!(f, "database is locked: {}", path.display()),
            Error::Injected(name) => write!(f, "injected failure at {name}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io { source, .. } => Some(source),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error as _;

    #[test]
    fn io_error_exposes_source_and_context() {
        let e = Error::io(
            "sync wal",
            "/db/wal-000001.log",
            io::Error::other("disk on fire"),
        );
        let msg = e.to_string();
        assert!(msg.contains("sync wal"), "{msg}");
        assert!(msg.contains("/db/wal-000001.log"), "{msg}");
        assert!(msg.contains("disk on fire"), "{msg}");
        assert_eq!(e.source().unwrap().to_string(), "disk on fire");
    }

    #[test]
    fn corruption_display_names_file_and_offset() {
        let e = Error::corruption("MANIFEST", 42, "bad crc");
        let msg = e.to_string();
        assert!(msg.contains("MANIFEST") && msg.contains("42") && msg.contains("bad crc"));
        assert!(e.source().is_none());
    }

    #[test]
    fn simple_variants_display() {
        assert!(Error::Poisoned.to_string().contains("poisoned"));
        assert!(Error::Closed.to_string().contains("closed"));
        assert!(
            Error::InvalidArgument("empty key".into())
                .to_string()
                .contains("empty key")
        );
        let v = Error::UnsupportedVersion {
            file: "x.sst".into(),
            version: 9,
        };
        assert!(v.to_string().contains('9'));
    }
}
