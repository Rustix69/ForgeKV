//! Filesystem helpers and file naming. Creating or removing a file is only
//! durable once the parent directory has been fsynced.
//!
//! Directory layout: `LOCK`, `MANIFEST` (`MANIFEST.tmp` transiently),
//! `wal-NNNNNN.log`, `NNNNNN.sst`. WAL and SST numbers come from one shared
//! counter (`next_file_number` in the manifest).

use std::fs::{self, File, OpenOptions, TryLockError};
use std::io;
use std::path::{Path, PathBuf};

use crate::error::{Error, Result};

pub const MANIFEST_FILE: &str = "MANIFEST";
pub const MANIFEST_TMP_FILE: &str = "MANIFEST.tmp";
pub const LOCK_FILE: &str = "LOCK";

pub fn sst_path(dir: &Path, file_no: u64) -> PathBuf {
    dir.join(format!("{file_no:06}.sst"))
}

pub fn wal_path(dir: &Path, file_no: u64) -> PathBuf {
    dir.join(format!("wal-{file_no:06}.log"))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileKind {
    Sst(u64),
    Wal(u64),
    Manifest,
    Lock,
    /// Any `*.tmp` (including `MANIFEST.tmp`): transient, deleted on open.
    Temp,
    /// Not ours; never touched.
    Unknown,
}

fn parse_number(s: &str) -> Option<u64> {
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    s.parse().ok()
}

pub fn classify(name: &str) -> FileKind {
    if name == MANIFEST_FILE {
        return FileKind::Manifest;
    }
    if name == LOCK_FILE {
        return FileKind::Lock;
    }
    if name.ends_with(".tmp") {
        return FileKind::Temp;
    }
    if let Some(n) = name
        .strip_prefix("wal-")
        .and_then(|r| r.strip_suffix(".log"))
        .and_then(parse_number)
    {
        return FileKind::Wal(n);
    }
    if let Some(n) = name.strip_suffix(".sst").and_then(parse_number) {
        return FileKind::Sst(n);
    }
    FileKind::Unknown
}

/// List the directory, classifying each entry. Errors are propagated.
pub fn list_dir(dir: &Path) -> Result<Vec<(FileKind, PathBuf)>> {
    let mut out = Vec::new();
    for entry in fs::read_dir(dir).map_err(|e| Error::io("list db dir", dir, e))? {
        let entry = entry.map_err(|e| Error::io("list db dir", dir, e))?;
        let kind = entry
            .file_name()
            .to_str()
            .map_or(FileKind::Unknown, classify);
        out.push((kind, entry.path()));
    }
    Ok(out)
}

pub fn remove_file(path: &Path) -> Result<()> {
    fs::remove_file(path).map_err(|e| Error::io("remove file", path, e))
}

/// Remove `path` if it exists.
pub fn remove_if_exists(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(Error::io("remove file", path, e)),
    }
}

/// fsync a directory so that file creations/removals inside it are durable.
pub fn sync_dir(dir: &Path) -> Result<()> {
    let f = File::open(dir).map_err(|e| Error::io("open dir for sync", dir, e))?;
    f.sync_all().map_err(|e| Error::io("sync dir", dir, e))
}

/// The exclusive advisory lock on `<dir>/LOCK` (`flock`), held until this
/// value is dropped.
///
/// # Why the lock is released explicitly
/// A `flock` lock belongs to the *open file description*, and it is only
/// released implicitly when **every** descriptor referring to that
/// description is closed. `std::process::Command` starts a child with
/// `clone3(CLONE_VM | CLONE_VFORK)` without `CLONE_FILES`, so the child holds
/// a copy of every descriptor until its `execve` closes the `O_CLOEXEC` ones
/// (Rust opens all files `O_CLOEXEC`). If any thread spawns a process while
/// another thread drops a database, the lock therefore outlived the drop, and
/// an immediate reopen in the same process failed with `Error::Locked`.
/// `Drop` now calls `flock(LOCK_UN)` (`File::unlock`), which releases the
/// lock for every descriptor of the description, so the release happens
/// exactly when the owner lets go.
///
/// Exclusivity is unchanged: the lock is still held for the whole life of
/// the owner, and another process's lock attempt opens its own description,
/// which conflicts. A crash cannot run `Drop`; then, as before, the kernel
/// releases the lock once every process holding a descriptor of the
/// description has exited or exec'd.
#[derive(Debug)]
pub struct DirLock {
    file: File,
}

impl DirLock {
    /// Take the lock, or `Error::Locked` if another owner holds it.
    pub fn acquire(dir: &Path) -> Result<DirLock> {
        let path = dir.join(LOCK_FILE);
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&path)
            .map_err(|e| Error::io("open lock file", &path, e))?;
        match file.try_lock() {
            Ok(()) => Ok(DirLock { file }),
            Err(TryLockError::WouldBlock) => Err(Error::Locked { path }),
            Err(TryLockError::Error(e)) => Err(Error::io("lock db", &path, e)),
        }
    }
}

impl Drop for DirLock {
    fn drop(&mut self) {
        // `flock(LOCK_UN)` does not block; retry only an interrupted call.
        // If it still fails, closing the descriptor below releases the lock
        // once no other process references the description (the pre-fix
        // behaviour): availability, not safety, is affected, and `drop`
        // cannot report errors.
        loop {
            match self.file.unlock() {
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                _ => break,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_lock_is_exclusive_and_released_on_drop() {
        let dir = tempfile::TempDir::new().unwrap();
        let lock = DirLock::acquire(dir.path()).unwrap();
        assert!(matches!(
            DirLock::acquire(dir.path()),
            Err(Error::Locked { .. })
        ));
        drop(lock);
        DirLock::acquire(dir.path()).unwrap();
    }

    /// Regression: a second reference to the lock's open file description
    /// (what a spawned child holds between `clone` and `execve`) kept the
    /// lock after its owner was dropped, so reacquiring failed with
    /// `Locked`. `try_clone` creates exactly such a reference, so this is
    /// deterministic.
    #[test]
    fn a_duplicated_descriptor_does_not_keep_the_lock_after_its_owner_drops() {
        let dir = tempfile::TempDir::new().unwrap();
        let lock = DirLock::acquire(dir.path()).unwrap();
        let extra_reference = lock.file.try_clone().unwrap();
        // While the owner lives, exclusivity holds.
        assert!(matches!(
            DirLock::acquire(dir.path()),
            Err(Error::Locked { .. })
        ));
        drop(lock);
        let again = DirLock::acquire(dir.path());
        assert!(again.is_ok(), "lock outlived its owner: {again:?}");
        drop(again);
        drop(extra_reference);
    }

    /// The same, with the extra reference held by a real child process that
    /// keeps it across `execve` (passed as its stdin): deterministic for as
    /// long as the child lives.
    #[test]
    fn a_child_process_holding_the_description_does_not_keep_the_lock() {
        let dir = tempfile::TempDir::new().unwrap();
        let lock = DirLock::acquire(dir.path()).unwrap();
        let mut child = std::process::Command::new("sleep")
            .arg("30")
            .stdin(std::process::Stdio::from(lock.file.try_clone().unwrap()))
            .spawn()
            .unwrap();
        assert!(matches!(
            DirLock::acquire(dir.path()),
            Err(Error::Locked { .. })
        ));
        drop(lock);
        let again = DirLock::acquire(dir.path());
        assert!(
            child.try_wait().unwrap().is_none(),
            "the child must still be alive"
        );
        child.kill().unwrap();
        child.wait().unwrap();
        assert!(again.is_ok(), "lock outlived its owner: {again:?}");
    }

    #[test]
    fn names_round_trip_and_classify() {
        let d = Path::new("/db");
        assert_eq!(sst_path(d, 7), Path::new("/db/000007.sst"));
        assert_eq!(wal_path(d, 12), Path::new("/db/wal-000012.log"));
        assert_eq!(classify("000007.sst"), FileKind::Sst(7));
        assert_eq!(classify("1234567.sst"), FileKind::Sst(1_234_567));
        assert_eq!(classify("wal-000012.log"), FileKind::Wal(12));
        assert_eq!(classify("MANIFEST"), FileKind::Manifest);
        assert_eq!(classify("LOCK"), FileKind::Lock);
        assert_eq!(classify("MANIFEST.tmp"), FileKind::Temp);
        assert_eq!(classify("x.sst.tmp"), FileKind::Temp);
        for other in [
            ".sst",
            "abc.sst",
            "-1.sst",
            "wal-.log",
            "wal-12.txt",
            "notes.txt",
            "+5.sst",
        ] {
            assert_eq!(classify(other), FileKind::Unknown, "{other}");
        }
    }
}
