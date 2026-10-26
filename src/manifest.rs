//! The MANIFEST: the durable list of live SSTables plus the file counters.
//!
//! The file is a [`crate::logfile`] record log (kind = MANIFEST, frame format
//! v2, CRC-32/IEEE). Each record is one *atomic* version change:
//! ```text
//! record  = edit_count u32 | edit{edit_count}
//! edit    = 1 SetNextFileNumber  next u64
//!         | 2 SetLogNumber       log u64
//!         | 3 AddFile            level u8 | file_no u64 | size u64 |
//!                                smallest_len u32 | smallest | largest_len u32 | largest
//!         | 4 DeleteFile         level u8 | file_no u64
//! ```
//! `log_number` is the oldest WAL that may still hold un-flushed data: WALs
//! numbered below it are obsolete. Levels are 0 and 1 only.
//!
//! # Lifecycle
//! - Every `Db::open` writes a fresh single-record *snapshot* to
//!   `MANIFEST.tmp`, fsyncs it, renames it over `MANIFEST`, and fsyncs the
//!   directory. So the file that is appended to is always one this process
//!   wrote, never one with a damaged tail. The same rewrite happens at run
//!   time once the file exceeds `Options::manifest_max_bytes`.
//! - Flushes and compactions append one record and fsync before the change
//!   is installed in memory.
//!
//! # Replay rules (deterministic)
//! - An *incomplete* final frame is a torn append that was never
//!   acknowledged: ignored (the next snapshot drops it).
//! - Any complete frame with a bad checksum, an undecodable record, or an
//!   edit that is inconsistent with the state so far (duplicate file, delete
//!   of a missing file, wrong level, log number going backwards, ...) is
//!   `Error::Corruption`. `RecoveryMode::SalvagePrefix` does *not* apply to
//!   the manifest: losing a manifest record could resurrect deleted files.
//! - After replay, the state must satisfy [`ManifestState`] invariants.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, Write};
use std::path::Path;

use crate::codec::Cur;
use crate::error::{Error, Result};
use crate::failpoint::FailPoints;
use crate::fs::{MANIFEST_FILE, MANIFEST_TMP_FILE, remove_if_exists, sync_dir};
use crate::logfile::{Frame, HEADER_LEN, LogKind, LogReader, LogWriter, encode_header};

/// Largest manifest record. A snapshot must fit (roughly 8k files with 4 KiB
/// keys); exceeding it is reported as an error, never truncated.
pub const MAX_MANIFEST_RECORD: usize = 64 * 1024 * 1024;
pub const MAX_LEVEL: u8 = 1;

const TAG_NEXT_FILE: u8 = 1;
const TAG_LOG_NUMBER: u8 = 2;
const TAG_ADD_FILE: u8 = 3;
const TAG_DELETE_FILE: u8 = 4;
/// Smallest encoded edit: tag + u64.
const MIN_EDIT_LEN: usize = 9;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileMeta {
    pub file_no: u64,
    pub size: u64,
    pub smallest: Vec<u8>,
    pub largest: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VersionEdit {
    SetNextFileNumber(u64),
    SetLogNumber(u64),
    AddFile { level: u8, meta: FileMeta },
    DeleteFile { level: u8, file_no: u64 },
}

pub fn encode_edits(edits: &[VersionEdit]) -> Vec<u8> {
    let mut b = Vec::new();
    b.extend_from_slice(&(edits.len() as u32).to_le_bytes());
    for e in edits {
        match e {
            VersionEdit::SetNextFileNumber(n) => {
                b.push(TAG_NEXT_FILE);
                b.extend_from_slice(&n.to_le_bytes());
            }
            VersionEdit::SetLogNumber(n) => {
                b.push(TAG_LOG_NUMBER);
                b.extend_from_slice(&n.to_le_bytes());
            }
            VersionEdit::AddFile { level, meta } => {
                b.push(TAG_ADD_FILE);
                b.push(*level);
                b.extend_from_slice(&meta.file_no.to_le_bytes());
                b.extend_from_slice(&meta.size.to_le_bytes());
                for k in [&meta.smallest, &meta.largest] {
                    b.extend_from_slice(&(k.len() as u32).to_le_bytes());
                    b.extend_from_slice(k);
                }
            }
            VersionEdit::DeleteFile { level, file_no } => {
                b.push(TAG_DELETE_FILE);
                b.push(*level);
                b.extend_from_slice(&file_no.to_le_bytes());
            }
        }
    }
    b
}

fn read_key(c: &mut Cur<'_>) -> std::result::Result<Vec<u8>, String> {
    let n = c.u32()? as usize;
    if n == 0 || n > c.remaining() {
        return Err(format!("invalid key length {n}"));
    }
    Ok(c.take(n)?.to_vec())
}

fn read_level(c: &mut Cur<'_>) -> std::result::Result<u8, String> {
    let level = c.u8()?;
    if level > MAX_LEVEL {
        return Err(format!("invalid level {level}"));
    }
    Ok(level)
}

pub fn decode_edits(payload: &[u8]) -> std::result::Result<Vec<VersionEdit>, String> {
    let mut c = Cur::new(payload);
    let count = c.u32()? as usize;
    if count == 0 || count > payload.len() / MIN_EDIT_LEN {
        return Err(format!("implausible edit count {count}"));
    }
    let mut edits = Vec::with_capacity(count);
    for _ in 0..count {
        let edit = match c.u8()? {
            TAG_NEXT_FILE => VersionEdit::SetNextFileNumber(c.u64()?),
            TAG_LOG_NUMBER => VersionEdit::SetLogNumber(c.u64()?),
            TAG_ADD_FILE => {
                let level = read_level(&mut c)?;
                let file_no = c.u64()?;
                let size = c.u64()?;
                let smallest = read_key(&mut c)?;
                let largest = read_key(&mut c)?;
                if smallest > largest {
                    return Err(format!("file {file_no}: smallest key > largest key"));
                }
                VersionEdit::AddFile {
                    level,
                    meta: FileMeta {
                        file_no,
                        size,
                        smallest,
                        largest,
                    },
                }
            }
            TAG_DELETE_FILE => VersionEdit::DeleteFile {
                level: read_level(&mut c)?,
                file_no: c.u64()?,
            },
            other => return Err(format!("unknown edit tag {other}")),
        };
        edits.push(edit);
    }
    if c.remaining() != 0 {
        return Err(format!("{} trailing bytes after edits", c.remaining()));
    }
    Ok(edits)
}

/// The fully replayed manifest.
///
/// Invariants (checked by [`ManifestState::validate`]):
/// - every file number is `< next_file_number`, and `log_number <= next_file_number`;
/// - file numbers are unique; levels are 0 or 1;
/// - L1 files are pairwise non-overlapping.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestState {
    pub next_file_number: u64,
    pub log_number: u64,
    /// file_no -> (level, meta)
    pub files: BTreeMap<u64, (u8, FileMeta)>,
}

impl ManifestState {
    /// State of a brand-new database.
    pub fn new_db() -> Self {
        ManifestState {
            next_file_number: 1,
            log_number: 0,
            files: BTreeMap::new(),
        }
    }

    pub fn snapshot(&self) -> Vec<VersionEdit> {
        let mut edits = vec![
            VersionEdit::SetNextFileNumber(self.next_file_number),
            VersionEdit::SetLogNumber(self.log_number),
        ];
        for (level, meta) in self.files.values() {
            edits.push(VersionEdit::AddFile {
                level: *level,
                meta: meta.clone(),
            });
        }
        edits
    }

    pub fn validate(&self) -> std::result::Result<(), String> {
        if self.log_number > self.next_file_number {
            return Err(format!(
                "log number {} beyond next file number {}",
                self.log_number, self.next_file_number
            ));
        }
        if let Some((&n, _)) = self.files.iter().next_back()
            && n >= self.next_file_number
        {
            return Err(format!(
                "file {n} not below next file number {}",
                self.next_file_number
            ));
        }
        check_l1_disjoint(
            self.files
                .values()
                .filter(|(level, _)| *level == 1)
                .map(|(_, m)| m),
        )
    }
}

/// L1 files must not overlap. Accepts files in any order.
pub fn check_l1_disjoint<'a>(
    files: impl Iterator<Item = &'a FileMeta>,
) -> std::result::Result<(), String> {
    let mut v: Vec<&FileMeta> = files.collect();
    v.sort_by(|a, b| a.smallest.cmp(&b.smallest));
    for w in v.windows(2) {
        if w[0].largest >= w[1].smallest {
            return Err(format!(
                "L1 files {} and {} overlap",
                w[0].file_no, w[1].file_no
            ));
        }
    }
    Ok(())
}

#[derive(Default)]
struct Replayer {
    next: Option<u64>,
    log: Option<u64>,
    files: BTreeMap<u64, (u8, FileMeta)>,
}

impl Replayer {
    fn apply(&mut self, edits: Vec<VersionEdit>) -> std::result::Result<(), String> {
        for e in edits {
            match e {
                VersionEdit::SetNextFileNumber(n) => self.next = Some(n),
                VersionEdit::SetLogNumber(n) => {
                    if let Some(old) = self.log
                        && n < old
                    {
                        return Err(format!("log number went backwards ({old} -> {n})"));
                    }
                    self.log = Some(n);
                }
                VersionEdit::AddFile { level, meta } => {
                    if self.files.contains_key(&meta.file_no) {
                        return Err(format!("file {} added twice", meta.file_no));
                    }
                    self.files.insert(meta.file_no, (level, meta));
                }
                VersionEdit::DeleteFile { level, file_no } => match self.files.remove(&file_no) {
                    Some((l, _)) if l == level => {}
                    Some((l, _)) => {
                        return Err(format!(
                            "delete of file {file_no} at level {level}, but it is at level {l}"
                        ));
                    }
                    None => return Err(format!("delete of unknown file {file_no}")),
                },
            }
        }
        Ok(())
    }

    fn finish(self) -> std::result::Result<ManifestState, String> {
        let (Some(next_file_number), Some(log_number)) = (self.next, self.log) else {
            return Err("manifest never set next file number and log number".into());
        };
        let st = ManifestState {
            next_file_number,
            log_number,
            files: self.files,
        };
        st.validate()?;
        Ok(st)
    }
}

/// Outcome of reading a MANIFEST.
#[derive(Debug)]
pub struct ManifestReplay {
    pub state: ManifestState,
    pub records: u64,
    /// Bytes of an incomplete final frame that were ignored.
    pub discarded_bytes: u64,
}

/// Read and validate the manifest at `path`. Never modifies the file.
pub fn replay(path: &Path) -> Result<ManifestReplay> {
    let file = File::open(path).map_err(|e| Error::io("open manifest", path, e))?;
    let len = file
        .metadata()
        .map_err(|e| Error::io("stat manifest", path, e))?
        .len();
    let Some(mut reader) = LogReader::open(
        BufReader::new(file),
        path,
        LogKind::Manifest,
        MAX_MANIFEST_RECORD,
    )?
    else {
        // MANIFEST only ever appears through rename of a synced file.
        return Err(Error::corruption(
            path,
            0,
            "manifest shorter than its header",
        ));
    };
    let mut r = Replayer::default();
    let mut records = 0u64;
    let discarded_bytes = loop {
        match reader.next_frame()? {
            Frame::Record { offset, payload } => {
                let edits =
                    decode_edits(&payload).map_err(|d| Error::corruption(path, offset, d))?;
                r.apply(edits)
                    .map_err(|d| Error::corruption(path, offset, d))?;
                records += 1;
            }
            Frame::Clean => break 0,
            Frame::Incomplete => break len - reader.good_end(),
        }
    };
    if records == 0 {
        return Err(Error::corruption(
            path,
            HEADER_LEN,
            "manifest has no records",
        ));
    }
    let state = r.finish().map_err(|d| Error::corruption(path, 0, d))?;
    Ok(ManifestReplay {
        state,
        records,
        discarded_bytes,
    })
}

/// Appender for the live MANIFEST.
pub struct Manifest {
    log: LogWriter<File>,
    max_bytes: u64,
}

impl Manifest {
    /// Atomically replace `MANIFEST` with a file holding the single record
    /// `edits`: write `MANIFEST.tmp`, fsync, rename, fsync the directory.
    /// A crash at any point leaves either the old or the new manifest.
    pub fn create(
        dir: &Path,
        edits: &[VersionEdit],
        max_bytes: u64,
        fp: &FailPoints,
    ) -> Result<Manifest> {
        let tmp = dir.join(MANIFEST_TMP_FILE);
        let path = dir.join(MANIFEST_FILE);
        remove_if_exists(&tmp)?;
        let mut f = OpenOptions::new()
            .append(true)
            .create_new(true)
            .open(&tmp)
            .map_err(|e| Error::io("create manifest tmp", &tmp, e))?;
        f.write_all(&encode_header(LogKind::Manifest))
            .map_err(|e| Error::io("write manifest header", &tmp, e))?;
        let mut log = LogWriter::new(f, &tmp, MAX_MANIFEST_RECORD, HEADER_LEN);
        log.append(&encode_edits(edits))?;
        log.sync()?;
        fail_point!(fp, "manifest:after_tmp_written");
        fs::rename(&tmp, &path).map_err(|e| Error::io("rename manifest", &path, e))?;
        sync_dir(dir)?;
        fail_point!(fp, "manifest:after_rename");
        // The open handle now refers to MANIFEST (rename keeps the inode).
        Ok(Manifest { log, max_bytes })
    }

    /// Append one atomic record and fsync it. On error the writer is
    /// poisoned; the caller must treat the database as poisoned too, since
    /// the record may or may not be on disk.
    pub fn append(&mut self, edits: &[VersionEdit]) -> Result<()> {
        self.log.append(&encode_edits(edits))?;
        self.log.sync()
    }

    pub fn needs_rewrite(&self) -> bool {
        self.log.good_end() > self.max_bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta(n: u64, a: &[u8], b: &[u8]) -> FileMeta {
        FileMeta {
            file_no: n,
            size: 100 + n,
            smallest: a.to_vec(),
            largest: b.to_vec(),
        }
    }

    fn add(level: u8, n: u64, a: &[u8], b: &[u8]) -> VersionEdit {
        VersionEdit::AddFile {
            level,
            meta: meta(n, a, b),
        }
    }

    #[test]
    fn golden_encoding() {
        let edits = vec![
            VersionEdit::SetNextFileNumber(5),
            VersionEdit::SetLogNumber(4),
            add(1, 3, b"a", b"z"),
            VersionEdit::DeleteFile {
                level: 0,
                file_no: 2,
            },
        ];
        let mut expected = vec![4, 0, 0, 0];
        expected.extend([1, 5, 0, 0, 0, 0, 0, 0, 0]);
        expected.extend([2, 4, 0, 0, 0, 0, 0, 0, 0]);
        expected.extend([3, 1, 3, 0, 0, 0, 0, 0, 0, 0, 103, 0, 0, 0, 0, 0, 0, 0]);
        expected.extend([1, 0, 0, 0, b'a', 1, 0, 0, 0, b'z']);
        expected.extend([4, 0, 2, 0, 0, 0, 0, 0, 0, 0]);
        assert_eq!(encode_edits(&edits), expected);
        assert_eq!(decode_edits(&expected).unwrap(), edits);
    }

    #[test]
    fn decode_rejects_malformed_records() {
        let good = encode_edits(&[add(0, 1, b"a", b"b")]);
        assert!(decode_edits(&[]).is_err());
        assert!(decode_edits(&[0, 0, 0, 0]).is_err(), "zero edits");
        assert!(
            decode_edits(&[0xff, 0xff, 0xff, 0xff, 1]).is_err(),
            "huge count"
        );
        let mut trailing = good.clone();
        trailing.push(0);
        assert!(decode_edits(&trailing).is_err());
        let mut short = good.clone();
        short.pop();
        assert!(decode_edits(&short).is_err());
        let mut bad_tag = good.clone();
        bad_tag[4] = 9;
        assert!(decode_edits(&bad_tag).is_err());
        let mut bad_level = good.clone();
        bad_level[5] = 2;
        assert!(decode_edits(&bad_level).is_err());
        assert!(
            decode_edits(&encode_edits(&[add(0, 1, b"z", b"a")])).is_err(),
            "inverted range"
        );
        assert!(
            decode_edits(&encode_edits(&[add(0, 1, b"", b"a")])).is_err(),
            "empty key"
        );
    }

    fn replayed(records: Vec<Vec<VersionEdit>>) -> std::result::Result<ManifestState, String> {
        let mut r = Replayer::default();
        for edits in records {
            r.apply(edits)?;
        }
        r.finish()
    }

    #[test]
    fn replay_applies_adds_and_deletes() {
        let st = replayed(vec![
            vec![
                VersionEdit::SetNextFileNumber(10),
                VersionEdit::SetLogNumber(1),
            ],
            vec![add(0, 2, b"a", b"m"), add(0, 3, b"c", b"z")],
            vec![
                add(1, 4, b"a", b"k"),
                add(1, 5, b"l", b"z"),
                VersionEdit::DeleteFile {
                    level: 0,
                    file_no: 2,
                },
                VersionEdit::DeleteFile {
                    level: 0,
                    file_no: 3,
                },
            ],
        ])
        .unwrap();
        assert_eq!(st.files.keys().copied().collect::<Vec<_>>(), vec![4, 5]);
        assert_eq!(st.snapshot().len(), 4);
    }

    #[test]
    fn replay_rejects_inconsistent_histories() {
        let base = || {
            vec![
                VersionEdit::SetNextFileNumber(10),
                VersionEdit::SetLogNumber(5),
            ]
        };
        let del = |level, file_no| VersionEdit::DeleteFile { level, file_no };
        let cases: Vec<(&str, Vec<Vec<VersionEdit>>)> = vec![
            ("no numbers", vec![vec![add(0, 1, b"a", b"b")]]),
            (
                "duplicate add",
                vec![
                    base(),
                    vec![add(0, 1, b"a", b"b")],
                    vec![add(0, 1, b"a", b"b")],
                ],
            ),
            ("delete unknown", vec![base(), vec![del(0, 1)]]),
            (
                "delete wrong level",
                vec![base(), vec![add(0, 1, b"a", b"b")], vec![del(1, 1)]],
            ),
            (
                "log backwards",
                vec![base(), vec![VersionEdit::SetLogNumber(4)]],
            ),
            (
                "file beyond next",
                vec![base(), vec![add(0, 10, b"a", b"b")]],
            ),
            (
                "log beyond next",
                vec![vec![
                    VersionEdit::SetNextFileNumber(3),
                    VersionEdit::SetLogNumber(4),
                ]],
            ),
            (
                "L1 overlap",
                vec![base(), vec![add(1, 1, b"a", b"m"), add(1, 2, b"m", b"z")]],
            ),
        ];
        for (name, records) in cases {
            assert!(replayed(records).is_err(), "{name}");
        }
        // L0 files may overlap.
        assert!(
            replayed(vec![
                base(),
                vec![add(0, 1, b"a", b"m"), add(0, 2, b"c", b"z")]
            ])
            .is_ok()
        );
    }
}
