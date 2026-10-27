//! Write-ahead log: record encoding, appending, and replay.
//!
//! WAL payload (`docs/FORMATS.md`, "WAL record payload"), inside a [`crate::logfile`] frame:
//! ```text
//! type u8 (1 = Put, 2 = Delete) | key_len u32 | key | [val_len u32 | value]   (Put only)
//! ```
//! Integers are little-endian. Decoding is strict: exact length, non-empty
//! key, limits enforced, unknown types rejected. A frame whose checksum is
//! valid but whose payload does not decode is `Corruption`, never skipped.

use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use crate::codec::Cur;
use crate::config::{Options, RecoveryMode, SyncMode};
use crate::error::{Error, Result};
use crate::fs::sync_dir;
use crate::logfile::{Frame, HEADER_LEN, LogKind, LogReader, LogSink, LogWriter, encode_header};

const TYPE_PUT: u8 = 1;
const TYPE_DELETE: u8 = 2;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalRecord {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

/// Largest legal WAL payload under `opts`.
pub fn max_record_size(opts: &Options) -> usize {
    1 + 4 + opts.max_key_len + 4 + opts.max_value_len
}

pub fn encode_put(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut b = Vec::with_capacity(1 + 4 + key.len() + 4 + value.len());
    b.push(TYPE_PUT);
    b.extend_from_slice(&(key.len() as u32).to_le_bytes());
    b.extend_from_slice(key);
    b.extend_from_slice(&(value.len() as u32).to_le_bytes());
    b.extend_from_slice(value);
    b
}

pub fn encode_delete(key: &[u8]) -> Vec<u8> {
    let mut b = Vec::with_capacity(1 + 4 + key.len());
    b.push(TYPE_DELETE);
    b.extend_from_slice(&(key.len() as u32).to_le_bytes());
    b.extend_from_slice(key);
    b
}

/// Decode a payload. `Err` carries a description for a `Corruption` error.
pub fn decode(payload: &[u8], opts: &Options) -> std::result::Result<WalRecord, String> {
    let mut c = Cur::new(payload);
    let ty = c.take(1)?[0];
    let key_len = c.u32()? as usize;
    if key_len == 0 || key_len > opts.max_key_len {
        return Err(format!(
            "key length {key_len} outside 1..={}",
            opts.max_key_len
        ));
    }
    let key = c.take(key_len)?.to_vec();
    let rec = match ty {
        TYPE_PUT => {
            let val_len = c.u32()? as usize;
            if val_len > opts.max_value_len {
                return Err(format!(
                    "value length {val_len} over {}",
                    opts.max_value_len
                ));
            }
            let value = c.take(val_len)?.to_vec();
            WalRecord::Put { key, value }
        }
        TYPE_DELETE => WalRecord::Delete { key },
        other => return Err(format!("unknown record type {other}")),
    };
    if c.remaining() != 0 {
        return Err(format!("{} trailing bytes after record", c.remaining()));
    }
    Ok(rec)
}

/// Appender for an open WAL.
pub struct Wal<S: LogSink = File> {
    log: LogWriter<S>,
}

impl<S: LogSink> Wal<S> {
    /// Wrap a sink positioned after the last good frame (testing and internal use).
    pub fn from_sink(sink: S, path: impl Into<PathBuf>, opts: &Options, good_end: u64) -> Self {
        Wal {
            log: LogWriter::new(sink, path, max_record_size(opts), good_end),
        }
    }

    /// Append a Put. Under `SyncMode::Always` the record is synced before
    /// this returns `Ok`; only then may the caller acknowledge the write.
    pub fn append_put(&mut self, key: &[u8], value: &[u8], mode: SyncMode) -> Result<()> {
        self.log.append(&encode_put(key, value))?;
        self.sync_if(mode)
    }

    pub fn append_delete(&mut self, key: &[u8], mode: SyncMode) -> Result<()> {
        self.log.append(&encode_delete(key))?;
        self.sync_if(mode)
    }

    fn sync_if(&mut self, mode: SyncMode) -> Result<()> {
        match mode {
            SyncMode::Always => self.log.sync(),
            SyncMode::Never => Ok(()),
        }
    }

    pub fn sync(&mut self) -> Result<()> {
        self.log.sync()
    }

    pub fn good_end(&self) -> u64 {
        self.log.good_end()
    }

    /// Offset covered by the last successful sync; see
    /// [`crate::logfile::LogWriter::synced_end`].
    pub fn synced_end(&self) -> u64 {
        self.log.synced_end()
    }

    pub fn is_poisoned(&self) -> bool {
        self.log.is_poisoned()
    }
}

impl Wal<File> {
    /// Create a brand-new WAL: header written and fsynced, then the parent
    /// directory fsynced so the file itself is durable. Fails if it exists.
    pub fn create(path: &Path, opts: &Options) -> Result<Self> {
        let mut f = OpenOptions::new()
            .append(true)
            .create_new(true)
            .open(path)
            .map_err(|e| Error::io("create wal", path, e))?;
        f.write_all(&encode_header(LogKind::Wal))
            .map_err(|e| Error::io("write wal header", path, e))?;
        f.sync_all()
            .map_err(|e| Error::io("sync new wal", path, e))?;
        if let Some(dir) = path.parent() {
            sync_dir(dir)?;
        }
        let mut wal = Wal::from_sink(f, path, opts, HEADER_LEN);
        // The header was fsynced above.
        wal.log.assume_synced();
        Ok(wal)
    }
}

/// How a replay ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplayEnd {
    /// File ended exactly at a frame boundary.
    Clean,
    /// File ended inside a frame; data after `Replay::good_end` is ignored.
    IncompleteTail,
    /// The file is shorter than a header: creation was interrupted, so it
    /// never held a record.
    HeaderIncomplete,
    /// `RecoveryMode::SalvagePrefix` only: an invalid frame at `Replay::good_end`
    /// (described by the string) and everything after it is to be dropped.
    Salvaged(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Replay {
    pub records: u64,
    /// Offset just past the last record that was applied.
    pub good_end: u64,
    pub end: ReplayEnd,
}

/// Read a WAL from `r`, calling `apply` for each valid record in order.
///
/// Never modifies anything: classification completes before the caller
/// decides what to keep (`docs/FORMATS.md`, "Record log"). In `Strict` mode, any corruption is an
/// error and `apply` may already have seen earlier records, so the caller must
/// discard its state on `Err`.
pub fn replay<R: Read>(
    r: R,
    path: &Path,
    opts: &Options,
    mut apply: impl FnMut(WalRecord),
) -> Result<Replay> {
    let Some(mut reader) = LogReader::open(r, path, LogKind::Wal, max_record_size(opts))? else {
        return Ok(Replay {
            records: 0,
            good_end: 0,
            end: ReplayEnd::HeaderIncomplete,
        });
    };
    let mut records = 0u64;
    let finish = |records, good_end, end| {
        Ok(Replay {
            records,
            good_end,
            end,
        })
    };
    loop {
        let err = match reader.next_frame() {
            Ok(Frame::Record { offset, payload }) => match decode(&payload, opts) {
                Ok(rec) => {
                    apply(rec);
                    records += 1;
                    continue;
                }
                Err(detail) => Error::corruption(path, offset, detail),
            },
            Ok(Frame::Clean) => return finish(records, reader.good_end(), ReplayEnd::Clean),
            Ok(Frame::Incomplete) => {
                return finish(records, reader.good_end(), ReplayEnd::IncompleteTail);
            }
            Err(e) => e,
        };
        // Only corruption can be salvaged, and only on explicit request.
        return match (&err, opts.recovery_mode) {
            (Error::Corruption { detail, .. }, RecoveryMode::SalvagePrefix) => {
                let end = ReplayEnd::Salvaged(detail.clone());
                finish(records, good_end_of(&reader, &err), end)
            }
            _ => Err(err),
        };
    }
}

/// Start of the offending frame. A decode failure happens after the reader
/// advanced past the frame, so use the offset stored in the error.
fn good_end_of<R: Read>(reader: &LogReader<R>, err: &Error) -> u64 {
    match err {
        Error::Corruption { offset, .. } => *offset,
        _ => reader.good_end(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logfile::encode_frame;
    use crate::test_io::{ChunkReader, ShortSink};
    use std::io::Cursor;

    fn opts() -> Options {
        Options {
            max_key_len: 16,
            max_value_len: 64,
            memtable_bytes: 1024,
            sst_target_bytes: 1024,
            ..Options::default()
        }
    }

    fn log_with(payloads: &[Vec<u8>]) -> Vec<u8> {
        let o = opts();
        let mut out = encode_header(LogKind::Wal).to_vec();
        for p in payloads {
            out.extend(encode_frame(p, max_record_size(&o)).unwrap());
        }
        out
    }

    fn run(bytes: Vec<u8>, o: &Options) -> Result<(Vec<WalRecord>, Replay)> {
        let mut got = vec![];
        let rep = replay(Cursor::new(bytes), Path::new("wal"), o, |r| got.push(r))?;
        Ok((got, rep))
    }

    #[test]
    fn golden_encoding() {
        assert_eq!(
            encode_put(b"ab", b"xyz"),
            [1, 2, 0, 0, 0, b'a', b'b', 3, 0, 0, 0, b'x', b'y', b'z']
        );
        assert_eq!(encode_delete(b"ab"), [2, 2, 0, 0, 0, b'a', b'b']);
    }

    #[test]
    fn encode_decode_roundtrip() {
        let o = opts();
        assert_eq!(
            decode(&encode_put(b"k", b"v"), &o).unwrap(),
            WalRecord::Put {
                key: b"k".to_vec(),
                value: b"v".to_vec()
            }
        );
        assert_eq!(
            decode(&encode_put(b"k", b""), &o).unwrap(),
            WalRecord::Put {
                key: b"k".to_vec(),
                value: vec![]
            },
            "empty value is a Put, not a Delete"
        );
        assert_eq!(
            decode(&encode_delete(b"k"), &o).unwrap(),
            WalRecord::Delete { key: b"k".to_vec() }
        );
    }

    #[test]
    fn decode_rejects_malformed_payloads() {
        let o = opts();
        assert!(decode(&[], &o).is_err());
        assert!(decode(&[1], &o).is_err());
        assert!(decode(&[9, 1, 0, 0, 0, b'k'], &o).is_err(), "unknown type");
        assert!(decode(&encode_put(b"", b"v"), &o).is_err(), "empty key");
        assert!(
            decode(&encode_put(&[b'k'; 17], b"v"), &o).is_err(),
            "key too long"
        );
        assert!(
            decode(&encode_put(b"k", &[0; 65]), &o).is_err(),
            "value too long"
        );
        let mut trailing = encode_delete(b"k");
        trailing.push(0);
        assert!(decode(&trailing, &o).is_err(), "trailing bytes");
        let mut put = encode_put(b"k", b"value");
        put.pop();
        assert!(decode(&put, &o).is_err(), "short value");
        // Declared key length larger than the payload.
        assert!(decode(&[2, 0xff, 0xff, 0xff, 0x7f], &o).is_err());
        // Delete carrying a value section is not a valid delete.
        let mut del_with_val = encode_put(b"k", b"v");
        del_with_val[0] = TYPE_DELETE;
        assert!(decode(&del_with_val, &o).is_err());
    }

    #[test]
    fn replay_applies_records_in_order() {
        let (got, rep) = run(
            log_with(&[
                encode_put(b"a", b"1"),
                encode_delete(b"a"),
                encode_put(b"b", b"2"),
            ]),
            &opts(),
        )
        .unwrap();
        assert_eq!(got.len(), 3);
        assert_eq!(got[1], WalRecord::Delete { key: b"a".to_vec() });
        assert_eq!(rep.records, 3);
        assert_eq!(rep.end, ReplayEnd::Clean);
    }

    #[test]
    fn replay_incomplete_tail_reports_boundary() {
        let full = log_with(&[encode_put(b"a", b"1"), encode_put(b"b", b"2")]);
        let one = log_with(&[encode_put(b"a", b"1")]);
        for cut in one.len() + 1..full.len() {
            let (got, rep) = run(full[..cut].to_vec(), &opts()).unwrap();
            assert_eq!(got.len(), 1, "cut={cut}");
            assert_eq!(rep.end, ReplayEnd::IncompleteTail, "cut={cut}");
            assert_eq!(rep.good_end, one.len() as u64, "cut={cut}");
        }
    }

    #[test]
    fn replay_with_short_reads_matches() {
        let log = log_with(&[encode_put(b"a", b"1"), encode_delete(b"a")]);
        let mut got = vec![];
        let rep = replay(
            ChunkReader::new(log.clone(), 1),
            Path::new("w"),
            &opts(),
            |r| got.push(r),
        )
        .unwrap();
        assert_eq!(got.len(), 2);
        assert_eq!(rep.good_end, log.len() as u64);
    }

    #[test]
    fn replay_header_incomplete() {
        let hdr = encode_header(LogKind::Wal);
        for n in 0..hdr.len() {
            let (got, rep) = run(hdr[..n].to_vec(), &opts()).unwrap();
            assert!(got.is_empty());
            assert_eq!(rep.end, ReplayEnd::HeaderIncomplete);
        }
    }

    #[test]
    fn strict_replay_errors_on_bad_crc_even_in_final_record() {
        let mut log = log_with(&[encode_put(b"a", b"1"), encode_put(b"b", b"2")]);
        let last = log.len() - 1;
        log[last] ^= 0x80;
        assert!(matches!(run(log, &opts()), Err(Error::Corruption { .. })));
    }

    #[test]
    fn valid_crc_but_undecodable_payload_is_corruption() {
        let log = log_with(&[encode_put(b"a", b"1"), vec![9, 9, 9]]);
        let err = run(log, &opts()).unwrap_err();
        assert!(matches!(err, Error::Corruption { .. }), "{err}");
    }

    #[test]
    fn salvage_prefix_truncates_at_first_bad_frame() {
        let o = Options {
            recovery_mode: RecoveryMode::SalvagePrefix,
            ..opts()
        };
        let good = log_with(&[encode_put(b"a", b"1")]);
        let mut log = log_with(&[encode_put(b"a", b"1"), encode_put(b"b", b"2")]);
        let last = log.len() - 1;
        log[last] ^= 1;
        let (got, rep) = run(log, &o).unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(rep.good_end, good.len() as u64);
        assert!(matches!(rep.end, ReplayEnd::Salvaged(_)));

        // Same for an undecodable-but-checksummed payload: good_end is the
        // start of that frame, not its end.
        let log = log_with(&[encode_put(b"a", b"1"), vec![9, 9, 9]]);
        let (got, rep) = run(log, &o).unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(rep.good_end, good.len() as u64);
    }

    #[test]
    fn salvage_does_not_cover_header_corruption() {
        let o = Options {
            recovery_mode: RecoveryMode::SalvagePrefix,
            ..opts()
        };
        let mut log = log_with(&[encode_put(b"a", b"1")]);
        log[0] = b'X';
        assert!(matches!(run(log, &o), Err(Error::Corruption { .. })));
    }

    #[test]
    fn always_syncs_after_append_and_never_does_not() {
        let o = opts();
        let mut w = Wal::from_sink(ShortSink::new(5, 0), "w", &o, HEADER_LEN);
        w.append_put(b"k", b"v", SyncMode::Always).unwrap();
        assert_eq!(w.log_sink().sync_calls, 1);
        // The synced bytes already contain the whole record.
        assert_eq!(w.log_sink().bytes.len() as u64, w.good_end() - HEADER_LEN);
        w.append_delete(b"k", SyncMode::Always).unwrap();
        assert_eq!(w.log_sink().sync_calls, 2);
        w.append_put(b"k2", b"v", SyncMode::Never).unwrap();
        assert_eq!(w.log_sink().sync_calls, 2);
        w.sync().unwrap();
        assert_eq!(w.log_sink().sync_calls, 3);
    }

    #[test]
    fn failed_sync_means_no_success_and_poisons() {
        let o = opts();
        let mut sink = ShortSink::new(8, 0);
        sink.fail_sync = true;
        let mut w = Wal::from_sink(sink, "w", &o, HEADER_LEN);
        assert!(w.append_put(b"k", b"v", SyncMode::Always).is_err());
        assert!(w.is_poisoned());
        assert!(matches!(
            w.append_put(b"k", b"v", SyncMode::Always),
            Err(Error::Poisoned)
        ));
    }

    impl Wal<ShortSink> {
        fn log_sink(&self) -> &ShortSink {
            self.log.sink_ref()
        }
    }
}
