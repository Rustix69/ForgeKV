//! Checksummed, append-only record log. Used by the WAL (and, later, the manifest).
//!
//! Format version 2 (PLAN.md §3.1), all integers little-endian:
//! ```text
//! file header (16 B): magic[8] = "FKVLOG\0\1" | kind u8 | version u8 = 2 | reserved[6] = 0
//! frame header (12 B): len u32 | header_crc u32 | payload_crc u32
//! frame:               frame header | payload[len]
//!   header_crc  = CRC-32/IEEE over the 4 bytes of `len`
//!   payload_crc = CRC-32/IEEE over (len as 4 LE bytes) || payload
//!   1 <= len <= max_record_size
//! ```
//! Version history: v1 (never released) had `crc32 | len | payload` with no
//! separate header checksum, so a corrupted `len` could not be told apart from
//! a torn write. Version 1 files are rejected with `UnsupportedVersion`.
//!
//! Write side: a frame is assembled in one buffer and written with a
//! write-all loop. We never assume the OS accepts it in one call or
//! atomically. Any write or sync error *poisons* the writer so that a
//! possibly-partial frame is never followed by more frames.
//!
//! Read side: classification follows PLAN.md §3.3.
//! - EOF before 8 bytes of the frame header are present: `Incomplete`
//!   (nothing to verify yet).
//! - With `len` and `header_crc` present (8..=11 bytes), `header_crc` is
//!   verified first. A mismatch is `Corruption`: a torn write only ever leaves a
//!   *prefix* of the correct bytes, so wrong bytes are damage, not tearing.
//! - A verified `len` of 0 or above the maximum is `Corruption`.
//! - EOF inside the payload (length now trusted) is `Incomplete`.
//! - A complete frame with a bad `payload_crc` is `Corruption`, even as the
//!   last record in the file.
//!
//! Remaining (unavoidable) ambiguity: fewer than 8 bytes of a final frame
//! header cannot be verified, so a damaged stub of that size is treated as torn.

use std::fs::File;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use crate::error::{Error, Result};

pub const MAGIC: [u8; 8] = *b"FKVLOG\0\x01";
pub const FORMAT_VERSION: u8 = 2;
pub const HEADER_LEN: u64 = 16;
/// `len` (4) + `header_crc` (4) + `payload_crc` (4).
pub const FRAME_HEADER_LEN: usize = 12;
/// Bytes needed to verify the header checksum: `len` + `header_crc`.
const VERIFIABLE_PREFIX_LEN: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogKind {
    Wal = 1,
    Manifest = 2,
}

pub fn encode_header(kind: LogKind) -> [u8; HEADER_LEN as usize] {
    let mut h = [0u8; HEADER_LEN as usize];
    h[..8].copy_from_slice(&MAGIC);
    h[8] = kind as u8;
    h[9] = FORMAT_VERSION;
    h
}

/// CRC-32/IEEE over the 4 little-endian bytes of `len`.
pub fn header_crc(len_le: [u8; 4]) -> u32 {
    crc32fast::hash(&len_le)
}

/// CRC-32/IEEE over `len` (4 LE bytes) followed by `payload`.
pub fn frame_crc(len_le: [u8; 4], payload: &[u8]) -> u32 {
    let mut h = crc32fast::Hasher::new();
    h.update(&len_le);
    h.update(payload);
    h.finalize()
}

/// Build a complete frame. Rejects empty payloads and payloads over `max_record`.
pub fn encode_frame(payload: &[u8], max_record: usize) -> Result<Vec<u8>> {
    if payload.is_empty() || payload.len() > max_record {
        return Err(Error::InvalidArgument(format!(
            "record length {} outside 1..={max_record}",
            payload.len()
        )));
    }
    let len = u32::try_from(payload.len())
        .map_err(|_| Error::InvalidArgument("record length exceeds u32".into()))?;
    let len_le = len.to_le_bytes();
    let mut frame = Vec::with_capacity(FRAME_HEADER_LEN + payload.len());
    frame.extend_from_slice(&len_le);
    frame.extend_from_slice(&header_crc(len_le).to_le_bytes());
    frame.extend_from_slice(&frame_crc(len_le, payload).to_le_bytes());
    frame.extend_from_slice(payload);
    Ok(frame)
}

/// A destination for log bytes. Implementations must not buffer in user
/// space: after `write_all` + `flush` the bytes are at least in the OS.
pub trait LogSink: Write {
    /// Flush file data to stable storage (`fdatasync`).
    fn sync_data(&mut self) -> io::Result<()>;
}

impl LogSink for File {
    fn sync_data(&mut self) -> io::Result<()> {
        File::sync_data(self)
    }
}

pub struct LogWriter<S: LogSink> {
    sink: S,
    path: PathBuf,
    max_record: usize,
    good_end: u64,
    poisoned: bool,
}

impl<S: LogSink> LogWriter<S> {
    /// `good_end` is the offset just past the last fully written frame (or
    /// [`HEADER_LEN`] for a fresh log whose header is already written).
    pub fn new(sink: S, path: impl Into<PathBuf>, max_record: usize, good_end: u64) -> Self {
        LogWriter {
            sink,
            path: path.into(),
            max_record,
            good_end,
            poisoned: false,
        }
    }

    /// Append one frame. On an I/O error the writer is poisoned: the file may
    /// now end in a partial frame, which recovery discards on reopen.
    /// Invalid arguments (bad length) are rejected without poisoning.
    pub fn append(&mut self, payload: &[u8]) -> Result<()> {
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        let frame = encode_frame(payload, self.max_record)?;
        let written = self.sink.write_all(&frame).and_then(|()| self.sink.flush());
        match written {
            Ok(()) => {
                self.good_end += frame.len() as u64;
                Ok(())
            }
            Err(e) => {
                self.poisoned = true;
                Err(Error::io("append log frame", &self.path, e))
            }
        }
    }

    /// Make everything appended so far durable. A failure poisons the writer
    /// and is never retried: after a failed fsync the state of the page cache
    /// is unknowable.
    pub fn sync(&mut self) -> Result<()> {
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        match self.sink.sync_data() {
            Ok(()) => Ok(()),
            Err(e) => {
                self.poisoned = true;
                Err(Error::io("sync log", &self.path, e))
            }
        }
    }

    pub fn good_end(&self) -> u64 {
        self.good_end
    }

    #[cfg(test)]
    pub(crate) fn sink_ref(&self) -> &S {
        &self.sink
    }

    pub fn is_poisoned(&self) -> bool {
        self.poisoned
    }
}

/// Result of reading one frame position.
#[derive(Debug, PartialEq, Eq)]
pub enum Frame {
    Record {
        offset: u64,
        payload: Vec<u8>,
    },
    /// The file ended exactly at a frame boundary.
    Clean,
    /// The file ended inside a frame (torn tail): fewer than 8 header bytes, a
    /// verified header with fewer than 12, or a verified length with a short payload.
    Incomplete,
}

pub struct LogReader<R: Read> {
    r: R,
    path: PathBuf,
    max_record: usize,
    /// Offset just past the last fully validated frame.
    offset: u64,
}

/// Read until `buf` is full or EOF; returns bytes read. Tolerates short
/// reads and retries `Interrupted`.
fn read_full<R: Read>(r: &mut R, buf: &mut [u8]) -> io::Result<usize> {
    let mut n = 0;
    while n < buf.len() {
        match r.read(&mut buf[n..]) {
            Ok(0) => break,
            Ok(k) => n += k,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(n)
}

impl<R: Read> LogReader<R> {
    /// Validate the file header. Returns `Ok(None)` if the file is shorter than
    /// a full header and what is present is a prefix of the expected header
    /// (creation was interrupted). A wrong magic/kind or non-zero reserved
    /// bytes are `Corruption`; an unknown version is `UnsupportedVersion`.
    pub fn open(mut r: R, path: &Path, kind: LogKind, max_record: usize) -> Result<Option<Self>> {
        let expected = encode_header(kind);
        let mut h = [0u8; HEADER_LEN as usize];
        let n = read_full(&mut r, &mut h).map_err(|e| Error::io("read log header", path, e))?;
        if n < h.len() {
            if h[..n] != expected[..n] {
                return Err(Error::corruption(path, 0, "log header prefix mismatch"));
            }
            return Ok(None);
        }
        if h[..8] != MAGIC {
            return Err(Error::corruption(path, 0, "bad log magic"));
        }
        if h[8] != kind as u8 {
            return Err(Error::corruption(
                path,
                8,
                format!("log kind {} does not match expected {}", h[8], kind as u8),
            ));
        }
        if h[9] != FORMAT_VERSION {
            return Err(Error::UnsupportedVersion {
                file: path.to_path_buf(),
                version: u32::from(h[9]),
            });
        }
        // Not covered by any checksum, so check them: version 2 writers
        // always write zeros, and any other value is damage.
        if h[10..] != expected[10..] {
            return Err(Error::corruption(
                path,
                10,
                "reserved log header bytes are not zero",
            ));
        }
        Ok(Some(LogReader {
            r,
            path: path.to_path_buf(),
            max_record,
            offset: HEADER_LEN,
        }))
    }

    /// Offset just past the last frame returned as a `Record`.
    pub fn good_end(&self) -> u64 {
        self.offset
    }

    pub fn next_frame(&mut self) -> Result<Frame> {
        let at = self.offset;
        let io_err = |e| Error::io("read log frame", &self.path, e);

        let mut hdr = [0u8; FRAME_HEADER_LEN];
        let n = read_full(&mut self.r, &mut hdr).map_err(io_err)?;
        if n == 0 {
            return Ok(Frame::Clean);
        }
        if n < VERIFIABLE_PREFIX_LEN {
            return Ok(Frame::Incomplete);
        }
        let len_le = [hdr[0], hdr[1], hdr[2], hdr[3]];
        let stored_header_crc = u32::from_le_bytes([hdr[4], hdr[5], hdr[6], hdr[7]]);
        if header_crc(len_le) != stored_header_crc {
            return Err(Error::corruption(
                &self.path,
                at,
                "frame header checksum mismatch",
            ));
        }
        let len = u32::from_le_bytes(len_le) as usize;
        if len == 0 || len > self.max_record {
            return Err(Error::corruption(
                &self.path,
                at,
                format!("invalid record length {len} (max {})", self.max_record),
            ));
        }
        if n < FRAME_HEADER_LEN {
            return Ok(Frame::Incomplete);
        }
        let stored_crc = u32::from_le_bytes([hdr[8], hdr[9], hdr[10], hdr[11]]);

        // `take` + `read_to_end` grows the buffer as data actually arrives, so
        // a bogus-but-in-range length on a tiny file does not allocate `len`.
        let mut payload = Vec::new();
        let got = (&mut self.r)
            .take(len as u64)
            .read_to_end(&mut payload)
            .map_err(io_err)?;
        if got < len {
            return Ok(Frame::Incomplete);
        }
        if frame_crc(len_le, &payload) != stored_crc {
            return Err(Error::corruption(
                &self.path,
                at,
                "payload checksum mismatch in complete record",
            ));
        }
        self.offset = at + FRAME_HEADER_LEN as u64 + len as u64;
        Ok(Frame::Record {
            offset: at,
            payload,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_io::{ChunkReader, FailingSink, ShortSink};
    use std::io::Cursor;

    const MAX: usize = 1024;

    fn path() -> &'static Path {
        Path::new("test.log")
    }

    fn build_log(payloads: &[&[u8]]) -> Vec<u8> {
        let mut out = encode_header(LogKind::Wal).to_vec();
        for p in payloads {
            out.extend(encode_frame(p, MAX).unwrap());
        }
        out
    }

    #[test]
    fn crc32_ieee_known_answer() {
        let mut h = crc32fast::Hasher::new();
        h.update(b"123456789");
        assert_eq!(h.finalize(), 0xCBF4_3926);
    }

    #[test]
    fn golden_header_and_frame_bytes() {
        assert_eq!(
            encode_header(LogKind::Wal),
            [
                b'F', b'K', b'V', b'L', b'O', b'G', 0, 1, 1, 2, 0, 0, 0, 0, 0, 0
            ]
        );
        // len = 3; header_crc = crc32(03 00 00 00) = 0x33f170f2;
        // payload_crc = crc32(03 00 00 00 01 02 03) = 0x06799cec (both computed with zlib).
        assert_eq!(
            encode_frame(&[1, 2, 3], MAX).unwrap(),
            [
                3, 0, 0, 0, // len
                0xf2, 0x70, 0xf1, 0x33, // header_crc
                0xec, 0x9c, 0x79, 0x06, // payload_crc
                1, 2, 3, // payload
            ]
        );
        assert_eq!(FRAME_HEADER_LEN, 12);
    }

    #[test]
    fn encode_frame_rejects_bad_lengths() {
        assert!(encode_frame(&[], MAX).is_err());
        assert!(encode_frame(&vec![0; MAX + 1], MAX).is_err());
        assert!(encode_frame(&vec![0; MAX], MAX).is_ok());
    }

    #[test]
    fn roundtrip_many_records() {
        let log = build_log(&[b"a", b"bb", b"ccc"]);
        let mut r = LogReader::open(Cursor::new(&log), path(), LogKind::Wal, MAX)
            .unwrap()
            .unwrap();
        let mut got = vec![];
        loop {
            match r.next_frame().unwrap() {
                Frame::Record { payload, .. } => got.push(payload),
                Frame::Clean => break,
                Frame::Incomplete => panic!("unexpected incomplete"),
            }
        }
        assert_eq!(got, vec![b"a".to_vec(), b"bb".to_vec(), b"ccc".to_vec()]);
        assert_eq!(r.good_end(), log.len() as u64);
    }

    #[test]
    fn short_writes_and_interrupts_produce_identical_bytes() {
        let mut sink = ShortSink::new(3, 2);
        sink.write_all(&encode_header(LogKind::Wal)).unwrap();
        let mut w = LogWriter::new(sink, path(), MAX, HEADER_LEN);
        for p in [&b"hello"[..], b"x", &[9u8; 200][..]] {
            w.append(p).unwrap();
        }
        let expected = build_log(&[b"hello", b"x", &[9u8; 200]]);
        assert_eq!(w.sink.bytes, expected);
        assert_eq!(w.good_end(), expected.len() as u64);
    }

    #[test]
    fn mid_frame_write_failure_poisons_writer() {
        let first = encode_frame(b"first", MAX).unwrap();
        // Allow the first frame plus a few bytes of the second, then fail.
        let mut w = LogWriter::new(FailingSink::new(first.len() + 5), path(), MAX, HEADER_LEN);
        w.append(b"first").unwrap();
        let good = w.good_end();
        assert!(matches!(w.append(b"second"), Err(Error::Io { .. })));
        assert!(w.is_poisoned());
        assert_eq!(
            w.good_end(),
            good,
            "good_end must not cover the partial frame"
        );
        assert!(matches!(w.append(b"third"), Err(Error::Poisoned)));
        assert!(matches!(w.sync(), Err(Error::Poisoned)));
        // The partial frame is on "disk"; the reader must see it as incomplete.
        let mut bytes = encode_header(LogKind::Wal).to_vec();
        bytes.extend_from_slice(&w.sink.bytes);
        let mut r = LogReader::open(Cursor::new(bytes), path(), LogKind::Wal, MAX)
            .unwrap()
            .unwrap();
        assert!(matches!(r.next_frame().unwrap(), Frame::Record { .. }));
        assert_eq!(r.next_frame().unwrap(), Frame::Incomplete);
    }

    #[test]
    fn invalid_append_does_not_poison() {
        let mut w = LogWriter::new(ShortSink::new(8, 0), path(), MAX, HEADER_LEN);
        assert!(matches!(w.append(&[]), Err(Error::InvalidArgument(_))));
        assert!(!w.is_poisoned());
        w.append(b"ok").unwrap();
    }

    #[test]
    fn sync_failure_poisons_and_is_not_retried() {
        let mut sink = ShortSink::new(8, 0);
        sink.fail_sync = true;
        let mut w = LogWriter::new(sink, path(), MAX, HEADER_LEN);
        w.append(b"a").unwrap();
        assert!(matches!(w.sync(), Err(Error::Io { .. })));
        assert!(w.is_poisoned());
        assert_eq!(w.sink.sync_calls, 1);
        assert!(matches!(w.sync(), Err(Error::Poisoned)));
        assert_eq!(w.sink.sync_calls, 1, "fsync must not be retried");
    }

    /// Cut the final frame at every possible length: every cut is either a
    /// clean boundary (cut == 0) or an incomplete frame, never a record or
    /// an error, and earlier records are unaffected.
    #[test]
    fn truncation_at_every_offset_of_last_frame_is_incomplete() {
        let log = build_log(&[b"first", b"the-last-record"]);
        let last_len = FRAME_HEADER_LEN + b"the-last-record".len();
        let boundary = log.len() - last_len;
        for cut in 0..last_len {
            let bytes = &log[..boundary + cut];
            let mut r = LogReader::open(Cursor::new(bytes), path(), LogKind::Wal, MAX)
                .unwrap()
                .unwrap();
            assert!(matches!(r.next_frame().unwrap(), Frame::Record { .. }));
            let expect = if cut == 0 {
                Frame::Clean
            } else {
                Frame::Incomplete
            };
            assert_eq!(r.next_frame().unwrap(), expect, "cut={cut}");
            assert_eq!(r.good_end(), boundary as u64, "cut={cut}");
        }
    }

    #[test]
    fn one_byte_reads_match_normal_reads() {
        let log = build_log(&[b"alpha", b"beta", b"gamma"]);
        for chunk in [1, 2, 3, 7] {
            let mut r = LogReader::open(
                ChunkReader::new(log.clone(), chunk),
                path(),
                LogKind::Wal,
                MAX,
            )
            .unwrap()
            .unwrap();
            let mut n = 0;
            while let Frame::Record { .. } = r.next_frame().unwrap() {
                n += 1;
            }
            assert_eq!(n, 3, "chunk={chunk}");
            assert_eq!(r.good_end(), log.len() as u64);
        }
        // Also with an incomplete tail.
        let cut = &log[..log.len() - 2];
        let mut r = LogReader::open(ChunkReader::new(cut.to_vec(), 1), path(), LogKind::Wal, MAX)
            .unwrap()
            .unwrap();
        assert!(matches!(r.next_frame().unwrap(), Frame::Record { .. }));
        assert!(matches!(r.next_frame().unwrap(), Frame::Record { .. }));
        assert_eq!(r.next_frame().unwrap(), Frame::Incomplete);
    }

    fn first_error_or_records(bytes: Vec<u8>) -> std::result::Result<Vec<Vec<u8>>, Error> {
        let mut r = LogReader::open(Cursor::new(bytes), path(), LogKind::Wal, MAX)?.unwrap();
        let mut out = vec![];
        loop {
            match r.next_frame()? {
                Frame::Record { payload, .. } => out.push(payload),
                Frame::Clean | Frame::Incomplete => return Ok(out),
            }
        }
    }

    /// Every bit of every byte of the final frame (header and payload): a
    /// complete-looking but damaged final record is always `Corruption`,
    /// never silently truncated and never accepted.
    #[test]
    fn bit_flip_anywhere_in_final_frame_is_corruption() {
        let log = build_log(&[b"first", b"final-record"]);
        let last_start = log.len() - (FRAME_HEADER_LEN + b"final-record".len());
        for pos in last_start..log.len() {
            for bit in 0..8 {
                let mut bad = log.clone();
                bad[pos] ^= 1 << bit;
                assert!(
                    matches!(
                        first_error_or_records(bad),
                        Err(Error::Corruption { offset, .. }) if offset == last_start as u64
                    ),
                    "pos={pos} bit={bit}"
                );
            }
        }
    }

    /// The case v1 could not detect: a length enlarged past EOF used to look
    /// like a torn write. With `header_crc` it is Corruption, for every bit.
    #[test]
    fn length_field_bit_flips_are_always_corruption() {
        let log = build_log(&[b"first", b"final-record"]);
        let last_start = log.len() - (FRAME_HEADER_LEN + b"final-record".len());
        for pos in last_start..last_start + 4 {
            for bit in 0..8 {
                let mut bad = log.clone();
                bad[pos] ^= 1 << bit;
                assert!(
                    matches!(first_error_or_records(bad), Err(Error::Corruption { .. })),
                    "len byte {} bit {bit}",
                    pos - last_start
                );
            }
        }
        // Same for a frame in the middle of the log.
        let first_start = HEADER_LEN as usize;
        for bit in 0..8 {
            let mut bad = log.clone();
            bad[first_start + 1] ^= 1 << bit;
            assert!(matches!(
                first_error_or_records(bad),
                Err(Error::Corruption { .. })
            ));
        }
    }

    /// A header_crc field that is itself damaged is Corruption too.
    #[test]
    fn header_crc_field_damage_is_corruption() {
        let log = build_log(&[b"final-record"]);
        let start = HEADER_LEN as usize;
        for pos in start + 4..start + 8 {
            let mut bad = log.clone();
            bad[pos] ^= 0x10;
            assert!(matches!(
                first_error_or_records(bad),
                Err(Error::Corruption { .. })
            ));
        }
    }

    /// Cuts inside the 12-byte frame header. Fewer than 8 bytes cannot be
    /// verified (Incomplete). With `len` + `header_crc` present they are
    /// verified: intact => Incomplete, damaged => Corruption (a torn write
    /// only leaves a prefix of the correct bytes).
    #[test]
    fn truncated_frame_headers() {
        let base = build_log(&[b"first"]);
        let frame = encode_frame(b"second-record", MAX).unwrap();
        for cut in 1..FRAME_HEADER_LEN {
            let mut intact = base.clone();
            intact.extend_from_slice(&frame[..cut]);
            let mut r = LogReader::open(Cursor::new(intact), path(), LogKind::Wal, MAX)
                .unwrap()
                .unwrap();
            assert!(matches!(r.next_frame().unwrap(), Frame::Record { .. }));
            assert_eq!(r.next_frame().unwrap(), Frame::Incomplete, "cut={cut}");
            assert_eq!(r.good_end(), base.len() as u64, "cut={cut}");

            // Damage a byte of the header_crc (only visible once 8 bytes exist).
            let mut damaged = base.clone();
            let mut part = frame[..cut].to_vec();
            if cut > 4 {
                part[4] ^= 0xFF;
            }
            damaged.extend_from_slice(&part);
            let res = first_error_or_records(damaged);
            if cut >= VERIFIABLE_PREFIX_LEN {
                assert!(matches!(res, Err(Error::Corruption { .. })), "cut={cut}");
            } else {
                // Unverifiable stub: documented as torn.
                assert_eq!(res.unwrap(), vec![b"first".to_vec()], "cut={cut}");
            }
        }
    }

    /// Verified length with a short payload is a torn tail at every cut.
    #[test]
    fn truncated_payload_after_verified_header_is_incomplete() {
        let base = build_log(&[b"first"]);
        let frame = encode_frame(b"second-record", MAX).unwrap();
        for cut in FRAME_HEADER_LEN..frame.len() {
            let mut bytes = base.clone();
            bytes.extend_from_slice(&frame[..cut]);
            let mut r = LogReader::open(Cursor::new(bytes), path(), LogKind::Wal, MAX)
                .unwrap()
                .unwrap();
            assert!(matches!(r.next_frame().unwrap(), Frame::Record { .. }));
            assert_eq!(r.next_frame().unwrap(), Frame::Incomplete, "cut={cut}");
        }
    }

    #[test]
    fn mid_log_bit_flip_is_corruption() {
        let log = build_log(&[b"first", b"second", b"third"]);
        let mut bad = log.clone();
        bad[HEADER_LEN as usize + FRAME_HEADER_LEN] ^= 0x01; // payload of first frame
        assert!(matches!(
            first_error_or_records(bad),
            Err(Error::Corruption { offset, .. }) if offset == HEADER_LEN
        ));
    }

    #[test]
    fn impossible_lengths_are_corruption_without_allocating() {
        for bad_len in [0u32, MAX as u32 + 1, u32::MAX] {
            let len_le = bad_len.to_le_bytes();
            // Valid header_crc: the length itself is rejected.
            let mut log = build_log(&[b"first"]);
            log.extend_from_slice(&len_le);
            log.extend_from_slice(&header_crc(len_le).to_le_bytes());
            log.extend_from_slice(&0u32.to_le_bytes());
            assert!(
                matches!(first_error_or_records(log), Err(Error::Corruption { .. })),
                "len={bad_len}"
            );
            // Invalid header_crc: rejected before the length is even considered.
            let mut log = build_log(&[b"first"]);
            log.extend_from_slice(&len_le);
            log.extend_from_slice(&0u32.to_le_bytes());
            log.extend_from_slice(&0u32.to_le_bytes());
            assert!(
                matches!(first_error_or_records(log), Err(Error::Corruption { .. })),
                "len={bad_len} bad header crc"
            );
        }
    }

    #[test]
    fn zero_filled_tail_is_corruption() {
        let mut log = build_log(&[b"first"]);
        log.extend(std::iter::repeat_n(0u8, 64));
        assert!(matches!(
            first_error_or_records(log),
            Err(Error::Corruption { .. })
        ));
    }

    /// 1..=7 zero bytes cannot be verified and are treated as a torn stub; 8 or
    /// more (header_crc of len 0 is not 0) are Corruption.
    #[test]
    fn short_zero_extension_is_torn_but_long_is_corruption() {
        for n in 1..=7 {
            let mut log = build_log(&[b"first"]);
            log.extend(std::iter::repeat_n(0u8, n));
            assert_eq!(first_error_or_records(log).unwrap().len(), 1, "n={n}");
        }
        for n in 8..=40 {
            let mut log = build_log(&[b"first"]);
            log.extend(std::iter::repeat_n(0u8, n));
            assert!(
                matches!(first_error_or_records(log), Err(Error::Corruption { .. })),
                "n={n}"
            );
        }
    }

    #[test]
    fn header_cases() {
        let ok = encode_header(LogKind::Wal);
        // Every proper prefix: creation interrupted.
        for n in 0..ok.len() {
            let r = LogReader::open(Cursor::new(&ok[..n]), path(), LogKind::Wal, MAX).unwrap();
            assert!(r.is_none(), "n={n}");
        }
        // Prefix that is not a prefix of the header.
        assert!(matches!(
            LogReader::open(Cursor::new(b"XYZ".to_vec()), path(), LogKind::Wal, MAX),
            Err(Error::Corruption { .. })
        ));
        // Wrong magic, wrong kind, unknown version.
        let mut bad = ok;
        bad[0] = b'X';
        assert!(matches!(
            LogReader::open(Cursor::new(bad.to_vec()), path(), LogKind::Wal, MAX),
            Err(Error::Corruption { .. })
        ));
        assert!(matches!(
            LogReader::open(Cursor::new(ok.to_vec()), path(), LogKind::Manifest, MAX),
            Err(Error::Corruption { .. })
        ));
        // Version 1 (no header checksum) and future versions are rejected.
        for v in [1u8, 3, 255] {
            let mut bad = ok;
            bad[9] = v;
            assert!(matches!(
                LogReader::open(Cursor::new(bad.to_vec()), path(), LogKind::Wal, MAX),
                Err(Error::UnsupportedVersion { version, .. }) if version == u32::from(v)
            ));
        }
    }

    /// Regression (Phase 8 corruption sweep): a flipped bit in the reserved
    /// header bytes was accepted silently; it is not covered by any CRC.
    #[test]
    fn non_zero_reserved_header_bytes_are_corruption() {
        for kind in [LogKind::Wal, LogKind::Manifest] {
            let good = encode_header(kind);
            assert!(
                LogReader::open(Cursor::new(good.to_vec()), path(), kind, MAX)
                    .unwrap()
                    .is_some()
            );
            for i in 10..HEADER_LEN as usize {
                let mut bad = good;
                bad[i] = 0x01;
                let err = LogReader::open(Cursor::new(bad.to_vec()), path(), kind, MAX)
                    .err()
                    .unwrap();
                assert!(
                    matches!(&err, Error::Corruption { offset: 10, .. }),
                    "byte {i}: {err}"
                );
            }
        }
    }
}
