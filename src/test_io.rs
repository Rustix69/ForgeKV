//! I/O fault-injection helpers for unit tests.

use std::io::{self, Read, Write};

use crate::logfile::LogSink;

/// Accepts at most `max_chunk` bytes per `write` call and returns
/// `Interrupted` before every `interrupt_every`-th call (0 = never).
pub struct ShortSink {
    pub bytes: Vec<u8>,
    pub sync_calls: usize,
    pub fail_sync: bool,
    max_chunk: usize,
    interrupt_every: usize,
    calls: usize,
}

impl ShortSink {
    pub fn new(max_chunk: usize, interrupt_every: usize) -> Self {
        ShortSink {
            bytes: Vec::new(),
            sync_calls: 0,
            fail_sync: false,
            max_chunk,
            interrupt_every,
            calls: 0,
        }
    }
}

impl Write for ShortSink {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.calls += 1;
        if self.interrupt_every != 0 && self.calls.is_multiple_of(self.interrupt_every) {
            return Err(io::ErrorKind::Interrupted.into());
        }
        let n = buf.len().min(self.max_chunk);
        self.bytes.extend_from_slice(&buf[..n]);
        Ok(n)
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl LogSink for ShortSink {
    fn sync_data(&mut self) -> io::Result<()> {
        self.sync_calls += 1;
        if self.fail_sync {
            Err(io::Error::other("injected sync failure"))
        } else {
            Ok(())
        }
    }
}

/// Accepts `budget` bytes in total (in short writes), then fails every write.
pub struct FailingSink {
    pub bytes: Vec<u8>,
    budget: usize,
}

impl FailingSink {
    pub fn new(budget: usize) -> Self {
        FailingSink {
            bytes: Vec::new(),
            budget,
        }
    }
}

impl Write for FailingSink {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.budget == 0 {
            return Err(io::Error::other("injected write failure"));
        }
        let n = buf.len().min(self.budget).min(4);
        self.bytes.extend_from_slice(&buf[..n]);
        self.budget -= n;
        Ok(n)
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl LogSink for FailingSink {
    fn sync_data(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Returns at most `chunk` bytes per `read` call.
pub struct ChunkReader {
    data: Vec<u8>,
    pos: usize,
    chunk: usize,
}

impl ChunkReader {
    pub fn new(data: Vec<u8>, chunk: usize) -> Self {
        ChunkReader {
            data,
            pos: 0,
            chunk,
        }
    }
}

impl Read for ChunkReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = buf.len().min(self.chunk).min(self.data.len() - self.pos);
        buf[..n].copy_from_slice(&self.data[self.pos..self.pos + n]);
        self.pos += n;
        Ok(n)
    }
}
