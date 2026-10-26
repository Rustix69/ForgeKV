//! Bounds-checked little-endian reader shared by the binary format parsers.
//! Errors are plain strings; callers wrap them in `Error::Corruption` with
//! the file and offset they know about.

pub(crate) struct Cur<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cur<'a> {
    pub(crate) fn new(buf: &'a [u8]) -> Self {
        Cur { buf, pos: 0 }
    }

    /// Bytes consumed so far.
    pub(crate) fn pos(&self) -> usize {
        self.pos
    }

    pub(crate) fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    pub(crate) fn take(&mut self, n: usize) -> Result<&'a [u8], String> {
        if self.remaining() < n {
            return Err(format!("need {n} bytes, have {}", self.remaining()));
        }
        let s = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }

    pub(crate) fn u8(&mut self) -> Result<u8, String> {
        Ok(self.take(1)?[0])
    }

    pub(crate) fn u32(&mut self) -> Result<u32, String> {
        let b = self.take(4)?;
        Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }

    pub(crate) fn u64(&mut self) -> Result<u64, String> {
        let b = self.take(8)?;
        Ok(u64::from_le_bytes([
            b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
        ]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_and_bounds() {
        let data = [1u8, 2, 0, 0, 0, 9, 0, 0, 0, 0, 0, 0, 0, 7];
        let mut c = Cur::new(&data);
        assert_eq!(c.u8().unwrap(), 1);
        assert_eq!(c.u32().unwrap(), 2);
        assert_eq!(c.u64().unwrap(), 9);
        assert_eq!(c.pos(), 13);
        assert_eq!(c.remaining(), 1);
        assert!(c.u32().is_err());
        assert_eq!(c.take(1).unwrap(), &[7]);
        assert!(c.take(1).is_err());
        assert!(c.take(usize::MAX).is_err(), "no overflow on huge lengths");
    }
}
