use crate::error::Error;

/// CDR Little Endian serializer for ROS2 DDS message *bodies* (field data only).
///
/// **Do not** include the 4-byte CDR encapsulation header (`00 01 00 00`) when
/// publishing through `XrceSession::write_data` — Fast-DDS adds it when it
/// builds the SerializedPayload. Doubling the encap silently breaks
/// deserialization (`ros2 topic echo` will show nothing while `--raw` reveals
/// two encap headers in a row). The legacy `header()` method is kept for
/// edge cases that handle the SerializedPayload manually; the standard
/// `serialize_*` helpers in `ros2::msg::*` no longer call it.
///
/// Alignment follows the CDR standard: each scalar is aligned to its own size,
/// computed from byte 0 of the buffer (which is the start of the body when no
/// encap header is written).
///
/// Writes never panic. If the buffer is too small, the writer latches an
/// overflow flag, stops writing, and [`CdrWriter::finish_checked`] returns
/// [`Error::BufferTooSmall`]. Callers that publish data must use
/// `finish_checked`; the legacy [`CdrWriter::finish`] returns the (possibly
/// truncated) prefix for compatibility only.
pub struct CdrWriter<'a> {
    buf: &'a mut [u8],
    pos: usize,
    overflow: bool,
}

impl<'a> CdrWriter<'a> {
    pub fn new(buf: &'a mut [u8]) -> Self {
        Self {
            buf,
            pos: 0,
            overflow: false,
        }
    }

    /// Write the 4-byte CDR Little Endian encapsulation header.
    ///
    /// Provided for callers that own the full SerializedPayload (e.g. raw DDS
    /// experiments). Do **not** call this when handing the result to
    /// `XrceSession::write_data` — the agent / Fast-DDS layer adds the encap.
    pub fn header(&mut self) {
        self.raw(&[0x00, 0x01, 0x00, 0x00]);
    }

    pub fn u8_val(&mut self, v: u8) {
        self.raw(&[v]);
    }

    pub fn i8_val(&mut self, v: i8) {
        self.raw(&v.to_le_bytes());
    }

    pub fn u16_val(&mut self, v: u16) {
        self.align(2);
        self.raw(&v.to_le_bytes());
    }

    pub fn i16_val(&mut self, v: i16) {
        self.align(2);
        self.raw(&v.to_le_bytes());
    }

    pub fn u32_val(&mut self, v: u32) {
        self.align(4);
        self.raw(&v.to_le_bytes());
    }

    pub fn i32_val(&mut self, v: i32) {
        self.align(4);
        self.raw(&v.to_le_bytes());
    }

    pub fn bool_val(&mut self, v: bool) {
        self.raw(&[v as u8]);
    }

    pub fn f32_val(&mut self, v: f32) {
        self.align(4);
        self.raw(&v.to_le_bytes());
    }

    pub fn f64_val(&mut self, v: f64) {
        self.align(8);
        self.raw(&v.to_le_bytes());
    }

    pub fn i64_val(&mut self, v: i64) {
        self.align(8);
        self.raw(&v.to_le_bytes());
    }

    pub fn u64_val(&mut self, v: u64) {
        self.align(8);
        self.raw(&v.to_le_bytes());
    }

    /// Write `N` raw bytes with no alignment (CDR `octet[N]`).
    pub fn bytes_raw(&mut self, b: &[u8]) {
        self.raw(b);
    }

    /// Align the cursor to `n` bytes, padding with zeros.  Public so wrapper
    /// types can align before each element of a CDR sequence.
    pub fn align_to(&mut self, n: usize) {
        self.align(n);
    }

    /// Write a CDR `sequence<i32>` from a slice — `u32` length prefix + each
    /// element 4-byte aligned.
    pub fn i32_seq(&mut self, items: &[i32]) {
        self.u32_val(items.len() as u32);
        for v in items {
            self.i32_val(*v);
        }
    }

    /// CDR string: u32 length (including null terminator) + bytes + '\0'.
    /// The u32 length field is 4-byte aligned before writing.
    pub fn string(&mut self, s: &str) {
        let len = match u32::try_from(s.len()).ok().and_then(|n| n.checked_add(1)) {
            Some(len) => len,
            None => {
                self.overflow = true;
                return;
            }
        };
        self.align(4);
        self.raw(&len.to_le_bytes());
        self.raw(s.as_bytes());
        self.raw(&[0x00]); // null terminator (no trailing pad — caller aligns next field)
    }

    pub fn f64_array<const N: usize>(&mut self, arr: &[f64; N]) {
        self.align(8);
        for v in arr {
            self.raw(&v.to_le_bytes());
        }
    }

    pub fn f64_slice(&mut self, arr: &[f64]) {
        self.align(8);
        for v in arr {
            self.raw(&v.to_le_bytes());
        }
    }

    /// Number of bytes successfully written.
    pub fn bytes_written(&self) -> usize {
        self.pos
    }

    /// `true` once any write did not fit in the buffer.
    pub fn overflowed(&self) -> bool {
        self.overflow
    }

    /// Finish serialization, failing if any write did not fit.
    pub fn finish_checked(self) -> Result<&'a [u8], Error> {
        if self.overflow {
            Err(Error::BufferTooSmall)
        } else {
            Ok(&self.buf[..self.pos])
        }
    }

    /// Legacy unchecked finish: returns the bytes written so far, which are
    /// truncated if [`CdrWriter::overflowed`] is `true`.
    pub fn finish(self) -> &'a [u8] {
        &self.buf[..self.pos]
    }

    // ── internal helpers ────────────────────────────────────────────────────

    fn align(&mut self, n: usize) {
        let rem = self.pos % n;
        if rem != 0 {
            self.fill_zero(n - rem);
        }
    }

    fn fill_zero(&mut self, n: usize) {
        if self.overflow || self.buf.len() - self.pos < n {
            self.overflow = true;
            return;
        }
        self.buf[self.pos..self.pos + n].fill(0);
        self.pos += n;
    }

    fn raw(&mut self, bytes: &[u8]) {
        if self.overflow || self.buf.len() - self.pos < bytes.len() {
            self.overflow = true;
            return;
        }
        self.buf[self.pos..self.pos + bytes.len()].copy_from_slice(bytes);
        self.pos += bytes.len();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overflow_is_latched_without_panic() {
        let mut buf = [0u8; 6];
        let mut w = CdrWriter::new(&mut buf);
        w.u8_val(1);
        w.u32_val(7);
        w.u32_val(8);
        w.u8_val(9);
        assert!(w.overflowed());
        assert_eq!(w.bytes_written(), 4);
        assert_eq!(w.finish_checked(), Err(Error::BufferTooSmall));
    }

    #[test]
    fn small_ints_are_aligned() {
        let mut buf = [0xFFu8; 8];
        let mut w = CdrWriter::new(&mut buf);
        w.i8_val(-1);
        w.u16_val(0x0102);
        w.i16_val(-2);
        assert_eq!(w.finish_checked().unwrap(), &[0xFF, 0, 2, 1, 0xFE, 0xFF]);
    }
}
