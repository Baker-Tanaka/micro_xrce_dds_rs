/// CDR Little Endian serializer for ROS2 DDS messages.
///
/// Alignment follows the CDR standard: each scalar is aligned to its own size.
/// Alignment is computed from byte 0 of the entire buffer (including the 4-byte
/// encapsulation header written by `header()`).
pub struct CdrWriter<'a> {
    buf: &'a mut [u8],
    pos: usize,
}

impl<'a> CdrWriter<'a> {
    pub fn new(buf: &'a mut [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    /// Write the 4-byte CDR Little Endian encapsulation header.
    /// Must be called first; establishes the alignment origin.
    pub fn header(&mut self) {
        self.raw(&[0x00, 0x01, 0x00, 0x00]);
    }

    pub fn u8_val(&mut self, v: u8) {
        self.raw(&[v]);
    }

    pub fn u32_val(&mut self, v: u32) {
        self.align(4);
        self.raw(&v.to_le_bytes());
    }

    pub fn f32_val(&mut self, v: f32) {
        self.align(4);
        self.raw(&v.to_le_bytes());
    }

    pub fn f64_val(&mut self, v: f64) {
        self.align(8);
        self.raw(&v.to_le_bytes());
    }

    /// CDR string: u32 length (including null terminator) + bytes + '\0'.
    /// The u32 length field is 4-byte aligned before writing.
    pub fn string(&mut self, s: &str) {
        self.align(4);
        self.raw(&((s.len() as u32 + 1).to_le_bytes()));
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

    /// Number of bytes written (capped at buffer length).
    pub fn bytes_written(&self) -> usize {
        self.pos.min(self.buf.len())
    }

    pub fn finish(&self) -> &[u8] {
        &self.buf[..self.bytes_written()]
    }

    // ── internal helpers ────────────────────────────────────────────────────

    fn align(&mut self, n: usize) {
        let rem = self.pos % n;
        if rem != 0 {
            let pad = n - rem;
            for _ in 0..pad {
                if self.pos < self.buf.len() {
                    self.buf[self.pos] = 0;
                }
                self.pos += 1;
            }
        }
    }

    fn raw(&mut self, bytes: &[u8]) {
        let space = self.buf.len().saturating_sub(self.pos);
        let n = bytes.len().min(space);
        self.buf[self.pos..self.pos + n].copy_from_slice(&bytes[..n]);
        self.pos += bytes.len();
    }
}
