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
pub struct CdrWriter<'a> {
    buf: &'a mut [u8],
    pos: usize,
}

impl<'a> CdrWriter<'a> {
    pub fn new(buf: &'a mut [u8]) -> Self {
        Self { buf, pos: 0 }
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

    pub fn finish(self) -> &'a [u8] {
        let end = self.pos.min(self.buf.len());
        &self.buf[..end]
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
