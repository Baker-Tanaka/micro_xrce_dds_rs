//! CDR Little Endian deserializer for ROS2 DDS message bodies.
//!
//! Companion to [`crate::cdr::CdrWriter`]. Two constructors:
//!
//! - [`CdrReader::new`] — skips the 4-byte CDR encapsulation header (use when
//!   the buffer starts with `\x00\x01\x00\x00`).
//! - [`CdrReader::from_body`] — no skip (use when the buffer is a raw CDR body
//!   as delivered by the micro-ROS agent in DATA submessages).

use crate::error::Error;
use core::convert::TryInto;

pub struct CdrReader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> CdrReader<'a> {
    /// Construct a reader over a SerializedPayload — the first 4 bytes
    /// (encapsulation header) are skipped automatically. Alignment for typed
    /// reads is computed from the start of the *body* (post-encap).
    pub fn new(buf: &'a [u8]) -> Self {
        let body = if buf.len() >= 4 { &buf[4..] } else { &[] };
        Self { buf: body, pos: 0 }
    }

    /// Construct a reader over already-stripped body bytes (no encap header
    /// expected). Origin = byte 0.
    pub fn from_body(body: &'a [u8]) -> Self {
        Self { buf: body, pos: 0 }
    }

    pub fn remaining(&self) -> usize {
        self.buf.len().saturating_sub(self.pos)
    }

    pub fn u8_val(&mut self) -> Result<u8, Error> {
        let b = *self.buf.get(self.pos).ok_or(Error::Deserialization)?;
        self.pos += 1;
        Ok(b)
    }

    pub fn bool_val(&mut self) -> Result<bool, Error> {
        Ok(self.u8_val()? != 0)
    }

    pub fn i32_val(&mut self) -> Result<i32, Error> {
        self.align(4);
        let bytes = self.take(4)?;
        Ok(i32::from_le_bytes(bytes.try_into().unwrap()))
    }

    pub fn u32_val(&mut self) -> Result<u32, Error> {
        self.align(4);
        let bytes = self.take(4)?;
        Ok(u32::from_le_bytes(bytes.try_into().unwrap()))
    }

    pub fn f32_val(&mut self) -> Result<f32, Error> {
        Ok(f32::from_bits(self.u32_val()?))
    }

    pub fn f64_val(&mut self) -> Result<f64, Error> {
        self.align(8);
        let bytes = self.take(8)?;
        Ok(f64::from_le_bytes(bytes.try_into().unwrap()))
    }

    pub fn i64_val(&mut self) -> Result<i64, Error> {
        self.align(8);
        let bytes = self.take(8)?;
        Ok(i64::from_le_bytes(bytes.try_into().unwrap()))
    }

    pub fn u64_val(&mut self) -> Result<u64, Error> {
        self.align(8);
        let bytes = self.take(8)?;
        Ok(u64::from_le_bytes(bytes.try_into().unwrap()))
    }

    /// Read `N` raw bytes with no alignment (CDR `octet[N]`).
    pub fn bytes_array<const N: usize>(&mut self) -> Result<[u8; N], Error> {
        let bytes = self.take(N)?;
        Ok(bytes.try_into().unwrap())
    }

    /// Align the cursor to `n` bytes.  Public for sequence-of-struct readers.
    pub fn align_to(&mut self, n: usize) {
        self.align(n);
    }

    /// Read a CDR `sequence<i32>` into a heapless `Vec` capped at `CAP`.
    /// Returns `Error::Deserialization` if the agent sent more elements than
    /// the local cap allows.
    pub fn i32_seq_into<const CAP: usize>(&mut self) -> Result<heapless::Vec<i32, CAP>, Error> {
        let n = self.u32_val()? as usize;
        if n > CAP {
            return Err(Error::Deserialization);
        }
        let mut v: heapless::Vec<i32, CAP> = heapless::Vec::new();
        for _ in 0..n {
            v.push(self.i32_val()?).ok();
        }
        Ok(v)
    }

    pub fn f64_array<const N: usize>(&mut self) -> Result<[f64; N], Error> {
        self.align(8);
        let mut out = [0.0; N];
        for slot in out.iter_mut() {
            *slot = self.f64_val()?;
        }
        Ok(out)
    }

    /// CDR string: u32 length (incl. null) + bytes + '\0'. Returns the string
    /// without the trailing null.
    pub fn string_bytes(&mut self) -> Result<&'a [u8], Error> {
        let len_with_null = self.u32_val()? as usize;
        if len_with_null == 0 {
            return Ok(&[]);
        }
        let body_len = len_with_null - 1;
        let bytes = self.take(body_len)?;
        // Skip the null terminator.
        let _ = self.take(1)?;
        Ok(bytes)
    }

    fn align(&mut self, n: usize) {
        let rem = self.pos % n;
        if rem != 0 {
            self.pos += n - rem;
        }
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], Error> {
        if self.pos + n > self.buf.len() {
            return Err(Error::Deserialization);
        }
        let s = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }
}
