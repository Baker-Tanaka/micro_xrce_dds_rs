use crate::error::Error;
use embedded_io_async::{Read, Write};

#[cfg(feature = "defmt")]
use defmt::{debug, error};
#[cfg(not(feature = "defmt"))]
macro_rules! debug {
    ($($t:tt)*) => {};
}
#[cfg(not(feature = "defmt"))]
macro_rules! error {
    ($($t:tt)*) => {};
}

/// Write a length-prefixed XRCE-DDS frame over TCP.
/// Format: [payload_len: u16 LE] [payload: payload_len bytes]
/// Matches the eProsima Micro XRCE-DDS Agent TCP transport framing.
pub async fn write_framed<W: Write>(writer: &mut W, payload: &[u8]) -> Result<(), Error> {
    write_framed_unflushed(writer, payload).await?;
    writer.flush().await.map_err(|_| Error::Io)?;
    Ok(())
}

/// Like [`write_framed`] but without the final `flush`. For transports that
/// transmit written data on their own (TCP), where `flush` would wait for
/// the peer's acknowledgement and cap throughput at one frame per round trip.
pub async fn write_framed_unflushed<W: Write>(
    writer: &mut W,
    payload: &[u8],
) -> Result<(), Error> {
    if payload.len() > u16::MAX as usize {
        return Err(Error::FrameTooLarge);
    }
    let len_bytes = (payload.len() as u16).to_le_bytes();
    write_all(writer, &len_bytes).await?;
    write_all(writer, payload).await?;
    Ok(())
}

/// Read one length-prefixed XRCE-DDS frame into `buf`.
/// Returns the sub-slice that was filled.
pub async fn read_framed<'b, R: Read>(
    reader: &mut R,
    buf: &'b mut [u8],
) -> Result<&'b [u8], Error> {
    debug!("[framing] waiting for frame header...");
    let mut len_buf = [0u8; 2];
    read_exact(reader, &mut len_buf).await?;
    let len = u16::from_le_bytes(len_buf) as usize;
    debug!("[framing] frame header: len={}", len);
    if len > buf.len() {
        error!("[framing] frame too large: len={} buf={}", len, buf.len());
        return Err(Error::BufferTooSmall);
    }
    read_exact(reader, &mut buf[..len]).await?;
    let _show = len.min(8);
    debug!("[framing] rx {} bytes head={=[u8]}", len, &buf[.._show]);
    Ok(&buf[..len])
}

async fn write_all<W: Write>(w: &mut W, mut buf: &[u8]) -> Result<(), Error> {
    while !buf.is_empty() {
        let n = w.write(buf).await.map_err(|_| Error::Io)?;
        if n == 0 {
            return Err(Error::Disconnected);
        }
        buf = &buf[n..];
    }
    Ok(())
}

async fn read_exact<R: Read>(r: &mut R, mut buf: &mut [u8]) -> Result<(), Error> {
    while !buf.is_empty() {
        match r.read(buf).await {
            Err(_) => {
                error!("[framing] read: IO error (TCP reset or socket error)");
                return Err(Error::Io);
            }
            Ok(0) => {
                error!("[framing] read: TCP connection closed (0 bytes)");
                return Err(Error::Disconnected);
            }
            Ok(n) => {
                debug!("[framing] read: +{} bytes ({} remaining)", n, buf.len() - n);
                buf = &mut buf[n..];
            }
        }
    }
    Ok(())
}

/// Persistent, cancellation-safe frame reassembler.
///
/// Unlike [`read_framed`], the partially received length prefix and payload
/// live in this struct, so dropping a [`FrameReader::read_frame`] future
/// (e.g. losing a `select`) never loses bytes as long as the underlying
/// `Read::read` is itself cancel-safe (embassy-net `TcpSocket::read` is).
pub struct FrameReader<const N: usize> {
    buf: [u8; N],
    header: [u8; 2],
    header_len: u8,
    filled: usize,
    complete: bool,
}

impl<const N: usize> Default for FrameReader<N> {
    fn default() -> Self {
        Self::new()
    }
}

impl<const N: usize> FrameReader<N> {
    pub const fn new() -> Self {
        Self {
            buf: [0; N],
            header: [0; 2],
            header_len: 0,
            filled: 0,
            complete: false,
        }
    }

    /// Discard any partial or completed frame (e.g. before reconnecting).
    pub fn reset(&mut self) {
        self.header_len = 0;
        self.filled = 0;
        self.complete = false;
    }

    /// `true` if bytes of an incomplete frame are buffered.
    pub fn has_partial(&self) -> bool {
        !self.complete && (self.header_len != 0 || self.filled != 0)
    }

    /// The last completed frame, if [`FrameReader::read_frame`] returned one
    /// and no new read has started.
    pub fn frame(&self) -> Option<&[u8]> {
        self.complete.then(|| &self.buf[..self.frame_len()])
    }

    fn frame_len(&self) -> usize {
        u16::from_le_bytes(self.header) as usize
    }

    /// Read until one full frame is available and return it. The returned
    /// frame stays valid until the next call.
    pub async fn read_frame<R: Read>(&mut self, reader: &mut R) -> Result<&[u8], Error> {
        if self.complete {
            self.reset();
        }
        loop {
            if self.header_len < 2 {
                let start = self.header_len as usize;
                let n = read_some(reader, &mut self.header[start..]).await?;
                self.header_len += n as u8;
                if self.header_len == 2 && self.frame_len() > N {
                    error!(
                        "[framing] frame too large: len={} buf={}",
                        self.frame_len(),
                        N
                    );
                    self.reset();
                    return Err(Error::FrameTooLarge);
                }
                continue;
            }
            let len = self.frame_len();
            if self.filled < len {
                let n = read_some(reader, &mut self.buf[self.filled..len]).await?;
                self.filled += n;
                continue;
            }
            self.complete = true;
            return Ok(&self.buf[..len]);
        }
    }
}

async fn read_some<R: Read>(r: &mut R, buf: &mut [u8]) -> Result<usize, Error> {
    match r.read(buf).await {
        Err(_) => Err(Error::Io),
        Ok(0) => Err(Error::Disconnected),
        Ok(n) => Ok(n),
    }
}
