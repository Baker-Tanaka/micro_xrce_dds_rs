use embedded_io_async::{Read, Write};
use crate::error::XrceError;

#[cfg(feature = "defmt")]
use defmt::{debug, error};
#[cfg(not(feature = "defmt"))]
macro_rules! debug { ($($t:tt)*) => {}; }
#[cfg(not(feature = "defmt"))]
macro_rules! error { ($($t:tt)*) => {}; }

/// Write a length-prefixed XRCE-DDS frame over TCP.
/// Format: [payload_len: u16 LE] [payload: payload_len bytes]
/// Matches the eProsima Micro XRCE-DDS Agent TCP transport framing.
pub async fn write_framed<W: Write>(writer: &mut W, payload: &[u8]) -> Result<(), XrceError> {
    debug!("[framing] tx {} bytes", payload.len());
    let len_bytes = (payload.len() as u16).to_le_bytes();
    write_all(writer, &len_bytes).await?;
    write_all(writer, payload).await?;
    writer.flush().await.map_err(|_| XrceError::Io)?;
    debug!("[framing] tx flush OK");
    Ok(())
}

/// Read one length-prefixed XRCE-DDS frame into `buf`.
/// Returns the sub-slice that was filled.
pub async fn read_framed<'b, R: Read>(
    reader: &mut R,
    buf: &'b mut [u8],
) -> Result<&'b [u8], XrceError> {
    debug!("[framing] waiting for frame header...");
    let mut len_buf = [0u8; 2];
    read_exact(reader, &mut len_buf).await?;
    let len = u16::from_le_bytes(len_buf) as usize;
    debug!("[framing] frame header: len={}", len);
    if len > buf.len() {
        error!("[framing] frame too large: len={} buf={}", len, buf.len());
        return Err(XrceError::BufferTooSmall);
    }
    read_exact(reader, &mut buf[..len]).await?;
    let show = len.min(8);
    debug!("[framing] rx {} bytes head={=[u8]}", len, &buf[..show]);
    Ok(&buf[..len])
}

async fn write_all<W: Write>(w: &mut W, mut buf: &[u8]) -> Result<(), XrceError> {
    while !buf.is_empty() {
        let n = w.write(buf).await.map_err(|_| XrceError::Io)?;
        if n == 0 {
            return Err(XrceError::Disconnected);
        }
        buf = &buf[n..];
    }
    Ok(())
}

async fn read_exact<R: Read>(r: &mut R, mut buf: &mut [u8]) -> Result<(), XrceError> {
    while !buf.is_empty() {
        let n = r.read(buf).await.map_err(|_| XrceError::Io)?;
        if n == 0 {
            return Err(XrceError::Disconnected);
        }
        buf = &mut buf[n..];
    }
    Ok(())
}
