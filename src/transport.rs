//! Byte-stream transport abstraction used by the framework session engine.

use embedded_io_async::{Read, Write};

/// A reliable byte stream to the XRCE-DDS Agent (TCP framing is applied on
/// top of it).
///
/// `reset` must drop the connection immediately and leave the transport in a
/// state where it can be reconnected (for TCP: abort/RST, discard buffers).
/// Reads must be cancel-safe: a dropped `read` future must not lose bytes.
pub trait Transport: Read + Write {
    fn reset(&mut self);
}

impl<T: Transport + ?Sized> Transport for &mut T {
    fn reset(&mut self) {
        T::reset(self)
    }
}

#[cfg(feature = "embassy-net")]
impl Transport for embassy_net::tcp::TcpSocket<'_> {
    fn reset(&mut self) {
        self.abort();
    }
}
