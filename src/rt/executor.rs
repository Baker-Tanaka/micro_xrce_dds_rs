//! [`Executor`] — the single Embassy task that owns the TCP socket.
//!
//! Bridges the TX queue (outgoing frames from user tasks via [`crate::Publisher`])
//! and the TCP receive path (incoming DATA → subscriptions, STATUS → creation
//! mailbox). Spawned once by the application after [`super::Runtime::start`]
//! completes the initial CREATE_CLIENT handshake.

use embassy_futures::select::{select, Either};
use embedded_io_async::{Read, Write};

use crate::{
    error::Error,
    framing,
    protocol::*,
    rt::{encode::msg_header_len, inner::SessionInner},
};

#[cfg(feature = "defmt")]
use defmt::{debug, error, warn};
#[cfg(not(feature = "defmt"))]
macro_rules! debug {
    ($($t:tt)*) => {};
}
#[cfg(not(feature = "defmt"))]
macro_rules! error {
    ($($t:tt)*) => {};
}
#[cfg(not(feature = "defmt"))]
macro_rules! warn {
    ($($t:tt)*) => {};
}

// Must fit the largest incoming DATA frame (sensor_msgs/Imu body ~325 B +
// headers).  512 matches FRAME_BUF_SIZE so the same stack slot covers both.
const RX_BUF_SIZE: usize = 512;

/// Executor task state.  Produced by [`super::Runtime::start`] and moved into
/// an [`embassy_executor::task`] by the application.
///
/// ```ignore
/// #[embassy_executor::task]
/// async fn xrce_executor(exec: Executor<TcpSocket<'static>>) -> ! {
///     exec.run().await
/// }
///
/// // In main:
/// let (ctx, exec) = RUNTIME.start(socket, config).await.unwrap();
/// spawner.spawn(xrce_executor(exec)).unwrap();
/// ```
pub struct Executor<T> {
    inner: &'static SessionInner,
    transport: T,
    rx_buf: [u8; RX_BUF_SIZE],
}

impl<T: Read + Write> Executor<T> {
    pub(crate) fn new(inner: &'static SessionInner, transport: T) -> Self {
        Self {
            inner,
            transport,
            rx_buf: [0u8; RX_BUF_SIZE],
        }
    }

    /// Run the executor loop forever.
    ///
    /// Multiplexes outgoing frames from the TX queue and incoming frames from
    /// the agent. On transport error: sets the disconnect flag, signals any
    /// pending CREATE waiter with `Error::Disconnected`, then parks permanently
    /// (v0.2 — reconnect is v0.5).
    pub async fn run(mut self) -> ! {
        loop {
            // Scope the mutable borrows of self.transport and self.rx_buf so
            // the borrow checker sees them released before the match arms run.
            let action = {
                select(
                    self.inner.tx_channel.receive(),
                    read_one_frame(&mut self.transport, &mut self.rx_buf),
                )
                .await
            };

            match action {
                Either::First(frame) => {
                    if framing::write_framed(&mut self.transport, &frame.bytes[..frame.len])
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                Either::Second(Ok(len)) => {
                    dispatch_frame(&self.rx_buf[..len], self.inner).await;
                }
                Either::Second(Err(e)) => {
                    error!("[executor] rx error: {} — terminating loop", e);
                    break;
                }
            }
        }

        self.inner.set_disconnected();
        self.inner.creation_signal.signal(Err(Error::Disconnected));
        core::future::pending::<()>().await;
        unreachable!()
    }
}

// ── Frame dispatch ────────────────────────────────────────────────────────────

/// Dispatch every submessage in one received XRCE-DDS message.
///
/// One TCP frame can carry multiple submessages back-to-back (the agent
/// batches when several samples are ready at once).  Each submessage header
/// sits at a 4-byte aligned offset within the message — after consuming a
/// submessage, advance to the next 4-byte boundary before reading the next
/// header.  Stopping at the first submessage silently drops every following
/// sample in a batched frame.
async fn dispatch_frame(msg: &[u8], inner: &'static SessionInner) {
    let session_id = inner.session_id();
    let hdr_len = msg_header_len(session_id);
    let mut pos = hdr_len;

    while pos + 4 <= msg.len() {
        let submsg_id = msg[pos];
        let submsg_len = u16::from_le_bytes([msg[pos + 2], msg[pos + 3]]) as usize;
        let payload_start = pos + 4;
        let payload_end = (payload_start + submsg_len).min(msg.len());
        let payload = &msg[payload_start..payload_end];

        match submsg_id {
            SUBMSG_DATA => dispatch_data(payload, inner).await,
            SUBMSG_STATUS => {
                if let Some((req_id, result)) = parse_status(payload) {
                    inner.deliver_status(req_id, result);
                }
            }
            SUBMSG_STATUS_AGENT => {
                debug!("[executor] stray STATUS_AGENT ignored");
            }
            _ => {
                debug!("[executor] ignoring submsg 0x{:02X}", submsg_id);
            }
        }

        // Submessage headers are 4-byte aligned within the message buffer.
        pos = payload_end;
        let rem = pos % 4;
        if rem != 0 {
            pos += 4 - rem;
        }
    }
}

async fn dispatch_data(payload: &[u8], inner: &'static SessionInner) {
    if payload.len() < 4 {
        debug!("[executor] DATA payload too short ({})", payload.len());
        return;
    }
    // BaseObjectReply: req_id[2 BE] + obj_id[2 BE]
    let dr_oid = u16::from_be_bytes([payload[2], payload[3]]);
    let user_data = &payload[4..];
    debug!(
        "[executor] DATA dr_oid=0x{:04X} body_len={}",
        dr_oid,
        user_data.len()
    );
    let subs = inner.subs.lock().await;
    for slot in subs.iter() {
        if slot.dr_id() == dr_oid {
            if slot.try_deliver(user_data).is_err() {
                warn!(
                    "[executor] DATA dropped (overflow or decode error) dr_oid=0x{:04X}",
                    dr_oid
                );
            }
            return;
        }
    }
    debug!("[executor] DATA for unknown dr_oid=0x{:04X}", dr_oid);
}

fn parse_status(payload: &[u8]) -> Option<(u16, Result<(), Error>)> {
    if payload.len() < 5 {
        return None;
    }
    let req_id = u16::from_be_bytes([payload[0], payload[1]]);
    let status = payload[4];
    let result = match status {
        STATUS_OK => Ok(()),
        STATUS_OK_MATCHED => {
            warn!(
                "[executor] STATUS_OK_MATCHED — stale entity reused; restart agent if subsequent CREATEs fail"
            );
            Ok(())
        }
        _ => Err(Error::AgentRejected(status)),
    };
    Some((req_id, result))
}

// ── Transport helpers ─────────────────────────────────────────────────────────

async fn read_one_frame<T: Read>(transport: &mut T, rx_buf: &mut [u8]) -> Result<usize, Error> {
    let mut len_buf = [0u8; 2];
    read_exact(transport, &mut len_buf).await?;
    let len = u16::from_le_bytes(len_buf) as usize;
    if len > rx_buf.len() {
        return Err(Error::BufferTooSmall);
    }
    read_exact(transport, &mut rx_buf[..len]).await?;
    Ok(len)
}

async fn read_exact<R: Read>(r: &mut R, mut buf: &mut [u8]) -> Result<(), Error> {
    while !buf.is_empty() {
        match r.read(buf).await {
            Err(_) => return Err(Error::Io),
            Ok(0) => return Err(Error::Disconnected),
            Ok(n) => buf = &mut buf[n..],
        }
    }
    Ok(())
}
