//! [`Publisher<M>`] — typed publisher handle.

use core::marker::PhantomData;

use crate::{
    cdr::CdrWriter,
    error::Error,
    message::Message,
    rt::{
        encode::{finalize_write_data_headers, msg_header_len},
        inner::{Frame, FRAME_BUF_SIZE},
        Context,
    },
};

/// Typed publisher handle. `Copy` — pass by value to any Embassy task.
///
/// Obtain from [`crate::Node::create_publisher`].  Call [`Publisher::publish`]
/// to serialize a message and enqueue it for transmission.
pub struct Publisher<M: Message> {
    pub(crate) dw_id: u16,
    pub(crate) ctx: Context,
    _phantom: PhantomData<fn() -> M>,
}

impl<M: Message> Publisher<M> {
    pub(crate) fn new(dw_id: u16, ctx: Context) -> Self {
        Self {
            dw_id,
            ctx,
            _phantom: PhantomData,
        }
    }

    /// Serialize `msg` and enqueue it for transmission.
    ///
    /// Returns immediately after the frame enters the TX queue — the Executor
    /// task handles the actual socket write.  Returns
    /// [`Error::Disconnected`] if the transport is known to be down.
    pub async fn publish(&self, msg: &M) -> Result<(), Error> {
        let inner = self.ctx.inner();
        if inner.is_disconnected() {
            return Err(Error::Disconnected);
        }

        let session_id = inner.session_id();
        // Prefix = session header + WRITE_DATA submsg header + BaseObjectRequest.
        let prefix = msg_header_len(session_id) + 4 + 4;
        let mut frame = Frame::zero();

        let body_len = {
            let mut w = CdrWriter::new(&mut frame.bytes[prefix..]);
            msg.serialize(&mut w);
            w.bytes_written()
        };
        let total = prefix + body_len;
        if total > FRAME_BUF_SIZE {
            return Err(Error::BufferTooSmall);
        }

        let seq = inner.next_seq();
        finalize_write_data_headers(
            &mut frame.bytes[..total],
            session_id,
            seq,
            &inner.client_key(),
            self.dw_id,
        );
        frame.len = total;
        inner.tx_channel.send(frame).await;
        Ok(())
    }
}

// Manual Clone/Copy: PhantomData<fn() -> M> is always Copy regardless of M.
impl<M: Message> Clone for Publisher<M> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<M: Message> Copy for Publisher<M> {}
