//! [`send_create_and_wait`] — serialized CREATE_* request/response helper.
//!
//! Holds `creation_lock` for the full round-trip so that at most one CREATE
//! is in-flight at any given time (the agent processes them serially anyway).

use crate::{
    error::Error,
    rt::inner::{Frame, SessionInner, FRAME_BUF_SIZE},
};

/// Send an XRCE-DDS CREATE_* request and wait for the STATUS reply.
///
/// `build_fn(session_id, client_key, seq, req_id, buf) -> Result<usize, Error>`
/// must encode the complete frame into `buf` (at most [`FRAME_BUF_SIZE`] bytes)
/// and return the number of bytes written.
///
/// Pitfall: `arm_creation` resets the signal **before** the frame is enqueued,
/// so a STATUS that arrives the instant the executor drains the queue cannot
/// be missed.
pub(crate) async fn send_create_and_wait<F>(
    inner: &'static SessionInner,
    build_fn: F,
) -> Result<(), Error>
where
    F: FnOnce(u8, [u8; 4], u16, u16, &mut [u8]) -> Result<usize, Error>,
{
    // Hold the lock for the entire round-trip.
    let _guard = inner.creation_lock.lock().await;

    let session_id = inner.session_id();
    if session_id == 0 {
        return Err(Error::NotStarted);
    }

    let client_key = inner.client_key();
    let seq = inner.next_seq();
    let req_id = inner.next_req();

    let mut frame = Frame::zero();
    let len = build_fn(session_id, client_key, seq, req_id, &mut frame.bytes)?;
    debug_assert!(len <= FRAME_BUF_SIZE);
    frame.len = len;

    // Arm before sending — see pitfall note in runtime_roadmap.md §Known Pitfalls 4.
    inner.arm_creation(req_id);

    // Fast-fail if the connection is already gone.
    if inner.is_disconnected() {
        inner.disarm_creation();
        return Err(Error::Disconnected);
    }

    // Enqueue the frame.  The Executor task drains the TX channel.
    inner.tx_channel.send(frame).await;

    // Wait for the Executor to deliver the STATUS reply.
    let result = inner.creation_signal.wait().await;

    inner.disarm_creation();
    result
}
