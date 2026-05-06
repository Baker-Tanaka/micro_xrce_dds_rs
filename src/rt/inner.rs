//! [`SessionInner`] — the single `'static` state shared by all tasks.
//!
//! Stored inside [`super::Runtime`].  Tasks hold a `Context = &'static
//! SessionInner` and call `&self` methods; the `Executor` (Phase 3) is the
//! only writer of the TCP socket.
//!
//! Design constraints:
//! - All fields must be const-initializable → [`SessionInner::new`] is `const fn`.
//! - `SessionInner: Sync` so it can live in a `static`.
//! - No heap; all state is fixed-size.

use embassy_sync::blocking_mutex::raw::CriticalSectionRawMutex;
use embassy_sync::channel::Channel;
use embassy_sync::mutex::Mutex;
use embassy_sync::signal::Signal;
use heapless::Vec as HVec;
use portable_atomic::{AtomicBool, AtomicU16, AtomicU32, AtomicU8, Ordering};

use crate::{error::Error, subscription::SubscriptionSlot};

// ── Constants ────────────────────────────────────────────────────────────────

/// Maximum number of concurrent subscriptions in the dispatch table.
pub const MAX_SUBS: usize = 8;

/// TX queue depth: at most this many outgoing frames can be buffered waiting
/// for the Executor to drain them.  2 is sufficient for a typical publish +
/// one in-flight CREATE.
pub const TX_QUEUE_DEPTH: usize = 2;

/// Byte capacity of one outgoing frame.
///
/// Must fit the largest CREATE frame (topic XML ~200 B + headers ~30 B)
/// and the largest WRITE_DATA frame (sensor_msgs/Imu body ~325 B + headers
/// 12 B).  512 gives comfortable headroom and is identical to the legacy
/// `TX_BUF_SIZE` in session.rs.
pub const FRAME_BUF_SIZE: usize = 512;

// ── Frame ────────────────────────────────────────────────────────────────────

/// One length-prefixed XRCE-DDS outgoing frame waiting in the TX queue.
///
/// Produced by `Publisher::publish` and CREATE helpers; consumed by the
/// Executor task which writes `bytes[..len]` to the TCP socket.
pub struct Frame {
    pub bytes: [u8; FRAME_BUF_SIZE],
    pub len: usize,
}

impl Frame {
    pub const fn zero() -> Self {
        Self {
            bytes: [0u8; FRAME_BUF_SIZE],
            len: 0,
        }
    }
}

// embassy_sync::channel::Channel<_, Frame, _> needs Frame: Send.
// Frame only contains bytes and a usize, so this is safe.
unsafe impl Send for Frame {}

// ── SessionInner ─────────────────────────────────────────────────────────────

/// Shared `'static` state for one XRCE-DDS session.
///
/// Placed inside [`super::Runtime`] which is declared `static` by the user.
/// All tasks share a `Context = &'static SessionInner`.
///
/// Thread-safety invariants:
/// - Atomic fields are read/written directly with appropriate `Ordering`.
/// - `creation_lock` serializes all CREATE_* request-response exchanges.
/// - `subs` is protected by an async `Mutex`; locked briefly for push
///   (on create_subscription) and for dispatch (on each incoming DATA frame).
/// - `tx_channel` is an MPMC channel from embassy-sync; multiple tasks
///   can call `.sender().send()` concurrently.
/// - `disconnected` is a one-way flag: once set it is never cleared.
pub struct SessionInner {
    // ── Session identity (written once by Runtime::start, then read-only) ──
    pub(crate) session_id: AtomicU8,
    /// client_key packed as LE u32; reconstruct with `.to_le_bytes()`.
    pub(crate) client_key: AtomicU32,

    // ── Sequence / request-id counters ───────────────────────────────────────
    pub(crate) seq: AtomicU16,
    pub(crate) req_id: AtomicU16,

    // ── Entity-index allocators (monotonically increasing) ───────────────────
    pub(crate) next_participant: AtomicU16,
    pub(crate) next_topic: AtomicU16,
    pub(crate) next_publisher: AtomicU16,
    pub(crate) next_subscriber: AtomicU16,
    pub(crate) next_dw: AtomicU16,
    pub(crate) next_dr: AtomicU16,

    // ── CREATE_* mailbox (one in-flight CREATE at a time) ────────────────────
    /// Async mutex that serializes CREATE_* exchanges.  A task holds this
    /// lock for the full round-trip (send + wait for STATUS).
    pub(crate) creation_lock: Mutex<CriticalSectionRawMutex, ()>,
    /// `req_id` of the currently pending CREATE, or 0 if none.
    pub(crate) creation_pending_req: AtomicU16,
    /// Executor signals this after parsing the STATUS reply.
    pub(crate) creation_signal: Signal<CriticalSectionRawMutex, Result<(), Error>>,

    // ── TX queue (producers: user tasks; consumer: Executor) ─────────────────
    /// Outgoing frame queue.  Multiple tasks write via `.sender().send()`;
    /// the Executor reads via `.receiver().receive()`.
    pub(crate) tx_channel: Channel<CriticalSectionRawMutex, Frame, TX_QUEUE_DEPTH>,

    // ── Subscription dispatch table ───────────────────────────────────────────
    /// Slot registry.  Locked briefly on `create_subscription` (push) and on
    /// each incoming DATA frame (linear scan).
    pub(crate) subs: Mutex<CriticalSectionRawMutex, HVec<&'static dyn SubscriptionSlot, MAX_SUBS>>,

    // ── Disconnect flag + supervisor signal ───────────────────────────────────
    pub(crate) disconnected: AtomicBool,
    /// Fired by the Executor on transport failure.  A single supervisor task
    /// can `await` this via [`super::Runtime::wait_for_disconnect`] to drive
    /// a reconnect (v0.5).
    pub(crate) disconnect_signal: Signal<CriticalSectionRawMutex, ()>,
}

impl SessionInner {
    /// Const constructor — place inside a `static` via [`super::Runtime::new`].
    pub const fn new() -> Self {
        Self {
            session_id: AtomicU8::new(0),
            client_key: AtomicU32::new(0),
            seq: AtomicU16::new(0),
            req_id: AtomicU16::new(1),
            next_participant: AtomicU16::new(1),
            next_topic: AtomicU16::new(1),
            next_publisher: AtomicU16::new(1),
            next_subscriber: AtomicU16::new(1),
            next_dw: AtomicU16::new(1),
            next_dr: AtomicU16::new(1),
            creation_lock: Mutex::new(()),
            creation_pending_req: AtomicU16::new(0),
            creation_signal: Signal::new(),
            tx_channel: Channel::new(),
            subs: Mutex::new(HVec::new()),
            disconnected: AtomicBool::new(false),
            disconnect_signal: Signal::new(),
        }
    }

    // ── Identity helpers ──────────────────────────────────────────────────────

    pub(crate) fn session_id(&self) -> u8 {
        self.session_id.load(Ordering::Acquire)
    }

    pub(crate) fn client_key(&self) -> [u8; 4] {
        self.client_key.load(Ordering::Acquire).to_le_bytes()
    }

    pub(crate) fn set_session_identity(&self, session_id: u8, key: [u8; 4]) {
        self.client_key
            .store(u32::from_le_bytes(key), Ordering::Release);
        self.session_id.store(session_id, Ordering::Release);
    }

    // ── Counter helpers ───────────────────────────────────────────────────────

    /// Fetch-and-increment message sequence number (wraps at u16::MAX → 0).
    pub(crate) fn next_seq(&self) -> u16 {
        self.seq.fetch_add(1, Ordering::Relaxed)
    }

    /// Fetch-and-increment request ID, skipping 0 (reserved).
    pub(crate) fn next_req(&self) -> u16 {
        loop {
            let prev = self.req_id.fetch_add(1, Ordering::Relaxed);
            // After wrapping, fetch_add(1) would yield 0 next call; skip it.
            if prev != 0 {
                return prev;
            }
        }
    }

    pub(crate) fn alloc_participant(&self) -> u16 {
        self.next_participant.fetch_add(1, Ordering::Relaxed)
    }

    pub(crate) fn alloc_topic(&self) -> u16 {
        self.next_topic.fetch_add(1, Ordering::Relaxed)
    }

    pub(crate) fn alloc_publisher(&self) -> u16 {
        self.next_publisher.fetch_add(1, Ordering::Relaxed)
    }

    pub(crate) fn alloc_subscriber(&self) -> u16 {
        self.next_subscriber.fetch_add(1, Ordering::Relaxed)
    }

    pub(crate) fn alloc_dw(&self) -> u16 {
        self.next_dw.fetch_add(1, Ordering::Relaxed)
    }

    pub(crate) fn alloc_dr(&self) -> u16 {
        self.next_dr.fetch_add(1, Ordering::Relaxed)
    }

    // ── Disconnect ────────────────────────────────────────────────────────────

    pub fn is_disconnected(&self) -> bool {
        self.disconnected.load(Ordering::Acquire)
    }

    pub(crate) fn set_disconnected(&self) {
        self.disconnected.store(true, Ordering::Release);
        self.disconnect_signal.signal(());
    }

    /// Clear the disconnect flag.  Used by [`super::Runtime::resume`] after a
    /// successful re-handshake.  Re-arms the disconnect signal so a future
    /// transport failure can be detected.
    pub(crate) fn clear_disconnected(&self) {
        self.disconnect_signal.reset();
        self.disconnected.store(false, Ordering::Release);
    }

    // ── Creation mailbox helpers ──────────────────────────────────────────────

    /// Store the req_id we're waiting a STATUS reply for, and reset the signal.
    ///
    /// # Safety contract
    /// Must be called with `creation_lock` held (enforced by `send_create_and_wait`
    /// in the creation module — Phase 3).
    pub(crate) fn arm_creation(&self, req_id: u16) {
        self.creation_signal.reset();
        self.creation_pending_req.store(req_id, Ordering::Release);
    }

    /// Disarm after the STATUS reply (or timeout/error).
    pub(crate) fn disarm_creation(&self) {
        self.creation_pending_req.store(0, Ordering::Release);
    }

    /// Called by the Executor when it parses a STATUS submessage.
    /// If `req_id` matches the pending request, signal the waiting task.
    pub(crate) fn deliver_status(&self, req_id: u16, result: Result<(), Error>) {
        if req_id == self.creation_pending_req.load(Ordering::Acquire) && req_id != 0 {
            self.creation_signal.signal(result);
        }
    }
}
