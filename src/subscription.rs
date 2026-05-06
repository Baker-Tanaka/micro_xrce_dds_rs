//! [`Subscription<M, N>`] — typed subscription handle backed by an async
//! channel inbox.
//!
//! Subscriptions are registered with [`crate::Node::create_subscription`]
//! by passing a `&'static Subscription<M, N>`; the canonical pattern is to
//! declare the slot in a `static` via `static_cell::StaticCell` and call
//! [`Subscription::new()`] (a `const fn`) to initialize it.
//!
//! Once registered the user task awaits incoming messages with
//! [`Subscription::recv`]. The session's spin loop handles dispatch and
//! deserialization in the background.

use core::marker::PhantomData;

use embassy_sync::blocking_mutex::raw::CriticalSectionRawMutex;
use embassy_sync::channel::Channel;
use portable_atomic::{AtomicU16, Ordering};

use crate::{cdr_reader::CdrReader, error::Error, message::Message};

/// Async-receive subscription for messages of type `M`. `N` is the inbox depth
/// — older samples are kept; newer ones are dropped (returned as
/// [`Error::SubscriptionOverflow`] from the dispatch path) when the inbox is full.
pub struct Subscription<M: Message, const N: usize = 4>
where
    M: Send + 'static,
{
    /// DataReader object_id. Set during [`crate::Node::create_subscription`];
    /// `0` until then (and never matches an incoming DATA submessage).
    dr_id: AtomicU16,
    inbox: Channel<CriticalSectionRawMutex, M, N>,
    _phantom: PhantomData<fn() -> M>,
}

impl<M, const N: usize> Subscription<M, N>
where
    M: Message + Send + 'static,
{
    /// Construct an empty subscription slot. `const fn` so it can live in a
    /// `static`. The `dr_id` is filled in when
    /// [`crate::Node::create_subscription`] registers this slot.
    pub const fn new() -> Self {
        Self {
            dr_id: AtomicU16::new(0),
            inbox: Channel::new(),
            _phantom: PhantomData,
        }
    }

    /// Wait for the next sample. Yields until the dispatch loop delivers one.
    pub async fn recv(&self) -> M {
        self.inbox.receive().await
    }

    /// Non-blocking variant — returns `None` if no sample is queued.
    pub fn try_recv(&self) -> Option<M> {
        self.inbox.try_receive().ok()
    }

    pub fn set_dr_id(&self, id: u16) {
        self.dr_id.store(id, Ordering::Release);
    }
}

impl<M, const N: usize> Default for Subscription<M, N>
where
    M: Message + Send + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

/// Type-erased view of a subscription, for the session's dispatch table.
///
/// Implemented automatically for every `Subscription<M, N>`. The dispatch
/// loop matches incoming DATA submessages by `dr_id` and calls `try_deliver`
/// with the raw SerializedPayload bytes.
pub trait SubscriptionSlot: Sync {
    fn dr_id(&self) -> u16;
    fn try_deliver(&self, payload: &[u8]) -> Result<(), Error>;
}

impl<M, const N: usize> SubscriptionSlot for Subscription<M, N>
where
    M: Message + Send + 'static,
{
    fn dr_id(&self) -> u16 {
        self.dr_id.load(Ordering::Acquire)
    }

    fn try_deliver(&self, payload: &[u8]) -> Result<(), Error> {
        // The micro-ROS agent delivers raw CDR body bytes (no encap header).
        let mut r = CdrReader::from_body(payload);
        let msg = M::deserialize(&mut r)?;
        self.inbox
            .try_send(msg)
            .map_err(|_| Error::SubscriptionOverflow)
    }
}

/// Declare a `'static` subscription slot.
///
/// ```ignore
/// subscription_slot!(static TEMP_SUB: Float32, depth = 4);
/// // or with default depth (4):
/// subscription_slot!(static TEMP_SUB: Float32);
///
/// // Then in your node task:
/// node.create_subscription("/temperature", &TEMP_SUB).await?;
/// loop {
///     let msg = TEMP_SUB.recv().await;
/// }
/// ```
#[macro_export]
macro_rules! subscription_slot {
    (static $name:ident : $M:ty , depth = $N:expr) => {
        static $name: $crate::Subscription<$M, $N> = $crate::Subscription::new();
    };
    (static $name:ident : $M:ty) => {
        static $name: $crate::Subscription<$M, 4> = $crate::Subscription::new();
    };
}
