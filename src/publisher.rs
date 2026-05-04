//! [`Publisher<M>`] — typed publisher handle.

use core::marker::PhantomData;

use crate::message::Message;

/// Typed publisher handle. Pass to [`crate::Session::publish`] together with a
/// matching `M` value.
///
/// The type parameter ensures a `Publisher<Float32>` cannot be used to send a
/// `Range` — mismatches are caught at compile time.
pub struct Publisher<M: Message> {
    pub(crate) dw_id: u16,
    _phantom: PhantomData<fn() -> M>,
}

impl<M: Message> Publisher<M> {
    pub(crate) const fn new(dw_id: u16) -> Self {
        Self {
            dw_id,
            _phantom: PhantomData,
        }
    }
}

// Manual Clone/Copy: PhantomData<fn() -> M> is always Copy regardless of M.
impl<M: Message> Clone for Publisher<M> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<M: Message> Copy for Publisher<M> {}
