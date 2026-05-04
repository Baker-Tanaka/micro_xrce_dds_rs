/// Errors produced by the SDK.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    /// Underlying transport I/O error.
    Io,
    /// TCP connection closed unexpectedly.
    Disconnected,
    /// A serialized message did not fit in the SDK-internal buffer.
    BufferTooSmall,
    /// Agent rejected an entity request with the given XRCE status code.
    AgentRejected(u8),
    /// Reply was not the expected submessage type.
    UnexpectedReply,
    /// STATUS reply's `request_id` did not match the request we just sent.
    StatusReqMismatch,
    /// CDR deserialization failed (truncated buffer, invalid encoding, etc.).
    Deserialization,
    /// A subscription's inbox was full and a sample was dropped.
    SubscriptionOverflow,
    /// Too many subscriptions registered for the dispatch table.
    TooManySubscriptions,
}

#[cfg(feature = "defmt")]
impl defmt::Format for Error {
    fn format(&self, f: defmt::Formatter) {
        match self {
            Error::Io => defmt::write!(f, "Io"),
            Error::Disconnected => defmt::write!(f, "Disconnected"),
            Error::BufferTooSmall => defmt::write!(f, "BufferTooSmall"),
            Error::AgentRejected(s) => defmt::write!(f, "AgentRejected(0x{:02X})", s),
            Error::UnexpectedReply => defmt::write!(f, "UnexpectedReply"),
            Error::StatusReqMismatch => defmt::write!(f, "StatusReqMismatch"),
            Error::Deserialization => defmt::write!(f, "Deserialization"),
            Error::SubscriptionOverflow => defmt::write!(f, "SubscriptionOverflow"),
            Error::TooManySubscriptions => defmt::write!(f, "TooManySubscriptions"),
        }
    }
}
