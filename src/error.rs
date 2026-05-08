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
    /// A CREATE_* request did not receive a STATUS reply within the timeout.
    Timeout,
    /// A `Context` method was called before `Runtime::start()` completed.
    NotStarted,
    /// Service-call response did not arrive in time.
    ServiceCallTimeout,
    /// Service slot inbox overflowed (too many pending requests on a server).
    ServiceOverflow,
    /// A new request arrived but the service-server inbox is at capacity.
    NoServiceSlot,
    /// `ActionClient::send_goal` got `accepted = false` from the action server.
    GoalRejected,
    /// `GoalHandle::await_result` finished with a non-`STATUS_SUCCEEDED`
    /// status code (carried as the wrapped value).
    GoalNotSucceeded(i8),
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
            Error::Timeout => defmt::write!(f, "Timeout"),
            Error::NotStarted => defmt::write!(f, "NotStarted"),
            Error::ServiceCallTimeout => defmt::write!(f, "ServiceCallTimeout"),
            Error::ServiceOverflow => defmt::write!(f, "ServiceOverflow"),
            Error::NoServiceSlot => defmt::write!(f, "NoServiceSlot"),
            Error::GoalRejected => defmt::write!(f, "GoalRejected"),
            Error::GoalNotSucceeded(s) => defmt::write!(f, "GoalNotSucceeded({})", s),
        }
    }
}
