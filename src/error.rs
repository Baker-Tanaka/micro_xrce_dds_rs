#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum XrceError {
    Io,
    Disconnected,
    BufferTooSmall,
    AgentRejected(u8),
    UnexpectedReply,
    StatusReqMismatch,
}

#[cfg(feature = "defmt")]
impl defmt::Format for XrceError {
    fn format(&self, f: defmt::Formatter) {
        match self {
            XrceError::Io => defmt::write!(f, "Io"),
            XrceError::Disconnected => defmt::write!(f, "Disconnected"),
            XrceError::BufferTooSmall => defmt::write!(f, "BufferTooSmall"),
            XrceError::AgentRejected(s) => defmt::write!(f, "AgentRejected({})", s),
            XrceError::UnexpectedReply => defmt::write!(f, "UnexpectedReply"),
            XrceError::StatusReqMismatch => defmt::write!(f, "StatusReqMismatch"),
        }
    }
}
