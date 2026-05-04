//! [`Message`] trait — DDS type name + (de)serialization routines for
//! ROS2 message types.

use crate::{cdr::CdrWriter, cdr_reader::CdrReader, error::Error};

/// A ROS2 message type that can be CDR-serialized for publish and
/// CDR-deserialized for subscribe.
///
/// All implementations emit only the **field bytes** — the 4-byte CDR
/// encapsulation header (`00 01 00 00`) is added by Fast-DDS when the agent
/// builds the SerializedPayload, and is consumed automatically by
/// [`CdrReader::new`] on the receive path.
pub trait Message: Sized {
    /// DDS type name, e.g. `"std_msgs::msg::dds_::Float32_"`. This is what
    /// goes into the `<dataType>...</dataType>` element of CREATE_TOPIC and
    /// CREATE_DATAWRITER XML.
    const TYPE_NAME: &'static str;

    /// Worst-case CDR-serialized body size in bytes (no encap header). Used
    /// by [`crate::Session`] to size internal scratch buffers and reject
    /// over-large messages early.
    const MAX_SERIALIZED_SIZE: usize;

    /// Serialize the message body into `w`.
    fn serialize(&self, w: &mut CdrWriter);

    /// Deserialize a message body from `r`. The reader is already positioned
    /// past the encapsulation header.
    fn deserialize(r: &mut CdrReader<'_>) -> Result<Self, Error>;
}
