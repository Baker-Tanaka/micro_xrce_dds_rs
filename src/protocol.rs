// XRCE-DDS v1.0 submessage identifiers (eProsima Micro XRCE-DDS Client values).
// Verify against Wireshark capture of real micro-ROS Agent traffic if behaviour
// is unexpected — the OMG spec table numbering and the eProsima implementation
// both assign 0x0D to WRITE_DATA, which is what matters for interop.
pub const SUBMSG_CREATE_CLIENT: u8 = 0x00;
pub const SUBMSG_DELETE_CLIENT: u8 = 0x01;
pub const SUBMSG_STATUS_AGENT: u8 = 0x02;
pub const SUBMSG_CREATE: u8 = 0x03;
pub const SUBMSG_DELETE: u8 = 0x05;
pub const SUBMSG_STATUS: u8 = 0x06;
pub const SUBMSG_WRITE_DATA: u8 = 0x0D;
pub const SUBMSG_DATA: u8 = 0x0F;

// Session IDs: 0x00 = NULL (no session), 0x81–0xFE = with 4-byte client key.
pub const SESSION_NULL: u8 = 0x00;

// Stream IDs: 0x00 = no stream, 0x01–0x7F = best-effort.
pub const STREAM_NONE: u8 = 0x00;
pub const STREAM_BEST_EFFORT: u8 = 0x01;

// Entity type nibble packed into the lower 4 bits of ObjectId.
pub const ENTITY_PARTICIPANT: u8 = 0x01;
pub const ENTITY_TOPIC: u8 = 0x02;
pub const ENTITY_PUBLISHER: u8 = 0x03;
pub const ENTITY_DATAWRITER: u8 = 0x04;
pub const ENTITY_SUBSCRIBER: u8 = 0x05;
pub const ENTITY_DATAREADER: u8 = 0x06;

// Submessage flags.
pub const FLAG_LE: u8 = 0x01;     // Little-endian payload
pub const FLAG_REUSE: u8 = 0x02;  // Reuse existing object if it exists
pub const FLAG_REPLACE: u8 = 0x04; // Replace existing object
pub const FLAGS_CREATE: u8 = FLAG_LE | FLAG_REUSE | FLAG_REPLACE;

// Object representation format inside CREATE payload.
// 0x01 = XML string (DDS-XRCE spec Section 8.7.3; confirmed by eProsima source).
pub const REPR_XML: u8 = 0x01;

// Result status codes in STATUS / STATUS_AGENT replies.
pub const STATUS_OK: u8 = 0x00;

// XRCE magic cookie ("XRCE") sent in CREATE_CLIENT and echoed in STATUS_AGENT.
pub const XRCE_COOKIE: [u8; 4] = [0x58, 0x52, 0x43, 0x45];
pub const XRCE_VERSION: [u8; 2] = [0x01, 0x00]; // major=1, minor=0
pub const VENDOR_ID: [u8; 2] = [0x01, 0x0F];    // eProsima vendor ID

/// Encode an XRCE ObjectId: upper 12 bits = entity index, lower 4 bits = entity type.
#[inline]
pub const fn object_id(idx: u16, entity_type: u8) -> u16 {
    (idx << 4) | (entity_type as u16)
}
