// XRCE-DDS wire protocol constants — eProsima Micro-XRCE-DDS-Client values.
//
// These match the agent the project ships with (`microros/micro-ros-agent:jazzy`),
// which speaks the format defined by eProsima's C client, *not* the OMG DDS-XRCE
// spec PDF. When the spec and the implementation disagree, the implementation
// wins because it is what the agent parses.
//
// Authoritative sources:
//   src/c/core/session/submessage_internal.h
//   src/c/core/serialization/xrce_header_internal.h
//   src/c/core/type/xrce_types.h
//
// Reference (project): /tenshi-no-hana/.claude/xrce_dds_protocol.md

// ── Submessage IDs (submessage_internal.h: SubmessageId enum) ────────────────

pub const SUBMSG_CREATE_CLIENT: u8 = 0;
pub const SUBMSG_CREATE: u8 = 1;
pub const SUBMSG_GET_INFO: u8 = 2;
pub const SUBMSG_DELETE: u8 = 3;
pub const SUBMSG_STATUS_AGENT: u8 = 4;
pub const SUBMSG_STATUS: u8 = 5;
pub const SUBMSG_INFO: u8 = 6;
pub const SUBMSG_WRITE_DATA: u8 = 7;
pub const SUBMSG_READ_DATA: u8 = 8;
pub const SUBMSG_DATA: u8 = 9;
pub const SUBMSG_ACKNACK: u8 = 10;
pub const SUBMSG_HEARTBEAT: u8 = 11;
pub const SUBMSG_RESET: u8 = 12;
pub const SUBMSG_FRAGMENT: u8 = 13;

// ── Submessage flags (submessage_internal.h: SubmessageFlags) ────────────────

pub const FLAG_LE: u8 = 0x01; // Little-endian payload (FLAG_ENDIANNESS)
pub const FLAG_LAST_FRAGMENT: u8 = 0x02;
pub const FLAG_REUSE: u8 = 0x02; // CreationMode bit (overlaps LAST_FRAGMENT, context-dependent)
pub const FLAG_REPLACE: u8 = 0x04;
/// Idempotent CREATE: reuse if exists, replace if mismatched.
pub const FLAGS_CREATE: u8 = FLAG_LE | FLAG_REUSE | FLAG_REPLACE;
/// FORMAT_DATA = 0 — used as the WRITE_DATA flags before OR-ing endianness.
pub const FORMAT_DATA: u8 = 0x00;

// ── Session / stream IDs ─────────────────────────────────────────────────────

/// Threshold from xrce_header_internal.h. session_id values < 0x80 carry a
/// 4-byte client_key in the message header; values >= 0x80 do not.
pub const SESSION_ID_WITHOUT_CLIENT_KEY: u8 = 0x80;

pub const STREAM_NONE: u8 = 0x00;
/// First best-effort stream (range 0x01–0x7F).
pub const STREAM_BEST_EFFORT: u8 = 0x01;
/// First reliable stream (range 0x80–0xFF) — not used by this crate yet.
pub const STREAM_RELIABLE: u8 = 0x80;

// ── Object kinds (object_id.h: UXR_*_ID and xrce_types.h: DDS_XRCE_OBJK_*) ───

pub const ENTITY_INVALID: u8 = 0x00;
pub const ENTITY_PARTICIPANT: u8 = 0x01;
pub const ENTITY_TOPIC: u8 = 0x02;
pub const ENTITY_PUBLISHER: u8 = 0x03;
pub const ENTITY_SUBSCRIBER: u8 = 0x04;
pub const ENTITY_DATAWRITER: u8 = 0x05;
pub const ENTITY_DATAREADER: u8 = 0x06;
pub const ENTITY_REQUESTER: u8 = 0x07;
pub const ENTITY_REPLIER: u8 = 0x08;
pub const ENTITY_OTHER: u8 = 0x0F;

// ── Representation format (xrce_types.h: DDS_XRCE_REPRESENTATION_*) ──────────

pub const REPR_BY_REFERENCE: u8 = 0x01;
pub const REPR_AS_XML: u8 = 0x02;
pub const REPR_IN_BINARY: u8 = 0x03;

// ── Result codes (xrce_types.h: ResultStatus) ───────────────────────────────

pub const STATUS_OK: u8 = 0x00;
pub const STATUS_OK_MATCHED: u8 = 0x01;
pub const STATUS_ERR_DDS_ERROR: u8 = 0x80;
pub const STATUS_ERR_MISMATCH: u8 = 0x81;
pub const STATUS_ERR_ALREADY_EXISTS: u8 = 0x82;
pub const STATUS_ERR_DENIED: u8 = 0x83;
pub const STATUS_ERR_UNKNOWN_REFERENCE: u8 = 0x84;
pub const STATUS_ERR_INVALID_DATA: u8 = 0x85;
pub const STATUS_ERR_INCOMPATIBLE: u8 = 0x86;
pub const STATUS_ERR_RESOURCES: u8 = 0x87;

// ── XRCE magic identifiers (xrce_types.h) ────────────────────────────────────

pub const XRCE_COOKIE: [u8; 4] = [0x58, 0x52, 0x43, 0x45]; // "XRCE"
pub const XRCE_VERSION: [u8; 2] = [0x01, 0x00]; // major=1, minor=0
pub const VENDOR_ID_EPROSIMA: [u8; 2] = [0x01, 0x0F];

// ── ObjectId / RequestId helpers ────────────────────────────────────────────

/// Pack an ObjectId into the 16-bit form `(idx << 4) | kind` that the agent
/// expects. The result is meant to be written as **big-endian 2 bytes** (raw,
/// not CDR-aligned) — see [`object_id_be_bytes`].
#[inline]
pub const fn object_id(idx: u16, kind: u8) -> u16 {
    (idx << 4) | (kind as u16 & 0x0F)
}

/// Big-endian 2-byte encoding of an ObjectId, as the agent serializes it
/// (`ucdr_serialize_array_uint8_t` of `data[2]`).
#[inline]
pub const fn object_id_be_bytes(packed: u16) -> [u8; 2] {
    [(packed >> 8) as u8, packed as u8]
}

/// Big-endian 2-byte encoding of a RequestId (same convention as ObjectId).
#[inline]
pub const fn request_id_be_bytes(req: u16) -> [u8; 2] {
    [(req >> 8) as u8, req as u8]
}
