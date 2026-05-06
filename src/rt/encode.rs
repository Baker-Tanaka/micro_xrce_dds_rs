//! Wire-level encoders for XRCE-DDS frames.
//!
//! All functions here are non-generic — they operate on raw byte slices and
//! protocol constants.  Generic code (e.g. CDR serialization of `M: Message`)
//! calls these to write the fixed headers, which is where ROM savings happen
//! when multiple `Publisher<M>` types share the same non-generic code path.

use heapless::String as HString;

use crate::{error::Error, protocol::*};

#[cfg(feature = "defmt")]
use defmt::error;
#[cfg(not(feature = "defmt"))]
macro_rules! error { ($($t:tt)*) => {}; }

// ── Public constants ──────────────────────────────────────────────────────────

/// Maximum byte length of a ROS2 topic name (after `rt/` prefix expansion)
/// stored in the XML entity description.
pub const TOPIC_NAME_MAX: usize = 96;

// ── Header length helper ──────────────────────────────────────────────────────

/// Byte length of the XRCE-DDS message header for a given `session_id`.
///
/// Sessions with `session_id >= SESSION_ID_WITHOUT_CLIENT_KEY (0x80)` omit the
/// 4-byte `client_key` from the header.
#[inline]
pub fn msg_header_len(session_id: u8) -> usize {
    if session_id < SESSION_ID_WITHOUT_CLIENT_KEY {
        8
    } else {
        4
    }
}

// ── Topic name helper ─────────────────────────────────────────────────────────

/// Convert a ROS2 topic name (`/foo/bar`) to its DDS form (`rt/foo/bar`).
pub fn ros2_topic_name<const N: usize>(topic: &str) -> Result<HString<N>, Error> {
    let mut s = HString::<N>::new();
    let body = topic.strip_prefix('/').unwrap_or(topic);
    s.push_str("rt/").map_err(|_| Error::BufferTooSmall)?;
    s.push_str(body).map_err(|_| Error::BufferTooSmall)?;
    Ok(s)
}

// ── CREATE_CLIENT ─────────────────────────────────────────────────────────────

/// Build a `CREATE_CLIENT` message into `buf`. Returns the number of bytes written.
pub fn build_create_client(
    buf: &mut [u8],
    session_id: u8,
    client_key: &[u8; 4],
    mtu: u16,
) -> usize {
    let mut b = MsgWriter::new(buf);

    let masked_sid = session_id & SESSION_ID_WITHOUT_CLIENT_KEY;
    b.u8(masked_sid);
    b.u8(STREAM_NONE);
    b.u16_raw(0);
    if masked_sid < SESSION_ID_WITHOUT_CLIENT_KEY {
        b.bytes(client_key);
    }

    b.align_buf(4);
    let hdr_off = b.pos();
    b.u8(SUBMSG_CREATE_CLIENT);
    b.u8(FLAG_LE);
    b.u16_raw(0);
    let payload_origin = b.pos();

    b.bytes(&XRCE_COOKIE);
    b.bytes(&XRCE_VERSION);
    b.bytes(&VENDOR_ID_EPROSIMA);
    b.bytes(client_key);
    b.u8(session_id);
    b.u8(0); // optional_properties = false
    b.cdr_u16(mtu, payload_origin);

    let payload_len = b.pos() - payload_origin;
    b.patch_u16_at(hdr_off + 2, payload_len as u16);
    b.pos()
}

// ── CREATE_PARTICIPANT ────────────────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
pub fn encode_create_participant(
    buf: &mut [u8],
    session_id: u8,
    seq: u16,
    client_key: &[u8; 4],
    req_id: u16,
    obj_id: u16,
    xml: &str,
    domain_id: i16,
) -> Result<usize, Error> {
    let mut b = MsgWriter::new(buf);
    write_session_header(&mut b, session_id, STREAM_BEST_EFFORT, seq, client_key);

    b.align_buf(4);
    let hdr_off = b.pos();
    b.u8(SUBMSG_CREATE);
    b.u8(FLAGS_CREATE);
    b.u16_raw(0);
    let origin = b.pos();

    b.bytes(&request_id_be_bytes(req_id));
    b.bytes(&object_id_be_bytes(obj_id));
    b.u8(ENTITY_PARTICIPANT);
    b.u8(REPR_AS_XML);
    b.cdr_string(xml, origin)?;
    b.cdr_i16(domain_id, origin);

    let payload_len = b.pos() - origin;
    b.patch_u16_at(hdr_off + 2, payload_len as u16);
    if b.overflow() {
        return Err(Error::BufferTooSmall);
    }
    Ok(b.pos())
}

// ── CREATE with parent (Publisher / Subscriber / Topic / DataWriter / DataReader) ──

#[allow(clippy::too_many_arguments)]
pub fn encode_create_with_parent(
    buf: &mut [u8],
    session_id: u8,
    seq: u16,
    client_key: &[u8; 4],
    req_id: u16,
    obj_id: u16,
    obj_kind: u8,
    xml: &str,
    parent_obj_id: u16,
) -> Result<usize, Error> {
    let mut b = MsgWriter::new(buf);
    write_session_header(&mut b, session_id, STREAM_BEST_EFFORT, seq, client_key);

    b.align_buf(4);
    let hdr_off = b.pos();
    b.u8(SUBMSG_CREATE);
    b.u8(FLAGS_CREATE);
    b.u16_raw(0);
    let origin = b.pos();

    b.bytes(&request_id_be_bytes(req_id));
    b.bytes(&object_id_be_bytes(obj_id));
    b.u8(obj_kind);
    b.u8(REPR_AS_XML);
    b.cdr_string(xml, origin)?;
    b.bytes(&object_id_be_bytes(parent_obj_id));

    let payload_len = b.pos() - origin;
    b.patch_u16_at(hdr_off + 2, payload_len as u16);
    if b.overflow() {
        return Err(Error::BufferTooSmall);
    }
    Ok(b.pos())
}

// ── READ_DATA ─────────────────────────────────────────────────────────────────

pub fn encode_read_data(
    buf: &mut [u8],
    session_id: u8,
    seq: u16,
    client_key: &[u8; 4],
    req_id: u16,
    dr_oid: u16,
) -> Result<usize, Error> {
    let mut b = MsgWriter::new(buf);
    write_session_header(&mut b, session_id, STREAM_BEST_EFFORT, seq, client_key);

    b.align_buf(4);
    let hdr_off = b.pos();
    b.u8(SUBMSG_READ_DATA);
    b.u8(FLAG_LE);
    b.u16_raw(0);
    let origin = b.pos();

    b.bytes(&request_id_be_bytes(req_id));
    b.bytes(&object_id_be_bytes(dr_oid));
    b.u8(STREAM_BEST_EFFORT);
    b.u8(FORMAT_DATA);
    b.u8(0); // optional_content_filter_expression = false
    b.u8(0); // optional_delivery_control = false (continuous)

    let payload_len = b.pos() - origin;
    b.patch_u16_at(hdr_off + 2, payload_len as u16);
    if b.overflow() {
        return Err(Error::BufferTooSmall);
    }
    Ok(b.pos())
}

// ── WRITE_DATA header finalizer ───────────────────────────────────────────────

/// Fill in the WRITE_DATA session and submessage headers for a buffer whose
/// CDR body is already present starting at byte `prefix`.
///
/// `buf` must span the full frame `[0..prefix + body_len]`; the caller writes
/// the CDR body first, then passes the whole slice here.
pub fn finalize_write_data_headers(
    buf: &mut [u8],
    session_id: u8,
    seq: u16,
    client_key: &[u8; 4],
    dw_oid: u16,
) {
    let total = buf.len();
    let mut b = MsgWriter::new(buf);
    write_session_header(&mut b, session_id, STREAM_BEST_EFFORT, seq, client_key);

    let hdr_off = b.pos();
    b.u8(SUBMSG_WRITE_DATA);
    b.u8(FLAG_LE | FORMAT_DATA);
    let payload_len = (total - hdr_off - 4) as u16;
    b.u16_raw(payload_len);

    b.bytes(&request_id_be_bytes(0)); // req_id = 0: WRITE_DATA is fire-and-forget
    b.bytes(&object_id_be_bytes(dw_oid));
    // CDR body already in buf[b.pos()..].
}

// ── Service WRITE_DATA finalizer ──────────────────────────────────────────────

/// Identical wire format to [`finalize_write_data_headers`] — service requests
/// and replies are sent as ordinary WRITE_DATA submessages addressed to the
/// requester / replier object_id.  This alias exists for documentation.
#[inline]
pub fn finalize_service_write_data(
    buf: &mut [u8],
    session_id: u8,
    seq: u16,
    client_key: &[u8; 4],
    requester_or_replier_oid: u16,
) {
    finalize_write_data_headers(buf, session_id, seq, client_key, requester_or_replier_oid);
}

// ── STATUS_AGENT parser ───────────────────────────────────────────────────────

/// Parse the STATUS_AGENT reply to CREATE_CLIENT.
///
/// Returns `Ok(())` on success, `Err(UnexpectedReply)` if the message is
/// malformed or not a STATUS_AGENT, `Err(AgentRejected(status))` if the
/// agent replied with an error status.
pub(crate) fn parse_status_agent(msg: &[u8], session_id: u8) -> Result<(), Error> {
    let hdr_len = msg_header_len(session_id);
    if msg.len() < hdr_len + 4 + 2 {
        error!("[rt] STATUS_AGENT too short: {}", msg.len());
        return Err(Error::UnexpectedReply);
    }
    let submsg_id = msg[hdr_len];
    if submsg_id != SUBMSG_STATUS_AGENT {
        error!("[rt] expected STATUS_AGENT (4), got 0x{:02X}", submsg_id);
        return Err(Error::UnexpectedReply);
    }
    let payload = &msg[hdr_len + 4..];
    let status = payload[0];
    if status != STATUS_OK {
        return Err(Error::AgentRejected(status));
    }
    Ok(())
}

// ── Internal: session header ──────────────────────────────────────────────────

fn write_session_header(
    b: &mut MsgWriter,
    session_id: u8,
    stream_id: u8,
    seq: u16,
    client_key: &[u8; 4],
) {
    b.u8(session_id);
    b.u8(stream_id);
    b.u16_raw(seq);
    if session_id < SESSION_ID_WITHOUT_CLIENT_KEY {
        b.bytes(client_key);
    }
}

// ── MsgWriter ─────────────────────────────────────────────────────────────────

/// Cursor-style buffer writer for building XRCE-DDS wire frames.
///
/// Tracks overflow instead of panicking — callers check [`MsgWriter::overflow`]
/// at the end to detect buffer exhaustion.
pub(crate) struct MsgWriter<'a> {
    buf: &'a mut [u8],
    pos: usize,
    overflow: bool,
}

impl<'a> MsgWriter<'a> {
    pub(crate) fn new(buf: &'a mut [u8]) -> Self {
        Self {
            buf,
            pos: 0,
            overflow: false,
        }
    }

    pub(crate) fn pos(&self) -> usize {
        self.pos
    }

    pub(crate) fn overflow(&self) -> bool {
        self.overflow || self.pos > self.buf.len()
    }

    pub(crate) fn u8(&mut self, v: u8) {
        if self.pos < self.buf.len() {
            self.buf[self.pos] = v;
        } else {
            self.overflow = true;
        }
        self.pos += 1;
    }

    pub(crate) fn u16_raw(&mut self, v: u16) {
        self.bytes(&v.to_le_bytes());
    }

    pub(crate) fn bytes(&mut self, data: &[u8]) {
        let space = self.buf.len().saturating_sub(self.pos);
        if data.len() > space {
            self.overflow = true;
        }
        let n = data.len().min(space);
        self.buf[self.pos..self.pos + n].copy_from_slice(&data[..n]);
        self.pos += data.len();
    }

    pub(crate) fn align_buf(&mut self, n: usize) {
        let rem = self.pos % n;
        if rem != 0 {
            for _ in 0..(n - rem) {
                self.u8(0);
            }
        }
    }

    fn cdr_align(&mut self, origin: usize, n: usize) {
        let rem = (self.pos - origin) % n;
        if rem != 0 {
            for _ in 0..(n - rem) {
                self.u8(0);
            }
        }
    }

    pub(crate) fn cdr_u16(&mut self, v: u16, origin: usize) {
        self.cdr_align(origin, 2);
        self.bytes(&v.to_le_bytes());
    }

    pub(crate) fn cdr_i16(&mut self, v: i16, origin: usize) {
        self.cdr_align(origin, 2);
        self.bytes(&v.to_le_bytes());
    }

    fn cdr_u32(&mut self, v: u32, origin: usize) {
        self.cdr_align(origin, 4);
        self.bytes(&v.to_le_bytes());
    }

    pub(crate) fn cdr_string(&mut self, s: &str, origin: usize) -> Result<(), Error> {
        let len_with_null = s.len() as u32 + 1;
        self.cdr_u32(len_with_null, origin);
        self.bytes(s.as_bytes());
        self.u8(0); // null terminator
        if self.overflow() {
            return Err(Error::BufferTooSmall);
        }
        Ok(())
    }

    pub(crate) fn patch_u16_at(&mut self, offset: usize, value: u16) {
        if offset + 2 <= self.buf.len() {
            let b = value.to_le_bytes();
            self.buf[offset] = b[0];
            self.buf[offset + 1] = b[1];
        } else {
            self.overflow = true;
        }
    }
}
