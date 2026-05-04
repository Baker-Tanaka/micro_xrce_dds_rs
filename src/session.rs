//! XRCE-DDS session over a length-prefixed TCP stream.
//!
//! Wire format follows eProsima Micro-XRCE-DDS-Client (the format spoken by
//! `microros/micro-ros-agent`). See `/tenshi-no-hana/.claude/xrce_dds_protocol.md`
//! for a byte-for-byte reference.
//!
//! Reliability: this crate currently uses BEST_EFFORT (stream 0x01) only.
//! Entity creation waits for STATUS by request_id; WRITE_DATA is fire-and-forget.

use crate::{error::XrceError, framing, protocol::*};
use embedded_io_async::{Read, Write};

#[cfg(feature = "defmt")]
use defmt::{debug, error};
#[cfg(not(feature = "defmt"))]
macro_rules! debug { ($($t:tt)*) => {}; }
#[cfg(not(feature = "defmt"))]
macro_rules! error { ($($t:tt)*) => {}; }

/// An XRCE-DDS entity ID, in its packed `(idx << 4) | kind` form.
#[derive(Clone, Copy)]
pub struct ObjectId(pub u16);

/// Opaque DataWriter handle — pass to [`XrceSession::write_data`].
#[derive(Clone, Copy)]
pub struct DataWriterId(pub u16);

/// Established XRCE-DDS session over a TCP connection to the micro-ROS Agent.
pub struct XrceSession<T: Read + Write> {
    transport: T,
    session_id: u8,
    client_key: [u8; 4],
    seq: u16,
    req_id: u16,
    tx_buf: [u8; 512],
    rx_buf: [u8; 128],
}

/// Build a CREATE_CLIENT message into `buf`. Returns bytes written (without the
/// 2-byte TCP framing prefix; pass the slice to [`framing::write_framed`]).
///
/// Useful for diagnostic flows that want to send CREATE_CLIENT manually before
/// constructing a full [`XrceSession`].
pub fn create_client_msg(buf: &mut [u8], session_id: u8, client_key: &[u8; 4]) -> usize {
    build_create_client(buf, session_id, client_key, DEFAULT_MTU)
}

const DEFAULT_MTU: u16 = 512;

impl<T: Read + Write> XrceSession<T> {
    /// Reuse an already-handshaken transport. Caller is responsible for having
    /// completed the CREATE_CLIENT/STATUS_AGENT exchange.
    pub fn from_connected(transport: T, session_id: u8, client_key: [u8; 4]) -> Self {
        Self {
            transport,
            session_id,
            client_key,
            seq: 0,
            req_id: 1,
            tx_buf: [0u8; 512],
            rx_buf: [0u8; 128],
        }
    }

    /// Establish a session: CREATE_CLIENT → STATUS_AGENT.
    /// `session_id` should typically be in `0x81..=0xFE` so the message header
    /// stays at 4 bytes (no client_key tail).
    pub async fn connect(
        mut transport: T,
        session_id: u8,
        client_key: [u8; 4],
    ) -> Result<Self, XrceError> {
        let mut tx = [0u8; 64];
        let mut rx = [0u8; 128];

        let n = build_create_client(&mut tx, session_id, &client_key, DEFAULT_MTU);
        debug!("[session] sending CREATE_CLIENT ({} bytes)", n);
        framing::write_framed(&mut transport, &tx[..n]).await?;

        let reply = framing::read_framed(&mut transport, &mut rx).await?;
        debug!("[session] STATUS_AGENT raw ({} bytes)", reply.len());
        parse_status_agent(reply, session_id)?;

        Ok(Self {
            transport,
            session_id,
            client_key,
            seq: 0,
            req_id: 1,
            tx_buf: [0u8; 512],
            rx_buf: [0u8; 128],
        })
    }

    // ── DDS entity creation ────────────────────────────────────────────────

    /// Create a DDS DomainParticipant with the given node name in DDS XML form.
    ///
    /// `participant_idx` is a small client-chosen integer that becomes part of
    /// the ObjectId — typically `1` for a single-node firmware.
    pub async fn create_participant(
        &mut self,
        participant_idx: u16,
        node_name: &str,
    ) -> Result<ObjectId, XrceError> {
        let oid = object_id(participant_idx, ENTITY_PARTICIPANT);
        let mut xml_buf = [0u8; 192];
        let xml = fmt_participant_xml(&mut xml_buf, node_name);
        let req = self.next_req();
        let seq = self.next_seq();
        let session_id = self.session_id;
        let key = self.client_key;
        let n = encode_create_participant(
            &mut self.tx_buf,
            session_id,
            seq,
            &key,
            req,
            oid,
            xml,
            0, // domain_id 0 (matches `ROS_DOMAIN_ID=0`, the agent default)
        )?;
        framing::write_framed(&mut self.transport, &self.tx_buf[..n]).await?;
        self.wait_status(req).await?;
        Ok(ObjectId(oid))
    }

    /// Create a DDS Topic referencing a previously-created Participant.
    pub async fn create_topic(
        &mut self,
        topic_idx: u16,
        participant_idx: u16,
        dds_name: &str,
        type_name: &str,
    ) -> Result<ObjectId, XrceError> {
        let oid = object_id(topic_idx, ENTITY_TOPIC);
        let parent = object_id(participant_idx, ENTITY_PARTICIPANT);
        let mut xml_buf = [0u8; 256];
        let xml = fmt_topic_xml(&mut xml_buf, dds_name, type_name);
        let req = self.next_req();
        let seq = self.next_seq();
        let session_id = self.session_id;
        let key = self.client_key;
        let n = encode_create_with_parent(
            &mut self.tx_buf,
            session_id,
            seq,
            &key,
            req,
            oid,
            ENTITY_TOPIC,
            xml,
            parent,
        )?;
        framing::write_framed(&mut self.transport, &self.tx_buf[..n]).await?;
        self.wait_status(req).await?;
        Ok(ObjectId(oid))
    }

    /// Create a DDS Publisher under the given Participant.
    pub async fn create_publisher(
        &mut self,
        publisher_idx: u16,
        participant_idx: u16,
    ) -> Result<ObjectId, XrceError> {
        let oid = object_id(publisher_idx, ENTITY_PUBLISHER);
        let parent = object_id(participant_idx, ENTITY_PARTICIPANT);
        let xml = "<dds><publisher><name>MyPublisher</name></publisher></dds>";
        let req = self.next_req();
        let seq = self.next_seq();
        let session_id = self.session_id;
        let key = self.client_key;
        let n = encode_create_with_parent(
            &mut self.tx_buf,
            session_id,
            seq,
            &key,
            req,
            oid,
            ENTITY_PUBLISHER,
            xml,
            parent,
        )?;
        framing::write_framed(&mut self.transport, &self.tx_buf[..n]).await?;
        self.wait_status(req).await?;
        Ok(ObjectId(oid))
    }

    /// Create a DDS DataWriter under the given Publisher, bound to a Topic.
    /// `dds_name` and `type_name` must match the Topic.
    pub async fn create_datawriter(
        &mut self,
        datawriter_idx: u16,
        publisher_idx: u16,
        dds_name: &str,
        type_name: &str,
    ) -> Result<DataWriterId, XrceError> {
        let oid = object_id(datawriter_idx, ENTITY_DATAWRITER);
        let parent = object_id(publisher_idx, ENTITY_PUBLISHER);
        let mut xml_buf = [0u8; 320];
        let xml = fmt_datawriter_xml(&mut xml_buf, dds_name, type_name);
        let req = self.next_req();
        let seq = self.next_seq();
        let session_id = self.session_id;
        let key = self.client_key;
        let n = encode_create_with_parent(
            &mut self.tx_buf,
            session_id,
            seq,
            &key,
            req,
            oid,
            ENTITY_DATAWRITER,
            xml,
            parent,
        )?;
        framing::write_framed(&mut self.transport, &self.tx_buf[..n]).await?;
        self.wait_status(req).await?;
        Ok(DataWriterId(oid))
    }

    // ── Data publishing ────────────────────────────────────────────────────

    /// Publish a CDR-serialized message on the given DataWriter.
    /// BEST_EFFORT — no STATUS reply.
    pub async fn write_data(
        &mut self,
        dw: DataWriterId,
        cdr_payload: &[u8],
    ) -> Result<(), XrceError> {
        let seq = self.next_seq();
        let session_id = self.session_id;
        let key = self.client_key;
        let n = encode_write_data(
            &mut self.tx_buf,
            session_id,
            seq,
            &key,
            dw.0,
            cdr_payload,
        )?;
        framing::write_framed(&mut self.transport, &self.tx_buf[..n]).await
    }

    // ── internal ───────────────────────────────────────────────────────────

    async fn wait_status(&mut self, expected_req: u16) -> Result<(), XrceError> {
        let msg = framing::read_framed(&mut self.transport, &mut self.rx_buf).await?;
        parse_status(msg, self.session_id, expected_req)
    }

    fn next_req(&mut self) -> u16 {
        let r = self.req_id;
        self.req_id = self.req_id.wrapping_add(1).max(1);
        r
    }

    fn next_seq(&mut self) -> u16 {
        let s = self.seq;
        self.seq = self.seq.wrapping_add(1);
        s
    }
}

// ── Encoding helpers ────────────────────────────────────────────────────────

/// CREATE_CLIENT message. Layout: [msg header][submsg header][CLIENT_Representation].
fn build_create_client(buf: &mut [u8], session_id: u8, client_key: &[u8; 4], mtu: u16) -> usize {
    let mut b = MsgWriter::new(buf);

    // ── Message header ─────────────────────────────────────────────────────
    // CREATE_CLIENT-specific rule: header session_id = info->id & 0x80,
    // and key is included only when the masked value is < 0x80.
    let masked_sid = session_id & SESSION_ID_WITHOUT_CLIENT_KEY;
    b.u8(masked_sid);
    b.u8(STREAM_NONE);
    b.u16_raw(0); // seq_num
    if masked_sid < SESSION_ID_WITHOUT_CLIENT_KEY {
        b.bytes(client_key);
    }

    // ── Submessage header (length patched) ─────────────────────────────────
    b.align_buf(4);
    let hdr_off = b.pos();
    b.u8(SUBMSG_CREATE_CLIENT);
    b.u8(FLAG_LE);
    b.u16_raw(0); // payload length (patched below)
    let payload_origin = b.pos();

    // ── CLIENT_Representation (CDR origin = payload start) ─────────────────
    b.bytes(&XRCE_COOKIE);
    b.bytes(&XRCE_VERSION);
    b.bytes(&VENDOR_ID_EPROSIMA);
    b.bytes(client_key); // raw bytes, no alignment
    b.u8(session_id); // payload session_id is the *real* id, not masked
    b.u8(0); // optional_properties = false
    // mtu is uint16 → 2-byte aligned in CDR. Current pos = origin+14 → already 2-aligned.
    b.cdr_u16(mtu, payload_origin);

    let payload_len = b.pos() - payload_origin;
    b.patch_u16_at(hdr_off + 2, payload_len as u16);
    b.pos()
}

/// CREATE_PARTICIPANT message.
#[allow(clippy::too_many_arguments)]
fn encode_create_participant(
    buf: &mut [u8],
    session_id: u8,
    seq: u16,
    client_key: &[u8; 4],
    req_id: u16,
    obj_id: u16,
    xml: &str,
    domain_id: i16,
) -> Result<usize, XrceError> {
    let mut b = MsgWriter::new(buf);
    write_session_header(&mut b, session_id, STREAM_BEST_EFFORT, seq, client_key);

    b.align_buf(4);
    let hdr_off = b.pos();
    b.u8(SUBMSG_CREATE);
    b.u8(FLAGS_CREATE);
    b.u16_raw(0);
    let payload_origin = b.pos();

    // BaseObjectRequest (4 bytes raw)
    b.bytes(&request_id_be_bytes(req_id));
    b.bytes(&object_id_be_bytes(obj_id));
    // ObjectVariant kind byte
    b.u8(ENTITY_PARTICIPANT);
    // Representation3_Base.format
    b.u8(REPR_AS_XML);
    // CDR string (length is uint32 → align to 4 from CDR origin)
    b.cdr_string(xml, payload_origin)?;
    // domain_id is int16 → 2-aligned in CDR stream
    b.cdr_i16(domain_id, payload_origin);

    let payload_len = b.pos() - payload_origin;
    b.patch_u16_at(hdr_off + 2, payload_len as u16);
    if b.overflow() {
        return Err(XrceError::BufferTooSmall);
    }
    Ok(b.pos())
}

/// CREATE message for entities that take a parent ObjectId trailer
/// (Topic → Participant, Publisher → Participant, DataWriter → Publisher).
#[allow(clippy::too_many_arguments)]
fn encode_create_with_parent(
    buf: &mut [u8],
    session_id: u8,
    seq: u16,
    client_key: &[u8; 4],
    req_id: u16,
    obj_id: u16,
    obj_kind: u8,
    xml: &str,
    parent_obj_id: u16,
) -> Result<usize, XrceError> {
    let mut b = MsgWriter::new(buf);
    write_session_header(&mut b, session_id, STREAM_BEST_EFFORT, seq, client_key);

    b.align_buf(4);
    let hdr_off = b.pos();
    b.u8(SUBMSG_CREATE);
    b.u8(FLAGS_CREATE);
    b.u16_raw(0);
    let payload_origin = b.pos();

    b.bytes(&request_id_be_bytes(req_id));
    b.bytes(&object_id_be_bytes(obj_id));
    b.u8(obj_kind);
    b.u8(REPR_AS_XML);
    b.cdr_string(xml, payload_origin)?;
    // ObjectId trailer is 2 raw bytes (no CDR alignment).
    b.bytes(&object_id_be_bytes(parent_obj_id));

    let payload_len = b.pos() - payload_origin;
    b.patch_u16_at(hdr_off + 2, payload_len as u16);
    if b.overflow() {
        return Err(XrceError::BufferTooSmall);
    }
    Ok(b.pos())
}

/// WRITE_DATA message (FORMAT_DATA, fire-and-forget).
fn encode_write_data(
    buf: &mut [u8],
    session_id: u8,
    seq: u16,
    client_key: &[u8; 4],
    dw_id: u16,
    cdr_payload: &[u8],
) -> Result<usize, XrceError> {
    let mut b = MsgWriter::new(buf);
    write_session_header(&mut b, session_id, STREAM_BEST_EFFORT, seq, client_key);

    b.align_buf(4);
    let hdr_off = b.pos();
    b.u8(SUBMSG_WRITE_DATA);
    b.u8(FLAG_LE | FORMAT_DATA);
    b.u16_raw(0);
    let payload_origin = b.pos();

    // BaseObjectRequest. request_id unused for BEST_EFFORT writes.
    b.bytes(&request_id_be_bytes(0));
    b.bytes(&object_id_be_bytes(dw_id));
    // CDR-encapsulated user data appended raw.
    b.bytes(cdr_payload);

    let payload_len = b.pos() - payload_origin;
    b.patch_u16_at(hdr_off + 2, payload_len as u16);
    if b.overflow() {
        return Err(XrceError::BufferTooSmall);
    }
    Ok(b.pos())
}

/// Standard message header used for everything except CREATE_CLIENT.
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

// ── Reply parsers ───────────────────────────────────────────────────────────

/// STATUS_AGENT layout (eProsima):
///   msg_hdr (4 or 8 bytes) + submsg_hdr (4) + ResultStatus (2) + AGENT_Representation (≥9).
fn parse_status_agent(msg: &[u8], session_id: u8) -> Result<(), XrceError> {
    let hdr = expected_header_len(session_id);
    if msg.len() < hdr + 4 + 2 {
        error!("[session] STATUS_AGENT too short: {}", msg.len());
        return Err(XrceError::UnexpectedReply);
    }
    let submsg_id = msg[hdr];
    if submsg_id != SUBMSG_STATUS_AGENT {
        error!(
            "[session] expected STATUS_AGENT (0x04), got 0x{:02X}",
            submsg_id
        );
        return Err(XrceError::UnexpectedReply);
    }
    let payload = &msg[hdr + 4..];
    let status = payload[0];
    debug!("[session] STATUS_AGENT result.status=0x{:02X}", status);
    if status != STATUS_OK {
        return Err(XrceError::AgentRejected(status));
    }
    Ok(())
}

/// STATUS layout (response to CREATE/DELETE):
///   msg_hdr + submsg_hdr (4) + BaseObjectReply
///     related_request: req_id (2 raw) + obj_id (2 raw)
///     result:          status (1) + impl_status (1)
fn parse_status(msg: &[u8], session_id: u8, expected_req: u16) -> Result<(), XrceError> {
    let hdr = expected_header_len(session_id);
    if msg.len() < hdr + 4 + 6 {
        return Err(XrceError::UnexpectedReply);
    }
    let submsg_id = msg[hdr];
    if submsg_id != SUBMSG_STATUS {
        error!(
            "[session] expected STATUS (0x05), got 0x{:02X}",
            submsg_id
        );
        return Err(XrceError::UnexpectedReply);
    }
    let payload = &msg[hdr + 4..];
    let got_req = u16::from_be_bytes([payload[0], payload[1]]);
    if got_req != expected_req {
        error!(
            "[session] STATUS req mismatch: got {} expected {}",
            got_req, expected_req
        );
        return Err(XrceError::StatusReqMismatch);
    }
    let status = payload[4];
    if status != STATUS_OK && status != STATUS_OK_MATCHED {
        return Err(XrceError::AgentRejected(status));
    }
    Ok(())
}

#[inline]
fn expected_header_len(session_id: u8) -> usize {
    if session_id < SESSION_ID_WITHOUT_CLIENT_KEY {
        8
    } else {
        4
    }
}

// ── XML builders (no_alloc) ─────────────────────────────────────────────────

fn fmt_participant_xml<'b>(buf: &'b mut [u8], name: &str) -> &'b str {
    let mut w = StrWriter::new(buf);
    w.s("<dds><participant><rtps><name>");
    w.s(name);
    w.s("</name></rtps></participant></dds>");
    w.finish()
}

fn fmt_topic_xml<'b>(buf: &'b mut [u8], dds_name: &str, type_name: &str) -> &'b str {
    let mut w = StrWriter::new(buf);
    w.s("<dds><topic><name>");
    w.s(dds_name);
    w.s("</name><dataType>");
    w.s(type_name);
    w.s("</dataType></topic></dds>");
    w.finish()
}

fn fmt_datawriter_xml<'b>(buf: &'b mut [u8], dds_name: &str, type_name: &str) -> &'b str {
    let mut w = StrWriter::new(buf);
    w.s("<dds><data_writer><topic><kind>NO_KEY</kind><name>");
    w.s(dds_name);
    w.s("</name><dataType>");
    w.s(type_name);
    w.s("</dataType></topic></data_writer></dds>");
    w.finish()
}

struct StrWriter<'a> {
    buf: &'a mut [u8],
    pos: usize,
}

impl<'a> StrWriter<'a> {
    fn new(buf: &'a mut [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn s(&mut self, s: &str) {
        let bytes = s.as_bytes();
        let space = self.buf.len().saturating_sub(self.pos);
        let n = bytes.len().min(space);
        self.buf[self.pos..self.pos + n].copy_from_slice(&bytes[..n]);
        self.pos += bytes.len();
    }

    fn finish(self) -> &'a str {
        let end = self.pos.min(self.buf.len());
        core::str::from_utf8(&self.buf[..end]).unwrap_or("")
    }
}

// ── Low-level message byte builder ──────────────────────────────────────────

/// Writes XRCE messages with two distinct alignment domains:
///   - Buffer-relative `align_buf(n)`: used for submessage header placement
///     (always 4-byte aligned in the message buffer).
///   - CDR-stream `cdr_*(value, origin)`: used inside a submessage payload,
///     where the origin is the byte offset of the payload start.
struct MsgWriter<'a> {
    buf: &'a mut [u8],
    pos: usize,
    overflow: bool,
}

impl<'a> MsgWriter<'a> {
    fn new(buf: &'a mut [u8]) -> Self {
        Self {
            buf,
            pos: 0,
            overflow: false,
        }
    }

    fn pos(&self) -> usize {
        self.pos
    }

    fn overflow(&self) -> bool {
        self.overflow || self.pos > self.buf.len()
    }

    fn u8(&mut self, v: u8) {
        if self.pos < self.buf.len() {
            self.buf[self.pos] = v;
        } else {
            self.overflow = true;
        }
        self.pos += 1;
    }

    /// Write a u16 LE without aligning (for raw-byte fields like seq_num that
    /// happen to fall on a 2-aligned offset in their parent layout).
    fn u16_raw(&mut self, v: u16) {
        self.bytes(&v.to_le_bytes());
    }

    fn bytes(&mut self, data: &[u8]) {
        let space = self.buf.len().saturating_sub(self.pos);
        if data.len() > space {
            self.overflow = true;
        }
        let n = data.len().min(space);
        self.buf[self.pos..self.pos + n].copy_from_slice(&data[..n]);
        self.pos += data.len();
    }

    /// Pad to a buffer-relative `n`-byte boundary.
    fn align_buf(&mut self, n: usize) {
        let rem = self.pos % n;
        if rem != 0 {
            for _ in 0..(n - rem) {
                self.u8(0);
            }
        }
    }

    /// Pad so that `(pos - origin) % n == 0`, then write `n` zero/value bytes.
    fn cdr_align(&mut self, origin: usize, n: usize) {
        let rem = (self.pos - origin) % n;
        if rem != 0 {
            for _ in 0..(n - rem) {
                self.u8(0);
            }
        }
    }

    fn cdr_u16(&mut self, v: u16, origin: usize) {
        self.cdr_align(origin, 2);
        self.bytes(&v.to_le_bytes());
    }

    fn cdr_i16(&mut self, v: i16, origin: usize) {
        self.cdr_align(origin, 2);
        self.bytes(&v.to_le_bytes());
    }

    fn cdr_u32(&mut self, v: u32, origin: usize) {
        self.cdr_align(origin, 4);
        self.bytes(&v.to_le_bytes());
    }

    /// CDR string: u32 length (incl. null terminator) + bytes + '\0'.
    fn cdr_string(&mut self, s: &str, origin: usize) -> Result<(), XrceError> {
        let len_with_null = s.len() as u32 + 1;
        self.cdr_u32(len_with_null, origin);
        self.bytes(s.as_bytes());
        self.u8(0);
        if self.overflow() {
            return Err(XrceError::BufferTooSmall);
        }
        Ok(())
    }

    fn patch_u16_at(&mut self, offset: usize, value: u16) {
        if offset + 2 <= self.buf.len() {
            let b = value.to_le_bytes();
            self.buf[offset] = b[0];
            self.buf[offset + 1] = b[1];
        } else {
            self.overflow = true;
        }
    }
}
