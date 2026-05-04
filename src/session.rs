use embedded_io_async::{Read, Write};
use crate::{error::XrceError, framing, protocol::*};

#[cfg(feature = "defmt")]
use defmt::{debug, error};
#[cfg(not(feature = "defmt"))]
macro_rules! debug { ($($t:tt)*) => {}; }
#[cfg(not(feature = "defmt"))]
macro_rules! error { ($($t:tt)*) => {}; }

/// An XRCE-DDS entity ID as packed by the eProsima convention:
/// upper 12 bits = entity index, lower 4 bits = entity type.
#[derive(Clone, Copy)]
pub struct ObjectId(pub u16);

/// Opaque handle returned by `create_datawriter`.
#[derive(Clone, Copy)]
pub struct DataWriterId(pub u16);

/// Established XRCE-DDS session over a TCP connection to the micro-ROS Agent.
///
/// # Transport
/// Uses TCP framing: each XRCE message is prefixed by a 2-byte LE payload length.
///
/// # Reliability
/// All entity-creation operations use the BEST_EFFORT output stream (stream_id = 0x01)
/// but wait for a STATUS reply (matched by req_id). WRITE_DATA is fire-and-forget.
pub struct XrceSession<T: Read + Write> {
    transport: T,
    session_id: u8,
    client_key: [u8; 4],
    seq: u16,
    req_id: u16,
    tx_buf: [u8; 512],
    rx_buf: [u8; 128],
}

impl<T: Read + Write> XrceSession<T> {
    /// Establish an XRCE-DDS session with the micro-ROS Agent.
    ///
    /// Sends CREATE_CLIENT and waits for STATUS_AGENT.
    /// `session_id` should be in range 0x81–0xFE (with 4-byte client key).
    pub async fn connect(
        mut transport: T,
        session_id: u8,
        client_key: [u8; 4],
    ) -> Result<Self, XrceError> {
        let mut tx = [0u8; 64];
        let mut rx = [0u8; 128];

        let n = build_create_client(&mut tx, session_id);
        debug!("[session] sending CREATE_CLIENT ({} bytes)", n);
        framing::write_framed(&mut transport, &tx[..n]).await?;
        debug!("[session] CREATE_CLIENT sent, waiting for STATUS_AGENT");

        let reply = framing::read_framed(&mut transport, &mut rx).await?;
        let show = reply.len().min(16);
        debug!("[session] STATUS_AGENT raw ({} bytes): {=[u8]}", reply.len(), &reply[..show]);
        parse_status_agent(reply)?;

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

    // ── DDS entity creation ─────────────────────────────────────────────────

    /// Create a DDS Participant (the top-level DDS node).
    pub async fn create_participant(&mut self, name: &str) -> Result<ObjectId, XrceError> {
        let id = ObjectId(object_id(0x001, ENTITY_PARTICIPANT));
        let mut xml_buf = [0u8; 128];
        let xml = fmt_participant_xml(&mut xml_buf, name);
        self.create_entity(id.0, xml).await?;
        Ok(id)
    }

    /// Create a DDS Topic with the given ROS2-style DDS name and type name.
    ///
    /// `dds_name`: DDS topic name, e.g. `"rt/angel_nose/temperature"`.
    /// `type_name`: DDS type name, e.g. `"std_msgs::msg::dds_::Float32_"`.
    pub async fn create_topic(
        &mut self,
        topic_idx: u16,
        dds_name: &str,
        type_name: &str,
    ) -> Result<ObjectId, XrceError> {
        let id = ObjectId(object_id(topic_idx, ENTITY_TOPIC));
        let mut xml_buf = [0u8; 256];
        let xml = fmt_topic_xml(&mut xml_buf, dds_name, type_name);
        self.create_entity(id.0, xml).await?;
        Ok(id)
    }

    /// Create a DDS Publisher (groups DataWriters, one per Participant is enough).
    pub async fn create_publisher(
        &mut self,
        pub_idx: u16,
    ) -> Result<ObjectId, XrceError> {
        let id = ObjectId(object_id(pub_idx, ENTITY_PUBLISHER));
        self.create_entity(id.0, "<dds><publisher/></dds>").await?;
        Ok(id)
    }

    /// Create a DDS DataWriter that publishes on the given Topic.
    ///
    /// `dw_idx`: unique index for this DataWriter (1-based).
    /// `dds_name` / `type_name`: must match the Topic created earlier.
    pub async fn create_datawriter(
        &mut self,
        dw_idx: u16,
        dds_name: &str,
        type_name: &str,
    ) -> Result<DataWriterId, XrceError> {
        let id = object_id(dw_idx, ENTITY_DATAWRITER);
        let mut xml_buf = [0u8; 256];
        let xml = fmt_datawriter_xml(&mut xml_buf, dds_name, type_name);
        self.create_entity(id, xml).await?;
        Ok(DataWriterId(id))
    }

    // ── Data publishing ─────────────────────────────────────────────────────

    /// Publish a CDR-serialized message on the given DataWriter (BEST_EFFORT, no reply).
    pub async fn write_data(
        &mut self,
        dw: DataWriterId,
        cdr_payload: &[u8],
    ) -> Result<(), XrceError> {
        let n = self.encode_write_data(dw.0, cdr_payload)?;
        framing::write_framed(&mut self.transport, &self.tx_buf[..n]).await
    }

    // ── internal ────────────────────────────────────────────────────────────

    async fn create_entity(&mut self, obj_id: u16, xml: &str) -> Result<(), XrceError> {
        let req = self.next_req();
        let n = self.encode_create(req, obj_id, xml)?;
        framing::write_framed(&mut self.transport, &self.tx_buf[..n]).await?;
        self.wait_status(req).await
    }

    async fn wait_status(&mut self, expected_req: u16) -> Result<(), XrceError> {
        let msg = framing::read_framed(&mut self.transport, &mut self.rx_buf).await?;
        parse_status(msg, expected_req)
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

    /// Encode a CREATE submessage into `self.tx_buf`. Returns bytes written.
    fn encode_create(&mut self, req_id: u16, obj_id: u16, xml: &str) -> Result<usize, XrceError> {
        let seq = self.next_seq();
        let mut b = MsgBuilder::new(&mut self.tx_buf);

        // 8-byte message header (session with client key)
        b.u8(self.session_id);
        b.u8(STREAM_BEST_EFFORT);
        b.u16(seq);
        b.bytes(&self.client_key);

        // Submessage header (length patched below)
        let hdr_off = b.pos;
        b.u8(SUBMSG_CREATE);
        b.u8(FLAGS_CREATE);
        b.u16(0); // length TBD

        // CREATE payload
        b.u16(req_id);  // request_id
        b.u16(obj_id);  // object_id
        b.u8(REPR_XML); // representation_kind = XML

        // CDR string for the XML representation (u32 len + bytes + null)
        // Must be 4-byte aligned; current pos after REPR_XML:
        //   8 (msg hdr) + 4 (submsg hdr) + 2 + 2 + 1 = 17 → pad to 20
        b.align(4);
        b.u32((xml.len() as u32) + 1); // +1 for null
        b.bytes(xml.as_bytes());
        b.u8(0x00); // null terminator

        b.patch_len(hdr_off);
        Ok(b.pos)
    }

    /// Encode a WRITE_DATA submessage into `self.tx_buf`. Returns bytes written.
    fn encode_write_data(&mut self, dw_id: u16, cdr: &[u8]) -> Result<usize, XrceError> {
        if 8 + 4 + 4 + cdr.len() > self.tx_buf.len() {
            return Err(XrceError::BufferTooSmall);
        }
        let seq = self.next_seq();
        let mut b = MsgBuilder::new(&mut self.tx_buf);

        // 8-byte message header
        b.u8(self.session_id);
        b.u8(STREAM_BEST_EFFORT);
        b.u16(seq);
        b.bytes(&self.client_key);

        // Submessage header
        let hdr_off = b.pos;
        b.u8(SUBMSG_WRITE_DATA);
        b.u8(FLAG_LE); // LE, single DATA format
        b.u16(0);      // length TBD

        // WRITE_DATA payload: req_id=0, DataWriter object_id, then CDR bytes
        b.u16(0x0000); // req_id (unused for BEST_EFFORT)
        b.u16(dw_id);  // DataWriter object_id
        b.bytes(cdr);

        b.patch_len(hdr_off);
        Ok(b.pos)
    }
}

// ── Free-standing protocol helpers ──────────────────────────────────────────

/// Build a CREATE_CLIENT message into `buf`. Returns bytes written.
/// Uses NULL session header (4 bytes, no client key).
fn build_create_client(buf: &mut [u8], session_id: u8) -> usize {
    let mut b = MsgBuilder::new(buf);

    // 4-byte NULL session message header (session_id < 0x80 → no client key)
    b.u8(SESSION_NULL);
    b.u8(STREAM_NONE);
    b.u16(0); // sequence_nr

    // Submessage header
    let hdr_off = b.pos;
    b.u8(SUBMSG_CREATE_CLIENT);
    b.u8(FLAG_LE); // CREATE_CLIENT uses only LE flag; REUSE/REPLACE are for entity CREATE
    b.u16(0); // TBD

    // CREATE_CLIENT payload
    b.bytes(&XRCE_COOKIE);   // "XRCE"
    b.bytes(&XRCE_VERSION);  // [0x01, 0x00]
    b.bytes(&VENDOR_ID);     // [0x01, 0x0F] eProsima
    b.u32(0);                // client_timestamp.seconds
    b.u32(0);                // client_timestamp.nanoseconds
    b.u8(session_id);        // requested session_id
    b.align(4);              // CDR alignment: pad to 4-byte boundary before u32
    b.u32(0);                // properties sequence (size=0)

    b.patch_len(hdr_off);
    b.pos
}

/// Parse STATUS_AGENT reply; only checks result.status == OK.
///
/// STATUS_AGENT payload layout (DDS-XRCE spec):
///   [0..3]  xrce_cookie  (4 bytes)
///   [4..5]  xrce_version (2 bytes)
///   [6..7]  vendor_id    (2 bytes)
///   [8..15] agent_timestamp (8 bytes: sec u32 + nsec u32)
///   [16]    result.status
///   [17]    result.implementation_status
fn parse_status_agent(msg: &[u8]) -> Result<(), XrceError> {
    // 4-byte null-session header + 4-byte submsg header + 18-byte payload
    if msg.len() < 26 {
        error!("[session] STATUS_AGENT too short: {} bytes", msg.len());
        return Err(XrceError::UnexpectedReply);
    }
    // Skip message header (4 bytes for NULL session)
    let submsg = &msg[4..];
    debug!("[session] STATUS_AGENT submsg[0]=0x{:02X} (expect 0x{:02X})", submsg[0], SUBMSG_STATUS_AGENT);
    if submsg[0] != SUBMSG_STATUS_AGENT {
        error!("[session] unexpected submsg type 0x{:02X}", submsg[0]);
        return Err(XrceError::UnexpectedReply);
    }
    // Submessage header is 4 bytes; payload starts at [4]
    // result.status is at payload[16]: after cookie(4) + version(2) + vendor(2) + timestamp(8)
    let payload = &submsg[4..];
    let status = payload[16];
    debug!("[session] STATUS_AGENT result.status=0x{:02X} (expect STATUS_OK=0x00)", status);
    if status != STATUS_OK {
        error!("[session] agent rejected: status=0x{:02X}", status);
        return Err(XrceError::AgentRejected(status));
    }
    Ok(())
}

/// Parse STATUS reply to a CREATE submessage; checks req_id and result.status.
fn parse_status(msg: &[u8], expected_req: u16) -> Result<(), XrceError> {
    // Minimum: 8-byte header + 4-byte submsg header + 4-byte payload
    if msg.len() < 16 {
        return Err(XrceError::UnexpectedReply);
    }
    // Skip 8-byte message header
    let submsg = &msg[8..];
    if submsg[0] != SUBMSG_STATUS {
        return Err(XrceError::UnexpectedReply);
    }
    // Payload: [req_id_lo, req_id_hi, status, impl_status]
    let payload = &submsg[4..];
    let got_req = u16::from_le_bytes([payload[0], payload[1]]);
    if got_req != expected_req {
        return Err(XrceError::StatusReqMismatch);
    }
    let status = payload[2];
    if status != STATUS_OK {
        return Err(XrceError::AgentRejected(status));
    }
    Ok(())
}

// ── XML builder helpers ──────────────────────────────────────────────────────

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

// ── Minimal no_alloc string builder ─────────────────────────────────────────

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

struct MsgBuilder<'a> {
    buf: &'a mut [u8],
    pos: usize,
}

impl<'a> MsgBuilder<'a> {
    fn new(buf: &'a mut [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn u8(&mut self, v: u8) {
        if self.pos < self.buf.len() {
            self.buf[self.pos] = v;
        }
        self.pos += 1;
    }

    fn u16(&mut self, v: u16) {
        let b = v.to_le_bytes();
        self.bytes(&b);
    }

    fn u32(&mut self, v: u32) {
        let b = v.to_le_bytes();
        self.bytes(&b);
    }

    fn bytes(&mut self, data: &[u8]) {
        let space = self.buf.len().saturating_sub(self.pos);
        let n = data.len().min(space);
        self.buf[self.pos..self.pos + n].copy_from_slice(&data[..n]);
        self.pos += data.len();
    }

    fn align(&mut self, n: usize) {
        let rem = self.pos % n;
        if rem != 0 {
            let pad = n - rem;
            for _ in 0..pad {
                self.u8(0);
            }
        }
    }

    /// Patch the submessage length field at `hdr_off + 2` with the number of
    /// payload bytes (everything after the 4-byte submessage header).
    fn patch_len(&mut self, hdr_off: usize) {
        let payload_len = (self.pos - hdr_off - 4) as u16;
        let b = payload_len.to_le_bytes();
        if hdr_off + 4 <= self.buf.len() {
            self.buf[hdr_off + 2] = b[0];
            self.buf[hdr_off + 3] = b[1];
        }
    }
}
