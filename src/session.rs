//! ROS2-style XRCE-DDS session.
//!
//! Wire format follows eProsima Micro-XRCE-DDS-Client (the format spoken by
//! `microros/micro-ros-agent`). See `/tenshi-no-hana/.claude/xrce_dds_protocol.md`
//! for byte-for-byte reference.
//!
//! The session owns the TCP transport and exposes a small ROS2-like API:
//! `create_node` → `create_publisher` / `create_subscription` → `publish`
//! and `spin` / `spin_once` for the receive loop.

use core::fmt::Write as _;

use embedded_io_async::{Read, Write};
use heapless::{String as HString, Vec as HVec};

use crate::{
    error::Error,
    framing,
    message::Message,
    node::Node,
    protocol::*,
    publisher::Publisher,
    subscription::{Subscription, SubscriptionSlot},
};

#[cfg(feature = "defmt")]
use defmt::{debug, error, warn};
#[cfg(not(feature = "defmt"))]
macro_rules! debug { ($($t:tt)*) => {}; }
#[cfg(not(feature = "defmt"))]
macro_rules! error { ($($t:tt)*) => {}; }
#[cfg(not(feature = "defmt"))]
macro_rules! warn { ($($t:tt)*) => {}; }

const MAX_SUBSCRIPTIONS: usize = 8;
const TX_BUF_SIZE: usize = 768;
const RX_BUF_SIZE: usize = 768;
const TOPIC_NAME_MAX: usize = 96;

/// XRCE-DDS session over a length-prefixed TCP transport.
pub struct Session<T: Read + Write> {
    transport: T,
    session_id: u8,
    client_key: [u8; 4],
    seq: u16,
    req_id: u16,

    // Monotonic object index allocators. The agent doesn't care about the
    // numeric value as long as different entities of the same kind have
    // different indices, so a single counter per kind is enough.
    next_participant_idx: u16,
    next_topic_idx: u16,
    next_publisher_idx: u16,
    next_subscriber_idx: u16,
    next_dw_idx: u16,
    next_dr_idx: u16,

    tx_buf: [u8; TX_BUF_SIZE],
    rx_buf: [u8; RX_BUF_SIZE],

    subscriptions: HVec<&'static dyn SubscriptionSlot, MAX_SUBSCRIPTIONS>,
}

impl<T: Read + Write> Session<T> {
    // ── Connection ────────────────────────────────────────────────────────

    /// Establish a session: send CREATE_CLIENT, await STATUS_AGENT.
    /// `session_id` should be in `0x81..=0xFE` to keep message headers compact.
    pub async fn connect(
        mut transport: T,
        session_id: u8,
        client_key: [u8; 4],
    ) -> Result<Self, Error> {
        let mut tx = [0u8; 64];
        let n = build_create_client(&mut tx, session_id, &client_key, 512);
        debug!("[session] sending CREATE_CLIENT ({} bytes)", n);
        framing::write_framed(&mut transport, &tx[..n]).await?;

        let mut rx = [0u8; 128];
        let reply = framing::read_framed(&mut transport, &mut rx).await?;
        parse_status_agent(reply, session_id)?;
        debug!("[session] STATUS_AGENT OK");

        Ok(Self {
            transport,
            session_id,
            client_key,
            seq: 0,
            req_id: 1,
            next_participant_idx: 1,
            next_topic_idx: 1,
            next_publisher_idx: 1,
            next_subscriber_idx: 1,
            next_dw_idx: 1,
            next_dr_idx: 1,
            tx_buf: [0; TX_BUF_SIZE],
            rx_buf: [0; RX_BUF_SIZE],
            subscriptions: HVec::new(),
        })
    }

    // ── ROS2-style API ────────────────────────────────────────────────────

    /// Create a ROS2 Node (Participant + default Publisher + Subscriber).
    pub async fn create_node(&mut self, name: &str) -> Result<Node, Error> {
        let participant_idx = self.alloc_participant();
        let publisher_idx = self.alloc_publisher();
        let subscriber_idx = self.alloc_subscriber();

        let mut xml = HString::<128>::new();
        let _ = write!(xml, "<dds><participant><rtps><name>{}</name></rtps></participant></dds>", name);
        self.create_participant_entity(participant_idx, xml.as_str()).await?;

        self.create_publisher_entity(publisher_idx, participant_idx).await?;
        self.create_subscriber_entity(subscriber_idx, participant_idx).await?;

        Ok(Node {
            participant_idx,
            publisher_idx,
            subscriber_idx,
        })
    }

    /// Create a typed Publisher on `topic`. The leading `/` of `topic` is
    /// turned into the DDS `rt/` prefix automatically.
    pub async fn create_publisher<M: Message>(
        &mut self,
        node: &Node,
        topic: &str,
    ) -> Result<Publisher<M>, Error> {
        let dds_topic = ros2_topic_name::<TOPIC_NAME_MAX>(topic)?;

        // Topic — one per (name, type) tuple is enough, but it's simpler to
        // allocate a fresh idx each call. The agent's REUSE|REPLACE flag in
        // CREATE deduplicates by name so this is harmless.
        let topic_idx = self.alloc_topic();
        let topic_oid = object_id(topic_idx, ENTITY_TOPIC);
        let mut xml = HString::<256>::new();
        let _ = write!(xml, "<dds><topic><name>{}</name><dataType>{}</dataType></topic></dds>",
            dds_topic.as_str(), M::TYPE_NAME);
        self.create_with_parent_entity(
            topic_oid, ENTITY_TOPIC, xml.as_str(),
            object_id(node.participant_idx, ENTITY_PARTICIPANT),
        ).await?;

        // DataWriter
        let dw_idx = self.alloc_dw();
        let dw_oid = object_id(dw_idx, ENTITY_DATAWRITER);
        let mut xml = HString::<320>::new();
        let _ = write!(xml,
            "<dds><data_writer><topic><kind>NO_KEY</kind><name>{}</name><dataType>{}</dataType></topic></data_writer></dds>",
            dds_topic.as_str(), M::TYPE_NAME);
        self.create_with_parent_entity(
            dw_oid, ENTITY_DATAWRITER, xml.as_str(),
            object_id(node.publisher_idx, ENTITY_PUBLISHER),
        ).await?;

        Ok(Publisher::new(dw_oid))
    }

    /// Register a subscription. The slot must outlive the session
    /// (typically a `static_cell::StaticCell`-backed `&'static Subscription`).
    /// Returns the same `&'static` reference for ergonomic chaining.
    pub async fn create_subscription<M: Message + Send + 'static, const N: usize>(
        &mut self,
        node: &Node,
        topic: &str,
        slot: &'static Subscription<M, N>,
    ) -> Result<&'static Subscription<M, N>, Error> {
        if self.subscriptions.is_full() {
            return Err(Error::TooManySubscriptions);
        }
        let dds_topic = ros2_topic_name::<TOPIC_NAME_MAX>(topic)?;

        // Topic
        let topic_idx = self.alloc_topic();
        let topic_oid = object_id(topic_idx, ENTITY_TOPIC);
        let mut xml = HString::<256>::new();
        let _ = write!(xml, "<dds><topic><name>{}</name><dataType>{}</dataType></topic></dds>",
            dds_topic.as_str(), M::TYPE_NAME);
        self.create_with_parent_entity(
            topic_oid, ENTITY_TOPIC, xml.as_str(),
            object_id(node.participant_idx, ENTITY_PARTICIPANT),
        ).await?;

        // DataReader
        let dr_idx = self.alloc_dr();
        let dr_oid = object_id(dr_idx, ENTITY_DATAREADER);
        let mut xml = HString::<320>::new();
        let _ = write!(xml,
            "<dds><data_reader><topic><kind>NO_KEY</kind><name>{}</name><dataType>{}</dataType></topic></data_reader></dds>",
            dds_topic.as_str(), M::TYPE_NAME);
        self.create_with_parent_entity(
            dr_oid, ENTITY_DATAREADER, xml.as_str(),
            object_id(node.subscriber_idx, ENTITY_SUBSCRIBER),
        ).await?;

        // Send READ_DATA so the agent starts streaming samples to us.
        self.send_read_data(dr_oid).await?;

        slot.set_dr_id(dr_oid);
        // Hashes the static reference into the dispatch table. SubscriptionSlot
        // is implemented for Subscription<M, N>.
        self.subscriptions
            .push(slot as &'static dyn SubscriptionSlot)
            .map_err(|_| Error::TooManySubscriptions)?;

        Ok(slot)
    }

    /// Publish a message via the DataWriter referenced by `pub_`.
    pub async fn publish<M: Message>(
        &mut self,
        pub_: &Publisher<M>,
        msg: &M,
    ) -> Result<(), Error> {
        // Lay out the WRITE_DATA message in tx_buf:
        //   [msg hdr][submsg hdr][BaseObjectRequest][CDR body]
        //
        // CDR body is written first into a slice of tx_buf reserved past
        // the headers, then the headers are filled in with the now-known
        // payload length.

        let prefix = msg_header_len(self.session_id) + 4 /* sub hdr */ + 4 /* BaseObjReq */;
        if prefix > self.tx_buf.len() {
            return Err(Error::BufferTooSmall);
        }
        let body_slot = &mut self.tx_buf[prefix..];
        let body_len = {
            let mut w = crate::cdr::CdrWriter::new(body_slot);
            msg.serialize(&mut w);
            w.bytes_written()
        };
        if body_len > body_slot.len() {
            return Err(Error::BufferTooSmall);
        }
        if body_len > M::MAX_SERIALIZED_SIZE {
            warn!(
                "[session] message exceeded MAX_SERIALIZED_SIZE ({} > {})",
                body_len,
                M::MAX_SERIALIZED_SIZE
            );
        }
        let total = prefix + body_len;
        let seq = self.next_seq();

        // Now write headers in front (they fit in `prefix` bytes by construction).
        let session_id = self.session_id;
        let key = self.client_key;
        finalize_write_data_headers(
            &mut self.tx_buf[..total],
            session_id,
            seq,
            &key,
            pub_.dw_id,
        );

        framing::write_framed(&mut self.transport, &self.tx_buf[..total]).await
    }

    /// Read and dispatch one incoming frame.
    /// DATA submessages are routed to matching subscriptions; everything else
    /// (stray STATUS/HEARTBEAT) is logged and dropped.
    pub async fn spin_once(&mut self) -> Result<(), Error> {
        let len = Self::read_one_frame(&mut self.transport, &mut self.rx_buf).await?;
        Self::dispatch_frame(
            &self.rx_buf[..len],
            self.session_id,
            &self.subscriptions,
        );
        Ok(())
    }

    /// Run the dispatch loop forever. Returns the first error encountered
    /// (typically a transport disconnect).
    pub async fn spin(&mut self) -> Error {
        loop {
            if let Err(e) = self.spin_once().await {
                return e;
            }
        }
    }

    // ── Internal: entity creation primitives ──────────────────────────────

    async fn create_participant_entity(&mut self, idx: u16, xml: &str) -> Result<(), Error> {
        let oid = object_id(idx, ENTITY_PARTICIPANT);
        let req = self.next_req();
        let seq = self.next_seq();
        let session_id = self.session_id;
        let key = self.client_key;
        let n = encode_create_participant(&mut self.tx_buf, session_id, seq, &key, req, oid, xml, 0)?;
        framing::write_framed(&mut self.transport, &self.tx_buf[..n]).await?;
        self.wait_status_for(req).await
    }

    async fn create_publisher_entity(&mut self, pub_idx: u16, participant_idx: u16) -> Result<(), Error> {
        let oid = object_id(pub_idx, ENTITY_PUBLISHER);
        let parent = object_id(participant_idx, ENTITY_PARTICIPANT);
        let xml = "<dds><publisher><name>MyPublisher</name></publisher></dds>";
        self.create_with_parent_entity(oid, ENTITY_PUBLISHER, xml, parent).await
    }

    async fn create_subscriber_entity(&mut self, sub_idx: u16, participant_idx: u16) -> Result<(), Error> {
        let oid = object_id(sub_idx, ENTITY_SUBSCRIBER);
        let parent = object_id(participant_idx, ENTITY_PARTICIPANT);
        let xml = "<dds><subscriber><name>MySubscriber</name></subscriber></dds>";
        self.create_with_parent_entity(oid, ENTITY_SUBSCRIBER, xml, parent).await
    }

    async fn create_with_parent_entity(
        &mut self,
        obj_oid: u16,
        obj_kind: u8,
        xml: &str,
        parent_oid: u16,
    ) -> Result<(), Error> {
        let req = self.next_req();
        let seq = self.next_seq();
        let session_id = self.session_id;
        let key = self.client_key;
        let n = encode_create_with_parent(
            &mut self.tx_buf, session_id, seq, &key, req, obj_oid, obj_kind, xml, parent_oid,
        )?;
        framing::write_framed(&mut self.transport, &self.tx_buf[..n]).await?;
        self.wait_status_for(req).await
    }

    async fn send_read_data(&mut self, dr_oid: u16) -> Result<(), Error> {
        // READ_DATA is fire-and-forget — eProsima's `uxr_buffer_request_data`
        // never waits for a STATUS reply, and the agent doesn't send one.
        // Waiting here would hang until the TCP read times out.
        let req = self.next_req();
        let seq = self.next_seq();
        let session_id = self.session_id;
        let key = self.client_key;
        let n = encode_read_data(&mut self.tx_buf, session_id, seq, &key, req, dr_oid)?;
        framing::write_framed(&mut self.transport, &self.tx_buf[..n]).await
    }

    /// Read frames until we see the STATUS for `expected_req`. Any DATA
    /// frames seen along the way are dispatched to their subscriptions.
    async fn wait_status_for(&mut self, expected_req: u16) -> Result<(), Error> {
        loop {
            let len = Self::read_one_frame(&mut self.transport, &mut self.rx_buf).await?;
            let msg = &self.rx_buf[..len];
            let hdr_len = msg_header_len(self.session_id);
            if msg.len() < hdr_len + 4 {
                return Err(Error::UnexpectedReply);
            }
            let submsg_id = msg[hdr_len];
            let submsg_len = u16::from_le_bytes([msg[hdr_len + 2], msg[hdr_len + 3]]) as usize;
            let payload = &msg[hdr_len + 4..hdr_len + 4 + submsg_len.min(msg.len() - hdr_len - 4)];

            match submsg_id {
                SUBMSG_STATUS => {
                    return parse_status_payload(payload, expected_req);
                }
                SUBMSG_DATA => {
                    Self::dispatch_data_payload(payload, &self.subscriptions);
                    continue;
                }
                _ => {
                    debug!("[session] ignoring submsg 0x{:02X}", submsg_id);
                    continue;
                }
            }
        }
    }

    async fn read_one_frame(transport: &mut T, rx_buf: &mut [u8]) -> Result<usize, Error> {
        let mut len_buf = [0u8; 2];
        read_exact(transport, &mut len_buf).await?;
        let len = u16::from_le_bytes(len_buf) as usize;
        if len > rx_buf.len() {
            return Err(Error::BufferTooSmall);
        }
        read_exact(transport, &mut rx_buf[..len]).await?;
        Ok(len)
    }

    fn dispatch_frame(
        msg: &[u8],
        session_id: u8,
        subs: &[&'static dyn SubscriptionSlot],
    ) {
        let hdr_len = msg_header_len(session_id);
        if msg.len() < hdr_len + 4 {
            return;
        }
        let submsg_id = msg[hdr_len];
        let submsg_len = u16::from_le_bytes([msg[hdr_len + 2], msg[hdr_len + 3]]) as usize;
        let payload_end = (hdr_len + 4 + submsg_len).min(msg.len());
        let payload = &msg[hdr_len + 4..payload_end];

        match submsg_id {
            SUBMSG_DATA => Self::dispatch_data_payload(payload, subs),
            SUBMSG_STATUS | SUBMSG_STATUS_AGENT => {
                debug!("[session] stray STATUS submsg 0x{:02X}", submsg_id);
            }
            _ => {
                debug!("[session] ignoring submsg 0x{:02X}", submsg_id);
            }
        }
    }

    fn dispatch_data_payload(payload: &[u8], subs: &[&'static dyn SubscriptionSlot]) {
        if payload.len() < 4 {
            return;
        }
        // BaseObjectReply: req_id (2 BE) + obj_id (2 BE)
        let dr_oid = u16::from_be_bytes([payload[2], payload[3]]);
        let user_data = &payload[4..];
        let show = user_data.len().min(16);
        debug!(
            "[session] DATA dr_oid=0x{:04X} user_data_len={} head={=[u8]}",
            dr_oid, user_data.len(), &user_data[..show]
        );
        for slot in subs {
            if slot.dr_id() == dr_oid {
                if let Err(e) = slot.try_deliver(user_data) {
                    warn!(
                        "[session] sub deliver failed: {} (user_data_len={} head={=[u8]})",
                        e, user_data.len(), &user_data[..show]
                    );
                }
                return;
            }
        }
        debug!("[session] DATA for unknown dr_oid=0x{:04X}", dr_oid);
    }

    // ── Counter helpers ───────────────────────────────────────────────────

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
    fn alloc_participant(&mut self) -> u16 { let n = self.next_participant_idx; self.next_participant_idx += 1; n }
    fn alloc_topic(&mut self) -> u16 { let n = self.next_topic_idx; self.next_topic_idx += 1; n }
    fn alloc_publisher(&mut self) -> u16 { let n = self.next_publisher_idx; self.next_publisher_idx += 1; n }
    fn alloc_subscriber(&mut self) -> u16 { let n = self.next_subscriber_idx; self.next_subscriber_idx += 1; n }
    fn alloc_dw(&mut self) -> u16 { let n = self.next_dw_idx; self.next_dw_idx += 1; n }
    fn alloc_dr(&mut self) -> u16 { let n = self.next_dr_idx; self.next_dr_idx += 1; n }
}

// ── Free helpers ────────────────────────────────────────────────────────────

/// Convert a ROS2 topic name (`/foo/bar`) to its DDS form (`rt/foo/bar`).
fn ros2_topic_name<const N: usize>(topic: &str) -> Result<HString<N>, Error> {
    let mut s = HString::<N>::new();
    let body = topic.strip_prefix('/').unwrap_or(topic);
    s.push_str("rt/").map_err(|_| Error::BufferTooSmall)?;
    s.push_str(body).map_err(|_| Error::BufferTooSmall)?;
    Ok(s)
}

#[inline]
fn msg_header_len(session_id: u8) -> usize {
    if session_id < SESSION_ID_WITHOUT_CLIENT_KEY { 8 } else { 4 }
}

async fn read_exact<R: Read>(r: &mut R, mut buf: &mut [u8]) -> Result<(), Error> {
    while !buf.is_empty() {
        match r.read(buf).await {
            Err(_) => return Err(Error::Io),
            Ok(0) => return Err(Error::Disconnected),
            Ok(n) => buf = &mut buf[n..],
        }
    }
    Ok(())
}

// ── Wire encoders ───────────────────────────────────────────────────────────

/// CREATE_CLIENT message.
fn build_create_client(buf: &mut [u8], session_id: u8, client_key: &[u8; 4], mtu: u16) -> usize {
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
    if b.overflow() { return Err(Error::BufferTooSmall); }
    Ok(b.pos())
}

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
    if b.overflow() { return Err(Error::BufferTooSmall); }
    Ok(b.pos())
}

fn encode_read_data(
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

    // BaseObjectRequest
    b.bytes(&request_id_be_bytes(req_id));
    b.bytes(&object_id_be_bytes(dr_oid));
    // ReadSpecification:
    //   preferred_stream_id (u8) = STREAM_BEST_EFFORT
    //   data_format (u8)         = FORMAT_DATA
    //   optional_content_filter_expression (bool) = false
    //   optional_delivery_control (bool)          = false (continuous)
    b.u8(STREAM_BEST_EFFORT);
    b.u8(FORMAT_DATA);
    b.u8(0);
    b.u8(0);

    let payload_len = b.pos() - origin;
    b.patch_u16_at(hdr_off + 2, payload_len as u16);
    if b.overflow() { return Err(Error::BufferTooSmall); }
    Ok(b.pos())
}

/// Fill in the WRITE_DATA headers for a buffer whose CDR body is already
/// present at offset `prefix = msg_hdr + sub_hdr + BaseObjectRequest`.
fn finalize_write_data_headers(
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

    b.bytes(&request_id_be_bytes(0));
    b.bytes(&object_id_be_bytes(dw_oid));
    // CDR body already present in buf[b.pos()..].
}

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

// ── Parsers ─────────────────────────────────────────────────────────────────

fn parse_status_agent(msg: &[u8], session_id: u8) -> Result<(), Error> {
    let hdr_len = msg_header_len(session_id);
    if msg.len() < hdr_len + 4 + 2 {
        error!("[session] STATUS_AGENT too short: {}", msg.len());
        return Err(Error::UnexpectedReply);
    }
    let submsg_id = msg[hdr_len];
    if submsg_id != SUBMSG_STATUS_AGENT {
        error!("[session] expected STATUS_AGENT (4), got 0x{:02X}", submsg_id);
        return Err(Error::UnexpectedReply);
    }
    let payload = &msg[hdr_len + 4..];
    let status = payload[0];
    if status != STATUS_OK {
        return Err(Error::AgentRejected(status));
    }
    Ok(())
}

fn parse_status_payload(payload: &[u8], expected_req: u16) -> Result<(), Error> {
    if payload.len() < 6 {
        return Err(Error::UnexpectedReply);
    }
    let got_req = u16::from_be_bytes([payload[0], payload[1]]);
    if got_req != expected_req {
        error!("[session] STATUS req mismatch: got {} expected {}", got_req, expected_req);
        return Err(Error::StatusReqMismatch);
    }
    let obj_id = u16::from_be_bytes([payload[2], payload[3]]);
    let status = payload[4];
    match status {
        STATUS_OK => Ok(()),
        STATUS_OK_MATCHED => {
            // Matched a pre-existing entity from an earlier session. Often
            // benign, but if the previous firmware used the same entity index
            // for a *different* topic/type, downstream CREATE_DATAREADER /
            // CREATE_DATAWRITER on the same id will likely be rejected with
            // STATUS_ERR_DDS_ERROR (0x80). Restart the agent (`docker restart
            // micro_ros_agent`) or use a fresh `client_key` to clear it.
            warn!(
                "[session] STATUS_OK_MATCHED for obj_id=0x{:04X} — stale entity reused from a previous session; restart the agent if subsequent CREATEs fail",
                obj_id
            );
            Ok(())
        }
        _ => Err(Error::AgentRejected(status)),
    }
}

// ── MsgWriter (re-used internally) ──────────────────────────────────────────

struct MsgWriter<'a> {
    buf: &'a mut [u8],
    pos: usize,
    overflow: bool,
}

impl<'a> MsgWriter<'a> {
    fn new(buf: &'a mut [u8]) -> Self { Self { buf, pos: 0, overflow: false } }
    fn pos(&self) -> usize { self.pos }
    fn overflow(&self) -> bool { self.overflow || self.pos > self.buf.len() }

    fn u8(&mut self, v: u8) {
        if self.pos < self.buf.len() {
            self.buf[self.pos] = v;
        } else {
            self.overflow = true;
        }
        self.pos += 1;
    }
    fn u16_raw(&mut self, v: u16) { self.bytes(&v.to_le_bytes()); }
    fn bytes(&mut self, data: &[u8]) {
        let space = self.buf.len().saturating_sub(self.pos);
        if data.len() > space { self.overflow = true; }
        let n = data.len().min(space);
        self.buf[self.pos..self.pos + n].copy_from_slice(&data[..n]);
        self.pos += data.len();
    }
    fn align_buf(&mut self, n: usize) {
        let rem = self.pos % n;
        if rem != 0 { for _ in 0..(n - rem) { self.u8(0); } }
    }
    fn cdr_align(&mut self, origin: usize, n: usize) {
        let rem = (self.pos - origin) % n;
        if rem != 0 { for _ in 0..(n - rem) { self.u8(0); } }
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
    fn cdr_string(&mut self, s: &str, origin: usize) -> Result<(), Error> {
        let len_with_null = s.len() as u32 + 1;
        self.cdr_u32(len_with_null, origin);
        self.bytes(s.as_bytes());
        self.u8(0);
        if self.overflow() { return Err(Error::BufferTooSmall); }
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
