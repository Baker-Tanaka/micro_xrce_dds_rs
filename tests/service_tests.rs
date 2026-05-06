//! Service-layer infrastructure tests (v0.3).
//!
//! Verifies:
//! - SampleIdentity CDR round-trip (24 bytes, alignment intact).
//! - ServiceClientSlot routes a reply only when the SampleIdentity sequence
//!   number matches the currently-pending call.
//! - ServiceServerSlot delivers requests with their identity attached.
//! - The `subscription_slot!` dispatch path can route by oid into a service
//!   slot just like a regular subscription.
//!
//! These run on the host (x86_64) without a live transport.

use micro_xrce_dds_rs::{
    cdr::CdrWriter,
    cdr_reader::CdrReader,
    msg::std_msgs::Float32,
    Message, SampleIdentity, Service, ServiceClientSlot, ServiceServerSlot, SubscriptionSlot,
};

// ── Helper: a stub Service whose request/response are both Float32 ───────────

struct PingPong;
impl Service for PingPong {
    type Request = Float32;
    type Response = Float32;
    const SERVICE_NAME: &'static str = "/pingpong";
    const REQUEST_TYPE_NAME: &'static str = "test::srv::dds_::PingPong_Request_";
    const RESPONSE_TYPE_NAME: &'static str = "test::srv::dds_::PingPong_Response_";
}

// ── Helper: build a service request body = SampleIdentity + Float32 ──────────

fn service_body(seq: i64, payload: f32) -> [u8; 28] {
    let mut buf = [0u8; 28];
    let mut w = CdrWriter::new(&mut buf);
    SampleIdentity {
        writer_guid: *b"GUID-0123456789X",
        sequence_number: seq,
    }
    .serialize(&mut w);
    Float32(payload).serialize(&mut w);
    let n = w.bytes_written();
    assert_eq!(n, 28, "body should be 16 GUID + 8 i64 + 4 f32");
    buf
}

// ── Test 1: SampleIdentity CDR round-trip ────────────────────────────────────

#[test]
fn sample_identity_round_trip() {
    let id = SampleIdentity {
        writer_guid: [
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E,
            0x0F, 0x10,
        ],
        sequence_number: 0x1122_3344_5566_7788,
    };
    let mut buf = [0u8; 32];
    let n = {
        let mut w = CdrWriter::new(&mut buf);
        id.serialize(&mut w);
        w.bytes_written()
    };
    assert_eq!(n, SampleIdentity::SERIALIZED_LEN);
    assert_eq!(n, 24);

    // The 16 GUID bytes are raw, then 8-aligned i64 (16 already 8-aligned),
    // so bytes [16..24] = sequence_number LE.
    assert_eq!(&buf[0..16], &id.writer_guid);
    assert_eq!(
        &buf[16..24],
        &(0x1122_3344_5566_7788i64).to_le_bytes()
    );

    let mut r = CdrReader::from_body(&buf[..n]);
    let id2 = SampleIdentity::deserialize(&mut r).unwrap();
    assert_eq!(id2, id);
}

// ── Test 2: ServiceClientSlot routes only when sequence matches ──────────────

#[test]
fn service_client_slot_matches_sequence() {
    static SLOT: ServiceClientSlot<PingPong> = ServiceClientSlot::new();
    SLOT.set_requester_oid(0x0017); // (idx=1, kind=REQUESTER=7)

    // Slot has no pending call yet → reply is silently dropped.
    let body = service_body(42, 3.5);
    assert!(SLOT.try_deliver(&body).is_ok());

    // Arm the slot for sequence 42 (mimics ServiceClient::call in-flight).
    SLOT.set_pending_seq(42);

    // A reply with sequence 7 → mismatch → still no inbox push.
    let stale = service_body(7, 1.0);
    assert!(SLOT.try_deliver(&stale).is_ok());

    // Reply with sequence 42 → delivered.
    let body = service_body(42, 3.5);
    assert!(SLOT.try_deliver(&body).is_ok());
    let resp = SLOT.try_recv_response().unwrap();
    assert_eq!(resp.0, 3.5);
    assert!(SLOT.try_recv_response().is_none());
}

// ── Test 3: ServiceServerSlot delivers identity + payload ───────────────────

#[test]
fn service_server_slot_delivers_request_with_identity() {
    static SLOT: ServiceServerSlot<PingPong, 4> = ServiceServerSlot::new();
    SLOT.set_replier_oid(0x0018); // (idx=1, kind=REPLIER=8)

    let body = service_body(99, 7.25);
    SLOT.try_deliver(&body).unwrap();

    let req = SLOT.try_recv_request().unwrap();
    assert_eq!(req.identity.sequence_number, 99);
    assert_eq!(&req.identity.writer_guid, b"GUID-0123456789X");
    assert_eq!(req.payload.0, 7.25);
}

// ── Test 4: subscription_slot dispatch trait works for service slots ────────
//
// The executor's dispatch table holds `&'static dyn SubscriptionSlot`.  We
// verify that ServiceClientSlot / ServiceServerSlot are usable as such — i.e.
// they expose `dr_id()` returning the requester/replier oid and route the
// payload through `try_deliver`.
#[test]
fn service_slots_implement_subscription_slot() {
    static CLIENT: ServiceClientSlot<PingPong> = ServiceClientSlot::new();
    static SERVER: ServiceServerSlot<PingPong, 2> = ServiceServerSlot::new();
    CLIENT.set_requester_oid(0xABCD);
    SERVER.set_replier_oid(0x1234);

    let table: [&'static dyn SubscriptionSlot; 2] = [&CLIENT, &SERVER];
    assert_eq!(table[0].dr_id(), 0xABCD);
    assert_eq!(table[1].dr_id(), 0x1234);

    // Route a server-bound request via the dyn trait object.
    let body = service_body(1, 0.5);
    table[1].try_deliver(&body).unwrap();
    assert!(SERVER.try_recv_request().is_some());
}
