//! Wire-format fixture tests.
//!
//! Each test locks the exact byte sequence emitted by the low-level encoders
//! for a specific XRCE-DDS frame type.  If any byte changes during a refactor
//! the test fails immediately, making silent wire-format regressions impossible.
//!
//! Run on the host (x86_64):
//!   cargo test --no-default-features

use micro_xrce_dds_rs::{
    cdr::CdrWriter,
    msg::std_msgs::Float32,
    protocol::{object_id, ENTITY_DATAREADER, ENTITY_DATAWRITER, ENTITY_PARTICIPANT},
    rt::encode::{
        build_create_client, encode_create_participant, encode_read_data,
        finalize_write_data_headers, msg_header_len,
    },
    Message,
};

const SESSION_ID: u8 = 0x81;
const CLIENT_KEY: [u8; 4] = [0xBA, 0xCE, 0xA1, 0x05];

// ── Helper: build a WRITE_DATA frame the same way Publisher::publish does ────

fn write_data_frame(
    session_id: u8,
    seq: u16,
    client_key: &[u8; 4],
    dw_oid: u16,
    body: &[u8],
) -> ([u8; 64], usize) {
    let prefix = msg_header_len(session_id) + 4 /* submsg hdr */ + 4 /* BaseObjReq */;
    let total = prefix + body.len();
    assert!(total <= 64, "test frame exceeds scratch buffer");
    let mut buf = [0u8; 64];
    buf[prefix..prefix + body.len()].copy_from_slice(body);
    finalize_write_data_headers(&mut buf[..total], session_id, seq, client_key, dw_oid);
    (buf, total)
}

// ── Test 1: CREATE_CLIENT ────────────────────────────────────────────────────
//
// build_create_client(buf, session_id=0x81, key=[0xBA,0xCE,0xA1,0x05], mtu=512)
//
// Derivation (24 bytes):
//   [0]    0x80  masked session_id (0x81 & 0x80; >= 0x80 → no key in hdr)
//   [1]    0x00  STREAM_NONE
//   [2..4] 0x00 0x00  seq=0 LE
//   [4]    0x00  SUBMSG_CREATE_CLIENT
//   [5]    0x01  FLAG_LE
//   [6..8] 0x10 0x00  payload_len=16 LE  (patched at end)
//   [8..12]  58 52 43 45  XRCE_COOKIE "XRCE"
//   [12..14] 01 00  XRCE_VERSION 1.0
//   [14..16] 01 0F  VENDOR_ID_EPROSIMA
//   [16..20] BA CE A1 05  client_key
//   [20]   0x81  session_id (full)
//   [21]   0x00  optional_properties = false
//   [22..24] 00 02  MTU=512 CDR-aligned u16 LE
#[test]
fn create_client_wire_bytes() {
    let mut buf = [0u8; 64];
    let n = build_create_client(&mut buf, SESSION_ID, &CLIENT_KEY, 512);
    assert_eq!(n, 24);
    #[rustfmt::skip]
    let expected: [u8; 24] = [
        0x80, 0x00, 0x00, 0x00,  // session hdr: masked_sid, STREAM_NONE, seq=0 LE
        0x00, 0x01, 0x10, 0x00,  // CREATE_CLIENT: id=0, FLAG_LE, len=16 LE
        0x58, 0x52, 0x43, 0x45,  // XRCE_COOKIE "XRCE"
        0x01, 0x00,              // XRCE_VERSION 1.0
        0x01, 0x0F,              // VENDOR_ID_EPROSIMA
        0xBA, 0xCE, 0xA1, 0x05, // client_key
        0x81,                   // session_id (full, not masked)
        0x00,                   // optional_properties = false
        0x00, 0x02,             // MTU = 512 LE (CDR u16)
    ];
    assert_eq!(&buf[..n], &expected[..]);
}

// ── Test 2: CREATE_PARTICIPANT ───────────────────────────────────────────────
//
// encode_create_participant(buf, 0x81, seq=0, key, req_id=1,
//   obj_id=object_id(1,ENTITY_PARTICIPANT)=0x11, xml="my_node", domain_id=0)
//
// Derivation (30 bytes):
//   [0]      0x81  session_id (>= 0x80 → no key in hdr)
//   [1]      0x01  STREAM_BEST_EFFORT
//   [2..4]   00 00  seq=0 LE
//   [4]      0x01  SUBMSG_CREATE
//   [5]      0x07  FLAGS_CREATE (LE|REUSE|REPLACE)
//   [6..8]   16 00  payload_len=22 LE  (patched at end)
//   [8..10]  00 01  req_id=1 BE
//   [10..12] 00 11  obj_id=0x0011 BE  (idx=1, kind=PARTICIPANT=1)
//   [12]     0x01  ENTITY_PARTICIPANT
//   [13]     0x02  REPR_AS_XML
//   [14..16] 00 00  CDR-align-4 padding  ((14-8)%4=2 → 2 pad bytes)
//   [16..20] 08 00 00 00  CDR string len=8 LE u32 (7 chars + null)
//   [20..27] 6D 79 5F 6E 6F 64 65  "my_node"
//   [27]     0x00  null terminator
//   [28..30] 00 00  domain_id=0 LE i16
#[test]
fn create_participant_wire_bytes() {
    let obj_id = object_id(1, ENTITY_PARTICIPANT);
    assert_eq!(obj_id, 0x0011);

    let mut buf = [0u8; 64];
    let n = encode_create_participant(
        &mut buf,
        SESSION_ID,
        /*seq=*/ 0,
        &CLIENT_KEY,
        /*req_id=*/ 1,
        obj_id,
        "my_node",
        /*domain_id=*/ 0,
    )
    .unwrap();
    assert_eq!(n, 30);
    #[rustfmt::skip]
    let expected: [u8; 30] = [
        0x81, 0x01, 0x00, 0x00,  // session hdr: session_id, STREAM_BE, seq=0 LE
        0x01, 0x07, 0x16, 0x00,  // CREATE: id=1, FLAGS_CREATE=7, len=22 LE
        0x00, 0x01,              // req_id=1 BE
        0x00, 0x11,              // obj_id=0x0011 BE
        0x01,                   // ENTITY_PARTICIPANT
        0x02,                   // REPR_AS_XML
        0x00, 0x00,             // CDR align(4) pad
        0x08, 0x00, 0x00, 0x00, // CDR string len = 8 LE
        0x6D, 0x79, 0x5F, 0x6E, 0x6F, 0x64, 0x65, // "my_node"
        0x00,                   // null terminator
        0x00, 0x00,             // domain_id = 0 LE i16
    ];
    assert_eq!(&buf[..n], &expected[..]);
}

// ── Test 3: WRITE_DATA Float32(1.0) ─────────────────────────────────────────
//
// Publisher::publish for Float32(1.0), dw_oid=object_id(1,DW)=0x15.
// CDR body: f32 1.0 = 0x3F800000 LE = [00 00 80 3F]
// prefix = msg_header_len(0x81)=4 + sub_hdr=4 + BaseObjReq=4 = 12
// total  = 12 + 4 = 16
//
// Derivation (16 bytes):
//   [0]      0x81  session_id
//   [1]      0x01  STREAM_BEST_EFFORT
//   [2..4]   00 00  seq=0 LE
//   [4]      0x07  SUBMSG_WRITE_DATA
//   [5]      0x01  FLAG_LE | FORMAT_DATA
//   [6..8]   08 00  payload_len=8 LE
//   [8..10]  00 00  req_id=0 BE  (fire-and-forget)
//   [10..12] 00 15  dw_oid=0x0015 BE  (idx=1, DATAWRITER=5)
//   [12..16] 00 00 80 3F  f32 1.0 LE CDR body
#[test]
fn write_data_float32_wire_bytes() {
    let dw_oid = object_id(1, ENTITY_DATAWRITER);
    assert_eq!(dw_oid, 0x0015);

    let mut body = [0u8; 4];
    let body_len = {
        let mut w = CdrWriter::new(&mut body);
        Float32(1.0).serialize(&mut w);
        w.bytes_written()
    };
    assert_eq!(body_len, 4);

    let (frame, total) = write_data_frame(SESSION_ID, 0, &CLIENT_KEY, dw_oid, &body[..body_len]);
    assert_eq!(total, 16);
    #[rustfmt::skip]
    let expected: [u8; 16] = [
        0x81, 0x01, 0x00, 0x00,  // session hdr
        0x07, 0x01, 0x08, 0x00,  // WRITE_DATA: id=7, FLAG_LE, len=8 LE
        0x00, 0x00,              // req_id = 0 BE
        0x00, 0x15,              // dw_oid = 0x0015 BE
        0x00, 0x00, 0x80, 0x3F, // f32 1.0 LE
    ];
    assert_eq!(&frame[..total], &expected[..]);
}

// ── Test 4: READ_DATA ────────────────────────────────────────────────────────
//
// encode_read_data(buf, 0x81, seq=0, key, req_id=1,
//   dr_oid=object_id(1,ENTITY_DATAREADER)=0x16)
//
// Derivation (24 bytes):
//   [0]      0x81  session_id
//   [1]      0x01  STREAM_BEST_EFFORT
//   [2..4]   00 00  seq=0 LE
//   [4]      0x08  SUBMSG_READ_DATA
//   [5]      0x01  FLAG_LE
//   [6..8]   10 00  payload_len=16 LE
//   [8..10]  00 01  req_id=1 BE
//   [10..12] 00 16  dr_oid=0x0016 BE  (idx=1, DATAREADER=6)
//   [12]     0x01  STREAM_BEST_EFFORT (delivery stream)
//   [13]     0x00  FORMAT_DATA
//   [14]     0x00  optional_content_filter_expression = false
//   [15]     0x01  optional_delivery_control = true
//   [16..18] FF FF max_samples = UNLIMITED
//   [18..20] FF FF max_elapsed_time = UNLIMITED
//   [20..22] FF FF max_bytes_per_seconds = UNLIMITED
//   [22..24] 00 00 min_pace_period = 0
#[test]
fn read_data_wire_bytes() {
    let dr_oid = object_id(1, ENTITY_DATAREADER);
    assert_eq!(dr_oid, 0x0016);

    let mut buf = [0u8; 64];
    let n = encode_read_data(&mut buf, SESSION_ID, 0, &CLIENT_KEY, 1, dr_oid).unwrap();
    assert_eq!(n, 24);
    #[rustfmt::skip]
    let expected: [u8; 24] = [
        0x81, 0x01, 0x00, 0x00,  // session hdr
        0x08, 0x01, 0x10, 0x00,  // READ_DATA: id=8, FLAG_LE, payload_len=16 LE
        0x00, 0x01,              // req_id=1 BE
        0x00, 0x16,              // dr_oid=0x0016 BE
        0x01,                    // STREAM_BEST_EFFORT
        0x00,                    // FORMAT_DATA
        0x00,                    // optional_content_filter_expression = false
        0x01,                    // optional_delivery_control = true
        0xFF, 0xFF,              // max_samples = UNLIMITED
        0xFF, 0xFF,              // max_elapsed_time = UNLIMITED
        0xFF, 0xFF,              // max_bytes_per_seconds = UNLIMITED
        0x00, 0x00,              // min_pace_period = 0
    ];
    assert_eq!(&buf[..n], &expected[..]);
}
