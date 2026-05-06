//! Action-layer scaffolding tests (v0.4 trait scaffolding only).
//!
//! Verifies the small set of primitives that all per-action wrappers will
//! share: the 16-byte `GoalId` CDR encoding, the `from_seq` derivation, and
//! the `Action` trait shape.

use micro_xrce_dds_rs::{
    action::{goal_status, GoalId},
    cdr::CdrWriter,
    cdr_reader::CdrReader,
};

#[test]
fn goal_id_cdr_round_trip() {
    let id = GoalId([
        0x10, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70, 0x80, 0x90, 0xA0, 0xB0, 0xC0, 0xD0, 0xE0, 0xF0,
        0x00,
    ]);
    let mut buf = [0u8; 32];
    let n = {
        let mut w = CdrWriter::new(&mut buf);
        id.serialize(&mut w);
        w.bytes_written()
    };
    assert_eq!(n, GoalId::SERIALIZED_LEN);
    assert_eq!(n, 16);
    assert_eq!(&buf[..16], &id.0);

    let mut r = CdrReader::from_body(&buf[..n]);
    let id2 = GoalId::deserialize(&mut r).unwrap();
    assert_eq!(id2, id);
}

#[test]
fn goal_id_from_seq_is_deterministic() {
    let a = GoalId::from_seq([0xAA, 0xBB, 0xCC, 0xDD], 0x0007, 42);
    let b = GoalId::from_seq([0xAA, 0xBB, 0xCC, 0xDD], 0x0007, 42);
    assert_eq!(a, b);

    // Bytes 0..4 = client_key, 4..6 = action_idx BE, 6..14 = seq LE,
    // 14..16 = b"AC".
    assert_eq!(&a.0[0..4], &[0xAA, 0xBB, 0xCC, 0xDD]);
    assert_eq!(&a.0[4..6], &[0x00, 0x07]);
    assert_eq!(&a.0[6..14], &42i64.to_le_bytes());
    assert_eq!(&a.0[14..16], b"AC");
}

#[test]
fn goal_id_from_seq_changes_with_inputs() {
    let base = GoalId::from_seq([0; 4], 0, 0);
    assert_ne!(base, GoalId::from_seq([1, 0, 0, 0], 0, 0));
    assert_ne!(base, GoalId::from_seq([0; 4], 1, 0));
    assert_ne!(base, GoalId::from_seq([0; 4], 0, 1));
}

#[test]
fn goal_status_constants_match_action_msgs() {
    // Sanity: numeric values match action_msgs/msg/GoalStatus.idl.
    assert_eq!(goal_status::STATUS_UNKNOWN, 0);
    assert_eq!(goal_status::STATUS_ACCEPTED, 1);
    assert_eq!(goal_status::STATUS_EXECUTING, 2);
    assert_eq!(goal_status::STATUS_CANCELING, 3);
    assert_eq!(goal_status::STATUS_SUCCEEDED, 4);
    assert_eq!(goal_status::STATUS_CANCELED, 5);
    assert_eq!(goal_status::STATUS_ABORTED, 6);
}
