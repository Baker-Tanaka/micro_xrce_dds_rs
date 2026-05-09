//! Subscription dispatch infrastructure tests.
//!
//! Verifies that the subscription slot machinery (`set_dr_id`, `try_deliver`,
//! `try_recv`) routes incoming CDR payloads to the correct slot and rejects
//! payloads that belong to a different DataReader.
//!
//! These tests exercise the path the Executor's `dispatch_data` function
//! uses — without requiring a live TCP transport.

use micro_xrce_dds_rs::{
    cdr::CdrWriter, msg::std_msgs::Float32, Message, Subscription, SubscriptionSlot,
};

// Static subscription slots — must be 'static for SubscriptionSlot dispatch.
static SUB_A: Subscription<Float32> = Subscription::new();
static SUB_B: Subscription<Float32> = Subscription::new();
static SUB_FULL: Subscription<Float32, 1> = Subscription::new();

// ── Helper: serialize a Float32 to a CDR body byte array ─────────────────────

fn float32_cdr(v: f32) -> [u8; 4] {
    let mut body = [0u8; 4];
    let mut w = CdrWriter::new(&mut body);
    Float32(v).serialize(&mut w);
    body
}

// ── Test 1: try_deliver routes to the matching slot ──────────────────────────

#[test]
fn deliver_routes_to_correct_slot() {
    SUB_A.set_dr_id(0x0016); // dr_oid for DataReader idx=1
    SUB_B.set_dr_id(0x0026); // dr_oid for DataReader idx=2

    let payload = float32_cdr(42.0);

    // Deliver to SUB_A only.
    assert!(SUB_A.try_deliver(&payload).is_ok());

    assert_eq!(SUB_A.try_recv().unwrap().0, 42.0f32);
    assert!(SUB_B.try_recv().is_none());
}

// ── Test 2: dr_id accessor reflects set value ────────────────────────────────

#[test]
fn dr_id_returns_set_value() {
    static S: Subscription<Float32> = Subscription::new();
    assert_eq!(S.dr_id(), 0); // initial value
    S.set_dr_id(0x1234);
    assert_eq!(S.dr_id(), 0x1234);
}

// ── Test 3: multiple deliveries fill inbox in order ──────────────────────────

#[test]
fn multiple_deliveries_fifo_order() {
    static MULTI: Subscription<Float32, 4> = Subscription::new();
    MULTI.set_dr_id(0x0036);

    for i in 0..4u32 {
        assert!(MULTI.try_deliver(&float32_cdr(i as f32)).is_ok());
    }
    for i in 0..4u32 {
        assert_eq!(MULTI.try_recv().unwrap().0, i as f32);
    }
    assert!(MULTI.try_recv().is_none());
}

// ── Test 4: inbox overflow returns SubscriptionOverflow ──────────────────────

#[test]
fn overflow_when_inbox_full() {
    SUB_FULL.set_dr_id(0x0046);
    let payload = float32_cdr(1.0);

    // First delivery fills the depth-1 inbox.
    assert!(SUB_FULL.try_deliver(&payload).is_ok());
    // Second delivery should overflow.
    assert!(SUB_FULL.try_deliver(&payload).is_err());

    // Original message is still retrievable.
    assert_eq!(SUB_FULL.try_recv().unwrap().0, 1.0f32);
}

// ── Test 5: subscription_slot! macro creates a usable static slot ────────────

#[test]
fn subscription_slot_macro_creates_usable_slot() {
    use micro_xrce_dds_rs::subscription_slot;

    subscription_slot!(static MY_SLOT: Float32);
    subscription_slot!(static MY_SLOT4: Float32, depth = 4);

    MY_SLOT.set_dr_id(0x0056);
    MY_SLOT4.set_dr_id(0x0066);

    assert!(MY_SLOT.try_deliver(&float32_cdr(7.0)).is_ok());
    assert_eq!(MY_SLOT.try_recv().unwrap().0, 7.0f32);

    assert!(MY_SLOT4.try_deliver(&float32_cdr(8.0)).is_ok());
    assert_eq!(MY_SLOT4.try_recv().unwrap().0, 8.0f32);
}
