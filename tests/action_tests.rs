//! Action-layer tests.
//!
//! v0.4 baseline (GoalId CDR + `goal_status` constants) plus v0.4-rc1 wire
//! shape coverage for the wrapper messages (`SendGoalRequest`,
//! `SendGoalResponse`, `GetResultRequest`, `GetResultResponse`,
//! `FeedbackMessage`) and the `Service` trait wiring on `SendGoalSrv` /
//! `GetResultSrv`.

use micro_xrce_dds_rs::{
    action::{
        cancel_response, goal_status, Action, CancelGoalRequest, CancelGoalResponse, CancelGoalSrv,
        FeedbackMessage, GetResultRequest, GetResultResponse, GetResultSrv, GoalId, GoalInfo,
        GoalStatus, GoalStatusArray, SendGoalRequest, SendGoalResponse, SendGoalResponseFor,
        SendGoalSrv, Time, MAX_CANCEL_GOALS, MAX_STATUS_GOALS,
    },
    cdr::CdrWriter,
    cdr_reader::CdrReader,
    msg::std_msgs::Float32,
    Message, Service,
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

// ── v0.4-rc1: stub Action whose Goal/Result/Feedback are all Float32 ─────────

struct Fib;
impl Action for Fib {
    type Goal = Float32;
    type Result = Float32;
    type Feedback = Float32;

    const ACTION_NAME: &'static str = "/fibonacci";
    const GOAL_TYPE_NAME: &'static str = "test::action::dds_::Fibonacci_Goal_";
    const RESULT_TYPE_NAME: &'static str = "test::action::dds_::Fibonacci_Result_";
    const FEEDBACK_TYPE_NAME: &'static str = "test::action::dds_::Fibonacci_Feedback_";

    const SEND_GOAL_SERVICE_NAME: &'static str = "/fibonacci/_action/send_goal";
    const GET_RESULT_SERVICE_NAME: &'static str = "/fibonacci/_action/get_result";
    const CANCEL_GOAL_SERVICE_NAME: &'static str = "/fibonacci/_action/cancel_goal";
    const FEEDBACK_TOPIC_NAME: &'static str = "/fibonacci/_action/feedback";
    const STATUS_TOPIC_NAME: &'static str = "/fibonacci/_action/status";

    const SEND_GOAL_REQUEST_TYPE_NAME: &'static str =
        "test::action::dds_::Fibonacci_SendGoal_Request_";
    const SEND_GOAL_RESPONSE_TYPE_NAME: &'static str =
        "test::action::dds_::Fibonacci_SendGoal_Response_";
    const GET_RESULT_REQUEST_TYPE_NAME: &'static str =
        "test::action::dds_::Fibonacci_GetResult_Request_";
    const GET_RESULT_RESPONSE_TYPE_NAME: &'static str =
        "test::action::dds_::Fibonacci_GetResult_Response_";
    const FEEDBACK_MESSAGE_TYPE_NAME: &'static str =
        "test::action::dds_::Fibonacci_FeedbackMessage_";
}

const TEST_GOAL_ID: GoalId = GoalId([
    0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10,
]);

// ── Test 5: Time CDR round-trip ──────────────────────────────────────────────

#[test]
fn time_cdr_round_trip() {
    let t = Time {
        sec: 1_700_000_000,
        nanosec: 123_456_789,
    };
    let mut buf = [0u8; 16];
    let n = {
        let mut w = CdrWriter::new(&mut buf);
        t.serialize(&mut w);
        w.bytes_written()
    };
    assert_eq!(n, Time::SERIALIZED_LEN);
    assert_eq!(n, 8);
    assert_eq!(&buf[0..4], &1_700_000_000i32.to_le_bytes());
    assert_eq!(&buf[4..8], &123_456_789u32.to_le_bytes());

    let mut r = CdrReader::from_body(&buf[..n]);
    let t2 = Time::deserialize(&mut r).unwrap();
    assert_eq!(t2, t);
}

// ── Test 6: SendGoalRequest<Fib> wire shape ──────────────────────────────────
//
// Layout: [GoalId 16 raw bytes][f32 goal LE 4 bytes] = 20 bytes.
//   - GoalId offset 0..16, no alignment.
//   - Float32 needs 4-aligned offset; offset 16 % 4 = 0 → no padding.
#[test]
fn send_goal_request_wire_shape() {
    let req = SendGoalRequest::<Fib> {
        goal_id: TEST_GOAL_ID,
        goal: Float32(2.5),
    };
    let mut buf = [0u8; 32];
    let n = {
        let mut w = CdrWriter::new(&mut buf);
        req.serialize(&mut w);
        w.bytes_written()
    };
    assert_eq!(n, 20);
    assert_eq!(&buf[0..16], &TEST_GOAL_ID.0);
    assert_eq!(&buf[16..20], &2.5f32.to_le_bytes());

    let mut r = CdrReader::from_body(&buf[..n]);
    let req2 = SendGoalRequest::<Fib>::deserialize(&mut r).unwrap();
    assert_eq!(req2.goal_id, TEST_GOAL_ID);
    assert_eq!(req2.goal.0, 2.5);

    assert_eq!(
        SendGoalRequest::<Fib>::TYPE_NAME,
        "test::action::dds_::Fibonacci_SendGoal_Request_"
    );
}

// ── Test 7: SendGoalResponseFor<Fib> wire shape ──────────────────────────────
//
// Layout: [bool 1][pad 3][i32 sec 4][u32 nanosec 4] = 12 bytes.
#[test]
fn send_goal_response_wire_shape() {
    let resp = SendGoalResponseFor::<Fib>::new(SendGoalResponse {
        accepted: true,
        stamp: Time {
            sec: 0x0102_0304,
            nanosec: 0xAABB_CCDD,
        },
    });
    let mut buf = [0u8; 16];
    let n = {
        let mut w = CdrWriter::new(&mut buf);
        resp.serialize(&mut w);
        w.bytes_written()
    };
    assert_eq!(n, 12);
    assert_eq!(buf[0], 0x01); // accepted = true
    assert_eq!(&buf[1..4], &[0, 0, 0]); // 3 bytes alignment pad
    assert_eq!(&buf[4..8], &0x0102_0304i32.to_le_bytes());
    assert_eq!(&buf[8..12], &0xAABB_CCDDu32.to_le_bytes());

    let mut r = CdrReader::from_body(&buf[..n]);
    let resp2 = SendGoalResponseFor::<Fib>::deserialize(&mut r).unwrap();
    assert!(resp2.inner.accepted);
    assert_eq!(resp2.inner.stamp.sec, 0x0102_0304);
    assert_eq!(resp2.inner.stamp.nanosec, 0xAABB_CCDD);

    assert_eq!(
        SendGoalResponseFor::<Fib>::TYPE_NAME,
        "test::action::dds_::Fibonacci_SendGoal_Response_"
    );
}

// ── Test 8: GetResultRequest<Fib> wire shape ─────────────────────────────────

#[test]
fn get_result_request_wire_shape() {
    let req = GetResultRequest::<Fib>::new(TEST_GOAL_ID);
    let mut buf = [0u8; 32];
    let n = {
        let mut w = CdrWriter::new(&mut buf);
        req.serialize(&mut w);
        w.bytes_written()
    };
    assert_eq!(n, 16);
    assert_eq!(&buf[..16], &TEST_GOAL_ID.0);

    let mut r = CdrReader::from_body(&buf[..n]);
    let req2 = GetResultRequest::<Fib>::deserialize(&mut r).unwrap();
    assert_eq!(req2.goal_id, TEST_GOAL_ID);
}

// ── Test 9: GetResultResponse<Fib> wire shape ────────────────────────────────
//
// Layout: [i8 status 1][pad 3 to 4-align Float32][f32 result] = 8 bytes.
#[test]
fn get_result_response_wire_shape() {
    let resp = GetResultResponse::<Fib> {
        status: goal_status::STATUS_SUCCEEDED,
        result: Float32(7.0),
    };
    let mut buf = [0u8; 16];
    let n = {
        let mut w = CdrWriter::new(&mut buf);
        resp.serialize(&mut w);
        w.bytes_written()
    };
    assert_eq!(n, 8);
    assert_eq!(buf[0], goal_status::STATUS_SUCCEEDED as u8);
    assert_eq!(&buf[1..4], &[0, 0, 0]); // alignment pad
    assert_eq!(&buf[4..8], &7.0f32.to_le_bytes());

    let mut r = CdrReader::from_body(&buf[..n]);
    let resp2 = GetResultResponse::<Fib>::deserialize(&mut r).unwrap();
    assert_eq!(resp2.status, goal_status::STATUS_SUCCEEDED);
    assert_eq!(resp2.result.0, 7.0);
}

// ── Test 10: FeedbackMessage<Fib> wire shape ─────────────────────────────────

#[test]
fn feedback_message_wire_shape() {
    let fb = FeedbackMessage::<Fib> {
        goal_id: TEST_GOAL_ID,
        feedback: Float32(0.25),
    };
    let mut buf = [0u8; 32];
    let n = {
        let mut w = CdrWriter::new(&mut buf);
        fb.serialize(&mut w);
        w.bytes_written()
    };
    assert_eq!(n, 20);
    assert_eq!(&buf[0..16], &TEST_GOAL_ID.0);
    assert_eq!(&buf[16..20], &0.25f32.to_le_bytes());
}

// ── Test 11: SendGoalSrv<A> / GetResultSrv<A> Service trait wiring ───────────
//
// Verifies the Service impls forward the per-action TYPE_NAME constants
// correctly so a `ServiceClient<SendGoalSrv<A>>` would address the right
// agent-side topics.
#[test]
fn action_service_traits_wire_type_names() {
    assert_eq!(
        <SendGoalSrv<Fib> as Service>::SERVICE_NAME,
        "/fibonacci/_action/send_goal"
    );
    assert_eq!(
        <SendGoalSrv<Fib> as Service>::REQUEST_TYPE_NAME,
        "test::action::dds_::Fibonacci_SendGoal_Request_"
    );
    assert_eq!(
        <SendGoalSrv<Fib> as Service>::RESPONSE_TYPE_NAME,
        "test::action::dds_::Fibonacci_SendGoal_Response_"
    );

    assert_eq!(
        <GetResultSrv<Fib> as Service>::SERVICE_NAME,
        "/fibonacci/_action/get_result"
    );
    assert_eq!(
        <GetResultSrv<Fib> as Service>::REQUEST_TYPE_NAME,
        "test::action::dds_::Fibonacci_GetResult_Request_"
    );
    assert_eq!(
        <GetResultSrv<Fib> as Service>::RESPONSE_TYPE_NAME,
        "test::action::dds_::Fibonacci_GetResult_Response_"
    );
}

// ── Test 12: ActionClientHandles<A> can live in a `static` ───────────────────

#[test]
fn action_client_handles_const_new() {
    static HANDLES: micro_xrce_dds_rs::ActionClientHandles<Fib> =
        micro_xrce_dds_rs::ActionClientHandles::new();
    // Prove const-init compiles and the goal sequence counter starts at 0.
    assert_eq!(HANDLES.goal_seq.load(portable_atomic::Ordering::Relaxed), 0);
    // Verify the inner service-client slots are reachable (field access).
    let _ = &HANDLES.send_goal.slot;
    let _ = &HANDLES.get_result.slot;
}

// ── Phase 2/3 — ActionClient / GoalHandle wiring ────────────────────────────

// We can't spin a real Runtime in a host test (the executor needs a transport),
// but we can verify the public types compose correctly: ActionClient is Copy,
// GoalHandle is Copy + holds the goal_id we expect, feedback filtering uses
// the goal_id discriminator, and the goal_seq counter is consumed in order.
//
// These tests stop short of an actual SendGoal RPC — that needs a live agent
// and is covered by v0.4-rc1 Phase 6 on-target validation.

use micro_xrce_dds_rs::{Subscription, SubscriptionSlot};

#[test]
fn action_client_is_copy_and_has_expected_size() {
    // Compile-time proof that ActionClient<A, FB_N> is Copy.
    fn assert_copy<T: Copy>() {}
    assert_copy::<micro_xrce_dds_rs::ActionClient<Fib, 4>>();
    assert_copy::<micro_xrce_dds_rs::GoalHandle<Fib, 4>>();
}

#[test]
fn feedback_subscription_routes_by_oid() {
    // FeedbackMessage<A> implements Message and can be the payload type of a
    // Subscription<M, N>.  Verify the slot dispatches correctly through the
    // SubscriptionSlot trait, which is what executor::dispatch_data calls.
    static FB: Subscription<FeedbackMessage<Fib>, 4> = Subscription::new();
    FB.set_dr_id(0x00A6); // (idx=10, kind=DATAREADER=6)

    // Build a feedback DATA payload: GoalId (16 raw) + Float32 (4 LE).
    let mut body = [0u8; 32];
    let n = {
        let mut w = CdrWriter::new(&mut body);
        let fb = FeedbackMessage::<Fib> {
            goal_id: TEST_GOAL_ID,
            feedback: Float32(1.5),
        };
        fb.serialize(&mut w);
        w.bytes_written()
    };
    assert_eq!(n, 20);

    // Route via the type-erased SubscriptionSlot trait, exactly as the
    // executor would.
    let slot: &'static dyn SubscriptionSlot = &FB;
    assert_eq!(slot.dr_id(), 0x00A6);
    slot.try_deliver(&body[..n]).unwrap();

    // The slot's inbox now holds the message — recv it as the GoalHandle's
    // next_feedback would.
    let received = FB.try_recv().unwrap();
    assert_eq!(received.goal_id, TEST_GOAL_ID);
    assert_eq!(received.feedback.0, 1.5);
}

#[test]
fn feedback_message_filter_drops_other_goals() {
    // Two feedback samples for different goals; mimic the
    // GoalHandle::next_feedback drop-loop logic (which the test invokes
    // directly because the real method requires a live runtime).
    static FB: Subscription<FeedbackMessage<Fib>, 4> = Subscription::new();
    FB.set_dr_id(0x00B6);

    let other_goal = GoalId([0u8; 16]);
    assert_ne!(other_goal, TEST_GOAL_ID);

    for &(gid, val) in &[(other_goal, 9.0f32), (TEST_GOAL_ID, 7.0f32)] {
        let mut buf = [0u8; 32];
        let n = {
            let mut w = CdrWriter::new(&mut buf);
            FeedbackMessage::<Fib> {
                goal_id: gid,
                feedback: Float32(val),
            }
            .serialize(&mut w);
            w.bytes_written()
        };
        FB.try_deliver(&buf[..n]).unwrap();
    }

    // Filter loop: discard everything that doesn't match TEST_GOAL_ID.
    let filtered = loop {
        let fb = FB.try_recv().unwrap();
        if fb.goal_id == TEST_GOAL_ID {
            break fb.feedback.0;
        }
    };
    assert_eq!(filtered, 7.0);
    assert!(FB.try_recv().is_none());
}

#[test]
fn goal_seq_counter_skips_zero_on_wraparound() {
    // The `send_goal` sequence-allocation loop must skip 0 because GoalId
    // derivation interprets seq=0 as "uninitialized".
    static HANDLES: micro_xrce_dds_rs::ActionClientHandles<Fib> =
        micro_xrce_dds_rs::ActionClientHandles::new();
    // Force the counter to i64::MAX so the next fetch_add wraps to MIN, then
    // the next one wraps to 0 — that one must be skipped.
    HANDLES
        .goal_seq
        .store(i64::MAX, portable_atomic::Ordering::Release);

    // Simulate the loop in ActionClient::send_goal (which is private).
    let mut produced: alloc_vec::Vec<i64> = alloc_vec::Vec::new();
    for _ in 0..3 {
        let n = loop {
            let prev = HANDLES
                .goal_seq
                .fetch_add(1, portable_atomic::Ordering::Relaxed);
            let n = prev.wrapping_add(1);
            if n != 0 {
                break n;
            }
        };
        produced.push(n);
    }
    // First call: i64::MAX + 1 = i64::MIN. Second: i64::MIN + 1. Third: i64::MIN + 2.
    // None should be 0.
    assert!(produced.iter().all(|&n| n != 0), "seq must skip 0");
    assert_eq!(produced[0], i64::MIN);
}

// ── Phase 4 — CancelGoal / GoalStatus / GoalStatusArray wire shapes ─────────

#[test]
fn goal_info_cdr_round_trip() {
    let gi = GoalInfo {
        goal_id: TEST_GOAL_ID,
        stamp: Time {
            sec: 0x4242_4242,
            nanosec: 0x9999_AAAA,
        },
    };
    let mut buf = [0u8; 32];
    let n = {
        let mut w = CdrWriter::new(&mut buf);
        gi.serialize(&mut w);
        w.bytes_written()
    };
    assert_eq!(n, GoalInfo::SERIALIZED_LEN);
    assert_eq!(n, 24);
    assert_eq!(&buf[0..16], &TEST_GOAL_ID.0);
    assert_eq!(&buf[16..20], &0x4242_4242i32.to_le_bytes());
    assert_eq!(&buf[20..24], &0x9999_AAAAu32.to_le_bytes());

    let mut r = CdrReader::from_body(&buf[..n]);
    let gi2 = GoalInfo::deserialize(&mut r).unwrap();
    assert_eq!(gi2, gi);
}

#[test]
fn cancel_goal_request_wire_shape() {
    let req = CancelGoalRequest {
        goal_info: GoalInfo {
            goal_id: TEST_GOAL_ID,
            stamp: Time::ZERO,
        },
    };
    let mut buf = [0u8; 32];
    let n = {
        let mut w = CdrWriter::new(&mut buf);
        req.serialize(&mut w);
        w.bytes_written()
    };
    assert_eq!(n, 24);
    assert_eq!(&buf[0..16], &TEST_GOAL_ID.0);
    assert_eq!(&buf[16..24], &[0u8; 8]);

    assert_eq!(
        CancelGoalRequest::TYPE_NAME,
        "action_msgs::srv::dds_::CancelGoal_Request_"
    );
}

#[test]
fn cancel_goal_response_empty_wire_shape() {
    // return_code=ERROR_NONE, no goals.
    // Layout: [i8 1][pad 3][u32 length=0] = 8 bytes.
    let resp = CancelGoalResponse::default();
    assert_eq!(resp.return_code, cancel_response::ERROR_NONE);
    assert!(resp.goals_canceling.is_empty());

    let mut buf = [0u8; 16];
    let n = {
        let mut w = CdrWriter::new(&mut buf);
        resp.serialize(&mut w);
        w.bytes_written()
    };
    assert_eq!(n, 8);
    assert_eq!(buf[0], 0);
    assert_eq!(&buf[1..4], &[0, 0, 0]);
    assert_eq!(&buf[4..8], &0u32.to_le_bytes());

    let mut r = CdrReader::from_body(&buf[..n]);
    let resp2 = CancelGoalResponse::deserialize(&mut r).unwrap();
    assert_eq!(resp2.return_code, cancel_response::ERROR_NONE);
    assert!(resp2.goals_canceling.is_empty());
}

#[test]
fn cancel_goal_response_with_two_goals_wire_shape() {
    // Two GoalInfo elements; each 24 bytes naturally aligned.
    // Layout: [i8 1][pad 3][u32 len=2][24][24] = 8 + 48 = 56 bytes.
    let mut goals: heapless::Vec<GoalInfo, MAX_CANCEL_GOALS> = heapless::Vec::new();
    goals
        .push(GoalInfo {
            goal_id: GoalId([1u8; 16]),
            stamp: Time {
                sec: 100,
                nanosec: 200,
            },
        })
        .ok();
    goals
        .push(GoalInfo {
            goal_id: GoalId([2u8; 16]),
            stamp: Time {
                sec: 300,
                nanosec: 400,
            },
        })
        .ok();
    let resp = CancelGoalResponse {
        return_code: cancel_response::ERROR_REJECTED,
        goals_canceling: goals,
    };

    let mut buf = [0u8; 64];
    let n = {
        let mut w = CdrWriter::new(&mut buf);
        resp.serialize(&mut w);
        w.bytes_written()
    };
    assert_eq!(n, 56);
    assert_eq!(buf[0], cancel_response::ERROR_REJECTED as u8);
    assert_eq!(&buf[4..8], &2u32.to_le_bytes());
    assert_eq!(&buf[8..24], &[1u8; 16]);
    assert_eq!(&buf[24..28], &100i32.to_le_bytes());
    assert_eq!(&buf[28..32], &200u32.to_le_bytes());
    assert_eq!(&buf[32..48], &[2u8; 16]);
    assert_eq!(&buf[48..52], &300i32.to_le_bytes());
    assert_eq!(&buf[52..56], &400u32.to_le_bytes());

    // Round-trip
    let mut r = CdrReader::from_body(&buf[..n]);
    let resp2 = CancelGoalResponse::deserialize(&mut r).unwrap();
    assert_eq!(resp2.return_code, cancel_response::ERROR_REJECTED);
    assert_eq!(resp2.goals_canceling.len(), 2);
    assert_eq!(resp2.goals_canceling[0].goal_id, GoalId([1u8; 16]));
    assert_eq!(resp2.goals_canceling[1].stamp.nanosec, 400);
}

#[test]
fn cancel_goal_response_overflow_rejected() {
    // Manually craft a response claiming MAX_CANCEL_GOALS + 1 elements.
    let claimed = (MAX_CANCEL_GOALS + 1) as u32;
    let mut buf = [0u8; 16];
    {
        let mut w = CdrWriter::new(&mut buf);
        w.u8_val(0);
        w.u32_val(claimed);
    }
    let mut r = CdrReader::from_body(&buf);
    assert!(CancelGoalResponse::deserialize(&mut r).is_err());
}

#[test]
fn goal_status_array_wire_shape() {
    let mut list: heapless::Vec<GoalStatus, MAX_STATUS_GOALS> = heapless::Vec::new();
    list.push(GoalStatus {
        goal_info: GoalInfo {
            goal_id: GoalId([0xAA; 16]),
            stamp: Time { sec: 7, nanosec: 8 },
        },
        status: goal_status::STATUS_EXECUTING,
    })
    .ok();

    let arr = GoalStatusArray { status_list: list };

    let mut buf = [0u8; 64];
    let n = {
        let mut w = CdrWriter::new(&mut buf);
        arr.serialize(&mut w);
        w.bytes_written()
    };
    // [u32 length=1] + GoalInfo (24) + i8 status (1) = 4 + 25 = 29 bytes.
    assert_eq!(n, 29);
    assert_eq!(&buf[0..4], &1u32.to_le_bytes());
    assert_eq!(&buf[4..20], &[0xAA; 16]);
    assert_eq!(&buf[20..24], &7i32.to_le_bytes());
    assert_eq!(&buf[24..28], &8u32.to_le_bytes());
    assert_eq!(buf[28], goal_status::STATUS_EXECUTING as u8);

    let mut r = CdrReader::from_body(&buf[..n]);
    let arr2 = GoalStatusArray::deserialize(&mut r).unwrap();
    assert_eq!(arr2.status_list.len(), 1);
    assert_eq!(arr2.status_list[0].status, goal_status::STATUS_EXECUTING);
    assert_eq!(arr2.status_list[0].goal_info.goal_id, GoalId([0xAA; 16]));
}

#[test]
fn cancel_goal_srv_service_wiring() {
    // Verify the CancelGoalSrv<A> Service impl forwards the per-action service
    // name and the shared action_msgs type names.
    assert_eq!(
        <CancelGoalSrv<Fib> as Service>::SERVICE_NAME,
        "/fibonacci/_action/cancel_goal"
    );
    assert_eq!(
        <CancelGoalSrv<Fib> as Service>::REQUEST_TYPE_NAME,
        "action_msgs::srv::dds_::CancelGoal_Request_"
    );
    assert_eq!(
        <CancelGoalSrv<Fib> as Service>::RESPONSE_TYPE_NAME,
        "action_msgs::srv::dds_::CancelGoal_Response_"
    );
}

#[test]
fn cdr_i32_seq_round_trip() {
    // Verify the new sequence helpers in cdr.rs / cdr_reader.rs.
    let items = [3i32, -2, 7, 0, 100];
    let mut buf = [0u8; 32];
    let n = {
        let mut w = CdrWriter::new(&mut buf);
        w.i32_seq(&items);
        w.bytes_written()
    };
    // 4 bytes length + 5 * 4 = 24.
    assert_eq!(n, 24);
    assert_eq!(&buf[0..4], &5u32.to_le_bytes());
    assert_eq!(&buf[4..8], &3i32.to_le_bytes());
    assert_eq!(&buf[20..24], &100i32.to_le_bytes());

    let mut r = CdrReader::from_body(&buf[..n]);
    let v: heapless::Vec<i32, 8> = r.i32_seq_into::<8>().unwrap();
    assert_eq!(v.as_slice(), &items[..]);
}

// ── Phase 5 — ActionServer scaffolding (compile-time wiring checks) ─────────

#[test]
fn action_server_handles_const_new_and_copy_semantics() {
    // ActionServer is built at runtime by Node::create_action_server, but its
    // *Handles* bundle is const-constructible.  Make sure all three slots
    // initialize cleanly at link time.
    static SRV: micro_xrce_dds_rs::ActionServerHandles<Fib, 4, 4, 4> =
        micro_xrce_dds_rs::ActionServerHandles::new();
    // Slots default to dr_id == 0 (uninitialized).  Set markers and read back
    // through the SubscriptionSlot trait, to confirm trait routing works.
    use micro_xrce_dds_rs::SubscriptionSlot;
    SRV.send_goal.set_replier_oid(0x0028);
    SRV.get_result.set_replier_oid(0x0038);
    SRV.cancel_goal.set_replier_oid(0x0048);
    let table: [&'static dyn SubscriptionSlot; 3] =
        [&SRV.send_goal, &SRV.get_result, &SRV.cancel_goal];
    assert_eq!(table[0].dr_id(), 0x0028);
    assert_eq!(table[1].dr_id(), 0x0038);
    assert_eq!(table[2].dr_id(), 0x0048);
}

#[test]
fn action_server_types_are_copy() {
    fn assert_copy<T: Copy>() {}
    assert_copy::<micro_xrce_dds_rs::ActionServer<Fib, 4, 4, 4>>();
}

// ── ActiveGoalCancelState lock-free primitives ──────────────────────────────

#[test]
fn active_goal_cancel_state_round_trip() {
    use micro_xrce_dds_rs::ActiveGoalCancelState;

    let s = ActiveGoalCancelState::new();
    assert!(!s.is_cancel_requested());
    assert!(!s.matches(&TEST_GOAL_ID));

    s.set_active(TEST_GOAL_ID);
    assert!(s.matches(&TEST_GOAL_ID));
    // A different goal_id must not match.
    assert!(!s.matches(&GoalId([0xFFu8; 16])));

    // Cancel flag transitions.
    assert!(!s.is_cancel_requested());
    s.request_cancel();
    assert!(s.is_cancel_requested());

    // set_active resets the cancel flag.
    s.set_active(GoalId([0x99u8; 16]));
    assert!(!s.is_cancel_requested());

    // clear_active wipes both halves.
    s.clear_active();
    assert!(!s.matches(&GoalId([0x99u8; 16])));
}

#[test]
fn action_server_handles_includes_cancel_state() {
    static H: micro_xrce_dds_rs::ActionServerHandles<Fib, 4, 4, 4> =
        micro_xrce_dds_rs::ActionServerHandles::new();
    // Default state: no active goal.
    assert!(!H.cancel_state.is_cancel_requested());
    assert!(!H.cancel_state.matches(&TEST_GOAL_ID));
}

// Tiny stand-in for std::vec since the dev-deps don't pull alloc.
mod alloc_vec {
    pub struct Vec<T> {
        buf: [Option<T>; 8],
        len: usize,
    }
    impl<T> Vec<T> {
        pub fn new() -> Self {
            Self {
                buf: [const { None }; 8],
                len: 0,
            }
        }
        pub fn push(&mut self, v: T) {
            self.buf[self.len] = Some(v);
            self.len += 1;
        }
        pub fn iter(&self) -> impl Iterator<Item = &T> {
            self.buf[..self.len].iter().filter_map(|s| s.as_ref())
        }
    }
    impl<T: Copy> core::ops::Index<usize> for Vec<T> {
        type Output = T;
        fn index(&self, i: usize) -> &T {
            self.buf[i].as_ref().unwrap()
        }
    }
}
