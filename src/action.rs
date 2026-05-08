//! ROS2 Action support — **v0.4-rc1 wire types and Service composition**.
//!
//! A ROS2 action decomposes into **3 services** and **2 topics**:
//!
//! | Component        | DDS entity                                                  |
//! | ---------------- | ----------------------------------------------------------- |
//! | `send_goal`      | service `<action>/_action/send_goal`                        |
//! | `cancel_goal`    | service `<action>/_action/cancel_goal`                      |
//! | `get_result`     | service `<action>/_action/get_result`                       |
//! | `feedback`       | topic   `<action>/_action/feedback`                         |
//! | `status`         | topic   `<action>/_action/status`                           |
//!
//! v0.4-rc1 ships:
//! - The wrapper messages every Action shares ([`SendGoalRequest`],
//!   [`SendGoalResponse`], [`GetResultRequest`], [`GetResultResponse`],
//!   [`FeedbackMessage`]) with full CDR (de)serialization.
//! - The [`Action`] trait extended with the per-entity DDS type names.
//! - [`SendGoalSrv`] / [`GetResultSrv`] — ZSTs that implement
//!   [`crate::Service`] so an `ActionClient` can be assembled from two
//!   `ServiceClient`s plus a feedback subscription.
//! - [`ActionClientHandles`] — a `'static` bundle holding the
//!   `ServiceClientHandles` for both inner services so the caller writes one
//!   `static` per action client.
//!
//! End-to-end glue (`Node::create_action_client`, full `ActionClient::send_goal`
//! composition, `ActionServer`) is still pending — once the wrappers and the
//! Service impls are stable, the remaining work is plumbing.

use core::marker::PhantomData;

use portable_atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};

use crate::{
    cdr::CdrWriter,
    cdr_reader::CdrReader,
    error::Error,
    message::Message,
    rt::Context,
    service::{Service, ServiceClient, ServiceClientHandles},
    subscription::Subscription,
};

// ── Action trait ──────────────────────────────────────────────────────────────

/// Defines a ROS2 action by its goal / result / feedback message types and
/// the rosidl-generated DDS type names of every entity in the composition.
///
/// All `*_TYPE_NAME` constants follow the rosidl convention
/// `<pkg>::action::dds_::<Action>_<Component>_`.
///
/// The five entity-name constants
/// (`SEND_GOAL_SERVICE_NAME`, `GET_RESULT_SERVICE_NAME`, `CANCEL_GOAL_SERVICE_NAME`,
/// `FEEDBACK_TOPIC_NAME`, `STATUS_TOPIC_NAME`) avoid runtime string
/// concatenation — set them at the `<action>/_action/{send_goal,...}` form
/// expected by ROS2 actions.
pub trait Action: 'static {
    type Goal: Message + Send + 'static;
    type Result: Message + Send + 'static;
    type Feedback: Message + Send + 'static;

    /// ROS action namespace, e.g. `"/fibonacci"`.
    const ACTION_NAME: &'static str;

    /// rosidl-generated DDS type name of the goal payload.
    const GOAL_TYPE_NAME: &'static str;
    /// rosidl-generated DDS type name of the result payload.
    const RESULT_TYPE_NAME: &'static str;
    /// rosidl-generated DDS type name of the feedback payload.
    const FEEDBACK_TYPE_NAME: &'static str;

    // ── Composition: ROS service / topic names ────────────────────────────────

    /// Full ROS service name, e.g. `"/fibonacci/_action/send_goal"`.
    const SEND_GOAL_SERVICE_NAME: &'static str;
    /// Full ROS service name, e.g. `"/fibonacci/_action/get_result"`.
    const GET_RESULT_SERVICE_NAME: &'static str;
    /// Full ROS service name, e.g. `"/fibonacci/_action/cancel_goal"`.
    const CANCEL_GOAL_SERVICE_NAME: &'static str;
    /// Full ROS topic name, e.g. `"/fibonacci/_action/feedback"`.
    const FEEDBACK_TOPIC_NAME: &'static str;
    /// Full ROS topic name, e.g. `"/fibonacci/_action/status"`.
    const STATUS_TOPIC_NAME: &'static str;

    // ── Composition: rosidl DDS type names ────────────────────────────────────

    /// `<pkg>::action::dds_::<Action>_SendGoal_Request_`
    const SEND_GOAL_REQUEST_TYPE_NAME: &'static str;
    /// `<pkg>::action::dds_::<Action>_SendGoal_Response_`
    const SEND_GOAL_RESPONSE_TYPE_NAME: &'static str;
    /// `<pkg>::action::dds_::<Action>_GetResult_Request_`
    const GET_RESULT_REQUEST_TYPE_NAME: &'static str;
    /// `<pkg>::action::dds_::<Action>_GetResult_Response_`
    const GET_RESULT_RESPONSE_TYPE_NAME: &'static str;
    /// `<pkg>::action::dds_::<Action>_FeedbackMessage_`
    const FEEDBACK_MESSAGE_TYPE_NAME: &'static str;
}

// ── GoalId ────────────────────────────────────────────────────────────────────

/// 16-byte UUID identifying one in-flight goal.
///
/// On the wire this is `unique_identifier_msgs::msg::UUID` — a fixed-length
/// `octet[16]` array, no length prefix, no alignment padding.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GoalId(pub [u8; 16]);

impl GoalId {
    /// Length of the CDR-serialised form (16 bytes, raw).
    pub const SERIALIZED_LEN: usize = 16;

    /// Build a `GoalId` from a 4-byte client_key, 2-byte requester oid and
    /// an i64 monotonic sequence.  This matches the convention used by the
    /// service layer's `derive_writer_guid`, so action GoalIds and service
    /// SampleIdentities cannot collide within a single client.
    pub const fn from_seq(client_key: [u8; 4], action_idx: u16, seq: i64) -> Self {
        let s = seq.to_le_bytes();
        Self([
            client_key[0],
            client_key[1],
            client_key[2],
            client_key[3],
            (action_idx >> 8) as u8,
            action_idx as u8,
            s[0],
            s[1],
            s[2],
            s[3],
            s[4],
            s[5],
            s[6],
            s[7],
            b'A',
            b'C',
        ])
    }

    pub fn serialize(&self, w: &mut CdrWriter) {
        w.bytes_raw(&self.0);
    }

    pub fn deserialize(r: &mut CdrReader) -> Result<Self, Error> {
        Ok(Self(r.bytes_array::<16>()?))
    }
}

#[cfg(feature = "defmt")]
impl defmt::Format for GoalId {
    fn format(&self, f: defmt::Formatter) {
        defmt::write!(f, "GoalId({:02x})", self.0);
    }
}

// ── Time (builtin_interfaces::msg::Time) ─────────────────────────────────────

/// `builtin_interfaces::msg::Time` — i32 seconds + u32 nanoseconds.
///
/// Both fields are 4-aligned scalars; total CDR size is 8 bytes when
/// serialised at a 4-byte-aligned offset.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Time {
    pub sec: i32,
    pub nanosec: u32,
}

impl Time {
    pub const SERIALIZED_LEN: usize = 8;
    pub const ZERO: Self = Self { sec: 0, nanosec: 0 };

    pub fn serialize(&self, w: &mut CdrWriter) {
        w.i32_val(self.sec);
        w.u32_val(self.nanosec);
    }

    pub fn deserialize(r: &mut CdrReader) -> Result<Self, Error> {
        let sec = r.i32_val()?;
        let nanosec = r.u32_val()?;
        Ok(Self { sec, nanosec })
    }
}

// ── GoalInfo (action_msgs::msg::GoalInfo) ────────────────────────────────────

/// `action_msgs/msg/GoalInfo.msg` — `unique_identifier_msgs/UUID goal_id` +
/// `builtin_interfaces/Time stamp`.
///
/// CDR layout (24 bytes when written at a 4-byte-aligned offset):
/// `[goal_id 16 raw][stamp.sec 4 LE][stamp.nanosec 4 LE]`.  The 16-byte
/// `goal_id` keeps any 4-byte aligned starting position aligned for the
/// following `i32`, so no internal padding.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GoalInfo {
    pub goal_id: GoalId,
    pub stamp: Time,
}

impl GoalInfo {
    pub const SERIALIZED_LEN: usize = 24;

    pub fn serialize(&self, w: &mut CdrWriter) {
        self.goal_id.serialize(w);
        self.stamp.serialize(w);
    }

    pub fn deserialize(r: &mut CdrReader) -> Result<Self, Error> {
        let goal_id = GoalId::deserialize(r)?;
        let stamp = Time::deserialize(r)?;
        Ok(Self { goal_id, stamp })
    }
}

// ── Sequence capacity bounds ─────────────────────────────────────────────────

/// Maximum number of `GoalInfo` entries inside one `CancelGoalResponse`.
pub const MAX_CANCEL_GOALS: usize = 4;

/// Maximum number of `GoalStatus` entries inside one `GoalStatusArray`.
pub const MAX_STATUS_GOALS: usize = 8;

// ── GoalStatus codes ──────────────────────────────────────────────────────────

/// Status code constants from `action_msgs/msg/GoalStatus.idl`.  Use as `i8`
/// values inside `GoalStatusArray.status_list[].status`.
pub mod goal_status {
    pub const STATUS_UNKNOWN: i8 = 0;
    pub const STATUS_ACCEPTED: i8 = 1;
    pub const STATUS_EXECUTING: i8 = 2;
    pub const STATUS_CANCELING: i8 = 3;
    pub const STATUS_SUCCEEDED: i8 = 4;
    pub const STATUS_CANCELED: i8 = 5;
    pub const STATUS_ABORTED: i8 = 6;
}

// ── CancelGoal response codes ─────────────────────────────────────────────────

/// Return codes from `action_msgs/srv/CancelGoal.idl` (`Response.return_code`).
pub mod cancel_response {
    pub const ERROR_NONE: i8 = 0;
    pub const ERROR_REJECTED: i8 = 1;
    pub const ERROR_UNKNOWN_GOAL_ID: i8 = 2;
    pub const ERROR_GOAL_TERMINATED: i8 = 3;
}

// ── GetResult response codes ──────────────────────────────────────────────────

/// Return codes from `action_msgs/msg/GoalInfo.idl` style — used by the
/// `<Action>_GetResult_Response_` `status` field.
pub mod get_result_status {
    pub const STATUS_UNKNOWN: i8 = 0;
    pub const STATUS_ACCEPTED: i8 = 1;
    pub const STATUS_EXECUTING: i8 = 2;
    pub const STATUS_CANCELING: i8 = 3;
    pub const STATUS_SUCCEEDED: i8 = 4;
    pub const STATUS_CANCELED: i8 = 5;
    pub const STATUS_ABORTED: i8 = 6;
}

// ── Wrapper message: SendGoalRequest<A> ──────────────────────────────────────

/// `<Action>_SendGoal_Request_` wire shape: `goal_id` (16 bytes raw) followed
/// by the user goal payload.
pub struct SendGoalRequest<A: Action> {
    pub goal_id: GoalId,
    pub goal: A::Goal,
}

impl<A: Action> Message for SendGoalRequest<A> {
    const TYPE_NAME: &'static str = A::SEND_GOAL_REQUEST_TYPE_NAME;
    const MAX_SERIALIZED_SIZE: usize = GoalId::SERIALIZED_LEN + A::Goal::MAX_SERIALIZED_SIZE;

    fn serialize(&self, w: &mut CdrWriter) {
        self.goal_id.serialize(w);
        self.goal.serialize(w);
    }

    fn deserialize(r: &mut CdrReader<'_>) -> Result<Self, Error> {
        let goal_id = GoalId::deserialize(r)?;
        let goal = A::Goal::deserialize(r)?;
        Ok(Self { goal_id, goal })
    }
}

// ── Wrapper message: SendGoalResponse ────────────────────────────────────────

/// `<Action>_SendGoal_Response_` wire shape: `accepted` boolean (1 byte) then
/// 3 bytes of CDR alignment padding then a [`Time`] stamp.
///
/// Note: the ROS message is **not** generic — every action's SendGoal response
/// has the same fields, so this is a concrete struct.
pub struct SendGoalResponse {
    pub accepted: bool,
    pub stamp: Time,
}

/// Type-name shim used when constructing `Service` wrappers — see
/// [`SendGoalResponseFor`].
pub trait SendGoalResponseTypeName {
    const TYPE_NAME: &'static str;
}

/// Type-name-bound wrapper around [`SendGoalResponse`].  Carries the
/// per-action TYPE_NAME so the inner [`SendGoalSrv<A>`] `Service` impl
/// can hand it to the agent.
pub struct SendGoalResponseFor<A: Action> {
    pub inner: SendGoalResponse,
    _phantom: PhantomData<fn() -> A>,
}

impl<A: Action> SendGoalResponseFor<A> {
    pub fn new(inner: SendGoalResponse) -> Self {
        Self {
            inner,
            _phantom: PhantomData,
        }
    }
}

impl<A: Action> Message for SendGoalResponseFor<A> {
    const TYPE_NAME: &'static str = A::SEND_GOAL_RESPONSE_TYPE_NAME;
    /// 1 byte (`accepted`) + up to 3 bytes pad + 8 bytes (`Time`) = 12 bytes.
    const MAX_SERIALIZED_SIZE: usize = 12;

    fn serialize(&self, w: &mut CdrWriter) {
        w.bool_val(self.inner.accepted);
        // The next field (Time.sec, i32) needs 4-byte alignment from the
        // body origin.  CdrWriter::i32_val pads automatically.
        self.inner.stamp.serialize(w);
    }

    fn deserialize(r: &mut CdrReader<'_>) -> Result<Self, Error> {
        let accepted = r.bool_val()?;
        let stamp = Time::deserialize(r)?;
        Ok(Self {
            inner: SendGoalResponse { accepted, stamp },
            _phantom: PhantomData,
        })
    }
}

// ── Wrapper message: GetResultRequest ────────────────────────────────────────

/// `<Action>_GetResult_Request_` wire shape: just `goal_id` (16 bytes raw).
pub struct GetResultRequest<A: Action> {
    pub goal_id: GoalId,
    _phantom: PhantomData<fn() -> A>,
}

impl<A: Action> GetResultRequest<A> {
    pub fn new(goal_id: GoalId) -> Self {
        Self {
            goal_id,
            _phantom: PhantomData,
        }
    }
}

impl<A: Action> Message for GetResultRequest<A> {
    const TYPE_NAME: &'static str = A::GET_RESULT_REQUEST_TYPE_NAME;
    const MAX_SERIALIZED_SIZE: usize = GoalId::SERIALIZED_LEN;

    fn serialize(&self, w: &mut CdrWriter) {
        self.goal_id.serialize(w);
    }

    fn deserialize(r: &mut CdrReader<'_>) -> Result<Self, Error> {
        Ok(Self::new(GoalId::deserialize(r)?))
    }
}

// ── Wrapper message: GetResultResponse<A> ────────────────────────────────────

/// `<Action>_GetResult_Response_` wire shape: `status` (i8, 1 byte) followed
/// by the user result payload.  The user payload's first field's alignment
/// determines the padding after `status`.
pub struct GetResultResponse<A: Action> {
    pub status: i8,
    pub result: A::Result,
}

impl<A: Action> Message for GetResultResponse<A> {
    const TYPE_NAME: &'static str = A::GET_RESULT_RESPONSE_TYPE_NAME;
    const MAX_SERIALIZED_SIZE: usize = 1 + 7 /* worst-case pad */ + A::Result::MAX_SERIALIZED_SIZE;

    fn serialize(&self, w: &mut CdrWriter) {
        // status is a CDR `int8` — same width and alignment as `octet`.
        w.u8_val(self.status as u8);
        self.result.serialize(w);
    }

    fn deserialize(r: &mut CdrReader<'_>) -> Result<Self, Error> {
        let status = r.u8_val()? as i8;
        let result = A::Result::deserialize(r)?;
        Ok(Self { status, result })
    }
}

// ── Wrapper message: FeedbackMessage<A> ──────────────────────────────────────

/// `<Action>_FeedbackMessage_` wire shape: `goal_id` (16 bytes raw) followed
/// by the user feedback payload.
pub struct FeedbackMessage<A: Action> {
    pub goal_id: GoalId,
    pub feedback: A::Feedback,
}

impl<A: Action> Message for FeedbackMessage<A> {
    const TYPE_NAME: &'static str = A::FEEDBACK_MESSAGE_TYPE_NAME;
    const MAX_SERIALIZED_SIZE: usize = GoalId::SERIALIZED_LEN + A::Feedback::MAX_SERIALIZED_SIZE;

    fn serialize(&self, w: &mut CdrWriter) {
        self.goal_id.serialize(w);
        self.feedback.serialize(w);
    }

    fn deserialize(r: &mut CdrReader<'_>) -> Result<Self, Error> {
        let goal_id = GoalId::deserialize(r)?;
        let feedback = A::Feedback::deserialize(r)?;
        Ok(Self { goal_id, feedback })
    }
}

// ── Wrapper message: CancelGoalRequest ───────────────────────────────────────

/// `action_msgs/srv/CancelGoal_Request_` — a single `GoalInfo`.
///
/// The DDS type name is **not** generic over `A` (CancelGoal is shared across
/// all actions), so this is a plain struct.  Its [`Message`] impl always
/// reports the rosidl-canonical name `action_msgs::srv::dds_::CancelGoal_Request_`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CancelGoalRequest {
    pub goal_info: GoalInfo,
}

impl Message for CancelGoalRequest {
    const TYPE_NAME: &'static str = "action_msgs::srv::dds_::CancelGoal_Request_";
    const MAX_SERIALIZED_SIZE: usize = GoalInfo::SERIALIZED_LEN;

    fn serialize(&self, w: &mut CdrWriter) {
        self.goal_info.serialize(w);
    }

    fn deserialize(r: &mut CdrReader<'_>) -> Result<Self, Error> {
        Ok(Self {
            goal_info: GoalInfo::deserialize(r)?,
        })
    }
}

// ── Wrapper message: CancelGoalResponse ──────────────────────────────────────

/// `action_msgs/srv/CancelGoal_Response_` — `int8 return_code` followed by a
/// `sequence<GoalInfo>` capped at [`MAX_CANCEL_GOALS`].
///
/// CDR layout: `[return_code 1][pad 3][len u32][GoalInfo × len]`.  Each
/// GoalInfo element starts at a 4-byte-aligned offset because the writer
/// aligns before the `i32 sec` field of `Time`.
pub struct CancelGoalResponse {
    pub return_code: i8,
    pub goals_canceling: heapless::Vec<GoalInfo, MAX_CANCEL_GOALS>,
}

impl Default for CancelGoalResponse {
    fn default() -> Self {
        Self {
            return_code: cancel_response::ERROR_NONE,
            goals_canceling: heapless::Vec::new(),
        }
    }
}

impl Message for CancelGoalResponse {
    const TYPE_NAME: &'static str = "action_msgs::srv::dds_::CancelGoal_Response_";
    /// 1 byte (`return_code`) + 3 pad + 4 (length) + 24 × MAX = 8 + 96 = 104.
    const MAX_SERIALIZED_SIZE: usize = 8 + GoalInfo::SERIALIZED_LEN * MAX_CANCEL_GOALS;

    fn serialize(&self, w: &mut CdrWriter) {
        w.u8_val(self.return_code as u8);
        // u32_val (length) auto-aligns to 4 → 3 pad bytes after the i8.
        w.u32_val(self.goals_canceling.len() as u32);
        for gi in self.goals_canceling.iter() {
            // Each GoalInfo's first 4-byte-aligned member is `Time.sec`,
            // which is at element_start + 16.  When element_start is itself
            // 4-aligned, no extra alignment is needed before the goal_id.
            // align_to(4) is a no-op the first time (we just wrote a u32);
            // safe to call before each element regardless.
            w.align_to(4);
            gi.serialize(w);
        }
    }

    fn deserialize(r: &mut CdrReader<'_>) -> Result<Self, Error> {
        let return_code = r.u8_val()? as i8;
        let n = r.u32_val()? as usize;
        if n > MAX_CANCEL_GOALS {
            return Err(Error::Deserialization);
        }
        let mut goals_canceling: heapless::Vec<GoalInfo, MAX_CANCEL_GOALS> = heapless::Vec::new();
        for _ in 0..n {
            r.align_to(4);
            let gi = GoalInfo::deserialize(r)?;
            goals_canceling.push(gi).ok();
        }
        Ok(Self {
            return_code,
            goals_canceling,
        })
    }
}

// ── Wrapper message: GoalStatus / GoalStatusArray ────────────────────────────

/// `action_msgs/msg/GoalStatus.msg` — `GoalInfo` + `int8 status`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GoalStatus {
    pub goal_info: GoalInfo,
    pub status: i8,
}

impl GoalStatus {
    /// 24 (GoalInfo) + 1 (status) = 25 bytes — but inside a sequence each
    /// element is padded to a 4-byte boundary because GoalInfo's max-aligned
    /// member is `i32`.  Use 28 when sizing buffers conservatively.
    pub const SERIALIZED_LEN: usize = 25;

    pub fn serialize(&self, w: &mut CdrWriter) {
        self.goal_info.serialize(w);
        w.u8_val(self.status as u8);
    }

    pub fn deserialize(r: &mut CdrReader) -> Result<Self, Error> {
        let goal_info = GoalInfo::deserialize(r)?;
        let status = r.u8_val()? as i8;
        Ok(Self { goal_info, status })
    }
}

/// `action_msgs/msg/GoalStatusArray.msg` — `sequence<GoalStatus>` capped at
/// [`MAX_STATUS_GOALS`].
pub struct GoalStatusArray {
    pub status_list: heapless::Vec<GoalStatus, MAX_STATUS_GOALS>,
}

impl Default for GoalStatusArray {
    fn default() -> Self {
        Self {
            status_list: heapless::Vec::new(),
        }
    }
}

impl Message for GoalStatusArray {
    const TYPE_NAME: &'static str = "action_msgs::msg::dds_::GoalStatusArray_";
    /// 4 (length) + 28 × MAX (per-element with trailing pad) = 4 + 224 = 228.
    const MAX_SERIALIZED_SIZE: usize = 4 + 28 * MAX_STATUS_GOALS;

    fn serialize(&self, w: &mut CdrWriter) {
        w.u32_val(self.status_list.len() as u32);
        for s in self.status_list.iter() {
            w.align_to(4);
            s.serialize(w);
        }
    }

    fn deserialize(r: &mut CdrReader<'_>) -> Result<Self, Error> {
        let n = r.u32_val()? as usize;
        if n > MAX_STATUS_GOALS {
            return Err(Error::Deserialization);
        }
        let mut status_list: heapless::Vec<GoalStatus, MAX_STATUS_GOALS> = heapless::Vec::new();
        for _ in 0..n {
            r.align_to(4);
            status_list.push(GoalStatus::deserialize(r)?).ok();
        }
        Ok(Self { status_list })
    }
}

// ── Service trait impls: SendGoalSrv<A> / GetResultSrv<A> / CancelGoalSrv<A> ─

/// ZST that provides a [`Service`] impl for the `send_goal` half of an action.
/// Use it as `ServiceClient<SendGoalSrv<A>>` inside [`ActionClient`].
pub struct SendGoalSrv<A: Action>(PhantomData<fn() -> A>);

impl<A: Action> Service for SendGoalSrv<A> {
    type Request = SendGoalRequest<A>;
    type Response = SendGoalResponseFor<A>;

    const SERVICE_NAME: &'static str = A::SEND_GOAL_SERVICE_NAME;
    const REQUEST_TYPE_NAME: &'static str = A::SEND_GOAL_REQUEST_TYPE_NAME;
    const RESPONSE_TYPE_NAME: &'static str = A::SEND_GOAL_RESPONSE_TYPE_NAME;
}

/// ZST that provides a [`Service`] impl for the `get_result` half of an action.
pub struct GetResultSrv<A: Action>(PhantomData<fn() -> A>);

impl<A: Action> Service for GetResultSrv<A> {
    type Request = GetResultRequest<A>;
    type Response = GetResultResponse<A>;

    const SERVICE_NAME: &'static str = A::GET_RESULT_SERVICE_NAME;
    const REQUEST_TYPE_NAME: &'static str = A::GET_RESULT_REQUEST_TYPE_NAME;
    const RESPONSE_TYPE_NAME: &'static str = A::GET_RESULT_RESPONSE_TYPE_NAME;
}

/// ZST that provides a [`Service`] impl for the `cancel_goal` half of an
/// action.  The CancelGoal request/response types are fixed by `action_msgs`
/// and shared across all actions — only the service name varies per-action.
pub struct CancelGoalSrv<A: Action>(PhantomData<fn() -> A>);

impl<A: Action> Service for CancelGoalSrv<A> {
    type Request = CancelGoalRequest;
    type Response = CancelGoalResponse;

    const SERVICE_NAME: &'static str = A::CANCEL_GOAL_SERVICE_NAME;
    const REQUEST_TYPE_NAME: &'static str = "action_msgs::srv::dds_::CancelGoal_Request_";
    const RESPONSE_TYPE_NAME: &'static str = "action_msgs::srv::dds_::CancelGoal_Response_";
}

// ── ActionClientHandles ──────────────────────────────────────────────────────

/// `'static` storage bundle for an [`ActionClient`].  Holds the
/// `ServiceClientHandles` for both inner services plus a goal-sequence
/// counter, so the user declares a single `static` per action client:
///
/// ```ignore
/// static FIB_HANDLES: ActionClientHandles<Fibonacci> = ActionClientHandles::new();
/// ```
pub struct ActionClientHandles<A: Action> {
    pub send_goal: ServiceClientHandles<SendGoalSrv<A>>,
    pub get_result: ServiceClientHandles<GetResultSrv<A>>,
    pub cancel_goal: ServiceClientHandles<CancelGoalSrv<A>>,
    /// Monotonic counter feeding [`GoalId::from_seq`].  Starts at 0; the
    /// first goal_id derives from sequence `1`.
    pub goal_seq: AtomicI64,
}

impl<A: Action> ActionClientHandles<A> {
    pub const fn new() -> Self {
        Self {
            send_goal: ServiceClientHandles::new(),
            get_result: ServiceClientHandles::new(),
            cancel_goal: ServiceClientHandles::new(),
            goal_seq: AtomicI64::new(0),
        }
    }
}

impl<A: Action> Default for ActionClientHandles<A> {
    fn default() -> Self {
        Self::new()
    }
}

// ── ActionClient (v0.4-rc1 Phase 3 — real composition) ──────────────────────

/// Client handle for invoking a ROS2 action.
///
/// Built from two `ServiceClient`s (`SendGoalSrv<A>`, `GetResultSrv<A>`) and a
/// shared feedback `Subscription<FeedbackMessage<A>, FB_N>`.  Construct via
/// [`crate::Node::create_action_client`].
///
/// Cheap-Copy — pass by value into any Embassy task.
pub struct ActionClient<A: Action, const FB_N: usize = 4> {
    send_goal_client: ServiceClient<SendGoalSrv<A>>,
    get_result_client: ServiceClient<GetResultSrv<A>>,
    cancel_goal_client: ServiceClient<CancelGoalSrv<A>>,
    feedback: &'static Subscription<FeedbackMessage<A>, FB_N>,
    handles: &'static ActionClientHandles<A>,
    /// Stored copy of the runtime's client_key, so `from_seq` can derive a
    /// stable GoalId without locking the inner state on every call.
    client_key: [u8; 4],
    /// `send_goal` requester object_id — used as the `action_idx` argument to
    /// [`GoalId::from_seq`].
    action_idx: u16,
}

impl<A: Action, const FB_N: usize> Clone for ActionClient<A, FB_N> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<A: Action, const FB_N: usize> Copy for ActionClient<A, FB_N> {}

impl<A: Action, const FB_N: usize> ActionClient<A, FB_N> {
    pub(crate) fn new(
        send_goal_client: ServiceClient<SendGoalSrv<A>>,
        get_result_client: ServiceClient<GetResultSrv<A>>,
        cancel_goal_client: ServiceClient<CancelGoalSrv<A>>,
        feedback: &'static Subscription<FeedbackMessage<A>, FB_N>,
        handles: &'static ActionClientHandles<A>,
        client_key: [u8; 4],
        action_idx: u16,
    ) -> Self {
        Self {
            send_goal_client,
            get_result_client,
            cancel_goal_client,
            feedback,
            handles,
            client_key,
            action_idx,
        }
    }

    /// Send `goal` to the action server and wait for the accept/reject reply.
    ///
    /// On `accepted = true` returns a [`GoalHandle`] which the caller uses to
    /// await the final result and stream feedback.  On `accepted = false`
    /// returns [`Error::GoalRejected`].
    pub async fn send_goal(&self, goal: A::Goal) -> Result<GoalHandle<A, FB_N>, Error> {
        // Allocate a unique sequence number, skipping 0 (reserved sentinel).
        let seq = loop {
            let prev = self.handles.goal_seq.fetch_add(1, Ordering::Relaxed);
            let n = prev.wrapping_add(1);
            if n != 0 {
                break n;
            }
        };
        let goal_id = GoalId::from_seq(self.client_key, self.action_idx, seq);

        let req = SendGoalRequest::<A> { goal_id, goal };
        let resp = self.send_goal_client.call(&req).await?;
        if !resp.inner.accepted {
            return Err(Error::GoalRejected);
        }

        Ok(GoalHandle {
            goal_id,
            stamp: resp.inner.stamp,
            get_result_client: self.get_result_client,
            cancel_goal_client: self.cancel_goal_client,
            feedback: self.feedback,
        })
    }

    /// Object-id of the underlying SendGoal requester.  For debugging.
    pub fn send_goal_requester_oid(&self) -> u16 {
        self.send_goal_client.requester_oid()
    }
}

// ── GoalHandle ────────────────────────────────────────────────────────────────

/// Handle returned by [`ActionClient::send_goal`] for an accepted goal.
///
/// Use [`GoalHandle::await_result`] to block on the final result, or
/// [`GoalHandle::next_feedback`] to consume one feedback sample.  Cheap-Copy
/// so the same goal can be tracked from multiple tasks (terminal methods that
/// consume `self` enforce single-shot semantics where it matters).
pub struct GoalHandle<A: Action, const FB_N: usize = 4> {
    pub goal_id: GoalId,
    /// Server-stamped accept time, echoed from `SendGoalResponse.stamp`.
    pub stamp: Time,
    get_result_client: ServiceClient<GetResultSrv<A>>,
    cancel_goal_client: ServiceClient<CancelGoalSrv<A>>,
    feedback: &'static Subscription<FeedbackMessage<A>, FB_N>,
}

impl<A: Action, const FB_N: usize> Clone for GoalHandle<A, FB_N> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<A: Action, const FB_N: usize> Copy for GoalHandle<A, FB_N> {}

impl<A: Action, const FB_N: usize> GoalHandle<A, FB_N> {
    /// Wait for the action server to publish the final result for this goal.
    ///
    /// Calls the `get_result` service which blocks until the server posts a
    /// terminal status.  Returns:
    /// - `Ok(result)` when the server reports `STATUS_SUCCEEDED`.
    /// - [`Error::GoalNotSucceeded`] for any other terminal status (canceled,
    ///   aborted, etc.) — the wrapped i8 is the raw `goal_status::*` code.
    pub async fn await_result(self) -> Result<A::Result, Error> {
        let req = GetResultRequest::<A>::new(self.goal_id);
        let resp = self.get_result_client.call(&req).await?;
        if resp.status != goal_status::STATUS_SUCCEEDED {
            return Err(Error::GoalNotSucceeded(resp.status));
        }
        Ok(resp.result)
    }

    /// Wait for the next feedback sample addressed to *this* goal.
    ///
    /// Feedback messages addressed to other goals (sharing the same
    /// subscription slot) are silently consumed and discarded; only a sample
    /// whose `goal_id` matches is returned.
    pub async fn next_feedback(&self) -> A::Feedback {
        loop {
            let fb = self.feedback.recv().await;
            if fb.goal_id == self.goal_id {
                return fb.feedback;
            }
            // Sample is for a different goal — drop and keep listening.
        }
    }

    /// Request the server to cancel this goal.
    ///
    /// Sends a `CancelGoal` service call carrying this goal's `GoalInfo` and
    /// returns once the server has replied.  Possible outcomes:
    /// - `Ok(())` — server returned `cancel_response::ERROR_NONE` (cancel
    ///   accepted; the goal will eventually transition to `STATUS_CANCELED`,
    ///   observable via `await_result`).
    /// - [`Error::AgentRejected`] wrapping the `return_code` for any other
    ///   non-success cancel reply (`ERROR_REJECTED`,
    ///   `ERROR_UNKNOWN_GOAL_ID`, `ERROR_GOAL_TERMINATED`).
    pub async fn cancel(self) -> Result<(), Error> {
        let req = CancelGoalRequest {
            goal_info: GoalInfo {
                goal_id: self.goal_id,
                stamp: self.stamp,
            },
        };
        let resp = self.cancel_goal_client.call(&req).await?;
        if resp.return_code == cancel_response::ERROR_NONE {
            Ok(())
        } else {
            // Reuse AgentRejected to surface the raw cancel return_code as a
            // u8 — callers that care about the exact failure mode can match
            // against `cancel_response::*`.
            Err(Error::AgentRejected(resp.return_code as u8))
        }
    }
}

// ── ActiveGoalCancelState (shared between ActionServer and AcceptedGoal) ────

/// Lock-free single-flight cancel state for an [`ActionServer`].
///
/// `accept_next_goal` stores the active `goal_id` (split into two `u64`
/// halves) and clears `cancel_requested`.  A separate cancel-server task
/// (typically `ActionServer::serve_cancels_for_active`) compares incoming
/// `CancelGoalRequest` goal_ids against the stored value and sets
/// `cancel_requested` on a match.  The application reads the flag through
/// [`AcceptedGoal::is_cancel_requested`] to cooperatively abort.
pub struct ActiveGoalCancelState {
    active_lo: AtomicU64,
    active_hi: AtomicU64,
    cancel_requested: AtomicBool,
}

impl ActiveGoalCancelState {
    pub const fn new() -> Self {
        Self {
            active_lo: AtomicU64::new(0),
            active_hi: AtomicU64::new(0),
            cancel_requested: AtomicBool::new(false),
        }
    }

    /// Mark `goal_id` as the active goal.  Normally invoked internally by
    /// `ActionServer::accept_next_goal`; exposed for tests and for advanced
    /// users driving the state machine manually.
    pub fn set_active(&self, goal_id: GoalId) {
        let mut lo_bytes = [0u8; 8];
        let mut hi_bytes = [0u8; 8];
        lo_bytes.copy_from_slice(&goal_id.0[0..8]);
        hi_bytes.copy_from_slice(&goal_id.0[8..16]);
        self.active_lo
            .store(u64::from_le_bytes(lo_bytes), Ordering::Release);
        self.active_hi
            .store(u64::from_le_bytes(hi_bytes), Ordering::Release);
        self.cancel_requested.store(false, Ordering::Release);
    }

    /// Mark the active slot as empty and clear any pending cancel flag.
    pub fn clear_active(&self) {
        self.active_lo.store(0, Ordering::Release);
        self.active_hi.store(0, Ordering::Release);
        self.cancel_requested.store(false, Ordering::Release);
    }

    /// Returns `true` if `goal_id` matches the currently-active goal.
    pub fn matches(&self, goal_id: &GoalId) -> bool {
        let mut lo_bytes = [0u8; 8];
        let mut hi_bytes = [0u8; 8];
        lo_bytes.copy_from_slice(&goal_id.0[0..8]);
        hi_bytes.copy_from_slice(&goal_id.0[8..16]);
        self.active_lo.load(Ordering::Acquire) == u64::from_le_bytes(lo_bytes)
            && self.active_hi.load(Ordering::Acquire) == u64::from_le_bytes(hi_bytes)
    }

    pub fn is_cancel_requested(&self) -> bool {
        self.cancel_requested.load(Ordering::Acquire)
    }

    /// Mark the active goal as cancellation-requested.  Idempotent.
    pub fn request_cancel(&self) {
        self.cancel_requested.store(true, Ordering::Release);
    }
}

impl Default for ActiveGoalCancelState {
    fn default() -> Self {
        Self::new()
    }
}

// ── ActionServerHandles ──────────────────────────────────────────────────────

/// `'static` storage bundle for an [`ActionServer`].  Holds the three
/// [`ServiceServerSlot`]s required by a ROS2 action plus the lock-free
/// [`ActiveGoalCancelState`] that ties cancel-server replies to
/// [`AcceptedGoal::is_cancel_requested`].
///
/// Inbox depths default to 4 each; tune via the const generics if your
/// scenario demands more concurrent service request buffering.
///
/// ```ignore
/// static FIB_SRV_HANDLES: ActionServerHandles<Fibonacci> = ActionServerHandles::new();
/// ```
pub struct ActionServerHandles<
    A: Action,
    const SG_N: usize = 4,
    const GR_N: usize = 4,
    const CG_N: usize = 4,
> {
    pub send_goal: crate::service::ServiceServerSlot<SendGoalSrv<A>, SG_N>,
    pub get_result: crate::service::ServiceServerSlot<GetResultSrv<A>, GR_N>,
    pub cancel_goal: crate::service::ServiceServerSlot<CancelGoalSrv<A>, CG_N>,
    pub cancel_state: ActiveGoalCancelState,
}

impl<A: Action, const SG_N: usize, const GR_N: usize, const CG_N: usize>
    ActionServerHandles<A, SG_N, GR_N, CG_N>
{
    pub const fn new() -> Self {
        Self {
            send_goal: crate::service::ServiceServerSlot::new(),
            get_result: crate::service::ServiceServerSlot::new(),
            cancel_goal: crate::service::ServiceServerSlot::new(),
            cancel_state: ActiveGoalCancelState::new(),
        }
    }
}

impl<A: Action, const SG_N: usize, const GR_N: usize, const CG_N: usize> Default
    for ActionServerHandles<A, SG_N, GR_N, CG_N>
{
    fn default() -> Self {
        Self::new()
    }
}

// ── ActionServer (real composition) ──────────────────────────────────────────

/// Server-side composition of one ROS2 action.
///
/// Owns three [`ServiceServer`](crate::ServiceServer)s (send_goal, get_result,
/// cancel_goal) and one [`Publisher`](crate::Publisher) (feedback).  Status
/// publishing is *not* wired in v0.4-rc1 — the agent observes per-goal
/// terminal states through the GetResult reply, which is sufficient for ROS2
/// `action_client` integration.
///
/// **Single-flight semantics**: this server assumes one goal is being executed
/// at a time.  `accept_next_goal` produces an [`AcceptedGoal`] whose
/// `succeed` / `abort` consume the next matching `GetResult` request — which
/// works as long as the user awaits each goal to completion before accepting
/// the next.
///
/// Cancel handling: pending cancel requests are *not* automatically replied to.
/// Call [`ActionServer::serve_pending_cancels`] in a separate task to keep
/// the inbox drained (always replies with `ERROR_REJECTED` in this release —
/// proper goal-cancel routing is future work).
///
/// `Copy` so it can be shared between the goal-execution task and the
/// cancel-server task.
pub struct ActionServer<
    A: Action,
    const SG_N: usize = 4,
    const GR_N: usize = 4,
    const CG_N: usize = 4,
> {
    send_goal_server: crate::service::ServiceServer<SendGoalSrv<A>, SG_N>,
    get_result_server: crate::service::ServiceServer<GetResultSrv<A>, GR_N>,
    cancel_goal_server: crate::service::ServiceServer<CancelGoalSrv<A>, CG_N>,
    feedback_pub: crate::publisher::Publisher<FeedbackMessage<A>>,
    status_pub: crate::publisher::Publisher<GoalStatusArray>,
    cancel_state: &'static ActiveGoalCancelState,
}

impl<A: Action, const SG_N: usize, const GR_N: usize, const CG_N: usize> Clone
    for ActionServer<A, SG_N, GR_N, CG_N>
{
    fn clone(&self) -> Self {
        *self
    }
}
impl<A: Action, const SG_N: usize, const GR_N: usize, const CG_N: usize> Copy
    for ActionServer<A, SG_N, GR_N, CG_N>
{
}

impl<A: Action, const SG_N: usize, const GR_N: usize, const CG_N: usize>
    ActionServer<A, SG_N, GR_N, CG_N>
{
    pub(crate) fn new(
        send_goal_server: crate::service::ServiceServer<SendGoalSrv<A>, SG_N>,
        get_result_server: crate::service::ServiceServer<GetResultSrv<A>, GR_N>,
        cancel_goal_server: crate::service::ServiceServer<CancelGoalSrv<A>, CG_N>,
        feedback_pub: crate::publisher::Publisher<FeedbackMessage<A>>,
        status_pub: crate::publisher::Publisher<GoalStatusArray>,
        cancel_state: &'static ActiveGoalCancelState,
    ) -> Self {
        Self {
            send_goal_server,
            get_result_server,
            cancel_goal_server,
            feedback_pub,
            status_pub,
            cancel_state,
        }
    }

    /// Wait for the next incoming `SendGoal` request, auto-reply
    /// `accepted = true` with stamp = `Time::ZERO`, and return an
    /// [`AcceptedGoal`] tied to the goal's `goal_id`.
    ///
    /// The caller is responsible for driving the goal to completion via
    /// `publish_feedback` / `succeed` / `abort`.
    pub async fn accept_next_goal(&self) -> Result<AcceptedGoal<A, GR_N>, Error> {
        let req = self.send_goal_server.recv_request().await;
        let goal_id = req.payload.goal_id;
        let resp = SendGoalResponseFor::<A>::new(SendGoalResponse {
            accepted: true,
            stamp: Time::ZERO,
        });
        req.reply(&resp).await?;
        let goal = req.payload.goal;
        // Mark this as the active goal so `serve_cancels_for_active` can
        // route incoming cancels to the AtomicBool that AcceptedGoal reads.
        self.cancel_state.set_active(goal_id);
        Ok(AcceptedGoal {
            goal_id,
            goal,
            get_result_server: self.get_result_server,
            feedback_pub: self.feedback_pub,
            cancel_state: self.cancel_state,
        })
    }

    /// Drain pending CancelGoal requests with a fixed `ERROR_REJECTED` reply.
    ///
    /// Useful as a cheap "we don't support cancel" stub.  For active-goal-aware
    /// cancel handling that wakes [`AcceptedGoal::is_cancel_requested`], use
    /// [`ActionServer::serve_cancels_for_active`] instead.
    pub async fn serve_pending_cancels(&self) -> ! {
        loop {
            let req = self.cancel_goal_server.recv_request().await;
            let resp = CancelGoalResponse {
                return_code: cancel_response::ERROR_REJECTED,
                goals_canceling: heapless::Vec::new(),
            };
            req.reply(&resp).await.ok();
        }
    }

    /// Drain pending CancelGoal requests, comparing each request's `goal_id`
    /// against the currently-active goal stored by `accept_next_goal`.
    ///
    /// On match: sets the cancel flag (visible via
    /// [`AcceptedGoal::is_cancel_requested`]) and replies `ERROR_NONE`.
    /// On mismatch: replies `ERROR_UNKNOWN_GOAL_ID`.
    /// While no goal is active: replies `ERROR_GOAL_TERMINATED` for any
    /// incoming cancel.
    pub async fn serve_cancels_for_active(&self) -> ! {
        loop {
            let req = self.cancel_goal_server.recv_request().await;
            let target = req.payload.goal_info.goal_id;
            let return_code = if self.cancel_state.matches(&target) {
                self.cancel_state.request_cancel();
                cancel_response::ERROR_NONE
            } else if self.cancel_state.matches(&GoalId([0u8; 16])) {
                cancel_response::ERROR_GOAL_TERMINATED
            } else {
                cancel_response::ERROR_UNKNOWN_GOAL_ID
            };
            let resp = CancelGoalResponse {
                return_code,
                goals_canceling: heapless::Vec::new(),
            };
            req.reply(&resp).await.ok();
        }
    }

    /// Publish a `GoalStatusArray` on `<action>/_action/status`.
    ///
    /// The action client subscribes to this topic to track goal lifecycle
    /// transitions independently of the GetResult reply.  Many ROS2 clients
    /// (rclpy, rclcpp) work without status publishing as long as GetResult
    /// returns a terminal status — call this only if you need full fidelity.
    pub async fn publish_status(&self, list: GoalStatusArray) -> Result<(), Error> {
        self.status_pub.publish(&list).await
    }

    /// Convenience: build a `GoalStatusArray` containing a single
    /// `GoalStatus` for the active goal at `status` and publish it.
    pub async fn publish_status_for_active(
        &self,
        active_goal_id: GoalId,
        status: i8,
    ) -> Result<(), Error> {
        let mut list: heapless::Vec<GoalStatus, MAX_STATUS_GOALS> = heapless::Vec::new();
        list.push(GoalStatus {
            goal_info: GoalInfo {
                goal_id: active_goal_id,
                stamp: Time::ZERO,
            },
            status,
        })
        .ok();
        self.publish_status(GoalStatusArray { status_list: list })
            .await
    }
}

/// Accepted-goal handle returned by [`ActionServer::accept_next_goal`].
///
/// Owns the path back to the originating client: a feedback `Publisher` and
/// a `ServiceServer<GetResultSrv<A>>` it consumes one matching request from
/// when the user calls `succeed` / `abort`.  Also holds a reference to the
/// shared [`ActiveGoalCancelState`] so the user can poll
/// [`AcceptedGoal::is_cancel_requested`] mid-execution.
pub struct AcceptedGoal<A: Action, const GR_N: usize = 4> {
    pub goal_id: GoalId,
    pub goal: A::Goal,
    get_result_server: crate::service::ServiceServer<GetResultSrv<A>, GR_N>,
    feedback_pub: crate::publisher::Publisher<FeedbackMessage<A>>,
    cancel_state: &'static ActiveGoalCancelState,
}

impl<A: Action, const GR_N: usize> AcceptedGoal<A, GR_N> {
    /// Publish a feedback sample addressed to this goal.
    ///
    /// The feedback message is wrapped in a [`FeedbackMessage<A>`] with
    /// `goal_id` set so client-side `GoalHandle::next_feedback` can filter.
    pub async fn publish_feedback(&self, feedback: A::Feedback) -> Result<(), Error> {
        let msg = FeedbackMessage::<A> {
            goal_id: self.goal_id,
            feedback,
        };
        self.feedback_pub.publish(&msg).await
    }

    /// Returns `true` if a `CancelGoal` request matching this goal_id has
    /// been received by `serve_cancels_for_active`.  The application is
    /// expected to poll this between work units and abort cooperatively.
    pub fn is_cancel_requested(&self) -> bool {
        self.cancel_state.is_cancel_requested()
    }

    /// Reply to the next `GetResult` request with `STATUS_SUCCEEDED` + result.
    ///
    /// Blocks until a request arrives.  Returns [`Error::UnexpectedReply`]
    /// if the request's `goal_id` does not match this goal — the
    /// single-flight contract has been violated.
    pub async fn succeed(self, result: A::Result) -> Result<(), Error> {
        self.respond(goal_status::STATUS_SUCCEEDED, result).await
    }

    /// Reply to the next `GetResult` request with `STATUS_ABORTED` + result.
    pub async fn abort(self, result: A::Result) -> Result<(), Error> {
        self.respond(goal_status::STATUS_ABORTED, result).await
    }

    /// Reply to the next `GetResult` request with `STATUS_CANCELED` + result.
    /// Use this after observing `is_cancel_requested() == true`.
    pub async fn canceled(self, result: A::Result) -> Result<(), Error> {
        self.respond(goal_status::STATUS_CANCELED, result).await
    }

    async fn respond(self, status: i8, result: A::Result) -> Result<(), Error> {
        let req = self.get_result_server.recv_request().await;
        if req.payload.goal_id != self.goal_id {
            // Don't leave the cancel-state pinned to a goal we're abandoning.
            self.cancel_state.clear_active();
            return Err(Error::UnexpectedReply);
        }
        let resp = GetResultResponse::<A> { status, result };
        let r = req.reply(&resp).await;
        // Goal lifecycle ends; release the cancel slot for the next goal.
        self.cancel_state.clear_active();
        r
    }
}

// Suppress unused-import warning for `Context` until ActionServer uses it.
#[allow(dead_code)]
fn _ctx_imported(_c: Context) {}
