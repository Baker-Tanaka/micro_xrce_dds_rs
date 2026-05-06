//! ROS2 Action support — **trait-level scaffolding only (v0.4)**.
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
//! With v0.3 in place every entity above is expressible as a [`crate::Service`]
//! or [`crate::Publisher`] / subscription — a working `ActionServer<A>` /
//! `ActionClient<A>` is therefore a **typed composition**, not new wire
//! format.  This module ships the trait definitions and the small set of
//! shared building blocks (UUID-based `GoalId`, `GoalStatus` codes, the
//! `GoalStatusArray` topic shape) that all per-action wrappers need.
//!
//! The remaining work (rosidl-generated wrapper messages such as
//! `SendGoal_Request_<A>`, `GetResult_Response_<A>`, `FeedbackMessage<A>`)
//! must be supplied by the user, because every action defines its own goal,
//! result and feedback types.  Once those are available, an `ActionClient<A>`
//! is a thin wrapper around three [`ServiceClient`]s plus two subscriptions —
//! see the design doc for the exact composition.
//!
//! [`ServiceClient`]: crate::ServiceClient

use crate::{cdr::CdrWriter, cdr_reader::CdrReader, error::Error, message::Message};

// ── Action trait ──────────────────────────────────────────────────────────────

/// Defines a ROS2 action by its goal / result / feedback message types and
/// the rosidl-generated DDS type names of each.
///
/// All five `*_TYPE_NAME` constants follow the rosidl convention
/// `<pkg>::action::dds_::<Action>_<Component>_`.
pub trait Action: 'static {
    type Goal: Message + Send + 'static;
    type Result: Message + Send + 'static;
    type Feedback: Message + Send + 'static;

    /// ROS action namespace, e.g. `"/fibonacci"`.  The five DDS entities
    /// derive from this — `<name>/_action/{send_goal,cancel_goal,get_result,
    /// feedback,status}`.
    const ACTION_NAME: &'static str;

    /// rosidl-generated DDS type name of the goal message.
    const GOAL_TYPE_NAME: &'static str;
    /// rosidl-generated DDS type name of the result message.
    const RESULT_TYPE_NAME: &'static str;
    /// rosidl-generated DDS type name of the feedback message.
    const FEEDBACK_TYPE_NAME: &'static str;
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

// ── ActionClient / ActionServer scaffolds ─────────────────────────────────────
//
// These are intentionally **not implemented** in v0.4 — they would require
// per-action wrapper messages (`SendGoal_Request_<A>`, `GetResult_Response_<A>`,
// etc.) that depend on user-supplied rosidl bindings.  The shape below is
// frozen so v0.4-rc1 can drop in implementations without API breakage.

/// Client handle for invoking a ROS2 action.  **v0.4 placeholder** — see the
/// module-level docs for the missing pieces.
pub struct ActionClient<A: Action> {
    _phantom: core::marker::PhantomData<fn() -> A>,
}

impl<A: Action> ActionClient<A> {
    /// Reserved for v0.4-rc1.  Returns [`Error::NotStarted`] today so calls
    /// fail fast in user code that forgets to gate the feature.
    pub async fn send_goal(&self, _goal: &A::Goal) -> Result<GoalHandle<A>, Error> {
        Err(Error::NotStarted)
    }
}

/// Handle returned by [`ActionClient::send_goal`].  v0.4 placeholder.
pub struct GoalHandle<A: Action> {
    pub goal_id: GoalId,
    _phantom: core::marker::PhantomData<fn() -> A>,
}

impl<A: Action> GoalHandle<A> {
    /// v0.4 placeholder.
    pub async fn await_result(self) -> Result<A::Result, Error> {
        Err(Error::NotStarted)
    }

    /// v0.4 placeholder.
    pub async fn cancel(self) -> Result<(), Error> {
        Err(Error::NotStarted)
    }
}

/// Server handle for accepting and executing action goals.  v0.4 placeholder.
pub struct ActionServer<A: Action> {
    _phantom: core::marker::PhantomData<fn() -> A>,
}

impl<A: Action> ActionServer<A> {
    /// v0.4 placeholder.
    pub async fn accept_next_goal(&self) -> Result<AcceptedGoal<A>, Error> {
        Err(Error::NotStarted)
    }
}

/// Accepted-goal handle returned by [`ActionServer::accept_next_goal`].
pub struct AcceptedGoal<A: Action> {
    pub goal_id: GoalId,
    pub goal: A::Goal,
    _phantom: core::marker::PhantomData<fn() -> A>,
}

impl<A: Action> AcceptedGoal<A> {
    /// v0.4 placeholder.
    pub async fn publish_feedback(&self, _fb: &A::Feedback) -> Result<(), Error> {
        Err(Error::NotStarted)
    }

    /// v0.4 placeholder.
    pub async fn succeed(self, _result: &A::Result) -> Result<(), Error> {
        Err(Error::NotStarted)
    }

    /// v0.4 placeholder.
    pub async fn abort(self, _result: &A::Result) -> Result<(), Error> {
        Err(Error::NotStarted)
    }
}
