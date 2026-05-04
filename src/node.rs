//! [`Node`] — ROS2-style node handle. Owns one DDS Participant plus a default
//! Publisher and Subscriber under it. Returned by [`crate::Session::create_node`].

/// Opaque ROS2 node handle. Cheap to copy and pass around — the underlying
/// XRCE entities live on the agent.
#[derive(Clone, Copy, Debug)]
pub struct Node {
    pub(crate) participant_idx: u16,
    pub(crate) publisher_idx: u16,
    pub(crate) subscriber_idx: u16,
}
