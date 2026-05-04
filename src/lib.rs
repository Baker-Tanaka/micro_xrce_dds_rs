//! `micro_xrce_dds_rs` — `no_std` micro-ROS / XRCE-DDS client SDK.
//!
//! Targets the eProsima Micro-XRCE-DDS-Agent TCP transport (the format spoken
//! by `microros/micro-ros-agent`). See `/.claude/xrce_dds_protocol.md` for the
//! wire-format reference; this crate's surface is ROS2-flavoured:
//!
//! ```ignore
//! use micro_xrce_dds_rs::{Session, msg};
//!
//! let mut session = Session::connect(socket, 0x81, [0xBA, 0xCE, 0xA1, 0x05]).await?;
//! let node = session.create_node("my_node").await?;
//! let pub_hi = session.create_publisher::<msg::std_msgs::String>(&node, "/hello").await?;
//! session.publish(&pub_hi, &msg::std_msgs::String("hi")).await?;
//! ```
//!
//! Subscriptions follow a `&'static`-slot pattern (see the [`Subscription`]
//! docs) — the slot is registered with the session, and the user task awaits
//! samples on `slot.recv()` while a separate task drives `session.spin()`.

#![no_std]

pub mod cdr;
pub mod cdr_reader;
pub mod error;
pub mod framing;
pub mod message;
pub mod node;
pub mod protocol;
pub mod publisher;
pub mod ros2;
pub mod session;
pub mod subscription;

pub use error::Error;
pub use message::Message;
pub use node::Node;
pub use publisher::Publisher;
pub use session::Session;
pub use subscription::{Subscription, SubscriptionSlot};

/// Convenience re-export so users can write `msg::std_msgs::Float32`.
pub use ros2::msg;
