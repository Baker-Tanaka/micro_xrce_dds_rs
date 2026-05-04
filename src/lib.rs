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

/// Derive a 4-byte XRCE-DDS `client_key` from a static application identifier
/// using FNV-1a 32-bit hashing at compile time.
///
/// Use a unique `id` per firmware/example so the agent treats each flash as a
/// distinct client. Without this, two firmwares that share a `client_key` will
/// trip `STATUS_OK_MATCHED` reuse of stale Fast-DDS entities from the previous
/// run — and downstream `CREATE_DATAREADER` / `CREATE_DATAWRITER` on those
/// reused ids will be rejected with `STATUS_ERR_DDS_ERROR (0x80)` whenever
/// topic name or type differs from the previous run.
///
/// ```ignore
/// const KEY: [u8; 4] = micro_xrce_dds_rs::client_key_from_app_id("microros_hello");
/// let session = Session::connect(socket, 0x81, KEY).await?;
/// ```
pub const fn client_key_from_app_id(id: &str) -> [u8; 4] {
    // FNV-1a 32-bit
    let mut hash: u32 = 0x811c_9dc5;
    let bytes = id.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        hash ^= bytes[i] as u32;
        hash = hash.wrapping_mul(0x0100_0193);
        i += 1;
    }
    hash.to_le_bytes()
}

/// Derive a `client_key` from the current crate / binary name at compile time.
/// Resolves to `client_key_from_app_id(concat!(CARGO_PKG_NAME, "/", CARGO_BIN_NAME))`,
/// so each example/binary in a workspace gets a distinct key automatically.
///
/// ```ignore
/// let key: [u8; 4] = micro_xrce_dds_rs::client_key!();
/// let session = Session::connect(socket, 0x81, key).await?;
/// ```
#[macro_export]
macro_rules! client_key {
    () => {
        $crate::client_key_from_app_id(concat!(
            env!("CARGO_PKG_NAME"),
            "/",
            env!("CARGO_BIN_NAME"),
        ))
    };
    ($id:expr) => {
        $crate::client_key_from_app_id($id)
    };
}
