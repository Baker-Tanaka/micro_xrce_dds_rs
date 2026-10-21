//! `embassy-ros`: a static, `no_std`, allocation-free node/topic framework
//! for Embassy applications talking to a micro-ROS (XRCE-DDS) Agent.
//!
//! ```ignore
//! static ROS: ContextStorage = ContextStorage::new();
//! static CMD_VEL: Topic<Twist> = Topic::new("/cmd_vel", Qos::command(Duration::from_millis(500)));
//!
//! #[embassy_ros::node(name = "motor")]
//! async fn motor(node: Node, motors: Motors, #[subscription(topic = &CMD_VEL)] mut cmds: Subscription<Twist>) {
//!     loop { let sample = cmds.recv_sample().await; /* ... */ }
//! }
//!
//! let context = embassy_ros::init(&ROS, InitOptions::default())?;
//! let nodes = NodeSpawner::new(spawner, context);
//! nodes.register(motor, motors)?;      // local only, no network I/O
//! let context = nodes.finish()?;       // seal the graph
//! // network task: Agent::connect(..) -> ConnectionTask::run_with_driver(..), retry in a loop
//! ```
//!
//! Scope: typed publishers and subscriptions over best-effort XRCE streams,
//! keep-last bounded queues, connection epochs, Agent time synchronization
//! and reconnection with entity re-creation. Services, actions, parameters,
//! reliable XRCE streams and fragmentation are not provided by this crate.

#![no_std]

#[macro_use]
mod fmt;

mod context;
mod node;
mod topic;
pub mod transport;

#[cfg(feature = "executor")]
mod spawner;

pub use context::{
    init, Clock, Connection, ConnectionState, ConnectionStatus, ConnectionWatch, Context,
    ContextStorage, InitOptions, MAX_CONNECTION_WATCHERS, MAX_ENDPOINTS, MAX_NODES,
};
pub use micro_xrce_dds_rs::{Error, Message, Timestamp};
pub use node::{Node, NodeBuilder, StartupError, StartupPhase};
pub use topic::{Publisher, Qos, Sample, Subscription, Topic};

#[cfg(feature = "executor")]
pub use spawner::NodeSpawner;

/// Turn an `async fn(node: Node, resources: R, #[publisher(topic = &T)] p: Publisher<M>, ...)`
/// into a registration function `fn(&NodeSpawner, R) -> Result<(), StartupError>`
/// for [`NodeSpawner::register`]. Requires the `executor` feature.
pub use embassy_ros_macros::node;

#[doc(hidden)]
pub mod __private {
    #[cfg(feature = "executor")]
    pub use embassy_executor;
}
