# micro_xrce_dds_rs

A `no_std`, allocation-free Rust micro-ROS client stack. It talks to an
eProsima micro-ROS / Micro XRCE-DDS Agent over TCP. It was written for the
`tenshi-no-hana` RP2040 rover, which runs Embassy. It covers the rover's
publish/subscribe contract and does **not** implement every micro-ROS
feature (see [Limitations](#limitations)).

The stack has two layers:

| Layer | Crate | Role |
|---|---|---|
| Low level | `micro-xrce-dds-rs` (this directory) | XRCE wire encoding, TCP framing, checked CDR writer/reader, the `Message` trait, `Timestamp`, the `Transport` trait, and the legacy `Runtime`/`Node`/service/action API |
| Facade | `embassy-ros` (`embassy_ros/`, macros in `embassy_ros/macros/`) | Static nodes, typed topics, publishers and subscriptions, connection epochs, a synchronized clock, the Agent session engine with reconnect, the `#[embassy_ros::node]` macro, and the `embassy-net` TCP adapter |

ROS message types are generated from the official IDL into separate crates
by `codegen/` (see `codegen/README.md`). Those crates implement `Message`
using `CdrWriter`/`CdrReader`.

## Low-level crate

Features:
- `defmt` (default): `defmt::Format` for errors and `Timestamp`.
- `embassy-net`: implements `Transport` for `embassy_net::tcp::TcpSocket`.

API:
- **CDR.** `CdrWriter` writes CDR *bodies* only. Fast-DDS adds the
  encapsulation header, so do not add it yourself. The writer never panics:
  an overflow is latched, and `finish_checked()` then returns
  `Err(Error::BufferTooSmall)`. `finish()` is kept for legacy callers and
  returns the written prefix. `CdrReader` is bounds-checked. Both provide
  `i8/u8/i16/u16/i32/u32/i64/u64/f32/f64/bool` plus string/array helpers.
- **`Message`.** Defines `TYPE_NAME`, `MAX_SERIALIZED_SIZE`, `serialize`
  and `deserialize`.
- **`Timestamp { sec: i32, nanosec: u32 }`.** Shared with the generated
  `builtin_interfaces/Time` (`Header::set_stamp`).
- **Framing.** `framing::FrameReader<N>` is a persistent, cancel-safe
  reader for the Agent's 2-byte length-prefixed TCP frames. A dropped
  `read_frame` future keeps partial header/body state. `write_framed`
  rejects frames longer than 65535 bytes.
- **Encoders.** `rt::encode` covers CREATE_CLIENT, CREATE
  (participant/topic/publisher/subscriber/writer/reader), READ_DATA,
  WRITE_DATA, and TIMESTAMP / TIMESTAMP_REPLY (NTP-style offset).
- **Legacy runtime.** `Runtime`/`Executor`/`Node`, plus services and
  actions, are kept with their low-level tests. `Executor::run_until_error`
  and `into_transport` were added. New application code should use
  `embassy-ros`.

## embassy-ros facade

Features:
- `defmt`: logging and `defmt::Format`.
- `embassy-net`: `transport::embassy_net::{Agent, AgentConfig, TcpBuffers, ...}`.
- `executor`: `NodeSpawner` and the `#[embassy_ros::node]` macro.

Lifecycle:

```rust
static ROS: ContextStorage = ContextStorage::new();
static CMD_VEL: Topic<Twist> = Topic::new("/cmd_vel", Qos::command(Duration::from_millis(500)));
static IMU: Topic<Imu, 2> = Topic::new("/imu/data", Qos::sensor_data());

#[embassy_ros::node(name = "motor")]
async fn motor(node: Node, motors: Motors,
               #[subscription(topic = &CMD_VEL)] mut cmds: Subscription<Twist>) { /* ... */ }

let context = embassy_ros::init(&ROS, InitOptions::default())?;   // once
let nodes = NodeSpawner::new(spawner, context);
nodes.register(motor, motors)?;          // local only: validates and binds endpoints, spawns the task
let context = nodes.finish()?;           // seals the graph (fails if any registration failed)

// network task (the caller owns the retry loop)
let agent = Agent::new(context);
loop {
    match agent.connect(socket, AgentConfig::new(endpoint, RuntimeConfig::new(0x81, client_key!()))).await {
        Ok((connection, mut driver)) => {
            let _ = select(link_down, connection.run_with_driver(&mut driver)).await;
            socket = driver.into_inner();
        }
        Err(failure) => socket = failure.socket,   // failure.kind says why
    }
    socket.abort();
    Timer::after_secs(2).await;
}
```

### Guarantees

- **Static storage, no heap.** Bounds are `MAX_NODES = 8`,
  `MAX_ENDPOINTS = 16` and `MAX_CONNECTION_WATCHERS = 4`. Each `Topic<M, N>`
  has a keep-last queue of depth `N`. Frames are at most 1024 bytes.
- **Node startup before the network.** Registration does no I/O. Any
  registration error (bad name, duplicate, double-bound topic, capacity,
  spawn failure) poisons the context, so `finish()` and `connect()` refuse
  to run with a partial graph.
- **Connect.** `connect` runs CREATE_CLIENT (with the same key this resets
  any previous Agent session). It then attempts a clock sync. Next it
  recreates every participant, publisher, subscriber, topic and
  writer/reader with REUSE|REPLACE and deterministic object ids, which also
  works after an Agent restart. It sends READ_DATA for subscriptions,
  clears all queues, increments the **epoch**, and sets the status to
  `Ready`.
- **Run and invalidation.** `run_with_driver` returns:
  - `Ok(())` if the Agent closed the stream,
  - `Err(Timeout)` if nothing was received for `liveness_timeout`
    (periodic time-sync replies act as the heartbeat),
  - or the I/O error.

  When it returns or is cancelled (drop guard), the status immediately
  becomes `Disconnected` and all queues are cleared. Recover the transport
  with `Driver::into_inner`. If a handshake fails, the transport is reset
  and returned in `ConnectFailure::socket`.
- **Publishing.** `Publisher::publish` is synchronous and non-blocking
  (keep-last). While not `Ready`, messages are dropped and the call returns
  `Ok`. A serialization overflow is reported by the next `publish` call.
- **Subscriptions.**
  - `Subscription::recv_sample` returns `Sample { message, received_at,
    epoch }` and is cancel-safe.
  - `received_at` is the instant the frame was admitted from the transport.
  - Samples from an older epoch, or received while not ready, are
    discarded.
  - With `Qos::command(max_age)`, a sample older than `max_age` since
    admission returns `Error::StaleSample`.
- **Connection watching.** `connection().status()` and `watch()`
  (`current()` / `changed()`) expose `ConnectionStatus { epoch, state }`.
  Safety consumers should stop when `!is_ready()` or when the epoch
  changes.
- **Clock.** `clock().now()` returns the Agent time (offset from the XRCE
  TIMESTAMP exchange at connect and every `time_sync_interval`) once
  synchronized. Before that it returns monotonic time since boot (check
  `is_synchronized()`). The last offset is kept across reconnects.

### QoS

All XRCE streams are best-effort. DDS writers and readers are created
without a QoS profile, so the Agent's Fast-DDS defaults apply. For
DataWriters that default is RELIABLE / KEEP_LAST, which stays compatible
with RViz and Nav2 subscriptions using either Reliable or Best-Effort (the
same behaviour as the previous backend). `Qos` controls only local
behaviour: the keep-last depth comes from the topic type parameter, and
`max_age` applies to commands.

## Limitations

- No services, actions, parameters, or graph/introspection in
  `embassy-ros`. The legacy low-level service/action code is not
  integrated with reconnect.
- Only best-effort XRCE streams (no reliable streams), no fragmentation,
  and frames of at most 1024 bytes. A message that does not fit is
  rejected with `BufferTooSmall`.
- Single Agent session per context. TCP only (via `embassy-net`), though
  the engine is generic over `Transport`.
- Cancelling `Agent::connect` itself drops the socket. Cancel
  `run_with_driver` instead.
- Without a successful time sync, stamps are boot-relative, not wall clock.

## Tests

```sh
# low-level crate (host; .cargo/config.toml selects x86_64)
cargo test --no-default-features
cargo check --no-default-features --features defmt,embassy-net --target thumbv6m-none-eabi

# embassy-ros (host tests: mock fragmented transport + fake Agent, mock time driver)
cd embassy_ros
cargo test --features executor
cargo check --target thumbv6m-none-eabi --no-default-features --features defmt,embassy-net,executor
```

The `embassy-ros` host tests cover:
- fragmented and cancelled frame reads
- handshake and entity ids/XML
- data in both directions
- reconnect with entity re-creation and sequence reset
- stale-queue clearing and epoch filtering
- command max-age
- cancellation invalidation and a duplicate session
- clock fallback and synchronization
- liveness timeout
- handshake timeout returning the socket
- keep-last and serialization overflow
- registry validation and poisoning
- macro expansion with `NodeSpawner` on the std executor
