//! Runtime layer — `Runtime`, `Context`, `RuntimeConfig`, and `Executor`.
//!
//! # Quick-start
//!
//! ```ignore
//! use micro_xrce_dds_rs::{Runtime, RuntimeConfig, client_key};
//!
//! static RUNTIME: Runtime = Runtime::new();
//!
//! #[embassy_executor::task]
//! async fn xrce_executor(exec: Executor<TcpSocket<'static>>) -> ! {
//!     exec.run().await
//! }
//!
//! #[embassy_executor::main]
//! async fn main(spawner: Spawner) {
//!     // ... WiFi / embassy-net setup, TCP connect ...
//!     let (ctx, exec) = RUNTIME
//!         .start(socket, RuntimeConfig::new(0x81, client_key!()))
//!         .await
//!         .unwrap();
//!     spawner.spawn(xrce_executor(exec)).unwrap();
//!     spawner.spawn(my_node(ctx)).unwrap();
//! }
//! ```

pub mod creation;
pub mod encode;
pub mod executor;
pub mod inner;

pub use executor::Executor;
pub use inner::{Frame, SessionInner, FRAME_BUF_SIZE, MAX_SUBS, TX_QUEUE_DEPTH};

use embedded_io_async::{Read, Write};

use crate::{error::Error, framing};

// ── RuntimeConfig ─────────────────────────────────────────────────────────────

/// Configuration for [`Runtime::start`].
#[derive(Clone, Copy)]
pub struct RuntimeConfig {
    /// XRCE-DDS session identifier.  Use values in `0x81..=0xFE` to keep
    /// message headers compact (no client_key in the header; see
    /// `SESSION_ID_WITHOUT_CLIENT_KEY` in `protocol.rs`).
    pub session_id: u8,
    /// 4-byte client key.  Use the `client_key!()` macro to derive a unique
    /// key per firmware/binary from the crate name at compile time.
    pub client_key: [u8; 4],
}

impl RuntimeConfig {
    /// Construct a configuration with explicit values.
    pub const fn new(session_id: u8, client_key: [u8; 4]) -> Self {
        Self {
            session_id,
            client_key,
        }
    }
}

// ── Runtime ───────────────────────────────────────────────────────────────────

/// The `'static` XRCE-DDS runtime.  Declare exactly one per application:
///
/// ```ignore
/// static RUNTIME: micro_xrce_dds_rs::Runtime = micro_xrce_dds_rs::Runtime::new();
/// ```
///
/// Call [`Runtime::start`] once (from `main`) to connect to the micro-ROS
/// Agent, obtain a [`Context`] handle and an [`Executor`] ready to spawn.
pub struct Runtime {
    pub(crate) inner: SessionInner,
}

// SAFETY: Runtime only contains SessionInner, which is Sync by construction
// (all mutable state is behind atomics or embassy-sync mutexes).
unsafe impl Sync for Runtime {}

impl Runtime {
    /// Create an uninitialized `Runtime`.  Call this in a `static` initializer.
    ///
    /// ```ignore
    /// static RUNTIME: Runtime = Runtime::new();
    /// ```
    pub const fn new() -> Self {
        Self {
            inner: SessionInner::new(),
        }
    }

    /// Return a `Context` pointing at this `Runtime`'s inner state.
    ///
    /// Low-level helper for testing.  Prefer [`Runtime::start`] in application
    /// code.
    pub fn context(&'static self) -> Context {
        Context {
            inner: &self.inner,
        }
    }

    /// Connect to the micro-ROS Agent and return a `(Context, Executor)` pair.
    ///
    /// `transport` must already be connected to the agent's TCP endpoint.
    /// `start` sends CREATE_CLIENT, waits for STATUS_AGENT, stores the session
    /// identity, and returns:
    ///
    /// - A [`Context`] handle for use by any number of Embassy tasks.
    /// - An [`Executor`] that must be moved into a single Embassy task:
    ///
    /// ```ignore
    /// let (ctx, exec) = RUNTIME.start(socket, config).await?;
    /// spawner.spawn(xrce_executor(exec)).unwrap();
    /// ```
    pub async fn start<T: Read + Write>(
        &'static self,
        mut transport: T,
        config: RuntimeConfig,
    ) -> Result<(Context, Executor<T>), Error> {
        let mut tx_buf = [0u8; 64];
        let n = encode::build_create_client(&mut tx_buf, config.session_id, &config.client_key, 512);
        framing::write_framed(&mut transport, &tx_buf[..n]).await?;

        let mut rx_buf = [0u8; 128];
        let reply = framing::read_framed(&mut transport, &mut rx_buf).await?;
        encode::parse_status_agent(reply, config.session_id)?;

        self.inner.set_session_identity(config.session_id, config.client_key);

        let ctx = self.context();
        let exec = Executor::new(&self.inner, transport);
        Ok((ctx, exec))
    }
}

// ── Context ───────────────────────────────────────────────────────────────────

/// A cheap, copyable handle to the shared XRCE-DDS session state.
///
/// Pass by value (it is `Copy`) into any number of Embassy tasks.  Each task
/// uses it to create a [`crate::Node`] → Publishers / Subscriptions and to
/// publish messages via `Publisher::publish` (Phase 4).
///
/// Obtained from `Runtime::start` or [`Runtime::context`] (testing only).
#[derive(Clone, Copy)]
pub struct Context {
    pub(crate) inner: &'static SessionInner,
}

// &'static SessionInner is Send + Sync because SessionInner: Sync.
unsafe impl Send for Context {}
unsafe impl Sync for Context {}

impl Context {
    /// Returns `true` once the Executor has set the disconnect flag.
    /// All `publish` calls after this point return `Error::Disconnected`.
    pub fn is_disconnected(&self) -> bool {
        self.inner.is_disconnected()
    }

    /// Access the inner state.  `pub(crate)` so that node/publisher modules
    /// can call inner counter methods without exposing SessionInner publicly.
    pub(crate) fn inner(&self) -> &SessionInner {
        self.inner
    }
}
