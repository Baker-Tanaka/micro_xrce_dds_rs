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

    /// Clear the disconnect flag without re-running the CREATE_CLIENT
    /// handshake.  Normally [`Runtime::resume`] does this automatically;
    /// exposed for tests and for callers that drive the protocol manually.
    pub fn clear_disconnect(&'static self) {
        self.inner.clear_disconnected();
    }

    /// Force the runtime into the disconnected state.
    ///
    /// Normally the Executor sets this internally on transport failure.
    /// Exposed for application-level health checks (e.g. heartbeat timeout
    /// detected from elsewhere) and for tests.  After calling this, awaiting
    /// supervisors are unblocked and all `publish` / service calls return
    /// `Error::Disconnected` until [`Runtime::resume`] succeeds.
    pub fn force_disconnect(&'static self) {
        self.inner.set_disconnected();
    }

    /// Wait until the Executor reports a transport failure.
    ///
    /// Resolves the first time `set_disconnected` fires after this call; the
    /// signal is one-shot per disconnect cycle, so a single supervisor task
    /// can drive a reconnect loop:
    ///
    /// ```ignore
    /// loop {
    ///     RUNTIME.wait_for_disconnect().await;
    ///     // re-establish WiFi + TCP, build a fresh transport...
    ///     match RUNTIME.resume(new_socket, config).await {
    ///         Ok(exec) => spawner.spawn(xrce_exec(exec).unwrap()),
    ///         Err(_)   => Timer::after_millis(2000).await,
    ///     }
    /// }
    /// ```
    ///
    /// Only one task should await this at a time.  If the runtime is already
    /// disconnected when called, returns immediately.
    pub async fn wait_for_disconnect(&'static self) {
        if self.inner.is_disconnected() {
            return;
        }
        self.inner.disconnect_signal.wait().await;
    }

    /// Re-establish the XRCE-DDS session on a freshly-connected transport.
    ///
    /// Replays `CREATE_CLIENT` with the same `client_key` so the agent
    /// recognises this as a session resume — DDS entities created in the
    /// previous cycle are retained, so user tasks can keep using their
    /// existing `Publisher` / `Subscription` / `ServiceClient` handles.
    /// Pending CREATEs in flight are aborted with [`Error::Disconnected`]
    /// before the resume.
    ///
    /// On success returns a fresh [`Executor`] which the caller spawns on
    /// the same task slot the previous Executor lived in.
    pub async fn resume<T: Read + Write>(
        &'static self,
        mut transport: T,
        config: RuntimeConfig,
    ) -> Result<Executor<T>, Error> {
        let mut tx_buf = [0u8; 64];
        let n = encode::build_create_client(&mut tx_buf, config.session_id, &config.client_key, 512);
        framing::write_framed(&mut transport, &tx_buf[..n]).await?;

        let mut rx_buf = [0u8; 128];
        let reply = framing::read_framed(&mut transport, &mut rx_buf).await?;
        encode::parse_status_agent(reply, config.session_id)?;

        // Confirm session identity is unchanged (resume contract).
        self.inner.set_session_identity(config.session_id, config.client_key);
        // Drain any outgoing frames that were queued after the disconnect —
        // they refer to obsolete sequence numbers / headers.
        while self.inner.tx_channel.try_receive().is_ok() {}
        // Ensure no CREATE waiter is left armed against the dead executor.
        self.inner.creation_signal.signal(Err(Error::Disconnected));
        self.inner.disarm_creation();
        self.inner.creation_signal.reset();
        // Re-arm publish path.
        self.inner.clear_disconnected();

        Ok(Executor::new(&self.inner, transport))
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
