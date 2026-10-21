//! Framework context: static storage, node/endpoint registry, connection
//! status/epochs and the synchronized clock.

use core::cell::{Cell, RefCell};

use embassy_sync::{
    blocking_mutex::{raw::CriticalSectionRawMutex, Mutex},
    signal::Signal,
    watch::{Receiver, Watch},
};
use embassy_time::{Duration, Instant};
use micro_xrce_dds_rs::{Error, Timestamp};

use crate::{
    node::{NodeBuilder, StartupError, StartupPhase},
    topic::Endpoint,
};

/// Maximum number of nodes per context.
pub const MAX_NODES: usize = 8;
/// Maximum number of publishers + subscriptions per context.
pub const MAX_ENDPOINTS: usize = 16;
/// Maximum number of simultaneous [`ConnectionWatch`]es.
pub const MAX_CONNECTION_WATCHERS: usize = 4;

/// Longest accepted node name (bytes).
pub(crate) const NODE_NAME_MAX: usize = 48;

type Cs = CriticalSectionRawMutex;

/// Runtime options for [`init`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct InitOptions {
    /// Timeout for each Agent request during the handshake (CREATE_CLIENT,
    /// entity creation, time synchronization).
    pub request_timeout: Duration,
    /// Period of Agent time re-synchronization while connected. The replies
    /// double as a liveness check.
    pub time_sync_interval: Duration,
    /// The connection fails with [`Error::Timeout`] if nothing is received
    /// from the Agent for this long. Must exceed `time_sync_interval`.
    pub liveness_timeout: Duration,
}

impl InitOptions {
    pub const fn new() -> Self {
        Self {
            request_timeout: Duration::from_secs(2),
            time_sync_interval: Duration::from_secs(5),
            liveness_timeout: Duration::from_secs(16),
        }
    }

    fn validate(&self) -> Result<(), Error> {
        if self.request_timeout.as_ticks() == 0
            || self.time_sync_interval.as_ticks() == 0
            || self.liveness_timeout <= self.time_sync_interval
        {
            return Err(Error::InvalidInput);
        }
        Ok(())
    }
}

impl Default for InitOptions {
    fn default() -> Self {
        Self::new()
    }
}

/// Coarse connection state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum ConnectionState {
    Disconnected,
    Connecting,
    Ready,
}

/// Connection status. `epoch` increments every time a session becomes
/// [`ConnectionState::Ready`]; samples and commands from an older epoch are
/// never delivered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct ConnectionStatus {
    pub epoch: u32,
    pub state: ConnectionState,
}

impl ConnectionStatus {
    pub const fn is_ready(&self) -> bool {
        matches!(self.state, ConnectionState::Ready)
    }
}

const INITIAL_STATUS: ConnectionStatus = ConnectionStatus {
    epoch: 0,
    state: ConnectionState::Disconnected,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Phase {
    Uninit,
    Registering,
    Sealed,
    Poisoned,
}

#[derive(Clone, Copy)]
pub(crate) struct EndpointEntry {
    pub node: u8,
    pub endpoint: &'static dyn Endpoint,
}

pub(crate) struct Registry {
    pub phase: Phase,
    pub nodes: heapless::Vec<&'static str, MAX_NODES>,
    pub endpoints: heapless::Vec<EndpointEntry, MAX_ENDPOINTS>,
    pub session_active: bool,
    pub options: InitOptions,
}

/// Static storage for one framework instance. Declare once:
/// `static ROS: ContextStorage = ContextStorage::new();`
pub struct ContextStorage {
    registry: Mutex<Cs, RefCell<Registry>>,
    status: Mutex<Cs, Cell<ConnectionStatus>>,
    watch: Watch<Cs, ConnectionStatus, MAX_CONNECTION_WATCHERS>,
    clock_offset: Mutex<Cs, Cell<Option<i64>>>,
    pub(crate) tx: Signal<Cs, ()>,
}

impl ContextStorage {
    pub const fn new() -> Self {
        Self {
            registry: Mutex::new(RefCell::new(Registry {
                phase: Phase::Uninit,
                nodes: heapless::Vec::new(),
                endpoints: heapless::Vec::new(),
                session_active: false,
                options: InitOptions::new(),
            })),
            status: Mutex::new(Cell::new(INITIAL_STATUS)),
            watch: Watch::new_with(INITIAL_STATUS),
            clock_offset: Mutex::new(Cell::new(None)),
            tx: Signal::new(),
        }
    }

    pub(crate) fn with_registry<R>(&self, f: impl FnOnce(&mut Registry) -> R) -> R {
        self.registry.lock(|r| f(&mut r.borrow_mut()))
    }

    pub(crate) fn status(&self) -> ConnectionStatus {
        self.status.lock(|s| s.get())
    }

    pub(crate) fn set_status(&self, status: ConnectionStatus) {
        self.status.lock(|s| s.set(status));
        self.watch.sender().send(status);
    }

    pub(crate) fn options(&self) -> InitOptions {
        self.with_registry(|r| r.options)
    }

    pub(crate) fn endpoint(&self, index: usize) -> Option<EndpointEntry> {
        self.with_registry(|r| r.endpoints.get(index).copied())
    }

    pub(crate) fn node_name(&self, index: usize) -> Option<&'static str> {
        self.with_registry(|r| r.nodes.get(index).copied())
    }

    pub(crate) fn counts(&self) -> (usize, usize) {
        self.with_registry(|r| (r.nodes.len(), r.endpoints.len()))
    }

    /// Drop every queued sample/outgoing message.
    pub(crate) fn clear_queues(&self) {
        let (_, endpoints) = self.counts();
        for i in 0..endpoints {
            if let Some(e) = self.endpoint(i) {
                e.endpoint.reset();
            }
        }
    }

    pub(crate) fn set_clock_offset(&self, offset: Option<i64>) {
        self.clock_offset.lock(|c| c.set(offset));
    }

    pub(crate) fn clock_offset(&self) -> Option<i64> {
        self.clock_offset.lock(|c| c.get())
    }
}

impl Default for ContextStorage {
    fn default() -> Self {
        Self::new()
    }
}

/// Initialize the framework. Fails with [`Error::AlreadyStarted`] if called
/// twice on the same storage and [`Error::InvalidInput`] for bad options.
pub fn init(storage: &'static ContextStorage, options: InitOptions) -> Result<Context, Error> {
    options.validate()?;
    storage.with_registry(|r| {
        if r.phase != Phase::Uninit {
            return Err(Error::AlreadyStarted);
        }
        r.phase = Phase::Registering;
        r.options = options;
        Ok(())
    })?;
    Ok(Context { storage })
}

/// Cheap, copyable handle to the framework.
#[derive(Clone, Copy)]
pub struct Context {
    pub(crate) storage: &'static ContextStorage,
}

impl Context {
    pub fn clock(&self) -> Clock {
        Clock {
            storage: self.storage,
        }
    }

    pub fn connection(&self) -> Connection {
        Connection {
            storage: self.storage,
        }
    }

    /// Start registering a node (local only, no network I/O). Normally used
    /// through `#[embassy_ros::node]` + `NodeSpawner::register`.
    pub fn create_node(&self, name: &'static str) -> Result<NodeBuilder, StartupError> {
        let fail = |error| StartupError {
            node: name,
            topic: None,
            phase: StartupPhase::Node,
            error,
        };
        let valid = !name.is_empty()
            && name.len() <= NODE_NAME_MAX
            && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
            && !name.as_bytes()[0].is_ascii_digit();
        let index = self
            .storage
            .with_registry(|r| {
                phase_check(r.phase)?;
                if !valid || r.nodes.iter().any(|n| *n == name) {
                    r.phase = Phase::Poisoned;
                    return Err(Error::InvalidInput);
                }
                let index = r.nodes.len();
                if r.nodes.push(name).is_err() {
                    r.phase = Phase::Poisoned;
                    return Err(Error::CapacityExceeded);
                }
                Ok(index as u8)
            })
            .map_err(fail)?;
        Ok(NodeBuilder::new(*self, name, index))
    }

    /// Seal the node graph so a connection may start. Fails if any earlier
    /// registration failed (the configuration is poisoned).
    pub fn finish_registration(&self) -> Result<(), StartupError> {
        self.storage
            .with_registry(|r| match r.phase {
                Phase::Registering => {
                    r.phase = Phase::Sealed;
                    Ok(())
                }
                other => phase_check(other),
            })
            .map_err(|error| StartupError {
                node: "",
                topic: None,
                phase: StartupPhase::Finish,
                error,
            })
    }

    pub(crate) fn poison(&self) {
        self.storage.with_registry(|r| {
            if r.phase == Phase::Registering {
                r.phase = Phase::Poisoned;
            }
        });
    }
}

pub(crate) fn phase_check(phase: Phase) -> Result<(), Error> {
    match phase {
        Phase::Registering => Ok(()),
        Phase::Uninit => Err(Error::NotStarted),
        Phase::Sealed => Err(Error::AlreadyStarted),
        Phase::Poisoned => Err(Error::InvalidInput),
    }
}

/// Connection status access.
#[derive(Clone, Copy)]
pub struct Connection {
    storage: &'static ContextStorage,
}

impl Connection {
    pub fn status(&self) -> ConnectionStatus {
        self.storage.status()
    }

    /// Subscribe to status changes. At most [`MAX_CONNECTION_WATCHERS`]
    /// watches may exist at once ([`Error::CapacityExceeded`] otherwise).
    pub fn watch(&self) -> Result<ConnectionWatch, Error> {
        self.storage
            .watch
            .receiver()
            .map(|rx| ConnectionWatch { rx })
            .ok_or(Error::CapacityExceeded)
    }
}

/// Receiver of connection status updates.
pub struct ConnectionWatch {
    rx: Receiver<'static, Cs, ConnectionStatus, MAX_CONNECTION_WATCHERS>,
}

impl ConnectionWatch {
    /// The current status (marks it as seen).
    pub async fn current(&mut self) -> ConnectionStatus {
        self.rx.get().await
    }

    /// Wait for the next status update not yet seen by this watch.
    pub async fn changed(&mut self) -> ConnectionStatus {
        self.rx.changed().await
    }
}

/// ROS clock. Agent-synchronized once a time sync succeeded, otherwise
/// monotonic time since boot (not wall-clock time).
#[derive(Clone, Copy)]
pub struct Clock {
    storage: &'static ContextStorage,
}

impl Clock {
    pub fn now(&self) -> Timestamp {
        let local = monotonic_nanos(Instant::now());
        match self.storage.clock_offset() {
            Some(offset) => Timestamp::from_nanos(local.saturating_add(offset)),
            None => Timestamp::from_nanos(local),
        }
    }

    /// `true` once the clock has been synchronized with an Agent.
    pub fn is_synchronized(&self) -> bool {
        self.storage.clock_offset().is_some()
    }
}

pub(crate) fn monotonic_nanos(instant: Instant) -> i64 {
    (instant.as_micros() as i64).saturating_mul(1000)
}
