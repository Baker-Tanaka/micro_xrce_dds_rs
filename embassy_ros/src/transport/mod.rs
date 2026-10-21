//! Agent session engine (transport independent) and transport adapters.
//!
//! Lifecycle: [`connect`] performs the XRCE handshake on a [`Transport`],
//! (re)creates every DDS entity of the sealed node graph and returns a
//! [`ConnectionTask`] plus the [`Driver`] owning the transport.
//! [`ConnectionTask::run_with_driver`] pumps publishers/subscriptions until
//! the connection fails. When it returns **or is cancelled**, the status
//! becomes `Disconnected`, all queues are cleared and a new `connect` may be
//! started with the transport recovered by [`Driver::into_inner`].

#[cfg(feature = "embassy-net")]
pub mod embassy_net;

use core::fmt::Write as _;

use embassy_futures::select::{select3, Either3};
use embassy_time::{with_deadline, Duration, Instant, Timer};
use heapless::String;
use micro_xrce_dds_rs::{
    framing::{write_framed, FrameReader},
    protocol::*,
    rt::encode::{
        build_create_client, encode_create_participant, encode_create_with_parent,
        encode_read_data, encode_timestamp, finalize_write_data_headers, msg_header_len,
        parse_status_payload, parse_timestamp_reply,
    },
    Error, RuntimeConfig, Timestamp, Transport,
};

use crate::{
    context::{monotonic_nanos, ConnectionState, ConnectionStatus, Context, ContextStorage, Phase},
    topic::Direction,
};

/// Maximum XRCE frame size (both directions) handled by the engine.
pub const FRAME_CAPACITY: usize = 1024;
/// MTU announced in CREATE_CLIENT.
const CLIENT_MTU: u16 = 512;
const XML_CAPACITY: usize = 512;
const DOMAIN_ID: i16 = 0;

/// Owns the transport and the per-session framing/sequence state.
pub struct Driver<T> {
    transport: T,
    reader: FrameReader<FRAME_CAPACITY>,
    tx: [u8; FRAME_CAPACITY],
    session_id: u8,
    key: [u8; 4],
    seq: u16,
    req: u16,
}

impl<T> Driver<T> {
    fn new(transport: T, runtime: RuntimeConfig) -> Self {
        Self {
            transport,
            reader: FrameReader::new(),
            tx: [0; FRAME_CAPACITY],
            session_id: runtime.session_id,
            key: runtime.client_key,
            seq: 0,
            req: 0,
        }
    }

    /// Recover the transport (e.g. to abort and reconnect the TCP socket).
    pub fn into_inner(self) -> T {
        self.transport
    }

    fn next_seq(&mut self) -> u16 {
        let s = self.seq;
        self.seq = self.seq.wrapping_add(1);
        s
    }

    fn next_req(&mut self) -> u16 {
        self.req = self.req.wrapping_add(1);
        if self.req == 0 {
            self.req = 1;
        }
        self.req
    }
}

/// Why [`connect`] failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum ConnectFailureKind {
    /// Invalid session id / client key, or the node graph is not sealed
    /// (`NodeSpawner::finish` not called or startup failed). Not retryable.
    InvalidConfiguration,
    /// The TCP connection could not be established.
    #[cfg(feature = "embassy-net")]
    Tcp(::embassy_net::tcp::ConnectError),
    /// The TCP connection timed out (`AgentConfig::tcp_timeout`).
    #[cfg(feature = "embassy-net")]
    TcpTimeout,
    /// XRCE handshake or entity creation failed. `Handshake(AlreadyStarted)`
    /// means another connection of this context is still active.
    Handshake(Error),
}

/// A failed connection attempt; the transport is handed back for retry.
pub struct ConnectFailure<T> {
    pub kind: ConnectFailureKind,
    pub socket: T,
}

/// Marks the context session active; invalidates the connection on drop.
struct SessionGuard {
    storage: &'static ContextStorage,
}

impl SessionGuard {
    fn acquire(storage: &'static ContextStorage) -> Option<Self> {
        let acquired = storage.with_registry(|r| {
            if r.session_active {
                false
            } else {
                r.session_active = true;
                true
            }
        });
        if !acquired {
            return None;
        }
        let epoch = storage.status().epoch;
        storage.set_status(ConnectionStatus {
            epoch,
            state: ConnectionState::Connecting,
        });
        Some(Self { storage })
    }
}

impl Drop for SessionGuard {
    fn drop(&mut self) {
        let epoch = self.storage.status().epoch;
        self.storage.set_status(ConnectionStatus {
            epoch,
            state: ConnectionState::Disconnected,
        });
        self.storage.clear_queues();
        self.storage.tx.reset();
        self.storage.with_registry(|r| r.session_active = false);
    }
}

/// A ready Agent session. Dropping it (including cancelling
/// [`ConnectionTask::run_with_driver`]) immediately marks the connection
/// `Disconnected` and discards queued samples.
pub struct ConnectionTask {
    storage: &'static ContextStorage,
    _guard: SessionGuard,
}

pub(crate) fn validate(
    context: Context,
    runtime: &RuntimeConfig,
) -> Result<(), ConnectFailureKind> {
    let sid_ok = runtime.session_id != 0x00 && runtime.session_id != SESSION_ID_WITHOUT_CLIENT_KEY;
    let key_ok = runtime.client_key != [0; 4];
    let sealed = context.storage.with_registry(|r| r.phase == Phase::Sealed);
    if !(sid_ok && key_ok && sealed) {
        return Err(ConnectFailureKind::InvalidConfiguration);
    }
    if context.storage.with_registry(|r| r.session_active) {
        return Err(ConnectFailureKind::Handshake(Error::AlreadyStarted));
    }
    Ok(())
}

/// Establish an Agent session over an already connected byte stream.
///
/// Recreates all DDS entities with REUSE|REPLACE semantics (also after an
/// Agent restart), synchronizes the clock (best effort), clears all queues,
/// increments the connection epoch and sets the status to `Ready`.
/// On failure the transport is `reset()` and returned.
pub async fn connect<T: Transport>(
    context: Context,
    transport: T,
    runtime: RuntimeConfig,
) -> Result<(ConnectionTask, Driver<T>), ConnectFailure<T>> {
    let mut transport = transport;
    if let Err(kind) = validate(context, &runtime) {
        return Err(ConnectFailure {
            kind,
            socket: transport,
        });
    }
    let storage = context.storage;
    let Some(guard) = SessionGuard::acquire(storage) else {
        transport.reset();
        return Err(ConnectFailure {
            kind: ConnectFailureKind::Handshake(Error::AlreadyStarted),
            socket: transport,
        });
    };
    let mut driver = Driver::new(transport, runtime);
    if let Err(error) = handshake(storage, &mut driver).await {
        warn!("[embassy_ros] Agent handshake failed: {}", error);
        drop(guard);
        driver.transport.reset();
        return Err(ConnectFailure {
            kind: ConnectFailureKind::Handshake(error),
            socket: driver.transport,
        });
    }
    storage.clear_queues();
    storage.tx.reset();
    let epoch = storage.status().epoch.wrapping_add(1).max(1);
    storage.set_status(ConnectionStatus {
        epoch,
        state: ConnectionState::Ready,
    });
    info!("[embassy_ros] Agent session ready (epoch {})", epoch);
    Ok((
        ConnectionTask {
            storage,
            _guard: guard,
        },
        driver,
    ))
}

fn for_each_submessage(frame: &[u8], session_id: u8, mut f: impl FnMut(u8, u8, &[u8])) {
    let mut pos = msg_header_len(session_id);
    while pos + 4 <= frame.len() {
        let id = frame[pos];
        let flags = frame[pos + 1];
        let len = u16::from_le_bytes([frame[pos + 2], frame[pos + 3]]) as usize;
        let start = pos + 4;
        let end = start.saturating_add(len).min(frame.len());
        f(id, flags, &frame[start..end]);
        pos = (end + 3) & !3;
    }
}

async fn send<T: Transport>(
    driver: &mut Driver<T>,
    len: usize,
    deadline: Instant,
) -> Result<(), Error> {
    match with_deadline(
        deadline,
        write_framed(&mut driver.transport, &driver.tx[..len]),
    )
    .await
    {
        Ok(r) => r,
        Err(_) => Err(Error::Timeout),
    }
}

/// Read frames until `f` returns `Some`, or the deadline passes.
async fn wait_for<T: Transport, R>(
    driver: &mut Driver<T>,
    deadline: Instant,
    mut f: impl FnMut(&[u8]) -> Option<R>,
) -> Result<R, Error> {
    loop {
        let frame =
            match with_deadline(deadline, driver.reader.read_frame(&mut driver.transport)).await {
                Ok(r) => r?,
                Err(_) => return Err(Error::Timeout),
            };
        if let Some(r) = f(frame) {
            return Ok(r);
        }
    }
}

async fn request<T: Transport>(
    driver: &mut Driver<T>,
    len: usize,
    req: u16,
    timeout: Duration,
) -> Result<(), Error> {
    let deadline = Instant::now() + timeout;
    send(driver, len, deadline).await?;
    let sid = driver.session_id;
    wait_for(driver, deadline, |frame| {
        let mut result = None;
        for_each_submessage(frame, sid, |id, _, payload| {
            if id == SUBMSG_STATUS {
                if let Some((r, status)) = parse_status_payload(payload) {
                    if r == req {
                        result = Some(status.map(|_| ()));
                    }
                }
            }
        });
        result
    })
    .await?
}

fn local_timestamp(now: Instant) -> Timestamp {
    Timestamp::from_nanos(monotonic_nanos(now))
}

fn send_timestamp_frame<T>(driver: &mut Driver<T>) -> Result<usize, Error> {
    encode_timestamp(
        &mut driver.tx,
        driver.session_id,
        &driver.key,
        local_timestamp(Instant::now()),
    )
}

fn handle_timestamp_reply(storage: &ContextStorage, flags: u8, payload: &[u8]) -> bool {
    let Some(reply) = parse_timestamp_reply(payload, flags) else {
        return false;
    };
    let now = local_timestamp(Instant::now());
    if reply.originate > now {
        return false;
    }
    storage.set_clock_offset(Some(reply.offset_nanos(now)));
    true
}

fn xml<const N: usize>(args: core::fmt::Arguments<'_>) -> Result<String<N>, Error> {
    let mut s = String::new();
    s.write_fmt(args).map_err(|_| Error::BufferTooSmall)?;
    Ok(s)
}

async fn handshake<T: Transport>(
    storage: &'static ContextStorage,
    driver: &mut Driver<T>,
) -> Result<(), Error> {
    let options = storage.options();
    let timeout = options.request_timeout;
    let sid = driver.session_id;

    // CREATE_CLIENT -> STATUS_AGENT (resets an existing session with this key).
    let len = build_create_client(&mut driver.tx, sid, &driver.key, CLIENT_MTU);
    let deadline = Instant::now() + timeout;
    send(driver, len, deadline).await?;
    wait_for(driver, deadline, |frame| {
        let mut result = None;
        for_each_submessage(frame, sid, |id, _, payload| {
            if id == SUBMSG_STATUS_AGENT {
                result = Some(match payload.first() {
                    Some(&STATUS_OK) | Some(&STATUS_OK_MATCHED) => Ok(()),
                    Some(&s) => Err(Error::AgentRejected(s)),
                    None => Err(Error::UnexpectedReply),
                });
            }
        });
        result
    })
    .await??;

    // Clock synchronization (best effort; the Clock falls back to boot time).
    let len = send_timestamp_frame(driver)?;
    let deadline = Instant::now() + timeout;
    send(driver, len, deadline).await?;
    let synced = wait_for(driver, deadline, |frame| {
        let mut ok = None;
        for_each_submessage(frame, sid, |id, flags, payload| {
            if id == SUBMSG_TIMESTAMP_REPLY && handle_timestamp_reply(storage, flags, payload) {
                ok = Some(());
            }
        });
        ok
    })
    .await;
    match synced {
        Ok(()) => {}
        Err(Error::Timeout) => {
            warn!("[embassy_ros] Agent time sync timed out; using last offset/boot time")
        }
        Err(e) => return Err(e),
    }

    let (nodes, endpoints) = storage.counts();
    for n in 0..nodes {
        let name = storage.node_name(n).ok_or(Error::InvalidInput)?;
        let idx = (n + 1) as u16;
        let participant = object_id(idx, ENTITY_PARTICIPANT);
        let x: String<XML_CAPACITY> = xml(format_args!(
            "<dds><participant><rtps><name>{}</name></rtps></participant></dds>",
            name
        ))?;
        let (seq, req) = (driver.next_seq(), driver.next_req());
        let len = encode_create_participant(
            &mut driver.tx,
            sid,
            seq,
            &driver.key,
            req,
            participant,
            &x,
            DOMAIN_ID,
        )?;
        request(driver, len, req, timeout).await?;

        let mut has_pub = false;
        let mut has_sub = false;
        for j in 0..endpoints {
            if let Some(e) = storage.endpoint(j) {
                if e.node as usize == n {
                    match e.endpoint.direction() {
                        Some(Direction::Publish) => has_pub = true,
                        Some(Direction::Subscribe) => has_sub = true,
                        None => {}
                    }
                }
            }
        }
        for (present, kind, x) in [
            (
                has_pub,
                ENTITY_PUBLISHER,
                "<dds><publisher><name>default</name></publisher></dds>",
            ),
            (
                has_sub,
                ENTITY_SUBSCRIBER,
                "<dds><subscriber><name>default</name></subscriber></dds>",
            ),
        ] {
            if present {
                let (seq, req) = (driver.next_seq(), driver.next_req());
                let len = encode_create_with_parent(
                    &mut driver.tx,
                    sid,
                    seq,
                    &driver.key,
                    req,
                    object_id(idx, kind),
                    kind,
                    x,
                    participant,
                )?;
                request(driver, len, req, timeout).await?;
            }
        }
    }

    for j in 0..endpoints {
        let entry = storage.endpoint(j).ok_or(Error::InvalidInput)?;
        let ep = entry.endpoint;
        let direction = ep.direction().ok_or(Error::InvalidInput)?;
        let node_idx = entry.node as u16 + 1;
        let idx = (j + 1) as u16;
        let dds_name: String<{ crate::topic::TOPIC_NAME_MAX + 3 }> =
            xml(format_args!("rt/{}", &ep.name()[1..]))?;
        let type_name = ep.type_name();

        let topic_x: String<XML_CAPACITY> = xml(format_args!(
            "<dds><topic><name>{}</name><dataType>{}</dataType></topic></dds>",
            dds_name, type_name
        ))?;
        let (seq, req) = (driver.next_seq(), driver.next_req());
        let len = encode_create_with_parent(
            &mut driver.tx,
            sid,
            seq,
            &driver.key,
            req,
            object_id(idx, ENTITY_TOPIC),
            ENTITY_TOPIC,
            &topic_x,
            object_id(node_idx, ENTITY_PARTICIPANT),
        )?;
        request(driver, len, req, timeout).await?;

        let (kind, parent, tag) = match direction {
            Direction::Publish => (
                ENTITY_DATAWRITER,
                object_id(node_idx, ENTITY_PUBLISHER),
                "data_writer",
            ),
            Direction::Subscribe => (
                ENTITY_DATAREADER,
                object_id(node_idx, ENTITY_SUBSCRIBER),
                "data_reader",
            ),
        };
        let x: String<XML_CAPACITY> = xml(format_args!(
            "<dds><{tag}><topic><kind>NO_KEY</kind><name>{}</name><dataType>{}</dataType></topic></{tag}></dds>",
            dds_name, type_name, tag = tag
        ))?;
        let (seq, req) = (driver.next_seq(), driver.next_req());
        let oid = object_id(idx, kind);
        let len = encode_create_with_parent(
            &mut driver.tx,
            sid,
            seq,
            &driver.key,
            req,
            oid,
            kind,
            &x,
            parent,
        )?;
        request(driver, len, req, timeout).await?;

        if direction == Direction::Subscribe {
            let (seq, req) = (driver.next_seq(), driver.next_req());
            let len = encode_read_data(&mut driver.tx, sid, seq, &driver.key, req, oid)?;
            send(driver, len, Instant::now() + timeout).await?;
        }
        debug!("[embassy_ros] endpoint {} ready", ep.name());
    }
    Ok(())
}

impl ConnectionTask {
    /// Current connection epoch of this session.
    pub fn epoch(&self) -> u32 {
        self.storage.status().epoch
    }

    /// Pump the session until it fails. Returns `Ok(())` if the Agent closed
    /// the stream, `Err(Timeout)` if nothing was received for
    /// `InitOptions::liveness_timeout`, or the I/O error.
    ///
    /// Cancel-safe in the sense that dropping the future invalidates the
    /// session (status `Disconnected`, queues cleared). The driver must then
    /// be discarded via [`Driver::into_inner`] and the transport reset.
    pub async fn run_with_driver<T: Transport>(self, driver: &mut Driver<T>) -> Result<(), Error> {
        let result = self.run_inner(driver).await;
        driver.transport.reset();
        driver.reader.reset();
        drop(self);
        match result {
            Err(Error::Disconnected) => Ok(()),
            other => other,
        }
    }

    async fn flush<T: Transport>(
        &self,
        driver: &mut Driver<T>,
        epoch: u32,
        timeout: Duration,
    ) -> Result<(), Error> {
        let (_, endpoints) = self.storage.counts();
        let prefix = msg_header_len(driver.session_id) + 8;
        for j in 0..endpoints {
            let Some(entry) = self.storage.endpoint(j) else {
                continue;
            };
            if entry.endpoint.direction() != Some(Direction::Publish) {
                continue;
            }
            let oid = object_id((j + 1) as u16, ENTITY_DATAWRITER);
            while let Some(result) = entry
                .endpoint
                .serialize_next(&mut driver.tx[prefix..], epoch)
            {
                match result {
                    Ok(n) => {
                        let seq = driver.next_seq();
                        let key = driver.key;
                        finalize_write_data_headers(
                            &mut driver.tx[..prefix + n],
                            driver.session_id,
                            seq,
                            &key,
                            oid,
                        );
                        send(driver, prefix + n, Instant::now() + timeout).await?;
                    }
                    Err(e) => warn!(
                        "[embassy_ros] {}: serialization failed: {}",
                        entry.endpoint.name(),
                        e
                    ),
                }
            }
        }
        Ok(())
    }

    fn dispatch(&self, frame: &[u8], session_id: u8, received_at: Instant, epoch: u32) {
        let storage = self.storage;
        for_each_submessage(frame, session_id, |id, flags, payload| match id {
            SUBMSG_DATA if payload.len() >= 4 => {
                let oid = u16::from_be_bytes([payload[2], payload[3]]);
                if oid & 0x0F != ENTITY_DATAREADER as u16 || oid >> 4 == 0 {
                    return;
                }
                let j = (oid >> 4) as usize - 1;
                if let Some(e) = storage.endpoint(j) {
                    if e.endpoint.direction() == Some(Direction::Subscribe) {
                        e.endpoint.deliver(&payload[4..], received_at, epoch);
                    }
                }
            }
            SUBMSG_TIMESTAMP_REPLY => {
                handle_timestamp_reply(storage, flags, payload);
            }
            SUBMSG_STATUS => {
                if let Some((_, Err(e))) = parse_status_payload(payload) {
                    warn!("[embassy_ros] Agent reported error: {}", e);
                }
            }
            _ => {}
        });
    }

    async fn run_inner<T: Transport>(&self, driver: &mut Driver<T>) -> Result<(), Error> {
        let options = self.storage.options();
        let epoch = self.storage.status().epoch;
        let mut last_rx = Instant::now();
        let mut next_sync = last_rx + options.time_sync_interval;
        loop {
            self.flush(driver, epoch, options.request_timeout).await?;
            let liveness_deadline = last_rx + options.liveness_timeout;
            let deadline = next_sync.min(liveness_deadline);
            let sid = driver.session_id;
            match select3(
                driver.reader.read_frame(&mut driver.transport),
                self.storage.tx.wait(),
                Timer::at(deadline),
            )
            .await
            {
                Either3::First(Ok(frame)) => {
                    last_rx = Instant::now();
                    self.dispatch(frame, sid, last_rx, epoch);
                }
                Either3::First(Err(e)) => return Err(e),
                Either3::Second(()) => {}
                Either3::Third(()) => {
                    let now = Instant::now();
                    if now >= liveness_deadline {
                        return Err(Error::Timeout);
                    }
                    if now >= next_sync {
                        next_sync = now + options.time_sync_interval;
                        let len = send_timestamp_frame(driver)?;
                        send(driver, len, now + options.request_timeout).await?;
                    }
                }
            }
        }
    }
}
