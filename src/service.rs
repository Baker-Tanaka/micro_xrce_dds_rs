//! ROS2-style service support — `Service` trait, `ServiceClient`, `ServiceServer`.
//!
//! Backed by XRCE-DDS `OBJK_REQUESTER` (0x07) / `OBJK_REPLIER` (0x08) entities.
//! At the wire level a requester (resp. replier) owns one DataWriter and one
//! DataReader behind a single object_id; `WRITE_DATA(requester_oid, body)`
//! sends a request and incoming `DATA(requester_oid, body)` carries the reply.
//!
//! Each ROS service body starts with a 24-byte `SampleIdentity` (16-byte writer
//! GUID + 8-byte i64 sequence number) which the agent echoes from request to
//! reply.  `ServiceClient::call` uses the sequence number to correlate replies
//! to the right pending call; `ServiceRequest::reply` echoes it back so the
//! original requester can match.
//!
//! ```ignore
//! pub struct AddTwoInts;
//! impl Service for AddTwoInts {
//!     type Request  = AddTwoIntsRequest;   // implements Message
//!     type Response = AddTwoIntsResponse;
//!     const SERVICE_NAME:        &'static str = "/add_two_ints";
//!     const REQUEST_TYPE_NAME:   &'static str = "example_interfaces::srv::dds_::AddTwoInts_Request_";
//!     const RESPONSE_TYPE_NAME:  &'static str = "example_interfaces::srv::dds_::AddTwoInts_Response_";
//! }
//!
//! static ADD_TWO_HANDLES: ServiceClientHandles<AddTwoInts> = ServiceClientHandles::new();
//!
//! let client = node.create_service_client::<AddTwoInts>(&ADD_TWO_HANDLES).await?;
//! let resp = client.call(&req).await?;
//! ```

use core::marker::PhantomData;

use embassy_sync::blocking_mutex::raw::CriticalSectionRawMutex;
use embassy_sync::channel::Channel;
use embassy_sync::mutex::Mutex;
use portable_atomic::{AtomicI64, AtomicU16, Ordering};

use crate::{
    cdr::CdrWriter,
    cdr_reader::CdrReader,
    error::Error,
    message::Message,
    rt::{
        encode::{finalize_service_write_data, msg_header_len},
        inner::{Frame, FRAME_BUF_SIZE},
        Context,
    },
    subscription::SubscriptionSlot,
};

// ── Service trait ─────────────────────────────────────────────────────────────

/// Defines a ROS2 service by its request / response message types and DDS
/// type names.
///
/// The DDS type names follow the rosidl convention
/// `<pkg>::srv::dds_::<Service>_Request_` / `..._Response_`.
pub trait Service: 'static {
    type Request: Message + Send + 'static;
    type Response: Message + Send + 'static;

    /// ROS service name, e.g. `"/add_two_ints"`.  Mapped to DDS topics
    /// `rq/<name>Request` and `rr/<name>Reply` automatically.
    const SERVICE_NAME: &'static str;
    /// rosidl-generated DDS type name for the request half.
    const REQUEST_TYPE_NAME: &'static str;
    /// rosidl-generated DDS type name for the response half.
    const RESPONSE_TYPE_NAME: &'static str;
}

// ── SampleIdentity ────────────────────────────────────────────────────────────

/// 24-byte RPC correlation prefix carried at the start of every service
/// request / reply CDR body.
///
/// `writer_guid` is a stable per-client identifier; `sequence_number` is a
/// monotonically increasing counter so the receiver can match a reply to the
/// request that produced it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SampleIdentity {
    pub writer_guid: [u8; 16],
    pub sequence_number: i64,
}

impl SampleIdentity {
    /// Length of the CDR-serialised form (16 + 8 bytes, naturally aligned).
    pub const SERIALIZED_LEN: usize = 24;

    pub fn serialize(&self, w: &mut CdrWriter) {
        w.bytes_raw(&self.writer_guid);
        w.i64_val(self.sequence_number);
    }

    pub fn deserialize(r: &mut CdrReader) -> Result<Self, Error> {
        let writer_guid = r.bytes_array::<16>()?;
        let sequence_number = r.i64_val()?;
        Ok(Self {
            writer_guid,
            sequence_number,
        })
    }
}

#[cfg(feature = "defmt")]
impl defmt::Format for SampleIdentity {
    fn format(&self, f: defmt::Formatter) {
        defmt::write!(f, "SampleIdentity(seq={})", self.sequence_number);
    }
}

// ── ServiceClient ─────────────────────────────────────────────────────────────

/// Inbox + correlation state for a single [`ServiceClient`].
pub struct ServiceClientSlot<S: Service> {
    requester_oid: AtomicU16,
    pending_seq: AtomicI64,
    inbox: Channel<CriticalSectionRawMutex, S::Response, 1>,
    _phantom: PhantomData<fn() -> S>,
}

impl<S: Service> ServiceClientSlot<S> {
    pub const fn new() -> Self {
        Self {
            requester_oid: AtomicU16::new(0),
            pending_seq: AtomicI64::new(0),
            inbox: Channel::new(),
            _phantom: PhantomData,
        }
    }

    /// Bind the slot to a requester object_id.  Normally invoked by
    /// [`crate::Node::create_service_client`]; exposed for tests and advanced
    /// integrations.
    pub fn set_requester_oid(&self, oid: u16) {
        self.requester_oid.store(oid, Ordering::Release);
    }

    /// Arm the slot for an in-flight call.  Replies are dropped unless their
    /// `SampleIdentity.sequence_number` matches `seq`.  Set to 0 to disarm.
    /// Normally driven by [`ServiceClient::call`]; exposed for tests.
    pub fn set_pending_seq(&self, seq: i64) {
        self.pending_seq.store(seq, Ordering::Release);
    }

    /// Non-blocking peek at the response inbox.  Returns `None` if no reply
    /// has been routed yet.
    pub fn try_recv_response(&self) -> Option<S::Response> {
        self.inbox.try_receive().ok()
    }
}

impl<S: Service> Default for ServiceClientSlot<S> {
    fn default() -> Self {
        Self::new()
    }
}

impl<S: Service> SubscriptionSlot for ServiceClientSlot<S> {
    fn dr_id(&self) -> u16 {
        self.requester_oid.load(Ordering::Acquire)
    }

    fn try_deliver(&self, payload: &[u8]) -> Result<(), Error> {
        let mut r = CdrReader::from_body(payload);
        let id = SampleIdentity::deserialize(&mut r)?;
        let pending = self.pending_seq.load(Ordering::Acquire);
        if pending == 0 || id.sequence_number != pending {
            // Stale / mismatched reply — drop silently.
            return Ok(());
        }
        let resp = S::Response::deserialize(&mut r)?;
        self.inbox
            .try_send(resp)
            .map_err(|_| Error::ServiceOverflow)
    }
}

/// Bundle of `'static` state a [`ServiceClient`] needs.  Declare one per
/// service client — typically as a `static` so it lives forever:
///
/// ```ignore
/// static ADD_TWO_HANDLES: ServiceClientHandles<AddTwoInts> = ServiceClientHandles::new();
/// ```
pub struct ServiceClientHandles<S: Service> {
    pub slot: ServiceClientSlot<S>,
    pub seq: AtomicI64,
    pub call_lock: Mutex<CriticalSectionRawMutex, ()>,
}

impl<S: Service> ServiceClientHandles<S> {
    pub const fn new() -> Self {
        Self {
            slot: ServiceClientSlot::new(),
            seq: AtomicI64::new(0),
            call_lock: Mutex::new(()),
        }
    }
}

impl<S: Service> Default for ServiceClientHandles<S> {
    fn default() -> Self {
        Self::new()
    }
}

/// Client handle for invoking a ROS2 service.
///
/// Cheap-Copy: pass by value into any Embassy task.  Concurrent calls on the
/// same client are serialized internally so only one round-trip is in flight.
pub struct ServiceClient<S: Service> {
    requester_oid: u16,
    ctx: Context,
    handles: &'static ServiceClientHandles<S>,
    writer_guid: [u8; 16],
}

impl<S: Service> Clone for ServiceClient<S> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<S: Service> Copy for ServiceClient<S> {}

impl<S: Service> ServiceClient<S> {
    pub(crate) fn new(
        requester_oid: u16,
        ctx: Context,
        handles: &'static ServiceClientHandles<S>,
        writer_guid: [u8; 16],
    ) -> Self {
        Self {
            requester_oid,
            ctx,
            handles,
            writer_guid,
        }
    }

    /// Send `req` and wait for a matching reply.  Concurrent callers are
    /// serialized — the second `call().await` waits for the first to complete.
    ///
    /// Returns `Error::Disconnected` if the executor has stopped.  The caller
    /// is responsible for racing against an external timeout (e.g.
    /// `embassy_time::with_timeout`); this function itself awaits indefinitely.
    pub async fn call(&self, req: &S::Request) -> Result<S::Response, Error> {
        let _guard = self.handles.call_lock.lock().await;
        let inner = self.ctx.inner;
        if inner.is_disconnected() {
            return Err(Error::Disconnected);
        }

        // Drain any stale reply leftover from a previous cancelled call.
        while self.handles.slot.inbox.try_receive().is_ok() {}

        let seq = loop {
            let prev = self.handles.seq.fetch_add(1, Ordering::Relaxed);
            let n = prev.wrapping_add(1);
            if n != 0 {
                break n;
            }
        };
        self.handles.slot.pending_seq.store(seq, Ordering::Release);

        let identity = SampleIdentity {
            writer_guid: self.writer_guid,
            sequence_number: seq,
        };

        let session_id = inner.session_id();
        let prefix = msg_header_len(session_id) + 4 + 4;
        let mut frame = Frame::zero();
        let body_len = {
            let body = &mut frame.bytes[prefix..];
            let mut w = CdrWriter::new(body);
            identity.serialize(&mut w);
            req.serialize(&mut w);
            w.bytes_written()
        };
        let total = prefix + body_len;
        if total > FRAME_BUF_SIZE {
            self.handles.slot.pending_seq.store(0, Ordering::Release);
            return Err(Error::BufferTooSmall);
        }
        let msg_seq = inner.next_seq();
        finalize_service_write_data(
            &mut frame.bytes[..total],
            session_id,
            msg_seq,
            &inner.client_key(),
            self.requester_oid,
        );
        frame.len = total;
        inner.tx_channel.send(frame).await;

        let resp = self.handles.slot.inbox.receive().await;
        self.handles.slot.pending_seq.store(0, Ordering::Release);
        Ok(resp)
    }

    /// XRCE object_id of this requester (idx<<4 | 0x07).  For debugging.
    pub fn requester_oid(&self) -> u16 {
        self.requester_oid
    }
}

// ── ServiceServer ─────────────────────────────────────────────────────────────

#[doc(hidden)]
pub struct IncomingRequest<S: Service> {
    pub identity: SampleIdentity,
    pub payload: S::Request,
}

unsafe impl<S: Service> Send for IncomingRequest<S> {}

/// Inbox slot for a [`ServiceServer`].  Each slot buffers up to `N`
/// pending requests waiting for the user to call `recv_request().await`.
pub struct ServiceServerSlot<S: Service, const N: usize = 4> {
    replier_oid: AtomicU16,
    inbox: Channel<CriticalSectionRawMutex, IncomingRequest<S>, N>,
}

impl<S: Service, const N: usize> ServiceServerSlot<S, N> {
    pub const fn new() -> Self {
        Self {
            replier_oid: AtomicU16::new(0),
            inbox: Channel::new(),
        }
    }

    /// Bind the slot to a replier object_id.  Normally invoked by
    /// [`crate::Node::create_service_server`]; exposed for tests.
    pub fn set_replier_oid(&self, oid: u16) {
        self.replier_oid.store(oid, Ordering::Release);
    }

    /// Non-blocking peek at the request inbox.  Returns `None` if no request
    /// is waiting.
    pub fn try_recv_request(&self) -> Option<IncomingRequest<S>> {
        self.inbox.try_receive().ok()
    }
}

impl<S: Service, const N: usize> Default for ServiceServerSlot<S, N> {
    fn default() -> Self {
        Self::new()
    }
}

impl<S: Service, const N: usize> SubscriptionSlot for ServiceServerSlot<S, N> {
    fn dr_id(&self) -> u16 {
        self.replier_oid.load(Ordering::Acquire)
    }

    fn try_deliver(&self, payload: &[u8]) -> Result<(), Error> {
        let mut r = CdrReader::from_body(payload);
        let identity = SampleIdentity::deserialize(&mut r)?;
        let payload = S::Request::deserialize(&mut r)?;
        self.inbox
            .try_send(IncomingRequest { identity, payload })
            .map_err(|_| Error::ServiceOverflow)
    }
}

/// Server handle for replying to incoming service requests.
pub struct ServiceServer<S: Service, const N: usize = 4> {
    replier_oid: u16,
    ctx: Context,
    slot: &'static ServiceServerSlot<S, N>,
}

impl<S: Service, const N: usize> Clone for ServiceServer<S, N> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<S: Service, const N: usize> Copy for ServiceServer<S, N> {}

impl<S: Service, const N: usize> ServiceServer<S, N> {
    pub(crate) fn new(
        replier_oid: u16,
        ctx: Context,
        slot: &'static ServiceServerSlot<S, N>,
    ) -> Self {
        Self {
            replier_oid,
            ctx,
            slot,
        }
    }

    /// Wait for the next incoming request.  The returned [`ServiceRequest`]
    /// owns the reply path — call `.reply(&resp).await` to send the response.
    pub async fn recv_request(&self) -> ServiceRequest<S> {
        let req = self.slot.inbox.receive().await;
        ServiceRequest {
            identity: req.identity,
            payload: req.payload,
            replier_oid: self.replier_oid,
            ctx: self.ctx,
        }
    }

    pub fn replier_oid(&self) -> u16 {
        self.replier_oid
    }
}

/// One pending service request received from a client.
///
/// Holds the deserialized request plus the [`SampleIdentity`] needed to route
/// the reply back to the originating client.  `reply` borrows `&self`, so the
/// caller can `req.reply(&resp).await?;` then continue using `req.payload` —
/// this is the pattern action-server `accept_next_goal` follows.
pub struct ServiceRequest<S: Service> {
    pub identity: SampleIdentity,
    pub payload: S::Request,
    pub(crate) replier_oid: u16,
    pub(crate) ctx: Context,
}

impl<S: Service> ServiceRequest<S> {
    /// Reply with the given response payload.  The original `SampleIdentity`
    /// is echoed so the client can match it to the call that produced it.
    ///
    /// Borrows `&self` rather than consuming it: idempotent retries are
    /// safe (each call resends a fresh WRITE_DATA frame), and callers can
    /// reply *before* unpacking `payload` if the response carries no fields
    /// derived from the request.
    pub async fn reply(&self, resp: &S::Response) -> Result<(), Error> {
        let inner = self.ctx.inner;
        if inner.is_disconnected() {
            return Err(Error::Disconnected);
        }

        let session_id = inner.session_id();
        let prefix = msg_header_len(session_id) + 4 + 4;
        let mut frame = Frame::zero();
        let body_len = {
            let body = &mut frame.bytes[prefix..];
            let mut w = CdrWriter::new(body);
            self.identity.serialize(&mut w);
            resp.serialize(&mut w);
            w.bytes_written()
        };
        let total = prefix + body_len;
        if total > FRAME_BUF_SIZE {
            return Err(Error::BufferTooSmall);
        }
        let msg_seq = inner.next_seq();
        finalize_service_write_data(
            &mut frame.bytes[..total],
            session_id,
            msg_seq,
            &inner.client_key(),
            self.replier_oid,
        );
        frame.len = total;
        inner.tx_channel.send(frame).await;
        Ok(())
    }
}

// ── Macros ────────────────────────────────────────────────────────────────────

/// Declare a `'static` service-client bundle (slot + seq counter + call lock).
///
/// ```ignore
/// service_client_slot!(static ADD_TWO: AddTwoInts);
/// // Then:
/// let client = node.create_service_client::<AddTwoInts>(&ADD_TWO).await?;
/// ```
#[macro_export]
macro_rules! service_client_slot {
    (static $name:ident : $S:ty) => {
        static $name: $crate::service::ServiceClientHandles<$S> =
            $crate::service::ServiceClientHandles::new();
    };
}

/// Declare a `'static` service-server slot.  Optional `depth = N` controls
/// inbox capacity (default 4).
///
/// ```ignore
/// service_server_slot!(static ADD_TWO_SRV: AddTwoInts);
/// service_server_slot!(static ADD_TWO_SRV: AddTwoInts, depth = 8);
/// ```
#[macro_export]
macro_rules! service_server_slot {
    (static $name:ident : $S:ty , depth = $N:expr) => {
        static $name: $crate::service::ServiceServerSlot<$S, $N> =
            $crate::service::ServiceServerSlot::new();
    };
    (static $name:ident : $S:ty) => {
        static $name: $crate::service::ServiceServerSlot<$S, 4> =
            $crate::service::ServiceServerSlot::new();
    };
}

// ── Internal helper used by Node::create_service_client ──────────────────────

/// Derive a stable 16-byte client GUID from `client_key + requester_oid`.
/// Bit-pattern: `[ck0, ck1, ck2, ck3, oid_hi, oid_lo, 0,0,0,0, 'X','R','C','E','C','L']`.
pub(crate) fn derive_writer_guid(client_key: &[u8; 4], requester_oid: u16) -> [u8; 16] {
    let mut g = [0u8; 16];
    g[0..4].copy_from_slice(client_key);
    g[4] = (requester_oid >> 8) as u8;
    g[5] = requester_oid as u8;
    g[10..16].copy_from_slice(b"XRCECL");
    g
}
