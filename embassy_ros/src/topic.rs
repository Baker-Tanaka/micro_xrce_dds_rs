//! Typed topics with static keep-last queues, publishers and subscriptions.

use core::{
    cell::RefCell,
    future::poll_fn,
    task::{Context as TaskContext, Poll},
};

use embassy_sync::{
    blocking_mutex::{raw::CriticalSectionRawMutex, Mutex},
    waitqueue::AtomicWaker,
};
use embassy_time::{Duration, Instant};
use heapless::Deque;
use micro_xrce_dds_rs::{cdr::CdrWriter, cdr_reader::CdrReader, Error, Message};

use crate::context::ContextStorage;

/// Maximum ROS topic name length (bytes, including the leading `/`).
pub(crate) const TOPIC_NAME_MAX: usize = 92;
/// Maximum DDS type name length (bytes).
pub(crate) const TYPE_NAME_MAX: usize = 128;

/// Endpoint delivery policy.
///
/// Both policies use the Agent's default DDS QoS for the created
/// DataWriter/DataReader and a local keep-last queue of the topic depth `N`.
/// `command` additionally rejects samples older than `max_age`, measured from
/// the instant the frame was admitted from the transport.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct Qos {
    max_age: Option<Duration>,
}

impl Qos {
    /// Periodic sensor data: keep-last, no age limit.
    pub const fn sensor_data() -> Self {
        Self { max_age: None }
    }

    /// Safety-relevant commands: keep-last; samples older than `max_age`
    /// (since admission) are reported as [`Error::StaleSample`].
    pub const fn command(max_age: Duration) -> Self {
        Self {
            max_age: Some(max_age),
        }
    }

    pub const fn max_age(&self) -> Option<Duration> {
        self.max_age
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Direction {
    Publish,
    Subscribe,
}

/// A received message with its admission time and connection epoch.
#[derive(Debug, Clone)]
pub struct Sample<M> {
    pub message: M,
    /// When the frame carrying the sample was received from the transport.
    pub received_at: Instant,
    pub epoch: u32,
}

/// Type-erased endpoint used by the session engine.
pub(crate) trait Endpoint: Sync {
    fn name(&self) -> &'static str;
    fn type_name(&self) -> &'static str;
    fn direction(&self) -> Option<Direction>;
    /// Drop all queued entries.
    fn reset(&self);
    /// Pop the oldest outgoing message of `epoch` and serialize it into `buf`.
    fn serialize_next(&self, buf: &mut [u8], epoch: u32) -> Option<Result<usize, Error>>;
    /// Admit an incoming CDR body.
    fn deliver(&self, body: &[u8], received_at: Instant, epoch: u32);
}

pub(crate) trait PublishEndpoint<M>: Sync {
    fn publish(&self, message: &M) -> Result<(), Error>;
}

pub(crate) trait SubscribeEndpoint<M>: Sync {
    fn poll_recv(&self, cx: &mut TaskContext<'_>) -> Poll<Result<Sample<M>, Error>>;
}

struct Entry<M> {
    message: Result<M, Error>,
    received_at: Instant,
    epoch: u32,
}

struct TopicInner<M, const N: usize> {
    binding: Option<(Direction, &'static ContextStorage)>,
    queue: Deque<Entry<M>, N>,
    pending_error: Option<Error>,
}

/// A statically allocated ROS topic with a keep-last queue of depth `N`.
///
/// One topic can be bound to exactly one publisher **or** one subscription.
pub struct Topic<M, const N: usize = 1> {
    name: &'static str,
    qos: Qos,
    inner: Mutex<CriticalSectionRawMutex, RefCell<TopicInner<M, N>>>,
    waker: AtomicWaker,
}

impl<M, const N: usize> Topic<M, N> {
    pub const fn new(name: &'static str, qos: Qos) -> Self {
        assert!(N > 0, "topic queue depth must be at least 1");
        Self {
            name,
            qos,
            inner: Mutex::new(RefCell::new(TopicInner {
                binding: None,
                queue: Deque::new(),
                pending_error: None,
            })),
            waker: AtomicWaker::new(),
        }
    }

    pub const fn name(&self) -> &'static str {
        self.name
    }

    pub const fn qos(&self) -> Qos {
        self.qos
    }
}

pub(crate) fn valid_topic_name(name: &str) -> bool {
    let b = name.as_bytes();
    b.len() >= 2
        && b.len() <= TOPIC_NAME_MAX
        && b[0] == b'/'
        && b[b.len() - 1] != b'/'
        && !name.contains("//")
        && b[1..]
            .iter()
            .all(|c| c.is_ascii_alphanumeric() || *c == b'_' || *c == b'/')
}

impl<M: Message + Clone + Send + 'static, const N: usize> Topic<M, N> {
    pub(crate) fn bind(
        &'static self,
        storage: &'static ContextStorage,
        direction: Direction,
    ) -> Result<(), Error> {
        if !valid_topic_name(self.name)
            || M::TYPE_NAME.is_empty()
            || M::TYPE_NAME.len() > TYPE_NAME_MAX
        {
            return Err(Error::InvalidInput);
        }
        self.inner.lock(|i| {
            let mut i = i.borrow_mut();
            if i.binding.is_some() {
                return Err(Error::AlreadyStarted);
            }
            i.binding = Some((direction, storage));
            Ok(())
        })
    }
}

impl<M: Message + Clone + Send + 'static, const N: usize> Endpoint for Topic<M, N> {
    fn name(&self) -> &'static str {
        self.name
    }

    fn type_name(&self) -> &'static str {
        M::TYPE_NAME
    }

    fn direction(&self) -> Option<Direction> {
        self.inner.lock(|i| i.borrow().binding.map(|(d, _)| d))
    }

    fn reset(&self) {
        self.inner.lock(|i| i.borrow_mut().queue.clear());
    }

    fn serialize_next(&self, buf: &mut [u8], epoch: u32) -> Option<Result<usize, Error>> {
        self.inner.lock(|i| {
            let mut i = i.borrow_mut();
            loop {
                let entry = i.queue.pop_front()?;
                if entry.epoch != epoch {
                    continue;
                }
                let Ok(message) = entry.message else { continue };
                let mut w = CdrWriter::new(buf);
                message.serialize(&mut w);
                let result = w.finish_checked().map(|b| b.len());
                if let Err(e) = result {
                    i.pending_error = Some(e);
                }
                return Some(result);
            }
        })
    }

    fn deliver(&self, body: &[u8], received_at: Instant, epoch: u32) {
        let message = M::deserialize(&mut CdrReader::from_body(body));
        self.inner.lock(|i| {
            let mut i = i.borrow_mut();
            if i.queue.is_full() {
                i.queue.pop_front();
            }
            let _ = i.queue.push_back(Entry {
                message,
                received_at,
                epoch,
            });
        });
        self.waker.wake();
    }
}

impl<M: Message + Clone + Send + 'static, const N: usize> PublishEndpoint<M> for Topic<M, N> {
    fn publish(&self, message: &M) -> Result<(), Error> {
        let (storage, previous_error) = self.inner.lock(|i| {
            let mut i = i.borrow_mut();
            let Some((Direction::Publish, storage)) = i.binding else {
                return Err(Error::NotStarted);
            };
            let previous_error = i.pending_error.take();
            let status = storage.status();
            if !status.is_ready() {
                return Ok((None, previous_error));
            }
            if i.queue.is_full() {
                i.queue.pop_front();
            }
            let _ = i.queue.push_back(Entry {
                message: Ok(message.clone()),
                received_at: Instant::now(),
                epoch: status.epoch,
            });
            Ok((Some(storage), previous_error))
        })?;
        if let Some(storage) = storage {
            storage.tx.signal(());
        }
        match previous_error {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
}

impl<M: Message + Clone + Send + 'static, const N: usize> SubscribeEndpoint<M> for Topic<M, N> {
    fn poll_recv(&self, cx: &mut TaskContext<'_>) -> Poll<Result<Sample<M>, Error>> {
        self.waker.register(cx.waker());
        self.inner.lock(|i| {
            let mut i = i.borrow_mut();
            let Some((Direction::Subscribe, storage)) = i.binding else {
                return Poll::Ready(Err(Error::NotStarted));
            };
            let status = storage.status();
            while let Some(entry) = i.queue.pop_front() {
                if !status.is_ready() || entry.epoch != status.epoch {
                    continue;
                }
                if let Some(max_age) = self.qos.max_age {
                    if Instant::now().saturating_duration_since(entry.received_at) > max_age {
                        return Poll::Ready(Err(Error::StaleSample));
                    }
                }
                return Poll::Ready(entry.message.map(|message| Sample {
                    message,
                    received_at: entry.received_at,
                    epoch: entry.epoch,
                }));
            }
            Poll::Pending
        })
    }
}

/// Typed, non-blocking publisher handle.
pub struct Publisher<M: 'static> {
    topic: &'static dyn PublishEndpoint<M>,
}

impl<M: 'static> Publisher<M> {
    pub(crate) fn new(topic: &'static dyn PublishEndpoint<M>) -> Self {
        Self { topic }
    }

    /// Queue `message` (keep-last; the oldest queued message is replaced when
    /// full) and wake the connection task. Never blocks.
    ///
    /// While no Agent session is ready the message is discarded and `Ok` is
    /// returned. `Ok` means accepted for sending, not delivered. A
    /// serialization failure (e.g. [`Error::BufferTooSmall`]) of an earlier
    /// message is reported once by the next call.
    pub fn publish(&self, message: &M) -> Result<(), Error> {
        self.topic.publish(message)
    }
}

/// Typed subscription handle.
pub struct Subscription<M: 'static> {
    topic: &'static dyn SubscribeEndpoint<M>,
}

impl<M: 'static> Subscription<M> {
    pub(crate) fn new(topic: &'static dyn SubscribeEndpoint<M>) -> Self {
        Self { topic }
    }

    /// Wait for the next sample of the current connection epoch.
    ///
    /// Samples from older epochs are discarded. Returns
    /// [`Error::StaleSample`] for a sample older than the QoS `max_age` and
    /// [`Error::Deserialization`] for a malformed one; both consume it.
    /// Cancel-safe.
    pub async fn recv_sample(&mut self) -> Result<Sample<M>, Error> {
        poll_fn(|cx| self.topic.poll_recv(cx)).await
    }

    /// Like [`Subscription::recv_sample`], returning only the message.
    pub async fn recv(&mut self) -> Result<M, Error> {
        self.recv_sample().await.map(|s| s.message)
    }
}
