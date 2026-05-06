//! [`Node`] — ROS2-style node handle, and [`Context::create_node`].
//!
//! Each Embassy task that acts as a ROS2 node calls [`Context::create_node`]
//! once, then uses the returned `Node` to create publishers and subscriptions.
//!
//! ```ignore
//! #[embassy_executor::task]
//! async fn sensor_node(ctx: Context) -> ! {
//!     let node = ctx.create_node("sensor").await.unwrap();
//!     let pub_temp = node.create_publisher::<Float32>("/temperature").await.unwrap();
//!     loop {
//!         node.publish(&pub_temp, &Float32(read_temp())).await.ok();
//!         Timer::after_millis(100).await;
//!     }
//! }
//! ```

use core::fmt::Write as _;

use heapless::String as HString;

use crate::{
    error::Error,
    message::Message,
    protocol::*,
    publisher::Publisher,
    rt::{
        creation::send_create_and_wait,
        encode::{
            encode_create_participant, encode_create_with_parent, encode_read_data,
            ros2_topic_name, TOPIC_NAME_MAX,
        },
        inner::{Frame, FRAME_BUF_SIZE},
        Context,
    },
    service::{
        derive_writer_guid, Service, ServiceClient, ServiceClientHandles, ServiceServer,
        ServiceServerSlot,
    },
    subscription::{Subscription, SubscriptionSlot},
};

/// ROS2 node handle.  Cheap to copy — the underlying DDS entities live on the
/// agent.
///
/// Obtained from [`Context::create_node`].
#[derive(Clone, Copy)]
pub struct Node {
    pub(crate) ctx: Context,
    pub(crate) participant_idx: u16,
    pub(crate) publisher_idx: u16,
    pub(crate) subscriber_idx: u16,
}

// ── Context::create_node ──────────────────────────────────────────────────────

impl Context {
    /// Create a ROS2 node: allocates a DDS Participant plus a default Publisher
    /// and Subscriber on the agent, then returns a [`Node`] handle.
    pub async fn create_node(&self, name: &str) -> Result<Node, Error> {
        let inner = self.inner;

        let participant_idx = inner.alloc_participant();
        let publisher_idx = inner.alloc_publisher();
        let subscriber_idx = inner.alloc_subscriber();

        let participant_oid = object_id(participant_idx, ENTITY_PARTICIPANT);

        // ── Participant ───────────────────────────────────────────────────────
        let mut xml = HString::<128>::new();
        let _ = write!(
            xml,
            "<dds><participant><rtps><name>{}</name></rtps></participant></dds>",
            name
        );
        let xml_snap = xml; // copy so the closure can capture by value
        send_create_and_wait(inner, move |sid, key, seq, req, buf| {
            encode_create_participant(buf, sid, seq, &key, req, participant_oid, xml_snap.as_str(), 0)
        })
        .await?;

        // ── Default Publisher ─────────────────────────────────────────────────
        let publisher_oid = object_id(publisher_idx, ENTITY_PUBLISHER);
        send_create_and_wait(inner, move |sid, key, seq, req, buf| {
            encode_create_with_parent(
                buf, sid, seq, &key, req, publisher_oid, ENTITY_PUBLISHER,
                "<dds><publisher><name>default</name></publisher></dds>",
                participant_oid,
            )
        })
        .await?;

        // ── Default Subscriber ────────────────────────────────────────────────
        let subscriber_oid = object_id(subscriber_idx, ENTITY_SUBSCRIBER);
        send_create_and_wait(inner, move |sid, key, seq, req, buf| {
            encode_create_with_parent(
                buf, sid, seq, &key, req, subscriber_oid, ENTITY_SUBSCRIBER,
                "<dds><subscriber><name>default</name></subscriber></dds>",
                participant_oid,
            )
        })
        .await?;

        Ok(Node {
            ctx: *self,
            participant_idx,
            publisher_idx,
            subscriber_idx,
        })
    }
}

// ── Node methods ──────────────────────────────────────────────────────────────

impl Node {
    /// Create a typed Publisher for `topic` under this node.
    ///
    /// Allocates a DDS Topic and DataWriter on the agent.  The returned
    /// [`Publisher`] is `Copy` and can be passed to any Embassy task.
    pub async fn create_publisher<M: Message>(&self, topic: &str) -> Result<Publisher<M>, Error> {
        let inner = self.ctx.inner;

        let dds_topic = ros2_topic_name::<TOPIC_NAME_MAX>(topic)?;
        let participant_oid = object_id(self.participant_idx, ENTITY_PARTICIPANT);
        let publisher_oid = object_id(self.publisher_idx, ENTITY_PUBLISHER);

        // Topic
        let topic_idx = inner.alloc_topic();
        let topic_oid = object_id(topic_idx, ENTITY_TOPIC);
        let mut xml = HString::<256>::new();
        let _ = write!(
            xml,
            "<dds><topic><name>{}</name><dataType>{}</dataType></topic></dds>",
            dds_topic.as_str(),
            M::TYPE_NAME
        );
        let xml_snap = xml;
        send_create_and_wait(inner, move |sid, key, seq, req, buf| {
            encode_create_with_parent(
                buf, sid, seq, &key, req, topic_oid, ENTITY_TOPIC,
                xml_snap.as_str(), participant_oid,
            )
        })
        .await?;

        // DataWriter
        let dw_idx = inner.alloc_dw();
        let dw_oid = object_id(dw_idx, ENTITY_DATAWRITER);
        let mut dw_xml = HString::<320>::new();
        let _ = write!(
            dw_xml,
            "<dds><data_writer><topic><kind>NO_KEY</kind><name>{}</name><dataType>{}</dataType></topic></data_writer></dds>",
            dds_topic.as_str(),
            M::TYPE_NAME
        );
        let dw_xml_snap = dw_xml;
        send_create_and_wait(inner, move |sid, key, seq, req, buf| {
            encode_create_with_parent(
                buf, sid, seq, &key, req, dw_oid, ENTITY_DATAWRITER,
                dw_xml_snap.as_str(), publisher_oid,
            )
        })
        .await?;

        Ok(Publisher::new(dw_oid, self.ctx))
    }

    /// Register a subscription for `topic` under this node.
    ///
    /// Allocates a DDS Topic and DataReader on the agent, registers `slot` in
    /// the executor's dispatch table, and sends READ_DATA so the agent starts
    /// streaming samples.
    ///
    /// `slot` must be a `&'static Subscription<M, N>` declared with e.g.
    /// `static MY_SUB: Subscription<Float32> = Subscription::new();`
    pub async fn create_subscription<M: Message + Send + 'static, const N: usize>(
        &self,
        topic: &str,
        slot: &'static Subscription<M, N>,
    ) -> Result<&'static Subscription<M, N>, Error> {
        let inner = self.ctx.inner;

        // Early capacity check (definitive check is at the push below).
        {
            let subs = inner.subs.lock().await;
            if subs.is_full() {
                return Err(Error::TooManySubscriptions);
            }
        }

        let dds_topic = ros2_topic_name::<TOPIC_NAME_MAX>(topic)?;
        let participant_oid = object_id(self.participant_idx, ENTITY_PARTICIPANT);
        let subscriber_oid = object_id(self.subscriber_idx, ENTITY_SUBSCRIBER);

        // Topic
        let topic_idx = inner.alloc_topic();
        let topic_oid = object_id(topic_idx, ENTITY_TOPIC);
        let mut xml = HString::<256>::new();
        let _ = write!(
            xml,
            "<dds><topic><name>{}</name><dataType>{}</dataType></topic></dds>",
            dds_topic.as_str(),
            M::TYPE_NAME
        );
        let xml_snap = xml;
        send_create_and_wait(inner, move |sid, key, seq, req, buf| {
            encode_create_with_parent(
                buf, sid, seq, &key, req, topic_oid, ENTITY_TOPIC,
                xml_snap.as_str(), participant_oid,
            )
        })
        .await?;

        // DataReader
        let dr_idx = inner.alloc_dr();
        let dr_oid = object_id(dr_idx, ENTITY_DATAREADER);
        let mut dr_xml = HString::<320>::new();
        let _ = write!(
            dr_xml,
            "<dds><data_reader><topic><kind>NO_KEY</kind><name>{}</name><dataType>{}</dataType></topic></data_reader></dds>",
            dds_topic.as_str(),
            M::TYPE_NAME
        );
        let dr_xml_snap = dr_xml;
        send_create_and_wait(inner, move |sid, key, seq, req, buf| {
            encode_create_with_parent(
                buf, sid, seq, &key, req, dr_oid, ENTITY_DATAREADER,
                dr_xml_snap.as_str(), subscriber_oid,
            )
        })
        .await?;

        // Register slot BEFORE sending READ_DATA so the executor dispatch table
        // is ready by the time data arrives.
        slot.set_dr_id(dr_oid);
        inner
            .subs
            .lock()
            .await
            .push(slot as &'static dyn SubscriptionSlot)
            .map_err(|_| Error::TooManySubscriptions)?;

        // READ_DATA: fire-and-forget (no STATUS reply expected).
        let session_id = inner.session_id();
        let client_key = inner.client_key();
        let seq = inner.next_seq();
        let req = inner.next_req();
        let mut frame = Frame::zero();
        let len = encode_read_data(&mut frame.bytes, session_id, seq, &client_key, req, dr_oid)?;
        debug_assert!(len <= FRAME_BUF_SIZE);
        frame.len = len;
        inner.tx_channel.send(frame).await;

        Ok(slot)
    }

    /// Convenience wrapper: publish `msg` via a `Publisher` that belongs to
    /// this node.  Equivalent to `pub_.publish(msg)`.
    pub async fn publish<M: Message>(
        &self,
        pub_: &Publisher<M>,
        msg: &M,
    ) -> Result<(), Error> {
        pub_.publish(msg).await
    }

    /// Create a ROS2 service client for service `S`.  Allocates a `REQUESTER`
    /// entity on the agent and registers `handles.slot` in the dispatch
    /// table so incoming replies are routed to it.
    ///
    /// `handles` must be a `&'static ServiceClientHandles<S>` — declare one
    /// with `static FOO: ServiceClientHandles<S> = ServiceClientHandles::new();`
    /// or via the [`crate::service_client_slot!`] macro.
    pub async fn create_service_client<S: Service>(
        &self,
        handles: &'static ServiceClientHandles<S>,
    ) -> Result<ServiceClient<S>, Error> {
        let inner = self.ctx.inner;

        // Capacity check on the dispatch table before sending CREATE.
        {
            let subs = inner.subs.lock().await;
            if subs.is_full() {
                return Err(Error::TooManySubscriptions);
            }
        }

        let participant_oid = object_id(self.participant_idx, ENTITY_PARTICIPANT);

        // Use the DR allocator for requester index — its only purpose is to
        // keep counters monotonic.  Idx is shared across DR/REQUESTER/REPLIER.
        let requester_idx = inner.alloc_dr();
        let requester_oid = object_id(requester_idx, ENTITY_REQUESTER);

        let mut xml = HString::<320>::new();
        let _ = write!(
            xml,
            "<dds><requester><service_name>{}</service_name><request_type>{}</request_type><reply_type>{}</reply_type></requester></dds>",
            S::SERVICE_NAME,
            S::REQUEST_TYPE_NAME,
            S::RESPONSE_TYPE_NAME,
        );
        let xml_snap = xml;
        send_create_and_wait(inner, move |sid, key, seq, req, buf| {
            encode_create_with_parent(
                buf, sid, seq, &key, req, requester_oid, ENTITY_REQUESTER,
                xml_snap.as_str(), participant_oid,
            )
        })
        .await?;

        // Register the slot for dispatch BEFORE returning so the executor
        // can route any reply that arrives.
        handles.slot.set_requester_oid(requester_oid);
        inner
            .subs
            .lock()
            .await
            .push(&handles.slot as &'static dyn SubscriptionSlot)
            .map_err(|_| Error::TooManySubscriptions)?;

        let writer_guid = derive_writer_guid(&inner.client_key(), requester_oid);
        Ok(ServiceClient::new(
            requester_oid,
            self.ctx,
            handles,
            writer_guid,
        ))
    }

    /// Create a ROS2 service server for service `S`.  Allocates a `REPLIER`
    /// entity on the agent and registers `slot` in the dispatch table so
    /// incoming requests are routed to it.
    pub async fn create_service_server<S: Service, const N: usize>(
        &self,
        slot: &'static ServiceServerSlot<S, N>,
    ) -> Result<ServiceServer<S, N>, Error> {
        let inner = self.ctx.inner;

        {
            let subs = inner.subs.lock().await;
            if subs.is_full() {
                return Err(Error::TooManySubscriptions);
            }
        }

        let participant_oid = object_id(self.participant_idx, ENTITY_PARTICIPANT);

        let replier_idx = inner.alloc_dr();
        let replier_oid = object_id(replier_idx, ENTITY_REPLIER);

        let mut xml = HString::<320>::new();
        let _ = write!(
            xml,
            "<dds><replier><service_name>{}</service_name><request_type>{}</request_type><reply_type>{}</reply_type></replier></dds>",
            S::SERVICE_NAME,
            S::REQUEST_TYPE_NAME,
            S::RESPONSE_TYPE_NAME,
        );
        let xml_snap = xml;
        send_create_and_wait(inner, move |sid, key, seq, req, buf| {
            encode_create_with_parent(
                buf, sid, seq, &key, req, replier_oid, ENTITY_REPLIER,
                xml_snap.as_str(), participant_oid,
            )
        })
        .await?;

        slot.set_replier_oid(replier_oid);
        inner
            .subs
            .lock()
            .await
            .push(slot as &'static dyn SubscriptionSlot)
            .map_err(|_| Error::TooManySubscriptions)?;

        // READ_DATA so the agent starts streaming requests at us.
        let session_id = inner.session_id();
        let client_key = inner.client_key();
        let seq = inner.next_seq();
        let req = inner.next_req();
        let mut frame = Frame::zero();
        let len = encode_read_data(&mut frame.bytes, session_id, seq, &client_key, req, replier_oid)?;
        debug_assert!(len <= FRAME_BUF_SIZE);
        frame.len = len;
        inner.tx_channel.send(frame).await;

        Ok(ServiceServer::new(replier_oid, self.ctx, slot))
    }
}
