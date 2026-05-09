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
            ros2_replier_xml, ros2_requester_xml, ros2_topic_name, SERVICE_XML_MAX,
            TOPIC_NAME_MAX,
        },
        inner::{Frame, FRAME_BUF_SIZE},
        Context,
    },
    action::{
        Action, ActionClient, ActionClientHandles, ActionServer, ActionServerHandles,
        CancelGoalSrv, FeedbackMessage, GetResultSrv, GoalStatusArray, SendGoalSrv,
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

        let xml_snap = ros2_requester_xml::<SERVICE_XML_MAX>(
            S::SERVICE_NAME,
            S::REQUEST_TYPE_NAME,
            S::RESPONSE_TYPE_NAME,
        )?;
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

        let xml_snap = ros2_replier_xml::<SERVICE_XML_MAX>(
            S::SERVICE_NAME,
            S::REQUEST_TYPE_NAME,
            S::RESPONSE_TYPE_NAME,
        )?;
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

    /// Create a ROS2 action client for action `A`.
    ///
    /// Composes the underlying entities required by a ROS2 action:
    /// - 1 REQUESTER for the `send_goal` service (via `handles.send_goal`).
    /// - 1 REQUESTER for the `get_result` service (via `handles.get_result`).
    /// - 1 DataReader subscribed to the feedback topic
    ///   (`A::FEEDBACK_TOPIC_NAME`), backed by `feedback_slot`.
    ///
    /// `handles` and `feedback_slot` must both be `&'static`.  Typical wiring:
    ///
    /// ```ignore
    /// static FIB_HANDLES: ActionClientHandles<Fibonacci> = ActionClientHandles::new();
    /// subscription_slot!(static FIB_FB: FeedbackMessage<Fibonacci>, depth = 4);
    ///
    /// let client = node.create_action_client(&FIB_HANDLES, &FIB_FB).await?;
    /// let handle = client.send_goal(my_goal).await?;
    /// let result = handle.await_result().await?;
    /// ```
    ///
    /// CancelGoal service / GoalStatusArray topic are *not* created in this
    /// release — `GoalHandle::cancel` returns `Error::NotStarted` until
    /// v0.4-rc1 Phase 4 ships.
    pub async fn create_action_client<A: Action, const FB_N: usize>(
        &self,
        handles: &'static ActionClientHandles<A>,
        feedback_slot: &'static Subscription<FeedbackMessage<A>, FB_N>,
    ) -> Result<ActionClient<A, FB_N>, Error> {
        // 1. send_goal service client.
        let send_goal_client = self
            .create_service_client::<SendGoalSrv<A>>(&handles.send_goal)
            .await?;

        // 2. get_result service client.
        let get_result_client = self
            .create_service_client::<GetResultSrv<A>>(&handles.get_result)
            .await?;

        // 3. cancel_goal service client.
        let cancel_goal_client = self
            .create_service_client::<CancelGoalSrv<A>>(&handles.cancel_goal)
            .await?;

        // 4. feedback subscription — the create_subscription path performs its
        //    own CREATE_TOPIC + CREATE_DATAREADER + READ_DATA.
        self.create_subscription::<FeedbackMessage<A>, FB_N>(
            A::FEEDBACK_TOPIC_NAME,
            feedback_slot,
        )
        .await?;

        let client_key = self.ctx.inner.client_key();
        let action_idx = send_goal_client.requester_oid();
        Ok(ActionClient::new(
            send_goal_client,
            get_result_client,
            cancel_goal_client,
            feedback_slot,
            handles,
            client_key,
            action_idx,
        ))
    }

    /// Create a ROS2 action server for action `A`.
    ///
    /// Composes the server-side entities of a ROS2 action:
    /// - 3 REPLIER entities (`send_goal`, `get_result`, `cancel_goal`),
    ///   backed by the [`ActionServerHandles`] inbox slots.
    /// - 1 DataWriter on the feedback topic (`A::FEEDBACK_TOPIC_NAME`),
    ///   exposed through [`ActionServer`]'s internal `Publisher`.
    ///
    /// Status topic publishing is not wired in v0.4-rc1 — the server signals
    /// terminal goal state through `GetResult` replies, which the ROS2
    /// `action_client` interprets correctly.
    pub async fn create_action_server<
        A: Action,
        const SG_N: usize,
        const GR_N: usize,
        const CG_N: usize,
    >(
        &self,
        handles: &'static ActionServerHandles<A, SG_N, GR_N, CG_N>,
    ) -> Result<ActionServer<A, SG_N, GR_N, CG_N>, Error> {
        let send_goal_server = self
            .create_service_server::<SendGoalSrv<A>, SG_N>(&handles.send_goal)
            .await?;
        let get_result_server = self
            .create_service_server::<GetResultSrv<A>, GR_N>(&handles.get_result)
            .await?;
        let cancel_goal_server = self
            .create_service_server::<CancelGoalSrv<A>, CG_N>(&handles.cancel_goal)
            .await?;
        let feedback_pub = self
            .create_publisher::<FeedbackMessage<A>>(A::FEEDBACK_TOPIC_NAME)
            .await?;
        let status_pub = self
            .create_publisher::<GoalStatusArray>(A::STATUS_TOPIC_NAME)
            .await?;
        Ok(ActionServer::new(
            send_goal_server,
            get_result_server,
            cancel_goal_server,
            feedback_pub,
            status_pub,
            &handles.cancel_state,
        ))
    }
}
