//! Session engine host tests (handshake, entities, reconnect, epochs,
//! freshness, clock, keep-last, fragmentation/cancellation).

mod common;

use std::pin::pin;

use common::*;
use embassy_ros::{
    transport::{connect, ConnectFailureKind, ConnectionTask, Driver},
    ConnectionState, Context, ContextStorage, Error, InitOptions, Qos, Topic,
};
use embassy_time::{Duration, Instant};
use micro_xrce_dds_rs::{framing::FrameReader, RuntimeConfig};

const RT: RuntimeConfig = RuntimeConfig::new(0x81, [1, 2, 3, 4]);

fn connect_ok(ctx: Context, mock: &Mock, agent: &mut FakeAgent) -> (ConnectionTask, Driver<Mock>) {
    let fut = pin!(connect(ctx, mock.clone(), RT));
    match drive(fut, agent, 10_000) {
        Ok(v) => {
            agent.process();
            v
        }
        Err(f) => panic!("connect failed: {:?}", f.kind),
    }
}

#[test]
fn handshake_entities_data_and_reconnect() {
    let _l = lock();
    static S: ContextStorage = ContextStorage::new();
    static CHATTER: Topic<Cmd> = Topic::new("/chatter", Qos::sensor_data());
    static CMD: Topic<Cmd, 2> = Topic::new("/cmd_vel", Qos::command(Duration::from_millis(500)));

    let ctx = embassy_ros::init(&S, InitOptions::default()).unwrap();
    let mut talker = ctx.create_node("talker").unwrap();
    let publisher = talker.publisher(&CHATTER).unwrap();
    talker.finish();
    let mut listener = ctx.create_node("listener").unwrap();
    let mut sub = listener.subscription(&CMD).unwrap();
    listener.finish();
    ctx.finish_registration().unwrap();

    let mut watch = ctx.connection().watch().unwrap();
    assert_eq!(
        pump(pin!(watch.current()), &mut FakeAgent::new(Mock::new(1)))
            .unwrap()
            .epoch,
        0
    );

    let mock = Mock::new(1); // 1-byte reads: every frame is fragmented
    let mut agent = FakeAgent::new(mock.clone());
    let (task, mut driver) = connect_ok(ctx, &mock, &mut agent);

    let status = ctx.connection().status();
    assert_eq!((status.epoch, status.state), (1, ConnectionState::Ready));
    let changed = pump(pin!(watch.changed()), &mut agent).unwrap();
    assert!(changed.is_ready() && changed.epoch == 1);

    let ids: Vec<(u8, u16)> = agent.created.iter().map(|c| (c.kind, c.oid)).collect();
    assert_eq!(
        ids,
        vec![
            (1, 0x11),
            (3, 0x13),
            (1, 0x21),
            (4, 0x24),
            (2, 0x12),
            (5, 0x15),
            (2, 0x22),
            (6, 0x26)
        ]
    );
    assert!(agent.created[4].xml.contains("<name>rt/chatter</name>"));
    assert!(agent.created[4].xml.contains("test_msgs::msg::dds_::Cmd_"));
    assert!(agent.created[5].xml.starts_with("<dds><data_writer>"));
    assert!(agent.created[7].xml.contains("rt/cmd_vel"));
    assert_eq!(agent.created[0].seq, 0);
    assert_eq!(agent.reads, vec![0x26]);

    // Clock synchronized to the Agent.
    assert!(ctx.clock().is_synchronized());
    let now = ctx.clock().now();
    assert!((now.sec - FakeAgent::agent_now().sec).abs() <= 1);

    {
        let mut run = pin!(task.run_with_driver(&mut driver));
        publisher.publish(&Cmd { v: 7 }).unwrap();
        assert!(pump(run.as_mut(), &mut agent).is_none());
        assert_eq!(agent.writes, vec![(0x15, vec![7, 0, 0, 0])]);

        agent.send_data(0x26, &42i32.to_le_bytes());
        assert!(pump(run.as_mut(), &mut agent).is_none());
        let sample = pump(pin!(sub.recv_sample()), &mut agent).unwrap().unwrap();
        assert_eq!(sample.message, Cmd { v: 42 });
        assert_eq!(sample.epoch, 1);
        assert_eq!(sample.received_at, Instant::now());

        // A sample is queued, then the Agent closes the stream.
        agent.send_data(0x26, &1i32.to_le_bytes());
        assert!(pump(run.as_mut(), &mut agent).is_none());
        mock.s().closed = true;
        assert_eq!(pump(run.as_mut(), &mut agent), Some(Ok(())));
    }
    assert_eq!(
        ctx.connection().status().state,
        ConnectionState::Disconnected
    );
    assert!(
        pump(pin!(sub.recv_sample()), &mut agent).is_none(),
        "stale queue must be cleared"
    );
    // Publishing while disconnected is dropped silently.
    publisher.publish(&Cmd { v: 9 }).unwrap();

    // Reconnect (e.g. Agent restarted): entities are recreated, seq restarts.
    let socket = driver.into_inner();
    {
        let mut s = socket.s();
        s.closed = false;
        s.rx.clear();
    }
    let created_before = agent.created.len();
    let (task, mut driver) = connect_ok(ctx, &mock, &mut agent);
    assert_eq!(agent.client_creates, 2);
    assert_eq!(agent.created.len(), 2 * created_before);
    assert_eq!(agent.created[created_before].seq, 0);
    assert_eq!(agent.reads, vec![0x26, 0x26]);
    assert_eq!(ctx.connection().status().epoch, 2);
    assert_eq!(
        agent.writes.len(),
        1,
        "message published while disconnected must not be sent"
    );

    let mut run = pin!(task.run_with_driver(&mut driver));
    agent.send_data(0x26, &5i32.to_le_bytes());
    assert!(pump(run.as_mut(), &mut agent).is_none());
    let sample = pump(pin!(sub.recv_sample()), &mut agent).unwrap().unwrap();
    assert_eq!((sample.message.v, sample.epoch), (5, 2));
}

#[test]
fn cancellation_invalidates_and_command_freshness() {
    let _l = lock();
    static S: ContextStorage = ContextStorage::new();
    static CMD: Topic<Cmd> = Topic::new("/cmd", Qos::command(Duration::from_millis(500)));

    let ctx = embassy_ros::init(&S, InitOptions::default()).unwrap();
    let mut n = ctx.create_node("motor").unwrap();
    let mut sub = n.subscription(&CMD).unwrap();
    ctx.finish_registration().unwrap();

    let mock = Mock::new(3);
    let mut agent = FakeAgent::new(mock.clone());
    let (task, mut driver) = connect_ok(ctx, &mock, &mut agent);
    let dr = 0x16;
    {
        let mut run = pin!(task.run_with_driver(&mut driver));
        // Fresh command passes.
        agent.send_data(dr, &1i32.to_le_bytes());
        assert!(pump(run.as_mut(), &mut agent).is_none());
        advance(400);
        assert_eq!(pump(pin!(sub.recv()), &mut agent), Some(Ok(Cmd { v: 1 })));
        // Command older than max-age (measured from admission) is rejected.
        agent.send_data(dr, &2i32.to_le_bytes());
        assert!(pump(run.as_mut(), &mut agent).is_none());
        advance(501);
        assert_eq!(
            pump(pin!(sub.recv()), &mut agent),
            Some(Err(Error::StaleSample))
        );
        // Malformed sample reported as deserialization error.
        agent.send_data(dr, &[1, 2]);
        assert!(pump(run.as_mut(), &mut agent).is_none());
        assert_eq!(
            pump(pin!(sub.recv()), &mut agent),
            Some(Err(Error::Deserialization))
        );
        // Queued command then run cancelled (e.g. WiFi link down).
        agent.send_data(dr, &3i32.to_le_bytes());
        assert!(pump(run.as_mut(), &mut agent).is_none());
        assert!(ctx.connection().status().is_ready());
    } // run future dropped here
    let status = ctx.connection().status();
    assert_eq!(
        (status.epoch, status.state),
        (1, ConnectionState::Disconnected)
    );
    assert!(pump(pin!(sub.recv()), &mut agent).is_none());

    // A second connect while the first session is active is refused.
    let mock2 = Mock::new(8);
    let (task, driver) = connect_ok(ctx, &mock, &mut agent);
    let mut agent2 = FakeAgent::new(mock2.clone());
    let second = pump(pin!(connect(ctx, mock2.clone(), RT)), &mut agent2).unwrap();
    assert!(
        matches!(second, Err(f) if f.kind == ConnectFailureKind::Handshake(Error::AlreadyStarted))
    );
    assert_eq!(ctx.connection().status().epoch, 2);
    drop(task);
    drop(driver);
    assert!(!ctx.connection().status().is_ready());
}

#[test]
fn clock_fallback_then_periodic_sync_and_liveness() {
    let _l = lock();
    static S: ContextStorage = ContextStorage::new();
    static OUT: Topic<Cmd> = Topic::new("/out", Qos::sensor_data());
    let ctx = embassy_ros::init(&S, InitOptions::default()).unwrap();
    let mut n = ctx.create_node("n").unwrap();
    let _p = n.publisher(&OUT).unwrap();
    ctx.finish_registration().unwrap();

    advance(1_500);
    let t = ctx.clock().now();
    assert_eq!((t.sec, t.nanosec), (1, 500_000_000));
    assert!(!ctx.clock().is_synchronized());

    let mock = Mock::new(16);
    let mut agent = FakeAgent::new(mock.clone());
    agent.answer_timestamps = false;
    let (task, mut driver) = connect_ok(ctx, &mock, &mut agent);
    assert!(!ctx.clock().is_synchronized(), "sync timeout is non-fatal");
    assert!(ctx.connection().status().is_ready());

    agent.answer_timestamps = true;
    let mut run = pin!(task.run_with_driver(&mut driver));
    assert!(pump(run.as_mut(), &mut agent).is_none());
    advance(5_000);
    assert!(pump(run.as_mut(), &mut agent).is_none());
    assert!(pump(run.as_mut(), &mut agent).is_none());
    assert!(ctx.clock().is_synchronized());
    assert_eq!(ctx.clock().now().sec, FakeAgent::agent_now().sec);

    // Agent stops answering: liveness timeout ends the session.
    agent.silent = true;
    let result = drive(run.as_mut(), &mut agent, 30_000);
    assert_eq!(result, Err(Error::Timeout));
    assert_eq!(
        ctx.connection().status().state,
        ConnectionState::Disconnected
    );
    assert!(mock.s().resets >= 1);
    // The offset survives the disconnect.
    assert!(ctx.clock().is_synchronized());
}

#[test]
fn handshake_timeout_returns_socket() {
    let _l = lock();
    static S: ContextStorage = ContextStorage::new();
    let ctx = embassy_ros::init(&S, InitOptions::default()).unwrap();
    ctx.create_node("n").unwrap();
    ctx.finish_registration().unwrap();
    let mock = Mock::new(4);
    let mut agent = FakeAgent::new(mock.clone());
    agent.silent = true;
    let result = drive(pin!(connect(ctx, mock.clone(), RT)), &mut agent, 10_000);
    let Err(failure) = result else {
        panic!("expected failure")
    };
    assert_eq!(failure.kind, ConnectFailureKind::Handshake(Error::Timeout));
    assert_eq!(failure.socket.s().resets, 1);
    assert_eq!(
        ctx.connection().status().state,
        ConnectionState::Disconnected
    );
    // Retry works with the returned socket.
    agent.silent = false;
    let _ok = connect_ok(ctx, &failure.socket, &mut agent);
    assert_eq!(ctx.connection().status().epoch, 1);
}

#[test]
fn keep_last_and_serialization_overflow() {
    let _l = lock();
    static S: ContextStorage = ContextStorage::new();
    static OUT: Topic<Cmd, 2> = Topic::new("/out", Qos::sensor_data());
    static BIG: Topic<Blob> = Topic::new("/big", Qos::sensor_data());
    let ctx = embassy_ros::init(&S, InitOptions::default()).unwrap();
    let mut n = ctx.create_node("n").unwrap();
    let out = n.publisher(&OUT).unwrap();
    let big = n.publisher(&BIG).unwrap();
    ctx.finish_registration().unwrap();

    let mock = Mock::new(64);
    let mut agent = FakeAgent::new(mock.clone());
    let (task, mut driver) = connect_ok(ctx, &mock, &mut agent);
    for v in 1..=3 {
        out.publish(&Cmd { v }).unwrap();
    }
    big.publish(&Blob { n: 5000 }).unwrap();
    let mut run = pin!(task.run_with_driver(&mut driver));
    assert!(pump(run.as_mut(), &mut agent).is_none());
    let sent: Vec<i32> = agent
        .writes
        .iter()
        .filter(|w| w.0 == 0x15)
        .map(|w| i32::from_le_bytes(w.1[..4].try_into().unwrap()))
        .collect();
    assert_eq!(sent, vec![2, 3], "keep-last depth 2");
    assert!(
        agent.writes.iter().all(|w| w.0 != 0x25),
        "oversized message not sent"
    );
    assert_eq!(big.publish(&Blob { n: 10 }), Err(Error::BufferTooSmall));
    assert!(pump(run.as_mut(), &mut agent).is_none());
    assert_eq!(agent.writes.last().unwrap(), &(0x25, vec![0xAB; 10]));
    assert_eq!(big.publish(&Blob { n: 1 }), Ok(()));
}

#[test]
fn frame_reader_survives_cancellation() {
    let _l = lock();
    let mut mock = Mock::new(1);
    {
        let mut s = mock.s();
        s.rx.extend([5u8, 0, 1, 2]);
    }
    let mut reader = FrameReader::<16>::new();
    for _ in 0..3 {
        // Each read attempt is dropped after consuming what is available.
        let r = pump(
            pin!(reader.read_frame(&mut mock)),
            &mut FakeAgent::new(Mock::new(1)),
        );
        assert!(r.is_none());
    }
    assert!(reader.has_partial());
    mock.s().rx.extend([3u8, 4, 5, 2, 0, 9, 9]);
    let r = pump(
        pin!(reader.read_frame(&mut mock)),
        &mut FakeAgent::new(Mock::new(1)),
    );
    assert_eq!(r.unwrap().unwrap(), &[1, 2, 3, 4, 5]);
    let r = pump(
        pin!(reader.read_frame(&mut mock)),
        &mut FakeAgent::new(Mock::new(1)),
    );
    assert_eq!(r.unwrap().unwrap(), &[9, 9]);
    mock.s().rx.extend([0xFF, 0x00]);
    let r = pump(
        pin!(reader.read_frame(&mut mock)),
        &mut FakeAgent::new(Mock::new(1)),
    );
    assert_eq!(r.unwrap(), Err(Error::FrameTooLarge));
}
