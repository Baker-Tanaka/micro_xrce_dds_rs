//! Local startup/registry validation (no transport).

mod common;

use common::*;
use embassy_ros::{
    transport::{connect, ConnectFailureKind},
    ContextStorage, Error, InitOptions, Qos, StartupPhase, Topic, MAX_CONNECTION_WATCHERS,
};
use embassy_time::Duration;
use micro_xrce_dds_rs::RuntimeConfig;
use std::pin::pin;

#[test]
fn init_twice_and_invalid_options() {
    static S: ContextStorage = ContextStorage::new();
    static S2: ContextStorage = ContextStorage::new();
    let bad = InitOptions {
        liveness_timeout: Duration::from_secs(1),
        ..InitOptions::default()
    };
    assert_eq!(embassy_ros::init(&S2, bad).err(), Some(Error::InvalidInput));
    embassy_ros::init(&S, InitOptions::default()).unwrap();
    assert_eq!(
        embassy_ros::init(&S, InitOptions::default()).err(),
        Some(Error::AlreadyStarted)
    );
}

#[test]
fn double_bind_poisons_registration() {
    static S: ContextStorage = ContextStorage::new();
    static T: Topic<Cmd> = Topic::new("/t", Qos::sensor_data());
    let ctx = embassy_ros::init(&S, InitOptions::default()).unwrap();
    let mut a = ctx.create_node("a").unwrap();
    a.publisher(&T).unwrap();
    let mut b = ctx.create_node("b").unwrap();
    let err = b.subscription(&T).err().unwrap();
    assert_eq!(
        (err.node, err.topic, err.phase, err.error),
        (
            "b",
            Some("/t"),
            StartupPhase::Subscription,
            Error::AlreadyStarted
        )
    );
    let err = ctx.finish_registration().unwrap_err();
    assert_eq!(err.error, Error::InvalidInput);
    // Not sealed => connect refuses (not retryable).
    let mock = Mock::new(1);
    let mut agent = FakeAgent::new(mock.clone());
    let r = pump(
        pin!(connect(ctx, mock, RuntimeConfig::new(0x81, [1, 2, 3, 4]))),
        &mut agent,
    )
    .unwrap();
    assert!(matches!(r, Err(f) if f.kind == ConnectFailureKind::InvalidConfiguration));
    assert_eq!(agent.client_creates, 0);
}

#[test]
fn names_and_capacity() {
    static S: ContextStorage = ContextStorage::new();
    static BAD: Topic<Cmd> = Topic::new("no_slash", Qos::sensor_data());
    let ctx = embassy_ros::init(&S, InitOptions::default()).unwrap();
    for name in ["", "has-dash", "9lead", "has space"] {
        assert_eq!(
            ctx.create_node(name).err().unwrap().error,
            Error::InvalidInput
        );
    }
    static S2: ContextStorage = ContextStorage::new();
    let ctx = embassy_ros::init(&S2, InitOptions::default()).unwrap();
    ctx.create_node("dup").unwrap();
    assert!(ctx.create_node("dup").is_err());

    static S3: ContextStorage = ContextStorage::new();
    let ctx = embassy_ros::init(&S3, InitOptions::default()).unwrap();
    let mut n = ctx.create_node("n").unwrap();
    assert_eq!(n.publisher(&BAD).err().unwrap().error, Error::InvalidInput);

    static S4: ContextStorage = ContextStorage::new();
    let ctx = embassy_ros::init(&S4, InitOptions::default()).unwrap();
    let watches: Vec<_> = (0..MAX_CONNECTION_WATCHERS)
        .map(|_| ctx.connection().watch().unwrap())
        .collect();
    assert_eq!(
        ctx.connection().watch().err(),
        Some(Error::CapacityExceeded)
    );
    drop(watches);
    ctx.create_node("ok").unwrap();
    ctx.finish_registration().unwrap();
    assert_eq!(
        ctx.create_node("late").err().unwrap().error,
        Error::AlreadyStarted
    );

    // Invalid session id / key are configuration errors.
    let mock = Mock::new(1);
    let mut agent = FakeAgent::new(mock.clone());
    for rt in [
        RuntimeConfig::new(0x80, [1, 2, 3, 4]),
        RuntimeConfig::new(0x81, [0; 4]),
    ] {
        let r = pump(pin!(connect(ctx, mock.clone(), rt)), &mut agent).unwrap();
        assert!(matches!(r, Err(f) if f.kind == ConnectFailureKind::InvalidConfiguration));
    }
}
