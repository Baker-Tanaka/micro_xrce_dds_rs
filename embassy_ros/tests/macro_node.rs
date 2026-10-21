//! `#[embassy_ros::node]` expansion + NodeSpawner registration on the std executor.

mod common;

use std::sync::mpsc;

use common::Cmd;
use embassy_ros::{
    ContextStorage, InitOptions, Node, NodeSpawner, Publisher, Qos, StartupPhase, Subscription,
    Topic,
};
use embassy_time::Duration;

static OUT: Topic<Cmd, 2> = Topic::new("/out", Qos::sensor_data());
static CMD: Topic<Cmd> = Topic::new("/cmd", Qos::command(Duration::from_millis(500)));

pub struct Resources {
    pub id: u8,
}

#[embassy_ros::node(name = "worker")]
async fn worker(
    node: Node,
    mut resources: Resources,
    #[publisher(topic = &OUT)] publisher: Publisher<Cmd>,
    #[subscription(topic = &CMD)] mut commands: Subscription<Cmd>,
) {
    resources.id += 1;
    let _ = (node.name(), &publisher, resources.id);
    loop {
        let _ = commands.recv_sample().await;
    }
}

#[embassy_ros::node(name = "dup")]
async fn duplicate(_node: Node, #[publisher(topic = &OUT)] _p: Publisher<Cmd>) {}

#[test]
fn macro_registers_and_spawns() {
    static S: ContextStorage = ContextStorage::new();
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let executor = Box::leak(Box::new(embassy_executor::Executor::new()));
        executor.run(|spawner| {
            let context = embassy_ros::init(&S, InitOptions::default()).unwrap();
            let nodes = NodeSpawner::new(spawner, context);
            let first = nodes.register(worker, Resources { id: 1 });
            let second = nodes.register(duplicate, ());
            let finish = nodes.finish().map(|_| ());
            tx.send((first, second, finish)).unwrap();
        });
    });
    let (first, second, finish) = rx.recv().unwrap();
    assert_eq!(first, Ok(()));
    let err = second.unwrap_err();
    assert_eq!(
        (err.node, err.topic, err.phase),
        ("dup", Some("/out"), StartupPhase::Publisher)
    );
    assert_eq!(err.error, embassy_ros::Error::AlreadyStarted);
    assert_eq!(finish.unwrap_err().phase, StartupPhase::Finish);
}
