//! Nodes and local (pre-network) endpoint registration.

use micro_xrce_dds_rs::{Error, Message};

use crate::{
    context::{phase_check, Context, EndpointEntry},
    topic::{Direction, Publisher, Subscription, Topic},
};

/// Registration step that failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum StartupPhase {
    Node,
    Publisher,
    Subscription,
    Spawn,
    Finish,
}

/// Local startup failure. Any failure poisons the context: `finish` will
/// fail and no Agent connection can be started with a partial graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct StartupError {
    pub node: &'static str,
    pub topic: Option<&'static str>,
    pub phase: StartupPhase,
    pub error: Error,
}

/// A registered node handle (passed to node tasks).
#[derive(Clone, Copy)]
pub struct Node {
    context: Context,
    name: &'static str,
    index: u8,
}

impl Node {
    pub fn context(&self) -> Context {
        self.context
    }

    pub fn name(&self) -> &'static str {
        self.name
    }

    pub fn index(&self) -> u8 {
        self.index
    }
}

/// Binds endpoints to a node during local startup.
pub struct NodeBuilder {
    context: Context,
    name: &'static str,
    index: u8,
}

impl NodeBuilder {
    pub(crate) fn new(context: Context, name: &'static str, index: u8) -> Self {
        Self {
            context,
            name,
            index,
        }
    }

    pub fn name(&self) -> &'static str {
        self.name
    }

    fn bind<M: Message + Clone + Send + 'static, const N: usize>(
        &self,
        topic: &'static Topic<M, N>,
        direction: Direction,
    ) -> Result<(), StartupError> {
        let storage = self.context.storage;
        let phase = match direction {
            Direction::Publish => StartupPhase::Publisher,
            Direction::Subscribe => StartupPhase::Subscription,
        };
        let result = storage
            .with_registry(|r| {
                phase_check(r.phase)?;
                if r.endpoints.is_full() {
                    return Err(Error::CapacityExceeded);
                }
                topic.bind(storage, direction)?;
                r.endpoints
                    .push(EndpointEntry {
                        node: self.index,
                        endpoint: topic,
                    })
                    .map_err(|_| Error::CapacityExceeded)
            })
            .map_err(|error| StartupError {
                node: self.name,
                topic: Some(topic.name()),
                phase,
                error,
            });
        if result.is_err() {
            self.context.poison();
        }
        result
    }

    pub fn publisher<M: Message + Clone + Send + 'static, const N: usize>(
        &mut self,
        topic: &'static Topic<M, N>,
    ) -> Result<Publisher<M>, StartupError> {
        self.bind(topic, Direction::Publish)?;
        Ok(Publisher::new(topic))
    }

    pub fn subscription<M: Message + Clone + Send + 'static, const N: usize>(
        &mut self,
        topic: &'static Topic<M, N>,
    ) -> Result<Subscription<M>, StartupError> {
        self.bind(topic, Direction::Subscribe)?;
        Ok(Subscription::new(topic))
    }

    pub fn finish(self) -> Node {
        Node {
            context: self.context,
            name: self.name,
            index: self.index,
        }
    }
}
