//! Executor integration: register `#[embassy_ros::node]` functions.

use embassy_executor::{SpawnError, SpawnToken, Spawner};
use micro_xrce_dds_rs::Error;

use crate::{
    context::Context,
    node::{NodeBuilder, StartupError, StartupPhase},
};

/// Registers nodes and spawns their tasks. All registration is local; the
/// Agent connection is started separately after [`NodeSpawner::finish`].
pub struct NodeSpawner {
    spawner: Spawner,
    context: Context,
}

impl NodeSpawner {
    pub fn new(spawner: Spawner, context: Context) -> Self {
        Self { spawner, context }
    }

    pub fn context(&self) -> Context {
        self.context
    }

    pub fn spawner(&self) -> Spawner {
        self.spawner
    }

    /// Register one node: `nodes.register(imu::node, resources)?`.
    pub fn register<R, F>(&self, node: F, resources: R) -> Result<(), StartupError>
    where
        F: FnOnce(&Self, R) -> Result<(), StartupError>,
    {
        let result = node(self, resources);
        if result.is_err() {
            self.context.poison();
        }
        result
    }

    /// Seal the node graph and return the context for the network task.
    pub fn finish(self) -> Result<Context, StartupError> {
        self.context.finish_registration()?;
        Ok(self.context)
    }

    #[doc(hidden)]
    pub fn __create_node(&self, name: &'static str) -> Result<NodeBuilder, StartupError> {
        self.context.create_node(name)
    }

    #[doc(hidden)]
    pub fn __spawn<S>(
        &self,
        name: &'static str,
        token: Result<SpawnToken<S>, SpawnError>,
    ) -> Result<(), StartupError> {
        match token {
            Ok(token) => {
                self.spawner.spawn(token);
                Ok(())
            }
            Err(_) => {
                self.context.poison();
                Err(StartupError {
                    node: name,
                    topic: None,
                    phase: StartupPhase::Spawn,
                    error: Error::CapacityExceeded,
                })
            }
        }
    }
}
