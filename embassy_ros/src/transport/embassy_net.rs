//! TCP adapter for `embassy-net` (eProsima Agent `tcp4` transport).

use embassy_net::{tcp::TcpSocket, IpEndpoint, Stack};
use embassy_time::{with_timeout, Duration};

pub use micro_xrce_dds_rs::{client_key, RuntimeConfig};

use super::validate;
pub use super::{ConnectFailure, ConnectFailureKind, ConnectionTask, Driver};
use crate::Context;

/// TCP receive buffer size.
pub const TCP_RX_BUFFER_SIZE: usize = 2048;
/// TCP transmit buffer size. Frames are queued without waiting for ACKs, so
/// this must cover the data rate times the round-trip time of a Wi-Fi link
/// that may batch traffic for ~100 ms (power save).
pub const TCP_TX_BUFFER_SIZE: usize = 8192;

/// Static TCP socket buffers: `static B: StaticCell<TcpBuffers>`.
pub struct TcpBuffers {
    rx: [u8; TCP_RX_BUFFER_SIZE],
    tx: [u8; TCP_TX_BUFFER_SIZE],
}

impl TcpBuffers {
    pub const fn new() -> Self {
        Self {
            rx: [0; TCP_RX_BUFFER_SIZE],
            tx: [0; TCP_TX_BUFFER_SIZE],
        }
    }

    /// Create the (reusable) Agent socket.
    pub fn socket(&'static mut self, stack: Stack<'static>) -> TcpSocket<'static> {
        TcpSocket::new(stack, &mut self.rx, &mut self.tx)
    }
}

impl Default for TcpBuffers {
    fn default() -> Self {
        Self::new()
    }
}

/// Agent endpoint and session parameters.
#[derive(Clone, Copy)]
pub struct AgentConfig {
    pub endpoint: IpEndpoint,
    pub runtime: RuntimeConfig,
    /// TCP connect timeout and socket inactivity timeout.
    pub tcp_timeout: Duration,
    /// TCP keep-alive interval.
    pub keep_alive: Option<Duration>,
}

impl AgentConfig {
    pub fn new(endpoint: IpEndpoint, runtime: RuntimeConfig) -> Self {
        Self {
            endpoint,
            runtime,
            tcp_timeout: Duration::from_secs(10),
            keep_alive: Some(Duration::from_secs(3)),
        }
    }
}

/// Connects the context to a micro-ROS Agent over TCP.
pub struct Agent {
    context: Context,
}

impl Agent {
    pub fn new(context: Context) -> Self {
        Self { context }
    }

    /// TCP connect + XRCE handshake + entity creation. The socket must be
    /// closed (fresh or `abort()`ed). On failure the socket is returned
    /// aborted. Cancelling this future drops (and thereby loses) the socket.
    pub async fn connect<'a>(
        &self,
        mut socket: TcpSocket<'a>,
        config: AgentConfig,
    ) -> Result<(ConnectionTask, Driver<TcpSocket<'a>>), ConnectFailure<TcpSocket<'a>>> {
        if let Err(kind) = validate(self.context, &config.runtime) {
            return Err(ConnectFailure { kind, socket });
        }
        socket.set_timeout(Some(config.tcp_timeout));
        socket.set_keep_alive(config.keep_alive);
        let kind = match with_timeout(config.tcp_timeout, socket.connect(config.endpoint)).await {
            Ok(Ok(())) => None,
            Ok(Err(e)) => Some(ConnectFailureKind::Tcp(e)),
            Err(_) => Some(ConnectFailureKind::TcpTimeout),
        };
        if let Some(kind) = kind {
            socket.abort();
            return Err(ConnectFailure { kind, socket });
        }
        super::connect(self.context, socket, config.runtime).await
    }
}
