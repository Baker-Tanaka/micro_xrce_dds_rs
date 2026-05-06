//! Reconnect & resilience tests (v0.5).
//!
//! Verifies the disconnect-signal + clear-disconnect mechanism that lets a
//! supervisor task race against an Executor failure.  The full
//! `Runtime::resume` round-trip needs a transport, so it is exercised on
//! target rather than here — what we test here is the SessionInner state
//! machine that resume reuses.

use embassy_futures::block_on;
use micro_xrce_dds_rs::Runtime;

#[test]
fn is_disconnected_starts_false() {
    static RT: Runtime = Runtime::new();
    let ctx = RT.context();
    assert!(!ctx.is_disconnected());
}

#[test]
fn wait_for_disconnect_is_immediate_when_already_disconnected() {
    static RT: Runtime = Runtime::new();
    RT.force_disconnect();
    // wait_for_disconnect should resolve immediately — no transport needed.
    block_on(RT.wait_for_disconnect());
    assert!(RT.context().is_disconnected());
}

#[test]
fn clear_disconnected_resets_flag() {
    static RT: Runtime = Runtime::new();
    RT.force_disconnect();
    assert!(RT.context().is_disconnected());
    RT.clear_disconnect();
    assert!(!RT.context().is_disconnected());
}

#[test]
fn disconnect_signal_only_fires_once_per_cycle() {
    // After clear_disconnect the supervisor should be able to await again
    // for a fresh disconnect.
    static RT: Runtime = Runtime::new();
    RT.force_disconnect();
    block_on(RT.wait_for_disconnect()); // first cycle resolves.
    RT.clear_disconnect();
    // After clear the runtime is no longer disconnected; next set fires again.
    RT.force_disconnect();
    block_on(RT.wait_for_disconnect());
    assert!(RT.context().is_disconnected());
}
