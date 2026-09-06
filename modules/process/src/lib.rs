//! `lumio-server` — MS-00002 Hello World dedicated server process.
//!
//! Composition root for this milestone: dynamic-port loopback WebSocket
//! listener, two-session admission, SDK DLL verification, `CoreCLR` runtime
//! bridge, authoritative tick routing and NDJSON audit. Wire truth is the
//! architecture repo's `engine/wire/hello-wire-v1.json`, loaded at startup
//! via `--wire-contract`; process behaviour (readiness, shutdown, exit codes,
//! audit vocabulary) follows its `process` block.
//!
//! Exit codes: 0 normal shutdown, 1 initialization failure, 2 fatal runtime
//! error, 3 argument error.

pub mod audit;
#[cfg(any(test, feature = "test-harness"))]
pub mod cli;
pub mod entity_chat;
pub mod persistence;
pub mod runtime_bridge;
pub mod sdk_loader;
#[cfg(any(test, feature = "test-harness"))]
pub mod server;
#[cfg(any(test, feature = "test-harness"))]
pub mod session;
pub mod wire;
#[cfg(any(test, feature = "test-harness"))]
pub mod world;

#[cfg(any(test, feature = "test-harness"))]
mod legacy;
#[cfg(any(test, feature = "test-harness"))]
pub use legacy::run;
