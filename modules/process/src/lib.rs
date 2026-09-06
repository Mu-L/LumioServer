//! Lumio dedicated-server infrastructure.
//!
//! `lumio-ds` is the authenticated deployment entry. The historical Hello
//! composition root is compiled only for tests or the explicit test-harness
//! feature. Runtime owns all authoritative entity/gameplay semantics.

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
