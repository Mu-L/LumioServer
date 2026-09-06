//! Slice-scoped entity-chat Rust host: clock, owner thread, bounded queues,
//! Account Server admission verify, Room world-slot, `CoreCLR` Runtime consume.
#![allow(
    clippy::chunks_exact_to_as_chunks,
    clippy::doc_markdown,
    clippy::double_must_use,
    clippy::manual_let_else,
    clippy::map_unwrap_or,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::must_use_candidate,
    clippy::needless_as_bytes,
    clippy::needless_pass_by_value,
    clippy::similar_names,
    clippy::single_match,
    clippy::struct_field_names,
    clippy::too_many_lines,
    clippy::while_let_loop
)]

#[cfg(any(test, feature = "test-harness"))]
mod account;
mod admission;
#[cfg(any(test, feature = "test-harness"))]
mod bots;
#[cfg(any(test, feature = "test-harness"))]
mod browser;
mod clr;
mod crypto;
#[cfg(any(test, feature = "test-harness"))]
mod discover;
mod host;
mod runtime;
mod secure;
#[cfg(any(test, feature = "test-harness"))]
pub use secure::issue_bound_test_credential;
pub use secure::{AllocationContext, BoundAdmissionVerifier};
#[cfg(any(test, feature = "test-harness"))]
mod suite;
mod wire;

#[cfg(any(test, feature = "test-harness"))]
pub use account::{AccountLoginResult, AccountServerProcess};
pub use admission::{
    generate_keys, issue_admission_credential, issue_bot_tool_credential, verify_admission,
    AdmissionPayload, Ed25519KeyPair,
};
#[cfg(any(test, feature = "test-harness"))]
pub use bots::{
    discover_bot_host, run_client_bot_fleet, start_client_bot_fleet, wait_for_client_bot_fleet,
    ClientBotFleet, ClientBotTrace, ClientInputEvidence,
};
pub use clr::{ClrGameplay, ClrGameplayConfig};
#[cfg(any(test, feature = "test-harness"))]
pub use discover::{discover, ReplayArtifacts};
pub use host::{
    AttributeQueryRequest, ConnectionBinding, EntityChatHost, EntityResolution, HostHealth,
    RoomAdmitResult, WireInputObserver, DISPATCH_EXPIRE, DISPATCH_TICK,
    MAX_DEFERRED_FRAMES_PER_CONNECTION, MAX_DEFERRED_FRAME_BYTES_PER_CONNECTION,
    MAX_DEFERRED_FRAME_CONNECTIONS, MAX_PENDING_ADMISSIONS, MAX_PENDING_EGRESS_CONNECTIONS,
    MAX_PENDING_EGRESS_PER_CONNECTION, MAX_PENDING_QUERIES, MAX_PENDING_WIRE_INPUTS,
    MAX_PENDING_WIRE_INPUT_BYTES, MAX_RUNTIME_QUERY_HISTORY,
};
pub use runtime::{
    AttributeQueryOutcome, AttributeQueryScope, BoundEntityKind, ChatOpKind, ChatOperation,
    PersistRecord, QueryResult, RebindMode, RuntimeAdmit, RuntimeBinding, RuntimeControlError,
    RuntimeControlResult, RuntimeDisconnect, RuntimeFrame, RuntimeQuery, RuntimeQueryRecord,
    RuntimeSurface, RuntimeTick,
};
#[cfg(any(test, feature = "test-harness"))]
pub use suite::{
    apply_pending_chat_ticks, drain_chat_event_deltas, run_round, run_round_blocking,
    run_two_rounds, SuiteOptions, SuiteReport,
};
#[cfg(any(test, feature = "test-harness"))]
pub use wire::RoomClient;
pub use wire::{RoomListener, MAX_WIRE_TEXT_BYTES};

pub const MAIN_ROOM: &str = "room-main";
pub const ISO_ROOM: &str = "room-iso";
pub const BROWSER_NAME: &str = "Browser01";
pub const TEST_PASSWORD: &str = "123456";
pub const ADMISSION_KEY_ID: u8 = 1;
pub const RECONNECT_WINDOW_MS: u64 = 300_000;
pub const INGRESS_QUEUE_PER_CONNECTION: usize = 64;
/// Runtime `ChatIngressWorld` default `MaxChangeEntries` is 128; each chat.input
/// commits two ChatComponent fields, so one `RunTick` can take at most 64 chats.
pub const MAX_CHAT_INPUTS_PER_TICK: usize = 64;
pub const BOT_COUNT: u32 = 100;

/// Formats `Bot01`…`Bot100`.
#[must_use]
pub fn bot_name(index: u32) -> String {
    format!("Bot{index:02}")
}
