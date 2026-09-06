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
pub use wire::{CloseReasonCode, RoomListener, TransportConfig, MAX_WIRE_TEXT_BYTES};

use serde::{Deserialize, Serialize};

pub const MAIN_ROOM: &str = "room-main";
pub const ISO_ROOM: &str = "room-iso";
pub const BROWSER_NAME: &str = "Browser01";
pub const TEST_PASSWORD: &str = "123456";
pub const ADMISSION_KEY_ID: u8 = 1;
pub const RECONNECT_WINDOW_MS: u64 = 300_000;
/// Reconnect window the replay harness runs with.
///
/// The product window is five minutes ([`RECONNECT_WINDOW_MS`]); the harness
/// cannot wait that long and — per ADR-057 §6 — must not fast-forward a
/// production clock to get there. It therefore builds its host with a short
/// window and waits real monotonic time for the NativeCore wall-clock timer.
/// **Harness only** — never pass this to a deployed host; the product window is
/// [`RECONNECT_WINDOW_MS`]. Expiry timers are armed by `disconnect` and by
/// `fail_connection` (delivery / observer-flush failures), so a short window
/// makes those fire within a run; the suite's only `disconnect` is c-bot99,
/// which never reconnects.
pub const SUITE_RECONNECT_WINDOW_MS: u64 = 2_000;
pub const INGRESS_QUEUE_PER_CONNECTION: usize = 64;
/// Runtime `ChatIngressWorld` default `MaxChangeEntries` is 128; each chat.input
/// commits two ChatComponent fields, so one `RunTick` can take at most 64 chats.
pub const MAX_CHAT_INPUTS_PER_TICK: usize = 64;
pub const BOT_COUNT: u32 = 100;

const fn default_tick_hz() -> u32 {
    100
}
const fn default_reconnect_window_ms() -> u64 {
    300_000
}
const fn default_max_pending_egress_connections() -> usize {
    1024
}
const fn default_max_pending_admissions() -> usize {
    1024
}
const fn default_max_pending_egress_per_connection() -> usize {
    8
}
const fn default_max_deferred_frame_connections() -> usize {
    1024
}
const fn default_max_deferred_frames_per_connection() -> usize {
    64
}
const fn default_max_deferred_frame_bytes_per_connection() -> usize {
    1024 * 1024
}
const fn default_max_pending_queries() -> usize {
    1024
}
const fn default_max_pending_wire_inputs() -> usize {
    1024
}
const fn default_ingress_queue_per_connection() -> usize {
    64
}
const fn default_max_chat_inputs_per_tick() -> usize {
    64
}

/// Host capacity and execution limits. All sizing and limits are configurable per ds-server redline.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HostLimitsConfig {
    #[serde(default = "default_tick_hz")]
    pub tick_hz: u32,
    #[serde(default = "default_reconnect_window_ms")]
    pub reconnect_window_ms: u64,
    #[serde(default = "default_max_pending_egress_connections")]
    pub max_pending_egress_connections: usize,
    #[serde(default = "default_max_pending_admissions")]
    pub max_pending_admissions: usize,
    #[serde(default = "default_max_pending_egress_per_connection")]
    pub max_pending_egress_per_connection: usize,
    #[serde(default = "default_max_deferred_frame_connections")]
    pub max_deferred_frame_connections: usize,
    #[serde(default = "default_max_deferred_frames_per_connection")]
    pub max_deferred_frames_per_connection: usize,
    #[serde(default = "default_max_deferred_frame_bytes_per_connection")]
    pub max_deferred_frame_bytes_per_connection: usize,
    #[serde(default = "default_max_pending_queries")]
    pub max_pending_queries: usize,
    #[serde(default = "default_max_pending_wire_inputs")]
    pub max_pending_wire_inputs: usize,
    #[serde(default = "default_ingress_queue_per_connection")]
    pub ingress_queue_per_connection: usize,
    #[serde(default = "default_max_chat_inputs_per_tick")]
    pub max_chat_inputs_per_tick: usize,
}

impl Default for HostLimitsConfig {
    fn default() -> Self {
        Self {
            tick_hz: default_tick_hz(),
            reconnect_window_ms: default_reconnect_window_ms(),
            max_pending_egress_connections: default_max_pending_egress_connections(),
            max_pending_admissions: default_max_pending_admissions(),
            max_pending_egress_per_connection: default_max_pending_egress_per_connection(),
            max_deferred_frame_connections: default_max_deferred_frame_connections(),
            max_deferred_frames_per_connection: default_max_deferred_frames_per_connection(),
            max_deferred_frame_bytes_per_connection:
                default_max_deferred_frame_bytes_per_connection(),
            max_pending_queries: default_max_pending_queries(),
            max_pending_wire_inputs: default_max_pending_wire_inputs(),
            ingress_queue_per_connection: default_ingress_queue_per_connection(),
            max_chat_inputs_per_tick: default_max_chat_inputs_per_tick(),
        }
    }
}

impl HostLimitsConfig {
    /// Validates all range constraints.
    ///
    /// # Errors
    /// Returns an error string if any field is zero or out of reasonable bounds.
    pub fn validate(&self) -> Result<(), String> {
        if self.tick_hz == 0 || self.tick_hz > 1000 {
            return Err("host tick_hz must be between 1 and 1000".into());
        }
        if self.reconnect_window_ms < 100 || self.reconnect_window_ms > 3_600_000 {
            return Err("host reconnect_window_ms must be between 100 and 3600000".into());
        }
        if self.max_pending_egress_connections == 0 || self.max_pending_egress_connections > 65_536
        {
            return Err("host max_pending_egress_connections must be between 1 and 65536".into());
        }
        if self.max_pending_admissions == 0 || self.max_pending_admissions > 65_536 {
            return Err("host max_pending_admissions must be between 1 and 65536".into());
        }
        if self.max_pending_egress_per_connection == 0
            || self.max_pending_egress_per_connection > 1024
        {
            return Err("host max_pending_egress_per_connection must be between 1 and 1024".into());
        }
        if self.max_deferred_frame_connections == 0 || self.max_deferred_frame_connections > 65_536
        {
            return Err("host max_deferred_frame_connections must be between 1 and 65536".into());
        }
        if self.max_deferred_frames_per_connection == 0
            || self.max_deferred_frames_per_connection > 4096
        {
            return Err(
                "host max_deferred_frames_per_connection must be between 1 and 4096".into(),
            );
        }
        if self.max_deferred_frame_bytes_per_connection < 1024
            || self.max_deferred_frame_bytes_per_connection > 64 * 1024 * 1024
        {
            return Err(
                "host max_deferred_frame_bytes_per_connection must be between 1024 and 67108864"
                    .into(),
            );
        }
        if self.max_pending_queries == 0 || self.max_pending_queries > 65_536 {
            return Err("host max_pending_queries must be between 1 and 65536".into());
        }
        if self.max_pending_wire_inputs == 0 || self.max_pending_wire_inputs > 65_536 {
            return Err("host max_pending_wire_inputs must be between 1 and 65536".into());
        }
        if self.ingress_queue_per_connection == 0 || self.ingress_queue_per_connection > 4096 {
            return Err("host ingress_queue_per_connection must be between 1 and 4096".into());
        }
        if self.max_chat_inputs_per_tick == 0 || self.max_chat_inputs_per_tick > 4096 {
            return Err("host max_chat_inputs_per_tick must be between 1 and 4096".into());
        }
        Ok(())
    }
}

/// Formats `Bot01`…`Bot100`.
#[must_use]
pub fn bot_name(index: u32) -> String {
    format!("Bot{index:02}")
}
