//! Runtime consume port. Binding, query, snapshot and persist live in Runtime.

use super::admission::classify_entity_kind;

/// Player or Bot, classified from login name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoundEntityKind {
    Player,
    Bot,
}

impl BoundEntityKind {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Player => "player",
            Self::Bot => "bot",
        }
    }
}

/// Rebind mode matching Runtime `RebindMode`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RebindMode {
    Reconnect,
    Takeover,
}

/// Frozen binding five-tuple from Runtime. Session id is not a binding field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeBinding {
    pub account_id: String,
    pub room_id: String,
    pub net_entity_id: String,
    pub entity_type: BoundEntityKind,
    pub connection_generation: u64,
}

/// Admit / rebind outcome forwarded from Runtime.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeAdmit {
    pub accepted: bool,
    pub code: Option<String>,
    pub binding: Option<RuntimeBinding>,
    pub frames: Vec<RuntimeFrame>,
}

impl RuntimeAdmit {
    #[must_use]
    pub fn ok(binding: RuntimeBinding) -> Self {
        Self {
            accepted: true,
            code: None,
            binding: Some(binding),
            frames: Vec::new(),
        }
    }

    #[must_use]
    pub fn reject(code: &str) -> Self {
        Self::reject_with_frames(code, Vec::new())
    }

    #[must_use]
    pub fn reject_with_frames(code: &str, frames: Vec<RuntimeFrame>) -> Self {
        Self {
            accepted: false,
            code: Some(code.to_owned()),
            binding: None,
            frames,
        }
    }
}

/// A Runtime-owned opaque wire frame with addressed metadata preserved.
/// The host routes bytes but never decodes the C-1 payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeFrame {
    pub connection: Option<String>,
    pub bytes: Vec<u8>,
    pub observer_net_entity_id: Option<String>,
    pub connection_generation: Option<u64>,
    pub message_type: Option<String>,
    pub code: Option<String>,
}

/// Runtime result for a disconnect, including lifecycle frames emitted by the world manager.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeDisconnect {
    pub binding: RuntimeBinding,
    pub frames: Vec<RuntimeFrame>,
}

/// One owner-thread control result plus every raw C-1 frame emitted by its tick.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeControlResult<T> {
    pub value: T,
    pub frames: Vec<RuntimeFrame>,
}

impl<T> RuntimeControlResult<T> {
    #[must_use]
    pub const fn new(value: T, frames: Vec<RuntimeFrame>) -> Self {
        Self { value, frames }
    }
}

/// An explicit Runtime control failure plus any C-1 frames emitted by its tick.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeControlError {
    pub message: String,
    pub frames: Vec<RuntimeFrame>,
    /// Correlation id when the Runtime accepted an asynchronous C-2 request.
    pub request_id: Option<String>,
}

/// Runtime C-2 query result drained by the owner tick. The server keeps this
/// record opaque and correlates it by `request_id`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeQueryRecord {
    pub request_id: String,
    pub result_type: String,
    pub outcome: String,
    pub binding: Option<RuntimeBinding>,
    pub value: Option<String>,
    pub net_entity_id: Option<String>,
    pub room_id: Option<String>,
    pub attribute_id: Option<String>,
    pub code: Option<String>,
    pub detail: Option<String>,
    pub observed_revision: Option<u64>,
    pub observed_tick: Option<u64>,
}

impl RuntimeControlError {
    #[must_use]
    pub const fn new(message: String, frames: Vec<RuntimeFrame>) -> Self {
        Self {
            message,
            frames,
            request_id: None,
        }
    }

    #[must_use]
    pub fn pending(request_id: &str) -> Self {
        Self {
            message: "runtime_query_pending".to_owned(),
            frames: Vec::new(),
            request_id: Some(request_id.to_owned()),
        }
    }
}

impl From<String> for RuntimeControlError {
    fn from(message: String) -> Self {
        Self::new(message, Vec::new())
    }
}

/// Attribute query forwarded to Runtime.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeQuery {
    pub caller_scope: AttributeQueryScope,
    pub room_id: String,
    pub net_entity_id: String,
    pub attribute_id: String,
    pub connection_generation: Option<u64>,
}

/// Query caller.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AttributeQueryScope {
    ServerAuthoritative,
    ClientReplica,
}

impl AttributeQueryScope {
    #[must_use]
    pub fn as_runtime_str(self) -> &'static str {
        match self {
            Self::ServerAuthoritative => "server-authoritative",
            Self::ClientReplica => "client-replica",
        }
    }
}

/// Five-outcome plus request-error query result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttributeQueryOutcome {
    Ok,
    RequestError,
    NonExistent,
    StaleGeneration,
    Invisible,
    Unauthorized,
    Tombstoned,
}

/// Attribute query result. Failures never alias another entity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryResult {
    pub outcome: AttributeQueryOutcome,
    pub value: Option<String>,
    pub error_code: Option<String>,
    pub observed_tick: u64,
    pub observed_revision: u64,
    /// Stable request id while the owner tick has not emitted a result.
    pub request_id: Option<String>,
}

impl QueryResult {
    #[must_use]
    pub fn ok(value: String, tick: u64, revision: u64) -> Self {
        Self {
            outcome: AttributeQueryOutcome::Ok,
            value: Some(value),
            error_code: None,
            observed_tick: tick,
            observed_revision: revision,
            request_id: None,
        }
    }

    #[must_use]
    pub fn fail(outcome: AttributeQueryOutcome) -> Self {
        Self {
            outcome,
            value: None,
            error_code: None,
            observed_tick: 0,
            observed_revision: 0,
            request_id: None,
        }
    }

    #[must_use]
    pub fn request_error(code: &str) -> Self {
        Self {
            outcome: AttributeQueryOutcome::RequestError,
            value: None,
            error_code: Some(code.to_owned()),
            observed_tick: 0,
            observed_revision: 0,
            request_id: None,
        }
    }

    #[must_use]
    pub fn pending(request_id: &str) -> Self {
        let mut result = Self::request_error("runtime_query_pending");
        result.request_id = Some(request_id.to_owned());
        result
    }

    #[must_use]
    pub fn from_runtime(outcome: &str, code: Option<&str>, value: Option<String>) -> Self {
        match outcome {
            "ok" => Self::ok(value.unwrap_or_default(), 0, 0),
            "non_existent" => Self::fail(AttributeQueryOutcome::NonExistent),
            "stale_generation" => Self::fail(AttributeQueryOutcome::StaleGeneration),
            "invisible" => Self::fail(AttributeQueryOutcome::Invisible),
            "unauthorized" => Self::fail(AttributeQueryOutcome::Unauthorized),
            "tombstoned" => Self::fail(AttributeQueryOutcome::Tombstoned),
            "request_error" => Self::request_error(code.unwrap_or("invalid_request")),
            other => Self::request_error(other),
        }
    }
}

/// Tick result used only to know which tick/revision to request on the wire.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RuntimeTick {
    pub applied_tick: u64,
    pub revision: u64,
    pub ok: bool,
    pub event_count: u64,
    pub code: Option<String>,
    pub frames: Vec<RuntimeFrame>,
}

impl RuntimeTick {
    #[must_use]
    pub fn failed(code: &str) -> Self {
        Self {
            applied_tick: 0,
            revision: 0,
            ok: false,
            event_count: 0,
            code: Some(code.to_owned()),
            frames: Vec::new(),
        }
    }

    #[must_use]
    pub fn committed(applied_tick: u64, revision: u64, event_count: u64) -> Self {
        Self {
            applied_tick,
            revision,
            ok: true,
            event_count,
            code: None,
            frames: Vec::new(),
        }
    }
}

/// Chat admit/apply outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChatOperation {
    pub kind: ChatOpKind,
    pub error_code: Option<String>,
}

/// Chat operation kind matching ChatOperationKind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChatOpKind {
    Admitted,
    Committed,
    Rejected,
    Fatal,
}

impl ChatOperation {
    #[must_use]
    pub fn admitted() -> Self {
        Self {
            kind: ChatOpKind::Admitted,
            error_code: None,
        }
    }

    #[must_use]
    pub fn rejected(code: &str) -> Self {
        Self {
            kind: ChatOpKind::Rejected,
            error_code: Some(code.to_owned()),
        }
    }
}

/// Opaque persist record from Runtime `CapturePersist`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistRecord {
    pub bytes: Vec<u8>,
}

/// Runtime public surface consumed by the host. The host does not implement it.
pub trait RuntimeSurface: Send {
    fn admit(
        &mut self,
        connection: &str,
        account_id: &str,
        room_id: &str,
        entity_type: BoundEntityKind,
    ) -> RuntimeAdmit;

    fn disconnect(
        &mut self,
        connection: &str,
        binding: &RuntimeBinding,
    ) -> Result<RuntimeDisconnect, String>;

    fn rebind(
        &mut self,
        connection: &str,
        account_id: &str,
        room_id: &str,
        mode: RebindMode,
        entity_type: BoundEntityKind,
    ) -> RuntimeAdmit;

    fn expire(
        &mut self,
        net_entity_id: &str,
    ) -> Result<RuntimeControlResult<()>, RuntimeControlError>;

    /// Enqueues an expiry with a caller-owned stable request id. Implementors
    /// that provide asynchronous C-2 may override this; synchronous doubles
    /// retain the original API through the default.
    fn expire_with_request_id(
        &mut self,
        _request_id: &str,
        net_entity_id: &str,
    ) -> Result<RuntimeControlResult<()>, RuntimeControlError> {
        self.expire(net_entity_id)
    }

    fn resolve_by_net_entity_id(
        &mut self,
        room_id: &str,
        net_entity_id: &str,
    ) -> Result<RuntimeControlResult<Option<RuntimeBinding>>, RuntimeControlError>;

    /// Enqueues a resolve with a caller-owned stable request id.
    fn resolve_by_net_entity_id_with_request_id(
        &mut self,
        _request_id: &str,
        room_id: &str,
        net_entity_id: &str,
    ) -> Result<RuntimeControlResult<Option<RuntimeBinding>>, RuntimeControlError> {
        self.resolve_by_net_entity_id(room_id, net_entity_id)
    }

    fn query_attribute(
        &mut self,
        request: &RuntimeQuery,
    ) -> Result<RuntimeControlResult<QueryResult>, RuntimeControlError>;

    /// Enqueues an attribute query with a caller-owned stable request id.
    fn query_attribute_with_request_id(
        &mut self,
        _request_id: &str,
        request: &RuntimeQuery,
    ) -> Result<RuntimeControlResult<QueryResult>, RuntimeControlError> {
        self.query_attribute(request)
    }

    fn attach_member(&mut self, room_id: &str, connection: &str) -> Result<(), String>;

    fn admit_input_command(
        &mut self,
        room_id: &str,
        connection: &str,
        generation: u64,
        net_entity_id: &str,
        envelope_bytes: &[u8],
    ) -> ChatOperation;

    fn run_tick(&mut self, room_id: &str, tick_id: u64) -> RuntimeTick;

    /// Drains C-2 query records emitted by the most recent owner tick.
    fn drain_queries(&mut self) -> Vec<RuntimeQueryRecord> {
        Vec::new()
    }

    fn persist(&mut self, room_id: &str) -> Result<PersistRecord, String>;

    fn restore(&mut self, room_id: &str, bytes: &[u8]) -> Result<(), String>;
}

/// Maps login classification onto Runtime `entityType`.
#[must_use]
pub fn entity_type_of(login_name: &str, bot_tool_context: bool) -> BoundEntityKind {
    classify_entity_kind(login_name, bot_tool_context)
}
