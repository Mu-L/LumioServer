//! Consume-only Room host: session table + Runtime forward + NativeCore timers + wire.

use std::collections::{HashMap, HashSet};
use std::thread;
use std::thread::ThreadId;

use lumio_host_runtime::{
    bounded_channel, spawn_supervised, HostClock, KernelHandle, KernelTimer, Sender, SharedClock,
    SupervisedTask, TimerMode,
};

use super::admission::{is_bot_namespace, verify_admission, AdmissionPayload};
use super::runtime::BoundEntityKind;
use super::runtime::{
    AttributeQueryScope, ChatOpKind, ChatOperation, PersistRecord, QueryResult, RebindMode,
    RuntimeBinding, RuntimeFrame, RuntimeQuery, RuntimeQueryRecord, RuntimeSurface, RuntimeTick,
};
use super::wire::{RoomListener, WireEvent, WireSendError, WireSender, MAX_WIRE_TEXT_BYTES};
use super::MAX_CHAT_INPUTS_PER_TICK;

/// Maximum number of sockets waiting for admission before new sockets are closed.
pub const MAX_PENDING_EGRESS_CONNECTIONS: usize = 1_024;
/// Maximum observers retained for one connection while admission is pending/active.
pub const MAX_PENDING_EGRESS_PER_CONNECTION: usize = 8;
/// Maximum number of connection queues retaining frames without an attached socket.
pub const MAX_DEFERRED_FRAME_CONNECTIONS: usize = 1_024;
/// Maximum deferred Runtime frames retained for one connection.
pub const MAX_DEFERRED_FRAMES_PER_CONNECTION: usize = 64;
/// Maximum bytes retained by one deferred Runtime frame queue.
pub const MAX_DEFERRED_FRAME_BYTES_PER_CONNECTION: usize = 1_048_576;

/// Bounded sink for exact Runtime-admitted input bytes used by external evidence consumers.
pub type WireInputObserver = Sender<Vec<u8>>;

/// WallClock expire dispatch id (NativeCore slot).
pub const DISPATCH_EXPIRE: u32 = 1;
/// TickFrame room tick dispatch id (NativeCore slot).
pub const DISPATCH_TICK: u32 = 2;

fn session_id_for(login_name: &str, reconnected: bool) -> String {
    if reconnected {
        format!("sess-{login_name}-re")
    } else {
        format!("sess-{login_name}")
    }
}

/// Binding five-tuple plus the host session id (not a Runtime binding field).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectionBinding {
    pub account_id: String,
    pub room_id: String,
    pub net_entity_id: String,
    pub session_id: String,
    pub entity_type: BoundEntityKind,
    pub connection_generation: u64,
}

impl ConnectionBinding {
    fn from_runtime(binding: RuntimeBinding, session_id: String) -> Self {
        Self {
            account_id: binding.account_id,
            room_id: binding.room_id,
            net_entity_id: binding.net_entity_id,
            session_id,
            entity_type: binding.entity_type,
            connection_generation: binding.connection_generation,
        }
    }
}

/// Server resolution of a live entity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntityResolution {
    pub net_entity_id: String,
    pub room_id: String,
    pub entity_type: BoundEntityKind,
    pub account_id: String,
}

/// Room admission outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoomAdmitResult {
    pub accepted: bool,
    pub error_code: Option<String>,
    pub reconnected: bool,
    pub takeover: bool,
}

impl RoomAdmitResult {
    fn pending(reconnected: bool, takeover: bool) -> Self {
        Self {
            accepted: true,
            error_code: None,
            reconnected,
            takeover,
        }
    }

    fn ok(reconnected: bool, takeover: bool) -> Self {
        Self {
            accepted: true,
            error_code: None,
            reconnected,
            takeover,
        }
    }

    fn reject(code: &str) -> Self {
        Self {
            accepted: false,
            error_code: Some(code.to_owned()),
            reconnected: false,
            takeover: false,
        }
    }
}

/// Attribute query request forwarded to Runtime.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttributeQueryRequest {
    pub caller_scope: AttributeQueryScope,
    pub room_id: String,
    pub net_entity_id: String,
    pub attribute_id: String,
    pub connection_generation: Option<u64>,
}

struct Session {
    session_id: String,
    account_id: String,
    room_id: String,
    net_entity_id: String,
    entity_type: BoundEntityKind,
    generation: u64,
    egresses: Vec<WireSender>,
}

struct PendingAdmission {
    room_id: String,
    payload: AdmissionPayload,
    reconnected: bool,
    takeover: bool,
    staged_tick: u64,
}

struct ExpireTarget {
    room_id: String,
    net_entity_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum PendingQueryKey {
    Resolve {
        room_id: String,
        net_entity_id: String,
    },
    Attribute {
        caller_scope: AttributeQueryScope,
        room_id: String,
        net_entity_id: String,
        attribute_id: String,
        connection_generation: Option<u64>,
    },
}

struct Inner {
    clock: SharedClock,
    reconnect_window_ms: u64,
    admission_key_id: u8,
    admission_public: Vec<u8>,
    unix_seconds: u64,
    runtime: Box<dyn RuntimeSurface>,
    kernel: Box<dyn KernelTimer>,
    sessions: HashMap<String, Session>,
    pending_admissions: HashMap<String, PendingAdmission>,
    runtime_queries: Vec<RuntimeQueryRecord>,
    completed_queries: HashMap<String, RuntimeQueryRecord>,
    pending_queries: HashMap<PendingQueryKey, String>,
    pending_expiries: HashMap<String, ExpireTarget>,
    next_query_id: u64,
    query_failures: Vec<String>,
    expire_watch: HashMap<KernelHandle, ExpireTarget>,
    pending_egress: HashMap<String, Vec<WireSender>>,
    deferred_frames: HashMap<String, Vec<Vec<u8>>>,
    tick_id: u64,
    wire_chat_pending: u64,
    wire_input_observer: Option<WireInputObserver>,
}

enum OwnerWork {
    Run(Box<dyn FnOnce(&mut Inner) + Send>),
    Wire(WireEvent),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Delivery {
    Delivered,
    Backpressured,
    Unavailable,
    Invalid,
}

fn deliver_to_egresses(egresses: &mut Vec<WireSender>, bytes: &[u8]) -> Delivery {
    let mut delivered = false;
    let mut backpressured = false;
    let mut invalid = false;
    egresses.retain(|egress| match egress.try_send_bytes(bytes) {
        Ok(()) => {
            delivered = true;
            true
        }
        Err(WireSendError::Full) => {
            backpressured = true;
            true
        }
        Err(WireSendError::Closed) => false,
        Err(WireSendError::TooLarge | WireSendError::InvalidUtf8) => {
            invalid = true;
            false
        }
    });
    if delivered {
        Delivery::Delivered
    } else if invalid {
        Delivery::Invalid
    } else if backpressured {
        Delivery::Backpressured
    } else {
        Delivery::Unavailable
    }
}

/// Slice-scoped Room host. All authoritative work runs on one owner thread.
pub struct EntityChatHost {
    tx: Sender<OwnerWork>,
    _listener: RoomListener,
    _forward: SupervisedTask,
    _owner: SupervisedTask,
    owner_id: ThreadId,
    listen_uri: String,
    clock: SharedClock,
}

impl EntityChatHost {
    /// Builds a consume-only host. Kernel due-decision stays in NativeCore ABI.
    #[must_use]
    pub fn new(
        reconnect_window_ms: u64,
        clock: SharedClock,
        runtime: Box<dyn RuntimeSurface>,
        kernel: Box<dyn KernelTimer>,
        admission_key_id: u8,
        admission_public: Vec<u8>,
        unix_seconds: u64,
    ) -> Self {
        let (tx, rx) = bounded_channel(256);
        let (wire_tx, wire_rx) = bounded_channel(256);
        let (id_tx, id_rx) = bounded_channel(1);
        let listener = RoomListener::bind(wire_tx).expect("room wire bind");
        let listen_uri = listener.uri();
        let forward_tx = tx.clone();
        let forward = spawn_supervised("lumio-entity-chat-wire-fwd", move |_| {
            while let Ok(event) = wire_rx.recv() {
                if forward_tx.send(OwnerWork::Wire(event)).is_err() {
                    break;
                }
            }
        });
        let owner_clock = clock.clone();
        let owner = spawn_supervised("lumio-entity-chat-owner", move |_cancel| {
            let _ = id_tx.send(thread::current().id());
            let mut inner = Inner {
                clock: owner_clock,
                reconnect_window_ms,
                admission_key_id,
                admission_public,
                unix_seconds,
                runtime,
                kernel,
                sessions: HashMap::new(),
                pending_admissions: HashMap::new(),
                runtime_queries: Vec::new(),
                completed_queries: HashMap::new(),
                pending_queries: HashMap::new(),
                pending_expiries: HashMap::new(),
                next_query_id: 1,
                query_failures: Vec::new(),
                expire_watch: HashMap::new(),
                pending_egress: HashMap::new(),
                deferred_frames: HashMap::new(),
                tick_id: 0,
                wire_chat_pending: 0,
                wire_input_observer: None,
            };
            if inner
                .kernel
                .schedule_repeating(TimerMode::TickFrame, 1, 1, DISPATCH_TICK)
                .is_err()
            {
                return;
            }
            loop {
                match rx.recv() {
                    Ok(OwnerWork::Run(work)) => work(&mut inner),
                    Ok(OwnerWork::Wire(event)) => inner.on_wire(event),
                    Err(_) => break,
                }
            }
        });
        let owner_id = id_rx.recv().expect("owner thread id");
        Self {
            tx,
            _listener: listener,
            _forward: forward,
            _owner: owner,
            owner_id,
            listen_uri,
            clock,
        }
    }

    /// Attaches a bounded observer for exact admitted input bytes.
    pub fn attach_wire_input_observer(&self, observer: WireInputObserver) {
        self.on_owner(move |inner| inner.wire_input_observer = Some(observer));
    }

    fn on_owner<T, F>(&self, work: F) -> T
    where
        T: Send + 'static,
        F: FnOnce(&mut Inner) -> T + Send + 'static,
    {
        let (tx, rx) = bounded_channel(1);
        self.tx
            .send(OwnerWork::Run(Box::new(move |inner| {
                let _ = tx.send(work(inner));
            })))
            .unwrap_or_else(|_| panic!("entity-chat owner thread closed"));
        rx.recv().expect("entity-chat owner result")
    }

    /// Loopback Room wire URI.
    #[must_use]
    pub fn listen_uri(&self) -> String {
        self.listen_uri.clone()
    }

    /// Game Server never accepts username/password in place of admission.
    #[must_use]
    pub fn try_admit_username_password(
        &self,
        _room_id: &str,
        _connection_id: &str,
        _login_name: &str,
        _password: &str,
    ) -> bool {
        false
    }

    /// Admits a connection by verifying the Account Server credential.
    #[must_use]
    pub fn admit(
        &self,
        room_id: String,
        connection_id: String,
        credential: String,
    ) -> RoomAdmitResult {
        self.on_owner(move |inner| inner.admit(&room_id, &connection_id, &credential))
    }

    /// Admits an already-verified payload (suite path after local verify).
    #[must_use]
    pub fn admit_verified(
        &self,
        room_id: String,
        connection_id: String,
        payload: AdmissionPayload,
    ) -> RoomAdmitResult {
        self.on_owner(move |inner| inner.admit_verified(&room_id, &connection_id, &payload))
    }

    /// Disconnects a live connection and schedules the NativeCore wallClock expire.
    #[must_use]
    pub fn disconnect(&self, connection_id: String) -> Result<bool, String> {
        self.on_owner(move |inner| inner.disconnect(&connection_id))
    }

    /// Pumps NativeCore wallClock at the host monotonic reading.
    pub fn drive_kernel(&self) -> bool {
        self.on_owner(Inner::drive_wall)
    }

    /// Test/suite clock handle. Expiry still fires only via kernel pump.
    #[must_use]
    pub fn clock(&self) -> SharedClock {
        self.clock.clone()
    }

    /// Forwards one complete Runtime-produced InputCommand frame without inspecting it.
    #[must_use]
    pub fn admit_input_command(
        &self,
        connection_id: String,
        envelope_bytes: Vec<u8>,
    ) -> ChatOperation {
        self.on_owner(move |inner| inner.admit_input_command(&connection_id, &envelope_bytes))
    }

    /// Advances kernel tickFrame and routes Runtime-owned outbox frames.
    #[must_use]
    pub fn run_tick(&self, room_id: String) -> RuntimeTick {
        self.on_owner(move |inner| inner.run_tick(&room_id))
    }

    /// Tick cadence is kernel tickFrame, not a caller for-loop.
    #[must_use]
    pub fn schedule_room_tick(&self, room_id: String, _delay_ms: u64) -> RuntimeTick {
        self.run_tick(room_id)
    }

    /// Client self-lookup via Runtime, joined with the host session id.
    #[must_use]
    pub fn try_self_lookup(&self, connection_id: String) -> Option<ConnectionBinding> {
        self.on_owner(move |inner| inner.try_self_lookup(&connection_id))
    }

    /// Binding or panic if missing.
    ///
    /// # Panics
    ///
    /// Panics when the connection is not bound.
    #[must_use]
    pub fn must_self(&self, connection_id: &str) -> ConnectionBinding {
        self.try_self_lookup(connection_id.to_owned())
            .unwrap_or_else(|| panic!("connection is not bound: {connection_id}"))
    }

    /// Resolve a NetEntityId in a room via Runtime.
    ///
    /// # Errors
    ///
    /// Returns malformed, missing, mismatched, and request-error Runtime results.
    #[must_use]
    pub fn try_resolve_by_net_entity_id(
        &self,
        room_id: String,
        net_entity_id: String,
    ) -> Result<Option<EntityResolution>, String> {
        self.on_owner(move |inner| inner.resolve_by_net_entity_id(&room_id, &net_entity_id))
    }

    /// Chat.input frames admitted from Room WS and not yet applied by a tick.
    #[must_use]
    pub fn pending_wire_chat_inputs(&self) -> usize {
        self.on_owner(move |inner| usize::try_from(inner.wire_chat_pending).unwrap_or(usize::MAX))
    }

    /// Returns C-2 query records drained by owner ticks since the last drain.
    #[must_use]
    pub fn drain_runtime_queries(&self) -> Vec<RuntimeQueryRecord> {
        self.on_owner(|inner| std::mem::take(&mut inner.runtime_queries))
    }

    /// Count live Room WS observers for a connection (harness wait).
    #[must_use]
    pub fn wire_observer_count(&self, connection_id: String) -> usize {
        self.on_owner(move |inner| {
            inner
                .sessions
                .get(&connection_id)
                .map(|session| session.egresses.len())
                .unwrap_or(0)
                + inner
                    .pending_egress
                    .get(&connection_id)
                    .map(Vec::len)
                    .unwrap_or(0)
        })
    }

    /// C-2 attribute query forwarded to Runtime.
    #[must_use]
    pub fn query_attribute(&self, request: AttributeQueryRequest) -> QueryResult {
        self.on_owner(move |inner| inner.query_attribute(&request))
    }

    /// Runtime persist bytes. Restore must not create Active bindings.
    #[must_use]
    pub fn capture_persist_snapshot(&self, room_id: String) -> Result<PersistRecord, String> {
        self.on_owner(move |inner| inner.runtime.persist(&room_id))
    }

    /// Restores persist-only fields. Does not Admit or create sessions.
    pub fn restore_persist_snapshot(
        &self,
        room_id: String,
        snapshot: PersistRecord,
    ) -> Result<(), String> {
        self.on_owner(move |inner| inner.runtime.restore(&room_id, &snapshot.bytes))
    }

    /// Owner thread id (tests).
    #[must_use]
    pub fn owner_thread_id(&self) -> ThreadId {
        self.owner_id
    }
}

impl Inner {
    fn next_query_id(&mut self, kind: &str) -> String {
        let id = format!("server-a2-{kind}-{}", self.next_query_id);
        self.next_query_id = self.next_query_id.saturating_add(1);
        id
    }

    fn absorb_runtime_queries(&mut self) {
        let mut seen = HashSet::new();
        let mut unknown_request = false;
        for record in self.runtime.drain_queries() {
            let known = self
                .pending_queries
                .values()
                .any(|request_id| request_id == &record.request_id)
                || self.pending_expiries.contains_key(&record.request_id);
            if !known {
                unknown_request = true;
                self.query_failures
                    .push("runtime query result requestId mismatch".to_owned());
            }
            if !seen.insert(record.request_id.clone()) {
                self.query_failures
                    .push("runtime query result has duplicate requestId".to_owned());
                self.fail_pending_request(&record.request_id);
                continue;
            }
            if record.result_type == "ExpireEntityResult"
                && self.pending_expiries.remove(&record.request_id).is_some()
                && record.outcome == "request_error"
            {
                self.query_failures.push(
                    record
                        .code
                        .clone()
                        .unwrap_or_else(|| "runtime_failure".to_owned()),
                );
            }
            self.completed_queries
                .insert(record.request_id.clone(), record.clone());
            self.runtime_queries.push(record);
        }
        if unknown_request {
            self.pending_queries.clear();
            self.pending_expiries.clear();
            self.completed_queries.clear();
        }
    }

    fn pending_record(&self, key: &PendingQueryKey) -> Option<RuntimeQueryRecord> {
        self.pending_queries
            .get(key)
            .and_then(|id| self.completed_queries.get(id))
            .cloned()
    }

    fn fail_pending_request(&mut self, request_id: &str) {
        self.pending_queries
            .retain(|_, pending_id| pending_id != request_id);
        self.pending_expiries.remove(request_id);
        self.completed_queries.remove(request_id);
    }

    fn clear_query(&mut self, key: &PendingQueryKey) {
        if let Some(request_id) = self.pending_queries.remove(key) {
            self.completed_queries.remove(&request_id);
        }
    }

    fn query_error_message(record: &RuntimeQueryRecord) -> String {
        match (&record.code, &record.detail) {
            (Some(code), Some(detail)) => format!("{code}: {detail}"),
            (Some(code), None) => code.clone(),
            _ => "runtime_failure".to_owned(),
        }
    }

    fn resolve_completed(
        &mut self,
        key: &PendingQueryKey,
        room_id: &str,
        net_entity_id: &str,
        record: RuntimeQueryRecord,
    ) -> Result<Option<EntityResolution>, String> {
        self.clear_query(key);
        if record.result_type != "ResolveBindingResult" {
            return Err("runtime query result type mismatch".to_owned());
        }
        match record.outcome.as_str() {
            "ok" => {
                let Some(binding) = record.binding else {
                    return Err("runtime query result missing binding".to_owned());
                };
                if binding.room_id != room_id || binding.net_entity_id != net_entity_id {
                    return Err("runtime query result request mismatch".to_owned());
                }
                Ok(Some(EntityResolution {
                    net_entity_id: binding.net_entity_id,
                    room_id: binding.room_id,
                    entity_type: binding.entity_type,
                    account_id: binding.account_id,
                }))
            }
            "request_error" => Err(Self::query_error_message(&record)),
            "non_existent" | "stale_generation" | "invisible" | "unauthorized" | "tombstoned" => {
                Ok(None)
            }
            _ => Err("runtime query result outcome invalid".to_owned()),
        }
    }

    fn attribute_completed(
        &mut self,
        key: &PendingQueryKey,
        request: &AttributeQueryRequest,
        record: RuntimeQueryRecord,
    ) -> QueryResult {
        self.clear_query(key);
        if record.result_type != "AttributeQueryResult" {
            return QueryResult::request_error("runtime query result type mismatch");
        }
        if record.outcome == "ok" {
            if record.net_entity_id.as_deref() != Some(request.net_entity_id.as_str())
                || record.room_id.as_deref() != Some(request.room_id.as_str())
                || record.attribute_id.as_deref() != Some(request.attribute_id.as_str())
                || record.value.is_none()
                || record.observed_tick.is_none()
                || record.observed_revision.is_none()
            {
                return QueryResult::request_error("runtime query result request mismatch");
            }
            return QueryResult::ok(
                record.value.unwrap_or_default(),
                record.observed_tick.unwrap_or_default(),
                record.observed_revision.unwrap_or_default(),
            );
        }
        QueryResult::from_runtime(&record.outcome, record.code.as_deref(), record.value)
    }

    fn session_binding(session: &Session) -> RuntimeBinding {
        RuntimeBinding {
            account_id: session.account_id.clone(),
            room_id: session.room_id.clone(),
            net_entity_id: session.net_entity_id.clone(),
            entity_type: session.entity_type,
            connection_generation: session.generation,
        }
    }

    fn admit(&mut self, room_id: &str, connection_id: &str, credential: &str) -> RoomAdmitResult {
        match verify_admission(
            credential,
            self.admission_key_id,
            &self.admission_public,
            self.unix_seconds,
        ) {
            Ok(payload) => self.admit_verified(room_id, connection_id, &payload),
            Err(code) => RoomAdmitResult::reject(&code),
        }
    }

    fn admit_verified(
        &mut self,
        room_id: &str,
        connection_id: &str,
        payload: &AdmissionPayload,
    ) -> RoomAdmitResult {
        if room_id.is_empty()
            || connection_id.is_empty()
            || payload.account_id.is_empty()
            || payload.login_name.is_empty()
        {
            return RoomAdmitResult::reject("invalid_request");
        }
        if is_bot_namespace(&payload.login_name) && !payload.bot_tool_context {
            return RoomAdmitResult::reject("bot_namespace_admission_forbidden");
        }
        if self.sessions.contains_key(connection_id) {
            return RoomAdmitResult::reject("invalid_request");
        }
        let kind = super::runtime::entity_type_of(&payload.login_name, payload.bot_tool_context);
        let admitted = self
            .runtime
            .admit(connection_id, &payload.account_id, room_id, kind);
        if admitted.accepted {
            return self.stage_admission(
                room_id,
                connection_id,
                payload,
                admitted.frames,
                false,
                false,
            );
        }
        if !self.route_frames(room_id, &admitted.frames) {
            return RoomAdmitResult::reject("runtime_failure");
        }
        if admitted.code.as_deref() == Some("cross_room_reference") {
            return RoomAdmitResult::reject("invalid_request");
        }
        if admitted.code.as_deref() == Some("account_already_online") {
            return self.takeover(room_id, connection_id, payload, kind);
        }
        let rebound = self.runtime.rebind(
            connection_id,
            &payload.account_id,
            room_id,
            RebindMode::Reconnect,
            kind,
        );
        if rebound.accepted {
            return self.stage_admission(
                room_id,
                connection_id,
                payload,
                rebound.frames,
                true,
                false,
            );
        }
        if !self.route_frames(room_id, &rebound.frames) {
            return RoomAdmitResult::reject("runtime_failure");
        }
        RoomAdmitResult::reject(rebound.code.as_deref().unwrap_or("invalid_request"))
    }

    fn takeover(
        &mut self,
        room_id: &str,
        connection_id: &str,
        payload: &AdmissionPayload,
        kind: BoundEntityKind,
    ) -> RoomAdmitResult {
        let rebound = self.runtime.rebind(
            connection_id,
            &payload.account_id,
            room_id,
            RebindMode::Takeover,
            kind,
        );
        if !rebound.accepted {
            if !self.route_frames(room_id, &rebound.frames) {
                return RoomAdmitResult::reject("runtime_failure");
            }
            return RoomAdmitResult::reject(rebound.code.as_deref().unwrap_or("invalid_request"));
        }
        self.stage_admission(room_id, connection_id, payload, rebound.frames, false, true)
    }

    fn stage_admission(
        &mut self,
        room_id: &str,
        connection_id: &str,
        payload: &AdmissionPayload,
        frames: Vec<RuntimeFrame>,
        reconnected: bool,
        takeover: bool,
    ) -> RoomAdmitResult {
        let pending = PendingAdmission {
            room_id: room_id.to_owned(),
            payload: payload.clone(),
            reconnected,
            takeover,
            staged_tick: self.tick_id,
        };
        self.pending_admissions
            .insert(connection_id.to_owned(), pending);
        let completed = self
            .complete_pending_admission(connection_id, &frames)
            .is_some();
        // Route all Runtime frames even while admission is pending. In
        // particular, takeover's addressed ConnectionSuperseded must not be
        // lost merely because Welcome is emitted on a later owner tick.
        let _ = self.route_frames(room_id, &frames);
        if completed {
            self.retire_superseded(connection_id, &frames);
            return RoomAdmitResult::ok(reconnected, takeover);
        }
        RoomAdmitResult::pending(reconnected, takeover)
    }

    fn complete_pending_admission(
        &mut self,
        connection_id: &str,
        frames: &[RuntimeFrame],
    ) -> Option<ConnectionBinding> {
        let pending = self.pending_admissions.get(connection_id)?;
        let welcome = frames.iter().find(|frame| {
            frame.connection.as_deref() == Some(connection_id)
                && frame.message_type.as_deref() == Some("Welcome")
                && frame.observer_net_entity_id.is_some()
                && frame.connection_generation.is_some()
        })?;
        let room_id = pending.room_id.clone();
        let payload = pending.payload.clone();
        let reconnected = pending.reconnected;
        let takeover = pending.takeover;
        let runtime_binding = RuntimeBinding {
            account_id: payload.account_id.clone(),
            room_id: room_id.clone(),
            net_entity_id: welcome.observer_net_entity_id.clone().unwrap_or_default(),
            entity_type: super::runtime::entity_type_of(
                &payload.login_name,
                payload.bot_tool_context,
            ),
            connection_generation: welcome.connection_generation.unwrap_or_default(),
        };
        if self.runtime.attach_member(&room_id, connection_id).is_err() {
            self.pending_admissions.remove(connection_id);
            return None;
        }
        let session_id = session_id_for(&payload.login_name, reconnected || takeover);
        let egresses = self
            .pending_egress
            .remove(connection_id)
            .unwrap_or_default();
        let session = Session {
            session_id: session_id.clone(),
            account_id: payload.account_id.clone(),
            room_id: room_id.clone(),
            net_entity_id: runtime_binding.net_entity_id.clone(),
            entity_type: runtime_binding.entity_type,
            generation: runtime_binding.connection_generation,
            egresses,
        };
        self.sessions.insert(connection_id.to_owned(), session);
        self.pending_admissions.remove(connection_id);
        Some(ConnectionBinding::from_runtime(runtime_binding, session_id))
    }

    fn retire_superseded(&mut self, connection_id: &str, frames: &[RuntimeFrame]) {
        let superseded_connections: Vec<String> = frames
            .iter()
            .filter(|frame| frame.message_type.as_deref() == Some("ConnectionSuperseded"))
            .filter_map(|frame| frame.connection.clone())
            .filter(|connection| connection != connection_id)
            .collect();
        for old_id in superseded_connections {
            if let Some(old) = self.sessions.remove(&old_id) {
                for egress in &old.egresses {
                    let _ = egress.try_close();
                }
            }
            if let Some(egresses) = self.pending_egress.remove(&old_id) {
                for egress in egresses {
                    let _ = egress.try_close();
                }
            }
            self.deferred_frames.remove(&old_id);
        }
    }

    fn route_pending_frames(&mut self, frames: &[RuntimeFrame]) {
        let pending_ids: Vec<String> = self.pending_admissions.keys().cloned().collect();
        for connection_id in pending_ids {
            let relevant: Vec<RuntimeFrame> = frames
                .iter()
                .filter(|frame| frame.connection.as_deref() == Some(connection_id.as_str()))
                .cloned()
                .collect();
            if relevant.is_empty() {
                continue;
            }
            if relevant
                .iter()
                .any(|frame| frame.message_type.as_deref() == Some("Error"))
            {
                continue;
            }
            if self
                .complete_pending_admission(&connection_id, &relevant)
                .is_some()
            {
                self.retire_superseded(&connection_id, &relevant);
            }
        }
    }

    fn retire_failed_admissions(&mut self, frames: &[RuntimeFrame]) {
        let failed: Vec<String> = self
            .pending_admissions
            .keys()
            .filter(|connection_id| {
                frames.iter().any(|frame| {
                    frame.connection.as_deref() == Some(connection_id.as_str())
                        && frame.message_type.as_deref() == Some("Error")
                })
            })
            .cloned()
            .collect();
        for connection_id in failed {
            self.pending_admissions.remove(&connection_id);
            self.deferred_frames.remove(&connection_id);
            self.pending_egress
                .remove(&connection_id)
                .into_iter()
                .flatten()
                .for_each(|egress| egress.abort());
        }
    }

    fn retire_unresolved_admissions(&mut self, room_id: &str) {
        let unresolved: Vec<String> = self
            .pending_admissions
            .iter()
            .filter(|(_, pending)| pending.room_id == room_id && pending.staged_tick < self.tick_id)
            .map(|(connection_id, _)| connection_id.clone())
            .collect();
        for connection_id in unresolved {
            self.pending_admissions.remove(&connection_id);
            self.deferred_frames.remove(&connection_id);
            self.pending_egress
                .remove(&connection_id)
                .into_iter()
                .flatten()
                .for_each(|egress| egress.abort());
        }
    }

    fn disconnect(&mut self, connection_id: &str) -> Result<bool, String> {
        let Some(session) = self.sessions.get(connection_id) else {
            return Ok(false);
        };
        let room_id = session.room_id.clone();
        let runtime_binding = Self::session_binding(session);
        let runtime_result = self.runtime.disconnect(connection_id, &runtime_binding)?;
        let Some(session) = self.sessions.remove(connection_id) else {
            return Ok(false);
        };
        self.deferred_frames.remove(connection_id);
        for egress in &session.egresses {
            let _ = egress.try_close();
        }
        let routed = self.route_frames(&room_id, &runtime_result.frames);
        self.schedule_expire(&session.room_id, &session.net_entity_id);
        if routed {
            Ok(true)
        } else {
            Err("runtime_failure".to_owned())
        }
    }

    fn schedule_expire(&mut self, room_id: &str, net_entity_id: &str) {
        let due = self.clock.now_ms().saturating_add(self.reconnect_window_ms);
        if let Ok(handle) =
            self.kernel
                .schedule_one_shot(TimerMode::WallClock, due, DISPATCH_EXPIRE)
        {
            self.expire_watch.insert(
                handle,
                ExpireTarget {
                    room_id: room_id.to_owned(),
                    net_entity_id: net_entity_id.to_owned(),
                },
            );
        }
    }

    fn drive_wall(&mut self) -> bool {
        let now = self.clock.now_ms();
        let Ok(fired) = self.kernel.pump_wall_clock(now) else {
            return false;
        };
        let mut succeeded = true;
        for event in fired {
            if event.dispatch_id != DISPATCH_EXPIRE {
                continue;
            }
            if let Some(target) = self.expire_watch.remove(&event.handle) {
                let request_id = self.next_query_id("expire");
                match self
                    .runtime
                    .expire_with_request_id(&request_id, &target.net_entity_id)
                {
                    Ok(result) if self.route_frames(&target.room_id, &result.frames) => {}
                    Ok(_) => succeeded = false,
                    Err(error) => {
                        let _ = self.route_frames(&target.room_id, &error.frames);
                        if error.request_id.as_deref() == Some(request_id.as_str())
                            && error.message == "runtime_query_pending"
                        {
                            self.pending_expiries.insert(request_id, target);
                        } else {
                            succeeded = false;
                        }
                    }
                }
            }
        }
        succeeded
    }

    fn admit_input_command(&mut self, connection_id: &str, envelope_bytes: &[u8]) -> ChatOperation {
        if envelope_bytes.len() > MAX_WIRE_TEXT_BYTES {
            return ChatOperation::rejected("bad_envelope");
        }
        let Some(session) = self.sessions.get(connection_id) else {
            return ChatOperation::rejected("disconnected");
        };
        let room_id = session.room_id.clone();
        let generation = session.generation;
        let net_entity_id = session.net_entity_id.clone();
        self.runtime.admit_input_command(
            &room_id,
            connection_id,
            generation,
            &net_entity_id,
            envelope_bytes,
        )
    }

    fn run_tick(&mut self, room_id: &str) -> RuntimeTick {
        if self.wire_chat_pending > MAX_CHAT_INPUTS_PER_TICK as u64 {
            self.wire_chat_pending = 0;
            return RuntimeTick::failed("runtime_failure");
        }
        self.tick_id = self.tick_id.saturating_add(1);
        let Ok(fired) = self.kernel.advance_tick_frame(self.tick_id) else {
            self.wire_chat_pending = 0;
            return RuntimeTick::failed("runtime_failure");
        };
        if !fired.iter().any(|row| row.dispatch_id == DISPATCH_TICK) {
            self.wire_chat_pending = 0;
            return RuntimeTick::failed("runtime_failure");
        }
        let tick = self.runtime.run_tick(room_id, self.tick_id);
        self.absorb_runtime_queries();
        let routed = self.route_frames(room_id, &tick.frames);
        self.route_pending_frames(&tick.frames);
        self.retire_failed_admissions(&tick.frames);
        self.retire_unresolved_admissions(room_id);
        self.wire_chat_pending = 0;
        if !tick.ok {
            self.query_failures.clear();
            return tick;
        }
        if !self.query_failures.is_empty() {
            let mut failed = tick;
            failed.ok = false;
            failed.code = self.query_failures.first().cloned();
            self.query_failures.clear();
            return failed;
        }
        if !routed {
            return RuntimeTick::failed("runtime_failure");
        }
        tick
    }

    fn route_frames(&mut self, room_id: &str, frames: &[RuntimeFrame]) -> bool {
        let mut routed = true;
        for frame in frames {
            self.apply_runtime_identity(frame);
            if let Some(connection) = frame.connection.as_deref() {
                routed &= self.route_frame_to_connection(connection, &frame.bytes);
                continue;
            }
            let targets: Vec<String> = self
                .sessions
                .iter()
                .filter(|(_, session)| session.room_id == room_id)
                .map(|(connection, _)| connection.clone())
                .collect();
            for connection in targets {
                routed &= self.route_frame_to_connection(&connection, &frame.bytes);
            }
        }
        routed
    }

    fn route_frame_to_connection(&mut self, connection: &str, bytes: &[u8]) -> bool {
        match self.deliver_to_connection(connection, bytes) {
            Delivery::Delivered => true,
            Delivery::Invalid => {
                let _ = self.fail_connection(connection);
                false
            }
            Delivery::Backpressured | Delivery::Unavailable => {
                if self.defer_frame(connection, bytes) {
                    true
                } else {
                    let _ = self.fail_connection(connection);
                    false
                }
            }
        }
    }

    fn apply_runtime_identity(&mut self, frame: &RuntimeFrame) {
        let Some(connection) = frame.connection.as_deref() else {
            return;
        };
        if frame.message_type.as_deref() != Some("Welcome") {
            return;
        }
        let Some(net_entity_id) = frame.observer_net_entity_id.as_ref() else {
            return;
        };
        if let Some(session) = self.sessions.get_mut(connection) {
            session.net_entity_id.clone_from(net_entity_id);
            if let Some(generation) = frame.connection_generation {
                session.generation = generation;
            }
        }
    }

    fn deliver_to_connection(&mut self, connection: &str, bytes: &[u8]) -> Delivery {
        if bytes.len() > MAX_WIRE_TEXT_BYTES || std::str::from_utf8(bytes).is_err() {
            return Delivery::Invalid;
        }
        if let Some(egresses) = self.pending_egress.get_mut(connection) {
            let result = deliver_to_egresses(egresses, bytes);
            if result != Delivery::Unavailable {
                return result;
            }
        }
        let Some(session) = self.sessions.get_mut(connection) else {
            return Delivery::Unavailable;
        };
        deliver_to_egresses(&mut session.egresses, bytes)
    }

    fn flush_deferred(&mut self, connection: &str) {
        let Some(frames) = self.deferred_frames.remove(connection) else {
            return;
        };
        for bytes in frames {
            if !self.route_frame_to_connection(connection, &bytes) {
                break;
            }
        }
    }

    fn defer_frame(&mut self, connection: &str, bytes: &[u8]) -> bool {
        if bytes.len() > MAX_DEFERRED_FRAME_BYTES_PER_CONNECTION {
            return false;
        }
        if let Some(frames) = self.deferred_frames.get_mut(connection) {
            let queued_bytes: usize = frames.iter().map(Vec::len).sum();
            if frames.len() >= MAX_DEFERRED_FRAMES_PER_CONNECTION
                || queued_bytes.saturating_add(bytes.len())
                    > MAX_DEFERRED_FRAME_BYTES_PER_CONNECTION
            {
                return false;
            }
            frames.push(bytes.to_vec());
            return true;
        }
        if self.deferred_frames.len() >= MAX_DEFERRED_FRAME_CONNECTIONS {
            return false;
        }
        self.deferred_frames
            .insert(connection.to_owned(), vec![bytes.to_vec()]);
        true
    }

    fn fail_connection(&mut self, connection: &str) -> Result<(), String> {
        let Some(session) = self.sessions.get(connection) else {
            self.pending_egress
                .remove(connection)
                .into_iter()
                .flatten()
                .for_each(|egress| {
                    egress.abort();
                });
            self.deferred_frames.remove(connection);
            return Ok(());
        };
        let runtime_binding = Self::session_binding(session);
        let runtime_result = self.runtime.disconnect(connection, &runtime_binding)?;
        let Some(session) = self.sessions.remove(connection) else {
            return Ok(());
        };
        self.deferred_frames.remove(connection);
        for egress in &session.egresses {
            egress.abort();
        }
        if let Some(egresses) = self.pending_egress.remove(connection) {
            for egress in egresses {
                egress.abort();
            }
        }

        for frame in runtime_result.frames {
            if frame.connection.as_deref() == Some(connection) {
                continue;
            }
            let _ = self.route_frames(&session.room_id, &[frame]);
        }
        self.schedule_expire(&session.room_id, &session.net_entity_id);
        Ok(())
    }

    fn on_wire(&mut self, event: WireEvent) {
        match event {
            WireEvent::Attached {
                connection_id,
                egress,
            } => {
                if self.sessions.contains_key(&connection_id) {
                    if let Some(session) = self.sessions.get_mut(&connection_id) {
                        if session.egresses.len() >= MAX_PENDING_EGRESS_PER_CONNECTION {
                            egress.abort();
                            let _ = self.fail_connection(&connection_id);
                            return;
                        }
                        session.egresses.push(egress.clone());
                    }
                    self.flush_deferred(&connection_id);
                } else {
                    if self.pending_egress.len() >= MAX_PENDING_EGRESS_CONNECTIONS {
                        egress.abort();
                        return;
                    }
                    let mut accepted = false;
                    {
                        let queue = self
                            .pending_egress
                            .entry(connection_id.clone())
                            .or_default();
                        if queue.len() >= MAX_PENDING_EGRESS_PER_CONNECTION {
                            egress.abort();
                        } else {
                            queue.push(egress);
                            accepted = true;
                        }
                    }
                    if accepted && self.deferred_frames.contains_key(&connection_id) {
                        self.flush_deferred(&connection_id);
                    }
                }
            }
            WireEvent::Input {
                connection_id,
                text,
            } => {
                if text.as_bytes().len() > MAX_WIRE_TEXT_BYTES {
                    let _ = self.fail_connection(&connection_id);
                    return;
                }
                let room_id = self
                    .sessions
                    .get(&connection_id)
                    .map(|session| session.room_id.clone());
                let envelope_bytes = text.into_bytes();
                let admitted = self.admit_input_command(&connection_id, &envelope_bytes);
                if admitted.kind == ChatOpKind::Admitted {
                    if let Some(observer) = &self.wire_input_observer {
                        let _ = observer.try_send(envelope_bytes);
                    }
                    self.wire_chat_pending = self.wire_chat_pending.saturating_add(1);
                    if self.wire_chat_pending >= MAX_CHAT_INPUTS_PER_TICK as u64 {
                        if let Some(room_id) = room_id {
                            let _ = self.run_tick(&room_id);
                        }
                    }
                }
            }
            WireEvent::Closed { connection_id } => {
                if !self.disconnect(&connection_id).unwrap_or(false) {
                    self.pending_admissions.remove(&connection_id);
                    self.deferred_frames.remove(&connection_id);
                    self.pending_egress
                        .remove(&connection_id)
                        .into_iter()
                        .flatten()
                        .for_each(|egress| egress.abort());
                }
            }
        }
    }

    fn try_self_lookup(&mut self, connection_id: &str) -> Option<ConnectionBinding> {
        let session = self.sessions.get(connection_id)?;
        Some(ConnectionBinding {
            account_id: session.account_id.clone(),
            room_id: session.room_id.clone(),
            net_entity_id: session.net_entity_id.clone(),
            session_id: session.session_id.clone(),
            entity_type: session.entity_type,
            connection_generation: session.generation,
        })
    }

    fn resolve_by_net_entity_id(
        &mut self,
        room_id: &str,
        net_entity_id: &str,
    ) -> Result<Option<EntityResolution>, String> {
        let key = PendingQueryKey::Resolve {
            room_id: room_id.to_owned(),
            net_entity_id: net_entity_id.to_owned(),
        };
        if let Some(record) = self.pending_record(&key) {
            return self.resolve_completed(&key, room_id, net_entity_id, record);
        }
        if self.pending_queries.contains_key(&key) {
            return Err("runtime_query_pending".to_owned());
        }
        let request_id = self.next_query_id("resolve");
        self.pending_queries.insert(key.clone(), request_id.clone());
        let result = match self.runtime.resolve_by_net_entity_id_with_request_id(
            &request_id,
            room_id,
            net_entity_id,
        ) {
            Ok(result) => result,
            Err(error) => {
                if !self.route_frames(room_id, &error.frames) {
                    self.pending_queries.remove(&key);
                    return Err("runtime_failure".to_owned());
                }
                if error.request_id.as_deref() == Some(request_id.as_str())
                    && error.message == "runtime_query_pending"
                {
                    return Err("runtime_query_pending".to_owned());
                }
                self.pending_queries.remove(&key);
                return Err(error.message);
            }
        };
        self.pending_queries.remove(&key);
        if !self.route_frames(room_id, &result.frames) {
            return Err("runtime_failure".to_owned());
        }
        Ok(result.value.map(|runtime| EntityResolution {
            net_entity_id: runtime.net_entity_id,
            room_id: runtime.room_id,
            entity_type: runtime.entity_type,
            account_id: runtime.account_id,
        }))
    }

    fn query_attribute(&mut self, request: &AttributeQueryRequest) -> QueryResult {
        let key = PendingQueryKey::Attribute {
            caller_scope: request.caller_scope,
            room_id: request.room_id.clone(),
            net_entity_id: request.net_entity_id.clone(),
            attribute_id: request.attribute_id.clone(),
            connection_generation: request.connection_generation,
        };
        if let Some(record) = self.pending_record(&key) {
            return self.attribute_completed(&key, request, record);
        }
        if self.pending_queries.contains_key(&key) {
            let request_id = self.pending_queries.get(&key).expect("pending query");
            return QueryResult::pending(request_id);
        }
        let request_id = self.next_query_id("attribute");
        self.pending_queries.insert(key.clone(), request_id.clone());
        let result = self.runtime.query_attribute_with_request_id(
            &request_id,
            &RuntimeQuery {
                caller_scope: request.caller_scope,
                room_id: request.room_id.clone(),
                net_entity_id: request.net_entity_id.clone(),
                attribute_id: request.attribute_id.clone(),
                connection_generation: request.connection_generation,
            },
        );
        match result {
            Ok(result) if self.route_frames(&request.room_id, &result.frames) => {
                self.pending_queries.remove(&key);
                result.value
            }
            Ok(_) => {
                self.pending_queries.remove(&key);
                QueryResult::request_error("runtime_failure")
            }
            Err(error) => {
                let routed = self.route_frames(&request.room_id, &error.frames);
                if error.request_id.as_deref() == Some(request_id.as_str())
                    && error.message == "runtime_query_pending"
                {
                    return QueryResult::pending(&request_id);
                }
                self.pending_queries.remove(&key);
                QueryResult::request_error(if routed {
                    &error.message
                } else {
                    "runtime_failure"
                })
            }
        }
    }
}
