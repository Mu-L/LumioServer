//! Consume-only Room host: session table + Runtime forward + NativeCore timers + wire.

use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::time::Duration;

use lumio_host_runtime::{
    bounded_channel, spawn_supervised, HostClock, KernelHandle, KernelTimer, Sender, SharedClock,
    SupervisedTask, TimerMode,
};

use super::admission::{is_bot_namespace, verify_admission, AdmissionPayload};
use super::runtime::BoundEntityKind;
use super::runtime::{
    AttributeQueryScope, ChatOperation, PersistRecord, QueryResult, RebindMode, RuntimeBinding,
    RuntimeFrame, RuntimeQuery, RuntimeQueryRecord, RuntimeSurface, RuntimeTick,
};
use super::secure::BoundAdmissionVerifier;
use super::wire::{RoomListener, WireEvent, WireSendError, WireSender, MAX_WIRE_TEXT_BYTES};
use super::{INGRESS_QUEUE_PER_CONNECTION, MAX_CHAT_INPUTS_PER_TICK};

/// Maximum number of sockets waiting for admission before new sockets are closed.
pub const MAX_PENDING_EGRESS_CONNECTIONS: usize = 1_024;
/// Maximum connections whose Runtime admission intent may await an owner tick.
pub const MAX_PENDING_ADMISSIONS: usize = MAX_PENDING_EGRESS_CONNECTIONS;
/// Maximum observers retained for one connection while admission is pending/active.
pub const MAX_PENDING_EGRESS_PER_CONNECTION: usize = 8;
/// Maximum number of connection queues retaining frames without an attached socket.
pub const MAX_DEFERRED_FRAME_CONNECTIONS: usize = 1_024;
/// Maximum deferred Runtime frames retained for one connection.
pub const MAX_DEFERRED_FRAMES_PER_CONNECTION: usize = 64;
/// Maximum bytes retained by one deferred Runtime frame queue.
pub const MAX_DEFERRED_FRAME_BYTES_PER_CONNECTION: usize = 1_048_576;
/// Maximum transient Runtime request correlations retained by the host.
///
/// A completed record is attached to its still-pending key and therefore does
/// not consume a second correlation slot. Failed records consume a slot until
/// the caller retries the exact request and consumes the terminal result.
pub const MAX_PENDING_QUERIES: usize = 1_024;
/// Completed Runtime query records are diagnostic output, not an unbounded log.
pub const MAX_RUNTIME_QUERY_HISTORY: usize = MAX_PENDING_QUERIES;
/// Aggregate pending wire inputs across all connections.
pub const MAX_PENDING_WIRE_INPUTS: usize = MAX_PENDING_QUERIES;
/// Aggregate pending wire bytes across all connections.
pub const MAX_PENDING_WIRE_INPUT_BYTES: usize = MAX_PENDING_WIRE_INPUTS * MAX_WIRE_TEXT_BYTES;
const OWNER_CADENCE_MS: u64 = 10;

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
    egresses: Vec<ObserverEgress>,
}

struct ObserverEgress {
    sender: WireSender,
    pending: VecDeque<Vec<u8>>,
    pending_bytes: usize,
    close_requested: bool,
    close_enqueued: bool,
}

impl ObserverEgress {
    fn new(sender: WireSender) -> Self {
        Self {
            sender,
            pending: VecDeque::new(),
            pending_bytes: 0,
            close_requested: false,
            close_enqueued: false,
        }
    }

    fn request_close(&mut self) {
        self.close_requested = true;
    }
}

struct PendingWireInput {
    room_id: String,
    connection_id: String,
    envelope_bytes: Vec<u8>,
}

struct PendingAdmission {
    room_id: String,
    payload: AdmissionPayload,
    reconnected: bool,
    takeover: bool,
    staged_tick: u64,
}

#[derive(Clone)]
struct ExpireTarget {
    room_id: String,
    net_entity_id: String,
    due_ms: u64,
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
    admission_clock_origin_ms: u64,
    admission_verifier: Option<BoundAdmissionVerifier>,
    // Activated Runtime rooms live until this host is destroyed, not until the
    // last socket disappears. Bound this registry at the admission boundary.
    active_rooms: BTreeSet<String>,
    runtime: Box<dyn RuntimeSurface>,
    kernel: Box<dyn KernelTimer>,
    sessions: HashMap<String, Session>,
    pending_admissions: HashMap<String, PendingAdmission>,
    runtime_queries: VecDeque<RuntimeQueryRecord>,
    completed_queries: HashMap<String, RuntimeQueryRecord>,
    failed_queries: HashMap<PendingQueryKey, String>,
    pending_queries: HashMap<PendingQueryKey, String>,
    pending_expiries: HashMap<String, ExpireTarget>,
    next_query_id: u64,
    query_failures: Vec<String>,
    expire_watch: HashMap<KernelHandle, ExpireTarget>,
    retry_expiries: HashMap<String, ExpireTarget>,
    reconnect_targets: HashMap<String, ExpireTarget>,
    pending_egress: HashMap<String, Vec<ObserverEgress>>,
    deferred_frames: HashMap<String, Vec<Vec<u8>>>,
    retired_connections: HashSet<String>,
    tick_id: u64,
    wire_chat_pending: u64,
    pending_wire_inputs: Vec<PendingWireInput>,
    pending_wire_input_bytes: usize,
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
    Overflow,
}

fn deliver_to_egresses(egresses: &mut Vec<ObserverEgress>, bytes: &[u8]) -> Delivery {
    let mut delivered = false;
    let mut backpressured = false;
    let mut invalid = false;
    let mut overflow = false;
    egresses.retain_mut(|egress| {
        match flush_observer_egress(egress) {
            Delivery::Unavailable => return false,
            Delivery::Invalid => {
                invalid = true;
                return false;
            }
            Delivery::Backpressured => {
                if egress.pending.len() >= MAX_DEFERRED_FRAMES_PER_CONNECTION
                    || egress.pending_bytes.saturating_add(bytes.len())
                        > MAX_DEFERRED_FRAME_BYTES_PER_CONNECTION
                {
                    overflow = true;
                } else {
                    egress.pending.push_back(bytes.to_vec());
                    egress.pending_bytes = egress.pending_bytes.saturating_add(bytes.len());
                    backpressured = true;
                }
                return true;
            }
            Delivery::Delivered | Delivery::Overflow => {}
        }
        match egress.sender.try_send_bytes(bytes) {
            Ok(()) => {
                delivered = true;
                true
            }
            Err(WireSendError::Full) => {
                if egress.pending.len() >= MAX_DEFERRED_FRAMES_PER_CONNECTION
                    || egress.pending_bytes.saturating_add(bytes.len())
                        > MAX_DEFERRED_FRAME_BYTES_PER_CONNECTION
                {
                    overflow = true;
                } else {
                    egress.pending.push_back(bytes.to_vec());
                    egress.pending_bytes = egress.pending_bytes.saturating_add(bytes.len());
                    backpressured = true;
                }
                true
            }
            Err(WireSendError::Closed) => false,
            Err(WireSendError::TooLarge | WireSendError::InvalidUtf8) => {
                invalid = true;
                false
            }
        }
    });
    if overflow {
        Delivery::Overflow
    } else if invalid {
        Delivery::Invalid
    } else if backpressured {
        Delivery::Backpressured
    } else if delivered {
        Delivery::Delivered
    } else {
        Delivery::Unavailable
    }
}

fn flush_observer_egress(egress: &mut ObserverEgress) -> Delivery {
    if egress.sender.is_closed() {
        return Delivery::Unavailable;
    }
    loop {
        let Some(bytes) = egress.pending.front() else {
            if egress.close_requested && !egress.close_enqueued {
                match egress.sender.try_close_ordered() {
                    Ok(()) => egress.close_enqueued = true,
                    Err(WireSendError::Full) => return Delivery::Backpressured,
                    Err(WireSendError::Closed) => return Delivery::Unavailable,
                    Err(WireSendError::TooLarge | WireSendError::InvalidUtf8) => {
                        return Delivery::Invalid
                    }
                }
            }
            return Delivery::Delivered;
        };
        match egress.sender.try_send_bytes(bytes) {
            Ok(()) => {
                if let Some(bytes) = egress.pending.pop_front() {
                    egress.pending_bytes = egress.pending_bytes.saturating_sub(bytes.len());
                }
            }
            Err(WireSendError::Full) => return Delivery::Backpressured,
            Err(WireSendError::Closed) => return Delivery::Unavailable,
            Err(WireSendError::TooLarge | WireSendError::InvalidUtf8) => return Delivery::Invalid,
        }
    }
}

fn flush_observer_egresses(egresses: &mut Vec<ObserverEgress>) -> bool {
    let mut invalid = false;
    egresses.retain_mut(|egress| match flush_observer_egress(egress) {
        Delivery::Invalid => {
            invalid = true;
            false
        }
        Delivery::Unavailable => false,
        Delivery::Delivered | Delivery::Backpressured | Delivery::Overflow => true,
    });
    !invalid
}

/// Slice-scoped Room host. All authoritative work runs on one owner thread.
pub struct EntityChatHost {
    tx: Sender<OwnerWork>,
    _listener: RoomListener,
    _forward: SupervisedTask,
    _owner: SupervisedTask,
    listen_uri: String,
    clock: SharedClock,
}

impl EntityChatHost {
    /// Builds a consume-only host. Kernel due-decision stays in NativeCore ABI.
    #[must_use]
    #[cfg(any(test, feature = "test-harness"))]
    pub fn new(
        reconnect_window_ms: u64,
        clock: SharedClock,
        runtime: Box<dyn RuntimeSurface>,
        kernel: Box<dyn KernelTimer>,
        admission_key_id: u8,
        admission_public: Vec<u8>,
        unix_seconds: u64,
    ) -> Self {
        Self::build(
            (
                reconnect_window_ms,
                clock,
                admission_key_id,
                admission_public,
                unix_seconds,
            ),
            runtime,
            kernel,
            None,
        )
        .expect("test host startup")
    }

    /// Secure Host entry. Public clients cannot select an existing connection identity.
    pub fn new_authenticated(
        reconnect_window_ms: u64,
        runtime: Box<dyn RuntimeSurface>,
        kernel: Box<dyn KernelTimer>,
        verifier: BoundAdmissionVerifier,
    ) -> Result<Self, String> {
        Self::build(
            (
                reconnect_window_ms,
                verifier.clock.clone(),
                verifier.key_id,
                verifier.public_key.clone(),
                verifier.unix_origin,
            ),
            runtime,
            kernel,
            Some(verifier),
        )
    }

    fn build(
        config: (u64, SharedClock, u8, Vec<u8>, u64),
        runtime: Box<dyn RuntimeSurface>,
        kernel: Box<dyn KernelTimer>,
        admission_verifier: Option<BoundAdmissionVerifier>,
    ) -> Result<Self, String> {
        let (reconnect_window_ms, clock, admission_key_id, admission_public, unix_seconds) = config;

        let (tx, rx) = bounded_channel(256);
        let (wire_tx, wire_rx) = bounded_channel(256);
        let listener = match admission_verifier.clone() {
            Some(verifier) => RoomListener::bind_authenticated(wire_tx, verifier)?,
            None => {
                #[cfg(any(test, feature = "test-harness"))]
                {
                    RoomListener::bind(wire_tx)?
                }
                #[cfg(not(any(test, feature = "test-harness")))]
                {
                    return Err("test-harness transport is unavailable".to_owned());
                }
            }
        };
        let listen_uri = listener.uri();
        let forward_tx = tx.clone();
        let forward = spawn_supervised("lumio-entity-chat-wire-fwd", move |cancel| {
            while !cancel.is_cancelled() {
                match wire_rx.recv_timeout(Duration::from_millis(10)) {
                    Ok(event) => {
                        if forward_tx
                            .send_timeout(OwnerWork::Wire(event), Duration::from_secs(2))
                            .is_err()
                        {
                            if cancel.is_cancelled() {
                                break;
                            }
                            panic!("owner forwarding deadline exceeded or owner closed");
                        }
                    }
                    Err(lumio_host_runtime::RecvError::Empty) => {}
                    Err(lumio_host_runtime::RecvError::Closed) => break,
                }
            }
        });
        let owner_clock = clock.clone();
        let admission_clock_origin_ms = clock.now_ms();
        let owner = spawn_supervised("lumio-entity-chat-owner", move |cancel| {
            let mut inner = Inner {
                clock: owner_clock,
                reconnect_window_ms,
                admission_verifier,
                admission_key_id,
                admission_public,
                unix_seconds,
                admission_clock_origin_ms,
                active_rooms: BTreeSet::new(),
                runtime,
                kernel,
                sessions: HashMap::new(),
                pending_admissions: HashMap::new(),
                runtime_queries: VecDeque::new(),
                completed_queries: HashMap::new(),
                failed_queries: HashMap::new(),
                pending_queries: HashMap::new(),
                pending_expiries: HashMap::new(),
                next_query_id: 1,
                query_failures: Vec::new(),
                expire_watch: HashMap::new(),
                retry_expiries: HashMap::new(),
                reconnect_targets: HashMap::new(),
                pending_egress: HashMap::new(),
                deferred_frames: HashMap::new(),
                retired_connections: HashSet::new(),
                tick_id: 0,
                wire_chat_pending: 0,
                pending_wire_inputs: Vec::new(),
                pending_wire_input_bytes: 0,
                wire_input_observer: None,
            };
            if inner
                .kernel
                .schedule_repeating(TimerMode::TickFrame, 1, 1, DISPATCH_TICK)
                .is_err()
            {
                return;
            }
            let self_drive = !inner.clock.is_deterministic();
            let mut last_cadence_ms = inner.clock.now_ms();
            loop {
                if cancel.is_cancelled() {
                    break;
                }
                let work = rx.recv_timeout(Duration::from_millis(OWNER_CADENCE_MS));
                match work {
                    Ok(OwnerWork::Run(work)) => work(&mut inner),
                    Ok(OwnerWork::Wire(event)) => inner.on_wire(event),
                    Err(lumio_host_runtime::RecvError::Empty) if self_drive => {
                        inner.drive_owner_cadence();
                        last_cadence_ms = inner.clock.now_ms();
                    }
                    Err(lumio_host_runtime::RecvError::Empty) => {}
                    Err(lumio_host_runtime::RecvError::Closed) => break,
                }
                if self_drive
                    && inner.clock.now_ms().saturating_sub(last_cadence_ms) >= OWNER_CADENCE_MS
                {
                    inner.drive_owner_cadence();
                    last_cadence_ms = inner.clock.now_ms();
                }
            }
        });
        Ok(Self {
            tx,
            _listener: listener,
            _forward: forward,
            _owner: owner,
            listen_uri,
            clock,
        })
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
            .send_timeout(
                OwnerWork::Run(Box::new(move |inner| {
                    let _ = tx.try_send(work(inner));
                })),
                Duration::from_secs(2),
            )
            .unwrap_or_else(|_| panic!("entity-chat owner thread closed"));
        rx.recv_timeout(Duration::from_secs(2))
            .expect("entity-chat owner result deadline")
    }

    /// Nonblocking process-supervision probe. Does not enqueue work to a stuck owner.
    #[must_use]
    pub fn is_healthy(&self) -> bool {
        !self._owner.is_finished()
            && self._owner.failure().is_none()
            && !self._forward.is_finished()
            && self._forward.failure().is_none()
            && self._listener.is_healthy()
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
    #[cfg(any(test, feature = "test-harness"))]
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

    /// Crate-internal suite pacing state; not part of the public HostEntry API.
    #[must_use]
    pub(crate) fn pending_wire_chat_inputs(&self) -> usize {
        self.on_owner(move |inner| usize::try_from(inner.wire_chat_pending).unwrap_or(usize::MAX))
    }

    /// Returns C-2 query records drained by owner ticks since the last drain.
    #[must_use]
    pub fn drain_runtime_queries(&self) -> Vec<RuntimeQueryRecord> {
        self.on_owner(|inner| inner.runtime_queries.drain(..).collect())
    }

    /// Crate-internal suite synchronization; not part of the public HostEntry API.
    #[must_use]
    pub(crate) fn wire_observer_count(&self, connection_id: String) -> usize {
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
        self.on_owner(move |inner| {
            if !inner.active_rooms.contains(&room_id)
                && inner.active_rooms.len() >= MAX_PENDING_ADMISSIONS
            {
                return Err("admission_capacity".to_owned());
            }
            inner.runtime.restore(&room_id, &snapshot.bytes)?;
            inner.active_rooms.insert(room_id);
            Ok(())
        })
    }
}

impl Drop for EntityChatHost {
    fn drop(&mut self) {
        self._listener.shutdown();
        self._forward.cancel();
        self._owner.cancel();
    }
}

impl Inner {
    fn record_query_failure(&mut self, reason: impl Into<String>) {
        if self.query_failures.len() < MAX_PENDING_QUERIES {
            self.query_failures.push(reason.into());
        }
    }

    fn correlation_slots_used(&self) -> usize {
        self.pending_queries
            .len()
            .saturating_add(self.failed_queries.len())
            .saturating_add(self.pending_expiries.len())
    }

    fn correlation_capacity_available(&self) -> bool {
        self.correlation_slots_used() < MAX_PENDING_QUERIES
    }

    fn next_query_id(&mut self, kind: &str) -> String {
        let id = format!("server-a2-{kind}-{}", self.next_query_id);
        self.next_query_id = self.next_query_id.saturating_add(1);
        id
    }

    fn pending_request_ids_for_tick(&self) -> HashSet<String> {
        self.pending_queries
            .iter()
            .filter(|(_, request_id)| !self.completed_queries.contains_key(*request_id))
            .map(|(_, request_id)| request_id.clone())
            .chain(self.pending_expiries.keys().cloned())
            .collect()
    }

    fn pending_keys_for_request(&self, request_id: &str) -> Vec<PendingQueryKey> {
        self.pending_queries
            .iter()
            .filter(|(_, pending_id)| pending_id.as_str() == request_id)
            .map(|(key, _)| key.clone())
            .collect()
    }

    fn fail_pending_request(&mut self, request_id: &str, reason: &str) {
        let keys = self.pending_keys_for_request(request_id);
        for key in keys {
            self.pending_queries.remove(&key);
            self.failed_queries.insert(key, reason.to_owned());
        }
        self.pending_expiries.remove(request_id);
        self.completed_queries.remove(request_id);
    }

    fn fail_pending_expiry(&mut self, request_id: &str, reason: &str) {
        if self.pending_expiries.remove(request_id).is_some() {
            self.record_query_failure(reason);
        }
    }

    fn take_query_failure(&mut self, key: &PendingQueryKey) -> Option<String> {
        self.failed_queries.remove(key)
    }

    fn absorb_runtime_queries(&mut self) -> HashSet<String> {
        let mut seen = HashSet::new();
        let mut valid = HashSet::new();
        let mut unknown_request = false;
        for record in self.runtime.drain_queries() {
            let request_id = record.request_id.clone();
            let keys = self.pending_keys_for_request(&request_id);
            let expiry = self.pending_expiries.get(&request_id).cloned();
            if keys.is_empty() && expiry.is_none() {
                unknown_request = true;
                self.record_query_failure("runtime_failure");
                continue;
            }
            if !seen.insert(request_id.clone()) {
                self.record_query_failure("runtime_failure");
                self.fail_pending_request(&request_id, "runtime_failure");
                valid.remove(&request_id);
                continue;
            }
            if !Self::query_record_is_valid(&record, &keys, expiry.as_ref()) {
                self.record_query_failure("runtime_failure");
                if expiry.is_some() {
                    self.fail_pending_expiry(&request_id, "runtime_failure");
                } else {
                    self.fail_pending_request(&request_id, "runtime_failure");
                }
                continue;
            }
            valid.insert(request_id.clone());
            if expiry.is_some() {
                self.pending_expiries.remove(&request_id);
                if let Some(target) = expiry {
                    self.reconnect_targets
                        .retain(|_, row| row.net_entity_id != target.net_entity_id);
                }
                if record.outcome == "request_error" {
                    self.record_query_failure(
                        record
                            .code
                            .clone()
                            .unwrap_or_else(|| "runtime_failure".to_owned()),
                    );
                }
            } else {
                if self.completed_queries.len() >= MAX_PENDING_QUERIES
                    && !self.completed_queries.contains_key(&request_id)
                {
                    self.fail_pending_request(&request_id, "runtime_failure");
                    self.record_query_failure("runtime_failure");
                    continue;
                }
                self.completed_queries.insert(request_id, record.clone());
            }
            if self.runtime_queries.len() >= MAX_RUNTIME_QUERY_HISTORY {
                self.runtime_queries.pop_front();
            }
            self.runtime_queries.push_back(record);
        }
        if unknown_request {
            let pending_ids: HashSet<String> = self
                .pending_queries
                .values()
                .cloned()
                .chain(self.pending_expiries.keys().cloned())
                .collect();
            for request_id in pending_ids {
                self.fail_pending_request(&request_id, "runtime_failure");
                self.fail_pending_expiry(&request_id, "runtime_failure");
            }
        }
        valid
    }

    fn query_record_is_valid(
        record: &RuntimeQueryRecord,
        keys: &[PendingQueryKey],
        expiry: Option<&ExpireTarget>,
    ) -> bool {
        if record.outcome == "request_error" && (record.code.is_none() || record.detail.is_none()) {
            return false;
        }
        if let Some(_target) = expiry {
            return record.result_type == "ExpireEntityResult"
                && matches!(
                    record.outcome.as_str(),
                    "accepted" | "tombstoned" | "non_existent" | "request_error"
                );
        }
        if keys.len() != 1 {
            return false;
        }
        match &keys[0] {
            PendingQueryKey::Resolve {
                room_id,
                net_entity_id,
            } => {
                if record.result_type != "ResolveBindingResult" {
                    return false;
                }
                match record.outcome.as_str() {
                    "ok" => {
                        record.observed_revision.is_some()
                            && record.binding.as_ref().is_some_and(|binding| {
                                binding.room_id == *room_id
                                    && binding.net_entity_id == *net_entity_id
                            })
                    }
                    "request_error" | "non_existent" | "stale_generation" | "invisible"
                    | "unauthorized" | "tombstoned" => true,
                    _ => false,
                }
            }
            PendingQueryKey::Attribute {
                room_id,
                net_entity_id,
                attribute_id,
                ..
            } => {
                if record.result_type != "AttributeQueryResult" {
                    return false;
                }
                match record.outcome.as_str() {
                    "ok" => {
                        record.net_entity_id.as_deref() == Some(net_entity_id.as_str())
                            && record.room_id.as_deref() == Some(room_id.as_str())
                            && record.attribute_id.as_deref() == Some(attribute_id.as_str())
                            && record.value.is_some()
                            && record.observed_tick.is_some()
                            && record.observed_revision.is_some()
                    }
                    "request_error" | "non_existent" | "stale_generation" | "invisible"
                    | "unauthorized" | "tombstoned" => true,
                    _ => false,
                }
            }
        }
    }

    fn pending_record(&self, key: &PendingQueryKey) -> Option<RuntimeQueryRecord> {
        self.pending_queries
            .get(key)
            .and_then(|id| self.completed_queries.get(id))
            .cloned()
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

    fn admission_unix_seconds(&self) -> u64 {
        // The supplied Unix reading is anchored to the monotonic reading at
        // construction. It must advance on every verification, independently
        // of simulation ticks (including an idle or disconnected room).
        self.unix_seconds.saturating_add(
            self.clock
                .now_ms()
                .saturating_sub(self.admission_clock_origin_ms)
                / 1_000,
        )
    }

    fn admit(&mut self, room_id: &str, connection_id: &str, credential: &str) -> RoomAdmitResult {
        if let Some(verifier) = &self.admission_verifier {
            if room_id != verifier.allocation.room_id {
                return RoomAdmitResult::reject("admission_binding_mismatch");
            }
            return match verifier.verify(credential) {
                Ok(proof) => self.admit_verified(room_id, connection_id, &proof.payload),
                Err(code) => RoomAdmitResult::reject(&code),
            };
        }
        match verify_admission(
            credential,
            self.admission_key_id,
            &self.admission_public,
            self.admission_unix_seconds(),
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
        if self.pending_admissions.contains_key(connection_id) {
            return RoomAdmitResult::reject("admission_pending");
        }
        if !self.active_rooms.contains(room_id) && self.active_rooms.len() >= MAX_PENDING_ADMISSIONS
        {
            return RoomAdmitResult::reject("admission_capacity");
        }
        if self.pending_admissions.len() >= MAX_PENDING_ADMISSIONS {
            self.clear_pending_admission(connection_id);
            return RoomAdmitResult::reject("admission_capacity");
        }
        let kind = super::runtime::entity_type_of(&payload.login_name, payload.bot_tool_context);
        // Runtime admission is asynchronous. Use the host's live session table
        // to select the takeover path before enqueueing a duplicate account.
        // Runtime remains authoritative for the actual rebind and emits the
        // supersession/Welcome frames on the next owner tick.
        if self
            .sessions
            .values()
            .any(|session| session.account_id == payload.account_id)
        {
            return self.takeover(room_id, connection_id, payload, kind);
        }
        self.retired_connections.remove(connection_id);
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
        let routed = self.route_frames(room_id, &rebound.frames);
        self.rearm_expiry_for_account(&payload.account_id);
        if !routed {
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
        self.active_rooms.insert(room_id.to_owned());
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
        // Runtime ownership moves at the supersession notice, even when the
        // replacement Welcome is delayed or malformed.
        self.retire_superseded(connection_id, &frames);
        if completed {
            return RoomAdmitResult::ok(reconnected, takeover);
        }
        RoomAdmitResult::pending(reconnected, takeover)
    }

    fn clear_pending_admission(&mut self, connection_id: &str) {
        let rearm_account = self
            .pending_admissions
            .remove(connection_id)
            .filter(|pending| pending.reconnected)
            .map(|pending| pending.payload.account_id);
        self.deferred_frames.remove(connection_id);
        self.pending_egress
            .remove(connection_id)
            .into_iter()
            .flatten()
            .for_each(|egress| egress.sender.abort());
        if self.retired_connections.len() >= MAX_PENDING_ADMISSIONS {
            if let Some(evicted) = self.retired_connections.iter().next().cloned() {
                self.retired_connections.remove(&evicted);
            }
        }
        self.retired_connections.insert(connection_id.to_owned());
        if let Some(account_id) = rearm_account {
            self.rearm_expiry_for_account(&account_id);
        }
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
            self.clear_pending_admission(connection_id);
            return None;
        }
        if reconnected {
            self.cancel_expiry_for_account(&payload.account_id);
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
        self.retire_superseded_frames(frames, Some(connection_id));
    }

    fn retire_superseded_frames(
        &mut self,
        frames: &[RuntimeFrame],
        replacement_connection: Option<&str>,
    ) {
        let superseded_connections: Vec<String> = frames
            .iter()
            .filter(|frame| frame.message_type.as_deref() == Some("ConnectionSuperseded"))
            .filter_map(|frame| frame.connection.clone())
            .filter(|connection| replacement_connection != Some(connection.as_str()))
            .collect();
        for old_id in superseded_connections {
            // Keep an unattached observer long enough to flush the addressed
            // supersession frame before its close marker is queued.
            self.pending_admissions.remove(&old_id);
            if let Some(mut old) = self.sessions.remove(&old_id) {
                for egress in &mut old.egresses {
                    egress.request_close();
                }
                self.pending_egress
                    .entry(old_id.clone())
                    .or_default()
                    .extend(old.egresses);
            }
            if let Some(egresses) = self.pending_egress.get_mut(&old_id) {
                for egress in egresses {
                    egress.request_close();
                }
            }
            self.deferred_frames.remove(&old_id);
            if self.retired_connections.len() >= MAX_PENDING_ADMISSIONS {
                if let Some(evicted) = self.retired_connections.iter().next().cloned() {
                    self.retired_connections.remove(&evicted);
                }
            }
            self.retired_connections.insert(old_id);
        }
    }

    fn route_pending_frames(&mut self, frames: &[RuntimeFrame]) {
        // Rebind emits the old-connection supersede and the replacement
        // Welcome in one owner tick. Retire every addressed old connection
        // before resolving pending admissions so stale input cannot remain live.
        self.retire_superseded_frames(frames, None);
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
            self.retire_superseded(&connection_id, &relevant);
            let _ = self.complete_pending_admission(&connection_id, &relevant);
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
            self.clear_pending_admission(&connection_id);
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
            self.clear_pending_admission(&connection_id);
        }
    }

    fn disconnect(&mut self, connection_id: &str) -> Result<bool, String> {
        self.clear_pending_wire_inputs(connection_id);
        if let Some(pending) = self.pending_admissions.get(connection_id) {
            let room_id = pending.room_id.clone();
            let runtime_result = self.runtime.disconnect_pending(connection_id);
            let (frames, runtime_error) = match runtime_result {
                Ok(frames) => (frames, None),
                Err(error) => (Vec::new(), Some(error)),
            };
            let routed = self.route_frames(&room_id, &frames);
            self.clear_pending_admission(connection_id);
            if let Some(error) = runtime_error {
                return Err(error);
            }
            return if routed {
                Ok(true)
            } else {
                Err("runtime_failure".to_owned())
            };
        }
        let Some(session) = self.sessions.get(connection_id) else {
            return Ok(false);
        };
        let room_id = session.room_id.clone();
        let runtime_binding = Self::session_binding(session);
        let runtime_result = self.runtime.disconnect(connection_id, &runtime_binding)?;
        let Some(session) = self.sessions.remove(connection_id) else {
            return Ok(false);
        };
        self.reconnect_targets.insert(
            session.account_id.clone(),
            ExpireTarget {
                room_id: session.room_id.clone(),
                net_entity_id: session.net_entity_id.clone(),
                due_ms: self.clock.now_ms().saturating_add(self.reconnect_window_ms),
            },
        );
        self.deferred_frames.remove(connection_id);
        for egress in &session.egresses {
            let _ = egress.sender.try_close();
        }
        let routed = self.route_frames(&room_id, &runtime_result.frames);
        self.schedule_expire(&session.room_id, &session.net_entity_id)?;
        if routed {
            Ok(true)
        } else {
            Err("runtime_failure".to_owned())
        }
    }

    fn schedule_expire(&mut self, room_id: &str, net_entity_id: &str) -> Result<(), String> {
        let due = self.clock.now_ms().saturating_add(self.reconnect_window_ms);
        if self.schedule_expire_at(due, room_id, net_entity_id) {
            Ok(())
        } else {
            self.retain_expiry_retry(due, room_id, net_entity_id);
            Err("kernel_timer_schedule_failed".to_owned())
        }
    }

    fn schedule_expire_at(&mut self, due: u64, room_id: &str, net_entity_id: &str) -> bool {
        let Ok(handle) = self
            .kernel
            .schedule_one_shot(TimerMode::WallClock, due, DISPATCH_EXPIRE)
        else {
            return false;
        };
        self.expire_watch.insert(
            handle,
            ExpireTarget {
                room_id: room_id.to_owned(),
                net_entity_id: net_entity_id.to_owned(),
                due_ms: due,
            },
        );
        true
    }

    fn retain_expiry_retry(&mut self, due_ms: u64, room_id: &str, net_entity_id: &str) {
        self.retry_expiries.insert(
            net_entity_id.to_owned(),
            ExpireTarget {
                room_id: room_id.to_owned(),
                net_entity_id: net_entity_id.to_owned(),
                due_ms,
            },
        );
    }

    fn cancel_expiry_for_account(&mut self, account_id: &str) {
        let Some(target) = self.reconnect_targets.remove(account_id) else {
            return;
        };
        let handles: Vec<KernelHandle> = self
            .expire_watch
            .iter()
            .filter(|(_, row)| row.net_entity_id == target.net_entity_id)
            .map(|(handle, _)| *handle)
            .collect();
        for handle in handles {
            let _ = self.kernel.cancel(handle);
            self.expire_watch.remove(&handle);
        }
        self.retry_expiries.remove(&target.net_entity_id);
        let pending: Vec<String> = self
            .pending_expiries
            .iter()
            .filter(|(_, row)| row.net_entity_id == target.net_entity_id)
            .map(|(request_id, _)| request_id.clone())
            .collect();
        for request_id in pending {
            self.pending_expiries.remove(&request_id);
        }
    }

    fn rearm_expiry_for_account(&mut self, account_id: &str) {
        let Some(target) = self.reconnect_targets.get(account_id).cloned() else {
            return;
        };
        let handles: Vec<KernelHandle> = self
            .expire_watch
            .iter()
            .filter(|(_, row)| row.net_entity_id == target.net_entity_id)
            .map(|(handle, _)| *handle)
            .collect();
        for handle in handles {
            let _ = self.kernel.cancel(handle);
            self.expire_watch.remove(&handle);
        }
        self.retry_expiries.remove(&target.net_entity_id);
        let pending: Vec<String> = self
            .pending_expiries
            .iter()
            .filter(|(_, row)| row.net_entity_id == target.net_entity_id)
            .map(|(request_id, _)| request_id.clone())
            .collect();
        for request_id in pending {
            self.pending_expiries.remove(&request_id);
        }
        let due = self.clock.now_ms().saturating_add(self.reconnect_window_ms);
        if !self.schedule_expire_at(due, &target.room_id, &target.net_entity_id) {
            self.retain_expiry_retry(due, &target.room_id, &target.net_entity_id);
        }
    }

    fn pending_rebind_for_target(&self, target: &ExpireTarget) -> Option<String> {
        self.pending_admissions
            .values()
            .find(|pending| {
                pending.reconnected
                    && self
                        .reconnect_targets
                        .get(&pending.payload.account_id)
                        .is_some_and(|row| row.net_entity_id == target.net_entity_id)
            })
            .map(|pending| pending.payload.account_id.clone())
    }

    fn drive_wall(&mut self) -> bool {
        let now = self.clock.now_ms();
        let retries: Vec<ExpireTarget> = self.retry_expiries.drain().map(|(_, row)| row).collect();
        for target in retries {
            if !self.schedule_expire_at(
                target.due_ms.max(now),
                &target.room_id,
                &target.net_entity_id,
            ) {
                self.retry_expiries
                    .insert(target.net_entity_id.clone(), target);
            }
        }
        let Ok(fired) = self.kernel.pump_wall_clock(now) else {
            return false;
        };
        let mut succeeded = true;
        for event in fired {
            if event.dispatch_id != DISPATCH_EXPIRE {
                continue;
            }
            if let Some(target) = self.expire_watch.remove(&event.handle) {
                if let Some(account_id) = self.pending_rebind_for_target(&target) {
                    self.rearm_expiry_for_account(&account_id);
                    continue;
                }
                if !self.correlation_capacity_available() {
                    self.record_query_failure("runtime_query_capacity");
                    let due = now.saturating_add(1);
                    if !self.schedule_expire_at(due, &target.room_id, &target.net_entity_id) {
                        self.retain_expiry_retry(due, &target.room_id, &target.net_entity_id);
                    }
                    succeeded = false;
                    continue;
                }
                let request_id = self.next_query_id("expire");
                match self
                    .runtime
                    .expire_with_request_id(&request_id, &target.net_entity_id)
                {
                    Ok(result) if self.route_frames(&target.room_id, &result.frames) => {
                        self.reconnect_targets
                            .retain(|_, row| row.net_entity_id != target.net_entity_id);
                    }
                    Ok(_) => {
                        self.record_query_failure("runtime_failure");
                        succeeded = false;
                    }
                    Err(error) => {
                        let _ = self.route_frames(&target.room_id, &error.frames);
                        if error.request_id.as_deref() == Some(request_id.as_str())
                            && error.message == "runtime_query_pending"
                        {
                            self.pending_expiries.insert(request_id, target);
                        } else {
                            self.record_query_failure(if error.message.is_empty() {
                                "runtime_failure".to_owned()
                            } else {
                                error.message
                            });
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

    fn flush_pending_wire_inputs(&mut self, room_id: &str) {
        let mut ordered: Vec<(String, usize)> = self
            .pending_wire_inputs
            .iter()
            .enumerate()
            .filter(|(_, pending)| pending.room_id == room_id)
            .map(|(index, pending)| {
                let sender = self
                    .sessions
                    .get(&pending.connection_id)
                    .map(|session| session.net_entity_id.clone())
                    .unwrap_or_default();
                (sender, index)
            })
            .collect();
        ordered.sort_by(|left, right| left.0.cmp(&right.0).then(left.1.cmp(&right.1)));
        ordered.truncate(MAX_CHAT_INPUTS_PER_TICK);

        let mut removal: Vec<usize> = ordered.iter().map(|(_, index)| *index).collect();
        removal.sort_unstable_by(|left, right| right.cmp(left));
        let mut selected = Vec::with_capacity(removal.len());
        for index in removal {
            let pending = self.pending_wire_inputs.remove(index);
            self.pending_wire_input_bytes = self
                .pending_wire_input_bytes
                .saturating_sub(pending.envelope_bytes.len());
            selected.push((index, pending));
        }
        selected.sort_by(|(left_index, left), (right_index, right)| {
            let left_sender = self
                .sessions
                .get(&left.connection_id)
                .map(|session| session.net_entity_id.as_str())
                .unwrap_or_default();
            let right_sender = self
                .sessions
                .get(&right.connection_id)
                .map(|session| session.net_entity_id.as_str())
                .unwrap_or_default();
            left_sender
                .cmp(right_sender)
                .then(left_index.cmp(right_index))
        });
        for (_, pending) in selected {
            let _ = self.admit_input_command(&pending.connection_id, &pending.envelope_bytes);
        }
        self.wire_chat_pending = self.pending_wire_inputs.len() as u64;
    }

    fn run_tick(&mut self, room_id: &str) -> RuntimeTick {
        self.flush_pending_wire_inputs(room_id);
        self.tick_id = self.tick_id.saturating_add(1);
        let Ok(fired) = self.kernel.advance_tick_frame(self.tick_id) else {
            self.wire_chat_pending = self.pending_wire_inputs.len() as u64;
            return RuntimeTick::failed("runtime_failure");
        };
        if !fired.iter().any(|row| row.dispatch_id == DISPATCH_TICK) {
            self.wire_chat_pending = self.pending_wire_inputs.len() as u64;
            return RuntimeTick::failed("runtime_failure");
        }
        self.run_tick_after_advance(room_id)
    }

    fn run_tick_after_advance(&mut self, room_id: &str) -> RuntimeTick {
        self.flush_pending_observers();
        let expected_queries = self.pending_request_ids_for_tick();
        let tick = self.runtime.run_tick(room_id, self.tick_id);
        let completed = self.absorb_runtime_queries();
        for request_id in expected_queries {
            let still_pending = self
                .pending_queries
                .values()
                .any(|pending_id| pending_id == &request_id)
                || self.pending_expiries.contains_key(&request_id);
            if still_pending && !completed.contains(&request_id) {
                self.fail_pending_request(&request_id, "runtime_failure");
                self.record_query_failure("runtime_failure");
            }
        }
        let routed = self.route_frames(room_id, &tick.frames);
        self.route_pending_frames(&tick.frames);
        self.retire_failed_admissions(&tick.frames);
        self.retire_unresolved_admissions(room_id);
        self.wire_chat_pending = self.pending_wire_inputs.len() as u64;
        if !tick.ok {
            if let Some(code) = self.query_failures.first().cloned() {
                let mut failed = tick;
                failed.code = Some(code);
                self.query_failures.clear();
                return failed;
            }
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

    /// Production cadence consumes NativeCore firings on the owner thread.
    /// Tests with deterministic clocks retain explicit control through the
    /// public `run_tick`/`drive_kernel` probes.
    fn drive_owner_cadence(&mut self) {
        let _ = self.drive_wall();
        self.flush_pending_observers();
        self.tick_id = self.tick_id.saturating_add(1);
        let Ok(fired) = self.kernel.advance_tick_frame(self.tick_id) else {
            return;
        };
        if !fired.iter().any(|row| row.dispatch_id == DISPATCH_TICK) {
            return;
        }
        // Stable visitation order, independent of HashMap/HashSet seed and
        // socket presence. A disconnected room continues simulation.
        let mut rooms = self.active_rooms.clone();
        rooms.extend(
            self.sessions
                .values()
                .map(|session| session.room_id.clone()),
        );
        rooms.extend(
            self.pending_admissions
                .values()
                .map(|pending| pending.room_id.clone()),
        );
        rooms.extend(
            self.pending_wire_inputs
                .iter()
                .map(|pending| pending.room_id.clone()),
        );
        rooms.extend(
            self.pending_expiries
                .values()
                .map(|target| target.room_id.clone()),
        );
        rooms.extend(
            self.retry_expiries
                .values()
                .map(|target| target.room_id.clone()),
        );
        for room_id in rooms {
            self.flush_pending_wire_inputs(&room_id);
            let _ = self.run_tick_after_advance(&room_id);
        }
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
        if self.retired_connections.contains(connection) {
            return true;
        }
        match self.deliver_to_connection(connection, bytes) {
            Delivery::Delivered | Delivery::Backpressured => true,
            Delivery::Invalid | Delivery::Overflow => {
                let _ = self.fail_connection(connection);
                false
            }
            Delivery::Unavailable => {
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

    fn flush_pending_observers(&mut self) {
        let mut invalid_sessions = Vec::new();
        for (connection, session) in &mut self.sessions {
            let had_socket = !session.egresses.is_empty();
            if !flush_observer_egresses(&mut session.egresses)
                || (had_socket && session.egresses.is_empty())
            {
                invalid_sessions.push(connection.clone());
            }
        }
        let pending_connections: Vec<String> = self.pending_egress.keys().cloned().collect();
        for connection in pending_connections {
            let remove = self
                .pending_egress
                .get_mut(&connection)
                .map(|egresses| {
                    let valid = flush_observer_egresses(egresses);
                    egresses.is_empty() || !valid
                })
                .unwrap_or(false);
            if remove {
                self.pending_egress.remove(&connection);
            }
        }
        for connection in invalid_sessions {
            let _ = self.fail_connection(&connection);
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
        self.clear_pending_wire_inputs(connection);
        let Some(session) = self.sessions.get(connection) else {
            self.pending_egress
                .remove(connection)
                .into_iter()
                .flatten()
                .for_each(|egress| {
                    egress.sender.abort();
                });
            self.deferred_frames.remove(connection);
            return Ok(());
        };
        let runtime_binding = Self::session_binding(session);
        let runtime_result = self.runtime.disconnect(connection, &runtime_binding)?;
        let Some(session) = self.sessions.remove(connection) else {
            return Ok(());
        };
        self.reconnect_targets.insert(
            session.account_id.clone(),
            ExpireTarget {
                room_id: session.room_id.clone(),
                net_entity_id: session.net_entity_id.clone(),
                due_ms: self.clock.now_ms().saturating_add(self.reconnect_window_ms),
            },
        );
        self.deferred_frames.remove(connection);
        for egress in &session.egresses {
            egress.sender.abort();
        }
        if let Some(egresses) = self.pending_egress.remove(connection) {
            for egress in egresses {
                egress.sender.abort();
            }
        }

        for frame in runtime_result.frames {
            if frame.connection.as_deref() == Some(connection) {
                continue;
            }
            let _ = self.route_frames(&session.room_id, &[frame]);
        }
        self.schedule_expire(&session.room_id, &session.net_entity_id)?;
        Ok(())
    }

    fn clear_pending_wire_inputs(&mut self, connection_id: &str) {
        let mut removed_bytes = 0usize;
        let mut retained = Vec::with_capacity(self.pending_wire_inputs.len());
        for pending in self.pending_wire_inputs.drain(..) {
            if pending.connection_id == connection_id {
                removed_bytes = removed_bytes.saturating_add(pending.envelope_bytes.len());
            } else {
                retained.push(pending);
            }
        }
        self.pending_wire_inputs = retained;
        self.pending_wire_input_bytes = self.pending_wire_input_bytes.saturating_sub(removed_bytes);
        self.wire_chat_pending = self.pending_wire_inputs.len() as u64;
    }

    fn on_wire(&mut self, event: WireEvent) {
        self.flush_pending_observers();
        match event {
            WireEvent::Authenticated {
                connection_id,
                egress,
                proof,
            } => {
                let result = self.admit_verified(&proof.room_id, &connection_id, &proof.payload);
                if !result.accepted {
                    let _ = egress.try_close();
                    return;
                }
                if let Some(session) = self.sessions.get_mut(&connection_id) {
                    session.egresses.push(ObserverEgress::new(egress));
                } else {
                    self.pending_egress
                        .insert(connection_id.clone(), vec![ObserverEgress::new(egress)]);
                }
                self.flush_deferred(&connection_id);
            }
            WireEvent::Attached {
                connection_id,
                egress,
            } => {
                if self.admission_verifier.is_some() {
                    egress.abort();
                    return;
                }
                if self.sessions.contains_key(&connection_id) {
                    if let Some(session) = self.sessions.get_mut(&connection_id) {
                        if session.egresses.len() >= MAX_PENDING_EGRESS_PER_CONNECTION {
                            egress.abort();
                            return;
                        }
                        session.egresses.push(ObserverEgress::new(egress.clone()));
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
                            queue.push(ObserverEgress::new(egress));
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
                observer_id,
                text,
            } => {
                if self.admission_verifier.is_some()
                    && !self.sessions.get(&connection_id).is_some_and(|session| {
                        session.egresses.iter().any(|egress| {
                            egress.sender.observer_id() == observer_id && !egress.sender.is_closed()
                        })
                    })
                {
                    return;
                }
                if text.as_bytes().len() > MAX_WIRE_TEXT_BYTES {
                    let _ = self.fail_connection(&connection_id);
                    return;
                }
                let Some(room_id) = self
                    .sessions
                    .get(&connection_id)
                    .map(|session| session.room_id.clone())
                else {
                    return;
                };
                if self
                    .pending_wire_inputs
                    .iter()
                    .filter(|pending| pending.connection_id == connection_id)
                    .count()
                    >= INGRESS_QUEUE_PER_CONNECTION
                {
                    let _ = self.fail_connection(&connection_id);
                    return;
                }
                let envelope_bytes = text.into_bytes();
                if self.pending_wire_inputs.len() >= MAX_PENDING_WIRE_INPUTS
                    || self
                        .pending_wire_input_bytes
                        .saturating_add(envelope_bytes.len())
                        > MAX_PENDING_WIRE_INPUT_BYTES
                {
                    let _ = self.fail_connection(&connection_id);
                    return;
                }
                if let Some(observer) = &self.wire_input_observer {
                    let _ = observer.try_send(envelope_bytes.clone());
                }
                self.pending_wire_inputs.push(PendingWireInput {
                    room_id: room_id.clone(),
                    connection_id: connection_id.clone(),
                    envelope_bytes,
                });
                self.pending_wire_input_bytes = self.pending_wire_input_bytes.saturating_add(
                    self.pending_wire_inputs
                        .last()
                        .map_or(0, |pending| pending.envelope_bytes.len()),
                );
                self.wire_chat_pending = self.pending_wire_inputs.len() as u64;
                // Ingress never advances logical time. Only the owner cadence
                // (or an explicit deterministic test tick) drains this budget.
            }
            WireEvent::Closed {
                connection_id,
                observer_id,
            } => {
                let mut removed = false;
                let mut has_observer = false;
                if let Some(session) = self.sessions.get_mut(&connection_id) {
                    let before = session.egresses.len();
                    session
                        .egresses
                        .retain(|egress| egress.sender.observer_id() != observer_id);
                    removed = before != session.egresses.len();
                    has_observer = !session.egresses.is_empty();
                }
                if !removed {
                    if let Some(egresses) = self.pending_egress.get_mut(&connection_id) {
                        let before = egresses.len();
                        egresses.retain(|egress| egress.sender.observer_id() != observer_id);
                        removed = before != egresses.len();
                        has_observer = !egresses.is_empty();
                    }
                }
                if !removed || has_observer {
                    return;
                }
                if self.sessions.contains_key(&connection_id) {
                    let _ = self.disconnect(&connection_id);
                } else {
                    self.pending_admissions.remove(&connection_id);
                    self.deferred_frames.remove(&connection_id);
                    self.pending_egress.remove(&connection_id);
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
        if let Some(error) = self.take_query_failure(&key) {
            return Err(error);
        }
        if let Some(record) = self.pending_record(&key) {
            return self.resolve_completed(&key, room_id, net_entity_id, record);
        }
        if self.pending_queries.contains_key(&key) {
            return Err("runtime_query_pending".to_owned());
        }
        if !self.correlation_capacity_available() {
            return Err("runtime_query_capacity".to_owned());
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
        if let Some(error) = self.take_query_failure(&key) {
            return QueryResult::request_error(&error);
        }
        if let Some(record) = self.pending_record(&key) {
            return self.attribute_completed(&key, request, record);
        }
        if self.pending_queries.contains_key(&key) {
            let request_id = self.pending_queries.get(&key).expect("pending query");
            return QueryResult::pending(request_id);
        }
        if !self.correlation_capacity_available() {
            return QueryResult::request_error("runtime_query_capacity");
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

#[cfg(test)]
mod tests {
    use super::super::wire::test_sender_pair;
    use super::super::wire::WireOut;
    use super::*;

    #[test]
    fn pending_observer_frame_flushes_after_consumer_wake() {
        let (sender, rx) = test_sender_pair(1);
        let mut egresses = vec![ObserverEgress::new(sender.clone())];

        sender.try_send_bytes(b"occupied").expect("fill egress");
        assert_eq!(
            deliver_to_egresses(&mut egresses, b"queued"),
            Delivery::Backpressured
        );
        assert!(matches!(
            rx.recv().expect("consumer receives occupied"),
            WireOut::Text(bytes) if bytes == b"occupied"
        ));

        assert!(flush_observer_egresses(&mut egresses));
        assert!(matches!(
            rx.recv().expect("consumer wake receives queued"),
            WireOut::Text(bytes) if bytes == b"queued"
        ));
    }

    #[test]
    fn ordered_close_waits_for_pending_frames() {
        let (sender, rx) = test_sender_pair(1);
        let mut egress = ObserverEgress::new(sender.clone());

        sender.try_send_bytes(b"occupied").expect("fill egress");
        egress.pending.push_back(b"superseded".to_vec());
        egress.pending_bytes = b"superseded".len();
        egress.request_close();

        assert_eq!(flush_observer_egress(&mut egress), Delivery::Backpressured);
        assert!(
            matches!(rx.recv().expect("occupied"), WireOut::Text(bytes) if bytes == b"occupied")
        );
        assert_eq!(flush_observer_egress(&mut egress), Delivery::Backpressured);
        assert!(
            matches!(rx.recv().expect("superseded"), WireOut::Text(bytes) if bytes == b"superseded")
        );
        assert_eq!(flush_observer_egress(&mut egress), Delivery::Delivered);
        assert!(matches!(rx.recv().expect("close"), WireOut::Close));
    }
}

#[cfg(test)]
#[path = "host_hardening_tests.rs"]
mod hardening_tests;
