//! Deterministic owner-thread regressions. Runtime/Kernel below are test doubles;
//! these tests do not claim Native, CoreCLR, or end-to-end gameplay coverage.

use std::sync::{Arc, Mutex};

use lumio_host_runtime::{KernelError, KernelFired};

use super::super::admission::{generate_keys, issue_admission_credential};
use super::super::runtime::{
    RuntimeAdmit, RuntimeControlError, RuntimeControlResult, RuntimeDisconnect,
};
use super::super::wire::{test_sender_pair, WireOut};
use super::*;

#[derive(Default)]
struct Trace {
    inputs: Vec<(String, Vec<u8>)>,
    ticks: Vec<(String, u64)>,
    next_outcome: Option<ChatOperation>,
}

struct RecordingRuntime(Arc<Mutex<Trace>>);

impl RuntimeSurface for RecordingRuntime {
    fn admit(
        &mut self,
        connection: &str,
        account_id: &str,
        room_id: &str,
        entity_type: BoundEntityKind,
    ) -> RuntimeAdmit {
        let binding = RuntimeBinding {
            account_id: account_id.to_owned(),
            room_id: room_id.to_owned(),
            net_entity_id: format!("entity-{account_id}"),
            entity_type,
            connection_generation: 1,
        };
        RuntimeAdmit {
            accepted: true,
            code: None,
            frames: vec![RuntimeFrame {
                connection: Some(connection.to_owned()),
                bytes: b"test-welcome".to_vec(),
                observer_net_entity_id: Some(binding.net_entity_id.clone()),
                connection_generation: Some(1),
                message_type: Some("Welcome".to_owned()),
                code: None,
            }],
            binding: Some(binding),
        }
    }

    fn disconnect(
        &mut self,
        _connection: &str,
        binding: &RuntimeBinding,
    ) -> Result<RuntimeDisconnect, String> {
        Ok(RuntimeDisconnect {
            binding: binding.clone(),
            frames: Vec::new(),
        })
    }

    fn rebind(
        &mut self,
        _connection: &str,
        _account_id: &str,
        _room_id: &str,
        _mode: RebindMode,
        _entity_type: BoundEntityKind,
    ) -> RuntimeAdmit {
        panic!("this regression must not rebind")
    }

    fn expire(
        &mut self,
        _net_entity_id: &str,
    ) -> Result<RuntimeControlResult<()>, RuntimeControlError> {
        panic!("this regression must not expire an entity")
    }

    fn resolve_by_net_entity_id(
        &mut self,
        _room_id: &str,
        _net_entity_id: &str,
    ) -> Result<RuntimeControlResult<Option<RuntimeBinding>>, RuntimeControlError> {
        panic!("this regression must not resolve a binding")
    }

    fn query_attribute(
        &mut self,
        _request: &RuntimeQuery,
    ) -> Result<RuntimeControlResult<QueryResult>, RuntimeControlError> {
        panic!("this regression must not query an attribute")
    }

    fn attach_member(&mut self, _room_id: &str, _connection: &str) -> Result<(), String> {
        Ok(())
    }

    fn admit_input_command(
        &mut self,
        _room_id: &str,
        connection: &str,
        _generation: u64,
        _net_entity_id: &str,
        bytes: &[u8],
    ) -> ChatOperation {
        self.0
            .lock()
            .expect("trace lock")
            .inputs
            .push((connection.to_owned(), bytes.to_vec()));
        self.0
            .lock()
            .expect("trace lock")
            .next_outcome
            .take()
            .unwrap_or_else(ChatOperation::admitted)
    }

    fn run_tick(&mut self, room_id: &str, tick_id: u64) -> RuntimeTick {
        self.0
            .lock()
            .expect("trace lock")
            .ticks
            .push((room_id.to_owned(), tick_id));
        RuntimeTick::committed(tick_id, tick_id, 0)
    }

    fn persist(&mut self, _room_id: &str) -> Result<PersistRecord, String> {
        panic!("this regression must not capture a snapshot")
    }

    fn restore(&mut self, _room_id: &str, _bytes: &[u8]) -> Result<(), String> {
        panic!("this regression must not restore a snapshot")
    }
}

struct TickKernel;

const HANDLE: KernelHandle = KernelHandle {
    index: 1,
    generation: 1,
    context: 1,
};

impl KernelTimer for TickKernel {
    fn schedule_one_shot(
        &mut self,
        _mode: TimerMode,
        _due: u64,
        _dispatch_id: u32,
    ) -> Result<KernelHandle, KernelError> {
        Ok(HANDLE)
    }

    fn schedule_repeating(
        &mut self,
        _mode: TimerMode,
        _first_due: u64,
        _interval: u64,
        _dispatch_id: u32,
    ) -> Result<KernelHandle, KernelError> {
        Ok(HANDLE)
    }

    fn cancel(&mut self, _handle: KernelHandle) -> Result<(), KernelError> {
        Ok(())
    }

    fn pump_wall_clock(&mut self, _now_ms: u64) -> Result<Vec<KernelFired>, KernelError> {
        Ok(Vec::new())
    }

    fn advance_tick_frame(&mut self, to_tick: u64) -> Result<Vec<KernelFired>, KernelError> {
        Ok(vec![KernelFired {
            handle: HANDLE,
            due: to_tick,
            schedule_sequence: to_tick,
            dispatch_id: DISPATCH_TICK,
        }])
    }
}

fn owner() -> (Inner, Arc<Mutex<Trace>>) {
    let trace = Arc::new(Mutex::new(Trace::default()));
    let clock = SharedClock::test();
    assert!(clock.advance_test_clock(5_000));
    let inner = Inner {
        health: Arc::new(HealthState::default()),
        admission_clock_origin_ms: clock.now_ms(),
        active_rooms: BTreeSet::new(),
        admission_verifier: None,
        clock,
        reconnect_window_ms: 300_000,
        admission_key_id: 1,
        admission_public: Vec::new(),
        unix_seconds: 1_000,
        runtime: Box::new(RecordingRuntime(trace.clone())),
        kernel: Box::new(TickKernel),
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
        last_committed_tick: 0,
        wire_chat_pending: 0,
        pending_wire_inputs: Vec::new(),
        pending_wire_input_bytes: 0,
        wire_input_observer: None,
    };
    (inner, trace)
}

fn admit(inner: &mut Inner, room: &str, connection: &str) {
    let payload = AdmissionPayload {
        key_id: 1,
        account_id: connection.to_owned(),
        login_name: connection.to_owned(),
        bot_tool_context: false,
        issued_at: 1,
        expires_at: 9_000,
    };
    assert!(inner.admit_verified(room, connection, &payload).accepted);
    assert!(inner.sessions.contains_key(connection));
}

fn input(inner: &mut Inner, connection: &str, text: &str) {
    inner.on_wire(WireEvent::Input {
        connection_id: connection.to_owned(),
        text: text.to_owned(),
        observer_id: 0,
    });
}

#[test]
fn same_connection_fifo_survives_reverse_index_removal() {
    let (mut inner, trace) = owner();
    admit(&mut inner, "room", "a");
    for text in ["first", "second", "third"] {
        input(&mut inner, "a", text);
    }
    assert!(inner.run_tick("room").ok);
    let received: Vec<Vec<u8>> = trace
        .lock()
        .expect("trace lock")
        .inputs
        .iter()
        .map(|(_, bytes)| bytes.clone())
        .collect();
    assert_eq!(
        received,
        vec![b"first".to_vec(), b"second".to_vec(), b"third".to_vec()]
    );
    assert_eq!(inner.pending_wire_input_bytes, 0);
}

#[test]
fn sender_order_is_deterministic_and_each_sender_is_fifo() {
    let (mut inner, trace) = owner();
    admit(&mut inner, "room", "b");
    admit(&mut inner, "room", "a");
    for (connection, text) in [("b", "b1"), ("a", "a1"), ("b", "b2"), ("a", "a2")] {
        input(&mut inner, connection, text);
    }
    assert!(inner.run_tick("room").ok);
    let actual = trace.lock().expect("trace lock").inputs.clone();
    let expected: Vec<(String, Vec<u8>)> = [("a", "a1"), ("a", "a2"), ("b", "b1"), ("b", "b2")]
        .into_iter()
        .map(|(connection, text)| (connection.to_owned(), text.as_bytes().to_vec()))
        .collect();
    assert_eq!(actual, expected);
}

#[test]
fn budget_keeps_the_remainder_and_its_byte_accounting() {
    let (mut inner, trace) = owner();
    admit(&mut inner, "room", "a");
    admit(&mut inner, "room", "b");
    for index in 0..MAX_CHAT_INPUTS_PER_TICK {
        input(&mut inner, "a", &index.to_string());
    }
    input(&mut inner, "b", "remaining");
    assert!(trace.lock().expect("trace lock").ticks.is_empty());
    assert!(inner.run_tick("room").ok);
    assert_eq!(
        trace.lock().expect("trace lock").inputs.len(),
        MAX_CHAT_INPUTS_PER_TICK
    );
    assert_eq!(inner.pending_wire_inputs.len(), 1);
    assert_eq!(inner.pending_wire_input_bytes, b"remaining".len());
    assert!(inner.run_tick("room").ok);
    assert_eq!(
        trace
            .lock()
            .expect("trace lock")
            .inputs
            .last()
            .expect("last")
            .1,
        b"remaining"
    );
    assert!(inner.pending_wire_inputs.is_empty());
    assert_eq!(inner.pending_wire_input_bytes, 0);
}

#[test]
fn input_at_and_over_the_limit_never_advances_logical_time() {
    let (mut inner, trace) = owner();
    admit(&mut inner, "room", "a");
    for _ in 0..INGRESS_QUEUE_PER_CONNECTION {
        input(&mut inner, "a", "input");
    }
    assert_eq!(inner.tick_id, 0);
    assert!(trace.lock().expect("trace lock").inputs.is_empty());
    input(&mut inner, "a", "overflow");
    assert_eq!(inner.tick_id, 0);
    assert!(trace.lock().expect("trace lock").ticks.is_empty());
    assert!(!inner.sessions.contains_key("a"));
}

#[test]
fn last_disconnect_does_not_pause_the_runtime_room() {
    let (mut inner, trace) = owner();
    admit(&mut inner, "room", "a");
    assert!(inner.disconnect("a").expect("disconnect"));
    assert!(inner.sessions.is_empty());
    for _ in 0..3 {
        inner.drive_owner_cadence();
    }
    assert_eq!(
        trace.lock().expect("trace lock").ticks,
        vec![
            ("room".to_owned(), 1),
            ("room".to_owned(), 2),
            ("room".to_owned(), 3)
        ]
    );
}

#[test]
fn active_room_visitation_is_sorted_and_bounded() {
    let (mut inner, trace) = owner();
    admit(&mut inner, "z-room", "z");
    admit(&mut inner, "a-room", "a");
    inner.drive_owner_cadence();
    assert_eq!(
        trace.lock().expect("trace lock").ticks,
        vec![("a-room".to_owned(), 1), ("z-room".to_owned(), 1)]
    );
    inner.active_rooms = (0..MAX_PENDING_ADMISSIONS)
        .map(|i| format!("room-{i}"))
        .collect();
    let payload = AdmissionPayload {
        key_id: 1,
        account_id: "new".to_owned(),
        login_name: "New".to_owned(),
        bot_tool_context: false,
        issued_at: 1,
        expires_at: 9_000,
    };
    let result = inner.admit_verified("extra-room", "extra", &payload);
    assert_eq!(result.error_code.as_deref(), Some("admission_capacity"));
    assert_eq!(inner.active_rooms.len(), MAX_PENDING_ADMISSIONS);
}

#[test]
fn admission_time_advances_without_ticks_from_the_construction_origin() {
    let (mut inner, _) = owner();
    let keys = generate_keys();
    inner.admission_public = keys.public.to_vec();
    let token = issue_admission_credential(&keys.seed, 1, "account", "Player", false, 1_000, 1_001);
    assert_eq!(inner.admission_unix_seconds(), 1_000);
    assert!(inner.clock.advance_test_clock(2_000));
    assert_eq!(inner.admission_unix_seconds(), 1_002);
    assert_eq!(inner.tick_id, 0);
    let result = inner.admit("room", "expired", &token);
    assert_eq!(
        result.error_code.as_deref(),
        Some("admission_credential_expired")
    );
    assert!(inner.sessions.is_empty());
}

#[test]
fn new_egress_frames_queue_behind_an_existing_backlog() {
    let (sender, rx) = test_sender_pair(1);
    sender.try_send_bytes(b"first").expect("fill");
    let mut egress = ObserverEgress::new(sender);
    egress.pending.push_back(b"second".to_vec());
    egress.pending_bytes = b"second".len();
    let mut egresses = vec![egress];
    assert_eq!(
        deliver_to_egresses(&mut egresses, b"third"),
        Delivery::Backpressured
    );
    for expected in [
        b"first".as_slice(),
        b"second".as_slice(),
        b"third".as_slice(),
    ] {
        assert!(
            matches!(rx.try_recv().expect("queued frame"), WireOut::Text(bytes) if bytes == expected)
        );
        assert!(flush_observer_egresses(&mut egresses));
    }
    assert!(egresses[0].pending.is_empty());
    assert_eq!(egresses[0].pending_bytes, 0);
}

#[test]
fn quiesced_world_rejects_input_and_does_not_advance_cadence() {
    let (mut inner, trace) = owner();
    admit(&mut inner, "room", "a");
    inner.health.draining.store(true, Ordering::Release);
    input(&mut inner, "a", "must not apply");
    inner.drive_owner_cadence();
    assert!(inner.pending_wire_inputs.is_empty());
    assert!(trace.lock().expect("trace").ticks.is_empty());
    assert!(
        !inner
            .admit_verified(
                "room",
                "b",
                &AdmissionPayload {
                    key_id: 1,
                    account_id: "b".into(),
                    login_name: "bbb".into(),
                    bot_tool_context: false,
                    issued_at: 1,
                    expires_at: 9000
                }
            )
            .accepted
    );
}

#[test]
fn rejected_input_is_counted_and_fatal_input_seals_world() {
    let (mut inner, trace) = owner();
    admit(&mut inner, "room", "a");
    trace.lock().expect("trace").next_outcome = Some(ChatOperation::rejected("bad_envelope"));
    input(&mut inner, "a", "rejected");
    assert!(inner.run_tick("room").ok);
    assert_eq!(inner.health.rejected_inputs.load(Ordering::Relaxed), 1);
    trace.lock().expect("trace").next_outcome = Some(ChatOperation {
        kind: ChatOpKind::Fatal,
        error_code: Some("runtime_failure".into()),
    });
    input(&mut inner, "a", "fatal");
    assert!(!inner.run_tick("room").ok);
    assert!(inner.health.faulted.load(Ordering::Acquire));
    assert!(!inner.run_tick("room").ok);
    assert_eq!(trace.lock().expect("trace").ticks.len(), 1);
}
