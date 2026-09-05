//! Test doubles of Runtime and `NativeCore` ABI. Not production kernels or binding tables.
#![allow(dead_code)]
#![allow(clippy::struct_excessive_bools)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use lumio_host_runtime::{KernelError, KernelFired, KernelHandle, KernelTimer, TimerMode};
use lumio_server_process::entity_chat::{
    AttributeQueryOutcome, AttributeQueryScope, BoundEntityKind, ChatOperation, PersistRecord,
    QueryResult, RebindMode, RuntimeAdmit, RuntimeBinding, RuntimeControlError,
    RuntimeControlResult, RuntimeDisconnect, RuntimeFrame, RuntimeQuery, RuntimeQueryRecord,
    RuntimeSurface, RuntimeTick, MAX_CHAT_INPUTS_PER_TICK,
};

pub const DISPATCH_EXPIRE: u32 = 1;
pub const DISPATCH_TICK: u32 = 2;
pub const RUNTIME_WIRE_CHAT_INPUT: &str = r#"{"commands":[{"mappingId":"chat.input","payload":"020000006767","payloadSha256":"5dbd584f1718b8bcd0dab4abeea83169f4a990defab81a8316ed845798d92dab"}],"messageType":"InputCommand"}"#;

pub fn runtime_wire_chat_input() -> Vec<u8> {
    RUNTIME_WIRE_CHAT_INPUT.as_bytes().to_vec()
}

pub struct TestKernel {
    one_shots: Vec<(u64, u32, KernelHandle)>,
    repeating: Vec<(u64, u64, u32, KernelHandle)>,
    next: u32,
    committed_ms: u64,
    committed_tick: u64,
}

impl TestKernel {
    #[must_use]
    pub fn new() -> Self {
        Self {
            one_shots: Vec::new(),
            repeating: Vec::new(),
            next: 1,
            committed_ms: 0,
            committed_tick: 0,
        }
    }

    fn alloc(&mut self) -> KernelHandle {
        let handle = KernelHandle {
            index: self.next,
            generation: 1,
            context: 1,
        };
        self.next += 1;
        handle
    }
}

impl Default for TestKernel {
    fn default() -> Self {
        Self::new()
    }
}

impl KernelTimer for TestKernel {
    fn schedule_one_shot(
        &mut self,
        mode: TimerMode,
        due: u64,
        dispatch_id: u32,
    ) -> Result<KernelHandle, KernelError> {
        assert_eq!(mode, TimerMode::WallClock);
        let handle = self.alloc();
        self.one_shots.push((due, dispatch_id, handle));
        Ok(handle)
    }

    fn schedule_repeating(
        &mut self,
        mode: TimerMode,
        first_due: u64,
        interval: u64,
        dispatch_id: u32,
    ) -> Result<KernelHandle, KernelError> {
        assert_eq!(mode, TimerMode::TickFrame);
        let handle = self.alloc();
        self.repeating
            .push((first_due, interval, dispatch_id, handle));
        Ok(handle)
    }

    fn cancel(&mut self, handle: KernelHandle) -> Result<(), KernelError> {
        self.one_shots.retain(|row| row.2 != handle);
        self.repeating.retain(|row| row.3 != handle);
        Ok(())
    }

    fn pump_wall_clock(&mut self, now_ms: u64) -> Result<Vec<KernelFired>, KernelError> {
        self.committed_ms = now_ms;
        let mut fired = Vec::new();
        self.one_shots.retain(|(due, dispatch, handle)| {
            if *due <= now_ms {
                fired.push(KernelFired {
                    handle: *handle,
                    due: *due,
                    schedule_sequence: 1,
                    dispatch_id: *dispatch,
                });
                false
            } else {
                true
            }
        });
        Ok(fired)
    }

    fn advance_tick_frame(&mut self, to_tick: u64) -> Result<Vec<KernelFired>, KernelError> {
        let mut fired = Vec::new();
        for (next_due, interval, dispatch, handle) in &mut self.repeating {
            while *next_due <= to_tick {
                fired.push(KernelFired {
                    handle: *handle,
                    due: *next_due,
                    schedule_sequence: 1,
                    dispatch_id: *dispatch,
                });
                *next_due = next_due.saturating_add(*interval);
            }
        }
        self.committed_tick = to_tick;
        Ok(fired)
    }
}

#[derive(Clone)]
struct Occupancy {
    binding: RuntimeBinding,
    live_connection: Option<String>,
}

pub struct ScriptedRuntime {
    next: u64,
    by_connection: HashMap<String, RuntimeBinding>,
    retained: HashMap<String, Occupancy>,
    entities: HashMap<String, Occupancy>,
    tombstoned: HashMap<String, String>,
    planted_query: HashMap<(String, String, String), QueryResult>,
    planted_snapshot: Option<Vec<u8>>,
    snapshot_failed: bool,
    planted_delta: Vec<Vec<u8>>,
    persist_bytes: Vec<u8>,
    tick: u64,
    revision: u64,
    expire_calls: Vec<String>,
    disconnect_calls: Vec<String>,
    admit_calls: Vec<String>,
    restore_calls: usize,
    pending_chats: Vec<(String, String)>,
    events_by_tick: HashMap<u64, Vec<(String, String)>>,
    run_tick_input_counts: Vec<usize>,
    resolve_error: Option<RuntimeControlError>,
    query_error: Option<RuntimeControlError>,
    reject_next_admit: Option<String>,
    disconnect_error: Option<String>,
    tick_error: Option<String>,
    persist_error: Option<String>,
    restore_error: Option<String>,
    async_queries: bool,
    async_admissions: bool,
    queued_admissions: Vec<(String, RuntimeBinding)>,
    queued_queries: Vec<RuntimeQueryRecord>,
    query_calls: Vec<String>,
    async_query_error: Option<String>,
    suppress_next_async_query_result: bool,
    malformed_next_async_query_result: bool,
    suppress_rebind_frames: bool,
    suppress_rebind_welcome: bool,
}

impl ScriptedRuntime {
    #[must_use]
    pub fn new() -> Self {
        Self {
            next: 1,
            by_connection: HashMap::new(),
            retained: HashMap::new(),
            entities: HashMap::new(),
            tombstoned: HashMap::new(),
            planted_query: HashMap::new(),
            planted_snapshot: None,
            snapshot_failed: false,
            planted_delta: Vec::new(),
            persist_bytes: b"persist".to_vec(),
            tick: 0,
            revision: 0,
            expire_calls: Vec::new(),
            disconnect_calls: Vec::new(),
            admit_calls: Vec::new(),
            restore_calls: 0,
            pending_chats: Vec::new(),
            events_by_tick: HashMap::new(),
            run_tick_input_counts: Vec::new(),
            resolve_error: None,
            query_error: None,
            reject_next_admit: None,
            disconnect_error: None,
            tick_error: None,
            persist_error: None,
            restore_error: None,
            async_queries: false,
            async_admissions: false,
            queued_admissions: Vec::new(),
            queued_queries: Vec::new(),
            query_calls: Vec::new(),
            async_query_error: None,
            suppress_next_async_query_result: false,
            malformed_next_async_query_result: false,
            suppress_rebind_frames: false,
            suppress_rebind_welcome: false,
        }
    }

    #[must_use]
    pub fn run_tick_input_counts(&self) -> &[usize] {
        &self.run_tick_input_counts
    }

    pub fn plant_query(&mut self, room: &str, net: &str, attr: &str, result: QueryResult) {
        self.planted_query
            .insert((room.to_owned(), net.to_owned(), attr.to_owned()), result);
    }

    pub fn fail_resolve(&mut self, message: &str) {
        self.resolve_error = Some(RuntimeControlError::new(message.to_owned(), Vec::new()));
    }

    pub fn fail_query(&mut self, message: &str) {
        self.query_error = Some(RuntimeControlError::new(message.to_owned(), Vec::new()));
    }

    pub fn reject_next_admit_with_frame(&mut self, code: &str) {
        self.reject_next_admit = Some(code.to_owned());
    }

    pub fn plant_snapshot(&mut self, json: &str) {
        self.snapshot_failed = false;
        self.planted_snapshot = Some(json.as_bytes().to_vec());
    }

    pub fn fail_snapshot(&mut self) {
        self.snapshot_failed = true;
        self.planted_snapshot = None;
    }

    pub fn plant_delta(&mut self, frames: Vec<String>) {
        self.planted_delta = frames.into_iter().map(String::into_bytes).collect();
    }

    pub fn plant_raw_delta(&mut self, frames: Vec<Vec<u8>>) {
        self.planted_delta = frames;
    }

    pub fn fail_disconnect(&mut self, message: &str) {
        self.disconnect_error = Some(message.to_owned());
    }

    pub fn fail_next_tick(&mut self, message: &str) {
        self.tick_error = Some(message.to_owned());
    }

    pub fn fail_persist(&mut self, message: &str) {
        self.persist_error = Some(message.to_owned());
    }

    pub fn fail_restore(&mut self, message: &str) {
        self.restore_error = Some(message.to_owned());
    }

    pub fn enable_async_queries(&mut self) {
        self.async_queries = true;
    }

    pub fn enable_async_admissions(&mut self) {
        self.async_admissions = true;
    }

    pub fn suppress_next_async_query_result(&mut self) {
        self.async_queries = true;
        self.suppress_next_async_query_result = true;
    }

    pub fn malform_next_async_query_result(&mut self) {
        self.async_queries = true;
        self.malformed_next_async_query_result = true;
    }

    pub fn fail_next_async_query(&mut self, message: &str) {
        self.async_queries = true;
        self.async_query_error = Some(message.to_owned());
    }

    pub fn suppress_rebind_frames(&mut self) {
        self.suppress_rebind_frames = true;
    }

    pub fn suppress_rebind_welcome(&mut self) {
        self.suppress_rebind_welcome = true;
    }

    pub fn seed_live_binding(
        &mut self,
        connection: &str,
        account_id: &str,
        room_id: &str,
        entity_type: BoundEntityKind,
    ) {
        let binding = RuntimeBinding {
            account_id: account_id.to_owned(),
            room_id: room_id.to_owned(),
            net_entity_id: self.alloc(),
            entity_type,
            connection_generation: 1,
        };
        self.by_connection
            .insert(connection.to_owned(), binding.clone());
        self.entities.insert(
            binding.net_entity_id.clone(),
            Occupancy {
                binding,
                live_connection: Some(connection.to_owned()),
            },
        );
    }

    #[must_use]
    pub fn expire_calls(&self) -> &[String] {
        &self.expire_calls
    }

    #[must_use]
    pub fn disconnect_calls(&self) -> &[String] {
        &self.disconnect_calls
    }

    #[must_use]
    pub fn admit_calls(&self) -> &[String] {
        &self.admit_calls
    }

    #[must_use]
    pub fn query_calls(&self) -> &[String] {
        &self.query_calls
    }

    #[must_use]
    pub fn restore_calls(&self) -> usize {
        self.restore_calls
    }

    fn alloc(&mut self) -> String {
        let id = format!("{:032x}", self.next);
        self.next += 1;
        id
    }

    fn async_outcome(
        result_type: &str,
        request_id: &str,
        result: QueryResult,
    ) -> RuntimeQueryRecord {
        let (outcome, code) = match result.outcome {
            AttributeQueryOutcome::Ok => ("ok", None),
            AttributeQueryOutcome::RequestError => (
                "request_error",
                result
                    .error_code
                    .or_else(|| Some("runtime_failure".to_owned())),
            ),
            AttributeQueryOutcome::NonExistent => ("non_existent", None),
            AttributeQueryOutcome::StaleGeneration => ("stale_generation", None),
            AttributeQueryOutcome::Invisible => ("invisible", None),
            AttributeQueryOutcome::Unauthorized => ("unauthorized", None),
            AttributeQueryOutcome::Tombstoned => ("tombstoned", None),
        };
        RuntimeQueryRecord {
            request_id: request_id.to_owned(),
            result_type: result_type.to_owned(),
            outcome: outcome.to_owned(),
            binding: None,
            value: result.value,
            net_entity_id: None,
            room_id: None,
            attribute_id: None,
            code,
            detail: (outcome == "request_error").then(|| "test failure".to_owned()),
            observed_revision: Some(result.observed_revision),
            observed_tick: Some(result.observed_tick),
        }
    }

    fn queue_async_error(&mut self, result_type: &str, request_id: &str) {
        let code = self
            .async_query_error
            .take()
            .unwrap_or_else(|| "runtime_failure".to_owned());
        self.queued_queries.push(RuntimeQueryRecord {
            request_id: request_id.to_owned(),
            result_type: result_type.to_owned(),
            outcome: "request_error".to_owned(),
            binding: None,
            value: None,
            net_entity_id: None,
            room_id: None,
            attribute_id: None,
            code: Some(code),
            detail: Some("test failure".to_owned()),
            observed_revision: None,
            observed_tick: None,
        });
    }
}

impl Default for ScriptedRuntime {
    fn default() -> Self {
        Self::new()
    }
}

impl RuntimeSurface for ScriptedRuntime {
    fn admit(
        &mut self,
        connection: &str,
        account_id: &str,
        room_id: &str,
        entity_type: BoundEntityKind,
    ) -> RuntimeAdmit {
        self.admit_calls.push(connection.to_owned());
        if let Some(code) = self.reject_next_admit.take() {
            return RuntimeAdmit {
                accepted: false,
                code: Some(code.clone()),
                binding: None,
                frames: vec![RuntimeFrame {
                    connection: Some(connection.to_owned()),
                    bytes: format!(
                        r#"{{"code":"{code}","detail":"rejected","messageType":"Error"}}"#
                    )
                    .into_bytes(),
                    observer_net_entity_id: None,
                    connection_generation: None,
                    message_type: Some("Error".to_owned()),
                    code: Some(code),
                }],
            };
        }
        if self
            .by_connection
            .values()
            .any(|row| row.account_id == account_id)
        {
            return RuntimeAdmit::reject("account_already_online");
        }
        if let Some(live) = self.retained.get(account_id) {
            if live.live_connection.is_some() {
                return RuntimeAdmit::reject("invalid_binding_shape");
            }
            return RuntimeAdmit::reject("invalid_binding_shape");
        }
        if self
            .by_connection
            .values()
            .any(|row| row.account_id == account_id && row.room_id != room_id)
        {
            return RuntimeAdmit::reject("cross_room_reference");
        }
        let binding = RuntimeBinding {
            account_id: account_id.to_owned(),
            room_id: room_id.to_owned(),
            net_entity_id: self.alloc(),
            entity_type,
            connection_generation: 1,
        };
        self.by_connection
            .insert(connection.to_owned(), binding.clone());
        self.entities.insert(
            binding.net_entity_id.clone(),
            Occupancy {
                binding: binding.clone(),
                live_connection: Some(connection.to_owned()),
            },
        );
        if self.async_admissions {
            self.queued_admissions
                .push((connection.to_owned(), binding.clone()));
            return RuntimeAdmit::ok(binding);
        }
        let mut result = RuntimeAdmit::ok(binding.clone());
        if !self.snapshot_failed {
            result.frames.push(RuntimeFrame {
                connection: Some(connection.to_owned()),
                bytes: self
                    .planted_snapshot
                    .clone()
                    .unwrap_or_else(|| default_snapshot().into_bytes()),
                observer_net_entity_id: Some(binding.net_entity_id.clone()),
                connection_generation: Some(binding.connection_generation),
                message_type: Some("Welcome".to_owned()),
                code: None,
            });
        }
        result
    }

    fn disconnect(
        &mut self,
        connection: &str,
        _binding: &RuntimeBinding,
    ) -> Result<RuntimeDisconnect, String> {
        self.disconnect_calls.push(connection.to_owned());
        if let Some(error) = &self.disconnect_error {
            return Err(error.clone());
        }
        let binding = self
            .by_connection
            .remove(connection)
            .ok_or_else(|| "binding_not_found".to_owned())?;
        if let Some(occupancy) = self.entities.get_mut(&binding.net_entity_id) {
            occupancy.live_connection = None;
            occupancy.binding = binding.clone();
        }
        self.retained.insert(
            binding.account_id.clone(),
            Occupancy {
                binding: binding.clone(),
                live_connection: None,
            },
        );
        Ok(RuntimeDisconnect {
            binding,
            frames: Vec::new(),
        })
    }

    fn disconnect_pending(&mut self, connection: &str) -> Result<Vec<RuntimeFrame>, String> {
        self.queued_admissions
            .retain(|(queued, _)| queued != connection);
        self.disconnect(
            connection,
            &RuntimeBinding {
                account_id: String::new(),
                room_id: String::new(),
                net_entity_id: String::new(),
                entity_type: BoundEntityKind::Player,
                connection_generation: 0,
            },
        )
        .map(|result| result.frames)
    }

    fn rebind(
        &mut self,
        connection: &str,
        account_id: &str,
        room_id: &str,
        mode: RebindMode,
        _entity_type: BoundEntityKind,
    ) -> RuntimeAdmit {
        if mode == RebindMode::Takeover {
            let Some(old_conn) = self
                .by_connection
                .iter()
                .find(|(_, row)| row.account_id == account_id)
                .map(|(id, _)| id.clone())
            else {
                return RuntimeAdmit::reject("binding_not_found");
            };
            let mut binding = self.by_connection.remove(&old_conn).expect("live");
            if binding.room_id != room_id {
                return RuntimeAdmit::reject("cross_room_reference");
            }
            binding.connection_generation += 1;
            self.by_connection
                .insert(connection.to_owned(), binding.clone());
            let mut result = RuntimeAdmit::ok(binding.clone());
            if !self.suppress_rebind_frames {
                result.frames.push(RuntimeFrame {
                    connection: Some(old_conn),
                    bytes: superseded_frame(result.binding.as_ref().expect("binding")),
                    observer_net_entity_id: None,
                    connection_generation: Some(binding.connection_generation),
                    message_type: Some("ConnectionSuperseded".to_owned()),
                    code: None,
                });
            }
            if self.suppress_rebind_frames || self.suppress_rebind_welcome {
                return result;
            }
            result.frames.push(RuntimeFrame {
                connection: Some(connection.to_owned()),
                bytes: default_snapshot().into_bytes(),
                observer_net_entity_id: Some(binding.net_entity_id.clone()),
                connection_generation: Some(binding.connection_generation),
                message_type: Some("Welcome".to_owned()),
                code: None,
            });
            return result;
        }
        let Some(occupancy) = self.retained.remove(account_id) else {
            return RuntimeAdmit::reject("binding_not_found");
        };
        let mut binding = occupancy.binding;
        if binding.room_id != room_id {
            return RuntimeAdmit::reject("cross_room_reference");
        }
        binding.connection_generation += 1;
        self.by_connection
            .insert(connection.to_owned(), binding.clone());
        self.entities.insert(
            binding.net_entity_id.clone(),
            Occupancy {
                binding: binding.clone(),
                live_connection: Some(connection.to_owned()),
            },
        );
        let mut result = RuntimeAdmit::ok(binding.clone());
        result.frames.push(RuntimeFrame {
            connection: Some(connection.to_owned()),
            bytes: default_snapshot().into_bytes(),
            observer_net_entity_id: Some(binding.net_entity_id.clone()),
            connection_generation: Some(binding.connection_generation),
            message_type: Some("Welcome".to_owned()),
            code: None,
        });
        result
    }

    fn expire(
        &mut self,
        net_entity_id: &str,
    ) -> Result<RuntimeControlResult<()>, RuntimeControlError> {
        let net_entity_id = net_entity_id.to_owned();
        self.expire_calls.push(net_entity_id.clone());
        if let Some(occupancy) = self.entities.remove(&net_entity_id) {
            self.tombstoned
                .insert(net_entity_id.clone(), occupancy.binding.room_id);
            self.retained
                .retain(|_, row| row.binding.net_entity_id != net_entity_id);
        }
        Ok(RuntimeControlResult::new((), Vec::new()))
    }

    fn expire_with_request_id(
        &mut self,
        request_id: &str,
        net_entity_id: &str,
    ) -> Result<RuntimeControlResult<()>, RuntimeControlError> {
        if !self.async_queries {
            return self.expire(net_entity_id);
        }
        let id = net_entity_id.to_owned();
        self.expire(net_entity_id)?;
        if self.async_query_error.is_some() {
            self.queue_async_error("ExpireEntityResult", request_id);
        } else {
            self.queued_queries.push(RuntimeQueryRecord {
                request_id: request_id.to_owned(),
                result_type: "ExpireEntityResult".to_owned(),
                outcome: "tombstoned".to_owned(),
                binding: None,
                value: None,
                net_entity_id: Some(id),
                room_id: None,
                attribute_id: None,
                code: None,
                detail: None,
                observed_revision: None,
                observed_tick: None,
            });
        }
        Err(RuntimeControlError::pending(request_id))
    }

    fn resolve_by_net_entity_id(
        &mut self,
        room_id: &str,
        net_entity_id: &str,
    ) -> Result<RuntimeControlResult<Option<RuntimeBinding>>, RuntimeControlError> {
        if let Some(error) = self.resolve_error.take() {
            return Err(error);
        }
        let Some(occupancy) = self.entities.get(net_entity_id) else {
            return Ok(RuntimeControlResult::new(None, Vec::new()));
        };
        if occupancy.binding.room_id != room_id {
            return Ok(RuntimeControlResult::new(None, Vec::new()));
        }
        Ok(RuntimeControlResult::new(
            Some(occupancy.binding.clone()),
            Vec::new(),
        ))
    }

    fn resolve_by_net_entity_id_with_request_id(
        &mut self,
        request_id: &str,
        room_id: &str,
        net_entity_id: &str,
    ) -> Result<RuntimeControlResult<Option<RuntimeBinding>>, RuntimeControlError> {
        self.query_calls.push(request_id.to_owned());
        if !self.async_queries {
            return self.resolve_by_net_entity_id(room_id, net_entity_id);
        }
        if self.async_query_error.is_some() {
            self.queue_async_error("ResolveBindingResult", request_id);
        } else if self.suppress_next_async_query_result {
            self.suppress_next_async_query_result = false;
        } else {
            let binding = self.entities.get(net_entity_id).and_then(|occupancy| {
                (occupancy.binding.room_id == room_id).then(|| occupancy.binding.clone())
            });
            let mut record = RuntimeQueryRecord {
                request_id: request_id.to_owned(),
                result_type: "ResolveBindingResult".to_owned(),
                outcome: binding.as_ref().map_or("non_existent", |_| "ok").to_owned(),
                binding,
                value: None,
                net_entity_id: None,
                room_id: None,
                attribute_id: None,
                code: None,
                detail: None,
                observed_revision: Some(self.revision),
                observed_tick: None,
            };
            if self.malformed_next_async_query_result {
                self.malformed_next_async_query_result = false;
                record.binding = None;
            }
            self.queued_queries.push(record);
        }
        Err(RuntimeControlError::pending(request_id))
    }

    fn query_attribute(
        &mut self,
        request: &RuntimeQuery,
    ) -> Result<RuntimeControlResult<QueryResult>, RuntimeControlError> {
        if let Some(error) = self.query_error.take() {
            return Err(error);
        }
        let net_entity_id = request.net_entity_id.clone();
        if let Some(planted) = self.planted_query.get(&(
            request.room_id.clone(),
            net_entity_id.clone(),
            request.attribute_id.clone(),
        )) {
            return Ok(RuntimeControlResult::new(planted.clone(), Vec::new()));
        }
        if let Some(room) = self.tombstoned.get(&net_entity_id) {
            if room != &request.room_id {
                return Ok(RuntimeControlResult::new(
                    QueryResult::request_error("cross_room_reference"),
                    Vec::new(),
                ));
            }
            return Ok(RuntimeControlResult::new(
                QueryResult::fail(AttributeQueryOutcome::Tombstoned),
                Vec::new(),
            ));
        }
        let Some(occupancy) = self.entities.get(&net_entity_id) else {
            return Ok(RuntimeControlResult::new(
                QueryResult::fail(AttributeQueryOutcome::NonExistent),
                Vec::new(),
            ));
        };
        if occupancy.binding.room_id != request.room_id {
            return Ok(RuntimeControlResult::new(
                QueryResult::request_error("cross_room_reference"),
                Vec::new(),
            ));
        }
        if let Some(generation) = request.connection_generation {
            if generation < occupancy.binding.connection_generation {
                return Ok(RuntimeControlResult::new(
                    QueryResult::fail(AttributeQueryOutcome::StaleGeneration),
                    Vec::new(),
                ));
            }
        }
        if request.caller_scope == AttributeQueryScope::ClientReplica
            && request.attribute_id == "EntityIdentity.claimedMark"
        {
            return Ok(RuntimeControlResult::new(
                QueryResult::fail(AttributeQueryOutcome::Unauthorized),
                Vec::new(),
            ));
        }
        Ok(RuntimeControlResult::new(
            QueryResult::ok(occupancy.binding.entity_type.as_str().to_owned(), 0, 0),
            Vec::new(),
        ))
    }

    fn query_attribute_with_request_id(
        &mut self,
        request_id: &str,
        request: &RuntimeQuery,
    ) -> Result<RuntimeControlResult<QueryResult>, RuntimeControlError> {
        self.query_calls.push(request_id.to_owned());
        if !self.async_queries {
            return self.query_attribute(request);
        }
        if self.async_query_error.is_some() {
            self.queue_async_error("AttributeQueryResult", request_id);
        } else if self.suppress_next_async_query_result {
            self.suppress_next_async_query_result = false;
        } else {
            let result = self.query_attribute(request)?;
            let mut record = Self::async_outcome("AttributeQueryResult", request_id, result.value);
            if record.outcome == "ok" {
                record.net_entity_id = Some(request.net_entity_id.clone());
                record.room_id = Some(request.room_id.clone());
                record.attribute_id = Some(request.attribute_id.clone());
            }
            if self.malformed_next_async_query_result {
                self.malformed_next_async_query_result = false;
                record.value = None;
            }
            self.queued_queries.push(record);
        }
        Err(RuntimeControlError::pending(request_id))
    }

    fn attach_member(&mut self, _room_id: &str, _connection: &str) -> Result<(), String> {
        Ok(())
    }

    fn admit_input_command(
        &mut self,
        room_id: &str,
        connection: &str,
        _generation: u64,
        _net_entity_id: &str,
        _envelope_bytes: &[u8],
    ) -> ChatOperation {
        if self.by_connection.contains_key(connection) {
            self.pending_chats
                .push((room_id.to_owned(), connection.to_owned()));
            ChatOperation::admitted()
        } else {
            ChatOperation::rejected("disconnected")
        }
    }

    fn run_tick(&mut self, _room_id: &str, tick_id: u64) -> RuntimeTick {
        self.tick = tick_id;
        self.revision += 1;
        let admissions = std::mem::take(&mut self.queued_admissions);
        let pending = std::mem::take(&mut self.pending_chats);
        self.run_tick_input_counts.push(pending.len());
        if let Some(error) = self.tick_error.take() {
            return RuntimeTick::failed(&error);
        }
        if pending.len() > MAX_CHAT_INPUTS_PER_TICK {
            return RuntimeTick {
                applied_tick: 0,
                revision: self.revision,
                ok: false,
                event_count: 0,
                code: Some("runtime_failure".to_owned()),
                frames: Vec::new(),
            };
        }
        let event_count = pending.len() as u64;
        self.events_by_tick.insert(tick_id, pending);
        let mut result = RuntimeTick::committed(self.tick, self.revision, event_count);
        for (connection, binding) in admissions {
            result.frames.push(RuntimeFrame {
                connection: Some(connection),
                bytes: default_snapshot().into_bytes(),
                observer_net_entity_id: Some(binding.net_entity_id),
                connection_generation: Some(binding.connection_generation),
                message_type: Some("Welcome".to_owned()),
                code: None,
            });
        }
        if !self.planted_delta.is_empty() {
            result.frames = self
                .planted_delta
                .clone()
                .into_iter()
                .map(|bytes| RuntimeFrame {
                    connection: None,
                    bytes,
                    observer_net_entity_id: None,
                    connection_generation: None,
                    message_type: Some("WorldChange".to_owned()),
                    code: None,
                })
                .collect();
        } else if event_count > 0 {
            result.frames.push(RuntimeFrame {
                connection: None,
                bytes: world_change_frame(tick_id).into_bytes(),
                observer_net_entity_id: None,
                connection_generation: None,
                message_type: Some("WorldChange".to_owned()),
                code: None,
            });
        }
        result
    }

    fn drain_queries(&mut self) -> Vec<RuntimeQueryRecord> {
        std::mem::take(&mut self.queued_queries)
    }

    fn persist(&mut self, _room_id: &str) -> Result<PersistRecord, String> {
        if let Some(error) = &self.persist_error {
            return Err(error.clone());
        }
        Ok(PersistRecord {
            bytes: self.persist_bytes.clone(),
        })
    }

    fn restore(&mut self, _room_id: &str, _bytes: &[u8]) -> Result<(), String> {
        if let Some(error) = &self.restore_error {
            return Err(error.clone());
        }
        self.restore_calls += 1;
        Ok(())
    }
}

#[derive(Clone)]
pub struct SharedRuntime(pub Arc<Mutex<ScriptedRuntime>>);

impl SharedRuntime {
    #[must_use]
    pub fn new() -> Self {
        Self(Arc::new(Mutex::new(ScriptedRuntime::new())))
    }

    pub fn lock(&self) -> std::sync::MutexGuard<'_, ScriptedRuntime> {
        self.0.lock().expect("scripted runtime")
    }
}

impl RuntimeSurface for SharedRuntime {
    fn admit(
        &mut self,
        connection: &str,
        account_id: &str,
        room_id: &str,
        entity_type: BoundEntityKind,
    ) -> RuntimeAdmit {
        self.lock()
            .admit(connection, account_id, room_id, entity_type)
    }

    fn disconnect(
        &mut self,
        connection: &str,
        binding: &RuntimeBinding,
    ) -> Result<RuntimeDisconnect, String> {
        self.lock().disconnect(connection, binding)
    }

    fn disconnect_pending(&mut self, connection: &str) -> Result<Vec<RuntimeFrame>, String> {
        self.lock().disconnect_pending(connection)
    }

    fn rebind(
        &mut self,
        connection: &str,
        account_id: &str,
        room_id: &str,
        mode: RebindMode,
        entity_type: BoundEntityKind,
    ) -> RuntimeAdmit {
        self.lock()
            .rebind(connection, account_id, room_id, mode, entity_type)
    }

    fn expire(
        &mut self,
        net_entity_id: &str,
    ) -> Result<RuntimeControlResult<()>, RuntimeControlError> {
        self.lock().expire(net_entity_id)
    }

    fn expire_with_request_id(
        &mut self,
        request_id: &str,
        net_entity_id: &str,
    ) -> Result<RuntimeControlResult<()>, RuntimeControlError> {
        self.lock()
            .expire_with_request_id(request_id, net_entity_id)
    }

    fn resolve_by_net_entity_id(
        &mut self,
        room_id: &str,
        net_entity_id: &str,
    ) -> Result<RuntimeControlResult<Option<RuntimeBinding>>, RuntimeControlError> {
        self.lock().resolve_by_net_entity_id(room_id, net_entity_id)
    }

    fn resolve_by_net_entity_id_with_request_id(
        &mut self,
        request_id: &str,
        room_id: &str,
        net_entity_id: &str,
    ) -> Result<RuntimeControlResult<Option<RuntimeBinding>>, RuntimeControlError> {
        self.lock()
            .resolve_by_net_entity_id_with_request_id(request_id, room_id, net_entity_id)
    }

    fn query_attribute(
        &mut self,
        request: &RuntimeQuery,
    ) -> Result<RuntimeControlResult<QueryResult>, RuntimeControlError> {
        self.lock().query_attribute(request)
    }

    fn query_attribute_with_request_id(
        &mut self,
        request_id: &str,
        request: &RuntimeQuery,
    ) -> Result<RuntimeControlResult<QueryResult>, RuntimeControlError> {
        self.lock()
            .query_attribute_with_request_id(request_id, request)
    }

    fn attach_member(&mut self, room_id: &str, connection: &str) -> Result<(), String> {
        self.lock().attach_member(room_id, connection)
    }

    fn admit_input_command(
        &mut self,
        room_id: &str,
        connection: &str,
        generation: u64,
        net_entity_id: &str,
        envelope_bytes: &[u8],
    ) -> ChatOperation {
        self.lock().admit_input_command(
            room_id,
            connection,
            generation,
            net_entity_id,
            envelope_bytes,
        )
    }

    fn run_tick(&mut self, room_id: &str, tick_id: u64) -> RuntimeTick {
        self.lock().run_tick(room_id, tick_id)
    }

    fn drain_queries(&mut self) -> Vec<RuntimeQueryRecord> {
        self.lock().drain_queries()
    }

    fn persist(&mut self, room_id: &str) -> Result<PersistRecord, String> {
        self.lock().persist(room_id)
    }

    fn restore(&mut self, room_id: &str, bytes: &[u8]) -> Result<(), String> {
        self.lock().restore(room_id, bytes)
    }
}

pub fn welcome_frame() -> String {
    r#"{"connectionGeneration":1,"instanceId":0,"messageType":"Welcome","selfNetEntityId":"00000000000000000000000000000001"}"#
        .to_owned()
}

fn default_snapshot() -> String {
    welcome_frame()
}

fn superseded_frame(binding: &RuntimeBinding) -> Vec<u8> {
    format!(
        "{{\"messageType\":\"ConnectionSuperseded\",\"netEntityId\":\"{}\",\"newConnectionGeneration\":{},\"reasonCode\":\"connection_superseded\"}}",
        binding.net_entity_id, binding.connection_generation
    )
    .into_bytes()
}

pub fn world_change_frame(seq: u64) -> String {
    format!(
        r#"{{"creates":[],"destroys":[],"fields":[],"messageType":"WorldChange","rpcs":[{{"appliedTick":{seq},"args":["{seq}"],"componentId":"ChatComponent","messageId":{seq},"method":"OnChatMessage","roomSequence":{seq},"scope":"room","sender":"00000000000000000000000000000001","target":"00000000000000000000000000000001"}}],"tick":{seq}}}"#
    )
}
