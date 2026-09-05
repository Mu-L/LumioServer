//! CoreCLR consume host for Runtime WorldManager and opaque C-1 persistence.

use std::path::PathBuf;

use serde_json::{json, Value};

use crate::runtime_bridge::{BridgeError, ClrBridge, ClrStart};
use crate::sdk_loader;

use super::runtime::BoundEntityKind;
use super::runtime::{
    ChatOperation, PersistRecord, QueryResult, RebindMode, RuntimeAdmit, RuntimeBinding,
    RuntimeControlError, RuntimeControlResult, RuntimeDisconnect, RuntimeFrame, RuntimeQuery,
    RuntimeSurface, RuntimeTick,
};

/// Files needed to create the CoreCLR Runtime consume host.
#[derive(Debug, Clone)]
pub struct ClrGameplayConfig {
    pub engine_native: PathBuf,
    pub hostfxr: PathBuf,
    pub runtime_config: PathBuf,
    pub assembly: PathBuf,
    pub entry_type: String,
    pub entry_method: String,
    pub replication_assembly: PathBuf,
    pub ecs_assembly: PathBuf,
    pub registry_assembly: PathBuf,
}

/// CoreCLR-backed [`RuntimeSurface`].
pub struct ClrGameplay {
    bridge: ClrBridge,
    replication_assembly: String,
    ecs_assembly: String,
    registry_assembly: String,
    booted: bool,
    next_request_id: u64,
}

impl ClrGameplay {
    /// Loads the native SDK and creates the process-wide CoreCLR host.
    ///
    /// # Errors
    ///
    /// Returns a human-readable failure when the SDK or CLR host cannot start.
    pub fn start(config: &ClrGameplayConfig) -> Result<Self, String> {
        let lease = sdk_loader::load(&config.engine_native).map_err(|error| error.to_string())?;
        let start = ClrStart {
            hostfxr: config.hostfxr.to_string_lossy().into_owned(),
            runtime_config: config.runtime_config.to_string_lossy().into_owned(),
            assembly: config.assembly.to_string_lossy().into_owned(),
            entry_type: config.entry_type.clone(),
            entry_method: config.entry_method.clone(),
        };
        let bridge = ClrBridge::start(lease, &start)?;
        Ok(Self {
            bridge,
            replication_assembly: config.replication_assembly.to_string_lossy().into_owned(),
            ecs_assembly: config.ecs_assembly.to_string_lossy().into_owned(),
            registry_assembly: config.registry_assembly.to_string_lossy().into_owned(),
            booted: false,
            next_request_id: 1,
        })
    }

    fn call(&mut self, request: Value) -> Result<Value, String> {
        if !self.booted {
            let boot = json!({
                "op": "boot",
                "replicationAssembly": self.replication_assembly,
                "ecsAssembly": self.ecs_assembly,
                "registryAssembly": self.registry_assembly,
            });
            let body = self
                .bridge
                .invoke_json(&boot.to_string())
                .map_err(bridge_err)?;
            let parsed: Value =
                serde_json::from_str(&body).map_err(|_| "boot response is not JSON".to_owned())?;
            if parsed.get("ok").and_then(Value::as_bool) != Some(true) {
                let detail = parsed
                    .get("code")
                    .and_then(Value::as_str)
                    .unwrap_or("boot_failed");
                return Err(detail.to_owned());
            }
            self.booted = true;
        }
        let body = self
            .bridge
            .invoke_json(&request.to_string())
            .map_err(bridge_err)?;
        serde_json::from_str(&body).map_err(|_| "runtime response is not JSON".to_owned())
    }

    fn enqueue(&mut self, message: Value) -> Result<(), String> {
        let value = self.call(message)?;
        if value.get("ok").and_then(Value::as_bool) == Some(true) {
            Ok(())
        } else {
            Err(value
                .get("code")
                .and_then(Value::as_str)
                .unwrap_or("runtime_failure")
                .to_owned())
        }
    }

    fn tick_and_drain(&mut self) -> Result<(Value, RuntimeDrain), String> {
        let tick = self.call(json!({ "op": "tick" }))?;
        let drain = self.call(json!({ "op": "drain" }))?;
        parse_drain_response(&drain).map(|drain| (tick, drain))
    }

    fn next_request_id(&mut self, kind: &str) -> String {
        let id = format!("server-a2-{kind}-{}", self.next_request_id);
        self.next_request_id = self.next_request_id.saturating_add(1);
        id
    }
}

fn bridge_err(error: BridgeError) -> String {
    match error {
        BridgeError::Rejected { code } => code.as_str().to_owned(),
        BridgeError::Failed { detail } => detail.to_owned(),
    }
}

fn frames_from_runtime(value: &Value) -> Result<Vec<RuntimeFrame>, String> {
    let Some(raw_frames) = value.get("frames") else {
        return Ok(Vec::new());
    };
    let rows = raw_frames
        .as_array()
        .ok_or_else(|| "runtime frames must be an array".to_owned())?;
    rows.iter()
        .enumerate()
        .map(|(index, row)| {
            let object = row
                .as_object()
                .ok_or_else(|| format!("runtime frame {index} is not an object"))?;
            let encoded = object
                .get("bytesBase64")
                .and_then(Value::as_str)
                .ok_or_else(|| format!("runtime frame {index} is missing bytesBase64"))?;
            let bytes = decode_base64(encoded)
                .ok_or_else(|| "runtime frame bytesBase64 is malformed".to_owned())?;
            Ok(RuntimeFrame {
                connection: optional_frame_string(object, "connection")?,
                bytes,
                observer_net_entity_id: optional_frame_string(object, "observerNetEntityId")?,
                connection_generation: optional_frame_u64(object, "connectionGeneration")?,
                message_type: optional_frame_string(object, "messageType")?,
                code: optional_frame_string(object, "code")?,
            })
        })
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RuntimeDrain {
    frames: Vec<RuntimeFrame>,
    queries: Vec<QueryRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum QueryRecord {
    Expire {
        request_id: String,
        outcome: String,
        code: Option<String>,
        detail: Option<String>,
    },
    Resolve {
        request_id: String,
        outcome: String,
        binding: Option<RuntimeBinding>,
        observed_revision: Option<u64>,
        code: Option<String>,
        detail: Option<String>,
    },
    Attribute {
        request_id: String,
        outcome: String,
        net_entity_id: Option<String>,
        room_id: Option<String>,
        attribute_id: Option<String>,
        value: Option<String>,
        observed_revision: Option<u64>,
        observed_tick: Option<u64>,
        code: Option<String>,
        detail: Option<String>,
    },
}

impl QueryRecord {
    fn request_id(&self) -> &str {
        match self {
            Self::Expire { request_id, .. }
            | Self::Resolve { request_id, .. }
            | Self::Attribute { request_id, .. } => request_id,
        }
    }

    fn type_name(&self) -> &'static str {
        match self {
            Self::Expire { .. } => "ExpireEntityResult",
            Self::Resolve { .. } => "ResolveBindingResult",
            Self::Attribute { .. } => "AttributeQueryResult",
        }
    }
}

/// Parses the internal `drain.queries` records emitted by Runtime.
///
/// The result type and outcome determine the exact required fields. This parser
/// intentionally does not decode C-1 frame bytes or infer any Runtime state.
fn parse_drain_response(value: &Value) -> Result<RuntimeDrain, String> {
    if value.get("ok").and_then(Value::as_bool) != Some(true) {
        return Err("runtime drain response is not ok".to_owned());
    }
    if value.get("frames").is_none() {
        return Err("runtime drain response missing frames array".to_owned());
    }
    let frames = frames_from_runtime(value)?;
    let queries = parse_query_records(value)?;
    Ok(RuntimeDrain { frames, queries })
}

fn parse_query_records(value: &Value) -> Result<Vec<QueryRecord>, String> {
    let rows = value
        .get("queries")
        .and_then(Value::as_array)
        .ok_or_else(|| "runtime drain response missing queries array".to_owned())?;
    rows.iter()
        .enumerate()
        .map(|(index, row)| parse_query_record(index, row))
        .collect()
}

fn parse_query_record(index: usize, value: &Value) -> Result<QueryRecord, String> {
    let object = value
        .as_object()
        .ok_or_else(|| format!("runtime query result {index} is not an object"))?;
    let type_name = required_query_string(object, index, "type")?;
    let request_id = required_query_string(object, index, "requestId")?;
    if request_id.is_empty() {
        return Err(format!("runtime query result {index} requestId is empty"));
    }
    let outcome = required_query_string(object, index, "outcome")?;
    match type_name.as_str() {
        "ExpireEntityResult" => parse_expire_record(index, object, request_id, outcome),
        "ResolveBindingResult" => parse_resolve_record(index, object, request_id, outcome),
        "AttributeQueryResult" => parse_attribute_record(index, object, request_id, outcome),
        other => Err(format!(
            "runtime query result {index} has unknown type {other}"
        )),
    }
}

fn parse_expire_record(
    index: usize,
    object: &serde_json::Map<String, Value>,
    request_id: String,
    outcome: String,
) -> Result<QueryRecord, String> {
    match outcome.as_str() {
        "accepted" | "tombstoned" | "non_existent" => {
            ensure_query_keys(index, object, &["type", "requestId", "outcome"])?;
            Ok(QueryRecord::Expire {
                request_id,
                outcome,
                code: None,
                detail: None,
            })
        }
        "request_error" => {
            ensure_query_keys(
                index,
                object,
                &["type", "requestId", "outcome", "code", "detail"],
            )?;
            Ok(QueryRecord::Expire {
                request_id,
                outcome,
                code: Some(required_query_string(object, index, "code")?),
                detail: Some(required_query_string(object, index, "detail")?),
            })
        }
        other => Err(format!(
            "runtime query result {index} has invalid ExpireEntityResult outcome {other}"
        )),
    }
}

fn parse_resolve_record(
    index: usize,
    object: &serde_json::Map<String, Value>,
    request_id: String,
    outcome: String,
) -> Result<QueryRecord, String> {
    match outcome.as_str() {
        "ok" => {
            ensure_query_keys(
                index,
                object,
                &[
                    "type",
                    "requestId",
                    "outcome",
                    "binding",
                    "observedRevision",
                ],
            )?;
            let binding = parse_binding_record(index, object.get("binding"))?;
            let observed_revision = required_query_u64(object, index, "observedRevision")?;
            Ok(QueryRecord::Resolve {
                request_id,
                outcome,
                binding: Some(binding),
                observed_revision: Some(observed_revision),
                code: None,
                detail: None,
            })
        }
        "non_existent" | "stale_generation" | "invisible" | "unauthorized" | "tombstoned" => {
            ensure_query_keys(index, object, &["type", "requestId", "outcome"])?;
            Ok(QueryRecord::Resolve {
                request_id,
                outcome,
                binding: None,
                observed_revision: None,
                code: None,
                detail: None,
            })
        }
        "request_error" => {
            ensure_query_keys(
                index,
                object,
                &["type", "requestId", "outcome", "code", "detail"],
            )?;
            Ok(QueryRecord::Resolve {
                request_id,
                outcome,
                binding: None,
                observed_revision: None,
                code: Some(required_query_string(object, index, "code")?),
                detail: Some(required_query_string(object, index, "detail")?),
            })
        }
        other => Err(format!(
            "runtime query result {index} has invalid ResolveBindingResult outcome {other}"
        )),
    }
}

fn parse_attribute_record(
    index: usize,
    object: &serde_json::Map<String, Value>,
    request_id: String,
    outcome: String,
) -> Result<QueryRecord, String> {
    match outcome.as_str() {
        "ok" => {
            ensure_query_keys(
                index,
                object,
                &[
                    "type",
                    "requestId",
                    "outcome",
                    "netEntityId",
                    "roomId",
                    "attributeId",
                    "value",
                    "observedRevision",
                    "observedTick",
                ],
            )?;
            let value = object
                .get("value")
                .filter(|value| !value.is_null())
                .ok_or_else(|| format!("runtime query result {index} is missing value"))?;
            let value = if let Some(value) = value.as_str() {
                value.to_owned()
            } else {
                serde_json::to_string(value).map_err(|_| {
                    format!("runtime query result {index} value is not serializable")
                })?
            };
            Ok(QueryRecord::Attribute {
                request_id,
                outcome,
                net_entity_id: Some(required_query_string(object, index, "netEntityId")?),
                room_id: Some(required_query_string(object, index, "roomId")?),
                attribute_id: Some(required_query_string(object, index, "attributeId")?),
                value: Some(value),
                observed_revision: Some(required_query_u64(object, index, "observedRevision")?),
                observed_tick: Some(required_query_u64(object, index, "observedTick")?),
                code: None,
                detail: None,
            })
        }
        "non_existent" | "stale_generation" | "invisible" | "unauthorized" | "tombstoned" => {
            ensure_query_keys(index, object, &["type", "requestId", "outcome"])?;
            Ok(QueryRecord::Attribute {
                request_id,
                outcome,
                net_entity_id: None,
                room_id: None,
                attribute_id: None,
                value: None,
                observed_revision: None,
                observed_tick: None,
                code: None,
                detail: None,
            })
        }
        "request_error" => {
            ensure_query_keys(
                index,
                object,
                &["type", "requestId", "outcome", "code", "detail"],
            )?;
            Ok(QueryRecord::Attribute {
                request_id,
                outcome,
                net_entity_id: None,
                room_id: None,
                attribute_id: None,
                value: None,
                observed_revision: None,
                observed_tick: None,
                code: Some(required_query_string(object, index, "code")?),
                detail: Some(required_query_string(object, index, "detail")?),
            })
        }
        other => Err(format!(
            "runtime query result {index} has invalid AttributeQueryResult outcome {other}"
        )),
    }
}

fn parse_binding_record(index: usize, value: Option<&Value>) -> Result<RuntimeBinding, String> {
    let object = value
        .and_then(Value::as_object)
        .ok_or_else(|| format!("runtime query result {index} is missing binding"))?;
    ensure_query_keys(
        index,
        object,
        &[
            "accountId",
            "roomId",
            "netEntityId",
            "entityType",
            "connectionGeneration",
        ],
    )?;
    let entity_type = match required_query_string(object, index, "entityType")?.as_str() {
        "player" => BoundEntityKind::Player,
        "bot" => BoundEntityKind::Bot,
        other => {
            return Err(format!(
                "runtime query result {index} has invalid entityType {other}"
            ))
        }
    };
    Ok(RuntimeBinding {
        account_id: required_query_string(object, index, "accountId")?,
        room_id: required_query_string(object, index, "roomId")?,
        net_entity_id: required_query_string(object, index, "netEntityId")?,
        entity_type,
        connection_generation: required_query_u64(object, index, "connectionGeneration")?,
    })
}

fn required_query_string(
    object: &serde_json::Map<String, Value>,
    index: usize,
    name: &str,
) -> Result<String, String> {
    object
        .get(name)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| format!("runtime query result {index} is missing or invalid {name}"))
}

fn required_query_u64(
    object: &serde_json::Map<String, Value>,
    index: usize,
    name: &str,
) -> Result<u64, String> {
    object
        .get(name)
        .and_then(Value::as_u64)
        .ok_or_else(|| format!("runtime query result {index} is missing or invalid {name}"))
}

fn ensure_query_keys(
    index: usize,
    object: &serde_json::Map<String, Value>,
    allowed: &[&str],
) -> Result<(), String> {
    if let Some(key) = object.keys().find(|key| !allowed.contains(&key.as_str())) {
        return Err(format!(
            "runtime query result {index} has unexpected field {key}"
        ));
    }
    Ok(())
}

fn query_error(code: Option<&str>, detail: Option<&str>) -> String {
    match (code, detail) {
        (Some(code), Some(detail)) => format!("{code}: {detail}"),
        (Some(code), None) => code.to_owned(),
        _ => "runtime_failure".to_owned(),
    }
}

fn ensure_tick_ok(tick: &Value, frames: &[RuntimeFrame]) -> Result<(), String> {
    if tick.get("ok").and_then(Value::as_bool) == Some(true) {
        return Ok(());
    }
    Err(tick
        .get("code")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .or_else(|| error_from_frames(frames))
        .unwrap_or_else(|| "runtime_failure".to_owned()))
}

fn correlated_query<'a>(
    queries: &'a [QueryRecord],
    request_id: &str,
    expected_type: &str,
) -> Result<&'a QueryRecord, String> {
    let matches: Vec<&QueryRecord> = queries
        .iter()
        .filter(|query| query.request_id() == request_id)
        .collect();
    if matches.is_empty() {
        return Err("runtime query result missing".to_owned());
    }
    if matches.len() != 1 {
        return Err("runtime query result has duplicate requestId".to_owned());
    }
    let result = matches[0];
    if result.type_name() != expected_type {
        return Err(format!(
            "runtime query result type mismatch: expected {expected_type}, got {}",
            result.type_name()
        ));
    }
    if queries.len() != 1 {
        return Err("runtime query result requestId mismatch".to_owned());
    }
    Ok(result)
}

fn optional_frame_string(
    object: &serde_json::Map<String, Value>,
    name: &str,
) -> Result<Option<String>, String> {
    let Some(value) = object.get(name) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    value
        .as_str()
        .map(str::to_owned)
        .map(Some)
        .ok_or_else(|| format!("runtime frame {name} must be a string"))
}

fn optional_frame_u64(
    object: &serde_json::Map<String, Value>,
    name: &str,
) -> Result<Option<u64>, String> {
    let Some(value) = object.get(name) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    value
        .as_u64()
        .map(Some)
        .ok_or_else(|| format!("runtime frame {name} must be an unsigned integer"))
}

fn error_from_frames(frames: &[RuntimeFrame]) -> Option<String> {
    frames.iter().find_map(|frame| {
        (frame.message_type.as_deref() == Some("Error")).then(|| {
            frame
                .code
                .clone()
                .unwrap_or_else(|| "runtime_failure".to_owned())
        })
    })
}

fn welcome_from_frames(frames: &[RuntimeFrame], connection: &str) -> Option<(String, u64)> {
    frames.iter().find_map(|frame| {
        (frame.connection.as_deref() == Some(connection)
            && frame.message_type.as_deref() == Some("Welcome"))
        .then(|| {
            Some((
                frame.observer_net_entity_id.clone()?,
                frame.connection_generation?,
            ))
        })
        .flatten()
    })
}

impl RuntimeSurface for ClrGameplay {
    fn admit(
        &mut self,
        connection: &str,
        account_id: &str,
        room_id: &str,
        entity_type: BoundEntityKind,
    ) -> RuntimeAdmit {
        let enqueue = json!({
            "op": "enqueue",
            "messageType": "AdmitConnectionMessage",
            "connection": connection,
            "accountId": account_id,
            "roomId": room_id,
            "entityType": entity_type.as_str(),
        });
        if let Err(code) = self.enqueue(enqueue) {
            return RuntimeAdmit::reject(&code);
        }
        let (tick, drain) = match self.tick_and_drain() {
            Ok(result) => result,
            Err(_) => return RuntimeAdmit::reject("runtime_failure"),
        };
        let RuntimeDrain { frames, queries } = drain;
        if !queries.is_empty() {
            return RuntimeAdmit::reject_with_frames("runtime_failure", frames);
        }
        if tick.get("ok").and_then(Value::as_bool) != Some(true) {
            let code = tick
                .get("code")
                .and_then(Value::as_str)
                .map(str::to_owned)
                .or_else(|| error_from_frames(&frames))
                .unwrap_or_else(|| "runtime_failure".to_owned());
            return RuntimeAdmit::reject_with_frames(&code, frames);
        }
        let Some((net_entity_id, generation)) = welcome_from_frames(&frames, connection) else {
            let code = error_from_frames(&frames).unwrap_or_else(|| "runtime_failure".to_owned());
            return RuntimeAdmit::reject_with_frames(&code, frames);
        };
        let binding = RuntimeBinding {
            account_id: account_id.to_owned(),
            room_id: room_id.to_owned(),
            net_entity_id,
            entity_type,
            connection_generation: generation,
        };
        let mut result = RuntimeAdmit::ok(binding);
        result.frames = frames;
        result
    }

    fn disconnect(
        &mut self,
        connection: &str,
        binding: &RuntimeBinding,
    ) -> Result<RuntimeDisconnect, String> {
        self.enqueue(json!({
            "op": "enqueue",
            "messageType": "DisconnectConnectionMessage",
            "connection": connection,
        }))?;
        let (tick, drain) = self.tick_and_drain()?;
        let RuntimeDrain { frames, queries } = drain;
        if !queries.is_empty() {
            return Err("unexpected runtime query result".to_owned());
        }
        if tick.get("ok").and_then(Value::as_bool) != Some(true) {
            let code = tick
                .get("code")
                .and_then(Value::as_str)
                .map(str::to_owned)
                .or_else(|| error_from_frames(&frames))
                .unwrap_or_else(|| "runtime_failure".to_owned());
            return Err(code);
        }
        Ok(RuntimeDisconnect {
            binding: binding.clone(),
            frames,
        })
    }

    fn rebind(
        &mut self,
        connection: &str,
        account_id: &str,
        room_id: &str,
        mode: RebindMode,
        entity_type: BoundEntityKind,
    ) -> RuntimeAdmit {
        let mode_text = match mode {
            RebindMode::Reconnect => "reconnect",
            RebindMode::Takeover => "takeover",
        };
        if let Err(code) = self.enqueue(json!({
            "op": "enqueue",
            "messageType": "RebindConnectionMessage",
            "connection": connection,
            "accountId": account_id,
            "roomId": room_id,
            "mode": mode_text,
        })) {
            return RuntimeAdmit::reject(&code);
        }
        let (tick, drain) = match self.tick_and_drain() {
            Ok(result) => result,
            Err(_) => return RuntimeAdmit::reject("runtime_failure"),
        };
        let RuntimeDrain { frames, queries } = drain;
        if !queries.is_empty() {
            return RuntimeAdmit::reject_with_frames("runtime_failure", frames);
        }
        if tick.get("ok").and_then(Value::as_bool) != Some(true) {
            let code = tick
                .get("code")
                .and_then(Value::as_str)
                .map(str::to_owned)
                .or_else(|| error_from_frames(&frames))
                .unwrap_or_else(|| "runtime_failure".to_owned());
            return RuntimeAdmit::reject_with_frames(&code, frames);
        }
        let Some((net_entity_id, generation)) = welcome_from_frames(&frames, connection) else {
            let code = error_from_frames(&frames).unwrap_or_else(|| "runtime_failure".to_owned());
            return RuntimeAdmit::reject_with_frames(&code, frames);
        };
        let binding = RuntimeBinding {
            account_id: account_id.to_owned(),
            room_id: room_id.to_owned(),
            net_entity_id,
            entity_type,
            connection_generation: generation,
        };
        let mut result = RuntimeAdmit::ok(binding);
        result.frames = frames;
        result
    }

    fn expire(
        &mut self,
        net_entity_id: &str,
    ) -> Result<RuntimeControlResult<()>, RuntimeControlError> {
        let request_id = self.next_request_id("expire");
        self.enqueue(json!({
            "op": "enqueue",
            "messageType": "ExpireEntityMessage",
            "requestId": request_id.clone(),
            "netEntityId": net_entity_id,
        }))?;
        let (tick, drain) = self.tick_and_drain()?;
        ensure_tick_ok(&tick, &drain.frames)?;
        let result = correlated_query(&drain.queries, &request_id, "ExpireEntityResult")?.clone();
        match result {
            QueryRecord::Expire { outcome, .. } if outcome != "request_error" => {
                Ok(RuntimeControlResult::new((), drain.frames))
            }
            QueryRecord::Expire { code, detail, .. } => Err(RuntimeControlError::new(
                query_error(code.as_deref(), detail.as_deref()),
                drain.frames,
            )),
            _ => Err(RuntimeControlError::new(
                "runtime query result type mismatch".to_owned(),
                drain.frames,
            )),
        }
    }

    fn resolve_by_net_entity_id(
        &mut self,
        room_id: &str,
        net_entity_id: &str,
    ) -> Result<RuntimeControlResult<Option<RuntimeBinding>>, RuntimeControlError> {
        let request_id = self.next_request_id("resolve");
        let result = self
            .enqueue(json!({
                "op": "enqueue",
                "messageType": "ResolveBindingMessage",
                "requestId": request_id.clone(),
                "roomId": room_id,
                "netEntityId": net_entity_id,
            }))
            .and_then(|()| self.tick_and_drain())
            .and_then(|(tick, drain)| {
                ensure_tick_ok(&tick, &drain.frames)?;
                let record =
                    correlated_query(&drain.queries, &request_id, "ResolveBindingResult")?.clone();
                Ok((record, drain.frames))
            });
        let (record, frames) = result?;
        match record {
            QueryRecord::Resolve {
                outcome,
                binding: Some(binding),
                ..
            } if outcome == "ok" => {
                if binding.room_id != room_id || binding.net_entity_id != net_entity_id {
                    return Err(RuntimeControlError::new(
                        "runtime query result request mismatch".to_owned(),
                        frames,
                    ));
                }
                Ok(RuntimeControlResult::new(Some(binding), frames))
            }
            QueryRecord::Resolve {
                outcome,
                code,
                detail,
                ..
            } if outcome == "request_error" => Err(RuntimeControlError::new(
                query_error(code.as_deref(), detail.as_deref()),
                frames,
            )),
            QueryRecord::Resolve { .. } => Ok(RuntimeControlResult::new(None, frames)),
            _ => Err(RuntimeControlError::new(
                "runtime query result type mismatch".to_owned(),
                frames,
            )),
        }
    }

    fn query_attribute(
        &mut self,
        request: &RuntimeQuery,
    ) -> Result<RuntimeControlResult<QueryResult>, RuntimeControlError> {
        let request_id = self.next_request_id("attribute");
        let mut message = json!({
            "op": "enqueue",
            "messageType": "AttributeQueryMessage",
            "requestId": request_id.clone(),
            "callerScope": request.caller_scope.as_runtime_str(),
            "roomId": request.room_id,
            "netEntityId": request.net_entity_id,
            "attributeId": request.attribute_id,
        });
        if let Some(generation) = request.connection_generation {
            message["connectionGeneration"] = json!(generation);
        }
        let result = self
            .enqueue(message)
            .and_then(|()| self.tick_and_drain())
            .and_then(|(tick, drain)| {
                ensure_tick_ok(&tick, &drain.frames)?;
                let record =
                    correlated_query(&drain.queries, &request_id, "AttributeQueryResult")?.clone();
                Ok((record, drain.frames))
            });
        let (record, frames) = result?;
        let value = match record {
            QueryRecord::Attribute {
                outcome,
                net_entity_id,
                room_id,
                attribute_id,
                value,
                observed_tick,
                observed_revision,
                ..
            } if outcome == "ok" => {
                if net_entity_id.as_deref() != Some(request.net_entity_id.as_str())
                    || room_id.as_deref() != Some(request.room_id.as_str())
                    || attribute_id.as_deref() != Some(request.attribute_id.as_str())
                {
                    return Err(RuntimeControlError::new(
                        "runtime query result request mismatch".to_owned(),
                        frames,
                    ));
                }
                QueryResult::ok(
                    value.unwrap_or_default(),
                    observed_tick.unwrap_or_default(),
                    observed_revision.unwrap_or_default(),
                )
            }
            QueryRecord::Attribute { outcome, code, .. } => {
                QueryResult::from_runtime(&outcome, code.as_deref(), None)
            }
            _ => {
                return Err(RuntimeControlError::new(
                    "runtime query result type mismatch".to_owned(),
                    frames,
                ))
            }
        };
        Ok(RuntimeControlResult::new(value, frames))
    }

    fn attach_member(&mut self, room_id: &str, connection: &str) -> Result<(), String> {
        let _ = (room_id, connection);
        Ok(())
    }

    fn admit_input_command(
        &mut self,
        _room_id: &str,
        connection: &str,
        _generation: u64,
        net_entity_id: &str,
        envelope_bytes: &[u8],
    ) -> ChatOperation {
        if let Err(code) = self.enqueue(json!({
            "op": "enqueue",
            "messageType": "InputCommandMessage",
            "senderNetEntityId": net_entity_id,
            "connection": connection,
            "envelopeBase64": base64_encode(envelope_bytes),
        })) {
            return ChatOperation::rejected(&code);
        }
        ChatOperation::admitted()
    }

    fn run_tick(&mut self, room_id: &str, tick_id: u64) -> RuntimeTick {
        let _ = (room_id, tick_id);
        let (value, drain) = match self.tick_and_drain() {
            Ok(result) => result,
            Err(_) => return RuntimeTick::failed("runtime_failure"),
        };
        if !drain.queries.is_empty() {
            return RuntimeTick::failed("unexpected_query_result");
        }
        let mut tick = match tick_from_hostentry_json(value) {
            Ok(tick) => tick,
            Err(_) => return RuntimeTick::failed("runtime_failure"),
        };
        tick.frames = drain.frames;
        if let Some(code) = error_from_frames(&tick.frames) {
            tick.ok = false;
            tick.code = Some(code);
        }
        tick
    }

    fn persist(&mut self, room_id: &str) -> Result<PersistRecord, String> {
        let value = self.call(json!({ "op": "snapshot", "roomId": room_id }))?;
        persist_record_from_hostentry_json(&value)
    }

    fn restore(&mut self, room_id: &str, bytes: &[u8]) -> Result<(), String> {
        let value = self.call(json!({
            "op": "restore",
            "roomId": room_id,
            "bytesBase64": base64_encode(bytes),
        }))?;
        restore_result_from_hostentry_json(&value)
    }
}

fn persist_record_from_hostentry_json(value: &Value) -> Result<PersistRecord, String> {
    if value.get("ok").and_then(Value::as_bool) != Some(true) {
        return Err(value
            .get("code")
            .and_then(Value::as_str)
            .unwrap_or("snapshot_failed")
            .to_owned());
    }
    let encoded = value
        .get("bytesBase64")
        .and_then(Value::as_str)
        .ok_or_else(|| "snapshot response missing bytesBase64".to_owned())?;
    let bytes =
        decode_base64(encoded).ok_or_else(|| "snapshot bytesBase64 is malformed".to_owned())?;
    Ok(PersistRecord { bytes })
}

fn restore_result_from_hostentry_json(value: &Value) -> Result<(), String> {
    if value.get("ok").and_then(Value::as_bool) == Some(true) {
        Ok(())
    } else {
        Err(value
            .get("code")
            .and_then(Value::as_str)
            .unwrap_or("restore_failed")
            .to_owned())
    }
}

fn decode_base64(value: &str) -> Option<Vec<u8>> {
    let bytes = value.as_bytes();
    if bytes.is_empty() || !bytes.len().is_multiple_of(4) {
        return None;
    }
    let mut out = Vec::with_capacity(bytes.len() / 4 * 3);
    let sextet = |byte: u8| -> Option<u8> {
        match byte {
            b'A'..=b'Z' => Some(byte - b'A'),
            b'a'..=b'z' => Some(byte - b'a' + 26),
            b'0'..=b'9' => Some(byte - b'0' + 52),
            b'+' => Some(62),
            b'/' => Some(63),
            _ => None,
        }
    };
    for (index, chunk) in bytes.chunks_exact(4).enumerate() {
        let last = index + 1 == bytes.len() / 4;
        if !last && (chunk[2] == b'=' || chunk[3] == b'=') {
            return None;
        }
        if chunk[2] == b'=' && chunk[3] != b'=' {
            return None;
        }
        let a = sextet(chunk[0])?;
        let b = sextet(chunk[1])?;
        let c = if chunk[2] == b'=' {
            0
        } else {
            sextet(chunk[2])?
        };
        let d = if chunk[3] == b'=' {
            0
        } else {
            sextet(chunk[3])?
        };
        if chunk[2] == b'=' && (b & 0x0f) != 0 {
            return None;
        }
        if chunk[3] == b'=' && chunk[2] != b'=' && (c & 0x03) != 0 {
            return None;
        }
        out.push((a << 2) | (b >> 4));
        if chunk[2] != b'=' {
            out.push((b << 4) | (c >> 2));
        }
        if chunk[3] != b'=' {
            out.push((c << 6) | d);
        }
    }
    Some(out)
}

fn base64_encode(bytes: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let a = chunk[0];
        let b = chunk.get(1).copied().unwrap_or(0);
        let c = chunk.get(2).copied().unwrap_or(0);
        out.push(char::from(TABLE[(a >> 2) as usize]));
        out.push(char::from(TABLE[((a << 4 | b >> 4) & 0x3f) as usize]));
        out.push(if chunk.len() > 1 {
            char::from(TABLE[((b << 2 | c >> 6) & 0x3f) as usize])
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            char::from(TABLE[(c & 0x3f) as usize])
        } else {
            '='
        });
    }
    out
}

/// HostEntry `tick` JSON: `ok:false` is a failed tick even if appliedTick >= 1.
pub(crate) fn tick_from_hostentry_json(value: Value) -> Result<RuntimeTick, String> {
    let frames = frames_from_runtime(&value)?;
    let event_count = value.get("eventCount").and_then(Value::as_u64).unwrap_or(0);
    let code = value
        .get("code")
        .and_then(Value::as_str)
        .filter(|code| !code.is_empty())
        .map(str::to_owned);
    if value.get("ok").and_then(Value::as_bool) != Some(true) {
        return Ok(RuntimeTick {
            applied_tick: 0,
            revision: value.get("revision").and_then(Value::as_u64).unwrap_or(0),
            ok: false,
            event_count: 0,
            code: code.or_else(|| Some("runtime_failure".to_owned())),
            frames,
        });
    }
    let mut tick = RuntimeTick::committed(
        value
            .get("appliedTick")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        value.get("revision").and_then(Value::as_u64).unwrap_or(0),
        event_count,
    );
    tick.frames = frames;
    Ok(tick)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn budget_fault_tick_is_not_success_even_when_applied_tick_is_one() {
        let tick = tick_from_hostentry_json(json!({
            "ok": false,
            "appliedTick": 1,
            "revision": 1,
            "eventCount": 0,
            "code": "runtime_failure"
        }))
        .expect("well-formed tick response");
        assert!(!tick.ok);
        assert_eq!(tick.applied_tick, 0);
        assert_eq!(tick.event_count, 0);
        assert_eq!(tick.code.as_deref(), Some("runtime_failure"));
    }

    #[test]
    fn runtime_frame_base64_round_trips_binary_wire_bytes() {
        let bytes = [0_u8, 1, 2, 253, 254, 255];
        assert_eq!(
            decode_base64(&base64_encode(&bytes)).as_deref(),
            Some(bytes.as_slice())
        );
    }

    #[test]
    fn runtime_frames_preserve_connection_association() {
        let encoded = base64_encode(b"wire");
        let value = json!({
            "frames": [{
                "connection": "c1",
                "bytesBase64": encoded,
                "messageType": "Welcome",
                "observerNetEntityId": "00000000000000010000000000000001",
                "connectionGeneration": 2
            }],
            "ok": true
        });
        let frame = &frames_from_runtime(&value).expect("well-formed frame")[0];
        assert_eq!(frame.connection.as_deref(), Some("c1"));
        assert_eq!(frame.bytes, b"wire");
        assert_eq!(
            frame.observer_net_entity_id.as_deref(),
            Some("00000000000000010000000000000001")
        );
        assert_eq!(frame.connection_generation, Some(2));
        assert_eq!(frame.message_type.as_deref(), Some("Welcome"));
        assert_eq!(
            frames_from_runtime(&json!({ "frames": [{ "bytesBase64": "bad" }] }))
                .expect_err("malformed Runtime frame must fail the bridge"),
            "runtime frame bytesBase64 is malformed"
        );
    }

    #[test]
    fn malformed_runtime_frame_metadata_fails_instead_of_being_dropped() {
        let value = json!({
            "frames": [{
                "bytesBase64": base64_encode(b"wire"),
                "connectionGeneration": "not-a-number"
            }]
        });
        assert_eq!(
            frames_from_runtime(&value).expect_err("malformed metadata must fail the bridge"),
            "runtime frame connectionGeneration must be an unsigned integer"
        );
    }

    #[test]
    fn strict_query_parser_accepts_correlated_c2_records() {
        let value = json!({
            "ok": true,
            "frames": [],
            "queries": [
                {
                    "type": "ExpireEntityResult",
                    "requestId": "expire-1",
                    "outcome": "tombstoned"
                },
                {
                    "type": "ResolveBindingResult",
                    "requestId": "resolve-1",
                    "outcome": "ok",
                    "binding": {
                        "accountId": "acct-1",
                        "roomId": "room-1",
                        "netEntityId": "00000000000000010000000000000001",
                        "entityType": "bot",
                        "connectionGeneration": 2
                    },
                    "observedRevision": 9
                },
                {
                    "type": "AttributeQueryResult",
                    "requestId": "attribute-1",
                    "outcome": "ok",
                    "netEntityId": "00000000000000010000000000000001",
                    "roomId": "room-1",
                    "attributeId": "EntityIdentity.entityType",
                    "value": "bot",
                    "observedRevision": 9,
                    "observedTick": 4
                }
            ]
        });
        let drain = parse_drain_response(&value).expect("valid C-2 records");
        assert_eq!(drain.queries.len(), 3);
        assert_eq!(drain.queries[1].request_id(), "resolve-1");
        assert_eq!(drain.queries[2].type_name(), "AttributeQueryResult");
    }

    #[test]
    fn strict_query_parser_rejects_malformed_missing_and_mismatched_records() {
        let malformed = json!({
            "queries": [{
                "type": "AttributeQueryResult",
                "requestId": "q-1",
                "outcome": "ok",
                "netEntityId": "n",
                "roomId": "r",
                "attributeId": "A.b",
                "value": "x",
                "observedRevision": 1,
                "observedTick": "bad"
            }]
        });
        assert!(parse_query_records(&malformed)
            .expect_err("wrong observedTick type")
            .contains("observedTick"));

        let missing = json!({ "ok": true, "frames": [] });
        assert_eq!(
            parse_drain_response(&missing).expect_err("missing query array"),
            "runtime drain response missing queries array"
        );

        let wrong_outcome = json!({
            "queries": [{
                "type": "ResolveBindingResult",
                "requestId": "q-1",
                "outcome": "accepted"
            }]
        });
        assert!(parse_query_records(&wrong_outcome)
            .expect_err("wrong result outcome")
            .contains("ResolveBindingResult outcome"));

        let duplicate = json!({
            "queries": [
                {"type": "ExpireEntityResult", "requestId": "q-1", "outcome": "accepted"},
                {"type": "ExpireEntityResult", "requestId": "q-1", "outcome": "tombstoned"}
            ]
        });
        let rows = parse_query_records(&duplicate).expect("shape-valid duplicate");
        assert!(correlated_query(&rows, "q-1", "ExpireEntityResult")
            .expect_err("duplicate correlation")
            .contains("duplicate"));
        assert!(correlated_query(&rows, "missing", "ExpireEntityResult")
            .expect_err("missing correlation")
            .contains("missing"));
    }

    #[test]
    fn c2_request_errors_require_code_and_detail() {
        let missing_detail = json!({
            "queries": [{
                "type": "AttributeQueryResult",
                "requestId": "q-1",
                "outcome": "request_error",
                "code": "undeclared_attribute"
            }]
        });
        assert!(parse_query_records(&missing_detail)
            .expect_err("request_error detail is required")
            .contains("detail"));
    }

    #[test]
    fn drain_response_requires_ok_frames_and_queries() {
        for (value, expected) in [
            (
                json!({ "ok": false, "frames": [], "queries": [] }),
                "not ok",
            ),
            (json!({ "ok": true, "queries": [] }), "missing frames"),
            (json!({ "ok": true, "frames": [] }), "missing queries"),
        ] {
            assert!(
                parse_drain_response(&value)
                    .expect_err("incomplete drain response")
                    .contains(expected),
                "{value}"
            );
        }
    }

    #[test]
    fn snapshot_response_preserves_call_and_decode_failures() {
        assert_eq!(
            persist_record_from_hostentry_json(&json!({
                "ok": false,
                "code": "snapshot_failed"
            })),
            Err("snapshot_failed".to_owned())
        );
        assert_eq!(
            persist_record_from_hostentry_json(&json!({ "ok": true })),
            Err("snapshot response missing bytesBase64".to_owned())
        );
        assert_eq!(
            persist_record_from_hostentry_json(&json!({
                "ok": true,
                "bytesBase64": "bad"
            })),
            Err("snapshot bytesBase64 is malformed".to_owned())
        );
    }

    #[test]
    fn restore_response_preserves_runtime_error_code() {
        assert_eq!(
            restore_result_from_hostentry_json(&json!({
                "ok": false,
                "code": "restore_rejected"
            })),
            Err("restore_rejected".to_owned())
        );
    }
}
