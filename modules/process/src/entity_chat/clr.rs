//! CoreCLR host of Runtime EntityBindingQuery + ChatCommandRuntime + Persist.

use std::path::PathBuf;

use serde_json::{json, Value};

use crate::runtime_bridge::{BridgeError, ClrBridge, ClrStart};
use crate::sdk_loader;

use super::runtime::BoundEntityKind;
use super::runtime::{
    ChatOperation, PersistRecord, QueryResult, RebindMode, RuntimeAdmit, RuntimeBinding,
    RuntimeDisconnect, RuntimeFrame, RuntimeQuery, RuntimeSurface, RuntimeTick,
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
}

/// CoreCLR-backed [`RuntimeSurface`].
pub struct ClrGameplay {
    bridge: ClrBridge,
    replication_assembly: String,
    ecs_assembly: String,
    booted: bool,
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
            booted: false,
        })
    }

    fn call(&mut self, request: Value) -> Result<Value, String> {
        if !self.booted {
            let boot = json!({
                "op": "boot",
                "replicationAssembly": self.replication_assembly,
                "ecsAssembly": self.ecs_assembly,
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
}

fn bridge_err(error: BridgeError) -> String {
    match error {
        BridgeError::Rejected { code } => code.as_str().to_owned(),
        BridgeError::Failed { detail } => detail.to_owned(),
    }
}

fn kind_from(value: Option<&str>) -> BoundEntityKind {
    match value {
        Some("bot") => BoundEntityKind::Bot,
        _ => BoundEntityKind::Player,
    }
}

fn binding_from(value: &Value) -> Option<RuntimeBinding> {
    Some(RuntimeBinding {
        account_id: value.get("accountId")?.as_str()?.to_owned(),
        room_id: value.get("roomId")?.as_str()?.to_owned(),
        net_entity_id: value.get("netEntityId")?.as_str()?.to_owned(),
        entity_type: kind_from(value.get("entityType").and_then(Value::as_str)),
        connection_generation: value.get("connectionGeneration")?.as_u64()?,
    })
}

fn binding_from_entity(value: &Value, room_id: &str) -> Option<RuntimeBinding> {
    if value.get("ok").and_then(Value::as_bool) != Some(true) {
        return None;
    }
    Some(RuntimeBinding {
        account_id: value
            .get("accountId")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned(),
        room_id: value
            .get("roomId")
            .and_then(Value::as_str)
            .unwrap_or(room_id)
            .to_owned(),
        net_entity_id: value.get("netEntityId")?.as_str()?.to_owned(),
        entity_type: kind_from(
            value
                .get("entityType")
                .and_then(Value::as_str)
                .or_else(|| value.get("value").and_then(Value::as_str)),
        ),
        connection_generation: value
            .get("connectionGeneration")
            .and_then(Value::as_u64)
            .unwrap_or(0),
    })
}

fn frames_from_runtime(value: &Value) -> Vec<RuntimeFrame> {
    value
        .get("frames")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|row| {
            let bytes = row
                .get("bytesBase64")
                .and_then(Value::as_str)
                .and_then(decode_base64)?;
            Some(RuntimeFrame {
                connection: row
                    .get("connection")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                bytes,
            })
        })
        .collect()
}

fn admit_from(value: Value) -> RuntimeAdmit {
    let frames = frames_from_runtime(&value);
    if value.get("ok").and_then(Value::as_bool) == Some(true) {
        if let Some(binding) = value.get("binding").and_then(binding_from) {
            let mut result = RuntimeAdmit::ok(binding);
            result.frames = frames;
            return result;
        }
    }
    let mut result = RuntimeAdmit::reject(
        value
            .get("code")
            .and_then(Value::as_str)
            .unwrap_or("invalid_request"),
    );
    result.frames = frames;
    result
}

impl RuntimeSurface for ClrGameplay {
    fn admit(
        &mut self,
        connection: &str,
        account_id: &str,
        room_id: &str,
        entity_type: BoundEntityKind,
    ) -> RuntimeAdmit {
        match self.call(json!({
            "op": "admit",
            "connection": connection,
            "accountId": account_id,
            "roomId": room_id,
            "entityType": entity_type.as_str(),
        })) {
            Ok(value) => admit_from(value),
            Err(_) => RuntimeAdmit::reject("runtime_failure"),
        }
    }

    fn disconnect(&mut self, connection: &str) -> Result<RuntimeDisconnect, String> {
        let value = self.call(json!({ "op": "disconnect", "connection": connection }))?;
        let binding = value
            .get("binding")
            .and_then(binding_from)
            .ok_or_else(|| "binding_not_found".to_owned())?;
        Ok(RuntimeDisconnect {
            binding,
            frames: frames_from_runtime(&value),
        })
    }

    fn rebind(
        &mut self,
        connection: &str,
        account_id: &str,
        room_id: &str,
        mode: RebindMode,
    ) -> RuntimeAdmit {
        let mode = match mode {
            RebindMode::Reconnect => "reconnect",
            RebindMode::Takeover => "takeover",
        };
        match self.call(json!({
            "op": "rebind",
            "connection": connection,
            "accountId": account_id,
            "roomId": room_id,
            "mode": mode,
        })) {
            Ok(value) => admit_from(value),
            Err(_) => RuntimeAdmit::reject("runtime_failure"),
        }
    }

    fn expire(&mut self, net_entity_id: &str) -> Result<(), String> {
        let value = self.call(json!({ "op": "expire", "netEntityId": net_entity_id }))?;
        if value.get("ok").and_then(Value::as_bool) == Some(true) {
            Ok(())
        } else {
            Err(value
                .get("code")
                .and_then(Value::as_str)
                .unwrap_or("invalid_request")
                .to_owned())
        }
    }

    fn self_lookup(&mut self, connection: &str) -> Option<RuntimeBinding> {
        self.call(json!({ "op": "self_lookup", "connection": connection }))
            .ok()
            .and_then(|value| value.get("binding").and_then(binding_from))
    }

    fn resolve_by_net_entity_id(
        &mut self,
        room_id: &str,
        net_entity_id: &str,
    ) -> Option<RuntimeBinding> {
        self.call(json!({ "op": "resolve", "roomId": room_id, "netEntityId": net_entity_id }))
            .ok()
            .and_then(|value| binding_from_entity(&value, room_id))
    }

    fn query_attribute(&mut self, request: &RuntimeQuery) -> QueryResult {
        match self.call(json!({
            "op": "query",
            "callerScope": request.caller_scope.as_runtime_str(),
            "roomId": request.room_id,
            "netEntityId": request.net_entity_id,
            "attributeId": request.attribute_id,
            "connectionGeneration": request.connection_generation,
        })) {
            Ok(value) => QueryResult::from_runtime(
                value
                    .get("outcome")
                    .and_then(Value::as_str)
                    .unwrap_or("request_error"),
                value.get("code").and_then(Value::as_str),
                value
                    .get("value")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
            ),
            Err(_) => QueryResult::request_error("runtime_failure"),
        }
    }

    fn list_bindings(&mut self, room_id: &str) -> Vec<RuntimeBinding> {
        let ids = self
            .call(json!({ "op": "live_ids" }))
            .ok()
            .and_then(|value| value.get("ids").and_then(Value::as_array).cloned())
            .unwrap_or_default()
            .iter()
            .filter_map(Value::as_str)
            .filter_map(|id| {
                self.call(json!({ "op": "resolve", "roomId": room_id, "netEntityId": id }))
                    .ok()
                    .and_then(|value| binding_from_entity(&value, room_id))
            })
            .collect();
        ids
    }

    fn attach_member(&mut self, room_id: &str, connection: &str) -> Result<(), String> {
        let value = self.call(json!({
            "op": "attach_member",
            "roomId": room_id,
            "connection": connection
        }))?;
        if value.get("ok").and_then(Value::as_bool) == Some(true) {
            Ok(())
        } else {
            Err("runtime_failure".to_owned())
        }
    }

    fn admit_input_command(
        &mut self,
        room_id: &str,
        connection: &str,
        generation: u64,
        envelope_bytes: &[u8],
    ) -> ChatOperation {
        match self.call(json!({
            "op": "admit_input",
            "roomId": room_id,
            "connection": connection,
            "connectionGeneration": generation,
            "envelopeBase64": base64_encode(envelope_bytes),
        })) {
            Ok(value) if value.get("ok").and_then(Value::as_bool) == Some(true) => {
                ChatOperation::admitted()
            }
            Ok(value) => ChatOperation::rejected(
                value
                    .get("code")
                    .and_then(Value::as_str)
                    .unwrap_or("invalid_request"),
            ),
            Err(_) => ChatOperation::rejected("runtime_failure"),
        }
    }

    fn run_tick(&mut self, room_id: &str, tick_id: u64) -> RuntimeTick {
        match self.call(json!({ "op": "tick", "roomId": room_id, "tickId": tick_id })) {
            Ok(value) => tick_from_hostentry_json(value),
            Err(_) => RuntimeTick::failed("runtime_failure"),
        }
    }

    fn persist(&mut self, room_id: &str) -> PersistRecord {
        let hex = self
            .call(json!({ "op": "snapshot", "roomId": room_id }))
            .ok()
            .and_then(|value| {
                value
                    .get("bytesBase64")
                    .and_then(Value::as_str)
                    .and_then(decode_base64)
            })
            .unwrap_or_default();
        PersistRecord { bytes: hex }
    }

    fn restore(&mut self, room_id: &str, bytes: &[u8]) -> Result<(), String> {
        let value = self.call(json!({
            "op": "restore",
            "roomId": room_id,
            "bytesBase64": base64_encode(bytes),
        }))?;
        if value.get("ok").and_then(Value::as_bool) == Some(true) {
            Ok(())
        } else {
            Err("restore_failed".to_owned())
        }
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
    for chunk in bytes.chunks_exact(4) {
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
pub(crate) fn tick_from_hostentry_json(value: Value) -> RuntimeTick {
    let event_count = value.get("eventCount").and_then(Value::as_u64).unwrap_or(0);
    let code = value
        .get("code")
        .and_then(Value::as_str)
        .filter(|code| !code.is_empty())
        .map(str::to_owned);
    if value.get("ok").and_then(Value::as_bool) != Some(true) {
        return RuntimeTick {
            applied_tick: 0,
            revision: value.get("revision").and_then(Value::as_u64).unwrap_or(0),
            ok: false,
            event_count: 0,
            code: code.or_else(|| Some("runtime_failure".to_owned())),
            frames: frames_from_runtime(&value),
        };
    }
    let mut tick = RuntimeTick::committed(
        value
            .get("appliedTick")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        value.get("revision").and_then(Value::as_u64).unwrap_or(0),
        event_count,
    );
    tick.frames = frames_from_runtime(&value);
    tick
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
        }));
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
        let value =
            json!({ "frames": [{ "connection": "c1", "bytesBase64": encoded }], "ok": true });
        assert_eq!(
            frames_from_runtime(&value)[0].connection.as_deref(),
            Some("c1")
        );
        assert_eq!(frames_from_runtime(&value)[0].bytes, b"wire");
        assert!(frames_from_runtime(&json!({ "frames": [{ "bytesBase64": "bad" }] })).is_empty());
    }
}
