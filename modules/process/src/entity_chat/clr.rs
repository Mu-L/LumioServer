//! CoreCLR consume host for Runtime WorldManager and opaque C-1 persistence.

use std::collections::HashMap;
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
    bindings: HashMap<String, RuntimeBinding>,
    retained: HashMap<String, RuntimeBinding>,
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
            bindings: HashMap::new(),
            retained: HashMap::new(),
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

    fn tick_and_drain(&mut self) -> Result<(Value, Vec<RuntimeFrame>), String> {
        let tick = self.call(json!({ "op": "tick" }))?;
        let drain = self.call(json!({ "op": "drain" }))?;
        Ok((tick, frames_from_runtime(&drain)))
    }
}

fn bridge_err(error: BridgeError) -> String {
    match error {
        BridgeError::Rejected { code } => code.as_str().to_owned(),
        BridgeError::Failed { detail } => detail.to_owned(),
    }
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
                observer_net_entity_id: row
                    .get("observerNetEntityId")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                connection_generation: row.get("connectionGeneration").and_then(Value::as_u64),
                message_type: row
                    .get("messageType")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                code: row.get("code").and_then(Value::as_str).map(str::to_owned),
            })
        })
        .collect()
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
        let (tick, frames) = match self.tick_and_drain() {
            Ok(result) => result,
            Err(_) => return RuntimeAdmit::reject("runtime_failure"),
        };
        if tick.get("ok").and_then(Value::as_bool) != Some(true) {
            let code = tick
                .get("code")
                .and_then(Value::as_str)
                .map(str::to_owned)
                .or_else(|| error_from_frames(&frames))
                .unwrap_or_else(|| "runtime_failure".to_owned());
            return RuntimeAdmit::reject(&code);
        }
        let Some((net_entity_id, generation)) = welcome_from_frames(&frames, connection) else {
            return RuntimeAdmit::reject(
                error_from_frames(&frames)
                    .as_deref()
                    .unwrap_or("runtime_failure"),
            );
        };
        let binding = RuntimeBinding {
            account_id: account_id.to_owned(),
            room_id: room_id.to_owned(),
            net_entity_id,
            entity_type,
            connection_generation: generation,
        };
        self.bindings.insert(connection.to_owned(), binding.clone());
        self.retained.remove(account_id);
        let mut result = RuntimeAdmit::ok(binding);
        result.frames = frames;
        result
    }

    fn disconnect(&mut self, connection: &str) -> Result<RuntimeDisconnect, String> {
        let binding = self
            .bindings
            .get(connection)
            .cloned()
            .ok_or_else(|| "binding_not_found".to_owned())?;
        self.enqueue(json!({
            "op": "enqueue",
            "messageType": "DisconnectConnectionMessage",
            "connection": connection,
        }))?;
        let (tick, frames) = self.tick_and_drain()?;
        if tick.get("ok").and_then(Value::as_bool) != Some(true) {
            let code = tick
                .get("code")
                .and_then(Value::as_str)
                .map(str::to_owned)
                .or_else(|| error_from_frames(&frames))
                .unwrap_or_else(|| "runtime_failure".to_owned());
            return Err(code);
        }
        self.bindings.remove(connection);
        self.retained
            .insert(binding.account_id.clone(), binding.clone());
        Ok(RuntimeDisconnect { binding, frames })
    }

    fn rebind(
        &mut self,
        connection: &str,
        account_id: &str,
        room_id: &str,
        mode: RebindMode,
    ) -> RuntimeAdmit {
        let mode_text = match mode {
            RebindMode::Reconnect => "reconnect",
            RebindMode::Takeover => "takeover",
        };
        let previous = self
            .bindings
            .iter()
            .find(|(_, binding)| binding.account_id == account_id)
            .map(|(connection, binding)| (connection.clone(), binding.clone()))
            .or_else(|| {
                self.retained
                    .get(account_id)
                    .map(|binding| (String::new(), binding.clone()))
            });
        let entity_type = previous
            .as_ref()
            .map(|(_, binding)| binding.entity_type)
            .unwrap_or(BoundEntityKind::Player);
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
        let (tick, frames) = match self.tick_and_drain() {
            Ok(result) => result,
            Err(_) => return RuntimeAdmit::reject("runtime_failure"),
        };
        if tick.get("ok").and_then(Value::as_bool) != Some(true) {
            let code = tick
                .get("code")
                .and_then(Value::as_str)
                .map(str::to_owned)
                .or_else(|| error_from_frames(&frames))
                .unwrap_or_else(|| "runtime_failure".to_owned());
            return RuntimeAdmit::reject(&code);
        }
        let Some((net_entity_id, generation)) = welcome_from_frames(&frames, connection) else {
            return RuntimeAdmit::reject(
                error_from_frames(&frames)
                    .as_deref()
                    .unwrap_or("runtime_failure"),
            );
        };
        let binding = RuntimeBinding {
            account_id: account_id.to_owned(),
            room_id: room_id.to_owned(),
            net_entity_id,
            entity_type,
            connection_generation: generation,
        };
        if let Some((old_connection, _)) = previous {
            if !old_connection.is_empty() && old_connection != connection {
                self.bindings.remove(&old_connection);
            }
        }
        self.retained.remove(account_id);
        self.bindings.insert(connection.to_owned(), binding.clone());
        let mut result = RuntimeAdmit::ok(binding);
        result.frames = frames;
        result
    }

    fn expire(&mut self, net_entity_id: &str) -> Result<(), String> {
        let _ = net_entity_id;
        Err("unsupported_runtime_op".to_owned())
    }

    fn self_lookup(&mut self, connection: &str) -> Option<RuntimeBinding> {
        self.bindings.get(connection).cloned()
    }

    fn resolve_by_net_entity_id(
        &mut self,
        room_id: &str,
        net_entity_id: &str,
    ) -> Option<RuntimeBinding> {
        self.bindings
            .values()
            .find(|binding| binding.room_id == room_id && binding.net_entity_id == net_entity_id)
            .cloned()
    }

    fn query_attribute(&mut self, request: &RuntimeQuery) -> QueryResult {
        let Some(binding) = self.bindings.values().find(|binding| {
            binding.room_id == request.room_id && binding.net_entity_id == request.net_entity_id
        }) else {
            return QueryResult::fail(super::runtime::AttributeQueryOutcome::NonExistent);
        };
        if request.attribute_id == "EntityIdentity.entityType" {
            return QueryResult::ok(binding.entity_type.as_str().to_owned(), 0, 0);
        }
        if request.caller_scope == super::runtime::AttributeQueryScope::ClientReplica
            && request.attribute_id == "EntityIdentity.claimedMark"
        {
            return QueryResult::fail(super::runtime::AttributeQueryOutcome::Unauthorized);
        }
        QueryResult::fail(super::runtime::AttributeQueryOutcome::Invisible)
    }

    fn list_bindings(&mut self, room_id: &str) -> Vec<RuntimeBinding> {
        self.bindings
            .values()
            .filter(|binding| binding.room_id == room_id)
            .cloned()
            .collect()
    }

    fn attach_member(&mut self, room_id: &str, connection: &str) -> Result<(), String> {
        let _ = (room_id, connection);
        Ok(())
    }

    fn admit_input_command(
        &mut self,
        room_id: &str,
        connection: &str,
        generation: u64,
        envelope_bytes: &[u8],
    ) -> ChatOperation {
        let Some(binding) = self.bindings.get(connection).cloned() else {
            return ChatOperation::rejected("disconnected");
        };
        if binding.room_id != room_id || binding.connection_generation != generation {
            return ChatOperation::rejected("stale_generation");
        }
        if let Err(code) = self.enqueue(json!({
            "op": "enqueue",
            "messageType": "InputCommandMessage",
            "senderNetEntityId": binding.net_entity_id,
            "connection": connection,
            "envelopeBase64": base64_encode(envelope_bytes),
        })) {
            return ChatOperation::rejected(&code);
        }
        ChatOperation::admitted()
    }

    fn run_tick(&mut self, room_id: &str, tick_id: u64) -> RuntimeTick {
        let _ = (room_id, tick_id);
        let (value, frames) = match self.tick_and_drain() {
            Ok(result) => result,
            Err(_) => return RuntimeTick::failed("runtime_failure"),
        };
        let mut tick = tick_from_hostentry_json(value);
        tick.frames = frames;
        if let Some(code) = error_from_frames(&tick.frames) {
            tick.ok = false;
            tick.code = Some(code);
        }
        tick
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
        let frame = &frames_from_runtime(&value)[0];
        assert_eq!(frame.connection.as_deref(), Some("c1"));
        assert_eq!(frame.bytes, b"wire");
        assert_eq!(
            frame.observer_net_entity_id.as_deref(),
            Some("00000000000000010000000000000001")
        );
        assert_eq!(frame.connection_generation, Some(2));
        assert_eq!(frame.message_type.as_deref(), Some("Welcome"));
        assert!(frames_from_runtime(&json!({ "frames": [{ "bytesBase64": "bad" }] })).is_empty());
    }
}
