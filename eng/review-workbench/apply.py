from pathlib import Path
import re

p=Path('modules/process/src/entity_chat/wire.rs');s=p.read_text()
s=s.replace('    pub fn try_send_text(', '    #[cfg(test)]\n    pub fn try_send_text(',1)
s=s.replace('async fn run_socket(', '#[expect(clippy::result_large_err, reason = "tungstenite Callback requires its concrete HTTP ErrorResponse") ]\nasync fn run_socket(',1)
p.write_text(s)
p=Path('modules/process/src/persistence.rs');s=p.read_text().replace('/// InputCommand bytes', '/// `InputCommand` bytes').replace('tick as u8', 'u8::try_from(tick).expect("small test tick")');p.write_text(s)
p=Path('modules/process/src/entity_chat/suite.rs');s=p.read_text().replace('''        write_evidence(out_dir, &evidence, &host_audit);
        return evidence;''','''        return match write_evidence(out_dir, &evidence, &host_audit) {
            Ok(()) => evidence,
            Err(error) => json!({"ok":false,"blocked":"evidence_write_failed","detail":error.to_string()}),
        };''');p.write_text(s)
p=Path('modules/process/src/entity_chat/host.rs');s=p.read_text()
for name in ['owner','listener','forward']:
    s=s.replace('self._'+name, 'self.'+name).replace('    _'+name+':', '    '+name+':')
for name in ['pending_wire_chat_inputs','wire_observer_count']:
    s=s.replace('    pub(crate) fn '+name, '    #[cfg(any(test, feature = "test-harness"))]\n    pub(crate) fn '+name)
s=s.replace('use std::time::Duration;', 'use std::time::Duration;\nuse std::sync::{Arc, atomic::{AtomicBool, AtomicU64, Ordering}};')
s=s.replace('use super::runtime::BoundEntityKind;', 'use super::runtime::{BoundEntityKind, ChatOpKind};')
s=s.replace('struct Inner {', '''#[derive(Default)]
struct HealthState {
    ready: AtomicBool,
    faulted: AtomicBool,
    draining: AtomicBool,
    heartbeat_ms: AtomicU64,
    rejected_inputs: AtomicU64,
}

/// Snapshot of bounded host diagnostics, never component state or credentials.
#[derive(Debug, Clone, Copy)]
pub struct HostHealth {
    pub ready: bool,
    pub faulted: bool,
    pub draining: bool,
    pub heartbeat_age_ms: u64,
    pub rejected_inputs: u64,
}

struct Inner {
    health: Arc<HealthState>,''')
s=s.replace('pub struct EntityChatHost {', 'pub struct EntityChatHost {\n    health: Arc<HealthState>,')
s=s.replace('        let (tx, rx) = bounded_channel(256);', '''        let health = Arc::new(HealthState::default());
        health.heartbeat_ms.store(clock.now_ms(), Ordering::Release);
        let owner_health = health.clone();
        let (tx, rx) = bounded_channel(256);''',1)
s=s.replace('            let mut inner = Inner {', '            let mut inner = Inner {\n                health: owner_health,',1)
s=s.replace('''            if inner
                .kernel''', '''            if inner.runtime.initialize().is_err() {
                inner.health.faulted.store(true, Ordering::Release);
                return;
            }
            if inner
                .kernel''',1)
s=s.replace('''            let self_drive = !inner.clock.is_deterministic();''', '''            if let Some(verifier) = &inner.admission_verifier {
                inner.active_rooms.insert(verifier.allocation.room_id.clone());
            }
            inner.health.ready.store(true, Ordering::Release);
            let self_drive = !inner.clock.is_deterministic();''',1)
s=s.replace('''                if cancel.is_cancelled() {
                    break;
                }
                let work = rx.recv_timeout''', '''                if cancel.is_cancelled() || inner.health.faulted.load(Ordering::Acquire) {
                    inner.health.ready.store(false, Ordering::Release);
                    break;
                }
                inner.health.heartbeat_ms.store(inner.clock.now_ms(), Ordering::Release);
                let work = rx.recv_timeout''',1)
s=s.replace('''                if self_drive
                    && inner.clock.now_ms()''', '''                inner.health.heartbeat_ms.store(inner.clock.now_ms(), Ordering::Release);
                if self_drive
                    && !inner.health.draining.load(Ordering::Acquire)
                    && inner.clock.now_ms()''',1)
s=s.replace('''Err(lumio_host_runtime::RecvError::Empty) if self_drive =>''', '''Err(lumio_host_runtime::RecvError::Empty) if self_drive && !inner.health.draining.load(Ordering::Acquire) =>''',1)
s=s.replace('''        Ok(Self {
            tx,''', '''        Ok(Self {
            health,
            tx,''',1)
s=s.replace('''        !self.owner.is_finished()''', '''        !self.health.faulted.load(Ordering::Acquire)
            && !self.owner.is_finished()''',1)
s=s.replace('''    /// Loopback Room wire URI.''', '''    /// Reads health without marshalling to the potentially stalled owner.
    #[must_use]
    pub fn health(&self) -> HostHealth {
        HostHealth {
            ready: self.health.ready.load(Ordering::Acquire) && self.is_healthy(),
            faulted: self.health.faulted.load(Ordering::Acquire) || !self.is_healthy(),
            draining: self.health.draining.load(Ordering::Acquire),
            heartbeat_age_ms: self.clock.now_ms().saturating_sub(self.health.heartbeat_ms.load(Ordering::Acquire)),
            rejected_inputs: self.health.rejected_inputs.load(Ordering::Relaxed),
        }
    }

    /// Freezes this room at an owner barrier, refusing new admission/input.
    /// Does not claim any data is durable; the caller must publish the snapshot.
    pub fn quiesce(&self) {
        self.on_owner(|inner| inner.health.draining.store(true, Ordering::Release));
    }

    /// Snapshot and tick metadata are captured in the same owner callback.
    /// File I/O is intentionally performed by the caller, not the owner.
    pub fn checkpoint(&self, room_id: String) -> Result<(u64, PersistRecord), String> {
        self.on_owner(move |inner| {
            if inner.health.faulted.load(Ordering::Acquire) { return Err("world_faulted".to_owned()); }
            inner.runtime.persist(&room_id).map(|bytes| (inner.tick_id, bytes))
        })
    }

    /// Loopback Room wire URI.''',1)
# Fail-stop is latched before any subsequent owner command, with no partial world reuse.
s=s.replace('''    fn admit_verified(
        &mut self,''', '''    fn admit_verified(
        &mut self,''',1)
needle='''        if room_id.is_empty()
            || connection_id.is_empty()'''
s=s.replace(needle, '''        if self.health.faulted.load(Ordering::Acquire) || self.health.draining.load(Ordering::Acquire) {
            return RoomAdmitResult::reject("session_closed");
        }
        if room_id.is_empty()
            || connection_id.is_empty()''',1)
s=s.replace('''        if envelope_bytes.len() > MAX_WIRE_TEXT_BYTES {
            return ChatOperation::rejected("bad_envelope");
        }''','''        if self.health.faulted.load(Ordering::Acquire) || self.health.draining.load(Ordering::Acquire) {
            return ChatOperation::rejected("session_closed");
        }
        if envelope_bytes.len() > MAX_WIRE_TEXT_BYTES {
            return ChatOperation::rejected("bad_envelope");
        }''',1)
# Do not retry unknown rejections; a rejected command is terminal, and fatal
# outcomes seal the whole world rather than silently discarding an input.
s=s.replace('''            let _ = self.admit_input_command(&pending.connection_id, &pending.envelope_bytes);''','''            let outcome = self.admit_input_command(&pending.connection_id, &pending.envelope_bytes);
            match outcome.kind {
                ChatOpKind::Admitted | ChatOpKind::Committed => {}
                ChatOpKind::Rejected => {
                    self.health.rejected_inputs.fetch_add(1, Ordering::Relaxed);
                    if outcome.error_code.as_deref() == Some("runtime_failure") {
                        self.health.faulted.store(true, Ordering::Release);
                        break;
                    }
                }
                ChatOpKind::Fatal => {
                    self.health.faulted.store(true, Ordering::Release);
                    break;
                }
            }''',1)
s=s.replace('''        self.flush_pending_wire_inputs(room_id);
        self.tick_id =''','''        if self.health.faulted.load(Ordering::Acquire) { return RuntimeTick::failed("runtime_failure"); }
        self.flush_pending_wire_inputs(room_id);
        if self.health.faulted.load(Ordering::Acquire) { return RuntimeTick::failed("runtime_failure"); }
        self.tick_id =''',1)
s=s.replace('''        let _ = self.drive_wall();
        self.flush_pending_observers();''','''        if self.health.faulted.load(Ordering::Acquire) || self.health.draining.load(Ordering::Acquire) { return; }
        if !self.drive_wall() {
            self.health.faulted.store(true, Ordering::Release);
            return;
        }
        self.flush_pending_observers();''',1)
s=s.replace('''            let _ = self.run_tick_after_advance(&room_id);''','''            let outcome = self.run_tick_after_advance(&room_id);
            if !outcome.ok && outcome.code.as_deref() == Some("runtime_failure") {
                self.health.faulted.store(true, Ordering::Release);
                break;
            }''',1)
# Incoming messages from closed/retired sockets cannot mutate a quiesced world.
s=s.replace('''        self.flush_pending_observers();
        match event {''','''        self.flush_pending_observers();
        if self.health.draining.load(Ordering::Acquire) || self.health.faulted.load(Ordering::Acquire) {
            match event {
                WireEvent::Authenticated { egress, .. } | WireEvent::Attached { egress, .. } => { let _ = egress.try_close(); }
                WireEvent::Input { .. } | WireEvent::Closed { .. } => {}
            }
            return;
        }
        match event {''',1)
p.write_text(s)
p=Path('modules/process/src/entity_chat/host_hardening_tests.rs');s=p.read_text().replace('    Inner {', '    Inner {\n        health: Arc::new(HealthState::default()),',1);p.write_text(s)
p=Path('modules/process/src/entity_chat/runtime.rs');s=p.read_text().replace('pub trait RuntimeSurface: Send {', '''pub trait RuntimeSurface: Send {
    /// Owner-thread startup validation. Doubles need no external initialization.
    fn initialize(&mut self) -> Result<(), String> { Ok(()) }
''');p.write_text(s)
p=Path('modules/process/src/entity_chat/clr.rs');s=p.read_text().replace('impl RuntimeSurface for ClrGameplay {', '''impl RuntimeSurface for ClrGameplay {
    fn initialize(&mut self) -> Result<(), String> {
        // Forces actual boot/signature checks before the host publishes Ready.
        let response = self.call(json!({"op":"drain"}))?;
        if response.get("ok").and_then(Value::as_bool) == Some(true) { Ok(()) }
        else { Err("runtime_initialization_failed".to_owned()) }
    }
''');p.write_text(s)
p=Path('modules/process/src/entity_chat/mod.rs');s=p.read_text().replace('EntityChatHost, EntityResolution, RoomAdmitResult,','EntityChatHost, EntityResolution, HostHealth, RoomAdmitResult,');p.write_text(s)
Path(__file__).unlink()
