from pathlib import Path

def edit(path, before, after, count=1):
    p = Path(path); text = p.read_text()
    assert text.count(before) == count, (path, before[:80], text.count(before))
    p.write_text(text.replace(before, after))

def span(text, marker):
    a = text.index(marker); brace = text.index('{', a); b = brace + 1; depth = 1
    while depth:
        if text[b] == '{': depth += 1
        elif text[b] == '}': depth -= 1
        b += 1
    return a, b, brace

# Fix a must-use detail found during the preceding compiler pass.
p = Path('modules/host-runtime/src/supervisor.rs')
p.write_text(p.read_text().replace('                self.join.take();', '                drop(self.join.take());'))

# This verifier consumes the CURRENT architecture account-port contract. The
# old short credential is usable only by the explicitly selected test harness.
Path('modules/process/src/entity_chat/secure.rs').write_text(r'''//! Allocation-bound admission. Contract source: LumioGameEngine@23401e178fdf346a0361b51a1ff881daf4d42554,
//! engine/wire/account-port-v1.json. No online nonce-consumption table: v1 is
//! explicitly a bounded bearer-replay policy, not a single-use-ticket protocol.
use lumio_host_runtime::{HostClock, SharedClock};
use serde::Deserialize;
use super::admission::{is_bot_namespace, AdmissionPayload};
use super::crypto::{base64url_decode, base64url_encode, verify_typed, BinReader,
    ADMISSION_PAYLOAD_VERSION, ADMISSION_PAYLOAD_TYPE, ADMISSION_TRUST_DOMAIN,
    NONCE_LEN, SIGNATURE_LEN};

const MAX_CREDENTIAL_BYTES: usize = 16_384;
const UNBOUND: &str = "__unbound__";

/// Trusted deployment/allocation registry data; never constructed from a client frame.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AllocationContext {
    pub server_audience: String,
    pub game_id: String,
    pub game_release_id: String,
    pub contract_id: String,
    pub room_id: String,
    pub allocation_id: String,
}
impl AllocationContext {
    fn fields(&self) -> [&str; 6] {
        [&self.server_audience, &self.game_id, &self.game_release_id,
         &self.contract_id, &self.room_id, &self.allocation_id]
    }
    pub fn validate(&self) -> Result<(), String> {
        if self.fields().iter().any(|s| s.is_empty() || *s == UNBOUND || s.len() > 256 || !s.is_ascii() || s.bytes().any(|b| b.is_ascii_control())) {
            return Err("admission_binding_mismatch".to_owned());
        }
        Ok(())
    }
}

/// Proof has private fields; the socket cannot invent an account or a room.
#[derive(Clone)]
pub(crate) struct VerifiedAdmission {
    pub(crate) payload: AdmissionPayload,
    pub(crate) room_id: String,
}

/// Immutable key/allocation snapshot with an advancing, testable clock.
#[derive(Clone)]
pub struct BoundAdmissionVerifier {
    pub(crate) allocation: AllocationContext,
    pub(crate) key_id: u8,
    pub(crate) public_key: Vec<u8>,
    pub(crate) clock: SharedClock,
    pub(crate) unix_origin: u64,
    clock_origin_ms: u64,
}
impl BoundAdmissionVerifier {
    pub fn new(allocation: AllocationContext, key_id: u8, public_key: Vec<u8>, clock: SharedClock, unix_origin: u64) -> Result<Self, String> {
        allocation.validate()?;
        if public_key.len() != 32 { return Err("admission key must contain 32 bytes".to_owned()); }
        let clock_origin_ms = clock.now_ms();
        Ok(Self { allocation, key_id, public_key, clock, unix_origin, clock_origin_ms })
    }
    pub(crate) fn now(&self) -> u64 {
        self.unix_origin.saturating_add(self.clock.now_ms().saturating_sub(self.clock_origin_ms) / 1000)
    }
    pub(crate) fn verify(&self, wire: &str) -> Result<VerifiedAdmission, String> {
        let malformed = || "admission_credential_malformed".to_owned();
        if wire.is_empty() || wire.len() > MAX_CREDENTIAL_BYTES { return Err(malformed()); }
        let bytes = base64url_decode(wire).ok_or_else(malformed)?;
        // Reject alternate encodings and padding rather than allowing aliases.
        if base64url_encode(&bytes) != wire || bytes.len() <= SIGNATURE_LEN { return Err(malformed()); }
        let (payload_bytes, signature) = bytes.split_at(bytes.len() - SIGNATURE_LEN);
        let mut reader = BinReader::new(payload_bytes);
        if reader.read_u16() != Some(ADMISSION_PAYLOAD_VERSION) { return Err(malformed()); }
        let key_id = reader.read_u8().ok_or_else(malformed)?;
        let account_id = reader.read_ascii().ok_or_else(malformed)?;
        let login_name = reader.read_ascii().ok_or_else(malformed)?;
        let bot = reader.read_u8().ok_or_else(malformed)?;
        let issued_at = reader.read_u64().ok_or_else(malformed)?;
        let expires_at = reader.read_u64().ok_or_else(malformed)?;
        let _nonce = reader.read_fixed(NONCE_LEN).ok_or_else(malformed)?;
        let mut binding = Vec::with_capacity(6);
        for _ in 0..6 { binding.push(reader.read_ascii().ok_or_else(malformed)?); }
        if reader.remaining() != 0 || bot > 1 || expires_at < issued_at { return Err(malformed()); }
        if key_id != self.key_id || !verify_typed(&self.public_key, ADMISSION_TRUST_DOMAIN, ADMISSION_PAYLOAD_TYPE, payload_bytes, signature) {
            return Err("admission_credential_invalid_signature".to_owned());
        }
        if self.now() > expires_at { return Err("admission_credential_expired".to_owned()); }
        if binding.iter().all(|s| s == UNBOUND) { return Err("admission_credential_unbound".to_owned()); }
        if binding.iter().zip(self.allocation.fields()).any(|(actual, expected)| actual.is_empty() || actual == UNBOUND || actual != expected) {
            return Err("admission_binding_mismatch".to_owned());
        }
        if account_id.len() != 37 || !account_id.starts_with("acct_") || !account_id[5..].bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)) {
            return Err(malformed());
        }
        if !(3..=32).contains(&login_name.len()) || !login_name.as_bytes()[0].is_ascii_alphabetic()
            || !login_name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-') { return Err(malformed()); }
        if is_bot_namespace(&login_name) && bot == 0 { return Err("bot_namespace_admission_forbidden".to_owned()); }
        Ok(VerifiedAdmission { payload: AdmissionPayload { key_id, account_id, login_name, bot_tool_context: bot == 1, issued_at, expires_at }, room_id: self.allocation.room_id.clone() })
    }
}

/// Test issuer, absent from normal library builds. No private key enters Host config.
#[cfg(any(test, feature = "test-harness"))]
pub fn issue_bound_test_credential(seed: &[u8; 32], allocation: &AllocationContext, expires_at: u64) -> String {
    use super::crypto::{BinWriter, sign_typed};
    let mut w = BinWriter::new();
    w.write_u16(1); w.write_u8(1);
    w.write_ascii("acct_0123456789abcdef0123456789abcdef"); w.write_ascii("Player01");
    w.write_u8(0); w.write_u64(1000); w.write_u64(expires_at); w.write_fixed(&[1; NONCE_LEN]);
    for field in allocation.fields() { w.write_ascii(field); }
    let mut payload = w.into_bytes();
    let signature = sign_typed(seed, ADMISSION_TRUST_DOMAIN, ADMISSION_PAYLOAD_TYPE, &payload);
    payload.extend_from_slice(&signature);
    base64url_encode(&payload)
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::admission::{generate_keys, issue_admission_credential};
    pub(super) fn context() -> AllocationContext {
        AllocationContext { server_audience:"ds-a".into(), game_id:"game-a".into(), game_release_id:"release-a".into(), contract_id:"lumio.gameplay-envelope.v1".into(), room_id:"room-a".into(), allocation_id:"allocation-a".into() }
    }
    #[test]
    fn bound_ticket_requires_every_trusted_dimension() {
        let keys = generate_keys(); let context = context();
        let ticket = issue_bound_test_credential(&keys.seed, &context, 1010);
        let clock = SharedClock::test();
        let verifier = BoundAdmissionVerifier::new(context.clone(), 1, keys.public.to_vec(), clock.clone(), 1000).unwrap();
        assert!(verifier.verify(&ticket).is_ok());
        // Reuse within its lease is permitted by the current bearer policy.
        assert!(verifier.verify(&ticket).is_ok());
        for index in 0..6 {
            let mut other = context.clone();
            match index { 0 => other.server_audience.push('x'), 1 => other.game_id.push('x'), 2 => other.game_release_id.push('x'), 3 => other.contract_id.push('x'), 4 => other.room_id.push('x'), _ => other.allocation_id.push('x') }
            let mismatch = BoundAdmissionVerifier::new(other, 1, keys.public.to_vec(), clock.clone(), 1000).unwrap();
            assert_eq!(mismatch.verify(&ticket).err().as_deref(), Some("admission_binding_mismatch"));
        }
        clock.advance_ms(11_000);
        assert_eq!(verifier.verify(&ticket).err().as_deref(), Some("admission_credential_expired"));
    }
    #[test]
    fn account_auth_and_old_short_tickets_cannot_enter_a_room() {
        let keys = generate_keys(); let mut unbound = context();
        unbound.server_audience = UNBOUND.into(); unbound.game_id = UNBOUND.into(); unbound.game_release_id = UNBOUND.into(); unbound.contract_id = UNBOUND.into(); unbound.room_id = UNBOUND.into(); unbound.allocation_id = UNBOUND.into();
        let verifier = BoundAdmissionVerifier::new(context(), 1, keys.public.to_vec(), SharedClock::test(), 1000).unwrap();
        let ticket = issue_bound_test_credential(&keys.seed, &unbound, 1010);
        assert_eq!(verifier.verify(&ticket).err().as_deref(), Some("admission_credential_unbound"));
        let old = issue_admission_credential(&keys.seed, 1, "acct_a", "Player01", false, 1, 2000);
        assert_eq!(verifier.verify(&old).err().as_deref(), Some("admission_credential_malformed"));
        assert!(verifier.verify(&"x".repeat(MAX_CREDENTIAL_BYTES + 1)).is_err());
    }
}
''')

wpath = Path('modules/process/src/entity_chat/wire.rs')
old = wpath.read_text()
client_start = old.index('/// Test/harness client that actually receives frames.')
tail = old[client_start:]
# The synchronous client remains a fixture, never the production transport.
tail = tail.replace('pub struct RoomClient {', '#[cfg(any(test, feature = "test-harness"))]\npub struct RoomClient {')
tail = tail.replace('impl RoomClient {', '#[cfg(any(test, feature = "test-harness"))]\nimpl RoomClient {', 1)
tail = tail.replace('    /// Reads one text frame.', '''    /// Test client for the authenticated HTTP Upgrade binding.
    pub fn connect_authenticated(uri: &str, credential: &str) -> Result<Self, String> {
        use tokio_tungstenite::tungstenite::client::IntoClientRequest;
        let mut request = uri.into_client_request().map_err(|e| e.to_string())?;
        request.headers_mut().insert("Authorization", HeaderValue::from_str(&format!("Bearer {credential}")).map_err(|e| e.to_string())?);
        let (mut ws, _) = ws_connect(request).map_err(|e| e.to_string())?;
        if let MaybeTlsStream::Plain(stream) = ws.get_mut() {
            stream.set_read_timeout(Some(Duration::from_secs(3))).map_err(|e| e.to_string())?;
        }
        Ok(Self { ws, received: Vec::new() })
    }

    /// Reads one text frame.''', 1)
tail = tail.replace('            observer_id: NEXT_OBSERVER_ID.fetch_add(1, Ordering::Relaxed),', '            observer_id: NEXT_OBSERVER_ID.fetch_add(1, Ordering::Relaxed),\n            wake: Arc::new(Notify::new()),')
head = r'''//! Bounded asynchronous socket reactor. The normal host requires an
//! allocation-bound bearer at HTTP Upgrade; arbitrary connectionId attachment
//! exists only in the explicit test-harness constructor. Bind stays loopback:
//! the deployment edge must supply WSS and forward Authorization securely.
use std::net::TcpListener;
#[cfg(any(test, feature = "test-harness"))]
use std::net::TcpStream;
use std::sync::{Arc, atomic::{AtomicU64, Ordering}};
use std::time::Duration;
use futures_util::{SinkExt, StreamExt};
use lumio_host_runtime::{bounded_channel, spawn_supervised, CancelToken, RecvError, SendError, Sender, SupervisedTask};
use serde_json::Value;
use tokio::sync::{watch, Notify};
use tokio::task::JoinSet;
use tokio_tungstenite::{accept_hdr_async_with_config, WebSocketStream};
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response, ErrorResponse};
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::protocol::{WebSocketConfig, CloseFrame, frame::coding::CloseCode};
use tokio_tungstenite::tungstenite::Message;
#[cfg(any(test, feature = "test-harness"))]
use tokio_tungstenite::tungstenite::{client::connect as ws_connect, protocol::WebSocket, stream::MaybeTlsStream};
use super::secure::{BoundAdmissionVerifier, VerifiedAdmission};

pub const MAX_WIRE_TEXT_BYTES: usize = 65_536;
const MAX_SOCKET_TASKS: usize = 256;
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(3);
const WRITE_TIMEOUT: Duration = Duration::from_secs(3);
const EGRESS_SLOTS: usize = 16;

#[derive(Clone)]
pub struct WireSender {
    inner: Sender<WireOut>, cancel: CancelToken, observer_id: u64, wake: Arc<Notify>,
}
#[derive(Debug, Clone)]
pub enum WireOut { Text(Vec<u8>), Close }
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WireSendError { Full, Closed, TooLarge, InvalidUtf8 }
impl WireSender {
    pub fn try_send_text(&self, text: String) -> Result<(), WireSendError> { self.try_send_bytes(text.as_bytes()) }
    pub fn try_send_bytes(&self, bytes: &[u8]) -> Result<(), WireSendError> {
        if self.cancel.is_cancelled() { return Err(WireSendError::Closed); }
        if bytes.len() > MAX_WIRE_TEXT_BYTES { return Err(WireSendError::TooLarge); }
        std::str::from_utf8(bytes).map_err(|_| WireSendError::InvalidUtf8)?;
        self.inner.try_send(WireOut::Text(bytes.to_vec())).map_err(map_send_error)?;
        self.wake.notify_one(); Ok(())
    }
    pub fn try_close(&self) -> Result<(), WireSendError> {
        let result = self.try_close_ordered();
        if result.is_err() { self.abort(); }
        result
    }
    pub(crate) fn try_close_ordered(&self) -> Result<(), WireSendError> {
        let result = self.inner.try_send(WireOut::Close).map_err(map_send_error);
        self.wake.notify_one(); result
    }
    pub(crate) fn observer_id(&self) -> u64 { self.observer_id }
    pub(crate) fn is_closed(&self) -> bool { self.cancel.is_cancelled() }
    pub(crate) fn abort(&self) { self.cancel.cancel(); self.wake.notify_one(); }
}
#[cfg(test)]
pub(crate) fn test_sender_pair(capacity: usize) -> (WireSender, lumio_host_runtime::Receiver<WireOut>) {
    let (inner, rx) = bounded_channel(capacity);
    (WireSender { inner, cancel: CancelToken::new(), observer_id: NEXT_OBSERVER_ID.fetch_add(1, Ordering::Relaxed), wake: Arc::new(Notify::new()) }, rx)
}
fn map_send_error(error: SendError<WireOut>) -> WireSendError {
    match error { SendError::Full(_) => WireSendError::Full, SendError::Closed(_) => WireSendError::Closed }
}
pub(crate) enum WireEvent {
    Attached { connection_id: String, egress: WireSender },
    Authenticated { connection_id: String, egress: WireSender, proof: VerifiedAdmission },
    Input { connection_id: String, observer_id: u64, text: String },
    Closed { connection_id: String, observer_id: u64 },
}
static NEXT_OBSERVER_ID: AtomicU64 = AtomicU64::new(1);

pub struct RoomListener { pub port: u16, task: SupervisedTask, stop: watch::Sender<bool> }
impl RoomListener {
    #[cfg(any(test, feature = "test-harness"))]
    pub(crate) fn bind(tx: Sender<WireEvent>) -> Result<Self, String> { Self::start(tx, None) }
    pub(crate) fn bind_authenticated(tx: Sender<WireEvent>, verifier: BoundAdmissionVerifier) -> Result<Self, String> { Self::start(tx, Some(verifier)) }
    fn start(tx: Sender<WireEvent>, verifier: Option<BoundAdmissionVerifier>) -> Result<Self, String> {
        let socket = TcpListener::bind("127.0.0.1:0").map_err(|e| e.to_string())?;
        socket.set_nonblocking(true).map_err(|e| e.to_string())?;
        let port = socket.local_addr().map_err(|e| e.to_string())?.port();
        let (stop, mut stopped) = watch::channel(false);
        let task = spawn_supervised("lumio-room-reactor", move |_| {
            let executor = tokio::runtime::Builder::new_current_thread().enable_all().build().expect("socket reactor");
            executor.block_on(async move {
                let listener = tokio::net::TcpListener::from_std(socket).expect("async listener");
                let mut connections = JoinSet::new();
                loop {
                    tokio::select! {
                        _ = stopped.changed() => break,
                        result = connections.join_next(), if !connections.is_empty() => {
                            if let Some(Err(error)) = result { panic!("socket task failed: {error}"); }
                        }
                        accepted = listener.accept() => {
                            let (stream, _) = accepted.expect("socket accept failed");
                            if connections.len() >= MAX_SOCKET_TASKS { drop(stream); continue; }
                            connections.spawn(run_socket(stream, tx.clone(), stopped.clone(), verifier.clone()));
                        }
                    }
                }
                connections.abort_all();
                while connections.join_next().await.is_some() {}
            });
        });
        Ok(Self { port, task, stop })
    }
    #[must_use]
    pub fn uri(&self) -> String { format!("ws://127.0.0.1:{}", self.port) }
    #[must_use]
    pub fn is_healthy(&self) -> bool { !self.task.is_finished() && self.task.failure().is_none() }
    pub fn shutdown(&self) { let _ = self.stop.send(true); }
}
impl Drop for RoomListener { fn drop(&mut self) { self.shutdown(); } }

struct SocketGuard { cancel: CancelToken, wake: Arc<Notify> }
impl Drop for SocketGuard { fn drop(&mut self) { self.cancel.cancel(); self.wake.notify_one(); } }

fn unauthorized() -> ErrorResponse {
    tokio_tungstenite::tungstenite::http::Response::builder().status(401).body(Some("unauthorized".to_owned())).expect("fixed response")
}

async fn run_socket(stream: tokio::net::TcpStream, tx: Sender<WireEvent>, mut stop: watch::Receiver<bool>, verifier: Option<BoundAdmissionVerifier>) {
    let mut proof = None;
    let limits = WebSocketConfig::default().read_buffer_size(4096).write_buffer_size(0)
        .max_write_buffer_size(2 * MAX_WIRE_TEXT_BYTES + 1024)
        .max_message_size(Some(MAX_WIRE_TEXT_BYTES)).max_frame_size(Some(MAX_WIRE_TEXT_BYTES));
    let handshake = accept_hdr_async_with_config(stream, |request: &Request, response: Response| {
        if let Some(verifier) = &verifier {
            let header = request.headers().get("Authorization").and_then(|v| v.to_str().ok()).ok_or_else(unauthorized)?;
            let credential = header.strip_prefix("Bearer ").ok_or_else(unauthorized)?;
            proof = Some(verifier.verify(credential).map_err(|_| unauthorized())?);
        }
        Ok(response)
    }, Some(limits));
    let mut ws = tokio::select! {
        _ = stop.changed() => return,
        result = tokio::time::timeout(HANDSHAKE_TIMEOUT, handshake) => match result { Ok(Ok(ws)) => ws, _ => return },
    };
    let connection_id = if proof.is_some() {
        let mut random = [0u8; 16];
        if getrandom::fill(&mut random).is_err() { return; }
        format!("conn-{}", super::crypto::hex_lower(&random))
    } else {
        let first = tokio::select! {
            _ = stop.changed() => return,
            result = tokio::time::timeout(HANDSHAKE_TIMEOUT, ws.next()) => match result {
                Ok(Some(Ok(Message::Text(text)))) => text,
                _ => return,
            },
        };
        let Some(id) = parse_connection_id(&first) else { return; }; id
    };
    let (out, pending) = bounded_channel(EGRESS_SLOTS);
    let cancel = CancelToken::new(); let wake = Arc::new(Notify::new());
    let _guard = SocketGuard { cancel: cancel.clone(), wake: wake.clone() };
    let observer_id = NEXT_OBSERVER_ID.fetch_add(1, Ordering::Relaxed);
    let egress = WireSender { inner: out, cancel: cancel.clone(), observer_id, wake: wake.clone() };
    let event = match proof {
        Some(proof) => WireEvent::Authenticated { connection_id: connection_id.clone(), egress, proof },
        None => WireEvent::Attached { connection_id: connection_id.clone(), egress },
    };
    if tx.try_send(event).is_err() { return; }
    let (mut sink, mut input) = ws.split();
    let reason = tokio::select! {
        _ = stop.changed() => "shutdown",
        reason = read_inputs(&mut input, &tx, &connection_id, observer_id) => reason,
        reason = write_frames(&mut sink, &pending, &cancel, &wake) => reason,
    };
    cancel.cancel(); wake.notify_one();
    let close = Message::Close(Some(CloseFrame { code: CloseCode::Policy, reason: reason.into() }));
    let _ = tokio::time::timeout(Duration::from_millis(250), sink.send(close)).await;
    // If the control queue is full, the guard's closed flag is swept by the
    // owner. No blocking send can hold the reactor or process shutdown hostage.
    let _ = tx.try_send(WireEvent::Closed { connection_id, observer_id });
}

type Socket = WebSocketStream<tokio::net::TcpStream>;
async fn read_inputs(input: &mut futures_util::stream::SplitStream<Socket>, tx: &Sender<WireEvent>, connection: &str, observer_id: u64) -> &'static str {
    while let Some(frame) = input.next().await {
        match frame {
            Ok(Message::Text(text)) if is_wire_text_size_valid(&text) => {
                if tx.try_send(WireEvent::Input { connection_id: connection.to_owned(), observer_id, text: text.to_string() }).is_err() { return "queue_full"; }
            }
            Ok(Message::Ping(_) | Message::Pong(_)) => {}
            Ok(Message::Close(_)) => return "session_closed",
            _ => return "bad_envelope",
        }
    }
    "session_closed"
}
async fn write_frames(sink: &mut futures_util::stream::SplitSink<Socket, Message>, pending: &lumio_host_runtime::Receiver<WireOut>, cancel: &CancelToken, wake: &Notify) -> &'static str {
    loop {
        let notified = wake.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if cancel.is_cancelled() { return "session_closed"; }
        let frame = match pending.try_recv() {
            Ok(frame) => frame,
            Err(RecvError::Empty) => { notified.await; continue; }
            Err(RecvError::Closed) => return "session_closed",
        };
        let message = match frame {
            WireOut::Text(bytes) => match String::from_utf8(bytes) {
                Ok(text) => Message::Text(text.into()), Err(_) => return "bad_envelope",
            },
            WireOut::Close => return "session_closed",
        };
        if !matches!(tokio::time::timeout(WRITE_TIMEOUT, sink.send(message)).await, Ok(Ok(()))) { return "queue_full"; }
    }
}
fn parse_connection_id(text: &str) -> Option<String> {
    let value: Value = serde_json::from_str(text).ok()?;
    value.get("connectionId").and_then(Value::as_str).filter(|s| !s.is_empty() && s.len() <= 256).map(str::to_owned)
}
fn is_wire_text_size_valid(text: &str) -> bool { text.len() <= MAX_WIRE_TEXT_BYTES }
#[cfg(any(test, feature = "test-harness"))]
fn is_would_block(error: &tokio_tungstenite::tungstenite::Error) -> bool {
    matches!(error, tokio_tungstenite::tungstenite::Error::Io(io) if io.kind() == std::io::ErrorKind::WouldBlock || io.kind() == std::io::ErrorKind::TimedOut)
}
'''
wpath.write_text(head + tail)

hpath = Path('modules/process/src/entity_chat/host.rs'); h = hpath.read_text()
h = h.replace('use super::runtime::BoundEntityKind;', 'use super::runtime::BoundEntityKind;\nuse super::secure::BoundAdmissionVerifier;')
h = h.replace('    admission_clock_origin_ms: u64,', '    admission_clock_origin_ms: u64,\n    admission_verifier: Option<BoundAdmissionVerifier>,')
a, b, brace = span(h, '    pub fn new(')
body = h[brace + 1:b - 1]
body = body.replace('        let listener = RoomListener::bind(wire_tx).expect("room wire bind");', '''        let listener = match admission_verifier.clone() {
            Some(verifier) => RoomListener::bind_authenticated(wire_tx, verifier)?,
            None => {
                #[cfg(any(test, feature = "test-harness"))]
                { RoomListener::bind(wire_tx)? }
                #[cfg(not(any(test, feature = "test-harness")))]
                { return Err("test-harness transport is unavailable".to_owned()); }
            }
        };''')
assert 'RoomListener::bind(wire_tx).expect' not in body
body = body.replace('                reconnect_window_ms,', '                reconnect_window_ms,\n                admission_verifier,')
last_self = body.rindex('        Self {')
body = body[:last_self] + body[last_self:].replace('        Self {', '        Ok(Self {', 1)
ending = body.rfind('        }'); body = body[:ending] + '        })' + body[ending + 9:]
wrapper = '''    #[cfg(any(test, feature = "test-harness"))]
    pub fn new(reconnect_window_ms: u64, clock: SharedClock, runtime: Box<dyn RuntimeSurface>, kernel: Box<dyn KernelTimer>, admission_key_id: u8, admission_public: Vec<u8>, unix_seconds: u64) -> Self {
        Self::build((reconnect_window_ms, clock, admission_key_id, admission_public, unix_seconds), runtime, kernel, None).expect("test host startup")
    }

    /// Secure Host entry. Public clients cannot select an existing connection identity.
    pub fn new_authenticated(reconnect_window_ms: u64, runtime: Box<dyn RuntimeSurface>, kernel: Box<dyn KernelTimer>, verifier: BoundAdmissionVerifier) -> Result<Self, String> {
        Self::build((reconnect_window_ms, verifier.clock.clone(), verifier.key_id, verifier.public_key.clone(), verifier.unix_origin), runtime, kernel, Some(verifier))
    }

    fn build(config: (u64, SharedClock, u8, Vec<u8>, u64), runtime: Box<dyn RuntimeSurface>, kernel: Box<dyn KernelTimer>, admission_verifier: Option<BoundAdmissionVerifier>) -> Result<Self, String> {
        let (reconnect_window_ms, clock, admission_key_id, admission_public, unix_seconds) = config;
''' + body + '\n    }'
h = h[:a] + wrapper + h[b:]
h = h.replace('    pub fn admit_verified(\n', '    #[cfg(any(test, feature = "test-harness"))]\n    pub fn admit_verified(\n', 1)
h = h.replace('        match verify_admission(\n', '''        if let Some(verifier) = &self.admission_verifier {
            if room_id != verifier.allocation.room_id { return RoomAdmitResult::reject("admission_binding_mismatch"); }
            return match verifier.verify(credential) {
                Ok(proof) => self.admit_verified(room_id, connection_id, &proof.payload),
                Err(code) => RoomAdmitResult::reject(&code),
            };
        }
        match verify_admission(
''', 1)
# Secure listener proofs never share a connectionId with another socket.
h = h.replace('''            WireEvent::Attached {
                connection_id,
                egress,
            } => {''', '''            WireEvent::Authenticated { connection_id, egress, proof } => {
                let result = self.admit_verified(&proof.room_id, &connection_id, &proof.payload);
                if !result.accepted { let _ = egress.try_close(); return; }
                if let Some(session) = self.sessions.get_mut(&connection_id) {
                    session.egresses.push(ObserverEgress::new(egress));
                } else {
                    self.pending_egress.insert(connection_id.clone(), vec![ObserverEgress::new(egress)]);
                }
                self.flush_deferred(&connection_id);
            }
            WireEvent::Attached {
                connection_id,
                egress,
            } => {
                if self.admission_verifier.is_some() { egress.abort(); return; }''', 1)
h = h.replace('''            WireEvent::Input {
                connection_id,
                text,
            } => {''', '''            WireEvent::Input {
                connection_id,
                observer_id,
                text,
            } => {
                if self.admission_verifier.is_some() && !self.sessions.get(&connection_id).is_some_and(|session| session.egresses.iter().any(|egress| egress.sender.observer_id() == observer_id && !egress.sender.is_closed())) {
                    return;
                }''', 1)
h = h.replace('fn flush_observer_egress(egress: &mut ObserverEgress) -> Delivery {', 'fn flush_observer_egress(egress: &mut ObserverEgress) -> Delivery {\n    if egress.sender.is_closed() { return Delivery::Unavailable; }')
h = h.replace('''        for (connection, session) in &mut self.sessions {
            if !flush_observer_egresses(&mut session.egresses) {''', '''        for (connection, session) in &mut self.sessions {
            let had_socket = !session.egresses.is_empty();
            if !flush_observer_egresses(&mut session.egresses) || (had_socket && session.egresses.is_empty()) {''')
h = h.replace('''        self._owner.cancel();
        self._forward.cancel();''', '''        self._listener.shutdown();
        self._forward.cancel();
        self._owner.cancel();''')
h = h.replace('    /// Loopback Room wire URI.', '''    /// Nonblocking process-supervision probe. Does not enqueue work to a stuck owner.
    #[must_use]
    pub fn is_healthy(&self) -> bool {
        !self._owner.is_finished() && self._owner.failure().is_none()
            && !self._forward.is_finished() && self._forward.failure().is_none()
            && self._listener.is_healthy()
    }

    /// Loopback Room wire URI.''', 1)
hpath.write_text(h)
p = Path('modules/process/src/entity_chat/host_hardening_tests.rs'); t = p.read_text()
t = t.replace('        active_rooms: BTreeSet::new(),', '        active_rooms: BTreeSet::new(),\n        admission_verifier: None,')
t = t.replace('        text: text.to_owned(),', '        text: text.to_owned(),\n        observer_id: 0,')
p.write_text(t)

# Test-only launchers and the old unauthenticated socket API are not available
# in a normal dependency build. Even an all-features build uses the secure
# constructor explicitly; a feature does not change that constructor's policy.
p = Path('modules/process/src/entity_chat/mod.rs'); m = p.read_text()
for name in ['account', 'bots', 'browser', 'discover', 'suite']:
    m = m.replace(f'mod {name};', f'#[cfg(any(test, feature = "test-harness"))]\nmod {name};')
    m = m.replace(f'pub use {name}::', f'#[cfg(any(test, feature = "test-harness"))]\npub use {name}::')
m = m.replace('mod runtime;', 'mod runtime;\nmod secure;\npub use secure::{AllocationContext, BoundAdmissionVerifier};\n#[cfg(any(test, feature = "test-harness"))]\npub use secure::issue_bound_test_credential;')
m = m.replace('pub use wire::{RoomClient, RoomListener, MAX_WIRE_TEXT_BYTES};', 'pub use wire::{RoomListener, MAX_WIRE_TEXT_BYTES};\n#[cfg(any(test, feature = "test-harness"))]\npub use wire::RoomClient;')
p.write_text(m)
p = Path('modules/process/Cargo.toml'); cargo = p.read_text()
cargo += '\n[features]\ndefault = []\ntest-harness = []\n'
cargo = cargo.replace('path = "src/main.rs"', 'path = "src/main.rs"\nrequired-features = ["test-harness"]')
cargo = cargo.replace('path = "src/entity_chat_replay.rs"', 'path = "src/entity_chat_replay.rs"\nrequired-features = ["test-harness"]')
for test in ['entity_chat_architecture', 'entity_chat_wire', 'entity_chat_host', 'entity_chat_acceptance', 'secure_transport']:
    cargo += f'\n[[test]]\nname = "{test}"\npath = "tests/{test}.rs"\nrequired-features = ["test-harness"]\n'
p.write_text(cargo)
p = Path('.github/workflows/repository-policy.yml'); ci = p.read_text()
ci = ci.replace('cargo clippy --workspace --all-targets --locked', 'cargo clippy --workspace --all-targets --all-features --locked')
ci = ci.replace('cargo test -p lumio-server-process --locked', 'cargo test -p lumio-server-process --all-features --locked')
ci = ci.replace('--test entity_chat_host', '--test entity_chat_host --test secure_transport')
p.write_text(ci)

Path('modules/process/tests/secure_transport.rs').write_text(r'''//! Real sockets against the production constructor; Runtime is an explicit test double.
mod common;
use common::{SharedRuntime, TestKernel};
use lumio_host_runtime::SharedClock;
use lumio_server_process::entity_chat::{AllocationContext, BoundAdmissionVerifier, EntityChatHost, RoomClient, generate_keys, issue_bound_test_credential};

fn context() -> AllocationContext {
    AllocationContext { server_audience:"ds-a".into(), game_id:"game-a".into(), game_release_id:"release-a".into(), contract_id:"lumio.gameplay-envelope.v1".into(), room_id:"room-a".into(), allocation_id:"allocation-a".into() }
}
#[test]
fn arbitrary_socket_id_is_not_an_authorization_capability() {
    let keys = generate_keys(); let allocation = context();
    let ticket = issue_bound_test_credential(&keys.seed, &allocation, 2000);
    let verifier = BoundAdmissionVerifier::new(allocation, 1, keys.public.to_vec(), SharedClock::test(), 1000).unwrap();
    let host = EntityChatHost::new_authenticated(300_000, Box::new(SharedRuntime::new()), Box::new(TestKernel::new()), verifier).unwrap();
    assert!(host.admit("room-a".to_owned(), "known-connection".to_owned(), ticket.clone()).accepted);
    assert!(RoomClient::connect(&host.listen_uri(), "known-connection").is_err());
    assert!(RoomClient::connect_authenticated(&host.listen_uri(), "bad-ticket").is_err());
    assert!(host.is_healthy());
}
#[test]
fn valid_socket_uses_server_allocated_identity_and_gets_runtime_welcome() {
    let keys = generate_keys(); let allocation = context();
    let ticket = issue_bound_test_credential(&keys.seed, &allocation, 2000);
    let verifier = BoundAdmissionVerifier::new(allocation, 1, keys.public.to_vec(), SharedClock::test(), 1000).unwrap();
    let host = EntityChatHost::new_authenticated(300_000, Box::new(SharedRuntime::new()), Box::new(TestKernel::new()), verifier).unwrap();
    let mut client = RoomClient::connect_authenticated(&host.listen_uri(), &ticket).expect("authenticated socket");
    assert!(client.recv_text().expect("welcome").contains("Welcome"));
    assert!(host.try_self_lookup("known-connection".to_owned()).is_none());
}
#[test]
fn stalled_handshake_cannot_hold_host_drop_open() {
    let keys = generate_keys();
    let verifier = BoundAdmissionVerifier::new(context(), 1, keys.public.to_vec(), SharedClock::test(), 1000).unwrap();
    let host = EntityChatHost::new_authenticated(300_000, Box::new(SharedRuntime::new()), Box::new(TestKernel::new()), verifier).unwrap();
    let address = host.listen_uri().trim_start_matches("ws://").to_owned();
    let _socket = std::net::TcpStream::connect(address).unwrap();
    let start = std::time::Instant::now();
    drop(host);
    assert!(start.elapsed() < std::time::Duration::from_secs(2));
}
''')
Path(__file__).unlink()
print('Applied secure constructor, current allocation binding, async transport, and test-only isolation.')
