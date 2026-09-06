//! Bounded asynchronous socket reactor. The normal host requires an
//! allocation-bound bearer at HTTP Upgrade; arbitrary connectionId attachment
//! exists only in the explicit test-harness constructor. Bind stays loopback:
//! the deployment edge must supply WSS and forward Authorization securely.
use super::secure::{BoundAdmissionVerifier, VerifiedAdmission};
use futures_util::{SinkExt, StreamExt};
use lumio_host_runtime::{
    bounded_channel, spawn_supervised, CancelToken, RecvError, SendError, Sender, SupervisedTask,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::net::TcpListener;
#[cfg(any(test, feature = "test-harness"))]
use std::net::TcpStream;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::sync::{watch, Notify};
use tokio::task::JoinSet;
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::protocol::{
    frame::coding::CloseCode, CloseFrame, WebSocketConfig,
};
use tokio_tungstenite::tungstenite::Message;
#[cfg(any(test, feature = "test-harness"))]
use tokio_tungstenite::tungstenite::{
    client::connect as ws_connect, protocol::WebSocket, stream::MaybeTlsStream,
};
use tokio_tungstenite::{accept_hdr_async_with_config, WebSocketStream};

pub const MAX_WIRE_TEXT_BYTES: usize = 65_536;

fn default_listen_address() -> String {
    "127.0.0.1".into()
}
const fn default_listen_port() -> u16 {
    0
}
const fn default_max_socket_tasks() -> usize {
    256
}
const fn default_max_admissions_per_second() -> u32 {
    256
}
const fn default_handshake_timeout_ms() -> u64 {
    3000
}
const fn default_write_timeout_ms() -> u64 {
    3000
}
const fn default_max_wire_text_bytes() -> usize {
    65_536
}
const fn default_egress_slots() -> usize {
    16
}
const fn default_unauthenticated_quota_bytes() -> usize {
    1024
}
const fn default_idle_timeout_ms() -> u64 {
    60_000
}

/// Transport and socket parameters. All sizing and limits are configurable per ds-server redline.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TransportConfig {
    #[serde(default = "default_listen_address")]
    pub listen_address: String,
    #[serde(default = "default_listen_port")]
    pub listen_port: u16,
    #[serde(default = "default_max_socket_tasks")]
    pub max_socket_tasks: usize,
    #[serde(default = "default_max_admissions_per_second")]
    pub max_admissions_per_second: u32,
    #[serde(default = "default_handshake_timeout_ms")]
    pub handshake_timeout_ms: u64,
    #[serde(default = "default_write_timeout_ms")]
    pub write_timeout_ms: u64,
    #[serde(default = "default_max_wire_text_bytes")]
    pub max_wire_text_bytes: usize,
    #[serde(default = "default_egress_slots")]
    pub egress_slots: usize,
    #[serde(default = "default_unauthenticated_quota_bytes")]
    pub unauthenticated_quota_bytes: usize,
    #[serde(default = "default_idle_timeout_ms")]
    pub idle_timeout_ms: u64,
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            listen_address: default_listen_address(),
            listen_port: default_listen_port(),
            max_socket_tasks: default_max_socket_tasks(),
            max_admissions_per_second: default_max_admissions_per_second(),
            handshake_timeout_ms: default_handshake_timeout_ms(),
            write_timeout_ms: default_write_timeout_ms(),
            max_wire_text_bytes: default_max_wire_text_bytes(),
            egress_slots: default_egress_slots(),
            unauthenticated_quota_bytes: default_unauthenticated_quota_bytes(),
            idle_timeout_ms: default_idle_timeout_ms(),
        }
    }
}

impl TransportConfig {
    /// Validates all range constraints, rejecting 0 or out-of-bounds numbers.
    ///
    /// # Errors
    /// Returns human-readable error on invalid parameter.
    pub fn validate(&self) -> Result<(), String> {
        if self.listen_address.trim().is_empty() {
            return Err("transport listen_address cannot be empty".into());
        }
        if self.max_socket_tasks == 0 || self.max_socket_tasks > 65_536 {
            return Err("transport max_socket_tasks must be between 1 and 65536".into());
        }
        if self.max_admissions_per_second == 0 || self.max_admissions_per_second > 100_000 {
            return Err("transport max_admissions_per_second must be between 1 and 100000".into());
        }
        if self.handshake_timeout_ms < 50 || self.handshake_timeout_ms > 60_000 {
            return Err("transport handshake_timeout_ms must be between 50 and 60000".into());
        }
        if self.write_timeout_ms < 50 || self.write_timeout_ms > 60_000 {
            return Err("transport write_timeout_ms must be between 50 and 60000".into());
        }
        if self.max_wire_text_bytes < 1024 || self.max_wire_text_bytes > 16_777_216 {
            return Err("transport max_wire_text_bytes must be between 1024 and 16777216".into());
        }
        if self.egress_slots == 0 || self.egress_slots > 1024 {
            return Err("transport egress_slots must be between 1 and 1024".into());
        }
        Ok(())
    }
}

/// Registered Close Reason Codes per ds-server M1⑤.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloseReasonCode {
    Shutdown,
    QueueFull,
    SendBufferOverflow,
    SessionClosed,
    BadEnvelope,
    ProtocolViolation,
    ConnectionTimeout,
    InputRateExceeded,
    NormalLogout,
    Superseded,
}

impl CloseReasonCode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Shutdown => "shutdown",
            Self::QueueFull => "queue_full",
            Self::SendBufferOverflow => "send_buffer_overflow",
            Self::SessionClosed => "session_closed",
            Self::BadEnvelope => "bad_envelope",
            Self::ProtocolViolation => "protocol_violation",
            Self::ConnectionTimeout => "connection_timeout",
            Self::InputRateExceeded => "input_rate_exceeded",
            Self::NormalLogout => "normal_logout",
            Self::Superseded => "superseded",
        }
    }

    #[must_use]
    pub fn close_code(self) -> CloseCode {
        match self {
            Self::Shutdown => CloseCode::Away,
            Self::NormalLogout | Self::SessionClosed | Self::Superseded => CloseCode::Normal,
            Self::QueueFull | Self::SendBufferOverflow => CloseCode::Size,
            Self::BadEnvelope
            | Self::ProtocolViolation
            | Self::InputRateExceeded
            | Self::ConnectionTimeout => CloseCode::Policy,
        }
    }

    #[must_use]
    pub fn into_close_frame(self) -> CloseFrame {
        CloseFrame {
            code: self.close_code(),
            reason: self.as_str().into(),
        }
    }
}

#[derive(Clone)]
pub struct WireSender {
    inner: Sender<WireOut>,
    cancel: CancelToken,
    observer_id: u64,
    wake: Arc<Notify>,
    max_text_bytes: usize,
}
#[derive(Debug, Clone)]
pub enum WireOut {
    Text(Vec<u8>),
    Close,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WireSendError {
    Full,
    Closed,
    TooLarge,
    InvalidUtf8,
}
impl WireSender {
    #[cfg(test)]
    pub fn try_send_text(&self, text: String) -> Result<(), WireSendError> {
        self.try_send_bytes(text.as_bytes())
    }
    pub fn try_send_bytes(&self, bytes: &[u8]) -> Result<(), WireSendError> {
        if self.cancel.is_cancelled() {
            return Err(WireSendError::Closed);
        }
        if bytes.len() > self.max_text_bytes {
            return Err(WireSendError::TooLarge);
        }
        std::str::from_utf8(bytes).map_err(|_| WireSendError::InvalidUtf8)?;
        self.inner
            .try_send(WireOut::Text(bytes.to_vec()))
            .map_err(map_send_error)?;
        self.wake.notify_one();
        Ok(())
    }
    pub fn try_close(&self) -> Result<(), WireSendError> {
        let result = self.try_close_ordered();
        if result.is_err() {
            self.abort();
        }
        result
    }
    pub(crate) fn try_close_ordered(&self) -> Result<(), WireSendError> {
        let result = self.inner.try_send(WireOut::Close).map_err(map_send_error);
        self.wake.notify_one();
        result
    }
    pub(crate) fn observer_id(&self) -> u64 {
        self.observer_id
    }
    pub(crate) fn is_closed(&self) -> bool {
        self.cancel.is_cancelled()
    }
    pub(crate) fn abort(&self) {
        self.cancel.cancel();
        self.wake.notify_one();
    }
}
#[cfg(test)]
pub(crate) fn test_sender_pair(
    capacity: usize,
) -> (WireSender, lumio_host_runtime::Receiver<WireOut>) {
    let (inner, rx) = bounded_channel(capacity);
    (
        WireSender {
            inner,
            cancel: CancelToken::new(),
            observer_id: NEXT_OBSERVER_ID.fetch_add(1, Ordering::Relaxed),
            wake: Arc::new(Notify::new()),
            max_text_bytes: MAX_WIRE_TEXT_BYTES,
        },
        rx,
    )
}
fn map_send_error(error: SendError<WireOut>) -> WireSendError {
    match error {
        SendError::Full(_) => WireSendError::Full,
        SendError::Closed(_) => WireSendError::Closed,
    }
}
pub(crate) enum WireEvent {
    Attached {
        connection_id: String,
        egress: WireSender,
    },
    Authenticated {
        connection_id: String,
        egress: WireSender,
        proof: VerifiedAdmission,
    },
    Input {
        connection_id: String,
        observer_id: u64,
        text: String,
    },
    Closed {
        connection_id: String,
        observer_id: u64,
    },
}
static NEXT_OBSERVER_ID: AtomicU64 = AtomicU64::new(1);

enum ListenerTask {
    Supervised(SupervisedTask),
    Tokio(tokio::task::JoinHandle<()>),
}

#[must_use]
#[allow(dead_code)]
#[cfg(any(test, feature = "test-harness"))]
pub fn is_wire_text_size_valid(text: &str) -> bool {
    text.as_bytes().len() <= MAX_WIRE_TEXT_BYTES
}

pub struct RoomListener {
    pub port: u16,
    task: ListenerTask,
    stop: watch::Sender<bool>,
}
impl RoomListener {
    #[allow(dead_code)]
    #[cfg(any(test, feature = "test-harness"))]
    pub(crate) fn bind(tx: Sender<WireEvent>) -> Result<Self, String> {
        Self::bind_with_config(tx, TransportConfig::default())
    }

    #[cfg(any(test, feature = "test-harness"))]
    pub(crate) fn bind_with_config(
        tx: Sender<WireEvent>,
        config: TransportConfig,
    ) -> Result<Self, String> {
        Self::start(tx, None, config)
    }

    #[allow(dead_code)]
    pub(crate) fn bind_authenticated(
        tx: Sender<WireEvent>,
        verifier: BoundAdmissionVerifier,
    ) -> Result<Self, String> {
        Self::bind_authenticated_with_config(tx, verifier, TransportConfig::default())
    }

    pub(crate) fn bind_authenticated_with_config(
        tx: Sender<WireEvent>,
        verifier: BoundAdmissionVerifier,
        config: TransportConfig,
    ) -> Result<Self, String> {
        Self::start(tx, Some(verifier), config)
    }

    fn start(
        tx: Sender<WireEvent>,
        verifier: Option<BoundAdmissionVerifier>,
        config: TransportConfig,
    ) -> Result<Self, String> {
        config.validate()?;
        let bind_addr = format!("{}:{}", config.listen_address, config.listen_port);
        let socket = TcpListener::bind(&bind_addr).map_err(|e| e.to_string())?;
        socket.set_nonblocking(true).map_err(|e| e.to_string())?;
        let port = socket.local_addr().map_err(|e| e.to_string())?.port();
        let (stop, stopped) = watch::channel(false);

        let reactor_logic = |listener: tokio::net::TcpListener,
                             cfg: TransportConfig,
                             event_tx: Sender<WireEvent>,
                             mut stop_rx: watch::Receiver<bool>,
                             ver: Option<BoundAdmissionVerifier>| async move {
            let mut connections = JoinSet::new();
            let mut admission_window = tokio::time::Instant::now();
            let mut admissions_in_window = 0_u32;
            loop {
                tokio::select! {
                    _ = stop_rx.changed() => break,
                    result = connections.join_next(), if !connections.is_empty() => {
                        if let Some(Err(_)) = result {}
                    }
                    accepted = listener.accept() => {
                        let (stream, _) = if let Ok(res) = accepted {
                            res
                        } else {
                            tokio::time::sleep(Duration::from_millis(20)).await;
                            continue;
                        };
                        if admission_window.elapsed() >= Duration::from_secs(1) {
                            admission_window = tokio::time::Instant::now();
                            admissions_in_window = 0;
                        }
                        if admissions_in_window >= cfg.max_admissions_per_second {
                            tokio::spawn(async move {
                                let mut stream = stream;
                                let _ = tokio::io::AsyncWriteExt::write_all(
                                    &mut stream,
                                    b"HTTP/1.1 429 Too Many Requests\r\nConnection: close\r\n\r\ninput_rate_exceeded",
                                ).await;
                            });
                            continue;
                        }
                        admissions_in_window += 1;
                        if connections.len() >= cfg.max_socket_tasks {
                            tokio::spawn(async move {
                                let mut stream = stream;
                                let _ = tokio::io::AsyncWriteExt::write_all(
                                    &mut stream,
                                    b"HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\n\r\nqueue_full",
                                ).await;
                            });
                            continue;
                        }
                        connections.spawn(run_socket(stream, cfg.clone(), event_tx.clone(), stop_rx.clone(), ver.clone()));
                    }
                }
            }
            let graceful_deadline = tokio::time::sleep(Duration::from_millis(250));
            tokio::pin!(graceful_deadline);
            loop {
                tokio::select! {
                    () = &mut graceful_deadline => break,
                    res = connections.join_next() => {
                        if res.is_none() { break; }
                    }
                }
            }
            connections.abort_all();
            while connections.join_next().await.is_some() {}
        };

        let task = if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let async_listener =
                tokio::net::TcpListener::from_std(socket).map_err(|e| e.to_string())?;
            let join_handle =
                handle.spawn(reactor_logic(async_listener, config, tx, stopped, verifier));
            ListenerTask::Tokio(join_handle)
        } else {
            let supervised = spawn_supervised("lumio-room-reactor", move |_| {
                let executor = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("socket reactor");
                executor.block_on(async move {
                    let async_listener =
                        tokio::net::TcpListener::from_std(socket).expect("async listener");
                    reactor_logic(async_listener, config, tx, stopped, verifier).await;
                });
            });
            ListenerTask::Supervised(supervised)
        };

        Ok(Self { port, task, stop })
    }
    #[must_use]
    pub fn uri(&self) -> String {
        format!("ws://127.0.0.1:{}", self.port)
    }
    #[must_use]
    pub fn is_healthy(&self) -> bool {
        match &self.task {
            ListenerTask::Supervised(t) => !t.is_finished() && t.failure().is_none(),
            ListenerTask::Tokio(h) => !h.is_finished(),
        }
    }
    pub fn shutdown(&self) {
        let _ = self.stop.send(true);
    }
}
impl Drop for RoomListener {
    fn drop(&mut self) {
        self.shutdown();
    }
}

struct SocketGuard {
    cancel: CancelToken,
    wake: Arc<Notify>,
}
impl Drop for SocketGuard {
    fn drop(&mut self) {
        self.cancel.cancel();
        self.wake.notify_one();
    }
}

fn upgrade_credential(request: &Request) -> Option<&str> {
    let header_values: Vec<_> = request.headers().get_all("Authorization").iter().collect();
    if header_values.len() > 1 {
        return None;
    }
    let authorization = header_values
        .first()
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    let offers = request
        .headers()
        .get("Sec-WebSocket-Protocol")
        .and_then(|v| v.to_str().ok());
    let mut tokens = offers
        .into_iter()
        .flat_map(|v| v.split(','))
        .filter_map(|v| v.trim().strip_prefix("lumio-admission."));
    let browser_token = tokens.next();
    if tokens.next().is_some()
        || (request.headers().contains_key("Authorization") && browser_token.is_some())
    {
        return None;
    }
    authorization.or(browser_token).filter(|v| !v.is_empty())
}

fn unauthorized() -> ErrorResponse {
    tokio_tungstenite::tungstenite::http::Response::builder()
        .status(401)
        .body(Some("unauthorized".to_owned()))
        .expect("fixed response")
}

#[expect(
    clippy::result_large_err,
    reason = "tungstenite Callback requires its concrete HTTP ErrorResponse"
)]
async fn run_socket(
    stream: tokio::net::TcpStream,
    config: TransportConfig,
    tx: Sender<WireEvent>,
    mut stop: watch::Receiver<bool>,
    verifier: Option<BoundAdmissionVerifier>,
) {
    let mut proof = None;
    let limits = WebSocketConfig::default()
        .read_buffer_size(4096)
        .write_buffer_size(0)
        .max_write_buffer_size(2 * config.max_wire_text_bytes + 1024)
        .max_message_size(Some(config.max_wire_text_bytes))
        .max_frame_size(Some(config.max_wire_text_bytes));
    let handshake = accept_hdr_async_with_config(
        stream,
        |request: &Request, mut response: Response| {
            if let Some(verifier) = &verifier {
                let credential = upgrade_credential(request).ok_or_else(unauthorized)?;
                proof = Some(verifier.verify(credential).map_err(|_| unauthorized())?);
            }
            if request
                .headers()
                .get("Sec-WebSocket-Protocol")
                .and_then(|v| v.to_str().ok())
                .is_some_and(|v| v.split(',').any(|item| item.trim() == "lumio.mvp.v0"))
            {
                response.headers_mut().insert(
                    "Sec-WebSocket-Protocol",
                    HeaderValue::from_static("lumio.mvp.v0"),
                );
            }
            Ok(response)
        },
        Some(limits),
    );
    let handshake_duration = Duration::from_millis(config.handshake_timeout_ms);
    let mut ws = tokio::select! {
        _ = stop.changed() => return,
        result = tokio::time::timeout(handshake_duration, handshake) => match result {
            Ok(Ok(ws)) => ws,
            _ => return,
        },
    };
    let connection_id = if proof.is_some() {
        let mut random = [0u8; 16];
        if getrandom::fill(&mut random).is_err() {
            let _ = ws
                .send(Message::Close(Some(
                    CloseReasonCode::Shutdown.into_close_frame(),
                )))
                .await;
            return;
        }
        format!("conn-{}", super::crypto::hex_lower(&random))
    } else {
        let first = tokio::select! {
            _ = stop.changed() => {
                let _ = ws.send(Message::Close(Some(CloseReasonCode::Shutdown.into_close_frame()))).await;
                return;
            }
            result = tokio::time::timeout(handshake_duration, ws.next()) => {
                if let Ok(Some(Ok(Message::Text(text)))) = result {
                    text
                } else {
                    let _ = ws.send(Message::Close(Some(CloseReasonCode::ConnectionTimeout.into_close_frame()))).await;
                    return;
                }
            }
        };
        let Some(id) = parse_connection_id(&first) else {
            let _ = ws
                .send(Message::Close(Some(
                    CloseReasonCode::ProtocolViolation.into_close_frame(),
                )))
                .await;
            return;
        };
        id
    };
    let (out, pending) = bounded_channel(config.egress_slots);
    let cancel = CancelToken::new();
    let wake = Arc::new(Notify::new());
    let _guard = SocketGuard {
        cancel: cancel.clone(),
        wake: wake.clone(),
    };
    let observer_id = NEXT_OBSERVER_ID.fetch_add(1, Ordering::Relaxed);
    let egress = WireSender {
        inner: out,
        cancel: cancel.clone(),
        observer_id,
        wake: wake.clone(),
        max_text_bytes: config.max_wire_text_bytes,
    };
    let event = match proof {
        Some(proof) => WireEvent::Authenticated {
            connection_id: connection_id.clone(),
            egress,
            proof,
        },
        None => WireEvent::Attached {
            connection_id: connection_id.clone(),
            egress,
        },
    };
    if tx.try_send(event).is_err() {
        let _ = ws
            .send(Message::Close(Some(
                CloseReasonCode::QueueFull.into_close_frame(),
            )))
            .await;
        return;
    }
    let (mut sink, mut input) = ws.split();
    let write_duration = Duration::from_millis(config.write_timeout_ms);
    let reason: CloseReasonCode = tokio::select! {
        _ = stop.changed() => CloseReasonCode::Shutdown,
        reason = read_inputs(&mut input, &tx, &connection_id, observer_id, config.max_wire_text_bytes) => reason,
        reason = write_frames(&mut sink, pending, &cancel, &wake, write_duration) => reason,
    };
    cancel.cancel();
    wake.notify_one();
    let close = Message::Close(Some(reason.into_close_frame()));
    let _ = tokio::time::timeout(Duration::from_millis(250), sink.send(close)).await;
    // If the control queue is full, the guard's closed flag is swept by the
    // owner. No blocking send can hold the reactor or process shutdown hostage.
    let _ = tx.try_send(WireEvent::Closed {
        connection_id,
        observer_id,
    });
}

type Socket = WebSocketStream<tokio::net::TcpStream>;
async fn read_inputs(
    input: &mut futures_util::stream::SplitStream<Socket>,
    tx: &Sender<WireEvent>,
    connection: &str,
    observer_id: u64,
    max_bytes: usize,
) -> CloseReasonCode {
    while let Some(frame) = input.next().await {
        match frame {
            Ok(Message::Text(text)) => {
                if text.len() > max_bytes {
                    return CloseReasonCode::BadEnvelope;
                }
                if tx
                    .try_send(WireEvent::Input {
                        connection_id: connection.to_owned(),
                        observer_id,
                        text: text.to_string(),
                    })
                    .is_err()
                {
                    return CloseReasonCode::QueueFull;
                }
            }
            Ok(Message::Ping(_) | Message::Pong(_)) => {}
            Ok(Message::Close(_)) => return CloseReasonCode::SessionClosed,
            _ => return CloseReasonCode::ProtocolViolation,
        }
    }
    CloseReasonCode::SessionClosed
}
async fn write_frames(
    sink: &mut futures_util::stream::SplitSink<Socket, Message>,
    pending: lumio_host_runtime::Receiver<WireOut>,
    cancel: &CancelToken,
    wake: &Notify,
    write_timeout: Duration,
) -> CloseReasonCode {
    loop {
        let notified = wake.notified();
        tokio::pin!(notified);
        let _ = notified.as_mut().enable();
        if cancel.is_cancelled() {
            return CloseReasonCode::SessionClosed;
        }
        let frame = match pending.try_recv() {
            Ok(frame) => frame,
            Err(RecvError::Empty) => {
                notified.await;
                continue;
            }
            Err(RecvError::Closed) => return CloseReasonCode::SessionClosed,
        };
        let message = match frame {
            WireOut::Text(bytes) => match String::from_utf8(bytes) {
                Ok(text) => Message::Text(text.into()),
                Err(_) => return CloseReasonCode::BadEnvelope,
            },
            WireOut::Close => return CloseReasonCode::SessionClosed,
        };
        if !matches!(
            tokio::time::timeout(write_timeout, sink.send(message)).await,
            Ok(Ok(()))
        ) {
            return CloseReasonCode::SendBufferOverflow;
        }
    }
}
fn parse_connection_id(text: &str) -> Option<String> {
    let value: Value = serde_json::from_str(text).ok()?;
    value
        .get("connectionId")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty() && s.len() <= 256)
        .map(str::to_owned)
}
#[cfg(test)]
mod close_reason_tests {
    use super::*;

    #[test]
    fn all_ten_close_reason_codes_have_distinct_str_and_valid_close_codes() {
        let codes = [
            CloseReasonCode::Shutdown,
            CloseReasonCode::QueueFull,
            CloseReasonCode::SendBufferOverflow,
            CloseReasonCode::SessionClosed,
            CloseReasonCode::BadEnvelope,
            CloseReasonCode::ProtocolViolation,
            CloseReasonCode::ConnectionTimeout,
            CloseReasonCode::InputRateExceeded,
            CloseReasonCode::NormalLogout,
            CloseReasonCode::Superseded,
        ];
        let mut strs = std::collections::HashSet::new();
        for code in codes {
            assert!(strs.insert(code.as_str()), "duplicate str for {code:?}");
            let frame = code.into_close_frame();
            assert_eq!(frame.code, code.close_code());
            assert_eq!(frame.reason.as_str(), code.as_str());
        }
        assert_eq!(strs.len(), 10);
    }

    #[test]
    fn transport_config_validation_rejects_out_of_range() {
        let mut cfg = TransportConfig::default();
        assert!(cfg.validate().is_ok());

        cfg.max_socket_tasks = 0;
        assert!(cfg.validate().is_err());
        cfg.max_socket_tasks = 256;

        cfg.listen_address = String::new();
        assert!(cfg.validate().is_err());
        cfg.listen_address = "127.0.0.1".into();

        cfg.handshake_timeout_ms = 10;
        assert!(cfg.validate().is_err());
    }
}

#[cfg(any(test, feature = "test-harness"))]
fn is_would_block(error: &tokio_tungstenite::tungstenite::Error) -> bool {
    matches!(error, tokio_tungstenite::tungstenite::Error::Io(io) if io.kind() == std::io::ErrorKind::WouldBlock || io.kind() == std::io::ErrorKind::TimedOut)
}
/// Test/harness client that actually receives frames.
#[cfg(any(test, feature = "test-harness"))]
pub struct RoomClient {
    ws: WebSocket<MaybeTlsStream<TcpStream>>,
    pub received: Vec<String>,
}

#[cfg(any(test, feature = "test-harness"))]
impl RoomClient {
    /// Connects and attaches as `connection_id`.
    ///
    /// # Errors
    ///
    /// Returns a human-readable failure.
    pub fn connect(uri: &str, connection_id: &str) -> Result<Self, String> {
        let url = format!("{uri}/");
        let (mut ws, _) =
            ws_connect(url).map_err(|error| format!("room client connect: {error}"))?;
        match ws.get_mut() {
            MaybeTlsStream::Plain(stream) => {
                stream
                    .set_read_timeout(Some(Duration::from_secs(3)))
                    .map_err(|error| format!("room client timeout: {error}"))?;
            }
            _ => {}
        }
        let hello = serde_json::json!({ "connectionId": connection_id }).to_string();
        ws.send(Message::Text(hello.into()))
            .map_err(|error| format!("room client hello: {error}"))?;
        Ok(Self {
            ws,
            received: Vec::new(),
        })
    }

    /// Test client for the authenticated HTTP Upgrade binding.
    pub fn connect_authenticated(uri: &str, credential: &str) -> Result<Self, String> {
        use tokio_tungstenite::tungstenite::client::IntoClientRequest;
        let mut request = uri.into_client_request().map_err(|e| e.to_string())?;
        request.headers_mut().insert(
            "Authorization",
            HeaderValue::from_str(&format!("Bearer {credential}")).map_err(|e| e.to_string())?,
        );
        let (mut ws, _) = ws_connect(request).map_err(|e| e.to_string())?;
        if let MaybeTlsStream::Plain(stream) = ws.get_mut() {
            stream
                .set_read_timeout(Some(Duration::from_secs(3)))
                .map_err(|e| e.to_string())?;
        }
        Ok(Self {
            ws,
            received: Vec::new(),
        })
    }

    /// Reads one text frame.
    ///
    /// # Errors
    ///
    /// Returns when the socket closes or times out.
    pub fn recv_text(&mut self) -> Result<String, String> {
        match self.ws.read() {
            Ok(Message::Text(text)) => {
                let owned = text.to_string();
                self.received.push(owned.clone());
                Ok(owned)
            }
            Ok(Message::Close(_)) => Err("closed".to_owned()),
            Ok(other) => Err(format!("unexpected {other}")),
            Err(error) => Err(error.to_string()),
        }
    }

    /// Non-blocking text read. `Ok(None)` means no frame is queued (WouldBlock / timeout).
    ///
    /// # Errors
    ///
    /// Returns when the socket closes or the read fails for a non-idle reason.
    pub fn try_recv_text(&mut self) -> Result<Option<String>, String> {
        self.set_nonblocking(true)?;
        let result = loop {
            match self.ws.read() {
                Ok(Message::Text(text)) => {
                    let owned = text.to_string();
                    self.received.push(owned.clone());
                    break Ok(Some(owned));
                }
                Ok(Message::Close(_)) => break Err("closed".to_owned()),
                Ok(Message::Ping(_) | Message::Pong(_) | Message::Frame(_)) => {}
                Ok(other) => break Err(format!("unexpected {other}")),
                Err(error) if is_would_block(&error) => break Ok(None),
                Err(error) => break Err(error.to_string()),
            }
        };
        let _ = self.set_nonblocking(false);
        result
    }

    fn set_nonblocking(&mut self, nonblocking: bool) -> Result<(), String> {
        match self.ws.get_mut() {
            MaybeTlsStream::Plain(stream) => {
                stream
                    .set_nonblocking(nonblocking)
                    .map_err(|error| format!("room client nonblocking: {error}"))?;
                if !nonblocking {
                    stream
                        .set_read_timeout(Some(Duration::from_secs(3)))
                        .map_err(|error| format!("room client timeout: {error}"))?;
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    /// Sends a C-1 InputCommand (or other JSON) text frame.
    ///
    /// # Errors
    ///
    /// Returns a send failure.
    pub fn send_text(&mut self, text: &str) -> Result<(), String> {
        self.ws
            .send(Message::Text(text.to_owned().into()))
            .map_err(|error| error.to_string())
    }

    /// True when a later `recv_text` observes close.
    #[must_use]
    pub fn is_closed_after(&mut self) -> bool {
        matches!(self.ws.read(), Ok(Message::Close(_)) | Err(_))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_sender_reports_full_without_blocking() {
        let (tx, _rx) = bounded_channel(1);
        let sender = WireSender {
            inner: tx,
            cancel: CancelToken::new(),
            observer_id: NEXT_OBSERVER_ID.fetch_add(1, Ordering::Relaxed),
            wake: Arc::new(Notify::new()),
            max_text_bytes: MAX_WIRE_TEXT_BYTES,
        };
        sender
            .try_send_text("first".to_owned())
            .expect("first slot");

        assert_eq!(
            sender.try_send_text("second".to_owned()),
            Err(WireSendError::Full)
        );
    }

    #[test]
    fn wire_sender_reports_closed_receiver() {
        let (tx, rx) = bounded_channel(1);
        let sender = WireSender {
            inner: tx,
            cancel: CancelToken::new(),
            observer_id: NEXT_OBSERVER_ID.fetch_add(1, Ordering::Relaxed),
            wake: Arc::new(Notify::new()),
            max_text_bytes: MAX_WIRE_TEXT_BYTES,
        };
        drop(rx);

        assert_eq!(
            sender.try_send_text("frame".to_owned()),
            Err(WireSendError::Closed)
        );
    }

    #[test]
    fn wire_sender_close_reports_full_without_blocking() {
        let (tx, _rx) = bounded_channel(1);
        let cancel = CancelToken::new();
        let sender = WireSender {
            inner: tx,
            cancel: cancel.clone(),
            observer_id: NEXT_OBSERVER_ID.fetch_add(1, Ordering::Relaxed),
            wake: Arc::new(Notify::new()),
            max_text_bytes: MAX_WIRE_TEXT_BYTES,
        };
        sender
            .try_send_text("frame".to_owned())
            .expect("first slot");

        assert_eq!(sender.try_close(), Err(WireSendError::Full));
        assert!(cancel.is_cancelled());
    }

    #[test]
    fn wire_sender_close_reports_closed_receiver() {
        let (tx, rx) = bounded_channel(1);
        let sender = WireSender {
            inner: tx,
            cancel: CancelToken::new(),
            observer_id: NEXT_OBSERVER_ID.fetch_add(1, Ordering::Relaxed),
            wake: Arc::new(Notify::new()),
            max_text_bytes: MAX_WIRE_TEXT_BYTES,
        };
        drop(rx);

        assert_eq!(sender.try_close(), Err(WireSendError::Closed));
    }

    #[test]
    fn wire_sender_accepts_exact_text_byte_limit() {
        let (tx, rx) = bounded_channel(1);
        let sender = WireSender {
            inner: tx,
            cancel: CancelToken::new(),
            observer_id: NEXT_OBSERVER_ID.fetch_add(1, Ordering::Relaxed),
            wake: Arc::new(Notify::new()),
            max_text_bytes: MAX_WIRE_TEXT_BYTES,
        };

        sender
            .try_send_bytes(&vec![b'a'; MAX_WIRE_TEXT_BYTES])
            .expect("exact limit");
        assert!(matches!(rx.recv(), Ok(WireOut::Text(text)) if text.len() == MAX_WIRE_TEXT_BYTES));
    }

    #[test]
    fn wire_sender_rejects_text_over_byte_limit() {
        let (tx, _rx) = bounded_channel(1);
        let sender = WireSender {
            inner: tx,
            cancel: CancelToken::new(),
            observer_id: NEXT_OBSERVER_ID.fetch_add(1, Ordering::Relaxed),
            wake: Arc::new(Notify::new()),
            max_text_bytes: MAX_WIRE_TEXT_BYTES,
        };

        assert_eq!(
            sender.try_send_bytes(&vec![b'a'; MAX_WIRE_TEXT_BYTES + 1]),
            Err(WireSendError::TooLarge)
        );
    }

    #[test]
    fn wire_sender_rejects_invalid_utf8() {
        let (tx, _rx) = bounded_channel(1);
        let sender = WireSender {
            inner: tx,
            cancel: CancelToken::new(),
            observer_id: NEXT_OBSERVER_ID.fetch_add(1, Ordering::Relaxed),
            wake: Arc::new(Notify::new()),
            max_text_bytes: MAX_WIRE_TEXT_BYTES,
        };

        assert_eq!(
            sender.try_send_bytes(&[0xff]),
            Err(WireSendError::InvalidUtf8)
        );
    }

    #[test]
    fn inbound_wire_text_accepts_exact_byte_limit() {
        assert!(is_wire_text_size_valid(&"a".repeat(MAX_WIRE_TEXT_BYTES)));
    }

    #[test]
    fn inbound_wire_text_rejects_over_byte_limit() {
        assert!(!is_wire_text_size_valid(
            &"a".repeat(MAX_WIRE_TEXT_BYTES + 1)
        ));
    }

    #[test]
    fn inbound_wire_text_limit_counts_utf8_bytes() {
        let exact = "é".repeat(MAX_WIRE_TEXT_BYTES / 2);
        let over = format!("{exact}é");
        assert_eq!(exact.as_bytes().len(), MAX_WIRE_TEXT_BYTES);
        assert!(is_wire_text_size_valid(&exact));
        assert!(!is_wire_text_size_valid(&over));
    }
}
