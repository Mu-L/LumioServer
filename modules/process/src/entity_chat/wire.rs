//! Bounded asynchronous socket reactor. The normal host requires an
//! allocation-bound bearer at HTTP Upgrade; arbitrary connectionId attachment
//! exists only in the explicit test-harness constructor. Bind stays loopback:
//! the deployment edge must supply WSS and forward Authorization securely.
use super::secure::{BoundAdmissionVerifier, VerifiedAdmission};
use futures_util::{SinkExt, StreamExt};
use lumio_host_runtime::{
    bounded_channel, spawn_supervised, CancelToken, RecvError, SendError, Sender, SupervisedTask,
};
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
const MAX_SOCKET_TASKS: usize = 256;
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(3);
const WRITE_TIMEOUT: Duration = Duration::from_secs(3);
const EGRESS_SLOTS: usize = 16;

#[derive(Clone)]
pub struct WireSender {
    inner: Sender<WireOut>,
    cancel: CancelToken,
    observer_id: u64,
    wake: Arc<Notify>,
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
        if bytes.len() > MAX_WIRE_TEXT_BYTES {
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

pub struct RoomListener {
    pub port: u16,
    task: SupervisedTask,
    stop: watch::Sender<bool>,
}
impl RoomListener {
    #[cfg(any(test, feature = "test-harness"))]
    pub(crate) fn bind(tx: Sender<WireEvent>) -> Result<Self, String> {
        Self::start(tx, None)
    }
    pub(crate) fn bind_authenticated(
        tx: Sender<WireEvent>,
        verifier: BoundAdmissionVerifier,
    ) -> Result<Self, String> {
        Self::start(tx, Some(verifier))
    }
    fn start(
        tx: Sender<WireEvent>,
        verifier: Option<BoundAdmissionVerifier>,
    ) -> Result<Self, String> {
        let socket = TcpListener::bind("127.0.0.1:0").map_err(|e| e.to_string())?;
        socket.set_nonblocking(true).map_err(|e| e.to_string())?;
        let port = socket.local_addr().map_err(|e| e.to_string())?.port();
        let (stop, mut stopped) = watch::channel(false);
        let task = spawn_supervised("lumio-room-reactor", move |_| {
            let executor = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("socket reactor");
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
    pub fn uri(&self) -> String {
        format!("ws://127.0.0.1:{}", self.port)
    }
    #[must_use]
    pub fn is_healthy(&self) -> bool {
        !self.task.is_finished() && self.task.failure().is_none()
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
    tx: Sender<WireEvent>,
    mut stop: watch::Receiver<bool>,
    verifier: Option<BoundAdmissionVerifier>,
) {
    let mut proof = None;
    let limits = WebSocketConfig::default()
        .read_buffer_size(4096)
        .write_buffer_size(0)
        .max_write_buffer_size(2 * MAX_WIRE_TEXT_BYTES + 1024)
        .max_message_size(Some(MAX_WIRE_TEXT_BYTES))
        .max_frame_size(Some(MAX_WIRE_TEXT_BYTES));
    let handshake = accept_hdr_async_with_config(
        stream,
        |request: &Request, mut response: Response| {
            if let Some(verifier) = &verifier {
                let header = request
                    .headers()
                    .get("Authorization")
                    .and_then(|v| v.to_str().ok())
                    .ok_or_else(unauthorized)?;
                let credential = header.strip_prefix("Bearer ").ok_or_else(unauthorized)?;
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
    let mut ws = tokio::select! {
        _ = stop.changed() => return,
        result = tokio::time::timeout(HANDSHAKE_TIMEOUT, handshake) => match result { Ok(Ok(ws)) => ws, _ => return },
    };
    let connection_id = if proof.is_some() {
        let mut random = [0u8; 16];
        if getrandom::fill(&mut random).is_err() {
            return;
        }
        format!("conn-{}", super::crypto::hex_lower(&random))
    } else {
        let first = tokio::select! {
            _ = stop.changed() => return,
            result = tokio::time::timeout(HANDSHAKE_TIMEOUT, ws.next()) => match result {
                Ok(Some(Ok(Message::Text(text)))) => text,
                _ => return,
            },
        };
        let Some(id) = parse_connection_id(&first) else {
            return;
        };
        id
    };
    let (out, pending) = bounded_channel(EGRESS_SLOTS);
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
        return;
    }
    let (mut sink, mut input) = ws.split();
    let reason = tokio::select! {
        _ = stop.changed() => "shutdown",
        reason = read_inputs(&mut input, &tx, &connection_id, observer_id) => reason,
        reason = write_frames(&mut sink, pending, &cancel, &wake) => reason,
    };
    cancel.cancel();
    wake.notify_one();
    let close = Message::Close(Some(CloseFrame {
        code: CloseCode::Policy,
        reason: reason.into(),
    }));
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
) -> &'static str {
    while let Some(frame) = input.next().await {
        match frame {
            Ok(Message::Text(text)) if is_wire_text_size_valid(&text) => {
                if tx
                    .try_send(WireEvent::Input {
                        connection_id: connection.to_owned(),
                        observer_id,
                        text: text.to_string(),
                    })
                    .is_err()
                {
                    return "queue_full";
                }
            }
            Ok(Message::Ping(_) | Message::Pong(_)) => {}
            Ok(Message::Close(_)) => return "session_closed",
            _ => return "bad_envelope",
        }
    }
    "session_closed"
}
async fn write_frames(
    sink: &mut futures_util::stream::SplitSink<Socket, Message>,
    pending: lumio_host_runtime::Receiver<WireOut>,
    cancel: &CancelToken,
    wake: &Notify,
) -> &'static str {
    loop {
        let notified = wake.notified();
        tokio::pin!(notified);
        let _ = notified.as_mut().enable();
        if cancel.is_cancelled() {
            return "session_closed";
        }
        let frame = match pending.try_recv() {
            Ok(frame) => frame,
            Err(RecvError::Empty) => {
                notified.await;
                continue;
            }
            Err(RecvError::Closed) => return "session_closed",
        };
        let message = match frame {
            WireOut::Text(bytes) => match String::from_utf8(bytes) {
                Ok(text) => Message::Text(text.into()),
                Err(_) => return "bad_envelope",
            },
            WireOut::Close => return "session_closed",
        };
        if !matches!(
            tokio::time::timeout(WRITE_TIMEOUT, sink.send(message)).await,
            Ok(Ok(()))
        ) {
            return "queue_full";
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
fn is_wire_text_size_valid(text: &str) -> bool {
    text.len() <= MAX_WIRE_TEXT_BYTES
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
