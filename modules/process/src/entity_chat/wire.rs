//! Loopback WebSocket Room wire. Bytes on the socket are Runtime/C-1 JSON.

use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::Duration;

use lumio_host_runtime::{
    bounded_channel, spawn_supervised, CancelToken, RecvError, SendError, Sender, SupervisedTask,
};
use serde_json::Value;
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
use tokio_tungstenite::tungstenite::http::header::SEC_WEBSOCKET_PROTOCOL;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::protocol::WebSocket;
use tokio_tungstenite::tungstenite::stream::MaybeTlsStream;
use tokio_tungstenite::tungstenite::{accept_hdr, client::connect as ws_connect, Message};

pub const MAX_WIRE_TEXT_BYTES: usize = 65_536;

/// Egress to one accepted socket.
#[derive(Clone)]
pub struct WireSender {
    inner: Sender<WireOut>,
    cancel: CancelToken,
    observer_id: u64,
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
    pub fn try_send_text(&self, text: String) -> Result<(), WireSendError> {
        self.try_send_bytes(text.as_bytes())
    }

    pub fn try_send_bytes(&self, bytes: &[u8]) -> Result<(), WireSendError> {
        if bytes.len() > MAX_WIRE_TEXT_BYTES {
            return Err(WireSendError::TooLarge);
        }
        std::str::from_utf8(bytes).map_err(|_| WireSendError::InvalidUtf8)?;
        self.inner
            .try_send(WireOut::Text(bytes.to_vec()))
            .map_err(map_send_error)
    }

    pub fn try_close(&self) -> Result<(), WireSendError> {
        match self.inner.try_send(WireOut::Close) {
            Ok(()) => Ok(()),
            Err(error) => {
                let error = map_send_error(error);
                self.cancel.cancel();
                Err(error)
            }
        }
    }

    pub(crate) fn try_close_ordered(&self) -> Result<(), WireSendError> {
        self.inner.try_send(WireOut::Close).map_err(map_send_error)
    }

    pub(crate) fn observer_id(&self) -> u64 {
        self.observer_id
    }

    pub(crate) fn abort(&self) {
        self.cancel.cancel();
    }
}

#[cfg(test)]
pub(crate) fn test_sender_pair(
    capacity: usize,
) -> (WireSender, lumio_host_runtime::Receiver<WireOut>) {
    let (inner, receiver) = bounded_channel(capacity);
    (
        WireSender {
            inner,
            cancel: CancelToken::new(),
            observer_id: NEXT_OBSERVER_ID.fetch_add(1, Ordering::Relaxed),
        },
        receiver,
    )
}

fn map_send_error(error: SendError<WireOut>) -> WireSendError {
    match error {
        SendError::Full(_) => WireSendError::Full,
        SendError::Closed(_) => WireSendError::Closed,
    }
}

/// Owner-thread notice from a socket.
pub enum WireEvent {
    Attached {
        connection_id: String,
        egress: WireSender,
    },
    Input {
        connection_id: String,
        text: String,
    },
    Closed {
        connection_id: String,
        observer_id: u64,
    },
}

static NEXT_OBSERVER_ID: AtomicU64 = AtomicU64::new(1);

/// Loopback listener. Handshake is blocking; frames are polled.
pub struct RoomListener {
    pub port: u16,
    _accept: SupervisedTask,
}

impl RoomListener {
    /// Binds `127.0.0.1:0` and accepts connections on a supervised thread.
    pub fn bind(event_tx: Sender<WireEvent>) -> Result<Self, String> {
        let listener = TcpListener::bind("127.0.0.1:0")
            .map_err(|error| format!("BLOCKED: room wire bind: {error}"))?;
        let port = listener
            .local_addr()
            .map_err(|error| format!("BLOCKED: room wire addr: {error}"))?
            .port();
        listener
            .set_nonblocking(true)
            .map_err(|error| format!("BLOCKED: room wire nonblocking: {error}"))?;
        let accept = spawn_supervised("lumio-entity-chat-wire-accept", move |cancel| {
            let mut conns = Vec::new();
            loop {
                if cancel.is_cancelled() {
                    break;
                }
                match listener.accept() {
                    Ok((stream, _)) => {
                        let tx = event_tx.clone();
                        conns.push(spawn_supervised(
                            "lumio-entity-chat-wire-conn",
                            move |conn_cancel| {
                                handle_conn(stream, tx, conn_cancel);
                            },
                        ));
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(10));
                    }
                    Err(_) => break,
                }
            }
        });
        Ok(Self {
            port,
            _accept: accept,
        })
    }

    #[must_use]
    pub fn uri(&self) -> String {
        format!("ws://127.0.0.1:{}", self.port)
    }
}

#[allow(clippy::result_large_err)]
fn handle_conn(stream: TcpStream, event_tx: Sender<WireEvent>, cancel: CancelToken) {
    if stream.set_nonblocking(false).is_err() {
        return;
    }
    let mut ws = match accept_hdr(stream, |request: &Request, mut response: Response| {
        let offers_mvp = request
            .headers()
            .get(SEC_WEBSOCKET_PROTOCOL)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.split(',').any(|item| item.trim() == "lumio.mvp.v0"));
        if offers_mvp {
            response.headers_mut().insert(
                SEC_WEBSOCKET_PROTOCOL,
                HeaderValue::from_static("lumio.mvp.v0"),
            );
        }
        Ok(response)
    }) {
        Ok(ws) => ws,
        Err(_) => return,
    };
    let first = match ws.read() {
        Ok(Message::Text(text)) if is_wire_text_size_valid(&text) => text,
        _ => return,
    };
    let connection_id = match parse_connection_id(&first) {
        Some(id) => id,
        None => return,
    };
    let (out_tx, out_rx) = bounded_channel(64);
    let egress = WireSender {
        inner: out_tx,
        cancel: cancel.clone(),
        observer_id: NEXT_OBSERVER_ID.fetch_add(1, Ordering::Relaxed),
    };
    let observer_id = egress.observer_id();
    if event_tx
        .send(WireEvent::Attached {
            connection_id: connection_id.clone(),
            egress,
        })
        .is_err()
    {
        return;
    }
    if ws.get_mut().set_nonblocking(true).is_err() {
        return;
    }
    loop {
        if cancel.is_cancelled() {
            break;
        }
        match out_rx.try_recv() {
            Ok(WireOut::Text(bytes)) => {
                let Ok(text) = String::from_utf8(bytes) else {
                    let _ = ws.close(None);
                    break;
                };
                if ws.send(Message::Text(text.into())).is_err() {
                    break;
                }
            }
            Ok(WireOut::Close) => {
                let _ = ws.close(None);
                break;
            }
            Err(RecvError::Empty) => {}
            Err(RecvError::Closed) => break,
        }
        match ws.read() {
            Ok(Message::Text(text)) if is_wire_text_size_valid(&text) => {
                if event_tx
                    .send(WireEvent::Input {
                        connection_id: connection_id.clone(),
                        text: text.to_string(),
                    })
                    .is_err()
                {
                    break;
                }
            }
            Ok(Message::Text(_)) => {
                let _ = ws.close(None);
                break;
            }
            Ok(Message::Close(_)) => break,
            Ok(Message::Ping(payload)) => {
                let _ = ws.send(Message::Pong(payload));
            }
            Ok(_) => {}
            Err(error) if is_would_block(&error) => {}
            Err(_) => break,
        }
        thread::sleep(Duration::from_millis(5));
    }
    let _ = event_tx.send(WireEvent::Closed {
        connection_id,
        observer_id,
    });
}

fn is_would_block(error: &tokio_tungstenite::tungstenite::Error) -> bool {
    matches!(
        error,
        tokio_tungstenite::tungstenite::Error::Io(io) if io.kind() == std::io::ErrorKind::WouldBlock
            || io.kind() == std::io::ErrorKind::TimedOut
    )
}

fn parse_connection_id(text: &str) -> Option<String> {
    let value: Value = serde_json::from_str(text).ok()?;
    value
        .get("connectionId")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
        .map(str::to_owned)
}

fn is_wire_text_size_valid(text: &str) -> bool {
    text.as_bytes().len() <= MAX_WIRE_TEXT_BYTES
}

/// Test/harness client that actually receives frames.
pub struct RoomClient {
    ws: WebSocket<MaybeTlsStream<TcpStream>>,
    pub received: Vec<String>,
}

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
