//! A connection: who is on the other end, its streams and datagrams, how it closes, and which path
//! it currently takes.

use bytes::Bytes;
use iroh::{
    TransportAddr,
    endpoint::{self, ConnectionError, VarInt},
};
use napi::bindgen_prelude::*;
use napi_derive::napi;

use crate::{
    js_err,
    stream::{RecvStream, SendStream},
};

/// How a connection ended; what `Connection.closed()` resolves with.
#[napi(object)]
pub struct CloseInfo {
    /// The application close code when either side closed with one; null otherwise.
    pub code: Option<u32>,
    /// The application close reason (UTF-8, invalid bytes replaced), or the transport error text.
    pub reason: String,
    /// One of "application", "locally_closed", "timed_out", "reset", "transport".
    pub kind: String,
}

impl From<ConnectionError> for CloseInfo {
    fn from(error: ConnectionError) -> Self {
        if let ConnectionError::ApplicationClosed(close) = &error {
            return Self {
                code: Some(close.error_code.into_inner() as u32),
                reason: String::from_utf8_lossy(&close.reason).into_owned(),
                kind: "application".to_owned(),
            };
        }
        let kind = match error {
            ConnectionError::LocallyClosed => "locally_closed",
            ConnectionError::TimedOut => "timed_out",
            ConnectionError::Reset => "reset",
            _ => "transport",
        };
        Self { code: None, reason: error.to_string(), kind: kind.to_owned() }
    }
}

/// Traffic counters of a connection, over all of its paths together.
#[napi(object)]
pub struct ConnectionStats {
    /// Bytes received in UDP datagrams, QUIC overhead included.
    pub udp_rx_bytes: f64,
    /// Bytes sent in UDP datagrams, QUIC overhead included.
    pub udp_tx_bytes: f64,
    /// UDP datagrams received.
    pub udp_rx_datagrams: f64,
    /// UDP datagrams sent.
    pub udp_tx_datagrams: f64,
    /// QUIC packets declared lost.
    pub lost_packets: f64,
}

/// An established QUIC connection.
#[napi]
pub struct Connection(endpoint::Connection);

impl Connection {
    pub(crate) fn new(inner: endpoint::Connection) -> Self {
        Self(inner)
    }

    // Applies `f` to the path the connection is using now, if any.
    fn selected_path<T>(&self, f: impl FnOnce(endpoint::Path<'_>) -> T) -> Option<T> {
        let paths = self.0.paths();
        paths.iter().find(|path| path.is_selected()).map(f)
    }
}

/// Both halves of a new stream, handed to JS as `{ send, recv }`.
pub struct BiStream {
    send: SendStream,
    recv: RecvStream,
}

impl BiStream {
    fn from_quic((send, recv): (endpoint::SendStream, endpoint::RecvStream)) -> Self {
        Self { send: SendStream::new(send), recv: RecvStream::new(recv) }
    }
}

impl ToNapiValue for BiStream {
    unsafe fn to_napi_value(env: sys::napi_env, value: Self) -> Result<sys::napi_value> {
        let mut pair = Object::new(&Env::from_raw(env))?;
        pair.set("send", value.send)?;
        pair.set("recv", value.recv)?;
        // SAFETY: `env` is the live environment napi handed to this conversion.
        unsafe { Object::to_napi_value(env, pair) }
    }
}

impl TypeName for BiStream {
    fn type_name() -> &'static str {
        "BiStream"
    }
    fn value_type() -> ValueType {
        ValueType::Object
    }
}

#[napi]
impl Connection {
    /// The peer's endpoint id (lowercase hex), proven by the TLS handshake.
    #[napi]
    pub fn remote_id(&self) -> String {
        self.0.remote_id().to_string()
    }

    /// The ALPN protocol both sides agreed on.
    #[napi]
    pub fn alpn(&self) -> Buffer {
        Buffer::from(self.0.alpn().to_vec())
    }

    /// Opens a bidirectional stream.
    #[napi(ts_return_type = "Promise<{ send: SendStream, recv: RecvStream }>")]
    pub async fn open_bi(&self) -> Result<BiStream> {
        let halves = self.0.open_bi().await.map_err(js_err)?;
        Ok(BiStream::from_quic(halves))
    }

    /// Takes the next bidirectional stream the peer opened; rejects once the connection is closed.
    #[napi(ts_return_type = "Promise<{ send: SendStream, recv: RecvStream }>")]
    pub async fn accept_bi(&self) -> Result<BiStream> {
        let halves = self.0.accept_bi().await.map_err(js_err)?;
        Ok(BiStream::from_quic(halves))
    }

    /// How many peer-opened bidirectional streams may be open at the same time (stream credit).
    #[napi]
    pub fn set_max_concurrent_bi_streams(&self, n: u32) {
        self.0.set_max_concurrent_bi_streams(n.into());
    }

    /// Closes with an application code and a reason given as text or bytes.
    #[napi]
    pub fn close(&self, code: u32, reason: Either<String, Uint8Array>) {
        let reason = match &reason {
            Either::A(text) => text.as_bytes(),
            Either::B(bytes) => bytes.as_ref(),
        };
        self.0.close(VarInt::from_u32(code), reason);
    }

    /// Resolves with the close code and reason once the connection has ended.
    #[napi]
    pub async fn closed(&self) -> CloseInfo {
        CloseInfo::from(self.0.closed().await)
    }

    /// "direct" when the path in use is UDP, "relay" when it goes through the relay, else "none".
    #[napi]
    pub fn path_kind(&self) -> String {
        let kind = self.selected_path(|path| {
            if path.is_ip() {
                "direct"
            } else if path.is_relay() {
                "relay"
            } else {
                "none"
            }
        });
        match kind {
            Some(kind) => kind.to_owned(),
            // No path is selected yet.
            None => "none".to_owned(),
        }
    }

    /// The peer's `host:port` when the path in use is direct, else null.
    #[napi]
    pub fn direct_remote_addr(&self) -> Option<String> {
        self.selected_path(|path| match path.remote_addr() {
            TransportAddr::Ip(addr) => Some(addr.to_string()),
            _ => None,
        })
        .flatten()
    }

    /// Traffic counters since the connection was established.
    #[napi]
    pub fn stats(&self) -> ConnectionStats {
        let stats = self.0.stats();
        ConnectionStats {
            udp_rx_bytes: stats.udp_rx.bytes as f64,
            udp_tx_bytes: stats.udp_tx.bytes as f64,
            udp_rx_datagrams: stats.udp_rx.datagrams as f64,
            udp_tx_datagrams: stats.udp_tx.datagrams as f64,
            lost_packets: stats.lost_packets as f64,
        }
    }

    /// The largest payload `sendDatagram` accepts right now; null when the peer takes no
    /// datagrams.
    #[napi]
    pub fn max_datagram_size(&self) -> Option<u32> {
        let size = self.0.max_datagram_size()?;
        Some(size as u32)
    }

    /// Sends one unreliable datagram (copied once). Throws when it is too large, the peer takes no
    /// datagrams or the connection is closed.
    #[napi]
    pub fn send_datagram(&self, data: &[u8]) -> Result<()> {
        let payload = Bytes::copy_from_slice(data);
        self.0.send_datagram(payload).map_err(js_err)
    }

    /// Resolves with the next datagram received; rejects once the connection is closed.
    #[napi]
    pub async fn read_datagram(&self) -> Result<Buffer> {
        match self.0.read_datagram().await {
            Ok(datagram) => Ok(Buffer::from(datagram.to_vec())),
            Err(error) => Err(js_err(error)),
        }
    }

    /// Round-trip time of the path in use, in milliseconds, when known.
    #[napi]
    pub fn rtt_ms(&self) -> Option<f64> {
        self.selected_path(|path| path.rtt().as_secs_f64() * 1e3)
    }
}
