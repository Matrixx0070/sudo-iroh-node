//! Connections: admission identity, stream open/accept, close with code + reason.

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

/// How a connection ended, as returned by `Connection.closed()`.
#[napi(object)]
pub struct CloseInfo {
    /// Application close code, when the peer or we closed with one; otherwise null.
    pub code: Option<u32>,
    /// Application close reason (UTF-8, lossy) or the transport error text.
    pub reason: String,
    /// "application", "locally_closed", "timed_out", "reset" or "transport".
    pub kind: String,
}

/// Traffic counters of a connection, summed over all its paths.
#[napi(object)]
pub struct ConnectionStats {
    /// Bytes received in UDP datagrams (QUIC overhead included).
    pub udp_rx_bytes: f64,
    /// Bytes sent in UDP datagrams (QUIC overhead included).
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
    pub(crate) fn new(c: endpoint::Connection) -> Self {
        Self(c)
    }
}

/// Opened/accepted stream halves before they become JS objects.
pub struct RawBi(SendStream, RecvStream);

impl ToNapiValue for RawBi {
    unsafe fn to_napi_value(env: sys::napi_env, v: Self) -> Result<sys::napi_value> {
        let env_w = Env::from_raw(env);
        let mut obj = Object::new(&env_w)?;
        obj.set("send", v.0)?;
        obj.set("recv", v.1)?;
        unsafe { Object::to_napi_value(env, obj) }
    }
}

impl TypeName for RawBi {
    fn type_name() -> &'static str {
        "BiStream"
    }
    fn value_type() -> ValueType {
        ValueType::Object
    }
}

#[napi]
impl Connection {
    /// Remote endpoint id (lowercase hex), authenticated by the TLS handshake.
    #[napi]
    pub fn remote_id(&self) -> String {
        self.0.remote_id().to_string()
    }

    /// The ALPN protocol the handshake agreed on.
    #[napi]
    pub fn alpn(&self) -> Buffer {
        self.0.alpn().to_vec().into()
    }

    /// Opens a bidirectional stream.
    #[napi(ts_return_type = "Promise<{ send: SendStream, recv: RecvStream }>")]
    pub async fn open_bi(&self) -> Result<RawBi> {
        let (s, r) = self.0.open_bi().await.map_err(js_err)?;
        Ok(RawBi(SendStream::new(s), RecvStream::new(r)))
    }

    /// Accepts the next peer-opened bidirectional stream; rejects once the connection is closed.
    #[napi(ts_return_type = "Promise<{ send: SendStream, recv: RecvStream }>")]
    pub async fn accept_bi(&self) -> Result<RawBi> {
        let (s, r) = self.0.accept_bi().await.map_err(js_err)?;
        Ok(RawBi(SendStream::new(s), RecvStream::new(r)))
    }

    /// Sets how many peer-opened bidi streams may be open at once (QUIC stream credit).
    #[napi]
    pub fn set_max_concurrent_bi_streams(&self, n: u32) {
        self.0.set_max_concurrent_bi_streams(VarInt::from_u32(n));
    }

    /// Closes the connection with an application code and reason (string or bytes).
    #[napi]
    pub fn close(&self, code: u32, reason: Either<String, Uint8Array>) {
        let bytes: Vec<u8> = match reason {
            Either::A(s) => s.into_bytes(),
            Either::B(b) => b.to_vec(),
        };
        self.0.close(VarInt::from_u32(code), &bytes);
    }

    /// Resolves with the numeric close code and reason once the connection ends.
    #[napi]
    pub async fn closed(&self) -> CloseInfo {
        close_info(self.0.closed().await)
    }

    /// "direct" if the selected path is UDP, "relay" if it goes through the relay, else "none".
    #[napi]
    pub fn path_kind(&self) -> String {
        let paths = self.0.paths();
        match paths.iter().find(|p| p.is_selected()) {
            Some(p) if p.is_ip() => "direct".into(),
            Some(p) if p.is_relay() => "relay".into(),
            _ => "none".into(),
        }
    }

    /// Remote socket address (`host:port`) of the selected path when it is direct, else null.
    #[napi]
    pub fn direct_remote_addr(&self) -> Option<String> {
        let paths = self.0.paths();
        let selected = paths.iter().find(|p| p.is_selected())?;
        match selected.remote_addr() {
            TransportAddr::Ip(addr) => Some(addr.to_string()),
            _ => None,
        }
    }

    /// Traffic counters since the connection was established.
    #[napi]
    pub fn stats(&self) -> ConnectionStats {
        let s = self.0.stats();
        ConnectionStats {
            udp_rx_bytes: s.udp_rx.bytes as f64,
            udp_tx_bytes: s.udp_tx.bytes as f64,
            udp_rx_datagrams: s.udp_rx.datagrams as f64,
            udp_tx_datagrams: s.udp_tx.datagrams as f64,
            lost_packets: s.lost_packets as f64,
        }
    }

    /// Largest payload `sendDatagram` takes right now, or null if the peer does not accept
    /// datagrams.
    #[napi]
    pub fn max_datagram_size(&self) -> Option<u32> {
        self.0.max_datagram_size().map(|n| n as u32)
    }

    /// Sends an unreliable datagram (one copy). Throws if it is too large, the peer does not
    /// accept datagrams or the connection is closed.
    #[napi]
    pub fn send_datagram(&self, data: &[u8]) -> Result<()> {
        self.0
            .send_datagram(Bytes::copy_from_slice(data))
            .map_err(js_err)
    }

    /// Resolves with the next received datagram; rejects once the connection is closed.
    #[napi]
    pub async fn read_datagram(&self) -> Result<Buffer> {
        let datagram = self.0.read_datagram().await.map_err(js_err)?;
        Ok(Vec::from(datagram).into())
    }

    /// Round-trip time of the selected path in milliseconds, if known.
    #[napi]
    pub fn rtt_ms(&self) -> Option<f64> {
        let paths = self.0.paths();
        paths
            .iter()
            .find(|p| p.is_selected())
            .map(|p| p.rtt().as_secs_f64() * 1000.0)
    }
}

// Maps a connection error to a code/reason/kind triple for JS.
fn close_info(e: ConnectionError) -> CloseInfo {
    let (code, reason, kind) = match &e {
        ConnectionError::ApplicationClosed(c) => (
            Some(c.error_code.into_inner() as u32),
            String::from_utf8_lossy(&c.reason).into_owned(),
            "application",
        ),
        ConnectionError::LocallyClosed => (None, e.to_string(), "locally_closed"),
        ConnectionError::TimedOut => (None, e.to_string(), "timed_out"),
        ConnectionError::Reset => (None, e.to_string(), "reset"),
        _ => (None, e.to_string(), "transport"),
    };
    CloseInfo {
        code,
        reason,
        kind: kind.into(),
    }
}
