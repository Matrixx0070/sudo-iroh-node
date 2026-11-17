//! The endpoint: binding one, dialing peers, taking incoming connections and following the home
//! relay.

use std::{net::SocketAddr, str::FromStr, sync::Arc, time::Duration};

use iroh::{
    Endpoint as IrohEndpoint, EndpointAddr, EndpointId, RelayMap, RelayMode, RelayUrl, SecretKey,
    Watcher,
    endpoint::{self, IncomingAddr, QuicTransportConfig, RelayStatus, presets},
};
use napi::bindgen_prelude::*;
use napi_derive::napi;
use tokio::sync::{Mutex, watch};

use crate::{conn::Connection, js_err};

/// Options for `bind`.
#[napi(object)]
pub struct BindOptions {
    /// The endpoint's secret key: exactly 32 bytes.
    pub secret_key: Uint8Array,
    /// URL of the one relay this endpoint uses; without it no relay is used at all.
    pub relay_url: Option<String>,
    /// Bearer token presented to that relay when attaching (relay-side admission).
    pub relay_token: Option<String>,
    /// Protocols (ALPN) this endpoint answers; only needed to accept connections.
    pub alpns: Option<Vec<Uint8Array>>,
    /// UDP socket to bind (`host:port`); iroh picks one when omitted.
    pub bind_addr: Option<String>,
    /// `false` binds no IP transport, so every path goes through the relay. Default `true`.
    pub ip_transports: Option<bool>,
    /// QUIC keep-alive interval in milliseconds; must not be 0.
    pub keep_alive_ms: Option<u32>,
}

// A `host:port` string as a socket address.
fn socket_addr(text: &str) -> Result<SocketAddr> {
    text.parse::<SocketAddr>().map_err(js_err)
}

fn relay_url(text: &str) -> Result<RelayUrl> {
    RelayUrl::from_str(text).map_err(js_err)
}

fn secret_key(bytes: &Uint8Array) -> Result<SecretKey> {
    let raw: &[u8] = bytes.as_ref();
    let Ok(key) = <[u8; 32]>::try_from(raw) else {
        return Err(js_err("secretKey must be 32 bytes"));
    };
    Ok(SecretKey::from_bytes(&key))
}

// Our relay and nothing else, or no relay. A token rides along with the relay it belongs to.
fn relay_mode(url: Option<&str>, token: Option<String>) -> Result<RelayMode> {
    let relays = match url {
        None => return Ok(RelayMode::Disabled),
        Some(url) => RelayMap::from_iter([relay_url(url)?]),
    };
    Ok(RelayMode::Custom(if let Some(token) = token {
        relays.with_auth_token(token)
    } else {
        relays
    }))
}

// The transport settings for a keep-alive interval, refusing a zero interval.
fn keep_alive(ms: u32) -> Result<QuicTransportConfig> {
    if ms > 0 {
        let interval = Duration::from_millis(ms.into());
        Ok(QuicTransportConfig::builder().keep_alive_interval(interval).build())
    } else {
        Err(js_err("keepAliveMs must be positive"))
    }
}

/// Binds an endpoint on the Minimal preset — no pkarr, no n0 DNS, no n0 relays — so the only
/// relay it knows is the one given here.
#[napi]
pub async fn bind(opts: BindOptions) -> Result<Endpoint> {
    let BindOptions { secret_key: key, relay_url, relay_token, alpns, bind_addr, ip_transports, keep_alive_ms } = opts;
    let key = secret_key(&key)?;
    let relays = relay_mode(relay_url.as_deref(), relay_token)?;
    let protocols: Vec<Vec<u8>> = alpns.into_iter().flatten().map(|alpn| alpn.to_vec()).collect();
    let mut builder = IrohEndpoint::builder(presets::Minimal).secret_key(key).alpns(protocols).relay_mode(relays);
    if let Some(ms) = keep_alive_ms {
        builder = builder.transport_config(keep_alive(ms)?);
    }
    if matches!(ip_transports, Some(false)) {
        builder = builder.clear_ip_transports();
    }
    if let Some(addr) = bind_addr {
        builder = builder.bind_addr(socket_addr(&addr)?).map_err(js_err)?;
    }
    let bound = builder.bind().await.map_err(js_err)?;
    Ok(Endpoint(Arc::new(bound)))
}

/// Options for `Endpoint.connect`.
#[napi(object)]
pub struct ConnectOptions {
    /// The remote endpoint's relay URL.
    pub relay_url: Option<String>,
    /// UDP addresses (`host:port`) the remote endpoint is known to listen on.
    pub direct_addrs: Option<Vec<String>>,
}

// Where to find a peer: its id plus whatever relay and direct addresses the caller knows.
fn peer_addr(endpoint_id: &str, opts: Option<ConnectOptions>) -> Result<EndpointAddr> {
    let mut addr = EndpointAddr::new(EndpointId::from_str(endpoint_id).map_err(js_err)?);
    let Some(ConnectOptions { relay_url: relay, direct_addrs }) = opts else {
        return Ok(addr);
    };
    if let Some(url) = relay {
        addr = addr.with_relay_url(relay_url(&url)?);
    }
    for direct in direct_addrs.unwrap_or_default() {
        addr = addr.with_ip_addr(socket_addr(&direct)?);
    }
    Ok(addr)
}

/// The state of one home relay.
#[napi(object)]
#[derive(Clone)]
pub struct HomeRelayStatus {
    /// The relay's URL.
    pub url: String,
    /// Whether the endpoint is attached to it right now.
    pub connected: bool,
    /// The latest connection error, as text for display.
    pub last_error: Option<String>,
    /// Present when the relay refused to admit this endpoint; trying again does not help until
    /// the relay's access policy changes.
    pub auth_denied_reason: Option<String>,
}

impl From<&RelayStatus> for HomeRelayStatus {
    fn from(status: &RelayStatus) -> Self {
        Self {
            url: status.url().to_string(),
            connected: status.is_connected(),
            // `{:#}` keeps the whole cause chain on one line.
            last_error: status.last_error().map(|error| format!("{error:#}")),
            auth_denied_reason: status.auth_denied_reason().map(String::from),
        }
    }
}

fn relay_statuses(list: &[RelayStatus]) -> Vec<HomeRelayStatus> {
    list.iter().map(HomeRelayStatus::from).collect()
}

/// Delivers home relay status changes; see `Endpoint.watchHomeRelay`.
#[napi]
pub struct HomeRelayWatcher(Mutex<watch::Receiver<Vec<HomeRelayStatus>>>);

#[napi]
impl HomeRelayWatcher {
    /// The status: right away on the first call, after each change on later calls, and null once
    /// the endpoint has closed.
    #[napi]
    pub async fn next(&self) -> Option<Vec<HomeRelayStatus>> {
        let mut updates = self.0.lock().await;
        match updates.changed().await {
            Ok(()) => Some(updates.borrow_and_update().clone()),
            // The forwarding task ended with the endpoint.
            Err(_) => None,
        }
    }
}

/// An incoming connection before its handshake. Look at where it comes from, then `complete` it
/// or `refuse` it — one of the two, once.
#[napi]
pub struct Incoming(std::sync::Mutex<Option<endpoint::Incoming>>);

impl Incoming {
    fn slot(&self) -> std::sync::MutexGuard<'_, Option<endpoint::Incoming>> {
        // A poisoned lock still holds a valid Option.
        self.0.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    // Hands the connection out exactly once.
    fn take(&self) -> Result<endpoint::Incoming> {
        match self.slot().take() {
            Some(incoming) => Ok(incoming),
            None => Err(js_err("incoming connection already completed or refused")),
        }
    }

    fn source(&self) -> Option<IncomingAddr> {
        self.slot().as_ref().map(endpoint::Incoming::remote_addr)
    }
}

#[napi]
impl Incoming {
    /// "direct" for a UDP packet from an IP address, "relay" when it came through the relay, and
    /// "none" otherwise — including after `complete` or `refuse`.
    #[napi]
    pub fn path_kind(&self) -> String {
        let kind = match self.source() {
            Some(IncomingAddr::Ip(_)) => "direct",
            Some(IncomingAddr::Relay { .. }) => "relay",
            _ => "none",
        };
        kind.to_owned()
    }

    /// `host:port` of a direct incoming connection, else null. Unauthenticated: the peer has not
    /// yet proven it owns this address or any endpoint id.
    #[napi]
    pub fn remote_addr(&self) -> Option<String> {
        if let Some(IncomingAddr::Ip(addr)) = self.source() {
            Some(addr.to_string())
        } else {
            None
        }
    }

    /// Runs the handshake; rejects when it fails.
    #[napi]
    pub async fn complete(&self) -> Result<Connection> {
        let connecting = self.take()?.accept().map_err(js_err)?;
        Ok(Connection::new(connecting.await.map_err(js_err)?))
    }

    /// Turns the connection away without a handshake; the peer sees CONNECTION_REFUSED.
    #[napi]
    pub fn refuse(&self) -> Result<()> {
        let incoming = self.take()?;
        incoming.refuse();
        Ok(())
    }
}

/// A bound iroh endpoint.
#[napi]
pub struct Endpoint(Arc<IrohEndpoint>);

#[napi]
impl Endpoint {
    /// The endpoint id: the public key in lowercase hex.
    #[napi]
    pub fn id(&self) -> String {
        self.0.id().to_string()
    }

    /// The local UDP sockets (`host:port`) peers can dial directly.
    #[napi]
    pub fn bound_sockets(&self) -> Vec<String> {
        let sockets = self.0.bound_sockets();
        sockets.iter().map(ToString::to_string).collect()
    }

    /// Resolves when the endpoint is attached to its home relay.
    #[napi]
    pub async fn online(&self) {
        self.0.online().await
    }

    /// The state of each home relay right now (empty until one is chosen).
    #[napi]
    pub fn home_relay_status(&self) -> Vec<HomeRelayStatus> {
        relay_statuses(&self.0.home_relay_status().get())
    }

    /// Follows home relay state, a relay refusing admission included.
    #[napi]
    pub async fn watch_home_relay(&self) -> HomeRelayWatcher {
        let mut source = self.0.home_relay_status();
        let (publish, updates) = watch::channel(relay_statuses(&source.get()));
        // The first `next()` reports the current state without waiting for a change.
        let mut updates = updates;
        updates.mark_changed();
        // Forwards changes until the endpoint closes or nobody listens any more; dropping
        // `publish` then ends every pending `next()` with null.
        let forward = async move {
            loop {
                let Ok(list) = source.updated().await else { break };
                if publish.send(relay_statuses(&list)).is_err() {
                    break;
                }
            }
        };
        tokio::spawn(self.0.closed().run_until(forward));
        HomeRelayWatcher(Mutex::new(updates))
    }

    /// Takes the next incoming connection and runs its handshake. Null once the endpoint has
    /// closed; a failed handshake rejects and the caller simply calls again.
    #[napi]
    pub async fn accept(&self) -> Result<Option<Connection>> {
        let incoming = match self.0.accept().await {
            Some(incoming) => incoming,
            None => return Ok(None),
        };
        let connecting = incoming.accept().map_err(js_err)?;
        let connection = connecting.await.map_err(js_err)?;
        Ok(Some(Connection::new(connection)))
    }

    /// Takes the next incoming connection without its handshake, so the caller can take the one
    /// after it meanwhile, or turn it away by where it comes from. Null once the endpoint has
    /// closed.
    #[napi]
    pub async fn accept_incoming(&self) -> Option<Incoming> {
        let incoming = self.0.accept().await?;
        Some(Incoming(std::sync::Mutex::new(Some(incoming))))
    }

    /// Announces an address (`host:port`) this endpoint can be reached at, such as a public one
    /// learned elsewhere. Peers try it as a direct path.
    #[napi]
    pub async fn add_external_addr(&self, addr: String) -> Result<()> {
        let addr = socket_addr(&addr)?;
        self.0.add_external_addr(addr).await;
        Ok(())
    }

    /// Stops announcing an address added with `addExternalAddr`; false when it was not there.
    #[napi]
    pub async fn remove_external_addr(&self, addr: String) -> Result<bool> {
        let addr = socket_addr(&addr)?;
        Ok(self.0.remove_external_addr(&addr).await)
    }

    /// Dials `endpointId` (hex) on `alpn` through the given relay and/or direct addresses.
    #[napi]
    pub async fn connect(&self, endpoint_id: String, alpn: Uint8Array, opts: Option<ConnectOptions>) -> Result<Connection> {
        let addr = peer_addr(&endpoint_id, opts)?;
        let connection = self.0.connect(addr, &alpn).await.map_err(js_err)?;
        Ok(Connection::new(connection))
    }

    /// Closes every connection, then the endpoint.
    #[napi]
    pub async fn close(&self) {
        self.0.close().await
    }
}
