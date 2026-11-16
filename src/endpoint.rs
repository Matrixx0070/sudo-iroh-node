//! Endpoint binding and dialing.

use std::{str::FromStr, sync::Arc, time::Duration};

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
    /// 32 secret key bytes.
    pub secret_key: Uint8Array,
    /// The only relay in the relay map; omitted means relays disabled.
    pub relay_url: Option<String>,
    /// Bearer token sent to the relay when attaching to it (relay-side admission).
    pub relay_token: Option<String>,
    /// ALPNs this endpoint accepts (needed only to accept connections).
    pub alpns: Option<Vec<Uint8Array>>,
    /// Local UDP socket address (`host:port`); default is iroh's default.
    pub bind_addr: Option<String>,
    /// When false, no IP transports are bound: relay-only (like the browser). Default true.
    pub ip_transports: Option<bool>,
    /// QUIC connection keep-alive interval in milliseconds (nonzero).
    pub keep_alive_ms: Option<u32>,
}

/// Binds an endpoint with the Minimal preset (no pkarr, no n0 DNS, no n0 relays) and our relay only.
#[napi]
pub async fn bind(opts: BindOptions) -> Result<Endpoint> {
    let key: [u8; 32] = opts
        .secret_key
        .as_ref()
        .try_into()
        .map_err(|_| js_err("secretKey must be 32 bytes"))?;
    let relay = match opts.relay_url {
        Some(u) => {
            let map = RelayMap::from_iter([RelayUrl::from_str(&u).map_err(js_err)?]);
            RelayMode::Custom(match opts.relay_token {
                Some(token) => map.with_auth_token(token),
                None => map,
            })
        }
        None => RelayMode::Disabled,
    };
    let alpns = opts
        .alpns
        .unwrap_or_default()
        .iter()
        .map(|a| a.to_vec())
        .collect();
    let mut b = IrohEndpoint::builder(presets::Minimal)
        .secret_key(SecretKey::from_bytes(&key))
        .alpns(alpns)
        .relay_mode(relay);
    if let Some(ms) = opts.keep_alive_ms {
        if ms == 0 {
            return Err(js_err("keepAliveMs must be positive"));
        }
        b = b.transport_config(
            QuicTransportConfig::builder()
                .keep_alive_interval(Duration::from_millis(u64::from(ms)))
                .build(),
        );
    }
    if opts.ip_transports == Some(false) {
        b = b.clear_ip_transports();
    }
    if let Some(addr) = opts.bind_addr {
        let sock = std::net::SocketAddr::from_str(&addr).map_err(js_err)?;
        b = b.bind_addr(sock).map_err(js_err)?;
    }
    let ep = b.bind().await.map_err(js_err)?;
    Ok(Endpoint(Arc::new(ep)))
}

/// Options for `Endpoint.connect`.
#[napi(object)]
pub struct ConnectOptions {
    /// Relay URL of the remote endpoint.
    pub relay_url: Option<String>,
    /// Known direct UDP addresses (`host:port`) of the remote endpoint.
    pub direct_addrs: Option<Vec<String>>,
}

/// Connection state of one home relay.
#[napi(object)]
#[derive(Clone)]
pub struct HomeRelayStatus {
    /// Relay URL.
    pub url: String,
    /// True while attached to the relay.
    pub connected: bool,
    /// Most recent connection error, for display only.
    pub last_error: Option<String>,
    /// Set when the relay refused this endpoint's admission; retrying will not help
    /// until the relay's access policy changes.
    pub auth_denied_reason: Option<String>,
}

fn home_relay_statuses(list: Vec<RelayStatus>) -> Vec<HomeRelayStatus> {
    list.iter()
        .map(|status| HomeRelayStatus {
            url: status.url().to_string(),
            connected: status.is_connected(),
            last_error: status.last_error().map(|error| format!("{error:#}")),
            auth_denied_reason: status.auth_denied_reason().map(ToOwned::to_owned),
        })
        .collect()
}

/// Follows home relay status changes; see `Endpoint.watchHomeRelay`.
#[napi]
pub struct HomeRelayWatcher(Mutex<watch::Receiver<Vec<HomeRelayStatus>>>);

#[napi]
impl HomeRelayWatcher {
    /// Resolves with the status: at once on the first call, then after each change.
    /// Resolves null once the endpoint is closed.
    #[napi]
    pub async fn next(&self) -> Option<Vec<HomeRelayStatus>> {
        let mut rx = self.0.lock().await;
        rx.changed().await.ok()?;
        Some(rx.borrow_and_update().clone())
    }
}

/// An incoming connection whose handshake has not run yet: look at where it comes from, then
/// `complete` or `refuse` it. Each can be called once.
#[napi]
pub struct Incoming(std::sync::Mutex<Option<endpoint::Incoming>>);

impl Incoming {
    fn take(&self) -> Result<endpoint::Incoming> {
        let taken = self.0.lock().unwrap_or_else(|e| e.into_inner()).take();
        taken.ok_or_else(|| js_err("incoming connection already completed or refused"))
    }

    fn addr(&self) -> Option<IncomingAddr> {
        let incoming = self.0.lock().unwrap_or_else(|e| e.into_inner());
        incoming.as_ref().map(|i| i.remote_addr())
    }
}

#[napi]
impl Incoming {
    /// "direct" for a UDP packet from an IP address, "relay" via the relay, else "none"
    /// (also once the connection was completed or refused).
    #[napi]
    pub fn path_kind(&self) -> String {
        match self.addr() {
            Some(IncomingAddr::Ip(_)) => "direct".into(),
            Some(IncomingAddr::Relay { .. }) => "relay".into(),
            _ => "none".into(),
        }
    }

    /// Remote socket address (`host:port`) of a direct incoming connection, else null. Not
    /// authenticated: the peer has not proven it owns this address or any endpoint id yet.
    #[napi]
    pub fn remote_addr(&self) -> Option<String> {
        match self.addr() {
            Some(IncomingAddr::Ip(addr)) => Some(addr.to_string()),
            _ => None,
        }
    }

    /// Runs the handshake. Rejects if it fails.
    #[napi]
    pub async fn complete(&self) -> Result<Connection> {
        let conn = self
            .take()?
            .accept()
            .map_err(js_err)?
            .await
            .map_err(js_err)?;
        Ok(Connection::new(conn))
    }

    /// Refuses the connection (the peer sees CONNECTION_REFUSED) without running the handshake.
    #[napi]
    pub fn refuse(&self) -> Result<()> {
        self.take()?.refuse();
        Ok(())
    }
}

/// A bound iroh endpoint.
#[napi]
pub struct Endpoint(Arc<IrohEndpoint>);

#[napi]
impl Endpoint {
    /// This endpoint's id (lowercase hex public key).
    #[napi]
    pub fn id(&self) -> String {
        self.0.id().to_string()
    }

    /// Local UDP socket addresses (`host:port`), for dialing directly.
    #[napi]
    pub fn bound_sockets(&self) -> Vec<String> {
        self.0
            .bound_sockets()
            .iter()
            .map(|a| a.to_string())
            .collect()
    }

    /// Resolves once the endpoint is attached to its home relay.
    #[napi]
    pub async fn online(&self) {
        self.0.online().await;
    }

    /// Current status of each home relay (empty before one is selected).
    #[napi]
    pub fn home_relay_status(&self) -> Vec<HomeRelayStatus> {
        home_relay_statuses(self.0.home_relay_status().get())
    }

    /// Starts following home relay status, including a relay-side admission denial.
    #[napi]
    pub async fn watch_home_relay(&self) -> HomeRelayWatcher {
        let mut source = self.0.home_relay_status();
        let (tx, mut rx) = watch::channel(home_relay_statuses(source.get()));
        rx.mark_changed();
        tokio::spawn(self.0.closed().run_until(async move {
            while let Ok(list) = source.updated().await {
                if tx.send(home_relay_statuses(list)).is_err() {
                    break;
                }
            }
        }));
        HomeRelayWatcher(Mutex::new(rx))
    }

    /// Waits for the next incoming connection and completes its handshake.
    /// Resolves null once the endpoint is closed. A failed handshake rejects; call again.
    #[napi]
    pub async fn accept(&self) -> Result<Option<Connection>> {
        let Some(incoming) = self.0.accept().await else {
            return Ok(None);
        };
        let conn = incoming.accept().map_err(js_err)?.await.map_err(js_err)?;
        Ok(Some(Connection::new(conn)))
    }

    /// Waits for the next incoming connection without running its handshake, so the caller can
    /// accept the following one while this one completes, or refuse it by its source.
    /// Resolves null once the endpoint is closed.
    #[napi]
    pub async fn accept_incoming(&self) -> Option<Incoming> {
        let incoming = self.0.accept().await?;
        Some(Incoming(std::sync::Mutex::new(Some(incoming))))
    }

    /// Advertises an address (`host:port`) this endpoint is reachable at, e.g. a public address
    /// learned from outside. Peers try it as a direct path.
    #[napi]
    pub async fn add_external_addr(&self, addr: String) -> Result<()> {
        let addr = std::net::SocketAddr::from_str(&addr).map_err(js_err)?;
        self.0.add_external_addr(addr).await;
        Ok(())
    }

    /// Stops advertising an address added with `addExternalAddr`; false if it was not there.
    #[napi]
    pub async fn remove_external_addr(&self, addr: String) -> Result<bool> {
        let addr = std::net::SocketAddr::from_str(&addr).map_err(js_err)?;
        Ok(self.0.remove_external_addr(&addr).await)
    }

    /// Dials `endpointId` (hex) on `alpn`, via the given relay and/or direct addresses.
    #[napi]
    pub async fn connect(
        &self,
        endpoint_id: String,
        alpn: Uint8Array,
        opts: Option<ConnectOptions>,
    ) -> Result<Connection> {
        let id = EndpointId::from_str(&endpoint_id).map_err(js_err)?;
        let mut addr = EndpointAddr::new(id);
        if let Some(o) = opts {
            if let Some(u) = o.relay_url {
                addr = addr.with_relay_url(RelayUrl::from_str(&u).map_err(js_err)?);
            }
            for a in o.direct_addrs.unwrap_or_default() {
                addr = addr.with_ip_addr(std::net::SocketAddr::from_str(&a).map_err(js_err)?);
            }
        }
        let conn = self.0.connect(addr, &alpn).await.map_err(js_err)?;
        Ok(Connection::new(conn))
    }

    /// Closes all connections and the endpoint.
    #[napi]
    pub async fn close(&self) {
        self.0.close().await;
    }
}
