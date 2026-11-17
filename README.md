# sudo-iroh-node

Node.js binding (napi-rs v3) for [iroh](https://github.com/n0-computer/iroh) 1.4.1 endpoints, connections
and streams, with every byte payload crossing the boundary as a `Buffer`.

- npm: `@sudo-ai/iroh-node` (loader) + `@sudo-ai/iroh-node-<platform>` (prebuilt binary, picked through
  `optionalDependencies`)
- Platforms: linux x64/arm64 (glibc and musl), macOS arm64/x64, Windows x64
- Node >= 22, no install scripts, no compiler needed on the installing machine

It is the transport under the SUDO-AI P2P tunnel, where it replaces `@number0/iroh`.

## Why Buffer

`@number0/iroh` moves stream bytes as `Array<number>`: one JS number per byte, built and torn down on the JS
thread. On a loopback transfer that caps the tunnel at 3-4 MB/s. This binding copies each payload exactly
once per direction:

- **write**: the bytes are copied on the JS thread into a refcounted buffer that QUIC sends from. The caller
  may reuse its `Buffer` as soon as `write` returns.
- **read**: QUIC hands out the decrypted packets as refcounted chunks; they are copied once into a JS-owned
  `Buffer` of the exact size. The chunks are freed right there, so read memory never waits for a GC.

Each stream half is owned by one task on the runtime. `write` and `read` only touch a queue on the JS thread,
so most calls settle without a thread round trip, and runs of small writes are sent as one batch.

### Numbers

Loopback, two processes, direct path, release build, medians of 5 (4 vCPU / 2 physical cores, Xeon 8175M):

| transfer | MB/s |
|---|---|
| download, 1 MiB frames, one stream | 263 |
| upload, 1 MiB frames, one stream | 256 |
| 8 streams on one connection | 287 |
| upload, 16 KiB frames | 262 |
| download, 16 KiB frames | 261 |
| upload, 1 KiB frames (one `write` per frame) | 175 |
| download, 1 KiB frames (one `read(1024)` per frame) | 157 |
| 8 connections in parallel (aggregate) | 427 |
| `@number0/iroh`, same path | 3-4 |

One connection is bounded by iroh's QUIC stack, not by the binding: a pure-Rust iroh 1.4.1 transfer on the
same machine does 232-260 MB/s, with both physical cores busy in packet processing, AES-GCM and UDP
syscalls. Throughput beyond that comes from parallel connections (the 8-connection row) or more cores.
Reproduce with `node test/helpers/bench-measure.cjs --n 5 --cases down:512,up:512,streams8:512,conns8:1024`.

## Usage

```js
const { bind } = require("@sudo-ai/iroh-node");
const crypto = require("node:crypto");

const ALPN = Buffer.from("example/1");
const server = await bind({ secretKey: crypto.randomBytes(32), alpns: [ALPN], bindAddr: "127.0.0.1:0" });
const client = await bind({ secretKey: crypto.randomBytes(32), bindAddr: "127.0.0.1:0" });

const accepted = server.accept();
const conn = await client.connect(server.id(), ALPN, { directAddrs: server.boundSockets() });
const peer = await accepted;

const { send, recv } = await conn.openBi();
await send.write(Buffer.from("hello"));
await send.finish();

const theirs = await peer.acceptBi();
for (let chunk = await theirs.recv.read(1 << 20); chunk; chunk = await theirs.recv.read(1 << 20)) {
  console.log(chunk.toString()); // Buffer
}
conn.close(0, "done");
await client.close();
await server.close();
```

## API

`index.d.ts` is generated from the Rust sources and documents every member. In short:

**`bind(options) -> Promise<Endpoint>`** binds an endpoint with iroh's Minimal preset: no pkarr, no n0 DNS,
no n0 relays. Options: `secretKey` (32 bytes), `alpns` (to accept connections), `relayUrl` (the only relay
in the relay map; omitted = relays disabled), `relayToken` (bearer token for the relay), `bindAddr`
(`host:port`), `ipTransports` (false = relay only), `keepAliveMs`, `streamWindow` and `sendWindow`
(flow-control windows in bytes, see below).

Flow control: one stream carries at most about `streamWindow / RTT`. The defaults (2.5 MiB per stream,
20 MiB unacknowledged per connection) are twice iroh's own, so a single stream moves twice as much on a
link with real latency (measured: 22 vs 11 MB/s at 100 ms RTT, 42 vs 21 MB/s at 50 ms). Larger windows
go faster still, but iroh drops packets its UDP socket cannot take, and when that leaves too many holes
in a stream QUIC closes the connection ("too many gaps in stream buffer"); at 50 ms RTT that started
from about 4 MiB per stream, and from 3 MiB with eight connections at full speed on one endpoint.

**`Endpoint`**

| member | |
|---|---|
| `id()` | endpoint id, lowercase hex |
| `boundSockets()` | local UDP sockets as `host:port` |
| `online()` | resolves once attached to the home relay |
| `homeRelayStatus()`, `watchHomeRelay()` | relay state, including a relay-side admission denial |
| `accept()` | next incoming connection, handshake done; `null` once closed |
| `acceptIncoming()` | next incoming connection *before* its handshake: `Incoming` with `pathKind()`, `remoteAddr()`, `complete()`, `refuse()` |
| `connect(id, alpn, { relayUrl, directAddrs })` | dials an endpoint id |
| `addExternalAddr(addr)`, `removeExternalAddr(addr)` | advertise a known public address |
| `close()` | closes all connections and the endpoint |

**`Connection`**

| member | |
|---|---|
| `remoteId()`, `alpn()` | authenticated peer id (hex), negotiated ALPN (`Buffer`) |
| `openBi()`, `acceptBi()` | `{ send: SendStream, recv: RecvStream }` |
| `setMaxConcurrentBiStreams(n)` | stream credit for peer-opened streams |
| `close(code, reason)`, `closed()` | `closed()` resolves `{ code?, reason, kind }`, kind is `application`, `locally_closed`, `timed_out`, `reset` or `transport` |
| `pathKind()`, `directRemoteAddr()`, `rttMs()` | the selected path: `direct`/`relay`/`none`, its remote socket, its RTT |
| `stats()` | UDP bytes and datagrams in both directions, lost packets |
| `sendDatagram(buf)`, `readDatagram()`, `maxDatagramSize()` | unreliable datagrams as Buffers |

**`SendStream`**

| member | |
|---|---|
| `write(buf)` | queues the bytes in call order. Resolves at once while less than 1 MiB is waiting for QUIC, otherwise when QUIC has accepted them: awaiting `write` is the backpressure |
| `writeMany(bufs)` | the same for several buffers, one promise |
| `flush()` | resolves when QUIC has accepted everything written so far |
| `finish()` | clean end (FIN) after everything written before it |
| `reset(code)` | abandons the send side; pending writes reject with `stream reset` |

**`RecvStream`**

| member | |
|---|---|
| `read(max)` | whatever has arrived, 1..`max` bytes (clamped to 4 MiB), as one Buffer; `null` at end of stream |
| `stop(code)` | asks the peer to stop sending; pending and later reads reject with `stream stopped` |

Error texts callers may match on (pinned by `test/binding.test.cjs`):

- a read after the peer reset its side: `stream reset by peer: error <code>`
- a write, flush or finish after the peer stopped reading: `sending stopped by peer: error <code>`

A write that already resolved can still fail later (the peer stops the stream, the connection is lost); the
failure then rejects the next `write`, `flush` or `finish`. Call `finish()` (or `flush()`) and await it to
know that everything was accepted.

Reading advice: pass a large `max` (64 KiB to 1 MiB). A read returns what is there, so a large `max` costs
nothing and lets one call cover many packets.

## How SUDO-AI uses it

The tunnel's transport adapter (`src/core/tunnel/transport/iroh.ts` in SUDO-AI) is the only file that talks
to an iroh addon. It prefers `@sudo-ai/iroh-node` and falls back to `@number0/iroh` when no binary exists
for the platform. Both run iroh 1.4.1, so the two sides of a tunnel may use different addons;
`test/compat.test.cjs` checks that against the installed `@number0/iroh` in both directions.

Everything above the adapter sees the same interface either way: `Buffer` payloads, numeric close and reset
codes, hex endpoint ids.

Measured through the whole tunnel (HTTP over the mux, gateway child in its own process, direct loopback):
137 MB/s down and 131 MB/s up with this binding, 4.5 MB/s with `@number0/iroh`.

## Development

```bash
npm install
npm run build        # release build (fat LTO, a few minutes) -> iroh-node.<platform>.node, index.js, index.d.ts
npm run build:fast   # same optimization level without LTO, ~20 s incremental
npm test             # binding tests; compat tests run when @number0/iroh is resolvable
npm run bench        # throughput, peak RSS and CPU per MB
```

`IROH_NUMBER0_DIR=<dir with node_modules/@number0/iroh>` points the compat tests at an installed
`@number0/iroh`; `IROH_RELAY_BIN=<iroh-relay binary>` enables the relay compat tests and the SUDO-AI tunnel protocol tests (test/helpers/tunnel-server.cjs).

`index.js` and `index.d.ts` are generated by the build and committed, so the tests and the published loader
need no build step of their own. Commit them whenever the Rust API changes.

## Bumping iroh

The wire protocol must match the SUDO relay and the browser client, so iroh is pinned exactly.

1. Change `iroh = "=1.4.1"` in `Cargo.toml`, run `cargo update -p iroh`.
2. `npm run build && npm test`. The binding uses a small part of iroh's API (`Endpoint`, `Incoming`,
   `Connection`, `SendStream::write_many_chunks`, `RecvStream::read_many_chunks`); compile errors point at
   what moved.
3. Run the compat tests against the `@number0/iroh` the other side of the tunnel will run. A failure there
   means both sides have to move together.
4. Check the pinned error texts (see API): they are iroh's own wording, and a test fails if it changed.
5. `npm run bench`, compare with the numbers above, then release as described in `PACKAGING.md`.

## License

MIT, see `LICENSE`. Third-party notices are in `THIRD-PARTY-NOTICES.md`.
