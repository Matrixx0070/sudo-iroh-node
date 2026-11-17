// On the wire this package must talk to @number0/iroh, the addon it replaces in the SUDO-AI
// tunnel: echo in both directions over a direct loopback path, and numeric close codes. With an
// iroh-relay 1.4.1 binary the same pairs also run through a dev relay of their own (own port, own
// temp dir), and the binding drives a server speaking the SUDO-AI tunnel protocol, relay only. A group is skipped when what it needs is missing: @number0/iroh (resolvable, or under
// IROH_NUMBER0_DIR) or the relay (IROH_RELAY_BIN, default ~/.cargo/bin/iroh-relay).
"use strict";
const assert = require("node:assert/strict");
const { randomBytes } = require("node:crypto");
const { after, before, describe, test } = require("node:test");
const { load, hasNumber0 } = require("./helpers/bindings.cjs");
const { relayBin, startRelay } = require("./helpers/processes.cjs");
const { ALPN, serveEndpoint, echo, sha256 } = require("./helpers/wire.cjs");
const { TUNNEL_ALPN, frame, FrameReader } = require("./helpers/tunnel-server.cjs");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { setTimeout: sleep } = require("node:timers/promises");
const { spawnJsonLine } = require("./helpers/processes.cjs");

const withoutNumber0 = hasNumber0() ? false : "@number0/iroh not installed (set IROH_NUMBER0_DIR)";
const withoutRelay = relayBin() ? false : "iroh-relay not found (set IROH_RELAY_BIN)";
// [server, client]
const MIXED = [
  ["number0", "own"],
  ["own", "number0"],
];

function endpoint(name, { relayUrl, accepts = false, relayOnly = false }) {
  // Only this package can turn IP transports off, so relay-only applies to its side.
  return load(name).bind({
    secretKey: randomBytes(32),
    relayUrl,
    alpns: accepts ? [ALPN] : undefined,
    ipTransports: relayOnly && name === "own" ? false : undefined,
  });
}

// Server binding `serverName`, client binding `clientName`, path "relay" or "direct": 4 MiB echo.
async function echoPair(relayUrl, serverName, clientName, path) {
  const relayOnly = path === "relay";
  const server = await endpoint(serverName, { relayUrl, accepts: true, relayOnly });
  if (relayUrl) await server.online();
  const serving = serveEndpoint(server);
  const client = await endpoint(clientName, { relayUrl, relayOnly });
  try {
    const opts = path === "direct" ? { relayUrl, directAddrs: server.directAddrs() } : { relayUrl };
    const conn = await client.connect(server.id(), ALPN, opts);
    assert.equal(conn.remoteId(), server.id());
    const { sent, received } = await echo(conn, 4 << 20);
    assert.equal(received, sent);
    // A direct pairing may start on the relay and move over once hole punching succeeds.
    for (let tries = 0; path === "direct" && tries < 40 && conn.pathKind() !== "direct"; tries++) {
      await echo(conn, 65536);
    }
    assert.equal(conn.pathKind(), path);
    conn.close(0, "done");
  } finally {
    await client.close();
    await server.close();
    await serving;
  }
}

// The server closes with (1, "not a member"); the client sees that code and reason.
async function closeReachesClient(relayUrl, serverName, clientName) {
  const server = await endpoint(serverName, { relayUrl, accepts: true });
  const client = await endpoint(clientName, { relayUrl });
  try {
    const accepting = server.accept();
    const conn = await client.connect(server.id(), ALPN, { relayUrl, directAddrs: server.directAddrs() });
    const serverSide = await accepting;
    assert.equal(serverSide.remoteId(), client.id());
    serverSide.close(1, "not a member");
    const info = await conn.closed();
    if (clientName === "own") {
      assert.deepEqual([info.code, info.reason], [1, "not a member"]);
    } else {
      assert.match(info.reason, /not a member/);
    }
  } finally {
    await client.close();
    await server.close();
  }
}


// One tunnel HTTP exchange on a new stream with an optional body; resolves { head, body }.
async function tunnelFetch(conn, request, body) {
  const { send, recv } = await conn.openBi();
  const head = { type: "http", method: "GET", headers: [], ...request };
  await send.write(frame(Buffer.from(JSON.stringify(head))));
  const payload = body ?? Buffer.alloc(0);
  for (let at = 0; at < payload.length; at += 1 << 20) await send.write(frame(payload.subarray(at, at + (1 << 20))));
  await send.finish();
  const frames = new FrameReader(recv);
  const response = JSON.parse(String(await frames.next()));
  const parts = [];
  for (let part; (part = await frames.next()); ) parts.push(part);
  return { head: response, body: Buffer.concat(parts) };
}

// The tunnel server in its own process against `relayUrl`; resolves { desc, dir, stop() }.
async function startTunnelServer(relayUrl) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "sudo-iroh-tunnel-"));
  const server = await spawnJsonLine(path.join(__dirname, "helpers", "tunnel-server.cjs"), [relayUrl, dir]);
  const stop = async () => {
    await server.stop();
    fs.rmSync(dir, { recursive: true, force: true });
  };
  // The decoy is written before the line is printed; wait for the file all the same, briefly.
  const decoy = path.join(dir, "decoy-descriptor.json");
  for (let i = 0; i < 100 && !fs.existsSync(decoy); i++) await sleep(100);
  if (!fs.existsSync(decoy)) {
    await stop();
    throw new Error("tunnel server wrote no decoy descriptor");
  }
  return { desc: server.info, dir, stop };
}

describe("compat with @number0/iroh, direct", { skip: withoutNumber0 }, () => {
  for (const [server, client] of MIXED) {
    test(`${client} client -> ${server} server: 4 MiB echo`, () => echoPair(undefined, server, client, "direct"));
    test(`${server} server close (1, "not a member") reaches ${client} client`, () => closeReachesClient(undefined, server, client));
  }
});

describe("compat via a local iroh-relay", { skip: withoutRelay }, () => {
  let relay;
  before(async () => {
    relay = await startRelay();
  });
  after(() => relay?.stop());

  test("own <-> own via relay only", () => echoPair(relay.url, "own", "own", "relay"));
  for (const [server, client] of MIXED) {
    for (const path of ["relay", "direct"]) {
      test(`${client} client -> ${server} server via ${path}: 4 MiB echo`, { skip: withoutNumber0 }, () => echoPair(relay.url, server, client, path));
    }
    test(`${server} server close (1, "not a member") reaches ${client} client`, { skip: withoutNumber0 }, () => closeReachesClient(relay.url, server, client));
  }
  describe("SUDO-AI tunnel protocol, relay only", () => {
    let tunnel;
    let ep;
    let conn;
    const dialTunnel = (endpoint, desc) => endpoint.connect(desc.endpointId, TUNNEL_ALPN, { relayUrl: desc.relayUrl });
    before(async () => {
      tunnel = await startTunnelServer(relay.url);
      ep = await load("own").bind({ secretKey: randomBytes(32), relayUrl: relay.url, ipTransports: false });
      fs.writeFileSync(path.join(tunnel.dir, "allow.txt"), ep.id() + "\n");
      conn = (await dialTunnel(ep, tunnel.desc)).raw;
    });
    after(async () => {
      conn?.close(0, "done");
      await ep?.close();
      await tunnel?.stop();
    });

    test("GET /sudo/hello -> 200 JSON over the relay", async () => {
      const { head, body } = await tunnelFetch(conn, { target: "/sudo/hello" });
      assert.equal(head.status, 200);
      assert.equal(JSON.parse(body).ok, true);
      assert.equal(conn.pathKind(), "relay");
    });

    test("GET /sudo/blob 5 MiB, byte i is i % 251", async () => {
      const n = 5 << 20;
      const { body } = await tunnelFetch(conn, { target: `/sudo/blob?n=${n}` });
      assert.equal(body.length, n);
      const wrong = body.findIndex((byte, i) => byte !== i % 251);
      assert.equal(wrong, -1, `byte ${wrong}`);
    });

    test("POST /sudo/echo 3 MiB comes back intact", async () => {
      const data = randomBytes(3 << 20);
      const { body } = await tunnelFetch(conn, { method: "POST", target: "/sudo/echo" }, data);
      assert.equal(sha256(body), sha256(data));
    });

    test("/sudo/fail resets the stream with code 3", async () => {
      await assert.rejects(tunnelFetch(conn, { target: "/sudo/fail" }), /error 3/);
    });

    test("WebSocket echo: text, binary, close", async () => {
      const { send, recv } = await conn.openBi();
      const frames = new FrameReader(recv);
      await send.write(frame(Buffer.from(JSON.stringify({ type: "ws", target: "/sudo", headers: [], subprotocols: [] }))));
      assert.equal(JSON.parse(String(await frames.next())).status, 101);
      await send.write(frame(Buffer.concat([Buffer.from([1]), Buffer.from("hello")])));
      assert.equal((await frames.next()).toString("utf8", 1), "hello");
      const binary = randomBytes(100_000);
      await send.write(frame(Buffer.concat([Buffer.from([2]), binary])));
      assert.equal(sha256((await frames.next()).subarray(1)), sha256(binary));
      await send.write(frame(Buffer.from([8, 3, 232])));
      assert.equal((await frames.next())[0], 8);
      await send.finish();
      assert.equal(await frames.next(), null);
    });

    test('a non-member is closed with code 1 "not a member"', async () => {
      const outsider = await load("own").bind({ secretKey: randomBytes(32), relayUrl: relay.url, ipTransports: false });
      try {
        const refused = await dialTunnel(outsider, tunnel.desc);
        const info = await refused.closed();
        assert.deepEqual([info.code, info.reason], [1, "not a member"]);
      } finally {
        await outsider.close();
      }
    });

    test("dialing a key nobody serves (decoy descriptor) fails", async () => {
      const decoy = JSON.parse(fs.readFileSync(path.join(tunnel.dir, "decoy-descriptor.json"), "utf8"));
      const timeout = sleep(8000).then(() => {
        throw new Error("timeout (no such endpoint)");
      });
      await assert.rejects(Promise.race([dialTunnel(ep, decoy), timeout]));
    });
  });
});
