// What the SUDO-AI tunnel relies on, checked without any network service: the exported API, byte
// integrity over a direct own <-> own path, the peer identities both sides see, numeric close codes
// and stream reset codes. Runs on every CI platform.
"use strict";
const assert = require("node:assert/strict");
const { execFile } = require("node:child_process");
const crypto = require("node:crypto");
const { test } = require("node:test");
const { promisify } = require("node:util");
const binding = require("../index.js");
const { load, loopbackAddrs } = require("./helpers/bindings.cjs");
const { ALPN, serveEndpoint, echo } = require("./helpers/wire.cjs");

// The public JS surface by class. Changing it changes the contract with the tunnel adapter.
const API = {
  Endpoint: ["accept", "acceptIncoming", "addExternalAddr", "boundSockets", "close", "connect", "homeRelayStatus", "id", "online", "removeExternalAddr", "watchHomeRelay"],
  Incoming: ["complete", "pathKind", "refuse", "remoteAddr"],
  HomeRelayWatcher: ["next"],
  Connection: ["acceptBi", "alpn", "close", "closed", "directRemoteAddr", "maxDatagramSize", "openBi", "pathKind", "readDatagram", "remoteId", "rttMs", "sendDatagram", "setMaxConcurrentBiStreams", "stats"],
  SendStream: ["finish", "flush", "reset", "write", "writeMany"],
  RecvStream: ["read", "stop"],
};

const newKey = () => crypto.randomBytes(32);

// A server that accepts ALPN and a client, relays off, connected directly over loopback.
async function directPair() {
  const server = await binding.bind({ secretKey: newKey(), alpns: [ALPN] });
  const client = await binding.bind({ secretKey: newKey(), keepAliveMs: 1_000 });
  const serverSide = server.accept();
  const conn = await client.connect(server.id(), ALPN, { directAddrs: loopbackAddrs(server.boundSockets()) });
  return { server, client, conn, sconn: await serverSide };
}

async function closeBoth({ server, client }) {
  await client.close();
  await server.close();
}

test("exports the binding API", () => {
  assert.equal(typeof binding.bind, "function");
  for (const [name, expected] of Object.entries(API)) {
    const methods = Object.getOwnPropertyNames(binding[name].prototype).filter((key) => key !== "constructor");
    assert.deepEqual([...methods].sort(), expected, name);
  }
});

test("bind refuses a keep-alive of 0 ms", async () => {
  await assert.rejects(binding.bind({ secretKey: newKey(), keepAliveMs: 0 }), /keepAliveMs must be positive/);
});

test("bind refuses a secret key of the wrong length", async () => {
  await assert.rejects(binding.bind({ secretKey: new Uint8Array(31) }), /32 bytes/);
  await assert.rejects(binding.bind({ secretKey: new Uint8Array(33) }), /32 bytes/);
});

test("bind reports a bad relay URL and a bad bind address", async () => {
  await assert.rejects(binding.bind({ secretKey: newKey(), relayUrl: "not a url" }));
  await assert.rejects(binding.bind({ secretKey: newKey(), bindAddr: "nowhere" }), /invalid/i);
});

test("bind honours ipTransports=false and an explicit bind address", async () => {
  // Relay only needs a relay; nothing listens there, so it binds and never gets online.
  const relayOnly = await binding.bind({ secretKey: newKey(), ipTransports: false, relayUrl: "https://127.0.0.1:9" });
  const pinned = await binding.bind({ secretKey: newKey(), bindAddr: "127.0.0.1:0" });
  try {
    assert.deepEqual(relayOnly.boundSockets(), []);
    // The given IPv4 socket replaces the default one; iroh still adds its IPv6 socket.
    const ipv4 = pinned.boundSockets().filter((socket) => !socket.startsWith("["));
    assert.ok(ipv4.length > 0 && ipv4.every((socket) => socket.startsWith("127.0.0.1:")), String(pinned.boundSockets()));
  } finally {
    await relayOnly.close();
    await pinned.close();
  }
});

for (const waitFirst of [true, false]) {
  test(`the home relay watcher ends and Node exits with a ${waitFirst ? "pending" : "later"} next`, async () => {
    // A native promise that never settles keeps Node running after JS is done, so the check runs in
    // a child process the parent can time out.
    const script = `
      const assert = require("node:assert/strict");
      const { bind } = require(${JSON.stringify(require.resolve("../index.js"))});
      (async () => {
        const endpoint = await bind({ secretKey: require("node:crypto").randomBytes(32) });
        const watcher = await endpoint.watchHomeRelay();
        assert.deepEqual(await watcher.next(), []);
        const waiting = ${waitFirst} ? watcher.next() : undefined;
        await endpoint.close();
        console.log("endpoint closed");
        assert.equal(await (waiting ?? watcher.next()), null);
        assert.equal(await watcher.next(), null);
        console.log("watcher ended");
      })().catch((error) => {
        console.error(error);
        process.exitCode = 1;
      });
    `;
    const { stdout } = await promisify(execFile)(process.execPath, ["-e", script], { timeout: 10_000 });
    assert.equal(stdout.replaceAll("\r\n", "\n"), "endpoint closed\nwatcher ended\n");
  });
}

test("direct echo keeps every byte and reports the authenticated peer", async () => {
  const own = load("own");
  const server = await own.bind({ secretKey: newKey(), alpns: [ALPN] });
  const serving = serveEndpoint(server);
  const client = await own.bind({ secretKey: newKey() });
  try {
    const conn = await client.connect(server.id(), ALPN, { directAddrs: server.directAddrs() });
    assert.match(server.id(), /^[0-9a-f]{64}$/);
    assert.equal(conn.remoteId(), server.id());
    const { sent, received } = await echo(conn, 4 << 20);
    assert.equal(received, sent);
    assert.equal(conn.pathKind(), "direct");
    assert.equal(typeof conn.raw.rttMs(), "number");
  } finally {
    await closeBoth({ server, client });
    await serving;
  }
});

test("the server sees the client's authenticated id", async () => {
  const pair = await directPair();
  try {
    assert.equal(pair.sconn.remoteId(), pair.client.id());
  } finally {
    await closeBoth(pair);
  }
});

test("an application close code and reason reach the peer, as text or bytes", async () => {
  const pair = await directPair();
  try {
    pair.sconn.close(1, "not a member");
    assert.deepEqual({ ...(await pair.conn.closed()) }, { code: 1, reason: "not a member", kind: "application" });
  } finally {
    await closeBoth(pair);
  }
  const bytes = await directPair();
  try {
    bytes.conn.close(4, new Uint8Array([0x6f, 0x6b, 0xff]));
    assert.deepEqual({ ...(await bytes.sconn.closed()) }, { code: 4, reason: "ok�", kind: "application" });
    // The side that closed sees its own close.
    assert.equal((await bytes.conn.closed()).kind, "locally_closed");
  } finally {
    await closeBoth(bytes);
  }
});

test("a stream reset code is visible to the reader", async () => {
  const pair = await directPair();
  try {
    const accepted = pair.sconn.acceptBi();
    const { send } = await pair.conn.openBi();
    await send.write(Buffer.from("x"));
    const { recv } = await accepted;
    assert.equal(String(await recv.read(1)), "x");
    await send.reset(3);
    // Callers parse the code out of this exact text.
    await assert.rejects(recv.read(16), /^Error: stream reset by peer: error 3$/);
  } finally {
    await closeBoth(pair);
  }
});

test("raising the peer's stream limit lets more streams be open at once", async () => {
  const pair = await directPair();
  try {
    // Far beyond the default limit; none of these streams finishes while the others open.
    const count = 300;
    pair.sconn.setMaxConcurrentBiStreams(count);
    const accepted = [];
    const accepting = (async () => {
      while (accepted.length < count) accepted.push(await pair.sconn.acceptBi());
    })();
    const opened = [];
    for (let i = 0; i < count; i++) {
      const bi = await within(pair.conn.openBi(), 5000, `stream ${i}`);
      await bi.send.write(Buffer.from([i & 0xff]));
      opened.push(bi);
    }
    await within(accepting, 5000, "accepting");
    assert.equal(accepted.length, count);
  } finally {
    await closeBoth(pair);
  }
});

// Fails instead of hanging when an operation that must not wait on the peer does wait.
function within(promise, ms, what) {
  let timer;
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => reject(new Error(`${what} still pending after ${ms} ms`)), ms);
  });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}

// Opens a stream from the client and returns both ends once the server has accepted it.
async function openedStream(conn, sconn) {
  const peerStream = sconn.acceptBi();
  const local = await conn.openBi();
  await local.send.write(Buffer.from("x"));
  const peer = await peerStream;
  assert.equal((await peer.recv.read(1)).toString(), "x");
  return { local, peer };
}

// Proves the connection still carries a fresh stream end to end.
async function assertConnectionUsable(conn, sconn) {
  const { local, peer } = await openedStream(conn, sconn);
  await peer.send.write(Buffer.from("ok"));
  await peer.send.finish();
  assert.equal((await within(local.recv.read(16), 2000, "read")).toString(), "ok");
}

test("stop interrupts a read the peer never answers", async () => {
  const { server, client, conn, sconn } = await directPair();
  try {
    const { local, peer } = await openedStream(conn, sconn);
    // The server keeps its sending half open and silent, so this read cannot complete.
    const pending = assert.rejects(local.recv.read(16), /stopped/);
    await within(local.recv.stop(7), 2000, "stop");
    await pending;
    // STOP_SENDING reaches the peer asynchronously; its writes start failing once it arrives.
    await within(writeUntilStopped(peer.send), 2000, "peer observing stop");
    await assertConnectionUsable(conn, sconn);
  } finally {
    await client.close();
    await server.close();
  }
});

test("reset abandons a write the peer never reads", async () => {
  const { server, client, conn, sconn } = await directPair();
  try {
    const { local, peer } = await openedStream(conn, sconn);
    // Far beyond the stream's flow-control window while the server reads nothing.
    const pending = assert.rejects(local.send.write(Buffer.alloc(64 << 20)), /reset/);
    await within(local.send.reset(5), 2000, "reset");
    await pending;
    await assert.rejects(within(drain(peer.recv), 2000, "peer read"), /reset by peer: error 5$/);
    await assertConnectionUsable(conn, sconn);
  } finally {
    await client.close();
    await server.close();
  }
});

// Reads until end of stream or error.
async function drain(recv) {
  while ((await recv.read(1 << 20)) !== null) {}
}

// Keeps writing small chunks until the peer's STOP_SENDING arrives and a write rejects with code 7.
async function writeUntilStopped(send) {
  for (;;) {
    try {
      await send.write(Buffer.from("late"));
    } catch (error) {
      assert.match(String(error), /7/);
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
}

// Reads exactly n bytes (or fewer at end of stream) with reads of at most `max` bytes.
async function readExactly(recv, n, max = 1 << 20) {
  const parts = [];
  for (let left = n; left > 0;) {
    const b = await recv.read(Math.min(left, max));
    if (b === null) {
      break;
    }
    parts.push(b);
    left -= b.length;
  }
  return Buffer.concat(parts);
}

test("writes that are not awaited arrive in call order, then the FIN", async () => {
  const { server, client, conn, sconn } = await directPair();
  try {
    const { local, peer } = await openedStream(conn, sconn);
    // 8 MiB is several flow-control windows, so later writes queue behind a waiting one.
    const frames = Array.from({ length: 2048 }, (_, i) => {
      const f = Buffer.alloc(4096, i & 0xff);
      f.writeUInt32BE(i, 0);
      return f;
    });
    const writes = frames.map((f) => local.send.write(f));
    const finished = local.send.finish();
    const got = await within(readExactly(peer.recv, frames.length * 4096 + 1), 20000, "read");
    await Promise.all(writes);
    await finished;
    assert.equal(got.length, frames.length * 4096);
    assert.ok(got.equals(Buffer.concat(frames)));
  } finally {
    await client.close();
    await server.close();
  }
});

test("writeMany sends a batch in order and a write may reuse its buffer at once", async () => {
  const { server, client, conn, sconn } = await directPair();
  try {
    const { local, peer } = await openedStream(conn, sconn);
    const batch = [Buffer.from("alpha"), Buffer.alloc(0), new Uint8Array([1, 2, 3]), Buffer.alloc(70000, 9)];
    const many = local.send.writeMany(batch);
    const scratch = Buffer.from("first");
    const one = local.send.write(scratch);
    scratch.fill(0x7a); // already copied: the peer still sees "first"
    await Promise.all([many, one, local.send.writeMany([]), local.send.write(Buffer.alloc(0))]);
    await local.send.finish();
    const got = await within(readExactly(peer.recv, 1 << 20), 5000, "read");
    assert.ok(got.equals(Buffer.concat([...batch, Buffer.from("first")])));
  } finally {
    await client.close();
    await server.close();
  }
});

test("reads honor max, carry the remainder over and clamp oversized requests", async () => {
  const { server, client, conn, sconn } = await directPair();
  try {
    const { local, peer } = await openedStream(conn, sconn);
    const payload = crypto.randomBytes(300000);
    await local.send.write(payload);
    await local.send.finish();
    const parts = [];
    for (const max of [1, 2, 7, 1000, 0xffffffff]) {
      const b = await within(peer.recv.read(max), 5000, "read");
      assert.ok(b.length >= 1 && b.length <= max, `read(${max}) returned ${b.length} bytes`);
      parts.push(b);
    }
    parts.push(await within(readExactly(peer.recv, payload.length), 5000, "read"));
    assert.ok(Buffer.concat(parts).equals(payload));
    assert.equal(await peer.recv.read(16), null);
    await assert.rejects(peer.recv.read(0), /max must be > 0/);
  } finally {
    await client.close();
    await server.close();
  }
});

test("a busy stream does not starve the event loop", async () => {
  const { server, client, conn, sconn } = await directPair();
  try {
    const { local, peer } = await openedStream(conn, sconn);
    let ticks = 0;
    const timer = setInterval(() => ticks++, 1);
    const started = Date.now();
    const frame = Buffer.alloc(256, 1);
    const total = 16 << 20;
    const writer = (async () => {
      for (let sent = 0; sent < total; sent += frame.length) {
        await local.send.write(frame);
      }
      await local.send.finish();
    })();
    let received = 0;
    for (let b; (b = await peer.recv.read(4096));) {
      received += b.length;
    }
    await writer;
    clearInterval(timer);
    const elapsedMs = Date.now() - started;
    assert.equal(received, total);
    // Tens of thousands of writes and reads settled on the JS thread; a 1 ms timer must have kept
    // firing in between (about 1000 times here; about 15 if settled promises never yield).
    assert.ok(ticks >= elapsedMs / 20, `timer ran only ${ticks} times in ${elapsedMs} ms`);
  } finally {
    await client.close();
    await server.close();
  }
});

test("a write past the high-water mark waits for the peer, smaller ones do not", async () => {
  const { server, client, conn, sconn } = await directPair();
  try {
    const { local, peer } = await openedStream(conn, sconn);
    // The peer reads nothing. Small writes still settle at once: they only queue.
    const chunk = Buffer.alloc(64 << 10, 3);
    let queued = 0;
    let stalled = null;
    while (queued < 64 << 20) {
      const write = local.send.write(chunk);
      queued += chunk.length;
      const settled = await Promise.race([write.then(() => true), new Promise((r) => setTimeout(r, 300, false))]);
      if (!settled) {
        stalled = write;
        break;
      }
    }
    // Flow control (about 1 MiB of stream credit) plus the 1 MiB queue is the bound, not 64 MiB.
    assert.ok(stalled, "writes never waited for the peer");
    assert.ok(queued <= 16 << 20, `queued ${queued} bytes before backpressure`);
    // Once the peer reads, the stalled write and a flush complete and every byte arrives.
    const flushed = local.send.flush();
    const got = readExactly(peer.recv, queued);
    await within(stalled, 5000, "stalled write");
    await within(flushed, 5000, "flush");
    await local.send.finish();
    assert.equal((await within(got, 5000, "peer read")).length, queued);
    assert.equal(await peer.recv.read(16), null);
  } finally {
    await client.close();
    await server.close();
  }
});

test("a failed write rejects the next write, flush and finish", async () => {
  const { server, client, conn, sconn } = await directPair();
  try {
    const { local, peer } = await openedStream(conn, sconn);
    await peer.recv.stop(7);
    // The first writes after the stop may still settle: they were queued before it arrived.
    await within(writeUntilStopped(local.send), 2000, "writer observing stop");
    await assert.rejects(local.send.write(Buffer.from("more")), /^Error: sending stopped by peer: error 7$/);
    await assert.rejects(local.send.writeMany([Buffer.from("more")]), /7/);
    await assert.rejects(local.send.flush(), /7/);
    await assert.rejects(local.send.finish(), /7/);
    await assertConnectionUsable(conn, sconn);
  } finally {
    await client.close();
    await server.close();
  }
});

test("flush on an idle stream resolves; writes after finish or reset reject", async () => {
  const { server, client, conn, sconn } = await directPair();
  try {
    const first = await openedStream(conn, sconn);
    await first.local.send.flush();
    await first.local.send.write(Buffer.alloc(0));
    const finished = first.local.send.finish();
    // Refused from the moment finish is called, not only once the FIN is out.
    await assert.rejects(first.local.send.write(Buffer.from("late")), /closed stream/);
    await finished;
    await assert.rejects(first.local.send.finish(), /closed stream/);
    await first.local.send.flush();
    assert.equal(await first.peer.recv.read(16), null);

    const second = await openedStream(conn, sconn);
    await second.local.send.reset(9);
    await assert.rejects(second.local.send.write(Buffer.from("late")), /reset/);
    await assert.rejects(second.local.send.flush(), /reset/);
    await assert.rejects(second.local.send.finish(), /reset/);
    await assert.rejects(second.local.send.reset(9), /closed stream/);
    await assert.rejects(drain(second.peer.recv), /9/);
  } finally {
    await client.close();
    await server.close();
  }
});

test("a write that is never awaited or finished is still delivered", async () => {
  const { server, client, conn, sconn } = await directPair();
  try {
    const accepted = sconn.acceptBi();
    const payload = crypto.randomBytes(300_000);
    const { send } = await conn.openBi();
    void send.write(payload);
    const peer = await accepted;
    const got = await within(readExactly(peer.recv, payload.length), 5000, "peer read");
    assert.ok(got.equals(payload));
  } finally {
    await client.close();
    await server.close();
  }
});

test("acceptIncoming shows the source before the handshake and completes it", async () => {
  const server = await binding.bind({ secretKey: crypto.randomBytes(32), alpns: [ALPN] });
  const client = await binding.bind({ secretKey: crypto.randomBytes(32) });
  try {
    const next = server.acceptIncoming();
    const dialing = client.connect(server.id(), ALPN, {
      directAddrs: loopbackAddrs(server.boundSockets()),
    });
    const incoming = await within(next, 5000, "incoming");
    assert.equal(incoming.pathKind(), "direct");
    const clientPorts = client.boundSockets().map((a) => a.slice(a.lastIndexOf(":") + 1));
    const from = incoming.remoteAddr();
    assert.ok(clientPorts.includes(from.slice(from.lastIndexOf(":") + 1)), from);
    const sconn = await incoming.complete();
    const conn = await dialing;
    assert.equal(sconn.remoteId(), client.id());
    assert.deepEqual(sconn.alpn(), ALPN);
    assert.deepEqual(conn.alpn(), ALPN);
    // Used up: no second handshake, no refusal after the fact.
    assert.equal(incoming.pathKind(), "none");
    assert.equal(incoming.remoteAddr(), null);
    await assert.rejects(incoming.complete(), /already completed or refused/);
    assert.throws(() => incoming.refuse(), /already completed or refused/);
    await assertConnectionUsable(conn, sconn);
  } finally {
    await client.close();
    await server.close();
  }
});

test("a refused incoming connection fails the dial and the next one is served", async () => {
  const server = await binding.bind({ secretKey: crypto.randomBytes(32), alpns: [ALPN] });
  const client = await binding.bind({ secretKey: crypto.randomBytes(32) });
  try {
    const directAddrs = loopbackAddrs(server.boundSockets());
    const refused = assert.rejects(client.connect(server.id(), ALPN, { directAddrs }));
    (await within(server.acceptIncoming(), 5000, "incoming")).refuse();
    await within(refused, 5000, "refused dial");
    const dialing = client.connect(server.id(), ALPN, { directAddrs });
    const sconn = await (await within(server.acceptIncoming(), 5000, "incoming")).complete();
    await assertConnectionUsable(await dialing, sconn);
    // A closed endpoint ends the accept loop.
    const last = server.acceptIncoming();
    await server.close();
    assert.equal(await within(last, 5000, "accept after close"), null);
  } finally {
    await client.close();
    await server.close();
  }
});

test("datagrams cross as Buffers; oversized ones are refused", async () => {
  const { server, client, conn, sconn } = await directPair();
  try {
    const max = conn.maxDatagramSize();
    assert.ok(max >= 1000 && max < 65536, `max datagram size ${max}`);
    const next = sconn.readDatagram();
    const sent = crypto.randomBytes(900);
    conn.sendDatagram(sent);
    const got = await within(next, 5000, "datagram");
    assert.ok(Buffer.isBuffer(got));
    assert.ok(got.equals(sent));
    // The limit follows path MTU discovery, so only a size far beyond any MTU is a sure refusal.
    assert.throws(() => conn.sendDatagram(Buffer.alloc(16_384)), /too large/i);
    // A pending read ends with the connection instead of hanging.
    const pending = assert.rejects(sconn.readDatagram());
    conn.close(0, "done");
    await within(pending, 5000, "datagram read after close");
  } finally {
    await client.close();
    await server.close();
  }
});

test("stats count traffic and directRemoteAddr names the peer's socket", async () => {
  const { server, client, conn, sconn } = await directPair();
  try {
    const { local, peer } = await openedStream(conn, sconn);
    await local.send.write(Buffer.alloc(200_000, 1));
    await local.send.finish();
    assert.equal((await readExactly(peer.recv, 200_000)).length, 200_000);
    const rx = sconn.stats();
    const tx = conn.stats();
    assert.ok(rx.udpRxBytes >= 200_000, `server received ${rx.udpRxBytes} bytes`);
    assert.ok(tx.udpTxBytes >= 200_000, `client sent ${tx.udpTxBytes} bytes`);
    assert.ok(rx.udpRxDatagrams > 100 && tx.udpTxDatagrams > 100);
    assert.equal(typeof tx.lostPackets, "number");
    const serverPorts = server.boundSockets().map((a) => a.slice(a.lastIndexOf(":") + 1));
    const remote = conn.directRemoteAddr();
    assert.ok(serverPorts.includes(remote.slice(remote.lastIndexOf(":") + 1)), remote);
  } finally {
    await client.close();
    await server.close();
  }
});

test("external addresses can be added and removed; a relay token is accepted", async () => {
  const endpoint = await binding.bind({
    secretKey: crypto.randomBytes(32),
    // Nothing listens there: the endpoint binds and simply never gets online.
    relayUrl: "https://127.0.0.1:9",
    relayToken: "secret",
  });
  try {
    await endpoint.addExternalAddr("203.0.113.7:4444");
    assert.equal(await endpoint.removeExternalAddr("203.0.113.7:4444"), true);
    assert.equal(await endpoint.removeExternalAddr("203.0.113.7:4444"), false);
    await assert.rejects(endpoint.addExternalAddr("not an address"), /invalid/i);
    assert.deepEqual(
      endpoint.homeRelayStatus().map((r) => r.connected),
      endpoint.homeRelayStatus().map(() => false),
    );
  } finally {
    await endpoint.close();
  }
});
