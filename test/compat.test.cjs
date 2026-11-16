// Wire compatibility with @number0/iroh (the addon this binding replaces under the SUDO-AI tunnel):
// both directions over a direct loopback path, and numeric close codes. With an iroh-relay 1.4.1
// binary the same pairings also run through a local dev relay (own port, own tmp dir).
// Each group skips when its dependency is missing: @number0/iroh (resolvable or under
// IROH_NUMBER0_DIR, a directory whose node_modules holds it) and IROH_RELAY_BIN (default
// ~/.cargo/bin/iroh-relay).
"use strict";
const assert = require("node:assert/strict");
const crypto = require("node:crypto");
const { after, before, describe, test } = require("node:test");
const { load, hasNumber0 } = require("./support/adapters.cjs");
const { relayBin, startRelay } = require("./support/procs.cjs");
const { ALPN, serveEndpoint, echo } = require("./support/proto.cjs");

const noNumber0 = hasNumber0() ? false : "@number0/iroh not installed (set IROH_NUMBER0_DIR)";
const noRelay = relayBin() ? false : "iroh-relay not found (set IROH_RELAY_BIN)";
const PAIRS = [
  ["number0", "own"],
  ["own", "number0"],
];

describe("compat with @number0/iroh, direct", { skip: noNumber0 }, () => {
  for (const [sb, cb] of PAIRS) {
    test(`${cb} client -> ${sb} server: 4 MiB echo`, () => pairing(undefined, sb, cb, "direct"));
    test(`${sb} server close (1, "not a member") reaches ${cb} client`, () =>
      closeCode(undefined, sb, cb));
  }
});

describe("compat via a local iroh-relay", { skip: noRelay }, () => {
  let relay;
  before(async () => {
    relay = await startRelay();
  });
  after(async () => {
    await relay?.stop();
  });

  test("own <-> own via relay only", () => pairing(relay.url, "own", "own", "relay"));

  for (const [sb, cb] of PAIRS) {
    for (const p of ["relay", "direct"]) {
      test(`${cb} client -> ${sb} server via ${p}: 4 MiB echo`, { skip: noNumber0 }, () =>
        pairing(relay.url, sb, cb, p),
      );
    }
    test(`${sb} server close (1, "not a member") reaches ${cb} client`, { skip: noNumber0 }, () =>
      closeCode(relay.url, sb, cb),
    );
  }
});

// One echo pairing: server binding sb, client binding cb, path "relay" | "direct". Relay-only is
// enforced on whichever side is this binding (@number0/iroh cannot clear IP transports).
async function pairing(relayUrl, sb, cb, p) {
  const server = await load(sb).bind({
    secretKey: crypto.randomBytes(32),
    relayUrl,
    alpns: [ALPN],
    ipTransports: p === "relay" && sb === "own" ? false : undefined,
  });
  if (relayUrl) {
    await server.online();
  }
  const serving = serveEndpoint(server);
  const client = await load(cb).bind({
    secretKey: crypto.randomBytes(32),
    relayUrl,
    ipTransports: p === "relay" && cb === "own" ? false : undefined,
  });
  try {
    const opts = p === "direct" ? { relayUrl, directAddrs: server.directAddrs() } : { relayUrl };
    const conn = await client.connect(server.id(), ALPN, opts);
    assert.equal(conn.remoteId(), server.id());
    const r = await echo(conn, 4 << 20);
    assert.equal(r.received, r.sent);
    // Direct paths may start on the relay and upgrade after holepunching.
    if (p === "direct") {
      for (let i = 0; i < 40 && conn.pathKind() !== "direct"; i++) {
        await echo(conn, 65536);
      }
    }
    assert.equal(conn.pathKind(), p);
    conn.close(0, "done");
  } finally {
    await client.close();
    await server.close();
    await serving;
  }
}

// Server closes with (1, "not a member"); the client must observe the code and reason.
async function closeCode(relayUrl, sb, cb) {
  const server = await load(sb).bind({
    secretKey: crypto.randomBytes(32),
    relayUrl,
    alpns: [ALPN],
  });
  const client = await load(cb).bind({ secretKey: crypto.randomBytes(32), relayUrl });
  try {
    const accepted = server.accept();
    const conn = await client.connect(server.id(), ALPN, {
      relayUrl,
      directAddrs: server.directAddrs(),
    });
    const sconn = await accepted;
    assert.equal(sconn.remoteId(), client.id());
    sconn.close(1, "not a member");
    const info = await conn.closed();
    if (cb === "own") {
      assert.equal(info.code, 1);
      assert.equal(info.reason, "not a member");
    } else {
      assert.match(info.reason, /not a member/);
    }
  } finally {
    await client.close();
    await server.close();
  }
}
