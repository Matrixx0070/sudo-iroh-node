#!/usr/bin/env node
// A small server speaking the SUDO-AI tunnel protocol (ALPN sudo-ai/tunnel/1) over this package,
// relay only, so the tests can drive the binding the way the tunnel does.
//   node tunnel-server.cjs <relayUrl> <dir>
// Writes <dir>/descriptor.json ({ endpointId, relayUrl }) and <dir>/decoy-descriptor.json (a key
// nobody serves), then prints the descriptor as one JSON line. Only endpoint ids listed in
// <dir>/allow.txt (read on each connection) are served; anyone else is closed with code 1
// "not a member".
//
// Wire: every frame is a u32 BE length + payload. A stream carries one exchange: the client sends a
// JSON head, then body frames (HTTP) or message frames (WebSocket: 1 opcode byte 1 text / 2 binary /
// 8 close, then data), then FIN; the server answers the same way. A failed request resets the stream
// with code 3.
//   GET  /sudo/hello       200 {"ok":true}
//   GET  /sudo/blob?n=N    200, N bytes where byte i is i % 251
//   POST /sudo/echo        200, the request body
//   GET  /sudo/fail        stream reset 3
//   ws   /sudo             101, then every text/binary message echoed until a close, which is echoed
"use strict";
const { randomBytes } = require("node:crypto");
const fs = require("node:fs");
const path = require("node:path");
const { load } = require("./bindings.cjs");

const TUNNEL_ALPN = Buffer.from("sudo-ai/tunnel/1");
const RESET_UPSTREAM = 3;
const NOT_MEMBER = 1;
const MAX_FRAME = 1 << 20;

/** u32 BE length + payload. */
function frame(payload) {
  const length = Buffer.alloc(4);
  length.writeUInt32BE(payload.length);
  return Buffer.concat([length, payload]);
}

/** Reads length-prefixed frames off a stream ({ read(max) }); `next()` is null at a clean end. */
class FrameReader {
  constructor(stream) {
    this.stream = stream;
    this.pending = Buffer.alloc(0);
  }

  // Buffers until `n` bytes are there; false when the stream ends first.
  async fill(n) {
    while (this.pending.length < n) {
      const chunk = await this.stream.read(MAX_FRAME);
      if (chunk === null) return false;
      this.pending = Buffer.concat([this.pending, chunk]);
    }
    return true;
  }

  async next() {
    if (!(await this.fill(4))) return null;
    const length = this.pending.readUInt32BE(0);
    if (!(await this.fill(4 + length))) throw new Error("truncated");
    const payload = Buffer.from(this.pending.subarray(4, 4 + length));
    this.pending = this.pending.subarray(4 + length);
    return payload;
  }
}

async function writeFrames(send, body) {
  for (let at = 0; at < body.length; at += MAX_FRAME) await send.write(frame(body.subarray(at, at + MAX_FRAME)));
}

async function respond(send, status, body) {
  await send.write(frame(Buffer.from(JSON.stringify({ status, headers: [] }))));
  await writeFrames(send, body);
  await send.finish();
}

function blob(n) {
  const out = Buffer.alloc(n);
  for (let i = 0; i < n; i++) out[i] = i % 251;
  return out;
}

async function serveHttp(head, frames, send) {
  const url = new URL(head.target, "http://tunnel.invalid");
  const body = [];
  for (let part; (part = await frames.next()); ) body.push(part);
  if (head.method === "GET" && url.pathname === "/sudo/hello") return respond(send, 200, Buffer.from(JSON.stringify({ ok: true })));
  if (head.method === "GET" && url.pathname === "/sudo/blob") return respond(send, 200, blob(Number(url.searchParams.get("n") || 0)));
  if (head.method === "POST" && url.pathname === "/sudo/echo") return respond(send, 200, Buffer.concat(body));
  if (url.pathname === "/sudo/fail") return send.reset(RESET_UPSTREAM);
  return respond(send, 404, Buffer.alloc(0));
}

async function serveWebSocket(frames, send) {
  await send.write(frame(Buffer.from(JSON.stringify({ status: 101, headers: [] }))));
  for (let message; (message = await frames.next()); ) {
    await send.write(frame(message));
    if (message[0] === 8) break;
  }
  await send.finish();
}

async function serveStream({ send, recv }) {
  const frames = new FrameReader(recv);
  try {
    const head = JSON.parse(String(await frames.next()));
    if (head.type === "http") return await serveHttp(head, frames, send);
    if (head.type === "ws") return await serveWebSocket(frames, send);
    await send.reset(RESET_UPSTREAM);
  } catch {
    await send.reset(RESET_UPSTREAM).catch(() => {});
  }
}

function members(dir) {
  try {
    return new Set(fs.readFileSync(path.join(dir, "allow.txt"), "utf8").split(/\s+/).filter(Boolean));
  } catch {
    return new Set();
  }
}

async function serveConnection(conn, dir) {
  if (!members(dir).has(conn.remoteId())) {
    conn.close(NOT_MEMBER, "not a member");
    return;
  }
  for (;;) {
    let stream;
    try {
      stream = await conn.acceptBi();
    } catch {
      return;
    }
    void serveStream(stream);
  }
}

async function main() {
  const [relayUrl, dir] = process.argv.slice(2);
  const own = require(path.join(__dirname, "..", ".."));
  const ep = await own.bind({ secretKey: randomBytes(32), relayUrl, alpns: [TUNNEL_ALPN], ipTransports: false });
  await ep.online();
  const descriptor = { endpointId: ep.id(), relayUrl };
  // A real key nobody serves: dialing it must fail rather than reach this server.
  const decoy = await load("own").bind({ secretKey: randomBytes(32), relayUrl, ipTransports: false });
  fs.writeFileSync(path.join(dir, "decoy-descriptor.json"), JSON.stringify({ endpointId: decoy.id(), relayUrl }));
  await decoy.close();
  fs.writeFileSync(path.join(dir, "descriptor.json"), JSON.stringify(descriptor));
  console.log(JSON.stringify(descriptor));
  process.once("SIGTERM", () => {
    ep.close().finally(() => process.exit(0));
  });
  for (let conn; (conn = await ep.accept().catch(() => undefined)) !== null; ) {
    if (conn) void serveConnection(conn, dir);
  }
}

if (require.main === module) {
  main().catch((error) => {
    console.error(error);
    process.exit(1);
  });
}

module.exports = { TUNNEL_ALPN, frame, FrameReader };
