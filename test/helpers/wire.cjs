// The test protocol (ALPN sudo-iroh-node/bench/0). The first byte a client writes on a stream picks
// what the server does with it:
//   U                       upload  — read to the end, answer the byte count (u64 BE), finish
//   D <u64 BE n> <u32 BE f>  download — send n bytes in writes of f bytes, finish
//   E                       echo    — send every chunk back until the end, finish
//   S                       stats   — answer { cpuS, maxRssMB } of the serving process as JSON
"use strict";
const crypto = require("node:crypto");

const ALPN = Buffer.from("sudo-iroh-node/bench/0");
const CHUNK = 1 << 20;
const DOWNLOAD_HEADER = 1 + 8 + 4;
// Throughput does not depend on the bytes, so one random megabyte is reused for every transfer.
const PAYLOAD = crypto.randomBytes(CHUNK);

/** Hex SHA-256 of a buffer. */
function sha256(buf) {
  return crypto.createHash("sha256").update(buf).digest("hex");
}

// Reads until the end of the stream, handing each chunk to `each`.
async function forEachChunk(stream, max, each) {
  for (;;) {
    const chunk = await stream.read(max);
    if (chunk === null) return;
    await each(chunk);
  }
}

async function answerUpload(stream, rest) {
  let total = rest.length;
  for (let chunk; (chunk = await stream.read(CHUNK)) !== null; ) total += chunk.length;
  const count = Buffer.alloc(8);
  count.writeBigUInt64BE(BigInt(total));
  await stream.write(count);
}

async function answerDownload(stream, rest) {
  // The header may arrive in pieces.
  let header = rest;
  while (header.length < DOWNLOAD_HEADER - 1) {
    header = Buffer.concat([header, await stream.read(DOWNLOAD_HEADER - 1 - header.length)]);
  }
  const frame = Math.max(1, Math.min(header.readUInt32BE(8), CHUNK));
  for (let left = Number(header.readBigUInt64BE(0)); left > 0; ) {
    const size = Math.min(left, frame);
    await stream.write(size === CHUNK ? PAYLOAD : PAYLOAD.subarray(0, size));
    left -= size;
  }
}

async function answerEcho(stream, rest) {
  if (rest.length > 0) await stream.write(rest);
  for (let chunk; (chunk = await stream.read(CHUNK)) !== null; ) await stream.write(chunk);
}

async function answerStats(stream) {
  const { user, system } = process.cpuUsage();
  const stats = { cpuS: (user + system) / 1e6, maxRssMB: process.resourceUsage().maxRSS / 1024 };
  await stream.write(Buffer.from(JSON.stringify(stats)));
}

/** Serves one stream ({ write, finish, read }) by its first byte; an unknown byte gets no answer. */
async function serveStream(stream) {
  const first = await stream.read(DOWNLOAD_HEADER);
  if (!first) return;
  const command = String.fromCharCode(first[0]);
  const rest = first.subarray(1);
  if (command === "U") await answerUpload(stream, rest);
  else if (command === "D") await answerDownload(stream, rest);
  else if (command === "E") await answerEcho(stream, rest);
  else if (command === "S") await answerStats(stream);
  else return;
  await stream.finish();
}

// Serves every stream of one connection until it closes.
async function serveConnection(conn, onError) {
  for (;;) {
    let stream;
    try {
      stream = await conn.acceptBi();
    } catch {
      return;
    }
    serveStream(stream).catch(onError);
  }
}

/** Serves every connection and stream until the endpoint closes. A failed handshake is reported and skipped. */
async function serveEndpoint(ep, onError = () => {}) {
  for (;;) {
    let conn;
    try {
      conn = await ep.accept();
    } catch (error) {
      onError(error);
      continue;
    }
    if (!conn) return;
    void serveConnection(conn, onError);
  }
}

// Reads a whole stream into one buffer.
async function readAll(stream, max) {
  const parts = [];
  await forEachChunk(stream, max, (chunk) => {
    parts.push(chunk);
  });
  return Buffer.concat(parts);
}

/** Downloads n bytes on a new stream (server writes of `frame`, reads of at most `readMax`); returns n. */
async function download(conn, n, { frame = CHUNK, readMax = CHUNK } = {}) {
  const stream = await conn.openBi();
  const header = Buffer.alloc(DOWNLOAD_HEADER);
  header.write("D", 0, "latin1");
  header.writeBigUInt64BE(BigInt(n), 1);
  header.writeUInt32BE(frame, 9);
  await stream.write(header);
  await stream.finish();
  let received = 0;
  await forEachChunk(stream, readMax, (chunk) => {
    received += chunk.length;
  });
  if (received !== n) throw new Error(`download short: ${received}`);
  return received;
}

/** Uploads n bytes in writes of `frame` on a new stream and checks the count the server answers. */
async function upload(conn, n, { frame = CHUNK } = {}) {
  const stream = await conn.openBi();
  await stream.write(Buffer.from("U"));
  const whole = frame === CHUNK ? PAYLOAD : PAYLOAD.subarray(0, frame);
  for (let sent = 0; sent < n; sent += frame) {
    await stream.write(n - sent >= frame ? whole : PAYLOAD.subarray(0, n - sent));
  }
  await stream.finish();
  const count = await stream.read(8);
  if (Number(count.readBigUInt64BE(0)) !== n) throw new Error("upload count mismatch");
}

/** The serving process's CPU seconds so far and peak RSS. */
async function peerStats(conn) {
  const stream = await conn.openBi();
  await stream.write(Buffer.from("S"));
  await stream.finish();
  return JSON.parse((await readAll(stream, 65536)).toString("utf8"));
}

/** Sends n random bytes through the echo command; returns { sent, received } SHA-256 digests. */
async function echo(conn, n) {
  const stream = await conn.openBi();
  const data = crypto.randomBytes(n);
  await stream.write(Buffer.from("E"));
  const writing = (async () => {
    for (let at = 0; at < n; at += 65536) await stream.write(data.subarray(at, at + 65536));
    await stream.finish();
  })();
  const back = await readAll(stream, CHUNK);
  await writing;
  return { sent: sha256(data), received: sha256(back) };
}

module.exports = { ALPN, CHUNK, PAYLOAD, serveStream, serveEndpoint, download, upload, peerStats, echo, sha256 };
