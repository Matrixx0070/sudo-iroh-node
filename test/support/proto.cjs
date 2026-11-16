// Test stream protocol (ALPN sudo-iroh-node/bench/0). First byte of each client-opened stream
// selects the behavior:
//   'U'                          upload: server reads to EOF, replies with the 8-byte BE count, finishes
//   'D' + u64 BE n + u32 BE f    download: server writes n bytes in frames of f bytes, finishes
//   'E'                          echo: server writes back every chunk until EOF
//   'S'                          stats: server replies with JSON { cpuS, maxRssMB } of its process
"use strict";
const crypto = require("node:crypto");

const ALPN = Buffer.from("sudo-iroh-node/bench/0");
const CHUNK = 1 << 20;
const DOWNLOAD_CMD = 13;
// One reusable random payload; contents do not matter for throughput.
const PAYLOAD = crypto.randomBytes(CHUNK);

/** Serves one stream ({ write, finish, read }) according to its command byte. */
async function serveStream(s) {
  let first = await s.read(DOWNLOAD_CMD);
  if (!first) {
    return;
  }
  const cmd = String.fromCharCode(first[0]);
  if (cmd === "U") {
    let n = first.length - 1;
    for (let b; (b = await s.read(CHUNK));) {
      n += b.length;
    }
    const out = Buffer.alloc(8);
    out.writeBigUInt64BE(BigInt(n));
    await s.write(out);
    await s.finish();
  } else if (cmd === "D") {
    // The command may arrive split across reads.
    while (first.length < DOWNLOAD_CMD) {
      first = Buffer.concat([first, await s.read(DOWNLOAD_CMD - first.length)]);
    }
    let left = Number(first.readBigUInt64BE(1));
    const frame = Math.min(Math.max(first.readUInt32BE(9), 1), CHUNK);
    while (left > 0) {
      const n = Math.min(left, frame);
      await s.write(n === CHUNK ? PAYLOAD : PAYLOAD.subarray(0, n));
      left -= n;
    }
    await s.finish();
  } else if (cmd === "E") {
    if (first.length > 1) {
      await s.write(first.subarray(1));
    }
    for (let b; (b = await s.read(CHUNK));) {
      await s.write(b);
    }
    await s.finish();
  } else if (cmd === "S") {
    const cpu = process.cpuUsage();
    await s.write(
      Buffer.from(
        JSON.stringify({
          cpuS: (cpu.user + cpu.system) / 1e6,
          maxRssMB: process.resourceUsage().maxRSS / 1024,
        }),
      ),
    );
    await s.finish();
  }
}

/** Accepts connections and their streams until the endpoint closes, serving each stream. */
async function serveEndpoint(ep, onError = () => {}) {
  for (;;) {
    let c;
    try {
      c = await ep.accept();
    } catch (e) {
      onError(e);
      continue;
    }
    if (!c) {
      return;
    }
    void serveConnection(c, onError);
  }
}

// Serves every stream of one connection until it closes.
async function serveConnection(c, onError) {
  for (;;) {
    let s;
    try {
      s = await c.acceptBi();
    } catch {
      return;
    }
    serveStream(s).catch(onError);
  }
}

/**
 * Downloads n bytes on a fresh stream; returns the byte count received. The server writes frames of
 * `frame` bytes and this side reads at most `readMax` bytes per call.
 */
async function download(conn, n, { frame = CHUNK, readMax = CHUNK } = {}) {
  const s = await conn.openBi();
  const cmd = Buffer.alloc(DOWNLOAD_CMD);
  cmd[0] = 68; // 'D'
  cmd.writeBigUInt64BE(BigInt(n), 1);
  cmd.writeUInt32BE(frame, 9);
  await s.write(cmd);
  await s.finish();
  let got = 0;
  for (let b; (b = await s.read(readMax));) {
    got += b.length;
  }
  if (got !== n) {
    throw new Error("download short: " + got);
  }
  return got;
}

/** Uploads n bytes in frames of `frame` bytes on a fresh stream and checks the server's byte count. */
async function upload(conn, n, { frame = CHUNK } = {}) {
  const s = await conn.openBi();
  await s.write(Buffer.from("U"));
  const full = frame === CHUNK ? PAYLOAD : PAYLOAD.subarray(0, frame);
  for (let left = n; left > 0; left -= frame) {
    await s.write(left >= frame ? full : PAYLOAD.subarray(0, left));
  }
  await s.finish();
  const ack = await s.read(8);
  if (Number(ack.readBigUInt64BE(0)) !== n) {
    throw new Error("upload count mismatch");
  }
}

/** Asks the serving process for its cumulative CPU seconds and peak RSS. */
async function peerStats(conn) {
  const s = await conn.openBi();
  await s.write(Buffer.from("S"));
  await s.finish();
  const parts = [];
  for (let b; (b = await s.read(65536));) {
    parts.push(b);
  }
  return JSON.parse(Buffer.concat(parts).toString());
}

/** Echoes n random bytes through 'E' and returns { sent, received } SHA-256 digests. */
async function echo(conn, n) {
  const s = await conn.openBi();
  const data = crypto.randomBytes(n);
  await s.write(Buffer.from("E"));
  const sending = (async () => {
    for (let o = 0; o < n; o += 65536) {
      await s.write(data.subarray(o, o + 65536));
    }
    await s.finish();
  })();
  const parts = [];
  for (let b; (b = await s.read(CHUNK));) {
    parts.push(b);
  }
  await sending;
  return { sent: sha256(data), received: sha256(Buffer.concat(parts)) };
}

/** Hex SHA-256 of a buffer. */
function sha256(b) {
  return crypto.createHash("sha256").update(b).digest("hex");
}

module.exports = {
  ALPN,
  CHUNK,
  PAYLOAD,
  serveStream,
  serveEndpoint,
  download,
  upload,
  peerStats,
  echo,
  sha256,
};
