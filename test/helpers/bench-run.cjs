#!/usr/bin/env node
// One measured transfer, alone in its own process so its CPU time and peak RSS are its own.
//   node bench-run.cjs --binding own|number0 --peer '<json>' [--relay <url>] --path direct|relay
//                      --test up|down|streams8|conns8 [--mb N] [--frame BYTES]
// streams8 and conns8 spread --mb over 8 downloads at once. --frame is the size of each write (for
// downloads also of each read). Prints one JSON line. rssBeforeMB is taken after dialing and right
// before the transfer, so maxRssMB - rssBeforeMB is what the transfer added. cpuSPerMB is this
// process, peerCpuSPerMB the serving one over the same span.
"use strict";
const { randomBytes } = require("node:crypto");
const { setTimeout: sleep } = require("node:timers/promises");
const { load } = require("./bindings.cjs");
const { ALPN, CHUNK, download, upload, peerStats } = require("./wire.cjs");

const MiB = 1 << 20;

function option(name, fallback) {
  const at = process.argv.indexOf(name);
  return at < 0 ? fallback : process.argv[at + 1];
}

// Dials and keeps moving small amounts until the requested path is the one in use.
async function dial(ep, peer, relayUrl, wanted) {
  const opts = wanted === "direct" ? { relayUrl, directAddrs: peer.directAddrs } : { relayUrl };
  const conn = await ep.connect(peer.id, ALPN, opts);
  const giveUpAt = Date.now() + 10_000;
  await download(conn, MiB);
  for (let kind = conn.pathKind(); kind !== wanted; kind = conn.pathKind()) {
    if (Date.now() > giveUpAt) throw new Error(`path never became ${wanted} (is ${kind})`);
    await download(conn, 64 * 1024);
  }
  return conn;
}

// The transfer a test name stands for.
function transfer(test, conns, bytes, io) {
  if (test === "up") return upload(conns[0], bytes, io);
  if (test === "down") return download(conns[0], bytes, io);
  if (test === "streams8") return Promise.all(Array.from({ length: 8 }, () => download(conns[0], bytes / 8, io)));
  if (test === "conns8") return Promise.all(conns.map((conn) => download(conn, bytes / 8, io)));
  throw new Error(`unknown test ${test}`);
}
const TESTS = new Set(["up", "down", "streams8", "conns8"]);

async function main() {
  const binding = load(option("--binding", "own"));
  const peer = JSON.parse(option("--peer"));
  const relayUrl = option("--relay");
  const path = option("--path", "direct");
  const test = option("--test");
  const bytes = Number(option("--mb", "64")) * MiB;
  const frame = Number(option("--frame", String(CHUNK)));
  // Refused before dialing, so a typo costs nothing.
  if (!TESTS.has(test)) throw new Error(`unknown test ${test}`);
  const ep = await binding.bind({ secretKey: randomBytes(32), relayUrl });
  const count = test === "conns8" ? 8 : 1;
  const conns = await Promise.all(Array.from({ length: count }, () => dial(ep, peer, relayUrl, path)));
  const peerBefore = await peerStats(conns[0]);
  const rssBeforeMB = process.memoryUsage().rss / MiB;
  const cpuBefore = process.cpuUsage();
  const started = performance.now();
  await transfer(test, conns, bytes, { frame, readMax: frame });
  const secs = (performance.now() - started) / 1000;
  const cpu = process.cpuUsage(cpuBefore);
  const peerAfter = await peerStats(conns[0]);
  const mb = bytes / MiB;
  const result = {
    binding: binding.name,
    path,
    test,
    mb,
    frame,
    secs,
    mbps: mb / secs,
    cpuSPerMB: (cpu.user + cpu.system) / 1e6 / mb,
    peerCpuSPerMB: (peerAfter.cpuS - peerBefore.cpuS) / mb,
    maxRssMB: process.resourceUsage().maxRSS / 1024,
    peerMaxRssMB: peerAfter.maxRssMB,
    rssBeforeMB,
    rssEndMB: process.memoryUsage().rss / MiB,
    pathAfter: conns[0].pathKind(),
  };
  console.log(JSON.stringify(result));
  for (const conn of conns) conn.close(0, "done");
  await Promise.race([ep.close(), sleep(1000)]);
  process.exit(0);
}

main().catch((error) => {
  console.error(String(error.stack || error));
  process.exit(1);
});
