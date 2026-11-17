#!/usr/bin/env node
// The serving side of a benchmark: binds with this package (so it is never the slow end), prints
// { id, directAddrs } as one JSON line and serves the test protocol until SIGTERM.
//   node bench-peer.cjs [--relay <url>] [--relay-only]
"use strict";
const { randomBytes } = require("node:crypto");
const { setTimeout: sleep } = require("node:timers/promises");
const { load } = require("./bindings.cjs");
const { ALPN, serveEndpoint } = require("./wire.cjs");

function flag(name) {
  const at = process.argv.indexOf(name);
  return at < 0 ? undefined : (process.argv[at + 1] ?? true);
}

async function main() {
  const relayUrl = flag("--relay");
  const ep = await load("own").bind({
    secretKey: randomBytes(32),
    relayUrl,
    alpns: [ALPN],
    ipTransports: flag("--relay-only") === undefined ? undefined : false,
  });
  if (relayUrl) await ep.online();
  console.log(JSON.stringify({ id: ep.id(), directAddrs: ep.directAddrs() }));
  process.once("SIGTERM", () => {
    Promise.race([ep.close(), sleep(1000)]).then(() => process.exit(0));
  });
  await serveEndpoint(ep);
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
