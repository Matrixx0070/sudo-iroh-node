// Processes the tests start: a dev iroh-relay on a free loopback port in a private temp dir, and
// node helper scripts whose first output line is JSON. Only processes started here are signalled.
"use strict";
const { spawn } = require("node:child_process");
const fs = require("node:fs");
const net = require("node:net");
const os = require("node:os");
const path = require("node:path");
const { setTimeout: sleep } = require("node:timers/promises");

/** The iroh-relay 1.4.1 binary (IROH_RELAY_BIN, else ~/.cargo/bin/iroh-relay), or null when absent. */
function relayBin() {
  const candidate = process.env.IROH_RELAY_BIN || path.join(os.homedir(), ".cargo", "bin", "iroh-relay");
  return fs.existsSync(candidate) ? candidate : null;
}

/** A loopback TCP port nobody was listening on a moment ago. */
async function freePort() {
  const probe = net.createServer();
  await new Promise((resolve, reject) => {
    probe.once("error", reject);
    probe.listen(0, "127.0.0.1", resolve);
  });
  const { port } = probe.address();
  await new Promise((resolve) => probe.close(resolve));
  return port;
}

function canConnect(port) {
  return new Promise((resolve) => {
    const socket = net.connect(port, "127.0.0.1");
    socket.once("connect", () => {
      socket.destroy();
      resolve(true);
    });
    socket.once("error", () => resolve(false));
  });
}

// Resolves once something accepts TCP on the port; rejects after `timeoutMs`.
async function waitForListener(port, timeoutMs) {
  const giveUpAt = Date.now() + timeoutMs;
  for (;;) {
    if (await canConnect(port)) return;
    if (Date.now() > giveUpAt) throw new Error(`relay did not start on ${port}`);
    await sleep(100);
  }
}

/** SIGTERM to a child this module started, SIGKILL after 3 s; resolves once it has exited. */
function stopChild(child) {
  if (child.exitCode !== null || child.signalCode) return Promise.resolve();
  return new Promise((resolve) => {
    const kill = setTimeout(() => child.kill("SIGKILL"), 3000);
    child.once("exit", () => {
      clearTimeout(kill);
      resolve();
    });
    child.kill("SIGTERM");
  });
}

/** Starts a plain-HTTP dev relay; resolves { url, dir, stop() }. */
async function startRelay() {
  const bin = relayBin();
  if (bin === null) throw new Error("iroh-relay not found (set IROH_RELAY_BIN)");
  const port = await freePort();
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "sudo-iroh-relay-"));
  const config = path.join(dir, "relay.toml");
  const lines = ["enable_relay = true", `http_bind_addr = "127.0.0.1:${port}"`, "enable_quic_addr_discovery = false", "enable_metrics = false"];
  fs.writeFileSync(config, lines.join("\n") + "\n");
  const logFd = fs.openSync(path.join(dir, "relay.log"), "a");
  const child = spawn(bin, ["--dev", "-c", config], { stdio: ["ignore", logFd, logFd] });
  fs.closeSync(logFd);
  try {
    await waitForListener(port, 10_000);
  } catch (error) {
    await stopChild(child);
    throw error;
  }
  const stop = async () => {
    await stopChild(child);
    fs.rmSync(dir, { recursive: true, force: true });
  };
  return { url: `http://127.0.0.1:${port}/`, dir, stop };
}

/** Runs a node script whose first stdout line is JSON; resolves { info, child, stop() } once it is printed. */
function spawnJsonLine(script, argv) {
  const child = spawn(process.execPath, [script, ...argv], { stdio: ["ignore", "pipe", "pipe"] });
  return new Promise((resolve, reject) => {
    let out = "";
    let errors = "";
    let done = false;
    child.stderr.on("data", (chunk) => {
      errors += chunk;
    });
    child.stdout.on("data", (chunk) => {
      out += chunk;
      const end = out.indexOf("\n");
      if (end >= 0 && !done) {
        done = true;
        resolve({ info: JSON.parse(out.slice(0, end)), child, stop: () => stopChild(child) });
      }
    });
    // Exiting before the line was printed is a failure; after it, the caller's stop() handles it.
    child.on("exit", (code) => done || reject(new Error(`${script} exited ${code}: ${errors}`)));
  });
}

module.exports = { relayBin, freePort, startRelay, spawnJsonLine, stopChild };
