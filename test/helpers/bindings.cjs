// Two Node bindings of iroh behind one interface, so each check runs unchanged against both:
//   "own"      this package (Buffers in and out)
//   "number0"  @number0/iroh (bytes as arrays of numbers)
// A stream here is { write(buf), finish(), reset(code), read(max) -> Buffer | null }.
"use strict";
const path = require("node:path");

const PACKAGE_ROOT = path.join(__dirname, "..", "..");

/** Where @number0/iroh is: under IROH_NUMBER0_DIR (a node_modules parent) when set, else the usual lookup. */
function number0Path() {
  const where = process.env.IROH_NUMBER0_DIR;
  const options = where ? { paths: [where] } : undefined;
  return options ? require.resolve("@number0/iroh", options) : require.resolve("@number0/iroh");
}

/** Whether @number0/iroh is installed for this host. */
function hasNumber0() {
  try {
    return Boolean(number0Path());
  } catch {
    return false;
  }
}

/** Peers on this host dial IPv4 sockets on loopback; the wildcard address is rewritten, IPv6 dropped. */
function loopbackAddrs(sockets) {
  const out = [];
  for (const socket of sockets) {
    if (socket.startsWith("[")) continue;
    out.push(socket.startsWith("0.0.0.0:") ? `127.0.0.1:${socket.slice("0.0.0.0:".length)}` : socket);
  }
  return out;
}

function ownBinding(api) {
  const stream = ({ send, recv }) => ({
    write: (buf) => send.write(buf),
    finish: () => send.finish(),
    reset: (code) => send.reset(code),
    read: (max) => recv.read(max),
  });
  const connection = (raw) => ({
    raw,
    remoteId: () => raw.remoteId(),
    openBi: () => raw.openBi().then(stream),
    acceptBi: () => raw.acceptBi().then(stream),
    close: (code, reason) => raw.close(code, reason),
    closed: () => raw.closed(),
    pathKind: () => raw.pathKind(),
  });
  async function bind({ secretKey, relayUrl, alpns, ipTransports, bindAddr }) {
    const ep = await api.bind({ secretKey, relayUrl, alpns, ipTransports, bindAddr });
    return {
      id: () => ep.id(),
      directAddrs: () => loopbackAddrs(ep.boundSockets()),
      online: () => ep.online(),
      accept: async () => {
        const raw = await ep.accept();
        return raw && connection(raw);
      },
      connect: (id, alpn, opts) => ep.connect(id, alpn, opts).then(connection),
      close: () => ep.close(),
    };
  }
  return { name: "own", bind };
}

function number0Binding(api) {
  const toArray = (bytes) => Array.from(bytes);
  const stream = ({ send, recv }) => ({
    write: (buf) => send.writeAll(toArray(buf)),
    finish: () => send.finish(),
    reset: (code) => send.reset(BigInt(code)),
    read: async (max) => {
      const bytes = await recv.read(max);
      return bytes.length > 0 ? Buffer.from(bytes) : null;
    },
  });
  const selectedKind = (raw) => {
    const selected = raw.paths().find((p) => p.isSelected);
    if (!selected) return "none";
    if (selected.isIp) return "direct";
    if (selected.isRelay) return "relay";
    return "none";
  };
  const connection = (raw) => ({
    raw,
    remoteId: () => String(raw.remoteId()),
    openBi: () => raw.openBi().then(stream),
    acceptBi: () => raw.acceptBi().then(stream),
    close: (code, reason) => raw.close(BigInt(code), toArray(Buffer.from(reason))),
    closed: () => raw.closed().then((reason) => ({ reason })),
    pathKind: () => selectedKind(raw),
  });
  async function bind({ secretKey, relayUrl, alpns, ipTransports, bindAddr }) {
    if (ipTransports === false) throw new Error("@number0/iroh cannot disable IP transports");
    const builder = api.Endpoint.builder();
    builder.applyMinimal();
    builder.secretKey(toArray(secretKey));
    if (alpns) builder.alpns(alpns.map(toArray));
    builder.relayMode(relayUrl ? api.RelayMode.customFromUrls([relayUrl]) : api.RelayMode.disabled());
    if (bindAddr) builder.bindAddr(bindAddr);
    const ep = await builder.bind();
    return {
      id: () => String(ep.id()),
      directAddrs: () => loopbackAddrs(ep.boundSockets()),
      online: () => ep.online(),
      accept: async () => {
        const incoming = await ep.acceptNext();
        return incoming && connection(await (await incoming.accept()).connect());
      },
      connect: async (id, alpn, opts = {}) => {
        const target = new api.EndpointAddr(api.EndpointId.fromString(id), opts.relayUrl || null, opts.directAddrs || []);
        return connection(await ep.connect(target, toArray(alpn)));
      },
      close: () => ep.close(),
    };
  }
  return { name: "number0", bind };
}

/** The binding called `name` ("own" or "number0"): { name, bind(options) }. */
function load(name) {
  if (name === "own") return ownBinding(require(PACKAGE_ROOT));
  if (name === "number0") return number0Binding(require(number0Path()));
  throw new Error(`unknown binding ${name}`);
}

module.exports = { load, hasNumber0, loopbackAddrs };
