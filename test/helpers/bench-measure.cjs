#!/usr/bin/env node
// Repeats bench-run.cjs n times per case against one direct serving process and reports medians.
//   node bench-measure.cjs [--n 5] [--binding own] [--cases down:256,up:256,up:64:1024] [--out file.json]
// A case is test:megabytes[:frameBytes]. throughput.test.cjs uses it too.
"use strict";
const { execFileSync } = require("node:child_process");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { spawnJsonLine } = require("./processes.cjs");

const DEFAULT_FRAME = 1 << 20;

/** The nearest-rank median. */
function median(values) {
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.ceil(sorted.length / 2) - 1];
}

/** A case's key in the summary: "down:256", with ":<frame>" only for a non-default frame. */
function caseKey({ test, mb, frame = DEFAULT_FRAME }) {
  const base = `${test}:${mb}`;
  return frame === DEFAULT_FRAME ? base : `${base}:${frame}`;
}

// One bench-run.cjs process; its last stdout line is the result.
function runOnce(binding, peer, { test, mb, frame }) {
  const argv = [path.join(__dirname, "bench-run.cjs"), "--binding", binding, "--peer", JSON.stringify(peer), "--path", "direct", "--test", test, "--mb", String(mb), "--frame", String(frame)];
  const out = execFileSync(process.execPath, argv, { encoding: "utf8", timeout: 300_000 });
  return JSON.parse(out.trim().split("\n").at(-1));
}

function summarize(runs, n) {
  for (const run of runs) run.rssGrowthMB = run.maxRssMB - run.rssBeforeMB;
  const field = (name) => runs.map((run) => run[name]);
  return {
    n,
    mbps: { median: median(field("mbps")), min: Math.min(...field("mbps")) },
    maxRssMB: { median: median(field("maxRssMB")), max: Math.max(...field("maxRssMB")) },
    peerMaxRssMB: { max: Math.max(...field("peerMaxRssMB")) },
    rssGrowthMB: { median: median(field("rssGrowthMB")), max: Math.max(...field("rssGrowthMB")) },
    cpuSPerMB: { median: median(field("cpuSPerMB")) },
    peerCpuSPerMB: { median: median(field("peerCpuSPerMB")) },
    runs,
  };
}

/** Measures each { test, mb, frame } case n times over a direct path; resolves the summary by case. */
async function measure({ cases, n = 3, binding = "own" }) {
  const peer = await spawnJsonLine(path.join(__dirname, "bench-peer.cjs"), []);
  const summary = {};
  try {
    for (const spec of cases) {
      const one = { ...spec, frame: spec.frame ?? DEFAULT_FRAME };
      const runs = [];
      for (let i = 0; i < n; i++) runs.push(runOnce(binding, peer.info, one));
      summary[caseKey(spec)] = summarize(runs, n);
    }
  } finally {
    await peer.stop();
  }
  return summary;
}

/** "down:256,up:64:1024" as [{ test, mb, frame }]. */
function parseCases(spec) {
  return spec.split(",").map((entry) => {
    const [test, mb, frame] = entry.split(":");
    return { test, mb: Number(mb), frame: frame ? Number(frame) : DEFAULT_FRAME };
  });
}

/** One readable line per case. */
function formatSummary(summary) {
  const fixed = (value, digits = 0) => value.toFixed(digits);
  return Object.entries(summary).map(([key, s]) =>
    [
      `${key}: ${fixed(s.mbps.median)} MB/s (min ${fixed(s.mbps.min)})`,
      `peak RSS ${fixed(s.maxRssMB.max)} MB (peer ${fixed(s.peerMaxRssMB.max)})`,
      `growth ${fixed(s.rssGrowthMB.max)} MB`,
      `CPU ${fixed(s.cpuSPerMB.median * 1000, 2)} ms/MB (peer ${fixed(s.peerCpuSPerMB.median * 1000, 2)})`,
    ].join(", "),
  );
}

if (require.main === module) {
  const option = (name, fallback) => {
    const at = process.argv.indexOf(name);
    return at < 0 ? fallback : process.argv[at + 1];
  };
  const run = {
    cases: parseCases(option("--cases", "down:256,up:256,streams8:512")),
    n: Number(option("--n", "5")),
    binding: option("--binding", "own"),
  };
  measure(run).then((summary) => {
    const meta = { host: os.hostname(), arch: process.arch, node: process.version, cpus: os.cpus().length, date: new Date().toISOString() };
    const out = option("--out");
    if (out) fs.writeFileSync(out, JSON.stringify({ meta, summary }, null, 2) + "\n");
    process.stderr.write(formatSummary(summary).join("\n") + "\n");
  });
}

module.exports = { measure, parseCases, formatSummary, caseKey, median };
