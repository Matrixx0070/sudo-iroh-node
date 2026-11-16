#!/usr/bin/env node
// Runs bench.cjs n times per case against one direct peer process and reports medians.
//   node measure.cjs [--n 5] [--binding own] [--cases down:256,up:256,up:64:1024] [--out file.json]
// A case is test:megabytes[:frameBytes]. Also used by throughput.test.cjs.
"use strict";
const { execFileSync } = require("node:child_process");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { spawnJsonLine } = require("./procs.cjs");

const DEFAULT_FRAME = 1 << 20;

/** Median (nearest-rank) of numbers. */
function median(values) {
  const s = values.toSorted((a, b) => a - b);
  return s[Math.ceil(s.length / 2) - 1];
}

/** Key of a case in the summary: "down:256" or "up:64:1024" for a non-default frame size. */
function caseKey({ test, mb, frame = DEFAULT_FRAME }) {
  return frame === DEFAULT_FRAME ? `${test}:${mb}` : `${test}:${mb}:${frame}`;
}

/** Measures each { test, mb, frame } case n times over a direct path; resolves a per-case summary. */
async function measure({ cases, n = 3, binding = "own" }) {
  const peer = await spawnJsonLine(path.join(__dirname, "peer.cjs"), []);
  const summary = {};
  try {
    for (const c of cases) {
      const { test, mb, frame = DEFAULT_FRAME } = c;
      const runs = [];
      for (let i = 0; i < n; i++) {
        const out = execFileSync(
          process.execPath,
          [
            path.join(__dirname, "bench.cjs"),
            "--binding",
            binding,
            "--peer",
            JSON.stringify(peer.info),
            "--path",
            "direct",
            "--test",
            test,
            "--mb",
            String(mb),
            "--frame",
            String(frame),
          ],
          { encoding: "utf8", timeout: 300000 },
        );
        runs.push(JSON.parse(out.trim().split("\n").pop()));
      }
      for (const r of runs) {
        r.rssGrowthMB = r.maxRssMB - r.rssBeforeMB;
      }
      const pick = (f) => runs.map((r) => r[f]);
      summary[caseKey(c)] = {
        n,
        mbps: { median: median(pick("mbps")), min: Math.min(...pick("mbps")) },
        maxRssMB: { median: median(pick("maxRssMB")), max: Math.max(...pick("maxRssMB")) },
        peerMaxRssMB: { max: Math.max(...pick("peerMaxRssMB")) },
        rssGrowthMB: { median: median(pick("rssGrowthMB")), max: Math.max(...pick("rssGrowthMB")) },
        cpuSPerMB: { median: median(pick("cpuSPerMB")) },
        peerCpuSPerMB: { median: median(pick("peerCpuSPerMB")) },
        runs,
      };
    }
  } finally {
    await peer.stop();
  }
  return summary;
}

/** Parses "down:256,up:64:1024" into [{ test, mb, frame }]. */
function parseCases(spec) {
  return spec.split(",").map((c) => {
    const [test, mb, frame] = c.split(":");
    return { test, mb: Number(mb), frame: frame ? Number(frame) : DEFAULT_FRAME };
  });
}

/** One human-readable line per case. */
function formatSummary(summary) {
  return Object.entries(summary).map(
    ([k, v]) =>
      `${k}: ${v.mbps.median.toFixed(0)} MB/s (min ${v.mbps.min.toFixed(0)}), peak RSS ${v.maxRssMB.max.toFixed(0)} MB (peer ${v.peerMaxRssMB.max.toFixed(0)}), growth ${v.rssGrowthMB.max.toFixed(0)} MB, CPU ${(v.cpuSPerMB.median * 1000).toFixed(2)} ms/MB (peer ${(v.peerCpuSPerMB.median * 1000).toFixed(2)})`,
  );
}

if (require.main === module) {
  const args = process.argv.slice(2);
  const opt = (k, d) => (args.includes(k) ? args[args.indexOf(k) + 1] : d);
  measure({
    cases: parseCases(opt("--cases", "down:256,up:256,streams8:512")),
    n: Number(opt("--n", "5")),
    binding: opt("--binding", "own"),
  }).then((summary) => {
    const meta = {
      host: os.hostname(),
      arch: process.arch,
      node: process.version,
      cpus: os.cpus().length,
      date: new Date().toISOString(),
    };
    const json = JSON.stringify({ meta, summary }, null, 2) + "\n";
    if (opt("--out")) {
      fs.writeFileSync(opt("--out"), json);
    }
    process.stderr.write(formatSummary(summary).join("\n") + "\n");
  });
}

module.exports = { measure, parseCases, formatSummary, caseKey };
