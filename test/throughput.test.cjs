// Throughput and memory check over a relay-less direct loopback connection between two processes
// (needs a release build). Opt-in with IROH_NODE_BENCH=1. Each case moves its bytes on one stream
// unless it says otherwise and prints MB/s, peak RSS and CPU seconds per MB.
//   IROH_NODE_MIN_MBPS        floor for the 256 MiB single-stream transfers, MB/s (default 100:
//                             far below what this binding does on a quiet machine, so slow CI
//                             runners pass, yet far above a per-byte marshalling binding)
//   IROH_NODE_MAX_GROWTH_MB   RSS growth ceiling for 8 x 64 MiB in parallel, MB (default 120)
//   IROH_NODE_BENCH_RUNS      runs per case; the median is reported (default 3)
//   IROH_NODE_BENCH_FRAMES=1  also measure 1 KiB and 16 KiB frames (one write/read call per frame)
"use strict";
const assert = require("node:assert/strict");
const { test } = require("node:test");
const { measure, formatSummary } = require("./support/measure.cjs");

const MIN_MBPS = Number(process.env.IROH_NODE_MIN_MBPS || 100);
const MAX_GROWTH_MB = Number(process.env.IROH_NODE_MAX_GROWTH_MB || 120);
const RUNS = Number(process.env.IROH_NODE_BENCH_RUNS || 3);
const skip = process.env.IROH_NODE_BENCH === "1" ? false : "set IROH_NODE_BENCH=1";

test("256 MiB on one stream: throughput, peak RSS, CPU", { skip, timeout: 600000 }, async () => {
  const summary = await measure({
    cases: [
      { test: "down", mb: 256 },
      { test: "up", mb: 256 },
      { test: "streams8", mb: 512 },
    ],
    n: RUNS,
  });
  console.log(formatSummary(summary).join("\n"));
  assert.ok(summary["down:256"].mbps.median >= MIN_MBPS, "single-stream download MB/s");
  assert.ok(summary["up:256"].mbps.median >= MIN_MBPS, "single-stream upload MB/s");
  assert.ok(summary["streams8:512"].rssGrowthMB.max <= MAX_GROWTH_MB, "8 x 64 MiB RSS growth MB");
});

test(
  "small frames: one call per 1 KiB / 16 KiB frame",
  { skip: skip || (process.env.IROH_NODE_BENCH_FRAMES === "1" ? false : "set IROH_NODE_BENCH_FRAMES=1"), timeout: 600000 },
  async () => {
    const summary = await measure({
      cases: [
        { test: "up", mb: 32, frame: 1024 },
        { test: "down", mb: 32, frame: 1024 },
        { test: "up", mb: 128, frame: 16384 },
        { test: "down", mb: 128, frame: 16384 },
      ],
      n: RUNS,
    });
    console.log(formatSummary(summary).join("\n"));
  },
);
