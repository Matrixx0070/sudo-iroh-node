// Throughput and memory over a direct loopback connection between two processes, with no relay.
// Needs a release build; runs only with IROH_NODE_BENCH=1. Unless a case says otherwise its bytes
// travel on one stream; each prints MB/s, peak RSS and CPU seconds per MB.
//   IROH_NODE_MIN_MBPS        lowest acceptable MB/s for the 256 MiB single-stream cases (default
//                             100: well under what this package reaches on a quiet machine, so slow
//                             CI passes, and well over what a byte-by-byte binding manages)
//   IROH_NODE_MAX_GROWTH_MB   highest acceptable RSS growth for 8 x 64 MiB at once (default 120)
//   IROH_NODE_BENCH_RUNS      runs per case, median reported (default 3)
//   IROH_NODE_BENCH_FRAMES=1  also measure 1 KiB and 16 KiB frames (one call per frame)
"use strict";
const assert = require("node:assert/strict");
const { test } = require("node:test");
const { measure, formatSummary } = require("./helpers/bench-measure.cjs");

const env = process.env;
const MIN_MBPS = Number(env.IROH_NODE_MIN_MBPS || 100);
const MAX_GROWTH_MB = Number(env.IROH_NODE_MAX_GROWTH_MB || 120);
const RUNS = Number(env.IROH_NODE_BENCH_RUNS || 3);
const benchSkip = env.IROH_NODE_BENCH === "1" ? false : "set IROH_NODE_BENCH=1";
const framesSkip = benchSkip || (env.IROH_NODE_BENCH_FRAMES === "1" ? false : "set IROH_NODE_BENCH_FRAMES=1");

test("256 MiB on one stream: throughput, peak RSS, CPU", { skip: benchSkip, timeout: 600_000 }, async () => {
  const cases = [
    { test: "down", mb: 256 },
    { test: "up", mb: 256 },
    { test: "streams8", mb: 512 },
  ];
  const summary = await measure({ cases, n: RUNS });
  console.log(formatSummary(summary).join("\n"));
  assert.ok(summary["down:256"].mbps.median >= MIN_MBPS, "single-stream download MB/s");
  assert.ok(summary["up:256"].mbps.median >= MIN_MBPS, "single-stream upload MB/s");
  assert.ok(summary["streams8:512"].rssGrowthMB.max <= MAX_GROWTH_MB, "8 x 64 MiB RSS growth MB");
});

test("small frames: one call per 1 KiB / 16 KiB frame", { skip: framesSkip, timeout: 600_000 }, async () => {
  const cases = ["up", "down"].flatMap((direction) => [
    { test: direction, mb: 32, frame: 1024 },
    { test: direction, mb: 128, frame: 16384 },
  ]);
  const summary = await measure({ cases, n: RUNS });
  console.log(formatSummary(summary).join("\n"));
});
