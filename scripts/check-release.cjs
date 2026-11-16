// Release gate, run by CI before anything is published: the tag, the loader (package.json and
// the generated index.js), every platform package and the crate must carry the same version, and
// every platform package must hold its binary. Usage: node scripts/check-release.cjs [vX.Y.Z]
"use strict";
const fs = require("node:fs");
const path = require("node:path");

const root = path.join(__dirname, "..");
const pkg = JSON.parse(fs.readFileSync(path.join(root, "package.json"), "utf8"));
const problems = [];
const tag = process.argv[2];
if (tag && tag !== `v${pkg.version}`) {
  problems.push(`tag ${tag} does not match package.json version ${pkg.version}`);
}
const crate = /^version = "([^"]+)"/m.exec(fs.readFileSync(path.join(root, "Cargo.toml"), "utf8"));
if (crate?.[1] !== pkg.version) {
  problems.push(`Cargo.toml version ${crate?.[1]} does not match ${pkg.version}`);
}
// The generated loader carries the version it expects of a platform package.
const loader = fs.readFileSync(path.join(root, "index.js"), "utf8");
const expects = new Set([...loader.matchAll(/bindingPackageVersion !== '([^']+)'/g)].map((m) => m[1]));
if (expects.size !== 1 || !expects.has(pkg.version)) {
  problems.push(`index.js expects platform packages at ${[...expects].join(", ") || "no version"}`);
}
const dirs = fs.readdirSync(path.join(root, "npm"));
const expected = pkg.napi.targets.length;
if (dirs.length !== expected) {
  problems.push(`${dirs.length} platform packages for ${expected} targets`);
}
for (const dir of dirs) {
  const sub = JSON.parse(fs.readFileSync(path.join(root, "npm", dir, "package.json"), "utf8"));
  if (sub.version !== pkg.version) {
    problems.push(`${sub.name} is ${sub.version}, expected ${pkg.version}`);
  }
  if (pkg.optionalDependencies[sub.name] !== pkg.version) {
    problems.push(`optionalDependencies["${sub.name}"] is ${pkg.optionalDependencies[sub.name]}`);
  }
  // Skipped without a tag, so the check also runs on a tree that has no binaries yet.
  if (tag && !fs.existsSync(path.join(root, "npm", dir, sub.main))) {
    problems.push(`${sub.name} has no ${sub.main}`);
  }
}
if (tag && fs.readdirSync(root).some((f) => f.endsWith(".node"))) {
  problems.push("the loader package must not contain a .node file when it is published");
}
if (problems.length > 0) {
  console.error(problems.join("\n"));
  process.exit(1);
}
console.log(`release ${pkg.version}: ${dirs.length} platform packages consistent`);
