// Run by `npm version` after `napi version` has stamped the platform packages: brings the
// loader's optionalDependencies, the crate (Cargo.toml, Cargo.lock) and the version the generated
// loader expects of a platform package (index.js) to the loader's version, so a release always
// installs the platform packages built from the same commit.
"use strict";
const fs = require("node:fs");
const path = require("node:path");

const root = path.join(__dirname, "..");
const file = path.join(root, "package.json");
const pkg = JSON.parse(fs.readFileSync(file, "utf8"));
for (const name of Object.keys(pkg.optionalDependencies)) {
  pkg.optionalDependencies[name] = pkg.version;
}
fs.writeFileSync(file, JSON.stringify(pkg, null, 2) + "\n");

// The first `version = "..."` line of Cargo.toml is the [package] one; in Cargo.lock it is the
// line after the crate's own `name = "sudo-iroh-node"`.
function stamp(name, ...patterns) {
  const target = path.join(root, name);
  let text = fs.readFileSync(target, "utf8");
  for (const pattern of patterns) {
    if (!pattern.test(text)) {
      throw new Error(`${name}: nothing matches ${pattern}`);
    }
    text = text.replace(pattern, `$1${pkg.version}$2`);
  }
  fs.writeFileSync(target, text);
}
stamp("Cargo.toml", /^(version = ")[^"]+(")/m);
stamp("Cargo.lock", /^(name = "sudo-iroh-node"\nversion = ")[^"]+(")/m);
// The same text `napi build` writes; a rebuild regenerates it from package.json.
stamp("index.js", /(bindingPackageVersion !== ')[^']+(')/g, /(version mismatch, expected )\S+( but got)/g);
