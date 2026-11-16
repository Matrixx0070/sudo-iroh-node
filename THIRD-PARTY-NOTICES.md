# Third-party notices

## OpenClaw (`crates/openclaw-iroh-node`)

Parts of this package are derived from the `crates/openclaw-iroh-node` crate of the OpenClaw project
(https://github.com/openclaw/openclaw, commit 9b7dcdc7): the overall binding design, parts of
`src/endpoint.rs`, `src/conn.rs` and `src/stream.rs`, and the test harness under `test/`. That code is
used under the MIT License:

```
MIT License

Copyright (c) 2026 OpenClaw Foundation

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

## Rust dependencies

The prebuilt binaries statically link [iroh](https://github.com/n0-computer/iroh) (MIT OR Apache-2.0),
[napi-rs](https://github.com/napi-rs/napi-rs) (MIT) and their transitive dependencies. Run
`cargo tree --edges normal --format '{p} {l}'` in this repository for the full list with licenses.
