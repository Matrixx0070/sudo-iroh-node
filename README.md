# sudo-iroh-node

Node.js binding (napi-rs v3) for [iroh](https://github.com/n0-computer/iroh) 1.4.1 endpoints and streams
with Buffer-based I/O. Published as `@sudo-ai/iroh-node`.

Work in progress: see `test/` for the contract and `index.d.ts` for the API.

```bash
npm install
npm run build   # napi build --platform --release --strip; writes iroh-node.<platform>.node
npm test
```
