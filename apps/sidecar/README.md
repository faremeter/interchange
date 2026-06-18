# @intx/sidecar-app

The canonical host process for hub-orchestrated agents. Turns a
single Bun process into a fleet of agent runtimes driven by the
hub.

`src/index.ts` resolves the tarball cache configuration, sweeps any
orphaned staging directories left by a crashed apply, then starts a
`@intx/hub-agent` sidecar orchestrator. The orchestrator is wired
with an in-memory mail transport (`@intx/mail-memory`), the Node
crypto provider (`@intx/crypto-node`), and the default server
harness builder from `src/default-harness.ts`, which composes the
POSIX, LSP, and mail tool sets with isomorphic-git context storage.

Run it under Bun. Startup requires `SIDECAR_DATA_DIR`, `HUB_WS_URL`,
`SIDECAR_ID`, and `SIDECAR_TOKEN`. Optional `SIDECAR_CACHE_DIR`
relocates the tarball cache (defaults to a subdirectory of the data
dir), and the cache and registry size caps are read through
`src/config.ts`.
