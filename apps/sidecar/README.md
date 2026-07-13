# @intx/sidecar-app

The canonical host process for hub-orchestrated agents. Turns a
single Bun process into a fleet of agent runtimes driven by the
hub.

`src/index.ts` resolves the tarball cache configuration, sweeps any
orphaned staging directories left by a crashed apply, then starts a
`@intx/hub-agent` sidecar orchestrator. The orchestrator is wired
with an in-memory mail transport (`@intx/mail-memory`), the Web
Crypto provider (`@intx/crypto`), and a deploy router; on each
inbound deploy the router creates an `@intx/workflow-host` supervisor
for the deployment and spawns a supervised workflow-process child.
The child assembles the step's runtime across two seams:
`src/step-agent-tools.ts` materializes the pinned tool-package
closure and composes the POSIX and LSP plugin chain, and
`src/workflow-substrate-factory.ts` builds the per-step environment —
the isomorphic-git context store and the supervisor-backed mail
transport the tools bind to. `src/default-harness.ts` provides only
the `HarnessBuilder` source-admission check (`canBuildSource`) the
deploy router uses to reject an unbuildable inference source before
spawning.

Run it under Bun. Startup requires `SIDECAR_DATA_DIR`, `HUB_WS_URL`,
`SIDECAR_ID`, and `SIDECAR_TOKEN`. Optional `SIDECAR_CACHE_DIR`
relocates the tarball cache (defaults to a subdirectory of the data
dir), and the cache and registry size caps are read through
`src/config.ts`.
