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
The child assembles the step's runtime across two seams. The app's
`src/step-tool-materialization.ts` materializes the pinned tool-package
closure. `@intx/workflow-host` composes the POSIX and LSP plugin chain
(`packages/workflow-host/src/child/step-tools.ts`) and builds the
per-step environment
(`packages/workflow-host/src/child/substrate-factory.ts`) — the
isomorphic-git context store and the supervisor-backed mail transport
the tools bind to. The binary closes that factory over the materializer
and `@intx/workflow-deploy`'s grant cap in
`src/workflow-child-bindings.ts`. `src/default-harness.ts` provides only
the `HarnessBuilder` source-admission check (`canBuildSource`) the
deploy router uses to reject an unbuildable inference source before
spawning.

The sidecar imports the repository substrate and workflow inspection entry
points without loading Hub database services or the agent execution runtime.
See the [workflow-host documentation](../../packages/workflow-host/README.md)
for channel validation and schema initialization.
`src/process-boot-graph.test.ts` guards these startup dependencies alongside
the workflow-child and probe-child import graphs.

Run it under Bun. Startup requires `SIDECAR_DATA_DIR`, `HUB_WS_URL`,
`SIDECAR_ID`, and `SIDECAR_TOKEN`. Optional `SIDECAR_CACHE_DIR`
relocates the tarball cache (defaults to a subdirectory of the data
dir), and the cache and registry size caps are read through
`src/config.ts`.

`SIDECAR_MAX_ACTIVE_DEPLOYMENTS` sets the active deployment limit (default 128,
positive integer). `SIDECAR_MAX_RETAINED_DEPLOYMENTS` sets the retained-record
limit (default 256, non-negative integer); zero triggers
[retention refusal and cleanup](../../docs/SIDECAR_PLACEMENT.md#active-and-retained-capacity). Their sum
must not exceed the protocol ceiling of 2,048 records. These settings are read
at startup and advertised to the Hub on every connection, so placement uses
the sidecar's configured active limit. The complete `hello` frame must also fit
the 64 MiB WebSocket frame-size limit. Errors in hello use the same
4,096-character limit as individual stop reports.

Restart is required to change capacity. If the stored active or retained
inventory exceeds the new limit, startup fails without deleting records; clean
up deployments before reducing capacity below their counts.

When raising active capacity, size each workflow's `maxDisconnected` for the
longer [restore time](../../docs/HARNESS_DESIGN.md#self-restoration).

`SIDECAR_DATA_DIR` must be durably established on a filesystem that supports
file and directory synchronization (`fsync`). Deployment creation, state
markers, and removal require both; filesystems that reject directory
synchronization are unsupported. A failed state-marker write or durable
removal keeps capacity reserved. If undeploy cannot persist its teardown marker,
it still stops the child but preserves local state until a retry can persist
the marker and delete the state. A directory-sync failure during the startup
inventory aborts boot, because the sidecar cannot safely
reuse capacity based on an unconfirmed record write or deletion. Invalid
record contents are skipped individually.
