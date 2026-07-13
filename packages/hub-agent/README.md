# @intx/hub-agent

Sidecar-side orchestrator. Wires the package-side pieces of the
sidecar runtime — the per-agent key and repo stores, a session
manager, and the hub WebSocket link — into a single start/close
handle, applies deploy and asset packs received from the hub, and
forwards a spawned child's verified inference events back to the hub.

In-process harness construction and agent provisioning are retired:
every agent now runs as a supervised workflow-process child on the
workflow-run substrate. What remains in `createSessionManager` is a
thin serialization layer over the agent repo store (deploy/asset-pack
applies, state-pack reads, deploy-ref reads, directory teardown);
operations run one at a time per agent so a teardown never races an
in-flight git op.

Consumed by `apps/sidecar` as the orchestrator that turns a sidecar
process into a host for hub-deployed agents.

`createSidecarOrchestrator` takes a `SidecarOrchestratorConfig`
specifying how to reach the hub (`hubURL`, `sidecarId`, `token`,
`transport`), where to persist agent state (`dataDir`), the sidecar's
crypto operations (`cryptoOps`), and the host-injected deploy-router
factory (`createDeployRouter`) that routes every `agent.deploy` frame
on the link. Optional fields supply the multi-step inbound routers
(`mailInboundRouter`, `signalInboundRouter`, `drainInboundRouter`,
`sourcesInboundRouter`), the workflow-address announce and routability
hooks, and the reconnect cadence. See `SidecarOrchestratorConfig` in
`src/sidecar-orchestrator.ts` for the full surface.

`HarnessBuilder` is a one-method source-admission seam
(`canBuildSource`) the host supplies: the deploy router calls it to
admit a step's pinned inference source before spawning, so an
unbuildable source is rejected on the control plane rather than during
the next inference call. It is not a harness-wiring seam — the package
declares the shape and stays free of the concrete inference packages
the check consults.
