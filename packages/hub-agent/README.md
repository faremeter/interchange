# @intx/hub-agent

Sidecar-side orchestrator. Builds harnesses for each agent the
sidecar hosts, manages session lifecycle (provision, restore,
shutdown), reconnects to the hub WebSocket on disconnect, owns
the per-agent key store, and applies deploy and asset packs
received from the hub.

Consumed by `apps/sidecar` as the core orchestrator that turns a
sidecar process into a fleet of agent runtimes.

`createSidecarOrchestrator` takes a `SidecarOrchestratorConfig`
specifying how to reach the hub (`hubURL`, `sidecarId`, `token`,
`transport`), where to persist agent state (`dataDir`), how to
build per-agent harnesses (`buildHarness`), and how to mint and
operate the sidecar's crypto material (`createAgentCrypto`,
`cryptoOps`). Optional fields control reconnect cadence. See
`SidecarOrchestratorConfig` in `src/sidecar-orchestrator.ts` for
the full surface; `createSessionManager` and `createHubLink` are
the companion factories that produce the `buildHarness` and
`transport` arguments.

`HarnessBuilder` is a seam type the embedder supplies (the
`buildHarness` field on both `SidecarOrchestratorConfig` and
`SessionManagerConfig`); the session manager calls into it to wire
`@intx/harness` together with the per-agent context store, mail
transport, and runtime capabilities. The hub link handles the
WebSocket transport and reconnect schedule against
`@intx/hub-sessions` on the hub side.
