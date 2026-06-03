# @intx/hub-sessions

Session-orchestration substrate for the hub. Owns the sidecar
WebSocket router, the session service that provisions and tears
down agent sessions, the agent repository store, the event
collector registry, the asset service, and the skill kind handler.

Sits between `@intx/hub-api` (HTTP surface) and `@intx/hub-agent`
(sidecar orchestrator): HTTP routes call into the session service
to start an agent, the session service drives the sidecar router
to provision it on the connected sidecar, and event collectors
feed agent events back to the HTTP layer for observability.

`createSessionService` takes a `SessionServiceDeps` of
`sidecarRouter`, `agentRepoStore`, and an optional
`assetService` paired with a `db` handle for the asset manifest
inserts. `createHubSessionOrchestrator` takes a
`HubSessionOrchestratorDeps` of `events`, `router`, `db`,
`eventCollectors`, `grantStore`, and `agentRepoStore`. See the
exported types in `src/session-service.ts` and
`src/hub-session-orchestrator.ts` for the authoritative shapes;
`@intx/hub-api` is the in-tree consumer that wires these
factories together.

The package does not host HTTP routes itself; it exposes the
factories `@intx/hub-api` composes into the application.
