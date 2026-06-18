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

## Surface

The single `@intx/types`-style entry point (`@intx/hub-sessions`)
re-exports the following, grouped by concern. See the source modules
for the authoritative type shapes.

### Session orchestration

- `createSessionService` / `SessionService`, `SessionLaunchError` —
  provisions and tears down agent sessions on the connected sidecar.
- `createHubSessionOrchestrator` / `HubSessionOrchestrator`,
  `HubSessionOrchestratorDeps`, `HubSessionRouterFacade` — the
  higher-level orchestrator that wires the router, event collectors,
  and grant store together.
- `createHubSessionLookups` / `HubSessionLookupsDeps` — builds the
  lookup callbacks the sidecar router needs to resolve sessions.

### Sidecar WebSocket router and events

- `createSidecarRouter` / `SidecarRouter`, `SidecarRouterConfig`,
  `WsHandle` — the hub-side WebSocket router for connected sidecars.
- `createSidecarEmitter` / `SidecarEventEmitter`, `SidecarEventMap`,
  `SidecarEventType`, `SidecarEventListener` — typed event emission
  over the router.
- `SidecarLookups`, `SidecarMailPersistedPayload`,
  `SidecarMailPersistedRow` — router lookup and mail-persistence
  payload shapes.

### Event collection

- `createEventCollectorRegistry` / `EventCollectorRegistry` —
  registry that feeds agent events back to the HTTP layer for
  observability.

### Agent repository store

- `createAgentRepoStore` / `AgentRepoStore`, `DeployContent` — the
  agent-specific repository store.
- `createRepoStore`, `UserPrincipal`, and the supporting types
  `AuthorizeFn`, `CreateRepoStoreConfig`, `InitRepoOpts`,
  `KindHandler`, `Principal`, `RefEntry`, `RepoAction`, `RepoId`,
  `RepoStore` — the generic repo-store substrate the kind handlers
  plug into.

### Repo kind handlers

- Skills: `skillKindHandler`, `skillAuthorize`,
  `skillFrontmatterSchema`, `getSkillIndex`, and the types
  `SkillIndexEntry`, `SkillFrontmatter`, `SkillPrincipal`,
  `SkillHubPrincipal`, `SkillSidecarPrincipal`.
- Package registry: `packageRegistryKindHandler`,
  `packageRegistryAuthorize`, `asTarballEntry`,
  `validateTarballPackageJSON`, and the constants `TARBALLS_PREFIX`,
  `TARBALL_FILENAME_PATTERN`, `REGISTRY_INDEX_PATH`,
  `WORKSPACE_BUILTINS_REGISTRY`.

### Asset service

- `createAssetService` / `AssetService`, `AssetServiceError`,
  `DEFAULT_ASSET_REF`, and the supporting types `Asset`, `AgentAsset`,
  `AgentAssetWithAsset`, `AccessMode`, `CreateAssetParams`,
  `PopulateAssetParams`, `AttachAssetParams`,
  `AssetServiceErrorReason`, `ReadAssetBlobParams`,
  `ListAssetBlobsParams`.

### Skills stanza and credential push

- `buildAvailableSkillsStanza` / `AvailableSkillEntry` — renders the
  available-skills stanza injected into agent context.
- `pushSourceUpdates` — pushes credential-source updates to the
  sidecar.
