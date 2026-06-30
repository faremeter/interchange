// Dependency-light entry for an agent's on-disk layout helpers.
//
// `@intx/hub-agent` is the sidecar orchestrator. Importing its barrel
// (`index.ts`) evaluates the orchestrator's own module graph -- the session
// manager, the hub-link WebSocket layer, the sidecar orchestrator -- plus
// `@intx/pack-transport`. A few consumers need only the small filesystem
// helpers for an agent's on-disk deploy state -- reading the deploy tree
// under an agent's directory and deriving that directory's name from the
// agent address. For the spawned workflow-child loading the orchestrator to
// get them is both wasted module-evaluation and a backwards dependency on the
// very component that launches it. Neither helper's module imports the
// orchestrator graph, so this entry exposes them without it.

export { readDeployTree, type DeployTree } from "./deploy-tree";
export { sanitizeAddress } from "./agent-paths";
// Type-only re-export: `export type` is erased under `verbatimModuleSyntax`,
// so it adds no runtime import edge to `harness-builder`.
export type { DeployApplyErrorEmitter } from "./harness-builder";
