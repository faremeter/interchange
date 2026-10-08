import { type } from "arktype";

/**
 * Tag identifying a kind of repository in the hub's kind-keyed RepoStore.
 * Lives in `@intx/types` because the wire-level pack frames reference it;
 * the substrate package re-exports it for handler authors.
 */
export const RepoKind = type.enumerated(
  "agent-state",
  "skill",
  "package-registry",
  "workflow",
  "workflow-run",
);
export type RepoKind = typeof RepoKind.infer;

/**
 * Operations a principal may invoke against a repo in the RepoStore.
 * Lives in `@intx/types` so storage layers (e.g. `@intx/db`) can validate
 * persisted action vocabularies without depending on the substrate
 * package. The substrate re-exports it for handler authors.
 */
export const RepoAction = type.enumerated(
  "init",
  "writeTree",
  "receivePack",
  "createPack",
  "resolveRef",
);
export type RepoAction = typeof RepoAction.infer;

/**
 * Hub-side identity of a repository in the RepoStore. Pack frames carry
 * this alongside `agentAddress` so the hub can map a pack back to the
 * originating repo independently of which sidecar/agent it is destined for.
 */
export const RepoId = type({
  kind: RepoKind,
  id: "string",
});
export type RepoId = typeof RepoId.infer;
