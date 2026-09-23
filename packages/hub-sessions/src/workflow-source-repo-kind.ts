import type { WorkflowDefinitionAssetSource } from "@intx/types/workflow-sources";

import type { RepoKind } from "./repo-store";

/**
 * The repo kind that holds each format of an asset-sourced workflow
 * definition. A `source` codebase lives in a `workflow` asset; a packed
 * `tarball` lives in a `package-registry` asset, the same kind that publishes
 * tool packages. The table is keyed by the format union, so a new format does
 * not compile until it names its kind, and the route that admits a deploy and
 * the resolvers that pack the asset read the same answer.
 */
const REPO_KIND_BY_FORMAT: Record<
  WorkflowDefinitionAssetSource["package"]["format"],
  RepoKind
> = {
  source: "workflow",
  tarball: "package-registry",
};

export function workflowSourceRepoKind(
  source: WorkflowDefinitionAssetSource,
): RepoKind {
  return REPO_KIND_BY_FORMAT[source.package.format];
}
