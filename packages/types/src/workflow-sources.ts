// Schemas for where a code-sourced workflow definition's bytes come from.
//
// A workflow install carries a `WorkflowDefinitionSource` to say where the
// definition should be fetched from at apply time. Two origins exist: the
// `registry` variant names an EXTERNAL npm registry that publishes the
// definition package; the `asset` variant names a hub asset -- a checked-out
// git repo -- that holds the definition either as a published `tarball`
// (selected by the install pin) or as a `source` codebase at a pinned commit.

import { type } from "arktype";

/**
 * A workflow definition published to an external npm registry, fetched at
 * apply time. The sidecar's registry config maps `registry` to a URL and
 * credentials. The install call's version pin selects the definition.
 */
export const WorkflowDefinitionRegistrySource = type({
  kind: "'registry'",
  registry: "string",
});
export type WorkflowDefinitionRegistrySource =
  typeof WorkflowDefinitionRegistrySource.infer;

/**
 * The definition is a published tarball inside the hub asset. This names
 * only the format: the install call's version pin selects which package
 * inside the asset is the definition, exactly as the `registry` variant
 * leaves the pin to travel separately.
 */
export const WorkflowDefinitionAssetTarball = type({
  format: "'tarball'",
});
export type WorkflowDefinitionAssetTarball =
  typeof WorkflowDefinitionAssetTarball.infer;

/**
 * The definition is the codebase in the hub asset's git checkout at a pinned
 * commit. `commitSha` IS the pin, and the content hash of the tree at that
 * commit is the definition's identity (the member `package.json` version is
 * only an advisory label). `packageName` selects which workspace member of a
 * monorepo codebase is the definition, by its `package.json` name; absent
 * means the codebase is a single package rooted at the tree.
 */
export const WorkflowDefinitionAssetSourceTree = type({
  format: "'source'",
  commitSha: "string",
  "packageName?": "string",
});
export type WorkflowDefinitionAssetSourceTree =
  typeof WorkflowDefinitionAssetSourceTree.infer;

/**
 * A workflow definition sourced from a hub `asset` -- a checked-out git repo.
 * The definition lives inside it either as a published `tarball` or as a
 * `source` codebase, discriminated by `package.format`.
 */
export const WorkflowDefinitionAssetSource = type({
  kind: "'asset'",
  assetId: "string",
  package: WorkflowDefinitionAssetTarball.or(WorkflowDefinitionAssetSourceTree),
});
export type WorkflowDefinitionAssetSource =
  typeof WorkflowDefinitionAssetSource.infer;

/**
 * Discriminated union over where a workflow definition's bytes come from,
 * keyed on `kind`. Widen it here and every by-value consumer
 * (`SourceRefPin`, the probe/deploy wire frames) follows; a consumer that
 * switches on `kind` gains a compile error for any unhandled variant.
 */
export const WorkflowDefinitionSource = WorkflowDefinitionRegistrySource.or(
  WorkflowDefinitionAssetSource,
);
export type WorkflowDefinitionSource = typeof WorkflowDefinitionSource.infer;
