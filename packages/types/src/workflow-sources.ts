// Where a code-sourced workflow definition's bytes come from at apply time:
// a `registry` variant names an EXTERNAL npm registry that publishes the
// definition package; an `asset` variant names a hub asset -- a checked-out
// git repo -- holding the definition as a published `tarball` (selected by
// the install pin) or as a `source` codebase at a pinned commit.

import { type } from "arktype";

/**
 * A workflow definition published to an external npm registry, fetched at
 * apply time; the install call's version pin selects the definition.
 */
export const WorkflowDefinitionRegistrySource = type({
  kind: "'registry'",
  registry: "string",
});
export type WorkflowDefinitionRegistrySource =
  typeof WorkflowDefinitionRegistrySource.infer;

/**
 * The definition is a published tarball inside the hub asset. Names only
 * the format; the install pin selects which package, as with `registry`.
 */
export const WorkflowDefinitionAssetTarball = type({
  format: "'tarball'",
});
export type WorkflowDefinitionAssetTarball =
  typeof WorkflowDefinitionAssetTarball.infer;

/**
 * The definition is the codebase in the hub asset's git checkout at a pinned
 * commit. `commitSha` IS the pin; the tree's content hash is the definition's
 * identity (the member `package.json` version is only an advisory label).
 * `packageName` selects a monorepo member by its `package.json` name; absent
 * means a single package rooted at the tree.
 */
export const WorkflowDefinitionAssetSourceTree = type({
  format: "'source'",
  commitSha: "string",
  "packageName?": "string",
});
export type WorkflowDefinitionAssetSourceTree =
  typeof WorkflowDefinitionAssetSourceTree.infer;

/**
 * A workflow definition sourced from a hub `asset` -- a checked-out git repo
 * -- as a published `tarball` or a `source` codebase, by `package.format`.
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
 * (`SourceRefPin`, the probe/deploy wire frames) follows.
 */
export const WorkflowDefinitionSource = WorkflowDefinitionRegistrySource.or(
  WorkflowDefinitionAssetSource,
);
export type WorkflowDefinitionSource = typeof WorkflowDefinitionSource.infer;
