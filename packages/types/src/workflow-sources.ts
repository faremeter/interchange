// Schemas for where a code-sourced workflow definition's bytes come from.
//
// A workflow install carries a `WorkflowDefinitionSource` to say where the
// definition should be fetched from at apply time. The registry variant
// names an npm registry that publishes the definition package; the sidecar's
// registry config maps `registry` to a URL and credentials.

import { type } from "arktype";

/**
 * A workflow definition published to the named npm registry, fetched at
 * apply time. The sidecar's registry config maps `registry` to a URL and
 * credentials.
 */
export const WorkflowDefinitionRegistrySource = type({
  kind: "'registry'",
  registry: "string",
});
export type WorkflowDefinitionRegistrySource =
  typeof WorkflowDefinitionRegistrySource.infer;

/**
 * Discriminated union over where a workflow definition's bytes come from.
 *
 * Only the registry variant exists today. The union is kept in union form so
 * asset- and git-sourced variants can be added later by widening it with
 * `.or(...)` without a breaking change to the exported type's shape.
 */
export const WorkflowDefinitionSource = WorkflowDefinitionRegistrySource;
export type WorkflowDefinitionSource = typeof WorkflowDefinitionSource.infer;
