// Hub-side dependency-closure resolution for a code-sourced workflow
// definition.
//
// Given a `WorkflowDefinitionRegistrySource` (which names an npm registry)
// and a pin, walk the full dependency closure to concrete versions plus
// integrity SRIs, reusing the tool-packaging closure resolver. This is the
// "what exactly would we install" step: packument and package.json metadata
// reads only, no code execution. The returned `ToolPackageManifest` is the
// frozen closure the deploy path later ships by source-ref.
//
// The source only carries the registry *name*; the URL and credentials for
// that name are supplied by the caller as a `RegistryConfig`, since the
// caller is the boundary that owns the registry configuration.

import {
  type PackumentFetcher,
  type RegistryConfig,
  HttpRegistrySource,
  createClosureResolver,
  parsePin,
} from "@intx/tool-packaging";
import type { ToolPackageManifest } from "@intx/types/tool-packages";
import type { WorkflowDefinitionRegistrySource } from "@intx/types/workflow-sources";

export type ResolveWorkflowClosureArgs = {
  /** Names the npm registry that publishes the workflow definition. */
  readonly source: WorkflowDefinitionRegistrySource;
  /** A `name@range` spec for the workflow definition package. */
  readonly pin: string;
  /** URL and credentials for the registry named by `source.registry`. */
  readonly registryConfig: RegistryConfig;
  /**
   * Test seam for packument fetches. Omitted in production, where the
   * default `npm-registry-fetch` wrapper reaches the configured URL.
   */
  readonly fetchPackument?: PackumentFetcher;
};

/**
 * Resolve a workflow definition's dependency closure to concrete versions
 * and integrity SRIs, reusing the tool-packaging closure resolver against
 * the single registry the source names.
 *
 * @returns the frozen closure: a `ToolPackageManifest` whose `topLevel`
 *   pins the definition package and whose `entries` carry the concrete
 *   version and integrity SRI of every transitive dependency.
 */
export async function resolveWorkflowClosure(
  args: ResolveWorkflowClosureArgs,
): Promise<ToolPackageManifest> {
  const registrySource = new HttpRegistrySource({
    name: args.source.registry,
    config: args.registryConfig,
    ...(args.fetchPackument !== undefined
      ? { fetchPackument: args.fetchPackument }
      : {}),
  });
  const resolver = createClosureResolver({
    registries: new Map([[registrySource.name, registrySource]]),
    defaultRegistry: registrySource.name,
  });
  return resolver.resolveClosure([parsePin(args.pin)]);
}
