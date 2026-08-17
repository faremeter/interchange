// Hub-side dependency-closure resolution for a code-sourced workflow
// definition.
//
// Given a `WorkflowDefinitionSource` and a pin, walk the full dependency
// closure to concrete versions plus integrity SRIs, reusing the tool-packaging
// closure resolver. This is the "what exactly would we install" step: packument
// and package.json metadata reads only, no code execution. The returned
// `ToolPackageManifest` is the frozen closure the deploy path later ships by
// source-ref.
//
// There are three arms, one per `(source.kind, package.format)`:
//   - `registry` names an npm registry; its URL and credentials are supplied by
//     the caller as a `RegistryConfig`, since the caller owns the registry
//     configuration.
//   - `asset` + `tarball` names a hub asset that publishes the definition
//     package as a tarball; the caller supplies bound `readBlob`/`listBlobs`
//     closures over that asset (the same shape `session-service` mints for
//     `AssetRegistrySource`).
//   - `asset` + `source` names a hub asset whose definition package lives as a
//     git subtree at a pinned commit; the caller supplies a `SourceTreeReads`
//     pinned to that commit plus a `RegistryConfig` for the external npm deps.
//     This arm delegates to `resolveSourceWorkflowClosure`, which reads the
//     tree directly and needs no `name@range` pin (it selects the member from
//     the source's `packageName`).
// Every arm takes pre-bound read capabilities, keeping this module free of any
// asset-service or repo-store dependency.

import { base64Encode } from "@intx/types";
import {
  type PackumentFetcher,
  type RegistryConfig,
  AssetRegistrySource,
  HttpRegistrySource,
  createClosureResolver,
  parsePin,
} from "@intx/tool-packaging";
import type { WorkflowSourceAssetMount } from "@intx/types/sidecar";
import type { ToolPackageManifest } from "@intx/types/tool-packages";
import type {
  WorkflowDefinitionAssetSource,
  WorkflowDefinitionRegistrySource,
} from "@intx/types/workflow-sources";

import {
  resolveSourceWorkflowClosure,
  type SourceTreeReads,
} from "./workflow-source-closure";

/**
 * Resolve a workflow definition's dependency closure against the npm registry
 * the source names.
 */
export type ResolveWorkflowClosureRegistryArgs = {
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
 * Resolve a workflow definition's dependency closure against a single hub
 * `package-registry` asset.
 *
 * The resolver is single-source by design: the asset is the sole and default
 * registry, so a self-contained definition (its `@intx/*` imports bundled at
 * author time) resolves to one `kind:"asset"` entry. A specifier the asset does
 * not publish fails loud from `AssetRegistrySource` -- there is no silent
 * fallback to npm. A definition that genuinely needs external npm dependencies
 * would resolve through the mixed `AssetRegistrySource` + `HttpRegistrySource`
 * map `session-service` already builds for tool packages; that path is not
 * threaded here.
 */
export type ResolveWorkflowClosureAssetTarballArgs = {
  /** An asset source whose `package.format` is `"tarball"`. */
  readonly source: WorkflowDefinitionAssetSource;
  /** A `name@range` spec for the workflow definition package. */
  readonly pin: string;
  /** Reads a blob at `path` from the asset the definition is sourced from. */
  readonly readBlob: (path: string) => Promise<Uint8Array>;
  /** Lists the blob names directly under `dir` in that asset. */
  readonly listBlobs: (dir: string) => Promise<string[]>;
};

/**
 * Resolve a source-format workflow definition's dependency closure from the
 * asset's git tree at the pinned commit. The workspace-local members become
 * `format:"source"` entries and the external npm deps resolve through the
 * npm registry the caller supplies; there is no `name@range` pin (the member
 * is selected from `source.package.packageName`).
 */
export type ResolveWorkflowClosureAssetSourceArgs = {
  /** An asset source whose `package.format` is `"source"`. */
  readonly source: WorkflowDefinitionAssetSource;
  /** Git-tree reads pinned to `source.package.commitSha`. */
  readonly reads: SourceTreeReads;
  /**
   * The registry name external deps are stamped with in the frozen closure.
   * Must be a name the sidecar's registry map is keyed by (its npm registry),
   * since the sidecar resolves each external entry's `source.registry` against
   * that map at materialization.
   */
  readonly registryName: string;
  /** URL and credentials for the npm registry external deps resolve against. */
  readonly registryConfig: RegistryConfig;
  /**
   * Test seam for packument fetches. Omitted in production, where the
   * default `npm-registry-fetch` wrapper reaches the configured URL.
   */
  readonly fetchPackument?: PackumentFetcher;
};

export type ResolveWorkflowClosureArgs =
  | ResolveWorkflowClosureRegistryArgs
  | ResolveWorkflowClosureAssetTarballArgs
  | ResolveWorkflowClosureAssetSourceArgs;

// Both asset arms carry an identical `source` field type, so narrow on the
// source's own `package.format` discriminant rather than adding a redundant
// discriminant to the args.
function isAssetSourceArgs(
  args: ResolveWorkflowClosureArgs,
): args is ResolveWorkflowClosureAssetSourceArgs {
  return (
    args.source.kind === "asset" && args.source.package.format === "source"
  );
}

function isAssetTarballArgs(
  args: ResolveWorkflowClosureArgs,
): args is ResolveWorkflowClosureAssetTarballArgs {
  return (
    args.source.kind === "asset" && args.source.package.format === "tarball"
  );
}

/**
 * Resolve a workflow definition's dependency closure to concrete versions
 * and integrity SRIs, reusing the tool-packaging closure resolver.
 *
 * @returns the frozen closure: a `ToolPackageManifest` whose `topLevel`
 *   pins the definition package and whose `entries` carry the concrete
 *   version and integrity SRI of every transitive dependency.
 */
export async function resolveWorkflowClosure(
  args: ResolveWorkflowClosureArgs,
): Promise<ToolPackageManifest> {
  if (isAssetSourceArgs(args)) {
    return resolveSourceWorkflowClosure({
      source: args.source,
      reads: args.reads,
      registryName: args.registryName,
      registryConfig: args.registryConfig,
      ...(args.fetchPackument !== undefined
        ? { fetchPackument: args.fetchPackument }
        : {}),
    });
  }

  if (isAssetTarballArgs(args)) {
    // The assetId is the source name so every walker resolution error is
    // traceable to the exact asset; it is the sole and default source.
    const name = args.source.assetId;
    const assetSource = new AssetRegistrySource({
      name,
      assetId: args.source.assetId,
      readBlob: args.readBlob,
      listBlobs: args.listBlobs,
    });
    const resolver = createClosureResolver({
      registries: new Map([[name, assetSource]]),
      defaultRegistry: name,
    });
    return resolver.resolveClosure([parsePin(args.pin)]);
  }

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

/**
 * The workspace-relative mount path a workflow definition's source asset is
 * delivered under, keyed by asset id. Both the probe (inline delivery) and the
 * deploy (streamed fan-out) build their `assetId -> mountPath` maps through
 * this one helper, so the same asset lands at the same path on both paths.
 */
export function workflowSourceAssetMountPath(assetId: string): string {
  return `source-assets/${assetId}/`;
}

/**
 * Resolves an asset (by id) to the git pack a source-ref frame delivers for it.
 * The caller binds the ref: an asset source binds the asset's default ref, so
 * the install/deploy glue stays ref-agnostic.
 */
export type ResolveAssetAttachmentFn = (
  assetId: string,
) => Promise<{ pack: Uint8Array; ref: string; commitSha: string }>;

/**
 * Build the inline asset mounts a source-ref frame delivers for an asset-sourced
 * closure: one per distinct asset the closure's `kind:"asset"` entries name (so
 * a multi-asset closure delivers every backing asset, not just the source), each
 * carrying the git pack the caller resolves for that asset. `mountPath` is
 * derived through the shared helper so probe and deploy agree on it.
 */
export async function buildSourceAssetMounts(
  closure: ToolPackageManifest,
  resolveAttachment: ResolveAssetAttachmentFn,
): Promise<WorkflowSourceAssetMount[]> {
  const assetIds = new Set<string>();
  for (const entry of closure.entries) {
    if (entry.source.kind === "asset") {
      assetIds.add(entry.source.assetId);
    }
  }
  const mounts: WorkflowSourceAssetMount[] = [];
  for (const assetId of assetIds) {
    const attachment = await resolveAttachment(assetId);
    mounts.push({
      assetId,
      mountPath: workflowSourceAssetMountPath(assetId),
      pack: base64Encode(attachment.pack),
      ref: attachment.ref,
      commitSha: attachment.commitSha,
    });
  }
  return mounts;
}
