// Deploy-side application of a code-sourced workflow's frozen closure.
//
// When a deploy frame carries a `source` (the npm registry the workflow
// definition package is published to) plus the hub's frozen dependency
// `closure` (concrete versions + integrity SRIs), the sidecar materializes
// EXACTLY that closure and evaluates the pinned code to a validated
// `WorkflowDefinition` -- rather than trusting an inline serialized
// projection. The closure is applied byte-for-byte as the hub froze it; the
// sidecar never re-resolves the pin against the registry at apply time.
//
// This is the DURABLE deploy counterpart to the airlocked install-time probe:
// it reuses the same `@intx/tool-packaging` apply machinery
// (`createTarballCache` / `createToolLoader` / `applyAtomic`) that
// `tool-materialization.ts` uses for a step's tool closure, so the fetch +
// SRI-verify + extract + `node_modules` layout is not reimplemented here.
//
// A workflow-definition package declares `interchange.workflow` (the module
// whose evaluation produces the definition), NOT `interchange.tools`.
// `applyAtomic`'s load phase imports each TOP-LEVEL package's
// `interchange.tools` entry and rejects a package that has none
// (`package.entry.missing`), so the layout manifest handed to `applyAtomic`
// carries an EMPTY `topLevel`: every entry is still materialized and laid out
// (the dependency layout walks `entries`, and each dependency resolves against
// the frozen closure), but no tool factory is imported. The workflow entry
// itself is imported by `loadWorkflowDefinitionFromClosure` -- the correct load
// site for a workflow definition -- against the materialized package directory.

import path from "node:path";

import { getLogger } from "@intx/log";
import {
  type RegistryConfig,
  type TarballFetcher,
  applyAtomic,
  createTarballCache,
  createToolLoader,
  storeEntryDir,
} from "@intx/tool-packaging";
import type { ToolPackageManifest } from "@intx/types/tool-packages";
import { getToolPackageSourceContentIdentity } from "@intx/types/tool-packages";
import type { WorkflowDefinitionSource } from "@intx/types/workflow-sources";
import { loadWorkflowDefinitionFromClosure } from "@intx/workflow-host";
import type { WorkflowDefinition } from "@intx/workflow/definition";

const logger = getLogger(["sidecar", "workflow-closure-apply"]);

export interface ApplyFrozenWorkflowClosureArgs {
  /** Names the registry the workflow definition package is published to. */
  readonly source: WorkflowDefinitionSource;
  /**
   * The hub's frozen dependency closure for the definition's pin: concrete
   * versions and integrity SRIs. Applied byte-for-byte; never re-resolved.
   */
  readonly closure: ToolPackageManifest;
  /**
   * Durable per-deployment directory the closure is staged under
   * (`<instanceDir>/packages/<deploy-id>/store/...`).
   */
  readonly instanceDir: string;
  /** Content-addressable tarball cache root shared across applies. */
  readonly cacheRoot: string;
  /** Byte cap for the tarball cache. */
  readonly cacheMaxBytes: number;
  /** Byte cap for a single HTTP-registry tarball fetch. */
  readonly registryMaxTarballBytes: number;
  /** Registry identifier -> URL + credentials the loader resolves entries against. */
  readonly registries: ReadonlyMap<string, RegistryConfig>;
  /**
   * Workspace root `kind: "asset"` closure entries mount against. A
   * registry-sourced workflow definition closure carries no asset entries, so
   * this defaults to `<instanceDir>/workspace`.
   */
  readonly assetRoot?: string;
  /** `assetId` -> mount path for tarball `asset` entries; empty by default. */
  readonly assetMounts?: ReadonlyMap<string, string>;
  /**
   * `assetId` -> absolute indexed git directory for source-format `asset`
   * entries. A registry- or tarball-sourced closure carries no source
   * entries, so this defaults to an empty map.
   */
  readonly gitDirs?: ReadonlyMap<string, string>;
  /**
   * Test seam for tarball fetching, forwarded to `createToolLoader`.
   * Production omits it and the loader fetches from the configured registry.
   */
  readonly fetchTarball?: TarballFetcher;
  /**
   * Test seam for the workflow entry's dynamic import, forwarded to
   * `loadWorkflowDefinitionFromClosure`. Production omits it and the loader
   * imports the materialized entry natively.
   */
  readonly importModule?: (importUrl: string) => Promise<unknown>;
}

export interface AppliedWorkflowClosure {
  /** The validated definition the pinned code evaluated to. */
  readonly definition: WorkflowDefinition;
  /** Directory of the materialized workflow package within the closure. */
  readonly packageDir: string;
  /** The staged, never-renamed deploy directory the closure was laid out under. */
  readonly deployDir: string;
}

/**
 * Materialize a workflow definition's frozen closure durably and load the
 * pinned code to a validated `WorkflowDefinition`.
 *
 * The closure's single top-level pin IS the workflow definition package: the
 * hub resolved the closure for exactly that pin. The frozen `entries` are
 * applied verbatim (concrete versions + SRIs), so no registry re-resolution
 * happens at apply time.
 *
 * @throws if the closure does not carry exactly one top-level pin, the source
 *   registry is not configured on this sidecar, the apply fails (integrity
 *   mismatch, fetch failure, extract failure, ...), or the pinned code does not
 *   evaluate to exactly one valid `WorkflowDefinition`.
 */
export async function applyFrozenWorkflowClosure(
  args: ApplyFrozenWorkflowClosureArgs,
): Promise<AppliedWorkflowClosure> {
  if (args.closure.topLevel.length !== 1) {
    throw new Error(
      `sidecar workflow-closure apply: the frozen closure must carry exactly one top-level pin (the workflow definition package), got ${String(args.closure.topLevel.length)}`,
    );
  }
  const workflowPin = args.closure.topLevel[0];
  if (workflowPin === undefined) {
    throw new Error(
      "sidecar workflow-closure apply: the frozen closure's single top-level pin is undefined",
    );
  }

  // Boundary check on the definition's source. The `registry` arm surfaces a
  // missing source registry loudly before any I/O (the per-entry registry
  // gates fire again inside the loader). A tarball-format `asset` closure
  // materializes its entries from `assetMounts` the caller populated from the
  // durable source-asset store; the loader reads each entry's tarball there and
  // fails loud (`asset.mount.missing`) if a mount is absent. A source-format
  // asset closure has no sidecar delivery path yet, so that arm fails closed.
  // The `never` default makes a future source kind a compile error rather than
  // a silent fallthrough.
  switch (args.source.kind) {
    case "registry":
      if (!args.registries.has(args.source.registry)) {
        throw new Error(
          `sidecar workflow-closure apply: source registry ${JSON.stringify(args.source.registry)} is not in the sidecar registry config`,
        );
      }
      break;
    case "asset":
      if (args.source.package.format === "source") {
        throw new Error(
          "sidecar workflow-closure apply: source-format asset workflow closures cannot be materialized on the sidecar yet",
        );
      }
      break;
    default: {
      const _exhaustive: never = args.source;
      throw new Error(
        `sidecar workflow-closure apply: unhandled workflow source kind ${String(_exhaustive)}`,
      );
    }
  }

  const cache = createTarballCache({
    rootDir: args.cacheRoot,
    maxBytes: args.cacheMaxBytes,
  });
  const loader = createToolLoader({
    cache,
    registries: args.registries,
    host: { os: process.platform, cpu: process.arch },
    maxRegistryTarballBytes: args.registryMaxTarballBytes,
    ...(args.fetchTarball !== undefined
      ? { fetchTarball: args.fetchTarball }
      : {}),
  });

  // Apply EXACTLY the frozen entries. `topLevel` is emptied so `applyAtomic`
  // imports no `interchange.tools` module (a workflow-definition package has
  // none); the full `entries` set is still materialized and laid out.
  const layoutManifest: ToolPackageManifest = {
    schemaVersion: args.closure.schemaVersion,
    topLevel: [],
    entries: args.closure.entries,
  };

  const result = await applyAtomic({
    manifest: layoutManifest,
    loader,
    instanceDir: args.instanceDir,
    assetRoot: args.assetRoot ?? path.join(args.instanceDir, "workspace"),
    assetMounts: args.assetMounts ?? new Map(),
    gitDirs: args.gitDirs ?? new Map(),
    attemptId: crypto.randomUUID(),
    // This apply stands alone per deployment: there is no prior deploy under
    // `instanceDir` to retain, so the sentinel disables the retention window.
    previousDeployId: "none",
    newDeployId: crypto.randomUUID(),
  });
  if (result.status === "failed") {
    throw new Error(
      `sidecar workflow-closure apply: materializing the frozen closure for ${workflowPin.name}@${workflowPin.version} failed (${result.category}): ${result.message}`,
    );
  }

  const packageDir = storeEntryDir(
    path.join(result.deployDir, "store"),
    workflowPin.name,
    workflowPin.version,
  );

  // The workflow package's own integrity is the natural ESM-cache-bust token:
  // Node keys its module cache by resolved URL, so a re-apply of changed bytes
  // under the same name@version reimports rather than resolving to the prior
  // instance.
  const workflowEntry = args.closure.entries.find(
    (entry) =>
      entry.name === workflowPin.name && entry.version === workflowPin.version,
  );

  const definition = await loadWorkflowDefinitionFromClosure({
    packageDir,
    ...(workflowEntry !== undefined
      ? {
          importCacheKey: getToolPackageSourceContentIdentity(
            workflowEntry.source,
          ),
        }
      : {}),
    ...(args.importModule !== undefined
      ? { importModule: args.importModule }
      : {}),
  });

  logger.debug`applied frozen workflow closure ${workflowPin.name}@${workflowPin.version}: loaded definition ${definition.id}`;
  return { definition, packageDir, deployDir: result.deployDir };
}
