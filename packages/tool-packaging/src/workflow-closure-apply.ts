// Deploy-side application of a code-sourced workflow's frozen closure:
// applies exactly the hub's frozen `closure` (never re-resolving the
// pin at apply time) through the shared tool-packaging machinery, then
// evaluates the pinned definition package via the caller-supplied
// `loadDefinition`. The layout manifest carries an EMPTY `topLevel` —
// a workflow-definition package declares `interchange.workflow`, not
// `interchange.tools`, so no tool factory is imported.

import path from "node:path";

import { getLogger } from "@intx/log";
import {
  type ToolPackageManifest,
  getToolPackageSourceContentIdentity,
} from "@intx/types/tool-packages";
import type { WorkflowDefinitionSource } from "@intx/types/workflow-sources";

import { applyAtomic } from "./atomic-apply";
import { createTarballCache } from "./cache";
import {
  createToolLoader,
  type HostPlatform,
  type TarballFetcher,
} from "./loader";
import type { RegistryConfig } from "./resolver";
import { storeEntryDir } from "./store-layout";

const logger = getLogger(["sidecar", "workflow-closure-apply"]);

export interface ApplyFrozenWorkflowClosureArgs<
  TDefinition extends { readonly id: string },
> {
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
  /** npm `os`/`cpu` pair the loader selects optional dependencies with. */
  readonly host: HostPlatform;
  /**
   * Import the materialized package's workflow entry and return the
   * definition it evaluated to. The caller owns validation; this function
   * reads `definition.id`.
   */
  readonly loadDefinition: (args: {
    readonly packageDir: string;
    readonly importCacheKey?: string;
  }) => Promise<TDefinition>;
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
}

export interface AppliedWorkflowClosure<
  TDefinition extends { readonly id: string },
> {
  /** The definition the caller-supplied loader returned. */
  readonly definition: TDefinition;
  /** Directory of the materialized workflow package within the closure. */
  readonly packageDir: string;
  /** The staged, never-renamed deploy directory the closure was laid out under. */
  readonly deployDir: string;
}

/**
 * Materialize a workflow definition's frozen closure durably and load
 * the pinned code through `loadDefinition`.
 */
export async function applyFrozenWorkflowClosure<
  TDefinition extends { readonly id: string },
>(
  args: ApplyFrozenWorkflowClosureArgs<TDefinition>,
): Promise<AppliedWorkflowClosure<TDefinition>> {
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

  // Boundary check on the definition's source. The `registry` arm
  // surfaces a missing source registry loudly before any I/O (the
  // per-entry gates fire again inside the loader); an `asset` closure
  // materializes its entries from the durable stores the caller
  // populated (`assetMounts` / `gitDirs`), and the loader fails loud if
  // either is absent. The `never` default makes a future source kind a
  // compile error.
  switch (args.source.kind) {
    case "registry":
      if (!args.registries.has(args.source.registry)) {
        throw new Error(
          `sidecar workflow-closure apply: source registry ${JSON.stringify(args.source.registry)} is not in the sidecar registry config`,
        );
      }
      break;
    case "asset":
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
    host: args.host,
    maxRegistryTarballBytes: args.registryMaxTarballBytes,
    ...(args.fetchTarball !== undefined
      ? { fetchTarball: args.fetchTarball }
      : {}),
  });

  // Apply exactly the frozen entries. `topLevel` is emptied so
  // `applyAtomic` imports no `interchange.tools` module (a
  // workflow-definition package has none); the full `entries` set is
  // still materialized and laid out.
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
    // No prior deploy exists under `instanceDir`; the sentinel disables
    // the retention window.
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

  // Node keys its module cache by resolved URL; cache-busting the
  // import with the package's content identity reimports changed bytes
  // instead of resolving to the prior instance.
  const workflowEntry = args.closure.entries.find(
    (entry) =>
      entry.name === workflowPin.name && entry.version === workflowPin.version,
  );

  const definition = await args.loadDefinition({
    packageDir,
    ...(workflowEntry !== undefined
      ? {
          importCacheKey: getToolPackageSourceContentIdentity(
            workflowEntry.source,
          ),
        }
      : {}),
  });

  logger.debug`applied frozen workflow closure ${workflowPin.name}@${workflowPin.version}: loaded definition ${definition.id}`;
  return { definition, packageDir, deployDir: result.deployDir };
}
