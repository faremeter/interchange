// Deploy-side application of a code-sourced workflow's frozen closure.
//
// When a deploy frame carries a `source` (the npm registry the workflow
// definition package is published to) plus the hub's frozen dependency
// `closure` (concrete versions + integrity SRIs), the sidecar materializes
// EXACTLY that closure for its workflow child to evaluate. The closure is
// applied byte-for-byte as the hub froze it; the sidecar never re-resolves the
// pin against the registry at apply time.
//
// This is the durable deploy counterpart to the airlocked install-time
// probe: both lay the closure out with `materializeClosure`, so the fetch +
// SRI-verify + extract + `node_modules` layout is not reimplemented here.
//
// The apply imports nothing. It runs in the long-lived sidecar process, which
// keeps every module it imports, so the workflow child is the only process
// that evaluates the workflow entry.
//
// `host` comes from the caller. This package must not import
// `@intx/hub-agent` or `@intx/workflow-host`, and it does not read the
// process: the sidecar process edge resolves the platform.

import { promises as fs } from "node:fs";
import path from "node:path";

import { getLogger } from "@intx/log";
import type { ToolPackageManifest } from "@intx/types/tool-packages";
import type { WorkflowDefinitionSource } from "@intx/types/workflow-sources";

import type { TarballCache } from "./cache";
import {
  ClosureMaterializationError,
  describeError,
} from "./materialization-internal";
import type { RegistryConfig } from "./resolver";
import {
  type HostPlatform,
  materializeClosure,
  storeEntryDir,
} from "./store-layout";
import { createTarballFetcher, type TarballFetcher } from "./tarball-fetcher";

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
   * (`<instanceDir>/packages/store/...`). An apply replaces whatever an earlier
   * one left there.
   */
  readonly instanceDir: string;
  /**
   * Tarball cache shared by every apply over its root. Its in-use count keeps
   * one apply from evicting an extraction another is still copying, and it
   * counts only callers of the same instance.
   */
  readonly cache: TarballCache;
  /** Byte cap for a single HTTP-registry tarball fetch. */
  readonly registryMaxTarballBytes: number;
  /** Registry identifier -> URL + credentials materialization resolves entries against. */
  readonly registries: ReadonlyMap<string, RegistryConfig>;
  /** npm `os`/`cpu` pair materialization selects optional dependencies with. */
  readonly host: HostPlatform;
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
   * Test seam for tarball fetching. Production omits it and gets
   * `createTarballFetcher`.
   */
  readonly fetchTarball?: TarballFetcher;
}

export interface AppliedWorkflowClosure {
  /** Directory of the materialized workflow package within the closure. */
  readonly packageDir: string;
}

/**
 * Materialize a workflow definition's frozen closure durably, without
 * evaluating it.
 *
 * The closure's single top-level pin IS the workflow definition package: the
 * hub resolved the closure for exactly that pin. The frozen `entries` are
 * applied verbatim (concrete versions + SRIs), so no registry re-resolution
 * happens at apply time.
 *
 * @throws if the closure does not carry exactly one top-level pin, the source
 *   registry is not configured on this sidecar, or the apply fails (integrity
 *   mismatch, fetch failure, extract failure, ...).
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
  // gates fire again inside materialization). An `asset` closure materializes its
  // entries from the durable stores the caller populated: tarball entries from
  // `assetMounts`, source entries from `gitDirs`; materialization fails loud
  // (`asset.mount.missing` / `git.materialization.failed`) if either is absent.
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
      break;
    default: {
      const _exhaustive: never = args.source;
      throw new Error(
        `sidecar workflow-closure apply: unhandled workflow source kind ${String(_exhaustive)}`,
      );
    }
  }

  // The layout keeps files that already exist, so a tree left by an earlier
  // attempt must not carry over into this one.
  const packagesDir = path.join(args.instanceDir, "packages");
  await fs.rm(packagesDir, { recursive: true, force: true });
  await fs.mkdir(packagesDir, { recursive: true });

  let storeDir: string;
  try {
    ({ storeDir } = await materializeClosure({
      manifest: args.closure,
      instanceScratchDir: packagesDir,
      assetRoot: args.assetRoot ?? path.join(args.instanceDir, "workspace"),
      assetMounts: args.assetMounts ?? new Map(),
      gitDirs: args.gitDirs ?? new Map(),
      host: args.host,
      cache: args.cache,
      registries: args.registries,
      fetchTarball:
        args.fetchTarball ??
        createTarballFetcher({
          maxRegistryTarballBytes: args.registryMaxTarballBytes,
        }),
    }));
  } catch (cause) {
    await fs.rm(packagesDir, { recursive: true, force: true });
    const detail =
      cause instanceof ClosureMaterializationError
        ? `${cause.category}: ${cause.message}`
        : describeError(cause);
    throw new Error(
      `sidecar workflow-closure apply: materializing the frozen closure for ${workflowPin.name}@${workflowPin.version} failed (${detail})`,
      { cause },
    );
  }

  const packageDir = storeEntryDir(
    storeDir,
    workflowPin.name,
    workflowPin.version,
  );

  logger.debug`applied frozen workflow closure ${workflowPin.name}@${workflowPin.version}`;
  return { packageDir };
}
