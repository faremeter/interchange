// Apply protocol for a ToolPackageManifest.
//
// Each apply materializes its closure into a stable, never-renamed
// `<instanceDir>/packages/<deploy-id>/store/<name>/<version>/...`
// directory, so the URLs Node keys its ESM module cache under stay
// valid for the life of the deploy (`import.meta.url`-relative
// resolution still works). This module only stages, loads, and
// validates; the caller commits by writing `active-deploy-id` to name
// `newDeployId`. A prelude sweep retains only `{newDeployId,
// previousDeployId}`, bounding disk to ~2 closures and preserving the
// prior deploy as a liveness window for sessions still draining.
// Boot never reads a deploy-id directory — the harness rebuilds by
// re-applying into a fresh id, so a half-written new deploy is
// self-healing and the next prelude sweep reclaims it.

import { promises as fs } from "node:fs";
import path from "node:path";

import { getLogger } from "@intx/log";
import type { DeployApplyErrorCategory } from "@intx/types/sidecar";
import type { ToolPackageManifest } from "@intx/types/tool-packages";

import {
  type LoadedToolPackage,
  type ToolLoader,
  ToolLoaderError,
} from "./loader";

const logger = getLogger(["sidecar", "tool-packaging", "atomic-apply"]);

const PACKAGES_DIR = "packages";

export interface ApplyAtomicArgs {
  readonly manifest: ToolPackageManifest;
  readonly loader: ToolLoader;
  readonly instanceDir: string;
  readonly assetRoot: string;
  /**
   * Maps a `source.assetId` to a workspace-relative mount path. The
   * loader uses this to resolve `kind: "asset"` manifest entries.
   * Forwarded verbatim to `ToolLoader.loadManifest`.
   */
  readonly assetMounts: ReadonlyMap<string, string>;
  /**
   * Maps a source-format asset entry's `source.assetId` to an absolute
   * indexed git directory. Forwarded verbatim to `ToolLoader.loadManifest`.
   */
  readonly gitDirs: ReadonlyMap<string, string>;
  readonly attemptId: string;
  /**
   * The deploy id the instance is currently running. Retained on disk
   * through this apply (its `packages/<previousDeployId>/` tree is not
   * swept) and carried back on a failure so the caller can confirm the
   * prior deploy is unchanged.
   */
  readonly previousDeployId: string;
  /** New deploy id; on the caller's commit becomes the active deploy id. */
  readonly newDeployId: string;
}

export interface ApplyAtomicSuccess {
  readonly status: "ok";
  readonly activeDeployId: string;
  /** Absolute path of the staged, never-renamed deploy directory. */
  readonly deployDir: string;
  readonly loaded: readonly LoadedToolPackage[];
}

export interface ApplyAtomicFailure {
  readonly status: "failed";
  readonly category: DeployApplyErrorCategory;
  readonly message: string;
  readonly package?: { readonly name: string; readonly version: string };
  /**
   * The deploy id the instance is still running. A failed apply never
   * wrote `active-deploy-id`, so this always equals the input
   * `previousDeployId`.
   */
  readonly previousDeployId: string;
  readonly attemptId: string;
  readonly occurredAt: string;
}

export type ApplyAtomicResult = ApplyAtomicSuccess | ApplyAtomicFailure;

/**
 * Stage a tool-package manifest into a per-deploy-id directory under
 * `instanceDir` and return the loaded packages. Single-threaded per
 * instance: the prelude sweep assumes exclusive write access to
 * `<instanceDir>/packages/`. The hub serializes applies per agent; a
 * host-side caller that bypasses that must provide its own
 * per-`instanceDir` lock.
 */
export async function applyAtomic(
  args: ApplyAtomicArgs,
): Promise<ApplyAtomicResult> {
  const packagesDir = path.join(args.instanceDir, PACKAGES_DIR);
  const deployDir = path.join(packagesDir, args.newDeployId);

  // Prelude sweep. Reclaim every prior deploy directory except the one
  // we are about to build and the one still live. Best-effort per
  // stray: an EIO/EPERM reclaiming one old deploy must not fail an
  // otherwise-valid apply (the next prelude retries). The deploy
  // directory we build must be a clean tree, so its removal+mkdir
  // below propagate on failure.
  const keep = new Set([args.newDeployId, args.previousDeployId]);
  let existing: string[];
  try {
    existing = await fs.readdir(packagesDir);
  } catch (err) {
    if (!isENOENT(err)) throw err;
    existing = [];
  }
  await Promise.all(
    existing
      .filter((id) => !keep.has(id))
      .map(async (id) => {
        try {
          await fs.rm(path.join(packagesDir, id), {
            recursive: true,
            force: true,
          });
        } catch (err) {
          logger.warn`apply prelude sweep could not reclaim stale deploy ${id} under ${packagesDir}: ${err instanceof Error ? err.message : String(err)}; next apply will retry`;
        }
      }),
  );

  // A leftover directory under this exact `newDeployId` (a crash mid-
  // build, or the astronomically unlikely uuid reuse) must be cleared
  // before staging so the loader builds into a clean tree.
  await fs.rm(deployDir, { recursive: true, force: true });
  await fs.mkdir(deployDir, { recursive: true });

  let loaded: readonly LoadedToolPackage[];
  try {
    loaded = await args.loader.loadManifest({
      manifest: args.manifest,
      instanceScratchDir: deployDir,
      assetRoot: args.assetRoot,
      assetMounts: args.assetMounts,
      gitDirs: args.gitDirs,
    });
  } catch (err) {
    await fs.rm(deployDir, { recursive: true, force: true });
    if (err instanceof ToolLoaderError) {
      logger.warn`apply rejected (${err.category}) for attempt ${args.attemptId}; previous deploy ${args.previousDeployId} retained`;
      const out: ApplyAtomicFailure = {
        status: "failed",
        category: err.category,
        message: err.message,
        ...(err.package !== undefined ? { package: err.package } : {}),
        previousDeployId: args.previousDeployId,
        attemptId: args.attemptId,
        occurredAt: new Date().toISOString(),
      };
      return out;
    }
    // Unknown error shape: surface as factory.construct.failed since
    // that is the closest catch-all in the taxonomy.
    logger.error`unexpected loader error for attempt ${args.attemptId}: ${err instanceof Error ? err.message : String(err)}`;
    return {
      status: "failed",
      category: "factory.construct.failed",
      message: err instanceof Error ? err.message : String(err),
      previousDeployId: args.previousDeployId,
      attemptId: args.attemptId,
      occurredAt: new Date().toISOString(),
    };
  }

  // Check for duplicate factory ids across loaded bundles. The loader
  // has already prefixed each tool factory id with its bundle id, so a
  // collision means two pinned packages shared a bundle id. Plugin
  // factories carry their own (non-prefixed) `id` and are addressed by
  // id at harness construction; a collision there would be undefined
  // behavior at the harness layer. Track the two id spaces separately
  // so the message points at the right surface; neither admits a
  // duplicate.
  const toolIdsSeen = new Set<string>();
  const pluginIdsSeen = new Set<string>();
  for (const pkg of loaded) {
    for (const factory of pkg.factories) {
      if (toolIdsSeen.has(factory.id)) {
        await fs.rm(deployDir, { recursive: true, force: true });
        const out: ApplyAtomicFailure = {
          status: "failed",
          category: "tool.name.duplicate",
          message: `tool factory id ${factory.id} appears in more than one pinned package`,
          package: { name: pkg.name, version: pkg.version },
          previousDeployId: args.previousDeployId,
          attemptId: args.attemptId,
          occurredAt: new Date().toISOString(),
        };
        return out;
      }
      toolIdsSeen.add(factory.id);
    }
    for (const plugin of pkg.plugins) {
      if (pluginIdsSeen.has(plugin.id)) {
        await fs.rm(deployDir, { recursive: true, force: true });
        const out: ApplyAtomicFailure = {
          status: "failed",
          category: "tool.name.duplicate",
          message: `plugin factory id ${plugin.id} appears in more than one pinned package`,
          package: { name: pkg.name, version: pkg.version },
          previousDeployId: args.previousDeployId,
          attemptId: args.attemptId,
          occurredAt: new Date().toISOString(),
        };
        return out;
      }
      pluginIdsSeen.add(plugin.id);
    }
  }

  logger.info`apply staged: attempt ${args.attemptId} ready as ${args.newDeployId} (caller commits via active-deploy-id)`;
  return {
    status: "ok",
    activeDeployId: args.newDeployId,
    deployDir,
    loaded,
  };
}

function isENOENT(err: unknown): boolean {
  return (
    err !== null &&
    typeof err === "object" &&
    "code" in err &&
    (err as { code: unknown }).code === "ENOENT"
  );
}
