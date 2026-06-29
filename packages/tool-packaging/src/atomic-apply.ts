// Apply protocol for a ToolPackageManifest.
//
// Each apply materializes its closure into a stable, per-deploy-id
// directory that is never moved:
//
//   <instanceDir>/packages/<deploy-id>/store/<name>/<version>/...
//
// The loader dynamic-imports each pinned package's `interchange.tools`
// module from an absolute path inside that deploy-id directory. Because
// the directory is never renamed, the URL Node keys its ESM module
// cache under stays valid for the life of the deploy: a tool package
// that uses `import.meta.url`, `require.resolve()`, or a call-time
// `await import("./sibling.js")` resolves against a path that still
// exists. (The earlier protocol staged under `pending/` and renamed
// `pending/` → `active/` after import; the renamed-away URL ENOENTed
// any late path resolution. This module exists to remove that swap.)
//
// This module does NOT commit. It stages, loads, validates, and
// returns the loaded packages plus the deploy-id directory. The commit
// point lives one layer up: the caller writes the instance's
// `active-deploy-id` file to name `newDeployId`. Until that write
// lands, the instance is still running `previousDeployId`, whose
// directory is left untouched here. The "atomic swap" is therefore the
// single `active-deploy-id` file write the caller performs, not a
// directory rename.
//
// Retention. A prelude sweep removes every `packages/<id>/` except
// `{newDeployId, previousDeployId}`. The keep set is invariant across
// the caller's commit: before the commit the live deploy is
// `previousDeployId`; after it the live deploy is `newDeployId` and
// `previousDeployId` is the retained prior tree. So a single sweep at
// the start of the apply both bounds disk to ~2 closures and preserves
// exactly the prior deploy as a liveness window for any session still
// draining against it.
//
// Retention invariant: the previous deploy's directory must survive
// until the next apply, because a session built against it may still
// perform a call-time `await import()` into its tree. The next apply's
// prelude reaps it. This is safe only because per-agent applies are
// serialized (the hub runs one apply per `agentAddress` at a time) and
// each successful apply replaces the running harness, so by the time
// apply N+2 reaps deploy N, generation-N's harness teardown has
// completed and no live code references deploy N's tree.
//
// Caller responsibilities (not handled here):
//
//   - Writing `active-deploy-id` to commit the staged deploy.
//   - Constructing the WebSocket frame from the returned error.
//   - Writing the rejected manifest + error to the sidecar's git audit
//     trail under `audit/rejected-applies/<attemptId>/`.
//
// Crash safety. Boot never reads a deploy-id directory: the harness
// rebuilds by re-running the apply against the current manifest into a
// fresh deploy id. A half-written `packages/<newDeployId>/` left by a
// crash mid-build is therefore self-healing — the next boot
// re-materializes into a new id and the prelude sweep reclaims the
// orphan. Only `active-deploy-id` needs durability, and the caller owns
// that. Consequently this module fsyncs nothing.
//
// Cross-apply ESM module-identity: Node keys its ESM cache by resolved
// URL. Per-deploy-id directories already make every apply's import URL
// unique, so a reused `(name, version)` whose bytes changed across
// applies resolves to a distinct path. The loader additionally
// cache-busts each import URL with an `?integrity=<sri>` query string;
// that remains correct (and harmless) under per-deploy-id paths.

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
   * The deploy id the instance is still running. The apply never wrote
   * `active-deploy-id`, so a failure always leaves the prior deploy
   * live: this equals the input `previousDeployId`. (The caller owns
   * the commit and is the only layer that can advance the active id;
   * the persist-degraded case it handles there carries its own
   * inverted meaning, but that case does not originate in this module.)
   */
  readonly previousDeployId: string;
  readonly attemptId: string;
  readonly occurredAt: string;
}

export type ApplyAtomicResult = ApplyAtomicSuccess | ApplyAtomicFailure;

/**
 * Stage a tool-package manifest into a per-deploy-id directory under
 * `instanceDir` and return the loaded packages. The orchestration is
 * single-threaded per instance: the prelude sweep assumes exclusive
 * write access to `<instanceDir>/packages/`. The hub-side session
 * manager already serializes applies per agent (one apply per
 * `agentAddress` at a time); host-side callers that bypass that
 * serialization must provide their own per-`instanceDir` lock, because
 * the prelude sweep deletes sibling deploy directories and a racing
 * apply could delete a directory the other just committed.
 */
export async function applyAtomic(
  args: ApplyAtomicArgs,
): Promise<ApplyAtomicResult> {
  const packagesDir = path.join(args.instanceDir, PACKAGES_DIR);
  const deployDir = path.join(packagesDir, args.newDeployId);

  // Prelude sweep. Reclaim every prior deploy directory except the one
  // we are about to build (`newDeployId`) and the one still live
  // (`previousDeployId`). `previousDeployId` may be the "no prior
  // deploy" sentinel, which simply matches no directory. The sweep is
  // best-effort per stray: an EIO/EPERM reclaiming one old deploy is a
  // disk-reclamation concern, not a correctness one, and must not fail
  // an otherwise-valid apply — the next apply's prelude retries the
  // reclaim. The deploy directory we then build, by contrast, must be
  // a clean tree, so its removal+mkdir below propagate on failure.
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

  // Check for duplicate factory ids across loaded bundles. The plan's
  // taxonomy distinguishes `tool.name.duplicate` from other
  // categories, so this check fires after a successful load but before
  // the caller commits. The loader has already prefixed each tool
  // factory id with its bundle id by the time we see it here, so a
  // collision in `factory.id` means two pinned packages shared a bundle
  // id (the per-bundle prefix did not produce unique ids across the
  // load).
  //
  // Plugin factories carry their own `id` (not bundle-prefixed) and
  // are addressed by id at harness construction; a collision between
  // two plugin ids would resolve to undefined behavior at the harness
  // layer. Treat that as the same apply-time gate: tool and plugin
  // id spaces are tracked separately so the operator-facing message
  // points at the right surface, but neither admits a duplicate.
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
