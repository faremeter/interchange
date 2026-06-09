// Atomic apply protocol for a ToolPackageManifest.
//
// Wraps the loader in a stage-and-swap transaction:
//
//   1. Stage everything under `<instanceDir>/pending/`.
//   2. Run the loader against the pending tree end-to-end (materialize
//      every entry, dynamic-import every top-level entry, validate
//      AnnotatedToolFactory exports).
//   3. On success, atomically swap: remove `<instanceDir>/previous/`,
//      rename `<instanceDir>/active/` → `<instanceDir>/previous/`,
//      rename `<instanceDir>/pending/` → `<instanceDir>/active/`. The
//      previous-deploy tree is retained as a safety net the caller may
//      keep or prune on its own schedule.
//   4. On any loader failure, delete `<instanceDir>/pending/`, leave
//      `<instanceDir>/active/` untouched, return a DeployApplyError
//      with the loader's category.
//
// Caller responsibilities (not handled here):
//
//   - Constructing the WebSocket frame from the returned error.
//   - Writing the rejected manifest + error to the sidecar's git audit
//     trail under `audit/rejected-applies/<attemptId>/`.
//   - Pruning `<instanceDir>/previous/` on whatever lifecycle suits the
//     deploy machinery (next-successful-apply, garbage collector, etc).
//
// The previousDeployId is opaque to this module; it is carried through
// on the returned DeployApplyError so the caller can confirm the
// atomicity invariant ("the instance's active deploy id is unchanged
// after a failed apply") without re-deriving it.
//
// Constraint for tool package authors: a top-level package's
// `interchange.tools` module is dynamic-imported from a path under
// `<instanceDir>/pending/` that the swap step renames to
// `<instanceDir>/active/`. Modules loaded this way therefore have
// `import.meta.url` bound to the pending path; any call-time dynamic
// import resolved relative to `import.meta.url` will fail after the
// swap completes because the original path is gone. Top-level
// imports (resolved at module-init time, before the swap) are
// unaffected. Future work may lift this constraint by realpath-
// resolving the import location or by re-loading from `active/`
// after the swap; until then, package authors must keep their
// dynamic imports module-relative through the bundler / package
// exports, not relative to `import.meta.url`.
//
// Cross-apply ESM module-identity: Node keys its ESM cache by
// resolved URL. Reapplying a manifest whose `(name, version)` pair is
// reused but whose bytes have changed (operator rebuild, hot-fix
// republish) would otherwise serve the previously-imported module
// until the sidecar restarts. The loader cache-busts each import URL
// with an `?integrity=<sri>` query string so distinct bytes resolve
// to distinct ESM cache entries.

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

const PENDING_DIR = "pending";
const ACTIVE_DIR = "active";
const PREVIOUS_DIR = "previous";
const PREVIOUS_STAGED_DIR = "previous.staged";
const PREVIOUS_REAP_DIR = "previous.reap";

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
  readonly previousDeployId: string;
  /** New deploy id; on success becomes the active deploy id. */
  readonly newDeployId: string;
}

export interface ApplyAtomicSuccess {
  readonly status: "ok";
  readonly activeDeployId: string;
  readonly loaded: readonly LoadedToolPackage[];
}

export interface ApplyAtomicFailure {
  readonly status: "failed";
  readonly category: DeployApplyErrorCategory;
  readonly message: string;
  readonly package?: { readonly name: string; readonly version: string };
  /**
   * The deploy id the instance is now running. For every category
   * except `apply.previous-rotation.failed` this equals the input
   * `previousDeployId` (the prior deploy is untouched). For
   * `apply.previous-rotation.failed` the pending→active swap
   * committed before the post-swap rotation failed, so the new
   * deploy is live on disk and this field carries the input
   * `newDeployId` — the caller persists that as the instance's
   * active id before emitting the failure.
   */
  readonly previousDeployId: string;
  readonly attemptId: string;
  readonly occurredAt: string;
}

export type ApplyAtomicResult = ApplyAtomicSuccess | ApplyAtomicFailure;

/**
 * Stage-and-swap a tool-package manifest into the per-instance
 * `instanceDir`. The orchestration is single-threaded per instance:
 * the swap, rollback, and post-swap-rotation steps each assume
 * exclusive write access to `<instanceDir>/{pending,active,previous,
 * previous.staged}/`. The hub-side session manager already
 * serializes applies per agent (one apply per `agentAddress` at a
 * time); host-side callers that bypass that serialization must
 * provide their own per-`instanceDir` lock.
 */
export async function applyAtomic(
  args: ApplyAtomicArgs,
): Promise<ApplyAtomicResult> {
  const pendingDir = path.join(args.instanceDir, PENDING_DIR);
  const activeDir = path.join(args.instanceDir, ACTIVE_DIR);
  const previousDir = path.join(args.instanceDir, PREVIOUS_DIR);
  const stagedDir = path.join(args.instanceDir, PREVIOUS_STAGED_DIR);
  const reapDir = path.join(args.instanceDir, PREVIOUS_REAP_DIR);

  // A pending dir left behind by a prior failure must be cleared
  // before staging; the rename-to-active step assumes it owns the path.
  // The previous.staged and previous.reap dirs are also swept here so
  // a leftover from a crashed previous apply (active→staged rename
  // committed but pending→active never ran, or post-swap rotation
  // failed after the prior-previous moved aside to reapDir) cannot
  // survive into this apply's swap window — the swap below assumes
  // it owns both paths. Sweeping at the top rather than mid-swap also
  // means a sidecar that boots with leftover staged/reap trees clears
  // them on the first apply regardless of whether the load step
  // succeeds.
  await Promise.all([
    fs.rm(pendingDir, { recursive: true, force: true }),
    fs.rm(stagedDir, { recursive: true, force: true }),
    fs.rm(reapDir, { recursive: true, force: true }),
  ]);
  await fs.mkdir(pendingDir, { recursive: true });

  let loaded: readonly LoadedToolPackage[];
  try {
    loaded = await args.loader.loadManifest({
      manifest: args.manifest,
      instanceScratchDir: pendingDir,
      assetRoot: args.assetRoot,
      assetMounts: args.assetMounts,
    });
  } catch (err) {
    await fs.rm(pendingDir, { recursive: true, force: true });
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
  // categories, so this check fires after a successful load but
  // before swap. The loader has already prefixed each tool factory id
  // with its bundle id by the time we see it here, so a collision in
  // `factory.id` means two pinned packages shared a bundle id (the
  // per-bundle prefix did not produce unique ids across the load).
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
        await fs.rm(pendingDir, { recursive: true, force: true });
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
        await fs.rm(pendingDir, { recursive: true, force: true });
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

  // Swap. Order:
  //   1. Remove any existing previous/ (prior backup we no longer need).
  //   2. Move active/ to previous/ if active/ exists (first apply has none).
  //   3. Move pending/ to active/.
  // If step 3 fails after step 2 committed, roll back step 2 so on-disk
  // state matches the unchanged previousDeployId. Without the rollback
  // the instance would be left with no active/ directory while the
  // recorded deploy id still points at the previous deploy — a partial
  // state the caller has no clean way to recover from.
  //
  // The swap stages the prior-active aside under `stagedDir` rather
  // than overwriting `previousDir` directly. If the pending→active
  // rename fails the rollback can put the prior-active back without
  // having ever touched the prior-previous tree, so the safety net
  // documented at the top of this file ("the previous-deploy tree
  // is retained as a safety net") survives swap failures.
  //
  // CRASH WINDOW: power loss between the active→staged rename below
  // and the pending→active rename that follows leaves the prior-
  // active tree at `previous.staged` with no `active` tree on disk.
  // The next boot is deploy-less by design (the active-deploy-id
  // file still points at the prior id; the active tree is absent),
  // and the next apply's prelude sweeps the stale `previous.staged`
  // before it would conflict. The prior `previous/` tree (the safety
  // net) is preserved through this window, but the further-back
  // `previous.staged` content from this apply is lost — acceptable
  // because the agent restarts deploy-less rather than half-applied.
  //
  // The sweep depends on a follow-up applyAtomic invocation ever
  // occurring. If the sidecar boots and the next operator action is
  // an undeploy (no apply, ever), the leftover `previous.staged`
  // persists in the instance directory harmlessly — it occupies disk
  // but participates in no boot path and is reaped by the next apply
  // if one eventually lands. (`stagedDir` and `reapDir` were already
  // swept in the prelude above; nothing to do here.)
  let activeExists = true;
  try {
    await fs.access(activeDir);
  } catch {
    activeExists = false;
  }
  if (activeExists) {
    try {
      await fs.rename(activeDir, stagedDir);
    } catch (err) {
      // The active tree is still in place on disk (rename either moved
      // it or did nothing). Sweep the pending tree so the next apply's
      // prelude does not have to inherit a fully-staged bundle from
      // the rejected attempt, then route through the structured-failure
      // path so the caller's audit-trail + WS-frame plumbing fires
      // the same way it would for any other rejected category.
      await fs.rm(pendingDir, { recursive: true, force: true });
      const out: ApplyAtomicFailure = {
        status: "failed",
        category: "apply.swap.failed",
        message: `active→staged swap failed: ${err instanceof Error ? err.message : String(err)}`,
        previousDeployId: args.previousDeployId,
        attemptId: args.attemptId,
        occurredAt: new Date().toISOString(),
      };
      return out;
    }
  }
  try {
    await fs.rename(pendingDir, activeDir);
  } catch (err) {
    if (activeExists) {
      try {
        await fs.rename(stagedDir, activeDir);
      } catch (rollbackErr) {
        // Both renames failed: on-disk state is no longer
        // {active=previousDeployId} and the structured-failure path
        // cannot honestly claim `previousDeployId: args.previousDeployId`
        // (the docstring at ApplyAtomicFailure.previousDeployId
        // promises that field reflects the instance's current
        // disk-state deploy id). Surface this as a thrown exception
        // so the caller tears the harness down instead of writing an
        // audit entry asserting an invariant the filesystem no
        // longer satisfies. The next boot's first apply will rebuild
        // from a deploy-less instance.
        logger.error`atomic apply swap failed in both directions for attempt ${args.attemptId}: ${err instanceof Error ? err.message : String(err)} / rollback: ${rollbackErr instanceof Error ? rollbackErr.message : String(rollbackErr)}`;
        throw new Error(
          `atomic apply diverged on disk for attempt ${args.attemptId}: ` +
            `pending→active rename failed (${err instanceof Error ? err.message : String(err)}) ` +
            `and staged→active rollback also failed (${rollbackErr instanceof Error ? rollbackErr.message : String(rollbackErr)}); ` +
            `harness must abort, next boot starts deploy-less`,
          { cause: err },
        );
      }
    }
    // Rollback succeeded (or there was no prior active to roll back).
    // `previousDir` is untouched in this branch — the prior-previous
    // safety net survives swap failure. Return through the
    // structured-failure path so the caller routes the failure to
    // emitDeployApplyError + audit-trail just like any other
    // rejected category. Match the loader-error and duplicate-id
    // branches by sweeping the leftover pendingDir before returning
    // so an aborted apply does not leave a fully-staged bundle tree
    // on disk until the next attempt. (A leftover `previous.staged`
    // here is swept by the next apply's `fs.rm(stagedDir, ...)` at
    // the top of the swap block.)
    await fs.rm(pendingDir, { recursive: true, force: true });
    const out: ApplyAtomicFailure = {
      status: "failed",
      category: "apply.swap.failed",
      message: `pending→active swap failed: ${err instanceof Error ? err.message : String(err)}`,
      previousDeployId: args.previousDeployId,
      attemptId: args.attemptId,
      occurredAt: new Date().toISOString(),
    };
    return out;
  }

  // Swap succeeded. Retire the prior-previous and promote the
  // newly-staged prior-active into its slot.
  //
  // The new active tree is already live on disk. If the prior-previous
  // rm or the staged→previous rename fails (EBUSY, EPERM, EXDEV on a
  // cross-fs rename, etc.) the caller's contract is to receive a
  // structured failure that travels through the same audit-trail /
  // WS-frame channel as every other rejected apply, not an exception
  // that bypasses the apply-error pipeline and leaves
  // active-deploy-id un-bumped while the on-disk tree advanced.
  // Surface the cleanup failure as apply.previous-rotation.failed so
  // the caller emits a deploy.apply.error frame referencing the
  // newly-active deploy id.
  //
  // The rotation moves the prior-previous tree aside to `reapDir`
  // before the staged→previous rename so the safety-net tree is not
  // destroyed by a rename that later fails — a sequence of rm-then-
  // rename would leave the slot empty if the rename failed. On rename
  // failure the prior-previous is restored from `reapDir`; on rename
  // success the prior-previous is reaped. The `reapDir` slot is swept
  // at the top of every apply alongside `stagedDir`, so a crash that
  // leaves an unreaped `reapDir` is cleaned up by the next apply.
  if (activeExists) {
    let priorPreviousMovedAside = false;
    try {
      await fs.access(previousDir);
      await fs.rename(previousDir, reapDir);
      priorPreviousMovedAside = true;
    } catch (err) {
      const code =
        err !== null &&
        typeof err === "object" &&
        "code" in err &&
        typeof err.code === "string"
          ? err.code
          : null;
      if (code !== "ENOENT") {
        logger.warn`apply post-swap rotation failed for attempt ${args.attemptId} (move-aside): ${err instanceof Error ? err.message : String(err)}; active deploy ${args.newDeployId} is live on disk, previous safety net retained`;
        const out: ApplyAtomicFailure = {
          status: "failed",
          category: "apply.previous-rotation.failed",
          message: `post-swap previous-dir rotation failed: ${err instanceof Error ? err.message : String(err)}`,
          previousDeployId: args.newDeployId,
          attemptId: args.attemptId,
          occurredAt: new Date().toISOString(),
        };
        return out;
      }
    }
    try {
      await fs.rename(stagedDir, previousDir);
    } catch (err) {
      if (priorPreviousMovedAside) {
        try {
          await fs.rename(reapDir, previousDir);
        } catch (rollbackErr) {
          logger.error`apply post-swap rotation rollback failed for attempt ${args.attemptId}: ${rollbackErr instanceof Error ? rollbackErr.message : String(rollbackErr)}; previous-deploy safety net lost on disk`;
        }
      }
      logger.warn`apply post-swap rotation failed for attempt ${args.attemptId}: ${err instanceof Error ? err.message : String(err)}; active deploy ${args.newDeployId} is live on disk`;
      const out: ApplyAtomicFailure = {
        status: "failed",
        category: "apply.previous-rotation.failed",
        message: `post-swap previous-dir rotation failed: ${err instanceof Error ? err.message : String(err)}`,
        previousDeployId: args.newDeployId,
        attemptId: args.attemptId,
        occurredAt: new Date().toISOString(),
      };
      return out;
    }
    if (priorPreviousMovedAside) {
      try {
        await fs.rm(reapDir, { recursive: true, force: true });
      } catch (err) {
        logger.warn`apply post-rotation reap of previous.reap failed for attempt ${args.attemptId}: ${err instanceof Error ? err.message : String(err)}; next apply prelude will sweep it`;
      }
    }
  }

  logger.info`apply ok: attempt ${args.attemptId} active as ${args.newDeployId}`;
  return {
    status: "ok",
    activeDeployId: args.newDeployId,
    loaded,
  };
}
