// Sidecar-side delivery of a workflow closure's source assets.
//
// A `WorkflowSourceAssetMount` carries a git pack for one hub asset. How that
// pack is materialized depends on how the closure references the asset:
//   - a tarball-format entry reads a `.tgz` blob from a plain-file checkout
//     (`applyAssetPack`), keyed by an `assetId -> mountPath` map; and
//   - a source-format entry checks a subtree out of the git objects, so the
//     pack is indexed into a RETAINED `.git` (a "gitDir"), keyed by an
//     `assetId -> gitDir` map the loader hands `materializeGitEntry`.
// One asset can be referenced both ways; a source-format workflow closure
// references only source entries, so it produces only gitDirs.

import fsp from "node:fs/promises";
import path from "node:path";

import { getLogger } from "@intx/log";
import { base64Decode } from "@intx/types";
import { applyAssetPack } from "@intx/hub-agent";
import {
  DEFAULT_PACK_MATERIALIZATION_LIMITS,
  indexPackIntoGitDir,
} from "@intx/storage-isogit/node";
import type { WorkflowSourceAssetMount } from "@intx/types/sidecar";
import type { ToolPackageManifest } from "@intx/types/tool-packages";

const logger = getLogger(["sidecar", "source-asset-delivery"]);

const SAFE_ASSET_ID = /^[a-zA-Z0-9_.-]+$/;

/**
 * Index a delivered asset pack into `gitDir` and RETAIN the object store, so a
 * source subtree can be checked out from it. Builds into a sibling temp `.git`
 * and RENAMES it into place, so the durable store is complete-or-absent: a crash
 * mid-materialization leaves only the temp, never a partial `gitDir` that the
 * dir-exists check `resolveDeploymentAssetMounts` runs on restore would trust.
 * The rename is same-filesystem (the temp is a sibling under `gitDir`'s parent).
 *
 * On a rename conflict (a stale `gitDir` from a torn prior attempt) the freshly
 * built store wins: the existing dir is removed and the temp renamed over it, so
 * a re-delivery at a new commit never keeps the old content. A secondary rm
 * failure is logged so it does not silently mask state; the primary error is
 * rethrown.
 */
export async function indexAssetPackIntoGitDir(args: {
  pack: Uint8Array;
  commitSha: string;
  gitDir: string;
}): Promise<void> {
  const { pack, commitSha, gitDir } = args;
  const parent = path.dirname(gitDir);
  await fsp.mkdir(parent, { recursive: true });
  const tempDir = await fsp.mkdtemp(path.join(parent, ".indexing-"));

  const cleanupTemp = async (): Promise<void> => {
    await fsp.rm(tempDir, { recursive: true, force: true }).catch((rmErr) => {
      const rmMsg = rmErr instanceof Error ? rmErr.message : String(rmErr);
      logger.warn`source-asset temp gitdir cleanup failed at ${tempDir}: ${rmMsg}`;
    });
  };

  try {
    await indexPackIntoGitDir(
      tempDir,
      pack,
      commitSha,
      DEFAULT_PACK_MATERIALIZATION_LIMITS,
    );
  } catch (err) {
    await cleanupTemp();
    throw err;
  }

  try {
    await fsp.rename(tempDir, gitDir);
  } catch (err) {
    // Only a "destination already exists" failure means a torn prior attempt we
    // may supersede; any other rename error (EXDEV, EACCES, ENOSPC, EIO) must
    // NOT destroy a possibly-good prior store -- clean up only the fresh temp
    // and surface it.
    if (!isDestinationExistsError(err)) {
      await cleanupTemp();
      throw err;
    }
    // The final path already holds a store (a torn prior attempt): rebuild
    // wins, so drop the stale store and rename the fresh one over it.
    await fsp.rm(gitDir, { recursive: true, force: true });
    try {
      await fsp.rename(tempDir, gitDir);
    } catch (retryErr) {
      await cleanupTemp();
      throw retryErr;
    }
  }
}

/** Whether `err` is a rename failure caused by a non-empty destination. */
function isDestinationExistsError(err: unknown): boolean {
  if (err === null || typeof err !== "object" || !("code" in err)) return false;
  const code = String(err.code);
  return code === "ENOTEMPTY" || code === "EEXIST" || code === "EISDIR";
}

/**
 * The materialization format(s) each asset id is referenced with in `closure`.
 * An asset with any tarball entry needs a plain-file checkout; an asset with
 * any source entry needs a gitDir.
 */
export function assetReferenceFormats(
  closure: ToolPackageManifest,
): Map<string, { tarball: boolean; source: boolean }> {
  const byAsset = new Map<string, { tarball: boolean; source: boolean }>();
  for (const entry of closure.entries) {
    if (entry.source.kind !== "asset") continue;
    const existing = byAsset.get(entry.source.assetId) ?? {
      tarball: false,
      source: false,
    };
    if (entry.source.package.format === "tarball") existing.tarball = true;
    else existing.source = true;
    byAsset.set(entry.source.assetId, existing);
  }
  return byAsset;
}

/** The absolute gitDir a source asset's objects are indexed into. */
export function sourceAssetGitDir(gitDirRoot: string, assetId: string): string {
  // Reject an all-dots assetId (".", "..", ...) before the join. SAFE_ASSET_ID
  // permits "." as a character, so a bare ".." would otherwise escape the
  // per-asset dir (`path.join(root, "..")` is root's parent) and "." would
  // resolve to the shared root itself. Mirrors `applyAssetPack`'s all-dots
  // segment guard.
  if (!SAFE_ASSET_ID.test(assetId) || /^\.+$/.test(assetId)) {
    throw new Error(
      `source-asset delivery: unsafe assetId ${JSON.stringify(assetId)}`,
    );
  }
  return path.join(gitDirRoot, assetId);
}

/**
 * The single cap on the total inline (base64) source-asset payload a workflow
 * closure may deliver in one frame. Both the probe and the deploy pass this to
 * `materializeWorkflowAssets`; a git-sourced asset that grows past it is the
 * signal to move that path's asset delivery to a streamed transfer. One
 * constant so the two paths cannot drift.
 */
export const MAX_INLINE_ASSET_PAYLOAD_BYTES = 32 * 1024 * 1024;

/**
 * Materialize a workflow closure's delivered source assets: for each asset,
 * check out plain tarball files under `assetRoot` (if the closure has tarball
 * entries for it) and/or index the pack into a gitDir under `gitDirRoot` (if it
 * has source entries). Returns both maps for the loader.
 */
export async function materializeWorkflowAssets(args: {
  assets: readonly WorkflowSourceAssetMount[];
  closure: ToolPackageManifest;
  assetRoot: string;
  gitDirRoot: string;
  maxAssetPayloadBytes: number;
}): Promise<{
  assetMounts: ReadonlyMap<string, string>;
  gitDirs: ReadonlyMap<string, string>;
}> {
  const formats = assetReferenceFormats(args.closure);
  const assetMounts = new Map<string, string>();
  const gitDirs = new Map<string, string>();
  const seen = new Set<string>();
  let totalPayloadBytes = 0;
  for (const asset of args.assets) {
    totalPayloadBytes += asset.pack.length;
    if (totalPayloadBytes > args.maxAssetPayloadBytes) {
      throw new Error(
        `workflow source-asset materialization: inline asset payload exceeds the ${String(args.maxAssetPayloadBytes)}-byte cap`,
      );
    }
    if (seen.has(asset.assetId)) {
      throw new Error(
        `workflow source-asset materialization: asset ${JSON.stringify(asset.assetId)} is delivered more than once`,
      );
    }
    seen.add(asset.assetId);
    // The frame delivers one mount per asset the closure references, so a
    // delivered asset with no closure entry is a hub/frame inconsistency.
    // Fail loud rather than silently ignore it (while still counting its
    // payload toward the cap above).
    const refs = formats.get(asset.assetId);
    if (refs === undefined) {
      throw new Error(
        `workflow source-asset materialization: asset ${JSON.stringify(asset.assetId)} is delivered but referenced by no closure entry`,
      );
    }
    const pack = base64Decode(asset.pack);
    if (refs.tarball) {
      await applyAssetPack({
        workspaceRoot: args.assetRoot,
        mountPath: asset.mountPath,
        pack,
        ref: asset.ref,
        commitSha: asset.commitSha,
      });
      assetMounts.set(asset.assetId, asset.mountPath);
    }
    if (refs.source) {
      const gitDir = sourceAssetGitDir(args.gitDirRoot, asset.assetId);
      await indexAssetPackIntoGitDir({
        pack,
        commitSha: asset.commitSha,
        gitDir,
      });
      gitDirs.set(asset.assetId, gitDir);
    }
  }
  return { assetMounts, gitDirs };
}
