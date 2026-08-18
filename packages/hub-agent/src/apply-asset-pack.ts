// Materialize an asset pack as plain files under a workspace mount path.
//
// Asset packs are git packfiles produced by the hub for assets attached
// to an agent (today only `skill`). The sidecar receives the pack at
// session start and writes its tree contents under
// `<workspaceRoot>/<mountPath>/`. The workspace is plain files, not a
// git working tree — asset packs must not share the agent's deploy
// `.git/` (separate ref namespaces, separate object lifecycles), so we
// index the pack against a scratch git directory and copy the tree out.
//
// Asset packs in v1 are unsigned: they originate from the hub itself
// (synthetic content authored via the asset service) and are validated
// by the kind handler's `validatePush` on the hub-side write path.
// The cryptographic signature scheme that `applyDeployPack` enforces
// does not apply.

import fs from "node:fs";
import fsp from "node:fs/promises";
import path from "node:path";
import git from "isomorphic-git";

import { getLogger } from "@intx/log";
import {
  DEFAULT_PACK_MATERIALIZATION_LIMITS,
  indexPackIntoGitDir,
  writeTreeToDisk,
} from "@intx/storage-isogit/node";

const logger = getLogger(["interchange", "hub-agent", "apply-asset-pack"]);

const SAFE_PATH_SEGMENT = /^[a-zA-Z0-9_./-]+$/;

export type ApplyAssetPackArgs = {
  workspaceRoot: string;
  /** Repo-relative directory (with or without trailing slash) under
   * `workspaceRoot` where the pack's tree contents should land. */
  mountPath: string;
  pack: Uint8Array;
  ref: string;
  commitSha: string;
};

/**
 * Materialize an asset pack at `<workspaceRoot>/<mountPath>/`.
 *
 * Throws an Error with prefix `asset_materialization_failed:` on any
 * failure (pack index error, missing commit, missing tree, blob write
 * error). Callers in the WS layer classify this as the existing
 * `pack.reject` reason `corrupt`.
 */
export async function applyAssetPack(args: ApplyAssetPackArgs): Promise<void> {
  const { workspaceRoot, mountPath, pack, ref, commitSha } = args;

  if (mountPath.length === 0 || mountPath.startsWith("/")) {
    throw new Error(
      `asset_materialization_failed: invalid mountPath ${JSON.stringify(mountPath)}`,
    );
  }
  // Reject any all-dots segment (".", "..", "...") before per-segment
  // SAFE_PATH_SEGMENT screening. The base regex permits "." since it
  // allows the character; without this guard a mountPath of "." would
  // resolve destDir to workspaceRoot itself and the subsequent
  // recursive rm would wipe the entire workspace.
  for (const segment of mountPath.split("/")) {
    if (segment === "") continue;
    if (/^\.+$/.test(segment) || !SAFE_PATH_SEGMENT.test(segment)) {
      throw new Error(
        `asset_materialization_failed: invalid mountPath segment in ${JSON.stringify(mountPath)}`,
      );
    }
  }
  // Defense-in-depth: after segment-level checks, normalize the path
  // and reject anything that still resolves to "." or contains ".."
  // (e.g. a permutation the per-segment loop missed).
  const normalized = path.posix.normalize(mountPath);
  if (
    normalized === "." ||
    normalized === "./" ||
    normalized.split("/").includes("..")
  ) {
    throw new Error(
      `asset_materialization_failed: mountPath ${JSON.stringify(mountPath)} normalizes to a workspace-root or escaping path`,
    );
  }

  const normalizedMount = mountPath.endsWith("/")
    ? mountPath.slice(0, -1)
    : mountPath;
  const destDir = path.join(workspaceRoot, normalizedMount);

  await fsp.mkdir(workspaceRoot, { recursive: true });

  const scratchDir = await fsp.mkdtemp(
    path.join(workspaceRoot, ".intx-asset-scratch-"),
  );
  // Set while a materialized-files temp dir awaits publish; cleared once it is
  // renamed onto the mount. The `finally` removes it if a failure leaves it.
  let materializeDir: string | undefined;

  try {
    // Index the pack into the scratch `.git` and assert the pinned commit is
    // present. The scratch dir is discarded in the `finally`; this reuses the
    // same "index a pack into a gitDir and assert the commit" step a durable
    // source-asset delivery keeps.
    await indexPackIntoGitDir(
      scratchDir,
      pack,
      commitSha,
      DEFAULT_PACK_MATERIALIZATION_LIMITS,
    );

    const { commit } = await git.readCommit({
      fs,
      dir: scratchDir,
      oid: commitSha,
    });

    // Materialize into a sibling temp dir, then atomically publish it to the
    // mount by rename, so a crash mid-write never leaves a partial mount that
    // restore's dir-exists check would trust. A re-delivery keeps the prior
    // mount until the new one is ready, so a failed materialization does not
    // destroy a working mount.
    materializeDir = await fsp.mkdtemp(
      path.join(workspaceRoot, ".intx-asset-materialize-"),
    );
    await writeTreeToDisk(
      scratchDir,
      materializeDir,
      commit.tree,
      DEFAULT_PACK_MATERIALIZATION_LIMITS,
    );
    // Publish atomically: ensure the mount's PARENT exists, clear any prior
    // mount, and rename the fully materialized temp into place.
    await fsp.mkdir(path.dirname(destDir), { recursive: true });
    await fsp.rm(destDir, { recursive: true, force: true });
    await fsp.rename(materializeDir, destDir);
    materializeDir = undefined; // published; nothing left to clean up

    logger.info`Materialized asset pack at ${destDir} (${commitSha.slice(0, 8)} on ${ref})`;
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    // The mount is published only by the atomic rename above, so a failure here
    // leaves the prior (complete) mount untouched -- do not remove it. The
    // partial temp is cleaned in the `finally`.
    throw new Error(`asset_materialization_failed: ${msg}`, { cause: err });
  } finally {
    for (const dir of [scratchDir, materializeDir]) {
      if (dir === undefined) continue;
      await fsp.rm(dir, { recursive: true, force: true }).catch((rmErr) => {
        const rmMsg = rmErr instanceof Error ? rmErr.message : String(rmErr);
        logger.warn`asset pack temp cleanup failed at ${dir}: ${rmMsg}`;
      });
    }
  }
}
