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

  try {
    await git.init({ fs, dir: scratchDir, defaultBranch: "main" });

    const packDir = path.join(scratchDir, ".git", "objects", "pack");
    await fsp.mkdir(packDir, { recursive: true });

    const packFilename = `pack-asset-${path.basename(scratchDir)}.pack`;
    const relPackPath = path.join(".git", "objects", "pack", packFilename);
    const absPackPath = path.join(scratchDir, relPackPath);
    await fsp.writeFile(absPackPath, pack);

    const { oids } = await git.indexPack({
      fs,
      dir: scratchDir,
      filepath: relPackPath,
    });
    if (!oids.includes(commitSha)) {
      throw new Error(`expected commit ${commitSha} not found in pack`);
    }

    const { commit } = await git.readCommit({
      fs,
      dir: scratchDir,
      oid: commitSha,
    });
    const { tree } = await git.readTree({
      fs,
      dir: scratchDir,
      oid: commit.tree,
    });

    // Clear any prior materialization at the mount so removed files
    // don't linger from an older asset version.
    await fsp.rm(destDir, { recursive: true, force: true });
    await fsp.mkdir(destDir, { recursive: true });

    await writeTreeEntries(scratchDir, destDir, tree);

    logger.info`Materialized asset pack at ${destDir} (${commitSha.slice(0, 8)} on ${ref})`;
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    // Best-effort cleanup so a partial materialization doesn't linger.
    await fsp
      .rm(destDir, { recursive: true, force: true })
      .catch(() => undefined);
    throw new Error(`asset_materialization_failed: ${msg}`, { cause: err });
  } finally {
    await fsp
      .rm(scratchDir, { recursive: true, force: true })
      .catch(() => undefined);
  }
}

type TreeEntry = {
  type: "blob" | "tree" | "commit";
  mode: string;
  path: string;
  oid: string;
};

async function writeTreeEntries(
  repoDir: string,
  targetDir: string,
  entries: TreeEntry[],
): Promise<void> {
  for (const entry of entries) {
    const entryPath = path.join(targetDir, entry.path);
    switch (entry.type) {
      case "tree": {
        await fsp.mkdir(entryPath, { recursive: true });
        const { tree } = await git.readTree({
          fs,
          dir: repoDir,
          oid: entry.oid,
        });
        await writeTreeEntries(repoDir, entryPath, tree);
        break;
      }
      case "blob": {
        const { blob } = await git.readBlob({
          fs,
          dir: repoDir,
          oid: entry.oid,
        });
        await fsp.writeFile(entryPath, blob, {
          mode: entry.mode === "100755" ? 0o755 : 0o644,
        });
        break;
      }
      case "commit": {
        // Submodule reference. Skill assets do not declare submodules
        // and validatePush has no reason to accept one, so seeing
        // entry.type === "commit" here means an asset tree was pushed
        // with content the v1 materialization pipeline cannot honor.
        // Fail loudly rather than silently dropping the entry: a
        // half-materialized workspace would diverge from the source
        // with no signal to the operator.
        throw new Error(
          `asset_materialization_failed: submodule reference at ${entry.path} is not supported`,
        );
      }
      default: {
        const exhaustive: never = entry.type;
        throw new Error(
          `asset_materialization_failed: unknown tree-entry type ${String(exhaustive)} at ${entry.path}`,
        );
      }
    }
  }
}
