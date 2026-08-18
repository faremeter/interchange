// Materialize a source-format asset closure entry (`kind:"asset"` with
// `package.format:"source"`) from an indexed pack.
//
// A source-format entry's files come from a subtree of a hub `workflow` git
// asset at a pinned commit, not a tarball. The pack is indexed once at
// checkout time (the caller supplies the resulting `gitDir`); this reads the
// pinned subtree straight from those authenticated objects, verifies its git
// tree oid against the frozen `treeOid`, and writes the subtree into a scratch
// directory the store layout then copies from. The tarball cache is bypassed:
// `treeOid` is a git oid, not an SRI.

import { promises as fs } from "node:fs";
import path from "node:path";

import git from "isomorphic-git";
import { getLogger } from "@intx/log";
import {
  DEFAULT_PACK_MATERIALIZATION_LIMITS,
  writeTreeToDisk,
} from "@intx/storage-isogit/node";
import type { ToolPackageAssetSourceTree } from "@intx/types/tool-packages";

import { ToolLoaderError, describeError } from "./loader-internal";

const logger = getLogger(["sidecar", "tool-packaging", "git-materialize"]);

/**
 * Read the entry's pinned subtree from `gitDir`, verify it against the frozen
 * `treeOid`, and write it into a fresh scratch directory under
 * `instanceScratchDir`. Returns the same `{ dir, release }` shape the tarball
 * path returns; `release` is a no-op because there is no content-addressed
 * cache handle to hold — the scratch dir lives under the deploy directory the
 * caller reclaims.
 */
export async function materializeGitEntry(args: {
  tree: ToolPackageAssetSourceTree;
  name: string;
  version: string;
  gitDir: string;
  instanceScratchDir: string;
}): Promise<{ dir: string; release: () => void }> {
  const { tree: source, name, version, gitDir, instanceScratchDir } = args;

  // `readTree` peels the commit to its tree; `filepath` then navigates to the
  // pinned member. `packageDir === "."` selects the repo root, i.e. the
  // commit's own tree, so it takes no `filepath`.
  let subtreeOid: string;
  try {
    const { oid } = await git.readTree({
      fs,
      dir: gitDir,
      oid: source.commitSha,
      ...(source.packageDir === "." ? {} : { filepath: source.packageDir }),
    });
    subtreeOid = oid;
  } catch (err) {
    throw new ToolLoaderError({
      category: "git.materialization.failed",
      message: `git subtree ${JSON.stringify(source.packageDir)} for ${name}@${version} could not be read at ${source.commitSha}: ${describeError(err)}`,
      package: { name, version },
    });
  }

  // The frozen `treeOid` is the content identity. The hub read it from the
  // same git objects, so a mismatch means the delivered pack diverges from
  // what was frozen; fail loud rather than materialize unverified bytes.
  if (subtreeOid !== source.treeOid) {
    throw new ToolLoaderError({
      category: "git.materialization.failed",
      message: `git subtree ${JSON.stringify(source.packageDir)} at ${source.commitSha} for ${name}@${version} hashed to tree ${subtreeOid}, not the frozen ${source.treeOid}`,
      package: { name, version },
    });
  }

  const dir = await fs.mkdtemp(path.join(instanceScratchDir, "git-"));
  try {
    await writeTreeToDisk(
      gitDir,
      dir,
      subtreeOid,
      DEFAULT_PACK_MATERIALIZATION_LIMITS,
    );
  } catch (err) {
    // Best-effort cleanup of the partial scratch dir; log a secondary rm
    // failure so it does not silently mask state. The primary error is still
    // thrown. Mirrors the cleanup logging in `applyAssetPack` and the sidecar's
    // source-asset delivery.
    await fs.rm(dir, { recursive: true, force: true }).catch((rmErr) => {
      logger.warn`git subtree scratch cleanup failed at ${dir}: ${describeError(rmErr)}`;
    });
    throw new ToolLoaderError({
      category: "git.materialization.failed",
      message: `writing git subtree for ${name}@${version} failed: ${describeError(err)}`,
      package: { name, version },
    });
  }

  return { dir, release: () => undefined };
}
