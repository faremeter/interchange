import git from "isomorphic-git";

import type { StorageRuntime } from "./runtime";

// A gitDir this helper produces holds exactly one pack, so the pack filename
// is a constant rather than a caller-supplied id.
const PACK_FILENAME = "pack-indexed.pack";

/**
 * Initialize a `.git` under `gitDir` and index `pack` into its object store,
 * RETAINING the store so a caller can later read the trees and blobs the pack
 * carries (for example, check a subtree out via `writeTreeToDisk`). Asserts the
 * pinned `commitSha` is one of the objects the pack delivers, so a pack that
 * does not carry the expected commit fails loud rather than leaving a gitDir a
 * later checkout would fault on.
 *
 * This is the shared "index a pack into a gitDir and assert the commit is
 * present" step. `applyAssetPack` runs it against a scratch dir it discards
 * after copying files out; a source-asset delivery runs it against a durable
 * dir it keeps.
 */
export async function indexPackIntoGitDir(
  runtime: StorageRuntime,
  gitDir: string,
  pack: Uint8Array,
  commitSha: string,
): Promise<void> {
  await runtime.fs.mkdir(gitDir, { recursive: true });
  await git.init({ fs: runtime.fs.git, dir: gitDir, defaultBranch: "main" });
  const packDir = runtime.path.join(gitDir, ".git", "objects", "pack");
  await runtime.fs.mkdir(packDir, { recursive: true });
  const relPackPath = runtime.path.join(
    ".git",
    "objects",
    "pack",
    PACK_FILENAME,
  );
  await runtime.fs.writeFile(runtime.path.join(gitDir, relPackPath), pack);
  const { oids } = await git.indexPack({
    fs: runtime.fs.git,
    dir: gitDir,
    filepath: relPackPath,
  });
  if (!oids.includes(commitSha)) {
    throw new Error(
      `indexPackIntoGitDir: expected commit ${commitSha} not found in the pack`,
    );
  }
}
