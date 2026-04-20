import fs from "node:fs";
import path from "node:path";
import git from "isomorphic-git";

const SAFE_PATH_SEGMENT = /^[a-zA-Z0-9_-]+$/;

/**
 * Apply a git packfile to a repository, update a ref, and verify the result.
 *
 * Writes the pack bytes to a temporary file inside .git, calls indexPack to
 * extract objects, then updates `ref` to point at `expectedSha`. Throws if
 * the resolved ref does not match after the update.
 *
 * The caller is responsible for ensuring `dir` is an initialized git repo.
 */
export async function applyPack(
  dir: string,
  pack: Uint8Array,
  ref: string,
  expectedSha: string,
  transferId: string,
): Promise<void> {
  if (!SAFE_PATH_SEGMENT.test(transferId)) {
    throw new Error(
      `transferId contains unsafe characters: ${JSON.stringify(transferId)}`,
    );
  }
  const filename = `pack-recv-${transferId}.pack`;
  const filepath = path.join(".git", filename);
  const fullPath = path.join(dir, filepath);

  try {
    await fs.promises.writeFile(fullPath, pack);
    const { oids } = await git.indexPack({ fs, dir, filepath });

    if (!oids.includes(expectedSha)) {
      throw new Error(
        `sha_mismatch: expected commit ${expectedSha} not found in pack`,
      );
    }

    await git.writeRef({ fs, dir, ref, value: expectedSha, force: true });
  } finally {
    await fs.promises.rm(fullPath, { force: true });
    // indexPack also creates a .idx file alongside the pack
    const idxPath = fullPath.replace(/\.pack$/, ".idx");
    await fs.promises.rm(idxPath, { force: true });
  }
}
