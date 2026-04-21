import fs from "node:fs";
import path from "node:path";
import git from "isomorphic-git";

const SAFE_PATH_SEGMENT = /^[a-zA-Z0-9_-]+$/;

type TreeEntry = {
  type: "blob" | "tree" | "commit";
  mode: string;
  path: string;
  oid: string;
};

async function topLevelNames(dir: string, oid: string): Promise<Set<string>> {
  const { tree } = await git.readTree({ fs, dir, oid });
  return new Set(tree.map((e) => e.path));
}

/**
 * Remove stale working-tree content and write the commit's tree to disk.
 *
 * Derives the set of deploy-managed top-level entries from the union of the
 * old and new commit trees, removes those entries, then writes the new tree.
 * Paths that never appear in any commit tree (e.g. .git, agent.json, state,
 * keys) are never touched.
 *
 * NOTE: The rm-then-write sequence is not atomic. If writeTree fails after rm
 * succeeds (e.g. disk full), the working tree will be missing the cleared
 * paths. The ref is not updated in that case (caller writes ref after this
 * function returns), so a restart will re-read from the prior commit, but the
 * working tree will be stale until the next successful applyPack.
 */
async function checkoutTree(
  dir: string,
  commitSha: string,
  ref: string,
): Promise<void> {
  const { commit } = await git.readCommit({ fs, dir, oid: commitSha });
  const { tree } = await git.readTree({ fs, dir, oid: commit.tree });

  // Collect top-level names managed by deploy trees (new + previous).
  const managed = new Set(tree.map((e) => e.path));

  const prevSha = await git.resolveRef({ fs, dir, ref }).catch(() => null);
  if (prevSha !== null) {
    const { commit: prev } = await git.readCommit({ fs, dir, oid: prevSha });
    for (const name of await topLevelNames(dir, prev.tree)) {
      managed.add(name);
    }
  }

  const existing = await fs.promises.readdir(dir);
  for (const name of existing) {
    if (!managed.has(name)) continue;
    await fs.promises.rm(path.join(dir, name), {
      recursive: true,
      force: true,
    });
  }

  await writeTreeEntries(dir, dir, tree);
}

async function writeTreeEntries(
  repoDir: string,
  targetDir: string,
  entries: TreeEntry[],
): Promise<void> {
  for (const entry of entries) {
    const entryPath = path.join(targetDir, entry.path);
    if (entry.type === "tree") {
      await fs.promises.mkdir(entryPath, { recursive: true });
      const { tree } = await git.readTree({ fs, dir: repoDir, oid: entry.oid });
      await writeTreeEntries(repoDir, entryPath, tree);
    } else if (entry.type === "blob") {
      const { blob } = await git.readBlob({
        fs,
        dir: repoDir,
        oid: entry.oid,
      });
      await fs.promises.writeFile(entryPath, blob, {
        mode: entry.mode === "100755" ? 0o755 : 0o644,
      });
    }
  }
}

/**
 * Apply a git packfile to a repository, check out the working tree, and
 * update the ref.
 *
 * Writes the pack to .git/objects/pack/ (the standard location for git
 * packfiles), creates the .idx index via indexPack, checks out the tree to
 * the working directory, then updates `ref` to point at `expectedSha`.
 * The ref is written last so it never points at a commit whose working tree
 * has not been materialized.
 *
 * Throws if the expected commit is not found in the pack.
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

  const packDir = path.join(dir, ".git", "objects", "pack");
  await fs.promises.mkdir(packDir, { recursive: true });

  const filename = `pack-recv-${transferId}.pack`;
  const filepath = path.join(".git", "objects", "pack", filename);
  const fullPath = path.join(dir, filepath);

  await fs.promises.writeFile(fullPath, pack);

  try {
    const { oids } = await git.indexPack({ fs, dir, filepath });

    if (!oids.includes(expectedSha)) {
      throw new Error(
        `sha_mismatch: expected commit ${expectedSha} not found in pack`,
      );
    }

    await checkoutTree(dir, expectedSha, ref);
    await git.writeRef({ fs, dir, ref, value: expectedSha, force: true });
  } catch (err) {
    // Clean up pack and idx on failure so we don't leave corrupt state.
    await fs.promises.rm(fullPath, { force: true });
    const idxPath = fullPath.replace(/\.pack$/, ".idx");
    await fs.promises.rm(idxPath, { force: true });
    throw err;
  }
}
