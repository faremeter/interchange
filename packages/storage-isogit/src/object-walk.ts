import fs from "node:fs";
import git from "isomorphic-git";

/**
 * Collect all unique object OIDs reachable from a commit: the commit itself,
 * its tree, and all blobs and subtrees recursively.
 */
export async function collectReachableObjects(
  dir: string,
  commitOid: string,
): Promise<string[]> {
  const seen = new Set<string>();
  seen.add(commitOid);

  const { object: commitObj } = await git.readObject({
    fs,
    dir,
    oid: commitOid,
  });
  const commit = commitObj as { tree: string };
  seen.add(commit.tree);

  async function walkTree(treeOid: string): Promise<void> {
    const { object: treeObj } = await git.readObject({
      fs,
      dir,
      oid: treeOid,
    });
    const entries = treeObj as { oid: string; type: string }[];
    for (const entry of entries) {
      if (seen.has(entry.oid)) continue;
      seen.add(entry.oid);
      if (entry.type === "tree") {
        await walkTree(entry.oid);
      }
    }
  }

  await walkTree(commit.tree);
  return [...seen];
}
