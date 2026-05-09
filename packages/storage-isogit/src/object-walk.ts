import { readCommitObject, readTreeEntries } from "./isogit-helpers";

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

  const commit = await readCommitObject(dir, commitOid);
  seen.add(commit.tree);

  async function walkTree(treeOid: string): Promise<void> {
    const entries = await readTreeEntries(dir, treeOid);
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
