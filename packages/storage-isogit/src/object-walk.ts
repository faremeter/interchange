import { readCommitObject, readTreeEntries } from "./isogit-helpers";
import type { StorageRuntime } from "./runtime";

/**
 * Collect all unique object OIDs reachable from a commit: the commit itself,
 * its tree, and all blobs and subtrees recursively.
 */
export async function collectReachableObjects(
  runtime: StorageRuntime,
  dir: string,
  commitOid: string,
): Promise<string[]> {
  const seen = new Set<string>();
  seen.add(commitOid);

  const commit = await readCommitObject(runtime, dir, commitOid);
  seen.add(commit.tree);

  async function walkTree(treeOid: string): Promise<void> {
    const entries = await readTreeEntries(runtime, dir, treeOid);
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
