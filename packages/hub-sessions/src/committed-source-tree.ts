// Adapts a repo-store `CommittedReads` handle (reads pinned to a commit,
// addressed by object id) to the `SourceTreeReads` shape the source-closure
// resolver consumes (reads addressed by repo-relative path). This bridge lives
// at the caller layer, not inside the resolver: the resolver stays agnostic of
// where its tree bytes come from (a test fake, this store, a future one), and
// the repo-store stays unaware of the closure interface. Only a layer that
// composes both types -- the install/deploy glue's caller -- owns the join.

import type { CommittedReads } from "./repo-store/types";
import type { SourceTreeReads } from "./workflow-source-closure";

/**
 * Wrap `reads` so `readBlob(path)` resolves a repo-relative POSIX path to its
 * blob: list the path's parent directory, match the final segment as a `blob`
 * entry, then read by its object id. `listDir` and `treeOid` pass straight
 * through -- both handles already speak repo-relative paths and return the same
 * shapes.
 */
export function committedReadsToSourceTree(
  reads: CommittedReads,
): SourceTreeReads {
  return {
    async readBlob(blobPath) {
      // The parent of a top-level path (no slash) is the root tree, which
      // `CommittedReads.listDir` addresses with the empty string.
      const slash = blobPath.lastIndexOf("/");
      const parentDir = slash === -1 ? "" : blobPath.slice(0, slash);
      const name = slash === -1 ? blobPath : blobPath.slice(slash + 1);
      const entries = await reads.listDir(parentDir);
      // Match on the blob type so a path naming a directory or a gitlink fails
      // loud here rather than handing a tree/commit oid to `readBlobByOid`.
      const entry = entries.find((e) => e.name === name && e.type === "blob");
      if (entry === undefined) {
        throw new Error(
          `committedReadsToSourceTree: no blob at ${JSON.stringify(blobPath)}`,
        );
      }
      return reads.readBlobByOid(entry.oid);
    },
    listDir: (dir) => reads.listDir(dir),
    treeOid: (dir) => reads.treeOid(dir),
  };
}
