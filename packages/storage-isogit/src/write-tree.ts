import git from "isomorphic-git";

import type { StorageRuntime } from "./runtime";
import type { PackMaterializationLimits } from "./materialization-limits";

/**
 * Materialize the git tree `treeOid` (and everything it references) as plain
 * files under `targetDir`. `dir` is a git directory whose object database
 * holds `treeOid`, its subtrees, and its blobs. Subtrees recurse; a regular
 * blob is written with mode `0o755` when its git mode is `100755`, else
 * `0o644`.
 *
 * Throws on a submodule (`commit`) entry and on a symlink (git mode
 * `120000`). Both are content this writer cannot reproduce faithfully: a
 * submodule's object is not in this pack, and writing a symlink verbatim
 * would let a link target escape `targetDir`. Failing loud keeps the
 * materialized tree from silently diverging from its source; a caller that
 * genuinely tolerates either must handle it before this.
 *
 * `limits` bounds the CUMULATIVE disk footprint: a checkout whose blob content
 * totals more than `maxTreeBytes`, or whose files plus directories exceed
 * `maxTreeEntries`, fails loud rather than filling the host or exhausting
 * inodes. The tree the pack carries is untrusted (a pushed asset). Note the
 * bound is on the running total, not a single blob's peak: `git.readBlob`
 * materializes each blob (delta-reconstructed when needed) fully into memory
 * before the byte check runs, so a single delta chain reconstructing to
 * gigabytes is DETECTED here but is allocated once before it is rejected --
 * bounding that single-blob peak is the pre-index guard's and the hub ingest's
 * job. The cumulative ceiling is what this owns.
 */
export async function writeTreeToDisk(
  runtime: StorageRuntime,
  dir: string,
  targetDir: string,
  treeOid: string,
  limits: PackMaterializationLimits,
): Promise<void> {
  await writeTreeInto(runtime, dir, targetDir, treeOid, limits, {
    bytes: 0,
    entries: 0,
  });
}

// A mutable tally threaded through the recursion so the ceilings are cumulative
// across the whole tree, not per-subtree.
interface CheckoutTally {
  bytes: number;
  entries: number;
}

async function writeTreeInto(
  runtime: StorageRuntime,
  dir: string,
  targetDir: string,
  treeOid: string,
  limits: PackMaterializationLimits,
  tally: CheckoutTally,
): Promise<void> {
  const { tree } = await git.readTree({
    fs: runtime.fs.git,
    dir,
    oid: treeOid,
  });
  for (const entry of tree) {
    const entryPath = runtime.path.join(targetDir, entry.path);
    tally.entries += 1;
    if (tally.entries > limits.maxTreeEntries) {
      throw new Error(
        `writeTreeToDisk: tree exceeds the ${String(limits.maxTreeEntries)}-entry cap`,
      );
    }
    switch (entry.type) {
      case "tree": {
        await runtime.fs.mkdir(entryPath, { recursive: true });
        await writeTreeInto(runtime, dir, entryPath, entry.oid, limits, tally);
        break;
      }
      case "blob": {
        if (entry.mode === "120000") {
          throw new Error(
            `writeTreeToDisk: symlink at ${entry.path} is not supported`,
          );
        }
        const { blob } = await git.readBlob({
          fs: runtime.fs.git,
          dir,
          oid: entry.oid,
        });
        tally.bytes += blob.length;
        if (tally.bytes > limits.maxTreeBytes) {
          throw new Error(
            `writeTreeToDisk: tree exceeds the ${String(limits.maxTreeBytes)}-byte cap`,
          );
        }
        await runtime.fs.writeFile(entryPath, blob, {
          mode: entry.mode === "100755" ? 0o755 : 0o644,
        });
        break;
      }
      case "commit": {
        throw new Error(
          `writeTreeToDisk: submodule reference at ${entry.path} is not supported`,
        );
      }
      default: {
        const exhaustive: never = entry.type;
        throw new Error(
          `writeTreeToDisk: unknown tree-entry type ${String(exhaustive)} at ${entry.path}`,
        );
      }
    }
  }
}
