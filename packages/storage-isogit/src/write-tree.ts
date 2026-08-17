import git from "isomorphic-git";

import type { StorageRuntime } from "./runtime";

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
 */
export async function writeTreeToDisk(
  runtime: StorageRuntime,
  dir: string,
  targetDir: string,
  treeOid: string,
): Promise<void> {
  const { tree } = await git.readTree({
    fs: runtime.fs.git,
    dir,
    oid: treeOid,
  });
  for (const entry of tree) {
    const entryPath = runtime.path.join(targetDir, entry.path);
    switch (entry.type) {
      case "tree": {
        await runtime.fs.mkdir(entryPath, { recursive: true });
        await writeTreeToDisk(runtime, dir, entryPath, entry.oid);
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
