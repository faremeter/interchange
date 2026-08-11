import git from "isomorphic-git";
import { hasCode } from "@intx/types";
import type { StorageRuntime } from "./runtime";

/**
 * Disk-occupancy snapshot of an agent repo's `.git` directory. Drives the
 * write-path GC trigger (pack count crossing a threshold) and the
 * disk-pressure observability warning (byte size crossing a threshold).
 */
export type RepoDiskUsage = {
  gitBytes: number;
  packCount: number;
  looseObjectCount: number;
};

/**
 * Count the immediate child entries of `dir`. Returns 0 when the directory
 * does not exist (a repo subtree that has not been created yet) -- absence
 * is a real zero, not an error to surface.
 */
async function countDirEntries(
  runtime: StorageRuntime,
  dir: string,
): Promise<number> {
  try {
    return (await runtime.fs.readdir(dir)).length;
  } catch (cause) {
    if (hasCode(cause) && cause.code === "ENOENT") {
      return 0;
    }
    throw cause;
  }
}

/**
 * Count loose git objects under `.git/objects/<xx>/`. Loose objects are the
 * un-packed per-commit objects isomorphic-git writes on each commit; their
 * count rising and collapsing after a repack is the pack-growth signature a
 * GC pass reclaims. The two-hex-char fan-out dirs plus `pack`/`info` are the
 * only children of `objects/`; the latter two are skipped. Keep this filter in
 * step with the synchronous Node counterpart in `node-metrics.ts`.
 */
export async function countLooseObjects(
  runtime: StorageRuntime,
  repoDir: string,
): Promise<number> {
  const objectsDir = runtime.path.join(repoDir, ".git", "objects");
  let fanoutDirs: string[];
  try {
    fanoutDirs = await runtime.fs.readdir(objectsDir);
  } catch (cause) {
    if (hasCode(cause) && cause.code === "ENOENT") {
      return 0;
    }
    throw cause;
  }
  let total = 0;
  for (const name of fanoutDirs) {
    if (name === "pack" || name === "info") continue;
    // Loose-object fan-out dirs are exactly two lowercase hex chars.
    if (!/^[0-9a-f]{2}$/.test(name)) continue;
    total += await countDirEntries(
      runtime,
      runtime.path.join(objectsDir, name),
    );
  }
  return total;
}

/**
 * Count `.pack` files under `.git/objects/pack/`. Each accepted receive
 * publishes one pack and the prior tip's pack is never reclaimed without a
 * GC pass, so the pack count is the monotonic accumulation a write-path GC
 * trigger watches.
 */
export async function countPackFiles(
  runtime: StorageRuntime,
  repoDir: string,
): Promise<number> {
  const packDir = runtime.path.join(repoDir, ".git", "objects", "pack");
  let entries: string[];
  try {
    entries = await runtime.fs.readdir(packDir);
  } catch (cause) {
    if (hasCode(cause) && cause.code === "ENOENT") {
      return 0;
    }
    throw cause;
  }
  return entries.filter((name) => name.endsWith(".pack")).length;
}

/**
 * Total byte size of the repo's `.git` directory (loose + pack + refs +
 * logs). A coarse repo-size proxy for the disk-pressure warning. Walks the
 * tree through the configured filesystem; bounded by the repo size, which is
 * the thing being measured.
 */
export async function gitBytes(
  runtime: StorageRuntime,
  repoDir: string,
): Promise<number> {
  const gitDir = runtime.path.join(repoDir, ".git");
  let total = 0;
  const stack: string[] = [gitDir];
  while (stack.length > 0) {
    const current = stack.pop();
    if (current === undefined) break;
    let stat;
    try {
      stat = await runtime.fs.lstat(current);
    } catch (cause) {
      if (hasCode(cause) && cause.code === "ENOENT") {
        continue;
      }
      throw cause;
    }
    if (stat.isDirectory()) {
      for (const child of await runtime.fs.readdir(current)) {
        stack.push(runtime.path.join(current, child));
      }
    } else if (stat.isFile()) {
      total += stat.size;
    }
  }
  return total;
}

/**
 * The object counts alone, without the full `.git` byte walk. The
 * write-path GC trigger samples these on every write, so they must stay
 * cheap — two directory reads — and leave the recursive byte walk
 * (`gitBytes`) for the infrequent reclaim path.
 */
export type RepoObjectCounts = {
  packCount: number;
  looseObjectCount: number;
};

export async function repoObjectCounts(
  runtime: StorageRuntime,
  dir: string,
): Promise<RepoObjectCounts> {
  const [packCount, looseObjectCount] = await Promise.all([
    countPackFiles(runtime, dir),
    countLooseObjects(runtime, dir),
  ]);
  return {
    packCount,
    looseObjectCount,
  };
}

/**
 * Snapshot the GC-relevant disk counters for the agent repo at `dir`,
 * including the full `.git` byte walk.
 */
export async function repoDiskUsage(
  runtime: StorageRuntime,
  dir: string,
): Promise<RepoDiskUsage> {
  const [bytes, counts] = await Promise.all([
    gitBytes(runtime, dir),
    repoObjectCounts(runtime, dir),
  ]);
  return {
    gitBytes: bytes,
    ...counts,
  };
}

/**
 * Resolve every local branch ref of the repo at `dir` to its tip SHA.
 *
 * Agent repos carry two diverging heads (`refs/heads/main` and
 * `refs/heads/deploy`); GC must union reachability across all of them, so it
 * enumerates them here rather than assuming a single ref. No tags or other
 * ref namespaces are created in these repos, so listing branches is
 * complete.
 */
export async function listRepoRefs(
  runtime: StorageRuntime,
  dir: string,
): Promise<{ ref: string; oid: string }[]> {
  const branches = await git.listBranches({ fs: runtime.fs.git, dir });
  const refs: { ref: string; oid: string }[] = [];
  for (const branch of branches) {
    const ref = `refs/heads/${branch}`;
    const oid = await git.resolveRef({ fs: runtime.fs.git, dir, ref });
    refs.push({ ref, oid });
  }
  return refs;
}
