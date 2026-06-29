import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import git from "isomorphic-git";
import { collectReachableObjects } from "./object-walk";
import { publishPackAtomically } from "./pack-receive";
import { listRepoRefs, repoDiskUsage, type RepoDiskUsage } from "./repo-disk";

/**
 * How much commit history a GC pass preserves.
 *
 * - `tip-only`: keep only the objects reachable from each ref tip's tree.
 *   Prior commits are dropped, leaving the tip commit with dangling parent
 *   pointers (the same shape the deploy ref already has). Smallest repo;
 *   suited to environments that treat the repo as current-state cache.
 * - `keep-history`: also keep every object reachable through the commit
 *   ancestry. Preserves the audit trail; suited to environments that treat
 *   the repo as a long-term archive.
 */
export type RetentionPolicy = "tip-only" | "keep-history";

export type GCResult = {
  before: RepoDiskUsage;
  after: RepoDiskUsage;
  reclaimedBytes: number;
  keptObjects: number;
};

/**
 * Collect every object reachable through a commit's ancestry, tolerating
 * commits whose parents are not present on disk.
 *
 * The deploy ref is applied tip-only, so its commit carries parent pointers
 * to objects that were never transferred. Reading an absent parent throws
 * `NotFoundError`; we stop descending that branch rather than aborting the
 * whole GC, because a missing parent is the expected steady state for these
 * repos. Only that specific absence is tolerated — any other read failure
 * (corruption, a non-commit oid, I/O) surfaces, since swallowing it here
 * would drop a present, reachable subtree from the keep set and the caller
 * would then delete it.
 */
async function collectHistoryObjects(
  dir: string,
  tipOid: string,
): Promise<Set<string>> {
  const objects = new Set<string>();
  const seenCommits = new Set<string>();
  const queue: string[] = [tipOid];

  while (queue.length > 0) {
    const commitOid = queue.shift();
    if (commitOid === undefined) break;
    if (seenCommits.has(commitOid)) continue;
    seenCommits.add(commitOid);

    let parents: string[];
    try {
      const { commit } = await git.readCommit({ fs, dir, oid: commitOid });
      parents = commit.parent;
    } catch (err) {
      if (err instanceof Error && "code" in err && err.code === "NotFoundError")
        continue;
      throw err;
    }

    for (const oid of await collectReachableObjects(dir, commitOid)) {
      objects.add(oid);
    }
    for (const parent of parents) {
      if (!seenCommits.has(parent)) queue.push(parent);
    }
  }

  return objects;
}

/**
 * Absolute paths of every `.pack` and `.idx` file currently in the repo's
 * pack directory. Snapshotted before the consolidated pack is published so
 * the freshly published pair is never in the removal set.
 */
function listPackFiles(dir: string): string[] {
  const packDir = path.join(dir, ".git", "objects", "pack");
  let entries: string[];
  try {
    entries = fs.readdirSync(packDir);
  } catch (cause) {
    if (
      cause instanceof Error &&
      (cause as NodeJS.ErrnoException).code === "ENOENT"
    ) {
      return [];
    }
    throw cause;
  }
  return entries
    .filter((name) => name.endsWith(".pack") || name.endsWith(".idx"))
    .map((name) => path.join(packDir, name));
}

/**
 * Remove every loose object fan-out directory under `.git/objects/`.
 *
 * Safe only after the consolidated pack containing the entire keep set is
 * published: every kept object that was loose is then also packed, and every
 * loose object outside the keep set is garbage. The `pack` and `info`
 * children are left untouched.
 */
async function removeLooseObjects(dir: string): Promise<void> {
  const objectsDir = path.join(dir, ".git", "objects");
  let entries: string[];
  try {
    entries = fs.readdirSync(objectsDir);
  } catch (cause) {
    if (
      cause instanceof Error &&
      (cause as NodeJS.ErrnoException).code === "ENOENT"
    ) {
      return;
    }
    throw cause;
  }
  for (const name of entries) {
    if (name === "pack" || name === "info") continue;
    if (!/^[0-9a-f]{2}$/.test(name)) continue;
    await fs.promises.rm(path.join(objectsDir, name), {
      recursive: true,
      force: true,
    });
  }
}

/**
 * Reclaim disk in an agent git repo by repacking everything reachable from
 * its refs into a single pack and dropping the superseded packs and loose
 * objects.
 *
 * Compute the keep set as the union of reachability over every head ref
 * (agent repos carry two diverging heads, `main` and `deploy`, so unioning
 * is mandatory — repacking one ref's reachability alone would discard the
 * other's live objects). Pack the keep set into one self-contained pack via
 * `git.packObjects` and publish it through the same atomic staging dance
 * receives use, so a concurrent unlocked reader never observes a torn pack.
 * Only then remove the packs that predated this pass and every loose object;
 * each kept object is by then present in the consolidated pack.
 *
 * # Concurrency
 *
 * The caller must hold the repo's write lock (`withRepoLock` on the hub,
 * `runRepoOp` on the sidecar). The lock excludes concurrent writers, so the
 * keep set computed from the refs cannot be invalidated by a commit landing
 * mid-pass. Removal of a superseded pack or loose object races only with
 * unlocked readers, the same POSIX window `unpublishPack` already accepts —
 * and strictly safer here, since every removed-but-reachable object is also
 * in the freshly published consolidated pack.
 *
 * Returns the disk usage before and after plus the reclaimed byte delta. A
 * repo with no resolvable refs is left untouched.
 */
export async function runGC(
  dir: string,
  opts: { retention: RetentionPolicy },
): Promise<GCResult> {
  const before = repoDiskUsage(dir);
  const refs = await listRepoRefs(dir);

  if (refs.length === 0) {
    return { before, after: before, reclaimedBytes: 0, keptObjects: 0 };
  }

  const keep = new Set<string>();
  for (const { oid } of refs) {
    const reachable =
      opts.retention === "tip-only"
        ? await collectReachableObjects(dir, oid)
        : await collectHistoryObjects(dir, oid);
    for (const objectOid of reachable) keep.add(objectOid);
  }

  const supersededPacks = listPackFiles(dir);

  const result = await git.packObjects({
    fs,
    dir,
    oids: [...keep],
    write: false,
  });
  if (result.packfile === undefined) {
    throw new Error(
      `packObjects returned no packfile while consolidating ${keep.size.toString()} objects in ${dir}`,
    );
  }
  const transferId = `gc-${crypto.randomUUID().replace(/-/g, "")}`;
  await publishPackAtomically(dir, result.packfile, transferId);

  for (const packFile of supersededPacks) {
    await fs.promises.rm(packFile, { force: true });
  }
  await removeLooseObjects(dir);

  const after = repoDiskUsage(dir);
  return {
    before,
    after,
    reclaimedBytes: before.gitBytes - after.gitBytes,
    keptObjects: keep.size,
  };
}
