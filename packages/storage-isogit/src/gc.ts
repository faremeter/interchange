import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import git from "isomorphic-git";
import { getLogger } from "@intx/log";
import { collectReachableObjects } from "./object-walk";
import { publishPackAtomically } from "./pack-receive";
import {
  gitBytes,
  listRepoRefs,
  repoDiskUsage,
  repoObjectCounts,
  type RepoDiskUsage,
} from "./repo-disk";
import { withRepoDirLock } from "./repo-lock";

const logger = getLogger(["interchange", "storage-isogit", "gc"]);

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
 * Write-path reclaim policy. A writer holding the per-directory lock samples
 * the repo's object counts after its mutation and repacks under `retention`
 * once the pack count reaches `packThreshold` OR the loose-object count
 * reaches `looseThreshold`. Both triggers matter: a hub repo accumulates
 * packs as it receives state, while a sidecar repo accumulates loose objects
 * as the reactor commits. When a reclaim runs, the `.git` byte size is
 * checked against `warnBytes` and a disk-pressure warning is emitted if it
 * is reached — surfacing runaway accumulation that survives a reclaim — so
 * the byte check rides the reclaim rather than every write.
 */
export type GCPolicy = {
  packThreshold: number;
  looseThreshold: number;
  warnBytes: number;
  retention: RetentionPolicy;
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
 * The caller MUST already hold the repo's per-directory lock
 * (`withRepoDirLock`). This is the lock-free core: writers trigger reclaim
 * inline after a commit/apply while still holding that lock, and external
 * callers not already under it use {@link runGC}, which acquires it. The
 * lock excludes concurrent writers, so the keep set computed from the refs
 * cannot be invalidated by a commit landing mid-pass. Removal of a
 * superseded pack or loose object races only with unlocked readers, the same
 * POSIX window `unpublishPack` already accepts — and strictly safer here,
 * since every removed-but-reachable object is also in the freshly published
 * consolidated pack.
 *
 * Returns the disk usage before and after plus the reclaimed byte delta. A
 * repo with no resolvable refs is left untouched. Exported for use within
 * the storage package only — it is intentionally absent from the package's
 * public barrel.
 */
export async function gcUnderLock(
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

/**
 * Garbage-collect the agent repo at `dir`, acquiring the repo's
 * per-directory lock for the duration. Use this from callers that are not
 * already holding the lock (e.g. the hub's substrate, which holds its own
 * higher-level lock but not the storage lock). Writers that trigger reclaim
 * while already under the lock call {@link gcUnderLock} directly.
 */
export async function runGC(
  dir: string,
  opts: { retention: RetentionPolicy },
): Promise<GCResult> {
  return withRepoDirLock(dir, () => gcUnderLock(dir, opts));
}

function warnIfOverBudget(dir: string, bytes: number, warnBytes: number): void {
  if (bytes >= warnBytes) {
    logger.warn`disk pressure on ${dir}: .git is ${String(bytes)} bytes, at or above the ${String(warnBytes)} byte threshold`;
  }
}

/**
 * Apply a write-path reclaim policy to the repo at `dir`. Reclaims when the
 * pack count or the loose-object count has reached its threshold. Intended
 * to be called by a writer that has just mutated the repo and is still
 * holding the per-directory lock, so the reclaim itself runs without
 * re-entering the lock.
 *
 * The trigger samples only the object counts — two directory reads — on
 * every write; the full `.git` byte walk that feeds the disk-pressure
 * warning runs only when a reclaim does (the collector computes it for its
 * before/after delta anyway, and the failure path walks it once). So the
 * warning is evaluated at reclaim time, not on every write, and the common
 * below-threshold write pays no byte walk.
 *
 * A reclaim failure is logged, not propagated: the write that triggered this
 * has already committed, so failing the caller would falsely report the
 * write as failed. The disk-pressure warning still fires on a failed reclaim
 * — the case where accumulation is most likely runaway.
 */
export async function maybeGCUnderLock(
  dir: string,
  policy: GCPolicy,
): Promise<void> {
  const counts = repoObjectCounts(dir);
  if (
    counts.packCount < policy.packThreshold &&
    counts.looseObjectCount < policy.looseThreshold
  ) {
    return;
  }
  try {
    const result = await gcUnderLock(dir, { retention: policy.retention });
    logger.info`reclaimed ${String(result.reclaimedBytes)} bytes from ${dir}: packs ${String(result.before.packCount)} to ${String(result.after.packCount)}, loose ${String(result.before.looseObjectCount)} to ${String(result.after.looseObjectCount)}`;
    warnIfOverBudget(dir, result.after.gitBytes, policy.warnBytes);
  } catch (err) {
    logger.warn`GC of ${dir} failed; the repo is unchanged but unreclaimed — ${err instanceof Error ? err.message : String(err)}`;
    warnIfOverBudget(dir, gitBytes(dir), policy.warnBytes);
  }
}

/**
 * {@link maybeGCUnderLock} for callers that do not already hold the repo's
 * per-directory lock — it acquires the lock for the duration. The hub's
 * substrate uses this from inside its own higher-level lock; sidecar writers
 * that already hold the per-directory lock call `maybeGCUnderLock` directly.
 */
export async function maybeGC(dir: string, policy: GCPolicy): Promise<void> {
  return withRepoDirLock(dir, () => maybeGCUnderLock(dir, policy));
}
