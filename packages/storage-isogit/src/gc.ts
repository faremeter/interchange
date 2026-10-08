import git from "isomorphic-git";
import { getLogger } from "@intx/log";
import { hasCode } from "@intx/types";
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
import { flushRuntime, type StorageRuntime } from "./runtime";

const logger = getLogger(["interchange", "storage-isogit", "gc"]);
const GC_TRASH_DIR = "gc-trash";

type ObjectStoreEntry = {
  name: string;
  filepath: string;
};

/**
 * How much commit history a GC pass preserves.
 *
 * - `tip-only`: keep only the objects reachable from each ref tip's tree.
 *   Prior commits are dropped, leaving the tip commit with dangling parent
 *   pointers (the shape the deploy ref already has). Smallest repo.
 * - `keep-history`: also keep every object reachable through the commit
 *   ancestry. Preserves the audit trail.
 */
export type RetentionPolicy = "tip-only" | "keep-history";

export type GCResult = {
  before: RepoDiskUsage;
  after: RepoDiskUsage;
  reclaimedBytes: number;
  keptObjects: number;
};

/**
 * Write-path reclaim policy. A writer holding the per-directory lock
 * repacks under `retention` once the pack count reaches `packThreshold`
 * or the loose-object count reaches `looseThreshold` (a hub repo
 * accumulates packs, a sidecar repo accumulates loose objects). A reclaim
 * also checks `.git` bytes against `warnBytes` and emits a disk-pressure
 * warning, so the byte walk rides the reclaim rather than every write.
 */
export type GCPolicy = {
  packThreshold: number;
  looseThreshold: number;
  warnBytes: number;
  retention: RetentionPolicy;
};

/**
 * Collect every object reachable through a commit's ancestry, tolerating
 * commits whose parents are not present on disk. The deploy ref is applied
 * tip-only, so its commit carries parent pointers to objects never
 * transferred; reading an absent parent throws `NotFoundError`, and we stop
 * descending that branch rather than aborting the whole GC. Only that
 * absence is tolerated — any other read failure surfaces, since swallowing
 * it would drop a present, reachable subtree from the keep set.
 */
async function collectHistoryObjects(
  runtime: StorageRuntime,
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
      const { commit } = await git.readCommit({
        fs: runtime.fs.git,
        dir,
        oid: commitOid,
      });
      parents = commit.parent;
    } catch (err) {
      if (err instanceof Error && "code" in err && err.code === "NotFoundError")
        continue;
      throw err;
    }

    for (const oid of await collectReachableObjects(runtime, dir, commitOid)) {
      objects.add(oid);
    }
    for (const parent of parents) {
      if (!seenCommits.has(parent)) queue.push(parent);
    }
  }

  return objects;
}

/**
 * Every `.pack` and `.idx` file currently in the repo's pack directory,
 * snapshotted before the consolidated pack is published so the freshly
 * published pair is never in the retirement set.
 */
async function listPackFiles(
  runtime: StorageRuntime,
  dir: string,
): Promise<ObjectStoreEntry[]> {
  const packDir = runtime.path.join(dir, ".git", "objects", "pack");
  let entries: string[];
  try {
    entries = await runtime.fs.readdir(packDir);
  } catch (cause) {
    if (hasCode(cause) && cause.code === "ENOENT") {
      return [];
    }
    throw cause;
  }
  return entries
    .filter((name) => name.endsWith(".pack") || name.endsWith(".idx"))
    .map((name) => ({
      name,
      filepath: runtime.path.join(packDir, name),
    }));
}

/**
 * Snapshot every loose-object fan-out directory under `.git/objects/`.
 * Non-object children such as `pack`, `info`, and `gc-trash` are excluded
 * by the two-lowercase-hex directory-name constraint.
 */
async function listLooseObjectDirectories(
  runtime: StorageRuntime,
  dir: string,
): Promise<ObjectStoreEntry[]> {
  const objectsDir = runtime.path.join(dir, ".git", "objects");
  let entries: string[];
  try {
    entries = await runtime.fs.readdir(objectsDir);
  } catch (cause) {
    if (hasCode(cause) && cause.code === "ENOENT") {
      return [];
    }
    throw cause;
  }
  return entries
    .filter((name) => /^[0-9a-f]{2}$/.test(name))
    .map((name) => ({
      name,
      filepath: runtime.path.join(objectsDir, name),
    }));
}

function gcTrashRoot(runtime: StorageRuntime, dir: string): string {
  return runtime.path.join(dir, ".git", "objects", GC_TRASH_DIR);
}

/**
 * Delete quarantine left by a prior GC that reached its durability commit
 * point but did not finish cleanup. Quarantine is outside every path Git
 * scans for packs or loose objects. If it is present in a durable
 * namespace, the replacement pack was part of that same flushed namespace;
 * a crash before the flush restores the prior namespace without
 * quarantine instead.
 */
async function removeStaleGCTrash(
  runtime: StorageRuntime,
  dir: string,
): Promise<boolean> {
  const trashRoot = gcTrashRoot(runtime, dir);
  let entries: string[];
  try {
    entries = await runtime.fs.readdir(trashRoot);
  } catch (cause) {
    if (hasCode(cause) && cause.code === "ENOENT") return false;
    throw cause;
  }
  if (entries.length === 0) return false;

  await runtime.fs.remove(trashRoot, { recursive: true, force: true });
  return true;
}

/**
 * Retire the old object-store namespace without deleting object bodies.
 * LightningFS persists file bodies eagerly but its directory namespace
 * only at flush time. Renaming into an unscanned quarantine therefore
 * leaves both crash outcomes valid: a pre-flush reopen sees the old
 * namespace and a post-flush reopen sees the consolidated pack plus
 * quarantined old bodies.
 */
async function quarantineSupersededObjects(
  runtime: StorageRuntime,
  dir: string,
  transferId: string,
  supersededPacks: readonly ObjectStoreEntry[],
  looseObjectDirectories: readonly ObjectStoreEntry[],
): Promise<string> {
  const trashDir = runtime.path.join(gcTrashRoot(runtime, dir), transferId);
  const packTrashDir = runtime.path.join(trashDir, "pack");
  const looseTrashDir = runtime.path.join(trashDir, "loose");
  await runtime.fs.mkdir(packTrashDir, { recursive: true });
  await runtime.fs.mkdir(looseTrashDir, { recursive: true });

  const orderedPacks = [...supersededPacks].sort((left, right) => {
    const leftRank = left.name.endsWith(".idx") ? 0 : 1;
    const rightRank = right.name.endsWith(".idx") ? 0 : 1;
    if (leftRank !== rightRank) return leftRank - rightRank;
    if (left.name < right.name) return -1;
    if (left.name > right.name) return 1;
    return 0;
  });
  for (const entry of orderedPacks) {
    await runtime.fs.rename(
      entry.filepath,
      runtime.path.join(packTrashDir, entry.name),
    );
  }
  for (const entry of looseObjectDirectories) {
    await runtime.fs.rename(
      entry.filepath,
      runtime.path.join(looseTrashDir, entry.name),
    );
  }

  return trashDir;
}

/**
 * Repack everything reachable from the repo's refs into one pack and
 * drop the superseded packs and loose objects. The keep set is the union
 * of reachability over every head ref (agent repos carry two diverging
 * heads, `main` and `deploy`; repacking one alone would discard the
 * other's live objects). The consolidated pack is published through the
 * same atomic staging dance receives use, so an unlocked reader never
 * observes a torn pack; only then are the prior packs and loose-object
 * fan-out dirs renamed into an unscanned quarantine. A flush makes the
 * new pack and retired namespace durable together; object bodies are
 * deleted only after that commit point, followed by a second cleanup
 * flush.
 *
 * Caller MUST already hold the per-directory lock (`withRepoDirLock`);
 * external callers use {@link runGC}. Retirement races only with
 * unlocked readers, the same window `unpublishPack` already accepts;
 * indexes are retired before their packs, and every retired-but-reachable
 * object is also in the fresh consolidated pack. Returns disk usage
 * before/after plus the reclaimed delta. A repo with no resolvable refs
 * is not repacked, though stale quarantine is still reclaimed.
 */
export async function gcUnderLock(
  runtime: StorageRuntime,
  dir: string,
  opts: { retention: RetentionPolicy },
): Promise<GCResult> {
  if (await removeStaleGCTrash(runtime, dir)) {
    await flushRuntime(runtime);
  }

  const before = await repoDiskUsage(runtime, dir);
  const refs = await listRepoRefs(runtime, dir);

  if (refs.length === 0) {
    return { before, after: before, reclaimedBytes: 0, keptObjects: 0 };
  }

  const keep = new Set<string>();
  for (const { oid } of refs) {
    const reachable =
      opts.retention === "tip-only"
        ? await collectReachableObjects(runtime, dir, oid)
        : await collectHistoryObjects(runtime, dir, oid);
    for (const objectOid of reachable) keep.add(objectOid);
  }

  const supersededPacks = await listPackFiles(runtime, dir);
  const looseObjectDirectories = await listLooseObjectDirectories(runtime, dir);

  const result = await git.packObjects({
    fs: runtime.fs.git,
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
  await publishPackAtomically(runtime, dir, result.packfile, transferId);

  const trashDir = await quarantineSupersededObjects(
    runtime,
    dir,
    transferId,
    supersededPacks,
    looseObjectDirectories,
  );

  // Durability commit point: deletion cannot begin until the replacement
  // pack is published and every retired body is reachable through
  // quarantine.
  await flushRuntime(runtime);
  await runtime.fs.remove(trashDir, { recursive: true, force: true });
  await flushRuntime(runtime);

  const after = await repoDiskUsage(runtime, dir);
  return {
    before,
    after,
    reclaimedBytes: before.gitBytes - after.gitBytes,
    keptObjects: keep.size,
  };
}

/**
 * Garbage-collect the agent repo at `dir`, acquiring the repo's
 * per-directory lock for the duration. Use this from callers not already
 * holding the lock (e.g. the hub's substrate, which holds its own
 * higher-level lock but not the storage lock); writers that trigger
 * reclaim while already under the lock call {@link gcUnderLock} directly.
 */
export async function runGC(
  runtime: StorageRuntime,
  dir: string,
  opts: { retention: RetentionPolicy },
): Promise<GCResult> {
  return withRepoDirLock(runtime, dir, () => gcUnderLock(runtime, dir, opts));
}

function warnIfOverBudget(dir: string, bytes: number, warnBytes: number): void {
  if (bytes >= warnBytes) {
    logger.warn`disk pressure on ${dir}: .git is ${String(bytes)} bytes, at or above the ${String(warnBytes)} byte threshold`;
  }
}

/**
 * Apply a write-path reclaim policy to the repo at `dir`, for a writer
 * that has just mutated the repo and still holds the per-directory lock.
 *
 * The trigger samples only the object counts — two directory reads — on
 * every write; the full `.git` byte walk for the disk-pressure warning
 * runs only when a reclaim does, so below-threshold writes pay no byte
 * walk. A reclaim failure is logged, not propagated: the write that
 * triggered this has already committed, so failing the caller would
 * falsely report the write as failed. The disk-pressure warning still
 * fires on a failed reclaim — the case where accumulation is most
 * likely runaway.
 */
export async function maybeGCUnderLock(
  runtime: StorageRuntime,
  dir: string,
  policy: GCPolicy,
): Promise<void> {
  const counts = await repoObjectCounts(runtime, dir);
  if (
    counts.packCount < policy.packThreshold &&
    counts.looseObjectCount < policy.looseThreshold
  ) {
    return;
  }
  try {
    const result = await gcUnderLock(runtime, dir, {
      retention: policy.retention,
    });
    logger.info`reclaimed ${String(result.reclaimedBytes)} bytes from ${dir}: packs ${String(result.before.packCount)} to ${String(result.after.packCount)}, loose ${String(result.before.looseObjectCount)} to ${String(result.after.looseObjectCount)}`;
    warnIfOverBudget(dir, result.after.gitBytes, policy.warnBytes);
  } catch (err) {
    logger.warn`GC of ${dir} failed; active refs remain intact, but garbage may remain quarantined — ${err instanceof Error ? err.message : String(err)}`;
    warnIfOverBudget(dir, await gitBytes(runtime, dir), policy.warnBytes);
  }
}

/**
 * {@link maybeGCUnderLock} for callers that do not already hold the repo's
 * per-directory lock — it acquires the lock for the duration. The hub's
 * substrate uses this from inside its own higher-level lock; sidecar
 * writers that already hold the per-directory lock call
 * `maybeGCUnderLock` directly.
 */
export async function maybeGC(
  runtime: StorageRuntime,
  dir: string,
  policy: GCPolicy,
): Promise<void> {
  return withRepoDirLock(runtime, dir, () =>
    maybeGCUnderLock(runtime, dir, policy),
  );
}
