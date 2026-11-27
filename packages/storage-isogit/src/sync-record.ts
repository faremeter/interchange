import {
  flushRuntime,
  syncPath,
  syncPaths,
  syncTree,
  type StorageRuntime,
} from "./runtime";

const FAN_OUT_DIR = /^[0-9a-f]{2}$/;

// Per runtime and repository, what this process has made durable. A write
// here, or in a process that stopped, can leave an object or a ref in
// place whose directory entry is unsynced, and nothing may answer from
// that ref, or move a ref to that object, while a crash could still lose
// it. A repository with no record is synced whole by its next
// `syncObjectsAndRefs`: this process has not synced it yet, or a failed
// object write or sync, a rebuild or the cap dropped its record. The cap
// drops the record used least recently that no caller holds, which costs
// only a repeat of its syncs.
type SyncRecord = {
  // The fan-out directories under `objects/` that an object was renamed
  // into since their last sync, such as by a writer dropped before
  // `finish` because its commit failed validation; null until a sync
  // lists the object directories.
  unsyncedFanouts: Set<string> | null;
  // Whether every file and directory under `refs/` is durable; false
  // again after a failed ref write.
  refsSynced: boolean;
};
export const SYNC_RECORDS_MAX = 4096;
const syncRecords = new WeakMap<StorageRuntime, Map<string, SyncRecord>>();
const recordHolds = new WeakMap<StorageRuntime, Map<string, number>>();

function recordsOf(runtime: StorageRuntime): Map<string, SyncRecord> {
  let records = syncRecords.get(runtime);
  if (records === undefined) {
    records = new Map();
    syncRecords.set(runtime, records);
  }
  return records;
}

// The repository's record, made the one used most recently.
function touchRecord(
  runtime: StorageRuntime,
  resolved: string,
): SyncRecord | undefined {
  const records = recordsOf(runtime);
  const record = records.get(resolved);
  if (record !== undefined) {
    records.delete(resolved);
    records.set(resolved, record);
  }
  return record;
}

function addRecord(
  runtime: StorageRuntime,
  resolved: string,
  record: SyncRecord,
): void {
  const records = recordsOf(runtime);
  // Holds can have kept the map past the cap, so this evicts down to it.
  const held = recordHolds.get(runtime);
  for (const key of records.keys()) {
    if (records.size < SYNC_RECORDS_MAX) break;
    if (held?.has(key) !== true) records.delete(key);
  }
  records.set(resolved, record);
}

/**
 * Keep the repository's sync record past the cap until the returned
 * release is called, as a receive does from its prepare to its commit so
 * that the commit, which can run under a database lock, does not repeat
 * the syncs. A failed write or sync, or a rebuild, still clears what it
 * covers.
 */
export function holdSyncRecord(
  runtime: StorageRuntime,
  dir: string,
): () => void {
  const resolved = runtime.path.resolve(dir);
  let holds = recordHolds.get(runtime);
  if (holds === undefined) {
    holds = new Map();
    recordHolds.set(runtime, holds);
  }
  const counts = holds;
  counts.set(resolved, (counts.get(resolved) ?? 0) + 1);
  let released = false;
  return () => {
    if (released) return;
    released = true;
    const left = (counts.get(resolved) ?? 1) - 1;
    if (left > 0) counts.set(resolved, left);
    else counts.delete(resolved);
  };
}

export function forgetSyncRecord(runtime: StorageRuntime, dir: string): void {
  syncRecords.get(runtime)?.delete(runtime.path.resolve(dir));
}

/** Record that a ref write may have left a ref in place unsynced. */
export function forgetRefsSynced(runtime: StorageRuntime, dir: string): void {
  const record = syncRecords.get(runtime)?.get(runtime.path.resolve(dir));
  if (record !== undefined) record.refsSynced = false;
}

/** Record that an object is about to be renamed into `fanout`. */
export function noteFanoutWrite(
  runtime: StorageRuntime,
  dir: string,
  fanout: string,
): void {
  syncRecords
    .get(runtime)
    ?.get(runtime.path.resolve(dir))
    ?.unsyncedFanouts?.add(fanout);
}

/**
 * Make every object and ref the repository holds durable where it is, with
 * the fan-out directories in `fanouts` besides. The first time in a
 * process, and after a failed object write or sync, it syncs
 * `objects/pack/` and every fan-out directory; else only the fan-out
 * directories renamed into since their last sync. Then `objects/`, and,
 * the first time and after a failed ref write, every file and directory
 * under `refs/`. With nothing to sync it does no I/O. The caller
 * serializes it with the repository's writes. It syncs directory entries
 * and ref files, not object files, so a repository that a store which did
 * not sync wrote needs `syncRepositoryDurably` once first.
 */
export async function syncObjectsAndRefs(
  runtime: StorageRuntime,
  dir: string,
  fanouts: readonly string[] = [],
): Promise<void> {
  const resolved = runtime.path.resolve(dir);
  const gitdir = runtime.path.join(dir, ".git");
  const objectsDir = runtime.path.join(gitdir, "objects");
  const record = touchRecord(runtime, resolved);
  const listed = record?.unsyncedFanouts ?? null;
  const refsSynced = record?.refsSynced ?? false;
  let pending: string[];
  try {
    pending =
      listed === null
        ? (await runtime.fs.readdir(objectsDir))
            .filter((name) => name === "pack" || FAN_OUT_DIR.test(name))
            .map((name) => runtime.path.join(objectsDir, name))
        : [...listed];
    const paths = [...new Set([...pending, ...fanouts])];
    if (listed !== null && paths.length === 0 && refsSynced) return;
    if (listed === null || paths.length > 0) {
      await syncPaths(runtime, paths);
      await syncPath(runtime, objectsDir);
    }
    // After the objects, as a ref names them.
    if (!refsSynced) await syncTree(runtime, runtime.path.join(gitdir, "refs"));
    await flushRuntime(runtime);
  } catch (cause) {
    // What the record names may be gone, so the next sync lists them again.
    forgetSyncRecord(runtime, dir);
    throw cause;
  }
  const current = recordsOf(runtime).get(resolved);
  if (current === undefined) {
    addRecord(runtime, resolved, {
      unsyncedFanouts: new Set(),
      refsSynced: true,
    });
    return;
  }
  if (current.unsyncedFanouts === null) current.unsyncedFanouts = new Set();
  else for (const synced of pending) current.unsyncedFanouts.delete(synced);
  current.refsSynced = true;
}

/**
 * Make every ref the repository holds durable where it is: the first time
 * in a process and after a failed ref write, every file and directory
 * under `refs/`; else nothing. The caller serializes it with the
 * repository's writes.
 */
export async function syncRefs(
  runtime: StorageRuntime,
  dir: string,
): Promise<void> {
  const resolved = runtime.path.resolve(dir);
  if (touchRecord(runtime, resolved)?.refsSynced === true) return;
  await syncTree(runtime, runtime.path.join(dir, ".git", "refs"));
  await flushRuntime(runtime);
  const current = recordsOf(runtime).get(resolved);
  if (current === undefined) {
    addRecord(runtime, resolved, { unsyncedFanouts: null, refsSynced: true });
    return;
  }
  current.refsSynced = true;
}
