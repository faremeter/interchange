import type { StorageRuntime } from "./runtime";

/**
 * Process-wide per-directory serialization for agent-repo object-store
 * mutations, keyed by the lexically-resolved absolute path of the repo
 * working directory.
 *
 * On the sidecar a single agent repo is written by several independent
 * drivers — the reactor's context commits, the mail-audit commits, deploy
 * pack applies — and read by the state-pack producer, none of which share
 * a higher-level lock. GC prunes loose objects and packs, so it cannot
 * run concurrently with any of them without risking the deletion of an
 * object a writer just produced. Every mutator and the collector acquire
 * this lock, so they run one-at-a-time per directory and GC observes a
 * quiescent object store.
 *
 * Single-process only — it does not serialize across a second process or
 * an external git client. The hub's higher-level `withRepoLock` already
 * serializes its own write paths; this lock nests underneath it (the hub
 * never holds this lock while acquiring its own, so acquisition is always
 * outer-to-inner and cannot deadlock).
 *
 * Each entry holds the tail of the in-flight critical-section chain for
 * that directory; the next acquirer awaits the current tail and replaces
 * it with its own pending completion. The tail-check on release prevents
 * the map from leaking entries once a directory's chain drains.
 */
const locks = new Map<string, Promise<void>>();

export async function withRepoDirLock<T>(
  runtime: StorageRuntime,
  dir: string,
  fn: () => Promise<T>,
): Promise<T> {
  const key = runtime.path.resolve(dir);
  const previous = locks.get(key) ?? Promise.resolve();
  let releaseFn: () => void = () => undefined;
  const tail = new Promise<void>((res) => {
    releaseFn = res;
  });
  locks.set(key, tail);
  try {
    await previous;
    return await fn();
  } finally {
    if (locks.get(key) === tail) {
      locks.delete(key);
    }
    releaseFn();
  }
}
