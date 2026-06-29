import { resolve } from "node:path";

/**
 * Process-wide per-directory serialization for agent-repo object-store
 * mutations. Keyed by the lexically-resolved absolute path of the repo
 * working directory.
 *
 * On the sidecar a single agent repo is written by several independent
 * drivers — the reactor's context commits, the mail-audit commits, deploy
 * pack applies — and read by the state-pack producer, none of which share a
 * higher-level lock. Garbage collection prunes loose objects and packs, so
 * it cannot run concurrently with any of them without risking the deletion
 * of an object a writer just produced. This lock makes the storage layer
 * the single owner of that constraint: every mutator and the collector
 * acquire it, so they run one-at-a-time per directory and GC observes a
 * quiescent object store.
 *
 * Single-process only — it serializes operations issued from one process,
 * not across a second process or an external git client touching the same
 * directory. The hub's higher-level `withRepoLock` already serializes its
 * own write paths; this lock nests harmlessly underneath it (the hub never
 * holds this lock while acquiring its own, so the acquisition order is
 * always outer-to-inner and cannot deadlock).
 *
 * Each entry holds the tail of the chain of in-flight critical sections for
 * that directory; the next acquirer awaits the current tail and replaces it
 * with its own pending completion. The tail-check on release prevents the
 * map from leaking entries once a directory's chain drains.
 */
const locks = new Map<string, Promise<void>>();

export async function withRepoDirLock<T>(
  dir: string,
  fn: () => Promise<T>,
): Promise<T> {
  const key = resolve(dir);
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
