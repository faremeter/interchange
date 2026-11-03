import type { StorageRuntime } from "./runtime";

/**
 * Process-wide per-directory serialization for a store's object-store
 * mutations. Keyed by the lexically-resolved absolute path of the repo
 * working directory.
 *
 * On the sidecar a single store is written by several independent
 * drivers — the reactor's context commits and the audit commits — none
 * of which share a higher-level lock. This lock makes the storage layer the
 * single owner of their serialization: every mutator acquires it, so they run
 * one-at-a-time per directory.
 *
 * Single-process only — it serializes operations issued from one process,
 * not across a second process or an external git client touching the same
 * directory.
 *
 * Each entry holds the tail of the chain of in-flight critical sections for
 * that directory; the next acquirer awaits the current tail and replaces it
 * with its own pending completion. The tail-check on release prevents the
 * map from leaking entries once a directory's chain drains.
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
