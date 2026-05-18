// Process-wide registry of held context-directory locks.
//
// The agent enforces a runtime singleton-per-contextDir invariant: at most one
// in-process agent may own a given context directory at a time. Holding two
// agents against the same directory simultaneously corrupts both the git
// state and the audit collector's bookkeeping.
//
// This is a best-effort in-process check. It does not coordinate across OS
// processes, and it compares lexically-resolved absolute paths — two paths
// that point to the same directory through symlinks or `..`/`/./` segments
// are normalized by `path.resolve`, but a hard link or a separately mounted
// bind to the same inode will not be detected. Callers passing their own
// `contextStore` (rather than a `contextDir` string) bypass the lock; they
// are responsible for their store's lifetime.

import { resolve } from "node:path";

const heldLocks = new Set<string>();

export class AgentInUseError extends Error {
  readonly contextDir: string;

  constructor(contextDir: string) {
    super(`an agent is already open for context directory: ${contextDir}`);
    this.name = "AgentInUseError";
    this.contextDir = contextDir;
  }
}

export type ContextDirLock = {
  /** Absolute, resolved path of the locked directory. */
  readonly path: string;
  /** Release the lock. Idempotent. */
  release(): void;
};

/**
 * Acquire the process-wide lock for `contextDir`. Throws `AgentInUseError`
 * if another agent already holds it. The returned `release` is idempotent.
 */
export function acquireContextDirLock(contextDir: string): ContextDirLock {
  const path = resolve(contextDir);
  if (heldLocks.has(path)) {
    throw new AgentInUseError(path);
  }
  heldLocks.add(path);

  let released = false;
  return {
    path,
    release() {
      if (released) return;
      released = true;
      heldLocks.delete(path);
    },
  };
}
