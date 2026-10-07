// Process-wide registry of held workdir locks, enforcing the runtime
// singleton-per-workdir invariant: at most one in-process agent may own
// a given workdir at a time. Two agents against the same workdir
// corrupt both the git state of any isogit-backed `ContextStore` rooted
// there and the audit collector's bookkeeping.
//
// Best-effort in-process check: it does not coordinate across OS
// processes, and it compares `path.resolve`d absolute paths, so a hard
// link or a separate bind to the same inode is not detected.

import { resolve } from "node:path";

const heldLocks = new Set<string>();

export class AgentContextLockError extends Error {
  readonly workdir: string;

  constructor(workdir: string) {
    super(`an agent is already open for workdir: ${workdir}`);
    this.name = "AgentContextLockError";
    this.workdir = workdir;
  }
}

export type ContextDirLock = {
  /** Absolute, resolved path of the locked directory. */
  readonly path: string;
  /** Release the lock. Idempotent. */
  release(): void;
};

/**
 * Acquire the process-wide lock for `workdir`. Throws
 * `AgentContextLockError` if another agent already holds it. The
 * returned `release` is idempotent.
 */
export function acquireContextDirLock(workdir: string): ContextDirLock {
  const path = resolve(workdir);
  if (heldLocks.has(path)) {
    throw new AgentContextLockError(path);
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
