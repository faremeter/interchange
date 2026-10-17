// Process-wide registry of held workdir locks.
//
// The agent enforces a runtime singleton-per-workdir invariant: at most one
// in-process agent may own a given workdir at a time. Holding two agents
// against the same workdir simultaneously corrupts both the git state of
// any isogit-backed `ContextStore` rooted there and the audit collector's
// bookkeeping.
//
// This is a best-effort in-process check. It does not coordinate across OS
// processes, and it compares lexically-resolved absolute paths — two paths
// that differ only in `..`, `.` or repeated `/` segments, a trailing `/`, or
// a relative spelling of the same absolute path are normalized to one key.
// The normalization is lexical: a symlink, a hard link or a separately
// mounted bind to the same directory is not detected, and `/link/..` keys as
// `/` even when `/link` points elsewhere. Callers are responsible for
// ensuring `env.workdir` matches the directory backing their `env.storage`
// (see `BaseEnv.workdir` for the documented invariant).
//
// The key is computed without `node:path`, so the agent loads on a host with
// no Node APIs. A relative workdir resolves against `process.cwd()` when the
// host has one; a host without one must pass an absolute workdir.

import { join, normalize } from "@intx/types/posix-path";

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
 * The absolute, normalized form of `workdir`: the lock key. A relative path
 * resolves against `process.cwd()` when the host provides it, matching
 * `path.resolve`, and is refused otherwise.
 */
function absoluteWorkdir(workdir: string): string {
  if (workdir.startsWith("/")) {
    return normalize(workdir);
  }
  const host: { process?: { cwd?: () => string } } = globalThis;
  const cwd = host.process?.cwd?.();
  if (cwd === undefined) {
    throw new Error(
      `workdir ${JSON.stringify(workdir)} is relative, and this host has no working directory to resolve it against; pass an absolute workdir`,
    );
  }
  return join(cwd, workdir);
}

/**
 * Acquire the process-wide lock for `workdir`. Throws
 * `AgentContextLockError` if another agent already holds it. The
 * returned `release` is idempotent.
 */
export function acquireContextDirLock(workdir: string): ContextDirLock {
  const path = absoluteWorkdir(workdir);
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
