// Child termination plus the resolve-only deadline / injectable-timer
// primitives its escalation drives. Two supervisor paths need to bound a
// wait on a workflow-process child with a timer and then force the child
// down: the recycle path (SIGTERM -> deadline -> SIGKILL between cohorts)
// and the spawn path's ready-handshake timeout (a child that spawns but
// never emits `ready`). Both need the same `killChildHandle` escalation
// and the same resolve-only `waitDeadline` raced against a child event,
// and both need injectable timers so tests can drive the deadline
// deterministically. Factoring them here keeps one implementation instead
// of a copy per path.

import { getLogger } from "@intx/log";

import type { SubprocessHandle } from "./types";

/**
 * Default kill-timeout between SIGTERM and SIGKILL. Used when a caller
 * supplies no override; the recycle path exposes it as a per-deployment
 * override and the spawn ready-timeout teardown uses it directly.
 */
export const DEFAULT_KILL_TIMEOUT_MS = 5_000;

/**
 * Default deadline for a child's `ready` handshake -- the window a freshly
 * spawned child has to emit `ready` before the supervisor kills it and
 * treats the spawn (or recycle respawn) as failed. Used when a caller
 * supplies no override. Shared here, alongside the kill default, so both
 * the spawn path and the recycle path bound the handshake identically
 * without either re-declaring the constant.
 */
export const DEFAULT_READY_TIMEOUT_MS = 30_000;

/**
 * Injected dependencies for `killChildHandle`. `setTimer`/`clearTimer`
 * default to the real `setTimeout`/`clearTimeout` when omitted so tests
 * can substitute a deterministic timer. `logger` is supplied by the
 * caller so the SIGKILL-escalation warning is attributed to the path that
 * initiated the kill (recycle vs. spawn) rather than a single shared
 * namespace.
 */
export interface KillChildHandleDeps {
  setTimer?: (cb: () => void, ms: number) => unknown;
  clearTimer?: (handle: unknown) => void;
  logger: ReturnType<typeof getLogger>;
}

/**
 * Issue SIGTERM and wait for the child to exit. If the exit does not land
 * within `killTimeoutMs`, escalate to SIGKILL and wait again. SIGKILL is
 * unignorable, so `exited` is guaranteed to settle -- a child that traps
 * or never services SIGTERM cannot wedge this call. The supervisor's
 * spawner returns the `exited` promise; this helper does not consult OS
 * primitives directly.
 */
export async function killChildHandle(
  handle: SubprocessHandle,
  killTimeoutMs: number,
  deps: KillChildHandleDeps,
): Promise<void> {
  const setTimer = deps.setTimer ?? defaultSetTimer;
  const clearTimer = deps.clearTimer ?? defaultClearTimer;

  handle.kill("SIGTERM");
  const sigTermDeadline = waitDeadline(setTimer, killTimeoutMs);
  const exitedFirst = await Promise.race([
    handle.exited.then(() => "exited" as const),
    sigTermDeadline.promise.then(() => "deadline" as const),
  ]);
  if (exitedFirst === "exited") {
    clearTimer(sigTermDeadline.handle);
    return;
  }
  clearTimer(sigTermDeadline.handle);
  deps.logger
    .warn`child termination: SIGTERM did not land within ${String(killTimeoutMs)}ms; escalating to SIGKILL`;
  handle.kill("SIGKILL");
  await handle.exited.catch(() => {
    /* swallowed: a non-zero exit on SIGKILL is the expected outcome;
       termination treats handle exit as success regardless of code. */
  });
}

/**
 * A resolve-only deadline: a promise that resolves after `ms` via the
 * injected `setTimer`, plus the timer handle so the caller can cancel it
 * with the matching `clearTimer` once the race settles. It only ever
 * resolves, so it contributes no rejection of its own to a race.
 */
export function waitDeadline(
  setTimer: (cb: () => void, ms: number) => unknown,
  ms: number,
): { promise: Promise<void>; handle: unknown } {
  let h: unknown;
  const promise = new Promise<void>((resolve) => {
    h = setTimer(() => resolve(), ms);
  });
  return { promise, handle: h };
}

export function defaultSetTimer(cb: () => void, ms: number): unknown {
  return setTimeout(cb, ms);
}

export function defaultClearTimer(handle: unknown): void {
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- production wiring; the handle is the value `setTimeout` returned, narrowed back at the boundary
  clearTimeout(handle as ReturnType<typeof setTimeout>);
}
