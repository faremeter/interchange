// Per-runId commit serialization and segment-buffered durable writes.
//
// Every event commit against a `RepoStore` goes through the chain
// here so the state machine's strictly-monotonic seq invariant
// survives concurrent writers. The runtime body's primitives (which
// own most commits) and the in-memory scheduler (which is the
// single-writer of `TimerFired` in the runLocal env) share the same
// per-runId chain so a TimerFired commit cannot collide on seq with
// a parallel-step commit from the runtime body.
//
// The map is module-scoped because all callers against the same runId
// share the same lock. The chain holds promise references, not
// resources; entries are dropped via `dropChain` when a run settles
// so long-lived processes do not accumulate dead promise chains.
//
// Segment buffering. The run body emits the per-execution run-event
// bracket (RunStarted, StepStarted, StepCompleted, terminal) as a
// sequence of back-to-back commits with nothing durably needed
// between them for a synchronous segment. `commitBuffered` keeps the
// per-event in-memory state-machine validation (the seq assignment
// and transition check are UNCHANGED) but defers the durable write,
// accumulating events in a per-runId pending buffer. The buffer is
// flushed in ONE `appendBatch` at a segment boundary -- a suspension
// (the run parks on a durable wait the outside world drives) or
// completion (the terminal event). This is a persistence-TIMING
// change only: the state machine sees the identical transition
// sequence; only the durable WRITE is coalesced.
//
// Because the seq the chain assigns and every `reloadState` caller's
// next-seq computation must account for buffered-but-unflushed
// events, `reloadState` folds the durable log together with the
// pending buffer. The buffer holds events this process validated
// since the last flush. A sibling primitive can buffer another event
// after a park flush, so the buffer is not empty just because one
// step has parked. Signal delivery, the host scheduler, and a
// control-plane cancel take `withRunCommitBarrier`, which flushes
// that buffer before their own write. The host scheduler still tails
// the durable `TimerSet`. `commit` (immediate)
// flushes any pending buffer before its own event so a buffered run
// body and an immediate writer never compute a colliding seq.

import {
  applyEvent,
  resumeFromLog,
  type RunState,
  type WorkflowEvent,
} from "../state-machine/index";

import type { RepoStore } from "./env";

const commitChains = new Map<string, Promise<unknown>>();

/**
 * Per-runId pending durable-write buffer. Holds events whose in-memory
 * state-machine transition has been validated but whose durable write
 * has been deferred to the next segment-boundary flush. Module-scoped
 * for the same reason as `commitChains`: all callers against a runId
 * share one buffer, serialized through that runId's chain.
 */
const pendingBuffers = new Map<string, WorkflowEvent[]>();

export type CommitEnv = {
  repoStore: RepoStore;
};

function getBuffer(runId: string): WorkflowEvent[] {
  let buf = pendingBuffers.get(runId);
  if (buf === undefined) {
    buf = [];
    pendingBuffers.set(runId, buf);
  }
  return buf;
}

const bufferWaiters = new Map<string, (() => void)[]>();

function wake(waiters: Map<string, (() => void)[]>, runId: string): void {
  const list = waiters.get(runId);
  if (list === undefined) return;
  waiters.delete(runId);
  for (const resolve of list) resolve();
}

/** Resolves when the next event is pushed into this run's buffer. */
export function whenRunNextBuffers(runId: string): Promise<void> {
  return new Promise((resolve) => {
    const list = bufferWaiters.get(runId) ?? [];
    list.push(resolve);
    bufferWaiters.set(runId, list);
  });
}

/**
 * Reconstruct the run's current state from the durable log folded
 * with any pending (buffered-but-unflushed) events. Used inside the
 * chain for seq assignment and by every `reloadState` caller in the
 * run body so a next-seq computation accounts for events this segment
 * has emitted but not yet flushed.
 */
async function readStateWithPending(
  env: CommitEnv,
  runId: string,
): Promise<RunState> {
  const durable = await env.repoStore.read(runId);
  let state = resumeFromLog(runId, durable);
  const buf = pendingBuffers.get(runId);
  if (buf !== undefined) {
    for (const event of buf) {
      state = applyEvent(state, event);
    }
  }
  return state;
}

/**
 * Flush the pending buffer in ONE durable `appendBatch`. Called under
 * the per-runId chain lock so the flushed seqs are contiguous on the
 * durable tip. No-op when the buffer is empty.
 */
const repoWriteFailure = Symbol("repoWriteFailure");

/**
 * The durable append threw, so this batch is not on disk. Settlement
 * must rethrow it: a substitute terminal event would replace the
 * prefix a re-fire adopts after a crash mid-flush.
 */
export function markRepoWriteFailure(cause: unknown): void {
  if (typeof cause === "object" && cause !== null) {
    Object.defineProperty(cause, repoWriteFailure, { value: true });
  }
}

export function isRepoWriteFailure(cause: unknown): boolean {
  return (
    typeof cause === "object" && cause !== null && repoWriteFailure in cause
  );
}

// A flush that threw dropped its batch: the events are neither durable
// nor still buffered. The writer that flushed them rejects on its own
// promise, and a barrier's finally drops the chain, so a later link
// would otherwise see an empty buffer and succeed. Later links rethrow
// the first cause. `dropChain` forgets it when the run settles.
const failedFlushes = new Map<string, unknown>();
const flushWaiters = new Map<string, (() => void)[]>();

/**
 * The cause is the failed flush recorded for this run. The marker on
 * the error is not enough: a nested run stamps the same marker, and
 * that error rejects into the parent with the marker still set.
 */
export function isRunFlushFailure(runId: string, cause: unknown): boolean {
  const recorded = failedFlushes.get(runId);
  return recorded !== undefined && recorded === cause;
}

/** The failed flush recorded for this run, if this process has seen one. */
export function flushFailureCause(runId: string): unknown {
  return failedFlushes.get(runId);
}

/**
 * Resolves when this run's buffer flush throws. Already resolved when
 * the failure was recorded before the caller started waiting. A parked
 * step never rejects, so the drive loop races this with its step
 * promises.
 */
export function whenRunFlushFails(runId: string): Promise<void> {
  if (failedFlushes.has(runId)) return Promise.resolve();
  return new Promise((resolve) => {
    const list = flushWaiters.get(runId) ?? [];
    list.push(resolve);
    flushWaiters.set(runId, list);
  });
}

function throwIfFlushFailed(runId: string): void {
  const cause = failedFlushes.get(runId);
  if (cause !== undefined) throw cause;
}

function followChain<T>(runId: string, body: () => Promise<T>): Promise<T> {
  const prev = commitChains.get(runId) ?? Promise.resolve();
  const next = (async () => {
    await prev.catch(() => undefined);
    throwIfFlushFailed(runId);
    return body();
  })();
  commitChains.set(runId, next);
  return next;
}

async function flushBuffer(env: CommitEnv, runId: string): Promise<void> {
  const buf = pendingBuffers.get(runId);
  if (buf === undefined || buf.length === 0) return;
  const events = buf.slice();
  buf.length = 0;
  try {
    await env.repoStore.appendBatch(runId, events);
  } catch (cause) {
    markRepoWriteFailure(cause);
    if (!failedFlushes.has(runId)) {
      failedFlushes.set(runId, cause);
      wake(flushWaiters, runId);
    }
    throw cause;
  }
}

/**
 * Serialize an event commit per `runId` and assign its seq under the
 * lock, then DEFER the durable write into the pending buffer. Callers
 * may build the event from their locally-observed state (which may
 * carry a stale `lastSeq` if another commit landed concurrently); the
 * chain reads the canonical state (durable + pending) inside the lock
 * and reassigns the event's seq to `fresh.lastSeq + 1` before
 * buffering. The transition is validated before buffering so a
 * state-machine rejection leaves the buffer clean.
 *
 * The durable write happens at the next `flushChain` / `commit`
 * (segment boundary). Use this for the run body's intra-segment
 * events; use `commit` for events that must persist immediately
 * (the segment-boundary suspension/terminal events flushed via the
 * run body's explicit `flushChain`, the run-local scheduler's
 * `TimerFired`, and the control-plane `cancel`).
 */
export async function commitBuffered(
  env: CommitEnv,
  runId: string,
  event: WorkflowEvent,
): Promise<RunState> {
  return followChain(runId, async () => {
    const fresh = await readStateWithPending(env, runId);
    const adjustedEvent: WorkflowEvent = { ...event, seq: fresh.lastSeq + 1 };
    const nextState = applyEvent(fresh, adjustedEvent);
    getBuffer(runId).push(adjustedEvent);
    wake(bufferWaiters, runId);
    return nextState;
  });
}

/**
 * Serialize an event commit per `runId` and assign its seq under the
 * lock, flushing any pending buffer and this event together in ONE
 * durable `appendBatch`. This is the immediate-durability path: the
 * event is on disk when the returned promise resolves.
 *
 * Flushing the pending buffer first keeps the durable tip contiguous
 * even when a buffering run body and an immediate external writer
 * interleave on the same runId: the chain serializes them, and the
 * immediate writer drains whatever the run body buffered before
 * landing its own event.
 */
export async function commit(
  env: CommitEnv,
  runId: string,
  event: WorkflowEvent,
): Promise<RunState> {
  return followChain(runId, async () => {
    const fresh = await readStateWithPending(env, runId);
    const adjustedEvent: WorkflowEvent = { ...event, seq: fresh.lastSeq + 1 };
    // Validate the transition before appending so a state-machine
    // rejection leaves the log clean. A subsequent commit on the same
    // chain reads back a coherent log instead of a stray event the
    // transition function refuses to replay.
    const nextState = applyEvent(fresh, adjustedEvent);
    getBuffer(runId).push(adjustedEvent);
    await flushBuffer(env, runId);
    return nextState;
  });
}

/**
 * Flush the per-runId pending buffer to durable storage in ONE
 * `appendBatch`, serialized through the chain. Called by the run body
 * at a segment boundary AFTER it has buffered the boundary event
 * (the suspension marker or terminal) so that event is the LAST in
 * the flushed batch and is durable before the run parks or settles.
 * No-op when the buffer is empty.
 */
export async function flushChain(env: CommitEnv, runId: string): Promise<void> {
  await followChain(runId, async () => {
    await flushBuffer(env, runId);
  });
}

/**
 * Queue `body` behind this run's commit chain, then discard the pending
 * buffer before `body` runs. Links already on the chain finish first,
 * including a barrier `write()` that has started. `body` appends through
 * the repo store. `commit`, `commitBuffered`, `flushChain`, and
 * `withRunCommitBarrier` queue another link that waits on this one.
 */
export function runAfterDiscardingBuffer<T>(
  runId: string,
  body: () => Promise<T>,
): Promise<T> {
  return followChain(runId, async () => {
    pendingBuffers.delete(runId);
    return body();
  });
}

/** Flush and exclude runtime commits while an external writer advances the log. */
export function withRunCommitBarrier<T>(
  env: CommitEnv,
  runId: string,
  write: () => Promise<T>,
): Promise<T> {
  const next = followChain(runId, async () => {
    await flushBuffer(env, runId);
    return write();
  });
  return next.finally(() => {
    if (commitChains.get(runId) !== next) return;
    commitChains.delete(runId);
    if (pendingBuffers.get(runId)?.length === 0) pendingBuffers.delete(runId);
  });
}

export async function reloadState(
  env: CommitEnv,
  runId: string,
): Promise<RunState> {
  return readStateWithPending(env, runId);
}

/**
 * Drop the per-runId commit chain entry and pending buffer. Called by
 * the runtime body when a run settles (success, failure, or thrown
 * body) so long-running processes accumulating many workflows do not
 * hold dead promise chains or buffers for runs that crashed during
 * resume seeding or a stall guard. A non-empty buffer at drop time is
 * the crash-mid-segment case: those events were never durable, so
 * discarding them leaves no `runs/<runId>/` partial state -- the
 * recovery substrate (the inbox claim-check) re-drives the message.
 */
export function dropChain(runId: string): void {
  commitChains.delete(runId);
  pendingBuffers.delete(runId);
  failedFlushes.delete(runId);
  flushWaiters.delete(runId);
  bufferWaiters.delete(runId);
}
