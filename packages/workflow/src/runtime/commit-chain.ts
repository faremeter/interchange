// Per-runId commit serialization and segment-buffered durable writes.
//
// Every event commit against a `RepoStore` goes through a per-runId
// chain here so the strictly-monotonic seq invariant survives
// concurrent writers: the runtime body's primitives and the in-memory
// scheduler (single-writer of `TimerFired` in runLocal) share the same
// chain, so a TimerFired commit cannot collide on seq with a
// parallel-step commit. The map is module-scoped; entries are dropped
// via `dropChain` when a run settles so long-lived processes do not
// accumulate dead promise chains.
//
// Segment buffering. `commitBuffered` keeps the per-event in-memory
// state-machine validation but defers the durable write into a
// per-runId pending buffer, flushed in ONE `appendBatch` at a segment
// boundary -- a suspension or completion. Persistence-TIMING only: the
// state machine sees the identical transition sequence; only the
// durable write is coalesced. `reloadState` folds the pending buffer
// into the durable log for seq assignment, and `commit` (immediate)
// flushes any pending buffer before its own event so a buffered run
// body and an external immediate writer never compute a colliding seq.

import {
  applyEvent,
  resumeFromLog,
  type RunState,
  type WorkflowEvent,
} from "../state-machine/index";

import type { RepoStore } from "./env";

const commitChains = new Map<string, Promise<unknown>>();

/**
 * Per-runId pending durable-write buffer: events whose in-memory
 * transition is validated but whose durable write is deferred to the
 * next segment-boundary flush. Module-scoped like `commitChains`.
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

/**
 * Reconstruct the run's state from the durable log folded with any
 * pending (buffered-but-unflushed) events.
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
 * Flush the pending buffer in one durable `appendBatch`, under the
 * per-runId chain lock so the flushed seqs are contiguous. No-op when
 * empty.
 */
async function flushBuffer(env: CommitEnv, runId: string): Promise<void> {
  const buf = pendingBuffers.get(runId);
  if (buf === undefined || buf.length === 0) return;
  const events = buf.slice();
  buf.length = 0;
  await env.repoStore.appendBatch(runId, events);
}

/**
 * Serialize an event commit per `runId` and assign its seq under the
 * lock, then DEFER the durable write into the pending buffer. The
 * chain reads the canonical state (durable + pending) inside the lock
 * and reassigns the event's seq to `fresh.lastSeq + 1`; the transition
 * is validated before buffering so a rejection leaves the buffer
 * clean. Use for intra-segment events; use `commit` for events that
 * must persist immediately.
 */
export async function commitBuffered(
  env: CommitEnv,
  runId: string,
  event: WorkflowEvent,
): Promise<RunState> {
  const prev = commitChains.get(runId) ?? Promise.resolve();
  const next = (async (): Promise<RunState> => {
    await prev.catch(() => undefined);
    const fresh = await readStateWithPending(env, runId);
    const adjustedEvent: WorkflowEvent = { ...event, seq: fresh.lastSeq + 1 };
    const nextState = applyEvent(fresh, adjustedEvent);
    getBuffer(runId).push(adjustedEvent);
    return nextState;
  })();
  commitChains.set(runId, next);
  return next;
}

/**
 * Serialize an event commit per `runId` and assign its seq under the
 * lock, flushing any pending buffer and this event together in one
 * durable `appendBatch`. Immediate-durability path: the event is on
 * disk when the returned promise resolves. Draining the pending buffer
 * first keeps the durable tip contiguous when a buffering run body and
 * an immediate external writer interleave.
 */
export async function commit(
  env: CommitEnv,
  runId: string,
  event: WorkflowEvent,
): Promise<RunState> {
  const prev = commitChains.get(runId) ?? Promise.resolve();
  const next = (async (): Promise<RunState> => {
    await prev.catch(() => undefined);
    const fresh = await readStateWithPending(env, runId);
    const adjustedEvent: WorkflowEvent = { ...event, seq: fresh.lastSeq + 1 };
    // Validate before appending so a state-machine rejection leaves
    // the log clean.
    const nextState = applyEvent(fresh, adjustedEvent);
    getBuffer(runId).push(adjustedEvent);
    await flushBuffer(env, runId);
    return nextState;
  })();
  commitChains.set(runId, next);
  return next;
}

/**
 * Flush the per-runId pending buffer in one `appendBatch`, serialized
 * through the chain. Called by the run body at a segment boundary so
 * the boundary event is durable before the run parks or settles.
 */
export async function flushChain(env: CommitEnv, runId: string): Promise<void> {
  const prev = commitChains.get(runId) ?? Promise.resolve();
  const next = (async (): Promise<void> => {
    await prev.catch(() => undefined);
    await flushBuffer(env, runId);
  })();
  commitChains.set(runId, next);
  await next;
}

/** Flush and exclude runtime commits while an external writer advances the log. */
export function withRunCommitBarrier<T>(
  env: CommitEnv,
  runId: string,
  write: () => Promise<T>,
): Promise<T> {
  const prev = commitChains.get(runId) ?? Promise.resolve();
  const next = (async () => {
    await prev.catch(() => undefined);
    await flushBuffer(env, runId);
    return write();
  })();
  commitChains.set(runId, next);
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
 * Drop the per-runId chain entry and pending buffer when a run
 * settles, so long-running processes do not hold dead chains. A
 * non-empty buffer at drop time is the crash-mid-segment case: those
 * events were never durable, so discarding them leaves no partial
 * state -- the recovery substrate re-drives the message.
 */
export function dropChain(runId: string): void {
  commitChains.delete(runId);
  pendingBuffers.delete(runId);
}
