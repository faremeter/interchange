// Per-runId commit serialization.
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

import {
  applyEvent,
  resumeFromLog,
  type WorkflowEvent,
} from "../state-machine/index";

import type { RepoStore } from "./env";

const commitChains = new Map<string, Promise<unknown>>();

export type CommitEnv = {
  repoStore: RepoStore;
};

/**
 * Serialize an event commit per `runId` and assign its seq under the
 * lock. Callers may build the event from their locally-observed
 * state (which may carry a stale `lastSeq` if another commit landed
 * concurrently); the chain reads the canonical state from the log
 * inside the lock and reassigns the event's seq to
 * `fresh.lastSeq + 1` before appending.
 */
export async function commit(
  env: CommitEnv,
  runId: string,
  event: WorkflowEvent,
): Promise<ReturnType<typeof resumeFromLog>> {
  const prev = commitChains.get(runId) ?? Promise.resolve();
  const next = (async (): Promise<ReturnType<typeof resumeFromLog>> => {
    await prev.catch(() => undefined);
    const fresh = await reloadState(env, runId);
    const adjustedEvent: WorkflowEvent = { ...event, seq: fresh.lastSeq + 1 };
    await env.repoStore.append(runId, adjustedEvent);
    return applyEvent(fresh, adjustedEvent);
  })();
  commitChains.set(runId, next);
  return next;
}

export async function reloadState(
  env: CommitEnv,
  runId: string,
): Promise<ReturnType<typeof resumeFromLog>> {
  const events = await env.repoStore.read(runId);
  return resumeFromLog(runId, events);
}

/**
 * Drop the per-runId commit chain entry. Called by the runtime body
 * when a run settles (success, failure, or thrown body) so
 * long-running processes accumulating many workflows do not hold
 * dead promise chains for runs that crashed during resume seeding or
 * a stall guard.
 */
export function dropChain(runId: string): void {
  commitChains.delete(runId);
}
