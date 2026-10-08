// Helpers for exercising the windows between a durable commit and the abort
// bridge that follows it. Each window opens on an `await`, so a test has to
// land its abort precisely inside one; these helpers trigger the abort from
// the durable write itself instead of from a timer, making the placement
// deterministic.

import { createDefaultDirectorRegistry } from "@intx/agent";

import type { WorkflowDefinition } from "../definition/index";
import { createInMemoryBlobSubstrate } from "../runlocal/blob-substrate";
import { createInMemoryRepoStore } from "../runlocal/repo-store";
import { createInMemoryScheduler } from "../runlocal/scheduler";
import { createInMemorySignalChannel } from "../runlocal/signal-channel";
import { createNoopDrainController } from "./drain";
import type { RepoStore, WorkflowRuntimeEnv } from "./env";
import type { WorkflowEvent } from "../state-machine/index";

/**
 * A repo store that aborts `teardown` as soon as an event of `kind` has been
 * durably written, placing the abort inside the window between that write and
 * whatever bridge the runtime builds next.
 *
 * Both write methods are wrapped: hooking only the batch form would leave a
 * test targeting a single-event write silently unarmed, passing without ever
 * placing its abort.
 */
export function abortOnDurableEvent(
  teardown: AbortController,
  kind: WorkflowEvent["kind"],
): RepoStore {
  const inner = createInMemoryRepoStore();
  const fireIfCarried = (events: readonly WorkflowEvent[]): void => {
    if (events.some((e) => e.kind === kind)) teardown.abort();
  };
  return {
    ...inner,
    append: async (runId, event) => {
      const result = await inner.append(runId, event);
      fireIfCarried([event]);
      return result;
    },
    appendBatch: async (runId, events) => {
      const result = await inner.appendBatch(runId, events);
      fireIfCarried(events);
      return result;
    },
  };
}

/**
 * Resolve `"settled"` when `promise` finishes either way, or `"pending"` once
 * `ms` elapses; a wedged run is indistinguishable from a slow one, so a bound
 * is the only way to assert the difference.
 */
export async function settlesWithin(
  promise: Promise<unknown>,
  ms: number,
): Promise<"settled" | "pending"> {
  const pending = Symbol("pending");
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    const outcome = await Promise.race([
      promise.then(
        () => "settled" as const,
        () => "settled" as const,
      ),
      new Promise<typeof pending>((resolve) => {
        timer = setTimeout(() => {
          resolve(pending);
        }, ms);
      }),
    ]);
    return outcome === pending ? "pending" : "settled";
  } finally {
    if (timer !== undefined) clearTimeout(timer);
  }
}

/**
 * An env wired to in-memory implementations, with the supplied repo store so a
 * test can place an abort against a durable write.
 */
export function buildAbortWindowEnv(
  def: WorkflowDefinition,
  opts: { repoStore: RepoStore },
): WorkflowRuntimeEnv {
  const clock = (): Date => new Date();
  return {
    repoStore: opts.repoStore,
    scheduler: createInMemoryScheduler({ repoStore: opts.repoStore, clock }),
    signalChannel: createInMemorySignalChannel(),
    blobs: createInMemoryBlobSubstrate(),
    directors: createDefaultDirectorRegistry(),
    authorize: async () => ({
      effect: "allow",
      matchingGrants: [],
      resolvedBy: null,
    }),
    invokeStep: async () => ({ output: null }),
    spawnChild: async () => ({ terminalStatus: "completed" }),
    clock,
    newId: (prefix) => `${prefix}-${Math.random().toString(36).slice(2, 8)}`,
    drain: createNoopDrainController(def),
    hasUpstreamSignalResolver: true,
  };
}
