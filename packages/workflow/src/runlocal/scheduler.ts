// In-process event-sourced scheduler.
//
// Single-writer-on-`TimerFired` invariant: the scheduler is the only
// component that commits `TimerFired` to the run's log. The runtime
// body's `waitForTimer` helper subscribes to the log tail and
// resolves on the scheduler-committed event.
//
// Not durable across restart: stranded `TimerSet` events from prior
// runs are not auto-serviced. The spec calls this out explicitly --
// `runLocal` is normative modulo durability. The production
// scheduler in `@intx/workflow-host` provides start-time recovery
// by enumerating unfired timers from the persisted log.

import type { RepoStore, Scheduler } from "../runtime/env";
import { commit } from "../runtime/commit-chain";
import type { WorkflowEvent } from "../state-machine/index";

export type CreateInMemorySchedulerOpts = {
  /**
   * The same `RepoStore` instance the runtime body writes through.
   * The scheduler commits `TimerFired` via this store, going through
   * the shared per-runId commit chain so its writes serialize with
   * the runtime body's concurrent commits.
   */
  repoStore: RepoStore;
  /**
   * Clock the scheduler reads to compute delays from a `fireAt`
   * `Date`. Production reads `new Date()`; tests inject a
   * deterministic clock so timer math is reproducible.
   */
  clock: () => Date;
};

export function createInMemoryScheduler(
  opts: CreateInMemorySchedulerOpts,
): Scheduler {
  return {
    scheduleIn(runId, timerId, fireAt) {
      const delayMs = Math.max(0, fireAt.getTime() - opts.clock().getTime());
      let disposed = false;
      const handle = setTimeout(() => {
        if (disposed) return;
        void commitTimerFired(opts, runId, timerId).catch((cause) => {
          // The scheduler's commit chain is shared with the runtime
          // body's; an error here means the log is corrupt or a
          // concurrent writer raced us. Surface as an unhandled
          // rejection rather than swallowing -- the runtime's
          // waiting consumer would otherwise hang forever.
          throw cause instanceof Error
            ? cause
            : new Error(
                `scheduler timer-fired commit failed: ${String(cause)}`,
              );
        });
      }, delayMs);
      return () => {
        disposed = true;
        clearTimeout(handle);
      };
    },
  };
}

async function commitTimerFired(
  opts: CreateInMemorySchedulerOpts,
  runId: string,
  timerId: string,
): Promise<void> {
  // The commit chain assigns the next seq under the lock; the seq we
  // place on the event here is a hint that the chain overwrites.
  const event: WorkflowEvent = {
    kind: "TimerFired",
    seq: 0,
    at: opts.clock().toISOString(),
    timerId,
  };
  await commit({ repoStore: opts.repoStore }, runId, event);
}
