// Adapter from the host-singleton `SchedulerHandle` to the runtime's
// per-call `Scheduler` interface.
//
// The host's `SchedulerHandle` (from `seams/scheduler.ts`) tails
// `TimerSet` commits via `subscribeKind` and commits `TimerFired` at
// the wall-clock deadline. The runtime body (`@intx/workflow`'s
// `runtimeRun`) expects a `Scheduler.scheduleIn(runId, timerId,
// fireAt) => dispose` whose dispose cancels any pending `TimerFired`.
//
// The adapter routes `scheduleIn` through to the host scheduler's
// live ingest by relying on the supervisor's commit-then-subscribe
// ordering: by the time the runtime body has committed `TimerSet`
// and called `scheduleIn`, the host scheduler's `subscribeKind` tail
// will queue the timer. The returned disposer asks the host scheduler
// to drop the queued entry for `(runId, timerId)`.

import type { Scheduler } from "@intx/workflow";

import type { SchedulerHandle } from "./scheduler";

export function adaptHostScheduler(handle: SchedulerHandle): Scheduler {
  return {
    scheduleIn(runId, timerId, _fireAt) {
      return () => {
        handle.cancelQueued(runId, timerId);
      };
    },
  };
}
