// When a signal for a resumed run may be committed.
//
// A resumed run plans from reads of its log. A signal committed between those
// reads moves a step the run has just read as parked, so the run acts on a park
// that is gone: an agent step in that state looks crashed and fails, since
// re-invoking one could run a tool twice, and a section parks again and loses
// the input. The log cannot tell this apart from a crash, so the child commits
// a signal for a park the run resumed with only once the run waits on it again.
//
// A run that does not park again must not keep the signal forever. A hold also
// ends with the run, which refuses the signal, and once the process goes quiet,
// when nothing is left that could park again.

import { whenQuiet } from "@intx/workflow/runtime";
import type { RunState } from "@intx/workflow";

export type ResumeDeliveryHold = {
  /** Hold signals for `names` until `runId` parks on them again. */
  hold(runId: string, names: readonly string[]): void;
  /** `runId` waits on `name` again, so a signal for it can land. */
  reparked(runId: string, name: string): void;
  runEnded(runId: string): void;
  /**
   * `undefined` when a signal for `name` can land now. Otherwise resolves once
   * it can, or with `false` when the run ended or the child is closing and it
   * must not.
   */
  until(runId: string, name: string): Promise<boolean> | undefined;
  close(): void;
};

/** The channels a run's steps are parked on. */
export function parkedSignalNames(state: RunState): string[] {
  const names: string[] = [];
  for (const step of state.steps.values()) {
    if (step.phase === "awaiting-signal" && step.awaitingSignal !== undefined) {
      names.push(step.awaitingSignal.name);
    }
  }
  return names;
}

export function createResumeDeliveryHold(): ResumeDeliveryHold {
  type Waiter = (canLand: boolean) => void;
  const held = new Map<string, Map<string, Waiter[]>>();
  const quietWatch = new AbortController();
  let watchingQuiet = false;

  const settle = (waiters: readonly Waiter[], canLand: boolean): void => {
    for (const waiter of waiters) waiter(canLand);
  };
  const settleRun = (runId: string, canLand: boolean): void => {
    const names = held.get(runId);
    if (names === undefined) return;
    held.delete(runId);
    for (const waiters of names.values()) settle(waiters, canLand);
  };
  const settleAll = (canLand: boolean): void => {
    for (const runId of [...held.keys()]) settleRun(runId, canLand);
  };

  return {
    hold(runId, names) {
      if (names.length === 0) return;
      held.set(runId, new Map(names.map((name) => [name, []])));
    },
    reparked(runId, name) {
      const names = held.get(runId);
      const waiters = names?.get(name);
      if (names === undefined || waiters === undefined) return;
      names.delete(name);
      if (names.size === 0) held.delete(runId);
      settle(waiters, true);
    },
    runEnded(runId) {
      settleRun(runId, false);
    },
    until(runId, name) {
      const waiters = held.get(runId)?.get(name);
      if (waiters === undefined) return undefined;
      if (!watchingQuiet) {
        watchingQuiet = true;
        void whenQuiet(quietWatch.signal).then(
          () => {
            settleAll(true);
          },
          () => undefined,
        );
      }
      return new Promise<boolean>((resolve) => {
        waiters.push(resolve);
      });
    },
    close() {
      quietWatch.abort();
      settleAll(false);
    },
  };
}
