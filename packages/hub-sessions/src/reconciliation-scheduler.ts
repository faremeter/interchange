import { getLogger } from "@intx/log";

const logger = getLogger(["hub", "reconciliation"]);

export const DEFAULT_SIDECAR_ALLOCATION_CONCURRENCY = 8;

export type ReconciliationSchedulerOptions = {
  readonly name: string;
  /** Claim and process at most one item; false means no due work was found. */
  readonly reconcileNext: () => Promise<boolean>;
  readonly concurrency?: number;
  readonly intervalMs?: number;
};

/** Polls for work without waiting for other occupied slots to finish. */
export function createReconciliationScheduler({
  name,
  reconcileNext,
  concurrency = DEFAULT_SIDECAR_ALLOCATION_CONCURRENCY,
  intervalMs = 1_000,
}: ReconciliationSchedulerOptions) {
  if (!Number.isSafeInteger(concurrency) || concurrency <= 0) {
    throw new Error("Reconciliation concurrency must be a positive integer");
  }
  if (!Number.isSafeInteger(intervalMs) || intervalMs <= 0) {
    throw new Error("Reconciliation interval must be a positive integer");
  }
  let stopped = true;
  let active = 0;
  let timer: ReturnType<typeof setTimeout> | undefined;

  function finish(worked: boolean): void {
    active -= 1;
    if (worked) wake();
  }

  function wake(): void {
    if (stopped) return;
    while (active < concurrency) {
      active += 1;
      void Promise.resolve()
        .then(() => (stopped ? false : reconcileNext()))
        .then(finish, (error: unknown) => {
          logger.error`${name} reconciliation failed: ${error instanceof Error ? error.message : String(error)}`;
          finish(false);
        });
    }
  }

  function schedule(delayMs: number): void {
    timer = setTimeout(() => {
      if (stopped) return;
      wake();
      schedule(intervalMs);
    }, delayMs);
    timer.unref?.();
  }

  return {
    start(): void {
      if (!stopped) return;
      stopped = false;
      schedule(0);
    },
    stop(): void {
      stopped = true;
      clearTimeout(timer);
    },
    wake,
  };
}
