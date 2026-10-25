// What the runtime is doing in this process. A host that stops the process
// while every run waits must know that nothing here is still working, and
// that nothing committed is still on its way to the run that waits for it.
//
// Work is a run loop or a step in progress. A step that waits on input from
// outside -- a signal, a timer, a nested run's next event -- is not working
// while it waits. Delivery is the host handing a committed input to the run
// that waits for it. Both counts are process-wide, like the commit chain,
// since every run in a process shares one stop.
//
// Whatever wakes a waiter hands its own count over: it resumes the waiter
// before it stops counting itself. A waiter that only counted again once its
// await continued would leave a gap in which nothing counts.

let working = 0;
let delivering = 0;
// Every start of work or of a delivery, so a check can tell that something
// began since it last looked, even work that has already ended again.
let starts = 0;
const waiters = new Set<() => void>();

function lowered(): void {
  for (const wake of [...waiters]) wake();
}

function until(settled: () => boolean, signal?: AbortSignal): Promise<void> {
  if (settled()) return Promise.resolve();
  if (signal?.aborted === true) return Promise.reject(signal.reason);
  return new Promise((resolve, reject) => {
    const wake = (): void => {
      if (!settled()) return;
      waiters.delete(wake);
      signal?.removeEventListener("abort", stop);
      resolve();
    };
    const stop = (): void => {
      waiters.delete(wake);
      reject(signal?.reason);
    };
    waiters.add(wake);
    signal?.addEventListener("abort", stop, { once: true });
  });
}

/** Count work in progress until the returned release is called. */
export function beginWork(): () => void {
  working += 1;
  starts += 1;
  let released = false;
  return () => {
    if (released) return;
    released = true;
    working -= 1;
    lowered();
  };
}

/**
 * Wait without counting the caller as working meanwhile. The caller is
 * counted work, and `wait` starts the one thing it waits for: input from
 * outside, or work that is counted on its own. Whatever settles that wait
 * calls `resume` first, while it still counts itself; `resume` is
 * idempotent, and the wait resumes on its own as a fallback.
 */
export async function waitIdle<T>(
  wait: (resume: () => void) => Promise<T>,
): Promise<T> {
  working -= 1;
  let resumed = false;
  const resume = (): void => {
    if (resumed) return;
    resumed = true;
    working += 1;
    starts += 1;
  };
  try {
    const waiting = wait(resume);
    lowered();
    return await waiting;
  } finally {
    resume();
  }
}

/** Waits whose inputs a caller starts before it stops working. */
export type IdleWait = {
  /** A wake hook for one input; pass it to whatever settles that input. */
  waker(): () => void;
  /**
   * Wait idle on `waiting`, which settles with one of `wakers`. A waker
   * that already fired settled its input before this wait began, so the
   * caller keeps counting.
   */
  wait<T>(waiting: Promise<T>, wakers: readonly (() => void)[]): Promise<T>;
};

export function createIdleWait(): IdleWait {
  const fired = new Set<() => void>();
  let current:
    | { resume: () => void; wakers: readonly (() => void)[] }
    | undefined;
  return {
    waker() {
      const wake = (): void => {
        fired.add(wake);
        if (current !== undefined && current.wakers.includes(wake)) {
          current.resume();
        }
      };
      return wake;
    },
    async wait(waiting, wakers) {
      try {
        return await waitIdle((resume) => {
          current = { resume, wakers };
          if (wakers.some((wake) => fired.has(wake))) resume();
          return waiting;
        });
      } finally {
        current = undefined;
      }
    },
  };
}

/** Count a delivery of committed input until the returned release is called. */
export function beginDelivery(): () => void {
  delivering += 1;
  starts += 1;
  let released = false;
  return () => {
    if (released) return;
    released = true;
    delivering -= 1;
    lowered();
  };
}

/** How many times work or a delivery has started in this process. */
export function activityStarts(): number {
  return starts;
}

/** Whether nothing in this process works or delivers input. */
export function isQuiet(): boolean {
  return working === 0 && delivering === 0;
}

/** Whether a run loop or a step in this process is working. */
export function isWorking(): boolean {
  return working > 0;
}

/** Resolves once no committed input is on its way to a run. */
export function whenDelivered(): Promise<void> {
  return until(() => delivering === 0);
}

/**
 * Resolves once nothing in this process works or delivers input, or rejects
 * when `signal` aborts first.
 */
export function whenQuiet(signal?: AbortSignal): Promise<void> {
  return until(isQuiet, signal);
}
