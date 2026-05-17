export type AdvanceOpts = { microtaskBudget?: number };
export type RunOpts = { microtaskBudget?: number; wallClockBudgetMs?: number };

export interface Clock {
  now(): number;
  schedule(virtualMs: number, fn: () => void): void;
  advanceTo(virtualMs: number, opts?: AdvanceOpts): Promise<void>;
  run(opts?: RunOpts): Promise<void>;
  onSyncCallbackError(hook: (err: unknown) => void): void;
  /**
   * Externally signal that consumer-observable work has landed (for
   * example, bytes pushed into a `ReadableStream` controller from a
   * fired callback). This bumps the activity counter that
   * `drainMicrotasks` watches, so the drain keeps flushing until the
   * downstream consumer's microtask chain (reader.read -> parser yield
   * -> awaiting test) has fully settled.
   *
   * Without this hook the drain exits as soon as a single microtask
   * pass shows no new `schedule()`/`fireOne()` activity, even though
   * the consumer chain triggered by the callback's side effect has not
   * yet caught up. The result is that a single conceptual `advanceTo`
   * may only deliver the first of several scheduled chunks to a
   * pre-attached reader.
   *
   * Callers must invoke this ONLY in response to genuine
   * consumer-observable work. Calling it gratuitously (every
   * microtask, every internal bookkeeping step) defeats the budget's
   * purpose: the drain exists to bound runaway scheduling, and a
   * spurious activity bump can mask an actual infinite loop.
   */
  notifyActivity(): void;
}

type HeapEntry = {
  time: number;
  seq: number;
  fn: () => void;
};

type ClockState = {
  now: number;
  seq: number;
  heap: HeapEntry[];
  lastFired: (() => void) | null;
  lastScheduler: (() => void) | null;
  insideCallback: (() => void) | null;
  errorHook: (err: unknown) => void;
  activity: number;
};

const DEFAULT_MICROTASK_BUDGET = 256;
const DEFAULT_WALL_CLOCK_BUDGET_MS = 250;
const MAX_HINT_ENTRIES = 8;

const NOOP_ERROR_HOOK = (_err: unknown): void => {
  return;
};

export type HeapSnapshotEntry = {
  virtualTime: number;
  seq: number;
  callback: string;
};

export class ClockOverrunError extends Error {
  readonly heapSnapshot: readonly HeapSnapshotEntry[];
  readonly hint: string;

  constructor(
    message: string,
    heapSnapshot: readonly HeapSnapshotEntry[],
    hint: string,
  ) {
    super(`${message} (hint: ${hint})`);
    this.name = "ClockOverrunError";
    this.heapSnapshot = heapSnapshot;
    this.hint = hint;
  }
}

export class ClockWallClockOverrunError extends Error {
  readonly inFlightHint: string;

  constructor(message: string, inFlightHint: string) {
    super(`${message} (in-flight: ${inFlightHint})`);
    this.name = "ClockWallClockOverrunError";
    this.inFlightHint = inFlightHint;
  }
}

function heapLess(a: HeapEntry, b: HeapEntry): boolean {
  if (a.time !== b.time) return a.time < b.time;
  return a.seq < b.seq;
}

// Index-bounds-checked accessor. Every caller below guards `i` against the
// heap length, so an undefined return here is a structural bug; surface it
// loudly rather than papering over it.
function heapAt(heap: HeapEntry[], i: number): HeapEntry {
  const entry = heap[i];
  if (entry === undefined) {
    throw new Error(
      `heap invariant violated: undefined entry at index ${String(i)}`,
    );
  }
  return entry;
}

function heapSwap(heap: HeapEntry[], i: number, j: number): void {
  const a = heapAt(heap, i);
  const b = heapAt(heap, j);
  heap[i] = b;
  heap[j] = a;
}

function heapPush(heap: HeapEntry[], entry: HeapEntry): void {
  heap.push(entry);
  let i = heap.length - 1;
  while (i > 0) {
    const parent = (i - 1) >> 1;
    if (heapLess(heapAt(heap, i), heapAt(heap, parent))) {
      heapSwap(heap, i, parent);
      i = parent;
    } else {
      break;
    }
  }
}

function heapPop(heap: HeapEntry[]): HeapEntry | undefined {
  if (heap.length === 0) return undefined;
  const top = heapAt(heap, 0);
  const last = heap.pop();
  if (heap.length === 0 || last === undefined) return top;
  heap[0] = last;
  let i = 0;
  const n = heap.length;
  for (;;) {
    const l = 2 * i + 1;
    const r = 2 * i + 2;
    let smallest = i;
    if (l < n && heapLess(heapAt(heap, l), heapAt(heap, smallest))) {
      smallest = l;
    }
    if (r < n && heapLess(heapAt(heap, r), heapAt(heap, smallest))) {
      smallest = r;
    }
    if (smallest === i) break;
    heapSwap(heap, i, smallest);
    i = smallest;
  }
  return top;
}

function heapPeek(heap: HeapEntry[]): HeapEntry | undefined {
  return heap[0];
}

function describeCallback(fn: (() => void) | null): string {
  if (fn === null) return "<none>";
  const name = fn.name;
  if (name.length === 0) return "<anonymous>";
  return name;
}

function snapshotHeap(heap: HeapEntry[]): readonly HeapSnapshotEntry[] {
  const sorted = [...heap].sort((a, b) => b.seq - a.seq);
  return sorted.slice(0, MAX_HINT_ENTRIES).map((e) => ({
    virtualTime: e.time,
    seq: e.seq,
    callback: describeCallback(e.fn),
  }));
}

function invokeErrorHook(state: ClockState, err: unknown): void {
  try {
    state.errorHook(err);
  } catch (_hookErr) {
    throw new Error(
      "onSyncCallbackError hook threw while propagating callback error",
      { cause: err instanceof Error ? err : new Error(String(err)) },
    );
  }
}

function validateMicrotaskBudget(budget: number): void {
  if (!Number.isInteger(budget) || budget < 1) {
    throw new Error(
      `microtaskBudget must be a positive integer (>= 1), got ${String(budget)}`,
    );
  }
}

function validateWallClockBudget(budget: number): void {
  if (typeof budget !== "number" || Number.isNaN(budget) || budget <= 0) {
    throw new Error(
      `wallClockBudgetMs must be a positive finite number or Infinity, got ${String(budget)}`,
    );
  }
}

type WallClockWatchdog = {
  check: () => void;
};

// Per drain iteration, flush the microtask queue several times before
// declaring quiescence. A single `await queueMicrotask(resolve)` pumps
// the JS microtask queue through two rounds; consumer chains that
// thread through async-generator composition (e.g.
// `reader.read` -> `parseSSE` yield -> `runInference` yield -> `reactor`
// consume) span more rounds per fired callback. Without the inner flush
// loop a drain iteration leaves the consumer mid-chain, the activity
// counter does not move (consumer-side settlement does not bump
// `activity` on its own), and the outer loop exits before the consumer
// has settled. The `microtaskBudget` still bounds the number of "saw
// new scheduling activity" cycles — the runaway-scheduler probe in
// `clock.test.ts` and the `parsesse-regression` probe continue to gate
// on it.
const MICROTASK_FLUSHES_PER_ITERATION = 8;

async function drainMicrotasks(
  state: ClockState,
  microtaskBudget: number,
  watchdog: WallClockWatchdog | null,
): Promise<void> {
  for (let iterations = 0; iterations < microtaskBudget; iterations++) {
    const activityBefore = state.activity;
    for (let i = 0; i < MICROTASK_FLUSHES_PER_ITERATION; i++) {
      await new Promise<void>((resolve) => {
        queueMicrotask(resolve);
      });
    }
    if (watchdog !== null) {
      watchdog.check();
    }
    if (state.activity === activityBefore) {
      return;
    }
  }
  const schedulerName =
    state.lastScheduler !== null
      ? describeCallback(state.lastScheduler)
      : describeCallback(state.lastFired);
  throw new ClockOverrunError(
    `microtask quiescence not reached after ${String(microtaskBudget)} iterations`,
    snapshotHeap(state.heap),
    `last scheduler: ${schedulerName}`,
  );
}

export function createClock(): Clock {
  return createClockInternal({});
}

type ClockInternalOpts = { initialSeq?: number };

/**
 * @internal TEST-ONLY: constructs a Clock with the monotonic sequence counter
 * pre-seeded. Used by `clock-internal.ts` to exercise the overflow guard
 * without scheduling Number.MAX_SAFE_INTEGER entries. Production code must
 * use `createClock()`.
 */
export function createClockInternal(opts: ClockInternalOpts): Clock {
  const state: ClockState = {
    now: 0,
    seq: opts.initialSeq ?? 0,
    heap: [],
    lastFired: null,
    lastScheduler: null,
    insideCallback: null,
    errorHook: NOOP_ERROR_HOOK,
    activity: 0,
  };

  const schedule = (virtualMs: number, fn: () => void): void => {
    if (typeof virtualMs !== "number" || !Number.isFinite(virtualMs)) {
      throw new Error(
        `Clock.schedule: virtualMs must be a finite number, got ${String(virtualMs)}`,
      );
    }
    if (typeof fn !== "function") {
      throw new Error("Clock.schedule: fn must be a function");
    }
    if (virtualMs < state.now) {
      throw new Error(
        `Clock.schedule: cannot schedule in the past (virtualMs=${String(virtualMs)} < now=${String(state.now)}); past-time scheduling is a programmer bug`,
      );
    }
    if (state.seq === Number.MAX_SAFE_INTEGER) {
      throw new Error(
        "Clock.schedule: monotonicSeq overflow (next seq would exceed Number.MAX_SAFE_INTEGER)",
      );
    }
    state.seq += 1;
    const entry: HeapEntry = {
      time: virtualMs,
      seq: state.seq,
      fn,
    };
    heapPush(state.heap, entry);
    state.lastScheduler = state.insideCallback;
    state.activity += 1;
  };

  const fireOne = (entry: HeapEntry): void => {
    // Increments activity so that re-entrant drains (a callback that synchronously
    // pumps another advanceTo/run through this clock) still see a fire as
    // observable work; the outer firing loop also runs outside drainMicrotasks,
    // so this is defensive rather than load-bearing in the simple case.
    state.activity += 1;
    state.now = entry.time;
    state.lastFired = entry.fn;
    state.insideCallback = entry.fn;
    try {
      entry.fn();
    } catch (err) {
      invokeErrorHook(state, err);
      throw err;
    } finally {
      state.insideCallback = null;
    }
  };

  const advanceTo = async (
    virtualMs: number,
    opts?: AdvanceOpts,
  ): Promise<void> => {
    if (typeof virtualMs !== "number" || !Number.isFinite(virtualMs)) {
      throw new Error(
        `Clock.advanceTo: virtualMs must be a finite number, got ${String(virtualMs)}`,
      );
    }
    if (virtualMs < state.now) {
      throw new Error(
        `Clock.advanceTo: cannot advance to the past (virtualMs=${String(virtualMs)} < now=${String(state.now)}); past-time advance is a programmer bug`,
      );
    }
    const microtaskBudget = opts?.microtaskBudget ?? DEFAULT_MICROTASK_BUDGET;
    validateMicrotaskBudget(microtaskBudget);

    for (;;) {
      while (true) {
        const top = heapPeek(state.heap);
        if (top === undefined || top.time > virtualMs) break;
        const entry = heapPop(state.heap);
        if (entry === undefined) break;
        fireOne(entry);
        await drainMicrotasks(state, microtaskBudget, null);
      }
      // Final drain at the latest fired entry's time (or 0 if none fired). If
      // this drain schedules new entries at <= virtualMs, re-enter the firing
      // loop above. Doing the drain BEFORE force-advancing `now` to virtualMs
      // ensures any schedule(0, ...) from a trailing microtask is still in
      // the past-time guard's window.
      await drainMicrotasks(state, microtaskBudget, null);
      const top = heapPeek(state.heap);
      if (top === undefined || top.time > virtualMs) {
        break;
      }
    }

    if (state.now < virtualMs) {
      state.now = virtualMs;
    }
  };

  const run = async (opts?: RunOpts): Promise<void> => {
    const microtaskBudget = opts?.microtaskBudget ?? DEFAULT_MICROTASK_BUDGET;
    validateMicrotaskBudget(microtaskBudget);
    const wallClockBudgetMs =
      opts?.wallClockBudgetMs ?? DEFAULT_WALL_CLOCK_BUDGET_MS;
    validateWallClockBudget(wallClockBudgetMs);
    const startWall = performance.now();
    const watchdog: WallClockWatchdog = {
      check: () => {
        if (wallClockBudgetMs === Infinity) return;
        const elapsed = performance.now() - startWall;
        if (elapsed > wallClockBudgetMs) {
          throw new ClockWallClockOverrunError(
            `Clock.run exceeded wall-clock budget of ${String(wallClockBudgetMs)}ms (elapsed=${String(elapsed)}ms)`,
            `last fired callback: ${describeCallback(state.lastFired)}`,
          );
        }
      },
    };
    for (;;) {
      const top = heapPeek(state.heap);
      if (top === undefined) {
        await drainMicrotasks(state, microtaskBudget, watchdog);
        if (state.heap.length === 0) return;
        continue;
      }
      const entry = heapPop(state.heap);
      if (entry === undefined) return;
      fireOne(entry);
      await drainMicrotasks(state, microtaskBudget, watchdog);
      watchdog.check();
    }
  };

  /**
   * Registers a hook invoked when a scheduled callback throws synchronously
   * inside `advanceTo` or `run`. The hook receives the original error, runs
   * before the throw propagates, and is used by the future harness layer to
   * close open simulated streams so the next test does not inherit dangling
   * readers. A throwing hook is wrapped in a new Error with the original
   * attached via `cause`. Calling this replaces any previously registered
   * hook; one hook per clock.
   */
  const onSyncCallbackError = (hook: (err: unknown) => void): void => {
    if (typeof hook !== "function") {
      throw new Error("Clock.onSyncCallbackError: hook must be a function");
    }
    state.errorHook = hook;
  };

  const notifyActivity = (): void => {
    state.activity += 1;
  };

  const clock: Clock = {
    now: () => state.now,
    schedule,
    advanceTo,
    run,
    onSyncCallbackError,
    notifyActivity,
  };
  return clock;
}
