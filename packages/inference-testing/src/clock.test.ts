import { describe, test, expect } from "bun:test";
import {
  createClock,
  ClockOverrunError,
  ClockWallClockOverrunError,
} from "./clock";
import { createClockWithSeq } from "./clock-internal";

const noop = (): void => {
  return;
};

const captureRejection = async (
  promise: Promise<unknown>,
): Promise<unknown> => {
  try {
    await promise;
  } catch (err) {
    return err;
  }
  return null;
};

const expectRejectionMessage = async (
  promise: Promise<unknown>,
  pattern: RegExp,
): Promise<void> => {
  const caught = await captureRejection(promise);
  expect(caught).toBeInstanceOf(Error);
  if (!(caught instanceof Error)) throw new Error("unreachable");
  expect(caught.message).toMatch(pattern);
};

describe("createClock", () => {
  test("now starts at 0 and advanceTo with no entries advances time", async () => {
    const clock = createClock();
    expect(clock.now()).toBe(0);
    await clock.advanceTo(100);
    expect(clock.now()).toBe(100);
  });

  test("single scheduled entry fires and time advances to the target", async () => {
    const clock = createClock();
    let fired = false;
    clock.schedule(50, () => {
      fired = true;
    });
    await clock.advanceTo(100);
    expect(fired).toBe(true);
    expect(clock.now()).toBe(100);
  });

  test("multiple entries fire in virtual time order", async () => {
    const clock = createClock();
    const order: number[] = [];
    clock.schedule(30, () => order.push(30));
    clock.schedule(10, () => order.push(10));
    clock.schedule(20, () => order.push(20));
    await clock.advanceTo(100);
    expect(order).toEqual([10, 20, 30]);
  });

  test("same-time entries fire in monotonicSeq order", async () => {
    const clock = createClock();
    const order: string[] = [];
    clock.schedule(50, () => order.push("A"));
    clock.schedule(50, () => order.push("B"));
    clock.schedule(50, () => order.push("C"));
    await clock.advanceTo(100);
    expect(order).toEqual(["A", "B", "C"]);
  });

  test("entries past the advance limit do not fire and remain queued", async () => {
    const clock = createClock();
    const fired: number[] = [];
    clock.schedule(50, () => fired.push(50));
    clock.schedule(150, () => fired.push(150));
    await clock.advanceTo(100);
    expect(fired).toEqual([50]);
    expect(clock.now()).toBe(100);
    await clock.advanceTo(200);
    expect(fired).toEqual([50, 150]);
    expect(clock.now()).toBe(200);
  });

  test("microtask quiescence drains chained microtasks between entries", async () => {
    const clock = createClock();
    let state = 0;
    const observed: number[] = [];
    clock.schedule(10, () => {
      queueMicrotask(() => {
        queueMicrotask(() => {
          queueMicrotask(() => {
            state += 1;
            observed.push(clock.now());
          });
        });
      });
    });
    clock.schedule(20, () => {
      state += 100;
    });
    await clock.advanceTo(100);
    expect(state).toBe(101);
    expect(observed).toEqual([10]);
  });

  test("microtaskBudget of 1 succeeds for a trivially quiet advanceTo", async () => {
    const clock = createClock();
    await clock.advanceTo(0, { microtaskBudget: 1 });
    expect(clock.now()).toBe(0);
  });

  test("entries scheduled from inside a callback within the advance window fire this round", async () => {
    const clock = createClock();
    const order: number[] = [];
    clock.schedule(10, () => {
      order.push(10);
      clock.schedule(15, () => {
        order.push(15);
      });
    });
    await clock.advanceTo(20);
    expect(order).toEqual([10, 15]);
  });

  test("entries scheduled from inside a callback beyond the advance window remain queued", async () => {
    const clock = createClock();
    const order: number[] = [];
    clock.schedule(10, () => {
      order.push(10);
      clock.schedule(50, () => {
        order.push(50);
      });
    });
    await clock.advanceTo(20);
    expect(order).toEqual([10]);
    expect(clock.now()).toBe(20);
    await clock.advanceTo(60);
    expect(order).toEqual([10, 50]);
  });

  test("microtask budget overrun throws ClockOverrunError", async () => {
    const clock = createClock();
    clock.schedule(10, function runawayCallback() {
      const chain = (depth: number): void => {
        queueMicrotask(() => {
          if (depth > 0) chain(depth - 1);
          clock.schedule(1000, noop);
        });
      };
      chain(100);
    });
    let caught: unknown = null;
    try {
      await clock.advanceTo(20, { microtaskBudget: 4 });
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(ClockOverrunError);
  });

  test("microtask overrun error carries heap snapshot and scheduler hint", async () => {
    const clock = createClock();
    clock.schedule(10, function runawayCallback() {
      const chain = (depth: number): void => {
        queueMicrotask(() => {
          clock.schedule(1000 + depth, noop);
          if (depth > 0) chain(depth - 1);
        });
      };
      chain(50);
    });
    let caught: unknown = null;
    try {
      await clock.advanceTo(20, { microtaskBudget: 4 });
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(ClockOverrunError);
    if (!(caught instanceof ClockOverrunError)) throw new Error("unreachable");
    expect(caught.heapSnapshot.length).toBeGreaterThan(0);
    expect(caught.heapSnapshot.length).toBeLessThanOrEqual(8);
    expect(caught.hint).toContain("runawayCallback");
    expect(caught.message).toContain(caught.hint);
    for (const entry of caught.heapSnapshot) {
      expect(typeof entry.virtualTime).toBe("number");
      expect(typeof entry.seq).toBe("number");
      expect(typeof entry.callback).toBe("string");
    }
    // Snapshot is sorted by seq descending: most recently scheduled first.
    for (let i = 1; i < caught.heapSnapshot.length; i++) {
      const prev = caught.heapSnapshot[i - 1];
      const curr = caught.heapSnapshot[i];
      if (prev === undefined || curr === undefined) {
        throw new Error("unreachable");
      }
      expect(prev.seq).toBeGreaterThan(curr.seq);
    }
  });

  test("empty-heap advanceTo drains a pending queueMicrotask", async () => {
    const clock = createClock();
    let ran = false;
    queueMicrotask(() => {
      ran = true;
    });
    await clock.advanceTo(0);
    expect(ran).toBe(true);
  });

  test("run drains heap and quiescence until empty", async () => {
    const clock = createClock();
    let secondFired = false;
    clock.schedule(100, () => {
      clock.schedule(200, () => {
        secondFired = true;
      });
    });
    await clock.run();
    expect(secondFired).toBe(true);
    expect(clock.now()).toBe(200);
  });

  test("run wall-clock overrun throws and Infinity opts out", async () => {
    const busyWait = (ms: number): void => {
      const end = performance.now() + ms;
      while (performance.now() < end) {
        /* burn wall clock deterministically */
      }
    };

    const overrunClock = createClock();
    overrunClock.schedule(10, () => {
      busyWait(50);
    });
    let caught: unknown = null;
    try {
      await overrunClock.run({ wallClockBudgetMs: 10 });
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(ClockWallClockOverrunError);

    const opfClock = createClock();
    opfClock.schedule(10, () => {
      busyWait(50);
    });
    await opfClock.run({ wallClockBudgetMs: Infinity });
    expect(opfClock.now()).toBe(10);
  });

  test("run wall-clock overrun trips inside microtask drain", async () => {
    const busyWait = (ms: number): void => {
      const end = performance.now() + ms;
      while (performance.now() < end) {
        /* burn wall clock deterministically */
      }
    };
    const clock = createClock();
    const chain = (depth: number): void => {
      queueMicrotask(() => {
        busyWait(10);
        if (depth > 0) chain(depth - 1);
      });
    };
    chain(5);
    let caught: unknown = null;
    try {
      await clock.run({ wallClockBudgetMs: 10 });
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(ClockWallClockOverrunError);
  });

  test("schedule with a past virtual time throws", async () => {
    const clock = createClock();
    await clock.advanceTo(50);
    expect(() => clock.schedule(10, noop)).toThrow(/past/i);
  });

  test("schedule rejects Infinity, -Infinity, and NaN", () => {
    const clock = createClock();
    expect(() => clock.schedule(Infinity, noop)).toThrow(/finite/i);
    expect(() => clock.schedule(-Infinity, noop)).toThrow(/finite/i);
    expect(() => clock.schedule(Number.NaN, noop)).toThrow(/finite/i);
  });

  test("advanceTo to a past virtual time throws", async () => {
    const clock = createClock();
    await clock.advanceTo(100);
    let caught: unknown = null;
    try {
      await clock.advanceTo(50);
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(Error);
    if (!(caught instanceof Error)) throw new Error("unreachable");
    expect(caught.message).toMatch(/past/i);
  });

  test("advanceTo rejects Infinity, -Infinity, and NaN", async () => {
    const clock = createClock();
    await expectRejectionMessage(clock.advanceTo(Infinity), /finite/i);
    await expectRejectionMessage(clock.advanceTo(-Infinity), /finite/i);
    await expectRejectionMessage(clock.advanceTo(Number.NaN), /finite/i);
  });

  test("advanceTo rejects invalid microtaskBudget values", async () => {
    const clock = createClock();
    await expectRejectionMessage(
      clock.advanceTo(10, { microtaskBudget: 0 }),
      /microtaskBudget/,
    );
    await expectRejectionMessage(
      clock.advanceTo(10, { microtaskBudget: -1 }),
      /microtaskBudget/,
    );
    await expectRejectionMessage(
      clock.advanceTo(10, { microtaskBudget: 1.5 }),
      /microtaskBudget/,
    );
    await expectRejectionMessage(
      clock.advanceTo(10, { microtaskBudget: Number.NaN }),
      /microtaskBudget/,
    );
    await expectRejectionMessage(
      clock.advanceTo(10, { microtaskBudget: Infinity }),
      /microtaskBudget/,
    );
  });

  test("run rejects invalid microtaskBudget values", async () => {
    const clock = createClock();
    await expectRejectionMessage(
      clock.run({ microtaskBudget: 0 }),
      /microtaskBudget/,
    );
    await expectRejectionMessage(
      clock.run({ microtaskBudget: -1 }),
      /microtaskBudget/,
    );
    await expectRejectionMessage(
      clock.run({ microtaskBudget: 1.5 }),
      /microtaskBudget/,
    );
    await expectRejectionMessage(
      clock.run({ microtaskBudget: Number.NaN }),
      /microtaskBudget/,
    );
  });

  test("run rejects invalid wallClockBudgetMs values", async () => {
    const clock = createClock();
    await expectRejectionMessage(
      clock.run({ wallClockBudgetMs: 0 }),
      /wallClockBudgetMs/,
    );
    await expectRejectionMessage(
      clock.run({ wallClockBudgetMs: -1 }),
      /wallClockBudgetMs/,
    );
    await expectRejectionMessage(
      clock.run({ wallClockBudgetMs: Number.NaN }),
      /wallClockBudgetMs/,
    );
  });

  test("sync throw in callback propagates and invokes the error hook", async () => {
    const clock = createClock();
    const captured: unknown[] = [];
    clock.onSyncCallbackError((err) => {
      captured.push(err);
    });
    const boom = new Error("kaboom");
    clock.schedule(10, () => {
      throw boom;
    });
    let caught: unknown = null;
    try {
      await clock.advanceTo(20);
    } catch (err) {
      caught = err;
    }
    expect(caught).toBe(boom);
    expect(captured).toEqual([boom]);
  });

  test("error hook that throws is wrapped and original error attached as cause", async () => {
    const clock = createClock();
    const hookErr = new Error("hook-failure");
    clock.onSyncCallbackError(() => {
      throw hookErr;
    });
    const original = new Error("original");
    clock.schedule(10, () => {
      throw original;
    });
    let caught: unknown = null;
    try {
      await clock.advanceTo(20);
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(Error);
    if (!(caught instanceof Error)) throw new Error("unreachable");
    expect(caught).not.toBe(original);
    expect(caught.cause).toBe(original);
  });

  test("monotonicSeq overflow throws", () => {
    const clock = createClockWithSeq(Number.MAX_SAFE_INTEGER - 1);
    clock.schedule(10, noop);
    expect(() => clock.schedule(20, noop)).toThrow(/overflow/i);
  });

  test("happy path with 1000 entries completes far under 100ms wall clock", async () => {
    const clock = createClock();
    let count = 0;
    for (let i = 0; i < 1000; i++) {
      clock.schedule(i, () => {
        count += 1;
      });
    }
    const start = performance.now();
    await clock.advanceTo(1000);
    const elapsed = performance.now() - start;
    expect(count).toBe(1000);
    expect(clock.now()).toBe(1000);
    expect(elapsed).toBeLessThan(100);
  });
});
