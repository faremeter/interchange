import { describe, test, expect } from "bun:test";

import { createSupervisorReaper } from "./supervisor-reaper";

describe("createSupervisorReaper", () => {
  test("reaps every tracked supervisor and forgets them", async () => {
    const reaper = createSupervisorReaper();
    const shutdowns: string[] = [];
    reaper.track({
      shutdown: async () => {
        shutdowns.push("a");
      },
    });
    reaper.track({
      shutdown: async () => {
        shutdowns.push("b");
      },
    });
    await reaper.reap();
    expect(shutdowns).toEqual(["a", "b"]);

    // A second reap must not shut the same pair down again: the hook runs
    // after every test, and a registry that kept its entries would re-reap
    // supervisors belonging to tests that had already finished.
    await reaper.reap();
    expect(shutdowns).toEqual(["a", "b"]);
  });

  test("one wedged supervisor does not strand the rest", async () => {
    const reaper = createSupervisorReaper();
    const shutdowns: string[] = [];
    reaper.track({
      shutdown: () => Promise.reject(new Error("wedged")),
    });
    reaper.track({
      shutdown: async () => {
        shutdowns.push("after-the-wedged-one");
      },
    });
    await expect(reaper.reap()).rejects.toThrow(/teardown threw/);
    // The throw surfaces the defect, but only after the cohort is reaped --
    // otherwise one wedged supervisor leaks every supervisor behind it.
    expect(shutdowns).toEqual(["after-the-wedged-one"]);
  });

  test("a shutdown that never settles does not strand the rest", async () => {
    const reaper = createSupervisorReaper();
    const shutdowns: string[] = [];
    // Never settles, and holds no timer or handle, so abandoning it at the end
    // of the test leaks nothing: only `reap`'s own resolution waits on it.
    reaper.track({ shutdown: () => new Promise<void>(() => undefined) });
    let reportRan = (): void => undefined;
    const ran = new Promise<void>((resolve) => {
      reportRan = resolve;
    });
    reaper.track({
      shutdown: async () => {
        shutdowns.push("behind-the-wedged-one");
        reportRan();
      },
    });

    // Racing the reap against the second teardown's own signal, rather than
    // awaiting the reap: `allSettled` waits for the wedged shutdown too, so
    // the reap never settles here. Folding both of its outcomes to values
    // keeps the test owning it -- a rejection cannot escape as an unhandled
    // one charged to whichever test is running when it lands. A reap that
    // awaited each shutdown in turn would never call the second one, so
    // neither arm would settle and the lane timeout would fail this test.
    const won = await Promise.race([
      ran.then(() => "teardown-behind-the-wedged-one-ran"),
      reaper.reap().then(
        () => "reap-settled",
        () => "reap-rejected",
      ),
    ]);
    expect(won).toBe("teardown-behind-the-wedged-one-ran");
    expect(shutdowns).toEqual(["behind-the-wedged-one"]);
  });

  test("two reapers do not see each other's supervisors", async () => {
    const first = createSupervisorReaper();
    const second = createSupervisorReaper();
    const shutdowns: string[] = [];
    first.track({
      shutdown: async () => {
        shutdowns.push("first");
      },
    });
    // The registry is per-call rather than module-level. One shared array
    // would let two test files reap each other's supervisors, since the unit
    // pass gives a worker one module registry for every file it runs.
    await second.reap();
    expect(shutdowns).toEqual([]);
    await first.reap();
    expect(shutdowns).toEqual(["first"]);
  });
});
