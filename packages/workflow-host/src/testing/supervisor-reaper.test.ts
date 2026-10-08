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

    // The hook runs after every test, so a second reap must not re-shut the
    // same pair down: reaping forgets the entries.
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
    // Never settles and holds no timer or handle, so only `reap`'s own
    // resolution waits on it.
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

    // `allSettled` waits for the wedged shutdown too, so the reap never
    // settles here; race it against the second teardown's own signal instead.
    // A reap that awaited each shutdown in turn would never call the second
    // one and the lane timeout would fail the test.
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
    // The registry is per-call: one shared array would let two test files
    // reap each other's supervisors (the unit pass shares one module registry
    // per worker).
    await second.reap();
    expect(shutdowns).toEqual([]);
    await first.reap();
    expect(shutdowns).toEqual(["first"]);
  });
});
