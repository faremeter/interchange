// Per-file registry of the supervisors a test built, so a lifecycle hook can
// tear them down whatever way the test ended.
//
// A supervisor is normally torn down by a `shutdown()` at the end of the test
// that built it, which an earlier failed assertion skips -- and some tests
// never had one. What that abandons is not inert: a dispatch suspended in
// `waitForRunTerminalOrPark` holds that function's armed five-minute
// backstop, whose clearing `finally` cannot run while the race it guards is
// pending. Aborting the cohort is what settles that race, so reaping is what
// disarms the timer. The unit pass shares a module registry per worker, so an
// un-reaped supervisor outlives its file and any failure it causes is charged
// to an unrelated test.
//
// The registry is per-call rather than module-level: two test files importing
// one shared array would reap each other's supervisors if their lifecycles
// ever overlapped.

/** The part of a supervisor a reaper needs. */
export type ReapableSupervisor = {
  shutdown(): Promise<void>;
};

export type SupervisorReaper = {
  /** Register a supervisor and return it, for use at the construction site. */
  track<T extends ReapableSupervisor>(supervisor: T): T;
  /**
   * Shut down every supervisor registered since the last call. Pass straight
   * to `afterEach`.
   *
   * The teardowns overlap: every `shutdown()` is called before any is awaited,
   * so they settle in whatever order they finish rather than registration
   * order. Nothing here sequences one supervisor's teardown after another's.
   *
   * `shutdown` early-returns once the phase is terminal. That guard does not
   * cover `stopping`, so a teardown already in flight is re-entered here --
   * harmlessly, because every branch of the teardown is guarded on the prior
   * phase and a second pass through `stopping` does no work. Either way the
   * tests that tear themselves down are unaffected.
   */
  reap(): Promise<void>;
};

export function createSupervisorReaper(): SupervisorReaper {
  const live: ReapableSupervisor[] = [];
  return {
    track(supervisor) {
      live.push(supervisor);
      return supervisor;
    },
    async reap() {
      // Every `shutdown()` is CALLED before any of them is awaited, so no
      // supervisor's teardown waits on its neighbours': one that REJECTS and
      // one that never SETTLES both leave the rest to run to completion.
      // Awaiting them one at a time would not -- a teardown that never
      // settled would mean the supervisors behind it never started theirs,
      // each keeping its child and its armed backstop.
      //
      // A non-settling `shutdown()` still blocks this function's own
      // resolution, because `allSettled` waits for that one too -- so the
      // `afterEach` calling it does not complete, and the lane timeout is what
      // fails the file. Abandoning the pending one would take a deadline,
      // which "Synchronizing on State, Not Time" in CONVENTIONS.md rules out,
      // and `shutdown()` offers no other signal to race. What this covers is
      // the leak, not the hang.
      //
      // `shutdown` is documented as total, so a throw is a defect and fails
      // the test rather than landing in a log nobody reads.
      const settled = await Promise.allSettled(
        live.splice(0).map((supervisor) => supervisor.shutdown()),
      );
      const failures = settled.flatMap((result) =>
        result.status === "rejected" ? [result.reason] : [],
      );
      if (failures.length > 0) {
        throw new AggregateError(failures, "supervisor teardown threw");
      }
    },
  };
}
