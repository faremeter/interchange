// Per-file registry of the supervisors a test built, torn down by a lifecycle
// hook however the test ended. A failed assertion skips the test's own
// `shutdown()`, and a supervisor left running keeps its armed five-minute
// backstop armed; reaping the cohort is what disarms it. The registry is
// per-call, not module-level, so two test files cannot reap each other's
// supervisors (the unit pass shares one module registry per worker).

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
   * Every `shutdown()` is called before any is awaited, so no teardown waits
   * on its neighbours and they settle in completion order. A teardown already
   * in flight is re-entered harmlessly: every branch guards on the prior
   * phase.
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
      // Every `shutdown()` is called before any is awaited, so a teardown
      // that rejects or never settles still leaves the rest to run; awaiting
      // one at a time would strand every supervisor behind it. A throw is a
      // defect (`shutdown` is documented as total) and fails the test rather
      // than landing in a log nobody reads. `allSettled` still waits on a
      // never-settling shutdown, so reap itself hangs on it; abandoning it
      // would need a deadline, which "Synchronizing on State, Not Time" in
      // CONVENTIONS.md rules out. This covers the leak, not the hang.
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
