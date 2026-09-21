export type AnchorRunSweepQueries<T extends { id: string }> = {
  /** The highest run id this pass visits, or `undefined` when none qualify. */
  findPassEnd(activeRunIds: string[]): Promise<string | undefined>;
  /** The lowest eligible run after `afterRunId`, up to and including `passEnd`. */
  findNext(range: {
    afterRunId: string | undefined;
    passEnd: string;
    activeRunIds: string[];
  }): Promise<T | undefined>;
};

/**
 * Visit top-level runs in id order, one bounded pass at a time, so a stream of
 * new runs cannot postpone revisiting earlier ones. A selected run stays
 * excluded from later selections until it is released.
 */
export function createAnchorRunSweep<T extends { id: string }>(
  queries: AnchorRunSweepQueries<T>,
  pause?: { intervalMs: number; now: () => Date },
) {
  const activeRuns = new Set<string>();
  let selectionTail: Promise<void> = Promise.resolve();
  let afterRunId: string | undefined;
  let passEnd: string | undefined;
  let nextPassAt = 0;

  function finishPass(): void {
    afterRunId = undefined;
    passEnd = undefined;
    if (pause !== undefined)
      nextPassAt = pause.now().getTime() + pause.intervalMs;
  }

  function select(): Promise<T | null> {
    // Only selection is serialized. Each caller owns its selected run until it
    // releases it, so work on different runs proceeds concurrently.
    const selecting = selectionTail.then(async () => {
      if (pause !== undefined && pause.now().getTime() < nextPassAt)
        return null;
      const activeRunIds = [...activeRuns];
      if (passEnd === undefined) {
        const end = await queries.findPassEnd(activeRunIds);
        if (end === undefined) {
          finishPass();
          return null;
        }
        passEnd = end;
      }
      const candidate = await queries.findNext({
        afterRunId,
        passEnd,
        activeRunIds,
      });
      if (candidate === undefined || candidate.id === passEnd) finishPass();
      else afterRunId = candidate.id;
      if (candidate === undefined) return null;
      activeRuns.add(candidate.id);
      return candidate;
    });
    selectionTail = selecting.then(
      () => undefined,
      () => undefined,
    );
    return selecting;
  }

  function release(runId: string): void {
    activeRuns.delete(runId);
  }

  return { select, release };
}
