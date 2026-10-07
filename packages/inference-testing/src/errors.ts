/**
 * Thrown by `harness.assertDeps()` when a `Dependencies` object carries a
 * `HarnessId` symbol that does not match the harness checking it. Catches
 * cross-harness contamination at the earliest test seam.
 */
export class WrongHarnessError extends Error {
  readonly expected: symbol;
  readonly received: symbol | undefined;

  constructor(expected: symbol, received: symbol | undefined) {
    super(
      `fetch invoked with deps tagged for a different harness ` +
        `(expected ${String(expected)}, received ${String(received)})`,
    );
    this.name = "WrongHarnessError";
    this.expected = expected;
    this.received = received;
  }
}

/**
 * Describes a fetch still waiting on a matcher when the harness reached
 * quiescence. Fields are read off the constructed `Request` inside the
 * harness.
 */
export type UnmatchedFetchInfo = {
  readonly url: string;
  readonly method: string;
  readonly headers: Readonly<Record<string, string>>;
};

/**
 * Thrown by `harness.run()` / `harness.advanceTo()` when the virtual clock
 * reaches quiescence with one or more fetches still parked in the waiting
 * set. Carries enough detail about each waiting fetch to diagnose a
 * missing matcher registration.
 *
 * Aborted fetches are NOT included: abort settles them with an
 * `AbortError` and removes them from the waiting set before quiescence.
 */
export class UnmatchedFetchError extends Error {
  readonly waiting: readonly UnmatchedFetchInfo[];

  constructor(waiting: readonly UnmatchedFetchInfo[]) {
    super(formatUnmatched(waiting));
    this.name = "UnmatchedFetchError";
    this.waiting = waiting;
  }
}

function formatUnmatched(waiting: readonly UnmatchedFetchInfo[]): string {
  const lines = waiting.map((w) => `  - ${w.method} ${w.url}`);
  return (
    `Harness reached quiescence with ${String(waiting.length)} unmatched ` +
    `fetch(es); register a matcher via scenario.whenRequestMatches(...) ` +
    `before driving the clock:\n${lines.join("\n")}`
  );
}

/**
 * Describes one of the conflicting fetches in an `AmbiguousRequestError`.
 */
export type AmbiguousFetchInfo = {
  readonly url: string;
  readonly method: string;
};

/**
 * Thrown when two or more concurrently-waiting fetches all bind to the same
 * unconsumed matcher on a single scan pass. Each matcher is single-use, so
 * the scan cannot decide which fetch deserves the response — register more
 * matchers or differentiate the predicates.
 */
export class AmbiguousRequestError extends Error {
  readonly fetches: readonly AmbiguousFetchInfo[];
  readonly matcherSource: string | undefined;

  constructor(
    fetches: readonly AmbiguousFetchInfo[],
    matcherSource: string | undefined,
  ) {
    super(formatAmbiguous(fetches, matcherSource));
    this.name = "AmbiguousRequestError";
    this.fetches = fetches;
    this.matcherSource = matcherSource;
  }
}

function formatAmbiguous(
  fetches: readonly AmbiguousFetchInfo[],
  matcherSource: string | undefined,
): string {
  const lines = fetches.map((f) => `  - ${f.method} ${f.url}`);
  const sourceSuffix =
    matcherSource !== undefined
      ? ` (matcher registered at ${matcherSource})`
      : "";
  return (
    `${String(fetches.length)} concurrently-waiting fetches all bound to ` +
    `the same single-use matcher${sourceSuffix}; register additional ` +
    `matchers or differentiate the predicates:\n${lines.join("\n")}`
  );
}
