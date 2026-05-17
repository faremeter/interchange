/**
 * Thrown by `harness.assertDeps()` when a `Dependencies` object carries a
 * `HarnessId` symbol that does not match the harness checking it. Used to
 * catch cross-harness contamination — e.g., wiring harness A's deps through
 * harness B's reactor — at the earliest test seam rather than letting the
 * mismatch surface later as a confusing routing failure.
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
