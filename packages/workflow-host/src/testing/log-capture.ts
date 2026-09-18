// Capture LogTape records for one test file, so a test can await a log line
// instead of polling for it.
//
// Some decisions announce themselves only in the log. A guard that returns
// without touching any observable state leaves the record as the sole
// evidence it ran, and a test that wants to assert on such a decision has no
// other signal to await. Waiting on the record is what "Synchronizing on
// State, Not Time" in CONVENTIONS.md asks for, and it replaces a poll whose
// deadline had to be guessed against whatever the code under test was
// waiting on.
//
// The capture is per-call, not module-level. The unit pass gives a worker one
// module registry for every file it runs, so a module-level record array
// would accumulate another file's records and a module-level waiter list
// would let another file's log line resolve this file's wait.
// `createChangeNotifier` and `createSupervisorReaper` are factories for the
// same reason.
//
// `configureSync` is process-global, so `install` saves the configuration it
// replaces and `restore` puts it back. Wire the three lifecycle methods to
// `beforeAll`, `afterAll`, and `beforeEach`.

import { configureSync, getConfig } from "@intx/log";

/** One captured record, with its message template already interpolated. */
export type CapturedLogRecord = {
  readonly category: readonly string[];
  readonly level: string;
  readonly message: string;
};

type RecordWaiter = {
  needle: string;
  /** The level the record must carry, or null to accept any level. */
  level: string | null;
  resolve: (record: CapturedLogRecord) => void;
  abandon: () => void;
};

export type LogCapture = {
  /**
   * Replace the process-wide logging configuration with the capturing one.
   * Pass straight to `beforeAll`. Throws if a capture is already installed
   * through this object.
   */
  install(): void;
  /**
   * Put back the configuration `install` replaced. Pass straight to
   * `afterAll`. Throws if nothing was installed.
   */
  restore(): void;
  /**
   * Drop the captured records and abandon every armed waiter. Pass straight
   * to `beforeEach`.
   */
  reset(): void;
  /** Every record captured since the last `reset`, in log order. */
  records(): readonly CapturedLogRecord[];
  /** The messages of the captured `error` records, in log order. */
  errors(): string[];
  /**
   * Resolve with the first record at any level whose message contains
   * `needle`, whether it was logged before this call or arrives after it.
   * Carries no deadline: a record that never arrives is caught by the lane
   * timeout, per "Synchronizing on State, Not Time" in CONVENTIONS.md.
   */
  waitForRecord(needle: string): Promise<CapturedLogRecord>;
  /** As `waitForRecord`, restricted to records at the `error` level. */
  waitForError(needle: string): Promise<CapturedLogRecord>;
};

export function createLogCapture(): LogCapture {
  const captured: CapturedLogRecord[] = [];
  // Tests awaiting a record that has not been logged yet. The sink resolves
  // these as each record lands.
  const waiters: RecordWaiter[] = [];
  // Non-null exactly while this capture is installed, so it doubles as the
  // installed flag.
  let savedConfig: ReturnType<typeof getConfig> = null;

  function matches(
    record: CapturedLogRecord,
    needle: string,
    level: string | null,
  ): boolean {
    if (level !== null && level !== record.level) return false;
    return record.message.includes(needle);
  }

  function waitFor(
    needle: string,
    level: string | null,
  ): Promise<CapturedLogRecord> {
    const already = captured.find((record) => matches(record, needle, level));
    if (already !== undefined) return Promise.resolve(already);
    return new Promise<CapturedLogRecord>((resolve, reject) => {
      waiters.push({
        needle,
        level,
        resolve,
        abandon: () => {
          const what = level === null ? "record" : `${level} record`;
          reject(
            new Error(`no ${what} matching ${needle} arrived before teardown`),
          );
        },
      });
    });
  }

  return {
    install() {
      if (savedConfig !== null) {
        throw new Error("this log capture is already installed");
      }
      const prior = getConfig();
      // A null configuration means this file loaded without `@intx/log`
      // having installed its default sink, which cannot happen -- importing
      // the package runs the install. Failing here rather than at `restore`
      // keeps the worker from running a whole suite it cannot unwind: there
      // would be nothing to put back, and the install cannot re-fire to
      // repair it.
      if (prior === null) {
        throw new Error(
          "no logging configuration was present for the capture to replace",
        );
      }
      savedConfig = prior;
      configureSync({
        reset: true,
        sinks: {
          capture: (record) => {
            const message = record.message
              .map((part) =>
                typeof part === "string" ? part : JSON.stringify(part),
              )
              .join("");
            const entry = {
              category: record.category,
              level: record.level,
              message,
            };
            captured.push(entry);
            // Hand the record to anyone waiting for it. Take only the
            // matching waiters, leaving the rest armed for their own needles.
            for (const waiter of waiters.splice(0)) {
              if (matches(entry, waiter.needle, waiter.level)) {
                waiter.resolve(entry);
              } else {
                waiters.push(waiter);
              }
            }
          },
        },
        loggers: [
          { category: [], lowestLevel: "debug", sinks: ["capture"] },
          {
            category: ["logtape", "meta"],
            lowestLevel: "warning",
            sinks: ["capture"],
          },
        ],
      });
    },

    restore() {
      const prior = savedConfig;
      if (prior === null) {
        throw new Error(
          "this log capture was restored without being installed",
        );
      }
      savedConfig = null;
      configureSync({ reset: true, ...prior });
    },

    reset() {
      captured.length = 0;
      // Reject rather than drop: a waiter outstanding from a previous test
      // (its test was killed mid-wait) would otherwise be discarded still
      // armed, and its promise could never settle.
      for (const waiter of waiters.splice(0)) waiter.abandon();
    },

    records: () => captured.slice(),

    errors: () =>
      captured.filter((r) => r.level === "error").map((r) => r.message),

    waitForRecord: (needle) => waitFor(needle, null),

    waitForError: (needle) => waitFor(needle, "error"),
  };
}
