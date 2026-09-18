import { describe, test, expect, afterEach } from "bun:test";

import { getLogger } from "@intx/log";

import { createLogCapture, type LogCapture } from "./log-capture";

const logger = getLogger(["workflow-host", "testing", "log-capture-test"]);

// Every capture this file installed and has not restored. `configureSync` is
// process-global, so a capture a failed assertion left installed would go on
// swallowing records for the rest of this worker's files.
const installed: LogCapture[] = [];

function startCapture(): LogCapture {
  const capture = createLogCapture();
  capture.install();
  installed.push(capture);
  return capture;
}

function stopCapture(capture: LogCapture): void {
  const at = installed.indexOf(capture);
  if (at === -1) throw new Error("this capture was not started by this file");
  installed.splice(at, 1);
  capture.restore();
}

afterEach(() => {
  // Innermost first: each capture saved the configuration the one before it
  // installed, so restoring out of order would reinstate a dead sink.
  for (const capture of installed.splice(0).reverse()) capture.restore();
});

describe("createLogCapture", () => {
  test("each capture holds only the records logged while it was installed", () => {
    const first = startCapture();
    logger.info`first-window`;
    stopCapture(first);

    const second = startCapture();
    logger.info`second-window`;
    stopCapture(second);

    // Two captures from one factory share nothing. A module-level record
    // array would put both messages in both lists, which is the failure this
    // pins: the unit pass gives a worker one module registry across files, so
    // shared state here would mix files together as readily as it mixes these
    // two captures.
    expect(first.records().map((r) => r.message)).toContain("first-window");
    expect(first.records().map((r) => r.message)).not.toContain(
      "second-window",
    );
    expect(second.records().map((r) => r.message)).toContain("second-window");
    expect(second.records().map((r) => r.message)).not.toContain(
      "first-window",
    );
  });

  test("a waiter resolves on a record that is not an error", async () => {
    const capture = startCapture();
    const waited = capture.waitForRecord("respawn backoff elapsed");
    logger.info`respawn backoff elapsed for a superseded cohort`;
    const record = await waited;
    expect(record.level).toBe("info");
    expect(record.message).toBe(
      "respawn backoff elapsed for a superseded cohort",
    );
  });

  test("a record logged before the wait still resolves it", async () => {
    const capture = startCapture();
    logger.info`already logged`;
    // Nothing further is logged, so only the scan of the already-captured
    // records can settle this. A purely edge-triggered waiter deadlocks here,
    // which is what makes a caller have to arm the wait before the code under
    // test runs -- and that ordering is not always available.
    const record = await capture.waitForRecord("already logged");
    expect(record.message).toBe("already logged");
  });

  test("a waiter whose needle does not match stays armed", async () => {
    const capture = startCapture();
    const wantsA = capture.waitForRecord("needle-a");
    const wantsB = capture.waitForRecord("needle-b");
    let bSettled = false;
    void wantsB.then(() => {
      bSettled = true;
    });

    logger.info`needle-a arrived first`;
    // Awaiting `wantsA` yields a microtask turn, which is all a wrongly
    // resolved `wantsB` would need to have run its continuation.
    expect((await wantsA).message).toBe("needle-a arrived first");
    expect(bSettled).toBe(false);

    logger.info`needle-b arrived second`;
    expect((await wantsB).message).toBe("needle-b arrived second");
  });

  test("waitForError ignores a matching record at another level", async () => {
    const capture = startCapture();
    const waited = capture.waitForError("cohort down");
    let settled = false;
    void waited.then(() => {
      settled = true;
    });

    logger.warn`cohort down, but only a warning`;
    await Promise.resolve();
    expect(settled).toBe(false);

    logger.error`cohort down for good`;
    const record = await waited;
    expect(record.level).toBe("error");
    expect(capture.errors()).toEqual(["cohort down for good"]);
  });

  test("reset clears the records and rejects an armed waiter", async () => {
    const capture = startCapture();
    logger.info`before the reset`;
    const abandoned = capture.waitForRecord("never-logged");

    capture.reset();
    expect(capture.records()).toEqual([]);
    // Rejected rather than dropped: a waiter carried into the next test could
    // never settle, and the test that awaited it would hang until the lane
    // timeout rather than report what it was waiting for.
    await expect(abandoned).rejects.toThrow(/never-logged/);
  });

  test("a record arriving after reset satisfies a wait, so needles must be unique per test", async () => {
    const capture = startCapture();

    // Stands in for work a previous test left running: `reset` marks the
    // boundary, and this record lands on the far side of it, exactly as a
    // fire-and-forget teardown's does. Nothing in the record says which test
    // caused it, so the wait below cannot refuse it.
    capture.reset();
    logger.info`teardown finished for the previous subject`;

    await capture.waitForRecord("teardown finished");

    // That is why `waitForRecord`'s contract puts the burden on the caller:
    // the needle has to be unique to its own test, and a test that starts
    // fire-and-forget work has to await that work before it returns. This
    // test pins the hazard those two rules exist for, so a change that
    // claims to remove them has something to contradict.
    expect(capture.records()).toHaveLength(1);
  });

  test("installing a capture twice throws", () => {
    const capture = startCapture();
    expect(() => capture.install()).toThrow(/already installed/);
  });

  test("restoring a capture that was never installed throws", () => {
    const capture = createLogCapture();
    expect(() => capture.restore()).toThrow(/without being installed/);
  });
});
