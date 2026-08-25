import { describe, test, expect } from "bun:test";

import { runBodyThenCleanup } from "./run-body-then-cleanup";

describe("runBodyThenCleanup", () => {
  test("returns the body result and runs cleanup on the success path", async () => {
    let cleaned = false;
    const result = await runBodyThenCleanup(
      () => Promise.resolve("value"),
      () => {
        cleaned = true;
        return Promise.resolve();
      },
      () => {
        throw new Error("onCleanupError must not be called on the ok path");
      },
    );
    expect(result).toBe("value");
    expect(cleaned).toBe(true);
  });

  test("surfaces a cleanup failure when the body succeeded", async () => {
    const cleanupError = new Error("teardown failed");
    let notified = false;
    await expect(
      runBodyThenCleanup(
        () => Promise.resolve("value"),
        () => Promise.reject(cleanupError),
        () => {
          notified = true;
        },
      ),
    ).rejects.toBe(cleanupError);
    // onCleanupErrorAfterBodyError is only for the body-already-threw path.
    expect(notified).toBe(false);
  });

  test("surfaces the body error and still runs cleanup when the body threw", async () => {
    const bodyError = new Error("body failed");
    let cleaned = false;
    await expect(
      runBodyThenCleanup(
        () => Promise.reject(bodyError),
        () => {
          cleaned = true;
          return Promise.resolve();
        },
        () => {
          throw new Error("onCleanupError must not fire when cleanup succeeds");
        },
      ),
    ).rejects.toBe(bodyError);
    expect(cleaned).toBe(true);
  });

  test("lets the body error win and reports the cleanup failure when both throw", async () => {
    const bodyError = new Error("body failed");
    const cleanupError = new Error("teardown failed");
    const reported: unknown[] = [];
    await expect(
      runBodyThenCleanup(
        () => Promise.reject(bodyError),
        () => Promise.reject(cleanupError),
        (cause) => {
          reported.push(cause);
        },
      ),
    ).rejects.toBe(bodyError);
    // The cleanup failure is not lost -- it is reported for logging, just
    // not allowed to mask the primary body error.
    expect(reported).toEqual([cleanupError]);
  });
});
