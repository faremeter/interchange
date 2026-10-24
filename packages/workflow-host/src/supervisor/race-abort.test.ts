import { describe, expect, test } from "bun:test";
import { getEventListeners } from "node:events";

import { raceAbort } from "./race-abort";

describe("raceAbort", () => {
  test("settles with each wait and leaves no abort listener behind", async () => {
    const controller = new AbortController();
    for (const value of [1, 2, 3]) {
      expect(await raceAbort(controller.signal, Promise.resolve(value))).toBe(
        value,
      );
    }
    expect(getEventListeners(controller.signal, "abort")).toHaveLength(0);
  });

  test("settles with undefined once the signal aborts", async () => {
    const controller = new AbortController();
    const raced = raceAbort(
      controller.signal,
      Promise.withResolvers<number>().promise,
    );
    controller.abort();
    expect(await raced).toBeUndefined();
    expect(getEventListeners(controller.signal, "abort")).toHaveLength(0);
  });

  test("a wait that rejects leaves no abort listener behind", async () => {
    const controller = new AbortController();
    const cause = await raceAbort(
      controller.signal,
      Promise.reject(new Error("the wait failed")),
    ).then(
      () => undefined,
      (rejected: unknown) => rejected,
    );
    expect(cause).toBeInstanceOf(Error);
    expect(getEventListeners(controller.signal, "abort")).toHaveLength(0);
  });
});
