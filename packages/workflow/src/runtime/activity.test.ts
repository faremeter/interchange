import { describe, expect, test } from "bun:test";

import { beginDelivery, beginWork, isQuiet, quietFor } from "./activity";

describe("quietFor", () => {
  test("is 0 while work or a delivery is in progress", () => {
    if (!isQuiet())
      throw new Error("an earlier test left work or a delivery running");
    const endWork = beginWork();
    expect(quietFor()).toBe(0);
    const endDelivery = beginDelivery();
    endWork();
    expect(quietFor()).toBe(0);
    endDelivery();
  });

  test("counts from the end of the last work, not from an earlier quiet time", () => {
    if (!isQuiet())
      throw new Error("an earlier test left work or a delivery running");
    const beforeWork = performance.now();
    beginWork()();
    const quiet = quietFor();
    expect(quiet).toBeLessThanOrEqual(performance.now() - beforeWork);
  });

  test("grows while the process stays quiet", () => {
    if (!isQuiet())
      throw new Error("an earlier test left work or a delivery running");
    beginWork()();
    const quietBy = performance.now();
    // Spin rather than sleep: the assertion needs only a later reading.
    let later = performance.now();
    while (later <= quietBy) later = performance.now();
    expect(quietFor()).toBeGreaterThanOrEqual(later - quietBy);
  });

  test("keeps counting while other work is still in progress when one ends", () => {
    if (!isQuiet())
      throw new Error("an earlier test left work or a delivery running");
    const endFirst = beginWork();
    const endSecond = beginWork();
    endFirst();
    expect(quietFor()).toBe(0);
    const beforeQuiet = performance.now();
    endSecond();
    expect(quietFor()).toBeLessThanOrEqual(performance.now() - beforeQuiet);
  });
});
