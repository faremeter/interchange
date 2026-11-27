import { describe, expect, test } from "bun:test";

import {
  beginDelivery,
  beginWork,
  createIdleWait,
  isQuiet,
  quietFor,
  waitIdle,
} from "./activity";

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

  test("counts from the end of the last work, not from its start or an earlier quiet time", () => {
    if (!isQuiet())
      throw new Error("an earlier test left work or a delivery running");
    const endWork = beginWork();
    const began = performance.now();
    // Work that takes no time cannot tell its start from its end. A
    // millisecond is far longer than the reads below take.
    let ending = performance.now();
    while (ending - began < 1) ending = performance.now();
    endWork();
    expect(quietFor()).toBeLessThanOrEqual(performance.now() - ending);
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

  test("is 0 until the last overlapping work ends, then counts from that end", () => {
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

  test("counts a run that parks through waitIdle from the park, not from an earlier quiet time", async () => {
    if (!isQuiet())
      throw new Error("an earlier test left work or a delivery running");
    const endWork = beginWork();
    // A millisecond of work, so counting from the quiet time before it fails.
    const began = performance.now();
    let working = performance.now();
    while (working - began < 1) working = performance.now();
    const settle = Promise.withResolvers<undefined>();
    let parkedAt = 0;
    const parked = waitIdle(() => {
      parkedAt = performance.now();
      return settle.promise;
    });
    expect(isQuiet()).toBe(true);
    expect(quietFor()).toBeLessThanOrEqual(performance.now() - parkedAt);
    settle.resolve(undefined);
    await parked;
    expect(quietFor()).toBe(0);
    endWork();
  });

  test("runs on across a delivery no run acts on, and starts over once one works", () => {
    if (!isQuiet())
      throw new Error("an earlier test left work or a delivery running");
    beginWork()();
    const quietBy = performance.now();
    let later = performance.now();
    while (later <= quietBy) later = performance.now();

    const endDelivery = beginDelivery();
    expect(quietFor()).toBe(0);
    endDelivery();
    expect(quietFor()).toBeGreaterThanOrEqual(later - quietBy);

    const endSecond = beginDelivery();
    const endWork = beginWork();
    endSecond();
    const beforeQuiet = performance.now();
    endWork();
    expect(quietFor()).toBeLessThanOrEqual(performance.now() - beforeQuiet);
  });

  test("is 0 for a run whose input arrived before it parked", async () => {
    if (!isQuiet())
      throw new Error("an earlier test left work or a delivery running");
    const endWork = beginWork();
    const idle = createIdleWait();
    const wake = idle.waker();
    wake();
    const waiting = idle.wait(Promise.resolve("input"), [wake]);
    expect(isQuiet()).toBe(false);
    expect(quietFor()).toBe(0);
    await waiting;
    expect(isQuiet()).toBe(false);
    expect(quietFor()).toBe(0);
    endWork();
  });
});
