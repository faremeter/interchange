import { describe, test, expect } from "bun:test";
import { createClock } from "./clock";

describe("boundary semantics", () => {
  test("entry at exactly virtualMs fires; nested entry at virtualMs also fires", async () => {
    const clock = createClock();
    const order: string[] = [];
    clock.schedule(20, () => {
      order.push("A");
      clock.schedule(20, () => {
        order.push("B");
      });
    });
    await clock.advanceTo(20);
    expect(order).toEqual(["A", "B"]);
    expect(clock.now()).toBe(20);
  });

  test("entry at virtualMs+1 nested in entry at virtualMs stays queued", async () => {
    const clock = createClock();
    const order: string[] = [];
    clock.schedule(20, () => {
      order.push("A");
      clock.schedule(21, () => {
        order.push("B");
      });
    });
    await clock.advanceTo(20);
    expect(order).toEqual(["A"]);
    expect(clock.now()).toBe(20);
  });

  test("now is exactly virtualMs after advanceTo even with no entries scheduled", async () => {
    const clock = createClock();
    await clock.advanceTo(500);
    expect(clock.now()).toBe(500);
  });

  test("now is exactly virtualMs after advanceTo with last entry at smaller time", async () => {
    const clock = createClock();
    clock.schedule(10, () => {
      /* no-op */
    });
    await clock.advanceTo(100);
    expect(clock.now()).toBe(100);
  });
});
