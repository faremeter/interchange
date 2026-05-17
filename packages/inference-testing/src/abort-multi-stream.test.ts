// Regression coverage for per-call abort isolation and sync-throw stream
// cleanup at N>=3 and N>=2 streams respectively. The shipped abort.test.ts
// covers the n=2 isolation case and the n=1 sync-throw case. These probes
// guard against a future change that handles "the matched stream" via a
// shared singleton instead of a per-stream listener / per-stream registry
// entry.

import { describe, test, expect } from "bun:test";

import { setupHarness } from "./harness";

const utf8 = (s: string): Uint8Array => new TextEncoder().encode(s);
const decode = (b: Uint8Array | undefined): string =>
  new TextDecoder().decode(b);

describe("per-call abort isolation across three concurrent streams", () => {
  test("aborting B does not affect A or C", async () => {
    const harness = setupHarness();
    try {
      const acA = new AbortController();
      const acB = new AbortController();
      const acC = new AbortController();
      const sA = harness.scenario.createStream();
      const sB = harness.scenario.createStream();
      const sC = harness.scenario.createStream();

      for (const [s, prefix] of [
        [sA, "A"],
        [sB, "B"],
        [sC, "C"],
      ] as const) {
        s.enqueueAt(5, utf8(`${prefix}-1`));
        s.enqueueAt(15, utf8(`${prefix}-2`));
        s.closeAt(20);
      }

      harness.scenario.whenRequestMatches((r) => r.url.endsWith("/a"), sA);
      harness.scenario.whenRequestMatches((r) => r.url.endsWith("/b"), sB);
      harness.scenario.whenRequestMatches((r) => r.url.endsWith("/c"), sC);

      const fA = harness.deps.fetch("https://x/a", { signal: acA.signal });
      const fB = harness.deps.fetch("https://x/b", { signal: acB.signal });
      const fC = harness.deps.fetch("https://x/c", { signal: acC.signal });
      const [rA, rB, rC] = await Promise.all([fA, fB, fC]);

      const bodyA = rA.body;
      const bodyB = rB.body;
      const bodyC = rC.body;
      if (bodyA === null || bodyB === null || bodyC === null) {
        throw new Error("body null");
      }
      const readerA = bodyA.getReader();
      const readerB = bodyB.getReader();
      const readerC = bodyC.getReader();

      await harness.advanceTo(10);
      expect(decode((await readerA.read()).value)).toBe("A-1");
      expect(decode((await readerB.read()).value)).toBe("B-1");
      expect(decode((await readerC.read()).value)).toBe("C-1");

      acB.abort();
      expect(acA.signal.aborted).toBe(false);
      expect(acC.signal.aborted).toBe(false);

      let bErr: unknown;
      try {
        await readerB.read();
      } catch (e) {
        bErr = e;
      }
      expect(bErr).toBeInstanceOf(DOMException);

      await harness.advanceTo(20);
      expect(decode((await readerA.read()).value)).toBe("A-2");
      expect(decode((await readerC.read()).value)).toBe("C-2");
      expect((await readerA.read()).done).toBe(true);
      expect((await readerC.read()).done).toBe(true);
    } finally {
      harness.dispose();
    }
  });
});

describe("sync-throw stream cleanup with multiple open streams", () => {
  test("two simultaneously-open streams both get errored", async () => {
    const harness = setupHarness();
    try {
      const sA = harness.scenario.createStream();
      const sB = harness.scenario.createStream();
      sA.enqueueAt(5, utf8("a1"));
      sB.enqueueAt(5, utf8("b1"));
      harness.scenario.whenRequestMatches((r) => r.url.endsWith("/a"), sA);
      harness.scenario.whenRequestMatches((r) => r.url.endsWith("/b"), sB);
      const [rA, rB] = await Promise.all([
        harness.deps.fetch("https://x/a"),
        harness.deps.fetch("https://x/b"),
      ]);
      const bodyA = rA.body;
      const bodyB = rB.body;
      if (bodyA === null || bodyB === null) {
        throw new Error("body null");
      }
      const readerA = bodyA.getReader();
      const readerB = bodyB.getReader();

      await harness.advanceTo(7);
      expect(decode((await readerA.read()).value)).toBe("a1");
      expect(decode((await readerB.read()).value)).toBe("b1");

      harness.clock.schedule(10, () => {
        throw new Error("boom-everywhere");
      });

      let advanceErr: unknown;
      try {
        await harness.advanceTo(15);
      } catch (e) {
        advanceErr = e;
      }
      expect(advanceErr).toBeInstanceOf(Error);

      let aErr: unknown;
      let bErr: unknown;
      try {
        await readerA.read();
      } catch (e) {
        aErr = e;
      }
      try {
        await readerB.read();
      } catch (e) {
        bErr = e;
      }
      expect(aErr).toBeInstanceOf(Error);
      expect(bErr).toBeInstanceOf(Error);
      if (!(aErr instanceof Error)) throw new Error("unreachable");
      if (!(bErr instanceof Error)) throw new Error("unreachable");
      expect(aErr.message).toMatch(/boom-everywhere/);
      expect(bErr.message).toMatch(/boom-everywhere/);
    } finally {
      harness.dispose();
    }
  });
});
