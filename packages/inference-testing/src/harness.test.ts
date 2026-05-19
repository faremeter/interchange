import { describe, test, expect } from "bun:test";

import { HarnessId, type Dependencies } from "@intx/inference";

import { setupHarness } from "./harness";
import { WrongHarnessError } from "./errors";

const utf8 = (s: string): Uint8Array => new TextEncoder().encode(s);

async function drainResponse(response: Response): Promise<string> {
  const body = response.body;
  if (body === null) throw new Error("response body is null");
  const reader = body.getReader();
  const decoder = new TextDecoder();
  let out = "";
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    if (value !== undefined) out += decoder.decode(value, { stream: true });
  }
  out += decoder.decode();
  return out;
}

describe("setupHarness", () => {
  test("deps.fetch is a function and carries the HarnessId tag", () => {
    const harness = setupHarness();
    try {
      expect(typeof harness.deps.fetch).toBe("function");
      expect(typeof harness.deps[HarnessId]).toBe("symbol");
    } finally {
      harness.dispose();
    }
  });

  test("fetch returns a Response whose body is the matched stream's body", async () => {
    const harness = setupHarness();
    try {
      const stream = harness.scenario.createStream();
      harness.scenario.whenRequestMatches(() => true, stream);
      stream.enqueueAt(10, utf8("event: hello\n\n"));
      stream.closeAt(20);

      const fetchPromise = harness.deps.fetch("https://example/test", {
        method: "POST",
      });
      await harness.clock.advanceTo(30);
      const response = await fetchPromise;

      expect(response.status).toBe(200);
      expect(response.headers.get("content-type")).toBe("text/event-stream");
      expect(await drainResponse(response)).toBe("event: hello\n\n");
    } finally {
      harness.dispose();
    }
  });

  test("dispose closes open streams and prevents further use", async () => {
    const harness = setupHarness();
    const stream = harness.scenario.createStream();
    harness.scenario.whenRequestMatches(() => true, stream);

    const fetchPromise = harness.deps.fetch("https://example/test");
    stream.enqueueAt(10, utf8("partial"));
    await harness.clock.advanceTo(15);
    const response = await fetchPromise;

    harness.dispose();

    expect(await drainResponse(response)).toBe("partial");

    expect(() => harness.scenario.createStream()).toThrow(/disposed/);
    expect(() =>
      harness.scenario.whenRequestMatches(() => true, stream),
    ).toThrow(/disposed/);
    expect(harness.deps.fetch("https://example/test")).rejects.toThrow(
      /disposed/,
    );
  });

  test("dispose is idempotent", () => {
    const harness = setupHarness();
    harness.dispose();
    expect(() => harness.dispose()).not.toThrow();
  });

  test("naturally closed streams are removed from the open set", async () => {
    const harness = setupHarness();
    try {
      const stream = harness.scenario.createStream();
      harness.scenario.whenRequestMatches(() => true, stream);
      stream.enqueueAt(10, utf8("done"));
      stream.closeAt(20);

      const fetchPromise = harness.deps.fetch("https://example/test");
      await harness.clock.advanceTo(30);
      const response = await fetchPromise;
      expect(await drainResponse(response)).toBe("done");

      harness.dispose();
    } finally {
      harness.dispose();
    }
  });

  test("assertDeps throws WrongHarnessError on a different harness's deps", () => {
    const a = setupHarness();
    const b = setupHarness();
    try {
      expect(() => a.assertDeps(b.deps)).toThrow(WrongHarnessError);
      expect(() => b.assertDeps(a.deps)).toThrow(WrongHarnessError);
      expect(() => a.assertDeps(a.deps)).not.toThrow();
      expect(() => b.assertDeps(b.deps)).not.toThrow();
    } finally {
      a.dispose();
      b.dispose();
    }
  });

  test("assertDeps surfaces expected and received symbols on the error", () => {
    const a = setupHarness();
    const b = setupHarness();
    try {
      let caught: unknown;
      try {
        a.assertDeps(b.deps);
      } catch (err) {
        caught = err;
      }
      expect(caught).toBeInstanceOf(WrongHarnessError);
      if (!(caught instanceof WrongHarnessError))
        throw new Error("unreachable");
      const aId = a.deps[HarnessId];
      const bId = b.deps[HarnessId];
      if (aId === undefined || bId === undefined) {
        throw new Error("harness deps missing HarnessId tag");
      }
      expect(caught.expected).toBe(aId);
      expect(caught.received).toBe(bId);
    } finally {
      a.dispose();
      b.dispose();
    }
  });

  test("assertDeps throws when [HarnessId] is missing entirely", () => {
    const a = setupHarness();
    try {
      const untagged: Dependencies = { fetch: a.deps.fetch };
      let caught: unknown;
      try {
        a.assertDeps(untagged);
      } catch (err) {
        caught = err;
      }
      expect(caught).toBeInstanceOf(WrongHarnessError);
      if (!(caught instanceof WrongHarnessError))
        throw new Error("unreachable");
      expect(caught.received).toBeUndefined();
    } finally {
      a.dispose();
    }
  });

  test("two harnesses do not share streams", () => {
    const a = setupHarness();
    const b = setupHarness();
    try {
      const streamA = a.scenario.createStream();
      const streamB = b.scenario.createStream();

      expect(() => b.scenario.whenRequestMatches(() => true, streamA)).toThrow(
        /was not minted by this harness/,
      );
      expect(() => a.scenario.whenRequestMatches(() => true, streamB)).toThrow(
        /was not minted by this harness/,
      );
    } finally {
      a.dispose();
      b.dispose();
    }
  });
});
