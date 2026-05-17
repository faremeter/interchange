import { describe, test, expect } from "bun:test";

import { setupHarness } from "./harness";
import { UnmatchedFetchError } from "./errors";

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

describe("per-call abort isolation", () => {
  test("two parked fetches each have independent abort handlers", async () => {
    // No matchers when fetches arrive → both park in waiting set. Aborting
    // A must reject only fA; fB remains parked until its matcher arrives.
    const harness = setupHarness();
    try {
      const acA = new AbortController();
      const acB = new AbortController();

      const fA = harness.deps.fetch("https://example/a", {
        signal: acA.signal,
      });
      const fB = harness.deps.fetch("https://example/b", {
        signal: acB.signal,
      });

      acA.abort();
      let aErr: unknown;
      try {
        await fA;
      } catch (err) {
        aErr = err;
      }
      expect(aErr).toBeInstanceOf(DOMException);
      if (!(aErr instanceof DOMException)) throw new Error("unreachable");
      expect(aErr.name).toBe("AbortError");

      // B should still be parked (not aborted). Aborting A must not have
      // caused B's signal to fire.
      expect(acB.signal.aborted).toBe(false);

      // Register a matcher for B → B resolves cleanly.
      const sB = harness.scenario.createStream();
      sB.enqueueAt(5, utf8("beta"));
      sB.closeAt(10);
      harness.scenario.whenRequestMatches(
        (req) => req.url === "https://example/b",
        sB,
      );
      await harness.advanceTo(20);
      const rB = await fB;
      expect(await drainResponse(rB)).toBe("beta");
    } finally {
      harness.dispose();
    }
  });

  test("aborting one parked fetch does not remove a second parked fetch from waiting set", async () => {
    // If the abort handler accidentally removed/affected B, then after A
    // aborts B should also be rejected — but it shouldn't be.
    const harness = setupHarness();
    try {
      const acA = new AbortController();
      const fA = harness.deps.fetch("https://example/a", {
        signal: acA.signal,
      });
      const fB = harness.deps.fetch("https://example/b");
      // Catch fA rejection so it doesn't leak unhandled.
      const fASettled = fA.catch((err: unknown) => err);

      acA.abort();
      const aErr = await fASettled;
      expect(aErr).toBeInstanceOf(DOMException);

      // fB must still be pending (waiting). Verify by attempting to attach
      // a then handler that resolves with a sentinel only when fB settles.
      let fBSettled = false;
      void fB.then(
        () => {
          fBSettled = true;
        },
        () => {
          fBSettled = true;
        },
      );
      // Yield microtasks; fB should still be unresolved.
      await new Promise<void>((r) => setTimeout(r, 5));
      expect(fBSettled).toBe(false);

      // Match B → it should now resolve cleanly.
      const sB = harness.scenario.createStream();
      sB.enqueueAt(5, utf8("beta"));
      sB.closeAt(10);
      harness.scenario.whenRequestMatches(() => true, sB);
      await harness.advanceTo(20);
      const rB = await fB;
      expect(await drainResponse(rB)).toBe("beta");
    } finally {
      harness.dispose();
    }
  });

  test("two waiting fetches, one aborted: quiescence error lists only the unaborted", async () => {
    // Confirms the abort handler removes A from the waiting set so it's
    // NOT included in UnmatchedFetchError thrown at quiescence.
    const harness = setupHarness();
    try {
      const acA = new AbortController();
      const fA = harness.deps.fetch("https://example/a", {
        signal: acA.signal,
      });
      const fB = harness.deps.fetch("https://example/b");
      const fASettled = fA.catch((err: unknown) => err);
      const fBSettled = fB.catch((err: unknown) => err);

      acA.abort();
      const aErr = await fASettled;
      expect(aErr).toBeInstanceOf(DOMException);

      let caught: unknown;
      try {
        await harness.run();
      } catch (err) {
        caught = err;
      }
      expect(caught).toBeInstanceOf(UnmatchedFetchError);
      if (!(caught instanceof UnmatchedFetchError))
        throw new Error("unreachable");
      expect(caught.waiting).toHaveLength(1);
      expect(caught.waiting[0]?.url).toBe("https://example/b");

      const bErr = await fBSettled;
      expect(bErr).toBeInstanceOf(UnmatchedFetchError);
    } finally {
      harness.dispose();
    }
  });
});
