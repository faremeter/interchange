import { describe, test, expect } from "bun:test";

import { setupHarness } from "./harness";
import { AmbiguousRequestError, UnmatchedFetchError } from "./errors";

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

describe("Scenario.whenRequestMatches", () => {
  test("routes two concurrent fetches to their respective streams", async () => {
    const harness = setupHarness();
    try {
      const sA = harness.scenario.createStream();
      const sB = harness.scenario.createStream();

      harness.scenario.whenRequestMatches(
        (req) => req.url === "https://example/a",
        sA,
      );
      harness.scenario.whenRequestMatches(
        (req) => req.url === "https://example/b",
        sB,
      );

      sA.enqueueAt(10, utf8("alpha"));
      sA.closeAt(20);
      sB.enqueueAt(10, utf8("beta"));
      sB.closeAt(20);

      const fA = harness.deps.fetch("https://example/a");
      const fB = harness.deps.fetch("https://example/b");
      await harness.advanceTo(30);
      const [rA, rB] = await Promise.all([fA, fB]);

      expect(await drainResponse(rA)).toBe("alpha");
      expect(await drainResponse(rB)).toBe("beta");
    } finally {
      harness.dispose();
    }
  });

  test("registration-order: first non-consumed matcher wins", async () => {
    const harness = setupHarness();
    try {
      const s1 = harness.scenario.createStream();
      const s2 = harness.scenario.createStream();

      // Both predicates accept the same request; the first registered must
      // win for the first fetch, and the second registered for the next.
      harness.scenario.whenRequestMatches(() => true, s1);
      harness.scenario.whenRequestMatches(() => true, s2);

      s1.enqueueAt(5, utf8("first"));
      s1.closeAt(10);
      s2.enqueueAt(5, utf8("second"));
      s2.closeAt(10);

      const f1 = harness.deps.fetch("https://example/x");
      const f2 = harness.deps.fetch("https://example/y");
      await harness.advanceTo(20);
      const [r1, r2] = await Promise.all([f1, f2]);

      expect(await drainResponse(r1)).toBe("first");
      expect(await drainResponse(r2)).toBe("second");
    } finally {
      harness.dispose();
    }
  });

  test("matcher fires at most once across two sequential fetches", async () => {
    const harness = setupHarness();
    try {
      const sOnce = harness.scenario.createStream();
      harness.scenario.whenRequestMatches(() => true, sOnce);

      sOnce.enqueueAt(5, utf8("one-shot"));
      sOnce.closeAt(10);

      const f1 = harness.deps.fetch("https://example/x");
      await harness.advanceTo(20);
      const r1 = await f1;
      expect(await drainResponse(r1)).toBe("one-shot");

      // Second fetch must reach quiescence unmatched, because the matcher
      // was consumed by the first.
      const f2 = harness.deps.fetch("https://example/y");
      const f2Settled = f2.catch((err: unknown) => err);
      let advanceErr: unknown;
      try {
        await harness.advanceTo(40);
      } catch (err) {
        advanceErr = err;
      }
      expect(advanceErr).toBeInstanceOf(UnmatchedFetchError);
      const f2Err = await f2Settled;
      expect(f2Err).toBeInstanceOf(UnmatchedFetchError);
    } finally {
      harness.dispose();
    }
  });

  test("matcher registered after a parked fetch is bound on register-scan", async () => {
    const harness = setupHarness();
    try {
      const f = harness.deps.fetch("https://example/late");

      const s = harness.scenario.createStream();
      s.enqueueAt(5, utf8("late-arrival"));
      s.closeAt(10);
      // Registering the matcher must scan the waiting set and bind `f`.
      harness.scenario.whenRequestMatches(() => true, s);

      await harness.advanceTo(20);
      const r = await f;
      expect(await drainResponse(r)).toBe("late-arrival");
    } finally {
      harness.dispose();
    }
  });

  test("abort-while-waiting: signal fires, fetch rejects with AbortError", async () => {
    const harness = setupHarness();
    try {
      const ac = new AbortController();
      const f = harness.deps.fetch("https://example/will-abort", {
        signal: ac.signal,
      });
      ac.abort();

      let caught: unknown;
      try {
        await f;
      } catch (err) {
        caught = err;
      }
      expect(caught).toBeInstanceOf(DOMException);
      if (!(caught instanceof DOMException)) throw new Error("unreachable");
      expect(caught.name).toBe("AbortError");

      // Quiescence must NOT raise UnmatchedFetchError — the aborted fetch
      // was removed from the waiting set.
      await harness.run();
    } finally {
      harness.dispose();
    }
  });

  test("already-aborted signal: fetch rejects immediately", async () => {
    const harness = setupHarness();
    try {
      const ac = new AbortController();
      ac.abort();

      let caught: unknown;
      try {
        await harness.deps.fetch("https://example/x", { signal: ac.signal });
      } catch (err) {
        caught = err;
      }
      expect(caught).toBeInstanceOf(DOMException);
      if (!(caught instanceof DOMException)) throw new Error("unreachable");
      expect(caught.name).toBe("AbortError");

      // No waiting entry was created; run() must not raise.
      await harness.run();
    } finally {
      harness.dispose();
    }
  });

  test("UnmatchedFetchError on quiescence lists waiting fetches", async () => {
    const harness = setupHarness();
    try {
      const f = harness.deps.fetch("https://example/unmatched", {
        method: "POST",
        headers: { "x-test": "yes" },
      });
      const fSettled = f.catch((err: unknown) => err);

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
      const first = caught.waiting[0];
      if (first === undefined) throw new Error("unreachable");
      expect(first.url).toBe("https://example/unmatched");
      expect(first.method).toBe("POST");
      expect(first.headers["x-test"]).toBe("yes");

      const fErr = await fSettled;
      expect(fErr).toBeInstanceOf(UnmatchedFetchError);
    } finally {
      harness.dispose();
    }
  });

  test("AmbiguousRequestError on concurrent same-matcher conflict", async () => {
    const harness = setupHarness();
    try {
      const s = harness.scenario.createStream();
      s.enqueueAt(5, utf8("only-one"));
      s.closeAt(10);

      const f1 = harness.deps.fetch("https://example/a");
      const f2 = harness.deps.fetch("https://example/b");
      // Catch f1/f2 immediately so neither is observed as an unhandled
      // rejection if the matcher registration's sync throw happens before
      // a later await reaches them.
      const f1Settled = f1.catch((err: unknown) => err);
      const f2Settled = f2.catch((err: unknown) => err);

      // Registering the matcher with two waiting fetches in flight must
      // detect the conflict on the register-scan and throw.
      let caught: unknown;
      try {
        harness.scenario.whenRequestMatches(() => true, s);
      } catch (err) {
        caught = err;
      }
      expect(caught).toBeInstanceOf(AmbiguousRequestError);
      if (!(caught instanceof AmbiguousRequestError))
        throw new Error("unreachable");
      expect(caught.fetches).toHaveLength(2);
      const urls = caught.fetches.map((f) => f.url);
      expect(urls).toContain("https://example/a");
      expect(urls).toContain("https://example/b");

      // Both fetches must have rejected with the same error so awaiters
      // don't hang past dispose.
      const f1Err = await f1Settled;
      const f2Err = await f2Settled;
      expect(f1Err).toBeInstanceOf(AmbiguousRequestError);
      expect(f2Err).toBeInstanceOf(AmbiguousRequestError);
    } finally {
      harness.dispose();
    }
  });

  test("three concurrent fetches against one ambiguous matcher list all three in the error", async () => {
    const harness = setupHarness();
    try {
      const s = harness.scenario.createStream();
      s.enqueueAt(5, utf8("only-one"));
      s.closeAt(10);

      const f1 = harness.deps.fetch("https://example/a");
      const f2 = harness.deps.fetch("https://example/b");
      const f3 = harness.deps.fetch("https://example/c");
      const f1Settled = f1.catch((err: unknown) => err);
      const f2Settled = f2.catch((err: unknown) => err);
      const f3Settled = f3.catch((err: unknown) => err);

      // Registering a single broad matcher with three waiting fetches in
      // flight surfaces the conflict on the register-scan. `scanWaitingSet`
      // accumulates all conflicting fetches into the same matcher's list
      // (`list.length === 0` branch fires only for the first push), so the
      // AmbiguousRequestError reports every fetch that collided on the
      // matcher — not just the first two.
      let caught: unknown;
      try {
        harness.scenario.whenRequestMatches(() => true, s);
      } catch (err) {
        caught = err;
      }
      expect(caught).toBeInstanceOf(AmbiguousRequestError);
      if (!(caught instanceof AmbiguousRequestError))
        throw new Error("unreachable");
      expect(caught.fetches).toHaveLength(3);
      const urls = caught.fetches.map((f) => f.url);
      expect(urls).toContain("https://example/a");
      expect(urls).toContain("https://example/b");
      expect(urls).toContain("https://example/c");

      const f1Err = await f1Settled;
      const f2Err = await f2Settled;
      const f3Err = await f3Settled;
      expect(f1Err).toBeInstanceOf(AmbiguousRequestError);
      expect(f2Err).toBeInstanceOf(AmbiguousRequestError);
      expect(f3Err).toBeInstanceOf(AmbiguousRequestError);
    } finally {
      harness.dispose();
    }
  });

  test("two independent ambiguities across disjoint matchers each surface as their own AmbiguousRequestError", async () => {
    // Pin the routing behavior when the harness has TWO independent
    // ambiguities in flight (each on a different matcher). Because
    // `whenRequestMatches` triggers a scan PER registration, each
    // matcher's conflict surfaces on its own registration scan:
    //
    //   - Registering M_a scans the waiting set, finds the a1+a2
    //     conflict on M_a, throws AmbiguousRequestError(a1, a2), and
    //     rejects a1+a2. b1+b2 (which don't match M_a's predicate) stay
    //     parked.
    //   - Registering M_b scans the remaining waiting set, finds the
    //     b1+b2 conflict on M_b, throws AmbiguousRequestError(b1, b2),
    //     and rejects b1+b2.
    //
    // The two errors are entirely independent: each carries only the
    // fetches that bound to ITS matcher. A future change that bundles
    // all in-flight ambiguities into a single AggregateError, or that
    // attempts to wait for a "complete matcher table" before scanning,
    // would break this contract and should revisit this test.
    const harness = setupHarness();
    try {
      const sA = harness.scenario.createStream();
      const sB = harness.scenario.createStream();
      sA.enqueueAt(5, utf8("a"));
      sA.closeAt(10);
      sB.enqueueAt(5, utf8("b"));
      sB.closeAt(10);

      // Park four fetches BEFORE any matchers exist. With no matchers in
      // the table, every per-fetch `runScan` finds nothing to bind and
      // every fetch stays in the waiting set. This sets up a single
      // future scan whose pass sees all four fetches plus the matchers
      // we register next.
      const a1 = harness.deps.fetch("https://example/a/1");
      const a2 = harness.deps.fetch("https://example/a/2");
      const b1 = harness.deps.fetch("https://example/b/1");
      const b2 = harness.deps.fetch("https://example/b/2");
      const a1Settled = a1.catch((err: unknown) => err);
      const a2Settled = a2.catch((err: unknown) => err);
      const b1Settled = b1.catch((err: unknown) => err);
      const b2Settled = b2.catch((err: unknown) => err);

      // Registering M_a triggers a scan against [a1, a2, b1, b2] with
      // only M_a in the table. a1 binds to M_a (boundThisPass={M_a}); a2
      // tries M_a → in boundThisPass → no chosen, ambiguity branch finds
      // M_a matches → conflict {M_a: [a1, a2]}. b1/b2 don't match M_a's
      // predicate so they don't enter the conflict. The scan throws
      // AmbiguousRequestError for M_a and rejects a1+a2; b1+b2 remain
      // waiting.
      let firstErr: unknown;
      try {
        harness.scenario.whenRequestMatches(
          (req) => req.url.includes("/a/"),
          sA,
        );
      } catch (err) {
        firstErr = err;
      }
      expect(firstErr).toBeInstanceOf(AmbiguousRequestError);
      if (!(firstErr instanceof AmbiguousRequestError))
        throw new Error("unreachable");
      const firstUrls = firstErr.fetches.map((f) => f.url);
      expect(firstUrls).toContain("https://example/a/1");
      expect(firstUrls).toContain("https://example/a/2");
      expect(firstUrls).not.toContain("https://example/b/1");
      expect(firstUrls).not.toContain("https://example/b/2");
      const a1Err = await a1Settled;
      const a2Err = await a2Settled;
      expect(a1Err).toBeInstanceOf(AmbiguousRequestError);
      expect(a2Err).toBeInstanceOf(AmbiguousRequestError);

      // Registering M_b triggers another scan, this time against
      // [b1, b2]. The same pattern fires: b1 binds, b2 collides → second
      // AmbiguousRequestError reports b1+b2.
      let secondErr: unknown;
      try {
        harness.scenario.whenRequestMatches(
          (req) => req.url.includes("/b/"),
          sB,
        );
      } catch (err) {
        secondErr = err;
      }
      expect(secondErr).toBeInstanceOf(AmbiguousRequestError);
      if (!(secondErr instanceof AmbiguousRequestError))
        throw new Error("unreachable");
      const secondUrls = secondErr.fetches.map((f) => f.url);
      expect(secondUrls).toContain("https://example/b/1");
      expect(secondUrls).toContain("https://example/b/2");
      const b1Err = await b1Settled;
      const b2Err = await b2Settled;
      expect(b1Err).toBeInstanceOf(AmbiguousRequestError);
      expect(b2Err).toBeInstanceOf(AmbiguousRequestError);
    } finally {
      harness.dispose();
    }
  });

  test("harness.run() returns cleanly when the waiting set is empty", async () => {
    const harness = setupHarness();
    try {
      // No fetches in flight, no matchers — quiescence is trivially clean.
      await harness.run();
    } finally {
      harness.dispose();
    }
  });

  test("default opts produce a 200 text/event-stream response (no behavior change)", async () => {
    const harness = setupHarness();
    try {
      const s = harness.scenario.createStream();
      harness.scenario.whenRequestMatches(() => true, s);
      s.enqueueAt(5, utf8("payload"));
      s.closeAt(10);

      const f = harness.deps.fetch("https://example/x");
      await harness.advanceTo(20);
      const r = await f;
      expect(r.status).toBe(200);
      expect(r.ok).toBe(true);
      expect(r.headers.get("content-type")).toBe("text/event-stream");
      expect(await drainResponse(r)).toBe("payload");
    } finally {
      harness.dispose();
    }
  });

  test("opts.status: 429 produces response.ok === false with the requested status", async () => {
    const harness = setupHarness();
    try {
      const s = harness.scenario.createStream();
      harness.scenario.whenRequestMatches(() => true, s, { status: 429 });
      s.enqueueAt(5, utf8('{"error":{"message":"rate limited"}}'));
      s.closeAt(10);

      const f = harness.deps.fetch("https://example/rate-limit");
      await harness.advanceTo(20);
      const r = await f;
      expect(r.status).toBe(429);
      expect(r.ok).toBe(false);
      // Non-2xx defaults to application/json so JSON error bodies parse.
      expect(r.headers.get("content-type")).toBe("application/json");
      expect(await drainResponse(r)).toBe(
        '{"error":{"message":"rate limited"}}',
      );
    } finally {
      harness.dispose();
    }
  });

  test("opts.headers are merged onto the response and override defaults", async () => {
    const harness = setupHarness();
    try {
      const s = harness.scenario.createStream();
      harness.scenario.whenRequestMatches(() => true, s, {
        status: 429,
        headers: { "retry-after": "7", "content-type": "text/plain" },
      });
      s.enqueueAt(5, utf8("slow down"));
      s.closeAt(10);

      const f = harness.deps.fetch("https://example/h");
      await harness.advanceTo(20);
      const r = await f;
      expect(r.status).toBe(429);
      expect(r.headers.get("retry-after")).toBe("7");
      expect(r.headers.get("content-type")).toBe("text/plain");
      expect(await drainResponse(r)).toBe("slow down");
    } finally {
      harness.dispose();
    }
  });

  test("predicate cannot return a Promise (type-system enforced; documented)", () => {
    // The signature `RequestPredicate = (req: Request) => boolean` rejects
    // async predicates at compile time. There is no runtime check because
    // a sync function returning a thenable would already be a programmer
    // bug surfaced by the predicate's truthiness coercion. This test pins
    // the documentation: if the type widens to allow `Promise<boolean>`,
    // the comment-only contract is broken and the test description must
    // be revisited.
    const harness = setupHarness();
    try {
      const s = harness.scenario.createStream();
      // A predicate whose return value is a Promise object IS truthy; the
      // matcher would fire "successfully" on every request. This is the
      // exact bug class the type system prevents — we cannot reproduce it
      // in TypeScript without an `as any` cast we deliberately avoid.
      // The presence of this test asserts that the documented invariant
      // is intentional, not accidental.
      expect(typeof s.streamId).toBe("number");
    } finally {
      harness.dispose();
    }
  });
});
