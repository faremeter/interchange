// Tests for `scenario.whenRequestBodyMatches`. Body-aware predicates
// receive the buffered request body alongside the original `Request`,
// letting tests route by content when URL/headers don't distinguish
// parallel fetches.
//
// The scan-pass ordering contract these tests pin:
//   1. Sync `whenRequestMatches` matchers always evaluate first.
//   2. With a body-aware matcher registered, the harness buffers the
//      body of every still-waiting fetch and runs a follow-up scan.
//   3. `AmbiguousRequestError` for body-aware matchers is detected over
//      a fully-buffered waiting set, never as bodies arrive one at a
//      time.

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

describe("Scenario.matchedRequests", () => {
  test("returns fresh clones every call so bodies can be read repeatedly", async () => {
    // Each call must return clones whose bodies are independently
    // consumable — the route-time stored clone is only used as a source
    // for further `.clone()` calls, never consumed itself.
    const harness = setupHarness();
    try {
      const s = harness.scenario.createStream();
      harness.scenario.whenRequestMatches(() => true, s);
      s.enqueueAt(5, utf8("reply"));
      s.closeAt(10);

      const body = JSON.stringify({ marker: "abc" });
      const f = harness.deps.fetch("https://example/x", {
        method: "POST",
        body,
      });
      await harness.run();
      await f;

      const first = harness.scenario.matchedRequests();
      expect(first).toHaveLength(1);
      const firstEntry = first[0];
      if (firstEntry === undefined) throw new Error("unreachable");
      expect(await firstEntry.text()).toBe(body);

      // Second call must succeed even though the first call's clone
      // had its body fully consumed.
      const second = harness.scenario.matchedRequests();
      expect(second).toHaveLength(1);
      const secondEntry = second[0];
      if (secondEntry === undefined) throw new Error("unreachable");
      expect(await secondEntry.text()).toBe(body);

      // The two calls' clones must be different objects — mutating one
      // must not leak into the next call's clones.
      expect(firstEntry).not.toBe(secondEntry);
    } finally {
      harness.dispose();
    }
  });

  test("captures matched requests in match order across multiple routings", async () => {
    const harness = setupHarness();
    try {
      const sA = harness.scenario.createStream();
      const sB = harness.scenario.createStream();
      harness.scenario.whenRequestMatches((req) => req.url.endsWith("/a"), sA);
      harness.scenario.whenRequestMatches((req) => req.url.endsWith("/b"), sB);
      sA.enqueueAt(5, utf8("a"));
      sA.closeAt(10);
      sB.enqueueAt(5, utf8("b"));
      sB.closeAt(10);

      const fA = harness.deps.fetch("https://example/a", {
        method: "POST",
        body: "first",
      });
      const fB = harness.deps.fetch("https://example/b", {
        method: "POST",
        body: "second",
      });
      await harness.run();
      await Promise.all([fA, fB]);

      const all = harness.scenario.matchedRequests();
      expect(all).toHaveLength(2);
      // Match order is fetch arrival order — fA was the first to park.
      const [first, second] = all;
      if (first === undefined || second === undefined) {
        throw new Error("unreachable");
      }
      expect(first.url).toBe("https://example/a");
      expect(second.url).toBe("https://example/b");
      expect(await first.text()).toBe("first");
      expect(await second.text()).toBe("second");
    } finally {
      harness.dispose();
    }
  });

  test("returns an empty array when nothing has been matched yet", () => {
    const harness = setupHarness();
    try {
      expect(harness.scenario.matchedRequests()).toEqual([]);
    } finally {
      harness.dispose();
    }
  });
});

describe("Scenario.whenRequestBodyMatches", () => {
  test("routes two concurrent POSTs to different streams by body substring", async () => {
    // N agents POST to the same URL with the same headers; only the
    // seed message in the body (here, a task id) distinguishes them.
    const harness = setupHarness();
    try {
      const sGreet = harness.scenario.createStream();
      const sFormat = harness.scenario.createStream();

      harness.scenario.whenRequestBodyMatches(
        (body) => body.includes("1a-greet"),
        sGreet,
      );
      harness.scenario.whenRequestBodyMatches(
        (body) => body.includes("1b-format"),
        sFormat,
      );

      sGreet.enqueueAt(10, utf8("greet-reply"));
      sGreet.closeAt(20);
      sFormat.enqueueAt(10, utf8("format-reply"));
      sFormat.closeAt(20);

      const fGreet = harness.deps.fetch("https://example/chat/completions", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ task: "1a-greet", model: "x" }),
      });
      const fFormat = harness.deps.fetch("https://example/chat/completions", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ task: "1b-format", model: "x" }),
      });

      await harness.run();
      const [rGreet, rFormat] = await Promise.all([fGreet, fFormat]);
      expect(await drainResponse(rGreet)).toBe("greet-reply");
      expect(await drainResponse(rFormat)).toBe("format-reply");
    } finally {
      harness.dispose();
    }
  });

  test("body-aware predicate sees the same body via second argument as Request", async () => {
    // Sanity-check that the `Request` argument carries the same body the
    // text argument was derived from.
    const harness = setupHarness();
    try {
      const s = harness.scenario.createStream();
      let seenBody: string | undefined;
      let seenReqUrl: string | undefined;
      harness.scenario.whenRequestBodyMatches((body, req) => {
        seenBody = body;
        seenReqUrl = req.url;
        return body.includes("needle");
      }, s);

      s.enqueueAt(5, utf8("matched"));
      s.closeAt(10);

      const f = harness.deps.fetch("https://example/route", {
        method: "POST",
        body: "haystack-needle-haystack",
      });
      await harness.run();
      const r = await f;
      expect(await drainResponse(r)).toBe("matched");
      expect(seenBody).toBe("haystack-needle-haystack");
      expect(seenReqUrl).toBe("https://example/route");
    } finally {
      harness.dispose();
    }
  });

  test("sync matcher takes priority when registered before a body-aware matcher", async () => {
    // The sync scan pass runs first: if a sync matcher's predicate
    // accepts a fetch, the body-aware matcher never sees it — and the
    // body is never read.
    const harness = setupHarness();
    try {
      const sSync = harness.scenario.createStream();
      const sBody = harness.scenario.createStream();

      harness.scenario.whenRequestMatches(
        (req) => req.url.endsWith("/sync"),
        sSync,
      );
      let bodyAwareSawCall = false;
      harness.scenario.whenRequestBodyMatches((_body) => {
        bodyAwareSawCall = true;
        return true;
      }, sBody);

      sSync.enqueueAt(5, utf8("sync-reply"));
      sSync.closeAt(10);

      const f = harness.deps.fetch("https://example/sync", {
        method: "POST",
        body: "anything",
      });
      await harness.run();
      const r = await f;
      expect(await drainResponse(r)).toBe("sync-reply");
      expect(bodyAwareSawCall).toBe(false);
    } finally {
      harness.dispose();
    }
  });

  test("body-aware matcher routes a fetch the sync matchers missed", async () => {
    // Sync scan misses -> body-aware scan kicks in -> fetch is bound.
    const harness = setupHarness();
    try {
      const sBody = harness.scenario.createStream();
      // A sync matcher that never accepts this fetch.
      const sSync = harness.scenario.createStream();
      harness.scenario.whenRequestMatches(
        (req) => req.url.endsWith("/sync-only"),
        sSync,
      );
      harness.scenario.whenRequestBodyMatches(
        (body) => body.includes("special-marker"),
        sBody,
      );

      sBody.enqueueAt(5, utf8("body-aware-reply"));
      sBody.closeAt(10);

      const f = harness.deps.fetch("https://example/chat", {
        method: "POST",
        body: "carries the special-marker",
      });
      await harness.run();
      const r = await f;
      expect(await drainResponse(r)).toBe("body-aware-reply");
    } finally {
      harness.dispose();
    }
  });

  test("body-aware matcher that rejects the body leaves the fetch unmatched", async () => {
    // A predicate that returns false for everything leaves the fetch
    // parked; quiescence surfaces UnmatchedFetchError.
    const harness = setupHarness();
    try {
      const s = harness.scenario.createStream();
      harness.scenario.whenRequestBodyMatches(
        (body) => body.includes("never-present"),
        s,
      );
      s.enqueueAt(5, utf8("unused"));
      s.closeAt(10);

      const f = harness.deps.fetch("https://example/x", {
        method: "POST",
        body: "the marker is not here",
      });
      const fSettled = f.catch((err: unknown) => err);

      let runErr: unknown;
      try {
        await harness.run();
      } catch (err) {
        runErr = err;
      }
      expect(runErr).toBeInstanceOf(UnmatchedFetchError);
      const fErr = await fSettled;
      expect(fErr).toBeInstanceOf(UnmatchedFetchError);
    } finally {
      harness.dispose();
    }
  });

  test("AmbiguousRequestError fires when two body-aware fetches collide on one matcher", async () => {
    // Two POSTs both contain the marker the matcher looks for; the
    // scan reports the second as a same-matcher conflict, like the sync
    // scan does for URL-based matchers.
    const harness = setupHarness();
    try {
      const s = harness.scenario.createStream();
      s.enqueueAt(5, utf8("only-one"));
      s.closeAt(10);

      const f1 = harness.deps.fetch("https://example/a", {
        method: "POST",
        body: "carries marker-X",
      });
      const f2 = harness.deps.fetch("https://example/b", {
        method: "POST",
        body: "also carries marker-X",
      });
      const f1Settled = f1.catch((err: unknown) => err);
      const f2Settled = f2.catch((err: unknown) => err);

      // Register the body-aware matcher AFTER both fetches park, so a
      // single scan sees both at once.
      harness.scenario.whenRequestBodyMatches(
        (body) => body.includes("marker-X"),
        s,
      );

      // The ambiguity surfaces on the async body-aware scan. `run`
      // awaits the scan promise via the drain loop and rethrows.
      let runErr: unknown;
      try {
        await harness.run();
      } catch (err) {
        runErr = err;
      }
      expect(runErr).toBeInstanceOf(AmbiguousRequestError);
      const f1Err = await f1Settled;
      const f2Err = await f2Settled;
      expect(f1Err).toBeInstanceOf(AmbiguousRequestError);
      expect(f2Err).toBeInstanceOf(AmbiguousRequestError);
    } finally {
      harness.dispose();
    }
  });

  test("GET request with no body: body-aware predicate sees an empty string", async () => {
    // GETs (and other body-less request shapes) buffer to ""; a
    // predicate accepting "" routes the GET, one requiring content
    // rejects it.
    const harness = setupHarness();
    try {
      const sEmpty = harness.scenario.createStream();
      let seenBody: string | undefined;
      harness.scenario.whenRequestBodyMatches((body) => {
        seenBody = body;
        return body === "";
      }, sEmpty);

      sEmpty.enqueueAt(5, utf8("got-the-get"));
      sEmpty.closeAt(10);

      const f = harness.deps.fetch("https://example/health");
      await harness.run();
      const r = await f;
      expect(await drainResponse(r)).toBe("got-the-get");
      expect(seenBody).toBe("");
    } finally {
      harness.dispose();
    }
  });

  test("abort before run(): aborted fetch is removed from the waiting set before any body-aware scan runs", async () => {
    // The abort fires synchronously BEFORE `harness.run()` begins. The
    // queued body-aware scan's buffer snapshots the entry before its
    // first `await`; by the time `Promise.all` resumes, the pre-route
    // abort listener has settled and removed it, and the settled-flag
    // guards in the buffer path prevent a spurious read error. This
    // pins that body-aware matchers tolerate a pre-run abort cleanly.
    const harness = setupHarness();
    try {
      const s = harness.scenario.createStream();
      harness.scenario.whenRequestBodyMatches(() => true, s);
      s.enqueueAt(5, utf8("unused"));
      s.closeAt(10);

      const ac = new AbortController();
      const f = harness.deps.fetch("https://example/abortme", {
        method: "POST",
        body: "anything",
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

      // Quiescence must NOT throw; the aborted fetch was removed from
      // the waiting set, so the matcher just stays unconsumed.
      await harness.run();
    } finally {
      harness.dispose();
    }
  });

  test("multiple body-aware matchers preserve registration-order priority", async () => {
    // When two body-aware matchers could both accept a fetch, the
    // first-registered wins — same rule as sync matchers.
    const harness = setupHarness();
    try {
      const sFirst = harness.scenario.createStream();
      const sSecond = harness.scenario.createStream();

      harness.scenario.whenRequestBodyMatches(
        (body) => body.includes("common"),
        sFirst,
      );
      harness.scenario.whenRequestBodyMatches(
        (body) => body.includes("common"),
        sSecond,
      );

      sFirst.enqueueAt(5, utf8("first-wins"));
      sFirst.closeAt(10);
      sSecond.enqueueAt(5, utf8("second-fallback"));
      sSecond.closeAt(10);

      const f1 = harness.deps.fetch("https://example/a", {
        method: "POST",
        body: "common-1",
      });
      const f2 = harness.deps.fetch("https://example/b", {
        method: "POST",
        body: "common-2",
      });
      await harness.run();
      const [r1, r2] = await Promise.all([f1, f2]);
      expect(await drainResponse(r1)).toBe("first-wins");
      expect(await drainResponse(r2)).toBe("second-fallback");
    } finally {
      harness.dispose();
    }
  });

  test("rejects non-function predicate at the registration site", () => {
    const harness = setupHarness();
    try {
      const s = harness.scenario.createStream();
      expect(() =>
        harness.scenario.whenRequestBodyMatches(
          // Intentionally pass a non-function value to verify the
          // defensive type-system + runtime guard rejects it.
          // eslint-disable-next-line @typescript-eslint/no-explicit-any, @typescript-eslint/no-unsafe-type-assertion -- runtime guard exercise
          "not a function" as any,
          s,
        ),
      ).toThrow(/predicate must be a function/);
    } finally {
      harness.dispose();
    }
  });

  test("rejects a foreign response stream", () => {
    const harnessA = setupHarness();
    const harnessB = setupHarness();
    try {
      const foreign = harnessA.scenario.createStream();
      expect(() =>
        harnessB.scenario.whenRequestBodyMatches(() => true, foreign),
      ).toThrow(/was not minted by this harness/);
    } finally {
      harnessA.dispose();
      harnessB.dispose();
    }
  });

  test("rejects registration after dispose", () => {
    const harness = setupHarness();
    const s = harness.scenario.createStream();
    harness.dispose();
    expect(() =>
      harness.scenario.whenRequestBodyMatches(() => true, s),
    ).toThrow(/has been disposed/);
  });

  test("genuine body-read failure surfaces as the scan error, not as UnmatchedFetchError", async () => {
    // Body-read failures must surface rather than map to "" and hide
    // behind a downstream "fetch was not matched" diagnostic.
    // Synthesizing a real failure is unreliable on Bun (its
    // `clone().text()` is permissive), so override `clone` on the
    // request to throw deterministically; the harness must re-throw it
    // from `harness.run()` rather than coerce to an empty body.
    const harness = setupHarness();
    try {
      const s = harness.scenario.createStream();
      harness.scenario.whenRequestBodyMatches(() => true, s);
      s.enqueueAt(5, utf8("unused"));
      s.closeAt(10);

      const SENTINEL = "intentional-clone-failure";
      const req = new Request("https://example/x", {
        method: "POST",
        body: "payload",
      });
      Object.defineProperty(req, "clone", {
        value: () => {
          throw new Error(SENTINEL);
        },
      });

      // Attach the rejection-catcher BEFORE driving run() so the parked
      // fetch's eventual rejection (delivered by `dispose()` once run()
      // has thrown) does not surface as an unhandled rejection.
      const f = harness.deps.fetch(req);
      f.catch(() => undefined);

      let runErr: unknown;
      try {
        await harness.run();
      } catch (err) {
        runErr = err;
      }
      expect(runErr).toBeInstanceOf(Error);
      expect(runErr).not.toBeInstanceOf(UnmatchedFetchError);
      if (!(runErr instanceof Error)) throw new Error("unreachable");
      expect(runErr.message).toBe(SENTINEL);
    } finally {
      harness.dispose();
    }
  });

  test("body-aware scan does NOT fire when no body-aware matchers are registered", async () => {
    // Cost-isolation: sync-only tests pay zero body-read cost. A
    // BodyText-reading predicate is never invoked against a fetch that
    // bound on the sync pass (covered above).
    const harness = setupHarness();
    try {
      const s = harness.scenario.createStream();
      harness.scenario.whenRequestMatches(() => true, s);
      s.enqueueAt(5, utf8("sync-only"));
      s.closeAt(10);
      const f = harness.deps.fetch("https://example/x", {
        method: "POST",
        body: "not-read",
      });
      await harness.run();
      expect(await drainResponse(await f)).toBe("sync-only");
    } finally {
      harness.dispose();
    }
  });
});
