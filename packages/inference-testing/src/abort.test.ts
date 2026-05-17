import { describe, test, expect } from "bun:test";

import { setupHarness } from "./harness";

const utf8 = (s: string): Uint8Array => new TextEncoder().encode(s);

async function expectAbortError(p: Promise<unknown>): Promise<DOMException> {
  let caught: unknown;
  try {
    await p;
  } catch (err) {
    caught = err;
  }
  expect(caught).toBeInstanceOf(DOMException);
  if (!(caught instanceof DOMException)) throw new Error("unreachable");
  expect(caught.name).toBe("AbortError");
  return caught;
}

describe("scenario.abortAt", () => {
  test("fires controller.abort() at the scheduled virtual time", async () => {
    const harness = setupHarness();
    try {
      const ac = new AbortController();
      const stream = harness.scenario.createStream();
      stream.enqueueAt(10, utf8("before-abort"));
      stream.enqueueAt(40, utf8("after-abort"));
      stream.closeAt(50);
      harness.scenario.whenRequestMatches(() => true, stream);

      harness.scenario.abortAt(25, ac);

      const fetchPromise = harness.deps.fetch("https://example/x", {
        signal: ac.signal,
      });
      // The matcher binds eagerly on the register-scan, so the response
      // resolves once the next runScan trigger fires; advance until the
      // abort lands and verify what arrived.
      await harness.advanceTo(20);
      const response = await fetchPromise;
      expect(ac.signal.aborted).toBe(false);

      // The reader is now consuming. Advance past the abort time; the
      // controller should be errored and subsequent reads must reject.
      const body = response.body;
      if (body === null) throw new Error("response body is null");
      const reader = body.getReader();
      const decoder = new TextDecoder();
      const first = await reader.read();
      expect(first.done).toBe(false);
      expect(decoder.decode(first.value)).toBe("before-abort");

      // Advance through the abort time. The matched-stream controller
      // is errored synchronously when the abort signal fires (per-call
      // abort isolation hook in `routeWaitingFetch`).
      await harness.advanceTo(30);
      expect(ac.signal.aborted).toBe(true);

      const abortErr = await expectAbortError(reader.read());
      expect(abortErr.name).toBe("AbortError");
    } finally {
      harness.dispose();
    }
  });

  test("chunks scheduled after the abort time do not appear", async () => {
    const harness = setupHarness();
    try {
      const ac = new AbortController();
      const stream = harness.scenario.createStream();
      stream.enqueueAt(5, utf8("delivered"));
      stream.enqueueAt(50, utf8("never"));
      stream.closeAt(60);
      harness.scenario.whenRequestMatches(() => true, stream);

      harness.scenario.abortAt(20, ac);

      const fetchPromise = harness.deps.fetch("https://example/x", {
        signal: ac.signal,
      });
      const response = await fetchPromise;
      const body = response.body;
      if (body === null) throw new Error("response body is null");
      const reader = body.getReader();

      await harness.advanceTo(10);
      const decoder = new TextDecoder();
      const first = await reader.read();
      expect(decoder.decode(first.value)).toBe("delivered");

      // Advance past the abort time. The per-call abort isolation
      // listener cancels every pending chunk on this stream when the
      // signal fires, so the t=50 chunk and t=60 close are no-ops; no
      // "enqueue after terminal state" throw escapes advanceTo.
      await harness.advanceTo(60);
      expect(ac.signal.aborted).toBe(true);
      const abortErr = await expectAbortError(reader.read());
      expect(abortErr.name).toBe("AbortError");
    } finally {
      harness.dispose();
    }
  });

  test("rejects when controller is not an AbortController", () => {
    const harness = setupHarness();
    try {
      expect(() =>
        // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- deliberately wrong type to exercise the guard
        harness.scenario.abortAt(10, {} as unknown as AbortController),
      ).toThrow(/AbortController/);
    } finally {
      harness.dispose();
    }
  });
});

describe("scenario.abortAfter (wire-event predicate)", () => {
  test("fires controller.abort() after a chunk matching the predicate is delivered", async () => {
    const harness = setupHarness();
    try {
      const ac = new AbortController();
      const decoder = new TextDecoder();
      const stream = harness.scenario.createStream();
      stream.enqueueAt(5, utf8("event: open\n\n"));
      stream.enqueueAt(15, utf8("event: trigger\n\n"));
      stream.enqueueAt(30, utf8("event: tail\n\n"));
      stream.closeAt(40);
      harness.scenario.whenRequestMatches(() => true, stream);

      harness.scenario.abortAfter((ev) => {
        const text = decoder.decode(ev.bytes);
        return text.includes("trigger");
      }, ac);

      const fetchPromise = harness.deps.fetch("https://example/x", {
        signal: ac.signal,
      });
      const response = await fetchPromise;
      const body = response.body;
      if (body === null) throw new Error("response body is null");
      const reader = body.getReader();

      // Drive the clock to t=5 and drain the first chunk before the
      // trigger chunk fires. This pins what the consumer sees BEFORE
      // the abort: bytes already delivered (read off the reader before
      // the controller errors) are durable.
      await harness.advanceTo(5);
      const first = await reader.read();
      expect(decoder.decode(first.value)).toBe("event: open\n\n");

      // Advance past the trigger chunk. The trigger fires, onChunkFired
      // runs, predicate matches, abort fires, the per-call isolation
      // listener cancels the tail chunk and close, and errors the
      // controller. Reads after this point reject with AbortError.
      await harness.advanceTo(40);
      expect(ac.signal.aborted).toBe(true);

      const abortErr = await expectAbortError(reader.read());
      expect(abortErr.name).toBe("AbortError");
    } finally {
      harness.dispose();
    }
  });

  test("matcher fires at most once; later matching chunks are ignored", async () => {
    const harness = setupHarness();
    try {
      const ac = new AbortController();
      let abortCount = 0;
      ac.signal.addEventListener("abort", () => {
        abortCount += 1;
      });
      const stream = harness.scenario.createStream();
      stream.enqueueAt(5, utf8("match"));
      stream.enqueueAt(10, utf8("match"));
      stream.closeAt(20);
      harness.scenario.whenRequestMatches(() => true, stream);

      const decoder = new TextDecoder();
      harness.scenario.abortAfter(
        (ev) => decoder.decode(ev.bytes) === "match",
        ac,
      );

      const fetchPromise = harness.deps.fetch("https://example/x", {
        signal: ac.signal,
      });
      await fetchPromise;

      // First match at t=5 aborts; the abort listener cancels the t=10
      // and t=20 entries, so advanceTo completes without re-firing the
      // matcher. The abort event listener counts exactly one call.
      await harness.advanceTo(20);
      expect(abortCount).toBe(1);
    } finally {
      harness.dispose();
    }
  });
});

describe("per-call abort isolation (matched stream)", () => {
  test("aborting one fetch's signal errors only its stream", async () => {
    const harness = setupHarness();
    try {
      const acA = new AbortController();
      const acB = new AbortController();

      const sA = harness.scenario.createStream();
      const sB = harness.scenario.createStream();
      sA.enqueueAt(5, utf8("alpha-1"));
      sA.enqueueAt(50, utf8("alpha-2"));
      sA.closeAt(60);
      sB.enqueueAt(5, utf8("beta-1"));
      sB.enqueueAt(20, utf8("beta-2"));
      sB.closeAt(30);

      harness.scenario.whenRequestMatches(
        (req) => req.url === "https://example/a",
        sA,
      );
      harness.scenario.whenRequestMatches(
        (req) => req.url === "https://example/b",
        sB,
      );

      const fA = harness.deps.fetch("https://example/a", {
        signal: acA.signal,
      });
      const fB = harness.deps.fetch("https://example/b", {
        signal: acB.signal,
      });
      const [rA, rB] = await Promise.all([fA, fB]);
      const bodyA = rA.body;
      if (bodyA === null) throw new Error("response A body is null");
      const bodyB = rB.body;
      if (bodyB === null) throw new Error("response B body is null");
      const readerA = bodyA.getReader();
      const readerB = bodyB.getReader();

      // Drive past the first chunk for each stream.
      await harness.advanceTo(10);
      const decoder = new TextDecoder();
      const a1 = await readerA.read();
      expect(decoder.decode(a1.value)).toBe("alpha-1");
      const b1 = await readerB.read();
      expect(decoder.decode(b1.value)).toBe("beta-1");

      // Abort A. The matched-stream listener cancels A's pending chunks
      // and errors A's controller. B must be unaffected.
      acA.abort();

      const aErr = await expectAbortError(readerA.read());
      expect(aErr.name).toBe("AbortError");
      expect(acB.signal.aborted).toBe(false);

      // Drive B to completion. A's pending chunks were cancelled by
      // the abort listener, so advanceTo runs cleanly — no spurious
      // close on B and no enqueue-after-terminal throws from A.
      await harness.advanceTo(30);
      const b2 = await readerB.read();
      expect(decoder.decode(b2.value)).toBe("beta-2");
      const bDone = await readerB.read();
      expect(bDone.done).toBe(true);
    } finally {
      harness.dispose();
    }
  });
});

describe("harness.abortBefore(streamId)", () => {
  test("cancels pending chunks so they do not appear in the body", async () => {
    const harness = setupHarness();
    try {
      const stream = harness.scenario.createStream();
      // Schedule chunks at t+10, t+20, t+30 (relative to t=0).
      stream.enqueueAt(10, utf8("ten"));
      stream.enqueueAt(20, utf8("twenty"));
      stream.enqueueAt(30, utf8("thirty"));
      stream.closeAt(40);
      harness.scenario.whenRequestMatches(() => true, stream);

      const fetchPromise = harness.deps.fetch("https://example/x");
      const response = await fetchPromise;
      const body = response.body;
      if (body === null) throw new Error("response body is null");
      const reader = body.getReader();

      // Tool-handler-style: a scheduled callback at t=5 calls
      // abortBefore on its own stream, cancelling the t=10/t=20/t=30
      // enqueues and the t=40 close ahead of when they would have
      // fired. The seq-ordering guarantee: cancelled callbacks pop
      // off the heap as no-ops, so the body never sees them.
      let aborted = false;
      harness.clock.schedule(5, () => {
        harness.abortBefore(stream.streamId);
        aborted = true;
      });

      await harness.advanceTo(40);
      expect(aborted).toBe(true);

      const result = await expectAbortError(reader.read());
      expect(result.name).toBe("AbortError");
    } finally {
      harness.dispose();
    }
  });

  test("rejects when streamId was not minted by this harness", () => {
    const harnessA = setupHarness();
    const harnessB = setupHarness();
    try {
      const streamA = harnessA.scenario.createStream();
      expect(() => harnessB.abortBefore(streamA.streamId)).toThrow(
        /no stream with id/,
      );
    } finally {
      harnessA.dispose();
      harnessB.dispose();
    }
  });

  test("rejects when the stream is already in a terminal state", async () => {
    const harness = setupHarness();
    try {
      const stream = harness.scenario.createStream();
      stream.closeAt(5);
      harness.scenario.whenRequestMatches(() => true, stream);
      const fetchPromise = harness.deps.fetch("https://example/x");
      await harness.advanceTo(10);
      await fetchPromise;
      expect(() => harness.abortBefore(stream.streamId)).toThrow(
        /already in a terminal state/,
      );
    } finally {
      harness.dispose();
    }
  });
});

describe("scenario.abortAt / abortAfter: disposed-state guards", () => {
  test("abortAt throws a disposed-naming error after harness.dispose()", () => {
    const harness = setupHarness();
    harness.dispose();
    const ac = new AbortController();
    expect(() => harness.scenario.abortAt(10, ac)).toThrow(/disposed/);
  });

  test("abortAfter throws a disposed-naming error after harness.dispose()", () => {
    const harness = setupHarness();
    harness.dispose();
    const ac = new AbortController();
    expect(() => harness.scenario.abortAfter(() => true, ac)).toThrow(
      /disposed/,
    );
  });
});

describe("sync-throw stream cleanup via onSyncCallbackError", () => {
  test("a throwing scheduled callback errors every open simulated stream", async () => {
    const harness = setupHarness();
    try {
      const stream = harness.scenario.createStream();
      stream.enqueueAt(5, utf8("first"));
      stream.enqueueAt(20, utf8("would-be-second"));
      harness.scenario.whenRequestMatches(() => true, stream);
      const fetchPromise = harness.deps.fetch("https://example/x");
      const response = await fetchPromise;
      const body = response.body;
      if (body === null) throw new Error("response body is null");
      const reader = body.getReader();

      // Drive the clock past the t=5 chunk so the reader has a chunk
      // in flight before the throwing callback fires. This pins the
      // semantics: bytes already DELIVERED to the consumer are not
      // retroactively lost; only the queue inside the controller is
      // dropped when the hook errors it.
      await harness.advanceTo(7);
      const decoder = new TextDecoder();
      const first = await reader.read();
      expect(decoder.decode(first.value)).toBe("first");

      // Schedule a callback at t=10 that throws synchronously. The
      // clock invokes the sync-throw hook before re-throwing; the
      // harness's hook errors every open stream.
      harness.clock.schedule(10, function syntheticBoom() {
        throw new Error("synthetic callback failure");
      });

      let advanceErr: unknown;
      try {
        await harness.advanceTo(15);
      } catch (err) {
        advanceErr = err;
      }
      expect(advanceErr).toBeInstanceOf(Error);
      if (!(advanceErr instanceof Error)) throw new Error("unreachable");
      expect(advanceErr.message).toMatch(/synthetic callback failure/);

      // The stream's controller was errored by the sync-throw hook.
      // The reader's next read must reject; the would-be-second chunk
      // at t=20 is never reached.
      let readErr: unknown;
      try {
        await reader.read();
      } catch (err) {
        readErr = err;
      }
      expect(readErr).toBeInstanceOf(Error);
      if (!(readErr instanceof Error)) throw new Error("unreachable");
      expect(readErr.message).toMatch(/synthetic callback failure/);

      // The sync-throw hook's `forceError` must have driven the stream
      // into a terminal state and removed it from the harness's open-
      // stream registry. We can't read the registry directly, but the
      // `abortBefore` guard rejects on terminal streams; if the hook
      // had not transitioned the stream, this would not throw.
      expect(() => harness.abortBefore(stream.streamId)).toThrow(
        /already in a terminal state/,
      );

      // Dispose must still be idempotent after sync-throw cleanup.
      harness.dispose();
      harness.dispose();
    } finally {
      harness.dispose();
    }
  });
});
