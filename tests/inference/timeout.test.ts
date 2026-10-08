// Per-call inactivity + total timeouts for the inference harness.
// Without them (INTR-87), a provider that stops emitting SSE chunks
// or never sends `[DONE]` deadlocks every downstream consumer of
// `runInference`.
//
// Driven against the harness's virtual clock, so these tests fire the
// timeouts without sleeping real wall-clock: which timer fired, which
// error category surfaces, and that the AbortController propagated to
// the fetch.

import { describe, test, expect } from "bun:test";

import { runInference } from "@intx/inference";
import type {
  InferenceEvent,
  InferenceError,
  ConversationTurn,
  InferenceSource,
} from "@intx/types/runtime";
import { setupHarness, wire } from "@intx/inference-testing";
import type { Harness } from "@intx/inference-testing";

async function withHarness<T>(body: (h: Harness) => Promise<T>): Promise<T> {
  // Wires the harness's virtual clock to the per-call timeout
  // scheduler; other suites leave it off so the 600s default total
  // timeout doesn't advance virtual time through ten minutes of heap.
  const harness = setupHarness({ enableInferenceTimers: true });
  try {
    return await body(harness);
  } finally {
    harness.dispose();
  }
}

const SOURCE: InferenceSource = {
  id: "openai:test-model",
  provider: "openai",
  baseURL: "https://test.invalid/v1",
  credentialId: "test",
  model: "test-model",
};

function makeTurns(): ConversationTurn[] {
  return [
    {
      role: "user",
      content: [{ type: "text", text: "hi" }],
      timestamp: 0,
    },
  ];
}

async function collect(
  stream: AsyncIterable<InferenceEvent>,
): Promise<InferenceEvent[]> {
  const out: InferenceEvent[] = [];
  for await (const event of stream) {
    out.push(event);
  }
  return out;
}

function findError(events: InferenceEvent[]): InferenceError | undefined {
  const errorEvent = events.find((e) => e.type === "inference.error");
  if (errorEvent?.type !== "inference.error") return undefined;
  return errorEvent.data.error;
}

function startConsumer(events: AsyncIterable<InferenceEvent>): {
  done: Promise<InferenceEvent[]>;
} {
  return { done: collect(events) };
}

// Pin a one-attempt policy for tests targeting per-attempt
// timeout-firing; the retry path is exercised by the three-stall test.
const ABORT_ONLY_RETRY_POLICY = {
  retryPolicy: () => ({ kind: "abort" as const }),
};

describe("runInference — per-call timeouts (virtual clock)", () => {
  test("inactivity timeout fires on each of the default policy's three attempts, then surfaces", async () => {
    await withHarness(async (harness) => {
      // Three single-use stalls, one per attempt: the inactivity timer
      // trips each, the default policy retries to the 3-attempt cap,
      // then the terminal `inference.error` surfaces.
      harness.scenario.stall();
      harness.scenario.stall();
      harness.scenario.stall();

      let seq = 0;
      const consumer = startConsumer(
        runInference({
          readMaterial: () => ({ secret: "test-secret" }),
          turns: makeTurns(),
          source: SOURCE,
          inferenceOptions: {
            inactivityTimeoutMs: 100,
            totalTimeoutMs: 10_000,
          },
          nextSeq: () => seq++,
          deps: harness.deps,
        }),
      );

      // Advance virtual time through all three attempts plus the
      // 500ms + 1000ms backoffs, draining every scheduled callback.
      await harness.run();

      const events = await consumer.done;

      // Two `inference.retry` events between the three attempts, with
      // the right per-attempt numbers; the third attempt's error has
      // category `"timeout"`.
      const retries = events.filter((e) => e.type === "inference.retry");
      expect(retries).toHaveLength(2);
      expect(
        retries[0]?.type === "inference.retry" ? retries[0].data : null,
      ).toMatchObject({ attempt: 1, delayMs: 500 });
      expect(
        retries[1]?.type === "inference.retry" ? retries[1].data : null,
      ).toMatchObject({ attempt: 2, delayMs: 1000 });

      const err = findError(events);
      expect(err).toBeDefined();
      expect(err?.category).toBe("timeout");
      expect(err?.message).toMatch(/inactivity/i);
      expect(err?.message).toMatch(/100/);
    });
  });

  test("inactivity timer is reset by each yielded event — slow but steady stream finishes", async () => {
    await withHarness(async (harness) => {
      const stream = harness.scenario.createStream();
      harness.scenario.whenRequestMatches(() => true, stream);

      // The two openai chunks, 60ms apart, each reset the 100ms
      // inactivity timer before it fires.
      const chunks = wire.completeResponse("openai", { text: "hello" });
      stream.enqueueAll(chunks, { startAt: 60, stepMs: 60 });

      let seq = 0;
      const consumer = startConsumer(
        runInference({
          readMaterial: () => ({ secret: "test-secret" }),
          turns: makeTurns(),
          source: SOURCE,
          inferenceOptions: {
            inactivityTimeoutMs: 100,
            totalTimeoutMs: 10_000,
          },
          nextSeq: () => seq++,
          deps: harness.deps,
        }),
      );

      await harness.run();

      const events = await consumer.done;
      expect(findError(events)).toBeUndefined();
      // Sanity: the stream actually produced something.
      expect(events.some((e) => e.type === "inference.done")).toBe(true);
    });
  });

  test("total timeout fires even when the stream is active enough to keep inactivity from firing", async () => {
    await withHarness(async (harness) => {
      const stream = harness.scenario.createStream();
      harness.scenario.whenRequestMatches(() => true, stream);

      // 100 chunks over a 1000ms span: the inactivity timer (5000ms)
      // never trips; the total cap (200ms) does.
      const longTrickle: Uint8Array[] = [];
      const encoder = new TextEncoder();
      for (let i = 0; i < 100; i++) {
        longTrickle.push(
          encoder.encode('data: {"choices":[{"index":0,"delta":{}}]}\n\n'),
        );
      }
      stream.enqueueAll(longTrickle, { startAt: 10, stepMs: 10 });

      let seq = 0;
      const consumer = startConsumer(
        runInference({
          readMaterial: () => ({ secret: "test-secret" }),
          turns: makeTurns(),
          source: SOURCE,
          inferenceOptions: {
            inactivityTimeoutMs: 5_000,
            totalTimeoutMs: 200,
            // Chunks pre-scheduled at setup time would all fire before
            // the second and third attempts start; the abort-only
            // policy keeps the assertion on the first-attempt fire.
            ...ABORT_ONLY_RETRY_POLICY,
          },
          nextSeq: () => seq++,
          deps: harness.deps,
        }),
      );

      await harness.run();

      const events = await consumer.done;
      const err = findError(events);
      expect(err).toBeDefined();
      expect(err?.category).toBe("timeout");
      expect(err?.message).toMatch(/total/i);
      expect(err?.message).toMatch(/200/);
    });
  });

  test("a healthy short call completes well inside both default timeouts", async () => {
    await withHarness(async (harness) => {
      // Defaults apply (120000 / 600000 ms); the reply lands at ~1ms.
      harness.scenario.replyOnce("openai", { text: "ok" });

      let seq = 0;
      const consumer = startConsumer(
        runInference({
          readMaterial: () => ({ secret: "test-secret" }),
          turns: makeTurns(),
          source: SOURCE,
          nextSeq: () => seq++,
          deps: harness.deps,
        }),
      );

      await harness.run();
      const events = await consumer.done;
      expect(findError(events)).toBeUndefined();
    });
  });

  test("underlying fetch AbortController fires on timeout for each attempt", async () => {
    await withHarness(async (harness) => {
      // Proves the abort actually fired at the fetch boundary
      // (`stall.aborted` flips, `stall.awaitAbort` resolves), the
      // INTR-87 checklist item the downstream error events only
      // imply. One stall per attempt: the abort must fire on each of
      // the three retried attempts.
      const stalls = [
        harness.scenario.stall(),
        harness.scenario.stall(),
        harness.scenario.stall(),
      ];
      for (const stall of stalls) expect(stall.aborted).toBe(false);

      let seq = 0;
      const consumer = startConsumer(
        runInference({
          readMaterial: () => ({ secret: "test-secret" }),
          turns: makeTurns(),
          source: SOURCE,
          inferenceOptions: {
            inactivityTimeoutMs: 50,
            totalTimeoutMs: 10_000,
          },
          nextSeq: () => seq++,
          deps: harness.deps,
        }),
      );

      await harness.run();
      for (const stall of stalls) {
        await stall.awaitAbort;
        expect(stall.aborted).toBe(true);
      }

      // Sanity: the run surfaced a timeout error after the third
      // attempt, so this is a genuine timeout-driven abort sequence.
      const events = await consumer.done;
      const err = findError(events);
      expect(err?.category).toBe("timeout");
      expect(err?.message).toMatch(/inactivity/i);
      expect(events.filter((e) => e.type === "inference.retry")).toHaveLength(
        2,
      );
    });
  });

  describe("per-call override on the same scripted wire", () => {
    // One event at t=1ms, 1000ms of silence, then the rest of the
    // reply: trips a 50ms inactivity threshold, lands cleanly under
    // a 5000ms one.
    function scriptOneSecondGap(
      stream: ReturnType<Harness["scenario"]["createStream"]>,
    ): void {
      const chunks = wire.completeResponse("openai", { text: "hi" });
      if (chunks.length < 2) {
        throw new Error(
          `scriptOneSecondGap: wire.completeResponse returned ${String(chunks.length)} chunks; needs at least 2 to model an inter-chunk gap`,
        );
      }
      const [first, ...rest] = chunks;
      if (first === undefined) {
        throw new Error("scriptOneSecondGap: first chunk unexpectedly missing");
      }
      stream.enqueueAt(1, first);
      stream.enqueueAll(rest, { startAt: 1001, stepMs: 1 });
    }

    test("short inactivity threshold trips on the 1s gap", async () => {
      await withHarness(async (harness) => {
        const stream = harness.scenario.createStream();
        harness.scenario.whenRequestMatches(() => true, stream);
        scriptOneSecondGap(stream);

        let seq = 0;
        const consumer = startConsumer(
          runInference({
            readMaterial: () => ({ secret: "test-secret" }),
            turns: makeTurns(),
            source: SOURCE,
            inferenceOptions: {
              inactivityTimeoutMs: 50,
              totalTimeoutMs: 10_000,
              // Override assertion only; the pinned single-attempt
              // policy rationale is above.
              ...ABORT_ONLY_RETRY_POLICY,
            },
            nextSeq: () => seq++,
            deps: harness.deps,
          }),
        );

        await harness.run();
        const events = await consumer.done;
        const err = findError(events);
        expect(err?.category).toBe("timeout");
        expect(err?.message).toMatch(/inactivity/i);
        expect(err?.message).toMatch(/\b50\b/);
      });
    });

    test("long inactivity threshold passes through on the same gap", async () => {
      await withHarness(async (harness) => {
        const stream = harness.scenario.createStream();
        harness.scenario.whenRequestMatches(() => true, stream);
        scriptOneSecondGap(stream);

        let seq = 0;
        const consumer = startConsumer(
          runInference({
            readMaterial: () => ({ secret: "test-secret" }),
            turns: makeTurns(),
            source: SOURCE,
            inferenceOptions: {
              inactivityTimeoutMs: 5_000,
              totalTimeoutMs: 10_000,
            },
            nextSeq: () => seq++,
            deps: harness.deps,
          }),
        );

        await harness.run();
        const events = await consumer.done;
        expect(findError(events)).toBeUndefined();
        expect(events.some((e) => e.type === "inference.done")).toBe(true);
      });
    });
  });
});

describe("runInference — buffered JSON read timeout/abort (virtual clock)", () => {
  // A non-streaming JSON response whose body never closes: the harness
  // parks in the buffered read until the total-timeout or the caller's
  // AbortSignal aborts it. The JSON-path analogue of the SSE stalls,
  // exercising the abort corner without an adapter JSON parser.
  function stallingJSONFetch() {
    return () =>
      Promise.resolve(
        new Response(
          new ReadableStream<Uint8Array>({
            start() {
              // Intentionally never enqueue or close.
            },
          }),
          { status: 200, headers: { "content-type": "application/json" } },
        ),
      );
  }

  test("total timeout fires during a stalled JSON-body read", async () => {
    await withHarness(async (harness) => {
      let seq = 0;
      const consumer = startConsumer(
        runInference({
          readMaterial: () => ({ secret: "test-secret" }),
          turns: makeTurns(),
          source: SOURCE,
          inferenceOptions: {
            inactivityTimeoutMs: 5_000,
            totalTimeoutMs: 200,
            // Single-attempt policy so the assertion targets the first
            // total-timeout fire (rationale above).
            ...ABORT_ONLY_RETRY_POLICY,
          },
          nextSeq: () => seq++,
          deps: { ...harness.deps, fetch: stallingJSONFetch() },
        }),
      );

      await harness.run();
      const err = findError(await consumer.done);
      expect(err?.category).toBe("timeout");
      expect(err?.message).toMatch(/total/i);
      expect(err?.message).toMatch(/200/);
    });
  });

  test("caller abort during a stalled JSON-body read surfaces an aborted error", async () => {
    await withHarness(async (harness) => {
      const controller = new AbortController();
      let seq = 0;
      const consumer = startConsumer(
        runInference({
          readMaterial: () => ({ secret: "test-secret" }),
          turns: makeTurns(),
          source: SOURCE,
          signal: controller.signal,
          inferenceOptions: {
            totalTimeoutMs: 10_000,
            ...ABORT_ONLY_RETRY_POLICY,
          },
          nextSeq: () => seq++,
          deps: { ...harness.deps, fetch: stallingJSONFetch() },
        }),
      );

      controller.abort();
      await harness.run();
      const err = findError(await consumer.done);
      expect(err?.category).toBe("aborted");
    });
  });
});
