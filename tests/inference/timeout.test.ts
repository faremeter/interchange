// Per-call inactivity + total timeouts for the inference harness.
// See INTR-87 — without these timeouts, a provider that stops emitting
// SSE chunks (or never sends `[DONE]`) deadlocks every downstream
// consumer of `runInference` because the iterator never terminates.
//
// Driven against the deterministic test harness's virtual clock — the
// production code uses real `setTimeout` only when no scheduler is
// injected via `Dependencies.scheduler`; the harness substitutes a
// scheduler backed by `clock.schedule`, so these tests fire the
// timeouts at virtual time without sleeping real wall-clock. We are
// not testing `setTimeout` itself; we are testing the timeout LOGIC
// (which timer fired, which error category surfaces, that the
// AbortController propagated to the fetch).

import { describe, test, expect } from "bun:test";

import { runInference } from "@intx/inference";
import type {
  InferenceEvent,
  InferenceError,
  ConversationTurn,
  ProviderConfig,
} from "@intx/types/runtime";
import { setupHarness, wire } from "@intx/inference-testing";
import type { Harness } from "@intx/inference-testing";

async function withHarness<T>(body: (h: Harness) => Promise<T>): Promise<T> {
  // `enableInferenceTimers: true` wires the harness's virtual clock to
  // the inference layer's per-call timeout scheduler. Every other test
  // suite leaves this off so the 600s default total-timeout doesn't
  // force `harness.run()` to advance virtual time through ten minutes
  // of empty heap.
  const harness = setupHarness({ enableInferenceTimers: true });
  try {
    return await body(harness);
  } finally {
    harness.dispose();
  }
}

const PROVIDER: ProviderConfig = {
  provider: "openai",
  baseURL: "https://test.invalid/v1",
  apiKey: "test",
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

describe("runInference — per-call timeouts (virtual clock)", () => {
  test("inactivity timeout fires when SSE stream goes silent past the threshold", async () => {
    await withHarness(async (harness) => {
      // `scenario.stall()` routes the fetch to a stream that never
      // emits anything. The SSE iterator's first read parks forever in
      // real time; in virtual time we advance past the inactivity
      // threshold and the timer aborts the fetch.
      harness.scenario.stall();

      let seq = 0;
      const consumer = startConsumer(
        runInference({
          turns: makeTurns(),
          model: "test-model",
          providerConfig: PROVIDER,
          inferenceOptions: {
            inactivityTimeoutMs: 100,
            totalTimeoutMs: 10_000,
          },
          nextSeq: () => seq++,
          deps: harness.deps,
        }),
      );

      // Advance virtual clock past the inactivity threshold. Drains
      // every scheduled callback (including the timeout), settles
      // microtasks, returns when the heap is empty + quiescent.
      await harness.run();

      const events = await consumer.done;
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

      // Five chunks at 60ms apart — under a 100ms inactivity timeout,
      // each chunk resets the timer well before it can fire. Total
      // virtual time elapsed: 5 * 60 = 300ms.
      const chunks = wire.completeResponse("openai", { text: "hello" });
      stream.enqueueAll(chunks, { startAt: 60, stepMs: 60 });

      let seq = 0;
      const consumer = startConsumer(
        runInference({
          turns: makeTurns(),
          model: "test-model",
          providerConfig: PROVIDER,
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

      // 100 chunks at 10ms apart — total virtual span 1000ms. The
      // inactivity timer (5000ms) never trips; the total cap (200ms)
      // does.
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
          turns: makeTurns(),
          model: "test-model",
          providerConfig: PROVIDER,
          inferenceOptions: {
            inactivityTimeoutMs: 5_000,
            totalTimeoutMs: 200,
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
      // No timeout options — defaults of 120000 / 600000 ms apply.
      // The reply lands at virtual time ~1ms and the call wraps.
      harness.scenario.replyOnce("openai", { text: "ok" });

      let seq = 0;
      const consumer = startConsumer(
        runInference({
          turns: makeTurns(),
          model: "test-model",
          providerConfig: PROVIDER,
          nextSeq: () => seq++,
          deps: harness.deps,
        }),
      );

      await harness.run();
      const events = await consumer.done;
      expect(findError(events)).toBeUndefined();
    });
  });

  test("underlying fetch AbortController fires on timeout", async () => {
    await withHarness(async (harness) => {
      // Direct assertion on abort propagation: `stall.aborted` flips
      // and `stall.awaitAbort` resolves the moment the matched fetch's
      // AbortSignal fires. This is the assertion INTR-87's checklist
      // names explicitly — the downstream `inference.error` event the
      // other tests look at is downstream of the abort, but this test
      // proves the abort actually fired at the fetch boundary.
      const stall = harness.scenario.stall();
      expect(stall.aborted).toBe(false);

      let seq = 0;
      const consumer = startConsumer(
        runInference({
          turns: makeTurns(),
          model: "test-model",
          providerConfig: PROVIDER,
          inferenceOptions: {
            inactivityTimeoutMs: 50,
            totalTimeoutMs: 10_000,
          },
          nextSeq: () => seq++,
          deps: harness.deps,
        }),
      );

      await harness.run();
      await stall.awaitAbort;
      expect(stall.aborted).toBe(true);

      // Sanity: the run also surfaced a timeout error, so this is a
      // genuine timeout-driven abort and not some other path.
      const events = await consumer.done;
      const err = findError(events);
      expect(err?.category).toBe("timeout");
      expect(err?.message).toMatch(/inactivity/i);
    });
  });

  describe("per-call override on the same scripted wire", () => {
    // Shared wire script: one event at virtual t=1ms, then a 1000ms
    // silence, then the remainder of a complete reply starting at
    // t=1001ms. With a 50ms inactivity threshold the silence trips
    // the timer; with a 5000ms inactivity threshold the silence is
    // well under the budget and the reply lands cleanly.
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
            turns: makeTurns(),
            model: "test-model",
            providerConfig: PROVIDER,
            inferenceOptions: {
              inactivityTimeoutMs: 50,
              totalTimeoutMs: 10_000,
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
            turns: makeTurns(),
            model: "test-model",
            providerConfig: PROVIDER,
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
