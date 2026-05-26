// Inference harness lifecycle hygiene: timers cancelled on every exit
// path; AbortSignal listeners on the caller's signal do not accumulate
// across calls. These tests pin the invariants the timeout work in
// INTR-87 added to `runInference` — they were originally written to
// demonstrate three concrete leaks in the first draft of that work and
// are kept here as regression coverage so future edits to the error
// paths or the signal-combining logic cannot quietly reintroduce them.
//
// Drive `runInference` with synthetic Dependencies (recording scheduler,
// counting signal, stub fetch) rather than the inference-testing harness
// — these tests scrutinise the harness's plumbing, not the wire
// behaviour, so going through `setupHarness` would only obscure the
// assertions.

import { describe, test, expect } from "bun:test";

import { runInference } from "@intx/inference";
import type { Dependencies, Scheduler } from "@intx/inference";
import type {
  ConversationTurn,
  InferenceEvent,
  InferenceSource,
} from "@intx/types/runtime";

const SOURCE: InferenceSource = {
  id: "openai:test-model",
  provider: "openai",
  baseURL: "https://test.invalid/v1",
  apiKey: "test",
  model: "test-model",
};

function makeTurns(): ConversationTurn[] {
  return [
    { role: "user", content: [{ type: "text", text: "hi" }], timestamp: 0 },
  ];
}

type ScheduledEntry = {
  callback: () => void;
  delayMs: number;
  cancelled: boolean;
};

function recordingScheduler(): {
  scheduler: Scheduler;
  entries: ScheduledEntry[];
} {
  const entries: ScheduledEntry[] = [];
  const scheduler: Scheduler = {
    setTimeout(callback, delayMs) {
      const entry: ScheduledEntry = { callback, delayMs, cancelled: false };
      entries.push(entry);
      return () => {
        entry.cancelled = true;
      };
    },
    now: () => 0,
  };
  return { scheduler, entries };
}

async function drain(
  stream: AsyncIterable<InferenceEvent>,
): Promise<InferenceEvent[]> {
  const out: InferenceEvent[] = [];
  for await (const event of stream) {
    out.push(event);
  }
  return out;
}

describe("runInference — timer cancellation on non-streaming exit paths", () => {
  // Both tests pin an abort-only retry policy: they assert that the
  // per-attempt timers are cancelled when the attempt exits before
  // streaming begins. The default policy retries on `retryable` (5xx)
  // and would block the wrapper on the recording scheduler's
  // setTimeout — which records but never fires. Abort-only keeps the
  // per-attempt invariant assertion focused.
  const ABORT_ONLY_POLICY = { retryPolicy: () => ({ kind: "abort" as const }) };

  test("non-OK HTTP response cancels the total timer", async () => {
    const { scheduler, entries } = recordingScheduler();
    const fetchStub: Dependencies["fetch"] = () =>
      Promise.resolve(
        new Response(JSON.stringify({ error: { message: "boom" } }), {
          status: 500,
          headers: { "content-type": "application/json" },
        }),
      );
    const deps: Dependencies = { fetch: fetchStub, scheduler };

    let seq = 0;
    const events = await drain(
      runInference({
        turns: makeTurns(),
        source: SOURCE,
        inferenceOptions: ABORT_ONLY_POLICY,
        nextSeq: () => seq++,
        deps,
      }),
    );

    expect(events.some((e) => e.type === "inference.error")).toBe(true);
    expect(entries.length).toBeGreaterThan(0);
    const totalTimer = entries[0];
    if (totalTimer === undefined) throw new Error("no timer registered");
    expect(totalTimer.cancelled).toBe(true);
  });

  test("204 response with null body cancels the total timer", async () => {
    const { scheduler, entries } = recordingScheduler();
    const fetchStub: Dependencies["fetch"] = () =>
      Promise.resolve(new Response(null, { status: 204 }));
    const deps: Dependencies = { fetch: fetchStub, scheduler };

    let seq = 0;
    const events = await drain(
      runInference({
        turns: makeTurns(),
        source: SOURCE,
        inferenceOptions: ABORT_ONLY_POLICY,
        nextSeq: () => seq++,
        deps,
      }),
    );

    expect(events.some((e) => e.type === "inference.error")).toBe(true);
    expect(entries.length).toBeGreaterThan(0);
    const totalTimer = entries[0];
    if (totalTimer === undefined) throw new Error("no timer registered");
    expect(totalTimer.cancelled).toBe(true);
  });
});

describe("runInference — timer cancellation on consumer abandonment", () => {
  test("aborting the caller signal mid-stream cancels every timer", async () => {
    // Earlier the consumer abandoned the iterator by `break`ing on the
    // first `inference.text.delta`. The retry wrapper buffers every
    // attempt's events until a terminal event arrives, so deltas no
    // longer reach the caller incrementally — there is no observable
    // event to break on while the upstream stream is still parked.
    // The same invariant (timer cleanup on a mid-stream exit path)
    // still holds via the caller-supplied `signal`: aborting it after
    // the fetch has resolved but before the stream completes flows
    // through `runSingleAttempt`'s try/finally and cancels both
    // timers via the recorded canceller.
    const { scheduler, entries } = recordingScheduler();
    let firstByteResolved: () => void = () => undefined;
    const firstByteEmitted = new Promise<void>((resolve) => {
      firstByteResolved = resolve;
    });
    const fetchStub: Dependencies["fetch"] = () => {
      const enc = new TextEncoder();
      return Promise.resolve(
        new Response(
          new ReadableStream({
            async start(controller) {
              controller.enqueue(
                enc.encode(
                  'data: {"choices":[{"index":0,"delta":{"content":"a"}}]}\n\n',
                ),
              );
              firstByteResolved();
              // Stream never closes. Caller aborts after the first
              // chunk; the generator's finally must cancel the timers.
            },
          }),
          { status: 200, headers: { "content-type": "text/event-stream" } },
        ),
      );
    };
    const deps: Dependencies = { fetch: fetchStub, scheduler };

    const controller = new AbortController();
    let seq = 0;
    const collector = (async () => {
      // Drain the iterator until it ends; with buffering the only
      // way out is the abort flowing through to `runSingleAttempt`
      // and yielding the terminal `inference.error` from the catch
      // block (category `aborted`), which the abort-only retry
      // policy then surfaces.
      for await (const _ev of runInference({
        turns: makeTurns(),
        source: SOURCE,
        signal: controller.signal,
        inferenceOptions: {
          retryPolicy: () => ({ kind: "abort" as const }),
        },
        nextSeq: () => seq++,
        deps,
      })) {
        // Discard — the assertion is downstream of the iteration
        // ending, not on any specific event.
      }
    })();

    await firstByteEmitted;
    controller.abort();
    await collector;

    const armed = entries.filter((e) => !e.cancelled).length;
    expect(armed).toBe(0);
  });
});

describe("runInference — caller-signal listener accounting", () => {
  test("a successful call does not leak abort listeners on the caller signal", async () => {
    // The DOM-shaped EventListener types are not in the project's
    // `lib` (ESNext only), but `Parameters<EventTarget["addEventListener"]>`
    // recovers the right tuple structurally without naming the missing
    // types directly.
    class CountingSignal extends EventTarget {
      added = 0;
      removed = 0;
      readonly aborted = false;
      readonly reason: unknown = undefined;
      override addEventListener(
        ...args: Parameters<EventTarget["addEventListener"]>
      ): void {
        if (args[0] === "abort") this.added += 1;
        super.addEventListener(...args);
      }
      override removeEventListener(
        ...args: Parameters<EventTarget["removeEventListener"]>
      ): void {
        if (args[0] === "abort") this.removed += 1;
        super.removeEventListener(...args);
      }
    }

    const inertScheduler: Scheduler = {
      setTimeout: () => () => {
        /* no-op: tests do not exercise the timer firing */
      },
      now: () => 0,
    };
    const successfulFetch: Dependencies["fetch"] = () => {
      const enc = new TextEncoder();
      return Promise.resolve(
        new Response(
          new ReadableStream({
            start(controller) {
              controller.enqueue(
                enc.encode(
                  'data: {"choices":[{"index":0,"delta":{"content":"hi"}}]}\n\n',
                ),
              );
              controller.enqueue(enc.encode("data: [DONE]\n\n"));
              controller.close();
            },
          }),
          { status: 200, headers: { "content-type": "text/event-stream" } },
        ),
      );
    };

    const counter = new CountingSignal();
    // The Signal-typed view of the counter is intentional: runInference
    // requires an AbortSignal at the parameter level, and the counter
    // satisfies the structural shape EventTarget exposes for that use.
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- CountingSignal extends EventTarget and exposes the AbortSignal shape runInference relies on (addEventListener/removeEventListener + aborted + reason).
    const fakeSignal = counter as unknown as AbortSignal;

    const deps: Dependencies = {
      fetch: successfulFetch,
      scheduler: inertScheduler,
    };

    let seq = 0;
    await drain(
      runInference({
        turns: makeTurns(),
        source: SOURCE,
        nextSeq: () => seq++,
        deps,
        signal: fakeSignal,
      }),
    );

    expect(counter.added).toBeGreaterThan(0);
    expect(counter.added - counter.removed).toBe(0);
  });
});
