// Inference harness lifecycle hygiene: timers cancelled on every exit
// path; AbortSignal listeners on the caller's signal do not accumulate
// across calls. Regression coverage for the leaks the INTR-87 timeout
// work fixed.
//
// Synthetic Dependencies (recording scheduler, counting signal, stub
// fetch) keep the assertions on the harness's plumbing, not the wire.

import { describe, test, expect } from "bun:test";

import { runInference } from "@intx/inference";
import type { Dependencies, Scheduler } from "@intx/inference";
import { createBuiltinRegistry } from "@intx/inference/providers";
import type {
  ConversationTurn,
  InferenceEvent,
  InferenceSource,
} from "@intx/types/runtime";

const SOURCE: InferenceSource = {
  id: "openai:test-model",
  provider: "openai",
  baseURL: "https://test.invalid/v1",
  credentialId: "test",
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
  // Abort-only policy: the default would retry 5xx and block on the
  // recording scheduler's never-firing setTimeout. The assertion is
  // per-attempt timer cancellation, so it stays focused.
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
    const deps: Dependencies = {
      fetch: fetchStub,
      scheduler,
      adapters: createBuiltinRegistry(),
    };

    let seq = 0;
    const events = await drain(
      runInference({
        readMaterial: () => ({ secret: "test-secret" }),
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
    const deps: Dependencies = {
      fetch: fetchStub,
      scheduler,
      adapters: createBuiltinRegistry(),
    };

    let seq = 0;
    const events = await drain(
      runInference({
        readMaterial: () => ({ secret: "test-secret" }),
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
    // Buffering means there is no incremental delta to `break` on
    // mid-stream; the same mid-stream-exit invariant holds via the
    // caller's `signal`: aborting after the fetch resolves cancels
    // both timers through runSingleAttempt's try/finally.
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
    const deps: Dependencies = {
      fetch: fetchStub,
      scheduler,
      adapters: createBuiltinRegistry(),
    };

    const controller = new AbortController();
    let seq = 0;
    const collector = (async () => {
      // With buffering, only the abort flows through to
      // runSingleAttempt and yields the terminal `aborted` error.
      for await (const _ev of runInference({
        readMaterial: () => ({ secret: "test-secret" }),
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
    // `Parameters<EventTarget["addEventListener"]>` recovers the
    // listener tuple without naming the DOM types missing from the
    // ESNext-only lib.
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
    // runInference requires an AbortSignal; the counter satisfies the
    // structural shape EventTarget exposes.
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- CountingSignal extends EventTarget and exposes the AbortSignal shape runInference relies on (addEventListener/removeEventListener + aborted + reason).
    const fakeSignal = counter as unknown as AbortSignal;

    const deps: Dependencies = {
      fetch: successfulFetch,
      scheduler: inertScheduler,
      adapters: createBuiltinRegistry(),
    };

    let seq = 0;
    await drain(
      runInference({
        readMaterial: () => ({ secret: "test-secret" }),
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
