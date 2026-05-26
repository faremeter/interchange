// Pluggable mechanical retry policy for the inference harness — INTR-88.
//
// The wrapper exposed as `runInference` buffers each attempt's events
// until the attempt terminates with `inference.done` or
// `inference.error`. On `inference.error` the wrapper consults the
// configured `RetryPolicy` (or `createDefaultRetryPolicy()` when none
// is supplied) and either flushes the buffered events to the caller
// or discards them, emits one `inference.retry` event between
// attempts, awaits a Scheduler-driven delay, and re-issues the same
// HTTP request.
//
// These tests drive the wrapper against the deterministic test
// harness's virtual clock (`enableInferenceTimers: true`) so every
// retry delay is asserted exactly without sleeping real wall-clock,
// and so the scheduler the wrapper awaits is the one whose firing
// timing the test controls.

import { describe, test, expect } from "bun:test";

import { runInference, createDefaultRetryPolicy } from "@intx/inference";
import type {
  InferenceEvent,
  InferenceError,
  ConversationTurn,
  InferenceSource,
  RetryPolicy,
  RetrySituation,
} from "@intx/types/runtime";
import { setupHarness } from "@intx/inference-testing";
import type { Harness } from "@intx/inference-testing";

async function withHarness<T>(body: (h: Harness) => Promise<T>): Promise<T> {
  // `enableInferenceTimers: true` is required: the wrapper awaits the
  // scheduler-driven retry delay and the inert default scheduler's
  // setTimeout is a no-op (deliberately, to keep tests that do not
  // exercise timers from advancing virtual time through ten-minute
  // defaults). Without virtual-clock firing the retry delay would
  // never resolve.
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
  apiKey: "test",
  model: "test-model",
};

function makeTurns(): ConversationTurn[] {
  return [
    { role: "user", content: [{ type: "text", text: "hi" }], timestamp: 0 },
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

function startConsumer(events: AsyncIterable<InferenceEvent>): {
  done: Promise<InferenceEvent[]>;
} {
  return { done: collect(events) };
}

function findError(events: InferenceEvent[]): InferenceError | undefined {
  const errorEvent = events.find((e) => e.type === "inference.error");
  if (errorEvent?.type !== "inference.error") return undefined;
  return errorEvent.data.error;
}

function retryEvents(
  events: InferenceEvent[],
): { attempt: number; delayMs: number; previousError: InferenceError }[] {
  const out: {
    attempt: number;
    delayMs: number;
    previousError: InferenceError;
  }[] = [];
  for (const e of events) {
    if (e.type === "inference.retry") out.push(e.data);
  }
  return out;
}

// Register N single-use 5xx responses with a small JSON body. Each
// classifies as `retryable` through `classifyHTTPError`. Used by tests
// that target `category: "retryable"` without depending on timeout
// semantics.
function registerRetryable5xx(harness: Harness, count: number): void {
  const encoder = new TextEncoder();
  for (let i = 0; i < count; i++) {
    const stream = harness.scenario.createStream();
    harness.scenario.whenRequestMatches(() => true, stream, { status: 503 });
    stream.enqueueAt(
      1,
      encoder.encode('{"error":{"message":"upstream unavailable"}}'),
    );
    stream.closeAt(2);
  }
}

describe("runInference — default retry policy", () => {
  test("makes up to 3 attempts on `retryable` with documented backoff before surfacing", async () => {
    await withHarness(async (harness) => {
      registerRetryable5xx(harness, 3);

      let seq = 0;
      const consumer = startConsumer(
        runInference({
          turns: makeTurns(),
          source: SOURCE,
          nextSeq: () => seq++,
          deps: harness.deps,
        }),
      );

      const before = harness.clock.now();
      await harness.run();
      const after = harness.clock.now();

      const events = await consumer.done;
      const retries = retryEvents(events);
      expect(retries).toHaveLength(2);
      expect(retries[0]?.attempt).toBe(1);
      expect(retries[0]?.delayMs).toBe(500);
      expect(retries[1]?.attempt).toBe(2);
      expect(retries[1]?.delayMs).toBe(1000);

      // The wrapper waited the documented backoff between attempts:
      // 500ms before attempt 2 plus 1000ms before attempt 3.
      expect(after - before).toBeGreaterThanOrEqual(1500);

      const err = findError(events);
      expect(err?.category).toBe("retryable");
      expect(harness.scenario.matchedRequests()).toHaveLength(3);
    });
  });

  test("never retries `credential_failure`, `context_overflow`, `fatal`, or `aborted`", async () => {
    const cases: {
      status: number;
      expectedCategory: InferenceError["category"];
    }[] = [
      { status: 401, expectedCategory: "credential_failure" },
      { status: 403, expectedCategory: "credential_failure" },
      { status: 400, expectedCategory: "fatal" },
    ];
    for (const { status, expectedCategory } of cases) {
      await withHarness(async (harness) => {
        const encoder = new TextEncoder();
        const stream = harness.scenario.createStream();
        harness.scenario.whenRequestMatches(() => true, stream, { status });
        stream.enqueueAt(1, encoder.encode('{"error":{"message":"nope"}}'));
        stream.closeAt(2);

        let seq = 0;
        const consumer = startConsumer(
          runInference({
            turns: makeTurns(),
            source: SOURCE,
            nextSeq: () => seq++,
            deps: harness.deps,
          }),
        );
        await harness.run();
        const events = await consumer.done;

        expect(retryEvents(events)).toHaveLength(0);
        expect(findError(events)?.category).toBe(expectedCategory);
        expect(harness.scenario.matchedRequests()).toHaveLength(1);
      });
    }

    // `context_overflow` is the 400-with-context-length-message branch.
    await withHarness(async (harness) => {
      const encoder = new TextEncoder();
      const stream = harness.scenario.createStream();
      harness.scenario.whenRequestMatches(() => true, stream, { status: 400 });
      stream.enqueueAt(
        1,
        encoder.encode(
          '{"error":{"message":"prompt is too long: maximum context length is 8192 tokens"}}',
        ),
      );
      stream.closeAt(2);

      let seq = 0;
      const consumer = startConsumer(
        runInference({
          turns: makeTurns(),
          source: SOURCE,
          nextSeq: () => seq++,
          deps: harness.deps,
        }),
      );
      await harness.run();
      const events = await consumer.done;
      expect(retryEvents(events)).toHaveLength(0);
      expect(findError(events)?.category).toBe("context_overflow");
    });

    // `aborted` — caller signal raised before any attempt produces.
    await withHarness(async (harness) => {
      harness.scenario.stall();
      const controller = new AbortController();
      controller.abort();

      let seq = 0;
      const consumer = startConsumer(
        runInference({
          turns: makeTurns(),
          source: SOURCE,
          signal: controller.signal,
          nextSeq: () => seq++,
          deps: harness.deps,
        }),
      );
      await harness.run();
      const events = await consumer.done;
      expect(retryEvents(events)).toHaveLength(0);
      expect(findError(events)?.category).toBe("aborted");
    });
  });
});

describe("runInference — custom retry policy", () => {
  test("single-attempt policy: `retryable` error surfaces immediately", async () => {
    await withHarness(async (harness) => {
      registerRetryable5xx(harness, 1);

      let policyCalls = 0;
      const policy: RetryPolicy = () => {
        policyCalls += 1;
        return { kind: "abort" };
      };

      let seq = 0;
      const consumer = startConsumer(
        runInference({
          turns: makeTurns(),
          source: SOURCE,
          inferenceOptions: { retryPolicy: policy },
          nextSeq: () => seq++,
          deps: harness.deps,
        }),
      );
      await harness.run();
      const events = await consumer.done;

      expect(policyCalls).toBe(1);
      expect(retryEvents(events)).toHaveLength(0);
      expect(findError(events)?.category).toBe("retryable");
      expect(harness.scenario.matchedRequests()).toHaveLength(1);
    });
  });

  test("unlimited retries on `retryable`: success on attempt N produces a single clean event stream", async () => {
    await withHarness(async (harness) => {
      // Two failures then one success. The custom policy retries
      // indefinitely with delayMs: 0 so the test does not need to
      // advance virtual time through real backoff.
      registerRetryable5xx(harness, 2);
      // Third matcher: a successful 200 response with a short payload.
      // The OpenAI adapter accepts a single text delta followed by the
      // SSE `[DONE]` sentinel; we serialise both inline rather than
      // building a wire helper since the test only needs one well-
      // formed reply.
      const encoder = new TextEncoder();
      const okStream = harness.scenario.createStream();
      harness.scenario.whenRequestMatches(() => true, okStream);
      okStream.enqueueAt(
        1,
        encoder.encode(
          'data: {"id":"x","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"hi"}}]}\n\n',
        ),
      );
      okStream.enqueueAt(2, encoder.encode("data: [DONE]\n\n"));
      okStream.closeAt(3);

      const policy: RetryPolicy = () => ({ kind: "retry", delayMs: 0 });

      let seq = 0;
      const consumer = startConsumer(
        runInference({
          turns: makeTurns(),
          source: SOURCE,
          inferenceOptions: { retryPolicy: policy },
          nextSeq: () => seq++,
          deps: harness.deps,
        }),
      );
      await harness.run();
      const events = await consumer.done;

      // Single inference.start, exactly two inference.retry events,
      // a successful tail (no inference.error visible to caller).
      const starts = events.filter((e) => e.type === "inference.start");
      const retries = retryEvents(events);
      const dones = events.filter((e) => e.type === "inference.done");
      const errors = events.filter((e) => e.type === "inference.error");

      expect(starts).toHaveLength(1);
      expect(retries).toHaveLength(2);
      expect(dones).toHaveLength(1);
      expect(errors).toHaveLength(0);
      expect(retries[0]?.attempt).toBe(1);
      expect(retries[1]?.attempt).toBe(2);
    });
  });

  test("custom policy reads `attempt` and can override the default's category", async () => {
    await withHarness(async (harness) => {
      // Default would retry 3 times on `retryable`; custom policy
      // aborts after the second failure regardless of category.
      registerRetryable5xx(harness, 3);

      const seen: number[] = [];
      const policy: RetryPolicy = (situation: RetrySituation) => {
        seen.push(situation.attempt);
        if (situation.attempt >= 2) return { kind: "abort" };
        return { kind: "retry", delayMs: 10 };
      };

      let seq = 0;
      const consumer = startConsumer(
        runInference({
          turns: makeTurns(),
          source: SOURCE,
          inferenceOptions: { retryPolicy: policy },
          nextSeq: () => seq++,
          deps: harness.deps,
        }),
      );
      await harness.run();
      const events = await consumer.done;

      expect(seen).toEqual([1, 2]);
      expect(retryEvents(events)).toHaveLength(1);
      expect(harness.scenario.matchedRequests()).toHaveLength(2);
      expect(findError(events)?.category).toBe("retryable");
    });
  });
});

describe("runInference — quota_exhausted handling", () => {
  test("uses error.retryAfterMs when present and fires the next attempt at exactly that delay", async () => {
    await withHarness(async (harness) => {
      const encoder = new TextEncoder();
      const headers = { "retry-after-ms": "750" } as const;

      // Two single-use 429s carrying retry-after-ms = 750. The default
      // policy retries on quota_exhausted; the second 429 surfaces.
      for (let i = 0; i < 2; i++) {
        const stream = harness.scenario.createStream();
        harness.scenario.whenRequestMatches(() => true, stream, {
          status: 429,
          headers,
        });
        stream.enqueueAt(
          1,
          encoder.encode('{"error":{"message":"rate limited"}}'),
        );
        stream.closeAt(2);
      }
      // Third 429 to satisfy the third attempt; default policy then
      // exhausts and surfaces the error.
      {
        const stream = harness.scenario.createStream();
        harness.scenario.whenRequestMatches(() => true, stream, {
          status: 429,
          headers,
        });
        stream.enqueueAt(
          1,
          encoder.encode('{"error":{"message":"rate limited"}}'),
        );
        stream.closeAt(2);
      }

      const before = harness.clock.now();
      let seq = 0;
      const consumer = startConsumer(
        runInference({
          turns: makeTurns(),
          source: SOURCE,
          nextSeq: () => seq++,
          deps: harness.deps,
        }),
      );
      await harness.run();
      const after = harness.clock.now();
      const events = await consumer.done;

      const retries = retryEvents(events);
      expect(retries).toHaveLength(2);
      // Both retries use the provider-supplied retry-after-ms.
      expect(retries[0]?.delayMs).toBe(750);
      expect(retries[1]?.delayMs).toBe(750);
      // Virtual clock advanced at least 2 * 750 ms across the two
      // delays (plus the fetch turnaround).
      expect(after - before).toBeGreaterThanOrEqual(1500);
      expect(findError(events)?.category).toBe("quota_exhausted");
    });
  });

  test("falls back to the 1000ms flat baseline when retry-after-ms is absent", async () => {
    await withHarness(async (harness) => {
      const encoder = new TextEncoder();
      for (let i = 0; i < 3; i++) {
        const stream = harness.scenario.createStream();
        harness.scenario.whenRequestMatches(() => true, stream, {
          status: 429,
        });
        stream.enqueueAt(
          1,
          encoder.encode('{"error":{"message":"rate limited"}}'),
        );
        stream.closeAt(2);
      }

      let seq = 0;
      const consumer = startConsumer(
        runInference({
          turns: makeTurns(),
          source: SOURCE,
          nextSeq: () => seq++,
          deps: harness.deps,
        }),
      );
      await harness.run();
      const events = await consumer.done;
      const retries = retryEvents(events);
      expect(retries).toHaveLength(2);
      expect(retries[0]?.delayMs).toBe(1000);
      expect(retries[1]?.delayMs).toBe(1000);
    });
  });
});

describe("runInference — inference.retry event payload", () => {
  test("carries attempt, delayMs, and previousError matching the most recent error", async () => {
    await withHarness(async (harness) => {
      registerRetryable5xx(harness, 3);

      let seq = 0;
      const consumer = startConsumer(
        runInference({
          turns: makeTurns(),
          source: SOURCE,
          nextSeq: () => seq++,
          deps: harness.deps,
        }),
      );
      await harness.run();
      const events = await consumer.done;
      const retries = retryEvents(events);
      expect(retries).toHaveLength(2);
      for (const r of retries) {
        expect(r.previousError.category).toBe("retryable");
        expect(typeof r.previousError.message).toBe("string");
        expect(r.previousError.message.length).toBeGreaterThan(0);
        expect(r.previousError.statusCode).toBe(503);
      }
    });
  });
});

describe("runInference — policy failure handling", () => {
  test("policy that throws synchronously: caller sees the original inference.error", async () => {
    await withHarness(async (harness) => {
      registerRetryable5xx(harness, 1);
      const policy: RetryPolicy = () => {
        throw new Error("policy boom");
      };

      let seq = 0;
      const consumer = startConsumer(
        runInference({
          turns: makeTurns(),
          source: SOURCE,
          inferenceOptions: { retryPolicy: policy },
          nextSeq: () => seq++,
          deps: harness.deps,
        }),
      );
      await harness.run();
      const events = await consumer.done;

      // No retry was emitted, the original retryable error surfaces,
      // and no `policy boom` text leaks through.
      expect(retryEvents(events)).toHaveLength(0);
      const err = findError(events);
      expect(err?.category).toBe("retryable");
      expect(err?.message).not.toContain("policy boom");
    });
  });

  test("policy that returns a rejected Promise: caller sees the original inference.error", async () => {
    await withHarness(async (harness) => {
      registerRetryable5xx(harness, 1);
      const policy: RetryPolicy = () =>
        Promise.reject(new Error("policy rejected"));

      let seq = 0;
      const consumer = startConsumer(
        runInference({
          turns: makeTurns(),
          source: SOURCE,
          inferenceOptions: { retryPolicy: policy },
          nextSeq: () => seq++,
          deps: harness.deps,
        }),
      );
      await harness.run();
      const events = await consumer.done;

      expect(retryEvents(events)).toHaveLength(0);
      const err = findError(events);
      expect(err?.category).toBe("retryable");
      expect(err?.message).not.toContain("policy rejected");
    });
  });

  test("policy throw does not allocate an extra seq for a phantom inference.retry", async () => {
    // The wrapper allocates one new seq via nextSeq() per
    // inference.retry it actually emits. A policy that throws and
    // gets coerced to abort must NOT call nextSeq() for the abort
    // path — the seq counter should match what a non-throwing
    // abort-only policy produces.
    async function seqsConsumed(policy: RetryPolicy): Promise<number> {
      return await withHarness(async (harness) => {
        registerRetryable5xx(harness, 1);
        let seq = 0;
        const consumer = startConsumer(
          runInference({
            turns: makeTurns(),
            source: SOURCE,
            inferenceOptions: { retryPolicy: policy },
            nextSeq: () => seq++,
            deps: harness.deps,
          }),
        );
        await harness.run();
        await consumer.done;
        return seq;
      });
    }

    const cleanAbort = await seqsConsumed(() => ({ kind: "abort" }));
    const throwingPolicy = await seqsConsumed(() => {
      throw new Error("policy boom");
    });
    expect(throwingPolicy).toBe(cleanAbort);
  });

  test("caller-visible seqs stay contiguous across a retry that discards an attempt", async () => {
    // The seq stream is documented as supporting gap detection for
    // missed events. A retry that discards a failed attempt's
    // buffered events must NOT leak the discarded attempt's seq
    // allocations into the caller's counter — otherwise the
    // consumer's first event arrives at a non-zero seq, looking
    // exactly like a network drop.
    await withHarness(async (harness) => {
      registerRetryable5xx(harness, 2);
      const encoder = new TextEncoder();
      const okStream = harness.scenario.createStream();
      harness.scenario.whenRequestMatches(() => true, okStream);
      okStream.enqueueAt(
        1,
        encoder.encode(
          'data: {"choices":[{"index":0,"delta":{"content":"hi"}}]}\n\n',
        ),
      );
      okStream.enqueueAt(2, encoder.encode("data: [DONE]\n\n"));
      okStream.closeAt(3);

      const policy: RetryPolicy = () => ({ kind: "retry", delayMs: 0 });

      let nextSeq = 0;
      const eventsP = collect(
        runInference({
          turns: makeTurns(),
          source: SOURCE,
          inferenceOptions: { retryPolicy: policy },
          nextSeq: () => nextSeq++,
          deps: harness.deps,
        }),
      );
      await harness.run();
      const events = await eventsP;

      // First event the caller sees starts at seq 0; the sequence
      // is strictly monotonically increasing by 1 across the entire
      // run (including the two `inference.retry` events between the
      // three attempts).
      expect(events[0]?.seq).toBe(0);
      for (let i = 1; i < events.length; i++) {
        expect(events[i]?.seq).toBe(i);
      }
    });
  });
});

describe("runInference — retry delay scheduling", () => {
  test("retry delay is honoured via the harness scheduler — virtual-clock advance is sufficient", async () => {
    await withHarness(async (harness) => {
      registerRetryable5xx(harness, 2);
      // Third matcher: a healthy response so the wrapper exits via
      // inference.done after the policy-driven retry.
      const okStream = harness.scenario.createStream();
      harness.scenario.whenRequestMatches(() => true, okStream);
      const encoder = new TextEncoder();
      okStream.enqueueAt(
        1,
        encoder.encode(
          'data: {"choices":[{"index":0,"delta":{"content":"a"}}]}\n\n',
        ),
      );
      okStream.enqueueAt(2, encoder.encode("data: [DONE]\n\n"));
      okStream.closeAt(3);

      const policy: RetryPolicy = () => ({ kind: "retry", delayMs: 250 });

      const before = harness.clock.now();
      let seq = 0;
      const consumer = startConsumer(
        runInference({
          turns: makeTurns(),
          source: SOURCE,
          inferenceOptions: { retryPolicy: policy },
          nextSeq: () => seq++,
          deps: harness.deps,
        }),
      );
      await harness.run();
      const after = harness.clock.now();
      const events = await consumer.done;

      // Two retries → virtual clock advanced at least 2 * 250 ms.
      // We assert greater-than-or-equal because the fetch chunks
      // themselves consume a small amount of virtual time on top.
      expect(retryEvents(events)).toHaveLength(2);
      expect(after - before).toBeGreaterThanOrEqual(500);
    });
  });

  test("aborting the caller signal partway through a retry delay surfaces an aborted error on the next attempt", async () => {
    await withHarness(async (harness) => {
      registerRetryable5xx(harness, 1);
      // The retry policy asks for a 1000ms delay before attempt 2.
      // The test schedules `controller.abort()` to fire at virtual
      // time +500ms — squarely inside the delay window — so the
      // wrapper is awaiting `scheduler.setTimeout` when the signal
      // aborts. When the delay's setTimeout fires the wrapper
      // re-enters `runSingleAttempt`, which checks `signal.aborted`
      // at entry and yields `inference.error` of category `aborted`.
      // The default policy aborts on that category; no further
      // attempts are issued.

      const controller = new AbortController();
      let policyCalls = 0;
      const policy: RetryPolicy = () => {
        policyCalls += 1;
        if (policyCalls === 1) {
          // First failure (the registered 503): schedule the abort
          // to fire mid-delay, then return a long-enough delay that
          // the abort lands well before the next attempt would
          // start. The scheduling is done against the harness clock
          // directly so the abort is settled in virtual time.
          harness.clock.schedule(harness.clock.now() + 500, () => {
            controller.abort();
          });
          return { kind: "retry", delayMs: 1000 };
        }
        return { kind: "abort" };
      };

      let seq = 0;
      const consumer = startConsumer(
        runInference({
          turns: makeTurns(),
          source: SOURCE,
          signal: controller.signal,
          inferenceOptions: { retryPolicy: policy },
          nextSeq: () => seq++,
          deps: harness.deps,
        }),
      );
      await harness.run();
      const events = await consumer.done;

      // One retry emitted before the signal-aborted error surfaces.
      expect(retryEvents(events)).toHaveLength(1);
      expect(findError(events)?.category).toBe("aborted");
    });
  });
});

describe("createDefaultRetryPolicy — direct unit coverage", () => {
  function situation(
    overrides: Partial<RetrySituation> & { error: InferenceError },
  ): RetrySituation {
    return {
      attempt: 1,
      elapsedMs: 0,
      ...overrides,
    };
  }
  function err(
    category: InferenceError["category"],
    extra: Partial<InferenceError> = {},
  ): InferenceError {
    return { category, message: `${category} error`, ...extra };
  }

  test("non-retryable categories always abort", async () => {
    const policy = createDefaultRetryPolicy();
    for (const category of [
      "credential_failure",
      "context_overflow",
      "fatal",
      "aborted",
      "protocol_mismatch",
    ] as const) {
      const decision = await policy(situation({ error: err(category) }));
      expect(decision).toEqual({ kind: "abort" });
    }
  });

  test("retryable + timeout: 500ms / 1000ms backoff, abort on 3rd failure", async () => {
    const policy = createDefaultRetryPolicy();
    for (const category of ["retryable", "timeout"] as const) {
      const e = err(category);
      const a1 = await policy(situation({ error: e, attempt: 1 }));
      const a2 = await policy(situation({ error: e, attempt: 2 }));
      const a3 = await policy(situation({ error: e, attempt: 3 }));
      expect(a1).toEqual({ kind: "retry", delayMs: 500 });
      expect(a2).toEqual({ kind: "retry", delayMs: 1000 });
      expect(a3).toEqual({ kind: "abort" });
    }
  });

  test("quota_exhausted: prefers retryAfterMs, falls back to flat 1000ms", async () => {
    const policy = createDefaultRetryPolicy();
    const withHeader = await policy(
      situation({
        error: err("quota_exhausted", { retryAfterMs: 1234 }),
        attempt: 1,
      }),
    );
    expect(withHeader).toEqual({ kind: "retry", delayMs: 1234 });

    const noHeader = await policy(
      situation({ error: err("quota_exhausted"), attempt: 1 }),
    );
    expect(noHeader).toEqual({ kind: "retry", delayMs: 1000 });

    const exhausted = await policy(
      situation({ error: err("quota_exhausted"), attempt: 3 }),
    );
    expect(exhausted).toEqual({ kind: "abort" });
  });
});
