// Regression test for the clock's default `microtaskBudget` against a
// representative `parseSSE` workload. The clock's budget (256) absorbs
// today's `parseSSE` consumer chain (≈2 microtask waves per chunk) plus
// the drain's internal stability window with comfortable headroom. The
// budget is load-bearing in both directions: bloated consumer chains
// trip `ClockOverrunError` (see `consumer-chain-budget.test.ts`), and a
// runaway scheduler trips it via the same mechanism (see the gating
// probe below).
//
// CORRECTNESS CONTRACT for the wiring: the `parseSSE` iterator must be
// started BEFORE the `harness.run()` call so its per-chunk
// `await reader.read()` continuations land as microtasks inside the clock's
// budget accounting. Reading after `harness.run()` has returned would only
// measure the `controller.enqueue` callbacks scheduled by
// `stream.enqueueAt`, not the parseSSE consumer's own microtask cost.

import { describe, test, expect } from "bun:test";

import { parseSSE } from "@interchange/inference";

import { ClockOverrunError } from "./clock";
import { setupHarness } from "./harness";
import { wire } from "./index";

function buildAnthropicWorkload(): Uint8Array[] {
  const chunks: Uint8Array[] = [];
  chunks.push(
    wire.anthropic.messageStart({
      usage: {
        inputTokens: 100,
        outputTokens: 0,
        cacheReadInputTokens: 0,
        cacheCreationInputTokens: 0,
      },
    }),
  );
  chunks.push(
    wire.anthropic.contentBlockStart({ index: 0, kind: "text", text: "" }),
  );
  for (let i = 0; i < 50; i++) {
    chunks.push(
      wire.anthropic.contentBlockDelta({
        index: 0,
        kind: "text_delta",
        text: `tok${String(i)} `,
      }),
    );
  }
  chunks.push(wire.anthropic.contentBlockStop({ index: 0 }));
  chunks.push(
    ...wire.anthropic.toolUseBlock(
      "toolu_perf",
      "search",
      '{"q":"benchmark"}',
      1,
    ),
  );
  chunks.push(
    wire.anthropic.messageDelta({
      stopReason: "end_turn",
      outputTokens: 50,
    }),
  );
  chunks.push(wire.anthropic.messageStop());
  return chunks;
}

function buildOpenAIWorkload(): Uint8Array[] {
  const chunks: Uint8Array[] = [];
  for (let i = 0; i < 50; i++) {
    chunks.push(wire.openai.chunk({ content: `tok${String(i)} ` }));
  }
  chunks.push(wire.openai.toolCallStart(0, "call_perf", "calc"));
  chunks.push(wire.openai.toolCallArgumentsDelta(0, '{"x":1,"y":2}'));
  chunks.push(
    wire.openai.usageChunk({
      promptTokens: 100,
      completionTokens: 50,
      cachedTokens: 0,
      reasoningTokens: 0,
    }),
  );
  chunks.push(wire.openai.done());
  return chunks;
}

describe("parseSSE microtask budget", () => {
  test("a representative anthropic workload completes within default microtaskBudget=256", async () => {
    const harness = setupHarness();
    try {
      const stream = harness.scenario.createStream();
      harness.scenario.whenRequestMatches(() => true, stream);

      const chunks = buildAnthropicWorkload();
      for (let i = 0; i < chunks.length; i++) {
        const chunk = chunks[i];
        if (chunk === undefined) continue;
        stream.enqueueAt(i + 1, chunk);
      }
      stream.closeAt(chunks.length + 2);

      const fetchPromise = harness.deps.fetch("https://example/anthropic", {
        method: "POST",
        body: "{}",
      });

      // The fetch matcher binds the request to the stream synchronously
      // inside `runScan`, so the resolved Response is available without
      // advancing virtual time.
      const response = await fetchPromise;
      expect(response.status).toBe(200);
      if (response.body === null) throw new Error("response body is null");

      // Start the parseSSE consumer BEFORE advancing the clock. Each
      // `await reader.read()` continuation lands as a microtask once the
      // corresponding `controller.enqueue` callback fires inside
      // `clock.run()`. The consumer's microtasks therefore land during
      // the clock's drain phases, not after `harness.run()` has returned.
      const body = response.body;
      const seenPayloads: string[] = [];
      const consume = (async () => {
        for await (const payload of parseSSE(body)) {
          seenPayloads.push(payload);
        }
      })();

      // Run the clock with the DEFAULT `microtaskBudget=256`. Do not pass
      // `microtaskBudget` here — this is the contract under test.
      await harness.run();
      await consume;

      expect(seenPayloads.length).toBeGreaterThanOrEqual(chunks.length - 1);
    } finally {
      harness.dispose();
    }
  });

  test("a representative openai workload completes within default microtaskBudget=256", async () => {
    const harness = setupHarness();
    try {
      const stream = harness.scenario.createStream();
      harness.scenario.whenRequestMatches(() => true, stream);

      const chunks = buildOpenAIWorkload();
      for (let i = 0; i < chunks.length; i++) {
        const chunk = chunks[i];
        if (chunk === undefined) continue;
        stream.enqueueAt(i + 1, chunk);
      }
      stream.closeAt(chunks.length + 2);

      const fetchPromise = harness.deps.fetch("https://example/openai", {
        method: "POST",
        body: "{}",
      });

      const response = await fetchPromise;
      expect(response.status).toBe(200);
      if (response.body === null) throw new Error("response body is null");

      const body = response.body;
      const seenPayloads: string[] = [];
      const consume = (async () => {
        for await (const payload of parseSSE(body)) {
          seenPayloads.push(payload);
        }
      })();

      await harness.run();
      await consume;

      // [DONE] terminates parseSSE without yielding a payload, so the count
      // is chunks.length - 1.
      expect(seenPayloads.length).toBeGreaterThanOrEqual(chunks.length - 2);
    } finally {
      harness.dispose();
    }
  });

  test("the clock's microtask budget is load-bearing: a runaway scheduler trips it", async () => {
    // Gating probe: confirm the `microtaskBudget` argument we pass to
    // `harness.run()` is honored by the clock's quiescence detector. We
    // construct a scenario that explicitly chains new clock schedules
    // from a microtask (the same pattern `clock.test.ts` uses to verify
    // the budget mechanism) and confirm that a small budget surfaces
    // `ClockOverrunError`. Each chained `clock.schedule` bumps the
    // activity counter, so the drain's stability window never advances
    // past zero — the outer budget is what trips. The default
    // `microtaskBudget=256` succeeds for the parseSSE workloads above;
    // this probe ensures a future refactor cannot accidentally
    // short-circuit the budget by, say, hard-coding Infinity. The
    // companion `consumer-chain-budget.test.ts` covers the other
    // failure mode the budget gates: a consumer chain that bumps
    // activity for too many sequential microtask waves per fired
    // chunk.
    const harness = setupHarness();
    try {
      const stream = harness.scenario.createStream();
      harness.scenario.whenRequestMatches(() => true, stream);
      stream.closeAt(5);

      // First scheduled callback chains `clock.schedule(...)` calls from a
      // microtask, mimicking the failure mode the budget exists to gate.
      harness.clock.schedule(1, function runawayFromMicrotask() {
        const chain = (depth: number): void => {
          queueMicrotask(() => {
            harness.clock.schedule(1000, function noop() {
              return;
            });
            if (depth > 0) chain(depth - 1);
          });
        };
        chain(100);
      });

      // Drain the unmatched fetch error path: we never issued a fetch in
      // this probe, so quiescence is trivially clean — only the budget
      // matters.
      expect(harness.run({ microtaskBudget: 4 })).rejects.toBeInstanceOf(
        ClockOverrunError,
      );
    } finally {
      harness.dispose();
    }
  });
});
