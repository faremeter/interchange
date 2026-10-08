// Live drift smoke test for the Gemini adapter. Runs only when
// `GEMINI_API_KEY` is set; CI and local runs skip cleanly.
//
// The fixtures freeze a moment in time; this runs the smallest
// end-to-end path against the real endpoint to catch wire-format
// drift the offline corpus cannot distinguish from a parser
// regression. Deliberately small to save quota; asserts shape, not
// content.

import { describe, expect, test } from "bun:test";

import { runInference } from "@intx/inference";
import type { Dependencies, Scheduler } from "@intx/inference";
import { createBuiltinRegistry } from "@intx/inference/providers";
import type { InferenceEvent, InferenceSource } from "@intx/types/runtime";

const GEMINI_API_KEY = process.env.GEMINI_API_KEY;

describe("Google GenAI adapter: live drift", () => {
  const inertScheduler: Scheduler = {
    setTimeout: () => () => {
      /* tests do not exercise timer firing */
    },
    now: () => 0,
  };

  test.skipIf(GEMINI_API_KEY === undefined || GEMINI_API_KEY === "")(
    "plain-text streaming against the live endpoint produces text deltas + a final inference.done turn",
    async () => {
      // `skipIf` evaluates at collection time; the inner guard narrows
      // the type without a non-null assertion.
      const apiKey = GEMINI_API_KEY;
      if (apiKey === undefined || apiKey === "") {
        throw new Error(
          "GEMINI_API_KEY guard inverted: the skipIf predicate should " +
            "have stopped this test from running.",
        );
      }

      const source: InferenceSource = {
        id: "google-genai:gemini-2.5-flash",
        provider: "google-genai",
        baseURL: "https://generativelanguage.googleapis.com",
        credentialId: "google-genai-live",
        model: "gemini-2.5-flash",
      };

      // Real fetch; the harness swaps the sentinel for the key between
      // buildRequest and the fetch call.
      const deps: Dependencies = {
        fetch: globalThis.fetch.bind(globalThis),
        scheduler: inertScheduler,
        adapters: createBuiltinRegistry(),
      };

      let seq = 0;
      const events: InferenceEvent[] = [];
      for await (const ev of runInference({
        turns: [
          {
            role: "user",
            content: [
              {
                type: "text",
                text: "Reply with the single word 'ready' and nothing else.",
              },
            ],
            timestamp: 0,
          },
        ],
        source,
        nextSeq: () => seq++,
        deps,
        readMaterial: () => ({ secret: apiKey }),
        // No thinking: minimal shape, low latency — a shape check,
        // not a quality check.
        inferenceOptions: {
          thinking: { enabled: false },
          maxTokens: 16,
        },
      })) {
        events.push(ev);
      }

      // Structural only; the model may vary punctuation or casing on
      // the "ready" reply.
      const textDeltas = events.filter(
        (e) => e.type === "inference.text.delta",
      );
      expect(textDeltas.length).toBeGreaterThan(0);

      const usageEvents = events.filter((e) => e.type === "inference.usage");
      expect(usageEvents.length).toBeGreaterThan(0);
      const lastUsage = usageEvents[usageEvents.length - 1];
      if (lastUsage?.type !== "inference.usage") {
        throw new Error("expected at least one inference.usage event");
      }
      expect(lastUsage.data.usage.input).toBeGreaterThan(0);
      expect(lastUsage.data.usage.output).toBeGreaterThan(0);

      const done = events.find((e) => e.type === "inference.done");
      if (done?.type !== "inference.done") {
        throw new Error("expected inference.done event");
      }
      expect(done.data.turn.role).toBe("assistant");
      expect(done.data.turn.content.length).toBeGreaterThan(0);
      // Must lead with a text block; any other kind means the parser
      // misrouted the response — a backstop against an unexpected
      // wire shape.
      const firstBlock = done.data.turn.content[0];
      if (firstBlock?.type !== "text") {
        throw new Error(
          `expected first content block to be text, got ${JSON.stringify(firstBlock?.type)}`,
        );
      }
      expect(firstBlock.text.length).toBeGreaterThan(0);

      // No inference.error events on a successful response.
      const errors = events.filter((e) => e.type === "inference.error");
      expect(errors).toHaveLength(0);

      // The harness emits inference.usage before inference.done.
      const usageIdx = events.findIndex((e) => e.type === "inference.usage");
      const doneIdx = events.findIndex((e) => e.type === "inference.done");
      expect(usageIdx).toBeGreaterThan(-1);
      expect(usageIdx).toBeLessThan(doneIdx);
    },
  );
});
