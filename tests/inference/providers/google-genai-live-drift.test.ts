// Live drift smoke test for the Gemini adapter. Runs only when
// `GEMINI_API_KEY` is set in the environment; CI and local
// developer runs without the variable skip cleanly.
//
// The captured fixtures pin every wire shape the parser handles,
// but they freeze a particular moment in time -- Google can
// (and does) change Gemini's wire format under us. This test
// exercises the smallest end-to-end path (a plain-text streaming
// inference) against the real endpoint and asserts that the
// parser still produces the structural shape downstream code
// depends on. A red here means either the API drifted (re-capture
// the fixtures) or the parser regressed against the real wire
// (fix the adapter); the offline test corpus alone cannot
// distinguish those.
//
// Deliberately small: a longer or more elaborate prompt would
// burn API quota for no signal. The test asserts shape, not
// content -- the model is free to answer however it wants.

import { describe, expect, test } from "bun:test";

import { runInference } from "@intx/inference";
import type { Dependencies, Scheduler } from "@intx/inference";
import type { InferenceEvent, InferenceSource } from "@intx/types/runtime";

const GEMINI_API_KEY = process.env.GEMINI_API_KEY;

describe("Google GenAI adapter: live drift", () => {
  const inertScheduler: Scheduler = {
    setTimeout: () => () => {
      /* tests do not exercise timer firing */
    },
  };

  test.skipIf(GEMINI_API_KEY === undefined || GEMINI_API_KEY === "")(
    "plain-text streaming against the live endpoint produces text deltas + a final inference.done turn",
    async () => {
      // `skipIf` evaluates the predicate at test-collection time;
      // the inner guard keeps the type-narrowing honest without a
      // non-null assertion.
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
        apiKey,
        model: "gemini-2.5-flash",
      };

      // Default `fetch` against the real endpoint. The harness
      // performs credential substitution between the adapter's
      // `buildRequest` (which emits a sentinel) and the actual
      // fetch call.
      const deps: Dependencies = {
        fetch: globalThis.fetch.bind(globalThis),
        scheduler: inertScheduler,
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
        // Disable thinking to keep the response shape minimal and
        // the round-trip latency low. The drift test is a shape
        // check, not a quality check.
        inferenceOptions: {
          thinking: { enabled: false },
          maxTokens: 16,
        },
      })) {
        events.push(ev);
      }

      // Structural assertions only. The model's exact text is not
      // pinned -- "ready" is the requested response, but the model
      // may add punctuation, capitalization, or a trailing period.
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
      // The final turn must lead with a text block (the model's
      // reply). A different leading block kind would mean the
      // parser misrouted the response; the offline corpus would
      // have caught that, but the assertion here is a backstop
      // against an unexpected wire shape.
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
