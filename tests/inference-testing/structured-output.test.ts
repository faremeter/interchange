// End-to-end replay of the committed structured-output captures.
//
// Each test loads a live wire fixture, drives it through the
// production SSE-parsing adapter via runCompatReplay, then takes
// the accumulated text content from the finalized assistant turn
// and asserts it parses as JSON conforming to the catalog intent's
// schema. The compat-replay corpus suite already validates that
// every captured fixture replays cleanly against the shape
// invariants; these tests are the extra step that turns the bytes
// into a typed value and pins the round-trip — the model produced
// schema-conformant JSON, the wire bytes survived capture and
// replay, the adapter assembled the text deltas into a coherent
// content block, and the bytes still parse on the other side.
//
// The refusal-path round-trip is covered as a unit test in
// packages/inference/src/providers/openai.test.ts via a hand-crafted
// SSE fixture because opencode-zen strips OpenAI's delta.refusal
// field on relay. INTR-124 lands a live refusal capture once a
// direct OpenAI deployment plug-in is wired.

import { describe, expect, test } from "bun:test";
import { type } from "arktype";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { runCompatReplay } from "@intx/inference-testing";
import type { InferenceEvent } from "@intx/types/runtime";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const FIXTURE_ROOT = path.resolve(
  __dirname,
  "..",
  "..",
  "packages",
  "inference-testing",
  "wire",
);

// The catalog intent in
// packages/inference-discovery/src/catalog/intent.ts declares this
// schema for the structured-output probe. Mirroring it here as an
// arktype validator turns "the accumulated assistant text" into a
// typed value and pins both the model's adherence and the adapter's
// assembly correctness.
const UserInfo = type({
  name: "string",
  age: "number.integer",
  email: "string",
});

// Pull the accumulated text from every text-delta event the
// streaming replay emitted. The captured JSON content for these
// fixtures lands in delta.content frames (OpenAI) or
// candidates[0].content.parts[0].text frames (Gemini); both
// providers' adapters surface them as inference.text.delta.
function accumulateText(events: readonly InferenceEvent[]): string {
  return events
    .map((e) => (e.type === "inference.text.delta" ? e.data.token : ""))
    .join("");
}

async function replayStreamingFixture(opts: {
  provider: string;
  model: string;
  capability: string;
}): Promise<readonly InferenceEvent[]> {
  const fixtureDir = path.join(
    FIXTURE_ROOT,
    opts.provider,
    opts.model,
    opts.capability,
  );
  const result = await runCompatReplay({
    fixtureDir,
    provider: opts.provider,
    model: opts.model,
  });
  if (result.kind !== "replayed") {
    throw new Error(
      `expected compat-replay to complete; got skipped: ${result.reason}`,
    );
  }
  if (result.violations.length > 0) {
    throw new Error(
      `compat-replay violations on ${opts.provider}/${opts.model}/${opts.capability}:\n${JSON.stringify(result.violations, null, 2)}`,
    );
  }
  return result.events;
}

describe("structured-output round-trip — opencode-zen gpt-5.4-mini", () => {
  test("streaming JSON parses against the catalog schema", async () => {
    const events = await replayStreamingFixture({
      provider: "opencode-zen",
      model: "gpt-5.4-mini",
      capability: "structured-output-streaming",
    });
    const accumulated = accumulateText(events).trim();
    const parsed: unknown = JSON.parse(accumulated);
    const validated = UserInfo.assert(parsed);
    // The catalog intent's prompt names Alice / 30 / alice@example.com
    // verbatim; on the capture day the model surfaced those values.
    // Future re-captures may produce different equivalent JSON, so
    // the assertion sticks to the schema shape rather than the
    // specific values.
    expect(typeof validated.name).toBe("string");
    expect(Number.isInteger(validated.age)).toBe(true);
    expect(typeof validated.email).toBe("string");
  });
});

describe("structured-output round-trip — google-genai gemini-2.5-flash", () => {
  test("streaming JSON parses against the catalog schema", async () => {
    const events = await replayStreamingFixture({
      provider: "google-genai",
      model: "gemini-2.5-flash",
      capability: "structured-output-streaming",
    });
    const accumulated = accumulateText(events).trim();
    const parsed: unknown = JSON.parse(accumulated);
    const validated = UserInfo.assert(parsed);
    expect(typeof validated.name).toBe("string");
    expect(Number.isInteger(validated.age)).toBe(true);
    expect(typeof validated.email).toBe("string");
  });
});
