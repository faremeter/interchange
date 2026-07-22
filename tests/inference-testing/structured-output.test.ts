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
import { readFileSync } from "node:fs";
import { type } from "arktype";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { runCompatReplay } from "@intx/inference-testing";
import {
  createOpenAIAdapter,
  createGoogleGenAIAdapter,
} from "@intx/inference/providers";
import {
  INTENTS,
  SUPPORT_MATRIX,
  getFixtureDir,
} from "@intx/inference-discovery/catalog";
import type {
  ConversationTurn,
  InferenceEvent,
  InferenceOptions,
  LastCycleSource,
} from "@intx/types/runtime";

const OPENAI_SOURCE: LastCycleSource = {
  sourceId: "test-openai",
  provider: "openai",
  model: "test-openai-model",
};

const GOOGLE_SOURCE: LastCycleSource = {
  sourceId: "test-google-genai",
  provider: "google-genai",
  model: "test-google-genai-model",
};

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const WORKSPACE_ROOT = path.resolve(__dirname, "..", "..");

// Resolve a fixture directory through the catalog's canonical resolver so
// this test follows the corpus wherever getFixtureDir points, rather than
// reconstructing a wire root of its own.
function fixtureDirFor(
  provider: string,
  model: string,
  capability: string,
): string {
  const entry = SUPPORT_MATRIX.find(
    (e) =>
      e.provider === provider &&
      e.model === model &&
      e.capability === capability,
  );
  if (entry === undefined) {
    throw new Error(
      `no support-matrix entry for ${provider}/${model}/${capability}`,
    );
  }
  const relDir = getFixtureDir(entry);
  if (relDir === null) {
    throw new Error(
      `entry ${provider}/${model}/${capability} is not fixture-bearing`,
    );
  }
  return path.resolve(WORKSPACE_ROOT, relDir);
}

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

async function replayFixture(opts: {
  provider: string;
  model: string;
  capability: string;
}): Promise<readonly InferenceEvent[]> {
  const fixtureDir = fixtureDirFor(opts.provider, opts.model, opts.capability);
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

function assertSchemaConformant(events: readonly InferenceEvent[]): void {
  const accumulated = accumulateText(events).trim();
  const parsed: unknown = JSON.parse(accumulated);
  const validated = UserInfo.assert(parsed);
  // The catalog intent's prompt names Alice / 30 / alice@example.com
  // verbatim; on the capture day the model surfaced those values.
  // Future re-captures may produce different equivalent JSON, so the
  // assertion sticks to the schema shape rather than the specific
  // values.
  expect(typeof validated.name).toBe("string");
  expect(Number.isInteger(validated.age)).toBe(true);
  expect(typeof validated.email).toBe("string");
}

// Non-streaming captures live as response.json (the raw provider
// response body), not response.sse. runCompatReplay is SSE-only —
// the harness consumes the streaming wire format end-to-end. For
// the non-streaming variant the test extracts the response payload
// directly from disk and pulls the assistant content via a
// provider-specific path. The runtime adapter does not parse
// non-streaming responses today (that's its own concern), so the
// extraction lives here.

const OpenAIChatCompletion = type({
  choices: type({
    message: type({
      "content?": "string | null",
    }),
  }).array(),
});

const GeminiResponse = type({
  candidates: type({
    content: type({
      parts: type({
        "text?": "string",
      }).array(),
    }),
  }).array(),
});

function readOpenAINonStreamingContent(opts: {
  provider: string;
  model: string;
  capability: string;
}): string {
  const responsePath = path.join(
    fixtureDirFor(opts.provider, opts.model, opts.capability),
    "response.json",
  );
  const raw = JSON.parse(readFileSync(responsePath, "utf8"));
  const parsed = OpenAIChatCompletion.assert(raw);
  const content = parsed.choices[0]?.message.content;
  if (typeof content !== "string") {
    throw new Error(
      `expected non-empty string content in ${responsePath}; got ${typeof content}`,
    );
  }
  return content;
}

function readGeminiNonStreamingContent(opts: {
  provider: string;
  model: string;
  capability: string;
}): string {
  const responsePath = path.join(
    fixtureDirFor(opts.provider, opts.model, opts.capability),
    "response.json",
  );
  const raw = JSON.parse(readFileSync(responsePath, "utf8"));
  const parsed = GeminiResponse.assert(raw);
  const text = parsed.candidates[0]?.content.parts
    .map((p) => p.text ?? "")
    .join("");
  if (text === undefined || text.length === 0) {
    throw new Error(`expected non-empty text in ${responsePath}`);
  }
  return text;
}

function assertContentSchemaConformant(content: string): void {
  const parsed: unknown = JSON.parse(content.trim());
  const validated = UserInfo.assert(parsed);
  expect(typeof validated.name).toBe("string");
  expect(Number.isInteger(validated.age)).toBe(true);
  expect(typeof validated.email).toBe("string");
}

describe("structured-output round-trip — opencode-zen gpt-5.4-mini", () => {
  test("non-streaming JSON parses against the catalog schema", () => {
    const content = readOpenAINonStreamingContent({
      provider: "opencode-zen",
      model: "gpt-5.4-mini",
      capability: "structured-output",
    });
    assertContentSchemaConformant(content);
  });

  test("streaming JSON parses against the catalog schema", async () => {
    const events = await replayFixture({
      provider: "opencode-zen",
      model: "gpt-5.4-mini",
      capability: "structured-output-streaming",
    });
    assertSchemaConformant(events);
  });
});

describe("structured-output round-trip — google-genai gemini-2.5-flash", () => {
  test("non-streaming JSON parses against the catalog schema", () => {
    const content = readGeminiNonStreamingContent({
      provider: "google-genai",
      model: "gemini-2.5-flash",
      capability: "structured-output",
    });
    assertContentSchemaConformant(content);
  });

  test("streaming JSON parses against the catalog schema", async () => {
    const events = await replayFixture({
      provider: "google-genai",
      model: "gemini-2.5-flash",
      capability: "structured-output-streaming",
    });
    assertSchemaConformant(events);
  });
});

// Drift guard: the per-provider responseFormat translation lives in
// two places — the runtime adapter (`@intx/inference`) and the
// discovery plug-in (`@intx/inference-discovery-*`). The two
// builders read from the same CapabilityIntent shape and must
// produce the same provider-native wire payload, but nothing in the
// type system pins that. These tests build a request through the
// adapter using the catalog intent's responseFormat as input, then
// load the captured request body from disk (which the discovery
// plug-in produced), and assert the provider-native structured-
// output field is byte-equal. A drift between the two builders
// fails one of these tests with the diff.

const STRUCTURED_INTENT = INTENTS["structured-output"];
const PROMPT_TURN: ConversationTurn = {
  role: "user",
  content: [{ type: "text", text: STRUCTURED_INTENT.prompt }],
  timestamp: 0,
};
const OPTIONS_FROM_INTENT: InferenceOptions = {
  ...(STRUCTURED_INTENT.responseFormat !== undefined
    ? { responseFormat: STRUCTURED_INTENT.responseFormat }
    : {}),
};

const CapturedOpenAIBody = type({
  "response_format?": "unknown",
});

const CapturedGeminiBody = type({
  "generationConfig?": type({
    "responseMimeType?": "string",
    "responseSchema?": "unknown",
  }),
});

describe("translation drift guard — adapter vs discovery plug-in", () => {
  test("OpenAI: adapter.response_format matches captured request body", () => {
    const adapter = createOpenAIAdapter(OPENAI_SOURCE);
    const adapterReq = adapter.buildRequest(
      [PROMPT_TURN],
      "gpt-5.4-mini",
      OPTIONS_FROM_INTENT,
    );
    const adapterBody = CapturedOpenAIBody.assert(JSON.parse(adapterReq.body));
    const capturedRaw = readFileSync(
      path.join(
        fixtureDirFor("opencode-zen", "gpt-5.4-mini", "structured-output"),
        "request.json",
      ),
      "utf8",
    );
    const capturedBody = CapturedOpenAIBody.assert(JSON.parse(capturedRaw));
    expect(adapterBody.response_format).toEqual(capturedBody.response_format);
  });

  test("Google GenAI: adapter.generationConfig matches captured request body", () => {
    const adapter = createGoogleGenAIAdapter(GOOGLE_SOURCE);
    const adapterReq = adapter.buildRequest(
      [PROMPT_TURN],
      "gemini-2.5-flash",
      OPTIONS_FROM_INTENT,
    );
    const adapterBody = CapturedGeminiBody.assert(JSON.parse(adapterReq.body));
    const capturedRaw = readFileSync(
      path.join(
        fixtureDirFor("google-genai", "gemini-2.5-flash", "structured-output"),
        "request.json",
      ),
      "utf8",
    );
    const capturedBody = CapturedGeminiBody.assert(JSON.parse(capturedRaw));
    // Only the structured-output-relevant subset of generationConfig
    // is in the contract: maxOutputTokens / thinkingConfig / etc.
    // may differ between the discovery probe and an adapter call
    // configured with different timeouts. Pin only responseMimeType
    // and responseSchema.
    expect(adapterBody.generationConfig?.responseMimeType).toBe(
      capturedBody.generationConfig?.responseMimeType,
    );
    expect(adapterBody.generationConfig?.responseSchema).toEqual(
      capturedBody.generationConfig?.responseSchema,
    );
  });
});
