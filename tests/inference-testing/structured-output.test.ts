// End-to-end replay of the committed structured-output captures.
//
// Each streaming test loads a captured session, replays its response
// through the production adapter via replayResponsesForParsing, then
// takes the accumulated text content from the finalized assistant turn
// and asserts it parses as JSON conforming to the catalog intent's
// schema. The session-corpus parser-regression suite already validates
// that every captured fixture replays cleanly against the shape
// invariants; these tests are the extra step that turns the bytes
// into a typed value and pins the round-trip — the model produced
// schema-conformant JSON, the wire bytes survived capture and
// replay, the adapter assembled the text deltas into a coherent
// content block, and the bytes still parse on the other side.
//
// The refusal-path round-trip is covered by synthetic SSE through the
// OpenAI adapter (tests/inference/providers/openai.test.ts) and the
// harness (tests/inference/refusal-harness.test.ts). A live
// openai/gpt-5.5/structured-output-refusal-streaming probe is retained
// as a misled matrix row: the model did not emit delta.refusal.

import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { type } from "arktype";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { replayResponsesForParsing } from "@intx/inference-testing";
import {
  createOpenAIAdapter,
  createGoogleGenAIAdapter,
} from "@intx/inference/providers";
import {
  INTENTS,
  SUPPORT_MATRIX,
  getSessionDir,
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

// Resolve a session directory through the catalog's canonical resolver so
// this test follows the corpus wherever getSessionDir points, rather than
// reconstructing a root of its own.
function sessionDirFor(
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
  const relDir = getSessionDir(entry);
  if (relDir === null) {
    throw new Error(
      `entry ${provider}/${model}/${capability} is not fixture-bearing`,
    );
  }
  return path.resolve(WORKSPACE_ROOT, relDir);
}

// A single-exchange capture's request/response bytes live under exchanges/0.
function exchange0(sessionDir: string, file: string): string {
  return path.join(sessionDir, "exchanges", "0", file);
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
  const sessionDir = sessionDirFor(opts.provider, opts.model, opts.capability);
  const results = await replayResponsesForParsing({ sessionDir });
  const replayed = results.filter((r) => r.kind === "replayed");
  if (replayed.length === 0) {
    throw new Error(
      `expected a replayed exchange for ${opts.provider}/${opts.model}/${opts.capability}`,
    );
  }
  const violations = replayed.flatMap((r) => r.violations);
  if (violations.length > 0) {
    throw new Error(
      `parser violations on ${opts.provider}/${opts.model}/${opts.capability}:\n${JSON.stringify(violations, null, 2)}`,
    );
  }
  return replayed.flatMap((r) => r.events);
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
// response body), not response.sse. For the non-streaming variant this
// test reads the response payload directly from disk and pulls the
// assistant content via a provider-specific path — a simpler check than
// replaying, and independent of the adapter's non-streaming decode,
// which the parser-regression suite exercises separately.

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
  const responsePath = exchange0(
    sessionDirFor(opts.provider, opts.model, opts.capability),
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
  const responsePath = exchange0(
    sessionDirFor(opts.provider, opts.model, opts.capability),
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

describe("structured-output round-trip — openai gpt-5.6-sol", () => {
  test("non-streaming JSON parses against the catalog schema", () => {
    const content = readOpenAINonStreamingContent({
      provider: "openai",
      model: "gpt-5.6-sol",
      capability: "structured-output",
    });
    assertContentSchemaConformant(content);
  });

  test("streaming JSON parses against the catalog schema", async () => {
    const events = await replayFixture({
      provider: "openai",
      model: "gpt-5.6-sol",
      capability: "structured-output-streaming",
    });
    assertSchemaConformant(events);
  });
});

describe("structured-output round-trip — google-genai gemini-3.6-flash", () => {
  test("non-streaming JSON parses against the catalog schema", () => {
    const content = readGeminiNonStreamingContent({
      provider: "google-genai",
      model: "gemini-3.6-flash",
      capability: "structured-output",
    });
    assertContentSchemaConformant(content);
  });

  test("streaming JSON parses against the catalog schema", async () => {
    const events = await replayFixture({
      provider: "google-genai",
      model: "gemini-3.6-flash",
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
      exchange0(
        sessionDirFor("opencode-zen", "gpt-5.4-mini", "structured-output"),
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
      exchange0(
        sessionDirFor("google-genai", "gemini-2.5-flash", "structured-output"),
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

  test("OpenAI first-party gpt-5.6-sol: adapter.response_format matches captured request body", () => {
    const adapter = createOpenAIAdapter(OPENAI_SOURCE);
    const adapterReq = adapter.buildRequest(
      [PROMPT_TURN],
      "gpt-5.6-sol",
      OPTIONS_FROM_INTENT,
    );
    const adapterBody = CapturedOpenAIBody.assert(JSON.parse(adapterReq.body));
    const capturedRaw = readFileSync(
      exchange0(
        sessionDirFor("openai", "gpt-5.6-sol", "structured-output"),
        "request.json",
      ),
      "utf8",
    );
    const capturedBody = CapturedOpenAIBody.assert(JSON.parse(capturedRaw));
    expect(adapterBody.response_format).toEqual(capturedBody.response_format);
  });

  test("Google GenAI gemini-3.6-flash: adapter structured-output fields match capture", () => {
    const adapter = createGoogleGenAIAdapter(GOOGLE_SOURCE);
    const adapterReq = adapter.buildRequest(
      [PROMPT_TURN],
      "gemini-3.6-flash",
      OPTIONS_FROM_INTENT,
    );
    const adapterBody = CapturedGeminiBody.assert(JSON.parse(adapterReq.body));
    const capturedRaw = readFileSync(
      exchange0(
        sessionDirFor("google-genai", "gemini-3.6-flash", "structured-output"),
        "request.json",
      ),
      "utf8",
    );
    const capturedBody = CapturedGeminiBody.assert(JSON.parse(capturedRaw));
    expect(adapterBody.generationConfig?.responseMimeType).toBe(
      capturedBody.generationConfig?.responseMimeType,
    );
    expect(adapterBody.generationConfig?.responseSchema).toEqual(
      capturedBody.generationConfig?.responseSchema,
    );
  });
});
