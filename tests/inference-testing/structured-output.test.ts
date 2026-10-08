// End-to-end replay of the committed structured-output captures. Each test
// replays a captured session through the production adapter via
// replayResponsesForParsing and asserts the accumulated assistant text
// parses as JSON per the catalog intent's schema — the step past the
// parser-regression suite's shape invariants that pins the typed round-trip.
// The refusal path is covered by synthetic SSE elsewhere; a live refusal
// probe stays as a misled matrix row (the model did not emit delta.refusal).

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

// Resolve a session directory through the catalog's canonical resolver.
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

// Mirrors the structured-output probe's schema from the catalog intent as an
// arktype validator, pinning the model's adherence and the adapter's assembly.
const UserInfo = type({
  name: "string",
  age: "number.integer",
  email: "string",
});

// Pull the accumulated text from every inference.text.delta the streaming
// replay emitted (OpenAI and Gemini both surface their text frames that way).
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
  // The model surfaced these values on the capture day; re-captures may
  // differ, so the assertion sticks to the schema shape.
  expect(typeof validated.name).toBe("string");
  expect(Number.isInteger(validated.age)).toBe(true);
  expect(typeof validated.email).toBe("string");
}

// Non-streaming captures live as response.json; this variant reads the
// payload directly from disk via a provider-specific path, independent of the
// adapter's non-streaming decode (covered by the parser-regression suite).

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

// Drift guard: the per-provider responseFormat translation lives in both the
// runtime adapter and the discovery plug-in, and nothing in the type system
// pins them together. These tests build a request through the adapter from
// the catalog intent's responseFormat, load the captured request body the
// plug-in produced, and assert the provider-native field is byte-equal.

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
    // Pin only responseMimeType/responseSchema; the rest of generationConfig
    // may differ between the probe and an adapter call.
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
