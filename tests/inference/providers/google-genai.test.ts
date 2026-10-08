// Tests for the Gemini (`google-genai`) provider adapter: buildRequest
// shape and fixture parity, parseResponse per-event behavior, and a
// harness-level round trip via `runInference`. Fixtures live in
// `packages/inference-discovery-google-genai/sessions/google-genai` and
// were captured against live Gemini endpoints.

import { readFileSync } from "node:fs";
import { join } from "node:path";

import { type } from "arktype";
import { beforeEach, describe, expect, test } from "bun:test";

import {
  CREDENTIAL_SENTINEL,
  parseSSE,
  ProtocolMismatchError,
  runInference,
  type Dependencies,
  type ProviderAdapter,
  type Scheduler,
} from "@intx/inference";
import {
  createBuiltinRegistry,
  createGoogleGenAIAdapter,
  loadAdapterRegistry,
} from "@intx/inference/providers";
import type {
  AssistantTurn,
  ContentBlock,
  ConversationTurn,
  InferenceEvent,
  InferenceSource,
  LastCycleSource,
} from "@intx/types/runtime";

const TEST_SOURCE: LastCycleSource = {
  sourceId: "test-google-genai",
  provider: "google-genai",
  model: "test-google-genai-model",
};

const FIXTURE_ROOT = join(
  import.meta.dir,
  "..",
  "..",
  "..",
  "packages",
  "inference-discovery-google-genai",
  "sessions",
  "google-genai",
);

// Permissive top-level body schema: each key is `unknown` so per-test
// schemas can narrow just the slice they assert on.
const GeminiBody = type({
  contents: "unknown",
  "systemInstruction?": "unknown",
  "tools?": "unknown",
  "generationConfig?": "unknown",
  "safetySettings?": "unknown",
});
function parseBody(body: string): typeof GeminiBody.infer {
  return GeminiBody.assert(JSON.parse(body));
}

// Same schema as parseBody so fixture/body comparisons stay toEqual-able
// without one side widening to `Record<string, unknown>`.
function readFixtureJSON(...path: string[]): typeof GeminiBody.infer {
  return GeminiBody.assert(
    JSON.parse(readFileSync(join(FIXTURE_ROOT, ...path), "utf-8")),
  );
}

const GeminiContent = type({
  role: "'user' | 'model'",
  parts: "unknown[]",
});
const GeminiContents = GeminiContent.array();

const SystemInstruction = type({
  parts: type({ text: "string" }).array(),
});

// Fresh adapter per test so no parser state leaks between tests.
let adapter: ProviderAdapter;
beforeEach(() => {
  adapter = createGoogleGenAIAdapter(TEST_SOURCE);
});

describe("Google GenAI adapter: URL and headers", () => {
  test("URL is path-only with model interpolated and streaming pinned", () => {
    const req = adapter.buildRequest(
      [
        {
          role: "user",
          content: [{ type: "text", text: "hi" }],
          timestamp: 0,
        },
      ],
      "gemini-2.5-flash",
      {},
    );
    expect(req.url).toBe(
      "/v1beta/models/gemini-2.5-flash:streamGenerateContent?alt=sse",
    );
  });

  test("model name is encoded with encodeURIComponent", () => {
    // URL-special characters in a model name are escaped at the path
    // layer rather than producing a malformed URL.
    const req = adapter.buildRequest(
      [
        {
          role: "user",
          content: [{ type: "text", text: "hi" }],
          timestamp: 0,
        },
      ],
      "weird/model?name",
      {},
    );
    expect(req.url).toBe(
      "/v1beta/models/weird%2Fmodel%3Fname:streamGenerateContent?alt=sse",
    );
  });

  test("headers include content-type and x-goog-api-key sentinel", () => {
    const req = adapter.buildRequest(
      [
        {
          role: "user",
          content: [{ type: "text", text: "hi" }],
          timestamp: 0,
        },
      ],
      "gemini-2.5-flash",
      {},
    );
    expect(req.headers["content-type"]).toBe("application/json");
    // The harness swaps the sentinel for InferenceSource.apiKey at send time.
    expect(req.headers["x-goog-api-key"]).toBe(CREDENTIAL_SENTINEL);
  });
});

describe("Google GenAI adapter: body shape", () => {
  test("plain text → contents[user].parts[text] (matches plain-text fixture)", () => {
    const req = adapter.buildRequest(
      [
        {
          role: "user",
          content: [
            { type: "text", text: "Reply with the single word 'ready'." },
          ],
          timestamp: 0,
        },
      ],
      "gemini-2.5-flash",
      {},
    );
    const body = parseBody(req.body);
    const fixture = readFixtureJSON(
      "gemini-2.5-flash",
      "plain-text",
      "exchanges",
      "0",
      "request.json",
    );
    expect(body).toEqual(fixture);
  });

  test("system turn → systemInstruction.parts[].text; not in contents[]", () => {
    const req = adapter.buildRequest(
      [
        {
          role: "system",
          content: [{ type: "text", text: "You are concise." }],
          timestamp: 0,
        },
        {
          role: "user",
          content: [{ type: "text", text: "Hi" }],
          timestamp: 0,
        },
      ],
      "gemini-2.5-flash",
      {},
    );
    const body = parseBody(req.body);
    expect(SystemInstruction.assert(body.systemInstruction)).toEqual({
      parts: [{ text: "You are concise." }],
    });
    expect(GeminiContents.assert(body.contents)).toEqual([
      { role: "user", parts: [{ text: "Hi" }] },
    ]);
  });

  test("multiple system turns are concatenated with blank-line joiners", () => {
    const req = adapter.buildRequest(
      [
        {
          role: "system",
          content: [{ type: "text", text: "Rule 1." }],
          timestamp: 0,
        },
        {
          role: "system",
          content: [{ type: "text", text: "Rule 2." }],
          timestamp: 0,
        },
        {
          role: "user",
          content: [{ type: "text", text: "Hi" }],
          timestamp: 0,
        },
      ],
      "gemini-2.5-flash",
      {},
    );
    const body = parseBody(req.body);
    expect(SystemInstruction.assert(body.systemInstruction)).toEqual({
      parts: [{ text: "Rule 1.\n\nRule 2." }],
    });
  });

  test("options.systemPrompt overrides any system turn", () => {
    const req = adapter.buildRequest(
      [
        {
          role: "system",
          content: [{ type: "text", text: "Should be overridden." }],
          timestamp: 0,
        },
        {
          role: "user",
          content: [{ type: "text", text: "Hi" }],
          timestamp: 0,
        },
      ],
      "gemini-2.5-flash",
      { systemPrompt: "Wins." },
    );
    const body = parseBody(req.body);
    expect(SystemInstruction.assert(body.systemInstruction)).toEqual({
      parts: [{ text: "Wins." }],
    });
  });

  test("system turn containing a non-text block throws (matches loud-failure discipline)", () => {
    // Same loud-failure discipline as the other unsupported block kinds.
    const turns: ConversationTurn[] = [
      {
        role: "system",
        content: [
          {
            type: "image",
            source: {
              kind: "base64",
              mimeType: "image/png",
              data: "AAAA",
            },
          },
        ],
        timestamp: 0,
      },
      {
        role: "user",
        content: [{ type: "text", text: "hi" }],
        timestamp: 0,
      },
    ];
    expect(() => adapter.buildRequest(turns, "gemini-2.5-flash", {})).toThrow(
      /system turn must contain only text blocks/,
    );
  });

  test("system turn with only empty-text blocks emits no systemInstruction", () => {
    const req = adapter.buildRequest(
      [
        {
          role: "system",
          content: [{ type: "text", text: "" }],
          timestamp: 0,
        },
        {
          role: "user",
          content: [{ type: "text", text: "Hi" }],
          timestamp: 0,
        },
      ],
      "gemini-2.5-flash",
      {},
    );
    const body = parseBody(req.body);
    expect(body.systemInstruction).toBeUndefined();
  });
});

describe("Google GenAI adapter: tools and thinking", () => {
  test("tools mapped under single functionDeclarations wrapper", () => {
    const req = adapter.buildRequest(
      [
        {
          role: "user",
          content: [
            {
              type: "text",
              text: "Think carefully, then use the getCurrentWeather tool to look up the current weather in Boston, MA.",
            },
          ],
          timestamp: 0,
        },
      ],
      "gemini-2.5-flash",
      {
        thinking: { enabled: true, budgetTokens: 1024 },
        tools: [
          {
            name: "getCurrentWeather",
            description:
              "Get the current weather conditions for a given city. Use this whenever the user asks about weather.",
            inputSchema: {
              type: "object",
              properties: {
                location: {
                  type: "string",
                  description:
                    "The city and optional state, e.g. 'Boston, MA'.",
                },
              },
              required: ["location"],
            },
          },
        ],
      },
    );
    const body = parseBody(req.body);

    // Tools sit under a single functionDeclarations wrapper, the shape
    // Gemini uses to group declarations alongside built-in tools.
    expect(body.tools).toEqual([
      {
        functionDeclarations: [
          {
            name: "getCurrentWeather",
            description:
              "Get the current weather conditions for a given city. Use this whenever the user asks about weather.",
            parameters: {
              type: "object",
              properties: {
                location: {
                  type: "string",
                  description:
                    "The city and optional state, e.g. 'Boston, MA'.",
                },
              },
              required: ["location"],
            },
          },
        ],
      },
    ]);

    expect(body.generationConfig).toEqual({
      thinkingConfig: { thinkingBudget: 1024, includeThoughts: true },
    });
  });

  test("thinking.enabled=false emits thinkingBudget=0 on models that allow it", () => {
    // Flash's default thinking budget is non-zero, so disabling
    // thinking needs an explicit zero.
    const req = adapter.buildRequest(
      [
        {
          role: "user",
          content: [{ type: "text", text: "hi" }],
          timestamp: 0,
        },
      ],
      "gemini-2.5-flash",
      { thinking: { enabled: false } },
    );
    const body = parseBody(req.body);
    expect(body.generationConfig).toEqual({
      thinkingConfig: { thinkingBudget: 0 },
    });
  });

  test("thinking.enabled=false uses dynamic budget on thinking-mandatory models", () => {
    for (const model of ["gemini-2.5-pro", "gemini-3.6-flash"] as const) {
      const req = adapter.buildRequest(
        [
          {
            role: "user",
            content: [{ type: "text", text: "hi" }],
            timestamp: 0,
          },
        ],
        model,
        { thinking: { enabled: false } },
      );
      const body = parseBody(req.body);
      expect(body.generationConfig).toEqual({
        thinkingConfig: { thinkingBudget: -1 },
      });
    }
  });

  test("thinking omitted → no thinkingConfig (model default applies)", () => {
    const req = adapter.buildRequest(
      [
        {
          role: "user",
          content: [{ type: "text", text: "hi" }],
          timestamp: 0,
        },
      ],
      "gemini-2.5-flash",
      {},
    );
    const body = parseBody(req.body);
    expect(body.generationConfig).toBeUndefined();
  });

  test("maxTokens and temperature populate generationConfig", () => {
    const req = adapter.buildRequest(
      [
        {
          role: "user",
          content: [{ type: "text", text: "hi" }],
          timestamp: 0,
        },
      ],
      "gemini-2.5-flash",
      { maxTokens: 256, temperature: 0.2 },
    );
    const body = parseBody(req.body);
    expect(body.generationConfig).toEqual({
      maxOutputTokens: 256,
      temperature: 0.2,
    });
  });
});

describe("Google GenAI adapter: responseModalities translation", () => {
  const GenConfigWithModalities = type({
    "+": "delete",
    "responseModalities?": "string[]",
  });

  test("lowercase modalities → uppercase wire shape", () => {
    const req = adapter.buildRequest(
      [
        {
          role: "user",
          content: [{ type: "text", text: "draw a cat" }],
          timestamp: 0,
        },
      ],
      "gemini-2.5-flash-image",
      { responseModalities: ["text", "image"] },
    );
    const body = parseBody(req.body);
    expect(body.generationConfig).toEqual({
      responseModalities: ["TEXT", "IMAGE"],
    });
  });

  test("audio modality also uppercases", () => {
    const req = adapter.buildRequest(
      [
        {
          role: "user",
          content: [{ type: "text", text: "speak" }],
          timestamp: 0,
        },
      ],
      "gemini-2.5-flash",
      { responseModalities: ["audio"] },
    );
    const body = parseBody(req.body);
    const gc = GenConfigWithModalities.assert(body.generationConfig);
    expect(gc.responseModalities).toEqual(["AUDIO"]);
  });

  test("empty responseModalities array does not emit the field", () => {
    const req = adapter.buildRequest(
      [
        {
          role: "user",
          content: [{ type: "text", text: "hi" }],
          timestamp: 0,
        },
      ],
      "gemini-2.5-flash",
      { responseModalities: [] },
    );
    const body = parseBody(req.body);
    expect(body.generationConfig).toBeUndefined();
  });
});

describe("Google GenAI adapter: responseFormat translation", () => {
  const GenConfigWithResponseFormat = type({
    "+": "delete",
    "responseMimeType?": "string",
    "responseSchema?": "unknown",
  });

  const conversation = [
    {
      role: "user" as const,
      content: [{ type: "text" as const, text: "Extract user fields." }],
      timestamp: 1000,
    },
  ];

  test("kind=text omits both responseMimeType and responseSchema", () => {
    // Plain text is Gemini's default; setting responseMimeType would
    // pin the output, so nothing is emitted.
    const req = adapter.buildRequest(conversation, "gemini-2.5-flash", {
      responseFormat: { kind: "text" },
    });
    const body = parseBody(req.body);
    expect(body.generationConfig).toBeUndefined();
  });

  test("kind=json sets responseMimeType without responseSchema", () => {
    const req = adapter.buildRequest(conversation, "gemini-2.5-flash", {
      responseFormat: { kind: "json" },
    });
    const body = parseBody(req.body);
    const gc = GenConfigWithResponseFormat.assert(body.generationConfig);
    expect(gc.responseMimeType).toBe("application/json");
    expect(gc.responseSchema).toBeUndefined();
  });

  test("kind=json-schema sets responseMimeType and forwards the schema verbatim", () => {
    const schema = {
      type: "object",
      properties: { name: { type: "string" } },
      required: ["name"],
    };
    const req = adapter.buildRequest(conversation, "gemini-2.5-flash", {
      responseFormat: { kind: "json-schema", name: "user_info", schema },
    });
    const body = parseBody(req.body);
    const gc = GenConfigWithResponseFormat.assert(body.generationConfig);
    expect(gc.responseMimeType).toBe("application/json");
    expect(gc.responseSchema).toEqual(schema);
  });

  test("kind=json-schema ignores OpenAI-specific name and strict fields", () => {
    // Gemini has no name/strict toggle at the responseSchema level; the
    // adapter ignores them without erroring.
    const schema = { type: "object", properties: {} };
    const req = adapter.buildRequest(conversation, "gemini-2.5-flash", {
      responseFormat: {
        kind: "json-schema",
        name: "ignored_by_gemini",
        schema,
        strict: true,
      },
    });
    const body = parseBody(req.body);
    const gc = GenConfigWithResponseFormat.assert(body.generationConfig);
    expect(gc.responseMimeType).toBe("application/json");
    expect(gc.responseSchema).toEqual(schema);
    // No name or strict leak.
    expect(JSON.stringify(body.generationConfig)).not.toContain(
      "ignored_by_gemini",
    );
    expect(JSON.stringify(body.generationConfig)).not.toContain("strict");
  });
});

describe("Google GenAI adapter: MediaSource variants", () => {
  function firstTurnParts(body: typeof GeminiBody.infer): unknown[] {
    const contents = GeminiContents.assert(body.contents);
    const first = contents[0];
    if (first === undefined) {
      throw new Error("expected contents[0] to be defined");
    }
    return first.parts;
  }

  test("base64 image → inlineData part", () => {
    const req = adapter.buildRequest(
      [
        {
          role: "user",
          content: [
            { type: "text", text: "describe" },
            {
              type: "image",
              source: {
                kind: "base64",
                mimeType: "image/png",
                data: "iVBORw0KGgo=",
              },
            },
          ],
          timestamp: 0,
        },
      ],
      "gemini-2.5-flash",
      {},
    );
    expect(firstTurnParts(parseBody(req.body))).toEqual([
      { text: "describe" },
      { inlineData: { mimeType: "image/png", data: "iVBORw0KGgo=" } },
    ]);
  });

  test("file-reference document → fileData part", () => {
    const req = adapter.buildRequest(
      [
        {
          role: "user",
          content: [
            {
              type: "document",
              source: {
                kind: "file-reference",
                reference: "files/abc123",
                mimeType: "application/pdf",
              },
            },
          ],
          timestamp: 0,
        },
      ],
      "gemini-2.5-flash",
      {},
    );
    expect(firstTurnParts(parseBody(req.body))).toEqual([
      {
        fileData: { mimeType: "application/pdf", fileUri: "files/abc123" },
      },
    ]);
  });

  test("url image → fileData part with public URL as fileUri", () => {
    // Gemini accepts public HTTP(S) URLs in the same fileData field
    // the Files API uses for uploaded-file URIs.
    const req = adapter.buildRequest(
      [
        {
          role: "user",
          content: [
            {
              type: "image",
              source: {
                kind: "url",
                url: "https://example.com/photo.jpg",
                mimeType: "image/jpeg",
              },
            },
          ],
          timestamp: 0,
        },
      ],
      "gemini-2.5-flash",
      {},
    );
    expect(firstTurnParts(parseBody(req.body))).toEqual([
      {
        fileData: {
          mimeType: "image/jpeg",
          fileUri: "https://example.com/photo.jpg",
        },
      },
    ]);
  });
});

// ---------------------------------------------------------------------------
// Multimodal-input round-trip parity against captured fixtures
// ---------------------------------------------------------------------------
//
// Byte-for-byte body parity: a regression in part ordering, optional-key
// emission, or field naming surfaces as a fixture mismatch. Fixture base64
// is read from disk and threaded back into the input turn so the data
// round-trips through the adapter unchanged.

describe("Google GenAI adapter: multimodal input fixture parity", () => {
  // Fixtures are external data, so arktype (not type assertions) is
  // used to pull `{mimeType, data}` and `{mimeType, fileUri}` out.
  const FixtureInlineDataPart = type({
    inlineData: { mimeType: "string", data: "string" },
  });
  const FixtureFileDataPart = type({
    fileData: { mimeType: "string", fileUri: "string" },
  });

  // Second part of a fixture's first user turn (text at parts[0], media
  // at parts[1]); bounds throw loudly instead of masking via `?.`.
  function mediaPartOf(fixture: typeof GeminiBody.infer): unknown {
    const contents = GeminiContents.assert(fixture.contents);
    const firstTurn = contents[0];
    if (firstTurn === undefined) {
      throw new Error("fixture contents[] is empty");
    }
    // Fixtures are single-turn user prompts; anything else fails loudly
    // rather than yielding a confusing byte-diff downstream.
    if (firstTurn.role !== "user") {
      throw new Error(
        `fixture first turn role is ${JSON.stringify(firstTurn.role)}; expected "user"`,
      );
    }
    if (firstTurn.parts.length < 2) {
      throw new Error(
        `fixture first turn has ${String(firstTurn.parts.length)} part(s); expected at least 2`,
      );
    }
    return firstTurn.parts[1];
  }

  // Read a fixture once: returns the parsed body plus the destructured
  // inlineData payload for threading back into the input turn.
  function loadInlineDataFixture(...path: string[]): {
    fixture: typeof GeminiBody.infer;
    mimeType: string;
    data: string;
  } {
    const fixture = readFixtureJSON(...path);
    const { mimeType, data } = FixtureInlineDataPart.assert(
      mediaPartOf(fixture),
    ).inlineData;
    return { fixture, mimeType, data };
  }

  test("vision: base64 image round-trips byte-for-byte against fixture", () => {
    const { fixture, mimeType, data } = loadInlineDataFixture(
      "gemini-2.5-flash",
      "vision-input-streaming",
      "exchanges",
      "0",
      "request.json",
    );
    const req = adapter.buildRequest(
      [
        {
          role: "user",
          content: [
            {
              type: "text",
              text: "Describe the picture in one short sentence.",
            },
            {
              type: "image",
              source: { kind: "base64", mimeType, data },
            },
          ],
          timestamp: 0,
        },
      ],
      "gemini-2.5-flash",
      {},
    );
    expect(parseBody(req.body)).toEqual(fixture);
  });

  test("audio: base64 audio round-trips byte-for-byte against fixture", () => {
    const { fixture, mimeType, data } = loadInlineDataFixture(
      "gemini-2.5-flash",
      "audio-input-streaming",
      "exchanges",
      "0",
      "request.json",
    );
    const req = adapter.buildRequest(
      [
        {
          role: "user",
          content: [
            {
              type: "text",
              text: "Transcribe the spoken words in this audio clip.",
            },
            {
              type: "audio",
              source: { kind: "base64", mimeType, data },
            },
          ],
          timestamp: 0,
        },
      ],
      "gemini-2.5-flash",
      {},
    );
    expect(parseBody(req.body)).toEqual(fixture);
  });

  test("video: base64 video round-trips byte-for-byte against fixture", () => {
    const { fixture, mimeType, data } = loadInlineDataFixture(
      "gemini-2.5-flash",
      "video-input-streaming",
      "exchanges",
      "0",
      "request.json",
    );
    const req = adapter.buildRequest(
      [
        {
          role: "user",
          content: [
            {
              type: "text",
              text: "Describe what happens in this video in one short sentence.",
            },
            {
              type: "video",
              source: { kind: "base64", mimeType, data },
            },
          ],
          timestamp: 0,
        },
      ],
      "gemini-2.5-flash",
      {},
    );
    expect(parseBody(req.body)).toEqual(fixture);
  });

  test("document: base64 PDF round-trips byte-for-byte against fixture", () => {
    const { fixture, mimeType, data } = loadInlineDataFixture(
      "gemini-2.5-flash",
      "document-input-streaming",
      "exchanges",
      "0",
      "request.json",
    );
    const req = adapter.buildRequest(
      [
        {
          role: "user",
          content: [
            { type: "text", text: "Summarize this PDF in one short sentence." },
            {
              type: "document",
              source: { kind: "base64", mimeType, data },
            },
          ],
          timestamp: 0,
        },
      ],
      "gemini-2.5-flash",
      {},
    );
    expect(parseBody(req.body)).toEqual(fixture);
  });

  test("files-api: file-reference document round-trips byte-for-byte against fixture", () => {
    // The URI is the Files API upload handle, not a public URL; both
    // `file-reference` and `url` MediaSources land in `fileData`.
    const fixture = readFixtureJSON(
      "gemini-2.5-flash",
      "files-api-reference-streaming",
      "exchanges",
      "1",
      "request.json",
    );
    const { mimeType, fileUri } = FixtureFileDataPart.assert(
      mediaPartOf(fixture),
    ).fileData;
    const req = adapter.buildRequest(
      [
        {
          role: "user",
          content: [
            {
              type: "text",
              text: "Summarize the attached document in one sentence.",
            },
            {
              type: "document",
              source: { kind: "file-reference", reference: fileUri, mimeType },
            },
          ],
          timestamp: 0,
        },
      ],
      "gemini-2.5-flash",
      {},
    );
    expect(parseBody(req.body)).toEqual(fixture);
  });
});

describe("Google GenAI adapter: conversation-turn mapping", () => {
  function lastTurnParts(body: typeof GeminiBody.infer): unknown[] {
    const contents = GeminiContents.assert(body.contents);
    const last = contents[contents.length - 1];
    if (last === undefined) {
      throw new Error("expected at least one content");
    }
    return last.parts;
  }

  test("assistant role becomes 'model'", () => {
    const req = adapter.buildRequest(
      [
        {
          role: "user",
          content: [{ type: "text", text: "Q" }],
          timestamp: 0,
        },
        {
          role: "assistant",
          content: [{ type: "text", text: "A" }],
          timestamp: 0,
        },
      ],
      "gemini-2.5-flash",
      {},
    );
    const body = parseBody(req.body);
    expect(GeminiContents.assert(body.contents)).toEqual([
      { role: "user", parts: [{ text: "Q" }] },
      { role: "model", parts: [{ text: "A" }] },
    ]);
  });

  test("tool_call/tool_result round-trip matches function-calling-multi-turn fixture", () => {
    // Three-turn conversation as the harness assembles it; the
    // functionResponse.name comes from the callId -> name lookup.
    const turns: ConversationTurn[] = [
      {
        role: "user",
        content: [
          {
            type: "text",
            text: "What is the current weather in Boston, MA? Use the getCurrentWeather tool.",
          },
        ],
        timestamp: 0,
      },
      {
        role: "assistant",
        content: [
          {
            type: "tool_call",
            id: "call_abc",
            name: "getCurrentWeather",
            arguments: { location: "Boston, MA" },
          },
        ],
        timestamp: 0,
      },
      {
        role: "user",
        content: [
          {
            type: "tool_result",
            callId: "call_abc",
            content: [
              {
                type: "text",
                text: JSON.stringify({
                  location: "Boston, MA",
                  temperatureF: 62,
                  conditions: "partly cloudy",
                  windMph: 8,
                }),
              },
            ],
          },
        ],
        timestamp: 0,
      },
    ];

    const req = adapter.buildRequest(turns, "gemini-2.5-flash", {
      thinking: { enabled: false },
      tools: [
        {
          name: "getCurrentWeather",
          description:
            "Get the current weather conditions for a given city. Use this whenever the user asks about weather.",
          inputSchema: {
            type: "object",
            properties: {
              location: {
                type: "string",
                description: "The city and optional state, e.g. 'Boston, MA'.",
              },
            },
            required: ["location"],
          },
        },
      ],
    });
    const body = parseBody(req.body);

    const fixture = readFixtureJSON(
      "gemini-2.5-flash",
      "function-calling-multi-turn-streaming",
      "exchanges",
      "1",
      "request.json",
    );
    expect(body).toEqual(fixture);
  });

  test("tool_result with single non-JSON text → wrapped under `result`", () => {
    const turns: ConversationTurn[] = [
      {
        role: "assistant",
        content: [
          { type: "tool_call", id: "call_x", name: "echo", arguments: {} },
        ],
        timestamp: 0,
      },
      {
        role: "user",
        content: [
          {
            type: "tool_result",
            callId: "call_x",
            content: [{ type: "text", text: "plain string" }],
          },
        ],
        timestamp: 0,
      },
    ];
    const req = adapter.buildRequest(turns, "gemini-2.5-flash", {});
    expect(lastTurnParts(parseBody(req.body))).toEqual([
      {
        functionResponse: {
          name: "echo",
          response: { result: "plain string" },
        },
      },
    ]);
  });

  test("tool_result with isError=true and non-JSON text → wrapped under `error`", () => {
    const turns: ConversationTurn[] = [
      {
        role: "assistant",
        content: [
          { type: "tool_call", id: "call_x", name: "echo", arguments: {} },
        ],
        timestamp: 0,
      },
      {
        role: "user",
        content: [
          {
            type: "tool_result",
            callId: "call_x",
            isError: true,
            content: [{ type: "text", text: "tool blew up" }],
          },
        ],
        timestamp: 0,
      },
    ];
    const req = adapter.buildRequest(turns, "gemini-2.5-flash", {});
    expect(lastTurnParts(parseBody(req.body))).toEqual([
      {
        functionResponse: {
          name: "echo",
          response: { error: "tool blew up" },
        },
      },
    ]);
  });

  test("tool_result with JSON-array text → wrapped under `result` (not promoted to response)", () => {
    // Only JSON objects ride verbatim; arrays/scalars/null take the
    // wrap path so the wire shape stays predictable.
    const turns: ConversationTurn[] = [
      {
        role: "assistant",
        content: [
          { type: "tool_call", id: "call_x", name: "echo", arguments: {} },
        ],
        timestamp: 0,
      },
      {
        role: "user",
        content: [
          {
            type: "tool_result",
            callId: "call_x",
            content: [{ type: "text", text: "[1,2,3]" }],
          },
        ],
        timestamp: 0,
      },
    ];
    const req = adapter.buildRequest(turns, "gemini-2.5-flash", {});
    expect(lastTurnParts(parseBody(req.body))).toEqual([
      {
        functionResponse: {
          name: "echo",
          response: { result: "[1,2,3]" },
        },
      },
    ]);
  });

  test("tool_result with unknown callId throws with diagnostic context", () => {
    const turns: ConversationTurn[] = [
      {
        role: "assistant",
        content: [
          {
            type: "tool_call",
            id: "call_known",
            name: "doThing",
            arguments: {},
          },
        ],
        timestamp: 0,
      },
      {
        role: "user",
        content: [
          {
            type: "tool_result",
            callId: "call_unknown",
            content: [{ type: "text", text: "{}" }],
          },
        ],
        timestamp: 0,
      },
    ];
    expect(() => adapter.buildRequest(turns, "gemini-2.5-flash", {})).toThrow(
      /call_unknown.*call_known|call_known.*call_unknown/,
    );
  });

  test("tool_result with multiple text blocks throws", () => {
    const turns: ConversationTurn[] = [
      {
        role: "assistant",
        content: [
          { type: "tool_call", id: "call_x", name: "echo", arguments: {} },
        ],
        timestamp: 0,
      },
      {
        role: "user",
        content: [
          {
            type: "tool_result",
            callId: "call_x",
            content: [
              { type: "text", text: "part 1" },
              { type: "text", text: "part 2" },
            ],
          },
        ],
        timestamp: 0,
      },
    ];
    expect(() => adapter.buildRequest(turns, "gemini-2.5-flash", {})).toThrow(
      /exactly one text block/,
    );
  });

  test("tool_call block on a user turn throws (role/block-pair mismatch)", () => {
    // The marshaling boundary catches a tool_call on a user turn with
    // a diagnostic rather than an opaque Gemini 400.
    const turns: ConversationTurn[] = [
      {
        role: "user",
        content: [
          {
            type: "tool_call",
            id: "call_bad",
            name: "echo",
            arguments: {},
          },
        ],
        timestamp: 0,
      },
    ];
    expect(() => adapter.buildRequest(turns, "gemini-2.5-flash", {})).toThrow(
      /tool_call blocks must appear on assistant turns.*call_bad/,
    );
  });

  test("tool_result block on an assistant turn throws", () => {
    // Symmetric: tool_result on an assistant turn is a caller bug.
    const turns: ConversationTurn[] = [
      {
        role: "assistant",
        content: [
          {
            type: "tool_result",
            callId: "call_bad",
            content: [{ type: "text", text: "{}" }],
          },
        ],
        timestamp: 0,
      },
    ];
    expect(() => adapter.buildRequest(turns, "gemini-2.5-flash", {})).toThrow(
      /tool_result blocks must appear on user turns.*call_bad/,
    );
  });

  test("tool_result with a non-text content block throws", () => {
    const turns: ConversationTurn[] = [
      {
        role: "assistant",
        content: [
          { type: "tool_call", id: "call_x", name: "echo", arguments: {} },
        ],
        timestamp: 0,
      },
      {
        role: "user",
        content: [
          {
            type: "tool_result",
            callId: "call_x",
            content: [
              {
                type: "image",
                source: {
                  kind: "base64",
                  mimeType: "image/png",
                  data: "AAAA",
                },
              },
            ],
          },
        ],
        timestamp: 0,
      },
    ];
    expect(() => adapter.buildRequest(turns, "gemini-2.5-flash", {})).toThrow(
      /must be of type "text"/,
    );
  });
});

describe("Google GenAI adapter: providerOptions escape hatch", () => {
  test("providerOptions with an explicit undefined drops the adapter-built field", () => {
    // Object.assign writes undefined through and JSON.stringify drops
    // it, so `providerOptions: { generationConfig: undefined }` erases
    // the adapter-built config. Pin the standard spread semantic so a
    // future "fix" that filters undefined breaks loudly.
    const req = adapter.buildRequest(
      [
        {
          role: "user",
          content: [{ type: "text", text: "hi" }],
          timestamp: 0,
        },
      ],
      "gemini-2.5-flash",
      {
        thinking: { enabled: true, budgetTokens: 1024 },
        providerOptions: { generationConfig: undefined },
      },
    );
    const body = parseBody(req.body);
    expect(body.generationConfig).toBeUndefined();
  });

  test("providerOptions shallow-merges over body top-level (and clobbers generationConfig)", () => {
    // providerOptions shallow-merges into the body top level; a
    // structured key like `generationConfig` clobbers the adapter-built
    // object. Pinned so a "helpful" deep-merge refactor breaks loudly.
    const req = adapter.buildRequest(
      [
        {
          role: "user",
          content: [{ type: "text", text: "hi" }],
          timestamp: 0,
        },
      ],
      "gemini-2.5-flash",
      {
        thinking: { enabled: true, budgetTokens: 1024 },
        providerOptions: {
          generationConfig: { temperature: 0.7 },
          safetySettings: [
            { category: "HARM_CATEGORY_HARASSMENT", threshold: "BLOCK_NONE" },
          ],
        },
      },
    );
    const body = parseBody(req.body);

    // The provided generationConfig fully replaces the adapter-built one.
    expect(body.generationConfig).toEqual({ temperature: 0.7 });
    expect(body.safetySettings).toEqual([
      { category: "HARM_CATEGORY_HARASSMENT", threshold: "BLOCK_NONE" },
    ]);
  });
});

describe("Google GenAI adapter: unsupported blocks", () => {
  test("redacted_thinking throws (Anthropic-specific)", () => {
    const turns: ConversationTurn[] = [
      {
        role: "assistant",
        content: [{ type: "redacted_thinking", data: "opaque" }],
        timestamp: 0,
      },
    ];
    expect(() => adapter.buildRequest(turns, "gemini-2.5-flash", {})).toThrow(
      /redacted_thinking/,
    );
  });

  test("citation in incoming turn throws", () => {
    const turns: ConversationTurn[] = [
      {
        role: "assistant",
        content: [
          {
            type: "citation",
            citedText: "cited",
            source: { uri: "https://example.com" },
          },
        ],
        timestamp: 0,
      },
    ];
    expect(() => adapter.buildRequest(turns, "gemini-2.5-flash", {})).toThrow(
      /citation/,
    );
  });

  test("code_execution_request throws", () => {
    const turns: ConversationTurn[] = [
      {
        role: "assistant",
        content: [
          {
            type: "code_execution_request",
            id: "exec1",
            code: "print(1)",
          },
        ],
        timestamp: 0,
      },
    ];
    expect(() => adapter.buildRequest(turns, "gemini-2.5-flash", {})).toThrow(
      /code_execution_request/,
    );
  });
});

// ---------------------------------------------------------------------------
// parseResponse -- plain-text streaming
// ---------------------------------------------------------------------------

// Drives a sequence of SSE-framed Uint8Array chunks through the
// production SSE parser and the supplied adapter's parseResponse,
// mirroring the harness's pipeline. Returns the flattened sequence
// of emitted events so the test site can assert on them.
async function parseWire(
  adapterInstance: ProviderAdapter,
  chunks: Uint8Array[],
): Promise<InferenceEvent[]> {
  const stream = new ReadableStream<Uint8Array>({
    start(controller) {
      for (const chunk of chunks) controller.enqueue(chunk);
      controller.close();
    },
  });
  const events: InferenceEvent[] = [];
  for await (const payload of parseSSE(stream)) {
    events.push(...adapterInstance.parseResponse(payload));
  }
  return events;
}

// Frames one JSON object as one SSE event (one `data:` line + blank
// line), matching the Gemini endpoint's framing.
function sseFrame(obj: unknown): Uint8Array {
  return new TextEncoder().encode(`data: ${JSON.stringify(obj)}\n\n`);
}

describe("Google GenAI adapter: parseResponse plain text", () => {
  test("text part emits inference.text.delta with the token and index 0", async () => {
    const events = await parseWire(adapter, [
      sseFrame({
        candidates: [
          {
            content: { role: "model", parts: [{ text: "Hello, world." }] },
            index: 0,
          },
        ],
      }),
    ]);
    expect(events).toEqual([
      {
        type: "inference.text.delta",
        seq: 0,
        data: {
          token: "Hello, world.",
          partial: { text: "" },
          index: 0,
        },
      },
    ]);
  });

  test("multiple text parts in one event emit multiple text deltas in order", async () => {
    // Two text parts produce two text.delta events in order, both at
    // index 0 (one logical plain-text block).
    const events = await parseWire(adapter, [
      sseFrame({
        candidates: [
          {
            content: {
              role: "model",
              parts: [{ text: "first " }, { text: "second" }],
            },
            index: 0,
          },
        ],
      }),
    ]);
    const tokens = events
      .filter((e) => e.type === "inference.text.delta")
      .map((e) => (e.type === "inference.text.delta" ? e.data.token : ""));
    expect(tokens).toEqual(["first ", "second"]);
  });

  test("empty text parts are dropped (no zero-token delta)", async () => {
    const events = await parseWire(adapter, [
      sseFrame({
        candidates: [
          {
            content: { role: "model", parts: [{ text: "" }, { text: "x" }] },
            index: 0,
          },
        ],
      }),
    ]);
    expect(
      events.filter((e) => e.type === "inference.text.delta"),
    ).toHaveLength(1);
  });

  test("finishReason event emits usage with mapped TokenUsage", async () => {
    const events = await parseWire(adapter, [
      sseFrame({
        candidates: [
          {
            content: { role: "model", parts: [{ text: "done." }] },
            finishReason: "STOP",
            index: 0,
          },
        ],
        usageMetadata: {
          promptTokenCount: 10,
          candidatesTokenCount: 3,
          totalTokenCount: 13,
        },
      }),
    ]);
    const usage = events.find((e) => e.type === "inference.usage");
    expect(usage).toBeDefined();
    if (usage?.type !== "inference.usage") {
      throw new Error("expected an inference.usage event");
    }
    expect(usage.data.usage).toEqual({
      input: 10,
      output: 3,
      cacheRead: 0,
      cacheWrite: 0,
      thinking: 0,
    });
  });

  test("non-terminal events emit no usage (cadence is finishReason-gated)", async () => {
    // Usage emits only at the terminal event; intermediate emissions
    // would be noise.
    const events = await parseWire(adapter, [
      sseFrame({
        candidates: [
          {
            content: { role: "model", parts: [{ text: "partial" }] },
            index: 0,
          },
        ],
        usageMetadata: {
          promptTokenCount: 10,
          candidatesTokenCount: 1,
          totalTokenCount: 11,
        },
      }),
    ]);
    expect(events.filter((e) => e.type === "inference.usage")).toHaveLength(0);
  });

  test("candidates-less event with usageMetadata emits nothing (usage gated on finishReason)", async () => {
    const events = await parseWire(adapter, [
      sseFrame({
        usageMetadata: {
          promptTokenCount: 10,
          candidatesTokenCount: 0,
          totalTokenCount: 10,
        },
      }),
    ]);
    expect(events).toHaveLength(0);
  });

  test("plain-text-streaming fixture replay yields exactly 8 text deltas + 1 usage", async () => {
    // 8 SSE events -> 8 text.delta events (the new tokens, in order) plus
    // one inference.usage with the final event's cumulative counts.
    const sseBytes = readFileSync(
      join(
        FIXTURE_ROOT,
        "gemini-2.5-flash",
        "plain-text-streaming",
        "exchanges",
        "0",
        "response.sse",
      ),
    );
    const events = await parseWire(adapter, [sseBytes]);

    const textEvents = events.filter((e) => e.type === "inference.text.delta");
    const usageEvents = events.filter((e) => e.type === "inference.usage");
    expect(textEvents).toHaveLength(8);
    expect(usageEvents).toHaveLength(1);
    expect(events.length).toBe(9);
    // Usage is the last emission (usage-before-done contract).
    expect(events[events.length - 1]?.type).toBe("inference.usage");

    // Final cumulative usage from the fixture: prompt=33, candidates=281,
    // total=314.
    const usage = usageEvents[0];
    if (usage?.type !== "inference.usage") {
      throw new Error("expected inference.usage");
    }
    expect(usage.data.usage).toEqual({
      input: 33,
      output: 281,
      cacheRead: 0,
      cacheWrite: 0,
      thinking: 0,
    });

    // Every text delta carries index 0 (single logical block for
    // plain text) and a non-empty token.
    for (const ev of textEvents) {
      if (ev.type !== "inference.text.delta") continue;
      expect(ev.data.index).toBe(0);
      expect(ev.data.token.length).toBeGreaterThan(0);
    }
  });
});

describe("Google GenAI adapter: parseResponse safety_rating", () => {
  test("safety-classification-streaming fixture emits safety_rating + usage", async () => {
    // Captured 2026-07-28: promptFeedback.blockReason PROHIBITED_CONTENT,
    // zero candidates, usageMetadata with prompt tokens only.
    const sseBytes = readFileSync(
      join(
        FIXTURE_ROOT,
        "gemini-2.5-flash",
        "safety-classification-streaming",
        "exchanges",
        "0",
        "response.sse",
      ),
    );
    const events = await parseWire(adapter, [sseBytes]);
    const safety = events.filter((e) => e.type === "inference.safety_rating");
    expect(safety).toHaveLength(1);
    if (safety[0]?.type === "inference.safety_rating") {
      expect(safety[0].data.safetyRating).toEqual({
        type: "safety_rating",
        blockReason: "PROHIBITED_CONTENT",
      });
    }
    const usage = events.filter((e) => e.type === "inference.usage");
    expect(usage).toHaveLength(1);
    if (usage[0]?.type === "inference.usage") {
      expect(usage[0].data.usage.input).toBe(18);
      expect(usage[0].data.usage.output).toBe(0);
    }
  });

  test("promptFeedback.blockReason without usageMetadata throws", () => {
    const bad = JSON.stringify({
      promptFeedback: { blockReason: "PROHIBITED_CONTENT" },
    });
    expect(() => adapter.parseResponse(bad)).toThrow(ProtocolMismatchError);
    expect(() => adapter.parseResponse(bad)).toThrow(/missing usageMetadata/);
  });

  test("safety_rating-only prior turns rewrite to text on follow-up request history", () => {
    // A prompt-blocked turn finalizes with only SafetyRatingBlock(s).
    // Rewrite to text keeps role alternation and a model-visible reason.
    const history: ConversationTurn[] = [
      {
        role: "user",
        content: [{ type: "text", text: "blocked prompt" }],
        timestamp: 1,
      },
      {
        role: "assistant",
        content: [{ type: "safety_rating", blockReason: "PROHIBITED_CONTENT" }],
        model: "gemini-2.5-flash",
        timestamp: 2,
      },
      {
        role: "user",
        content: [{ type: "text", text: "try again safely" }],
        timestamp: 3,
      },
    ];
    const req = adapter.buildRequest(history, "gemini-2.5-flash", {
      maxTokens: 100,
    });
    const ContentsShape = type({
      contents: type({
        role: "string",
        parts: type({ "text?": "string" }).array(),
      }).array(),
    });
    const body = ContentsShape.assert(JSON.parse(req.body));
    expect(body.contents).toHaveLength(3);
    expect(body.contents.map((c) => c.role)).toEqual(["user", "model", "user"]);
    expect(body.contents[1]?.parts[0]?.text).toBe(
      "Request blocked: PROHIBITED_CONTENT",
    );
  });

  test("safety-classification-streaming harness lands safety_rating on inference.done", async () => {
    const sseBytes = readFileSync(
      join(
        FIXTURE_ROOT,
        "gemini-2.5-flash",
        "safety-classification-streaming",
        "exchanges",
        "0",
        "response.sse",
      ),
    );
    const SOURCE: InferenceSource = {
      id: "google-genai:gemini-2.5-flash",
      provider: "google-genai",
      baseURL: "https://generativelanguage.googleapis.com",
      credentialId: "test-key",
      model: "gemini-2.5-flash",
    };
    const inertScheduler: Scheduler = {
      setTimeout: () => () => {
        /* no timers */
      },
      now: () => 0,
    };
    let seq = 0;
    const events: InferenceEvent[] = [];
    for await (const ev of runInference({
      readMaterial: () => ({ secret: "test-secret" }),
      turns: [
        {
          role: "user",
          content: [{ type: "text", text: "probe" }],
          timestamp: 0,
        },
      ],
      source: SOURCE,
      nextSeq: () => seq++,
      deps: {
        fetch: () =>
          Promise.resolve(
            new Response(
              new ReadableStream({
                start(controller) {
                  controller.enqueue(sseBytes);
                  controller.close();
                },
              }),
              {
                status: 200,
                headers: { "content-type": "text/event-stream" },
              },
            ),
          ),
        scheduler: inertScheduler,
        adapters: createBuiltinRegistry(),
      },
    })) {
      events.push(ev);
    }
    const done = events.find((e) => e.type === "inference.done");
    expect(done?.type).toBe("inference.done");
    if (done?.type === "inference.done") {
      expect(done.data.turn.content).toEqual([
        { type: "safety_rating", blockReason: "PROHIBITED_CONTENT" },
      ]);
    }
  });
});

describe("Google GenAI adapter: parseResponse error surface", () => {
  test("malformed JSON in SSE payload throws ProtocolMismatchError", () => {
    expect(() => adapter.parseResponse("{not json}")).toThrow(
      ProtocolMismatchError,
    );
    expect(() => adapter.parseResponse("{not json}")).toThrow(/malformed JSON/);
  });

  test("schema mismatch (usageMetadata as string) throws ProtocolMismatchError", () => {
    const bad = JSON.stringify({
      candidates: [
        { content: { role: "model", parts: [{ text: "x" }] }, index: 0 },
      ],
      usageMetadata: "not-an-object",
    });
    expect(() => adapter.parseResponse(bad)).toThrow(ProtocolMismatchError);
    expect(() => adapter.parseResponse(bad)).toThrow(/schema validation/);
  });

  test("candidates.length > 1 throws ProtocolMismatchError (adapter never requests n>1)", () => {
    const bad = JSON.stringify({
      candidates: [
        { content: { role: "model", parts: [{ text: "a" }] }, index: 0 },
        { content: { role: "model", parts: [{ text: "b" }] }, index: 1 },
      ],
    });
    expect(() => adapter.parseResponse(bad)).toThrow(ProtocolMismatchError);
    expect(() => adapter.parseResponse(bad)).toThrow(/at most one candidate/);
  });

  test("terminal event missing usageMetadata throws ProtocolMismatchError", () => {
    // A terminal event missing usageMetadata would silently zero the
    // tally; fail loudly instead.
    const bad = JSON.stringify({
      candidates: [
        {
          content: { role: "model", parts: [{ text: "x" }] },
          finishReason: "STOP",
          index: 0,
        },
      ],
    });
    expect(() => adapter.parseResponse(bad)).toThrow(ProtocolMismatchError);
    expect(() => adapter.parseResponse(bad)).toThrow(/missing usageMetadata/);
  });
});

// ---------------------------------------------------------------------------
// Harness-level round trip
// ---------------------------------------------------------------------------

describe("Google GenAI adapter: harness round trip", () => {
  const inertScheduler: Scheduler = {
    setTimeout: () => () => {
      /* tests do not exercise timer firing */
    },
    now: () => 0,
  };

  const SOURCE: InferenceSource = {
    id: "google-genai:gemini-2.5-flash",
    provider: "google-genai",
    baseURL: "https://generativelanguage.googleapis.com",
    credentialId: "test-key",
    model: "gemini-2.5-flash",
  };

  test("plain-text-streaming fixture flows through runInference end-to-end", async () => {
    // Replays the captured SSE response through the full harness pipeline
    // and asserts the accumulated PartialMessage text, the final
    // inference.done turn, and the usage match the wire capture.
    const sseBytes = readFileSync(
      join(
        FIXTURE_ROOT,
        "gemini-2.5-flash",
        "plain-text-streaming",
        "exchanges",
        "0",
        "response.sse",
      ),
    );

    const fetchImpl: Dependencies["fetch"] = () =>
      Promise.resolve(
        new Response(
          new ReadableStream({
            start(controller) {
              controller.enqueue(sseBytes);
              controller.close();
            },
          }),
          {
            status: 200,
            headers: { "content-type": "text/event-stream" },
          },
        ),
      );

    let seq = 0;
    const events: InferenceEvent[] = [];
    for await (const ev of runInference({
      readMaterial: () => ({ secret: "test-secret" }),
      turns: [
        {
          role: "user",
          content: [{ type: "text", text: "Tell me about sailboats." }],
          timestamp: 0,
        },
      ],
      source: SOURCE,
      nextSeq: () => seq++,
      deps: {
        fetch: fetchImpl,
        scheduler: inertScheduler,
        adapters: createBuiltinRegistry(),
      },
    })) {
      events.push(ev);
    }

    const done = events.find((e) => e.type === "inference.done");
    expect(done).toBeDefined();
    if (done?.type !== "inference.done") {
      throw new Error("expected inference.done event");
    }

    // Single text block with the full concatenated response; the prefix
    // comes from the fixture's first event so a dropped chunk fails.
    expect(done.data.turn.content).toHaveLength(1);
    const first = done.data.turn.content[0];
    if (first?.type !== "text") {
      throw new Error("expected first content block to be text");
    }
    expect(first.text.startsWith("A sailboat harnesses the wind")).toBe(true);
    expect(first.text.endsWith("powered solely by the wind.")).toBe(true);

    // Usage = the final cumulative snapshot from the last SSE event.
    expect(done.data.usage).toEqual({
      input: 33,
      output: 281,
      cacheRead: 0,
      cacheWrite: 0,
      thinking: 0,
    });

    // The harness emits inference.usage before inference.done.
    const usageIdx = events.findIndex((e) => e.type === "inference.usage");
    const doneIdx = events.findIndex((e) => e.type === "inference.done");
    expect(usageIdx).toBeGreaterThan(-1);
    expect(usageIdx).toBeLessThan(doneIdx);
  });
});

// ---------------------------------------------------------------------------
// parseResponse -- function-calling and thought-signature paths
// ---------------------------------------------------------------------------

describe("Google GenAI adapter: parseResponse function-calling", () => {
  test("single functionCall part emits tool_call.start + tool_call.delta at index 0", async () => {
    // turn-1 wire shape: one event, one functionCall part. The parser
    // synthesizes a callId (no wire-level id) and emits args atomically.
    const events = await parseWire(adapter, [
      sseFrame({
        candidates: [
          {
            content: {
              role: "model",
              parts: [
                {
                  functionCall: {
                    name: "getCurrentWeather",
                    args: { location: "Boston, MA" },
                  },
                },
              ],
            },
            finishReason: "STOP",
            index: 0,
          },
        ],
        usageMetadata: {
          promptTokenCount: 81,
          candidatesTokenCount: 16,
          totalTokenCount: 97,
        },
      }),
    ]);

    expect(events.map((e) => e.type)).toEqual([
      "inference.tool_call.start",
      "inference.tool_call.delta",
      "inference.usage",
    ]);

    const start = events[0];
    if (start?.type !== "inference.tool_call.start") {
      throw new Error("expected inference.tool_call.start");
    }
    expect(start.data.name).toBe("getCurrentWeather");
    expect(start.data.index).toBe(0);
    expect(start.data.callId).toBe("0");

    const delta = events[1];
    if (delta?.type !== "inference.tool_call.delta") {
      throw new Error("expected inference.tool_call.delta");
    }
    expect(delta.data.callId).toBe("0");
    expect(delta.data.index).toBe(0);
    expect(JSON.parse(delta.data.argumentFragment)).toEqual({
      location: "Boston, MA",
    });
  });

  test("thinking text part emits inference.thinking.delta at the thinking block index", async () => {
    const events = await parseWire(adapter, [
      sseFrame({
        candidates: [
          {
            content: {
              role: "model",
              parts: [{ text: "step-by-step reasoning", thought: true }],
            },
            index: 0,
          },
        ],
      }),
    ]);

    expect(events).toEqual([
      {
        type: "inference.thinking.delta",
        seq: 0,
        data: {
          token: "step-by-step reasoning",
          partial: { text: "" },
          index: 0,
        },
      },
    ]);
  });

  test("thinking text followed by functionCall-with-signature attaches the signature to the tool_call block", async () => {
    // Mirrors turn-1: thinking text in one event, a signed functionCall
    // in the next. The signature attaches to the tool_call block (the
    // part that carries it), emitted after the tool_call.start/delta pair.
    const events = await parseWire(adapter, [
      sseFrame({
        candidates: [
          {
            content: {
              role: "model",
              parts: [{ text: "Determining weather query.", thought: true }],
            },
            index: 0,
          },
        ],
      }),
      sseFrame({
        candidates: [
          {
            content: {
              role: "model",
              parts: [
                {
                  functionCall: {
                    name: "getCurrentWeather",
                    args: { location: "Boston, MA" },
                  },
                  thoughtSignature: "OPAQUE_SIGNATURE",
                },
              ],
            },
            finishReason: "STOP",
            index: 0,
          },
        ],
        usageMetadata: {
          promptTokenCount: 85,
          candidatesTokenCount: 15,
          totalTokenCount: 153,
          thoughtsTokenCount: 53,
        },
      }),
    ]);

    expect(events.map((e) => e.type)).toEqual([
      "inference.thinking.delta",
      "inference.tool_call.start",
      "inference.tool_call.delta",
      "inference.block.signature",
      "inference.usage",
    ]);

    const thinkingDelta = events[0];
    if (thinkingDelta?.type !== "inference.thinking.delta") {
      throw new Error("expected inference.thinking.delta");
    }
    expect(thinkingDelta.data.index).toBe(0);

    const toolStart = events[1];
    if (toolStart?.type !== "inference.tool_call.start") {
      throw new Error("expected inference.tool_call.start");
    }
    // Thinking is block 0; the tool_call must allocate the next
    // index (1), not collide with the thinking block.
    expect(toolStart.data.index).toBe(1);
    expect(toolStart.data.callId).toBe("1");

    const signature = events[3];
    if (signature?.type !== "inference.block.signature") {
      throw new Error("expected inference.block.signature");
    }
    // The signature rode on the functionCall part, so it authenticates
    // the tool_call block at index 1, not the thinking block at 0.
    expect(signature.data.index).toBe(1);
    expect(signature.data.signature).toBe("OPAQUE_SIGNATURE");

    const usage = events[4];
    if (usage?.type !== "inference.usage") {
      throw new Error("expected inference.usage");
    }
    expect(usage.data.usage).toEqual({
      input: 85,
      output: 15,
      cacheRead: 0,
      cacheWrite: 0,
      // thoughtsTokenCount=53 flows to TokenUsage.thinking.
      thinking: 53,
    });
  });

  test("functionCall-with-signature and no preceding thinking decodes with block.signature at the tool_call index", () => {
    // A signature on a functionCall part with no preceding thinking
    // block is valid: it attaches to the tool_call at that part's index.
    const events = adapter.parseResponse(
      JSON.stringify({
        candidates: [
          {
            content: {
              role: "model",
              parts: [
                {
                  functionCall: { name: "x", args: {} },
                  thoughtSignature: "SIG",
                },
              ],
            },
            index: 0,
          },
        ],
      }),
    );

    expect(events.map((e) => e.type)).toEqual([
      "inference.tool_call.start",
      "inference.tool_call.delta",
      "inference.block.signature",
    ]);
    const signature = events[2];
    if (signature?.type !== "inference.block.signature") {
      throw new Error("expected inference.block.signature");
    }
    expect(signature.data.index).toBe(0);
    expect(signature.data.signature).toBe("SIG");
  });

  test("text-with-signature and no preceding thinking decodes with block.signature at the text index", () => {
    // Symmetric: a signed text part signs its own text block.
    const events = adapter.parseResponse(
      JSON.stringify({
        candidates: [
          {
            content: {
              role: "model",
              parts: [{ text: "answer", thoughtSignature: "TSIG" }],
            },
            index: 0,
          },
        ],
      }),
    );

    expect(events.map((e) => e.type)).toEqual([
      "inference.text.delta",
      "inference.block.signature",
    ]);
    const signature = events[1];
    if (signature?.type !== "inference.block.signature") {
      throw new Error("expected inference.block.signature");
    }
    expect(signature.data.index).toBe(0);
    expect(signature.data.signature).toBe("TSIG");
  });

  test("interleaved text and functionCall in one candidate allocate separate block indices", async () => {
    // Not covered by the corpus (text-only OR thinking+functionCall in
    // practice); pin it so allocation rules don't drift.
    const events = await parseWire(adapter, [
      sseFrame({
        candidates: [
          {
            content: {
              role: "model",
              parts: [
                { text: "before " },
                {
                  functionCall: { name: "f", args: { k: "v" } },
                },
                { text: "after" },
              ],
            },
            finishReason: "STOP",
            index: 0,
          },
        ],
        usageMetadata: {
          promptTokenCount: 1,
          candidatesTokenCount: 1,
          totalTokenCount: 2,
        },
      }),
    ]);

    const indicesByType = events
      .filter(
        (e) =>
          e.type === "inference.text.delta" ||
          e.type === "inference.tool_call.start",
      )
      .map((e) => ({ type: e.type, index: e.data.index }));

    // text=0, functionCall=1 (closes the text block), text=2 (a NEW
    // block, not a return to 0). Different-kind parts close the current
    // block; the reopened span is a distinct logical block.
    expect(indicesByType).toEqual([
      { type: "inference.text.delta", index: 0 },
      { type: "inference.tool_call.start", index: 1 },
      { type: "inference.text.delta", index: 2 },
    ]);
  });

  test("part with multiple payload fields throws ProtocolMismatchError", () => {
    const bad = JSON.stringify({
      candidates: [
        {
          content: {
            role: "model",
            parts: [
              {
                text: "ambiguous",
                functionCall: { name: "f", args: {} },
              },
            ],
          },
          index: 0,
        },
      ],
    });
    expect(() => adapter.parseResponse(bad)).toThrow(ProtocolMismatchError);
    expect(() => adapter.parseResponse(bad)).toThrow(/multiple payload fields/);
  });

  test("part with multiple payload fields names every payload in the diagnostic", () => {
    // The diagnostic must name every payload present, not just the first.
    const bad = JSON.stringify({
      candidates: [
        {
          content: {
            role: "model",
            parts: [
              {
                text: "ambiguous",
                functionCall: { name: "f", args: {} },
              },
            ],
          },
          index: 0,
        },
      ],
    });
    expect(() => adapter.parseResponse(bad)).toThrow(/text\+functionCall/);
  });

  test("thought: true on a non-text part throws ProtocolMismatchError", () => {
    // `assertSinglePayload` rejects `thought: true` on parts where it has
    // no wire meaning, e.g. a functionCall.
    const bad = JSON.stringify({
      candidates: [
        {
          content: {
            role: "model",
            parts: [
              {
                functionCall: { name: "f", args: {} },
                thought: true,
              },
            ],
          },
          index: 0,
        },
      ],
    });
    expect(() => adapter.parseResponse(bad)).toThrow(ProtocolMismatchError);
    expect(() => adapter.parseResponse(bad)).toThrow(
      /`thought: true` set on a part with no `text` payload/,
    );
  });

  test("empty-text part bearing a thoughtSignature opens a text block and signs it", async () => {
    // An empty `text: ""` part with a signature still opens a text block
    // so the signature has its own block; it lands on the text block, not
    // the preceding thinking block.
    const events = await parseWire(adapter, [
      sseFrame({
        candidates: [
          {
            content: {
              role: "model",
              parts: [{ text: "reasoning", thought: true }],
            },
            index: 0,
          },
        ],
      }),
      sseFrame({
        candidates: [
          {
            content: {
              role: "model",
              parts: [{ text: "", thoughtSignature: "EMPTY_CARRIER_SIG" }],
            },
            index: 0,
          },
        ],
      }),
    ]);

    // thinking = block 0, the empty-text carrier = block 1.
    expect(events.map((e) => e.type)).toEqual([
      "inference.thinking.delta",
      "inference.text.delta",
      "inference.block.signature",
    ]);
    const sig = events[2];
    if (sig?.type !== "inference.block.signature") {
      throw new Error("expected inference.block.signature");
    }
    expect(sig.data.signature).toBe("EMPTY_CARRIER_SIG");
    expect(sig.data.index).toBe(1);
  });

  test("payload-free signature-only part throws (no block to authenticate)", async () => {
    // A payload-free signed part has no block to own the signature --
    // unmodeled shape, throws regardless of what precedes it.
    await expect(
      parseWire(adapter, [
        sseFrame({
          candidates: [
            {
              content: {
                role: "model",
                parts: [{ text: "non-thinking" }],
              },
              index: 0,
            },
          ],
        }),
        sseFrame({
          candidates: [
            {
              content: {
                role: "model",
                parts: [{ thoughtSignature: "STRAY" }],
              },
              index: 0,
            },
          ],
        }),
      ]),
    ).rejects.toThrow(/no block for the signature to authenticate/);
  });

  test("executableCode-with-signature and no preceding thinking decodes with block.signature at the request index", async () => {
    // Symmetric carrier: a signed executableCode part signs the
    // code-execution-request block at its own index.
    const events = await parseWire(adapter, [
      sseFrame({
        candidates: [
          {
            content: {
              role: "model",
              parts: [
                {
                  executableCode: { language: "PYTHON", code: "print(1)" },
                  thoughtSignature: "EXEC_SIG",
                },
                { codeExecutionResult: { outcome: "OUTCOME_OK", output: "1" } },
              ],
            },
            finishReason: "STOP",
            index: 0,
          },
        ],
        usageMetadata: {
          promptTokenCount: 1,
          candidatesTokenCount: 1,
          totalTokenCount: 2,
        },
      }),
    ]);

    expect(events.map((e) => e.type)).toEqual([
      "inference.code_execution.start",
      "inference.block.signature",
      "inference.code_execution.result",
      "inference.usage",
    ]);
    const sig = events[1];
    if (sig?.type !== "inference.block.signature") {
      throw new Error("expected inference.block.signature");
    }
    expect(sig.data.signature).toBe("EXEC_SIG");
    // The request block is index 0; its signature attaches there.
    expect(sig.data.index).toBe(0);
  });

  test("codeExecutionResult part carrying a thoughtSignature throws (not signable)", async () => {
    // codeExecutionResult blocks have no signature field; a signature on
    // that part is an unmodeled shape and must throw.
    await expect(
      parseWire(adapter, [
        sseFrame({
          candidates: [
            {
              content: {
                role: "model",
                parts: [
                  {
                    executableCode: { language: "PYTHON", code: "print(1)" },
                  },
                  {
                    codeExecutionResult: { outcome: "OUTCOME_OK", output: "1" },
                    thoughtSignature: "NOPE",
                  },
                ],
              },
              index: 0,
            },
          ],
        }),
      ]),
    ).rejects.toThrow(/code_execution_result block is not signable/);
  });

  test("unsigned parts across kinds each open a fresh block index", async () => {
    // Four different-kind parts allocate four distinct block indices; no
    // signatures, so no `inference.block.signature` is emitted.
    const events = await parseWire(adapter, [
      sseFrame({
        candidates: [
          {
            content: {
              role: "model",
              parts: [
                { text: "reasoning A", thought: true },
                { text: "carrier" },
                { text: "reasoning B", thought: true },
                { functionCall: { name: "f", args: {} } },
              ],
            },
            finishReason: "STOP",
            index: 0,
          },
        ],
        usageMetadata: {
          promptTokenCount: 1,
          candidatesTokenCount: 1,
          totalTokenCount: 2,
        },
      }),
    ]);

    // thinking-A=0, text=1, thinking-B=2, functionCall=3.
    expect(events.map((e) => e.type)).toEqual([
      "inference.thinking.delta",
      "inference.text.delta",
      "inference.thinking.delta",
      "inference.tool_call.start",
      "inference.tool_call.delta",
      "inference.usage",
    ]);
    const indices = events
      .filter(
        (e) =>
          e.type === "inference.thinking.delta" ||
          e.type === "inference.text.delta" ||
          e.type === "inference.tool_call.start",
      )
      .map((e) => e.data.index);
    expect(indices).toEqual([0, 1, 2, 3]);
  });

  test("empty part with no payload and no thoughtSignature throws ProtocolMismatchError", () => {
    const bad = JSON.stringify({
      candidates: [
        {
          content: { role: "model", parts: [{}] },
          index: 0,
        },
      ],
    });
    expect(() => adapter.parseResponse(bad)).toThrow(ProtocolMismatchError);
    expect(() => adapter.parseResponse(bad)).toThrow(/no payload/);
  });

  test("multi-turn-streaming fixture replay yields tool_call + delta + usage", async () => {
    const sseBytes = readFileSync(
      join(
        FIXTURE_ROOT,
        "gemini-2.5-flash",
        "function-calling-multi-turn-streaming",
        "exchanges",
        "0",
        "response.sse",
      ),
    );
    const events = await parseWire(adapter, [sseBytes]);

    expect(events.map((e) => e.type)).toEqual([
      "inference.tool_call.start",
      "inference.tool_call.delta",
      "inference.usage",
    ]);

    const delta = events[1];
    if (delta?.type !== "inference.tool_call.delta") {
      throw new Error("expected inference.tool_call.delta");
    }
    expect(JSON.parse(delta.data.argumentFragment)).toEqual({
      location: "Boston, MA",
    });

    const usage = events[2];
    if (usage?.type !== "inference.usage") {
      throw new Error("expected inference.usage");
    }
    expect(usage.data.usage).toEqual({
      input: 81,
      output: 16,
      cacheRead: 0,
      cacheWrite: 0,
      thinking: 0,
    });
  });

  test("with-thinking-streaming fixture replay attaches the signature to the tool_call block", async () => {
    const sseBytes = readFileSync(
      join(
        FIXTURE_ROOT,
        "gemini-2.5-flash",
        "function-calling-with-thinking-streaming",
        "exchanges",
        "0",
        "response.sse",
      ),
    );
    const events = await parseWire(adapter, [sseBytes]);

    expect(events.map((e) => e.type)).toEqual([
      "inference.thinking.delta",
      "inference.tool_call.start",
      "inference.tool_call.delta",
      "inference.block.signature",
      "inference.usage",
    ]);

    const toolStart = events[1];
    if (toolStart?.type !== "inference.tool_call.start") {
      throw new Error("expected inference.tool_call.start");
    }
    expect(toolStart.data.index).toBe(1);

    const signature = events[3];
    if (signature?.type !== "inference.block.signature") {
      throw new Error("expected inference.block.signature");
    }
    // On the real wire the signature rides the follow-on functionCall
    // part, so it authenticates the tool_call at index 1, not the
    // thinking block at 0.
    expect(signature.data.index).toBe(1);
    expect(signature.data.signature.length).toBeGreaterThan(0);

    const usage = events[4];
    if (usage?.type !== "inference.usage") {
      throw new Error("expected inference.usage");
    }
    expect(usage.data.usage).toEqual({
      input: 85,
      output: 15,
      cacheRead: 0,
      cacheWrite: 0,
      thinking: 53,
    });
  });
});

// ---------------------------------------------------------------------------
// buildRequest -- thinking and tool_call round-trip
// ---------------------------------------------------------------------------

describe("Google GenAI adapter: buildRequest thinking round trip", () => {
  test("thinking block translates to {text, thought: true} with no signature on that part", () => {
    const req = adapter.buildRequest(
      [
        {
          role: "assistant",
          content: [{ type: "thinking", thinking: "internal reasoning" }],
          timestamp: 0,
        },
      ],
      "gemini-2.5-flash",
      {},
    );
    const body = parseBody(req.body);
    const contents = GeminiContents.assert(body.contents);
    const parts = contents[0]?.parts;
    expect(parts).toEqual([{ text: "internal reasoning", thought: true }]);
  });

  test("signed tool_call rides its signature back onto the functionCall part", () => {
    // Echoes turn-2/request.json: each block rides its signature back
    // onto its own part; mis-placing it makes Gemini reject the request
    // as a corrupted thinking attestation.
    const req = adapter.buildRequest(
      [
        {
          role: "assistant",
          content: [
            {
              type: "thinking",
              thinking: "Determining weather query.",
            },
            {
              type: "tool_call",
              id: "1",
              name: "getCurrentWeather",
              arguments: { location: "Boston, MA" },
              signature: "OPAQUE_SIGNATURE",
            },
          ],
          timestamp: 0,
        },
      ],
      "gemini-2.5-flash",
      {},
    );
    const body = parseBody(req.body);
    const contents = GeminiContents.assert(body.contents);
    expect(contents[0]?.parts).toEqual([
      { text: "Determining weather query.", thought: true },
      {
        functionCall: {
          name: "getCurrentWeather",
          args: { location: "Boston, MA" },
        },
        thoughtSignature: "OPAQUE_SIGNATURE",
      },
    ]);
  });

  test("unsigned thinking + tool_call leaves the tool_call without a thoughtSignature", () => {
    // A signature-less thinking block does not force one on the next part.
    const req = adapter.buildRequest(
      [
        {
          role: "assistant",
          content: [
            { type: "thinking", thinking: "musing" },
            {
              type: "tool_call",
              id: "1",
              name: "noop",
              arguments: {},
            },
          ],
          timestamp: 0,
        },
      ],
      "gemini-2.5-flash",
      {},
    );
    const body = parseBody(req.body);
    const contents = GeminiContents.assert(body.contents);
    expect(contents[0]?.parts).toEqual([
      { text: "musing", thought: true },
      { functionCall: { name: "noop", args: {} } },
    ]);
  });

  test("signed text block rides its signature back onto the text part", () => {
    // Gemini signs plain-text parts too; the TextBlock rides the
    // signature back onto its text part.
    const req = adapter.buildRequest(
      [
        {
          role: "assistant",
          content: [{ type: "text", text: "the answer", signature: "TSIG" }],
          timestamp: 0,
        },
      ],
      "gemini-2.5-flash",
      {},
    );
    const contents = GeminiContents.assert(parseBody(req.body).contents);
    expect(contents[0]?.parts).toEqual([
      { text: "the answer", thoughtSignature: "TSIG" },
    ]);
  });

  test("signed image block rides its signature back onto the inlineData part", () => {
    // A signed inlineData part rides the signature back onto the
    // ImageBlock's inlineData part.
    const req = adapter.buildRequest(
      [
        {
          role: "assistant",
          content: [
            {
              type: "image",
              source: { kind: "base64", mimeType: "image/png", data: "AAA" },
              signature: "ISIG",
            },
          ],
          timestamp: 0,
        },
      ],
      "gemini-2.5-flash",
      {},
    );
    const contents = GeminiContents.assert(parseBody(req.body).contents);
    expect(contents[0]?.parts).toEqual([
      {
        inlineData: { mimeType: "image/png", data: "AAA" },
        thoughtSignature: "ISIG",
      },
    ]);
  });

  test("signed thinking block with no follow-on part rides its signature on the thinking part", () => {
    // A thinking block that carries its own signature rides it back onto
    // its thinking part; a turn can end on it cleanly.
    const req = adapter.buildRequest(
      [
        {
          role: "assistant",
          content: [
            {
              type: "thinking",
              thinking: "trailing",
              signature: "STRAY",
            },
          ],
          timestamp: 0,
        },
      ],
      "gemini-2.5-flash",
      {},
    );
    const contents = GeminiContents.assert(parseBody(req.body).contents);
    expect(contents[0]?.parts).toEqual([
      { text: "trailing", thought: true, thoughtSignature: "STRAY" },
    ]);
  });

  test("two signed thinking blocks each ride their own signature on their own part", () => {
    // Two consecutive signed thinking blocks are legal; the trailing
    // unsigned tool_call carries no signature.
    const req = adapter.buildRequest(
      [
        {
          role: "assistant",
          content: [
            { type: "thinking", thinking: "first", signature: "SIG1" },
            { type: "thinking", thinking: "second", signature: "SIG2" },
            {
              type: "tool_call",
              id: "1",
              name: "noop",
              arguments: {},
            },
          ],
          timestamp: 0,
        },
      ],
      "gemini-2.5-flash",
      {},
    );
    const contents = GeminiContents.assert(parseBody(req.body).contents);
    expect(contents[0]?.parts).toEqual([
      { text: "first", thought: true, thoughtSignature: "SIG1" },
      { text: "second", thought: true, thoughtSignature: "SIG2" },
      { functionCall: { name: "noop", args: {} } },
    ]);
  });

  test("turn-2 round-trip fixture parity for function-calling-with-thinking-streaming", () => {
    // The harness must reconstruct turn-2's request from the thinking +
    // tool_call + tool_result blocks; byte-equivalent parts guard the
    // signature placement.
    const FIXTURE = readFixtureJSON(
      "gemini-2.5-flash",
      "function-calling-with-thinking-streaming",
      "exchanges",
      "1",
      "request.json",
    );
    const fixtureParts = GeminiContents.assert(FIXTURE.contents)[1]?.parts;
    // arktype (not a type assertion) extracts `thinking` text and
    // `thoughtSignature` from the external fixture file.
    const FixtureThinkingPart = type({
      text: "string",
      thought: "true",
    });
    const FixtureFunctionCallPart = type({
      functionCall: { name: "string", args: "Record<string, unknown>" },
      thoughtSignature: "string",
    });
    const thinkingText = FixtureThinkingPart.assert(fixtureParts?.[0]).text;
    const thoughtSignature = FixtureFunctionCallPart.assert(
      fixtureParts?.[1],
    ).thoughtSignature;

    const req = adapter.buildRequest(
      [
        {
          role: "user",
          content: [
            {
              type: "text",
              text: "Think carefully, then use the getCurrentWeather tool to look up the current weather in Boston, MA.",
            },
          ],
          timestamp: 0,
        },
        {
          role: "assistant",
          content: [
            {
              type: "thinking",
              thinking: thinkingText,
            },
            {
              type: "tool_call",
              id: "1",
              name: "getCurrentWeather",
              arguments: { location: "Boston, MA" },
              signature: thoughtSignature,
            },
          ],
          timestamp: 0,
        },
        {
          role: "user",
          content: [
            {
              type: "tool_result",
              callId: "1",
              content: [
                {
                  type: "text",
                  text: JSON.stringify({
                    location: "Boston, MA",
                    temperatureF: 62,
                    conditions: "partly cloudy",
                    windMph: 8,
                  }),
                },
              ],
            },
          ],
          timestamp: 0,
        },
      ],
      "gemini-2.5-flash",
      {
        tools: [
          {
            name: "getCurrentWeather",
            description:
              "Get the current weather conditions for a given city. Use this whenever the user asks about weather.",
            inputSchema: {
              type: "object",
              properties: {
                location: {
                  type: "string",
                  description:
                    "The city and optional state, e.g. 'Boston, MA'.",
                },
              },
              required: ["location"],
            },
          },
        ],
        thinking: { enabled: true, budgetTokens: 1024 },
      },
    );

    const body = parseBody(req.body);
    // Second `contents[]` element pinned byte-for-byte; the user turns
    // are exercised elsewhere.
    const contents = GeminiContents.assert(body.contents);
    expect(contents[1]?.parts).toEqual(fixtureParts);
  });
});

// ---------------------------------------------------------------------------
// Harness round trip -- thinking + tool_call
// ---------------------------------------------------------------------------

describe("Google GenAI adapter: harness round trip with thinking + tool_call", () => {
  const inertScheduler: Scheduler = {
    setTimeout: () => () => {
      /* tests do not exercise timer firing */
    },
    now: () => 0,
  };

  const SOURCE: InferenceSource = {
    id: "google-genai:gemini-2.5-flash",
    provider: "google-genai",
    baseURL: "https://generativelanguage.googleapis.com",
    credentialId: "test-key",
    model: "gemini-2.5-flash",
  };

  test("function-calling-with-thinking-streaming fixture flows through runInference end-to-end", async () => {
    // Replays the captured SSE response; the final turn carries thinking
    // then a signed tool_call. Ordering matters: Gemini's wire convention
    // is thinking-then-functionCall for the follow-up echo.
    const sseBytes = readFileSync(
      join(
        FIXTURE_ROOT,
        "gemini-2.5-flash",
        "function-calling-with-thinking-streaming",
        "exchanges",
        "0",
        "response.sse",
      ),
    );

    const fetchImpl: Dependencies["fetch"] = () =>
      Promise.resolve(
        new Response(
          new ReadableStream({
            start(controller) {
              controller.enqueue(sseBytes);
              controller.close();
            },
          }),
          {
            status: 200,
            headers: { "content-type": "text/event-stream" },
          },
        ),
      );

    let seq = 0;
    const events: InferenceEvent[] = [];
    for await (const ev of runInference({
      readMaterial: () => ({ secret: "test-secret" }),
      turns: [
        {
          role: "user",
          content: [
            {
              type: "text",
              text: "Think carefully, then use the getCurrentWeather tool to look up the current weather in Boston, MA.",
            },
          ],
          timestamp: 0,
        },
      ],
      source: SOURCE,
      nextSeq: () => seq++,
      deps: {
        fetch: fetchImpl,
        scheduler: inertScheduler,
        adapters: createBuiltinRegistry(),
      },
    })) {
      events.push(ev);
    }

    const done = events.find((e) => e.type === "inference.done");
    if (done?.type !== "inference.done") {
      throw new Error("expected inference.done event");
    }

    const blocks = done.data.turn.content;
    expect(blocks.length).toBe(2);
    const thinking = blocks[0];
    if (thinking?.type !== "thinking") {
      throw new Error("expected first content block to be thinking");
    }
    expect(thinking.thinking.length).toBeGreaterThan(0);
    // The wire signs the functionCall part, so the thinking block
    // carries no signature.
    expect(thinking.signature).toBeUndefined();

    const toolCall = blocks[1];
    if (toolCall?.type !== "tool_call") {
      throw new Error("expected second content block to be tool_call");
    }
    expect(toolCall.name).toBe("getCurrentWeather");
    expect(toolCall.arguments).toEqual({ location: "Boston, MA" });
    // The signature rode the functionCall part; it authenticates the
    // tool_call block.
    expect(toolCall.signature).toBeDefined();
    expect(toolCall.signature?.length).toBeGreaterThan(0);

    expect(done.data.usage).toEqual({
      input: 85,
      output: 15,
      cacheRead: 0,
      cacheWrite: 0,
      thinking: 53,
    });
  });
});

// ---------------------------------------------------------------------------
// parseResponse -- image-output path
// ---------------------------------------------------------------------------

describe("Google GenAI adapter: parseResponse image output", () => {
  test("inlineData part emits inference.image_output with the bytes wrapped as a base64 ImageBlock", async () => {
    const events = await parseWire(adapter, [
      sseFrame({
        candidates: [
          {
            content: {
              role: "model",
              parts: [
                {
                  inlineData: {
                    mimeType: "image/png",
                    data: "iVBORw0KGgoAAAA",
                  },
                },
              ],
            },
            index: 0,
          },
        ],
      }),
    ]);

    expect(events.map((e) => e.type)).toEqual(["inference.image_output"]);
    const out = events[0];
    if (out?.type !== "inference.image_output") {
      throw new Error("expected inference.image_output");
    }
    expect(out.data.index).toBe(0);
    expect(out.data.image).toEqual({
      type: "image",
      source: {
        kind: "base64",
        mimeType: "image/png",
        data: "iVBORw0KGgoAAAA",
      },
    });
  });

  test("text then inlineData then text allocates three distinct block indices", async () => {
    // text coalesces at 0, image is atomic at 1, trailing text reopens
    // at 2 (a new logical block, not a return to 0).
    const events = await parseWire(adapter, [
      sseFrame({
        candidates: [
          {
            content: {
              role: "model",
              parts: [
                { text: "Here " },
                { text: "you go: " },
                {
                  inlineData: {
                    mimeType: "image/png",
                    data: "AAA",
                  },
                },
                { text: "(done)" },
              ],
            },
            finishReason: "STOP",
            index: 0,
          },
        ],
        usageMetadata: {
          promptTokenCount: 1,
          candidatesTokenCount: 1,
          totalTokenCount: 2,
        },
      }),
    ]);

    const indexedTypes = events
      .filter(
        (e) =>
          e.type === "inference.text.delta" ||
          e.type === "inference.image_output",
      )
      .map((e) => ({ type: e.type, index: e.data.index }));

    expect(indexedTypes).toEqual([
      { type: "inference.text.delta", index: 0 },
      { type: "inference.text.delta", index: 0 },
      { type: "inference.image_output", index: 1 },
      { type: "inference.text.delta", index: 2 },
    ]);
  });

  test("thinking text then inlineData with thoughtSignature attaches the signature to the image block", async () => {
    // A signed inlineData part authenticates the image block it carries,
    // not the preceding thinking block; the signature event follows the
    // image_output so the router lands it at the image's index.
    const events = await parseWire(adapter, [
      sseFrame({
        candidates: [
          {
            content: {
              role: "model",
              parts: [{ text: "reasoning", thought: true }],
            },
            index: 0,
          },
        ],
      }),
      sseFrame({
        candidates: [
          {
            content: {
              role: "model",
              parts: [
                {
                  inlineData: { mimeType: "image/png", data: "AAA" },
                  thoughtSignature: "IMG_CARRIER_SIG",
                },
              ],
            },
            finishReason: "STOP",
            index: 0,
          },
        ],
        usageMetadata: {
          promptTokenCount: 1,
          candidatesTokenCount: 1,
          totalTokenCount: 2,
        },
      }),
    ]);

    expect(events.map((e) => e.type)).toEqual([
      "inference.thinking.delta",
      "inference.image_output",
      "inference.block.signature",
      "inference.usage",
    ]);

    const image = events[1];
    if (image?.type !== "inference.image_output") {
      throw new Error("expected inference.image_output");
    }
    expect(image.data.index).toBe(1);

    const sig = events[2];
    if (sig?.type !== "inference.block.signature") {
      throw new Error("expected inference.block.signature");
    }
    expect(sig.data.signature).toBe("IMG_CARRIER_SIG");
    // The signature rode on the inlineData part, so it authenticates
    // the image block at index 1, not the thinking block at 0.
    expect(sig.data.index).toBe(1);
  });

  test("inlineData-with-signature and no preceding thinking decodes with block.signature at the image index", () => {
    // A signed inlineData part with no preceding thinking block is valid:
    // it signs the image block at that part's index.
    const events = adapter.parseResponse(
      JSON.stringify({
        candidates: [
          {
            content: {
              role: "model",
              parts: [
                {
                  inlineData: { mimeType: "image/png", data: "AAA" },
                  thoughtSignature: "STRAY",
                },
              ],
            },
            index: 0,
          },
        ],
      }),
    );

    expect(events.map((e) => e.type)).toEqual([
      "inference.image_output",
      "inference.block.signature",
    ]);
    const sig = events[1];
    if (sig?.type !== "inference.block.signature") {
      throw new Error("expected inference.block.signature");
    }
    expect(sig.data.index).toBe(0);
    expect(sig.data.signature).toBe("STRAY");
  });

  test("inlineData with a non-image MIME throws ProtocolMismatchError", () => {
    // inlineData is always wrapped as a base64 ImageBlock; non-image
    // MIME types must be rejected at the boundary, not mistyped.
    const bad = JSON.stringify({
      candidates: [
        {
          content: {
            role: "model",
            parts: [{ inlineData: { mimeType: "audio/wav", data: "AAA" } }],
          },
          index: 0,
        },
      ],
    });
    expect(() => adapter.parseResponse(bad)).toThrow(ProtocolMismatchError);
    expect(() => adapter.parseResponse(bad)).toThrow(/non-image mimeType/);
  });

  test("inlineData with a multi-payload part throws ProtocolMismatchError", () => {
    // Mutual-exclusivity of payload fields covers inlineData too.
    const bad = JSON.stringify({
      candidates: [
        {
          content: {
            role: "model",
            parts: [
              {
                text: "ambiguous",
                inlineData: { mimeType: "image/png", data: "AAA" },
              },
            ],
          },
          index: 0,
        },
      ],
    });
    expect(() => adapter.parseResponse(bad)).toThrow(ProtocolMismatchError);
    expect(() => adapter.parseResponse(bad)).toThrow(/text\+inlineData/);
  });

  test("image-output-streaming fixture replay yields text deltas, one image_output, and usage", async () => {
    // Events 0-1 coalesce into one text block at 0; event 2 carries the
    // image at 1; the final empty-text STOP event carries usage and the
    // finishReason but no delta.
    const sseBytes = readFileSync(
      join(
        FIXTURE_ROOT,
        "gemini-2.5-flash-image",
        "image-output-streaming",
        "exchanges",
        "0",
        "response.sse",
      ),
    );
    const events = await parseWire(adapter, [sseBytes]);

    const textDeltas = events.filter((e) => e.type === "inference.text.delta");
    const imageOutputs = events.filter(
      (e) => e.type === "inference.image_output",
    );
    const usageEvents = events.filter((e) => e.type === "inference.usage");

    expect(textDeltas).toHaveLength(2);
    expect(imageOutputs).toHaveLength(1);
    expect(usageEvents).toHaveLength(1);

    for (const d of textDeltas) {
      if (d.type !== "inference.text.delta") continue;
      expect(d.data.index).toBe(0);
    }
    const image = imageOutputs[0];
    if (image?.type !== "inference.image_output") {
      throw new Error("expected inference.image_output");
    }
    expect(image.data.index).toBe(1);
    if (image.data.image.source.kind !== "base64") {
      throw new Error("expected base64 source on the emitted image");
    }
    expect(image.data.image.source.mimeType).toBe("image/png");
    // ~380KB of base64 passes through verbatim; elision is the logger's concern.
    expect(image.data.image.source.data.length).toBeGreaterThan(100_000);
  });
});

// ---------------------------------------------------------------------------
// Harness round trip -- image output
// ---------------------------------------------------------------------------

describe("Google GenAI adapter: harness round trip with image output", () => {
  const inertScheduler: Scheduler = {
    setTimeout: () => () => {
      /* tests do not exercise timer firing */
    },
    now: () => 0,
  };

  const SOURCE: InferenceSource = {
    id: "google-genai:gemini-2.5-flash-image",
    provider: "google-genai",
    baseURL: "https://generativelanguage.googleapis.com",
    credentialId: "test-key",
    model: "gemini-2.5-flash-image",
  };

  test("image-output-streaming fixture replay produces a final turn with text then ImageBlock", async () => {
    // The final turn's content[] must carry the ImageBlock with the full
    // base64 intact; without the harness's image_output handler the image
    // would silently drop from replay.
    const sseBytes = readFileSync(
      join(
        FIXTURE_ROOT,
        "gemini-2.5-flash-image",
        "image-output-streaming",
        "exchanges",
        "0",
        "response.sse",
      ),
    );

    const fetchImpl: Dependencies["fetch"] = () =>
      Promise.resolve(
        new Response(
          new ReadableStream({
            start(controller) {
              controller.enqueue(sseBytes);
              controller.close();
            },
          }),
          {
            status: 200,
            headers: { "content-type": "text/event-stream" },
          },
        ),
      );

    let seq = 0;
    const events: InferenceEvent[] = [];
    for await (const ev of runInference({
      readMaterial: () => ({ secret: "test-secret" }),
      turns: [
        {
          role: "user",
          content: [
            {
              type: "text",
              text: "Generate a small illustration of a red apple on a white background.",
            },
          ],
          timestamp: 0,
        },
      ],
      source: SOURCE,
      nextSeq: () => seq++,
      deps: {
        fetch: fetchImpl,
        scheduler: inertScheduler,
        adapters: createBuiltinRegistry(),
      },
    })) {
      events.push(ev);
    }

    const done = events.find((e) => e.type === "inference.done");
    if (done?.type !== "inference.done") {
      throw new Error("expected inference.done event");
    }

    const blocks = done.data.turn.content;
    expect(blocks).toHaveLength(2);

    const text = blocks[0];
    if (text?.type !== "text") {
      throw new Error("expected first content block to be text");
    }
    expect(text.text).toBe("Here you go: ");

    const image = blocks[1];
    if (image?.type !== "image") {
      throw new Error("expected second content block to be image");
    }
    if (image.source.kind !== "base64") {
      throw new Error("expected base64 source on the image block");
    }
    expect(image.source.mimeType).toBe("image/png");
    expect(image.source.data.length).toBeGreaterThan(100_000);

    // The mid-stream inference.image_output event guards the harness's
    // image_output case handler against silent removal.
    const imageEvent = events.find((e) => e.type === "inference.image_output");
    expect(imageEvent).toBeDefined();
  });
});

// ---------------------------------------------------------------------------
// parseResponse -- grounding-as-citation
// ---------------------------------------------------------------------------

describe("Google GenAI adapter: parseResponse grounding", () => {
  test("groundingSupport expands into one citation per chunk index it references", async () => {
    const events = await parseWire(adapter, [
      sseFrame({
        candidates: [
          {
            content: {
              role: "model",
              parts: [{ text: "John won the prize." }],
            },
            groundingMetadata: {
              groundingChunks: [
                { web: { uri: "https://a.example", title: "A" } },
                { web: { uri: "https://b.example", title: "B" } },
              ],
              groundingSupports: [
                {
                  segment: {
                    startIndex: 0,
                    endIndex: 19,
                    text: "John won the prize.",
                  },
                  groundingChunkIndices: [0, 1],
                },
              ],
            },
            finishReason: "STOP",
            index: 0,
          },
        ],
        usageMetadata: {
          promptTokenCount: 5,
          candidatesTokenCount: 5,
          totalTokenCount: 10,
        },
      }),
    ]);

    const citations = events.filter((e) => e.type === "inference.citation");
    expect(citations).toHaveLength(2);

    const first = citations[0];
    const second = citations[1];
    if (
      first?.type !== "inference.citation" ||
      second?.type !== "inference.citation"
    ) {
      throw new Error("expected two inference.citation events");
    }

    // Same citedText/offset (the support's segment), distinct sources.
    expect(first.data.index).toBe(0);
    expect(first.data.citation.citedText).toBe("John won the prize.");
    expect(first.data.citation.textOffset).toEqual({ start: 0, end: 19 });
    expect(first.data.citation.source).toEqual({
      uri: "https://a.example",
      title: "A",
    });

    expect(second.data.index).toBe(0);
    expect(second.data.citation.source).toEqual({
      uri: "https://b.example",
      title: "B",
    });
  });

  test("citation events are emitted before the terminal inference.usage", async () => {
    // Citations must precede the closing signal; a citation arriving
    // after its block would land in the orphan-citation check.
    const events = await parseWire(adapter, [
      sseFrame({
        candidates: [
          {
            content: {
              role: "model",
              parts: [{ text: "x" }],
            },
            groundingMetadata: {
              groundingChunks: [{ web: { uri: "u", title: "t" } }],
              groundingSupports: [
                {
                  segment: { startIndex: 0, endIndex: 1, text: "x" },
                  groundingChunkIndices: [0],
                },
              ],
            },
            finishReason: "STOP",
            index: 0,
          },
        ],
        usageMetadata: {
          promptTokenCount: 1,
          candidatesTokenCount: 1,
          totalTokenCount: 2,
        },
      }),
    ]);
    const citationIdx = events.findIndex(
      (e) => e.type === "inference.citation",
    );
    const usageIdx = events.findIndex((e) => e.type === "inference.usage");
    expect(citationIdx).toBeGreaterThanOrEqual(0);
    expect(usageIdx).toBeGreaterThan(citationIdx);
  });

  test("groundingMetadata with no current text block throws ProtocolMismatchError", () => {
    const bad = JSON.stringify({
      candidates: [
        {
          content: { role: "model", parts: [] },
          groundingMetadata: {
            groundingChunks: [{ web: { uri: "u", title: "t" } }],
            groundingSupports: [
              {
                segment: { startIndex: 0, endIndex: 1, text: "x" },
                groundingChunkIndices: [0],
              },
            ],
          },
          index: 0,
        },
      ],
    });
    expect(() =>
      createGoogleGenAIAdapter(TEST_SOURCE).parseResponse(bad),
    ).toThrow(ProtocolMismatchError);
    expect(() =>
      createGoogleGenAIAdapter(TEST_SOURCE).parseResponse(bad),
    ).toThrow(/without a current text block/);
  });

  test("out-of-range groundingChunkIndex throws ProtocolMismatchError", () => {
    const bad = JSON.stringify({
      candidates: [
        {
          content: { role: "model", parts: [{ text: "x" }] },
          groundingMetadata: {
            groundingChunks: [{ web: { uri: "u", title: "t" } }],
            groundingSupports: [
              {
                segment: { startIndex: 0, endIndex: 1, text: "x" },
                groundingChunkIndices: [0, 99],
              },
            ],
          },
          index: 0,
        },
      ],
    });
    expect(() =>
      createGoogleGenAIAdapter(TEST_SOURCE).parseResponse(bad),
    ).toThrow(ProtocolMismatchError);
    expect(() =>
      createGoogleGenAIAdapter(TEST_SOURCE).parseResponse(bad),
    ).toThrow(/chunk index 99/);
  });

  test("non-web chunk kinds are skipped without throwing", async () => {
    // Non-web grounding chunks have no uri/title and are dropped rather
    // than synthesized into a placeholder citation.
    const events = await parseWire(adapter, [
      sseFrame({
        candidates: [
          {
            content: { role: "model", parts: [{ text: "x" }] },
            groundingMetadata: {
              groundingChunks: [
                {}, // non-web chunk: no `web` field
                { web: { uri: "u", title: "t" } },
              ],
              groundingSupports: [
                {
                  segment: { startIndex: 0, endIndex: 1, text: "x" },
                  groundingChunkIndices: [0, 1],
                },
              ],
            },
            finishReason: "STOP",
            index: 0,
          },
        ],
        usageMetadata: {
          promptTokenCount: 1,
          candidatesTokenCount: 1,
          totalTokenCount: 2,
        },
      }),
    ]);
    const citations = events.filter((e) => e.type === "inference.citation");
    expect(citations).toHaveLength(1);
    if (citations[0]?.type !== "inference.citation") {
      throw new Error("expected inference.citation");
    }
    expect(citations[0].data.citation.source.uri).toBe("u");
  });

  test("grounding-streaming fixture replay produces multiple citations anchored to the single text block", async () => {
    const sseBytes = readFileSync(
      join(
        FIXTURE_ROOT,
        "gemini-2.5-flash",
        "grounding-streaming",
        "exchanges",
        "0",
        "response.sse",
      ),
    );
    const events = await parseWire(adapter, [sseBytes]);

    const citations = events.filter((e) => e.type === "inference.citation");
    // Multiple chunks per support -> citations outnumber supports.
    expect(citations.length).toBeGreaterThan(3);

    // Every citation anchors to text block index 0 (the single
    // logical text block the response produced).
    for (const ev of citations) {
      if (ev.type !== "inference.citation") continue;
      expect(ev.data.index).toBe(0);
      expect(ev.data.citation.source.uri).toMatch(/^https:\/\//);
      expect(ev.data.citation.citedText.length).toBeGreaterThan(0);
    }

    // Citations precede the inference.usage emission in the event
    // stream.
    const usageIdx = events.findIndex((e) => e.type === "inference.usage");
    const lastCitationIdx = events.reduce(
      (acc, e, i) => (e.type === "inference.citation" ? i : acc),
      -1,
    );
    expect(lastCitationIdx).toBeGreaterThan(-1);
    expect(usageIdx).toBeGreaterThan(lastCitationIdx);
  });
});

// ---------------------------------------------------------------------------
// Harness round trip -- grounding
// ---------------------------------------------------------------------------

describe("Google GenAI adapter: harness round trip with grounding", () => {
  const inertScheduler: Scheduler = {
    setTimeout: () => () => {
      /* tests do not exercise timer firing */
    },
    now: () => 0,
  };

  const SOURCE: InferenceSource = {
    id: "google-genai:gemini-2.5-flash",
    provider: "google-genai",
    baseURL: "https://generativelanguage.googleapis.com",
    credentialId: "test-key",
    model: "gemini-2.5-flash",
  };

  test("grounding-streaming fixture replay produces a final turn with text then citations interleaved", async () => {
    const sseBytes = readFileSync(
      join(
        FIXTURE_ROOT,
        "gemini-2.5-flash",
        "grounding-streaming",
        "exchanges",
        "0",
        "response.sse",
      ),
    );

    const fetchImpl: Dependencies["fetch"] = () =>
      Promise.resolve(
        new Response(
          new ReadableStream({
            start(controller) {
              controller.enqueue(sseBytes);
              controller.close();
            },
          }),
          {
            status: 200,
            headers: { "content-type": "text/event-stream" },
          },
        ),
      );

    let seq = 0;
    const events: InferenceEvent[] = [];
    for await (const ev of runInference({
      readMaterial: () => ({ secret: "test-secret" }),
      turns: [
        {
          role: "user",
          content: [
            {
              type: "text",
              text: "Who won the 2025 Nobel Prize in Physics?",
            },
          ],
          timestamp: 0,
        },
      ],
      source: SOURCE,
      nextSeq: () => seq++,
      deps: {
        fetch: fetchImpl,
        scheduler: inertScheduler,
        adapters: createBuiltinRegistry(),
      },
    })) {
      events.push(ev);
    }

    const done = events.find((e) => e.type === "inference.done");
    if (done?.type !== "inference.done") {
      throw new Error("expected inference.done event");
    }

    const blocks = done.data.turn.content;
    // The final turn leads with the text block; citations interleave
    // immediately after it.
    const firstBlock = blocks[0];
    if (firstBlock?.type !== "text") {
      throw new Error("expected first content block to be text");
    }
    expect(firstBlock.text.length).toBeGreaterThan(0);

    const citationBlocks = blocks.filter((b) => b.type === "citation");
    expect(citationBlocks.length).toBeGreaterThan(3);
    // Citations append immediately after the attributed text block --
    // `[text, citation, citation, ...]` with no other block between.
    for (let i = 1; i < 1 + citationBlocks.length; i++) {
      expect(blocks[i]?.type).toBe("citation");
    }
  });
});

// ---------------------------------------------------------------------------
// parseResponse -- code-execution path
// ---------------------------------------------------------------------------

describe("Google GenAI adapter: parseResponse code execution", () => {
  test("executableCode then codeExecutionResult yields start + result with back-pointer", async () => {
    const events = await parseWire(adapter, [
      sseFrame({
        candidates: [
          {
            content: {
              role: "model",
              parts: [
                {
                  executableCode: {
                    language: "PYTHON",
                    code: "print(1 + 2)",
                  },
                },
              ],
            },
            index: 0,
          },
        ],
      }),
      sseFrame({
        candidates: [
          {
            content: {
              role: "model",
              parts: [
                {
                  codeExecutionResult: {
                    outcome: "OUTCOME_OK",
                    output: "3\n",
                  },
                },
              ],
            },
            finishReason: "STOP",
            index: 0,
          },
        ],
        usageMetadata: {
          promptTokenCount: 5,
          candidatesTokenCount: 10,
          totalTokenCount: 15,
        },
      }),
    ]);

    expect(events.map((e) => e.type)).toEqual([
      "inference.code_execution.start",
      "inference.code_execution.result",
      "inference.usage",
    ]);

    const start = events[0];
    if (start?.type !== "inference.code_execution.start") {
      throw new Error("expected inference.code_execution.start");
    }
    expect(start.data.index).toBe(0);
    expect(start.data.request).toEqual({
      type: "code_execution_request",
      id: "gemini-exec-0",
      code: "print(1 + 2)",
      language: "PYTHON",
    });

    const result = events[1];
    if (result?.type !== "inference.code_execution.result") {
      throw new Error("expected inference.code_execution.result");
    }
    expect(result.data.index).toBe(1);
    expect(result.data.result).toEqual({
      type: "code_execution_result",
      requestId: "gemini-exec-0",
      status: "ok",
      stdout: "3\n",
      providerOutcome: "OUTCOME_OK",
    });
  });

  test("OUTCOME_FAILED maps to status 'error' and OUTCOME_DEADLINE_EXCEEDED maps to 'timeout'", async () => {
    const failed = await parseWire(adapter, [
      sseFrame({
        candidates: [
          {
            content: {
              role: "model",
              parts: [{ executableCode: { language: "PYTHON", code: "x" } }],
            },
            index: 0,
          },
        ],
      }),
      sseFrame({
        candidates: [
          {
            content: {
              role: "model",
              parts: [
                {
                  codeExecutionResult: {
                    outcome: "OUTCOME_FAILED",
                    output: "Traceback...",
                  },
                },
              ],
            },
            index: 0,
          },
        ],
      }),
    ]);
    const failedResult = failed.find(
      (e) => e.type === "inference.code_execution.result",
    );
    if (failedResult?.type !== "inference.code_execution.result") {
      throw new Error("expected inference.code_execution.result");
    }
    expect(failedResult.data.result.status).toBe("error");
    expect(failedResult.data.result.providerOutcome).toBe("OUTCOME_FAILED");

    // Fresh adapter to reset per-request state for the second case.
    const adapter2 = createGoogleGenAIAdapter(TEST_SOURCE);
    const timeout = await parseWire(adapter2, [
      sseFrame({
        candidates: [
          {
            content: {
              role: "model",
              parts: [{ executableCode: { language: "PYTHON", code: "x" } }],
            },
            index: 0,
          },
        ],
      }),
      sseFrame({
        candidates: [
          {
            content: {
              role: "model",
              parts: [
                {
                  codeExecutionResult: {
                    outcome: "OUTCOME_DEADLINE_EXCEEDED",
                  },
                },
              ],
            },
            index: 0,
          },
        ],
      }),
    ]);
    const timeoutResult = timeout.find(
      (e) => e.type === "inference.code_execution.result",
    );
    if (timeoutResult?.type !== "inference.code_execution.result") {
      throw new Error("expected inference.code_execution.result");
    }
    expect(timeoutResult.data.result.status).toBe("timeout");
    // Output is optional; absent on the timeout response.
    expect(timeoutResult.data.result.stdout).toBeUndefined();
  });

  test("unknown outcome throws ProtocolMismatchError naming the value", () => {
    // Pairing needs a prior executableCode event; the parser clears
    // `pendingExecutionRequestId` only on success, so a throw leaves
    // residue -- each `toThrow` needs a fresh adapter.
    function buildPendingAdapter(): ProviderAdapter {
      const a = createGoogleGenAIAdapter(TEST_SOURCE);
      a.parseResponse(
        JSON.stringify({
          candidates: [
            {
              content: {
                role: "model",
                parts: [{ executableCode: { language: "PYTHON", code: "x" } }],
              },
              index: 0,
            },
          ],
        }),
      );
      return a;
    }
    const bad = JSON.stringify({
      candidates: [
        {
          content: {
            role: "model",
            parts: [
              {
                codeExecutionResult: { outcome: "OUTCOME_FOO", output: "x" },
              },
            ],
          },
          index: 0,
        },
      ],
    });
    expect(() => buildPendingAdapter().parseResponse(bad)).toThrow(
      ProtocolMismatchError,
    );
    expect(() => buildPendingAdapter().parseResponse(bad)).toThrow(
      /OUTCOME_FOO/,
    );
  });

  test("two executableCode parts without an intervening result throws", () => {
    const bad = JSON.stringify({
      candidates: [
        {
          content: {
            role: "model",
            parts: [
              { executableCode: { language: "PYTHON", code: "a" } },
              { executableCode: { language: "PYTHON", code: "b" } },
            ],
          },
          index: 0,
        },
      ],
    });
    expect(() => adapter.parseResponse(bad)).toThrow(ProtocolMismatchError);
    expect(() => adapter.parseResponse(bad)).toThrow(
      /second executableCode part/,
    );
  });

  test("codeExecutionResult with no preceding executableCode throws", () => {
    const bad = JSON.stringify({
      candidates: [
        {
          content: {
            role: "model",
            parts: [
              { codeExecutionResult: { outcome: "OUTCOME_OK", output: "" } },
            ],
          },
          index: 0,
        },
      ],
    });
    expect(() => adapter.parseResponse(bad)).toThrow(ProtocolMismatchError);
    expect(() => adapter.parseResponse(bad)).toThrow(
      /no preceding executableCode part/,
    );
  });

  test("terminal event with an unmatched executableCode throws", () => {
    const bad = JSON.stringify({
      candidates: [
        {
          content: {
            role: "model",
            parts: [{ executableCode: { language: "PYTHON", code: "x" } }],
          },
          finishReason: "STOP",
          index: 0,
        },
      ],
      usageMetadata: {
        promptTokenCount: 1,
        candidatesTokenCount: 1,
        totalTokenCount: 2,
      },
    });
    // Fresh adapter per `toThrow` so the first throw's residue doesn't
    // change the second call's error message.
    expect(() =>
      createGoogleGenAIAdapter(TEST_SOURCE).parseResponse(bad),
    ).toThrow(ProtocolMismatchError);
    expect(() =>
      createGoogleGenAIAdapter(TEST_SOURCE).parseResponse(bad),
    ).toThrow(/unmatched code-execution request/);
  });

  test("code-execution-streaming fixture replay produces start, result, two text deltas, and usage", async () => {
    const sseBytes = readFileSync(
      join(
        FIXTURE_ROOT,
        "gemini-2.5-flash",
        "code-execution-streaming",
        "exchanges",
        "0",
        "response.sse",
      ),
    );
    const events = await parseWire(adapter, [sseBytes]);

    const types = events.map((e) => e.type);
    expect(types).toEqual([
      "inference.code_execution.start",
      "inference.code_execution.result",
      "inference.text.delta",
      "inference.text.delta",
      "inference.usage",
    ]);

    const start = events[0];
    if (start?.type !== "inference.code_execution.start") {
      throw new Error("expected inference.code_execution.start");
    }
    expect(start.data.request.id).toBe("gemini-exec-0");
    expect(start.data.request.code).toContain("fibonacci");
    expect(start.data.request.language).toBe("PYTHON");
    expect(start.data.index).toBe(0);

    const result = events[1];
    if (result?.type !== "inference.code_execution.result") {
      throw new Error("expected inference.code_execution.result");
    }
    expect(result.data.result.requestId).toBe("gemini-exec-0");
    expect(result.data.result.status).toBe("ok");
    expect(result.data.result.stdout).toContain("6765");
    expect(result.data.index).toBe(1);

    // Follow-on text lands at a fresh index (2): the code pair closed
    // the block, so the next text is a new logical block.
    const textIndices = events
      .filter((e) => e.type === "inference.text.delta")
      .map((e) => (e.type === "inference.text.delta" ? e.data.index : -1));
    expect(textIndices).toEqual([2, 2]);
  });
});

// ---------------------------------------------------------------------------
// Harness round trip -- code execution
// ---------------------------------------------------------------------------

describe("Google GenAI adapter: harness round trip with code execution", () => {
  const inertScheduler: Scheduler = {
    setTimeout: () => () => {
      /* tests do not exercise timer firing */
    },
    now: () => 0,
  };

  const SOURCE: InferenceSource = {
    id: "google-genai:gemini-2.5-flash",
    provider: "google-genai",
    baseURL: "https://generativelanguage.googleapis.com",
    credentialId: "test-key",
    model: "gemini-2.5-flash",
  };

  test("code-execution-streaming fixture flows through runInference into a final turn carrying request + result + text", async () => {
    const sseBytes = readFileSync(
      join(
        FIXTURE_ROOT,
        "gemini-2.5-flash",
        "code-execution-streaming",
        "exchanges",
        "0",
        "response.sse",
      ),
    );

    const fetchImpl: Dependencies["fetch"] = () =>
      Promise.resolve(
        new Response(
          new ReadableStream({
            start(controller) {
              controller.enqueue(sseBytes);
              controller.close();
            },
          }),
          {
            status: 200,
            headers: { "content-type": "text/event-stream" },
          },
        ),
      );

    let seq = 0;
    const events: InferenceEvent[] = [];
    for await (const ev of runInference({
      readMaterial: () => ({ secret: "test-secret" }),
      turns: [
        {
          role: "user",
          content: [
            {
              type: "text",
              text: "Use Python to compute the 20th Fibonacci number.",
            },
          ],
          timestamp: 0,
        },
      ],
      source: SOURCE,
      nextSeq: () => seq++,
      deps: {
        fetch: fetchImpl,
        scheduler: inertScheduler,
        adapters: createBuiltinRegistry(),
      },
    })) {
      events.push(ev);
    }

    const done = events.find((e) => e.type === "inference.done");
    if (done?.type !== "inference.done") {
      throw new Error("expected inference.done event");
    }

    const blocks = done.data.turn.content;
    expect(blocks).toHaveLength(3);

    const request = blocks[0];
    if (request?.type !== "code_execution_request") {
      throw new Error(
        "expected first content block to be code_execution_request",
      );
    }
    expect(request.id).toBe("gemini-exec-0");
    expect(request.code).toContain("fibonacci");

    const result = blocks[1];
    if (result?.type !== "code_execution_result") {
      throw new Error(
        "expected second content block to be code_execution_result",
      );
    }
    expect(result.requestId).toBe("gemini-exec-0");
    expect(result.status).toBe("ok");
    expect(result.stdout).toContain("6765");

    const text = blocks[2];
    if (text?.type !== "text") {
      throw new Error("expected third content block to be text");
    }
    expect(text.text).toContain("Fibonacci");
  });

  test("harness accumulates code_execution.delta fragments into the final request block (synthetic adapter)", async () => {
    // Gemini delivers `executableCode` atomically, so no fixture covers
    // code_execution.delta; this drives the harness's delta path with a
    // hand-built sequence through a synthetic adapter.
    //
    // The adapter carries a per-call queue and yields one event per
    // parseResponse invocation, preserving the strict `InferenceEvent`
    // union typing (arktype's inferred type is broader than the union).
    const eventQueue: InferenceEvent[] = [
      {
        type: "inference.code_execution.start",
        seq: 0,
        data: {
          request: {
            type: "code_execution_request",
            id: "synth-1",
            code: "",
            language: "PYTHON",
          },
          index: 0,
        },
      },
      {
        type: "inference.code_execution.delta",
        seq: 0,
        data: { requestId: "synth-1", codeFragment: "print(", index: 0 },
      },
      {
        type: "inference.code_execution.delta",
        seq: 0,
        data: { requestId: "synth-1", codeFragment: "'hi')", index: 0 },
      },
      {
        type: "inference.code_execution.result",
        seq: 0,
        data: {
          result: {
            type: "code_execution_result",
            requestId: "synth-1",
            status: "ok",
            stdout: "hi\n",
            providerOutcome: "OUTCOME_OK",
          },
          index: 1,
        },
      },
      {
        type: "inference.usage",
        seq: 0,
        data: {
          usage: {
            input: 1,
            output: 1,
            cacheRead: 0,
            cacheWrite: 0,
            thinking: 0,
          },
          source: TEST_SOURCE,
        },
      },
    ];
    const syntheticAdapter: ProviderAdapter = {
      buildRequest: () => ({
        url: "/synthetic",
        headers: {},
        body: JSON.stringify({}),
      }),
      parseJSONResponse: () => [],
      // Each SSE frame carries a queue index; the parser returns that
      // strictly-typed event, so no narrowing is needed.
      parseResponse: (sseData) => {
        const parsed: unknown = JSON.parse(sseData);
        if (typeof parsed !== "string") {
          throw new Error(
            `synthetic frame payload must be a string queue index`,
          );
        }
        const idx = Number.parseInt(parsed, 10);
        const ev = eventQueue[idx];
        if (ev === undefined) {
          throw new Error(
            `synthetic queue has no event at index ${String(idx)}`,
          );
        }
        return [ev];
      },
    };

    // One frame per queued event; the payload is the queue index.
    const sseChunks = eventQueue
      .map((_, i) => `data: ${JSON.stringify(String(i))}\n\n`)
      .join("");
    const sseBytes = new TextEncoder().encode(sseChunks);

    const fetchImpl: Dependencies["fetch"] = () =>
      Promise.resolve(
        new Response(
          new ReadableStream({
            start(controller) {
              controller.enqueue(sseBytes);
              controller.close();
            },
          }),
          {
            status: 200,
            headers: { "content-type": "text/event-stream" },
          },
        ),
      );

    // Unique provider id keeps the synthetic adapter from disturbing the
    // built-ins.
    const adapters = await loadAdapterRegistry(
      [{ provider: "synthetic-code-exec", specifier: "x", export: "make" }],
      { import: () => Promise.resolve({ make: () => syntheticAdapter }) },
    );

    let seq = 0;
    const events: InferenceEvent[] = [];
    for await (const ev of runInference({
      readMaterial: () => ({ secret: "test-secret" }),
      turns: [
        {
          role: "user",
          content: [{ type: "text", text: "go" }],
          timestamp: 0,
        },
      ],
      source: {
        id: "synthetic-code-exec:test",
        provider: "synthetic-code-exec",
        baseURL: "https://example.invalid",
        credentialId: "test",
        model: "test",
      },
      nextSeq: () => seq++,
      deps: { fetch: fetchImpl, scheduler: inertScheduler, adapters },
    })) {
      events.push(ev);
    }

    const done = events.find((e) => e.type === "inference.done");
    if (done?.type !== "inference.done") {
      throw new Error("expected inference.done event");
    }

    const request = done.data.turn.content.find(
      (b) => b.type === "code_execution_request",
    );
    if (request?.type !== "code_execution_request") {
      throw new Error("expected code_execution_request in turn content");
    }
    // The harness accumulated the two deltas into the final code block.
    expect(request.code).toBe("print('hi')");
    expect(request.id).toBe("synth-1");
  });
});

describe("Google GenAI adapter: tool-name codec round-trip", () => {
  const PREFIXED = "@intx/tools-posix/sidecar-bundle:run_shell";
  const ToolsDecl = type({
    tools: type({
      functionDeclarations: type({ name: "string" }).array(),
    }).array(),
  });

  function wireToolName(body: string): string {
    const parsed = ToolsDecl(JSON.parse(body));
    if (parsed instanceof type.errors) {
      throw new Error(`unexpected request body shape: ${parsed.summary}`);
    }
    const name = parsed.tools[0]?.functionDeclarations[0]?.name;
    if (name === undefined) throw new Error("request body carried no tool");
    return name;
  }

  function requestWithTool(): string {
    return adapter.buildRequest(
      [{ role: "user", content: [{ type: "text", text: "hi" }], timestamp: 0 }],
      "gemini-2.5-flash",
      {
        tools: [
          {
            name: PREFIXED,
            description: "run a shell command",
            inputSchema: {},
          },
        ],
      },
    ).body;
  }

  test("buildRequest encodes a package-qualified tool name to the wire charset", () => {
    const body = requestWithTool();
    const wireName = wireToolName(body);
    expect(wireName).toMatch(/^[A-Za-z_][A-Za-z0-9_-]*$/);
    expect(body).not.toContain(PREFIXED);
  });

  test("a functionCall echoing the encoded name decodes back to the prefixed name", async () => {
    const wireName = wireToolName(requestWithTool());
    const events = await parseWire(adapter, [
      sseFrame({
        candidates: [
          {
            content: {
              role: "model",
              parts: [{ functionCall: { name: wireName, args: {} } }],
            },
            finishReason: "STOP",
            index: 0,
          },
        ],
        usageMetadata: {
          promptTokenCount: 5,
          candidatesTokenCount: 3,
          totalTokenCount: 8,
        },
      }),
    ]);
    const start = events[0];
    if (start?.type !== "inference.tool_call.start") {
      throw new Error("expected inference.tool_call.start");
    }
    expect(start.data.name).toBe(PREFIXED);
  });
});

describe("Google GenAI adapter: quirks", () => {
  test("constructs with an absent quirks bag", () => {
    expect(() => createGoogleGenAIAdapter(TEST_SOURCE)).not.toThrow();
  });

  test("constructs with an explicit undefined quirks bag", () => {
    expect(() =>
      createGoogleGenAIAdapter(TEST_SOURCE, undefined),
    ).not.toThrow();
  });

  test("constructs with an empty quirks bag", () => {
    expect(() => createGoogleGenAIAdapter(TEST_SOURCE, {})).not.toThrow();
  });

  test("rejects a populated quirks bag since it declares no quirks", () => {
    expect(() =>
      createGoogleGenAIAdapter(TEST_SOURCE, { anything: true }),
    ).toThrow(/invalid quirks/);
  });
});

describe("createGoogleGenAIAdapter — parseJSONResponse (non-streaming)", () => {
  const JSON_SOURCE: InferenceSource = {
    id: "google-genai:gemini-test",
    provider: "google-genai",
    baseURL: "https://generativelanguage.googleapis.com",
    credentialId: "test",
    model: "gemini-test",
  };

  const inertScheduler: Scheduler = {
    setTimeout: () => () => {
      /* no timers */
    },
    now: () => 0,
  };

  // Drives a response body through the real harness accumulator; the
  // content-type selects the JSON-vs-SSE decode path. Only the
  // accumulated turn matches across paths, not the raw events.
  async function driveTurn(
    body: string,
    contentType = "application/json",
  ): Promise<{ turn: AssistantTurn | undefined; events: InferenceEvent[] }> {
    const deps: Dependencies = {
      fetch: () =>
        Promise.resolve(
          new Response(body, {
            status: 200,
            headers: { "content-type": contentType },
          }),
        ),
      scheduler: inertScheduler,
      adapters: createBuiltinRegistry(),
    };
    let seq = 0;
    const events: InferenceEvent[] = [];
    for await (const ev of runInference({
      readMaterial: () => ({ secret: "test-secret" }),
      turns: [
        { role: "user", content: [{ type: "text", text: "hi" }], timestamp: 0 },
      ],
      source: JSON_SOURCE,
      nextSeq: () => seq++,
      deps,
    })) {
      events.push(ev);
    }
    const done = events.find(
      (e): e is Extract<InferenceEvent, { type: "inference.done" }> =>
        e.type === "inference.done",
    );
    return { turn: done?.data.turn, events };
  }

  function blocksOfType<T extends ContentBlock["type"]>(
    turn: AssistantTurn,
    blockType: T,
  ): Extract<ContentBlock, { type: T }>[] {
    return turn.content.filter(
      (b): b is Extract<ContentBlock, { type: T }> => b.type === blockType,
    );
  }

  function requireTurn(turn: AssistantTurn | undefined): AssistantTurn {
    if (turn === undefined) throw new Error("expected an inference.done turn");
    return turn;
  }

  const USAGE = {
    promptTokenCount: 14,
    candidatesTokenCount: 7,
    thoughtsTokenCount: 3,
  };

  function completion(parts: object[], extra: object = {}): string {
    return JSON.stringify({
      candidates: [
        { content: { role: "model", parts }, finishReason: "STOP", index: 0 },
      ],
      usageMetadata: USAGE,
      modelVersion: "gemini-test",
      responseId: "resp-1",
      ...extra,
    });
  }

  test("decodes plain text and usage, including thinking tokens", async () => {
    const { turn, events } = await driveTurn(
      completion([{ text: "The capital of France is Paris." }]),
    );
    const t = requireTurn(turn);
    expect(blocksOfType(t, "text").map((b) => b.text)).toEqual([
      "The capital of France is Paris.",
    ]);
    const done = events.find(
      (e): e is Extract<InferenceEvent, { type: "inference.done" }> =>
        e.type === "inference.done",
    );
    expect(done?.data.usage.input).toBe(14);
    expect(done?.data.usage.output).toBe(7);
    expect(done?.data.usage.thinking).toBe(3);
  });

  test("decodes a functionCall part into a tool call with parsed arguments", async () => {
    const t = requireTurn(
      (
        await driveTurn(
          completion([
            {
              functionCall: {
                name: "get_weather",
                args: { location: "Boston" },
              },
            },
          ]),
        )
      ).turn,
    );
    const calls = blocksOfType(t, "tool_call");
    expect(calls).toHaveLength(1);
    const call = calls[0];
    if (call === undefined) throw new Error("expected a tool call");
    expect(call.name).toBe("get_weather");
    expect(call.arguments).toEqual({ location: "Boston" });
  });

  test("decodes a thinking part carrying its own thoughtSignature", async () => {
    const t = requireTurn(
      (
        await driveTurn(
          completion([
            { text: "reasoning", thought: true, thoughtSignature: "sig-1" },
            { text: "answer" },
          ]),
        )
      ).turn,
    );
    const thinking = blocksOfType(t, "thinking");
    expect(thinking).toHaveLength(1);
    expect(thinking[0]?.thinking).toBe("reasoning");
    expect(thinking[0]?.signature).toBe("sig-1");
    expect(blocksOfType(t, "text").map((b) => b.text)).toEqual(["answer"]);
  });

  test("attaches a thoughtSignature riding a functionCall part to the tool_call block", async () => {
    // The signed functionCall part authenticates its tool_call block,
    // not the preceding thinking block -- here in one JSON parts array.
    const t = requireTurn(
      (
        await driveTurn(
          completion([
            { text: "reasoning", thought: true },
            {
              functionCall: { name: "get_weather", args: { location: "SF" } },
              thoughtSignature: "sig-carrier",
            },
          ]),
        )
      ).turn,
    );
    const thinking = blocksOfType(t, "thinking");
    expect(thinking).toHaveLength(1);
    expect(thinking[0]?.signature).toBeUndefined();
    const calls = blocksOfType(t, "tool_call");
    expect(calls).toHaveLength(1);
    expect(calls[0]?.signature).toBe("sig-carrier");
  });

  test("decodes an executableCode + codeExecutionResult pair", async () => {
    const t = requireTurn(
      (
        await driveTurn(
          completion([
            { executableCode: { language: "PYTHON", code: "print(1)" } },
            { codeExecutionResult: { outcome: "OUTCOME_OK", output: "1\n" } },
          ]),
        )
      ).turn,
    );
    expect(blocksOfType(t, "code_execution_request")).toHaveLength(1);
    expect(blocksOfType(t, "code_execution_result")).toHaveLength(1);
  });

  test("decodes an inlineData image part", async () => {
    const t = requireTurn(
      (
        await driveTurn(
          completion([
            { inlineData: { mimeType: "image/png", data: "aGVsbG8=" } },
          ]),
        )
      ).turn,
    );
    expect(blocksOfType(t, "image")).toHaveLength(1);
  });

  test("decodes a promptFeedback.blockReason body into a safety rating and usage", async () => {
    const body = JSON.stringify({
      promptFeedback: { blockReason: "PROHIBITED_CONTENT" },
      usageMetadata: USAGE,
    });
    const { turn, events } = await driveTurn(body);
    const t = requireTurn(turn);
    expect(blocksOfType(t, "safety_rating")).toHaveLength(1);
    expect(blocksOfType(t, "safety_rating")[0]?.blockReason).toBe(
      "PROHIBITED_CONTENT",
    );
    const done = events.find(
      (e): e is Extract<InferenceEvent, { type: "inference.done" }> =>
        e.type === "inference.done",
    );
    expect(done?.data.usage.input).toBe(14);
  });

  test("rejects a non-terminal body carrying no finishReason", async () => {
    // A candidate with parts but no finishReason is a truncated capture;
    // the terminality guard surfaces it.
    const body = JSON.stringify({
      candidates: [{ content: { role: "model", parts: [{ text: "x" }] } }],
      usageMetadata: USAGE,
    });
    const { events } = await driveTurn(body);
    const error = events.find(
      (e): e is Extract<InferenceEvent, { type: "inference.error" }> =>
        e.type === "inference.error",
    );
    if (error === undefined) throw new Error("expected inference.error");
    expect(error.data.error.category).toBe("protocol_mismatch");
  });

  test("rejects a body with no candidates and no terminal signal", async () => {
    const body = JSON.stringify({ modelVersion: "gemini-test" });
    const { events } = await driveTurn(body);
    const error = events.find(
      (e): e is Extract<InferenceEvent, { type: "inference.error" }> =>
        e.type === "inference.error",
    );
    if (error === undefined) throw new Error("expected inference.error");
    expect(error.data.error.category).toBe("protocol_mismatch");
  });

  test("rejects a multi-candidate body", async () => {
    const body = JSON.stringify({
      candidates: [
        {
          content: { role: "model", parts: [{ text: "a" }] },
          finishReason: "STOP",
          index: 0,
        },
        {
          content: { role: "model", parts: [{ text: "b" }] },
          finishReason: "STOP",
          index: 1,
        },
      ],
      usageMetadata: USAGE,
    });
    const { events } = await driveTurn(body);
    const error = events.find(
      (e): e is Extract<InferenceEvent, { type: "inference.error" }> =>
        e.type === "inference.error",
    );
    if (error === undefined) throw new Error("expected inference.error");
    expect(error.data.error.category).toBe("protocol_mismatch");
  });
});

describe("createGoogleGenAIAdapter — streaming vs non-streaming parity", () => {
  const JSON_SOURCE: InferenceSource = {
    id: "google-genai:gemini-test",
    provider: "google-genai",
    baseURL: "https://generativelanguage.googleapis.com",
    credentialId: "test",
    model: "gemini-test",
  };
  const inertScheduler: Scheduler = {
    setTimeout: () => () => {
      /* no timers */
    },
    now: () => 0,
  };
  async function driveTurn(
    body: string,
    contentType: string,
  ): Promise<{ turn: AssistantTurn | undefined; events: InferenceEvent[] }> {
    const deps: Dependencies = {
      fetch: () =>
        Promise.resolve(
          new Response(body, {
            status: 200,
            headers: { "content-type": contentType },
          }),
        ),
      scheduler: inertScheduler,
      adapters: createBuiltinRegistry(),
    };
    let seq = 0;
    const events: InferenceEvent[] = [];
    for await (const ev of runInference({
      readMaterial: () => ({ secret: "test-secret" }),
      turns: [
        { role: "user", content: [{ type: "text", text: "hi" }], timestamp: 0 },
      ],
      source: JSON_SOURCE,
      nextSeq: () => seq++,
      deps,
    })) {
      events.push(ev);
    }
    const done = events.find(
      (e): e is Extract<InferenceEvent, { type: "inference.done" }> =>
        e.type === "inference.done",
    );
    return { turn: done?.data.turn, events };
  }
  function geminiSSE(events: object[]): string {
    return events.map((e) => `data: ${JSON.stringify(e)}\n\n`).join("");
  }

  // Split a text block across two SSE events vs one JSON part, plus a
  // functionCall: the accumulator must reconcile both paths to identical
  // turn content and usage even though the raw event arrays differ.
  test("split-across-events streaming and single-part JSON decode to the same turn", async () => {
    const usage = {
      promptTokenCount: 20,
      candidatesTokenCount: 10,
      thoughtsTokenCount: 2,
    };

    const jsonBody = JSON.stringify({
      candidates: [
        {
          content: {
            role: "model",
            parts: [
              { text: "Hello world" },
              {
                functionCall: {
                  name: "get_weather",
                  args: { location: "Boston" },
                },
              },
            ],
          },
          finishReason: "STOP",
          index: 0,
        },
      ],
      usageMetadata: usage,
    });

    const streamBody = geminiSSE([
      {
        candidates: [
          { content: { role: "model", parts: [{ text: "Hello " }] }, index: 0 },
        ],
      },
      {
        candidates: [
          { content: { role: "model", parts: [{ text: "world" }] }, index: 0 },
        ],
      },
      {
        candidates: [
          {
            content: {
              role: "model",
              parts: [
                {
                  functionCall: {
                    name: "get_weather",
                    args: { location: "Boston" },
                  },
                },
              ],
            },
            index: 0,
          },
        ],
      },
      {
        candidates: [
          {
            content: { role: "model", parts: [] },
            finishReason: "STOP",
            index: 0,
          },
        ],
        usageMetadata: usage,
      },
    ]);

    const jsonResult = await driveTurn(jsonBody, "application/json");
    const streamResult = await driveTurn(streamBody, "text/event-stream");

    expect(jsonResult.events.some((e) => e.type === "inference.error")).toBe(
      false,
    );
    expect(streamResult.events.some((e) => e.type === "inference.error")).toBe(
      false,
    );

    const jt = jsonResult.turn;
    const st = streamResult.turn;
    if (jt === undefined || st === undefined) {
      throw new Error("both paths must produce a turn");
    }
    expect(jt.content).toEqual(st.content);

    const jdone = jsonResult.events.find(
      (e): e is Extract<InferenceEvent, { type: "inference.done" }> =>
        e.type === "inference.done",
    );
    const sdone = streamResult.events.find(
      (e): e is Extract<InferenceEvent, { type: "inference.done" }> =>
        e.type === "inference.done",
    );
    expect(jdone?.data.usage).toEqual(sdone?.data.usage);
  });

  // The deferred thoughtSignature thread and grounding citations must
  // reconcile across the split-vs-single boundary, not just decode on
  // the JSON path alone.
  test("deferred signature and grounding reconcile across split streaming and single JSON", async () => {
    const usage = {
      promptTokenCount: 30,
      candidatesTokenCount: 12,
      thoughtsTokenCount: 5,
    };
    const grounding = {
      groundingChunks: [{ web: { uri: "https://a.example", title: "A" } }],
      groundingSupports: [
        {
          segment: { text: "Paris", startIndex: 0, endIndex: 5 },
          groundingChunkIndices: [0],
        },
      ],
    };

    const jsonBody = JSON.stringify({
      candidates: [
        {
          content: {
            role: "model",
            parts: [
              { text: "let me think", thought: true, thoughtSignature: "sig" },
              { text: "The capital is Paris." },
            ],
          },
          groundingMetadata: grounding,
          finishReason: "STOP",
          index: 0,
        },
      ],
      usageMetadata: usage,
    });

    const streamBody = geminiSSE([
      {
        candidates: [
          {
            content: {
              role: "model",
              parts: [{ text: "let me ", thought: true }],
            },
            index: 0,
          },
        ],
      },
      {
        candidates: [
          {
            content: {
              role: "model",
              parts: [
                { text: "think", thought: true, thoughtSignature: "sig" },
              ],
            },
            index: 0,
          },
        ],
      },
      {
        candidates: [
          {
            content: { role: "model", parts: [{ text: "The capital " }] },
            index: 0,
          },
        ],
      },
      {
        candidates: [
          {
            content: { role: "model", parts: [{ text: "is Paris." }] },
            index: 0,
          },
        ],
      },
      {
        candidates: [
          {
            content: { role: "model", parts: [] },
            groundingMetadata: grounding,
            finishReason: "STOP",
            index: 0,
          },
        ],
        usageMetadata: usage,
      },
    ]);

    const j = await driveTurn(jsonBody, "application/json");
    const s = await driveTurn(streamBody, "text/event-stream");

    expect(j.events.some((e) => e.type === "inference.error")).toBe(false);
    expect(s.events.some((e) => e.type === "inference.error")).toBe(false);

    if (j.turn === undefined || s.turn === undefined) {
      throw new Error("both paths must produce a turn");
    }
    expect(j.turn.content).toEqual(s.turn.content);

    // Citation payloads match (block index + data); the streaming path
    // emits more deltas, so seq is not compared.
    const citations = (evs: InferenceEvent[]) =>
      evs.filter((e) => e.type === "inference.citation").map((e) => e.data);
    expect(citations(j.events)).toEqual(citations(s.events));
    expect(citations(j.events)).toHaveLength(1);
  });

  test("parseJSONResponse mints fresh parser state per call", () => {
    // Two calls on the SAME adapter: leaked block-index state from the
    // first call would offset the second call's indices. Each call must
    // start from fresh state.
    const parseJSON = adapter.parseJSONResponse;
    if (parseJSON === undefined) {
      throw new Error("expected the adapter to implement parseJSONResponse");
    }
    const body = JSON.stringify({
      candidates: [
        {
          content: {
            role: "model",
            parts: [{ text: "reasoning", thought: true }, { text: "answer" }],
          },
          finishReason: "STOP",
          index: 0,
        },
      ],
      usageMetadata: { promptTokenCount: 4, candidatesTokenCount: 1 },
    });
    const thinkingIndex = (evs: InferenceEvent[]): number => {
      const delta = evs.find(
        (
          e,
        ): e is Extract<InferenceEvent, { type: "inference.thinking.delta" }> =>
          e.type === "inference.thinking.delta",
      );
      if (delta === undefined) throw new Error("expected a thinking delta");
      const idx = delta.data.index;
      if (idx === undefined) throw new Error("thinking delta carried no index");
      return idx;
    };
    expect(thinkingIndex(parseJSON(body))).toBe(0);
    expect(thinkingIndex(parseJSON(body))).toBe(0);
  });
});
