// Tests for the Gemini (`google-genai`) provider adapter. Coverage
// splits into three layers:
//
//   - buildRequest shape assertions, including byte-for-byte
//     fixture parity for plain-text and function-calling-multi-turn
//   - parseResponse per-event behavior and end-to-end fixture replay
//     of the plain-text-streaming SSE capture
//   - a harness-level round trip via `runInference` that asserts the
//     accumulated `PartialMessage` and final `inference.done` turn
//     line up with the parser's emissions
//
// The fixtures live in `packages/inference-testing/wire/google-genai`
// and were captured against live Gemini endpoints; any drift between
// adapter output and fixture is a real protocol mismatch.

import { readFileSync } from "node:fs";
import { join } from "node:path";

import { type } from "arktype";
import { beforeEach, describe, expect, test } from "bun:test";

import {
  CREDENTIAL_SENTINEL,
  createGoogleGenAIAdapter,
  parseSSE,
  ProtocolMismatchError,
  runInference,
  type Dependencies,
  type ProviderAdapter,
  type Scheduler,
} from "@intx/inference";
import type {
  ConversationTurn,
  InferenceEvent,
  InferenceSource,
} from "@intx/types/runtime";

const FIXTURE_ROOT = join(
  import.meta.dir,
  "..",
  "..",
  "..",
  "packages",
  "inference-testing",
  "wire",
  "google-genai",
);

// Permissive top-level body schema. Each Gemini-recognized key is
// declared as `unknown` so per-test narrowing schemas can validate
// just the slice they care about; the alternative (a single deeply-
// typed body schema) would pin every field shape and turn schema
// edits into a churn surface unrelated to the assertion being made.
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

// Captured fixture files share the same top-level body shape. Asserting
// against the same schema as `parseBody` lets test sites compare the
// two with `toEqual` without TypeScript widening one side back to
// `Record<string, unknown>`.
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

// A fresh adapter per test: matches the openai.test.ts pattern.
// Per-request parser state on this adapter is reset by construction,
// so re-creating per test guarantees no test sees state leaked from a
// prior one regardless of how the parser surface grows.
let adapter: ProviderAdapter;
beforeEach(() => {
  adapter = createGoogleGenAIAdapter();
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
    // Legitimate model names (alphanumerics, hyphens, periods) pass
    // through unchanged; the escape is a defensive no-op for current
    // catalog entries. A model value containing a URL-special
    // character (e.g. a future model name with a `/` or `?`) is
    // escaped at the path layer rather than producing a malformed URL.
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
    // The harness substitutes the sentinel with InferenceSource.apiKey
    // at send time; the adapter must emit the sentinel verbatim so the
    // harness can find it.
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
    // The rest of the adapter throws on every unsupported block kind
    // rather than silently dropping content; the system-turn
    // extraction holds the same discipline. A caller who puts an
    // image into a system turn gets a clear error instead of a
    // request shape that omits part of what they sent.
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

    // Tools live under a single functionDeclarations wrapper, not as a
    // flat list -- the wrapper is how Gemini groups multiple declarations
    // alongside built-in tools (googleSearch, codeExecution).
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

  test("thinking.enabled=false emits thinkingConfig.thinkingBudget=0", () => {
    // Gemini 2.5's default thinking budget is NOT zero, so disabling
    // thinking requires an explicit zero rather than just omitting
    // thinkingConfig. Mirrors the discovery-side plainTextStreaming
    // capture shape.
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
    // Free-form text is Gemini's default; the adapter must not set
    // responseMimeType (which would otherwise pin the output) when the
    // caller asked for plain text. With no other generationConfig
    // fields populated, the whole object is omitted from the request.
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
    // Gemini has no responseSchema-level name or strict-mode toggle.
    // The caller may still supply both for cross-provider portability;
    // the adapter must not forward them and must not error on their
    // presence.
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
    // The MediaSource url variant maps to Gemini's fileData/fileUri:
    // Gemini accepts public HTTP(S) URLs in the same field the Files
    // API uses for uploaded-file URIs.
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
// The MediaSource variants are covered structurally in the block above;
// these tests pin the BYTE-FOR-BYTE body match between what the adapter
// produces for a `ConversationTurn` carrying a media block and what
// was captured on the wire when the same prompt was issued against the
// live Gemini endpoint. A regression in part ordering, optional-key
// emission, or field naming surfaces here as a fixture mismatch.
//
// The base64 in each fixture is read from disk and threaded back into
// the input turn so the test exercises the SHAPE translation, not
// arbitrary bytes; the data round-trips through the adapter unchanged.

describe("Google GenAI adapter: multimodal input fixture parity", () => {
  // Fixture payloads are validated with arktype rather than narrowed
  // via type assertion -- the fixtures are external data, so the
  // runtime schema is the honest way to pull `{mimeType, data}` and
  // `{mimeType, fileUri}` out of them.
  const FixtureInlineDataPart = type({
    inlineData: { mimeType: "string", data: "string" },
  });
  const FixtureFileDataPart = type({
    fileData: { mimeType: "string", fileUri: "string" },
  });

  // Drill into the second part of a fixture's first user turn. The
  // captured fixtures all place the prompt text at parts[0] and the
  // media part at parts[1]; reaching past either bound throws
  // explicitly so a future capture with a different shape surfaces
  // its mismatch at the indexing site rather than as an arktype
  // schema error against undefined (`?.` chaining would silently
  // forward undefined into the validator, masking the structural
  // problem as a schema mismatch).
  function mediaPartOf(fixture: typeof GeminiBody.infer): unknown {
    const contents = GeminiContents.assert(fixture.contents);
    const firstTurn = contents[0];
    if (firstTurn === undefined) {
      throw new Error("fixture contents[] is empty");
    }
    // The captured fixtures are all single-turn user prompts. A
    // model-role first turn would mean the capture started from a
    // priming response or a wholly different shape; either way it
    // is not what this helper is designed to read, so fail loudly
    // rather than silently return that turn's `parts[1]` and let a
    // downstream byte-equality assertion produce a confusing diff.
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

  // Read a fixture once and pull its inlineData payload out of it,
  // returning both the parsed body and the destructured payload so
  // a test site can equality-check the body and thread the payload
  // back into its input turn without re-parsing the file.
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
    // The Files API capture uses Gemini's `fileData/fileUri` shape
    // (the same field the URL variant targets); the URI here is the
    // upload endpoint's returned handle, not a public URL. The
    // adapter does not distinguish the two on the wire -- both
    // `file-reference` and `url` MediaSources land in `fileData`.
    const fixture = readFixtureJSON(
      "gemini-2.5-flash",
      "files-api-reference-streaming",
      "generate",
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
    // The full three-turn conversation as the harness would assemble
    // it from the prior assistant turn's tool_call and the user's
    // tool_result. The functionResponse.name comes from the callId ->
    // name lookup built over prior turns.
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
      "turn-2",
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
    // Defensive: only JSON *objects* get used verbatim. Arrays, scalars,
    // and null fall through to the wrap path so the wire shape is
    // predictable regardless of what the tool returned.
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
    // Internal types do not enforce role/block pairing on their own.
    // The adapter's marshaling boundary catches a misrouted tool_call
    // (a tool_call placed on a user turn) so a caller bug surfaces
    // with diagnostic context rather than reaching Gemini as an
    // opaque 400.
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
    // Symmetric to the tool_call check above: tool_result lives on
    // user turns; on an assistant turn it is a caller bug.
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
    // Object.assign writes undefined values through, and JSON.stringify
    // then drops them from the wire payload. A caller passing
    // `providerOptions: { generationConfig: undefined }` therefore
    // erases the adapter-built generationConfig (including the
    // thinkingConfig built from `options.thinking`). This is the
    // standard JS spread/Object.assign semantic, but pinning it here
    // means a future refactor that filters undefined values out (e.g.
    // to "fix" what looks like an accidental drop) breaks this test
    // and surfaces the behavior change loudly. The same rule applies
    // to providerOptions on every other adapter -- the caller owns
    // what they pass.
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
    // Documented behavior: providerOptions is a shallow merge into the
    // top level of the request body. A caller passing a structured key
    // like `generationConfig` wholesale replaces the object the adapter
    // built. This test pins the clobber semantics so a future
    // "helpful" deep-merge refactor breaks it loudly.
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

    // providerOptions.generationConfig fully replaces the
    // adapter-built one -- no thinkingConfig survives the merge.
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

// Frames a single JSON object as one SSE event (one `data:` line +
// terminating blank line). Mirrors what the Gemini endpoint emits per
// SSE event, so synthetic events can be driven through the same
// parseSSE -> parseResponse pipeline as the captured fixtures.
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
    // A single candidate.content.parts[] with two text entries
    // produces two text.delta events in the order parts appear,
    // both at index 0 (single logical block for plain text).
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
    // Every Gemini SSE event carries cumulative usageMetadata, but
    // the parser emits usage only at the terminal event. The harness's
    // inference.done captures the final usage snapshot via the single
    // emission; intermediate emissions would be pure noise.
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
    // The captured plain-text-streaming response.sse has 8 SSE
    // events; the last carries finishReason: "STOP" and the
    // cumulative usage snapshot. The parser is expected to produce
    // exactly 8 inference.text.delta events (the new tokens from
    // each event, in order) followed by exactly 1 inference.usage
    // event with the cumulative counts from the final event.
    const sseBytes = readFileSync(
      join(
        FIXTURE_ROOT,
        "gemini-2.5-flash",
        "plain-text-streaming",
        "response.sse",
      ),
    );
    const events = await parseWire(adapter, [sseBytes]);

    const textEvents = events.filter((e) => e.type === "inference.text.delta");
    const usageEvents = events.filter((e) => e.type === "inference.usage");
    expect(textEvents).toHaveLength(8);
    expect(usageEvents).toHaveLength(1);
    expect(events.length).toBe(9);
    // Usage is the last emission (per the inference.usage-before-
    // inference.done contract that the harness applies).
    expect(events[events.length - 1]?.type).toBe("inference.usage");

    // Final cumulative usage from the fixture: promptTokenCount=33,
    // candidatesTokenCount=281, totalTokenCount=314.
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
    // finishReason without usageMetadata would silently produce a
    // zero-usage tally; surface the malformed terminal event loudly
    // instead.
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
  };

  const SOURCE: InferenceSource = {
    id: "google-genai:gemini-2.5-flash",
    provider: "google-genai",
    baseURL: "https://generativelanguage.googleapis.com",
    apiKey: "test-key",
    model: "gemini-2.5-flash",
  };

  test("plain-text-streaming fixture flows through runInference end-to-end", async () => {
    // Replays the captured SSE response through the full harness
    // pipeline (parseSSE + parseResponse + partial-state
    // accumulation + inference.done emission). Asserts the
    // accumulated PartialMessage.text matches the concatenation of
    // every parsed text delta, and that the final inference.done
    // carries a turn with one text block whose text is the full
    // response and a usage that matches the wire's terminal
    // cumulative snapshot.
    const sseBytes = readFileSync(
      join(
        FIXTURE_ROOT,
        "gemini-2.5-flash",
        "plain-text-streaming",
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
      turns: [
        {
          role: "user",
          content: [{ type: "text", text: "Tell me about sailboats." }],
          timestamp: 0,
        },
      ],
      source: SOURCE,
      nextSeq: () => seq++,
      deps: { fetch: fetchImpl, scheduler: inertScheduler },
    })) {
      events.push(ev);
    }

    const done = events.find((e) => e.type === "inference.done");
    expect(done).toBeDefined();
    if (done?.type !== "inference.done") {
      throw new Error("expected inference.done event");
    }

    // The harness should produce a single text block whose text is
    // the full concatenated response. The exact prefix is captured
    // from the fixture's first SSE event so the assertion catches a
    // regression that drops the first chunk.
    expect(done.data.turn.content).toHaveLength(1);
    const first = done.data.turn.content[0];
    if (first?.type !== "text") {
      throw new Error("expected first content block to be text");
    }
    expect(first.text.startsWith("A sailboat harnesses the wind")).toBe(true);
    expect(first.text.endsWith("powered solely by the wind.")).toBe(true);

    // Usage should reflect the final cumulative snapshot from the
    // last SSE event.
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
    // Mirrors the function-calling-multi-turn-streaming/turn-1
    // wire shape: one SSE event, one functionCall part, finishReason
    // STOP, cumulative usageMetadata. The parser is expected to
    // synthesize a callId (Gemini has no wire-level id field) and
    // emit the args complete in a single delta (atomic; no JSON
    // streaming).
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

  test("thinking text followed by functionCall-with-signature in separate events pairs them positionally", async () => {
    // Mirrors function-calling-with-thinking-streaming/turn-1: a
    // thinking text part in one event, a functionCall part with
    // thoughtSignature in the next. The parser must emit the
    // signature event BEFORE the tool_call.start/delta pair so the
    // signature attaches to the thinking block's index and not to
    // the freshly opened tool_call block.
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
      "inference.thinking.signature",
      "inference.tool_call.start",
      "inference.tool_call.delta",
      "inference.usage",
    ]);

    const thinkingDelta = events[0];
    if (thinkingDelta?.type !== "inference.thinking.delta") {
      throw new Error("expected inference.thinking.delta");
    }
    expect(thinkingDelta.data.index).toBe(0);

    const signature = events[1];
    if (signature?.type !== "inference.thinking.signature") {
      throw new Error("expected inference.thinking.signature");
    }
    expect(signature.data.index).toBe(0);
    expect(signature.data.signature).toBe("OPAQUE_SIGNATURE");

    const toolStart = events[2];
    if (toolStart?.type !== "inference.tool_call.start") {
      throw new Error("expected inference.tool_call.start");
    }
    // Thinking is block 0; the tool_call must allocate the next
    // index (1), not collide with the thinking block.
    expect(toolStart.data.index).toBe(1);
    expect(toolStart.data.callId).toBe("1");

    const usage = events[4];
    if (usage?.type !== "inference.usage") {
      throw new Error("expected inference.usage");
    }
    expect(usage.data.usage).toEqual({
      input: 85,
      output: 15,
      cacheRead: 0,
      cacheWrite: 0,
      // thoughtsTokenCount=53 flows to TokenUsage.thinking. The
      // wire-up was already present in the parser; this test pins
      // it now that thinking is exercised.
      thinking: 53,
    });
  });

  test("thoughtSignature with no preceding thinking block throws ProtocolMismatchError", () => {
    const bad = JSON.stringify({
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
    });
    expect(() => adapter.parseResponse(bad)).toThrow(ProtocolMismatchError);
    expect(() => adapter.parseResponse(bad)).toThrow(
      /no preceding thinking block/,
    );
  });

  test("interleaved text and functionCall in one candidate allocate separate block indices", async () => {
    // No fixture exercises this shape -- Gemini's corpus is
    // text-only OR thinking+functionCall in practice -- but the
    // coalescing rules support it for free. Pin the behavior so a
    // future change to allocation doesn't drift.
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

    // The text-before block gets index 0; the functionCall block
    // gets index 1 (closing the text block); the text-after block
    // gets index 2 (the closed-then-reopened text block is a NEW
    // block, not a return to index 0). The rule is "consecutive
    // same-kind parts coalesce; different-kind closes the current
    // block and allocates a new one." Reopening with the same kind
    // after a different-kind interruption deliberately allocates a
    // fresh index because the wire semantics treat the spans as
    // distinct logical blocks.
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
    // The diagnostic must enumerate every payload that's set, not
    // just the first one detected -- the violation is "more than one
    // payload" and the user can only act on it if the error names
    // both.
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
    // arktype's open-object schema accepts `thought: true` on any
    // part shape; `assertSinglePayload` is the boundary that rejects
    // the flag on parts where it has no defined wire meaning. A
    // `thought` flag on a functionCall is one such wire violation.
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

  test("empty-text part bearing thoughtSignature claims the pending thinking anchor", async () => {
    // Pins the empty-text carrier path: a `text: ""` part with a
    // `thoughtSignature` lands the signature on the preceding
    // thinking block instead of silently evaporating. The empty-
    // payload-but-signature-present shape is spec-permitted and not
    // covered by the corpus, but the parser must handle it because
    // otherwise an authenticated thinking round-trip silently loses
    // its attestation.
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

    expect(events.map((e) => e.type)).toEqual([
      "inference.thinking.delta",
      "inference.thinking.signature",
    ]);
    const sig = events[1];
    if (sig?.type !== "inference.thinking.signature") {
      throw new Error("expected inference.thinking.signature");
    }
    expect(sig.data.signature).toBe("EMPTY_CARRIER_SIG");
    expect(sig.data.index).toBe(0);
  });

  test("signature-only part after a non-thinking currentBlock throws (no anchor to claim)", async () => {
    // After a text block, `currentBlock` is text and no thinking
    // anchor is pending. A signature-only part has nothing to
    // attach to and must throw. Verifies the close-then-consume
    // fix on the signature-only branch did NOT accidentally let a
    // stray signature succeed when no thinking precedes it.
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
    ).rejects.toThrow(/no preceding thinking block/);
  });

  test("signature-only part after thinking whose carrier already declined throws", async () => {
    // thinking → unsigned-text carrier → signature-only.
    // The text carrier had the anchor and declined to claim it
    // (settleCarrierOpportunity discards the anchor). A subsequent
    // signature-only part cannot retroactively claim the same
    // thinking block's anchor -- the carrier opportunity is gone.
    await expect(
      parseWire(adapter, [
        sseFrame({
          candidates: [
            {
              content: {
                role: "model",
                parts: [
                  { text: "reasoning", thought: true },
                  { text: "carrier" },
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
                parts: [{ thoughtSignature: "LATE" }],
              },
              index: 0,
            },
          ],
        }),
      ]),
    ).rejects.toThrow(/no preceding thinking block/);
  });

  test("signature-only part directly after thinking claims the pending anchor", async () => {
    // The signature-only carrier shape is the third reachable
    // payload-less variant (alongside empty-text-with-signature
    // and the corpus-seen functionCall-with-signature). Same
    // close-then-consume pattern: a still-open thinking block must
    // be closed so its index lands in `pendingSignatureAnchor`
    // before `consumeSignature` claims it. Without the close, the
    // anchor would still be null and consumeSignature would throw
    // "no preceding thinking block."
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
              parts: [{ thoughtSignature: "SIG_ONLY" }],
            },
            index: 0,
          },
        ],
      }),
    ]);

    expect(events.map((e) => e.type)).toEqual([
      "inference.thinking.delta",
      "inference.thinking.signature",
    ]);
    const sig = events[1];
    if (sig?.type !== "inference.thinking.signature") {
      throw new Error("expected inference.thinking.signature");
    }
    expect(sig.data.signature).toBe("SIG_ONLY");
    expect(sig.data.index).toBe(0);
  });

  test("unsigned non-thinking carrier between two thinking blocks does not trip the anchor guard", async () => {
    // After a thinking block closes, the FIRST non-thinking part is
    // the only carrier opportunity for that block's signature. A
    // carrier that passes without a signature ends the opportunity;
    // the anchor must be discarded so that a LATER thinking block
    // does not trip the "two thinking blocks closed" guard on a
    // stale anchor the first carrier already declined. The shape
    // (unsigned thinking → unsigned text → unsigned thinking →
    // functionCall) is spec-permitted and the parser must accept
    // it.
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

    // Block indices: thinking-A = 0, text = 1, thinking-B = 2,
    // functionCall = 3. The four parts allocate four distinct
    // indices because consecutive different-kind parts each close
    // the current block.
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
        "turn-1",
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

  test("with-thinking-streaming fixture replay pairs signature to the thinking block", async () => {
    const sseBytes = readFileSync(
      join(
        FIXTURE_ROOT,
        "gemini-2.5-flash",
        "function-calling-with-thinking-streaming",
        "turn-1",
        "response.sse",
      ),
    );
    const events = await parseWire(adapter, [sseBytes]);

    expect(events.map((e) => e.type)).toEqual([
      "inference.thinking.delta",
      "inference.thinking.signature",
      "inference.tool_call.start",
      "inference.tool_call.delta",
      "inference.usage",
    ]);

    const signature = events[1];
    if (signature?.type !== "inference.thinking.signature") {
      throw new Error("expected inference.thinking.signature");
    }
    // Signature attaches to the thinking block at index 0, NOT to
    // the tool_call block at index 1. Decoupling thinking-block
    // attachment from tool_call-block attachment is the point of
    // emitting the signature event before the tool_call.start.
    expect(signature.data.index).toBe(0);
    expect(signature.data.signature.length).toBeGreaterThan(0);

    const toolStart = events[2];
    if (toolStart?.type !== "inference.tool_call.start") {
      throw new Error("expected inference.tool_call.start");
    }
    expect(toolStart.data.index).toBe(1);

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

  test("signed thinking + tool_call attaches the signature to the functionCall part, not the thinking part", () => {
    // This is the round-trip shape captured in
    // function-calling-with-thinking-streaming/turn-2/request.json:
    // signature is on the functionCall, thinking text is signature-
    // less. A second turn echoing the model's prior thinking
    // requires this exact placement; mis-placing the signature
    // would cause Gemini to reject the request as a corrupted
    // thinking attestation.
    const req = adapter.buildRequest(
      [
        {
          role: "assistant",
          content: [
            {
              type: "thinking",
              thinking: "Determining weather query.",
              signature: "OPAQUE_SIGNATURE",
            },
            {
              type: "tool_call",
              id: "1",
              name: "getCurrentWeather",
              arguments: { location: "Boston, MA" },
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
    // A thinking block without a signature stands alone -- its
    // presence does not force a thoughtSignature on the next part.
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

  test("turn ending on a signed thinking block with no follow-on part throws", () => {
    expect(() =>
      adapter.buildRequest(
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
      ),
    ).toThrow(/signature awaiting a carrier part/);
  });

  test("two signed thinking blocks without an intervening non-thinking carrier throws", () => {
    expect(() =>
      adapter.buildRequest(
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
      ),
    ).toThrow(/second thinking block on assistant turn/);
  });

  test("turn-2 round-trip fixture parity for function-calling-with-thinking-streaming", () => {
    // The harness ought to be able to reconstruct turn-2's request
    // from a ConversationTurn list that includes the thinking +
    // tool_call + tool_result blocks. We assert byte-equivalent
    // parts -- a regression in the signature placement or in the
    // thinking text would break Gemini's signed-thinking
    // attestation on the next turn.
    const FIXTURE = readFixtureJSON(
      "gemini-2.5-flash",
      "function-calling-with-thinking-streaming",
      "turn-2",
      "request.json",
    );
    const fixtureParts = GeminiContents.assert(FIXTURE.contents)[1]?.parts;
    // Narrow the two fixture parts with arktype rather than a type
    // assertion -- the fixture file is external data and a runtime
    // schema is the honest way to extract `thinking` text and
    // `thoughtSignature`. A type assertion would be a compile-time
    // lie against the actual file contents.
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
              signature: thoughtSignature,
            },
            {
              type: "tool_call",
              id: "1",
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
    // The assistant turn is the second `contents[]` element. Pin
    // its parts byte-for-byte against the fixture; the surrounding
    // user turns are exercised by other tests.
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
  };

  const SOURCE: InferenceSource = {
    id: "google-genai:gemini-2.5-flash",
    provider: "google-genai",
    baseURL: "https://generativelanguage.googleapis.com",
    apiKey: "test-key",
    model: "gemini-2.5-flash",
  };

  test("function-calling-with-thinking-streaming fixture flows through runInference end-to-end", async () => {
    // Replays the captured SSE response through the full harness
    // pipeline and asserts the final turn carries a thinking block
    // (with its signature) followed by a tool_call block. The
    // ordering matters: a tool_call-before-thinking content array
    // could not be echoed back to Gemini in a follow-up turn
    // because Gemini's wire convention is thinking-then-functionCall.
    const sseBytes = readFileSync(
      join(
        FIXTURE_ROOT,
        "gemini-2.5-flash",
        "function-calling-with-thinking-streaming",
        "turn-1",
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
      deps: { fetch: fetchImpl, scheduler: inertScheduler },
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
    expect(thinking.signature).toBeDefined();
    expect(thinking.signature?.length).toBeGreaterThan(0);

    const toolCall = blocks[1];
    if (toolCall?.type !== "tool_call") {
      throw new Error("expected second content block to be tool_call");
    }
    expect(toolCall.name).toBe("getCurrentWeather");
    expect(toolCall.arguments).toEqual({ location: "Boston, MA" });

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
    // Pins the per-part block allocation for the image-output
    // shape: consecutive text parts coalesce into index 0, the
    // image is atomic at index 1, and trailing text reopens at
    // index 2 (a NEW logical block, not a return to index 0).
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

  test("thinking text then inlineData with thoughtSignature pairs the signature to the thinking block", async () => {
    // The inlineData carrier path mirrors the functionCall carrier
    // path: a thoughtSignature on the inlineData part settles
    // against the preceding thinking block via the pending anchor,
    // NOT against the newly-allocated image block. The signature
    // event must precede the image_output event so the harness's
    // per-index router lands the signature at the thinking block's
    // index rather than at the image's.
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
      "inference.thinking.signature",
      "inference.image_output",
      "inference.usage",
    ]);

    const sig = events[1];
    if (sig?.type !== "inference.thinking.signature") {
      throw new Error("expected inference.thinking.signature");
    }
    expect(sig.data.signature).toBe("IMG_CARRIER_SIG");
    expect(sig.data.index).toBe(0);

    const image = events[2];
    if (image?.type !== "inference.image_output") {
      throw new Error("expected inference.image_output");
    }
    expect(image.data.index).toBe(1);
  });

  test("inlineData with thoughtSignature but no preceding thinking throws", () => {
    const bad = JSON.stringify({
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
    });
    expect(() => adapter.parseResponse(bad)).toThrow(ProtocolMismatchError);
    expect(() => adapter.parseResponse(bad)).toThrow(
      /no preceding thinking block/,
    );
  });

  test("inlineData with a non-image MIME throws ProtocolMismatchError", () => {
    // The parser wraps inlineData as a base64 ImageBlock; an audio
    // or document MIME would silently mistype the payload as an
    // image. Reject at the boundary rather than produce a
    // confidently-wrong ContentBlock.
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
    // The captured response.sse delivers text "Here" + " you go: " in
    // events 0-1 (coalesce into one text block at index 0), the
    // complete image as a single inlineData part in event 2 (index 1),
    // and a final empty-text STOP event with cumulative usage. The
    // empty-text part emits no delta but does carry the terminal
    // finishReason.
    const sseBytes = readFileSync(
      join(
        FIXTURE_ROOT,
        "gemini-2.5-flash-image",
        "image-output-streaming",
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
    // The captured fixture's base64 is ~380KB. The parser passes the
    // bytes through verbatim; elision is the logger's concern.
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
  };

  const SOURCE: InferenceSource = {
    id: "google-genai:gemini-2.5-flash-image",
    provider: "google-genai",
    baseURL: "https://generativelanguage.googleapis.com",
    apiKey: "test-key",
    model: "gemini-2.5-flash-image",
  };

  test("image-output-streaming fixture replay produces a final turn with text then ImageBlock", async () => {
    // Proves the parser + harness wire image_output end-to-end: the
    // final turn's content[] must contain the ImageBlock with the
    // full base64 payload intact. Without the harness's
    // image_output case handler, this assertion would fail with the
    // single text block alone and the image would silently drop
    // from replay; with the handler, both blocks land in arrival
    // order.
    const sseBytes = readFileSync(
      join(
        FIXTURE_ROOT,
        "gemini-2.5-flash-image",
        "image-output-streaming",
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
      deps: { fetch: fetchImpl, scheduler: inertScheduler },
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

    // The harness also yields a mid-stream inference.image_output
    // event that consumers can subscribe to without waiting for
    // inference.done. Verifying it lands in the event stream guards
    // against a regression where the harness's case handler is
    // removed or silently broken.
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

    // Both citations carry identical citedText and textOffset --
    // the support's segment -- but distinct sources from the
    // chunks it references.
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
    // Citations belong to the model's output and should precede
    // the bookkeeping signal that closes the response. The harness
    // relies on this ordering for the final-walk's
    // post-block-emission interleave; a citation arriving after
    // its block emission would land in the orphan-citation check.
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
    expect(() => createGoogleGenAIAdapter().parseResponse(bad)).toThrow(
      ProtocolMismatchError,
    );
    expect(() => createGoogleGenAIAdapter().parseResponse(bad)).toThrow(
      /without a current text block/,
    );
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
    expect(() => createGoogleGenAIAdapter().parseResponse(bad)).toThrow(
      ProtocolMismatchError,
    );
    expect(() => createGoogleGenAIAdapter().parseResponse(bad)).toThrow(
      /chunk index 99/,
    );
  });

  test("non-web chunk kinds are skipped without throwing", async () => {
    // A future grounding chunk that lacks the `web` shape has no
    // `uri`/`title` to populate `CitationSource`. The parser
    // silently drops it rather than synthesize a placeholder
    // citation. Supports referencing only non-web chunks emit
    // zero citations; supports referencing a mix emit citations
    // only for the web-shaped chunks.
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
        "response.sse",
      ),
    );
    const events = await parseWire(adapter, [sseBytes]);

    const citations = events.filter((e) => e.type === "inference.citation");
    // The captured fixture's groundingSupports references multiple
    // chunks per support, so the expanded citation count is
    // greater than the number of supports.
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
  };

  const SOURCE: InferenceSource = {
    id: "google-genai:gemini-2.5-flash",
    provider: "google-genai",
    baseURL: "https://generativelanguage.googleapis.com",
    apiKey: "test-key",
    model: "gemini-2.5-flash",
  };

  test("grounding-streaming fixture replay produces a final turn with text then citations interleaved", async () => {
    const sseBytes = readFileSync(
      join(
        FIXTURE_ROOT,
        "gemini-2.5-flash",
        "grounding-streaming",
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
      deps: { fetch: fetchImpl, scheduler: inertScheduler },
    })) {
      events.push(ev);
    }

    const done = events.find((e) => e.type === "inference.done");
    if (done?.type !== "inference.done") {
      throw new Error("expected inference.done event");
    }

    const blocks = done.data.turn.content;
    // The final turn must lead with the text block, then carry
    // citations interleaved immediately after it (the harness's
    // emit() helper does this interleave at the matched index).
    const firstBlock = blocks[0];
    if (firstBlock?.type !== "text") {
      throw new Error("expected first content block to be text");
    }
    expect(firstBlock.text.length).toBeGreaterThan(0);

    const citationBlocks = blocks.filter((b) => b.type === "citation");
    expect(citationBlocks.length).toBeGreaterThan(3);
    // Citations interleave IMMEDIATELY after the text block they
    // attribute. The harness's `emit()` helper runs the per-index
    // citation append directly after a block emission, so the
    // expected shape is `[text, citation, citation, ...]` with no
    // intervening block of another kind. Locking this here keeps
    // the documented "interleaved immediately after" invariant
    // from quietly regressing if a future change introduces an
    // unrelated block kind between the text and its citations.
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
    const adapter2 = createGoogleGenAIAdapter();
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
    // The codeExecutionResult must follow an executableCode part
    // for the pairing to be valid; seed the pending request via a
    // prior event. The parser's state machine clears
    // `pendingExecutionRequestId` only on the success path, so a
    // throw leaves the state mid-mutation -- each `expect(() =>
    // ...)` call needs a fresh adapter so the second call does not
    // see the first throw's residue.
    function buildPendingAdapter(): ProviderAdapter {
      const a = createGoogleGenAIAdapter();
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
    // Fresh adapter per `toThrow` so the first call's mid-mutation
    // throw doesn't leave residue that changes the error message on
    // the second call.
    expect(() => createGoogleGenAIAdapter().parseResponse(bad)).toThrow(
      ProtocolMismatchError,
    );
    expect(() => createGoogleGenAIAdapter().parseResponse(bad)).toThrow(
      /unmatched code-execution request/,
    );
  });

  test("code-execution-streaming fixture replay produces start, result, two text deltas, and usage", async () => {
    const sseBytes = readFileSync(
      join(
        FIXTURE_ROOT,
        "gemini-2.5-flash",
        "code-execution-streaming",
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

    // The follow-on text deltas land at a fresh index (2), not at
    // index 0 -- the executableCode + codeExecutionResult pair
    // closed the current block, and the next text is a new logical
    // block.
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
  };

  const SOURCE: InferenceSource = {
    id: "google-genai:gemini-2.5-flash",
    provider: "google-genai",
    baseURL: "https://generativelanguage.googleapis.com",
    apiKey: "test-key",
    model: "gemini-2.5-flash",
  };

  test("code-execution-streaming fixture flows through runInference into a final turn carrying request + result + text", async () => {
    const sseBytes = readFileSync(
      join(
        FIXTURE_ROOT,
        "gemini-2.5-flash",
        "code-execution-streaming",
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
      deps: { fetch: fetchImpl, scheduler: inertScheduler },
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
    // No Gemini fixture exercises code_execution.delta -- Gemini
    // delivers `executableCode` atomically. The harness wires the
    // delta path for providers that DO stream code in chunks;
    // this test drives the path with a hand-built event sequence
    // through a synthetic adapter and asserts the final block
    // carries the concatenated code.
    //
    // The synthetic adapter sidesteps the JSON round-trip the SSE
    // pipeline imposes by carrying a per-call queue in the closure
    // and yielding one event per `parseResponse` invocation.
    // Crafting events as static literals and pulling them from the
    // queue preserves the full `InferenceEvent` discriminated-union
    // typing without resorting to runtime narrowing of `unknown`
    // through arktype (the inferred runtime type is broader than
    // the strict TypeScript union and would not satisfy
    // `ResponseParser`).
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
        },
      },
    ];
    const syntheticAdapter: ProviderAdapter = {
      buildRequest: () => ({
        url: "/synthetic",
        headers: {},
        body: JSON.stringify({}),
      }),
      // Each SSE frame carries a single integer index into the queue;
      // the parser returns that event. The queue holds the strictly-
      // typed `InferenceEvent` values, so no narrowing is required.
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

    // One SSE frame per queued event; each frame's payload is just
    // the queue index as a JSON-encoded string.
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

    // Register the synthetic adapter on a unique provider id so this
    // test doesn't disturb the production registration.
    const { registerProvider } = await import("@intx/inference");
    registerProvider("synthetic-code-exec", () => syntheticAdapter);

    let seq = 0;
    const events: InferenceEvent[] = [];
    for await (const ev of runInference({
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
        apiKey: "test",
        model: "test",
      },
      nextSeq: () => seq++,
      deps: { fetch: fetchImpl, scheduler: inertScheduler },
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
    // The harness accumulated `print(` + `'hi')` into the running
    // block; the final code is the concatenation.
    expect(request.code).toBe("print('hi')");
    expect(request.id).toBe("synth-1");
  });
});
