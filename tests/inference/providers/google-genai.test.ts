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
  test("thinking in incoming turn throws", () => {
    const turns: ConversationTurn[] = [
      {
        role: "assistant",
        content: [{ type: "thinking", thinking: "internal reasoning" }],
        timestamp: 0,
      },
    ];
    expect(() => adapter.buildRequest(turns, "gemini-2.5-flash", {})).toThrow(
      /thinking content blocks/,
    );
  });

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
