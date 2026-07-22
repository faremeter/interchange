import { type } from "arktype";
import { beforeEach, describe, test, expect } from "bun:test";
import { wire } from "@intx/inference-testing";
import {
  parseSSE,
  ProtocolMismatchError,
  type ProviderAdapter,
} from "@intx/inference";
import { createOpenAIAdapter } from "@intx/inference/providers";
import type {
  ConversationTurn,
  InferenceEvent,
  LastCycleSource,
} from "@intx/types/runtime";

const TEST_SOURCE: LastCycleSource = {
  sourceId: "test-openai",
  provider: "openai",
  model: "test-openai-model",
};

// A fresh adapter per test: the OpenAI parser holds per-request
// indexer state (text/thinking/tool_call block indices allocated in
// arrival order) on the adapter instance, and sharing one adapter
// across tests would leak indexer state, making per-index assertions
// order-dependent and surprising. A test-local re-creation guards
// against that footgun.
let adapter: ProviderAdapter;
beforeEach(() => {
  adapter = createOpenAIAdapter(TEST_SOURCE);
});

const OpenAIFunctionCall = type({
  name: "string",
  arguments: "string",
});

const OpenAIToolCall = type({
  id: "string",
  type: "string",
  function: OpenAIFunctionCall,
});

// `content` is either a plain string (text-only messages) or an array
// of content parts (multimodal: text + image_url etc). The schema
// accepts either shape with passthrough for parts so each test site
// validates the specific shape it cares about.
const OpenAIAssistantMessage = type({
  role: "string",
  "content?": "string | null | unknown[]",
  "tool_calls?": OpenAIToolCall.array(),
});

const OpenAIPlainMessage = type({
  role: "string",
  "content?": "string | null | unknown[]",
  "tool_call_id?": "string",
});

const OpenAIMessage = OpenAIAssistantMessage.or(OpenAIPlainMessage);

const OpenAIRequestBody = type({
  model: "string",
  max_tokens: "number",
  messages: OpenAIMessage.array(),
  stream: "boolean",
  "temperature?": "number",
  "tools?": "unknown[]",
  "response_format?": "unknown",
});

// Drives a sequence of wire DSL chunks (full SSE-framed Uint8Arrays) through
// the production SSE parser and the supplied adapter's parseResponse, mirroring
// the harness's pipeline. Returns the flattened sequence of emitted events so
// the test site can assert on them.
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

describe("OpenAI adapter: buildRequest", () => {
  test("builds a request with required fields", () => {
    const messages: ConversationTurn[] = [
      {
        role: "user",
        content: [{ type: "text", text: "Hello" }],
        timestamp: 1000,
      },
    ];

    const req = adapter.buildRequest(messages, "gpt-4o", {});

    // URL is relative (base URL already includes /v1).
    expect(req.url).toBe("/chat/completions");
    expect(req.headers["content-type"]).toBe("application/json");

    const body = OpenAIRequestBody.assert(JSON.parse(req.body));
    expect(body.model).toBe("gpt-4o");
    expect(body.stream).toBe(true);
    expect(typeof body.max_tokens).toBe("number");
  });

  test("converts text messages correctly", () => {
    const messages: ConversationTurn[] = [
      {
        role: "user",
        content: [{ type: "text", text: "What is 2+2?" }],
        timestamp: 1000,
      },
    ];

    const req = adapter.buildRequest(messages, "gpt-4o", {});
    const body = OpenAIRequestBody.assert(JSON.parse(req.body));

    expect(body.messages).toHaveLength(1);
    expect(body.messages[0]?.role).toBe("user");
    expect(body.messages[0]?.content).toBe("What is 2+2?");
  });

  test("converts system messages to system role", () => {
    const messages: ConversationTurn[] = [
      {
        role: "system",
        content: [{ type: "text", text: "Be concise." }],
        timestamp: 1000,
      },
      {
        role: "user",
        content: [{ type: "text", text: "Hi." }],
        timestamp: 1000,
      },
    ];

    const req = adapter.buildRequest(messages, "gpt-4o", {});
    const body = OpenAIRequestBody.assert(JSON.parse(req.body));

    expect(body.messages[0]?.role).toBe("system");
    expect(body.messages[0]?.content).toBe("Be concise.");
  });

  test("prepends systemPrompt from options", () => {
    const messages: ConversationTurn[] = [
      {
        role: "user",
        content: [{ type: "text", text: "Hi." }],
        timestamp: 1000,
      },
    ];

    const req = adapter.buildRequest(messages, "gpt-4o", {
      systemPrompt: "Always respond in JSON.",
    });
    const body = OpenAIRequestBody.assert(JSON.parse(req.body));

    expect(body.messages[0]?.role).toBe("system");
    expect(body.messages[0]?.content).toBe("Always respond in JSON.");
    expect(body.messages).toHaveLength(2);
  });

  test("converts assistant tool_call blocks to tool_calls format", () => {
    const messages: ConversationTurn[] = [
      {
        role: "assistant",
        content: [
          {
            type: "tool_call",
            id: "call_abc",
            name: "get_weather",
            arguments: { city: "London" },
          },
        ],
        timestamp: 1000,
      },
    ];

    const req = adapter.buildRequest(messages, "gpt-4o", {});
    const body = OpenAIRequestBody.assert(JSON.parse(req.body));
    const assistantMsg = OpenAIAssistantMessage.assert(body.messages[0]);
    const toolCalls = assistantMsg.tool_calls;
    expect(toolCalls).toHaveLength(1);
    expect(toolCalls?.[0]?.id).toBe("call_abc");
    expect(toolCalls?.[0]?.type).toBe("function");
    expect(toolCalls?.[0]?.function.name).toBe("get_weather");
    expect(JSON.parse(toolCalls?.[0]?.function.arguments ?? "{}")).toEqual({
      city: "London",
    });
  });

  test("converts tool_result blocks to tool role messages", () => {
    const messages: ConversationTurn[] = [
      {
        role: "user",
        content: [
          {
            type: "tool_result",
            callId: "call_abc",
            content: [{ type: "text", text: "Sunny, 22°C" }],
          },
        ],
        timestamp: 1000,
      },
    ];

    const req = adapter.buildRequest(messages, "gpt-4o", {});
    const body = OpenAIRequestBody.assert(JSON.parse(req.body));
    // tool_result blocks are flattened into tool role messages.
    const toolMsg = OpenAIPlainMessage.assert(body.messages[0]);
    expect(toolMsg.role).toBe("tool");
    expect(toolMsg.tool_call_id).toBe("call_abc");
    expect(toolMsg.content).toBe("Sunny, 22°C");
  });

  test("wraps error tool results in <error> and emits no is_error field", () => {
    const messages: ConversationTurn[] = [
      {
        role: "user",
        content: [
          {
            type: "tool_result",
            callId: "call_xyz",
            content: [{ type: "text", text: "file not found" }],
            isError: true,
          },
        ],
        timestamp: 1000,
      },
    ];

    const req = adapter.buildRequest(messages, "gpt-4o", {});
    const body = OpenAIRequestBody.assert(JSON.parse(req.body));
    const toolMsg = OpenAIPlainMessage.assert(body.messages[0]);
    expect(toolMsg.role).toBe("tool");
    expect(toolMsg.tool_call_id).toBe("call_xyz");
    expect(toolMsg.content).toBe("<error>\nfile not found\n</error>");
    // The OpenAI tool-message schema rejects unknown fields; is_error must
    // not appear on the wire even when the source block has isError=true.
    expect(req.body).not.toContain("is_error");
  });

  test("does not wrap non-error tool results", () => {
    const messages: ConversationTurn[] = [
      {
        role: "user",
        content: [
          {
            type: "tool_result",
            callId: "call_ok",
            content: [{ type: "text", text: "ok" }],
            isError: false,
          },
        ],
        timestamp: 1000,
      },
    ];

    const req = adapter.buildRequest(messages, "gpt-4o", {});
    const body = OpenAIRequestBody.assert(JSON.parse(req.body));
    const toolMsg = OpenAIPlainMessage.assert(body.messages[0]);
    expect(toolMsg.content).toBe("ok");
  });

  test("uses max_tokens from options", () => {
    const messages: ConversationTurn[] = [
      {
        role: "user",
        content: [{ type: "text", text: "Hi." }],
        timestamp: 1000,
      },
    ];

    const req = adapter.buildRequest(messages, "gpt-4o", { maxTokens: 256 });
    const body = OpenAIRequestBody.assert(JSON.parse(req.body));
    expect(body.max_tokens).toBe(256);
  });

  test("emits a URL image as { type: image_url, image_url: { url } } passing the URL verbatim", () => {
    // OpenAI's image_url accepts a public URL alongside the data-URL
    // form. The URL is passed verbatim — no data: prefix synthesis,
    // no mimeType on the wire (OpenAI infers from the URL response).
    const messages: ConversationTurn[] = [
      {
        role: "user",
        content: [
          {
            type: "image",
            source: {
              kind: "url",
              mimeType: "image/png",
              url: "https://example.com/cat.png",
            },
          },
        ],
        timestamp: 1000,
      },
    ];

    const req = adapter.buildRequest(messages, "gpt-4o", {});
    const body = OpenAIRequestBody.assert(JSON.parse(req.body));
    const message = body.messages[0];
    if (!message || !Array.isArray(message.content)) {
      throw new Error("expected a user message with array content");
    }
    const part = message.content[0];
    if (
      !part ||
      typeof part !== "object" ||
      !("type" in part) ||
      part.type !== "image_url" ||
      !("image_url" in part) ||
      typeof part.image_url !== "object" ||
      part.image_url === null ||
      !("url" in part.image_url) ||
      typeof part.image_url.url !== "string"
    ) {
      throw new Error("expected an image_url part with a string url");
    }
    expect(part.image_url.url).toBe("https://example.com/cat.png");
  });

  test("rejects a file-reference image source with a message naming the reference", () => {
    // OpenAI's Chat Completions doesn't accept opaque file references
    // — only data URLs and public URLs through `image_url`. The throw
    // names the actual reference value so an operator triaging the
    // failure sees what was sent (a stale Anthropic file_id, a Gemini
    // fileUri, etc.) rather than just "not supported."
    const messages: ConversationTurn[] = [
      {
        role: "user",
        content: [
          {
            type: "image",
            source: {
              kind: "file-reference",
              mimeType: "image/png",
              reference: "file_abc123",
            },
          },
        ],
        timestamp: 1000,
      },
    ];

    expect(() => adapter.buildRequest(messages, "gpt-4o", {})).toThrow(
      /file-reference image sources/,
    );
    expect(() => adapter.buildRequest(messages, "gpt-4o", {})).toThrow(
      /file_abc123/,
    );
  });

  test("emits multiple base64 image content parts preserving wire order", () => {
    // A single user turn may carry multiple image content blocks
    // (e.g., before/after comparison). The adapter must preserve their
    // order on the wire — a reordering bug would invert "compare
    // these two screenshots" prompts and produce wrong answers
    // silently. Each image lands as a distinct `image_url` content
    // part with its own data URL.
    const messages: ConversationTurn[] = [
      {
        role: "user",
        content: [
          { type: "text", text: "Compare:" },
          {
            type: "image",
            source: { kind: "base64", mimeType: "image/png", data: "FIRST" },
          },
          {
            type: "image",
            source: { kind: "base64", mimeType: "image/jpeg", data: "SECOND" },
          },
        ],
        timestamp: 1000,
      },
    ];

    const req = adapter.buildRequest(messages, "gpt-4o", {});
    const body = OpenAIRequestBody.assert(JSON.parse(req.body));
    const userMsg = body.messages[0];
    if (userMsg === undefined) {
      throw new Error("expected one message");
    }
    const content = userMsg.content;
    if (!Array.isArray(content)) {
      throw new Error("expected content to be an array of parts");
    }
    const isRecord = (v: unknown): v is Record<string, unknown> =>
      typeof v === "object" && v !== null && !Array.isArray(v);
    const isImagePart = (
      p: unknown,
    ): p is { type: "image_url"; image_url: { url: string } } => {
      if (!isRecord(p)) return false;
      if (p["type"] !== "image_url") return false;
      const inner = p["image_url"];
      if (!isRecord(inner)) return false;
      return typeof inner["url"] === "string";
    };
    const imageParts = content.filter(isImagePart);
    expect(imageParts).toHaveLength(2);
    expect(imageParts[0]?.image_url.url).toBe("data:image/png;base64,FIRST");
    expect(imageParts[1]?.image_url.url).toBe("data:image/jpeg;base64,SECOND");
  });

  test.each(["audio", "video"] as const)(
    "rejects a %s content block until provider support is wired",
    (blockType) => {
      const messages: ConversationTurn[] = [
        {
          role: "user",
          content: [
            {
              type: blockType,
              source: {
                kind: "base64",
                mimeType: "application/octet-stream",
                data: "aGVsbG8=",
              },
            },
          ],
          timestamp: 1000,
        },
      ];

      expect(() => adapter.buildRequest(messages, "gpt-4o", {})).toThrow(
        new RegExp(`${blockType} content blocks`),
      );
    },
  );

  test("rejects a document content block with a message naming the missing capture", () => {
    // OpenAI's Chat Completions has a `file` content type for PDFs,
    // but the exact wire shape (field names, required filename
    // metadata) is version-sensitive and the capture corpus has no
    // OpenAI document fixtures to verify against. The throw is the
    // honest answer: surface explicit context rather than emitting
    // an unverified shape that might silently land as malformed
    // input the model ignores.
    const messages: ConversationTurn[] = [
      {
        role: "user",
        content: [
          {
            type: "document",
            source: {
              kind: "base64",
              mimeType: "application/pdf",
              data: "JVBERi0xLjQK",
            },
          },
        ],
        timestamp: 1000,
      },
    ];

    expect(() => adapter.buildRequest(messages, "gpt-4o", {})).toThrow(
      /document content blocks/,
    );
    expect(() => adapter.buildRequest(messages, "gpt-4o", {})).toThrow(
      /captured fixture/,
    );
  });

  test.each(["code_execution_request", "code_execution_result"] as const)(
    "rejects a %s content block (no OpenAI surface)",
    (blockType) => {
      const block =
        blockType === "code_execution_request"
          ? {
              type: blockType,
              id: "srvtoolu_01",
              code: "print('hi')",
            }
          : {
              type: blockType,
              requestId: "srvtoolu_01",
              status: "ok" as const,
            };
      const messages: ConversationTurn[] = [
        {
          role: "assistant",
          content: [block],
          timestamp: 1000,
        },
      ];

      expect(() => adapter.buildRequest(messages, "gpt-4o", {})).toThrow(
        new RegExp(`${blockType} content blocks`),
      );
    },
  );

  test("throws on a mixed-content assistant turn that includes code execution", () => {
    // Guards the placement of the assistant-role detection loop. The
    // loop must fire even when valid text and tool_call blocks are
    // present alongside the code execution block — a regression that
    // moved the check after the existing field filters would silently
    // succeed on the simpler single-block test, but must fail here.
    const messages: ConversationTurn[] = [
      {
        role: "assistant",
        content: [
          { type: "text", text: "Running the calculation:" },
          {
            type: "code_execution_request",
            id: "srvtoolu_01",
            code: "print('hi')",
          },
          {
            type: "tool_call",
            id: "call_abc",
            name: "search",
            arguments: { q: "x" },
          },
        ],
        timestamp: 1000,
      },
    ];

    expect(() => adapter.buildRequest(messages, "gpt-4o", {})).toThrow(
      /code_execution_request content blocks/,
    );
  });

  test("throws on an assistant turn carrying a refusal content block", () => {
    // RefusalBlocks come from this adapter's own delta.refusal
    // parsing, but the round-trip back through OpenAI's input
    // message shape is not modeled. Surface the failure at the
    // marshaling boundary rather than silently drop the refusal
    // text (which would render as content: null in the wire body).
    const messages: ConversationTurn[] = [
      {
        role: "assistant",
        content: [{ type: "refusal", reason: "I cannot help with that." }],
        timestamp: 1000,
      },
    ];

    expect(() => adapter.buildRequest(messages, "gpt-4o", {})).toThrow(
      /refusal content blocks/,
    );
  });

  test("throws on a user turn carrying a refusal content block", () => {
    // Echoing a refusal into a user-role content array is even
    // odder than the assistant case — there's no OpenAI wire shape
    // for it at all. The user-role serializer must also fail
    // loudly rather than emit a `null` part.
    const messages: ConversationTurn[] = [
      {
        role: "user",
        content: [{ type: "refusal", reason: "Earlier refusal." }],
        timestamp: 1000,
      },
    ];

    expect(() => adapter.buildRequest(messages, "gpt-4o", {})).toThrow(
      /refusal content blocks/,
    );
  });

  test("silently drops redacted_thinking content blocks (opaque, no OpenAI surface)", () => {
    const messages: ConversationTurn[] = [
      {
        role: "user",
        content: [
          { type: "text", text: "Hello" },
          {
            type: "redacted_thinking",
            data: "EncryptedOpaqueBlobAAAA==",
          },
        ],
        timestamp: 1000,
      },
    ];

    const req = adapter.buildRequest(messages, "gpt-4o", {});
    const body = OpenAIRequestBody.assert(JSON.parse(req.body));
    const msg = OpenAIPlainMessage.assert(body.messages[0]);
    expect(msg.content).toBe("Hello");
  });

  test("silently drops citation content blocks (not part of OpenAI surface)", () => {
    const messages: ConversationTurn[] = [
      {
        role: "user",
        content: [
          { type: "text", text: "Hello" },
          {
            type: "citation",
            citedText: "x",
            source: { uri: "https://example.com/" },
          },
        ],
        timestamp: 1000,
      },
    ];

    const req = adapter.buildRequest(messages, "gpt-4o", {});
    const body = OpenAIRequestBody.assert(JSON.parse(req.body));
    // The citation block contributes an empty string; the text remains.
    const msg = OpenAIPlainMessage.assert(body.messages[0]);
    expect(msg.content).toBe("Hello");
  });
});

describe("OpenAI adapter: parseResponse", () => {
  test("parses text delta from choices", async () => {
    const events = await parseWire(adapter, [
      wire.openai.chunk({ content: "Hello" }),
    ]);
    expect(events).toHaveLength(1);
    expect(events[0]?.type).toBe("inference.text.delta");
    if (events[0]?.type === "inference.text.delta") {
      expect(events[0].data.token).toBe("Hello");
    }
  });

  test("returns empty for null content delta", async () => {
    const events = await parseWire(adapter, [
      wire.openai.chunk({ contentNull: true }),
    ]);
    expect(events).toEqual([]);
  });

  test("parses tool_call start with id and name and propagates the wire index", async () => {
    const events = await parseWire(adapter, [
      wire.openai.toolCallStart(0, "call_xyz", "search"),
    ]);
    expect(events).toHaveLength(1);
    expect(events[0]?.type).toBe("inference.tool_call.start");
    if (events[0]?.type === "inference.tool_call.start") {
      expect(events[0].data.callId).toBe("call_xyz");
      expect(events[0].data.name).toBe("search");
      expect(events[0].data.index).toBe(0);
    }
  });

  test("parses tool_call argument fragment and propagates the wire index", async () => {
    const events = await parseWire(adapter, [
      wire.openai.toolCallArgumentsDelta(0, '{"q":"'),
    ]);
    expect(events).toHaveLength(1);
    expect(events[0]?.type).toBe("inference.tool_call.delta");
    if (events[0]?.type === "inference.tool_call.delta") {
      expect(events[0].data.argumentFragment).toBe('{"q":"');
      expect(events[0].data.index).toBe(0);
    }
  });

  test("tool_call emitted before text content gets a distinct content-block index", async () => {
    // Regression: when the OpenAI stream emits a tool_call before any
    // text content, the parser's per-request indexer must allocate a
    // fresh content-block index for the tool_call rather than reusing
    // 0 — otherwise a later text delta would also land at 0 and
    // collide with the tool_use marker in the harness's per-index map.
    // The harness would (correctly) raise a protocol_mismatch error;
    // the bug surfaces as the whole turn failing instead of producing
    // [tool_call, text] in the final content[].
    const events = await parseWire(adapter, [
      wire.openai.toolCallStart(0, "call_first", "search"),
      wire.openai.toolCallArgumentsDelta(0, '{"q":"hi"}'),
      wire.openai.chunk({ content: "Calling search now." }),
    ]);

    const starts = events.filter((e) => e.type === "inference.tool_call.start");
    const textDeltas = events.filter((e) => e.type === "inference.text.delta");
    expect(starts).toHaveLength(1);
    expect(textDeltas).toHaveLength(1);

    const start = starts[0];
    const textDelta = textDeltas[0];
    if (
      start?.type !== "inference.tool_call.start" ||
      textDelta?.type !== "inference.text.delta"
    ) {
      throw new Error("expected one tool_call.start and one text.delta");
    }
    // The tool_call lands at content-block index 0 (first observed);
    // the text lands at 1 (next free) — NOT both at 0.
    expect(start.data.index).toBe(0);
    expect(textDelta.data.index).toBe(1);
  });

  test("propagates distinct tool_calls indices to data.index across parallel tool calls", async () => {
    // Multi-tool-call regression target: two parallel tool calls in
    // a single response stream interleaved deltas at indices 0 and 1.
    // The parser must propagate each delta's wire-level `index` to
    // the emitted event's `data.index` so the harness's per-block
    // routing resolves to the right tool. Collapsing both indices to
    // 0 would route the second tool's argument fragments onto the
    // first tool's accumulator.
    const events = await parseWire(adapter, [
      wire.openai.toolCallStart(0, "call_first", "alpha"),
      wire.openai.toolCallStart(1, "call_second", "beta"),
      wire.openai.toolCallArgumentsDelta(0, '{"a":1}'),
      wire.openai.toolCallArgumentsDelta(1, '{"b":2}'),
    ]);

    const starts = events.filter((e) => e.type === "inference.tool_call.start");
    const deltas = events.filter((e) => e.type === "inference.tool_call.delta");
    expect(starts).toHaveLength(2);
    expect(deltas).toHaveLength(2);

    const start0 = starts[0];
    const start1 = starts[1];
    if (
      start0?.type !== "inference.tool_call.start" ||
      start1?.type !== "inference.tool_call.start"
    ) {
      throw new Error("expected two start events");
    }
    expect(start0.data.callId).toBe("call_first");
    expect(start0.data.index).toBe(0);
    expect(start1.data.callId).toBe("call_second");
    expect(start1.data.index).toBe(1);

    const delta0 = deltas[0];
    const delta1 = deltas[1];
    if (
      delta0?.type !== "inference.tool_call.delta" ||
      delta1?.type !== "inference.tool_call.delta"
    ) {
      throw new Error("expected two delta events");
    }
    expect(delta0.data.index).toBe(0);
    expect(delta0.data.argumentFragment).toBe('{"a":1}');
    expect(delta1.data.index).toBe(1);
    expect(delta1.data.argumentFragment).toBe('{"b":2}');
  });

  test("parses usage from final chunk", async () => {
    const events = await parseWire(adapter, [
      wire.openai.usageChunk({ promptTokens: 50, completionTokens: 20 }),
    ]);
    expect(events).toHaveLength(1);
    expect(events[0]?.type).toBe("inference.usage");
    if (events[0]?.type === "inference.usage") {
      expect(events[0].data.usage.input).toBe(50);
      expect(events[0].data.usage.output).toBe(20);
    }
  });

  test("parses Fireworks-shaped tool-call deltas with null name/id on follow-up fragments", async () => {
    // Fireworks (and other OpenAI-compatible deployments routing through
    // opencode-zen) emits `id: null` and `function.name: null` on every
    // tool-call delta AFTER the start delta — semantically equivalent to
    // omitting the field, but lexically a different JSON value.
    // The schema must accept `null` and the consumer site must normalise
    // it to undefined, otherwise every fragment chunk fails validation
    // and `argsBuffer` never accumulates — the exact failure mode that
    // produced `arguments: {}` tool calls in kimi-k2.6 runs of
    // interchange-demo-dispatch.
    //
    // Hand-rolled via `wire.openai.raw()` because the wire DSL's typed
    // helpers (`toolCallStart`, `toolCallArgumentsDelta`) always emit
    // the canonical OpenAI shape, never the Fireworks variant.
    const events = await parseWire(adapter, [
      wire.openai.raw(
        'data: {"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_abc","function":{"name":"read_file","arguments":""}}]}}]}\n\n',
      ),
      wire.openai.raw(
        'data: {"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":null,"function":{"name":null,"arguments":"{\\"path\\":\\""}}]}}]}\n\n',
      ),
      wire.openai.raw(
        'data: {"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":null,"function":{"name":null,"arguments":"foo.ts\\"}"}}]}}]}\n\n',
      ),
    ]);

    // One start (from chunk 1) plus two fragments (from chunks 2 and 3).
    // Chunk 1's empty `arguments: ""` does not produce a fragment event
    // because the consumer-site length check skips zero-length fragments.
    const starts = events.filter((e) => e.type === "inference.tool_call.start");
    const fragments = events.filter(
      (e) => e.type === "inference.tool_call.delta",
    );
    expect(starts).toHaveLength(1);
    expect(fragments).toHaveLength(2);

    const start = starts[0];
    if (start?.type === "inference.tool_call.start") {
      expect(start.data.callId).toBe("call_abc");
      expect(start.data.name).toBe("read_file");
    }

    const accumulated = fragments
      .map((e) =>
        e.type === "inference.tool_call.delta" ? e.data.argumentFragment : "",
      )
      .join("");
    expect(accumulated).toBe('{"path":"foo.ts"}');
  });

  test("emits both start and fragment from a single Fireworks first-fragment delta", async () => {
    // Locks the dual-emission claim in the consumer-site comment: when
    // one chunk carries BOTH a start signal (id + non-null name) and
    // a non-empty argument fragment, both events must come out. This
    // is what Fireworks does on the first fragment delta following the
    // bare start — without this independent treatment, accepting null
    // name on the second-and-later deltas alone wouldn't be enough.
    const events = await parseWire(adapter, [
      wire.openai.raw(
        'data: {"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_xyz","function":{"name":"search","arguments":"{\\"q\\":\\"foo\\"}"}}]}}]}\n\n',
      ),
    ]);

    expect(events).toHaveLength(2);
    expect(events[0]?.type).toBe("inference.tool_call.start");
    expect(events[1]?.type).toBe("inference.tool_call.delta");
    if (events[0]?.type === "inference.tool_call.start") {
      expect(events[0].data.callId).toBe("call_xyz");
      expect(events[0].data.name).toBe("search");
    }
    if (events[1]?.type === "inference.tool_call.delta") {
      expect(events[1].data.argumentFragment).toBe('{"q":"foo"}');
    }
  });

  test("returns empty for empty choices array with no usage", async () => {
    // The `chunk()` helper always emits a non-empty choices entry unless a
    // usage block is supplied (in which case it also adds a usage object).
    // Emitting `{choices: []}` with no other fields requires `raw()`.
    const events = await parseWire(adapter, [
      wire.openai.raw('data: {"choices":[]}\n\n'),
    ]);
    expect(events).toEqual([]);
  });

  test("throws ProtocolMismatchError on malformed JSON in SSE payload", () => {
    // The harness's stream-error catch maps this throw to an
    // inference.error event with category "protocol_mismatch" via
    // classifyStreamError. The raw payload is carried in error.raw so
    // operators can inspect the bytes that failed to parse.
    expect(() => adapter.parseResponse("not json {")).toThrow(
      ProtocolMismatchError,
    );

    try {
      adapter.parseResponse("not json {");
      throw new Error("expected ProtocolMismatchError to be thrown");
    } catch (err) {
      expect(err).toBeInstanceOf(ProtocolMismatchError);
      if (err instanceof ProtocolMismatchError) {
        expect(err.message).toContain("malformed JSON");
        expect(err.raw).toBe("not json {");
      }
    }
  });

  test("throws ProtocolMismatchError on schema-mismatched chunk", () => {
    // `delta.role: 42` is well-formed JSON but rejects against the
    // OpenAIChunkDelta schema (role must be string when present).
    // The thrown error carries the parsed object in `raw` and the
    // arktype summary in `message` so operators reading audit logs see
    // exactly where the wire violated the contract.
    const malformed = '{"choices":[{"delta":{"role":42}}]}';

    expect(() => adapter.parseResponse(malformed)).toThrow(
      ProtocolMismatchError,
    );

    try {
      adapter.parseResponse(malformed);
      throw new Error("expected ProtocolMismatchError to be thrown");
    } catch (err) {
      expect(err).toBeInstanceOf(ProtocolMismatchError);
      if (err instanceof ProtocolMismatchError) {
        expect(err.message).toContain("schema validation");
        // The raw field carries the parsed object, not the original string.
        expect(err.raw).toEqual({ choices: [{ delta: { role: 42 } }] });
      }
    }
  });

  test("parses delta.refusal as inference.refusal.delta with a fresh block index", async () => {
    const events = await parseWire(adapter, [
      wire.openai.chunk({ refusal: "I can't help with that." }),
    ]);
    expect(events).toHaveLength(1);
    expect(events[0]?.type).toBe("inference.refusal.delta");
    if (events[0]?.type === "inference.refusal.delta") {
      expect(events[0].data.token).toBe("I can't help with that.");
      // First content block of the stream, so index 0.
      expect(events[0].data.index).toBe(0);
    }
  });

  test("accumulates refusal fragments under the same block index across chunks", async () => {
    const events = await parseWire(adapter, [
      wire.openai.chunk({ refusal: "I can't" }),
      wire.openai.chunk({ refusal: " help with" }),
      wire.openai.chunk({ refusal: " that." }),
    ]);
    expect(events).toHaveLength(3);
    for (const e of events) {
      expect(e.type).toBe("inference.refusal.delta");
      if (e.type === "inference.refusal.delta") {
        expect(e.data.index).toBe(0);
      }
    }
    const reason = events
      .map((e) => (e.type === "inference.refusal.delta" ? e.data.token : ""))
      .join("");
    expect(reason).toBe("I can't help with that.");
  });

  test("refusal arriving after text gets a distinct content-block index", async () => {
    // Text streams first and allocates content-block index 0. The
    // refusal then allocates the next free index. Distinct kinds at
    // distinct indices match the harness's per-index routing contract.
    const events = await parseWire(adapter, [
      wire.openai.chunk({ content: "I think" }),
      wire.openai.chunk({ refusal: "Actually I can't." }),
    ]);
    expect(events).toHaveLength(2);
    expect(events[0]?.type).toBe("inference.text.delta");
    if (events[0]?.type === "inference.text.delta") {
      expect(events[0].data.index).toBe(0);
    }
    expect(events[1]?.type).toBe("inference.refusal.delta");
    if (events[1]?.type === "inference.refusal.delta") {
      expect(events[1].data.index).toBe(1);
    }
  });

  test("ignores null and empty-string delta.refusal", async () => {
    const events = await parseWire(adapter, [
      wire.openai.chunk({ refusalNull: true }),
      wire.openai.chunk({ refusal: "" }),
    ]);
    expect(events).toEqual([]);
  });
});

describe("OpenAI adapter: responseFormat translation", () => {
  const conversation: ConversationTurn[] = [
    {
      role: "user",
      content: [{ type: "text", text: "Extract user fields." }],
      timestamp: 1000,
    },
  ];

  test("omits response_format when responseFormat is undefined", () => {
    const req = adapter.buildRequest(conversation, "gpt-4o", {});
    const body = OpenAIRequestBody.assert(JSON.parse(req.body));
    expect(body.response_format).toBeUndefined();
  });

  test("translates responseFormat.kind=text to { type: 'text' }", () => {
    const req = adapter.buildRequest(conversation, "gpt-4o", {
      responseFormat: { kind: "text" },
    });
    const body = OpenAIRequestBody.assert(JSON.parse(req.body));
    expect(body.response_format).toEqual({ type: "text" });
  });

  test("translates responseFormat.kind=json to { type: 'json_object' }", () => {
    const req = adapter.buildRequest(conversation, "gpt-4o", {
      responseFormat: { kind: "json" },
    });
    const body = OpenAIRequestBody.assert(JSON.parse(req.body));
    expect(body.response_format).toEqual({ type: "json_object" });
  });

  test("translates responseFormat.kind=json-schema to a json_schema body and omits strict when unset", () => {
    const schema = {
      type: "object",
      properties: { name: { type: "string" } },
      required: ["name"],
      additionalProperties: false,
    };
    const req = adapter.buildRequest(conversation, "gpt-4o", {
      responseFormat: { kind: "json-schema", name: "user_info", schema },
    });
    const body = OpenAIRequestBody.assert(JSON.parse(req.body));
    expect(body.response_format).toEqual({
      type: "json_schema",
      json_schema: { name: "user_info", schema },
    });
  });

  test("threads strict=true through to the json_schema body when supplied", () => {
    const schema = {
      type: "object",
      properties: {},
      additionalProperties: false,
    };
    const req = adapter.buildRequest(conversation, "gpt-4o", {
      responseFormat: {
        kind: "json-schema",
        name: "empty",
        schema,
        strict: true,
      },
    });
    const body = OpenAIRequestBody.assert(JSON.parse(req.body));
    expect(body.response_format).toEqual({
      type: "json_schema",
      json_schema: { name: "empty", schema, strict: true },
    });
  });
});

describe("OpenAI adapter: quirks", () => {
  const ReasoningContentView = type({ "reasoning_content?": "string" });

  const assistantWithText: ConversationTurn[] = [
    {
      role: "assistant",
      content: [{ type: "text", text: "hello" }],
      timestamp: 1000,
    },
  ];
  const assistantWithThinking: ConversationTurn[] = [
    {
      role: "assistant",
      content: [
        { type: "thinking", thinking: "pondering" },
        { type: "text", text: "hello" },
      ],
      timestamp: 1000,
    },
  ];

  function assistantMessage(
    adapterInstance: ProviderAdapter,
    turns: ConversationTurn[],
  ) {
    const req = adapterInstance.buildRequest(turns, "gpt-4o", {});
    const body = OpenAIRequestBody.assert(JSON.parse(req.body));
    const message = body.messages[0];
    if (message === undefined) throw new Error("expected an assistant message");
    return message;
  }

  test("absent quirks omit reasoning_content on a turn without thinking", () => {
    const message = assistantMessage(
      createOpenAIAdapter(TEST_SOURCE),
      assistantWithText,
    );
    expect("reasoning_content" in message).toBe(false);
  });

  test("absent quirks emit reasoning_content on a turn with thinking", () => {
    const message = assistantMessage(
      createOpenAIAdapter(TEST_SOURCE),
      assistantWithThinking,
    );
    expect(ReasoningContentView.assert(message).reasoning_content).toBe(
      "pondering",
    );
  });

  test("forceAssistantReasoningContent false omits reasoning_content on turns without thinking", () => {
    const message = assistantMessage(
      createOpenAIAdapter(TEST_SOURCE, {
        forceAssistantReasoningContent: false,
      }),
      assistantWithText,
    );
    expect("reasoning_content" in message).toBe(false);
  });

  test("forceAssistantReasoningContent false still emits reasoning_content on turns with thinking", () => {
    const message = assistantMessage(
      createOpenAIAdapter(TEST_SOURCE, {
        forceAssistantReasoningContent: false,
      }),
      assistantWithThinking,
    );
    expect(ReasoningContentView.assert(message).reasoning_content).toBe(
      "pondering",
    );
  });

  test("forceAssistantReasoningContent true forces reasoning_content on a turn without thinking", () => {
    const message = assistantMessage(
      createOpenAIAdapter(TEST_SOURCE, {
        forceAssistantReasoningContent: true,
      }),
      assistantWithText,
    );
    expect(ReasoningContentView.assert(message).reasoning_content).toBe("");
  });

  test("forceAssistantReasoningContent true emits reasoning_content on a turn with thinking", () => {
    const message = assistantMessage(
      createOpenAIAdapter(TEST_SOURCE, {
        forceAssistantReasoningContent: true,
      }),
      assistantWithThinking,
    );
    expect(ReasoningContentView.assert(message).reasoning_content).toBe(
      "pondering",
    );
  });

  test("absent quirks read reasoning from reasoning_content, then reasoning", async () => {
    const fromContent = await parseWire(createOpenAIAdapter(TEST_SOURCE), [
      wire.openai.chunk({ reasoningContent: "abc" }),
    ]);
    expect(fromContent).toHaveLength(1);
    expect(fromContent[0]?.type).toBe("inference.thinking.delta");
    if (fromContent[0]?.type === "inference.thinking.delta") {
      expect(fromContent[0].data.token).toBe("abc");
    }

    const fromReasoning = await parseWire(createOpenAIAdapter(TEST_SOURCE), [
      wire.openai.chunk({ reasoning: "xyz" }),
    ]);
    expect(fromReasoning).toHaveLength(1);
    if (fromReasoning[0]?.type === "inference.thinking.delta") {
      expect(fromReasoning[0].data.token).toBe("xyz");
    }
  });

  test("reasoningFieldNames [reasoning] reads only delta.reasoning", async () => {
    const ignored = await parseWire(
      createOpenAIAdapter(TEST_SOURCE, { reasoningFieldNames: ["reasoning"] }),
      [wire.openai.chunk({ reasoningContent: "abc" })],
    );
    expect(ignored).toEqual([]);

    const read = await parseWire(
      createOpenAIAdapter(TEST_SOURCE, { reasoningFieldNames: ["reasoning"] }),
      [wire.openai.chunk({ reasoning: "xyz" })],
    );
    expect(read).toHaveLength(1);
    if (read[0]?.type === "inference.thinking.delta") {
      expect(read[0].data.token).toBe("xyz");
    }
  });

  test("reasoningFieldNames [] reads no reasoning", async () => {
    const events = await parseWire(
      createOpenAIAdapter(TEST_SOURCE, { reasoningFieldNames: [] }),
      [wire.openai.chunk({ reasoningContent: "abc" })],
    );
    expect(events).toEqual([]);
  });

  test("reasoning precedence follows configured order, not wire order", async () => {
    const reasoningFirst = await parseWire(
      createOpenAIAdapter(TEST_SOURCE, {
        reasoningFieldNames: ["reasoning", "reasoning_content"],
      }),
      [wire.openai.chunk({ reasoningContent: "cc", reasoning: "rr" })],
    );
    expect(reasoningFirst).toHaveLength(1);
    if (reasoningFirst[0]?.type === "inference.thinking.delta") {
      expect(reasoningFirst[0].data.token).toBe("rr");
    }

    const contentFirst = await parseWire(createOpenAIAdapter(TEST_SOURCE), [
      wire.openai.chunk({ reasoningContent: "cc", reasoning: "rr" }),
    ]);
    expect(contentFirst).toHaveLength(1);
    if (contentFirst[0]?.type === "inference.thinking.delta") {
      expect(contentFirst[0].data.token).toBe("cc");
    }
  });

  test("rejects a quirks bag with a wrong field type at construction", () => {
    expect(() =>
      createOpenAIAdapter(TEST_SOURCE, {
        forceAssistantReasoningContent: "yes",
      }),
    ).toThrow(/invalid quirks/);
  });

  test("rejects a reasoning field name the adapter cannot read", () => {
    expect(() =>
      createOpenAIAdapter(TEST_SOURCE, { reasoningFieldNames: ["thinking"] }),
    ).toThrow(/invalid quirks/);
  });

  test("rejects an unknown quirk key so a typo fails loudly", () => {
    expect(() =>
      createOpenAIAdapter(TEST_SOURCE, {
        forceAssistantReasoningContnt: false,
      }),
    ).toThrow(/invalid quirks/);
  });

  test("an empty-string reasoning field claims its slot and suppresses lower precedence", async () => {
    // Matches the prior `reasoning_content ?? reasoning` short-circuit: an
    // empty string is a present value, so it wins its precedence slot and the
    // length gate then drops it — the lower-precedence `reasoning` is never
    // read. "First non-empty wins" would instead surface "xyz"; it must not.
    const events = await parseWire(createOpenAIAdapter(TEST_SOURCE), [
      wire.openai.chunk({ reasoningContent: "", reasoning: "xyz" }),
    ]);
    expect(events).toEqual([]);
  });

  test("a null reasoning field is skipped so a lower-precedence field is read", async () => {
    // `reasoning_content` is declared `string | null` on the wire; a null
    // value falls through to the next configured field, matching the prior
    // `??` null-skip. The wire DSL cannot emit an explicit null, so this drives
    // a raw chunk.
    const events = await parseWire(createOpenAIAdapter(TEST_SOURCE), [
      wire.openai.raw(
        `data: ${JSON.stringify({
          id: "chatcmpl-test",
          object: "chat.completion.chunk",
          choices: [
            {
              index: 0,
              delta: { reasoning_content: null, reasoning: "xyz" },
              finish_reason: null,
            },
          ],
        })}\n\n`,
      ),
    ]);
    expect(events).toHaveLength(1);
    expect(events[0]?.type).toBe("inference.thinking.delta");
    if (events[0]?.type === "inference.thinking.delta") {
      expect(events[0].data.token).toBe("xyz");
    }
  });
});
