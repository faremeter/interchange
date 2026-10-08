import { type } from "arktype";
import { beforeEach, describe, test, expect } from "bun:test";
import { wire } from "@intx/inference-testing";
import {
  parseSSE,
  ProtocolMismatchError,
  runInference,
  createDefaultScheduler,
  type ProviderAdapter,
  type Dependencies,
} from "@intx/inference";
import {
  createOpenAIAdapter,
  createBuiltinRegistry,
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
  sourceId: "test-openai",
  provider: "openai",
  model: "test-openai-model",
};

// Fresh adapter per test: the parser's per-request indexer state
// (block indices in arrival order) would leak across tests.
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

// `content` is a plain string (text-only) or an array of parts
// (multimodal); the schema accepts either shape.
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
  "reasoning_effort?": "'none'",
});

// Drives SSE-framed chunks through the production parseSSE + parseResponse
// pipeline and returns the flattened event sequence.
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

    const req = adapter.buildRequest(messages, "gpt-5.5", {});

    // URL is relative (base URL already includes /v1).
    expect(req.url).toBe("/chat/completions");
    expect(req.headers["content-type"]).toBe("application/json");

    const body = OpenAIRequestBody.assert(JSON.parse(req.body));
    expect(body.model).toBe("gpt-5.5");
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

    const req = adapter.buildRequest(messages, "gpt-5.5", {});
    const body = OpenAIRequestBody.assert(JSON.parse(req.body));

    expect(body.messages).toHaveLength(1);
    expect(body.messages[0]?.role).toBe("user");
    expect(body.messages[0]?.content).toBe("What is 2+2?");
  });

  test("rewrites safety_rating history to assistant text content", () => {
    const messages: ConversationTurn[] = [
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
        content: [{ type: "text", text: "try again" }],
        timestamp: 3,
      },
    ];
    const req = adapter.buildRequest(messages, "gpt-5.5", {});
    const body = OpenAIRequestBody.assert(JSON.parse(req.body));
    expect(body.messages).toHaveLength(3);
    expect(body.messages[1]?.role).toBe("assistant");
    expect(body.messages[1]?.content).toBe(
      "Request blocked: PROHIBITED_CONTENT",
    );
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

    const req = adapter.buildRequest(messages, "gpt-5.5", {});
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

    const req = adapter.buildRequest(messages, "gpt-5.5", {
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

    const req = adapter.buildRequest(messages, "gpt-5.5", {});
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

    const req = adapter.buildRequest(messages, "gpt-5.5", {});
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

    const req = adapter.buildRequest(messages, "gpt-5.5", {});
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

    const req = adapter.buildRequest(messages, "gpt-5.5", {});
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

    const req = adapter.buildRequest(messages, "gpt-5.5", { maxTokens: 256 });
    const body = OpenAIRequestBody.assert(JSON.parse(req.body));
    expect(body.max_tokens).toBe(256);
  });

  test("emits a URL image as { type: image_url, image_url: { url } } passing the URL verbatim", () => {
    // image_url accepts a public URL verbatim: no data: synthesis, no
    // mimeType on the wire (OpenAI infers it from the URL response).
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

    const req = adapter.buildRequest(messages, "gpt-5.5", {});
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
    // Chat Completions accepts only data URLs and public URLs via
    // `image_url`; the throw names the actual reference value so an
    // operator sees what was sent, not just "not supported".
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

    expect(() => adapter.buildRequest(messages, "gpt-5.5", {})).toThrow(
      /file-reference image sources/,
    );
    expect(() => adapter.buildRequest(messages, "gpt-5.5", {})).toThrow(
      /file_abc123/,
    );
  });

  test("emits multiple base64 image content parts preserving wire order", () => {
    // Multiple image blocks per turn must keep their order on the wire
    // (a reordering bug would invert a "compare these" prompt); each
    // image lands as its own `image_url` part.
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

    const req = adapter.buildRequest(messages, "gpt-5.5", {});
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

      expect(() => adapter.buildRequest(messages, "gpt-5.5", {})).toThrow(
        new RegExp(`${blockType} content blocks`),
      );
    },
  );

  test("emits a base64 PDF document as a Chat Completions file part", () => {
    // Shape grounded on openai/gpt-5.5/document-input capture: typed
    // text + file parts; file_data is a data URI; filename required.
    const messages: ConversationTurn[] = [
      {
        role: "user",
        content: [
          { type: "text", text: "Summarize the attached document." },
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

    const req = adapter.buildRequest(messages, "gpt-5.5", {});
    const body = OpenAIRequestBody.assert(JSON.parse(req.body));
    const content = body.messages[0]?.content;
    if (!Array.isArray(content)) {
      throw new Error("expected multimodal content array");
    }
    expect(content).toEqual([
      { type: "text", text: "Summarize the attached document." },
      {
        type: "file",
        file: {
          filename: "document.pdf",
          file_data: "data:application/pdf;base64,JVBERi0xLjQK",
        },
      },
    ]);
  });

  test("rejects a non-PDF base64 document mimeType", () => {
    const messages: ConversationTurn[] = [
      {
        role: "user",
        content: [
          {
            type: "document",
            source: {
              kind: "base64",
              mimeType: "text/plain",
              data: "aGVsbG8=",
            },
          },
        ],
        timestamp: 1000,
      },
    ];

    expect(() => adapter.buildRequest(messages, "gpt-5.5", {})).toThrow(
      /application\/pdf only/,
    );
    expect(() => adapter.buildRequest(messages, "gpt-5.5", {})).toThrow(
      /text\/plain/,
    );
  });

  test("emits a file-reference document as file_id", () => {
    const messages: ConversationTurn[] = [
      {
        role: "user",
        content: [
          {
            type: "document",
            source: {
              kind: "file-reference",
              mimeType: "application/pdf",
              reference: "file-abc123",
            },
          },
        ],
        timestamp: 1000,
      },
    ];

    const req = adapter.buildRequest(messages, "gpt-5.5", {});
    const body = OpenAIRequestBody.assert(JSON.parse(req.body));
    const content = body.messages[0]?.content;
    if (!Array.isArray(content)) {
      throw new Error("expected multimodal content array");
    }
    expect(content).toEqual([
      {
        type: "file",
        file: { file_id: "file-abc123" },
      },
    ]);
  });

  test("rejects a url document source with a message naming the url", () => {
    const messages: ConversationTurn[] = [
      {
        role: "user",
        content: [
          {
            type: "document",
            source: {
              kind: "url",
              mimeType: "application/pdf",
              url: "https://example.com/report.pdf",
            },
          },
        ],
        timestamp: 1000,
      },
    ];

    expect(() => adapter.buildRequest(messages, "gpt-5.5", {})).toThrow(
      /url document sources/,
    );
    expect(() => adapter.buildRequest(messages, "gpt-5.5", {})).toThrow(
      /https:\/\/example\.com\/report\.pdf/,
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

      expect(() => adapter.buildRequest(messages, "gpt-5.5", {})).toThrow(
        new RegExp(`${blockType} content blocks`),
      );
    },
  );

  test("throws on a mixed-content assistant turn that includes code execution", () => {
    // The assistant-role detection loop must fire even when valid text
    // and tool_call blocks sit alongside the code block; a regression
    // that moves the check after the field filters would pass the
    // single-block test but must fail here.
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

    expect(() => adapter.buildRequest(messages, "gpt-5.5", {})).toThrow(
      /code_execution_request content blocks/,
    );
  });

  test("throws on an assistant turn carrying a refusal content block", () => {
    // Refusals parse from delta.refusal but have no OpenAI input-message
    // shape; fail at the marshaling boundary rather than emit
    // content: null.
    const messages: ConversationTurn[] = [
      {
        role: "assistant",
        content: [{ type: "refusal", reason: "I cannot help with that." }],
        timestamp: 1000,
      },
    ];

    expect(() => adapter.buildRequest(messages, "gpt-5.5", {})).toThrow(
      /refusal content blocks/,
    );
  });

  test("throws on a user turn carrying a refusal content block", () => {
    // User-role has no wire shape for refusals either; fail loudly
    // rather than emit a `null` part.
    const messages: ConversationTurn[] = [
      {
        role: "user",
        content: [{ type: "refusal", reason: "Earlier refusal." }],
        timestamp: 1000,
      },
    ];

    expect(() => adapter.buildRequest(messages, "gpt-5.5", {})).toThrow(
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

    const req = adapter.buildRequest(messages, "gpt-5.5", {});
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

    const req = adapter.buildRequest(messages, "gpt-5.5", {});
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
    // Regression: a tool_call before any text must allocate a fresh
    // block index, not reuse 0 — otherwise the later text delta
    // collides in the harness's per-index map and the whole turn
    // fails instead of yielding [tool_call, text].
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
    // Two parallel tool calls interleave deltas at indices 0 and 1; the
    // parser must propagate each delta's wire `index` so the harness
    // routes fragments to the right tool. Collapsing both to 0 would
    // merge the second tool's args onto the first's accumulator.
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

  test("usage riding a choice-bearing chunk carries cacheRead and thinking", async () => {
    // Some relays attach the final usage to the last content-bearing
    // chunk instead of a choices-empty one; the detail sub-objects
    // (cacheRead/thinking) must still surface, not zero.
    const events = await parseWire(adapter, [
      wire.openai.raw(
        'data: {"choices":[{"index":0,"delta":{},"finish_reason":"stop"}],' +
          '"usage":{"prompt_tokens":50,"completion_tokens":20,' +
          '"prompt_tokens_details":{"cached_tokens":30},' +
          '"completion_tokens_details":{"reasoning_tokens":8}}}\n\n',
      ),
    ]);
    const usage = events.find((e) => e.type === "inference.usage");
    expect(usage?.type).toBe("inference.usage");
    if (usage?.type === "inference.usage") {
      expect(usage.data.usage.input).toBe(50);
      expect(usage.data.usage.output).toBe(20);
      expect(usage.data.usage.cacheRead).toBe(30);
      expect(usage.data.usage.thinking).toBe(8);
    }
  });

  test("parses Fireworks-shaped tool-call deltas with null name/id on follow-up fragments", async () => {
    // Fireworks emits `id: null` and `function.name: null` on tool-call
    // deltas after the start; the schema must accept null and the
    // consumer must normalise it to undefined, or argsBuffer never
    // accumulates (the failure mode behind `arguments: {}` in kimi-k2.6).
    //
    // Hand-rolled via `wire.openai.raw()`: the typed wire DSL helpers
    // always emit the canonical shape, never the Fireworks variant.
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

    // One start (chunk 1) plus two fragments (chunks 2-3); chunk 1's
    // empty `arguments: ""` is skipped by the zero-length fragment check.
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
    // One chunk carrying both a start signal and a non-empty argument
    // fragment must emit both events — what Fireworks does on the first
    // fragment delta after the bare start.
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
    // `chunk()` always emits a non-empty choices entry (plus usage when
    // supplied); `{choices: []}` alone requires `raw()`.
    const events = await parseWire(adapter, [
      wire.openai.raw('data: {"choices":[]}\n\n'),
    ]);
    expect(events).toEqual([]);
  });

  test("throws ProtocolMismatchError on malformed JSON in SSE payload", () => {
    // The harness maps this throw to inference.error with category
    // "protocol_mismatch"; the raw payload rides in error.raw.
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
    // `delta.role: 42` is well-formed JSON but rejects the schema; the
    // error carries the parsed object in `raw` and the arktype summary
    // in `message` for audit logs.
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
    // Text takes index 0; the refusal takes the next free index, matching
    // the harness's per-index routing contract.
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
    const req = adapter.buildRequest(conversation, "gpt-5.5", {});
    const body = OpenAIRequestBody.assert(JSON.parse(req.body));
    expect(body.response_format).toBeUndefined();
  });

  test("sets reasoning_effort none for gpt-5.6 tool calls and omits it for gpt-5.5", () => {
    const tools = [
      {
        name: "get_weather",
        description: "Look up weather",
        inputSchema: {
          type: "object",
          properties: { location: { type: "string" } },
          required: ["location"],
        },
      },
    ];
    for (const model of [
      "gpt-5.6-sol",
      "gpt-5.6-terra",
      "gpt-5.6-luna",
    ] as const) {
      const req = adapter.buildRequest(conversation, model, { tools });
      const body = OpenAIRequestBody.assert(JSON.parse(req.body));
      expect(body.reasoning_effort).toBe("none");
      expect(body.tools).toBeDefined();
    }
    const legacy = adapter.buildRequest(conversation, "gpt-5.5", { tools });
    const legacyBody = OpenAIRequestBody.assert(JSON.parse(legacy.body));
    expect(legacyBody.reasoning_effort).toBeUndefined();
  });

  test("translates responseFormat.kind=text to { type: 'text' }", () => {
    const req = adapter.buildRequest(conversation, "gpt-5.5", {
      responseFormat: { kind: "text" },
    });
    const body = OpenAIRequestBody.assert(JSON.parse(req.body));
    expect(body.response_format).toEqual({ type: "text" });
  });

  test("translates responseFormat.kind=json to { type: 'json_object' }", () => {
    const req = adapter.buildRequest(conversation, "gpt-5.5", {
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
    const req = adapter.buildRequest(conversation, "gpt-5.5", {
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
    const req = adapter.buildRequest(conversation, "gpt-5.5", {
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
    const req = adapterInstance.buildRequest(turns, "gpt-5.5", {});
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
    // Empty string is a present value: it wins the precedence slot, the
    // length gate drops it, and `reasoning` is never read. "First
    // non-empty wins" must not surface "xyz".
    const events = await parseWire(createOpenAIAdapter(TEST_SOURCE), [
      wire.openai.chunk({ reasoningContent: "", reasoning: "xyz" }),
    ]);
    expect(events).toEqual([]);
  });

  test("a null reasoning field is skipped so a lower-precedence field is read", async () => {
    // A null `reasoning_content` falls through to the next field (the
    // `??` null-skip); the wire DSL can't emit null, so this is a raw chunk.
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

const JSON_SOURCE: InferenceSource = {
  id: "openai:test-model",
  provider: "openai",
  baseURL: "https://test.invalid/v1",
  credentialId: "test",
  model: "test-model",
};

// Drives a response body through the real harness accumulator; the
// content-type selects the JSON-vs-SSE decode path. Asserting the
// decoded turn matters: the accumulator drops unmodeled events and
// unmatched tool deltas.
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
    scheduler: createDefaultScheduler(),
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
    nextSeq: () => ++seq,
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

function sseBody(parts: Uint8Array[]): string {
  const dec = new TextDecoder();
  return parts.map((p) => dec.decode(p)).join("");
}

describe("createOpenAIAdapter — parseJSONResponse (non-streaming)", () => {
  test("decodes plain text and the full usage detail sub-objects", async () => {
    const body = JSON.stringify({
      object: "chat.completion",
      model: "test-model",
      choices: [
        {
          index: 0,
          message: {
            role: "assistant",
            content: "The capital of France is Paris.",
          },
          finish_reason: "stop",
        },
      ],
      usage: {
        prompt_tokens: 19,
        completion_tokens: 23,
        prompt_tokens_details: { cached_tokens: 5 },
        completion_tokens_details: { reasoning_tokens: 7 },
      },
    });
    const { turn, events } = await driveTurn(body);
    const t = requireTurn(turn);
    expect(blocksOfType(t, "text").map((b) => b.text)).toEqual([
      "The capital of France is Paris.",
    ]);
    const done = events.find(
      (e): e is Extract<InferenceEvent, { type: "inference.done" }> =>
        e.type === "inference.done",
    );
    // cacheRead and thinking prove the full toInferenceUsage mapping is used,
    // not the lossy in-chunk mapping that zeroes them.
    expect(done?.data.usage.input).toBe(19);
    expect(done?.data.usage.output).toBe(23);
    expect(done?.data.usage.cacheRead).toBe(5);
    expect(done?.data.usage.thinking).toBe(7);
  });

  test("decodes tool_calls with empty content into a tool call at index 0", async () => {
    const body = JSON.stringify({
      object: "chat.completion",
      model: "test-model",
      choices: [
        {
          index: 0,
          message: {
            role: "assistant",
            content: "",
            tool_calls: [
              {
                index: 0,
                id: "call_1",
                type: "function",
                function: {
                  name: "get_weather",
                  arguments: '{"location":"Boston, MA"}',
                },
              },
            ],
          },
          finish_reason: "tool_calls",
        },
      ],
      usage: { prompt_tokens: 5, completion_tokens: 3 },
    });
    const t = requireTurn((await driveTurn(body)).turn);
    // Empty content must not produce a text block or claim an index.
    expect(blocksOfType(t, "text")).toHaveLength(0);
    const calls = blocksOfType(t, "tool_call");
    expect(calls).toHaveLength(1);
    const call = calls[0];
    if (call === undefined) throw new Error("expected a tool call");
    expect(call.name).toBe("get_weather");
    expect(call.id).toBe("call_1");
    expect(call.arguments).toEqual({ location: "Boston, MA" });
  });

  test("reasoning precedence: an empty reasoning_content shadows a populated reasoning", async () => {
    // Same quirk as streaming: the first non-null field claims the slot,
    // the length gate drops the empty value, so neither field emits.
    const body = JSON.stringify({
      object: "chat.completion",
      model: "test-model",
      choices: [
        {
          index: 0,
          message: {
            role: "assistant",
            reasoning_content: "",
            reasoning: "should be shadowed",
            content: "answer",
          },
          finish_reason: "stop",
        },
      ],
      usage: { prompt_tokens: 5, completion_tokens: 3 },
    });
    const t = requireTurn((await driveTurn(body)).turn);
    expect(blocksOfType(t, "thinking")).toHaveLength(0);
    expect(blocksOfType(t, "text").map((b) => b.text)).toEqual(["answer"]);
  });

  test("surfaces a protocol mismatch on a non-completion body", async () => {
    // A streaming chunk shape must not decode as a non-streaming completion.
    const body = JSON.stringify({
      object: "chat.completion.chunk",
      choices: [{ index: 0, delta: { content: "x" } }],
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

describe("createOpenAIAdapter — streaming vs non-streaming parity", () => {
  // A replayed non-streaming capture must decode to the same turn as
  // its streaming sibling; the SSE fixture is built in natural arrival
  // order (reasoning, content, tool call), which the JSON field-walk
  // reproduces.
  test("a reasoning + text + tool_call turn decodes identically through both paths", async () => {
    const jsonBody = JSON.stringify({
      object: "chat.completion",
      model: "test-model",
      choices: [
        {
          index: 0,
          message: {
            role: "assistant",
            reasoning_content: "thinking about weather",
            content: "Let me check.",
            tool_calls: [
              {
                index: 0,
                id: "call_1",
                type: "function",
                function: {
                  name: "get_weather",
                  arguments: '{"location":"Boston"}',
                },
              },
            ],
          },
          finish_reason: "tool_calls",
        },
      ],
      usage: {
        prompt_tokens: 20,
        completion_tokens: 10,
        prompt_tokens_details: { cached_tokens: 4 },
        completion_tokens_details: { reasoning_tokens: 6 },
      },
    });

    const streamBody = sseBody([
      wire.openai.chunk({ reasoningContent: "thinking about weather" }),
      wire.openai.chunk({ content: "Let me check." }),
      wire.openai.toolCallStart(0, "call_1", "get_weather"),
      wire.openai.toolCallArgumentsDelta(0, '{"location":"Boston"}'),
      wire.openai.usageChunk({
        promptTokens: 20,
        completionTokens: 10,
        cachedTokens: 4,
        reasoningTokens: 6,
      }),
      wire.openai.done(),
    ]);

    const jsonResult = await driveTurn(jsonBody, "application/json");
    const streamResult = await driveTurn(streamBody, "text/event-stream");

    expect(jsonResult.events.some((e) => e.type === "inference.error")).toBe(
      false,
    );
    expect(streamResult.events.some((e) => e.type === "inference.error")).toBe(
      false,
    );

    const jt = requireTurn(jsonResult.turn);
    const st = requireTurn(streamResult.turn);
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
});

describe("createOpenAIAdapter — parseJSONResponse parallel tool calls", () => {
  // Regression: non-streaming tool_calls[] items omit `index`; two
  // parallel calls must land on distinct block indices via array
  // position, not collapse onto slot 0. The streaming form is the
  // parity control.
  test("two indexless parallel tool calls decode to two distinct calls", async () => {
    const jsonBody = JSON.stringify({
      object: "chat.completion",
      model: "test-model",
      choices: [
        {
          index: 0,
          message: {
            role: "assistant",
            content: "",
            tool_calls: [
              {
                id: "call_a",
                type: "function",
                function: { name: "get_weather", arguments: '{"city":"A"}' },
              },
              {
                id: "call_b",
                type: "function",
                function: { name: "get_time", arguments: '{"tz":"B"}' },
              },
            ],
          },
          finish_reason: "tool_calls",
        },
      ],
      usage: { prompt_tokens: 5, completion_tokens: 3 },
    });

    const streamBody = sseBody([
      wire.openai.toolCallStart(0, "call_a", "get_weather"),
      wire.openai.toolCallArgumentsDelta(0, '{"city":"A"}'),
      wire.openai.toolCallStart(1, "call_b", "get_time"),
      wire.openai.toolCallArgumentsDelta(1, '{"tz":"B"}'),
      wire.openai.usageChunk({ promptTokens: 5, completionTokens: 3 }),
      wire.openai.done(),
    ]);

    const jsonResult = await driveTurn(jsonBody, "application/json");
    const streamResult = await driveTurn(streamBody, "text/event-stream");

    expect(jsonResult.events.some((e) => e.type === "inference.error")).toBe(
      false,
    );

    const jt = requireTurn(jsonResult.turn);
    const calls = blocksOfType(jt, "tool_call");
    expect(calls).toHaveLength(2);
    expect(calls.map((c) => c.name)).toEqual(["get_weather", "get_time"]);
    expect(calls.map((c) => c.id)).toEqual(["call_a", "call_b"]);
    expect(calls.map((c) => c.arguments)).toEqual([{ city: "A" }, { tz: "B" }]);

    // Parity with the streaming form (which carries distinct wire indices).
    const st = requireTurn(streamResult.turn);
    expect(jt.content).toEqual(st.content);
  });
});
