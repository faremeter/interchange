import { type } from "arktype";
import { describe, test, expect } from "bun:test";
import { wire } from "@intx/inference-testing";
import {
  parseSSE,
  ProtocolMismatchError,
  type ProviderAdapter,
} from "@intx/inference";
import { createAnthropicAdapter } from "@intx/inference/providers";
import type {
  ConversationTurn,
  InferenceEvent,
  LastCycleSource,
} from "@intx/types/runtime";

const TEST_SOURCE: LastCycleSource = {
  sourceId: "test-anthropic",
  provider: "anthropic",
  model: "test-anthropic-model",
};

const adapter = createAnthropicAdapter(TEST_SOURCE);

const AnthropicContentBlock = type({
  type: "string",
  "text?": "string",
  "thinking?": "string",
  "signature?": "string",
  "id?": "string",
  "name?": "string",
  "data?": "string",
  // `source`'s shape depends on the variant; validated at each test
  // site via AnthropicMediaSourceBase64 / AnthropicMediaSourceFile.
  "source?": "unknown",
  "title?": "string",
  "context?": "string",
  "cache_control?": { type: "string" },
});

const AnthropicMediaSourceBase64 = type({
  type: "'base64'",
  media_type: "string",
  data: "string",
});

const AnthropicMediaSourceFile = type({
  type: "'file'",
  file_id: "string",
});

const AnthropicMediaSourceUrl = type({
  type: "'url'",
  url: "string",
});

const AnthropicMessage = type({
  role: "string",
  content: AnthropicContentBlock.array(),
});

const AnthropicThinking = type({
  type: "string",
  "budget_tokens?": "number",
});

const AnthropicOutputConfig = type({
  effort: "string",
});

const AnthropicTool = type({
  name: "string",
  "description?": "string",
  input_schema: "unknown",
  "cache_control?": { type: "string" },
});

const AnthropicSystemBlock = type({
  type: "string",
  text: "string",
  "cache_control?": { type: "string" },
});

const AnthropicRequestBody = type({
  model: "string",
  max_tokens: "number",
  messages: AnthropicMessage.array(),
  stream: "boolean",
  "system?": AnthropicSystemBlock.array(),
  "thinking?": AnthropicThinking,
  "output_config?": AnthropicOutputConfig,
  "tools?": AnthropicTool.array(),
  "temperature?": "number",
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

describe("Anthropic adapter: buildRequest", () => {
  test("builds a request with required fields", () => {
    const messages: ConversationTurn[] = [
      {
        role: "user",
        content: [{ type: "text", text: "Hello" }],
        timestamp: 1000,
      },
    ];

    const req = adapter.buildRequest(
      messages,
      "claude-3-5-sonnet-20241022",
      {},
    );

    expect(req.url).toBe("/v1/messages");
    expect(req.headers["content-type"]).toBe("application/json");
    expect(req.headers["anthropic-version"]).toBe("2023-06-01");

    const body = AnthropicRequestBody.assert(JSON.parse(req.body));
    expect(body.model).toBe("claude-3-5-sonnet-20241022");
    expect(body.stream).toBe(true);
    expect(typeof body.max_tokens).toBe("number");
  });

  test("extracts system messages into top-level system field", () => {
    const messages: ConversationTurn[] = [
      {
        role: "system",
        content: [{ type: "text", text: "You are helpful." }],
        timestamp: 1000,
      },
      {
        role: "user",
        content: [{ type: "text", text: "Hi." }],
        timestamp: 1000,
      },
    ];

    const req = adapter.buildRequest(
      messages,
      "claude-3-5-sonnet-20241022",
      {},
    );
    const body = AnthropicRequestBody.assert(JSON.parse(req.body));

    expect(body.system).toEqual([
      {
        type: "text",
        text: "You are helpful.",
        cache_control: { type: "ephemeral" },
      },
    ]);
    // System message should not appear in messages array.
    expect(body.messages).toHaveLength(1);
    expect(body.messages[0]?.role).toBe("user");
  });

  test("options.systemPrompt overrides system messages", () => {
    const messages: ConversationTurn[] = [
      {
        role: "system",
        content: [{ type: "text", text: "Original system." }],
        timestamp: 1000,
      },
      {
        role: "user",
        content: [{ type: "text", text: "Hi." }],
        timestamp: 1000,
      },
    ];

    const req = adapter.buildRequest(messages, "claude-3-5-sonnet-20241022", {
      systemPrompt: "Override system.",
    });
    const body = AnthropicRequestBody.assert(JSON.parse(req.body));
    expect(body.system).toEqual([
      {
        type: "text",
        text: "Override system.",
        cache_control: { type: "ephemeral" },
      },
    ]);
  });

  test("includes classic thinking config when enabled on budget-token models", () => {
    const messages: ConversationTurn[] = [
      {
        role: "user",
        content: [{ type: "text", text: "Think deeply." }],
        timestamp: 1000,
      },
    ];

    const req = adapter.buildRequest(messages, "claude-3-7-sonnet-20250219", {
      thinking: { enabled: true, budgetTokens: 2048 },
    });
    const body = AnthropicRequestBody.assert(JSON.parse(req.body));
    expect(body.thinking?.type).toBe("enabled");
    expect(body.thinking?.budget_tokens).toBe(2048);
    expect(body.output_config).toBeUndefined();
  });

  test("uses adaptive thinking for sonnet-5, opus-5, and fable-5", () => {
    const messages: ConversationTurn[] = [
      {
        role: "user",
        content: [{ type: "text", text: "Think deeply." }],
        timestamp: 1000,
      },
    ];
    for (const model of [
      "claude-sonnet-5",
      "claude-opus-5",
      "claude-fable-5",
    ] as const) {
      const req = adapter.buildRequest(messages, model, {
        thinking: { enabled: true, budgetTokens: 2048 },
      });
      const body = AnthropicRequestBody.assert(JSON.parse(req.body));
      expect(body.thinking).toEqual({ type: "adaptive" });
      expect(body.thinking).not.toHaveProperty("budget_tokens");
      expect(body.output_config).toEqual({ effort: "high" });
    }
  });

  test("echoes thinking block signature back in the request body", () => {
    // Follow-up thinking blocks must carry the signature the API
    // issued at generation; without it the next request 400s with
    // "messages.N.content.M.thinking.signature: Field required".
    const messages: ConversationTurn[] = [
      {
        role: "user",
        content: [{ type: "text", text: "Question." }],
        timestamp: 1000,
      },
      {
        role: "assistant",
        content: [
          {
            type: "thinking",
            thinking: "Reasoning about the question...",
            signature: "sig_round_trip",
          },
          { type: "text", text: "Answer." },
        ],
        timestamp: 1100,
        model: "claude-sonnet-5",
      },
      {
        role: "user",
        content: [{ type: "text", text: "Follow-up." }],
        timestamp: 1200,
      },
    ];

    const req = adapter.buildRequest(messages, "claude-sonnet-5", {});
    const body = AnthropicRequestBody.assert(JSON.parse(req.body));
    const assistantMsg = body.messages.find((m) => m.role === "assistant");
    if (assistantMsg === undefined) {
      throw new Error("expected an assistant message in the request body");
    }
    const thinkingBlock = assistantMsg.content.find(
      (b) => b.type === "thinking",
    );
    if (thinkingBlock === undefined) {
      throw new Error("expected a thinking block in the assistant message");
    }
    expect(thinkingBlock.thinking).toBe("Reasoning about the question...");
    expect(thinkingBlock.signature).toBe("sig_round_trip");
  });

  test("converts tool_call blocks to tool_use type", () => {
    const messages: ConversationTurn[] = [
      {
        role: "assistant",
        content: [
          {
            type: "tool_call",
            id: "toolu_01",
            name: "read_file",
            arguments: { path: "/etc/hosts" },
          },
        ],
        timestamp: 1000,
      },
    ];

    const req = adapter.buildRequest(
      messages,
      "claude-3-5-sonnet-20241022",
      {},
    );
    const body = AnthropicRequestBody.assert(JSON.parse(req.body));
    const block = body.messages[0]?.content[0];
    expect(block?.type).toBe("tool_use");
    expect(block?.id).toBe("toolu_01");
    expect(block?.name).toBe("read_file");
  });

  test("serializes tool definitions with Anthropic wire format", () => {
    const messages: ConversationTurn[] = [
      {
        role: "user",
        content: [{ type: "text", text: "Hi." }],
        timestamp: 1000,
      },
    ];

    const req = adapter.buildRequest(messages, "claude-3-5-sonnet-20241022", {
      tools: [
        {
          name: "greet",
          description: "Greet someone",
          inputSchema: {
            type: "object",
            properties: { name: { type: "string" } },
          },
        },
      ],
    });
    const body = AnthropicRequestBody.assert(JSON.parse(req.body));
    expect(body.tools).toHaveLength(1);
    expect(body.tools?.[0]?.name).toBe("greet");
    expect(body.tools?.[0]?.description).toBe("Greet someone");
    // Anthropic uses input_schema, not inputSchema.
    expect(body.tools?.[0]?.input_schema).toEqual({
      type: "object",
      properties: { name: { type: "string" } },
    });
  });

  test("omits tools key when tools array is empty", () => {
    const messages: ConversationTurn[] = [
      {
        role: "user",
        content: [{ type: "text", text: "Hi." }],
        timestamp: 1000,
      },
    ];

    const req = adapter.buildRequest(messages, "claude-3-5-sonnet-20241022", {
      tools: [],
    });
    const body = AnthropicRequestBody.assert(JSON.parse(req.body));
    expect("tools" in body).toBe(false);
  });

  test("omits tools key when tools is undefined", () => {
    const messages: ConversationTurn[] = [
      {
        role: "user",
        content: [{ type: "text", text: "Hi." }],
        timestamp: 1000,
      },
    ];

    const req = adapter.buildRequest(
      messages,
      "claude-3-5-sonnet-20241022",
      {},
    );
    const body = AnthropicRequestBody.assert(JSON.parse(req.body));
    expect("tools" in body).toBe(false);
  });

  test("uses max_tokens from options", () => {
    const messages: ConversationTurn[] = [
      {
        role: "user",
        content: [{ type: "text", text: "Hi." }],
        timestamp: 1000,
      },
    ];

    const req = adapter.buildRequest(messages, "claude-3-5-sonnet-20241022", {
      maxTokens: 512,
    });
    const body = AnthropicRequestBody.assert(JSON.parse(req.body));
    expect(body.max_tokens).toBe(512);
  });

  test("emits a file-reference image as { type: file, file_id }", () => {
    // Files are identified by id alone; the content-type is encoded
    // server-side, so `mimeType` is not propagated.
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

    const req = adapter.buildRequest(
      messages,
      "claude-3-5-sonnet-20241022",
      {},
    );
    const body = AnthropicRequestBody.assert(JSON.parse(req.body));
    const block = body.messages[0]?.content[0];
    if (block?.type !== "image") {
      throw new Error("expected image block in the request");
    }
    const source = AnthropicMediaSourceFile.assert(block.source);
    expect(source.type).toBe("file");
    expect(source.file_id).toBe("file_abc123");
  });

  test("emits a URL image as { type: url, url }", () => {
    // Anthropic accepts public URLs for image and document inputs; it
    // fetches the URL itself and infers the content type, so
    // `mimeType` is not propagated.
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

    const req = adapter.buildRequest(
      messages,
      "claude-3-5-sonnet-20241022",
      {},
    );
    const body = AnthropicRequestBody.assert(JSON.parse(req.body));
    const block = body.messages[0]?.content[0];
    if (block?.type !== "image") {
      throw new Error("expected image block in the request");
    }
    const source = AnthropicMediaSourceUrl.assert(block.source);
    expect(source.type).toBe("url");
    expect(source.url).toBe("https://example.com/cat.png");
  });

  test("emits a URL document as { type: url, url }", () => {
    // The url variant accepts documents under the same shape as
    // images; no media_type goes on the wire.
    const messages: ConversationTurn[] = [
      {
        role: "user",
        content: [
          {
            type: "document",
            source: {
              kind: "url",
              mimeType: "application/pdf",
              url: "https://example.com/manual.pdf",
            },
          },
        ],
        timestamp: 1000,
      },
    ];

    const req = adapter.buildRequest(
      messages,
      "claude-3-5-sonnet-20241022",
      {},
    );
    const body = AnthropicRequestBody.assert(JSON.parse(req.body));
    const block = body.messages[0]?.content[0];
    if (block?.type !== "document") {
      throw new Error("expected document block in the request");
    }
    const source = AnthropicMediaSourceUrl.assert(block.source);
    expect(source.type).toBe("url");
    expect(source.url).toBe("https://example.com/manual.pdf");
  });

  test("emits a base64 document as { type: document, source: { type: base64, media_type, data } }", () => {
    // The documented multimodal-pdf shape: `media_type:
    // "application/pdf"` and a base64 payload.
    const messages: ConversationTurn[] = [
      {
        role: "user",
        content: [
          {
            type: "document",
            source: {
              kind: "base64",
              mimeType: "application/pdf",
              data: "JVBERi0xLjQK", // truncated PDF magic
            },
          },
        ],
        timestamp: 1000,
      },
    ];

    const req = adapter.buildRequest(
      messages,
      "claude-3-5-sonnet-20241022",
      {},
    );
    const body = AnthropicRequestBody.assert(JSON.parse(req.body));
    const block = body.messages[0]?.content[0];
    if (block?.type !== "document") {
      throw new Error("expected document block in the request");
    }
    const source = AnthropicMediaSourceBase64.assert(block.source);
    expect(source.type).toBe("base64");
    expect(source.media_type).toBe("application/pdf");
    expect(source.data).toBe("JVBERi0xLjQK");
  });

  test("emits document title and context when present", () => {
    // Grounded against a live Anthropic probe that accepted title and
    // context as siblings of source on the document content block.
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
            title: "Q3 Invoice",
            context:
              "Internal accounting document used to test citation grounding.",
          },
        ],
        timestamp: 1000,
      },
    ];

    const req = adapter.buildRequest(
      messages,
      "claude-3-5-sonnet-20241022",
      {},
    );
    const body = AnthropicRequestBody.assert(JSON.parse(req.body));
    const block = body.messages[0]?.content[0];
    if (block?.type !== "document") {
      throw new Error("expected document block in the request");
    }
    expect(block.title).toBe("Q3 Invoice");
    expect(block.context).toBe(
      "Internal accounting document used to test citation grounding.",
    );
  });

  test("drops document title and context when absent", () => {
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

    const req = adapter.buildRequest(
      messages,
      "claude-3-5-sonnet-20241022",
      {},
    );
    const body = AnthropicRequestBody.assert(JSON.parse(req.body));
    const block = body.messages[0]?.content[0];
    if (block?.type !== "document") {
      throw new Error("expected document block in the request");
    }
    expect(block).not.toHaveProperty("title");
    expect(block).not.toHaveProperty("context");
  });

  test("emits a file-reference document as { type: file, file_id }", () => {
    const messages: ConversationTurn[] = [
      {
        role: "user",
        content: [
          {
            type: "document",
            source: {
              kind: "file-reference",
              mimeType: "application/pdf",
              reference: "file_doc_456",
            },
          },
        ],
        timestamp: 1000,
      },
    ];

    const req = adapter.buildRequest(
      messages,
      "claude-3-5-sonnet-20241022",
      {},
    );
    const body = AnthropicRequestBody.assert(JSON.parse(req.body));
    const block = body.messages[0]?.content[0];
    if (block?.type !== "document") {
      throw new Error("expected document block in the request");
    }
    const source = AnthropicMediaSourceFile.assert(block.source);
    expect(source.type).toBe("file");
    expect(source.file_id).toBe("file_doc_456");
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

      expect(() =>
        adapter.buildRequest(messages, "claude-3-5-sonnet-20241022", {}),
      ).toThrow(new RegExp(`${blockType} content blocks`));
    },
  );

  test.each(["audio", "video", "document"] as const)(
    "rejects a %s content block inside a tool_result",
    (blockType) => {
      const messages: ConversationTurn[] = [
        {
          role: "user",
          content: [
            {
              type: "tool_result",
              callId: "call_xyz",
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
            },
          ],
          timestamp: 1000,
        },
      ];

      // tool_result.content accepts only text and image blocks;
      // others surface at the marshaling site, not as an opaque
      // Anthropic HTTP error.
      expect(() =>
        adapter.buildRequest(messages, "claude-3-5-sonnet-20241022", {}),
      ).toThrow(
        new RegExp(
          `does not handle ${blockType} content blocks inside tool_result`,
        ),
      );
    },
  );

  test("rewrites safety_rating history to text for request marshaling", () => {
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
    const req = adapter.buildRequest(
      messages,
      "claude-3-5-sonnet-20241022",
      {},
    );
    const body = AnthropicRequestBody.assert(JSON.parse(req.body));
    expect(body.messages).toHaveLength(3);
    expect(body.messages.map((m) => m.role)).toEqual([
      "user",
      "assistant",
      "user",
    ]);
    const assistant = body.messages[1];
    if (assistant === undefined || !Array.isArray(assistant.content)) {
      throw new Error("expected assistant content array");
    }
    const TextBlock = type({ type: "'text'", text: "string" });
    const texts = assistant.content.flatMap((b) => {
      const parsed = TextBlock(b);
      return parsed instanceof type.errors ? [] : [parsed.text];
    });
    expect(texts).toContain("Request blocked: PROHIBITED_CONTENT");
  });

  test("echoes a redacted_thinking content block back verbatim", () => {
    // redacted_thinking arrives as a one-shot start event with an
    // opaque `data` blob that must echo back verbatim on follow-up
    // turns; mutation or omission 400s or silently corrupts context.
    const data = "EncryptedOpaqueBlobAAAA==";
    const messages: ConversationTurn[] = [
      {
        role: "assistant",
        content: [
          {
            type: "redacted_thinking",
            data,
          },
        ],
        timestamp: 1000,
      },
    ];

    const req = adapter.buildRequest(
      messages,
      "claude-3-5-sonnet-20241022",
      {},
    );
    const body = AnthropicRequestBody.assert(JSON.parse(req.body));
    const assistantMsg = body.messages.find((m) => m.role === "assistant");
    if (assistantMsg === undefined) {
      throw new Error("expected an assistant message in the request body");
    }
    const block = assistantMsg.content.find(
      (b) => b.type === "redacted_thinking",
    );
    if (block === undefined) {
      throw new Error(
        "expected a redacted_thinking block in the assistant message",
      );
    }
    expect(block.data).toBe(data);
    // Negative: must not carry the legacy invalid shape
    // ({ type: "thinking", thinking: "", thinking_type: "redacted" }).
    expect(block.thinking).toBeUndefined();
    // The arktype schema does not list `thinking_type`, so the assert
    // call above already rejects any block carrying it.
  });

  test.each(["code_execution_request", "code_execution_result"] as const)(
    "rejects a %s content block in a request",
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

      expect(() =>
        adapter.buildRequest(messages, "claude-3-5-sonnet-20241022", {}),
      ).toThrow(new RegExp(`${blockType} content blocks`));
    },
  );

  test("rejects a citation content block in a request", () => {
    const messages: ConversationTurn[] = [
      {
        role: "user",
        content: [
          {
            type: "citation",
            citedText: "the answer",
            source: { uri: "https://example.com/" },
          },
        ],
        timestamp: 1000,
      },
    ];

    expect(() =>
      adapter.buildRequest(messages, "claude-3-5-sonnet-20241022", {}),
    ).toThrow(/citation content blocks/);
  });

  test("emits a file-reference image inside a tool_result", () => {
    const messages: ConversationTurn[] = [
      {
        role: "user",
        content: [
          {
            type: "tool_result",
            callId: "call_xyz",
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
          },
        ],
        timestamp: 1000,
      },
    ];

    const req = adapter.buildRequest(
      messages,
      "claude-3-5-sonnet-20241022",
      {},
    );
    // The request-body schema omits tool_result.content[].source
    // (deeply nested, variant-specific); walk in with type-guard
    // helpers instead of `as` casts.
    const parsed: unknown = JSON.parse(req.body);
    if (!isRecord(parsed)) throw new Error("expected body to be a JSON object");
    const msgs = parsed["messages"];
    if (!Array.isArray(msgs) || msgs.length === 0) {
      throw new Error("expected messages array");
    }
    const firstMsg = msgs[0];
    if (!isRecord(firstMsg)) throw new Error("expected first message");
    const content = firstMsg["content"];
    if (!Array.isArray(content) || content.length === 0) {
      throw new Error("expected content array");
    }
    const tr = content[0];
    if (!isRecord(tr) || tr["type"] !== "tool_result") {
      throw new Error("expected tool_result content[0]");
    }
    const trContent = tr["content"];
    if (!Array.isArray(trContent) || trContent.length === 0) {
      throw new Error("expected tool_result.content to be a non-empty array");
    }
    const inner = trContent[0];
    if (!isRecord(inner)) {
      throw new Error("expected tool_result.content[0] to be a JSON object");
    }
    expect(inner["type"]).toBe("image");
    const source = AnthropicMediaSourceFile.assert(inner["source"]);
    expect(source.type).toBe("file");
    expect(source.file_id).toBe("file_abc123");
  });
});

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

describe("Anthropic adapter: parseResponse", () => {
  test("throws ProtocolMismatchError on malformed JSON in SSE payload", () => {
    // The harness maps this to inference.error with category
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

  test("throws ProtocolMismatchError on schema-mismatched event", () => {
    // `{"type":42}` rejects the AnthropicSSEEvent schema; the error
    // carries the parsed object in raw and the arktype summary in
    // message.
    const malformed = '{"type":42}';

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
        expect(err.raw).toEqual({ type: 42 });
      }
    }
  });

  test("throws ProtocolMismatchError on input_json_delta with no preceding tool_use start", () => {
    // A delta for an unknown tool_use index means the upstream emitted
    // events out of order — a protocol violation, not a transport flake.
    const malformed =
      '{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\\"x\\":1}"}}';

    expect(() => adapter.parseResponse(malformed)).toThrow(
      ProtocolMismatchError,
    );

    try {
      adapter.parseResponse(malformed);
      throw new Error("expected ProtocolMismatchError to be thrown");
    } catch (err) {
      expect(err).toBeInstanceOf(ProtocolMismatchError);
      if (err instanceof ProtocolMismatchError) {
        expect(err.message).toContain("no preceding tool_use start");
      }
    }
  });

  test("parses text_delta", async () => {
    const events = await parseWire(adapter, [
      wire.anthropic.contentBlockDelta({
        index: 0,
        kind: "text_delta",
        text: "Hello",
      }),
    ]);
    expect(events).toHaveLength(1);
    const evt = events[0];
    expect(evt?.type).toBe("inference.text.delta");
    if (evt?.type === "inference.text.delta") {
      expect(evt.data.token).toBe("Hello");
    }
  });

  test("parses thinking_delta", async () => {
    const events = await parseWire(adapter, [
      wire.anthropic.contentBlockDelta({
        index: 0,
        kind: "thinking_delta",
        thinking: "reasoning...",
      }),
    ]);
    expect(events).toHaveLength(1);
    expect(events[0]?.type).toBe("inference.thinking.delta");
    if (events[0]?.type === "inference.thinking.delta") {
      expect(events[0].data.token).toBe("reasoning...");
    }
  });

  test("parses signature_delta into inference.block.signature", async () => {
    // The signature must propagate end-to-end; without it, follow-up
    // turns echoing the thinking block are rejected with
    // "messages.N.content.M.thinking.signature: Field required".
    const events = await parseWire(adapter, [
      wire.anthropic.contentBlockDelta({
        index: 0,
        kind: "signature_delta",
        signature: "sig_abc123",
      }),
    ]);
    expect(events).toHaveLength(1);
    expect(events[0]?.type).toBe("inference.block.signature");
    if (events[0]?.type === "inference.block.signature") {
      expect(events[0].data.signature).toBe("sig_abc123");
    }
  });

  test("parses redacted_thinking content_block_start into inference.thinking.redacted", async () => {
    // Redacted thinking is a one-shot in content_block_start (opaque
    // `data` blob, no deltas); surface it as
    // inference.thinking.redacted with the index propagated.
    const a = createAnthropicAdapter(TEST_SOURCE);
    const events = await parseWire(
      a,
      wire.anthropic.redactedThinkingBlock("OpaqueBlobXYZ==", 0),
    );
    const redactedEvents = events.filter(
      (e) => e.type === "inference.thinking.redacted",
    );
    expect(redactedEvents).toHaveLength(1);
    const ev = redactedEvents[0];
    if (ev?.type === "inference.thinking.redacted") {
      expect(ev.data.redactedThinking.type).toBe("redacted_thinking");
      expect(ev.data.redactedThinking.data).toBe("OpaqueBlobXYZ==");
      expect(ev.data.index).toBe(0);
    }
  });

  test("parses input_json_delta for tool arguments", async () => {
    // Per-test adapter instance: state for tool_use block index 1 must
    // come from the same adapter consuming the start event.
    const a = createAnthropicAdapter(TEST_SOURCE);
    const events = await parseWire(a, [
      wire.anthropic.contentBlockStart({
        index: 1,
        kind: "tool_use",
        id: "toolu_test",
        name: "write_file",
      }),
      wire.anthropic.contentBlockDelta({
        index: 1,
        kind: "input_json_delta",
        partialJson: '{"path":',
      }),
    ]);

    // Two events emitted: the start, then the delta. The original test only
    // asserted on the delta because it called parseResponse with the start
    // event in isolation first; the delta still carries the correct callId.
    const deltaEvents = events.filter(
      (e) => e.type === "inference.tool_call.delta",
    );
    expect(deltaEvents).toHaveLength(1);
    const evt = deltaEvents[0];
    if (evt?.type === "inference.tool_call.delta") {
      expect(evt.data.callId).toBe("toolu_test");
      expect(evt.data.argumentFragment).toBe('{"path":');
    }
  });

  test("parses content_block_start for tool_use", async () => {
    const events = await parseWire(adapter, [
      wire.anthropic.contentBlockStart({
        index: 0,
        kind: "tool_use",
        id: "toolu_01",
        name: "read_file",
      }),
    ]);
    expect(events).toHaveLength(1);
    expect(events[0]?.type).toBe("inference.tool_call.start");
    if (events[0]?.type === "inference.tool_call.start") {
      expect(events[0].data.callId).toBe("toolu_01");
      expect(events[0].data.name).toBe("read_file");
    }
  });

  test("parses message_start with usage", async () => {
    const events = await parseWire(adapter, [
      wire.anthropic.messageStart({
        usage: {
          inputTokens: 100,
          outputTokens: 0,
          cacheReadInputTokens: 50,
          cacheCreationInputTokens: 0,
        },
      }),
    ]);
    expect(events).toHaveLength(1);
    expect(events[0]?.type).toBe("inference.usage");
    if (events[0]?.type === "inference.usage") {
      expect(events[0].data.usage.input).toBe(100);
      expect(events[0].data.usage.cacheRead).toBe(50);
    }
  });

  test("parses message_delta with output usage", async () => {
    const events = await parseWire(adapter, [
      wire.anthropic.messageDelta({
        stopReason: "end_turn",
        outputTokens: 42,
      }),
    ]);
    expect(events).toHaveLength(1);
    expect(events[0]?.type).toBe("inference.usage");
    if (events[0]?.type === "inference.usage") {
      expect(events[0].data.usage.output).toBe(42);
    }
  });

  test("returns empty for ping events", async () => {
    const events = await parseWire(adapter, [wire.anthropic.ping()]);
    expect(events).toEqual([]);
  });

  test("returns empty for message_stop", async () => {
    const events = await parseWire(adapter, [wire.anthropic.messageStop()]);
    expect(events).toEqual([]);
  });

  test("returns empty for content_block_stop", async () => {
    const events = await parseWire(adapter, [
      wire.anthropic.contentBlockStop({ index: 0 }),
    ]);
    expect(events).toEqual([]);
  });

  test("tool call delta uses real callId when text precedes tool call", async () => {
    const a = createAnthropicAdapter(TEST_SOURCE);
    const events = await parseWire(a, [
      wire.anthropic.messageStart({
        usage: {
          inputTokens: 10,
          outputTokens: 0,
          cacheReadInputTokens: 0,
          cacheCreationInputTokens: 0,
        },
      }),
      wire.anthropic.contentBlockStart({ index: 0, kind: "text", text: "" }),
      wire.anthropic.contentBlockStart({
        index: 1,
        kind: "tool_use",
        id: "toolu_real_id",
        name: "write_file",
      }),
      wire.anthropic.contentBlockDelta({
        index: 1,
        kind: "input_json_delta",
        partialJson: '{"path":"test.ts"',
      }),
    ]);

    // The delta event is the assertion target; other events (usage, start)
    // are emitted earlier in the sequence.
    const deltaEvents = events.filter(
      (e) => e.type === "inference.tool_call.delta",
    );
    expect(deltaEvents).toHaveLength(1);
    const evt = deltaEvents[0];
    if (evt?.type === "inference.tool_call.delta") {
      expect(evt.data.callId).toBe("toolu_real_id");
      expect(evt.data.argumentFragment).toBe('{"path":"test.ts"');
    }
  });
});

describe("Anthropic adapter: quirks", () => {
  test("constructs with an absent quirks bag", () => {
    expect(() => createAnthropicAdapter(TEST_SOURCE)).not.toThrow();
  });

  test("constructs with an explicit undefined quirks bag", () => {
    expect(() => createAnthropicAdapter(TEST_SOURCE, undefined)).not.toThrow();
  });

  test("constructs with an empty quirks bag", () => {
    expect(() => createAnthropicAdapter(TEST_SOURCE, {})).not.toThrow();
  });

  test("rejects a populated quirks bag since it declares no quirks", () => {
    expect(() =>
      createAnthropicAdapter(TEST_SOURCE, { anything: true }),
    ).toThrow(/invalid quirks/);
  });
});
