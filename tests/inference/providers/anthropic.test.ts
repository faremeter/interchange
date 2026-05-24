import { type } from "arktype";
import { describe, test, expect } from "bun:test";
import { wire } from "@intx/inference-testing";
import {
  parseSSE,
  createAnthropicAdapter,
  ProtocolMismatchError,
  type ProviderAdapter,
} from "@intx/inference";
import type { ConversationTurn, InferenceEvent } from "@intx/types/runtime";

const adapter = createAnthropicAdapter();

const AnthropicContentBlock = type({
  type: "string",
  "text?": "string",
  "thinking?": "string",
  "signature?": "string",
  "id?": "string",
  "name?": "string",
  "cache_control?": { type: "string" },
});

const AnthropicMessage = type({
  role: "string",
  content: AnthropicContentBlock.array(),
});

const AnthropicThinking = type({
  type: "string",
  budget_tokens: "number",
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
  "tools?": AnthropicTool.array(),
  "temperature?": "number",
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

  test("includes thinking config when enabled", () => {
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
  });

  test("echoes thinking block signature back in the request body", () => {
    // Anthropic requires that any thinking block included in a
    // follow-up turn's history carries the cryptographic signature
    // the API issued when the block was originally generated. Without
    // the signature, the next request 400s with
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
        model: "claude-sonnet-4-6",
      },
      {
        role: "user",
        content: [{ type: "text", text: "Follow-up." }],
        timestamp: 1200,
      },
    ];

    const req = adapter.buildRequest(messages, "claude-sonnet-4-6", {});
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

  test("rejects a file-reference image source until Files API wiring lands", () => {
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

    expect(() =>
      adapter.buildRequest(messages, "claude-3-5-sonnet-20241022", {}),
    ).toThrow(/file-reference image sources\./);
  });

  test.each(["audio", "video", "document"] as const)(
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

      expect(() =>
        adapter.buildRequest(messages, "claude-3-5-sonnet-20241022", {}),
      ).toThrow(new RegExp(`${blockType} content blocks in tool results`));
    },
  );

  test("rejects a redacted_thinking content block (echo-back not yet wired)", () => {
    const messages: ConversationTurn[] = [
      {
        role: "assistant",
        content: [
          {
            type: "redacted_thinking",
            data: "EncryptedOpaqueBlobAAAA==",
          },
        ],
        timestamp: 1000,
      },
    ];

    expect(() =>
      adapter.buildRequest(messages, "claude-3-5-sonnet-20241022", {}),
    ).toThrow(/redacted_thinking content blocks/);
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

  test("rejects a file-reference image inside a tool_result", () => {
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

    expect(() =>
      adapter.buildRequest(messages, "claude-3-5-sonnet-20241022", {}),
    ).toThrow(/file-reference image sources in tool results/);
  });
});

describe("Anthropic adapter: parseResponse", () => {
  test("throws ProtocolMismatchError on malformed JSON in SSE payload", () => {
    // The harness's stream-error catch maps this to inference.error with
    // category "protocol_mismatch" via classifyStreamError. The raw
    // payload is carried in error.raw for operator inspection.
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
    // `{"type":42}` is well-formed JSON but rejects against the
    // AnthropicSSEEvent schema (type must be a string). The thrown
    // error carries the parsed object in raw and the arktype summary
    // in message.
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
    // The adapter tracks tool_use content-block IDs by index so that
    // subsequent input_json_delta events can resolve to the right
    // tool call. A delta for an unknown index means the upstream
    // emitted events out of order — a protocol violation, not a
    // transport flake.
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

  test("parses signature_delta into inference.thinking.signature", async () => {
    // Anthropic emits the thinking-block signature after the thinking
    // content stream. The signature must be propagated end-to-end —
    // without it, follow-up turns that echo the thinking block are
    // rejected by Anthropic with
    // "messages.N.content.M.thinking.signature: Field required".
    const events = await parseWire(adapter, [
      wire.anthropic.contentBlockDelta({
        index: 0,
        kind: "signature_delta",
        signature: "sig_abc123",
      }),
    ]);
    expect(events).toHaveLength(1);
    expect(events[0]?.type).toBe("inference.thinking.signature");
    if (events[0]?.type === "inference.thinking.signature") {
      expect(events[0].data.signature).toBe("sig_abc123");
    }
  });

  test("parses input_json_delta for tool arguments", async () => {
    // Per-test adapter instance: state for tool_use block index 1 must
    // come from the same adapter consuming the start event.
    const a = createAnthropicAdapter();
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
    const a = createAnthropicAdapter();
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
