import { type } from "arktype";
import { describe, test, expect } from "bun:test";
import { createAnthropicAdapter } from "./anthropic";
import type { ConversationMessage } from "@interchange/types/runtime";

const adapter = createAnthropicAdapter();

const AnthropicContentBlock = type({
  type: "string",
  "text?": "string",
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

describe("Anthropic adapter: buildRequest", () => {
  test("builds a request with required fields", () => {
    const messages: ConversationMessage[] = [
      { role: "user", content: [{ type: "text", text: "Hello" }] },
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
    const messages: ConversationMessage[] = [
      { role: "system", content: [{ type: "text", text: "You are helpful." }] },
      { role: "user", content: [{ type: "text", text: "Hi." }] },
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
    const messages: ConversationMessage[] = [
      { role: "system", content: [{ type: "text", text: "Original system." }] },
      { role: "user", content: [{ type: "text", text: "Hi." }] },
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
    const messages: ConversationMessage[] = [
      { role: "user", content: [{ type: "text", text: "Think deeply." }] },
    ];

    const req = adapter.buildRequest(messages, "claude-3-7-sonnet-20250219", {
      thinking: { enabled: true, budgetTokens: 2048 },
    });
    const body = AnthropicRequestBody.assert(JSON.parse(req.body));
    expect(body.thinking?.type).toBe("enabled");
    expect(body.thinking?.budget_tokens).toBe(2048);
  });

  test("converts tool_call blocks to tool_use type", () => {
    const messages: ConversationMessage[] = [
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
    const messages: ConversationMessage[] = [
      { role: "user", content: [{ type: "text", text: "Hi." }] },
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
    const messages: ConversationMessage[] = [
      { role: "user", content: [{ type: "text", text: "Hi." }] },
    ];

    const req = adapter.buildRequest(messages, "claude-3-5-sonnet-20241022", {
      tools: [],
    });
    const body = AnthropicRequestBody.assert(JSON.parse(req.body));
    expect("tools" in body).toBe(false);
  });

  test("omits tools key when tools is undefined", () => {
    const messages: ConversationMessage[] = [
      { role: "user", content: [{ type: "text", text: "Hi." }] },
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
    const messages: ConversationMessage[] = [
      { role: "user", content: [{ type: "text", text: "Hi." }] },
    ];

    const req = adapter.buildRequest(messages, "claude-3-5-sonnet-20241022", {
      maxTokens: 512,
    });
    const body = AnthropicRequestBody.assert(JSON.parse(req.body));
    expect(body.max_tokens).toBe(512);
  });
});

describe("Anthropic adapter: parseResponse", () => {
  test("returns empty array for non-JSON input", () => {
    const events = adapter.parseResponse("not json");
    expect(events).toEqual([]);
  });

  test("parses text_delta", () => {
    const sseData = JSON.stringify({
      type: "content_block_delta",
      index: 0,
      delta: { type: "text_delta", text: "Hello" },
    });

    const events = adapter.parseResponse(sseData);
    expect(events).toHaveLength(1);
    const evt = events[0];
    expect(evt?.type).toBe("inference.text.delta");
    if (evt?.type === "inference.text.delta") {
      expect(evt.data.token).toBe("Hello");
    }
  });

  test("parses thinking_delta", () => {
    const sseData = JSON.stringify({
      type: "content_block_delta",
      index: 0,
      delta: { type: "thinking_delta", thinking: "reasoning..." },
    });

    const events = adapter.parseResponse(sseData);
    expect(events).toHaveLength(1);
    expect(events[0]?.type).toBe("inference.thinking.delta");
    if (events[0]?.type === "inference.thinking.delta") {
      expect(events[0].data.token).toBe("reasoning...");
    }
  });

  test("parses input_json_delta for tool arguments", () => {
    const sseData = JSON.stringify({
      type: "content_block_delta",
      index: 1,
      delta: { type: "input_json_delta", partial_json: '{"path":' },
    });

    const events = adapter.parseResponse(sseData);
    expect(events).toHaveLength(1);
    expect(events[0]?.type).toBe("inference.tool_call.delta");
    if (events[0]?.type === "inference.tool_call.delta") {
      expect(events[0].data.argumentFragment).toBe('{"path":');
    }
  });

  test("parses content_block_start for tool_use", () => {
    const sseData = JSON.stringify({
      type: "content_block_start",
      index: 0,
      content_block: { type: "tool_use", id: "toolu_01", name: "read_file" },
    });

    const events = adapter.parseResponse(sseData);
    expect(events).toHaveLength(1);
    expect(events[0]?.type).toBe("inference.tool_call.start");
    if (events[0]?.type === "inference.tool_call.start") {
      expect(events[0].data.callId).toBe("toolu_01");
      expect(events[0].data.name).toBe("read_file");
    }
  });

  test("parses message_start with usage", () => {
    const sseData = JSON.stringify({
      type: "message_start",
      message: {
        usage: {
          input_tokens: 100,
          output_tokens: 0,
          cache_read_input_tokens: 50,
          cache_creation_input_tokens: 0,
        },
      },
    });

    const events = adapter.parseResponse(sseData);
    expect(events).toHaveLength(1);
    expect(events[0]?.type).toBe("inference.usage");
    if (events[0]?.type === "inference.usage") {
      expect(events[0].data.usage.input).toBe(100);
      expect(events[0].data.usage.cacheRead).toBe(50);
    }
  });

  test("parses message_delta with output usage", () => {
    const sseData = JSON.stringify({
      type: "message_delta",
      delta: { stop_reason: "end_turn" },
      usage: { output_tokens: 42 },
    });

    const events = adapter.parseResponse(sseData);
    expect(events).toHaveLength(1);
    expect(events[0]?.type).toBe("inference.usage");
    if (events[0]?.type === "inference.usage") {
      expect(events[0].data.usage.output).toBe(42);
    }
  });

  test("returns empty for ping events", () => {
    const events = adapter.parseResponse(JSON.stringify({ type: "ping" }));
    expect(events).toEqual([]);
  });

  test("returns empty for message_stop", () => {
    const events = adapter.parseResponse(
      JSON.stringify({ type: "message_stop" }),
    );
    expect(events).toEqual([]);
  });

  test("returns empty for content_block_stop", () => {
    const events = adapter.parseResponse(
      JSON.stringify({ type: "content_block_stop", index: 0 }),
    );
    expect(events).toEqual([]);
  });
});
