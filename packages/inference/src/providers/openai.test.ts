import { type } from "arktype";
import { describe, test, expect } from "bun:test";
import { createOpenAIAdapter } from "./openai";
import type { ConversationTurn } from "@interchange/types/runtime";

const adapter = createOpenAIAdapter();

const OpenAIFunctionCall = type({
  name: "string",
  arguments: "string",
});

const OpenAIToolCall = type({
  id: "string",
  type: "string",
  function: OpenAIFunctionCall,
});

const OpenAIAssistantMessage = type({
  role: "string",
  "content?": "string | null",
  "tool_calls?": OpenAIToolCall.array(),
});

const OpenAIPlainMessage = type({
  role: "string",
  "content?": "string | null",
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
});

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
});

describe("OpenAI adapter: parseResponse", () => {
  test("returns empty array for non-JSON input", () => {
    const events = adapter.parseResponse("not json");
    expect(events).toEqual([]);
  });

  test("parses text delta from choices", () => {
    const sseData = JSON.stringify({
      id: "chatcmpl-abc",
      object: "chat.completion.chunk",
      choices: [
        {
          index: 0,
          delta: { role: "assistant", content: "Hello" },
          finish_reason: null,
        },
      ],
    });

    const events = adapter.parseResponse(sseData);
    expect(events).toHaveLength(1);
    expect(events[0]?.type).toBe("inference.text.delta");
    if (events[0]?.type === "inference.text.delta") {
      expect(events[0].data.token).toBe("Hello");
    }
  });

  test("returns empty for null content delta", () => {
    const sseData = JSON.stringify({
      choices: [
        {
          index: 0,
          delta: { role: "assistant", content: null },
          finish_reason: null,
        },
      ],
    });
    const events = adapter.parseResponse(sseData);
    expect(events).toEqual([]);
  });

  test("parses tool_call start with id and name", () => {
    const sseData = JSON.stringify({
      choices: [
        {
          index: 0,
          delta: {
            tool_calls: [
              {
                index: 0,
                id: "call_xyz",
                type: "function",
                function: { name: "search", arguments: "" },
              },
            ],
          },
          finish_reason: null,
        },
      ],
    });

    const events = adapter.parseResponse(sseData);
    expect(events).toHaveLength(1);
    expect(events[0]?.type).toBe("inference.tool_call.start");
    if (events[0]?.type === "inference.tool_call.start") {
      expect(events[0].data.callId).toBe("call_xyz");
      expect(events[0].data.name).toBe("search");
    }
  });

  test("parses tool_call argument fragment", () => {
    const sseData = JSON.stringify({
      choices: [
        {
          index: 0,
          delta: {
            tool_calls: [
              {
                index: 0,
                function: { arguments: '{"q":"' },
              },
            ],
          },
          finish_reason: null,
        },
      ],
    });

    const events = adapter.parseResponse(sseData);
    expect(events).toHaveLength(1);
    expect(events[0]?.type).toBe("inference.tool_call.delta");
    if (events[0]?.type === "inference.tool_call.delta") {
      expect(events[0].data.argumentFragment).toBe('{"q":"');
    }
  });

  test("parses usage from final chunk", () => {
    const sseData = JSON.stringify({
      choices: [],
      usage: { prompt_tokens: 50, completion_tokens: 20 },
    });

    const events = adapter.parseResponse(sseData);
    expect(events).toHaveLength(1);
    expect(events[0]?.type).toBe("inference.usage");
    if (events[0]?.type === "inference.usage") {
      expect(events[0].data.usage.input).toBe(50);
      expect(events[0].data.usage.output).toBe(20);
    }
  });

  test("returns empty for empty choices array with no usage", () => {
    const events = adapter.parseResponse(JSON.stringify({ choices: [] }));
    expect(events).toEqual([]);
  });
});
