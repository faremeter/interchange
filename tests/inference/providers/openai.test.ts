import { type } from "arktype";
import { describe, test, expect } from "bun:test";
import { wire } from "@intx/inference-testing";
import {
  parseSSE,
  createOpenAIAdapter,
  ProtocolMismatchError,
  type ProviderAdapter,
} from "@intx/inference";
import type { ConversationTurn, InferenceEvent } from "@intx/types/runtime";

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

  test("parses tool_call start with id and name", async () => {
    const events = await parseWire(adapter, [
      wire.openai.toolCallStart(0, "call_xyz", "search"),
    ]);
    expect(events).toHaveLength(1);
    expect(events[0]?.type).toBe("inference.tool_call.start");
    if (events[0]?.type === "inference.tool_call.start") {
      expect(events[0].data.callId).toBe("call_xyz");
      expect(events[0].data.name).toBe("search");
    }
  });

  test("parses tool_call argument fragment", async () => {
    const events = await parseWire(adapter, [
      wire.openai.toolCallArgumentsDelta(0, '{"q":"'),
    ]);
    expect(events).toHaveLength(1);
    expect(events[0]?.type).toBe("inference.tool_call.delta");
    if (events[0]?.type === "inference.tool_call.delta") {
      expect(events[0].data.argumentFragment).toBe('{"q":"');
    }
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
});
