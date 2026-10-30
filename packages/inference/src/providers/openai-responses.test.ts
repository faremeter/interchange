import { describe, test, expect } from "bun:test";
import type {
  ConversationTurn,
  InferenceEvent,
  InferenceOptions,
  LastCycleSource,
} from "@intx/types/runtime";

import {
  createDefaultScheduler,
  runInference,
  type Dependencies,
} from "../harness";
import { createBuiltinRegistry } from "./index";
import {
  createOpenAIResponsesAdapter,
  isResponsesStreamTerminal,
  OPENAI_COMPATIBLE_RESPONSES_PROVIDER,
  OPENAI_RESPONSES_PROVIDER,
  responsesAdapterFactories,
  responsesAdapterFactory,
  tagSignature,
  type ResponsesQuirks,
} from "./openai-responses";

function createSource(
  provider: string = OPENAI_RESPONSES_PROVIDER,
): LastCycleSource {
  return { sourceId: "src-1", provider, model: "gpt-5-mini" };
}

function userTurn(text: string): ConversationTurn {
  return { role: "user", content: [{ type: "text", text }], timestamp: 0 };
}

function requestBodyOf(
  turns: ConversationTurn[],
  model: string,
  options: InferenceOptions,
  quirks?: unknown,
  provider: string = OPENAI_RESPONSES_PROVIDER,
): Record<string, unknown> {
  const adapter = createOpenAIResponsesAdapter(createSource(provider), quirks);
  const built = adapter.buildRequest(turns, model, options);
  return parsedBody(built);
}

async function collect(
  iter: AsyncIterable<InferenceEvent>,
): Promise<InferenceEvent[]> {
  const out: InferenceEvent[] = [];
  for await (const ev of iter) out.push(ev);
  return out;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function requestInput(body: Record<string, unknown>): unknown[] {
  const input = body["input"];
  if (!Array.isArray(input)) throw new Error("expected input to be an array");
  return input;
}

function parsedBody(built: { body: string }): Record<string, unknown> {
  const parsed: unknown = JSON.parse(built.body);
  if (!isRecord(parsed))
    throw new Error("expected request body to be a record");
  return parsed;
}

describe("openai-responses quirks", () => {
  test("absent quirks resolve to protocol-native defaults", () => {
    const adapter = createOpenAIResponsesAdapter(createSource(), undefined);
    const built = adapter.buildRequest([userTurn("hi")], "gpt-5-mini", {});
    expect(built.url).toBe("/responses");
    expect(built.headers["accept"]).toBe("text/event-stream");
    const body = parsedBody(built);
    expect(body["store"]).toBe(false);
    expect(body["stream"]).toBe(true);
    expect(body).not.toHaveProperty("parallel_tool_calls");
    expect(body).not.toHaveProperty("temperature");
    expect(body).not.toHaveProperty("max_output_tokens");
    expect(body).not.toHaveProperty("reasoning");
  });

  test("unknown quirk keys fail loudly", () => {
    expect(() =>
      createOpenAIResponsesAdapter(createSource(), {
        strom: true,
      }),
    ).toThrow(/invalid quirks/);
  });

  test("quirks must not set authorization", () => {
    expect(() =>
      createOpenAIResponsesAdapter(createSource(), {
        headers: { static: { Authorization: "x" } },
      }),
    ).toThrow(/must not set authorization/);
    expect(() =>
      createOpenAIResponsesAdapter(createSource(), {
        headers: { modelHeader: "AUTHORIZATION" },
      }),
    ).toThrow(/must not set authorization/);
  });

  test("header names are lowercased and static headers override stock", () => {
    const adapter = createOpenAIResponsesAdapter(createSource(), {
      headers: { static: { "X-Custom": "1", "Content-Type": "custom" } },
    });
    const built = adapter.buildRequest([userTurn("hi")], "gpt-5-mini", {});
    expect(built.headers["x-custom"]).toBe("1");
    expect(built.headers["content-type"]).toBe("custom");
  });

  test("path and instructions quirks land on the request", () => {
    const body = requestBodyOf(
      [userTurn("hi")],
      "gpt-5-mini",
      {},
      {
        path: "/v1/responses",
        instructions: "be brief",
      },
    );
    expect(body["instructions"]).toBe("be brief");
    const adapter = createOpenAIResponsesAdapter(createSource(), {
      path: "/v1/responses",
    });
    expect(adapter.buildRequest([userTurn("hi")], "gpt-5-mini", {}).url).toBe(
      "/v1/responses",
    );
  });
});

describe("openai-responses buildRequest", () => {
  test("system prompt rides as a leading system message by default", () => {
    const body = requestBodyOf([userTurn("hi")], "gpt-5-mini", {
      systemPrompt: "sys",
    });
    const input = requestInput(body);
    expect(input[0]).toEqual({
      type: "message",
      role: "system",
      content: "sys",
    });
  });

  test("function tools are flat with auto tool_choice", () => {
    const body = requestBodyOf([userTurn("hi")], "gpt-5-mini", {
      tools: [
        {
          name: "run_shell",
          description: "run",
          inputSchema: { type: "object" },
        },
      ],
    });
    expect(body["tools"]).toEqual([
      {
        type: "function",
        name: "run_shell",
        description: "run",
        parameters: { type: "object" },
      },
    ]);
    expect(body["tool_choice"]).toBe("auto");
  });

  test("assistant text uses output_text, user text uses input_text", () => {
    const body = requestBodyOf(
      [
        { role: "user", content: [{ type: "text", text: "q" }], timestamp: 0 },
        {
          role: "assistant",
          content: [{ type: "text", text: "a" }],
          model: "gpt-5-mini",
          timestamp: 1,
        },
      ],
      "gpt-5-mini",
      {},
    );
    const input = requestInput(body);
    expect(input[0]).toEqual({
      type: "message",
      role: "user",
      content: [{ type: "input_text", text: "q" }],
    });
    expect(input[1]).toEqual({
      type: "message",
      role: "assistant",
      content: [{ type: "output_text", text: "a" }],
    });
  });

  test("flat contentShape collapses text-only turns to strings", () => {
    const body = requestBodyOf(
      [userTurn("hi")],
      "gpt-5-mini",
      { systemPrompt: "sys" },
      { contentShape: "flat" },
    );
    const input = requestInput(body);
    expect(input[0]).toEqual({
      type: "message",
      role: "system",
      content: "sys",
    });
    expect(input[1]).toEqual({
      type: "message",
      role: "user",
      content: "hi",
    });
  });

  test("tool_call and tool_result become function_call items", () => {
    const body = requestBodyOf(
      [
        {
          role: "assistant",
          content: [
            {
              type: "tool_call",
              id: "call-1",
              name: "run_shell",
              arguments: { cmd: "ls" },
            },
          ],
          model: "gpt-5-mini",
          timestamp: 1,
        },
        {
          role: "user",
          content: [
            {
              type: "tool_result",
              callId: "call-1",
              content: [{ type: "text", text: "ok" }],
            },
          ],
          timestamp: 2,
        },
      ],
      "gpt-5-mini",
      {},
    );
    const input = requestInput(body);
    expect(input).toContainEqual({
      type: "function_call",
      name: "run_shell",
      arguments: JSON.stringify({ cmd: "ls" }),
      call_id: "call-1",
    });
    expect(input).toContainEqual({
      type: "function_call_output",
      call_id: "call-1",
      output: "ok",
    });
  });

  test("duplicate tool items keep the last occurrence", () => {
    const body = requestBodyOf(
      [
        {
          role: "assistant",
          content: [
            {
              type: "tool_call",
              id: "call-1",
              name: "a",
              arguments: { v: 1 },
            },
          ],
          model: "gpt-5-mini",
          timestamp: 1,
        },
        {
          role: "assistant",
          content: [
            {
              type: "tool_call",
              id: "call-1",
              name: "a",
              arguments: { v: 2 },
            },
          ],
          model: "gpt-5-mini",
          timestamp: 2,
        },
      ],
      "gpt-5-mini",
      {},
    );
    const input = requestInput(body);
    const calls = input.filter(
      (item) => isRecord(item) && item["type"] === "function_call",
    );
    expect(calls).toHaveLength(1);
    const call = calls[0];
    if (!isRecord(call)) throw new Error("expected function_call item");
    expect(call["arguments"]).toBe(JSON.stringify({ v: 2 }));
  });

  test("thinking replays only for matching provider and model", () => {
    const thinkingTurn: ConversationTurn = {
      role: "assistant",
      content: [
        {
          type: "thinking",
          thinking: "hmm",
          signature: tagSignature(OPENAI_RESPONSES_PROVIDER, "cipher"),
        },
      ],
      model: "gpt-5-mini",
      timestamp: 1,
    };
    const replayed = requestBodyOf(
      [thinkingTurn],
      "gpt-5-mini",
      {},
      undefined,
      OPENAI_RESPONSES_PROVIDER,
    );
    const replayInput = requestInput(replayed);
    expect(replayInput).toContainEqual({
      type: "reasoning",
      summary: [],
      encrypted_content: "cipher",
    });

    const otherProvider = requestBodyOf(
      [thinkingTurn],
      "gpt-5-mini",
      {},
      undefined,
      "openai-compatible-responses",
    );
    expect(
      requestInput(otherProvider).some(
        (item) => isRecord(item) && item["type"] === "reasoning",
      ),
    ).toBe(false);

    const otherModel = requestBodyOf(
      [thinkingTurn],
      "gpt-5-other",
      {},
      undefined,
      OPENAI_RESPONSES_PROVIDER,
    );
    expect(
      requestInput(otherModel).some(
        (item) => isRecord(item) && item["type"] === "reasoning",
      ),
    ).toBe(false);
  });

  test("error tool results use the shared Chat wire encoding", () => {
    const body = requestBodyOf(
      [
        {
          role: "user",
          content: [
            {
              type: "tool_result",
              callId: "call-1",
              content: [
                { type: "text", text: "first" },
                { type: "text", text: "second" },
              ],
              isError: true,
            },
          ],
          timestamp: 2,
        },
      ],
      "gpt-5-mini",
      {},
    );
    const input = requestInput(body);
    expect(input).toContainEqual({
      type: "function_call_output",
      call_id: "call-1",
      output: "<error>\nfirst\nsecond\n</error>",
    });
  });

  test("untagged signatures are never replayed", () => {
    const body = requestBodyOf(
      [
        {
          role: "assistant",
          content: [{ type: "thinking", thinking: "hmm", signature: "raw" }],
          model: "gpt-5-mini",
          timestamp: 1,
        },
      ],
      "gpt-5-mini",
      {},
    );
    expect(body["input"]).toEqual([]);
  });

  test("image and pdf inputs map to Responses parts", () => {
    const body = requestBodyOf(
      [
        {
          role: "user",
          content: [
            {
              type: "image",
              source: {
                kind: "base64",
                mimeType: "image/png",
                data: "AAA",
              },
            },
            {
              type: "document",
              source: {
                kind: "base64",
                mimeType: "application/pdf",
                data: "BBB",
              },
            },
          ],
          timestamp: 0,
        },
      ],
      "gpt-5-mini",
      {},
    );
    const input = requestInput(body);
    expect(input[0]).toEqual({
      type: "message",
      role: "user",
      content: [
        { type: "input_image", image_url: "data:image/png;base64,AAA" },
        {
          type: "input_file",
          filename: "document.pdf",
          file_data: "data:application/pdf;base64,BBB",
        },
      ],
    });
  });

  test("url documents and audio blocks throw", () => {
    expect(() =>
      requestBodyOf(
        [
          {
            role: "user",
            content: [
              {
                type: "document",
                source: {
                  kind: "url",
                  mimeType: "application/pdf",
                  url: "https://x/y.pdf",
                },
              },
            ],
            timestamp: 0,
          },
        ],
        "gpt-5-mini",
        {},
      ),
    ).toThrow(/input_file does not accept url/);
    expect(() =>
      requestBodyOf(
        [
          {
            role: "user",
            content: [
              {
                type: "audio",
                source: { kind: "base64", mimeType: "audio/mp3", data: "A" },
              },
            ],
            timestamp: 0,
          },
        ],
        "gpt-5-mini",
        {},
      ),
    ).toThrow(/does not yet handle audio/);
  });

  test("session id routes to prompt_cache_key and optional header", () => {
    const body = requestBodyOf(
      [userTurn("hi")],
      "gpt-5-mini",
      {
        providerOptions: { sid: "s-1" },
      },
      {
        sessionIdOption: "sid",
        sessionIdHeader: "X-Session",
      },
    );
    expect(body["prompt_cache_key"]).toBe("s-1");
    const adapter = createOpenAIResponsesAdapter(createSource(), {
      sessionIdOption: "sid",
      sessionIdHeader: "X-Session",
    });
    const built = adapter.buildRequest([userTurn("hi")], "gpt-5-mini", {
      providerOptions: { sid: "s-1" },
    });
    expect(built.headers["x-session"]).toBe("s-1");
  });

  test("model header, fromOption, maxTokens, temperature, parallel tools", () => {
    const adapter = createOpenAIResponsesAdapter(createSource(), {
      headers: {
        modelHeader: "X-Model",
        fromOption: [{ optionKey: "uid", header: "X-User" }],
      },
      parallelToolCalls: false,
    });
    const built = adapter.buildRequest([userTurn("hi")], "gpt-5-mini", {
      maxTokens: 10,
      temperature: 0.5,
      providerOptions: { uid: "u-1" },
    });
    const body = parsedBody(built);
    expect(built.headers["x-model"]).toBe("gpt-5-mini");
    expect(built.headers["x-user"]).toBe("u-1");
    expect(body["max_output_tokens"]).toBe(10);
    expect(body["temperature"]).toBe(0.5);
    expect(body["parallel_tool_calls"]).toBe(false);
  });

  test("maxOutputTokens and temperature opt out when false", () => {
    const body = requestBodyOf(
      [userTurn("hi")],
      "gpt-5-mini",
      {
        maxTokens: 10,
        temperature: 0.5,
      },
      {
        maxOutputTokens: false,
        temperature: false,
      },
    );
    expect(body).not.toHaveProperty("max_output_tokens");
    expect(body).not.toHaveProperty("temperature");
  });

  test("reasoning effort passes through and max maps via maxEffort", () => {
    const low = requestBodyOf([userTurn("hi")], "m", { effort: "low" });
    expect(low["reasoning"]).toEqual({ effort: "low" });
    const maxed = requestBodyOf(
      [userTurn("hi")],
      "m",
      { effort: "max" },
      {
        reasoning: { maxEffort: "xhigh" },
      },
    );
    expect(maxed).toMatchObject({ reasoning: { effort: "xhigh" } });
  });
});

describe("openai-responses parseResponse", () => {
  function parse(
    payloads: Record<string, unknown>[],
    provider: string = OPENAI_RESPONSES_PROVIDER,
  ): InferenceEvent[] {
    const adapter = createOpenAIResponsesAdapter(createSource(provider));
    const out: InferenceEvent[] = [];
    for (const p of payloads) {
      out.push(...adapter.parseResponse(JSON.stringify(p)));
    }
    return out;
  }

  test("text deltas carry per-item block indices", () => {
    const events = parse([
      { type: "response.output_text.delta", item_id: "m1", delta: "Hello" },
      { type: "response.output_text.delta", item_id: "m1", delta: " world" },
    ]);
    expect(events.map((e) => e.type)).toEqual([
      "inference.text.delta",
      "inference.text.delta",
    ]);
    for (const e of events) {
      if (e.type !== "inference.text.delta") throw new Error("mistyped");
      expect(e.data.index).toBe(0);
    }
    expect(events[0]?.type).toBe("inference.text.delta");
  });

  test("reasoning deltas register thinking blocks and capture signatures", () => {
    const events = parse([
      {
        type: "response.output_item.added",
        item: { type: "reasoning", id: "r1" },
      },
      { type: "response.reasoning_text.delta", item_id: "r1", delta: "think" },
      {
        type: "response.output_item.done",
        item: { type: "reasoning", id: "r1", encrypted_content: "blob" },
      },
    ]);
    const thinking = events.filter(
      (e) => e.type === "inference.thinking.delta",
    );
    expect(thinking).toHaveLength(2);
    const sig = events.find((e) => e.type === "inference.block.signature");
    if (sig?.type !== "inference.block.signature") {
      throw new Error("missing signature");
    }
    expect(sig.data.signature).toBe(
      tagSignature(OPENAI_RESPONSES_PROVIDER, "blob"),
    );
  });

  test("tool call round trip emits start and delta", () => {
    const events = parse([
      {
        type: "response.output_item.added",
        item: {
          type: "function_call",
          id: "fc1",
          call_id: "call-1",
          name: "run_shell",
        },
      },
      {
        type: "response.function_call_arguments.delta",
        item_id: "fc1",
        delta: '{"cmd"',
      },
      {
        type: "response.function_call_arguments.delta",
        item_id: "fc1",
        delta: ':"ls"}',
      },
    ]);
    expect(events.map((e) => e.type)).toEqual([
      "inference.tool_call.start",
      "inference.tool_call.delta",
      "inference.tool_call.delta",
    ]);
  });

  test("code interpreter deltas accumulate before the done result", () => {
    const events = parse([
      {
        type: "response.code_interpreter_call_code.delta",
        item_id: "ci-1",
        delta: "print(",
      },
      {
        type: "response.code_interpreter_call_code.delta",
        item_id: "ci-1",
        delta: "1)",
      },
      {
        type: "response.output_item.done",
        item: {
          type: "code_interpreter_call",
          id: "ci-1",
          status: "completed",
          code: "print(1)",
          outputs: null,
        },
      },
    ]);
    expect(events.map((e) => e.type)).toEqual([
      "inference.code_execution.start",
      "inference.code_execution.delta",
      "inference.code_execution.delta",
      "inference.code_execution.result",
    ]);
    const result = events.find(
      (e) => e.type === "inference.code_execution.result",
    );
    if (result?.type !== "inference.code_execution.result") {
      throw new Error("expected a code execution result");
    }
    expect(result.data.result).toMatchObject({
      requestId: "ci-1",
      status: "ok",
    });
  });

  test("content_part.done emits citations without duplicating text", () => {
    const events = parse([
      {
        type: "response.output_text.delta",
        item_id: "m1",
        delta: "hello world",
      },
      {
        type: "response.content_part.done",
        item_id: "m1",
        part: {
          type: "output_text",
          text: "hello world",
          annotations: [
            {
              type: "url_citation",
              start_index: 0,
              end_index: 5,
              url: "https://example.com",
            },
          ],
        },
      },
    ]);
    expect(events.map((e) => e.type)).toEqual([
      "inference.text.delta",
      "inference.citation",
    ]);
  });

  test("orphan argument delta throws protocol mismatch", () => {
    const adapter = createOpenAIResponsesAdapter(createSource());
    expect(() =>
      adapter.parseResponse(
        JSON.stringify({
          type: "response.function_call_arguments.delta",
          item_id: "ghost",
          delta: "{}",
        }),
      ),
    ).toThrow(/never announced by output_item.added/);
  });

  test("malformed JSON throws protocol mismatch", () => {
    const adapter = createOpenAIResponsesAdapter(createSource());
    expect(() => adapter.parseResponse("{not json")).toThrow(
      /malformed JSON in SSE data payload/,
    );
  });

  test("response.failed and error envelopes throw", () => {
    const adapter = createOpenAIResponsesAdapter(createSource());
    expect(() =>
      adapter.parseResponse(
        JSON.stringify({
          type: "response.failed",
          response: { error: { message: "boom" } },
        }),
      ),
    ).toThrow(/boom/);
    expect(() =>
      adapter.parseResponse(JSON.stringify({ type: "error", message: "bad" })),
    ).toThrow(/bad/);
  });

  test("unknown and lifecycle envelopes are ignored", () => {
    expect(
      parse([
        { type: "response.created", response: { id: "r" } },
        { type: "response.future.envelope", anything: 1 },
      ]),
    ).toEqual([]);
  });

  test("completed usage splits cached and write tokens", () => {
    const events = parse([
      {
        type: "response.completed",
        response: {
          usage: {
            input_tokens: 100,
            output_tokens: 20,
            input_tokens_details: {
              cached_tokens: 30,
              cache_write_tokens: 10,
            },
            output_tokens_details: { reasoning_tokens: 5 },
          },
        },
      },
    ]);
    expect(events).toHaveLength(1);
    const usage = events[0];
    if (usage?.type !== "inference.usage") throw new Error("mistyped");
    expect(usage.data.usage).toEqual({
      input: 60,
      output: 20,
      cacheRead: 30,
      cacheWrite: 10,
      thinking: 5,
    });
  });

  test("response.done shares the completed usage handling", () => {
    const events = parse([
      {
        type: "response.done",
        response: { usage: { input_tokens: 4, output_tokens: 2 } },
      },
    ]);
    expect(events).toHaveLength(1);
    const usage = events[0];
    if (usage?.type !== "inference.usage") throw new Error("mistyped");
    expect(usage.data.usage).toMatchObject({ input: 4, output: 2 });
  });

  test("incomplete reports usage like a completed turn", () => {
    const events = parse([
      {
        type: "response.incomplete",
        response: {
          incomplete_details: { reason: "max_output_tokens" },
          usage: { input_tokens: 4, output_tokens: 2 },
        },
      },
    ]);
    expect(events).toHaveLength(1);
    expect(events[0]?.type).toBe("inference.usage");
  });
});

describe("isResponsesStreamTerminal", () => {
  test("terminal events stop the stream", () => {
    for (const t of [
      "response.completed",
      "response.incomplete",
      "response.done",
    ]) {
      expect(isResponsesStreamTerminal(JSON.stringify({ type: t }))).toBe(true);
    }
  });

  test("non-terminal chunks keep the stream open", () => {
    expect(
      isResponsesStreamTerminal(
        JSON.stringify({
          type: "response.output_text.delta",
          item_id: "m",
          delta: "hi",
        }),
      ),
    ).toBe(false);
    expect(
      isResponsesStreamTerminal(JSON.stringify({ type: "response.created" })),
    ).toBe(false);
  });

  test("malformed payloads report not terminal", () => {
    expect(isResponsesStreamTerminal("{not json")).toBe(false);
    expect(isResponsesStreamTerminal(JSON.stringify({ nope: 1 }))).toBe(false);
  });
});

describe("openai-responses parseJSONResponse", () => {
  function parseJson(
    body: unknown,
    provider: string = OPENAI_RESPONSES_PROVIDER,
  ): InferenceEvent[] {
    const adapter = createOpenAIResponsesAdapter(createSource(provider));
    return adapter.parseJSONResponse(JSON.stringify(body));
  }

  test("buffered body emits text, reasoning, tool, and usage events", () => {
    const events = parseJson({
      output: [
        {
          type: "message",
          content: [{ type: "output_text", text: "hello" }],
        },
        {
          type: "reasoning",
          id: "r1",
          summary: [{ type: "summary_text", text: "thought" }],
          encrypted_content: "blob",
        },
        {
          type: "function_call",
          id: "fc1",
          call_id: "call-1",
          name: "run_shell",
          arguments: '{"cmd":"ls"}',
        },
      ],
      usage: { input_tokens: 10, output_tokens: 5 },
    });
    expect(events.map((e) => e.type)).toEqual([
      "inference.text.delta",
      "inference.thinking.delta",
      "inference.block.signature",
      "inference.tool_call.start",
      "inference.tool_call.delta",
      "inference.usage",
    ]);
    const usage = events.at(-1);
    if (usage?.type !== "inference.usage") throw new Error("mistyped");
    expect(usage.data.usage).toMatchObject({ input: 10, output: 5 });
  });

  test("explicit null error and incomplete_details decode", () => {
    const events = parseJson({
      status: "completed",
      output: [
        {
          type: "message",
          content: [{ type: "output_text", text: "hello" }],
        },
      ],
      error: null,
      incomplete_details: null,
      usage: { input_tokens: 1, output_tokens: 1 },
    });
    expect(events.map((e) => e.type)).toEqual([
      "inference.text.delta",
      "inference.usage",
    ]);
  });

  test("url_citation annotations emit anchored citations", () => {
    const events = parseJson({
      output: [
        {
          type: "message",
          content: [
            {
              type: "output_text",
              text: "hello world",
              annotations: [
                {
                  type: "url_citation",
                  start_index: 0,
                  end_index: 5,
                  url: "https://example.com",
                  title: "Example",
                },
              ],
            },
          ],
        },
      ],
    });
    const citation = events.find((e) => e.type === "inference.citation");
    if (citation?.type !== "inference.citation") {
      throw new Error("expected a citation event");
    }
    expect(citation.data.citation).toMatchObject({
      citedText: "hello",
      source: { uri: "https://example.com", title: "Example" },
      textOffset: { start: 0, end: 5 },
    });
  });

  test("code_interpreter_call emits code execution request and result", () => {
    const events = parseJson({
      output: [
        {
          type: "code_interpreter_call",
          id: "ci-1",
          status: "completed",
          code: "print(1)",
          outputs: null,
        },
      ],
    });
    expect(events.map((e) => e.type)).toEqual([
      "inference.code_execution.start",
      "inference.code_execution.result",
      "inference.usage",
    ]);
    const result = events.find(
      (e) => e.type === "inference.code_execution.result",
    );
    if (result?.type !== "inference.code_execution.result") {
      throw new Error("expected a code execution result");
    }
    expect(result.data.result).toMatchObject({
      requestId: "ci-1",
      status: "ok",
      providerOutcome: "completed",
    });
  });

  test("failed status throws instead of decoding an empty turn", () => {
    expect(() =>
      parseJson({ status: "failed", output: [], error: { message: "nope" } }),
    ).toThrow(/status is "failed"/);
  });

  test("malformed body throws protocol mismatch", () => {
    const adapter = createOpenAIResponsesAdapter(createSource());
    expect(() => adapter.parseJSONResponse("{bad")).toThrow(
      /malformed JSON response body/,
    );
  });
});

describe("responsesAdapterFactory composition", () => {
  const quirks: ResponsesQuirks = {
    path: "/v1/responses",
    headers: {
      static: { "x-client": "test" },
      modelHeader: "x-model",
    },
    systemPrompt: { role: "system", shape: "string" },
    contentShape: "flat",
    reasoning: { summary: "detailed" },
    maxOutputTokens: false,
    temperature: false,
  };

  test("fixed quirks bake in and per-source quirks are ignored", () => {
    const factory = responsesAdapterFactory(quirks);
    const adapter = factory(createSource(), { path: "/evil" });
    const built = adapter.buildRequest([userTurn("hi")], "m", {});
    expect(built.url).toBe("/v1/responses");
    expect(built.headers["x-client"]).toBe("test");
  });

  test("code hooks shape the request", () => {
    const factory = responsesAdapterFactory(quirks, {
      wrapSystemPrompt: (s) => `<sys>${s}</sys>`,
      includeReasoningEffort: (effort) => effort !== "low",
    });
    const adapter = factory(createSource());
    const wrapped = adapter.buildRequest([userTurn("hi")], "m", {
      systemPrompt: "sys",
      effort: "low",
    });
    const wrappedBody = parsedBody(wrapped);
    const firstInput = requestInput(wrappedBody)[0];
    if (!isRecord(firstInput)) throw new Error("expected input item");
    expect(firstInput["content"]).toBe("<sys>sys</sys>");
    // The hook excludes only the effort value; the summary quirk still lands.
    expect(wrappedBody["reasoning"]).toEqual({ summary: "detailed" });

    const kept = adapter.buildRequest([userTurn("hi")], "m", {
      effort: "high",
    });
    expect(parsedBody(kept)["reasoning"]).toEqual({
      summary: "detailed",
      effort: "high",
    });
  });

  test("per-request block indices do not leak across requests", () => {
    const factory = responsesAdapterFactory(quirks);
    const adapter = factory(createSource());
    const first = adapter.parseResponse(
      JSON.stringify({
        type: "response.output_text.delta",
        item_id: "m1",
        delta: "a",
      }),
    );
    adapter.buildRequest([userTurn("next")], "m", {});
    const second = adapter.parseResponse(
      JSON.stringify({
        type: "response.output_text.delta",
        item_id: "m1",
        delta: "b",
      }),
    );
    if (
      first[0]?.type !== "inference.text.delta" ||
      second[0]?.type !== "inference.text.delta"
    ) {
      throw new Error("mistyped");
    }
    expect(first[0].data.index).toBe(0);
    expect(second[0].data.index).toBe(0);
  });
});

describe("openai-responses registration", () => {
  test("both provider ids resolve to the shared Responses factory", () => {
    const registry = createBuiltinRegistry();
    expect(registry.has(OPENAI_RESPONSES_PROVIDER)).toBe(true);
    expect(registry.has(OPENAI_COMPATIBLE_RESPONSES_PROVIDER)).toBe(true);
    for (const provider of [
      OPENAI_RESPONSES_PROVIDER,
      OPENAI_COMPATIBLE_RESPONSES_PROVIDER,
    ]) {
      const adapter = registry.resolve(createSource(provider));
      expect(typeof adapter.buildRequest).toBe("function");
      expect(typeof adapter.isStreamTerminal).toBe("function");
    }
  });

  test("factories record maps both ids to the shared factory", () => {
    expect(responsesAdapterFactories[OPENAI_RESPONSES_PROVIDER]).toBe(
      createOpenAIResponsesAdapter,
    );
    expect(
      responsesAdapterFactories[OPENAI_COMPATIBLE_RESPONSES_PROVIDER],
    ).toBe(createOpenAIResponsesAdapter);
  });

  test("chat completions adapters carry no terminal predicate", async () => {
    const { createOpenAIAdapter } = await import("./openai");
    expect(
      createOpenAIAdapter(createSource("openai")).isStreamTerminal,
    ).toBeUndefined();
  });
});

describe("runInference with a held-open Responses stream", () => {
  test("response.completed ends the call with usage preserved", async () => {
    const frames = [
      { type: "response.output_text.delta", item_id: "m1", delta: "Hello" },
      { type: "response.output_text.delta", item_id: "m1", delta: " world" },
      {
        type: "response.completed",
        response: { usage: { input_tokens: 10, output_tokens: 5 } },
      },
    ];
    const sseBody = (): ReadableStream<Uint8Array> => {
      const enc = new TextEncoder();
      return new ReadableStream({
        start(controller) {
          for (const f of frames) {
            controller.enqueue(enc.encode(`data: ${JSON.stringify(f)}\n\n`));
          }
          // Held open: no [DONE], no close.
        },
      });
    };
    const deps: Dependencies = {
      fetch: () =>
        Promise.resolve(
          new Response(sseBody(), {
            status: 200,
            headers: { "content-type": "text/event-stream" },
          }),
        ),
      scheduler: createDefaultScheduler(),
      adapters: createBuiltinRegistry(),
    };
    let seq = 0;
    const events = await collect(
      runInference({
        turns: [userTurn("hi")],
        source: {
          id: "openai-responses:gpt-5-mini",
          provider: OPENAI_RESPONSES_PROVIDER,
          baseURL: "https://api.openai.com/v1",
          credentialId: "test",
          model: "gpt-5-mini",
        },
        nextSeq: () => ++seq,
        deps,
        readMaterial: () => ({ secret: "test-secret" }),
      }),
    );
    expect(events.map((e) => e.type)).toEqual([
      "inference.start",
      "inference.text.delta",
      "inference.text.delta",
      "inference.usage",
      "inference.done",
    ]);
    const usage = events.find((e) => e.type === "inference.usage");
    if (usage?.type !== "inference.usage") throw new Error("missing usage");
    expect(usage.data.usage).toMatchObject({ input: 10, output: 5 });
    const done = events.find((e) => e.type === "inference.done");
    if (done?.type !== "inference.done") throw new Error("missing done");
    expect(done.data.turn.content).toEqual([
      { type: "text", text: "Hello world" },
    ]);
  });

  test("buffered JSON body replays through the accumulator", async () => {
    const deps: Dependencies = {
      fetch: () =>
        Promise.resolve(
          new Response(
            JSON.stringify({
              output: [
                {
                  type: "message",
                  content: [{ type: "output_text", text: "buffered hi" }],
                },
              ],
              usage: { input_tokens: 7, output_tokens: 3 },
            }),
            {
              status: 200,
              headers: { "content-type": "application/json" },
            },
          ),
        ),
      scheduler: createDefaultScheduler(),
      adapters: createBuiltinRegistry(),
    };
    let seq = 0;
    const events = await collect(
      runInference({
        turns: [userTurn("hi")],
        source: {
          id: "openai-responses:gpt-5-mini",
          provider: OPENAI_RESPONSES_PROVIDER,
          baseURL: "https://api.openai.com/v1",
          credentialId: "test",
          model: "gpt-5-mini",
        },
        nextSeq: () => ++seq,
        deps,
        readMaterial: () => ({ secret: "test-secret" }),
      }),
    );
    expect(events.map((e) => e.type)).toEqual([
      "inference.start",
      "inference.text.delta",
      "inference.usage",
      "inference.done",
    ]);
    const done = events.find((e) => e.type === "inference.done");
    if (done?.type !== "inference.done") throw new Error("missing done");
    expect(done.data.turn.content).toEqual([
      { type: "text", text: "buffered hi" },
    ]);
  });
});
