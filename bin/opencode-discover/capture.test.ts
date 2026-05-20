import { describe, test, expect } from "bun:test";

import { redactRequestHeaders } from "../gemini-discover/capture.ts";
import {
  OPENCODE_REDACT_HEADERS,
  buildAuthHeaders,
  buildChatCompletionsRequest,
  buildChatCompletionsURL,
  detectFunctionCallingFromJson,
  detectReasoningFromJson,
  detectReasoningFromSseText,
} from "./capture.ts";
import { probeModel } from "./models.ts";

describe("buildAuthHeaders", () => {
  test("emits Bearer authorization with Content-Type", () => {
    const headers = buildAuthHeaders("sk-test123");
    expect(headers["Authorization"]).toBe("Bearer sk-test123");
    expect(headers["Content-Type"]).toBe("application/json");
  });
});

describe("buildChatCompletionsURL", () => {
  test("composes the path under the provided base URL", () => {
    expect(buildChatCompletionsURL("https://opencode.ai/zen/go/v1")).toBe(
      "https://opencode.ai/zen/go/v1/chat/completions",
    );
  });

  test("does not normalise duplicate slashes", () => {
    expect(buildChatCompletionsURL("https://example.invalid/")).toBe(
      "https://example.invalid//chat/completions",
    );
  });
});

describe("buildChatCompletionsRequest", () => {
  test("produces a minimal text request shape", () => {
    const body = buildChatCompletionsRequest({
      model: "kimi-k2.6",
      messages: [{ role: "user", content: "hi" }],
    });
    expect(body).toEqual({
      model: "kimi-k2.6",
      messages: [{ role: "user", content: "hi" }],
    });
  });

  test("sets stream when requested", () => {
    const body = buildChatCompletionsRequest({
      model: "glm-5.1",
      messages: [{ role: "user", content: "hi" }],
      stream: true,
    });
    expect(body.stream).toBe(true);
  });

  test("applies overrides verbatim", () => {
    const body = buildChatCompletionsRequest({
      model: "deepseek-v4-pro",
      messages: [{ role: "user", content: "hi" }],
      overrides: { temperature: 0, max_tokens: 32 },
    });
    expect(body.temperature).toBe(0);
    expect(body.max_tokens).toBe(32);
  });
});

describe("OPENCODE_REDACT_HEADERS / redactRequestHeaders interop", () => {
  test("the parameterised redactor strips a Bearer authorization", () => {
    const headers = {
      "Content-Type": "application/json",
      Authorization: "Bearer sk-secret",
    };
    const out = redactRequestHeaders(headers, OPENCODE_REDACT_HEADERS);
    expect(out["Authorization"]).toBe("<redacted>");
    expect(out["Content-Type"]).toBe("application/json");
  });

  test("the authorization match is case-insensitive", () => {
    for (const variant of ["authorization", "Authorization", "AUTHORIZATION"]) {
      const out = redactRequestHeaders(
        { [variant]: "Bearer sk-secret" },
        OPENCODE_REDACT_HEADERS,
      );
      expect(out[variant]).toBe("<redacted>");
    }
  });

  test("does not redact unrelated headers", () => {
    const out = redactRequestHeaders(
      {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": "AIzaSyTEST",
      },
      OPENCODE_REDACT_HEADERS,
    );
    expect(out["X-Goog-Api-Key"]).toBe("AIzaSyTEST");
  });
});

describe("detectReasoningFromJson", () => {
  test("picks up reasoning_content", () => {
    const ev = detectReasoningFromJson({
      choices: [
        {
          message: {
            content: "ok",
            reasoning_content: "step 1, step 2",
          },
        },
      ],
    });
    expect(ev?.fieldPath).toBe("choices.0.message.reasoning_content");
    expect(ev?.sample).toBe("step 1, step 2");
  });

  test("picks up reasoning", () => {
    const ev = detectReasoningFromJson({
      choices: [{ message: { reasoning: "thoughts" } }],
    });
    expect(ev?.fieldPath).toBe("choices.0.message.reasoning");
  });

  test("picks up reasoning_details", () => {
    const ev = detectReasoningFromJson({
      choices: [
        {
          message: {
            reasoning_details: [{ type: "thinking", text: "x" }],
          },
        },
      ],
    });
    expect(ev?.fieldPath).toBe("choices.0.message.reasoning_details");
  });

  test("returns null when reasoning fields are missing or empty", () => {
    expect(
      detectReasoningFromJson({ choices: [{ message: { content: "ok" } }] }),
    ).toBeNull();
    expect(
      detectReasoningFromJson({
        choices: [{ message: { reasoning_content: "" } }],
      }),
    ).toBeNull();
  });
});

describe("detectReasoningFromSseText", () => {
  test("detects delta.reasoning in any chunk", () => {
    const sse = [
      'data: {"choices":[{"delta":{"content":"hi"}}]}',
      'data: {"choices":[{"delta":{"reasoning":"thinking..."}}]}',
      "data: [DONE]",
      "",
    ].join("\n");
    const ev = detectReasoningFromSseText(sse);
    expect(ev?.fieldPath).toBe("choices.0.delta.reasoning");
  });

  test("detects delta.reasoning_content in any chunk", () => {
    const sse = [
      'data: {"choices":[{"delta":{"reasoning_content":"r"}}]}',
      "data: [DONE]",
      "",
    ].join("\n");
    const ev = detectReasoningFromSseText(sse);
    expect(ev?.fieldPath).toBe("choices.0.delta.reasoning_content");
  });

  test("returns null when no reasoning deltas appear", () => {
    const sse = [
      'data: {"choices":[{"delta":{"content":"hi"}}]}',
      "data: [DONE]",
      "",
    ].join("\n");
    expect(detectReasoningFromSseText(sse)).toBeNull();
  });
});

describe("detectFunctionCallingFromJson", () => {
  test("returns evidence when tool_calls is non-empty", () => {
    const ev = detectFunctionCallingFromJson({
      choices: [
        {
          finish_reason: "tool_calls",
          message: {
            tool_calls: [{ id: "1", function: { name: "getCurrentWeather" } }],
          },
        },
      ],
    });
    expect(ev?.finishReason).toBe("tool_calls");
    expect(Array.isArray(ev?.toolCalls)).toBe(true);
  });

  test("returns null when tool_calls is absent or empty", () => {
    expect(
      detectFunctionCallingFromJson({
        choices: [{ message: { content: "no tools" } }],
      }),
    ).toBeNull();
    expect(
      detectFunctionCallingFromJson({
        choices: [{ message: { tool_calls: [] } }],
      }),
    ).toBeNull();
  });
});

describe("probeModel (against constructed in-memory responses)", () => {
  test("derives capability flags from synthetic fetch responses", async () => {
    const responses: Record<string, () => Response> = {
      text_ns: () =>
        new Response(
          JSON.stringify({
            choices: [{ message: { content: "ready" } }],
          }),
          { status: 200, headers: { "content-type": "application/json" } },
        ),
      text_stream: () => {
        const body =
          'data: {"choices":[{"delta":{"content":"ready"}}]}\n\ndata: [DONE]\n\n';
        return new Response(body, {
          status: 200,
          headers: { "content-type": "text/event-stream" },
        });
      },
      function_calling: () =>
        new Response(
          JSON.stringify({
            choices: [
              {
                finish_reason: "tool_calls",
                message: {
                  tool_calls: [
                    { id: "1", function: { name: "getCurrentWeather" } },
                  ],
                },
              },
            ],
          }),
          { status: 200, headers: { "content-type": "application/json" } },
        ),
      reasoning_ns: () =>
        new Response(
          JSON.stringify({
            choices: [
              {
                message: {
                  content: "5 cents",
                  reasoning_content: "set bat = ball + 1, total 1.10 ...",
                },
              },
            ],
          }),
          { status: 200, headers: { "content-type": "application/json" } },
        ),
      reasoning_stream: () => {
        const body =
          'data: {"choices":[{"delta":{"content":"5 cents"}}]}\n\ndata: [DONE]\n\n';
        return new Response(body, {
          status: 200,
          headers: { "content-type": "text/event-stream" },
        });
      },
      vision: () =>
        new Response(
          JSON.stringify({
            choices: [{ message: { content: "A picture of a dog." } }],
          }),
          { status: 200, headers: { "content-type": "application/json" } },
        ),
    };

    const order = [
      "text_ns",
      "text_stream",
      "function_calling",
      "reasoning_ns",
      "reasoning_stream",
      "vision",
    ];
    let i = 0;
    const realFetch = globalThis.fetch;
    const mockFetch: typeof fetch = async () => {
      const key = order[i++];
      if (!key) throw new Error("Unexpected extra fetch call in probe test");
      const factory = responses[key];
      if (!factory) throw new Error(`No response factory for ${key}`);
      return factory();
    };
    globalThis.fetch = mockFetch;

    try {
      const result = await probeModel({
        baseUrl: "https://example.invalid/v1",
        apiKey: "sk-test",
        model: "fake-model",
      });
      expect(result.flags.text).toBe(true);
      expect(result.flags.functionCalling).toBe(true);
      expect(result.flags.reasoning).toBe(true);
      expect(result.flags.vision).toBe(true);
      expect(result.reasoningEvidence?.fieldPath).toBe(
        "choices.0.message.reasoning_content",
      );
    } finally {
      globalThis.fetch = realFetch;
    }
  });
});
