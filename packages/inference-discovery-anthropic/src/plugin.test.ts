import { describe, test, expect } from "bun:test";
import {
  INTENTS,
  type Capability,
  type CapabilityIntent,
} from "@intx/inference-discovery/catalog";
import type { CaptureStep, CapturedResponse } from "@intx/inference-discovery";
import { ANTHROPIC_VERSION } from "./auth";
import {
  buildFilesURL,
  buildMessagesURL,
  isStreamingCapability,
} from "./endpoint";
import {
  buildFilesApiGenerateBody,
  buildFunctionCallingTurn2Body,
  buildRedactedThinkingTurn2Body,
  buildRequestBody,
  isSupportedCapability,
} from "./request-body";
import { extractReasoningTrace } from "./reasoning";
import { createAnthropicPlugin, iterateCaptureSteps } from "./index";
import type { AnthropicContentBlock } from "./request-body";

const TEST_API_KEY = "test-anthropic-key";
const SONNET = "claude-sonnet-4-5-20250929";

function collectSteps(opts: {
  model: string;
  capability: Capability;
  responses: readonly CapturedResponse[];
}): CaptureStep[] {
  const intent = INTENTS[opts.capability];
  const iter = iterateCaptureSteps({
    model: opts.model,
    capability: opts.capability,
    intent,
  });
  const steps: CaptureStep[] = [];
  let i = 0;
  let next = iter.next();
  while (!next.done) {
    steps.push(next.value);
    const response = opts.responses[i];
    i += 1;
    if (response === undefined) break;
    next = iter.next(response);
  }
  return steps;
}

describe("createAnthropicPlugin", () => {
  test("declares provider name, all three models, and redaction lists", () => {
    const plugin = createAnthropicPlugin({ apiKey: TEST_API_KEY });
    expect(plugin.name).toBe("anthropic");
    expect(plugin.models).toEqual([
      "claude-sonnet-4-5-20250929",
      "claude-opus-4-1-20250805",
      "claude-haiku-4-5-20251022",
    ]);
    expect(plugin.redactRequestHeaders).toEqual(["x-api-key"]);
    expect(plugin.redactResponseHeaders).toEqual([]);
  });

  test("buildAuthHeaders returns x-api-key and pinned anthropic-version", () => {
    const plugin = createAnthropicPlugin({ apiKey: TEST_API_KEY });
    expect(plugin.buildAuthHeaders()).toEqual({
      "x-api-key": TEST_API_KEY,
      "anthropic-version": ANTHROPIC_VERSION,
    });
  });

  test("buildAuthHeaders never carries an anthropic-beta flag", () => {
    const plugin = createAnthropicPlugin({ apiKey: TEST_API_KEY });
    const headers = plugin.buildAuthHeaders();
    expect(headers["anthropic-beta"]).toBeUndefined();
  });

  test("buildAuthHeaders rejects an empty apiKey", () => {
    const plugin = createAnthropicPlugin({ apiKey: "" });
    expect(() => plugin.buildAuthHeaders()).toThrow(/apiKey/);
  });
});

describe("endpoint URL helpers", () => {
  test("messages URL is the v1/messages endpoint regardless of streaming", () => {
    expect(buildMessagesURL()).toBe("https://api.anthropic.com/v1/messages");
  });

  test("files URL is the v1/files endpoint", () => {
    expect(buildFilesURL()).toBe("https://api.anthropic.com/v1/files");
  });

  test("isStreamingCapability matches the -streaming suffix", () => {
    expect(isStreamingCapability("plain-text")).toBe(false);
    expect(isStreamingCapability("plain-text-streaming")).toBe(true);
    expect(isStreamingCapability("redacted-thinking-streaming")).toBe(true);
  });
});

describe("buildRequestBody — wire-shape spot checks", () => {
  test("plain-text produces a single user message with max_tokens, no stream flag", () => {
    const body = buildRequestBody({
      model: SONNET,
      capability: "plain-text",
      intent: INTENTS["plain-text"],
    });
    expect(body).toEqual({
      model: SONNET,
      max_tokens: 512,
      messages: [{ role: "user", content: INTENTS["plain-text"].prompt }],
    });
  });

  test("plain-text-streaming sets stream: true", () => {
    const body = buildRequestBody({
      model: SONNET,
      capability: "plain-text-streaming",
      intent: INTENTS["plain-text-streaming"],
    });
    expect(body.stream).toBe(true);
  });

  test("function-calling carries the tool decl as input_schema", () => {
    const body = buildRequestBody({
      model: SONNET,
      capability: "function-calling",
      intent: INTENTS["function-calling"],
    });
    expect(body.tools).toBeDefined();
    const tools = body.tools;
    if (tools === undefined) throw new Error("tools missing");
    const [tool] = tools;
    if (tool === undefined || "type" in tool) {
      throw new Error("expected function tool decl, got server-side tool");
    }
    expect(tool.name).toBe("get_weather");
    expect(tool.input_schema.type).toBe("object");
  });

  test("function-calling-with-thinking attaches thinking config + raised max_tokens", () => {
    const body = buildRequestBody({
      model: SONNET,
      capability: "function-calling-with-thinking",
      intent: INTENTS["function-calling-with-thinking"],
    });
    expect(body.thinking).toEqual({ type: "enabled", budget_tokens: 1024 });
    expect(body.max_tokens).toBeGreaterThan(1024);
  });

  test("reasoning-content enables thinking with budget_tokens=1024", () => {
    const body = buildRequestBody({
      model: SONNET,
      capability: "reasoning-content",
      intent: INTENTS["reasoning-content"],
    });
    expect(body.thinking).toEqual({ type: "enabled", budget_tokens: 1024 });
  });

  test("vision-input embeds a base64 image source", () => {
    const body = buildRequestBody({
      model: SONNET,
      capability: "vision-input",
      intent: INTENTS["vision-input"],
    });
    const first = body.messages[0];
    if (first === undefined || typeof first.content === "string") {
      throw new Error("expected an array-content user turn");
    }
    const image = first.content.find((b) => b.type === "image");
    if (image === undefined || image.type !== "image") {
      throw new Error("vision-input must carry an image content block");
    }
    expect(image.source.type).toBe("base64");
    expect(image.source.media_type).toBe("image/jpeg");
    expect(image.source.data.length).toBeGreaterThan(0);
  });

  test("document-input embeds a base64 application/pdf source", () => {
    const body = buildRequestBody({
      model: SONNET,
      capability: "document-input",
      intent: INTENTS["document-input"],
    });
    const first = body.messages[0];
    if (first === undefined || typeof first.content === "string") {
      throw new Error("expected an array-content user turn");
    }
    const doc = first.content.find((b) => b.type === "document");
    if (doc === undefined || doc.type !== "document") {
      throw new Error("document-input must carry a document content block");
    }
    if (doc.source.type !== "base64") {
      throw new Error("document-input must use base64 source on inline path");
    }
    expect(doc.source.media_type).toBe("application/pdf");
    expect(doc.source.data.length).toBeGreaterThan(0);
  });

  test("code-execution declares the server-side code_execution tool", () => {
    const body = buildRequestBody({
      model: SONNET,
      capability: "code-execution",
      intent: INTENTS["code-execution"],
    });
    expect(body.tools).toEqual([
      { type: "code_execution_20250522", name: "code_execution" },
    ]);
  });

  test("grounding declares the server-side web_search tool", () => {
    const body = buildRequestBody({
      model: SONNET,
      capability: "grounding",
      intent: INTENTS["grounding"],
    });
    expect(body.tools).toEqual([
      { type: "web_search_20250305", name: "web_search" },
    ]);
  });

  test("throws for files-api capabilities (use iterateCaptureSteps for multipart upload)", () => {
    const multipart: Capability[] = [
      "files-api-reference",
      "files-api-reference-streaming",
    ];
    for (const capability of multipart) {
      expect(() =>
        buildRequestBody({
          model: SONNET,
          capability,
          intent: INTENTS[capability],
        }),
      ).toThrow(/multipart/);
    }
  });

  test("multi-turn capabilities return the turn-1 body (used by iterateCaptureSteps)", () => {
    const turn1 = buildRequestBody({
      model: SONNET,
      capability: "function-calling-multi-turn",
      intent: INTENTS["function-calling-multi-turn"],
    });
    expect(turn1.tools).toBeDefined();
    expect(turn1.thinking).toBeUndefined();
    expect(turn1.stream).toBeUndefined();
  });

  test("redacted-thinking returns a turn-1 body with thinking enabled", () => {
    const turn1 = buildRequestBody({
      model: SONNET,
      capability: "redacted-thinking",
      intent: INTENTS["redacted-thinking"],
    });
    expect(turn1.thinking).toEqual({ type: "enabled", budget_tokens: 1024 });
    expect(turn1.messages[0]?.content).toMatch(/ANTHROPIC_MAGIC_STRING/);
  });

  test("throws for capabilities Anthropic does not expose", () => {
    const unsupported: Capability[] = [
      "audio-input",
      "audio-input-streaming",
      "video-input",
      "video-input-streaming",
      "image-output",
      "image-output-streaming",
    ];
    for (const capability of unsupported) {
      expect(() =>
        buildRequestBody({
          model: SONNET,
          capability,
          intent: INTENTS[capability],
        }),
      ).toThrow(/not supported/);
    }
  });
});

describe("buildFunctionCallingTurn2Body", () => {
  test("echoes assistant content verbatim and appends a tool_result user turn", () => {
    const intent = INTENTS["function-calling-multi-turn"];
    const turn1 = buildRequestBody({
      model: SONNET,
      capability: "function-calling-multi-turn",
      intent,
    });
    const assistantBlocks: AnthropicContentBlock[] = [
      { type: "text", text: "Calling tool" },
      {
        type: "tool_use",
        id: "tool_use_1",
        name: "get_weather",
        input: { location: "Boston, MA" },
      },
    ];
    const turn1Response = { content: assistantBlocks };
    const turn2 = buildFunctionCallingTurn2Body({
      model: SONNET,
      capability: "function-calling-multi-turn",
      intent,
      turn1Body: turn1,
      turn1Response,
    });
    expect(turn2.messages.length).toBe(turn1.messages.length + 2);
    const assistant = turn2.messages[turn1.messages.length];
    expect(assistant?.role).toBe("assistant");
    expect(assistant?.content).toEqual(turn1Response.content);
    const userTurn = turn2.messages[turn1.messages.length + 1];
    expect(userTurn?.role).toBe("user");
    if (userTurn === undefined || typeof userTurn.content === "string") {
      throw new Error("expected an array-content user follow-up");
    }
    const toolResult = userTurn.content[0];
    expect(toolResult).toEqual({
      type: "tool_result",
      tool_use_id: "tool_use_1",
      content:
        '{"location":"Boston, MA","temperatureF":68,"conditions":"clear"}',
    });
  });

  test("throws when turn-1 lacks a tool_use block", () => {
    const intent = INTENTS["function-calling-multi-turn"];
    const turn1 = buildRequestBody({
      model: SONNET,
      capability: "function-calling-multi-turn",
      intent,
    });
    expect(() =>
      buildFunctionCallingTurn2Body({
        model: SONNET,
        capability: "function-calling-multi-turn",
        intent,
        turn1Body: turn1,
        turn1Response: { content: [{ type: "text", text: "no tool" }] },
      }),
    ).toThrow(/tool_use/);
  });
});

describe("buildRedactedThinkingTurn2Body", () => {
  test("echoes redacted_thinking blocks verbatim and appends a user follow-up", () => {
    const intent: CapabilityIntent = INTENTS["redacted-thinking"];
    const turn1: ReturnType<typeof buildFunctionCallingTurn2Body> = {
      model: SONNET,
      max_tokens: 2048,
      messages: [{ role: "user", content: intent.prompt }],
      thinking: { type: "enabled", budget_tokens: 1024 },
    };
    const assistantBlocks: AnthropicContentBlock[] = [
      { type: "redacted_thinking", data: "encrypted-bytes" },
      { type: "text", text: "Continuing." },
    ];
    const turn1Response = { content: assistantBlocks };
    const turn2 = buildRedactedThinkingTurn2Body({
      model: SONNET,
      intent,
      turn1Body: turn1,
      turn1Response,
    });
    expect(turn2.thinking).toEqual({ type: "enabled", budget_tokens: 1024 });
    expect(turn2.messages.length).toBe(3);
    const assistant = turn2.messages[1];
    expect(assistant?.role).toBe("assistant");
    expect(assistant?.content).toEqual(turn1Response.content);
    const followUp = turn2.messages[2];
    expect(followUp).toEqual({
      role: "user",
      content: "Briefly summarize what you just said in one sentence.",
    });
  });
});

describe("buildFilesApiGenerateBody", () => {
  test("references the uploaded file by file_id and uses the intent prompt", () => {
    const body = buildFilesApiGenerateBody({
      model: SONNET,
      fileId: "file_abc",
      intent: INTENTS["files-api-reference"],
      stream: false,
    });
    const first = body.messages[0];
    if (first === undefined || typeof first.content === "string") {
      throw new Error("expected an array-content message");
    }
    expect(first.content[0]).toEqual({
      type: "document",
      source: { type: "file", file_id: "file_abc" },
    });
    expect(first.content[1]).toEqual({
      type: "text",
      text: INTENTS["files-api-reference"].prompt,
    });
    expect(body.stream).toBeUndefined();
  });

  test("sets stream when the streaming variant is requested", () => {
    const body = buildFilesApiGenerateBody({
      model: SONNET,
      fileId: "file_abc",
      intent: INTENTS["files-api-reference-streaming"],
      stream: true,
    });
    expect(body.stream).toBe(true);
  });
});

describe("iterateCaptureSteps", () => {
  test("single-step capability yields one JSON step with no subdir", () => {
    const steps = collectSteps({
      model: SONNET,
      capability: "plain-text",
      responses: [],
    });
    expect(steps.length).toBe(1);
    const [only] = steps;
    if (only === undefined) throw new Error("missing step");
    expect(only.kind).toBe("json");
    expect(only.subdir).toBeNull();
    expect(only.url).toBe("https://api.anthropic.com/v1/messages");
  });

  test("multi-turn function-calling yields turn-1 then turn-2 JSON steps", () => {
    const turn1Response: CapturedResponse = {
      status: 200,
      headers: {},
      parsed: {
        content: [
          {
            type: "tool_use",
            id: "tool_use_1",
            name: "get_weather",
            input: { location: "Boston, MA" },
          },
        ],
      },
    };
    const steps = collectSteps({
      model: SONNET,
      capability: "function-calling-multi-turn",
      responses: [turn1Response],
    });
    expect(steps.length).toBe(2);
    expect(steps[0]?.subdir).toBe("turn-1");
    expect(steps[1]?.subdir).toBe("turn-2");
    for (const step of steps) {
      expect(step.kind).toBe("json");
      expect(step.url).toBe("https://api.anthropic.com/v1/messages");
    }
  });

  test("redacted-thinking yields turn-1 then turn-2 JSON steps", () => {
    const turn1Response: CapturedResponse = {
      status: 200,
      headers: {},
      parsed: {
        content: [
          { type: "redacted_thinking", data: "opaque-bytes" },
          { type: "text", text: "Done." },
        ],
      },
    };
    const steps = collectSteps({
      model: SONNET,
      capability: "redacted-thinking",
      responses: [turn1Response],
    });
    expect(steps.length).toBe(2);
    expect(steps[0]?.subdir).toBe("turn-1");
    expect(steps[1]?.subdir).toBe("turn-2");
  });

  test("files-api yields a raw upload step then a JSON generate step", () => {
    const uploadResponse: CapturedResponse = {
      status: 200,
      headers: {},
      parsed: { id: "file_abc", filename: "sample.pdf" },
    };
    const steps = collectSteps({
      model: SONNET,
      capability: "files-api-reference",
      responses: [uploadResponse],
    });
    expect(steps.length).toBe(2);
    const [upload, generate] = steps;
    if (upload === undefined || generate === undefined) {
      throw new Error("missing steps");
    }
    expect(upload.kind).toBe("raw");
    expect(upload.subdir).toBe("upload");
    expect(upload.url).toBe("https://api.anthropic.com/v1/files");
    if (upload.kind !== "raw") throw new Error("unreachable");
    expect(upload.contentType).toMatch(/^multipart\/form-data; boundary=/);
    expect(upload.headers?.["anthropic-beta"]).toBe("files-api-2025-04-14");
    expect(upload.body.byteLength).toBeGreaterThan(0);

    expect(generate.kind).toBe("json");
    expect(generate.subdir).toBe("generate");
    expect(generate.url).toBe("https://api.anthropic.com/v1/messages");
  });

  test("code-execution carries the beta header on the per-step headers map", () => {
    const steps = collectSteps({
      model: SONNET,
      capability: "code-execution",
      responses: [],
    });
    const [only] = steps;
    if (only === undefined) throw new Error("missing step");
    expect(only.headers?.["anthropic-beta"]).toBe("code-execution-2025-05-22");
  });

  test("plain-text does NOT carry any anthropic-beta header", () => {
    const steps = collectSteps({
      model: SONNET,
      capability: "plain-text",
      responses: [],
    });
    const [only] = steps;
    if (only === undefined) throw new Error("missing step");
    expect(only.headers?.["anthropic-beta"]).toBeUndefined();
  });
});

describe("extractReasoningTrace", () => {
  test("returns the first thinking block with field path and signature", () => {
    const trace = extractReasoningTrace({
      content: [
        {
          type: "thinking",
          thinking: "Let me reason through this step by step.",
          signature: "sig-abc",
        },
        { type: "text", text: "Result." },
      ],
    });
    expect(trace).toEqual({
      blockType: "thinking",
      fieldPath: "content[0].thinking",
      sample: "Let me reason through this step by step.",
      signature: "sig-abc",
    });
  });

  test("returns the first redacted_thinking block with field path", () => {
    const trace = extractReasoningTrace({
      content: [
        {
          type: "redacted_thinking",
          data: "encrypted-bytes-here",
          signature: "sig-z",
        },
      ],
    });
    expect(trace).toEqual({
      blockType: "redacted_thinking",
      fieldPath: "content[0].data",
      sample: "encrypted-bytes-here",
      signature: "sig-z",
    });
  });

  test("returns null when no thinking-class block is present", () => {
    expect(
      extractReasoningTrace({
        content: [{ type: "text", text: "no thinking" }],
      }),
    ).toBeNull();
  });

  test("returns null for a non-object input", () => {
    expect(extractReasoningTrace("not an object")).toBeNull();
    expect(extractReasoningTrace(null)).toBeNull();
  });
});

describe("isSupportedCapability", () => {
  test("recognises everything Anthropic exposes and rejects the rest", () => {
    expect(isSupportedCapability("plain-text")).toBe(true);
    expect(isSupportedCapability("redacted-thinking")).toBe(true);
    expect(isSupportedCapability("audio-input")).toBe(false);
    expect(isSupportedCapability("image-output")).toBe(false);
  });
});
