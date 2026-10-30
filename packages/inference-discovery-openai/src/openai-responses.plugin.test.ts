import { describe, test, expect } from "bun:test";
import { INTENTS, SUPPORT_MATRIX } from "@intx/inference-discovery/catalog";
import type { CaptureStep, CapturedResponse } from "@intx/inference-discovery";
import type { Capability } from "@intx/inference-discovery/catalog";
import { createOpenAIResponsesPlugin } from "./index";
import { createResponsesIterator } from "./protocol/responses-iterator";
import {
  buildResponsesMultiTurnTurn1Body,
  buildResponsesMultiTurnTurn2Body,
  buildResponsesRequestBody,
} from "./protocol/responses-body";

const TEST_API_KEY = "test-key";
const TEST_BASE_URL = "https://api.openai.com/v1";
const TEST_RESPONSES_URL = `${TEST_BASE_URL}/responses`;

function makePlugin() {
  return createOpenAIResponsesPlugin({ apiKey: TEST_API_KEY });
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function collectSteps(opts: {
  model: string;
  capability: Capability;
  responses: readonly CapturedResponse[];
}): CaptureStep[] {
  const intent = INTENTS[opts.capability];
  const iter = createResponsesIterator(TEST_BASE_URL)({
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

describe("createOpenAIResponsesPlugin", () => {
  test("declares provider name, models, and redaction lists", () => {
    const plugin = makePlugin();
    expect(plugin.name).toBe("openai-responses");
    expect([...plugin.models]).toEqual([
      "gpt-6-astra",
      "gpt-6.1-sol",
      "gpt-6.1-terra",
      "gpt-6-luna",
      "gpt-6-sol",
      "gpt-5.6-sol",
      "gpt-5.6-terra",
      "gpt-5.6-luna",
      "gpt-5.5",
      "gpt-5",
      "gpt-5-mini",
      "gpt-5-nano",
      "gpt-5.1",
      "gpt-5.2",
      "gpt-5.4",
      "gpt-5.4-mini",
      "gpt-5.4-nano",
      "o1",
      "o3",
      "o3-mini",
      "o4-mini",
    ]);
    expect(plugin.redactRequestHeaders).toEqual(["authorization"]);
    expect(plugin.redactResponseHeaders).toEqual([
      "set-cookie",
      "x-request-id",
      "openai-organization",
      "openai-project",
    ]);
  });

  test("every openai-responses model in the support matrix is advertised", () => {
    const advertised = new Set(makePlugin().models);
    const matrixModels = new Set(
      SUPPORT_MATRIX.filter(
        (entry) => entry.provider === "openai-responses",
      ).map((entry) => entry.model),
    );
    const unadvertised = [...matrixModels].filter(
      (model) => !advertised.has(model),
    );
    expect(unadvertised).toEqual([]);
  });

  test("buildAuthHeaders attaches a Bearer token", () => {
    expect(makePlugin().buildAuthHeaders()).toEqual({
      Authorization: `Bearer ${TEST_API_KEY}`,
    });
  });

  test("targets the first-party api.openai.com responses endpoint", () => {
    const plugin = makePlugin();
    const iter = plugin.iterateCaptureSteps({
      model: "gpt-5.5",
      capability: "plain-text",
      intent: INTENTS["plain-text"],
    });
    const first = iter.next();
    if (first.done) throw new Error("expected a capture step");
    expect(first.value.url).toBe("https://api.openai.com/v1/responses");
  });
});

describe("buildResponsesRequestBody capability dispatch", () => {
  test("plain-text returns a user message with no stream flag", () => {
    const body = buildResponsesRequestBody({
      model: "gpt-5.5",
      capability: "plain-text",
      intent: INTENTS["plain-text"],
    });
    if (!isRecord(body)) throw new Error("expected record body");
    expect(body.model).toBe("gpt-5.5");
    expect(body.stream).toBeUndefined();
    const input = body.input;
    if (!Array.isArray(input)) throw new Error("expected input array");
    expect(input).toEqual([
      { type: "message", role: "user", content: INTENTS["plain-text"].prompt },
    ]);
  });

  test("plain-text-streaming sets stream: true", () => {
    const body = buildResponsesRequestBody({
      model: "gpt-5.5",
      capability: "plain-text-streaming",
      intent: INTENTS["plain-text-streaming"],
    });
    if (!isRecord(body)) throw new Error("expected record body");
    expect(body.stream).toBe(true);
  });

  test("function-calling produces flat tools with auto tool_choice", () => {
    const body = buildResponsesRequestBody({
      model: "gpt-5.5",
      capability: "function-calling",
      intent: INTENTS["function-calling"],
    });
    if (!isRecord(body)) throw new Error("expected record body");
    expect(body.tools).toEqual([
      {
        type: "function",
        name: "get_weather",
        description: "Look up the current weather for a city.",
        parameters: {
          type: "object",
          properties: {
            location: {
              type: "string",
              description: "City and optional region, e.g. 'Boston, MA'.",
            },
          },
          required: ["location"],
        },
      },
    ]);
    expect(body.tool_choice).toBe("auto");
  });

  test("function-calling-multi-turn throws (multi-step capability)", () => {
    expect(() =>
      buildResponsesRequestBody({
        model: "gpt-5.5",
        capability: "function-calling-multi-turn",
        intent: INTENTS["function-calling-multi-turn"],
      }),
    ).toThrow(/multi-step capability/);
  });

  test("reasoning-content requests medium effort", () => {
    const body = buildResponsesRequestBody({
      model: "gpt-5.5",
      capability: "reasoning-content",
      intent: INTENTS["reasoning-content"],
    });
    if (!isRecord(body)) throw new Error("expected record body");
    expect(body.reasoning).toEqual({ effort: "medium" });
    expect(body.stream).toBeUndefined();
  });

  test("reasoning-content-streaming sets stream: true", () => {
    const body = buildResponsesRequestBody({
      model: "gpt-5.5",
      capability: "reasoning-content-streaming",
      intent: INTENTS["reasoning-content-streaming"],
    });
    if (!isRecord(body)) throw new Error("expected record body");
    expect(body.stream).toBe(true);
  });

  test("vision-input embeds image as a data URI part", () => {
    const body = buildResponsesRequestBody({
      model: "gpt-5.5",
      capability: "vision-input",
      intent: INTENTS["vision-input"],
    });
    if (!isRecord(body)) throw new Error("expected record body");
    const input = body.input;
    if (!Array.isArray(input) || !isRecord(input[0])) {
      throw new Error("expected input[0] to be a record");
    }
    const content = input[0].content;
    if (!Array.isArray(content)) throw new Error("expected content array");
    const imagePart = content.find(
      (p) => isRecord(p) && p.type === "input_image",
    );
    if (!isRecord(imagePart)) throw new Error("expected input_image part");
    expect(typeof imagePart.image_url).toBe("string");
    expect(
      String(imagePart.image_url).startsWith("data:image/jpeg;base64,"),
    ).toBe(true);
  });

  test("document-input embeds PDF as an input_file part", () => {
    const body = buildResponsesRequestBody({
      model: "gpt-5.5",
      capability: "document-input",
      intent: INTENTS["document-input"],
    });
    if (!isRecord(body)) throw new Error("expected record body");
    const input = body.input;
    if (!Array.isArray(input) || !isRecord(input[0])) {
      throw new Error("expected input[0] to be a record");
    }
    const content = input[0].content;
    if (!Array.isArray(content)) throw new Error("expected content array");
    const filePart = content.find(
      (p) => isRecord(p) && p.type === "input_file",
    );
    if (!isRecord(filePart)) throw new Error("expected input_file part");
    expect(filePart.filename).toBe("sample.pdf");
    expect(typeof filePart.file_data).toBe("string");
    expect(
      String(filePart.file_data).startsWith("data:application/pdf;base64,"),
    ).toBe(true);
  });

  test("unsupported capabilities throw CapabilityNotBuildableError", () => {
    for (const capability of [
      "audio-input",
      "video-input",
      "structured-output",
      "structured-output-streaming",
      "image-output",
      "code-execution",
      "grounding",
    ] as const) {
      expect(() =>
        buildResponsesRequestBody({
          model: "gpt-5.5",
          capability,
          intent: INTENTS[capability],
        }),
      ).toThrow();
    }
  });
});

describe("responses multi-turn iterator", () => {
  const turn1Response: CapturedResponse = {
    status: 200,
    headers: {},
    parsed: {
      id: "resp-1",
      output: [
        {
          type: "function_call",
          id: "fc-1",
          call_id: "call_test",
          name: "get_weather",
          arguments: '{"location":"Boston, MA"}',
        },
      ],
    },
    bytes: null,
  };

  test("turn-2 replays the function_call and appends the tool output", () => {
    const steps = collectSteps({
      model: "gpt-5.5",
      capability: "function-calling-multi-turn",
      responses: [turn1Response],
    });
    expect(steps.length).toBe(2);
    const [step1, step2] = steps;
    if (step1 === undefined || step2 === undefined) {
      throw new Error("expected two steps for multi-turn");
    }
    expect(step1.url).toBe(TEST_RESPONSES_URL);
    expect(step2.url).toBe(TEST_RESPONSES_URL);
    if (!isRecord(step2.body)) throw new Error("expected record turn-2 body");
    expect(step2.body.input).toEqual([
      {
        type: "message",
        role: "user",
        content: INTENTS["function-calling-multi-turn"].prompt,
      },
      {
        type: "function_call",
        call_id: "call_test",
        name: "get_weather",
        arguments: '{"location":"Boston, MA"}',
      },
      {
        type: "function_call_output",
        call_id: "call_test",
        output:
          '{"location":"Boston, MA","temperatureF":68,"conditions":"clear"}',
      },
    ]);
    expect(step2.body.tool_choice).toBe("auto");
  });

  test("turn-2 without a function_call in turn-1 throws", () => {
    expect(() =>
      buildResponsesMultiTurnTurn2Body({
        model: "gpt-5.5",
        intent: INTENTS["function-calling-multi-turn"],
        turn1Body: buildResponsesMultiTurnTurn1Body({
          model: "gpt-5.5",
          intent: INTENTS["function-calling-multi-turn"],
        }),
        turn1Response: { output: [{ type: "message" }] },
      }),
    ).toThrow(/no function_call/);
  });
});
