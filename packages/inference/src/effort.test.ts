import {
  afterEach,
  beforeEach,
  describe,
  expect,
  mock,
  spyOn,
  test,
} from "bun:test";
import { type } from "arktype";

import { getLogger } from "@intx/log";
import type {
  ConversationTurn,
  LastCycleSource,
} from "@intx/types/runtime-core";

import {
  budgetSource,
  rejectBudgetOverCap,
  resetDroppedOptionReports,
} from "./effort";
import { createAnthropicAdapter } from "./providers/anthropic";
import { createGoogleGenAIAdapter } from "./providers/google-genai";
import { createOpenAIAdapter } from "./providers/openai";

const logger = getLogger(["interchange", "inference", "adapter"]);

const messages: ConversationTurn[] = [
  {
    role: "user",
    content: [{ type: "text", text: "hi" }],
    timestamp: 1000,
  },
];

const anthropicSource: LastCycleSource = {
  sourceId: "test-anthropic",
  provider: "anthropic",
  model: "test-anthropic-model",
};

const openaiSource: LastCycleSource = {
  sourceId: "test-openai",
  provider: "openai",
  model: "test-openai-model",
};

const geminiSource: LastCycleSource = {
  sourceId: "test-google-genai",
  provider: "google-genai",
  model: "test-google-genai-model",
};

const tool = {
  name: "t",
  description: "t",
  inputSchema: {},
};

beforeEach(resetDroppedOptionReports);

afterEach(() => {
  mock.restore();
});

function warnSpy() {
  return spyOn(logger, "warn");
}

const WireBody = type("Record<string, unknown>");

function wireBody(body: string): Record<string, unknown> {
  const parsed = WireBody(JSON.parse(body));
  if (parsed instanceof type.errors) {
    throw new Error(`unexpected request body shape: ${parsed.summary}`);
  }
  return parsed;
}

function droppedCall(warn: ReturnType<typeof warnSpy>): {
  option: unknown;
  model: unknown;
  reason: unknown;
} {
  expect(warn).toHaveBeenCalledTimes(1);
  const args = warn.mock.calls[0];
  if (args === undefined) throw new Error("expected a warn call");
  const values: unknown[] = [];
  for (const arg of args) values.push(arg);
  return { option: values[1], model: values[2], reason: values[3] };
}

describe("dropped option warn", () => {
  test("classic Anthropic with thinking off drops a named effort", () => {
    const warn = warnSpy();
    const req = createAnthropicAdapter(anthropicSource).buildRequest(
      messages,
      "claude-haiku-4-5-20251001",
      { thinking: { enabled: false }, effort: "high" },
    );
    const call = droppedCall(warn);
    expect(call.option).toBe('effort "high"');
    expect(call.model).toBe("claude-haiku-4-5-20251001");
    expect(call.reason).toBe(
      "classic Anthropic carries effort only as a thinking budget, and thinking is off",
    );
    const body = wireBody(req.body);
    expect(body).not.toHaveProperty("thinking");
    expect(body).not.toHaveProperty("output_config");
  });

  test("adaptive Anthropic drops a thinking budget", () => {
    const warn = warnSpy();
    createAnthropicAdapter(anthropicSource).buildRequest(
      messages,
      "claude-opus-5",
      { thinking: { enabled: true, budgetTokens: 2048 } },
    );
    const call = droppedCall(warn);
    expect(call.option).toBe("thinking.budgetTokens 2048");
    expect(call.reason).toBe(
      "adaptive Anthropic takes effort, not a thinking budget",
    );
  });

  test("adaptive Anthropic with thinking off keeps a named effort", () => {
    const warn = warnSpy();
    createAnthropicAdapter(anthropicSource).buildRequest(
      messages,
      "claude-opus-5",
      { thinking: { enabled: false }, effort: "low" },
    );
    expect(warn).not.toHaveBeenCalled();
  });

  test("Gemini with thinking off drops a named effort", () => {
    const warn = warnSpy();
    const req = createGoogleGenAIAdapter(geminiSource).buildRequest(
      messages,
      "gemini-2.5-flash",
      { thinking: { enabled: false }, effort: "medium" },
    );
    const call = droppedCall(warn);
    expect(call.option).toBe('effort "medium"');
    expect(call.reason).toBe(
      "Gemini carries effort only as a thinking setting, and thinking is off",
    );
    expect(wireBody(req.body)["generationConfig"]).toEqual({
      thinkingConfig: { thinkingBudget: 0 },
    });
  });

  test.each([["gemini-2.5-flash-image"], ["gemini-3.1-flash-image"]])(
    "the Gemini image model %p drops a named effort and nothing else",
    (model) => {
      const warn = warnSpy();
      const req = createGoogleGenAIAdapter(geminiSource).buildRequest(
        messages,
        model,
        { effort: "high" },
      );
      const call = droppedCall(warn);
      expect(call.option).toBe('effort "high"');
      expect(call.reason).toBe(
        "this model has no thinking setting an effort maps to",
      );
      expect(wireBody(req.body)).not.toHaveProperty("generationConfig");
    },
  );

  test("gpt-5.6 with tools drops a named effort", () => {
    const warn = warnSpy();
    const req = createOpenAIAdapter(openaiSource).buildRequest(
      messages,
      "gpt-5.6-sol",
      { effort: "high", tools: [tool] },
    );
    const call = droppedCall(warn);
    expect(call.option).toBe('effort "high"');
    expect(call.reason).toBe(
      "gpt-5.6 Chat Completions tool calls require reasoning_effort none",
    );
    expect(wireBody(req.body)["reasoning_effort"]).toBe("none");
  });

  test("gpt-5.6 with tools and max reports only the tool override", () => {
    const warn = warnSpy();
    createOpenAIAdapter(openaiSource).buildRequest(messages, "gpt-5.6-terra", {
      effort: "max",
      tools: [tool],
    });
    const call = droppedCall(warn);
    expect(call.reason).toBe(
      "gpt-5.6 Chat Completions tool calls require reasoning_effort none",
    );
  });

  test("a Gemini level model with thinking off drops a named effort", () => {
    const warn = warnSpy();
    const req = createGoogleGenAIAdapter(geminiSource).buildRequest(
      messages,
      "gemini-3.6-flash",
      { thinking: { enabled: false }, effort: "high" },
    );
    expect(droppedCall(warn).option).toBe('effort "high"');
    expect(wireBody(req.body)["generationConfig"]).toEqual({
      thinkingConfig: { thinkingBudget: -1 },
    });
  });

  test("the Gemini image models keep today's thinking budget", () => {
    for (const model of ["gemini-2.5-flash-image", "gemini-3.1-flash-image"]) {
      const req = createGoogleGenAIAdapter(geminiSource).buildRequest(
        messages,
        model,
        { thinking: { enabled: true }, effort: "high" },
      );
      expect(wireBody(req.body)["generationConfig"]).toEqual({
        thinkingConfig: { thinkingBudget: 1024, includeThoughts: true },
      });
    }
  });

  test("gpt-5.6 with tools and no named effort does not warn", () => {
    const warn = warnSpy();
    createOpenAIAdapter(openaiSource).buildRequest(messages, "gpt-5.6-sol", {
      tools: [tool],
    });
    expect(warn).not.toHaveBeenCalled();
  });

  test("OpenAI max with no maxEffort quirk sends high and warns", () => {
    const warn = warnSpy();
    const req = createOpenAIAdapter(openaiSource).buildRequest(
      messages,
      "gpt-5.5",
      { effort: "max" },
    );
    const call = droppedCall(warn);
    expect(call.option).toBe('effort "max"');
    expect(call.reason).toBe(
      'no maxEffort quirk on this source, so it sends "high"',
    );
    expect(wireBody(req.body)["reasoning_effort"]).toBe("high");
  });

  test("OpenAI max with a maxEffort quirk does not warn", () => {
    const warn = warnSpy();
    createOpenAIAdapter(openaiSource, { maxEffort: "xhigh" }).buildRequest(
      messages,
      "gpt-5.5",
      { effort: "max" },
    );
    expect(warn).not.toHaveBeenCalled();
  });

  test("a repeated identical drop is reported once", () => {
    const warn = warnSpy();
    const adapter = createGoogleGenAIAdapter(geminiSource);
    const options = { thinking: { enabled: false }, effort: "low" } as const;
    adapter.buildRequest(messages, "gemini-2.5-pro", options);
    adapter.buildRequest(messages, "gemini-2.5-pro", options);
    expect(warn).toHaveBeenCalledTimes(1);
  });

  test("drops of different values are reported separately", () => {
    const warn = warnSpy();
    const adapter = createGoogleGenAIAdapter(geminiSource);
    adapter.buildRequest(messages, "gemini-2.5-pro", {
      thinking: { enabled: false },
      effort: "low",
    });
    adapter.buildRequest(messages, "gemini-2.5-pro", {
      thinking: { enabled: false },
      effort: "high",
    });
    expect(warn).toHaveBeenCalledTimes(2);
  });

  test("unset effort never warns", () => {
    const warn = warnSpy();
    createAnthropicAdapter(anthropicSource).buildRequest(
      messages,
      "claude-haiku-4-5-20251001",
      { thinking: { enabled: true } },
    );
    createGoogleGenAIAdapter(geminiSource).buildRequest(
      messages,
      "gemini-2.5-flash",
      { thinking: { enabled: true } },
    );
    createOpenAIAdapter(openaiSource).buildRequest(messages, "gpt-5.5", {});
    expect(warn).not.toHaveBeenCalled();
  });
});

describe("budgetSource", () => {
  test("names the effort when no budget is set", () => {
    expect(budgetSource("medium", undefined)).toBe('effort "medium"');
  });

  test("names the explicit budget when one is set", () => {
    expect(budgetSource("low", 5000)).toBe("thinking.budgetTokens");
  });
});

describe("rejectBudgetOverCap", () => {
  test("accepts a cap above the budget", () => {
    expect(() =>
      rejectBudgetOverCap("m", 'effort "low"', 1024, 1025),
    ).not.toThrow();
  });

  test.each([[8192], [4096]])(
    "rejects a cap of %p under a budget of 8192",
    (maxTokens) => {
      expect(() =>
        rejectBudgetOverCap("m", 'effort "medium"', 8192, maxTokens),
      ).toThrow(
        `effort "medium" on model m needs a thinking budget of 8192 tokens, ` +
          `but maxTokens is ${maxTokens}; set maxTokens above 8192.`,
      );
    },
  );
});
