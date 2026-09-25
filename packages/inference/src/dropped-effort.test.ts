import { afterEach, describe, expect, mock, spyOn, test } from "bun:test";

import { getLogger } from "@intx/log";
import type { ConversationTurn, LastCycleSource } from "@intx/types/runtime";

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

afterEach(() => {
  mock.restore();
});

function warnSpy() {
  return spyOn(logger, "warn");
}

function droppedCall(warn: ReturnType<typeof warnSpy>): {
  effort: unknown;
  model: unknown;
  reason: unknown;
} {
  expect(warn).toHaveBeenCalledTimes(1);
  const args = warn.mock.calls[0];
  if (args === undefined) throw new Error("expected a warn call");
  const values: unknown[] = [];
  for (const arg of args) values.push(arg);
  return { effort: values[1], model: values[2], reason: values[3] };
}

describe("dropped effort warn", () => {
  test("classic Anthropic drops a named effort", () => {
    const warn = warnSpy();
    createAnthropicAdapter(anthropicSource).buildRequest(
      messages,
      "claude-3-7-sonnet-20250219",
      { thinking: { enabled: true, budgetTokens: 2048 }, effort: "max" },
    );
    const call = droppedCall(warn);
    expect(call.effort).toBe("max");
    expect(call.model).toBe("claude-3-7-sonnet-20250219");
    expect(call.reason).toBe(
      "classic Anthropic has no effort field on the wire",
    );
  });

  test("adaptive Anthropic without thinking drops a named effort", () => {
    const warn = warnSpy();
    createAnthropicAdapter(anthropicSource).buildRequest(
      messages,
      "claude-opus-5",
      { effort: "max" },
    );
    const call = droppedCall(warn);
    expect(call.effort).toBe("max");
    expect(call.model).toBe("claude-opus-5");
    expect(call.reason).toBe(
      "adaptive Anthropic emits effort only when thinking is enabled",
    );
  });

  test("adaptive Anthropic with thinking on does not warn", () => {
    const warn = warnSpy();
    createAnthropicAdapter(anthropicSource).buildRequest(
      messages,
      "claude-opus-5",
      { thinking: { enabled: true }, effort: "low" },
    );
    expect(warn).not.toHaveBeenCalled();
  });

  test("Gemini drops a named effort", () => {
    const warn = warnSpy();
    createGoogleGenAIAdapter(geminiSource).buildRequest(
      messages,
      "gemini-2.5-flash",
      { effort: "high" },
    );
    const call = droppedCall(warn);
    expect(call.effort).toBe("high");
    expect(call.model).toBe("gemini-2.5-flash");
    expect(call.reason).toBe("Gemini has no effort field on the wire");
  });

  test("gpt-5.6 with tools drops a named effort", () => {
    const warn = warnSpy();
    createOpenAIAdapter(openaiSource).buildRequest(messages, "gpt-5.6-sol", {
      effort: "high",
      tools: [tool],
    });
    const call = droppedCall(warn);
    expect(call.effort).toBe("high");
    expect(call.model).toBe("gpt-5.6-sol");
    expect(call.reason).toBe(
      "gpt-5.6 Chat Completions tool calls require reasoning_effort none",
    );
  });

  test("gpt-5.6 with tools and no named effort does not warn", () => {
    const warn = warnSpy();
    createOpenAIAdapter(openaiSource).buildRequest(messages, "gpt-5.6-sol", {
      tools: [tool],
    });
    expect(warn).not.toHaveBeenCalled();
  });

  test("OpenAI without the gpt-5.6 tool override does not warn", () => {
    const warn = warnSpy();
    createOpenAIAdapter(openaiSource).buildRequest(messages, "gpt-5.5", {
      effort: "high",
    });
    expect(warn).not.toHaveBeenCalled();
  });

  test("unset effort never warns", () => {
    const warn = warnSpy();
    createAnthropicAdapter(anthropicSource).buildRequest(
      messages,
      "claude-3-7-sonnet-20250219",
      {},
    );
    createGoogleGenAIAdapter(geminiSource).buildRequest(
      messages,
      "gemini-2.5-flash",
      {},
    );
    expect(warn).not.toHaveBeenCalled();
  });
});
