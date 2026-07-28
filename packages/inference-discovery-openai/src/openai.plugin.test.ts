import { describe, test, expect } from "bun:test";
import { INTENTS } from "@intx/inference-discovery/catalog";
import { createOpenAIPlugin } from "./index";
import { buildMultiTurnTurn1Body, buildRequestBody } from "./protocol/body";

const TEST_API_KEY = "test-key";

function makePlugin() {
  return createOpenAIPlugin({ apiKey: TEST_API_KEY });
}

function reasoningEffortOf(body: unknown): unknown {
  if (typeof body !== "object" || body === null) {
    throw new Error("expected a Chat Completions body object");
  }
  return Reflect.get(body, "reasoning_effort");
}

function toolsOf(body: unknown): unknown {
  if (typeof body !== "object" || body === null) {
    throw new Error("expected a Chat Completions body object");
  }
  return Reflect.get(body, "tools");
}

describe("createOpenAIPlugin", () => {
  test("declares provider name, model, and redaction lists", () => {
    const plugin = makePlugin();
    expect(plugin.name).toBe("openai");
    expect([...plugin.models]).toEqual([
      "gpt-5.5",
      "gpt-5.6-sol",
      "gpt-5.6-terra",
      "gpt-5.6-luna",
    ]);
    expect(plugin.redactRequestHeaders).toEqual(["authorization"]);
    expect(plugin.redactResponseHeaders).toEqual([
      "set-cookie",
      "x-request-id",
      "openai-organization",
    ]);
  });

  test("buildAuthHeaders attaches a Bearer token", () => {
    expect(makePlugin().buildAuthHeaders()).toEqual({
      Authorization: "Bearer test-key",
    });
  });

  test("targets the first-party api.openai.com chat completions endpoint", () => {
    const plugin = makePlugin();
    const iter = plugin.iterateCaptureSteps({
      model: "gpt-5.5",
      capability: "plain-text",
      intent: INTENTS["plain-text"],
    });
    const first = iter.next();
    if (first.done) throw new Error("expected a capture step");
    expect(first.value.url).toBe("https://api.openai.com/v1/chat/completions");
  });
});

describe("gpt-5.6 tool-calling reasoning_effort", () => {
  test("sets reasoning_effort none on gpt-5.6 function-calling bodies", () => {
    for (const model of [
      "gpt-5.6-sol",
      "gpt-5.6-terra",
      "gpt-5.6-luna",
    ] as const) {
      const body = buildRequestBody({
        model,
        capability: "function-calling",
        intent: INTENTS["function-calling"],
      });
      expect(reasoningEffortOf(body)).toBe("none");
      expect(toolsOf(body)).toBeDefined();

      const multiTurn = buildMultiTurnTurn1Body({
        model,
        intent: INTENTS["function-calling-multi-turn"],
      });
      expect(multiTurn.reasoning_effort).toBe("none");
    }
  });

  test("does not set reasoning_effort on gpt-5.5 function-calling bodies", () => {
    const body = buildRequestBody({
      model: "gpt-5.5",
      capability: "function-calling",
      intent: INTENTS["function-calling"],
    });
    expect(reasoningEffortOf(body)).toBeUndefined();
  });
});
