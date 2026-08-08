import { describe, test, expect } from "bun:test";
import { INTENTS } from "@intx/inference-discovery/catalog";
import { createOpenAIPlugin } from "./index";
import {
  buildMultiTurnTurn1Body,
  buildMultiTurnTurn2Body,
  buildRequestBody,
} from "./protocol/body";

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
      "gpt-4",
      "gpt-4-turbo",
      "gpt-4.1",
      "gpt-4.1-mini",
      "gpt-4.1-nano",
      "gpt-4o",
      "gpt-4o-mini",
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

  test("sets reasoning_effort none on gpt-5.6 multi-turn turn-2 bodies", () => {
    const intent = INTENTS["function-calling-multi-turn"];
    const turn1Response = {
      choices: [
        {
          message: {
            role: "assistant",
            content: null,
            tool_calls: [
              {
                id: "call_test",
                type: "function",
                function: {
                  name: "get_weather",
                  arguments: '{"location":"Boston, MA"}',
                },
              },
            ],
          },
        },
      ],
    };
    for (const model of [
      "gpt-5.6-sol",
      "gpt-5.6-terra",
      "gpt-5.6-luna",
    ] as const) {
      const turn1Body = buildMultiTurnTurn1Body({ model, intent });
      const turn2 = buildMultiTurnTurn2Body({
        model,
        intent,
        turn1Body,
        turn1Response,
      });
      expect(turn2.reasoning_effort).toBe("none");
      expect(turn2.tools).toBeDefined();
    }

    const legacyTurn1 = buildMultiTurnTurn1Body({
      model: "gpt-5.5",
      intent,
    });
    const legacyTurn2 = buildMultiTurnTurn2Body({
      model: "gpt-5.5",
      intent,
      turn1Body: legacyTurn1,
      turn1Response,
    });
    expect(legacyTurn2.reasoning_effort).toBeUndefined();
  });
});
