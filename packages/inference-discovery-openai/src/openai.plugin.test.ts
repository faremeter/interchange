import { describe, test, expect } from "bun:test";
import { INTENTS } from "@intx/inference-discovery/catalog";
import { createOpenAIPlugin } from "./index";

const TEST_API_KEY = "test-key";

function makePlugin() {
  return createOpenAIPlugin({ apiKey: TEST_API_KEY });
}

describe("createOpenAIPlugin", () => {
  test("declares provider name, model, and redaction lists", () => {
    const plugin = makePlugin();
    expect(plugin.name).toBe("openai");
    expect([...plugin.models]).toEqual(["gpt-5.5"]);
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
