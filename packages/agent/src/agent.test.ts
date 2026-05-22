import { describe, test, expect } from "bun:test";

import type { ContextStore, InferenceSource } from "@intx/types/runtime";

import { AgentConfigError, createAgent } from "./agent";

const SOURCE: InferenceSource = {
  id: "anthropic:claude-3-5-sonnet",
  provider: "anthropic",
  baseURL: "https://api.anthropic.com",
  apiKey: "sk-test",
  model: "claude-3-5-sonnet",
};

function stubContextStore(): ContextStore {
  // Storage-validation tests reject before touching the store; the stub
  // never has any of its methods called.
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test stub, never invoked
  return {} as ContextStore;
}

describe("createAgent storage configuration", () => {
  test("rejects when both contextStore and contextDir are given", async () => {
    await expect(
      createAgent({
        contextStore: stubContextStore(),
        contextDir: "/tmp/agent-config-1",
        sources: [SOURCE],
        defaultSource: SOURCE.id,
        systemPrompt: "test",
        tools: [],
      }),
    ).rejects.toBeInstanceOf(AgentConfigError);
  });

  test("rejects when neither contextStore nor contextDir is given", async () => {
    await expect(
      createAgent({
        sources: [SOURCE],
        defaultSource: SOURCE.id,
        systemPrompt: "test",
        tools: [],
      }),
    ).rejects.toBeInstanceOf(AgentConfigError);
  });
});
