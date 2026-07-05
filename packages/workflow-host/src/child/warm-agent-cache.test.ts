import { describe, test, expect } from "bun:test";
import type { Agent } from "@intx/agent";
import type { InferenceSource } from "@intx/types/runtime";

import {
  createWarmAgentCache,
  type WarmEventSinkRef,
} from "./warm-agent-cache";

function makeSource(id: string): InferenceSource {
  return {
    id,
    provider: "anthropic",
    baseURL: "https://api.anthropic.com",
    apiKey: `sk-${id}`,
    model: "claude-test",
  };
}

function stubAgent(): {
  agent: Agent;
  calls: { sources: InferenceSource[]; defaultSource: string }[];
} {
  const calls: { sources: InferenceSource[]; defaultSource: string }[] = [];
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test stub: applySources only calls setSources on the entry's agent
  const agent = {
    setSources(sources: InferenceSource[], defaultSource: string) {
      calls.push({ sources, defaultSource });
    },
  } as unknown as Agent;
  return { agent, calls };
}

describe("warm-agent cache applySources", () => {
  test("applies the rotated sources to a built warm agent", () => {
    const cache = createWarmAgentCache();
    const { agent, calls } = stubAgent();
    const sinkRef: WarmEventSinkRef = { current: null };
    cache.store("step-1", agent, sinkRef, Promise.resolve());

    const sources = [makeSource("primary"), makeSource("failover")];
    cache.applySources(sources, "primary");

    expect(calls).toHaveLength(1);
    expect(calls[0]?.sources).toEqual(sources);
    expect(calls[0]?.defaultSource).toBe("primary");
  });

  test("is a no-op when no warm agent is built yet", () => {
    const cache = createWarmAgentCache();
    // The pre-first-build window: a rotation arriving before the agent is
    // built must not throw; the ref carries it to the next build.
    expect(() =>
      cache.applySources([makeSource("primary")], "primary"),
    ).not.toThrow();
  });
});
