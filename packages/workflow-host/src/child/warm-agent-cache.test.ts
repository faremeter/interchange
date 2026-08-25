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

  test("applies to every agent even when one rejects the rotation", () => {
    // One agent rejecting the rotation must not skip the rest; the
    // failures surface together rather than stranding later agents.
    const cache = createWarmAgentCache();
    const rotateError = new Error("bad source");
    let secondApplied = false;
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test stub: applySources only calls setSources
    const firstAgent = {
      setSources() {
        throw rotateError;
      },
    } as unknown as Agent;
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test stub: applySources only calls setSources
    const secondAgent = {
      setSources() {
        secondApplied = true;
      },
    } as unknown as Agent;
    cache.store("step-1", firstAgent, { current: null }, Promise.resolve());
    cache.store("step-2", secondAgent, { current: null }, Promise.resolve());

    let thrown: unknown;
    try {
      cache.applySources([makeSource("primary")], "primary");
    } catch (cause) {
      thrown = cause;
    }
    // The first agent throwing did not skip the second.
    expect(secondApplied).toBe(true);
    expect(thrown).toBeInstanceOf(AggregateError);
    if (!(thrown instanceof AggregateError)) {
      throw new Error("expected an AggregateError from a failing rotation");
    }
    expect(thrown.errors).toContain(rotateError);
  });
});

function closingAgent(onClose: () => Promise<void>): Agent {
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test stub: evictAll only calls close on the entry's agent
  return {
    close: onClose,
  } as unknown as Agent;
}

describe("warm-agent cache eviction", () => {
  test("surfaces a failing wrapped agent close as an AggregateError", async () => {
    // The wrapped step-agent close rejects (itself an AggregateError) when
    // a disposer -- e.g. the LSP subprocess kill -- fails. evictAll must
    // propagate that so a leaked LSP subprocess is visible to the run-loop
    // rather than swallowed at the cache boundary.
    const cache = createWarmAgentCache();
    const closeError = new Error("lsp dispose boom");
    const sinkRef: WarmEventSinkRef = { current: null };
    cache.store(
      "step-1",
      closingAgent(() => Promise.reject(closeError)),
      sinkRef,
      Promise.resolve(),
    );

    let thrown: unknown;
    try {
      await cache.evictAll("test eviction");
    } catch (cause) {
      thrown = cause;
    }
    expect(thrown).toBeInstanceOf(AggregateError);
    if (!(thrown instanceof AggregateError)) {
      throw new Error("expected an AggregateError from a failing eviction");
    }
    expect(thrown.errors).toContain(closeError);
  });

  test("closes every warm agent even when an earlier close rejects", async () => {
    // A first agent's close rejecting must not strand the remaining
    // agents' teardown -- otherwise their LSP subprocesses leak. Every
    // agent is closed and the failures surface together.
    const cache = createWarmAgentCache();
    const firstError = new Error("first close boom");
    let secondClosed = false;
    cache.store(
      "step-1",
      closingAgent(() => Promise.reject(firstError)),
      { current: null },
      Promise.resolve(),
    );
    cache.store(
      "step-2",
      closingAgent(() => {
        secondClosed = true;
        return Promise.resolve();
      }),
      { current: null },
      Promise.resolve(),
    );

    let thrown: unknown;
    try {
      await cache.evictAll("test eviction");
    } catch (cause) {
      thrown = cause;
    }
    // The first failure did not strand the second: it was still closed.
    expect(secondClosed).toBe(true);
    expect(thrown).toBeInstanceOf(AggregateError);
    if (!(thrown instanceof AggregateError)) {
      throw new Error("expected an AggregateError from a failing eviction");
    }
    expect(thrown.errors).toContain(firstError);
  });
});
