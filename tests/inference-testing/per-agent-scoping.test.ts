// Pins the per-agent assertion-scoping pattern documented in the
// @intx/inference-testing README.
//
// Multi-agent dispatch tests routinely want to assert "agent A made
// tool call X" independently of "agent B made tool call Y". The
// harness already supports this without any explicit per-agent
// machinery: each call to `harness.runInference(...)` returns its own
// `AsyncIterable<InferenceEvent>`. If the test collects events per
// call into separate arrays, every matcher in `expectToolCalls` /
// `expectToolCall(name).from(events)` is automatically scoped to the
// agent whose events array it sees.
//
// This test pins that contract: two concurrent `harness.runInference`
// calls collect into two arrays; each `expectToolCalls(...)` invocation
// only sees the tool calls from its own agent.
//
// If a future harness change ever mixes events from multiple
// `runInference` calls into a shared stream, this test breaks and
// surfaces the regression at the source.

import { afterEach, describe, expect, test } from "bun:test";

import { setupHarness, wire } from "@intx/inference-testing";
import type { Harness } from "@intx/inference-testing";
import { expectToolCall, expectToolCalls } from "@intx/inference-testing";
import type {
  ConversationTurn,
  InferenceEvent,
  InferenceSource,
} from "@intx/types/runtime";

const SOURCE: InferenceSource = {
  id: "openai:test",
  provider: "openai",
  baseURL: "https://example/v1",
  apiKey: "test",
  model: "test",
};

const USAGE_HEAD = {
  input: 5,
  output: 0,
  cacheRead: 0,
  cacheWrite: 0,
  thinking: 0,
};
const USAGE_TAIL = {
  input: 0,
  output: 3,
  cacheRead: 0,
  cacheWrite: 0,
  thinking: 0,
};

function userTurn(text: string): ConversationTurn {
  return {
    role: "user",
    content: [{ type: "text", text }],
    timestamp: 0,
  };
}

let activeHarness: Harness | null = null;

afterEach(() => {
  if (activeHarness !== null) {
    activeHarness.dispose();
    activeHarness = null;
  }
});

describe("per-agent assertion scoping", () => {
  test("two concurrent runInference calls produce independently-assertable event arrays", async () => {
    const harness = setupHarness();
    activeHarness = harness;

    // Two agents fire inference calls in parallel. The harness routes
    // each to its own response stream (body-content routing — Phase 1
    // of INTR-83 — would also work here, but for simplicity we use
    // distinct URLs).
    harness.scenario.replyOnce("openai", {
      toolCalls: [{ name: "agentATool", args: { value: "from-A" } }],
      headUsage: USAGE_HEAD,
      tailUsage: USAGE_TAIL,
      predicate: (req) => req.url.includes("/agent-a/"),
    });
    harness.scenario.replyOnce("openai", {
      toolCalls: [{ name: "agentBTool", args: { value: "from-B" } }],
      headUsage: USAGE_HEAD,
      tailUsage: USAGE_TAIL,
      predicate: (req) => req.url.includes("/agent-b/"),
    });

    // Each agent's tool handler runs locally; we don't care about the
    // dispatched value, only about the events the harness emits.
    harness.scenario.onTool("agentATool", () => ({ ok: true }));
    harness.scenario.onTool("agentBTool", () => ({ ok: true }));

    let seqA = 0;
    let seqB = 0;
    const eventsA: InferenceEvent[] = [];
    const eventsB: InferenceEvent[] = [];

    const collectA = (async () => {
      for await (const ev of harness.runInference({
        turns: [userTurn("hello A")],
        source: {
          ...SOURCE,
          baseURL: "https://example/v1/agent-a",
        },
        nextSeq: () => ++seqA,
      })) {
        eventsA.push(ev);
      }
    })();
    const collectB = (async () => {
      for await (const ev of harness.runInference({
        turns: [userTurn("hello B")],
        source: {
          ...SOURCE,
          baseURL: "https://example/v1/agent-b",
        },
        nextSeq: () => ++seqB,
      })) {
        eventsB.push(ev);
      }
    })();

    await harness.run();
    await Promise.all([collectA, collectB]);

    // Agent A's events array contains agentATool and NOT agentBTool.
    expectToolCalls(eventsA).toInclude({
      name: "agentATool",
      arguments: { value: "from-A" },
    });
    expectToolCall("agentATool").from(eventsA).toHaveBeenCalledTimes(1);
    expectToolCall("agentBTool").from(eventsA).toHaveBeenCalledTimes(0);

    // Agent B's events array contains agentBTool and NOT agentATool.
    expectToolCalls(eventsB).toInclude({
      name: "agentBTool",
      arguments: { value: "from-B" },
    });
    expectToolCall("agentBTool").from(eventsB).toHaveBeenCalledTimes(1);
    expectToolCall("agentATool").from(eventsB).toHaveBeenCalledTimes(0);

    // Neither array bleeds events from the other agent: a sanity check
    // that the iteration didn't mix the streams.
    const aToolEnds = eventsA.filter(
      (e) => e.type === "inference.tool_call.end",
    );
    const bToolEnds = eventsB.filter(
      (e) => e.type === "inference.tool_call.end",
    );
    expect(aToolEnds).toHaveLength(1);
    expect(bToolEnds).toHaveLength(1);
    // Type guards so the property accesses below typecheck. The guards
    // also subsume the per-array length and type asserts above for a
    // reader skimming the test.
    const aEnd = aToolEnds[0];
    const bEnd = bToolEnds[0];
    if (aEnd?.type !== "inference.tool_call.end") {
      throw new Error("unreachable");
    }
    if (bEnd?.type !== "inference.tool_call.end") {
      throw new Error("unreachable");
    }
    expect(aEnd.data.name).toBe("agentATool");
    expect(bEnd.data.name).toBe("agentBTool");
  });

  test("body-content routing scopes per-agent assertions when URLs are identical", async () => {
    // The Phase 1 use case: N agents POST to the same URL, distinguished
    // only by body content. The events arrays remain independent because
    // they come from separate `runInference` iterators — body routing
    // only determines which response stream each agent's fetch receives.
    const harness = setupHarness();
    activeHarness = harness;

    const streamA = harness.scenario.createStream();
    streamA.enqueueAll(
      wire.completeResponse("openai", {
        toolCalls: [
          {
            callId: "call_a",
            name: "toolA",
            argsJSON: JSON.stringify({ tag: "A" }),
          },
        ],
      }),
      { startAt: harness.clock.now() + 1 },
    );
    harness.scenario.whenRequestBodyMatches(
      (body) => body.includes("agent-A-marker"),
      streamA,
    );

    const streamB = harness.scenario.createStream();
    streamB.enqueueAll(
      wire.completeResponse("openai", {
        toolCalls: [
          {
            callId: "call_b",
            name: "toolB",
            argsJSON: JSON.stringify({ tag: "B" }),
          },
        ],
      }),
      { startAt: harness.clock.now() + 1 },
    );
    harness.scenario.whenRequestBodyMatches(
      (body) => body.includes("agent-B-marker"),
      streamB,
    );

    harness.scenario.onTool("toolA", () => ({ ok: true }));
    harness.scenario.onTool("toolB", () => ({ ok: true }));

    let seqA = 0;
    let seqB = 0;
    const eventsA: InferenceEvent[] = [];
    const eventsB: InferenceEvent[] = [];

    // Both agents POST to the same /chat/completions endpoint. The
    // body carries the agent-X-marker that distinguishes them.
    const collectA = (async () => {
      for await (const ev of harness.runInference({
        turns: [userTurn("agent-A-marker")],
        source: SOURCE,
        nextSeq: () => ++seqA,
      })) {
        eventsA.push(ev);
      }
    })();
    const collectB = (async () => {
      for await (const ev of harness.runInference({
        turns: [userTurn("agent-B-marker")],
        source: SOURCE,
        nextSeq: () => ++seqB,
      })) {
        eventsB.push(ev);
      }
    })();

    await harness.run();
    await Promise.all([collectA, collectB]);

    expectToolCall("toolA").from(eventsA).toHaveBeenCalledTimes(1);
    expectToolCall("toolB").from(eventsA).toHaveBeenCalledTimes(0);
    expectToolCall("toolB").from(eventsB).toHaveBeenCalledTimes(1);
    expectToolCall("toolA").from(eventsB).toHaveBeenCalledTimes(0);
  });
});
