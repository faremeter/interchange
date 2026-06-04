// Behavioural tests for the default director's `decide()` function.
//
// These pin the per-event-shape decisions the built-in director makes
// when fed reactor events: `message.received` -> infer, tool-call
// turns -> checkpoint + execute_tools, text-only turns -> checkpoint
// + reply, refusal-only turns -> reply with the refusal reason,
// empty/whitespace turns -> checkpoint + wait, reactive-mode
// inference.done / tool.done -> checkpoint + wait, tool.done
// batching across multiple parallel calls, abort -> done, and
// inference.error -> checkpoint + reply-with-error-message.
//
// The director is exercised directly through the factory rather than
// through `createAgent`; the factory's `(config, env, agent)` shape
// constructs a `ReactorDirector` whose `decide` is a pure-ish
// function once instantiated, so the tests do not need a running
// reactor.
//
// Scope note. The decision logic itself is implemented by
// `createDefaultDirector` in `@intx/inference`. These tests pin the
// event-shape contract from the agent's vantage point: `@intx/agent`
// ships `defaultDirectorFactory` as a public surface, and what each
// reactor event resolves to (infer / checkpoint+execute_tools /
// reply / wait / done) is what consumers of `createAgent` see at the
// boundary. `@intx/inference`'s own `default-director.test.ts`
// covers a different slice -- policy hooks and the firing boundary
// for `afterInferenceDone` -- and the two suites are complementary,
// not duplicative. A refactor of the inference-side implementation
// that broke the agent-facing event shape is caught here; an
// inference-side hook-policy regression is caught there.

import { describe, test, expect } from "bun:test";

import { createInboundMessage } from "@intx/mime";
import type {
  ForkMode,
  GateType,
  LastCycleSource,
  ReactorCapabilities,
  ReactorInboundEvent,
  ReactorState,
  ToolCall,
  ToolDefinition,
  TokenUsage,
} from "@intx/types/runtime";

import { defaultDirectorFactory } from "./default-director";
import type { BaseEnv } from "./env";

const NO_TOOLS: readonly ToolDefinition[] = Object.freeze([]);

const TEST_SOURCE: LastCycleSource = {
  sourceId: "anthropic:claude-test",
  provider: "anthropic",
  model: "claude-test",
};

function emptyUsage(): TokenUsage {
  return { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, thinking: 0 };
}

function makeState(): ReactorState {
  return {
    turns: [],
    activeForks: [],
    pendingOperations: [],
    activeGates: [],
    tokenUsage: emptyUsage(),
    lastCycleUsage: null,
    lastCycleSource: null,
    sessionId: "test-session",
  };
}

// Capabilities mock that records every call and returns the same
// reactor-action shape the real capabilities object would.
function makeCapabilities(): ReactorCapabilities & {
  calls: { type: string; args: unknown[] }[];
} {
  const calls: { type: string; args: unknown[] }[] = [];
  return {
    calls,
    infer(options?: Parameters<ReactorCapabilities["infer"]>[0]) {
      calls.push({ type: "infer", args: [options] });
      return options === undefined
        ? { type: "infer" as const }
        : { type: "infer" as const, options };
    },
    executeTools(toolCalls: ToolCall[], parallel?: boolean) {
      calls.push({ type: "execute_tools", args: [toolCalls, parallel] });
      return {
        type: "execute_tools" as const,
        calls: toolCalls,
        parallel: parallel ?? true,
      };
    },
    suspend(gate: {
      type: GateType;
      gateId: string;
      timeoutMs: number;
      correlationId?: string;
    }) {
      calls.push({ type: "suspend", args: [gate] });
      return { type: "suspend" as const, gate };
    },
    fork(mode: ForkMode, forkId: string) {
      calls.push({ type: "fork", args: [mode, forkId] });
      return { type: "fork" as const, mode, forkId };
    },
    reply(content: string) {
      calls.push({ type: "reply", args: [content] });
      return { type: "reply" as const, content };
    },
    emit(eventType: `custom.${string}`, data: Record<string, unknown>) {
      calls.push({ type: "emit", args: [eventType, data] });
      return { type: "emit" as const, eventType, data };
    },
    checkpoint(message?: string) {
      calls.push({ type: "checkpoint", args: [message] });
      return {
        type: "checkpoint" as const,
        message: message ?? "checkpoint",
      };
    },
    compact(compactor: string, reason: string) {
      calls.push({ type: "compact", args: [compactor, reason] });
      return { type: "compact" as const, compactor, reason };
    },
    wait() {
      calls.push({ type: "wait", args: [] });
      return { type: "wait" as const };
    },
    done() {
      calls.push({ type: "done", args: [] });
      return { type: "done" as const };
    },
  };
}

function buildDirector(opts?: { mode?: "conversational" | "reactive" }) {
  // The factory ignores env in its constructor; only `agent.systemPrompt`
  // and `agent.toolDefinitions` are read. A bare object cast is the
  // narrowest way to satisfy the BaseEnv contract for these tests.
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test stub, the default director ignores env in its constructor
  const env = {} as BaseEnv;
  return defaultDirectorFactory(opts ?? {}, env, {
    systemPrompt: "You are helpful.",
    toolDefinitions: NO_TOOLS,
  });
}

function makeInboundMessageEvent(): ReactorInboundEvent {
  return {
    type: "message.received",
    message: createInboundMessage({
      from: "user@test",
      to: "agent@test",
      content: "hello",
      interchangeType: "conversation.message",
    }),
  };
}

describe("defaultDirector decide()", () => {
  test("message.received triggers infer action", async () => {
    const director = buildDirector();
    const caps = makeCapabilities();
    const state = makeState();

    const actions = await director.decide(
      makeInboundMessageEvent(),
      state,
      caps,
    );
    const normalized = Array.isArray(actions) ? actions : [actions];
    expect(normalized.some((a) => a.type === "infer")).toBe(true);
  });

  test("inference.done with tool calls triggers checkpoint and execute_tools", async () => {
    const director = buildDirector();
    const caps = makeCapabilities();
    const state = makeState();

    const event: ReactorInboundEvent = {
      type: "inference.done",
      turn: {
        role: "assistant",
        model: "claude-test",
        content: [
          {
            type: "tool_call",
            id: "tc1",
            name: "read_file",
            arguments: { path: "/test" },
          },
        ],
        timestamp: 1000,
      },
      usage: emptyUsage(),
      source: TEST_SOURCE,
    };

    const actions = await director.decide(event, state, caps);
    const normalized = Array.isArray(actions) ? actions : [actions];
    expect(normalized.some((a) => a.type === "checkpoint")).toBe(true);
    expect(normalized.some((a) => a.type === "execute_tools")).toBe(true);
  });

  test("inference.done without tool calls returns checkpoint and reply", async () => {
    const director = buildDirector();
    const caps = makeCapabilities();
    const state = makeState();

    const event: ReactorInboundEvent = {
      type: "inference.done",
      turn: {
        role: "assistant",
        model: "claude-test",
        content: [{ type: "text", text: "Here is my response." }],
        timestamp: 1000,
      },
      usage: emptyUsage(),
      source: TEST_SOURCE,
    };

    const actions = await director.decide(event, state, caps);
    const normalized = Array.isArray(actions) ? actions : [actions];

    expect(normalized.some((a) => a.type === "checkpoint")).toBe(true);
    expect(normalized.some((a) => a.type === "reply")).toBe(true);
    const replyAction = normalized.find((a) => a.type === "reply");
    if (replyAction === undefined || replyAction.type !== "reply") {
      throw new Error("unreachable");
    }
    expect(replyAction.content).toBe("Here is my response.");
  });

  test("tool.done triggers checkpoint and re-infer", async () => {
    const director = buildDirector();
    const caps = makeCapabilities();
    const state = makeState();

    const event: ReactorInboundEvent = {
      type: "tool.done",
      result: { callId: "tc1", content: "file contents" },
    };

    const actions = await director.decide(event, state, caps);
    const normalized = Array.isArray(actions) ? actions : [actions];
    expect(normalized.some((a) => a.type === "checkpoint")).toBe(true);
    expect(normalized.some((a) => a.type === "infer")).toBe(true);
  });

  test("inference.error returns checkpoint and reply with error message", async () => {
    const director = buildDirector();
    const caps = makeCapabilities();
    const state = makeState();

    const event: ReactorInboundEvent = {
      type: "inference.error",
      error: {
        category: "credential_failure",
        message: "invalid API key",
        statusCode: 401,
      },
      partial: { text: "" },
    };

    const actions = await director.decide(event, state, caps);
    const normalized = Array.isArray(actions) ? actions : [actions];
    expect(normalized.some((a) => a.type === "checkpoint")).toBe(true);

    const replyAction = normalized.find((a) => a.type === "reply");
    if (replyAction === undefined || replyAction.type !== "reply") {
      throw new Error("expected reply action");
    }
    expect(replyAction.content).toContain("credential error");
    expect(replyAction.content).toContain("invalid API key");
  });

  test("abort returns done", async () => {
    const director = buildDirector();
    const caps = makeCapabilities();
    const state = makeState();

    const event: ReactorInboundEvent = {
      type: "abort",
      reason: "user_disconnect",
    };

    const actions = await director.decide(event, state, caps);
    const normalized = Array.isArray(actions) ? actions : [actions];
    expect(normalized.some((a) => a.type === "done")).toBe(true);
  });

  test("inference.done with empty content returns checkpoint and wait", async () => {
    const director = buildDirector();
    const caps = makeCapabilities();
    const state = makeState();

    const event: ReactorInboundEvent = {
      type: "inference.done",
      turn: {
        role: "assistant",
        model: "claude-test",
        content: [],
        timestamp: 1000,
      },
      usage: emptyUsage(),
      source: TEST_SOURCE,
    };

    const actions = await director.decide(event, state, caps);
    const normalized = Array.isArray(actions) ? actions : [actions];
    expect(normalized.some((a) => a.type === "checkpoint")).toBe(true);
    expect(normalized.some((a) => a.type === "wait")).toBe(true);
    expect(normalized.some((a) => a.type === "done")).toBe(false);
    expect(normalized.some((a) => a.type === "reply")).toBe(false);
  });

  test("inference.done with a refusal-only turn replies with the refusal reason", async () => {
    // RefusalBlock is the OpenAI strict-mode policy-decline shape:
    // the model produced coherent output ("I cannot help with that")
    // in the dedicated refusal field instead of content. The
    // director's reply path must surface the refusal text to the
    // caller, not route the turn through the empty-response branch --
    // otherwise the human waits indefinitely for an answer the model
    // already declined to give.
    const director = buildDirector();
    const caps = makeCapabilities();
    const state = makeState();

    const event: ReactorInboundEvent = {
      type: "inference.done",
      turn: {
        role: "assistant",
        model: "gpt-test",
        content: [{ type: "refusal", reason: "I cannot help with that." }],
        timestamp: 1000,
      },
      usage: emptyUsage(),
      source: TEST_SOURCE,
    };

    const actions = await director.decide(event, state, caps);
    const normalized = Array.isArray(actions) ? actions : [actions];
    expect(normalized.some((a) => a.type === "checkpoint")).toBe(true);
    const replyAction = normalized.find((a) => a.type === "reply");
    if (replyAction === undefined || replyAction.type !== "reply") {
      throw new Error("expected reply action");
    }
    expect(replyAction.content).toBe("I cannot help with that.");
  });

  test("inference.done with whitespace-only text returns checkpoint and wait", async () => {
    const director = buildDirector();
    const caps = makeCapabilities();
    const state = makeState();

    const event: ReactorInboundEvent = {
      type: "inference.done",
      turn: {
        role: "assistant",
        model: "claude-test",
        content: [{ type: "text", text: "   \n\t  " }],
        timestamp: 1000,
      },
      usage: emptyUsage(),
      source: TEST_SOURCE,
    };

    const actions = await director.decide(event, state, caps);
    const normalized = Array.isArray(actions) ? actions : [actions];
    expect(normalized.some((a) => a.type === "checkpoint")).toBe(true);
    expect(normalized.some((a) => a.type === "wait")).toBe(true);
    expect(normalized.some((a) => a.type === "done")).toBe(false);
    expect(normalized.some((a) => a.type === "reply")).toBe(false);
  });

  test("reactive mode inference.done returns checkpoint and wait", async () => {
    const director = buildDirector({ mode: "reactive" });
    const caps = makeCapabilities();
    const state = makeState();

    const event: ReactorInboundEvent = {
      type: "inference.done",
      turn: {
        role: "assistant",
        model: "claude-test",
        content: [{ type: "text", text: "done processing" }],
        timestamp: 1000,
      },
      usage: emptyUsage(),
      source: TEST_SOURCE,
    };

    const actions = await director.decide(event, state, caps);
    const normalized = Array.isArray(actions) ? actions : [actions];
    expect(normalized.some((a) => a.type === "checkpoint")).toBe(true);
    expect(normalized.some((a) => a.type === "wait")).toBe(true);
  });

  test("reactive mode tool.done returns checkpoint and wait", async () => {
    const director = buildDirector({ mode: "reactive" });
    const caps = makeCapabilities();
    const state = makeState();

    const event: ReactorInboundEvent = {
      type: "tool.done",
      result: { callId: "tc1", content: "result" },
    };

    const actions = await director.decide(event, state, caps);
    const normalized = Array.isArray(actions) ? actions : [actions];
    expect(normalized.some((a) => a.type === "checkpoint")).toBe(true);
    expect(normalized.some((a) => a.type === "wait")).toBe(true);
    expect(normalized.some((a) => a.type === "infer")).toBe(false);
  });

  test("tool.done batching waits for all results before checkpoint", async () => {
    const director = buildDirector();
    const caps = makeCapabilities();
    const state = makeState();

    // First, drive inference.done with two parallel tool calls; the
    // director records the pending set internally.
    const inferDone: ReactorInboundEvent = {
      type: "inference.done",
      turn: {
        role: "assistant",
        model: "claude-test",
        content: [
          {
            type: "tool_call",
            id: "tc1",
            name: "read_file",
            arguments: { path: "/a" },
          },
          {
            type: "tool_call",
            id: "tc2",
            name: "read_file",
            arguments: { path: "/b" },
          },
        ],
        timestamp: 1000,
      },
      usage: emptyUsage(),
      source: TEST_SOURCE,
    };
    await director.decide(inferDone, state, caps);

    // First tool.done -- still one result outstanding, so no actions.
    const toolDone1: ReactorInboundEvent = {
      type: "tool.done",
      result: { callId: "tc1", content: "result1" },
    };
    const actions1 = await director.decide(toolDone1, state, caps);
    const normalized1 = Array.isArray(actions1) ? actions1 : [actions1];
    expect(normalized1).toEqual([]);

    // Second tool.done -- batch is complete; expect checkpoint + infer.
    const toolDone2: ReactorInboundEvent = {
      type: "tool.done",
      result: { callId: "tc2", content: "result2" },
    };
    const actions2 = await director.decide(toolDone2, state, caps);
    const normalized2 = Array.isArray(actions2) ? actions2 : [actions2];
    expect(normalized2.some((a) => a.type === "checkpoint")).toBe(true);
    expect(normalized2.some((a) => a.type === "infer")).toBe(true);
  });
});
