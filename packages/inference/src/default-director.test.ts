import { describe, test, expect } from "bun:test";

import { createInboundMessage } from "@intx/mime";
import type {
  AssistantTurn,
  InferenceError,
  LastCycleSource,
  PartialMessage,
  ReactorAction,
  ReactorInboundEvent,
  ReactorState,
  ToolCall,
  ToolResult,
  TokenUsage,
} from "@intx/types/runtime";

import { createDefaultDirector } from "./default-director";
import type {
  AfterInferenceDecision,
  AfterInferenceHook,
  DefaultDirectorPolicy,
} from "./default-director";
import { createCapabilities } from "./director";
import { validateActions } from "./actions";

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

const TEST_SOURCE: LastCycleSource = {
  sourceId: "anthropic:claude-test",
  provider: "anthropic",
  model: "claude-test",
};

const TEST_USAGE: TokenUsage = {
  input: 100,
  output: 50,
  cacheRead: 0,
  cacheWrite: 0,
  thinking: 0,
};

function makeState(overrides: Partial<ReactorState> = {}): ReactorState {
  return {
    sessionId: "test-session",
    turns: [],
    activeForks: [],
    pendingOperations: [],
    activeGates: [],
    tokenUsage: { ...TEST_USAGE },
    lastCycleUsage: { ...TEST_USAGE },
    lastCycleSource: { ...TEST_SOURCE },
    ...overrides,
  };
}

function makeAssistantTurn(text: string): AssistantTurn {
  return {
    role: "assistant",
    content: [{ type: "text", text }],
    model: "claude-test",
    timestamp: 1000,
  };
}

function makeAssistantTurnWithToolCall(
  callId: string,
  name: string,
): AssistantTurn {
  return {
    role: "assistant",
    content: [
      { type: "tool_call", id: callId, name, arguments: { q: "test" } },
    ],
    model: "claude-test",
    timestamp: 1000,
  };
}

function makeInferenceDoneEvent(turn: AssistantTurn): ReactorInboundEvent {
  return {
    type: "inference.done",
    turn,
    usage: TEST_USAGE,
    source: TEST_SOURCE,
  };
}

function makeInferenceErrorEvent(): ReactorInboundEvent {
  const error: InferenceError = {
    category: "fatal",
    message: "test error",
  };
  const partial: PartialMessage = { text: "" };
  return { type: "inference.error", error, partial };
}

async function decide(
  policy: DefaultDirectorPolicy,
  event: ReactorInboundEvent,
  state: ReactorState = makeState(),
): Promise<ReactorAction[]> {
  const director = createDefaultDirector("You are a test agent.", [], policy);
  const result = await director.decide(event, state, createCapabilities());
  return Array.isArray(result) ? result : [result];
}

// ---------------------------------------------------------------------------
// Hook decisions
// ---------------------------------------------------------------------------

describe("DefaultDirector — afterInferenceDone hook", () => {
  test("continue: existing post-inference logic runs unchanged", async () => {
    let receivedState: ReactorState | undefined;
    let receivedTurn: AssistantTurn | undefined;
    const hook: AfterInferenceHook = (state, turn) => {
      receivedState = state;
      receivedTurn = turn;
      return { type: "continue" };
    };
    const turn = makeAssistantTurn("Hello from the model");
    const actions = await decide(
      { afterInferenceDone: hook },
      makeInferenceDoneEvent(turn),
    );

    // The hook saw the post-cycle state and the turn — verifies the
    // director plumbed both arguments through, not just called the hook.
    expect(receivedTurn).toEqual(turn);
    expect(receivedState?.lastCycleSource).toEqual(TEST_SOURCE);
    expect(receivedState?.lastCycleUsage).toEqual(TEST_USAGE);

    // Continue falls through to the existing reply path.
    expect(actions).toEqual([
      { type: "checkpoint", message: "checkpoint: inference-done" },
      { type: "reply", content: "Hello from the model" },
    ]);
  });

  test("abort: returns [checkpoint, done], reason not surfaced", async () => {
    const hook: AfterInferenceHook = () => ({
      type: "abort",
      reason: "budget exhausted",
    });
    const actions = await decide(
      { afterInferenceDone: hook },
      makeInferenceDoneEvent(makeAssistantTurn("ignored")),
    );
    expect(actions).toEqual([
      { type: "checkpoint", message: "checkpoint: after-inference-abort" },
      { type: "done" },
    ]);
    expect(validateActions(actions).ok).toBe(true);
  });

  test("halt: returns [checkpoint, reply] with the policy reason", async () => {
    const hook: AfterInferenceHook = () => ({
      type: "halt",
      reason: "paused for top-up",
    });
    const actions = await decide(
      { afterInferenceDone: hook },
      makeInferenceDoneEvent(makeAssistantTurn("ignored")),
    );
    expect(actions).toEqual([
      { type: "checkpoint", message: "checkpoint: after-inference-halt" },
      { type: "reply", content: "paused for top-up" },
    ]);
    expect(validateActions(actions).ok).toBe(true);
  });

  test("hook not set: existing behavior preserved", async () => {
    const actions = await decide(
      {},
      makeInferenceDoneEvent(makeAssistantTurn("Hello")),
    );
    expect(actions).toEqual([
      { type: "checkpoint", message: "checkpoint: inference-done" },
      { type: "reply", content: "Hello" },
    ]);
  });

  test("hook throws: caught, routed to abort (terminates)", async () => {
    const hook: AfterInferenceHook = () => {
      throw new Error("policy died");
    };
    const actions = await decide(
      { afterInferenceDone: hook },
      makeInferenceDoneEvent(makeAssistantTurn("ignored")),
    );
    expect(actions).toEqual([
      { type: "checkpoint", message: "checkpoint: after-inference-abort" },
      { type: "done" },
    ]);
  });

  test("hook returning a Promise is awaited", async () => {
    const hook: AfterInferenceHook = async () => {
      await new Promise((resolve) => setTimeout(resolve, 5));
      const decision: AfterInferenceDecision = {
        type: "abort",
        reason: "async abort",
      };
      return decision;
    };
    const actions = await decide(
      { afterInferenceDone: hook },
      makeInferenceDoneEvent(makeAssistantTurn("ignored")),
    );
    expect(actions).toEqual([
      { type: "checkpoint", message: "checkpoint: after-inference-abort" },
      { type: "done" },
    ]);
  });

  test("hook does NOT fire on inference.error", async () => {
    let hookFired = false;
    const hook: AfterInferenceHook = () => {
      hookFired = true;
      return { type: "abort", reason: "should not run" };
    };
    const actions = await decide(
      { afterInferenceDone: hook },
      makeInferenceErrorEvent(),
    );
    expect(hookFired).toBe(false);
    // The inference.error branch produces its own checkpoint + reply
    // shape; the hook is not in that path.
    expect(actions[0]).toEqual({
      type: "checkpoint",
      message: "checkpoint: inference-error",
    });
  });

  test("abort fires before tool calls execute (tool calls dropped)", async () => {
    const hook: AfterInferenceHook = () => ({
      type: "abort",
      reason: "stop now",
    });
    const turn = makeAssistantTurnWithToolCall("call_1", "search");
    const actions = await decide(
      { afterInferenceDone: hook },
      makeInferenceDoneEvent(turn),
    );
    // The model's tool call is on the turn, but the hook's abort
    // routes to done before execute_tools is reached. The TSDoc warns
    // policy authors about this; the test pins the behavior.
    expect(actions).toEqual([
      { type: "checkpoint", message: "checkpoint: after-inference-abort" },
      { type: "done" },
    ]);
  });
});

// ---------------------------------------------------------------------------
// Firing boundary: hook fires only on inference.done
//
// The "does NOT fire on inference.error" test above pins one negative
// case; this block exhausts the rest of the ReactorInboundEvent union
// so a future switch refactor (e.g. extracting a shared post-event
// helper) can't quietly start invoking the hook on the wrong branch.
// ---------------------------------------------------------------------------

async function fireHook(event: ReactorInboundEvent): Promise<boolean> {
  let fired = false;
  const hook: AfterInferenceHook = () => {
    fired = true;
    return { type: "continue" };
  };
  await decide({ afterInferenceDone: hook }, event);
  return fired;
}

describe("DefaultDirector — afterInferenceDone firing boundary", () => {
  test("not fired on message.received", async () => {
    const event: ReactorInboundEvent = {
      type: "message.received",
      message: createInboundMessage({
        from: "test@example.com",
        to: "agent@example.com",
        content: "hi",
      }),
    };
    expect(await fireHook(event)).toBe(false);
  });

  test("not fired on tool.done", async () => {
    const result: ToolResult = { callId: "c1", content: "ok" };
    const event: ReactorInboundEvent = { type: "tool.done", result };
    expect(await fireHook(event)).toBe(false);
  });

  test("not fired on reactor.gate.cleared", async () => {
    const event: ReactorInboundEvent = {
      type: "reactor.gate.cleared",
      gateId: "g1",
      reason: "resolved",
    };
    expect(await fireHook(event)).toBe(false);
  });

  test("not fired on abort", async () => {
    const event: ReactorInboundEvent = {
      type: "abort",
      reason: "user_disconnect",
    };
    expect(await fireHook(event)).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// resume.execute_tools seeds the outstanding-result counter
//
// The re-dispatch path never passes through inference.done, which is the only
// place the tool batch's count is seeded. Without a seed off resume.execute_
// tools the count stays zero and the re-dispatched call's tool.done decrements
// to -1 and re-infers off a negative count by accident. These tests drive one
// director instance across the event sequence so the seed-then-decrement math
// is exercised against real state, not asserted per fresh instance.
// ---------------------------------------------------------------------------

describe("DefaultDirector — resume.execute_tools counter seeding", () => {
  function makeToolCall(id: string): ToolCall {
    return { id, name: "charge_card", arguments: {} };
  }

  async function decideOn(
    director: ReturnType<typeof createDefaultDirector>,
    event: ReactorInboundEvent,
  ): Promise<ReactorAction[]> {
    const result = await director.decide(
      event,
      makeState(),
      createCapabilities(),
    );
    return Array.isArray(result) ? result : [result];
  }

  test("seeds the count to the number of re-dispatched calls and re-infers only at zero", async () => {
    const director = createDefaultDirector("test agent", []);

    // Two calls are about to run. The director must return the execute_tools
    // action and seed its outstanding count to two.
    const dispatch = await decideOn(director, {
      type: "resume.execute_tools",
      calls: [makeToolCall("a"), makeToolCall("b")],
    });
    expect(dispatch).toEqual([
      {
        type: "execute_tools",
        calls: [makeToolCall("a"), makeToolCall("b")],
        parallel: false,
        addToHistory: true,
      },
    ]);

    // First result: count 2 -> 1, no re-inference yet. An unseeded count would
    // have gone 0 -> -1 and re-inferred here.
    const afterFirst = await decideOn(director, {
      type: "tool.done",
      result: { callId: "a", content: "ok" },
    });
    expect(afterFirst).toEqual([]);

    // Second result: count 1 -> 0, re-infer exactly once.
    const afterSecond = await decideOn(director, {
      type: "tool.done",
      result: { callId: "b", content: "ok" },
    });
    expect(afterSecond.map((action) => action.type)).toEqual([
      "checkpoint",
      "infer",
    ]);
  });

  test("a single re-dispatched call re-infers exactly once", async () => {
    const director = createDefaultDirector("test agent", []);

    await decideOn(director, {
      type: "resume.execute_tools",
      calls: [makeToolCall("a")],
    });

    const afterResult = await decideOn(director, {
      type: "tool.done",
      result: { callId: "a", content: "ok" },
    });
    expect(afterResult.map((action) => action.type)).toEqual([
      "checkpoint",
      "infer",
    ]);
  });
});
