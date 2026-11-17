import { describe, test, expect } from "bun:test";

import { createInboundMessage } from "@intx/mime";
import type {
  AssistantTurn,
  ConversationTurn,
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
      // Yield a full event-loop turn so the hook's promise cannot be settled
      // when decide() receives it. The director awaits the hook, so the
      // assertions below do not depend on how long this takes.
      await new Promise((resolve) => setTimeout(resolve, 0));
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
// Outstanding tool calls are tracked per batch
//
// A batch is the set of tool calls one inference.done emitted. The director
// re-infers once per batch, only when every call of the batch is answered: by
// its tool.done, or by the resume.tool_result that answers a parked call. One
// director instance drives each event sequence so the batch state is
// exercised across events, not asserted per fresh instance.
// ---------------------------------------------------------------------------

describe("DefaultDirector — outstanding tool calls per batch", () => {
  function makeToolCall(id: string): ToolCall {
    return { id, name: "charge_card", arguments: {} };
  }

  function makeBatchDone(...ids: string[]): ReactorInboundEvent {
    return makeInferenceDoneEvent({
      role: "assistant",
      content: ids.map((id) => ({
        type: "tool_call" as const,
        id,
        name: "charge_card",
        arguments: {},
      })),
      model: "claude-test",
      timestamp: 1000,
    });
  }

  function toolDone(callId: string): ReactorInboundEvent {
    return { type: "tool.done", result: { callId, content: "ok" } };
  }

  function rejected(callId: string): ReactorInboundEvent {
    return {
      type: "resume.tool_result",
      result: { callId, content: "rejected", isError: true },
    };
  }

  async function decideOn(
    director: ReturnType<typeof createDefaultDirector>,
    event: ReactorInboundEvent,
    state: ReactorState = makeState(),
  ): Promise<ReactorAction[]> {
    const result = await director.decide(event, state, createCapabilities());
    return Array.isArray(result) ? result : [result];
  }

  function types(actions: ReactorAction[]): string[] {
    return actions.map((action) => action.type);
  }

  // History as a respawned reactor loads it: one assistant turn holds the
  // batch, a tool_result turn answers each listed call, and a pending
  // operation parks each parked call.
  function historyState(
    batchIds: string[],
    answeredIds: string[] = [],
    parkedIds: string[] = [],
  ): ReactorState {
    const turns: ConversationTurn[] = [
      {
        role: "assistant",
        content: batchIds.map((id) => ({
          type: "tool_call" as const,
          id,
          name: "charge_card",
          arguments: {},
        })),
        model: "claude-test",
        timestamp: 1000,
      },
    ];
    for (const id of answeredIds) {
      turns.push({
        role: "user",
        content: [
          {
            type: "tool_result",
            callId: id,
            content: [{ type: "text", text: "rejected" }],
            isError: true,
          },
        ],
        timestamp: 1001,
      });
    }
    const pendingOperations = parkedIds.map((id) => ({
      correlationId: `corr-${id}`,
      kind: "approval" as const,
      registeredAt: 1000,
      gateId: `gate-${id}`,
      suspendedCall: makeToolCall(id),
    }));
    return makeState({ turns, pendingOperations });
  }

  test("an approved call does not re-infer while a sibling call is parked", async () => {
    const director = createDefaultDirector("test agent", []);
    await decideOn(director, makeBatchDone("a", "b"));

    const dispatch = await decideOn(director, {
      type: "resume.execute_tools",
      calls: [makeToolCall("a")],
    });
    expect(dispatch).toEqual([
      {
        type: "execute_tools",
        calls: [makeToolCall("a")],
        parallel: false,
        addToHistory: true,
      },
    ]);
    expect(await decideOn(director, toolDone("a"))).toEqual([]);

    await decideOn(director, {
      type: "resume.execute_tools",
      calls: [makeToolCall("b")],
    });
    expect(types(await decideOn(director, toolDone("b")))).toEqual([
      "checkpoint",
      "infer",
    ]);
  });

  test("a rejected call does not re-infer while a sibling call is parked", async () => {
    const director = createDefaultDirector("test agent", []);
    await decideOn(director, makeBatchDone("a", "b"));

    expect(await decideOn(director, rejected("a"))).toEqual([]);
    expect(types(await decideOn(director, rejected("b")))).toEqual([
      "checkpoint",
      "infer",
    ]);
  });

  test("a batch re-infers when it completes while an earlier batch's call stays parked", async () => {
    const director = createDefaultDirector("test agent", []);
    await decideOn(director, makeBatchDone("a"));

    await decideOn(director, makeBatchDone("x", "y"));
    expect(await decideOn(director, toolDone("x"))).toEqual([]);
    expect(types(await decideOn(director, toolDone("y")))).toEqual([
      "checkpoint",
      "infer",
    ]);

    expect(types(await decideOn(director, rejected("a")))).toEqual([
      "checkpoint",
      "infer",
    ]);
  });

  test("after a respawn, an approved call does not re-infer while a sibling call is parked", async () => {
    const director = createDefaultDirector("test agent", []);

    // The reactor appends each result to history before the director sees
    // its tool.done, so each step passes the history as it stands then.
    await decideOn(
      director,
      { type: "resume.execute_tools", calls: [makeToolCall("a")] },
      historyState(["a", "b"], [], ["b"]),
    );
    expect(
      await decideOn(
        director,
        toolDone("a"),
        historyState(["a", "b"], ["a"], ["b"]),
      ),
    ).toEqual([]);

    await decideOn(
      director,
      { type: "resume.execute_tools", calls: [makeToolCall("b")] },
      historyState(["a", "b"], ["a"]),
    );
    expect(
      types(
        await decideOn(
          director,
          toolDone("b"),
          historyState(["a", "b"], ["a", "b"]),
        ),
      ),
    ).toEqual(["checkpoint", "infer"]);
  });

  test("after a respawn, a rejected call does not re-infer while a sibling call is parked", async () => {
    const director = createDefaultDirector("test agent", []);

    expect(
      await decideOn(
        director,
        rejected("a"),
        historyState(["a", "b"], ["a"], ["b"]),
      ),
    ).toEqual([]);
    expect(
      types(
        await decideOn(
          director,
          rejected("b"),
          historyState(["a", "b"], ["a", "b"]),
        ),
      ),
    ).toEqual(["checkpoint", "infer"]);
  });

  // A sibling that was running at the crash has no result and no pending
  // operation. Nothing answers it, so the director does not wait on it.
  test("after a respawn, an approved call re-infers when its sibling was running at the crash", async () => {
    const director = createDefaultDirector("test agent", []);

    await decideOn(
      director,
      { type: "resume.execute_tools", calls: [makeToolCall("a")] },
      historyState(["a", "b"], [], ["a"]),
    );
    expect(
      types(
        await decideOn(
          director,
          toolDone("a"),
          historyState(["a", "b"], ["a"]),
        ),
      ),
    ).toEqual(["checkpoint", "infer"]);
  });

  test("after a respawn, a rejected call re-infers when its sibling was running at the crash", async () => {
    const director = createDefaultDirector("test agent", []);

    expect(
      types(
        await decideOn(
          director,
          rejected("a"),
          historyState(["a", "b"], ["a"], ["a"]),
        ),
      ),
    ).toEqual(["checkpoint", "infer"]);
  });

  test("resumed calls that history does not hold re-infer once after both results", async () => {
    const director = createDefaultDirector("test agent", []);

    await decideOn(director, {
      type: "resume.execute_tools",
      calls: [makeToolCall("a"), makeToolCall("b")],
    });
    expect(await decideOn(director, toolDone("a"))).toEqual([]);
    expect(types(await decideOn(director, toolDone("b")))).toEqual([
      "checkpoint",
      "infer",
    ]);
  });

  test("a rejection that history does not place in a batch re-infers", async () => {
    const director = createDefaultDirector("test agent", []);
    expect(types(await decideOn(director, rejected("a")))).toEqual([
      "checkpoint",
      "infer",
    ]);
  });
});
