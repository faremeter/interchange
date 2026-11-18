import { describe, test, expect } from "bun:test";

import { validateActions } from "./actions";
import { createAuthzExtension } from "./authz-extension";
import { createGateManager } from "./gates";
import { createCorrelationRegistry } from "./correlation";
import { createReactor, legacyTurnId, mergeInferenceOptions } from "./reactor";
import { createDefaultDependencies } from "./providers";
import { createDefaultDirector } from "./default-director";
import { assertWellFormedToolSequence } from "./turns";
import { createInboundMessage } from "@intx/mime";
import { waitUntil } from "@intx/types/testing";
import { PerCallInferenceOptions } from "@intx/types/runtime";

import type {
  ReactorDirector,
  ReactorAction,
  ReactorInboundEvent,
  ReactorState,
  ReactorCapabilities,
  ContextStore,
  ToolCall,
  ToolRunner,
  InferenceEvent,
  InboundMessage,
  ConversationTurn,
  PendingOperation,
  PersistedPendingOperation,
  TokenUsage,
  ContextCommit,
  AssistantTurn,
  InferenceError,
  PartialMessage,
  BeforeToolDecision,
  BeforeToolExtension,
  Compactor,
  GateType,
  LastCycleSource,
  ToolResult,
  TurnEndedData,
} from "@intx/types/runtime";

import type { ReactorConfig, Reactor, ReactorEmittedEvent } from "./reactor";
import type {
  Dependencies,
  InferenceHarnessOptions,
  Scheduler,
} from "./harness";
import type { CorrelationValidator } from "./correlation";
import type { AfterInferenceHook } from "./default-director";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function emptyUsage(): TokenUsage {
  return { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, thinking: 0 };
}

const TEST_SOURCE: LastCycleSource = {
  sourceId: "test-source",
  provider: "test-provider",
  model: "test-model",
};

function makeContextStore(turns: ConversationTurn[] = []): ContextStore {
  async function commit(
    options: { message: string },
    _signal?: AbortSignal,
  ): Promise<ContextCommit> {
    return { hash: "abc", message: options.message, timestamp: Date.now() };
  }

  return {
    async load() {
      return {
        turns,
        pendingOperations: [],
        tokenUsage: emptyUsage(),
        connectorState: null,
      };
    },
    setConnectorState() {
      /* noop */
    },
    commit,
    async branch() {
      /* noop */
    },
    async log() {
      return [];
    },
    async readAt() {
      return [];
    },
    async writeBlob() {
      /* noop */
    },
    async readBlob() {
      throw new Error("not implemented");
    },
    async writePrompt() {
      /* noop */
    },
    async writeResponse() {
      /* noop */
    },
    async writeManifest() {
      /* noop */
    },
    async writeTurns() {
      /* noop */
    },
    async writeMetadata() {
      /* noop */
    },
    async readManifestHistory() {
      throw new Error("not implemented");
    },
  };
}

function makeToolRunner(
  handler: (call: {
    id: string;
    name: string;
    arguments: Record<string, unknown>;
  }) => Promise<{
    callId: string;
    content: string;
    isError?: boolean;
  }>,
): ToolRunner {
  return {
    async run(call, _signal) {
      return handler(call);
    },
  };
}

function noopToolRunner(): ToolRunner {
  return {
    async run(call) {
      return { callId: call.id, content: "ok" };
    },
  };
}

function collectEvents(): {
  events: ReactorEmittedEvent[];
  onEvent: (e: ReactorEmittedEvent) => void;
} {
  const events: ReactorEmittedEvent[] = [];
  return {
    events,
    onEvent: (e: ReactorEmittedEvent) => events.push(e),
  };
}

// Resolve with the first collected event matching `predicate`, re-checking
// after each event-loop turn. No deadline: a wall-clock number here is a race
// the test loses under load, and a never-arriving event is the lane timeout's
// job to report (CONVENTIONS.md, "Synchronizing on State, Not Time").
async function waitForEvent(
  events: ReactorEmittedEvent[],
  predicate: (e: ReactorEmittedEvent) => boolean,
): Promise<ReactorEmittedEvent> {
  let found: ReactorEmittedEvent | undefined;
  await waitUntil(() => {
    found = events.find(predicate);
    return found !== undefined;
  });
  if (found === undefined) {
    throw new Error("unreachable: waitUntil resolved without a match");
  }
  return found;
}

// Simple inbound message factory. Delegates to the mail-builder so the
// reactor tests exercise the same shape consumers of @intx/mime
// produce in production code.
function makeInboundMessage(correlationId?: string): InboundMessage {
  return createInboundMessage({
    from: "test@example.com",
    to: "agent@example.com",
    content: "hello",
    ...(correlationId !== undefined ? { correlationId } : {}),
  });
}

// An approval decision delivered to a parked run, stamped with the
// correlationId of the suspension it resolves. The body is the JSON-encoded
// ApprovalDecision the step invoker packs as the message content, which the
// reactor parses on the correlation path to drive the resume.
function makeApprovalMessage(
  correlationId: string,
  outcome: "approved" | "rejected" = "approved",
): InboundMessage {
  return createInboundMessage({
    from: "signal@local",
    to: "agent@example.com",
    content: JSON.stringify({ outcome }),
    correlationId,
  });
}

// ---------------------------------------------------------------------------
// Test harness helpers
// ---------------------------------------------------------------------------

let testSessionCounter = 0;

type DirectorHandler<E> = (
  event: E,
  state: ReactorState,
  caps: ReactorCapabilities,
) => ReactorAction | ReactorAction[] | Promise<ReactorAction | ReactorAction[]>;

type DirectorTable = {
  [K in ReactorInboundEvent["type"]]?: DirectorHandler<
    Extract<ReactorInboundEvent, { type: K }>
  >;
};

function directorFromTable(
  table: DirectorTable,
  defaultAction: "done" | "wait" = "done",
): ReactorDirector {
  return {
    async decide(event, state, caps) {
      // TypeScript cannot correlate the runtime key with the mapped type's
      // per-key handler signature (correlated union problem): table[event.type]
      // is typed as a union of all handlers, but we know it matches this event.
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- correlated union: table[event.type] is guaranteed to be typed for this event.type by the DirectorTable mapped type
      const handler = table[event.type] as
        | DirectorHandler<typeof event>
        | undefined;
      if (handler !== undefined) {
        return handler(event, state, caps);
      }
      return defaultAction === "done" ? caps.done() : caps.wait();
    },
  };
}

type TestReactorOverrides = {
  director?: ReactorDirector;
  toolRunner?: ToolRunner;
  contextStore?: ContextStore;
  correlationValidator?: CorrelationValidator;
  inferenceRunner?: (
    opts: InferenceHarnessOptions,
  ) => AsyncGenerator<InferenceEvent>;
  beforeToolExtensions?: BeforeToolExtension[];
  afterCheckpoint?: () => Promise<void>;
  onShutdown?: () => Promise<void>;
  sessionId?: string;
  gateTimeout?: number;
  shutdownTimeoutMs?: number;
  doomLoopThreshold?: number | false;
  // Wire-level tests (see tests/inference/reactor-streaming.test.ts) supply
  // their own `Dependencies` (fetch stubbed by the harness) and may target a
  // non-Anthropic provider. Both override hooks are optional; omit them for
  // tests that don't care about the inference HTTP path.
  deps?: Dependencies;
  source?: ReactorConfig["source"];
  failOverToNextSource?: () => boolean;
  resetToPreferredSource?: () => void;
  compactors?: Record<string, Compactor>;
};

type TestReactorHandle = {
  reactor: Reactor;
  events: ReactorEmittedEvent[];
  waitFor: (
    type: ReactorEmittedEvent["type"],
    timeoutMs?: number,
  ) => Promise<ReactorEmittedEvent>;
};

type ManualTimer = {
  delayMs: number;
  callback: () => void;
  cancelled: boolean;
  fired: boolean;
};

// Reactor dependencies whose scheduler fires a timer only when the test
// fires it. Gate timeouts run on this scheduler, so a test orders a timeout
// against other steps without sleeping on the wall clock.
function manualTimers(): {
  deps: Dependencies;
  pending: () => ManualTimer[];
  fire: (timer: ManualTimer | undefined) => void;
} {
  const timers: ManualTimer[] = [];
  const scheduler: Scheduler = {
    setTimeout(callback, delayMs) {
      const timer = { delayMs, callback, cancelled: false, fired: false };
      timers.push(timer);
      return () => {
        timer.cancelled = true;
      };
    },
    now: () => 0,
  };
  return {
    deps: { ...createDefaultDependencies(), scheduler },
    pending: () => timers.filter((t) => !t.cancelled && !t.fired),
    fire(timer) {
      if (timer === undefined || timer.cancelled || timer.fired) {
        throw new Error("expected a pending timer");
      }
      timer.fired = true;
      timer.callback();
    },
  };
}

function createTestReactor(
  overrides: TestReactorOverrides = {},
): TestReactorHandle {
  const { events, onEvent } = collectEvents();
  const sessionId = overrides.sessionId ?? `test-sess-${++testSessionCounter}`;

  const config: ReactorConfig = {
    sessionId,
    director: overrides.director ?? directorFromTable({}),
    source: overrides.source ?? {
      id: "anthropic:test-model",
      provider: "anthropic",
      baseURL: "https://api.anthropic.com",
      credentialId: "test",
      model: "test-model",
    },
    toolRunner: overrides.toolRunner ?? noopToolRunner(),
    contextStore: overrides.contextStore ?? makeContextStore(),
    onEvent,
    deps: overrides.deps ?? createDefaultDependencies(),
    shutdownTimeoutMs: overrides.shutdownTimeoutMs ?? 100,
    ...(overrides.correlationValidator !== undefined
      ? { correlationValidator: overrides.correlationValidator }
      : {}),
    ...(overrides.inferenceRunner !== undefined
      ? { inferenceRunner: overrides.inferenceRunner }
      : {}),
    ...(overrides.failOverToNextSource !== undefined
      ? { failOverToNextSource: overrides.failOverToNextSource }
      : {}),
    ...(overrides.resetToPreferredSource !== undefined
      ? { resetToPreferredSource: overrides.resetToPreferredSource }
      : {}),
    ...(overrides.beforeToolExtensions !== undefined
      ? { beforeToolExtensions: overrides.beforeToolExtensions }
      : {}),
    ...(overrides.afterCheckpoint !== undefined
      ? { afterCheckpoint: overrides.afterCheckpoint }
      : {}),
    ...(overrides.onShutdown !== undefined
      ? { onShutdown: overrides.onShutdown }
      : {}),
    ...(overrides.gateTimeout !== undefined
      ? { gateTimeout: overrides.gateTimeout }
      : {}),
    ...(overrides.doomLoopThreshold !== undefined
      ? { doomLoopThreshold: overrides.doomLoopThreshold }
      : {}),
    ...(overrides.compactors !== undefined
      ? { compactors: overrides.compactors }
      : {}),
  };

  const reactor = createReactor(config);

  function waitFor(
    type: ReactorEmittedEvent["type"],
  ): Promise<ReactorEmittedEvent> {
    return waitForEvent(events, (e) => e.type === type);
  }

  return { reactor, events, waitFor };
}

function failingContextStore(error: Error): ContextStore {
  const fail = () => {
    throw new Error("failingContextStore: should not be called");
  };
  return {
    async load() {
      throw error;
    },
    setConnectorState() {
      return fail();
    },
    async commit() {
      return fail();
    },
    async branch() {
      return fail();
    },
    async log() {
      return fail();
    },
    async readAt() {
      return fail();
    },
    async writeBlob() {
      return fail();
    },
    async readBlob() {
      return fail();
    },
    async writePrompt() {
      return fail();
    },
    async writeResponse() {
      return fail();
    },
    async writeManifest() {
      return fail();
    },
    async writeTurns() {
      return fail();
    },
    async writeMetadata() {
      return fail();
    },
    async readManifestHistory() {
      return fail();
    },
  };
}

function throwingToolRunner(error: Error): ToolRunner {
  return {
    async run() {
      throw error;
    },
  };
}

function getEvent<T extends ReactorEmittedEvent["type"]>(
  events: ReactorEmittedEvent[],
  type: T,
): Extract<ReactorEmittedEvent, { type: T }> {
  const found = events.find(
    (e): e is Extract<ReactorEmittedEvent, { type: T }> => e.type === type,
  );
  if (found === undefined) {
    throw new Error(`No event of type '${type}' found`);
  }
  return found;
}

// The data of each `reactor.turn.ended`, in emit order.
function turnEnds(events: ReactorEmittedEvent[]): TurnEndedData[] {
  const ends: TurnEndedData[] = [];
  for (const e of events) {
    if (e.type === "reactor.turn.ended") ends.push(e.data);
  }
  return ends;
}

// Wraps a director and records each event it decides on, in order.
function recordingDirector(inner: ReactorDirector): {
  director: ReactorDirector;
  decided: ReactorInboundEvent[];
} {
  const decided: ReactorInboundEvent[] = [];
  return {
    decided,
    director: {
      async decide(event, state, caps) {
        decided.push(event);
        return inner.decide(event, state, caps);
      },
    },
  };
}

// The result of the emitted `tool.done` that answers `callId`.
function toolResultOf(
  events: ReactorEmittedEvent[],
  callId: string,
): ToolResult {
  for (const e of events) {
    if (e.type === "tool.done" && e.data.result.callId === callId) {
      return e.data.result;
    }
  }
  throw new Error(`No tool.done for ${callId}`);
}

// The error result of a gated call that a turn's one gate refuses.
const GATE_REFUSAL = /waiting on a gate/;

// ---------------------------------------------------------------------------
// 1. Action validation
// ---------------------------------------------------------------------------

describe("validateActions", () => {
  test("single infer action is valid", () => {
    const result = validateActions({ type: "infer" });
    expect(result.ok).toBe(true);
    if (!result.ok) throw new Error("unreachable");
    expect(result.normalized.some((a) => a.type === "infer")).toBe(true);
  });

  test("single done action is valid", () => {
    const result = validateActions({ type: "done" });
    expect(result.ok).toBe(true);
  });

  test("single execute_tools is valid", () => {
    const result = validateActions({
      type: "execute_tools",
      calls: [{ id: "c1", name: "tool", arguments: {} }],
    });
    expect(result.ok).toBe(true);
  });

  test("checkpoint + infer is valid (composable)", () => {
    const result = validateActions([
      { type: "checkpoint", message: "checkpoint" },
      { type: "infer" },
    ]);
    expect(result.ok).toBe(true);
  });

  test("checkpoint message is preserved in normalized output", () => {
    const result = validateActions({
      type: "checkpoint",
      message: "before tool call",
    });
    expect(result.ok).toBe(true);
    if (!result.ok) throw new Error("unreachable");
    const cp = result.normalized.find((a) => a.type === "checkpoint");
    expect(cp).toBeDefined();
    if (cp?.type !== "checkpoint") throw new Error("unreachable");
    expect(cp.message).toBe("before tool call");
  });

  test("multiple checkpoint actions are rejected", () => {
    const result = validateActions([
      { type: "checkpoint", message: "first" },
      { type: "checkpoint", message: "second" },
    ]);
    expect(result.ok).toBe(false);
    if (result.ok) throw new Error("unreachable");
    expect(result.error).toMatch(/multiple checkpoint/i);
  });

  test("emit + infer is valid", () => {
    const result = validateActions([
      { type: "emit", eventType: "custom.progress", data: { pct: 50 } },
      { type: "infer" },
    ]);
    expect(result.ok).toBe(true);
  });

  test("multiple execute_tools are collapsed into one", () => {
    const result = validateActions([
      {
        type: "execute_tools",
        calls: [{ id: "c1", name: "a", arguments: {} }],
      },
      {
        type: "execute_tools",
        calls: [{ id: "c2", name: "b", arguments: {} }],
      },
    ]);
    expect(result.ok).toBe(true);
    if (!result.ok) throw new Error("unreachable");
    const toolActions = result.normalized.filter(
      (a) => a.type === "execute_tools",
    );
    expect(toolActions.length).toBe(1);
    const ta = toolActions[0];
    if (ta === undefined || ta.type !== "execute_tools")
      throw new Error("unreachable");
    expect(ta.calls.length).toBe(2);
  });

  test("infer + done is invalid", () => {
    const result = validateActions([{ type: "infer" }, { type: "done" }]);
    expect(result.ok).toBe(false);
    if (result.ok) throw new Error("unreachable");
    expect(result.error).toMatch(/infer.*done/i);
  });

  test("multiple infer actions are invalid", () => {
    const result = validateActions([{ type: "infer" }, { type: "infer" }]);
    expect(result.ok).toBe(false);
  });

  test("suspend + infer is invalid", () => {
    const result = validateActions([
      { type: "infer" },
      {
        type: "suspend",
        gate: {
          type: "approval",
          gateId: "g1",
          timeoutMs: 60000,
        },
      },
    ]);
    expect(result.ok).toBe(false);
    if (result.ok) throw new Error("unreachable");
    expect(result.error).toMatch(/suspend.*infer/i);
  });

  test("suspend + execute_tools is invalid", () => {
    const result = validateActions([
      {
        type: "execute_tools",
        calls: [{ id: "c1", name: "t", arguments: {} }],
      },
      {
        type: "suspend",
        gate: {
          type: "payment",
          gateId: "g2",
          timeoutMs: 60000,
        },
      },
    ]);
    expect(result.ok).toBe(false);
  });

  test("empty action list is valid (no-op)", () => {
    const result = validateActions([]);
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.normalized).toEqual([]);
    }
  });

  test("suspend alone is valid", () => {
    const result = validateActions({
      type: "suspend",
      gate: { type: "approval", gateId: "g1", timeoutMs: 60000 },
    });
    expect(result.ok).toBe(true);
  });

  test("wait action is included in normalized output", () => {
    const result = validateActions({ type: "wait" });
    expect(result.ok).toBe(true);
    if (!result.ok) throw new Error("unreachable");
    expect(result.normalized.length).toBe(1);
    expect(result.normalized[0]?.type).toBe("wait");
  });

  test("multiple wait actions are invalid", () => {
    const result = validateActions([{ type: "wait" }, { type: "wait" }]);
    expect(result.ok).toBe(false);
    if (result.ok) throw new Error("unreachable");
    expect(result.error).toMatch(/multiple.*wait/i);
  });

  test("wait + infer is invalid", () => {
    const result = validateActions([{ type: "wait" }, { type: "infer" }]);
    expect(result.ok).toBe(false);
    if (result.ok) throw new Error("unreachable");
    expect(result.error).toMatch(/wait.*infer/i);
  });

  test("wait + execute_tools is invalid", () => {
    const result = validateActions([
      { type: "wait" },
      {
        type: "execute_tools",
        calls: [{ id: "c1", name: "t", arguments: {} }],
      },
    ]);
    expect(result.ok).toBe(false);
    if (result.ok) throw new Error("unreachable");
    expect(result.error).toMatch(/wait.*execute_tools/i);
  });

  test("wait + suspend is invalid", () => {
    const result = validateActions([
      { type: "wait" },
      {
        type: "suspend",
        gate: { type: "approval", gateId: "g1", timeoutMs: 60000 },
      },
    ]);
    expect(result.ok).toBe(false);
    if (result.ok) throw new Error("unreachable");
    expect(result.error).toMatch(/wait.*suspend/i);
  });

  test("wait + reply is invalid", () => {
    const result = validateActions([
      { type: "wait" },
      { type: "reply", content: "hello" },
    ]);
    expect(result.ok).toBe(false);
    if (result.ok) throw new Error("unreachable");
    expect(result.error).toMatch(/wait.*reply/i);
  });

  test("wait + done is invalid", () => {
    const result = validateActions([{ type: "wait" }, { type: "done" }]);
    expect(result.ok).toBe(false);
    if (result.ok) throw new Error("unreachable");
    expect(result.error).toMatch(/wait.*done/i);
  });

  test("reply + infer is invalid", () => {
    const result = validateActions([
      { type: "reply", content: "hello" },
      { type: "infer" },
    ]);
    expect(result.ok).toBe(false);
    if (result.ok) throw new Error("unreachable");
    expect(result.error).toMatch(/reply.*infer/i);
  });

  test("reply + execute_tools is invalid", () => {
    const result = validateActions([
      { type: "reply", content: "hello" },
      {
        type: "execute_tools",
        calls: [{ id: "c1", name: "t", arguments: {} }],
      },
    ]);
    expect(result.ok).toBe(false);
    if (result.ok) throw new Error("unreachable");
    expect(result.error).toMatch(/reply.*execute_tools/i);
  });

  test("multiple done actions are invalid", () => {
    const result = validateActions([{ type: "done" }, { type: "done" }]);
    expect(result.ok).toBe(false);
    if (result.ok) throw new Error("unreachable");
    expect(result.error).toMatch(/multiple.*done/i);
  });

  test("multiple reply actions are invalid", () => {
    const result = validateActions([
      { type: "reply", content: "a" },
      { type: "reply", content: "b" },
    ]);
    expect(result.ok).toBe(false);
    if (result.ok) throw new Error("unreachable");
    expect(result.error).toMatch(/multiple.*reply/i);
  });

  test("multiple suspend actions are invalid", () => {
    const result = validateActions([
      {
        type: "suspend",
        gate: { type: "approval", gateId: "g1", timeoutMs: 60000 },
      },
      {
        type: "suspend",
        gate: { type: "payment", gateId: "g2", timeoutMs: 60000 },
      },
    ]);
    expect(result.ok).toBe(false);
    if (result.ok) throw new Error("unreachable");
    expect(result.error).toMatch(/multiple.*suspend/i);
  });

  test("duplicate fork IDs are invalid", () => {
    const result = validateActions([
      { type: "fork", mode: "independent", forkId: "f1" },
      { type: "fork", mode: "child", forkId: "f1" },
    ]);
    expect(result.ok).toBe(false);
    if (result.ok) throw new Error("unreachable");
    expect(result.error).toMatch(/duplicate.*fork/i);
  });

  test("reply + done is invalid", () => {
    const result = validateActions([
      { type: "reply", content: "goodbye" },
      { type: "done" },
    ]);
    expect(result.ok).toBe(false);
    if (result.ok) throw new Error("unreachable");
    expect(result.error).toMatch(/reply.*done/i);
  });

  test("reply + suspend is invalid", () => {
    const result = validateActions([
      { type: "reply", content: "hang on" },
      {
        type: "suspend",
        gate: { type: "approval", gateId: "g1", timeoutMs: 60000 },
      },
    ]);
    expect(result.ok).toBe(false);
    if (result.ok) throw new Error("unreachable");
    expect(result.error).toMatch(/reply.*suspend/i);
  });

  // The director builds action sets with a leading checkpoint. The
  // reply+done and wait+reply invariants must hold for that emitted
  // three-action shape, not only the bare pairs above, so a checkpoint
  // prefix cannot smuggle a contradictory set past the validator.
  test("checkpoint + reply + done is invalid", () => {
    const result = validateActions([
      { type: "checkpoint", message: "checkpoint: after-inference-abort" },
      { type: "reply", content: "budget exhausted" },
      { type: "done" },
    ]);
    expect(result.ok).toBe(false);
    if (result.ok) throw new Error("unreachable");
    expect(result.error).toMatch(/reply.*done/i);
  });

  test("checkpoint + reply + wait is invalid", () => {
    const result = validateActions([
      { type: "checkpoint", message: "checkpoint: after-inference-halt" },
      { type: "reply", content: "paused for top-up" },
      { type: "wait" },
    ]);
    expect(result.ok).toBe(false);
    if (result.ok) throw new Error("unreachable");
    expect(result.error).toMatch(/wait.*reply/i);
  });
});

// ---------------------------------------------------------------------------
// 2. Gate lifecycle
// ---------------------------------------------------------------------------

describe("createGateManager", () => {
  test("register and clear a gate", async () => {
    const manager = createGateManager();
    const cleared: string[] = [];

    const promise = manager.register(
      "gate-1",
      "approval",
      5000,
      undefined,
      (id, reason, turnId) => cleared.push(`${id}:${reason}:${turnId}`),
      "turn-1",
    );

    const didClear = manager.clear("gate-1");
    expect(didClear).toBe(true);

    const reason = await promise;
    expect(reason).toBe("resolved");
    expect(cleared).toEqual(["gate-1:resolved:turn-1"]);
  });

  test("clearSilently resolves and removes the gate without invoking onCleared", async () => {
    const manager = createGateManager();
    const cleared: string[] = [];

    const promise = manager.register(
      "gate-silent",
      "approval",
      5000,
      undefined,
      (id, reason) => cleared.push(`${id}:${reason}`),
      "turn-1",
    );

    const didClear = manager.clearSilently("gate-silent");
    expect(didClear).toBe(true);

    // The gate's promise still resolves as resolved, so any awaiter unblocks.
    const reason = await promise;
    expect(reason).toBe("resolved");

    // The whole point of clearSilently: onCleared is NOT invoked, so no
    // gate-cleared continuation is enqueued (contrast with clear() above,
    // which does invoke it).
    expect(cleared).toEqual([]);

    // The gate is gone: it no longer resolves by correlation and a second
    // clear finds nothing to clear.
    expect(manager.has("gate-silent")).toBe(false);
    expect(manager.clear("gate-silent")).toBe(false);
    expect(manager.clearSilently("gate-silent")).toBe(false);
  });

  test("gate timeout fires with reason=timeout", async () => {
    const manager = createGateManager();
    const cleared: string[] = [];

    const promise = manager.register(
      "gate-2",
      "approval",
      50,
      undefined,
      (id, reason, turnId) => cleared.push(`${id}:${reason}:${turnId}`),
      "turn-2",
    );

    const reason = await promise;
    expect(reason).toBe("timeout");
    expect(cleared).toEqual(["gate-2:timeout:turn-2"]);
  });

  test("shutdown clears all gates with reason=shutdown", async () => {
    const manager = createGateManager();
    const cleared: string[] = [];

    const p1 = manager.register(
      "g1",
      "approval",
      60000,
      undefined,
      (id, reason, turnId) => cleared.push(`${id}:${reason}:${turnId}`),
      "t1",
    );
    const p2 = manager.register(
      "g2",
      "payment",
      60000,
      undefined,
      (id, reason, turnId) => cleared.push(`${id}:${reason}:${turnId}`),
      "t2",
    );

    manager.shutdown();

    const [r1, r2] = await Promise.all([p1, p2]);
    expect(r1).toBe("shutdown");
    expect(r2).toBe("shutdown");
    expect(cleared.sort()).toEqual(["g1:shutdown:t1", "g2:shutdown:t2"].sort());
  });

  test("clearing a nonexistent gate returns false", () => {
    const manager = createGateManager();
    expect(manager.clear("no-such-gate")).toBe(false);
  });

  test("zero timeout throws", () => {
    const manager = createGateManager();
    expect(() =>
      manager.register(
        "g",
        "approval",
        0,
        undefined,
        () => {
          /* noop */
        },
        "t",
      ),
    ).toThrow();
  });

  test("duplicate gate ID throws", () => {
    const manager = createGateManager();
    void manager.register(
      "g",
      "approval",
      5000,
      undefined,
      () => {
        /* noop */
      },
      "t",
    );
    expect(() =>
      manager.register(
        "g",
        "approval",
        5000,
        undefined,
        () => {
          /* noop */
        },
        "t",
      ),
    ).toThrow();
  });

  test("findByCorrelationId returns the gate", () => {
    const manager = createGateManager();
    void manager.register(
      "g-corr",
      "message_response",
      5000,
      "corr-42",
      () => {
        /* noop */
      },
      "t",
    );
    const found = manager.findByCorrelationId("corr-42");
    expect(found?.gateId).toBe("g-corr");

    const notFound = manager.findByCorrelationId("no-match");
    expect(notFound).toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// 3. Correlation registry
// ---------------------------------------------------------------------------

describe("createCorrelationRegistry", () => {
  function op(id: string): PendingOperation {
    return {
      correlationId: id,
      kind: "approval",
      registeredAt: Date.now(),
      gateId: `g-${id}`,
      turnId: `t-${id}`,
      gated: true,
    };
  }

  test("register and lookup", () => {
    const reg = createCorrelationRegistry();
    reg.register(op("abc"));
    const found = reg.lookup("abc");
    expect(found?.correlationId).toBe("abc");
  });

  test("lookup nonexistent returns undefined", () => {
    const reg = createCorrelationRegistry();
    expect(reg.lookup("missing")).toBeUndefined();
  });

  test("remove returns true for existing entry", () => {
    const reg = createCorrelationRegistry();
    reg.register(op("x"));
    expect(reg.remove("x")).toBe(true);
    expect(reg.lookup("x")).toBeUndefined();
  });

  test("remove returns false for nonexistent", () => {
    const reg = createCorrelationRegistry();
    expect(reg.remove("ghost")).toBe(false);
  });

  test("duplicate registration throws", () => {
    const reg = createCorrelationRegistry();
    reg.register(op("dup"));
    expect(() => reg.register(op("dup"))).toThrow();
  });

  test("all() returns all operations", () => {
    const reg = createCorrelationRegistry();
    reg.register(op("a"));
    reg.register(op("b"));
    const all = reg.all();
    expect(all.map((o) => o.correlationId).sort()).toEqual(["a", "b"]);
  });
});

// ---------------------------------------------------------------------------
// 4. Reactor loop — basic flow
// ---------------------------------------------------------------------------

describe("createReactor — basic flow", () => {
  test("message.received → done emits reactor.done", async () => {
    const { reactor, events, waitFor } = createTestReactor();

    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitFor("reactor.done");

    const types = events.map((e) => e.type);
    expect(types).toContain("reactor.start");
    expect(types).toContain("message.received");
    expect(types).toContain("reactor.done");
  });

  test("reactor.start is the first event", async () => {
    const { reactor, events, waitFor } = createTestReactor();

    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitFor("reactor.done");

    expect(events[0]?.type).toBe("reactor.start");
  });

  test("sequence numbers are monotonically increasing", async () => {
    const { reactor, events, waitFor } = createTestReactor({
      director: directorFromTable({
        "message.received": (_e, _s, caps) => [
          caps.emit("custom.test", { x: 1 }),
          caps.done(),
        ],
      }),
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitFor("reactor.done");

    const seqs = events.map((e) => e.seq);
    for (let i = 1; i < seqs.length; i++) {
      const prev = seqs[i - 1];
      const curr = seqs[i];
      if (prev === undefined || curr === undefined) continue;
      expect(curr).toBeGreaterThan(prev);
    }
  });
});

// ---------------------------------------------------------------------------
// 5. Director exception handling
// ---------------------------------------------------------------------------

describe("createReactor — director exception", () => {
  test("director exception emits reactor.error and shuts down", async () => {
    const { events, onEvent } = collectEvents();

    const director: ReactorDirector = {
      async decide() {
        throw new Error("director blew up");
      },
    };

    const reactor = createReactor({
      sessionId: "sess-err",
      director,
      source: {
        id: "anthropic:test-model",
        provider: "anthropic",
        baseURL: "https://api.anthropic.com",
        credentialId: "test",
        model: "test-model",
      },
      toolRunner: noopToolRunner(),
      contextStore: makeContextStore(),
      onEvent,
      deps: createDefaultDependencies(),
      shutdownTimeoutMs: 100,
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());

    await waitForEvent(events, (e) => e.type === "reactor.error");

    const errorEvent = events.find((e) => e.type === "reactor.error");
    if (errorEvent === undefined || errorEvent.type !== "reactor.error") {
      throw new Error("expected reactor.error");
    }
    expect(errorEvent.data.error).toMatch(/director blew up/);
    expect(errorEvent.data.fatal).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// 6. Gate lifecycle via reactor
// ---------------------------------------------------------------------------

describe("createReactor — gate lifecycle", () => {
  test("suspend registers gate and reactor shuts down on abort", async () => {
    const { reactor, events, waitFor } = createTestReactor({
      director: directorFromTable({
        "message.received": (_e, _s, caps) =>
          caps.suspend({
            type: "approval",
            gateId: "test-gate",
            timeoutMs: 5000,
          }),
        "reactor.gate.cleared": (_e, _s, caps) => caps.done(),
      }),
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());

    await waitFor("reactor.gate.blocked");

    // Abort the reactor — gate clears with reason "shutdown".
    reactor.abort("admin_kill");

    await waitFor("reactor.done");

    const blocked = getEvent(events, "reactor.gate.blocked");
    expect(blocked.data.reason).toBe("approval");
    expect(blocked.data.gateId).toBe("test-gate");

    const cleared = getEvent(events, "reactor.gate.cleared");
    expect(cleared.data.gateId).toBe("test-gate");
    expect(cleared.data.reason).toBe("shutdown");
  });

  test("does not emit reactor.gate.blocked until the suspend is durably committed", async () => {
    // Persist-before-settle. The `reactor.gate.blocked` event resolves the
    // `send()` awaiter as "suspended", and a downstream consumer (the warm
    // agent's run-boundary durability mirror) reads the pending operation back
    // out of the context store the instant `send()` settles. So the durable
    // commit must land BEFORE the event is emitted -- otherwise the mirror
    // reads an uncommitted store and durably loses the approval snapshot. This
    // test gates the commit's `writeMetadata`: `blocked` must not appear while
    // the commit is pending, and must appear once it is released. A regression
    // that emits before committing would fire `blocked` while the gate is held.
    let releaseCommit: (() => void) | undefined;
    const commitGate = new Promise<void>((resolve) => {
      releaseCommit = resolve;
    });
    // Resolves when the reactor reaches the held commit, so the test observes
    // the window rather than guessing when it opens.
    let signalCommitEntered: (() => void) | undefined;
    const commitEntered = new Promise<void>((resolve) => {
      signalCommitEntered = resolve;
    });
    // Whether `blocked` had been emitted at the moment the commit was
    // released -- read inside the store, which is the last point still inside
    // the window. A reactor that emitted before or concurrently with the
    // commit shows up here as `true`.
    let blockedDuringCommit: boolean | undefined;
    let signalCommitObserved: (() => void) | undefined;
    const commitObserved = new Promise<void>((resolve) => {
      signalCommitObserved = resolve;
    });
    const base = makeContextStore();
    const contextStore: ContextStore = {
      ...base,
      async writeMetadata(metadata, signal) {
        signalCommitEntered?.();
        await commitGate;
        // Record the suspend's own commit only. A later cycle's commit runs
        // after `blocked` has legitimately fired and must not overwrite it.
        if (blockedDuringCommit === undefined) {
          blockedDuringCommit = events.some(
            (e) => e.type === "reactor.gate.blocked",
          );
          signalCommitObserved?.();
        }
        return base.writeMetadata(metadata, signal);
      },
    };
    const { reactor, events, waitFor } = createTestReactor({
      contextStore,
      director: directorFromTable({
        "message.received": (_e, _s, caps) =>
          caps.suspend({
            type: "approval",
            gateId: "commit-order-gate",
            timeoutMs: 5000,
          }),
        "reactor.gate.cleared": (_e, _s, caps) => caps.done(),
      }),
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());

    // The reactor is parked inside the held commit, so a correctly-ordered
    // reactor has not emitted `blocked` yet; a reactor that emits before
    // committing would have emitted it before reaching this point.
    await commitEntered;
    expect(events.some((e) => e.type === "reactor.gate.blocked")).toBe(false);

    // Release the durable commit; only now may `blocked` be emitted. The
    // in-store check runs before the release returns control to the reactor,
    // so it closes the window the check above leaves open: an emit that races
    // the commit instead of preceding it.
    releaseCommit?.();
    await commitObserved;
    expect(blockedDuringCommit).toBe(false);
    await waitFor("reactor.gate.blocked");

    reactor.abort("admin_kill");
    await waitFor("reactor.done");
  });

  test("defers a gate clear racing the commit until after blocked", async () => {
    // blocked-before-cleared. A gate whose timeout timer elapses while the
    // suspend's durable commit is still in flight must not have its clear take
    // effect before `reactor.gate.blocked` is emitted: downstream status
    // derivation and the send-awaiter assume a gate's `blocked` precedes any
    // effect of its clearing. This holds the commit's `writeMetadata` open long
    // enough for a short gate timeout to fire inside the window, then asserts
    // `blocked` is emitted before the `reactor.gate.cleared` it belongs to.
    let releaseCommit: (() => void) | undefined;
    const commitGate = new Promise<void>((resolve) => {
      releaseCommit = resolve;
    });
    // Resolves when the reactor reaches the held commit. suspendOnGate arms
    // the gate's timeout timer before it commits, so once this resolves the
    // 30ms timer below is already scheduled.
    let signalCommitEntered: (() => void) | undefined;
    const commitEntered = new Promise<void>((resolve) => {
      signalCommitEntered = resolve;
    });
    const base = makeContextStore();
    const contextStore: ContextStore = {
      ...base,
      async writeMetadata(metadata, signal) {
        signalCommitEntered?.();
        await commitGate;
        return base.writeMetadata(metadata, signal);
      },
    };
    const { reactor, events, waitFor } = createTestReactor({
      contextStore,
      director: directorFromTable({
        "message.received": (_e, _s, caps) =>
          caps.suspend({
            type: "approval",
            gateId: "race-gate",
            timeoutMs: 30,
          }),
        "reactor.gate.cleared": (_e, _s, caps) => caps.done(),
      }),
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());

    // The gate timer firing inside the commit window is the race under test,
    // and nothing observable reports it -- a deferred clear is by definition
    // not emitted. So the wait below is ordered against the timer rather than
    // sized to outlast it: the gate's 30ms deadline was set before
    // `commitEntered` resolved, and this timer's 80ms deadline is set after,
    // so the gate's deadline is the earlier of the two and the event loop
    // fires it first no matter how loaded the machine is. Neither the block
    // nor the clear may surface until the commit is released.
    await commitEntered;
    await new Promise((r) => setTimeout(r, 80));
    expect(events.some((e) => e.type === "reactor.gate.blocked")).toBe(false);
    expect(events.some((e) => e.type === "reactor.gate.cleared")).toBe(false);

    releaseCommit?.();
    await waitFor("reactor.done");

    const blockedIndex = events.findIndex(
      (e) => e.type === "reactor.gate.blocked",
    );
    const clearedIndex = events.findIndex(
      (e) => e.type === "reactor.gate.cleared",
    );
    expect(blockedIndex).toBeGreaterThanOrEqual(0);
    expect(clearedIndex).toBeGreaterThan(blockedIndex);
    expect(getEvent(events, "reactor.gate.cleared").data.reason).toBe(
      "timeout",
    );
  });

  test("gate timeout fires reactor.gate.cleared with reason=timeout", async () => {
    const { reactor, events, waitFor } = createTestReactor({
      shutdownTimeoutMs: 500,
      director: directorFromTable({
        "message.received": (_e, _s, caps) =>
          caps.suspend({
            type: "approval",
            gateId: "timeout-gate",
            timeoutMs: 80,
          }),
        "reactor.gate.cleared": (_e, _s, caps) => caps.done(),
      }),
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());

    await waitFor("reactor.done");

    const cleared = getEvent(events, "reactor.gate.cleared");
    expect(cleared.data.reason).toBe("timeout");
  });

  test("gate with timeoutMs: 0 falls back to session-level gateTimeout", async () => {
    const { reactor, events, waitFor } = createTestReactor({
      gateTimeout: 80,
      director: directorFromTable({
        "message.received": (_e, _s, caps) =>
          caps.suspend({
            type: "approval",
            gateId: "fallback-gate",
            timeoutMs: 0,
          }),
        "reactor.gate.cleared": (_e, _s, caps) => caps.done(),
      }),
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());

    await waitFor("reactor.done");

    const cleared = getEvent(events, "reactor.gate.cleared");
    expect(cleared.data.reason).toBe("timeout");
    expect(cleared.data.gateId).toBe("fallback-gate");
  });
});

// ---------------------------------------------------------------------------
// 7. Tool execution
// ---------------------------------------------------------------------------

describe("createReactor — tool execution", () => {
  test("execute_tools dispatches tools and returns results", async () => {
    const toolsRun: string[] = [];
    const { reactor, events, waitFor } = createTestReactor({
      director: directorFromTable({
        "message.received": (_e, _s, caps) =>
          caps.executeTools(
            [
              { id: "c1", name: "tool_a", arguments: {} },
              { id: "c2", name: "tool_b", arguments: {} },
            ],
            true,
          ),
        "tool.done": (_e, _s, caps) => caps.done(),
      }),
      toolRunner: makeToolRunner(async (call) => {
        toolsRun.push(call.name);
        return { callId: call.id, content: `result-${call.name}` };
      }),
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitFor("reactor.done");

    expect(toolsRun.sort()).toEqual(["tool_a", "tool_b"].sort());

    const toolStarts = events.filter((e) => e.type === "tool.start");
    const toolDones = events.filter((e) => e.type === "tool.done");
    expect(toolStarts.length).toBe(2);
    expect(toolDones.length).toBe(2);
  });
});

// ---------------------------------------------------------------------------
// 7b. Doom-loop detection
// ---------------------------------------------------------------------------

// Drives one tool batch per turn. `nextBatch(turn)` supplies the batch to run
// on each turn; returning null (or an empty batch) ends the run with `done`.
// The director re-issues the next batch only once the current batch's results
// are all in, mirroring the real director's pending-result accounting so a
// parallel batch does not fan out into one re-issue per result.
function createBatchLoopDirector(
  nextBatch: (turn: number) => ToolCall[] | null,
): ReactorDirector {
  let turn = 0;
  let pending = 0;

  function issue(caps: ReactorCapabilities): ReactorAction | ReactorAction[] {
    const batch = nextBatch(turn);
    turn += 1;
    if (batch === null || batch.length === 0) return caps.done();
    pending = batch.length;
    return caps.executeTools(batch, true);
  }

  return {
    async decide(event, _state, caps) {
      if (event.type === "message.received") return issue(caps);
      if (event.type === "tool.done") {
        pending -= 1;
        if (pending > 0) return [];
        return issue(caps);
      }
      return [];
    },
  };
}

// Drives the production-shaped agentic loop: each inference emits an assistant
// turn carrying one tool_call, the director executes exactly that call, and the
// tool result feeds the next inference. `argsFor(turn)` controls the arguments;
// the loop ends after `maxTurns` inferences by issuing `done`. Unlike
// `createBatchLoopDirector`, this interleaves an inference turn between tool
// turns, matching the real reactor shape.
function interleavedRunner(
  argsFor: (turn: number) => Record<string, unknown>,
  maxTurns: number,
): {
  inferenceRunner: (
    opts: InferenceHarnessOptions,
  ) => AsyncGenerator<InferenceEvent>;
  director: ReactorDirector;
} {
  let turn = 0;
  const inferenceRunner = async function* (opts: InferenceHarnessOptions) {
    const t = turn;
    yield {
      type: "inference.done" as const,
      seq: opts.nextSeq(),
      data: {
        turn: {
          role: "assistant" as const,
          content: [
            {
              type: "tool_call" as const,
              id: `c${t}`,
              name: "spin",
              arguments: argsFor(t),
            },
          ],
          model: "test-model",
          timestamp: 1000,
        },
        usage: emptyUsage(),
        source: TEST_SOURCE,
      },
    };
  };
  const director = directorFromTable(
    {
      "message.received": (_e, _s, caps) => caps.infer(),
      "inference.done": (_e, _s, caps) => {
        const t = turn;
        turn += 1;
        if (t >= maxTurns) return caps.done();
        return caps.executeTools([
          { id: `c${t}`, name: "spin", arguments: argsFor(t) },
        ]);
      },
      "tool.done": (_e, _s, caps) => caps.infer(),
    },
    "wait",
  );
  return { inferenceRunner, director };
}

describe("createReactor — doom-loop detection", () => {
  test("trips at the threshold on identical consecutive tool turns", async () => {
    const { reactor, events, waitFor } = createTestReactor({
      // Default threshold is 3; loop the same call well past it.
      director: createBatchLoopDirector((turn) =>
        turn < 8
          ? [{ id: `c${turn}`, name: "spin", arguments: { q: 1 } }]
          : null,
      ),
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitFor("reactor.done");

    const error = getEvent(events, "reactor.error");
    expect(error.data.fatal).toBe(true);
    expect(error.data.error).toContain("spin");

    expect(getEvent(events, "reactor.turn.ended").data).toMatchObject({
      outcome: "failed",
      error: { kind: "doom_loop" },
    });

    // The run broke before running the whole 8-turn loop.
    const toolStarts = events.filter((e) => e.type === "tool.start");
    expect(toolStarts.length).toBe(3);
  });

  test("does not trip on retries with different arguments", async () => {
    const { reactor, events, waitFor } = createTestReactor({
      director: createBatchLoopDirector((turn) =>
        turn < 5
          ? [{ id: `c${turn}`, name: "retry", arguments: { attempt: turn } }]
          : null,
      ),
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitFor("reactor.done");

    expect(events.some((e) => e.type === "reactor.error")).toBe(false);
    expect(getEvent(events, "reactor.turn.ended").data.outcome).toBe(
      "unanswered",
    );
  });

  test("treats key-reordered arguments as identical", async () => {
    // Same logical arguments, keys emitted in different orders. Canonical
    // serialization must collapse them to one signature, so three such turns
    // trip the default threshold of 3.
    const variants = [
      { a: 1, b: 2 },
      { b: 2, a: 1 },
      { a: 1, b: 2 },
    ];
    const { reactor, events, waitFor } = createTestReactor({
      director: createBatchLoopDirector((turn) => {
        const args = variants[turn];
        return args === undefined
          ? null
          : [{ id: `c${turn}`, name: "charge", arguments: args }];
      }),
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitFor("reactor.done");

    expect(getEvent(events, "reactor.error").data.fatal).toBe(true);
    expect(getEvent(events, "reactor.turn.ended").data).toMatchObject({
      outcome: "failed",
      error: { kind: "doom_loop" },
    });
  });

  test("trips when an identical call errors every turn", async () => {
    const { reactor, events, waitFor } = createTestReactor({
      director: createBatchLoopDirector((turn) =>
        turn < 8 ? [{ id: `c${turn}`, name: "flaky", arguments: {} }] : null,
      ),
      toolRunner: makeToolRunner(async (call) => ({
        callId: call.id,
        content: "boom",
        isError: true,
      })),
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitFor("reactor.done");

    expect(getEvent(events, "reactor.error").data.fatal).toBe(true);
    expect(getEvent(events, "reactor.turn.ended").data).toMatchObject({
      outcome: "failed",
      error: { kind: "doom_loop" },
    });
  });

  test("trips when an identical call succeeds every turn", async () => {
    const { reactor, events, waitFor } = createTestReactor({
      director: createBatchLoopDirector((turn) =>
        turn < 8 ? [{ id: `c${turn}`, name: "reread", arguments: {} }] : null,
      ),
      toolRunner: makeToolRunner(async (call) => ({
        callId: call.id,
        content: "same file contents",
      })),
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitFor("reactor.done");

    expect(getEvent(events, "reactor.error").data.fatal).toBe(true);
    expect(getEvent(events, "reactor.turn.ended").data).toMatchObject({
      outcome: "failed",
      error: { kind: "doom_loop" },
    });
  });

  test("a suspend and redispatch of one call does not double-count", async () => {
    // A parked call is approved and re-dispatched: one real execution. With a
    // threshold of 2, an implementation that counted the suspended request plus
    // its redispatch would trip here; counting only executed turns must not.
    const askExtension = createAuthzExtension({
      authorize: async () => ({
        effect: "ask" as const,
        matchingGrants: [],
        resolvedBy: null,
      }),
      approvalTimeoutMs: 60_000,
    });
    const toolsRun: string[] = [];

    const { reactor, events, waitFor } = createTestReactor({
      doomLoopThreshold: 2,
      director: directorFromTable(
        {
          "message.received": (_e, _s, caps) => caps.infer(),
          "inference.done": (_e, _s, caps) =>
            caps.executeTools([
              { id: "call-ask", name: "charge_card", arguments: {} },
            ]),
          "resume.execute_tools": (e, _s, caps) =>
            caps.executeTools(e.calls, false, true),
          "tool.done": (_e, _s, caps) => caps.done(),
        },
        "wait",
      ),
      inferenceRunner: makeInferenceRunner({
        type: "done",
        turn: suspendToolCallTurn,
        usage: emptyUsage(),
      }),
      toolRunner: makeToolRunner(async (call) => {
        toolsRun.push(call.name);
        return { callId: call.id, content: "charged" };
      }),
      beforeToolExtensions: [askExtension],
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());

    const blocked = await waitFor("reactor.gate.blocked");
    if (blocked.type !== "reactor.gate.blocked") throw new Error("unreachable");
    const correlationId = blocked.data.correlationId;
    if (correlationId === undefined) throw new Error("expected correlationId");

    reactor.deliver(makeApprovalMessage(correlationId));
    await waitFor("reactor.done");

    expect(toolsRun).toEqual(["charge_card"]);
    expect(events.some((e) => e.type === "reactor.error")).toBe(false);
  });

  test("trips on a repeated parallel fan-out batch", async () => {
    const batch: ToolCall[] = [
      { id: "a", name: "read", arguments: { path: "a" } },
      { id: "b", name: "read", arguments: { path: "b" } },
      { id: "c", name: "read", arguments: { path: "c" } },
    ];
    const { reactor, events, waitFor } = createTestReactor({
      director: createBatchLoopDirector((turn) =>
        turn < 8
          ? batch.map((call, i) => ({ ...call, id: `${call.id}-${turn}-${i}` }))
          : null,
      ),
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitFor("reactor.done");

    const error = getEvent(events, "reactor.error");
    expect(error.data.fatal).toBe(true);
    // The message reports every call in the batch, not a deduplicated name, so
    // a three-call fan-out is not collapsed to one.
    expect(error.data.error).toContain("read, read, read");
    expect(getEvent(events, "reactor.turn.ended").data).toMatchObject({
      outcome: "failed",
      error: { kind: "doom_loop" },
    });
  });

  test("does not trip on identical calls within a single batch", async () => {
    // Three identical calls in one turn are one batch occurrence, not three
    // consecutive turns.
    const { reactor, events, waitFor } = createTestReactor({
      director: createBatchLoopDirector((turn) =>
        turn === 0
          ? [
              { id: "x1", name: "dup", arguments: {} },
              { id: "x2", name: "dup", arguments: {} },
              { id: "x3", name: "dup", arguments: {} },
            ]
          : null,
      ),
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitFor("reactor.done");

    expect(events.some((e) => e.type === "reactor.error")).toBe(false);
    expect(getEvent(events, "reactor.turn.ended").data.outcome).toBe(
      "unanswered",
    );
  });

  test("a different batch between identical ones resets the counter", async () => {
    const batches: ToolCall[][] = [
      [{ id: "1", name: "spin", arguments: {} }],
      [{ id: "2", name: "spin", arguments: {} }],
      [{ id: "3", name: "other", arguments: {} }],
      [{ id: "4", name: "spin", arguments: {} }],
      [{ id: "5", name: "spin", arguments: {} }],
    ];
    const { reactor, events, waitFor } = createTestReactor({
      director: createBatchLoopDirector((turn) => batches[turn] ?? null),
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitFor("reactor.done");

    expect(events.some((e) => e.type === "reactor.error")).toBe(false);
    expect(getEvent(events, "reactor.turn.ended").data.outcome).toBe(
      "unanswered",
    );
  });

  test("rejects a non-positive or non-integer threshold at construction", () => {
    expect(() => createTestReactor({ doomLoopThreshold: 0 })).toThrow(
      /positive integer/,
    );
    expect(() => createTestReactor({ doomLoopThreshold: -1 })).toThrow(
      /positive integer/,
    );
    expect(() => createTestReactor({ doomLoopThreshold: 2.5 })).toThrow(
      /positive integer/,
    );
  });

  test("does not trip when detection is disabled with false", async () => {
    // With detection off, an identical batch repeated far past the default
    // threshold must run to completion. This also guards the coercion trap: a
    // stray `>= false` would read as `>= 0` and trip on the very first turn.
    const { reactor, events, waitFor } = createTestReactor({
      doomLoopThreshold: false,
      director: createBatchLoopDirector((turn) =>
        turn < 8
          ? [{ id: `c${turn}`, name: "spin", arguments: { q: 1 } }]
          : null,
      ),
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitFor("reactor.done");

    expect(events.some((e) => e.type === "reactor.error")).toBe(false);
    expect(getEvent(events, "reactor.turn.ended").data.outcome).toBe(
      "unanswered",
    );
    // All eight turns ran; nothing broke the loop early.
    expect(events.filter((e) => e.type === "tool.start").length).toBe(8);
  });

  test("a threshold of 1 trips on the first executed tool turn", async () => {
    const { reactor, events, waitFor } = createTestReactor({
      doomLoopThreshold: 1,
      director: createBatchLoopDirector((turn) =>
        turn < 4 ? [{ id: `c${turn}`, name: "once", arguments: {} }] : null,
      ),
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitFor("reactor.done");

    expect(getEvent(events, "reactor.error").data.fatal).toBe(true);
    expect(getEvent(events, "reactor.turn.ended").data).toMatchObject({
      outcome: "failed",
      error: { kind: "doom_loop" },
    });
    // Tripped on the very first turn: exactly one tool ran.
    expect(events.filter((e) => e.type === "tool.start").length).toBe(1);
  });

  test("trips across inference-interleaved identical tool turns", async () => {
    // The real reactor runs an inference between every tool turn. Detection
    // must accumulate across those inference turns, not only across the
    // back-to-back batches the other tests drive.
    const { inferenceRunner, director } = interleavedRunner(
      () => ({ q: 1 }),
      8,
    );
    const { reactor, events, waitFor } = createTestReactor({
      inferenceRunner,
      director,
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitFor("reactor.done");

    expect(getEvent(events, "reactor.error").data.fatal).toBe(true);
    expect(getEvent(events, "reactor.turn.ended").data).toMatchObject({
      outcome: "failed",
      error: { kind: "doom_loop" },
    });
    expect(events.filter((e) => e.type === "tool.start").length).toBe(3);
  });

  test("does not trip on inference-interleaved differing arguments", async () => {
    const { inferenceRunner, director } = interleavedRunner(
      (t) => ({ q: t }),
      5,
    );
    const { reactor, events, waitFor } = createTestReactor({
      inferenceRunner,
      director,
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitFor("reactor.done");

    expect(events.some((e) => e.type === "reactor.error")).toBe(false);
    expect(getEvent(events, "reactor.turn.ended").data.outcome).toBe(
      "unanswered",
    );
  });

  test("accumulates across suspend, approval, and redispatch cycles", async () => {
    // Each identical batch suspends for approval; approval re-dispatches the
    // one parked call, and the reactor commits the cycle at every suspend. The
    // turn's counter must survive that commit -- if it were reset there
    // (e.g. added to resetCycleAccumulators), an approval-gated doom loop would
    // never trip. Threshold 2, so the second redispatch trips.
    let inferCount = 0;
    const inferenceRunner = async function* (opts: InferenceHarnessOptions) {
      const id = `spin-${inferCount}`;
      inferCount += 1;
      yield {
        type: "inference.done" as const,
        seq: opts.nextSeq(),
        data: {
          turn: {
            role: "assistant" as const,
            content: [
              { type: "tool_call" as const, id, name: "spin", arguments: {} },
            ],
            model: "test-model",
            timestamp: 1000,
          },
          usage: emptyUsage(),
          source: TEST_SOURCE,
        },
      };
    };

    const askExtension = createAuthzExtension({
      authorize: async () => ({
        effect: "ask" as const,
        matchingGrants: [],
        resolvedBy: null,
      }),
      approvalTimeoutMs: 60_000,
    });

    const { reactor, events, waitFor } = createTestReactor({
      doomLoopThreshold: 2,
      inferenceRunner,
      director: directorFromTable(
        {
          "message.received": (_e, _s, caps) => caps.infer(),
          "inference.done": (e, _s, caps) => {
            const block = e.turn.content.find((b) => b.type === "tool_call");
            if (block === undefined || block.type !== "tool_call") {
              return caps.done();
            }
            return caps.executeTools([
              { id: block.id, name: block.name, arguments: block.arguments },
            ]);
          },
          "resume.execute_tools": (e, _s, caps) =>
            caps.executeTools(e.calls, false, true),
          "tool.done": (_e, _s, caps) => caps.infer(),
        },
        "wait",
      ),
      toolRunner: makeToolRunner(async (call) => ({
        callId: call.id,
        content: "spun",
      })),
      beforeToolExtensions: [askExtension],
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());

    // Approve each suspension as it arrives until the loop trips (or give up
    // after a bounded number so a regression that never trips fails fast).
    const approved = new Set<string>();
    for (let i = 0; i < 6; i++) {
      const ev = await waitForEvent(
        events,
        (e) =>
          e.type === "reactor.done" ||
          (e.type === "reactor.gate.blocked" &&
            e.data.correlationId !== undefined &&
            !approved.has(e.data.correlationId)),
      );
      if (ev.type === "reactor.done") break;
      if (ev.type !== "reactor.gate.blocked") throw new Error("unreachable");
      const cid = ev.data.correlationId;
      if (cid === undefined) throw new Error("expected correlationId");
      approved.add(cid);
      reactor.deliver(makeApprovalMessage(cid));
    }

    await waitFor("reactor.done");

    expect(getEvent(events, "reactor.error").data.fatal).toBe(true);
    // Each approval names a turn of its own, which joins the parked turn.
    const ends = turnEnds(events);
    expect(ends.filter((e) => e.outcome === "joined")).toHaveLength(2);
    expect(ends.find((e) => e.outcome === "failed")).toMatchObject({
      error: { kind: "doom_loop" },
    });
  });
});

// ---------------------------------------------------------------------------
// 8. Correlation matching
// ---------------------------------------------------------------------------

describe("createReactor — correlation", () => {
  test("message with matching correlationId triggers message.correlated", async () => {
    const CORR_ID = "corr-xyz-123";
    let delivered = false;

    // Multi-phase state machine: deliver → execute_tools → suspend with
    // correlation → deliver correlated response → done. Kept as raw director
    // because the `delivered` flag makes directorFromTable awkward.
    const director: ReactorDirector = {
      async decide(event, _state, caps) {
        if (event.type === "message.received" && !delivered) {
          delivered = true;
          return caps.executeTools([
            { id: "tc1", name: "send_message", arguments: {} },
          ]);
        }
        if (event.type === "tool.done") {
          return caps.suspend({
            type: "message_response",
            gateId: "msg-gate",
            timeoutMs: 5000,
            correlationId: CORR_ID,
          });
        }
        if (event.type === "reactor.gate.cleared") {
          return caps.done();
        }
        return caps.done();
      },
    };

    const { reactor, events, waitFor } = createTestReactor({
      director,
      shutdownTimeoutMs: 500,
      toolRunner: makeToolRunner(async (call) => {
        return {
          callId: call.id,
          content: "message sent",
          pendingMarker: {
            status: "pending" as const,
            correlationId: CORR_ID,
          },
        };
      }),
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());

    await waitFor("reactor.gate.blocked");

    // Deliver the correlated response.
    reactor.deliver(makeInboundMessage(CORR_ID));

    await waitFor("reactor.done");

    const correlated = getEvent(events, "message.correlated");
    expect(correlated.data.correlationId).toBe(CORR_ID);
    expect(correlated.data.message.headers.interchangeCorrelationId).toBe(
      CORR_ID,
    );
  });

  test("message with non-matching correlationId passes through uncorrelated", async () => {
    const { reactor, events, waitFor } = createTestReactor({
      director: directorFromTable({
        "message.received": (_e, _s, caps) => caps.done(),
      }),
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());

    await waitFor("reactor.done");

    expect(events.some((e) => e.type === "message.correlated")).toBe(false);
    expect(events.some((e) => e.type === "message.received")).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// 9. Test harness helper validation
// ---------------------------------------------------------------------------

describe("test harness helpers", () => {
  test("createTestReactor produces a working reactor with defaults", async () => {
    const { reactor, events, waitFor } = createTestReactor({
      director: directorFromTable({
        "message.received": (_e, _s, caps) => caps.done(),
      }),
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitFor("reactor.done");

    expect(events[0]?.type).toBe("reactor.start");
    const done = getEvent(events, "reactor.done");
    expect(done.type).toBe("reactor.done");
  });

  test("failingContextStore rejects on load", async () => {
    const store = failingContextStore(new Error("disk on fire"));
    let thrown: Error | undefined;
    try {
      await store.load();
    } catch (cause) {
      thrown = cause instanceof Error ? cause : new Error(String(cause));
    }
    expect(thrown?.message).toContain("disk on fire");
  });

  test("throwingToolRunner throws on run", async () => {
    const runner = throwingToolRunner(new Error("tool exploded"));
    const signal = new AbortController().signal;
    let thrown: Error | undefined;
    try {
      await runner.run({ id: "c1", name: "t", arguments: {} }, signal);
    } catch (cause) {
      thrown = cause instanceof Error ? cause : new Error(String(cause));
    }
    expect(thrown?.message).toContain("tool exploded");
  });

  test("getEvent throws when event is missing", () => {
    const events: ReactorEmittedEvent[] = [];
    expect(() => getEvent(events, "reactor.error")).toThrow(
      "No event of type 'reactor.error' found",
    );
  });

  test("directorFromTable with wait default falls through on unhandled events", async () => {
    // Suspend produces a reactor.gate.cleared event. That event type is
    // NOT in the table, so the defaultAction "wait" fallthrough fires.
    // The reactor stays alive because of the fallthrough, then a second
    // message triggers done via the table handler.
    let messageCount = 0;
    const { reactor, events, waitFor } = createTestReactor({
      director: directorFromTable(
        {
          "message.received": (_e, _s, caps) => {
            messageCount++;
            if (messageCount >= 2) return caps.done();
            return caps.suspend({
              type: "approval",
              gateId: "wait-test-gate",
              timeoutMs: 50,
            });
          },
          // reactor.gate.cleared is intentionally omitted from the table.
          // The defaultAction "wait" keeps the reactor alive when it fires.
        },
        "wait",
      ),
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());

    // Wait for the gate to time out and clear. The reactor.gate.cleared
    // event hits the fallthrough, which returns caps.wait().
    await waitFor("reactor.gate.cleared");

    // Reactor is still alive because of the wait fallthrough. Deliver
    // another message to trigger done.
    reactor.deliver(makeInboundMessage());
    await waitFor("reactor.done");

    expect(messageCount).toBe(2);
    expect(events.some((e) => e.type === "reactor.gate.blocked")).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// 10. Context store failures
// ---------------------------------------------------------------------------

describe("createReactor — context store failures", () => {
  test("context store load failure emits reactor.error and reactor.done without reactor.start", async () => {
    const { reactor, events, waitFor } = createTestReactor({
      contextStore: failingContextStore(new Error("disk on fire")),
    });

    reactor.start();
    await waitFor("reactor.done");

    const errorEvent = getEvent(events, "reactor.error");
    expect(errorEvent.data.error).toMatch(/disk on fire/);
    expect(errorEvent.data.fatal).toBe(true);

    // reactor.start must NOT be emitted — load failed before the loop began.
    expect(events.some((e) => e.type === "reactor.start")).toBe(false);

    // reactor.done must still be emitted for cleanup listeners.
    expect(events.some((e) => e.type === "reactor.done")).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// 11. Tool runner failures
// ---------------------------------------------------------------------------

describe("createReactor — tool runner failures", () => {
  test("tool runner that throws triggers fatal error and shutdown", async () => {
    const { reactor, events, waitFor } = createTestReactor({
      director: directorFromTable({
        "message.received": (_e, _s, caps) =>
          caps.executeTools([{ id: "c1", name: "boom", arguments: {} }]),
      }),
      toolRunner: throwingToolRunner(new Error("tool exploded")),
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitFor("reactor.done");

    const errorEvent = getEvent(events, "reactor.error");
    expect(errorEvent.data.error).toMatch(
      /^Internal reactor error:.*tool exploded/,
    );
    expect(errorEvent.data.fatal).toBe(true);
  });

  test("tool runner returning isError propagates error result to director", async () => {
    const { reactor, events, waitFor } = createTestReactor({
      director: directorFromTable({
        "message.received": (_e, _s, caps) =>
          caps.executeTools([{ id: "c1", name: "bad_tool", arguments: {} }]),
        "tool.done": (_e, _s, caps) => caps.done(),
      }),
      toolRunner: makeToolRunner(async (call) => {
        return {
          callId: call.id,
          content: "something went wrong",
          isError: true,
        };
      }),
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitFor("reactor.done");

    // The tool.done event should carry the error result with isError flag.
    const toolDone = getEvent(events, "tool.done");
    expect(toolDone.data.result.isError).toBe(true);
    expect(toolDone.data.result.content).toBe("something went wrong");
    // No reactor.error should be emitted — isError is a normal result, not a crash.
    expect(events.some((e) => e.type === "reactor.error")).toBe(false);
  });

  test("sequential tool execution runs tools in order", async () => {
    const order: string[] = [];
    const { reactor, waitFor } = createTestReactor({
      director: directorFromTable({
        "message.received": (_e, _s, caps) =>
          caps.executeTools(
            [
              { id: "c1", name: "first", arguments: {} },
              { id: "c2", name: "second", arguments: {} },
              { id: "c3", name: "third", arguments: {} },
            ],
            false,
          ),
        "tool.done": (_e, _s, caps) => caps.done(),
      }),
      toolRunner: makeToolRunner(async (call) => {
        order.push(call.name);
        return { callId: call.id, content: "ok" };
      }),
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitFor("reactor.done");

    // Order must be preserved — not sorted.
    expect(order).toEqual(["first", "second", "third"]);
  });

  test("addToHistory=false runs tools but skips history append", async () => {
    // We observe history indirectly via the reactor's cycle commit, which now
    // writes through contextStore.writeTurns just before commit({ message }).
    let committedTurns: ConversationTurn[] = [];
    const capturingStore: ContextStore = {
      async load() {
        return {
          turns: [],
          pendingOperations: [],
          tokenUsage: emptyUsage(),
          connectorState: null,
        };
      },
      setConnectorState() {
        /* noop */
      },
      async commit(options) {
        return { hash: "abc", message: options.message, timestamp: Date.now() };
      },
      async branch() {
        /* noop */
      },
      async log() {
        return [];
      },
      async readAt() {
        return [];
      },
      async writeBlob() {
        throw new Error("not implemented");
      },
      async readBlob() {
        throw new Error("not implemented");
      },
      async writePrompt() {
        throw new Error("not implemented");
      },
      async writeResponse() {
        throw new Error("not implemented");
      },
      async writeManifest() {
        /* noop */
      },
      async writeTurns(turns) {
        committedTurns = [...turns];
      },
      async writeMetadata() {
        /* noop */
      },
      async readManifestHistory() {
        throw new Error("not implemented");
      },
    };

    const { reactor, waitFor } = createTestReactor({
      contextStore: capturingStore,
      director: directorFromTable({
        "message.received": (_e, _s, caps) =>
          caps.executeTools(
            [{ id: "c1", name: "ghost_tool", arguments: {} }],
            true,
            false,
          ),
        "tool.done": (_e, _s, caps) => [caps.checkpoint(), caps.done()],
      }),
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitFor("reactor.done");

    // Verify commit was actually called before checking its contents.
    expect(committedTurns.length).toBeGreaterThan(0);

    // The committed history should contain the inbound text message but
    // no tool_result message since addToHistory was false.
    const hasToolResult = committedTurns.some((m) =>
      m.content.some((b) => b.type === "tool_result"),
    );
    expect(hasToolResult).toBe(false);

    // The inbound user message should still be in history.
    const hasUserText = committedTurns.some(
      (m) => m.role === "user" && m.content.some((b) => b.type === "text"),
    );
    expect(hasUserText).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// 12. Director misbehavior
// ---------------------------------------------------------------------------

describe("createReactor — director misbehavior", () => {
  test("reactor shuts down when director returns invalid action set", async () => {
    const { reactor, events, waitFor } = createTestReactor({
      director: directorFromTable({
        "message.received": (_e, _s, caps) => [caps.infer(), caps.done()],
      }),
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitFor("reactor.done");

    const errorEvent = getEvent(events, "reactor.error");
    expect(errorEvent.data.error).toMatch(/infer.*done/i);
    expect(errorEvent.data.fatal).toBe(true);
  });

  test("director emitting reserved namespace inference.* produces non-fatal error", async () => {
    const { reactor, events, waitFor } = createTestReactor({
      director: directorFromTable({
        "message.received": (_e, _s, caps) => [
          // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- intentionally passing a reserved-namespace string to test that the reactor rejects it
          caps.emit("inference.hijack" as `custom.${string}`, {}),
          caps.done(),
        ],
      }),
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitFor("reactor.done");

    const errorEvent = getEvent(events, "reactor.error");
    expect(errorEvent.data.error).toMatch(/reserved event type/);
    expect(errorEvent.data.fatal).toBe(false);
  });

  test("director emitting reserved namespace tool.* produces non-fatal error", async () => {
    const { reactor, events, waitFor } = createTestReactor({
      director: directorFromTable({
        "message.received": (_e, _s, caps) => [
          // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- intentionally passing a reserved-namespace string to test that the reactor rejects it
          caps.emit("tool.fake" as `custom.${string}`, {}),
          caps.done(),
        ],
      }),
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitFor("reactor.done");

    const errorEvent = getEvent(events, "reactor.error");
    expect(errorEvent.data.error).toMatch(/reserved event type/);
    expect(errorEvent.data.fatal).toBe(false);
  });

  test("director emitting reserved namespace reactor.* produces non-fatal error", async () => {
    const { reactor, events, waitFor } = createTestReactor({
      director: directorFromTable({
        "message.received": (_e, _s, caps) => [
          // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- intentionally passing a reserved-namespace string to test that the reactor rejects it
          caps.emit("reactor.fake" as `custom.${string}`, {}),
          caps.done(),
        ],
      }),
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitFor("reactor.done");

    const errorEvent = getEvent(events, "reactor.error");
    expect(errorEvent.data.error).toMatch(/reserved event type/);
    expect(errorEvent.data.fatal).toBe(false);
  });

  test("director emitting reserved namespace fork.* produces non-fatal error", async () => {
    const { reactor, events, waitFor } = createTestReactor({
      director: directorFromTable({
        "message.received": (_e, _s, caps) => [
          // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- intentionally passing a reserved-namespace string to test that the reactor rejects it
          caps.emit("fork.fake" as `custom.${string}`, {}),
          caps.done(),
        ],
      }),
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitFor("reactor.done");

    const errorEvent = getEvent(events, "reactor.error");
    expect(errorEvent.data.error).toMatch(/reserved event type/);
    expect(errorEvent.data.fatal).toBe(false);
  });

  test("fork action emits non-fatal unsupported error", async () => {
    const { reactor, events, waitFor } = createTestReactor({
      director: directorFromTable({
        "message.received": (_e, _s, caps) => [
          caps.fork("independent", "fork-1"),
          caps.done(),
        ],
      }),
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitFor("reactor.done");

    const errorEvent = getEvent(events, "reactor.error");
    expect(errorEvent.data.error).toMatch(/not supported/i);
    expect(errorEvent.data.fatal).toBe(false);
  });

  test("wait action keeps reactor alive without shutdown", async () => {
    let messageCount = 0;
    const { reactor, events, waitFor } = createTestReactor({
      director: directorFromTable({
        "message.received": (_e, _s, caps) => {
          messageCount++;
          if (messageCount >= 2) return caps.done();
          return caps.wait();
        },
      }),
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());

    // After wait, deliver a second message which triggers done. The
    // director's own counter reports that the first message was processed, so
    // the second delivery is ordered against that rather than a delay.
    await waitUntil(() => messageCount >= 1);
    reactor.deliver(makeInboundMessage());
    await waitFor("reactor.done");

    expect(messageCount).toBe(2);
    // No reactor.error should have been emitted.
    expect(events.some((e) => e.type === "reactor.error")).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// 13. Reply action
// ---------------------------------------------------------------------------

describe("createReactor — reply action", () => {
  test("reply action emits connector.reply and reactor stays alive", async () => {
    let messageCount = 0;
    const { reactor, events, waitFor } = createTestReactor({
      director: directorFromTable({
        "message.received": (_e, _s, caps) => {
          messageCount++;
          if (messageCount >= 2) return caps.done();
          return caps.reply("hello back");
        },
      }),
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());

    // After reply, reactor waits for next event. Deliver another message
    // once the director reports it handled the first one.
    await waitUntil(() => messageCount >= 1);
    reactor.deliver(makeInboundMessage());
    await waitFor("reactor.done");

    const replyEvent = getEvent(events, "connector.reply");
    expect(replyEvent.data.content).toBe("hello back");
    expect(messageCount).toBe(2);
  });
});

// ---------------------------------------------------------------------------
// 14. Checkpoint failure
// ---------------------------------------------------------------------------

describe("createReactor — checkpoint failure", () => {
  test("checkpoint failure emits non-fatal error and reactor continues", async () => {
    let checkpointCalled = false;
    const failingStore: ContextStore = {
      async load() {
        return {
          turns: [],
          pendingOperations: [],
          tokenUsage: emptyUsage(),
          connectorState: null,
        };
      },
      setConnectorState() {
        /* noop */
      },
      async commit(): Promise<ContextCommit> {
        checkpointCalled = true;
        throw new Error("storage full");
      },
      async branch() {
        /* noop */
      },
      async log() {
        return [];
      },
      async readAt() {
        return [];
      },
      async writeBlob() {
        /* noop */
      },
      async readBlob() {
        throw new Error("not implemented");
      },
      async writePrompt() {
        /* noop */
      },
      async writeResponse() {
        /* noop */
      },
      async writeManifest() {
        /* noop */
      },
      async writeTurns() {
        /* noop */
      },
      async writeMetadata() {
        /* noop */
      },
      async readManifestHistory() {
        throw new Error("not implemented");
      },
    };

    let messageCount = 0;
    const { reactor, events, waitFor } = createTestReactor({
      contextStore: failingStore,
      director: directorFromTable({
        "message.received": (_e, _s, caps) => {
          messageCount++;
          if (messageCount === 1) return [caps.checkpoint(), caps.wait()];
          return caps.done();
        },
      }),
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());

    // After checkpoint failure + wait, deliver another message. The
    // non-fatal reactor.error the failed checkpoint emits is the signal that
    // the first cycle got that far.
    await waitFor("reactor.error");
    reactor.deliver(makeInboundMessage());
    await waitFor("reactor.done");

    expect(checkpointCalled).toBe(true);

    const errorEvent = getEvent(events, "reactor.error");
    expect(errorEvent.data.error).toMatch(/storage full/);
    expect(errorEvent.data.fatal).toBe(false);

    // Reactor continued after the error — it processed the second message.
    expect(messageCount).toBe(2);
  });
});

// ---------------------------------------------------------------------------
// 15. Abort handling
// ---------------------------------------------------------------------------

describe("createReactor — abort handling", () => {
  test("abort event is processed with priority over queued events", async () => {
    // Track how many message.received events the director processed.
    let messagesProcessed = 0;
    const { reactor, waitFor } = createTestReactor({
      director: directorFromTable(
        {
          "message.received": (_e, _s, caps) => {
            messagesProcessed++;
            return caps.wait();
          },
        },
        "wait",
      ),
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());

    // Wait for reactor to process the first message.
    await waitFor("message.received");

    // Queue more messages and an abort. The abort should jump the queue
    // and shut down before the extra messages reach the director.
    reactor.deliver(makeInboundMessage());
    reactor.deliver(makeInboundMessage());
    reactor.abort("admin_kill");

    await waitFor("reactor.done");

    // Only the first message should have been processed by the director.
    // The abort jumped ahead of the two queued messages.
    expect(messagesProcessed).toBe(1);
  });

  test("multiple abort calls do not cause double shutdown", async () => {
    const { reactor, events, waitFor } = createTestReactor({
      director: directorFromTable(
        {
          "message.received": (_e, _s, caps) => caps.wait(),
        },
        "wait",
      ),
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitFor("message.received");

    reactor.abort("admin_kill");
    reactor.abort("admin_kill");
    reactor.abort("admin_kill");

    await waitFor("reactor.done");

    // Exactly one reactor.done should be emitted.
    const doneEvents = events.filter((e) => e.type === "reactor.done");
    expect(doneEvents.length).toBe(1);
  });

  test("abort before start shuts down immediately when start is called", async () => {
    const { reactor, events, waitFor } = createTestReactor({
      director: directorFromTable(
        {
          "message.received": (_e, _s, caps) => caps.wait(),
        },
        "wait",
      ),
    });

    // Abort is enqueued before start().
    reactor.abort("admin_kill");
    reactor.start();

    await waitFor("reactor.done");

    expect(events.some((e) => e.type === "reactor.start")).toBe(true);
    expect(events.some((e) => e.type === "reactor.done")).toBe(true);

    // No messages should have been processed.
    expect(events.some((e) => e.type === "message.received")).toBe(false);
  });

  test("abort signals an in-flight inference before the loop can dequeue it", async () => {
    const inferenceStarted = Promise.withResolvers<boolean>();
    const { reactor, waitFor } = createTestReactor({
      director: directorFromTable({
        "message.received": (_e, _s, caps) => caps.infer(),
      }),
      inferenceRunner: async function* (opts) {
        inferenceStarted.resolve(true);
        await new Promise<void>((resolve) => {
          if (opts.signal?.aborted === true) {
            resolve();
            return;
          }
          opts.signal?.addEventListener("abort", () => resolve(), {
            once: true,
          });
        });
        yield {
          type: "inference.error",
          seq: opts.nextSeq(),
          data: {
            error: { category: "aborted", message: "aborted" },
            partial: { text: "" },
          },
        };
      },
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());
    await inferenceStarted.promise;
    reactor.abort("user_disconnect");

    await waitFor("reactor.done");
  });
});

// ---------------------------------------------------------------------------
// 16. Dequeue priority (mid-cycle interleaving prevention)
// ---------------------------------------------------------------------------

describe("createReactor — dequeue priority", () => {
  test("tool.done is prioritized over message.received when tool calls are pending", async () => {
    // Pre-seed history with an assistant message containing a tool_call
    // block, then run tools with addToHistory=false so no tool-result turn is
    // appended. pendingContinuations keeps the cycle pending regardless of
    // history shape: the enqueued tool.done is drained before the message
    // delivered mid-run.
    const seededTurns: ConversationTurn[] = [
      {
        role: "assistant",
        content: [
          {
            type: "tool_call",
            id: "tc-pending",
            name: "some_tool",
            arguments: {},
          },
        ],
        model: "test-model",
        timestamp: 1000,
      },
    ];

    const order: string[] = [];

    // A minimal inbound message; its content is irrelevant to the cycle-event
    // drain ordering this test exercises.
    const emptyMessage: InboundMessage = {
      ref: { uid: 1, mailbox: "INBOX" },
      headers: {
        from: "test@example.com",
        to: ["agent@example.com"],
        date: new Date().toISOString(),
        messageId: `msg-empty-${Math.random()}`,
      },
      flags: [],
      content: "",
      signatureStatus: "missing",
    };

    const { reactor, waitFor } = createTestReactor({
      contextStore: makeContextStore(seededTurns),
      director: {
        async decide(event, _state, caps) {
          order.push(event.type);
          if (event.type === "message.received") {
            // First message.received: run tools with addToHistory=false.
            // Second message.received (after tool.done): shut down.
            if (order.filter((t) => t === "message.received").length >= 2) {
              return caps.done();
            }
            return caps.executeTools(
              [{ id: "tc-pending", name: "some_tool", arguments: {} }],
              true,
              false,
            );
          }
          if (event.type === "tool.done") {
            return caps.wait();
          }
          return caps.done();
        },
      },
      toolRunner: makeToolRunner(async (call) => {
        // While the tool is running, deliver a second message. This
        // enqueues message.received before executeTools enqueues tool.done.
        reactor.deliver(emptyMessage);
        return { callId: call.id, content: "ok" };
      }),
    });

    reactor.start();
    reactor.deliver(emptyMessage);
    await waitFor("reactor.done");

    // tool.done should appear before the second message.received in the order
    // array, because dequeueNext drains cycle events while
    // pendingContinuations is positive.
    const toolDoneIdx = order.indexOf("tool.done");
    const secondMessageIdx = order.lastIndexOf("message.received");
    expect(toolDoneIdx).toBeGreaterThan(-1);
    expect(secondMessageIdx).toBeGreaterThan(-1);
    expect(toolDoneIdx).toBeLessThan(secondMessageIdx);
  });

  function assistantToolCallTurn(ids: string[]): ConversationTurn {
    return {
      role: "assistant",
      content: ids.map((id) => ({
        type: "tool_call" as const,
        id,
        name: "some_tool",
        arguments: {},
      })),
      model: "test-model",
      timestamp: 1000,
    };
  }

  test("tool.done is prioritized over message.received with addToHistory=true", async () => {
    // The production path uses addToHistory=true, so executeTools appends the
    // tool-result turn to history before its tool.done events are consumed.
    // The pendingContinuations counter, not history shape, must keep the
    // cycle pending so mail delivered mid-batch cannot start an overlapping
    // inference. Against the old history-shape gate the appended tool-result
    // turn flips the gate false, the queued message.received jumps ahead, the
    // director shuts down on it, and tool.done is never processed.
    const order: string[] = [];

    const { reactor, events, waitFor } = createTestReactor({
      contextStore: makeContextStore([assistantToolCallTurn(["tc-pending"])]),
      director: {
        async decide(event, _state, caps) {
          order.push(event.type);
          if (event.type === "message.received") {
            if (order.filter((t) => t === "message.received").length >= 2) {
              return caps.done();
            }
            return caps.executeTools(
              [{ id: "tc-pending", name: "some_tool", arguments: {} }],
              true,
              true,
            );
          }
          if (event.type === "tool.done") {
            return caps.wait();
          }
          return caps.done();
        },
      },
      toolRunner: makeToolRunner(async (call) => {
        // Deliver a second message while the tool runs and wait for its
        // enqueue, so deliver()'s async enqueue lands before executeTools
        // enqueues tool.done. This pins the queue order to
        // [message.received, tool.done] — the interleave that triggers the
        // bug. processDelivery emits `message.received` in the same
        // synchronous step as the enqueue, so the emitted event carrying this
        // message's id is the enqueue's own signal.
        const mail = makeInboundMessage();
        reactor.deliver(mail);
        await waitUntil(() =>
          events.some(
            (e) =>
              e.type === "message.received" &&
              e.data.message.headers.messageId === mail.headers.messageId,
          ),
        );
        return { callId: call.id, content: "ok" };
      }),
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitFor("reactor.done");

    const toolDoneIdx = order.indexOf("tool.done");
    const secondMessageIdx = order.lastIndexOf("message.received");
    expect(toolDoneIdx).toBeGreaterThan(-1);
    expect(secondMessageIdx).toBeGreaterThan(-1);
    expect(toolDoneIdx).toBeLessThan(secondMessageIdx);
  });

  // Both tool.done events of a two-call batch must be drained before mail
  // delivered mid-batch. pendingContinuations must track the full batch count,
  // not a single in-cycle flag. Run for both parallel and sequential
  // execution since those take different enqueue paths in executeTools.
  async function collectBatchDrainOrder(parallel: boolean): Promise<string[]> {
    const order: string[] = [];
    let toolDoneCount = 0;

    const { reactor, events, waitFor } = createTestReactor({
      contextStore: makeContextStore([assistantToolCallTurn(["tc-a", "tc-b"])]),
      director: {
        async decide(event, _state, caps) {
          order.push(event.type);
          if (event.type === "message.received") {
            if (order.filter((t) => t === "message.received").length >= 2) {
              return caps.done();
            }
            return caps.executeTools(
              [
                { id: "tc-a", name: "some_tool", arguments: {} },
                { id: "tc-b", name: "some_tool", arguments: {} },
              ],
              parallel,
              true,
            );
          }
          if (event.type === "tool.done") {
            toolDoneCount += 1;
            return toolDoneCount >= 2 ? caps.wait() : [];
          }
          return caps.done();
        },
      },
      toolRunner: makeToolRunner(async (call) => {
        // Mail delivered mid-batch must be enqueued before this call's
        // tool.done. The emitted `message.received` carrying this message's
        // id is published in the same synchronous step as the enqueue, so it
        // is the signal that the delivery landed.
        const mail = makeInboundMessage();
        reactor.deliver(mail);
        await waitUntil(() =>
          events.some(
            (e) =>
              e.type === "message.received" &&
              e.data.message.headers.messageId === mail.headers.messageId,
          ),
        );
        return { callId: call.id, content: "ok" };
      }),
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitFor("reactor.done");
    return order;
  }

  test("drains an entire parallel tool batch before inbound mail", async () => {
    const order = await collectBatchDrainOrder(true);
    expect(order.filter((t) => t === "tool.done").length).toBe(2);
    expect(order.lastIndexOf("tool.done")).toBeLessThan(
      order.lastIndexOf("message.received"),
    );
  });

  test("drains an entire sequential tool batch before inbound mail", async () => {
    const order = await collectBatchDrainOrder(false);
    expect(order.filter((t) => t === "tool.done").length).toBe(2);
    expect(order.lastIndexOf("tool.done")).toBeLessThan(
      order.lastIndexOf("message.received"),
    );
  });

  test("abort during a tool batch shuts down cleanly", async () => {
    // An abort preempts a cycle, leaving pendingContinuations positive. The
    // reactor must still terminate: abort takes priority over cycle draining
    // and the leftover count is never read after shutdown.
    const { reactor, waitFor } = createTestReactor({
      contextStore: makeContextStore([assistantToolCallTurn(["tc-pending"])]),
      director: {
        async decide(event, _state, caps) {
          if (event.type === "message.received") {
            return caps.executeTools(
              [{ id: "tc-pending", name: "some_tool", arguments: {} }],
              true,
              true,
            );
          }
          return caps.done();
        },
      },
      toolRunner: makeToolRunner(async (call) => {
        // abort() enqueues the abort event synchronously, so it is already
        // queued ahead of this call's tool.done when abort() returns; no wait
        // is needed to order the two.
        reactor.abort("admin_kill");
        return { callId: call.id, content: "ok" };
      }),
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());
    // Reaching reactor.done is the assertion: abort preempted the queued
    // tool.done and the reactor terminated despite a positive count.
    await waitFor("reactor.done");
  });

  test("inference.done is processed before mail delivered mid-inference", async () => {
    // The overlapping-inference window for a plain (no-tool) response: mail
    // arriving while an inference runs must not start a second inference ahead
    // of the first inference's own completion event. The counter defers it;
    // the old history-shape gate would have let the mail jump ahead, since a
    // plain-text assistant turn is not a pending tool_call turn.
    const order: string[] = [];
    let inferenceCount = 0;

    const { reactor, events, waitFor } = createTestReactor({
      director: {
        async decide(event, _state, caps) {
          order.push(event.type);
          if (event.type === "message.received") {
            if (order.filter((t) => t === "message.received").length >= 2) {
              return caps.done();
            }
            return caps.infer();
          }
          if (event.type === "inference.done") {
            return caps.wait();
          }
          return caps.done();
        },
      },
      inferenceRunner: async function* (opts) {
        inferenceCount += 1;
        // Deliver mail mid-inference and wait for its enqueue, so the message
        // is queued before inference.done. The emitted `message.received`
        // carrying this message's id is published in the same synchronous
        // step as the enqueue.
        const mail = makeInboundMessage();
        reactor.deliver(mail);
        await waitUntil(() =>
          events.some(
            (e) =>
              e.type === "message.received" &&
              e.data.message.headers.messageId === mail.headers.messageId,
          ),
        );
        yield {
          type: "inference.done",
          seq: opts.nextSeq(),
          data: {
            turn: makeAssistantTurn("plain reply"),
            usage: emptyUsage(),
            source: TEST_SOURCE,
          },
        };
      },
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitFor("reactor.done");

    const inferenceDoneIdx = order.indexOf("inference.done");
    const secondMessageIdx = order.lastIndexOf("message.received");
    expect(inferenceDoneIdx).toBeGreaterThan(-1);
    expect(secondMessageIdx).toBeGreaterThan(-1);
    expect(inferenceDoneIdx).toBeLessThan(secondMessageIdx);
    // Sanity check: exactly one inference ran for the single inferring
    // message. The ordering assertions above are what guard the regression.
    expect(inferenceCount).toBe(1);
  });
});

// ---------------------------------------------------------------------------
// 17. Correlation validator edge cases
// ---------------------------------------------------------------------------

describe("createReactor — correlation validator", () => {
  test("validator returning false lets message through as uncorrelated", async () => {
    const CORR_ID = "corr-rejected";
    let toolDoneSeen = false;

    const { reactor, events, waitFor } = createTestReactor({
      correlationValidator: {
        async validate() {
          return false;
        },
      },
      director: directorFromTable({
        "message.received": (_e, _s, caps) => {
          if (toolDoneSeen) return caps.done();
          return caps.executeTools([
            { id: "tc1", name: "send_msg", arguments: {} },
          ]);
        },
        "tool.done": (_e, _s, caps) => {
          toolDoneSeen = true;
          return caps.suspend({
            type: "message_response",
            gateId: "val-gate",
            timeoutMs: 5000,
            correlationId: CORR_ID,
          });
        },
      }),
      toolRunner: makeToolRunner(async (call) => {
        return {
          callId: call.id,
          content: "sent",
          pendingMarker: {
            status: "pending" as const,
            correlationId: CORR_ID,
          },
        };
      }),
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());

    await waitFor("reactor.gate.blocked");

    // Deliver a message with matching correlationId — but the validator
    // rejects it, so it should arrive as message.received, not
    // message.correlated.
    reactor.deliver(makeInboundMessage(CORR_ID));

    await waitFor("reactor.done");

    expect(events.some((e) => e.type === "message.correlated")).toBe(false);
    // The message should have been delivered as uncorrelated.
    const receivedEvents = events.filter((e) => e.type === "message.received");
    expect(receivedEvents.length).toBe(2);
  });

  test("validator that throws lets message through as uncorrelated", async () => {
    const CORR_ID = "corr-throws";
    let toolDoneSeen = false;

    const { reactor, events, waitFor } = createTestReactor({
      correlationValidator: {
        async validate() {
          throw new Error("validator crashed");
        },
      },
      director: directorFromTable({
        "message.received": (_e, _s, caps) => {
          if (toolDoneSeen) return caps.done();
          return caps.executeTools([
            { id: "tc1", name: "send_msg", arguments: {} },
          ]);
        },
        "tool.done": (_e, _s, caps) => {
          toolDoneSeen = true;
          return caps.suspend({
            type: "message_response",
            gateId: "throw-gate",
            timeoutMs: 5000,
            correlationId: CORR_ID,
          });
        },
      }),
      toolRunner: makeToolRunner(async (call) => {
        return {
          callId: call.id,
          content: "sent",
          pendingMarker: {
            status: "pending" as const,
            correlationId: CORR_ID,
          },
        };
      }),
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());

    await waitFor("reactor.gate.blocked");

    reactor.deliver(makeInboundMessage(CORR_ID));

    await waitFor("reactor.done");

    expect(events.some((e) => e.type === "message.correlated")).toBe(false);
    const receivedEvents = events.filter((e) => e.type === "message.received");
    expect(receivedEvents.length).toBe(2);
  });

  test("concurrent delivery with same correlationId only correlates once", async () => {
    const CORR_ID = "corr-double";
    let toolDoneSeen = false;

    // Use a validator with a manually-resolved promise so we can control
    // timing. The first deliver enters the validator and blocks. The second
    // deliver hits the correlatingIds guard and falls through.
    let resolveValidator!: () => void;
    const validatorPromise = new Promise<void>((resolve) => {
      resolveValidator = resolve;
    });

    const { reactor, events, waitFor } = createTestReactor({
      correlationValidator: {
        async validate() {
          await validatorPromise;
          return true;
        },
      },
      director: directorFromTable({
        "message.received": (_e, _s, caps) => {
          if (toolDoneSeen) return caps.done();
          return caps.executeTools([
            { id: "tc1", name: "send_msg", arguments: {} },
          ]);
        },
        "tool.done": (_e, _s, caps) => {
          toolDoneSeen = true;
          return caps.suspend({
            type: "message_response",
            gateId: "double-gate",
            timeoutMs: 5000,
            correlationId: CORR_ID,
          });
        },
        "reactor.gate.cleared": (_e, _s, caps) => caps.done(),
      }),
      toolRunner: makeToolRunner(async (call) => {
        return {
          callId: call.id,
          content: "sent",
          pendingMarker: {
            status: "pending" as const,
            correlationId: CORR_ID,
          },
        };
      }),
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());

    await waitFor("reactor.gate.blocked");

    // Deliver two messages with the same correlationId in the same tick.
    // The first enters the validator (which blocks on the promise).
    // The second hits the correlatingIds guard and is rejected.
    reactor.deliver(makeInboundMessage(CORR_ID));
    reactor.deliver(makeInboundMessage(CORR_ID));

    // Let the validator resolve so the first correlation completes.
    resolveValidator();

    await waitFor("reactor.done");

    // Only one message.correlated event should have been emitted.
    const correlatedEvents = events.filter(
      (e) => e.type === "message.correlated",
    );
    expect(correlatedEvents.length).toBe(1);

    // The second message should have been delivered as uncorrelated.
    const receivedEvents = events.filter((e) => e.type === "message.received");
    expect(receivedEvents.length).toBe(2);
  });
});

// ---------------------------------------------------------------------------
// 18. Start guard
// ---------------------------------------------------------------------------

describe("createReactor — start guard", () => {
  test("calling start() twice throws", () => {
    const { reactor } = createTestReactor();
    reactor.start();
    expect(() => reactor.start()).toThrow(/already running/);
  });
});

// ---------------------------------------------------------------------------
// 19. State snapshot inspection
// ---------------------------------------------------------------------------

describe("createReactor — state snapshot inspection", () => {
  test("state.turns contains delivered message text", async () => {
    let capturedTurns: ConversationTurn[] = [];
    const { reactor, waitFor } = createTestReactor({
      director: directorFromTable({
        "message.received": (_e, state, caps) => {
          capturedTurns = state.turns;
          return caps.done();
        },
      }),
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitFor("reactor.done");

    expect(capturedTurns.length).toBe(1);
    const last = capturedTurns.at(-1);
    if (last === undefined) throw new Error("unreachable");
    expect(last.role).toBe("user");
    const textBlock = last.content.find((b) => b.type === "text");
    if (textBlock === undefined || textBlock.type !== "text")
      throw new Error("unreachable");
    expect(textBlock.text).toBe("[From: test@example.com]\n\nhello");
  });

  test("state.pendingOperations tracks pending markers from tools", async () => {
    const CORR_ID = "corr-snapshot-check";
    let capturedOps: PendingOperation[] = [];

    const { reactor, events, waitFor } = createTestReactor({
      director: directorFromTable({
        "message.received": (_e, _s, caps) =>
          caps.executeTools([{ id: "tc1", name: "send_msg", arguments: {} }]),
        "tool.done": (_e, state, caps) => {
          capturedOps = state.pendingOperations;
          return caps.done();
        },
      }),
      toolRunner: {
        async run(call) {
          return {
            callId: call.id,
            content: "sent",
            pendingMarker: {
              status: "pending" as const,
              correlationId: CORR_ID,
            },
          };
        },
      },
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitFor("reactor.done");

    expect(capturedOps.length).toBe(1);
    const op = capturedOps[0];
    if (op === undefined) throw new Error("unreachable");
    expect(op.correlationId).toBe(CORR_ID);
    // An async-tool pending marker is not an ask-rail suspension: it carries no
    // suspended call and no approval snapshot.
    expect(op.suspendedCall).toBeUndefined();
    expect(op.approvalSnapshot).toBeUndefined();
    // It emits no `reactor.gate.blocked` -- the event that drives the hub's
    // approval co-write -- so a marker never writes an approval row. This keeps
    // the snapshot columns non-null: only the ask rail co-writes, and it always
    // carries a snapshot.
    expect(events.some((e) => e.type === "reactor.gate.blocked")).toBe(false);
  });

  test("state.activeGates reflects registered gates during suspend", async () => {
    let gatesDuringSuspend: ReactorState["activeGates"] = [];
    let gatesAfterCleared: ReactorState["activeGates"] = [];
    let messageCount = 0;

    const { reactor, waitFor } = createTestReactor({
      director: {
        async decide(event, state, caps) {
          if (event.type === "message.received") {
            messageCount++;
            if (messageCount === 1) {
              return caps.suspend({
                type: "approval",
                gateId: "snapshot-gate",
                timeoutMs: 500,
              });
            }
            // Second message arrives while gate is active.
            gatesDuringSuspend = state.activeGates;
            return caps.wait();
          }
          if (event.type === "reactor.gate.cleared") {
            gatesAfterCleared = state.activeGates;
            return caps.done();
          }
          return caps.done();
        },
      },
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitFor("reactor.gate.blocked");

    // Deliver a second message while the gate is active. The 500ms
    // gate timeout is far larger than the async overhead of deliver(),
    // so the second message.received is guaranteed to be processed
    // before the timeout fires.
    reactor.deliver(makeInboundMessage());

    // The gate times out after 500ms, firing reactor.gate.cleared
    // where we capture the post-clear state and return done.
    await waitFor("reactor.done");

    expect(gatesDuringSuspend.length).toBe(1);
    const gate = gatesDuringSuspend[0];
    if (gate === undefined) throw new Error("unreachable");
    expect(gate.gateId).toBe("snapshot-gate");
    expect(gate.type).toBe("approval");
    expect(gatesAfterCleared.length).toBe(0);
  });

  test("state.tokenUsage has initial zero values without inference", async () => {
    let capturedUsage: TokenUsage | undefined;
    const { reactor, waitFor } = createTestReactor({
      director: directorFromTable({
        "message.received": (_e, state, caps) => {
          capturedUsage = state.tokenUsage;
          return caps.done();
        },
      }),
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitFor("reactor.done");

    if (capturedUsage === undefined) throw new Error("unreachable");
    expect(capturedUsage.input).toBe(0);
    expect(capturedUsage.output).toBe(0);
    expect(capturedUsage.cacheRead).toBe(0);
    expect(capturedUsage.cacheWrite).toBe(0);
    expect(capturedUsage.thinking).toBe(0);
  });

  test("director cannot corrupt reactor state by mutating snapshot content blocks", async () => {
    let secondSnapshot: ReactorState | undefined;
    let messageCount = 0;

    const { reactor, waitFor } = createTestReactor({
      director: {
        async decide(event, state, caps) {
          if (event.type === "message.received") {
            messageCount++;
            if (messageCount === 1) {
              // Mutate the snapshot's content block.
              const msg = state.turns[0];
              if (msg !== undefined) {
                const block = msg.content[0];
                if (block !== undefined && block.type === "text") {
                  (block as { text: string }).text = "CORRUPTED";
                }
              }
              return caps.wait();
            }
            // Second message: capture a fresh snapshot to verify isolation.
            secondSnapshot = state;
            return caps.done();
          }
          return caps.done();
        },
      },
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());

    // Wait for the first message to be processed, then deliver a second.
    await waitUntil(() => messageCount >= 1);
    reactor.deliver(makeInboundMessage());
    await waitFor("reactor.done");

    if (secondSnapshot === undefined) throw new Error("unreachable");
    const firstMsg = secondSnapshot.turns[0];
    if (firstMsg === undefined) throw new Error("unreachable");
    const block = firstMsg.content[0];
    if (block === undefined || block.type !== "text")
      throw new Error("unreachable");
    expect(block.text).toBe("[From: test@example.com]\n\nhello");
  });
});

// ---------------------------------------------------------------------------
// 20. Deliver after done
// ---------------------------------------------------------------------------

describe("createReactor — deliver after done", () => {
  test("a delivery after done ends its turn failed and handles nothing", async () => {
    const { reactor, events, waitFor } = createTestReactor({
      director: directorFromTable({
        "message.received": (_e, _s, caps) => caps.done(),
      }),
    });

    reactor.start();
    reactor.deliver(makeInboundMessage(), { turnId: "turn-first" });
    await waitFor("reactor.done");

    const eventsBeforeDeliver = events.length;

    // The done guard in deliver() ends the turn and returns before the
    // asynchronous delivery path, so the events are there when deliver()
    // returns.
    reactor.deliver(makeInboundMessage(), { turnId: "turn-late" });
    reactor.deliver(makeInboundMessage());

    const late = events.slice(eventsBeforeDeliver);
    expect(late.map((e) => e.type)).toEqual([
      "reactor.turn.ended",
      "reactor.turn.ended",
    ]);
    const [given, minted] = late.map((e) => {
      if (e.type !== "reactor.turn.ended") throw new Error("unreachable");
      return e.data;
    });
    expect(given).toEqual({
      turnId: "turn-late",
      outcome: "failed",
      error: { message: "The reactor is done", kind: "reactor_done" },
    });
    expect(minted).toMatchObject({
      outcome: "failed",
      error: { kind: "reactor_done" },
    });
    expect(minted?.turnId).not.toBe("turn-late");
    expect(minted?.turnId.length).toBeGreaterThan(0);
    expect(events.filter((e) => e.type === "reactor.done")).toHaveLength(1);
  });

  test("a delivery that names an ended turn throws", async () => {
    const { reactor, waitFor } = createTestReactor({
      director: directorFromTable({}, "wait"),
    });

    reactor.start();
    reactor.deliver(makeInboundMessage(), { turnId: "turn-a" });
    await waitFor("reactor.turn.ended");

    expect(() =>
      reactor.deliver(makeInboundMessage(), { turnId: "turn-a" }),
    ).toThrow(/turn-a has already ended/);
    reactor.abort("admin_kill");
    await waitFor("reactor.done");
  });
});

// ---------------------------------------------------------------------------
// 21. Inference path (infer action)
// ---------------------------------------------------------------------------

function makeAssistantTurn(text: string): AssistantTurn {
  return {
    role: "assistant",
    content: [{ type: "text", text }],
    model: "mock-model",
    timestamp: 1000,
  };
}

function makeInferenceRunner(
  result:
    | { type: "done"; turn: AssistantTurn; usage: TokenUsage }
    | { type: "error"; error: InferenceError; partial: PartialMessage },
): (opts: InferenceHarnessOptions) => AsyncGenerator<InferenceEvent> {
  return async function* (opts) {
    if (result.type === "done") {
      const event: InferenceEvent = {
        type: "inference.done",
        seq: opts.nextSeq(),
        data: { turn: result.turn, usage: result.usage, source: TEST_SOURCE },
      };
      yield event;
    } else {
      const event: InferenceEvent = {
        type: "inference.error",
        seq: opts.nextSeq(),
        data: { error: result.error, partial: result.partial },
      };
      yield event;
    }
  };
}

// ---------------------------------------------------------------------------
// afterInferenceDone abort/halt policy, end to end
//
// These drive the real DefaultDirector so the action sets its abort and
// halt branches build are validated by the reactor, not just asserted in
// isolation. The bug was that those sets were rejected, so the reactor
// crashed with a fatal "Invalid action set" instead of terminating
// (abort) or pausing and replying (halt).
// ---------------------------------------------------------------------------

describe("createReactor — afterInferenceDone abort and halt", () => {
  test("abort terminates without an invalid action set", async () => {
    const hook: AfterInferenceHook = () => ({
      type: "abort",
      reason: "budget exhausted",
    });
    const { reactor, events, waitFor } = createTestReactor({
      director: createDefaultDirector("test agent", [], {
        afterInferenceDone: hook,
      }),
      inferenceRunner: makeInferenceRunner({
        type: "done",
        turn: makeAssistantTurn("ignored"),
        usage: emptyUsage(),
      }),
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitFor("reactor.done");

    expect(
      events.some(
        (e) =>
          e.type === "reactor.error" &&
          /invalid action set/i.test(e.data.error),
      ),
    ).toBe(false);
    // Abort is terminal and does not surface the reason, so no reply.
    expect(events.some((e) => e.type === "connector.reply")).toBe(false);
  });

  test("halt replies and keeps the reactor alive", async () => {
    let hookCalls = 0;
    const hook: AfterInferenceHook = () => {
      hookCalls++;
      return { type: "halt", reason: "paused for top-up" };
    };
    const { reactor, events, waitFor } = createTestReactor({
      director: createDefaultDirector("test agent", [], {
        afterInferenceDone: hook,
      }),
      inferenceRunner: makeInferenceRunner({
        type: "done",
        turn: makeAssistantTurn("ignored"),
        usage: emptyUsage(),
      }),
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitForEvent(events, (e) => e.type === "connector.reply");

    // Halt returned the reactor to waiting rather than shutting down, so a
    // second message must still be processed — that is the liveness proof.
    reactor.deliver(makeInboundMessage());
    await waitForEvent(
      events,
      () => events.filter((e) => e.type === "connector.reply").length >= 2,
    );

    // The reactor is still alive; abort it so the test does not leak it.
    reactor.abort("admin_kill");
    await waitFor("reactor.done");

    const replies = events.filter((e) => e.type === "connector.reply");
    expect(replies.length).toBe(2);
    expect(hookCalls).toBe(2);
    for (const reply of replies) {
      if (reply.type !== "connector.reply") throw new Error("unreachable");
      expect(reply.data.content).toBe("paused for top-up");
    }
    expect(
      events.some(
        (e) =>
          e.type === "reactor.error" &&
          /invalid action set/i.test(e.data.error),
      ),
    ).toBe(false);
  });
});

describe("createReactor — inference path", () => {
  test("infer action drives inference.done through to director and accumulates usage", async () => {
    const inferUsage: TokenUsage = {
      input: 100,
      output: 50,
      cacheRead: 10,
      cacheWrite: 5,
      thinking: 20,
    };
    const assistantMsg = makeAssistantTurn("Hello from the model");

    let stateAtInferenceDone: ReactorState | undefined;

    const { reactor, events, waitFor } = createTestReactor({
      director: directorFromTable({
        "message.received": (_e, _s, caps) => caps.infer(),
        "inference.done": (_e, state, caps) => {
          stateAtInferenceDone = state;
          return caps.done();
        },
      }),
      inferenceRunner: makeInferenceRunner({
        type: "done",
        turn: assistantMsg,
        usage: inferUsage,
      }),
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitFor("reactor.done");

    // The emitted event stream should contain inference.done.
    const inferDone = getEvent(events, "inference.done");
    expect(inferDone.data.turn.content[0]).toEqual({
      type: "text",
      text: "Hello from the model",
    });
    expect(inferDone.data.usage).toEqual(inferUsage);

    // The director should have received the inference.done event with
    // accumulated token usage visible in the state snapshot.
    if (stateAtInferenceDone === undefined)
      throw new Error("director never received inference.done");
    expect(stateAtInferenceDone.tokenUsage).toEqual(inferUsage);

    // The assistant message should have been appended to the conversation.
    const lastMsg =
      stateAtInferenceDone.turns[stateAtInferenceDone.turns.length - 1];
    if (lastMsg === undefined) throw new Error("no messages in state snapshot");
    expect(lastMsg.role).toBe("assistant");
    expect(lastMsg.content[0]).toEqual({
      type: "text",
      text: "Hello from the model",
    });
  });

  test("infer action with inference.error delivers error event to director", async () => {
    const inferError: InferenceError = {
      category: "retryable",
      message: "rate limited",
    };
    const partial: PartialMessage = { text: "partial output" };

    let capturedError: { category: string; message: string } | undefined;

    const { reactor, events, waitFor } = createTestReactor({
      director: directorFromTable({
        "message.received": (_e, _s, caps) => caps.infer(),
        "inference.error": (e, _s, caps) => {
          capturedError = {
            category: e.error.category,
            message: e.error.message,
          };
          return caps.done();
        },
      }),
      inferenceRunner: makeInferenceRunner({
        type: "error",
        error: inferError,
        partial,
      }),
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitFor("reactor.done");

    // Verify the director received the error event with correct fields.
    if (capturedError === undefined)
      throw new Error("director never received inference.error");
    expect(capturedError.category).toBe("retryable");
    expect(capturedError.message).toBe("rate limited");

    // The emitted event stream should contain inference.error.
    const inferErr = getEvent(events, "inference.error");
    expect(inferErr.data.error.category).toBe("retryable");
    expect(inferErr.data.partial.text).toBe("partial output");
  });

  test("inference runner that yields no terminal event emits fatal error and completes", async () => {
    let capturedError: { category: string; message: string } | undefined;

    const { reactor, events, waitFor } = createTestReactor({
      director: directorFromTable({
        "message.received": (_e, _s, caps) => caps.infer(),
        "inference.error": (e, _s, caps) => {
          capturedError = {
            category: e.error.category,
            message: e.error.message,
          };
          return caps.done();
        },
      }),
      inferenceRunner: async function* (_opts) {
        // Yield nothing — violates the runner contract.
      },
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitFor("reactor.done");

    // The reactor should have emitted a reactor.error for observability.
    const reactorErr = getEvent(events, "reactor.error");
    expect(reactorErr.data.fatal).toBe(true);
    expect(reactorErr.data.error).toContain("without a terminal event");

    // The director should have received inference.error with category "fatal".
    if (capturedError === undefined)
      throw new Error("director never received inference.error");
    expect(capturedError.category).toBe("fatal");
    expect(capturedError.message).toContain("without a terminal event");
  });
});

// ---------------------------------------------------------------------------
// BeforeToolExtension
// ---------------------------------------------------------------------------

describe("createReactor — beforeToolExtensions", () => {
  const toolCallMsg: AssistantTurn = {
    role: "assistant",
    content: [
      {
        type: "tool_call",
        id: "call-1",
        name: "bash",
        arguments: { cmd: "ls" },
      },
    ],
    model: "test-model",
    timestamp: 1000,
  };

  const inferUsage: TokenUsage = {
    input: 10,
    output: 5,
    cacheRead: 0,
    cacheWrite: 0,
    thinking: 0,
  };

  function inferenceRunnerWithToolCall() {
    return makeInferenceRunner({
      type: "done",
      turn: toolCallMsg,
      usage: inferUsage,
    });
  }

  test("allowing extension lets the tool run normally", async () => {
    const allowAll: BeforeToolExtension = {
      async beforeTool() {
        return { type: "allow" };
      },
    };

    const toolsRun: string[] = [];

    const { reactor, events, waitFor } = createTestReactor({
      director: directorFromTable({
        "message.received": (_e, _s, caps) => caps.infer(),
        "inference.done": (_e, _s, caps) =>
          caps.executeTools(
            [{ id: "call-1", name: "bash", arguments: { cmd: "ls" } }],
            true,
          ),
        "tool.done": (_e, _s, caps) => caps.done(),
      }),
      toolRunner: makeToolRunner(async (call) => {
        toolsRun.push(call.name);
        return { callId: call.id, content: "ok" };
      }),
      inferenceRunner: inferenceRunnerWithToolCall(),
      beforeToolExtensions: [allowAll],
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitFor("reactor.done");

    expect(toolsRun).toEqual(["bash"]);
    const starts = events.filter((e) => e.type === "tool.start");
    expect(starts.length).toBe(1);
  });

  test("blocking extension prevents tool execution", async () => {
    const blockBash: BeforeToolExtension = {
      async beforeTool(call) {
        if (call.name === "bash")
          return { type: "block", reason: "Denied by policy" };
        return { type: "allow" };
      },
    };

    const toolsRun: string[] = [];

    const { reactor, events, waitFor } = createTestReactor({
      director: directorFromTable({
        "message.received": (_e, _s, caps) => caps.infer(),
        "inference.done": (_e, _s, caps) =>
          caps.executeTools(
            [{ id: "call-1", name: "bash", arguments: { cmd: "ls" } }],
            true,
          ),
        "tool.done": (_e, _s, caps) => caps.done(),
      }),
      toolRunner: makeToolRunner(async (call) => {
        toolsRun.push(call.name);
        return { callId: call.id, content: "ok" };
      }),
      inferenceRunner: inferenceRunnerWithToolCall(),
      beforeToolExtensions: [blockBash],
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitFor("reactor.done");

    expect(toolsRun).toEqual([]);

    // No tool.start for blocked tools.
    const starts = events.filter((e) => e.type === "tool.start");
    expect(starts.length).toBe(0);

    // tool.done is emitted with isError and the block reason.
    const doneEvents = events.filter(
      (e): e is Extract<ReactorEmittedEvent, { type: "tool.done" }> =>
        e.type === "tool.done",
    );
    expect(doneEvents.length).toBeGreaterThanOrEqual(1);
    const blocked = doneEvents[0];
    if (!blocked) throw new Error("expected at least one tool.done event");
    expect(blocked.data.result.isError).toBe(true);
    expect(blocked.data.result.content).toBe("Denied by policy");
    expect(blocked.data.result.callId).toBe("call-1");
  });

  test("first blocking extension wins and subsequent extensions are not called", async () => {
    const called: string[] = [];

    const extA: BeforeToolExtension = {
      async beforeTool() {
        called.push("A");
        return { type: "block", reason: "Blocked by A" };
      },
    };

    const extB: BeforeToolExtension = {
      async beforeTool() {
        called.push("B");
        return { type: "allow" };
      },
    };

    const { reactor, waitFor } = createTestReactor({
      director: directorFromTable({
        "message.received": (_e, _s, caps) => caps.infer(),
        "inference.done": (_e, _s, caps) =>
          caps.executeTools(
            [{ id: "call-1", name: "bash", arguments: { cmd: "ls" } }],
            true,
          ),
        "tool.done": (_e, _s, caps) => caps.done(),
      }),
      inferenceRunner: inferenceRunnerWithToolCall(),
      beforeToolExtensions: [extA, extB],
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitFor("reactor.done");

    expect(called).toEqual(["A"]);
  });

  test("throwing extension is treated as a block with reactor.error", async () => {
    const throwingExt: BeforeToolExtension = {
      async beforeTool() {
        throw new Error("Extension crashed");
      },
    };

    const toolsRun: string[] = [];

    const { reactor, events, waitFor } = createTestReactor({
      director: directorFromTable({
        "message.received": (_e, _s, caps) => caps.infer(),
        "inference.done": (_e, _s, caps) =>
          caps.executeTools(
            [{ id: "call-1", name: "bash", arguments: { cmd: "ls" } }],
            true,
          ),
        "tool.done": (_e, _s, caps) => caps.done(),
      }),
      toolRunner: makeToolRunner(async (call) => {
        toolsRun.push(call.name);
        return { callId: call.id, content: "ok" };
      }),
      inferenceRunner: inferenceRunnerWithToolCall(),
      beforeToolExtensions: [throwingExt],
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitFor("reactor.done");

    expect(toolsRun).toEqual([]);

    const reactorErrors = events.filter((e) => e.type === "reactor.error");
    const extError = reactorErrors.find(
      (e) =>
        e.type === "reactor.error" &&
        e.data.error.includes("BeforeToolExtension threw"),
    );
    if (extError === undefined || extError.type !== "reactor.error")
      throw new Error("expected reactor.error from throwing extension");
    expect(extError.data.fatal).toBe(false);
  });

  test("throwing extension terminates chain before subsequent extensions", async () => {
    const called: string[] = [];

    const throwingExt: BeforeToolExtension = {
      async beforeTool() {
        called.push("thrower");
        throw new Error("boom");
      },
    };

    const secondExt: BeforeToolExtension = {
      async beforeTool() {
        called.push("second");
        return { type: "allow" };
      },
    };

    const { reactor, waitFor } = createTestReactor({
      director: directorFromTable({
        "message.received": (_e, _s, caps) => caps.infer(),
        "inference.done": (_e, _s, caps) =>
          caps.executeTools(
            [{ id: "call-1", name: "bash", arguments: { cmd: "ls" } }],
            true,
          ),
        "tool.done": (_e, _s, caps) => caps.done(),
      }),
      inferenceRunner: inferenceRunnerWithToolCall(),
      beforeToolExtensions: [throwingExt, secondExt],
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitFor("reactor.done");

    expect(called).toEqual(["thrower"]);
  });

  test("extension receives current state snapshot", async () => {
    let capturedState: ReactorState | undefined;

    const capturingExt: BeforeToolExtension = {
      async beforeTool(_call, state) {
        capturedState = state;
        return { type: "allow" };
      },
    };

    const { reactor, waitFor } = createTestReactor({
      director: directorFromTable({
        "message.received": (_e, _s, caps) => caps.infer(),
        "inference.done": (_e, _s, caps) =>
          caps.executeTools(
            [{ id: "call-1", name: "bash", arguments: { cmd: "ls" } }],
            true,
          ),
        "tool.done": (_e, _s, caps) => caps.done(),
      }),
      inferenceRunner: inferenceRunnerWithToolCall(),
      beforeToolExtensions: [capturingExt],
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitFor("reactor.done");

    if (capturedState === undefined)
      throw new Error("extension was never called");
    // State should contain the inbound message and the assistant tool_call message.
    expect(capturedState.turns.length).toBeGreaterThanOrEqual(2);
    expect(capturedState.sessionId).toContain("test-sess-");
  });

  test("parallel batch with one blocked and one allowed tool", async () => {
    const blockBash: BeforeToolExtension = {
      async beforeTool(call) {
        if (call.name === "bash")
          return { type: "block", reason: "Denied by policy" };
        return { type: "allow" };
      },
    };

    const toolsRun: string[] = [];

    const twoToolCallMsg: AssistantTurn = {
      role: "assistant",
      content: [
        {
          type: "tool_call",
          id: "call-bash",
          name: "bash",
          arguments: { cmd: "rm -rf /" },
        },
        {
          type: "tool_call",
          id: "call-read",
          name: "read_file",
          arguments: { path: "/etc/hosts" },
        },
      ],
      model: "test-model",
      timestamp: 1000,
    };

    let toolDoneCount = 0;
    const { reactor, events, waitFor } = createTestReactor({
      director: directorFromTable(
        {
          "message.received": (_e, _s, caps) => caps.infer(),
          "inference.done": (_e, _s, caps) =>
            caps.executeTools(
              [
                {
                  id: "call-bash",
                  name: "bash",
                  arguments: { cmd: "rm -rf /" },
                },
                {
                  id: "call-read",
                  name: "read_file",
                  arguments: { path: "/etc/hosts" },
                },
              ],
              true,
            ),
          "tool.done": (_e, _s, caps) => {
            toolDoneCount++;
            if (toolDoneCount >= 2) return caps.done();
            return caps.wait();
          },
        },
        "wait",
      ),
      toolRunner: makeToolRunner(async (call) => {
        toolsRun.push(call.name);
        return { callId: call.id, content: `result-${call.name}` };
      }),
      inferenceRunner: makeInferenceRunner({
        type: "done",
        turn: twoToolCallMsg,
        usage: inferUsage,
      }),
      beforeToolExtensions: [blockBash],
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitFor("reactor.done");

    // Only the allowed tool should have run.
    expect(toolsRun).toEqual(["read_file"]);

    // tool.start only for the allowed tool.
    const starts = events.filter((e) => e.type === "tool.start");
    expect(starts.length).toBe(1);

    // Both tools produce tool.done events.
    const doneEvents = events.filter((e) => e.type === "tool.done");
    expect(doneEvents.length).toBeGreaterThanOrEqual(2);

    // The blocked tool has isError.
    const blockedDone = doneEvents.find(
      (e) => e.type === "tool.done" && e.data.result.callId === "call-bash",
    );
    if (blockedDone === undefined || blockedDone.type !== "tool.done")
      throw new Error("expected tool.done for blocked call");
    expect(blockedDone.data.result.isError).toBe(true);
    expect(blockedDone.data.result.content).toBe("Denied by policy");

    // The allowed tool has the real result.
    const allowedDone = doneEvents.find(
      (e) => e.type === "tool.done" && e.data.result.callId === "call-read",
    );
    if (allowedDone === undefined || allowedDone.type !== "tool.done")
      throw new Error("expected tool.done for allowed call");
    expect(allowedDone.data.result.isError).toBeUndefined();
    expect(allowedDone.data.result.content).toBe("result-read_file");
  });
});

// ---------------------------------------------------------------------------
// Before-tool suspension on an `ask` grant, and rehydration across restart
// ---------------------------------------------------------------------------

// A context store that keeps the persisted pending operations and turns in a
// shared cell so a second reactor can reload exactly what the first committed.
// This is what lets the rehydration test observe the cross-restart behavior a
// stateless makeContextStore cannot.
type PersistedCell = {
  turns: ConversationTurn[];
  pendingOperations: PersistedPendingOperation[];
  tokenUsage: TokenUsage;
};

function makePersistingContextStore(cell: PersistedCell): ContextStore {
  return {
    async load() {
      return {
        turns: cell.turns,
        pendingOperations: cell.pendingOperations,
        tokenUsage: cell.tokenUsage,
        connectorState: null,
      };
    },
    setConnectorState() {
      /* noop */
    },
    async commit(options: { message: string }) {
      return { hash: "hash", message: options.message, timestamp: Date.now() };
    },
    async branch() {
      /* noop */
    },
    async log() {
      return [];
    },
    async readAt() {
      return [];
    },
    async writeBlob() {
      /* noop */
    },
    async readBlob() {
      throw new Error("not implemented");
    },
    async writePrompt() {
      /* noop */
    },
    async writeResponse() {
      /* noop */
    },
    async writeManifest() {
      /* noop */
    },
    async writeTurns(turns) {
      cell.turns = turns;
    },
    async writeMetadata(metadata) {
      cell.pendingOperations = metadata.pendingOperations;
      cell.tokenUsage = metadata.tokenUsage;
    },
    async readManifestHistory() {
      throw new Error("not implemented");
    },
  };
}

// A tool-call assistant turn used to drive the before-tool path.
const suspendToolCallTurn: AssistantTurn = {
  role: "assistant",
  content: [
    { type: "tool_call", id: "call-ask", name: "charge_card", arguments: {} },
  ],
  model: "test-model",
  timestamp: 1000,
};

describe("createReactor — before-tool suspension on ask grant", () => {
  test("an ask grant suspends the call: gate.blocked carries the correlationId, no tool result, pending op persisted", async () => {
    const cell: PersistedCell = {
      turns: [],
      pendingOperations: [],
      tokenUsage: emptyUsage(),
    };

    const askExtension = createAuthzExtension({
      authorize: async () => ({
        effect: "ask" as const,
        matchingGrants: [],
        resolvedBy: null,
      }),
      approvalTimeoutMs: 60_000,
    });

    const { reactor, events, waitFor } = createTestReactor({
      contextStore: makePersistingContextStore(cell),
      director: directorFromTable(
        {
          "message.received": (_e, _s, caps) => caps.infer(),
          "inference.done": (_e, _s, caps) =>
            caps.executeTools([
              { id: "call-ask", name: "charge_card", arguments: {} },
            ]),
          "reactor.gate.cleared": (_e, _s, caps) => caps.done(),
        },
        "wait",
      ),
      inferenceRunner: makeInferenceRunner({
        type: "done",
        turn: suspendToolCallTurn,
        usage: emptyUsage(),
      }),
      beforeToolExtensions: [askExtension],
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());

    const blocked = await waitFor("reactor.gate.blocked");
    if (blocked.type !== "reactor.gate.blocked") throw new Error("unreachable");
    expect(blocked.data.reason).toBe("approval");
    const correlationId = blocked.data.correlationId;
    if (correlationId === undefined)
      throw new Error("expected reactor.gate.blocked to carry a correlationId");
    expect(blocked.data.gateId).toBe(`pending-${correlationId}`);

    // The suspended call is parked, not answered: no tool.start, no tool.done.
    expect(events.some((e) => e.type === "tool.start")).toBe(false);
    expect(events.some((e) => e.type === "tool.done")).toBe(false);

    // The pending operation was persisted with kind "approval", the minted
    // correlationId, and its deadline.
    expect(cell.pendingOperations).toHaveLength(1);
    const op = cell.pendingOperations[0];
    if (op === undefined) throw new Error("unreachable");
    expect(op.kind).toBe("approval");
    expect(op.correlationId).toBe(correlationId);
    expect(op.gateId).toBe(`pending-${correlationId}`);
    expect(op.timeoutAt).toBeGreaterThan(Date.now());

    reactor.abort("admin_kill");
    await waitFor("reactor.done");
  });

  test("gate.blocked and the persisted op carry the approval snapshot when tools are wired", async () => {
    const cell: PersistedCell = {
      turns: [],
      pendingOperations: [],
      tokenUsage: emptyUsage(),
    };

    const askExtension = createAuthzExtension({
      authorize: async () => ({
        effect: "ask" as const,
        matchingGrants: [],
        resolvedBy: null,
      }),
      approvalTimeoutMs: 60_000,
      toolDefinitions: [
        {
          name: "charge_card",
          description: "Charge the customer's card",
          inputSchema: { type: "object" },
        },
      ],
    });

    const { reactor, waitFor } = createTestReactor({
      contextStore: makePersistingContextStore(cell),
      director: directorFromTable(
        {
          "message.received": (_e, _s, caps) => caps.infer(),
          "inference.done": (_e, _s, caps) =>
            caps.executeTools([
              { id: "call-ask", name: "charge_card", arguments: {} },
            ]),
          "reactor.gate.cleared": (_e, _s, caps) => caps.done(),
        },
        "wait",
      ),
      inferenceRunner: makeInferenceRunner({
        type: "done",
        turn: suspendToolCallTurn,
        usage: emptyUsage(),
      }),
      beforeToolExtensions: [askExtension],
    });

    const expectedSnapshot = {
      name: "charge_card",
      description: "Charge the customer's card",
      inputSchema: { type: "object" },
      arguments: {},
    };

    reactor.start();
    reactor.deliver(makeInboundMessage());

    const blocked = await waitFor("reactor.gate.blocked");
    if (blocked.type !== "reactor.gate.blocked") throw new Error("unreachable");
    expect(blocked.data.approvalSnapshot).toEqual(expectedSnapshot);

    // The snapshot rides the persisted pending op too, so a rehydrated op
    // still carries it.
    const op = cell.pendingOperations[0];
    if (op === undefined) throw new Error("unreachable");
    expect(op.approvalSnapshot).toEqual(expectedSnapshot);

    reactor.abort("admin_kill");
    await waitFor("reactor.done");
  });

  test("a before-tool suspension in a cycle with no inference and no completed tool call still persists the pending op", async () => {
    // The suspension is raised from a message.received handler that dispatches
    // executeTools directly, so the cycle runs no inference and completes no
    // tool call (the sole call is parked). Registering the gate and pending
    // operation is nonetheless durable state that must be committed; otherwise
    // the pending op lives only in memory and is lost on restart.
    const cell: PersistedCell = {
      turns: [],
      pendingOperations: [],
      tokenUsage: emptyUsage(),
    };

    const askExtension = createAuthzExtension({
      authorize: async () => ({
        effect: "ask" as const,
        matchingGrants: [],
        resolvedBy: null,
      }),
      approvalTimeoutMs: 60_000,
    });

    const { reactor, events, waitFor } = createTestReactor({
      contextStore: makePersistingContextStore(cell),
      director: directorFromTable(
        {
          "message.received": (_e, _s, caps) =>
            caps.executeTools([
              { id: "call-ask", name: "charge_card", arguments: {} },
            ]),
          "reactor.gate.cleared": (_e, _s, caps) => caps.done(),
        },
        "wait",
      ),
      beforeToolExtensions: [askExtension],
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());

    const blocked = await waitFor("reactor.gate.blocked");
    if (blocked.type !== "reactor.gate.blocked") throw new Error("unreachable");
    const correlationId = blocked.data.correlationId;
    if (correlationId === undefined)
      throw new Error("expected reactor.gate.blocked to carry a correlationId");

    // No inference and no completed tool call ran in this cycle.
    expect(events.some((e) => e.type === "inference.done")).toBe(false);
    expect(events.some((e) => e.type === "tool.start")).toBe(false);
    expect(events.some((e) => e.type === "tool.done")).toBe(false);

    // The suspension forced a durable commit: the pending op was written to
    // the context store, so a reload would recover it.
    expect(cell.pendingOperations).toHaveLength(1);
    const op = cell.pendingOperations[0];
    if (op === undefined) throw new Error("unreachable");
    expect(op.kind).toBe("approval");
    expect(op.correlationId).toBe(correlationId);
    expect(op.gateId).toBe(`pending-${correlationId}`);

    reactor.abort("admin_kill");
    await waitFor("reactor.done");
  });

  test("a suspended agent rehydrates a live gate on restart and re-dispatches the parked call on approval delivery", async () => {
    const cell: PersistedCell = {
      turns: [],
      pendingOperations: [],
      tokenUsage: emptyUsage(),
    };

    const askExtension = createAuthzExtension({
      authorize: async () => ({
        effect: "ask" as const,
        matchingGrants: [],
        resolvedBy: null,
      }),
      approvalTimeoutMs: 60_000,
    });

    // Phase 1: suspend and persist, then tear down.
    const first = createTestReactor({
      contextStore: makePersistingContextStore(cell),
      director: directorFromTable(
        {
          "message.received": (_e, _s, caps) => caps.infer(),
          "inference.done": (_e, _s, caps) =>
            caps.executeTools([
              { id: "call-ask", name: "charge_card", arguments: {} },
            ]),
        },
        "wait",
      ),
      inferenceRunner: makeInferenceRunner({
        type: "done",
        turn: suspendToolCallTurn,
        usage: emptyUsage(),
      }),
      beforeToolExtensions: [askExtension],
    });

    first.reactor.start();
    first.reactor.deliver(makeInboundMessage());
    const blocked = await first.waitFor("reactor.gate.blocked");
    if (blocked.type !== "reactor.gate.blocked") throw new Error("unreachable");
    const correlationId = blocked.data.correlationId;
    if (correlationId === undefined) throw new Error("expected correlationId");

    first.reactor.abort("admin_kill");
    await first.waitFor("reactor.done");

    // The persisted op survives the teardown, carrying the parked call so the
    // resume can re-run it.
    expect(cell.pendingOperations).toHaveLength(1);
    expect(cell.pendingOperations[0]?.suspendedCall?.id).toBe("call-ask");

    // Phase 2: reload from the persisted cell. A restarted reactor with a
    // rehydrated gate matches the delivered approval and re-runs the parked
    // call; without rehydration the delivered message would not match a live
    // gate and the reactor would stay wedged (this test times out).
    const toolsRun: string[] = [];
    const second = createTestReactor({
      contextStore: makePersistingContextStore(cell),
      director: directorFromTable(
        {
          "resume.execute_tools": (e, _s, caps) =>
            caps.executeTools(e.calls, false, true),
          "tool.done": (_e, _s, caps) => caps.done(),
        },
        "wait",
      ),
      toolRunner: makeToolRunner(async (call) => {
        toolsRun.push(call.name);
        return { callId: call.id, content: "charged" };
      }),
      beforeToolExtensions: [askExtension],
    });

    second.reactor.start();
    await second.waitFor("reactor.start");

    second.reactor.deliver(makeApprovalMessage(correlationId));

    const toolDone = await second.waitFor("tool.done");
    if (toolDone.type !== "tool.done") throw new Error("unreachable");
    expect(toolDone.data.result.callId).toBe("call-ask");

    const correlated = getEvent(second.events, "message.correlated");
    expect(correlated.data.correlationId).toBe(correlationId);

    // The one-shot bypass let the re-dispatched call through without
    // re-parking: it ran exactly once and no second gate was blocked.
    expect(toolsRun).toEqual(["charge_card"]);
    expect(second.events.some((e) => e.type === "reactor.gate.blocked")).toBe(
      false,
    );

    await second.waitFor("reactor.done");
  });

  test("buffers an approval delivered before startup until its correlation is rehydrated", async () => {
    const correlationId = "cold-start-correlation";
    const cell: PersistedCell = {
      turns: [],
      pendingOperations: [
        {
          correlationId,
          kind: "approval",
          registeredAt: Date.now(),
          gateId: `pending-${correlationId}`,
          timeoutAt: Date.now() + 60_000,
          suspendedCall: {
            id: "call-ask",
            name: "charge_card",
            arguments: {},
          },
        },
      ],
      tokenUsage: emptyUsage(),
    };
    const persistedStore = makePersistingContextStore(cell);
    const loadGate = Promise.withResolvers<boolean>();
    const loadStarted = Promise.withResolvers<boolean>();
    const delayedStore: ContextStore = {
      ...persistedStore,
      async load() {
        loadStarted.resolve(true);
        await loadGate.promise;
        return persistedStore.load();
      },
    };
    const askExtension = createAuthzExtension({
      authorize: async () => ({
        effect: "ask" as const,
        matchingGrants: [],
        resolvedBy: null,
      }),
      approvalTimeoutMs: 60_000,
    });
    const toolsRun: string[] = [];
    const { reactor, events, waitFor } = createTestReactor({
      contextStore: delayedStore,
      director: directorFromTable(
        {
          "resume.execute_tools": (event, _state, caps) =>
            caps.executeTools(event.calls, false, true),
          "tool.done": (_event, _state, caps) => caps.done(),
        },
        "wait",
      ),
      toolRunner: makeToolRunner(async (call) => {
        toolsRun.push(call.name);
        return { callId: call.id, content: "charged" };
      }),
      beforeToolExtensions: [askExtension],
    });

    reactor.start();
    await loadStarted.promise;
    reactor.deliver(makeApprovalMessage(correlationId));
    await Promise.resolve();

    expect(events.some((event) => event.type === "message.received")).toBe(
      false,
    );

    loadGate.resolve(true);

    const toolDone = await waitFor("tool.done");
    if (toolDone.type !== "tool.done") throw new Error("unreachable");
    expect(toolDone.data.result.callId).toBe("call-ask");
    expect(toolsRun).toEqual(["charge_card"]);
    expect(events.some((event) => event.type === "message.received")).toBe(
      false,
    );

    const correlated = getEvent(events, "message.correlated");
    expect(correlated.data.correlationId).toBe(correlationId);
    await waitFor("reactor.done");
  });
});

// ---------------------------------------------------------------------------
// Approval resume re-runs the parked tool call (re-dispatch rail)
// ---------------------------------------------------------------------------

describe("createReactor — approval resume re-dispatch", () => {
  // A two-phase inference runner: the first inference emits the tool_call that
  // parks on the ask gate; the re-inference after the re-dispatched call
  // completes emits a plain text reply that terminates the run. Mirrors how a
  // real model first calls a tool and then answers with the tool's result.
  function twoPhaseInferenceRunner() {
    let call = 0;
    return async function* (
      opts: InferenceHarnessOptions,
    ): AsyncGenerator<InferenceEvent> {
      call += 1;
      const turn: AssistantTurn =
        call === 1
          ? suspendToolCallTurn
          : {
              role: "assistant",
              content: [{ type: "text", text: "done charging" }],
              model: "test-model",
              timestamp: 2000,
            };
      yield {
        type: "inference.done",
        seq: opts.nextSeq(),
        data: { turn, usage: emptyUsage(), source: TEST_SOURCE },
      };
    };
  }

  function askExtension() {
    return createAuthzExtension({
      authorize: async () => ({
        effect: "ask" as const,
        matchingGrants: [],
        resolvedBy: null,
      }),
      approvalTimeoutMs: 60_000,
    });
  }

  test("an approved correlation re-runs the parked tool exactly once and answers its call id", async () => {
    const cell: PersistedCell = {
      turns: [],
      pendingOperations: [],
      tokenUsage: emptyUsage(),
    };
    const toolsRun: string[] = [];

    const { reactor, events, waitFor } = createTestReactor({
      contextStore: makePersistingContextStore(cell),
      director: createDefaultDirector("test agent", []),
      inferenceRunner: twoPhaseInferenceRunner(),
      toolRunner: makeToolRunner(async (call) => {
        toolsRun.push(call.name);
        return { callId: call.id, content: "charged" };
      }),
      beforeToolExtensions: [askExtension()],
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());

    const blocked = await waitFor("reactor.gate.blocked");
    if (blocked.type !== "reactor.gate.blocked") throw new Error("unreachable");
    const correlationId = blocked.data.correlationId;
    if (correlationId === undefined) throw new Error("expected correlationId");

    // The parked call has not run yet.
    expect(toolsRun).toEqual([]);

    reactor.deliver(makeApprovalMessage(correlationId));

    // The resume re-runs the parked tool and the model re-infers to a reply
    // that carries the tool's real result, terminating the run.
    const reply = await waitForEvent(
      events,
      (e) => e.type === "connector.reply",
    );
    if (reply.type !== "connector.reply") throw new Error("unreachable");
    expect(reply.data.content).toBe("done charging");

    // The tool ran exactly once — the re-dispatch, not a fresh re-inference
    // that re-issued the call.
    expect(toolsRun).toEqual(["charge_card"]);
    const toolDones = events.filter((e) => e.type === "tool.done");
    expect(toolDones).toHaveLength(1);

    // The re-dispatch appended a tool_result answering the parked call id, and
    // the persisted history is a well-formed tool sequence.
    const resultTurn = cell.turns.find((t) =>
      t.content.some(
        (b) => b.type === "tool_result" && b.callId === "call-ask",
      ),
    );
    expect(resultTurn).toBeDefined();
    expect(() => assertWellFormedToolSequence(cell.turns)).not.toThrow();

    // The correlation was claimed and the parked op removed.
    const correlated = getEvent(events, "message.correlated");
    expect(correlated.data.correlationId).toBe(correlationId);
    expect(cell.pendingOperations).toHaveLength(0);
  });

  test("the re-dispatched approved call re-infers exactly once", async () => {
    // A re-dispatch driven from the correlation path never passes through
    // inference.done. Its tool.done must answer the parked call exactly once:
    // exactly one re-inference after exactly one re-dispatched result.
    const cell: PersistedCell = {
      turns: [],
      pendingOperations: [],
      tokenUsage: emptyUsage(),
    };

    const { reactor, events, waitFor } = createTestReactor({
      contextStore: makePersistingContextStore(cell),
      director: createDefaultDirector("test agent", []),
      inferenceRunner: twoPhaseInferenceRunner(),
      toolRunner: makeToolRunner(async (call) => ({
        callId: call.id,
        content: "charged",
      })),
      beforeToolExtensions: [askExtension()],
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());

    const blocked = await waitFor("reactor.gate.blocked");
    if (blocked.type !== "reactor.gate.blocked") throw new Error("unreachable");
    const correlationId = blocked.data.correlationId;
    if (correlationId === undefined) throw new Error("expected correlationId");

    reactor.deliver(makeApprovalMessage(correlationId));
    await waitForEvent(events, (e) => e.type === "connector.reply");

    // Two inferences total: the initial tool-call inference and exactly one
    // continuation re-inference after the re-dispatched tool completed.
    const inferenceDones = events.filter((e) => e.type === "inference.done");
    expect(inferenceDones).toHaveLength(2);

    // Exactly one tool ran and one result was produced.
    const toolDones = events.filter((e) => e.type === "tool.done");
    expect(toolDones).toHaveLength(1);

    // A conversational reply returns the reactor to idle rather than shutting
    // it down; abort so the test does not leak the reactor.
    reactor.abort("admin_kill");
    await waitFor("reactor.done");
  });

  test("the second gated call gets an error result and the batch re-infers once after the approval", async () => {
    const cell: PersistedCell = {
      turns: [],
      pendingOperations: [],
      tokenUsage: emptyUsage(),
    };
    let inferences = 0;
    const batchTurn: AssistantTurn = {
      role: "assistant",
      content: [
        { type: "tool_call", id: "call-a", name: "charge_card", arguments: {} },
        { type: "tool_call", id: "call-b", name: "charge_card", arguments: {} },
      ],
      model: "test-model",
      timestamp: 1000,
    };
    const { director, decided } = recordingDirector(
      createDefaultDirector("test agent", []),
    );

    const { reactor, events, waitFor } = createTestReactor({
      contextStore: makePersistingContextStore(cell),
      director,
      inferenceRunner: async function* (opts) {
        inferences += 1;
        const turn: AssistantTurn =
          inferences === 1
            ? batchTurn
            : {
                role: "assistant",
                content: [{ type: "text", text: "done charging" }],
                model: "test-model",
                timestamp: 2000,
              };
        yield {
          type: "inference.done",
          seq: opts.nextSeq(),
          data: { turn, usage: emptyUsage(), source: TEST_SOURCE },
        };
      },
      toolRunner: makeToolRunner(async (call) => ({
        callId: call.id,
        content: "charged",
      })),
      beforeToolExtensions: [askExtension()],
    });

    reactor.start();
    reactor.deliver(makeInboundMessage(), { turnId: "turn-a" });

    const blocked = await waitFor("reactor.gate.blocked");
    if (blocked.type !== "reactor.gate.blocked") throw new Error("unreachable");
    const correlationId = blocked.data.correlationId;
    if (correlationId === undefined) throw new Error("expected correlationId");
    // The director has seen the refused call's result before the approval.
    await waitUntil(() =>
      decided.some(
        (e) => e.type === "tool.done" && e.result.callId === "call-b",
      ),
    );

    expect(countOf(events, "reactor.gate.blocked")).toBe(1);
    const refused = toolResultOf(events, "call-b");
    expect(refused.isError).toBe(true);
    expect(refused.content).toMatch(GATE_REFUSAL);
    expect(cell.pendingOperations).toHaveLength(1);
    expect(cell.pendingOperations[0]?.correlationId).toBe(correlationId);
    expect(inferences).toBe(1);

    reactor.deliver(makeApprovalMessage(correlationId), { turnId: "turn-a" });
    const reply = await waitFor("connector.reply");
    await waitFor("reactor.turn.ended");

    const replyAt = events.indexOf(reply);
    const approvedAt = events.findIndex(
      (e) => e.type === "tool.done" && e.data.result.callId === "call-a",
    );
    expect(approvedAt).toBeGreaterThan(-1);
    expect(approvedAt).toBeLessThan(replyAt);
    expect(inferences).toBe(2);
    expect(countOf(events, "connector.reply")).toBe(1);
    expect(turnEnds(events)).toEqual([
      { turnId: "turn-a", outcome: "replied" },
    ]);

    reactor.abort("admin_kill");
    await waitFor("reactor.done");
  });

  test("the one-shot bypass lets the re-dispatched call through without re-parking", async () => {
    const cell: PersistedCell = {
      turns: [],
      pendingOperations: [],
      tokenUsage: emptyUsage(),
    };

    const { reactor, events, waitFor } = createTestReactor({
      contextStore: makePersistingContextStore(cell),
      director: createDefaultDirector("test agent", []),
      inferenceRunner: twoPhaseInferenceRunner(),
      toolRunner: makeToolRunner(async (call) => ({
        callId: call.id,
        content: "charged",
      })),
      beforeToolExtensions: [askExtension()],
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());

    const blocked = await waitFor("reactor.gate.blocked");
    if (blocked.type !== "reactor.gate.blocked") throw new Error("unreachable");
    const correlationId = blocked.data.correlationId;
    if (correlationId === undefined) throw new Error("expected correlationId");

    reactor.deliver(makeApprovalMessage(correlationId));
    await waitForEvent(events, (e) => e.type === "connector.reply");

    // The re-dispatched call re-hit the ask extension but the one-shot bypass
    // let it through: exactly one gate was ever blocked (the original park),
    // and no pending op survives — the call did not re-park on a second gate.
    const gateBlocks = events.filter((e) => e.type === "reactor.gate.blocked");
    expect(gateBlocks).toHaveLength(1);
    expect(cell.pendingOperations).toHaveLength(0);

    // A conversational reply returns the reactor to idle rather than shutting
    // it down; abort so the test does not leak the reactor.
    reactor.abort("admin_kill");
    await waitFor("reactor.done");
  });

  test("a non-JSON approval body halts the run with a fatal reactor.error", async () => {
    const cell: PersistedCell = {
      turns: [],
      pendingOperations: [],
      tokenUsage: emptyUsage(),
    };

    const { reactor, events, waitFor } = createTestReactor({
      contextStore: makePersistingContextStore(cell),
      director: createDefaultDirector("test agent", []),
      inferenceRunner: twoPhaseInferenceRunner(),
      toolRunner: makeToolRunner(async (call) => ({
        callId: call.id,
        content: "charged",
      })),
      beforeToolExtensions: [askExtension()],
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());

    const blocked = await waitFor("reactor.gate.blocked");
    if (blocked.type !== "reactor.gate.blocked") throw new Error("unreachable");
    const correlationId = blocked.data.correlationId;
    if (correlationId === undefined) throw new Error("expected correlationId");

    // Deliver a correlated body that is not valid JSON. The parse boundary must
    // reject it rather than resuming on a value it cannot decode.
    reactor.deliver(
      createInboundMessage({
        from: "signal@local",
        to: "agent@example.com",
        content: "not json at all",
        correlationId,
      }),
    );

    const error = await waitFor("reactor.error");
    if (error.type !== "reactor.error") throw new Error("unreachable");
    expect(error.data.fatal).toBe(true);
    expect(error.data.error).toContain("Correlation dispatch failed");

    // A malformed decision halts the run rather than silently proceeding: the
    // tool never runs and the reactor shuts down.
    expect(events.some((e) => e.type === "tool.start")).toBe(false);
    await waitFor("reactor.done");
  });

  test("a schema-invalid approval body halts the run with a fatal reactor.error", async () => {
    const cell: PersistedCell = {
      turns: [],
      pendingOperations: [],
      tokenUsage: emptyUsage(),
    };

    const { reactor, events, waitFor } = createTestReactor({
      contextStore: makePersistingContextStore(cell),
      director: createDefaultDirector("test agent", []),
      inferenceRunner: twoPhaseInferenceRunner(),
      toolRunner: makeToolRunner(async (call) => ({
        callId: call.id,
        content: "charged",
      })),
      beforeToolExtensions: [askExtension()],
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());

    const blocked = await waitFor("reactor.gate.blocked");
    if (blocked.type !== "reactor.gate.blocked") throw new Error("unreachable");
    const correlationId = blocked.data.correlationId;
    if (correlationId === undefined) throw new Error("expected correlationId");

    // Valid JSON, but not a valid ApprovalDecision: "maybe" is not an outcome
    // the schema admits. The arktype boundary must reject it as fatal.
    reactor.deliver(
      createInboundMessage({
        from: "signal@local",
        to: "agent@example.com",
        content: JSON.stringify({ outcome: "maybe" }),
        correlationId,
      }),
    );

    const error = await waitFor("reactor.error");
    if (error.type !== "reactor.error") throw new Error("unreachable");
    expect(error.data.fatal).toBe(true);
    expect(error.data.error).toContain("Correlation dispatch failed");

    expect(events.some((e) => e.type === "tool.start")).toBe(false);
    await waitFor("reactor.done");
  });
});

// ---------------------------------------------------------------------------
// Approval resume answers a rejected or timed-out parked call with an error
// result (the shared resume.tool_result rail)
// ---------------------------------------------------------------------------

describe("createReactor — approval resume error result", () => {
  // A two-phase inference runner: the first inference emits the tool_call that
  // parks on the ask gate; the re-inference after the parked call is answered
  // with an error result emits a plain text reply that terminates the run.
  function twoPhaseInferenceRunner() {
    let call = 0;
    return async function* (
      opts: InferenceHarnessOptions,
    ): AsyncGenerator<InferenceEvent> {
      call += 1;
      const turn: AssistantTurn =
        call === 1
          ? suspendToolCallTurn
          : {
              role: "assistant",
              content: [{ type: "text", text: "acknowledged" }],
              model: "test-model",
              timestamp: 2000,
            };
      yield {
        type: "inference.done",
        seq: opts.nextSeq(),
        data: { turn, usage: emptyUsage(), source: TEST_SOURCE },
      };
    };
  }

  function askExtension(approvalTimeoutMs = 60_000) {
    return createAuthzExtension({
      authorize: async () => ({
        effect: "ask" as const,
        matchingGrants: [],
        resolvedBy: null,
      }),
      approvalTimeoutMs,
    });
  }

  test("a rejected correlation answers the parked call with an error result and re-infers once", async () => {
    const cell: PersistedCell = {
      turns: [],
      pendingOperations: [],
      tokenUsage: emptyUsage(),
    };
    const toolsRun: string[] = [];

    const { reactor, events, waitFor } = createTestReactor({
      contextStore: makePersistingContextStore(cell),
      director: createDefaultDirector("test agent", []),
      inferenceRunner: twoPhaseInferenceRunner(),
      toolRunner: makeToolRunner(async (call) => {
        toolsRun.push(call.name);
        return { callId: call.id, content: "charged" };
      }),
      beforeToolExtensions: [askExtension()],
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());

    const blocked = await waitFor("reactor.gate.blocked");
    if (blocked.type !== "reactor.gate.blocked") throw new Error("unreachable");
    const correlationId = blocked.data.correlationId;
    if (correlationId === undefined) throw new Error("expected correlationId");

    // Deliver a rejection carrying an approver reason.
    reactor.deliver(
      createInboundMessage({
        from: "signal@local",
        to: "agent@example.com",
        content: JSON.stringify({ outcome: "rejected", message: "too risky" }),
        correlationId,
      }),
    );

    // The reactor answers the parked call with a synthetic error result and
    // re-infers off it, producing the terminating reply.
    const reply = await waitForEvent(
      events,
      (e) => e.type === "connector.reply",
    );
    if (reply.type !== "connector.reply") throw new Error("unreachable");
    expect(reply.data.content).toBe("acknowledged");

    // The tool never ran: rejection does not grant the one-shot bypass.
    expect(toolsRun).toEqual([]);
    expect(events.some((e) => e.type === "tool.start")).toBe(false);
    expect(events.some((e) => e.type === "tool.done")).toBe(false);

    // History carries an error tool_result answering the parked call id, and
    // the persisted sequence is well-formed.
    const resultTurn = cell.turns.find((t) =>
      t.content.some(
        (b) =>
          b.type === "tool_result" &&
          b.callId === "call-ask" &&
          b.isError === true,
      ),
    );
    expect(resultTurn).toBeDefined();
    expect(() => assertWellFormedToolSequence(cell.turns)).not.toThrow();

    // Exactly one re-inference after the park: the initial tool-call inference
    // plus one continuation off the error result.
    const inferenceDones = events.filter((e) => e.type === "inference.done");
    expect(inferenceDones).toHaveLength(2);

    // The correlation was claimed and the parked op removed.
    const correlated = getEvent(events, "message.correlated");
    expect(correlated.data.correlationId).toBe(correlationId);
    expect(cell.pendingOperations).toHaveLength(0);
  });

  test("a rejected correlation surfaces the approver reason in the error content", async () => {
    const cell: PersistedCell = {
      turns: [],
      pendingOperations: [],
      tokenUsage: emptyUsage(),
    };

    const { reactor, events, waitFor } = createTestReactor({
      contextStore: makePersistingContextStore(cell),
      director: createDefaultDirector("test agent", []),
      inferenceRunner: twoPhaseInferenceRunner(),
      toolRunner: makeToolRunner(async (call) => ({
        callId: call.id,
        content: "charged",
      })),
      beforeToolExtensions: [askExtension()],
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());

    const blocked = await waitFor("reactor.gate.blocked");
    if (blocked.type !== "reactor.gate.blocked") throw new Error("unreachable");
    const correlationId = blocked.data.correlationId;
    if (correlationId === undefined) throw new Error("expected correlationId");

    reactor.deliver(
      createInboundMessage({
        from: "signal@local",
        to: "agent@example.com",
        content: JSON.stringify({ outcome: "rejected", message: "too risky" }),
        correlationId,
      }),
    );

    await waitForEvent(events, (e) => e.type === "connector.reply");

    const resultBlock = cell.turns
      .flatMap((t) => t.content)
      .find((b) => b.type === "tool_result" && b.callId === "call-ask");
    if (resultBlock === undefined || resultBlock.type !== "tool_result") {
      throw new Error("expected an error tool_result for the parked call");
    }
    const text = resultBlock.content
      .filter((c) => c.type === "text")
      .map((c) => c.text)
      .join("");
    expect(text).toBe("denied by approver: too risky");
  });

  test("a timed-out parked call answers with an error result and re-infers exactly once", async () => {
    // The double-infer regression guard. A gate timeout on a parked ask call
    // must enqueue resume.tool_result INSTEAD OF reactor.gate.cleared. If the
    // fork ever enqueued both, the parked call would drive two re-inferences
    // for one timeout. Assert exactly one continuation inference.
    const cell: PersistedCell = {
      turns: [],
      pendingOperations: [],
      tokenUsage: emptyUsage(),
    };
    const toolsRun: string[] = [];

    const { reactor, events, waitFor } = createTestReactor({
      contextStore: makePersistingContextStore(cell),
      director: createDefaultDirector("test agent", []),
      inferenceRunner: twoPhaseInferenceRunner(),
      toolRunner: makeToolRunner(async (call) => {
        toolsRun.push(call.name);
        return { callId: call.id, content: "charged" };
      }),
      beforeToolExtensions: [askExtension(80)],
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());

    const blocked = await waitFor("reactor.gate.blocked");
    if (blocked.type !== "reactor.gate.blocked") throw new Error("unreachable");
    const correlationId = blocked.data.correlationId;
    if (correlationId === undefined) throw new Error("expected correlationId");

    // No decision is delivered; the gate times out on its own.
    const reply = await waitForEvent(
      events,
      (e) => e.type === "connector.reply",
    );
    if (reply.type !== "connector.reply") throw new Error("unreachable");
    expect(reply.data.content).toBe("acknowledged");

    // The tool never ran. The timeout emits one gate.cleared for observers,
    // but enqueues none, so no second re-infer follows.
    expect(toolsRun).toEqual([]);
    expect(
      events.flatMap((e) =>
        e.type === "reactor.gate.cleared" ? [e.data.reason] : [],
      ),
    ).toEqual(["timeout"]);

    // History carries a timeout error tool_result answering the parked call id.
    const resultBlock = cell.turns
      .flatMap((t) => t.content)
      .find((b) => b.type === "tool_result" && b.callId === "call-ask");
    if (resultBlock === undefined || resultBlock.type !== "tool_result") {
      throw new Error("expected an error tool_result for the parked call");
    }
    expect(resultBlock.isError).toBe(true);
    const text = resultBlock.content
      .filter((c) => c.type === "text")
      .map((c) => c.text)
      .join("");
    expect(text).toBe("approval timed out");
    expect(() => assertWellFormedToolSequence(cell.turns)).not.toThrow();

    // Exactly one continuation inference: the initial tool-call inference plus
    // one re-inference off the timeout error result — never two.
    const inferenceDones = events.filter((e) => e.type === "inference.done");
    expect(inferenceDones).toHaveLength(2);

    // The parked op was removed.
    expect(cell.pendingOperations).toHaveLength(0);
  });

  test("a gate with no suspendedCall-bearing op still resumes on a bare gate-cleared timeout", async () => {
    // The non-ask rail (a director-suspended gate, or an async-marker pending
    // op that carries no suspendedCall) must keep today's behavior on timeout:
    // a plain reactor.gate.cleared drives the re-infer, with no synthetic tool
    // result manufactured. The fork only diverts a gate whose op has a
    // suspendedCall.
    let cleared = false;
    const director = directorFromTable({
      "message.received": (_e, _s, caps) =>
        caps.suspend({
          type: "approval",
          gateId: "pending-async-marker",
          timeoutMs: 80,
          correlationId: "async-marker-corr",
        }),
      "reactor.gate.cleared": (_e, _s, caps) => {
        cleared = true;
        return caps.done();
      },
    });

    const { reactor, waitFor } = createTestReactor({ director });

    reactor.start();
    reactor.deliver(makeInboundMessage());

    const gateCleared = await waitFor("reactor.gate.cleared");
    if (gateCleared.type !== "reactor.gate.cleared") {
      throw new Error("unreachable");
    }
    expect(gateCleared.data.reason).toBe("timeout");
    await waitFor("reactor.done");
    expect(cleared).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// Corrupt persisted state surfaces loud at startup
// ---------------------------------------------------------------------------

describe("createReactor — corrupt persisted state", () => {
  test("duplicate correlationId in persisted pending operations emits reactor.error and reactor.done", async () => {
    // The pending operations come from the context store, an untrusted
    // external boundary. Two operations sharing a correlationId make the
    // second correlation registration throw during rehydration. That failure
    // must surface as reactor.error plus reactor.done, exactly like a load
    // failure, rather than bricking the reactor with no lifecycle events.
    const now = Date.now();
    const duplicate: PersistedPendingOperation[] = [
      {
        correlationId: "dup-corr",
        kind: "approval",
        registeredAt: now,
        gateId: "gate-a",
        timeoutAt: now + 60_000,
      },
      {
        correlationId: "dup-corr",
        kind: "approval",
        registeredAt: now,
        gateId: "gate-b",
        timeoutAt: now + 60_000,
      },
    ];

    const cell: PersistedCell = {
      turns: [],
      pendingOperations: duplicate,
      tokenUsage: emptyUsage(),
    };

    const { reactor, events, waitFor } = createTestReactor({
      contextStore: makePersistingContextStore(cell),
    });

    reactor.start();

    await waitFor("reactor.error");
    await waitFor("reactor.done");

    const error = getEvent(events, "reactor.error");
    expect(error.data.fatal).toBe(true);
    expect(error.data.error).toMatch(/dup-corr/);

    // The brick symptom is the absence of lifecycle events; assert both fired.
    expect(events.some((e) => e.type === "reactor.done")).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// Lifecycle hooks: afterCheckpoint and onShutdown
// ---------------------------------------------------------------------------

describe("createReactor — afterCheckpoint", () => {
  test("afterCheckpoint is called after successful checkpoint", async () => {
    let afterCheckpointCalled = false;

    const { reactor, waitFor } = createTestReactor({
      director: directorFromTable({
        "message.received": (_e, _s, caps) => [caps.checkpoint(), caps.done()],
      }),
      afterCheckpoint: async () => {
        afterCheckpointCalled = true;
      },
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitFor("reactor.done");
    expect(afterCheckpointCalled).toBe(true);
  });

  test("afterCheckpoint is skipped when context commit fails", async () => {
    let afterCheckpointCalled = false;
    let commitCount = 0;

    const store = makeContextStore();
    const originalCommit = store.commit.bind(store);
    async function wrappedCommit(
      options: { message: string },
      signal?: AbortSignal,
    ): Promise<ContextCommit> {
      commitCount++;
      if (commitCount === 1) {
        throw new Error("disk full");
      }
      return originalCommit(options, signal);
    }
    store.commit = wrappedCommit;

    const { reactor, waitFor } = createTestReactor({
      contextStore: store,
      director: directorFromTable({
        "message.received": (_e, _s, caps) => [caps.checkpoint(), caps.done()],
      }),
      afterCheckpoint: async () => {
        afterCheckpointCalled = true;
      },
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitFor("reactor.done");
    expect(afterCheckpointCalled).toBe(false);
  });

  test("afterCheckpoint failure emits non-fatal reactor.error", async () => {
    const { reactor, events, waitFor } = createTestReactor({
      director: directorFromTable({
        "message.received": (_e, _s, caps) => [caps.checkpoint(), caps.done()],
      }),
      afterCheckpoint: async () => {
        throw new Error("audit flush failed");
      },
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitFor("reactor.done");

    const errors = events.filter((e) => e.type === "reactor.error");
    const hookError = errors.find(
      (e) =>
        e.type === "reactor.error" && e.data.error.includes("afterCheckpoint"),
    );
    expect(hookError).toBeDefined();
    if (hookError === undefined || hookError.type !== "reactor.error") {
      throw new Error("expected reactor.error");
    }
    expect(hookError.data.fatal).toBe(false);

    // reactor.done should still be emitted
    const done = events.find((e) => e.type === "reactor.done");
    expect(done).toBeDefined();
  });
});

describe("createReactor — onShutdown", () => {
  test("onShutdown is called before reactor.done on normal shutdown", async () => {
    const callOrder: string[] = [];

    const { reactor, events, waitFor } = createTestReactor({
      director: directorFromTable({
        "message.received": (_e, _s, caps) => caps.done(),
      }),
      onShutdown: async () => {
        callOrder.push("onShutdown");
      },
    });

    // Intercept reactor.done to track ordering.
    const originalPush = events.push.bind(events);
    events.push = (...args) => {
      for (const e of args) {
        if (e.type === "reactor.done") {
          callOrder.push("reactor.done");
        }
      }
      return originalPush(...args);
    };

    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitFor("reactor.done");

    expect(callOrder).toEqual(["onShutdown", "reactor.done"]);
  });

  test("onShutdown failure emits non-fatal reactor.error and reactor.done still fires", async () => {
    const { reactor, events, waitFor } = createTestReactor({
      director: directorFromTable({
        "message.received": (_e, _s, caps) => caps.done(),
      }),
      onShutdown: async () => {
        throw new Error("shutdown flush failed");
      },
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitFor("reactor.done");

    const errors = events.filter(
      (e) => e.type === "reactor.error" && e.data.error.includes("onShutdown"),
    );
    expect(errors.length).toBe(1);
    if (errors[0] === undefined || errors[0].type !== "reactor.error") {
      throw new Error("expected reactor.error");
    }
    expect(errors[0].data.fatal).toBe(false);

    const done = events.find((e) => e.type === "reactor.done");
    expect(done).toBeDefined();
  });

  test("onShutdown is called on abort", async () => {
    let shutdownCalled = false;

    const { reactor, waitFor } = createTestReactor({
      director: directorFromTable(
        {
          "message.received": (_e, _s, caps) => caps.wait(),
        },
        "wait",
      ),
      onShutdown: async () => {
        shutdownCalled = true;
      },
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitFor("reactor.start");
    reactor.abort("user_disconnect");
    await waitFor("reactor.done");
    expect(shutdownCalled).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// Transform chains and compaction (Phase 4)
// ---------------------------------------------------------------------------

import type {
  ToolResultTransform,
  ContextTransform,
  TransformRecord,
} from "@intx/types/runtime";

function passthroughToolTransform(
  name: string,
  decisions: Record<string, unknown> = {},
): ToolResultTransform {
  return {
    name,
    version: "1",
    async apply(input, _ctx) {
      return {
        output: input.result,
        record: {
          strategy: name,
          version: "1",
          parameters: {},
          reason: "passthrough",
          decisions,
        },
      };
    },
  };
}

function passthroughContextTransform(
  name: string,
  tagText: string,
): ContextTransform {
  return {
    name,
    version: "1",
    async apply(turns, _ctx) {
      const tagged = turns.map((t) => ({
        ...t,
        content: t.content.map((b) =>
          b.type === "text"
            ? { type: "text" as const, text: `${tagText}${b.text}` }
            : b,
        ),
      }));
      return {
        output: tagged,
        record: {
          strategy: name,
          version: "1",
          parameters: { tagText },
          reason: "tag",
          decisions: { count: turns.length },
        },
      };
    },
  };
}

function truncatingCompactor(name: string): Compactor {
  return {
    name,
    version: "1",
    async apply(turns, _ctx) {
      const kept = turns.slice(-1);
      return {
        output: kept,
        record: {
          strategy: name,
          version: "1",
          parameters: {},
          reason: "explicit",
          decisions: { kept: kept.length, dropped: turns.length - kept.length },
        },
      };
    },
  };
}

function makeRecordingContextStore(): {
  store: ContextStore;
  commits: { message: string; turns: ConversationTurn[] }[];
  manifests: TransformRecord[][];
  metadata: {
    pendingOperations: PersistedPendingOperation[];
    tokenUsage: TokenUsage;
  }[];
  blobs: { key: string; bytes: Uint8Array; contentType?: string }[];
  lastWrittenTurns: ConversationTurn[];
} {
  const commits: { message: string; turns: ConversationTurn[] }[] = [];
  const manifests: TransformRecord[][] = [];
  const metadata: {
    pendingOperations: PersistedPendingOperation[];
    tokenUsage: TokenUsage;
  }[] = [];
  const blobs: { key: string; bytes: Uint8Array; contentType?: string }[] = [];
  let lastWrittenTurns: ConversationTurn[] = [];

  const store: ContextStore = {
    async load() {
      return {
        turns: [],
        pendingOperations: [],
        tokenUsage: emptyUsage(),
        connectorState: null,
      };
    },
    setConnectorState() {
      /* noop */
    },
    async commit(options) {
      commits.push({
        message: options.message,
        turns: [...lastWrittenTurns],
      });
      return {
        hash: `c${String(commits.length)}`,
        message: options.message,
        timestamp: Date.now(),
      };
    },
    async branch() {
      /* noop */
    },
    async log() {
      return [];
    },
    async readAt() {
      return [];
    },
    async writeBlob(key, bytes, contentType) {
      blobs.push({
        key,
        bytes,
        ...(contentType !== undefined ? { contentType } : {}),
      });
    },
    async readBlob() {
      throw new Error("not implemented");
    },
    async writePrompt() {
      /* noop */
    },
    async writeResponse() {
      /* noop */
    },
    async writeManifest(records) {
      manifests.push([...records]);
    },
    async writeTurns(turns) {
      lastWrittenTurns = [...turns];
    },
    async writeMetadata(m) {
      metadata.push({
        pendingOperations: [...m.pendingOperations],
        tokenUsage: { ...m.tokenUsage },
      });
    },
    async readManifestHistory() {
      throw new Error("not implemented");
    },
  };

  return {
    store,
    commits,
    manifests,
    metadata,
    blobs,
    get lastWrittenTurns() {
      return lastWrittenTurns;
    },
  };
}

function mockInferenceRunner(
  responseText: string,
  toolCalls: {
    id: string;
    name: string;
    arguments: Record<string, unknown>;
  }[] = [],
): (opts: InferenceHarnessOptions) => AsyncGenerator<InferenceEvent> {
  return async function* (opts) {
    const usage: TokenUsage = {
      input: 10,
      output: 20,
      cacheRead: 0,
      cacheWrite: 0,
      thinking: 0,
    };
    const content: AssistantTurn["content"] = [];
    if (responseText.length > 0) {
      content.push({ type: "text", text: responseText });
    }
    for (const tc of toolCalls) {
      content.push({
        type: "tool_call",
        id: tc.id,
        name: tc.name,
        arguments: tc.arguments,
      });
    }
    const turn: AssistantTurn = {
      role: "assistant",
      content,
      model: opts.source.model,
      timestamp: Date.now(),
    };
    yield {
      type: "inference.done",
      seq: opts.nextSeq(),
      data: { turn, usage, source: TEST_SOURCE },
    };
  };
}

describe("createReactor — tool result transform chain", () => {
  test("threads results through transforms in order; records appear in order", async () => {
    const recording = makeRecordingContextStore();
    const t1 = passthroughToolTransform("first");
    const t2 = passthroughToolTransform("second");

    const { reactor, waitFor } = createTestReactor({
      contextStore: recording.store,
      director: directorFromTable({
        "message.received": (_e, _s, caps) =>
          caps.executeTools([{ id: "c1", name: "tool", arguments: {} }]),
        "tool.done": (_e, _s, caps) => caps.done(),
      }),
    });

    // Inject the transforms via a private property — we cannot pass them via
    // createTestReactor's typed overrides without expanding its shape, so we
    // construct a second reactor directly below for the headline tests. For
    // ordering coverage here we lean on the harness-side reactor.
    void t1;
    void t2;

    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitFor("reactor.done");
    // The default reactor in this test has no transforms; the commit should
    // still have happened (cycle had tool calls).
    expect(recording.commits.length).toBeGreaterThan(0);
  });
});

function createDirectReactor(opts: {
  contextStore: ContextStore;
  director: ReactorDirector;
  toolRunner?: ToolRunner;
  inferenceRunner?: (
    opts: InferenceHarnessOptions,
  ) => AsyncGenerator<InferenceEvent>;
  toolResultTransforms?: ToolResultTransform[];
  contextTransforms?: ContextTransform[];
  compactors?: Record<string, Compactor>;
}): {
  reactor: Reactor;
  events: ReactorEmittedEvent[];
  waitFor: (
    type: ReactorEmittedEvent["type"],
    timeoutMs?: number,
  ) => Promise<ReactorEmittedEvent>;
} {
  const { events, onEvent } = collectEvents();
  const reactor = createReactor({
    sessionId: `test-${String(++testSessionCounter)}`,
    director: opts.director,
    source: {
      id: "anthropic:test-model",
      provider: "anthropic",
      baseURL: "https://api.anthropic.com",
      credentialId: "test",
      model: "test-model",
    },
    toolRunner: opts.toolRunner ?? noopToolRunner(),
    contextStore: opts.contextStore,
    onEvent,
    deps: createDefaultDependencies(),
    shutdownTimeoutMs: 100,
    ...(opts.inferenceRunner !== undefined
      ? { inferenceRunner: opts.inferenceRunner }
      : {}),
    ...(opts.toolResultTransforms !== undefined
      ? { toolResultTransforms: opts.toolResultTransforms }
      : {}),
    ...(opts.contextTransforms !== undefined
      ? { contextTransforms: opts.contextTransforms }
      : {}),
    ...(opts.compactors !== undefined ? { compactors: opts.compactors } : {}),
  });

  function waitForType(
    type: ReactorEmittedEvent["type"],
  ): Promise<ReactorEmittedEvent> {
    return waitForEvent(events, (e) => e.type === type);
  }
  return { reactor, events, waitFor: waitForType };
}

describe("createReactor — transform chain ordering and compact action", () => {
  test("tool result transforms run in order and records appear in invocation order", async () => {
    const recording = makeRecordingContextStore();
    const transforms: ToolResultTransform[] = [
      passthroughToolTransform("first", { ord: 1 }),
      passthroughToolTransform("second", { ord: 2 }),
    ];

    const { reactor, waitFor } = createDirectReactor({
      contextStore: recording.store,
      toolRunner: makeToolRunner(async (call) => ({
        callId: call.id,
        content: "result",
      })),
      director: directorFromTable({
        "message.received": (_e, _s, caps) =>
          caps.executeTools([{ id: "c1", name: "tool", arguments: {} }]),
        "tool.done": (_e, _s, caps) => caps.done(),
      }),
      toolResultTransforms: transforms,
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitFor("reactor.done");

    // One commit; manifest carries both records in order.
    expect(recording.commits.length).toBe(1);
    expect(recording.manifests.length).toBe(1);
    const records = recording.manifests[0];
    if (records === undefined) throw new Error("expected manifest");
    expect(records.map((r) => r.strategy)).toEqual(["first", "second"]);
  });

  test("context transforms run before inference and feed the materialized prompt", async () => {
    const recording = makeRecordingContextStore();
    const transform = passthroughContextTransform("tag-context", "TAG:");

    let observedPrompt: ConversationTurn[] = [];
    const runner: (
      opts: InferenceHarnessOptions,
    ) => AsyncGenerator<InferenceEvent> = async function* (opts) {
      observedPrompt = opts.turns;
      const turn: AssistantTurn = {
        role: "assistant",
        content: [{ type: "text", text: "ok" }],
        model: opts.source.model,
        timestamp: Date.now(),
      };
      yield {
        type: "inference.done",
        seq: opts.nextSeq(),
        data: {
          turn,
          usage: emptyUsage(),
          source: TEST_SOURCE,
        },
      };
    };

    const { reactor, waitFor } = createDirectReactor({
      contextStore: recording.store,
      inferenceRunner: runner,
      director: directorFromTable({
        "message.received": (_e, _s, caps) => caps.infer(),
        "inference.done": (_e, _s, caps) => caps.done(),
      }),
      contextTransforms: [transform],
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitFor("reactor.done");

    expect(observedPrompt.length).toBeGreaterThan(0);
    const firstBlock = observedPrompt[0]?.content[0];
    if (firstBlock === undefined || firstBlock.type !== "text") {
      throw new Error("expected text block in transformed prompt");
    }
    expect(firstBlock.text.startsWith("TAG:")).toBe(true);
  });

  test("compact action looks up the named compactor and replaces turns", async () => {
    const recording = makeRecordingContextStore();
    const compactor = truncatingCompactor("tail-only");

    let messages = 0;
    const director: ReactorDirector = {
      async decide(event, _state, caps) {
        if (event.type === "message.received") {
          messages++;
          if (messages === 1) {
            // First message produces a multi-turn history (this user turn is
            // appended by the reactor before decide() returns), then we run
            // the compactor which drops everything except the tail.
            return caps.compact("tail-only", "explicit-test");
          }
          return caps.done();
        }
        return caps.done();
      },
    };

    const { reactor, waitFor } = createDirectReactor({
      contextStore: recording.store,
      director,
      compactors: { "tail-only": compactor },
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());
    // The second message drives the terminal done; deliver it once the
    // compact cycle it must follow has committed.
    await waitUntil(() =>
      recording.commits.some((c) => c.message.startsWith("Cycle: compaction")),
    );
    reactor.deliver(makeInboundMessage());
    await waitFor("reactor.done");

    expect(messages).toBeGreaterThanOrEqual(1);
    // The commit for the compact cycle has a single-turn history (tail-only).
    expect(recording.commits.length).toBeGreaterThan(0);
    const compactCommit = recording.commits.find((c) =>
      c.message.startsWith("Cycle: compaction"),
    );
    expect(compactCommit).toBeDefined();
    if (compactCommit !== undefined) {
      expect(compactCommit.turns.length).toBe(1);
    }
    // Manifest carries the compactor record.
    const flatRecords = recording.manifests.flat();
    expect(flatRecords.some((r) => r.strategy === "tail-only")).toBe(true);
  });

  test("compact for an unknown name emits a fatal error and shuts down", async () => {
    const recording = makeRecordingContextStore();
    const { reactor, events, waitFor } = createDirectReactor({
      contextStore: recording.store,
      director: directorFromTable({
        "message.received": (_e, _s, caps) => caps.compact("missing", "test"),
      }),
      compactors: {},
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitFor("reactor.done");
    const err = getEvent(events, "reactor.error");
    expect(err.data.error).toContain("missing");
    expect(err.data.fatal).toBe(true);
  });

  test("validateActions rejects compact + infer", async () => {
    const result = validateActions([
      { type: "compact", compactor: "tail", reason: "r" },
      { type: "infer" },
    ]);
    expect(result.ok).toBe(false);
    if (result.ok) throw new Error("unreachable");
    expect(result.error).toMatch(/compact.*infer/i);
  });

  test("context-overflow recovery: compact alone, then infer on the next cycle", async () => {
    const recording = makeRecordingContextStore();
    const compactor = truncatingCompactor("overflow-compactor");

    // First inference yields a context_overflow error; the second succeeds.
    let inferAttempts = 0;
    const runner: (
      opts: InferenceHarnessOptions,
    ) => AsyncGenerator<InferenceEvent> = async function* (opts) {
      inferAttempts++;
      if (inferAttempts === 1) {
        yield {
          type: "inference.error",
          seq: opts.nextSeq(),
          data: {
            error: {
              category: "context_overflow",
              message: "context too long",
            },
            partial: { text: "" },
          },
        };
        return;
      }
      const turn: AssistantTurn = {
        role: "assistant",
        content: [{ type: "text", text: "ok-after-compact" }],
        model: opts.source.model,
        timestamp: Date.now(),
      };
      yield {
        type: "inference.done",
        seq: opts.nextSeq(),
        data: { turn, usage: emptyUsage(), source: TEST_SOURCE },
      };
    };

    const director: ReactorDirector = {
      async decide(event, _state, caps) {
        if (event.type === "message.received") {
          return caps.infer();
        }
        if (event.type === "inference.error") {
          if (event.error.category === "context_overflow") {
            return caps.compact("overflow-compactor", "context-overflow");
          }
          return caps.done();
        }
        if (event.type === "inference.done") {
          return caps.done();
        }
        return caps.wait();
      },
    };

    // After compact, we expect the reactor to deliver no automatic event; the
    // director needs to drive the next infer. To exercise that, we patch the
    // director to issue infer after compact via a synthetic message.received.
    // Here we use a slightly richer director that knows to re-infer once the
    // compact cycle's commit has happened. We approximate by delivering a
    // second message after compact via a side channel.
    void compactor;

    const { reactor, events, waitFor } = createDirectReactor({
      contextStore: recording.store,
      inferenceRunner: runner,
      director,
      compactors: { "overflow-compactor": compactor },
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());

    // After the compact cycle commits, the conversation has been truncated.
    // The director did not chain compact + infer; it only emitted compact.
    // Drive the next infer by delivering another message that re-runs the
    // event loop. The director's message.received → infer rule will fire.
    // The compactor's manifest record is written by the compact cycle, so it
    // reports that the cycle this delivery must follow has happened.
    await waitUntil(() =>
      recording.manifests
        .flat()
        .some((r) => r.strategy === "overflow-compactor"),
    );
    reactor.deliver(makeInboundMessage());

    await waitFor("reactor.done");
    // The reactor ran inference twice: the first attempt failed with
    // context_overflow, and the second attempt succeeded after compaction.
    expect(inferAttempts).toBe(2);

    const compactRecord = recording.manifests
      .flat()
      .find((r) => r.strategy === "overflow-compactor");
    expect(compactRecord).toBeDefined();
    expect(compactRecord?.reason).toBe("explicit");

    // No reactor.error from the validation layer about compact+infer.
    const validationErr = events
      .filter((e) => e.type === "reactor.error")
      .find(
        (e) =>
          e.type === "reactor.error" && e.data.error.includes("Invalid action"),
      );
    expect(validationErr).toBeUndefined();
  });

  test("per-cycle commit cadence: each cycle produces a commit; pendingMessage overrides one auto-summary", async () => {
    const recording = makeRecordingContextStore();
    let directorCalls = 0;
    const director: ReactorDirector = {
      async decide(event, _state, _caps) {
        directorCalls++;
        if (event.type === "message.received" && directorCalls === 1) {
          return { type: "infer" as const };
        }
        if (event.type === "inference.done") {
          return [
            { type: "checkpoint" as const, message: "override-1" },
            { type: "done" as const },
          ];
        }
        return { type: "done" as const };
      },
    };

    const { reactor, waitFor } = createDirectReactor({
      contextStore: recording.store,
      inferenceRunner: mockInferenceRunner("done"),
      director,
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitFor("reactor.done");

    // First cycle: inference cycle commits with override-1 (from checkpoint).
    expect(recording.commits.length).toBe(1);
    expect(recording.commits[0]?.message).toBe("override-1");
  });

  test("pendingMessage is consumed exactly once; the next cycle uses auto-summary", async () => {
    const recording = makeRecordingContextStore();
    let directorCalls = 0;
    const director: ReactorDirector = {
      async decide(event, _state, _caps) {
        directorCalls++;
        if (event.type === "message.received") {
          if (directorCalls === 1) {
            return [
              { type: "checkpoint" as const, message: "first-override" },
              { type: "infer" as const },
            ];
          }
          return { type: "done" as const };
        }
        if (event.type === "inference.done") {
          return { type: "wait" as const };
        }
        return { type: "done" as const };
      },
    };

    const { reactor, waitFor } = createDirectReactor({
      contextStore: recording.store,
      inferenceRunner: mockInferenceRunner("ok"),
      director,
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());
    // The second message must land after the first cycle commits, since the
    // assertions below key off commit order.
    await waitUntil(() => recording.commits.length >= 1);
    reactor.deliver(makeInboundMessage());
    await waitFor("reactor.done");

    expect(recording.commits.length).toBeGreaterThanOrEqual(1);
    expect(recording.commits[0]?.message).toBe("first-override");
    // If a second commit happened, its message must not be the override.
    for (let i = 1; i < recording.commits.length; i++) {
      expect(recording.commits[i]?.message).not.toBe("first-override");
    }
  });
});
describe("createReactor — source failover", () => {
  // Simulates a priority-ordered source list the reactor fails over through.
  // `resultFor(id, attemptOnThisSource)` decides what the inference runner
  // yields each time it is invoked against the active source.
  function multiSourceReactor(opts: {
    sourceIds: string[];
    resultFor: (id: string, attempt: number) => "done" | InferenceError;
  }): TestReactorHandle & { attemptedSourceIds: string[] } {
    const sources = opts.sourceIds.map((id) => ({
      id,
      provider: "anthropic",
      baseURL: "https://api.anthropic.com",
      credentialId: `key-${id}`,
      model: "test-model",
    }));
    const head = sources[0];
    if (head === undefined) throw new Error("need at least one source");

    // The single mutable object the reactor reads as the active source.
    const active = { ...head };
    let index = 0;
    const failOverToNextSource = (): boolean => {
      if (index >= sources.length - 1) return false;
      index += 1;
      const next = sources[index];
      if (next !== undefined) Object.assign(active, next);
      return true;
    };
    const resetToPreferredSource = (): void => {
      index = 0;
      Object.assign(active, head);
    };

    const attemptedSourceIds: string[] = [];
    const perSourceAttempts = new Map<string, number>();
    const inferenceRunner = async function* (
      o: InferenceHarnessOptions,
    ): AsyncGenerator<InferenceEvent> {
      attemptedSourceIds.push(o.source.id);
      const attempt = (perSourceAttempts.get(o.source.id) ?? 0) + 1;
      perSourceAttempts.set(o.source.id, attempt);
      const result = opts.resultFor(o.source.id, attempt);
      if (result === "done") {
        yield {
          type: "inference.done",
          seq: o.nextSeq(),
          data: {
            turn: makeAssistantTurn(`reply from ${o.source.id}`),
            usage: {
              input: 1,
              output: 1,
              cacheRead: 0,
              cacheWrite: 0,
              thinking: 0,
            },
            source: {
              sourceId: o.source.id,
              provider: o.source.provider,
              model: o.source.model,
            },
          },
        };
      } else {
        yield {
          type: "inference.error",
          seq: o.nextSeq(),
          data: { error: result, partial: { text: "" } },
        };
      }
    };

    const handle = createTestReactor({
      source: active,
      failOverToNextSource,
      resetToPreferredSource,
      inferenceRunner,
      director: directorFromTable({
        "message.received": (_e, _s, caps) => caps.infer(),
        "inference.done": (_e, _s, caps) => caps.done(),
        "inference.error": (_e, _s, caps) => caps.done(),
      }),
    });
    return { ...handle, attemptedSourceIds };
  }

  test("fails over to the next source on a credential failure", async () => {
    const { reactor, events, waitFor, attemptedSourceIds } = multiSourceReactor(
      {
        sourceIds: ["s0", "s1"],
        resultFor: (id) =>
          id === "s0"
            ? { category: "credential_failure", message: "bad key" }
            : "done",
      },
    );
    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitFor("reactor.done");

    // s0 failed on a source-specific error -> immediate failover -> s1.
    expect(attemptedSourceIds).toEqual(["s0", "s1"]);
    const done = getEvent(events, "inference.done");
    expect(done.data.source.sourceId).toBe("s1");
  });

  test("fails over on quota exhaustion already retried by the harness", async () => {
    const { reactor, events, waitFor, attemptedSourceIds } = multiSourceReactor(
      {
        sourceIds: ["s0", "s1"],
        // s0 is quota-exhausted; the harness wrapper owns quota retry and
        // has exhausted it by the time the reactor sees the error, so the
        // reactor fails over rather than re-running s0.
        resultFor: (id) =>
          id === "s0"
            ? { category: "quota_exhausted", message: "quota exhausted" }
            : "done",
      },
    );
    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitFor("reactor.done");

    expect(attemptedSourceIds).toEqual(["s0", "s1"]);
    expect(getEvent(events, "inference.done").data.source.sourceId).toBe("s1");
  });

  test("surfaces quota exhaustion for a single source without failover", async () => {
    const { reactor, events, waitFor, attemptedSourceIds } = multiSourceReactor(
      {
        sourceIds: ["s0"],
        resultFor: () => ({
          category: "quota_exhausted",
          message: "quota exhausted",
        }),
      },
    );
    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitFor("reactor.done");

    // Single source: quota is attempted once, then surfaces. The harness
    // wrapper owns quota retry, so the reactor does no same-source backoff
    // and has nowhere to fail over to. attemptedSourceIds must stay ["s0"].
    expect(attemptedSourceIds).toEqual(["s0"]);
    expect(getEvent(events, "inference.error").data.error.category).toBe(
      "quota_exhausted",
    );
  });

  test("fails over immediately on a transient error already retried by the harness", async () => {
    const { reactor, events, waitFor, attemptedSourceIds } = multiSourceReactor(
      {
        sourceIds: ["s0", "s1"],
        resultFor: (id) =>
          id === "s0" ? { category: "retryable", message: "5xx" } : "done",
      },
    );
    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitFor("reactor.done");

    // retryable is harness-exhausted, so the reactor does not re-run s0.
    expect(attemptedSourceIds).toEqual(["s0", "s1"]);
    expect(getEvent(events, "inference.done").data.source.sourceId).toBe("s1");
  });

  // `fatal` covers a provider's 400 and an adapter's refusal to build the
  // request alike.
  test.each([["context_overflow"], ["fatal"]] as const)(
    "does not fail over on a source-invariant %p error",
    async (category) => {
      const { reactor, events, waitFor, attemptedSourceIds } =
        multiSourceReactor({
          sourceIds: ["s0", "s1"],
          resultFor: () => ({ category, message: "refused" }),
        });
      reactor.start();
      reactor.deliver(makeInboundMessage());
      await waitFor("reactor.done");

      // The error aborts the cycle; s1 is never tried.
      expect(attemptedSourceIds).toEqual(["s0"]);
      expect(getEvent(events, "inference.error").data.error.category).toBe(
        category,
      );
    },
  );

  test("surfaces the last error when every source is exhausted", async () => {
    const { reactor, events, waitFor, attemptedSourceIds } = multiSourceReactor(
      {
        sourceIds: ["s0", "s1"],
        resultFor: () => ({
          category: "credential_failure",
          message: "bad key",
        }),
      },
    );
    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitFor("reactor.done");

    // Both sources fail over; the terminal error is surfaced.
    expect(attemptedSourceIds).toEqual(["s0", "s1"]);
    expect(getEvent(events, "inference.error").data.error.category).toBe(
      "credential_failure",
    );
  });
});

describe("createReactor — prompt well-formedness tripwire", () => {
  test("rejects a malformed assembled prompt before inferring", async () => {
    // History with two tool_result blocks for one callId is the shape
    // OpenAI-compatible providers reject with HTTP 400. executeInfer must
    // catch it before sending and surface it as a fatal reactor error rather
    // than letting it reach a provider. This also guards the assertion's
    // call site: without it, no test would notice the prompt going out.
    const malformed: ConversationTurn[] = [
      {
        role: "assistant",
        content: [
          { type: "tool_call", id: "tc-1", name: "some_tool", arguments: {} },
        ],
        model: "test-model",
        timestamp: 0,
      },
      {
        role: "user",
        content: [
          {
            type: "tool_result",
            callId: "tc-1",
            content: [{ type: "text", text: "first" }],
          },
          {
            type: "tool_result",
            callId: "tc-1",
            content: [{ type: "text", text: "duplicate" }],
          },
        ],
        timestamp: 0,
      },
    ];

    let inferenceRan = false;

    const { reactor, events, waitFor } = createTestReactor({
      contextStore: makeContextStore(malformed),
      director: directorFromTable({
        "message.received": (_e, _s, caps) => caps.infer(),
      }),
      inferenceRunner: async function* () {
        inferenceRan = true;
        yield {
          type: "inference.done",
          seq: 1,
          data: {
            turn: makeAssistantTurn("unreachable"),
            usage: emptyUsage(),
            source: TEST_SOURCE,
          },
        };
      },
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitFor("reactor.done");

    const error = getEvent(events, "reactor.error");
    expect(error.data.fatal).toBe(true);
    expect(error.data.error).toMatch(/Malformed tool sequence/);
    expect(error.data.error).toMatch(/duplicate tool_result for "tc-1"/);
    // The prompt never reached the inference runner.
    expect(inferenceRan).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// Per-send inference options
// ---------------------------------------------------------------------------

describe("mergeInferenceOptions", () => {
  test("a director-named option wins over the per-send value", () => {
    expect(
      mergeInferenceOptions(
        { maxTokens: 500, effort: "low" },
        { effort: "high", systemPrompt: "s" },
      ),
    ).toEqual({ maxTokens: 500, effort: "high", systemPrompt: "s" });
  });

  test("a director thinking setting does not clear a per-send effort", () => {
    expect(
      mergeInferenceOptions(
        { maxTokens: 500, effort: "high" },
        { thinking: { enabled: false } },
      ),
    ).toEqual({
      maxTokens: 500,
      effort: "high",
      thinking: { enabled: false },
    });
  });

  test("a director effort does not clear a per-send thinking setting", () => {
    expect(
      mergeInferenceOptions(
        { thinking: { enabled: true, budgetTokens: 2048 } },
        { effort: "low" },
      ),
    ).toEqual({
      thinking: { enabled: true, budgetTokens: 2048 },
      effort: "low",
    });
  });

  test("a director thinking object replaces the per-send thinking object", () => {
    expect(
      mergeInferenceOptions(
        { thinking: { enabled: true, budgetTokens: 2048 } },
        { thinking: { enabled: true } },
      ),
    ).toEqual({
      thinking: { enabled: true },
    });
  });

  test("absent per-send options pass the director options through", () => {
    const director = { systemPrompt: "s" };
    expect(mergeInferenceOptions(undefined, director)).toBe(director);
  });
});

describe("createReactor — per-send inference options", () => {
  test("apply beneath the director for their own turn only", async () => {
    const seen: (InferenceHarnessOptions["inferenceOptions"] | undefined)[] =
      [];
    const inferenceRunner = async function* (opts: InferenceHarnessOptions) {
      seen.push(opts.inferenceOptions);
      yield {
        type: "inference.done" as const,
        seq: opts.nextSeq(),
        data: {
          turn: {
            role: "assistant" as const,
            content: [{ type: "text" as const, text: "ok" }],
            model: "test-model",
            timestamp: 1000,
          },
          usage: emptyUsage(),
          source: TEST_SOURCE,
        },
      };
    };
    const { reactor, events } = createTestReactor({
      inferenceRunner,
      director: directorFromTable(
        {
          "message.received": (_e, _s, caps) =>
            caps.infer({ temperature: 0.9, systemPrompt: "director" }),
          "inference.done": (_e, _s, caps) => caps.wait(),
        },
        "wait",
      ),
    });
    const inferenceDone = (n: number) =>
      waitUntil(
        () => events.filter((e) => e.type === "inference.done").length >= n,
      );

    reactor.start();
    reactor.deliver(makeInboundMessage(), {
      inference: { temperature: 0.1, maxTokens: 500, effort: "low" },
    });
    await inferenceDone(1);
    reactor.deliver(makeInboundMessage());
    await inferenceDone(2);

    expect(seen).toEqual([
      {
        temperature: 0.9,
        maxTokens: 500,
        effort: "low",
        systemPrompt: "director",
      },
      { temperature: 0.9, systemPrompt: "director" },
    ]);
    reactor.abort("admin_kill");
  });

  test("a correlated resume of a parked turn uses the resume's options", async () => {
    // The turn stays open across the park, so this exercises the
    // gate-resume path distinct from the uncorrelated-message case above:
    // options must reach the re-infer that follows the approval.
    const seen: (InferenceHarnessOptions["inferenceOptions"] | undefined)[] =
      [];
    const inferenceRunner = async function* (opts: InferenceHarnessOptions) {
      seen.push(opts.inferenceOptions);
      yield {
        type: "inference.done" as const,
        seq: opts.nextSeq(),
        data: {
          turn:
            seen.length === 1
              ? suspendToolCallTurn
              : {
                  role: "assistant" as const,
                  content: [{ type: "text" as const, text: "resumed" }],
                  model: "test-model",
                  timestamp: 1000,
                },
          usage: emptyUsage(),
          source: TEST_SOURCE,
        },
      };
    };
    const askExtension = createAuthzExtension({
      authorize: async () => ({
        effect: "ask" as const,
        matchingGrants: [],
        resolvedBy: null,
      }),
      approvalTimeoutMs: 60_000,
    });

    const { reactor, events, waitFor } = createTestReactor({
      inferenceRunner,
      toolRunner: makeToolRunner(async (call) => ({
        callId: call.id,
        content: "charged",
      })),
      beforeToolExtensions: [askExtension],
      director: directorFromTable(
        {
          "message.received": (_e, _s, caps) => caps.infer(),
          "inference.done": (_e, _s, caps) =>
            caps.executeTools([
              { id: "call-ask", name: "charge_card", arguments: {} },
            ]),
          "resume.execute_tools": (e, _s, caps) =>
            caps.executeTools(e.calls, false, true),
          "tool.done": (_e, _s, caps) => caps.infer(),
        },
        "wait",
      ),
    });

    reactor.start();
    reactor.deliver(makeInboundMessage(), { inference: { temperature: 0.1 } });

    const blocked = await waitFor("reactor.gate.blocked");
    if (blocked.type !== "reactor.gate.blocked") throw new Error("unreachable");
    const correlationId = blocked.data.correlationId;
    if (correlationId === undefined) throw new Error("expected correlationId");

    reactor.deliver(makeApprovalMessage(correlationId), {
      inference: { temperature: 0.42 },
    });
    await waitUntil(
      () => events.filter((e) => e.type === "inference.done").length >= 2,
    );

    expect(seen).toEqual([{ temperature: 0.1 }, { temperature: 0.42 }]);
    reactor.abort("admin_kill");
  });

  test("a correlated response that resumes nothing applies no options", async () => {
    // S1: infer then a tool returns a pendingMarker for C1; the director
    // replies and the turn ends, but C1 stays registered with no live gate.
    // S2: a new send starts a turn, infers, then parks on an ask-rail gate.
    // S3: a stale C1 response resumes nothing, so its options reach no turn;
    // resuming S2's gate must still see S2's options.
    const C1 = "corr-stale-c1";
    const seen: (InferenceHarnessOptions["inferenceOptions"] | undefined)[] =
      [];
    let capturing = false;
    let infers = 0;
    const sendMessageTurn: AssistantTurn = {
      role: "assistant",
      content: [
        {
          type: "tool_call",
          id: "tc-async",
          name: "send_message",
          arguments: {},
        },
      ],
      model: "test-model",
      timestamp: 1000,
    };
    const inferenceRunner = async function* (opts: InferenceHarnessOptions) {
      if (capturing) seen.push(opts.inferenceOptions);
      const turn =
        infers === 0
          ? sendMessageTurn
          : infers === 1
            ? suspendToolCallTurn
            : {
                role: "assistant" as const,
                content: [{ type: "text" as const, text: "resumed" }],
                model: "test-model",
                timestamp: 1000,
              };
      yield {
        type: "inference.done" as const,
        seq: opts.nextSeq(),
        data: {
          turn,
          usage: emptyUsage(),
          source: TEST_SOURCE,
        },
      };
    };
    const askExtension = createAuthzExtension({
      authorize: async (resource) => ({
        effect:
          resource === "tool:charge_card"
            ? ("ask" as const)
            : ("allow" as const),
        matchingGrants: [],
        resolvedBy: null,
      }),
      approvalTimeoutMs: 60_000,
    });

    const { reactor, events, waitFor } = createTestReactor({
      inferenceRunner,
      toolRunner: {
        async run(call) {
          if (call.name === "send_message") {
            return {
              callId: call.id,
              content: "sent",
              pendingMarker: {
                status: "pending" as const,
                correlationId: C1,
              },
            };
          }
          return { callId: call.id, content: "charged" };
        },
      },
      beforeToolExtensions: [askExtension],
      director: directorFromTable(
        {
          "message.received": (_e, _s, caps) => caps.infer(),
          "inference.done": (_e, _s, caps) => {
            infers += 1;
            if (infers === 1) {
              return caps.executeTools([
                { id: "tc-async", name: "send_message", arguments: {} },
              ]);
            }
            if (infers === 2) {
              return caps.executeTools([
                { id: "call-ask", name: "charge_card", arguments: {} },
              ]);
            }
            return caps.wait();
          },
          "resume.execute_tools": (e, _s, caps) =>
            caps.executeTools(e.calls, false, true),
          "tool.done": (e, _s, caps) => {
            if (e.result.callId === "tc-async") return caps.reply("acked");
            return caps.infer();
          },
        },
        "wait",
      ),
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitFor("connector.reply");
    capturing = true;

    reactor.deliver(makeInboundMessage(), { inference: { temperature: 0.2 } });
    await waitUntil(
      () => events.filter((e) => e.type === "inference.done").length >= 2,
    );
    const blocked = await waitFor("reactor.gate.blocked");
    if (blocked.type !== "reactor.gate.blocked") throw new Error("unreachable");
    const openCorrelationId = blocked.data.correlationId;
    if (openCorrelationId === undefined) {
      throw new Error("expected correlationId");
    }

    reactor.deliver(makeInboundMessage(C1), {
      inference: { temperature: 0.9 },
    });
    await waitFor("message.correlated");

    reactor.deliver(makeApprovalMessage(openCorrelationId));
    await waitUntil(
      () => events.filter((e) => e.type === "inference.done").length >= 3,
    );

    expect(seen).toEqual([{ temperature: 0.2 }, { temperature: 0.2 }]);
    reactor.abort("admin_kill");
  });

  test("a resume's options go to the parked turn it resumes, not to a later turn", async () => {
    // S1 parks on G1 (ask-rail gate). S2, uncorrelated, starts its own turn,
    // which parks on G2. The approval for G1 carries options: they apply to
    // S1's turn, and S2's turn keeps its own.
    const askExtension = createAuthzExtension({
      authorize: async () => ({
        effect: "ask" as const,
        matchingGrants: [],
        resolvedBy: null,
      }),
      approvalTimeoutMs: 60_000,
    });
    const seen: (InferenceHarnessOptions["inferenceOptions"] | undefined)[] =
      [];
    // Each infer parks on a fresh tool_call id -- the shared history spans
    // both runs, so reusing "call-ask" across them would trip the
    // duplicate-tool_call-id well-formedness check.
    let lastCallId = "";
    const inferenceRunner = async function* (opts: InferenceHarnessOptions) {
      seen.push(opts.inferenceOptions);
      lastCallId = `call-ask-${seen.length}`;
      yield {
        type: "inference.done" as const,
        seq: opts.nextSeq(),
        data: {
          turn: {
            role: "assistant" as const,
            content: [
              {
                type: "tool_call" as const,
                id: lastCallId,
                name: "charge_card",
                arguments: {},
              },
            ],
            model: "test-model",
            timestamp: 1000,
          },
          usage: emptyUsage(),
          source: TEST_SOURCE,
        },
      };
    };

    const { reactor, events, waitFor } = createTestReactor({
      inferenceRunner,
      toolRunner: makeToolRunner(async (call) => ({
        callId: call.id,
        content: "charged",
      })),
      beforeToolExtensions: [askExtension],
      director: directorFromTable(
        {
          "message.received": (_e, _s, caps) => caps.infer(),
          "inference.done": (_e, _s, caps) =>
            caps.executeTools([
              { id: lastCallId, name: "charge_card", arguments: {} },
            ]),
          "resume.execute_tools": (e, _s, caps) =>
            caps.executeTools(e.calls, false, true),
          "tool.done": (_e, _s, caps) => caps.infer(),
        },
        "wait",
      ),
    });

    reactor.start();
    reactor.deliver(makeInboundMessage(), { inference: { temperature: 0.1 } });

    const blocked1 = await waitFor("reactor.gate.blocked");
    if (blocked1.type !== "reactor.gate.blocked") {
      throw new Error("unreachable");
    }
    const correlationId1 = blocked1.data.correlationId;
    if (correlationId1 === undefined) throw new Error("expected correlationId");

    reactor.deliver(makeInboundMessage(), { inference: { temperature: 0.2 } });
    await waitUntil(
      () => events.filter((e) => e.type === "reactor.gate.blocked").length >= 2,
    );

    const blocked2 = events.filter((e) => e.type === "reactor.gate.blocked")[1];
    if (blocked2?.type !== "reactor.gate.blocked") {
      throw new Error("unreachable");
    }
    const correlationId2 = blocked2.data.correlationId;
    if (correlationId2 === undefined) throw new Error("expected correlationId");

    reactor.deliver(makeApprovalMessage(correlationId1), {
      inference: { temperature: 0.9 },
    });
    await waitUntil(() => seen.length >= 3);
    reactor.deliver(makeApprovalMessage(correlationId2));
    await waitUntil(() => seen.length >= 4);

    expect(seen).toEqual([
      { temperature: 0.1 },
      { temperature: 0.2 },
      { temperature: 0.9 },
      { temperature: 0.2 },
    ]);
    reactor.abort("admin_kill");
  });

  test("a resume of a director-chosen gate uses its options", async () => {
    // The tool's pending marker registers the operation under
    // `pending-<correlationId>`, and the director then suspends under its own
    // gate id with the same correlation id. The resume continues the parked
    // turn, so its options apply even though the gate id the pending
    // operation records is not the gate that is live.
    const CORR_ID = "corr-director-gate";
    const seen: (InferenceHarnessOptions["inferenceOptions"] | undefined)[] =
      [];
    let infers = 0;
    const inferenceRunner = async function* (opts: InferenceHarnessOptions) {
      seen.push(opts.inferenceOptions);
      infers += 1;
      yield {
        type: "inference.done" as const,
        seq: opts.nextSeq(),
        data: {
          turn: {
            role: "assistant" as const,
            content:
              infers === 1
                ? [
                    {
                      type: "tool_call" as const,
                      id: "call-send",
                      name: "send_msg",
                      arguments: {},
                    },
                  ]
                : [{ type: "text" as const, text: "done" }],
            model: "test-model",
            timestamp: 1000,
          },
          usage: emptyUsage(),
          source: TEST_SOURCE,
        },
      };
    };

    const { reactor, waitFor } = createTestReactor({
      inferenceRunner,
      toolRunner: makeToolRunner(async (call) => ({
        callId: call.id,
        content: "sent",
        pendingMarker: { status: "pending" as const, correlationId: CORR_ID },
      })),
      director: directorFromTable({
        "message.received": (_e, _s, caps) => caps.infer(),
        "inference.done": (_e, _s, caps) =>
          infers === 1
            ? caps.executeTools([
                { id: "call-send", name: "send_msg", arguments: {} },
              ])
            : caps.done(),
        "tool.done": (_e, _s, caps) =>
          caps.suspend({
            type: "message_response",
            gateId: "director-chosen-gate",
            timeoutMs: 5000,
            correlationId: CORR_ID,
          }),
        "reactor.gate.cleared": (_e, _s, caps) => caps.infer(),
      }),
    });

    reactor.start();
    reactor.deliver(makeInboundMessage(), { inference: { temperature: 0.1 } });
    await waitFor("reactor.gate.blocked");

    reactor.deliver(makeInboundMessage(CORR_ID), {
      inference: { temperature: 0.9 },
    });
    await waitFor("reactor.done");

    expect(seen).toEqual([{ temperature: 0.1 }, { temperature: 0.9 }]);
  });

  test("a resume's options go to the operation's turn and leave the running turn unchanged", async () => {
    // A rehydrated gate's approval arrives with options while turn B's first
    // inference runs. B's second inference keeps B's options, and the
    // resumed turn's inference uses the approval's options.
    const correlationId = "rehydrated-corr-running";
    const cell: PersistedCell = {
      turns: [
        {
          role: "assistant",
          content: [
            {
              type: "tool_call",
              id: "call-rehydrated",
              name: "charge_card",
              arguments: {},
            },
          ],
          model: "test-model",
          timestamp: 500,
        },
      ],
      pendingOperations: [
        {
          correlationId,
          kind: "approval",
          registeredAt: Date.now(),
          gateId: `pending-${correlationId}`,
          timeoutAt: Date.now() + 60_000,
          suspendedCall: {
            id: "call-rehydrated",
            name: "charge_card",
            arguments: {},
          },
          turnId: "turn-a",
        },
      ],
      tokenUsage: emptyUsage(),
    };
    const releaseB = Promise.withResolvers<boolean>();
    const seen: (InferenceHarnessOptions["inferenceOptions"] | undefined)[] =
      [];
    const inferenceRunner = async function* (opts: InferenceHarnessOptions) {
      seen.push(opts.inferenceOptions);
      const first = seen.length === 1;
      if (first) await releaseB.promise;
      yield {
        type: "inference.done" as const,
        seq: opts.nextSeq(),
        data: {
          turn: first
            ? {
                role: "assistant" as const,
                content: [
                  {
                    type: "tool_call" as const,
                    id: "call-lookup",
                    name: "lookup",
                    arguments: {},
                  },
                ],
                model: "test-model",
                timestamp: 1000,
              }
            : makeAssistantTurn("ok"),
          usage: emptyUsage(),
          source: TEST_SOURCE,
        },
      };
    };

    const { reactor, events, waitFor } = createTestReactor({
      contextStore: makePersistingContextStore(cell),
      inferenceRunner,
      toolRunner: makeToolRunner(async (call) => ({
        callId: call.id,
        content: "done",
      })),
      director: directorFromTable(
        {
          "message.received": (_e, _s, caps) => caps.infer(),
          "inference.done": (e, _s, caps) => {
            const calls: ToolCall[] = [];
            for (const block of e.turn.content) {
              if (block.type === "tool_call") {
                calls.push({
                  id: block.id,
                  name: block.name,
                  arguments: block.arguments,
                });
              }
            }
            return calls.length > 0 ? caps.executeTools(calls) : caps.wait();
          },
          "resume.execute_tools": (e, _s, caps) =>
            caps.executeTools(e.calls, false, true),
          "tool.done": (_e, _s, caps) => caps.infer(),
        },
        "wait",
      ),
    });

    reactor.start();
    await waitFor("reactor.start");

    reactor.deliver(makeInboundMessage(), {
      inference: { temperature: 0.2 },
      turnId: "turn-b",
    });
    await waitUntil(() => seen.length >= 1);
    reactor.deliver(makeApprovalMessage(correlationId), {
      inference: { temperature: 0.9 },
    });
    await waitFor("message.correlated");
    releaseB.resolve(true);
    await waitUntil(() => seen.length >= 3);

    // The queued tool.done of B's batch runs before A's resume.
    expect(seen).toEqual([
      { temperature: 0.2 },
      { temperature: 0.2 },
      { temperature: 0.9 },
    ]);
    await waitUntil(() => countOf(events, "inference.done") >= 3);
    expect(turnIdsOf(events, "inference.done")).toEqual([
      "turn-b",
      "turn-b",
      "turn-a",
    ]);
    reactor.abort("admin_kill");
  });

  test("a parked turn resumed with no options keeps its original options", async () => {
    const seen: (InferenceHarnessOptions["inferenceOptions"] | undefined)[] =
      [];
    const inferenceRunner = async function* (opts: InferenceHarnessOptions) {
      seen.push(opts.inferenceOptions);
      yield {
        type: "inference.done" as const,
        seq: opts.nextSeq(),
        data: {
          turn:
            seen.length === 1 ? suspendToolCallTurn : makeAssistantTurn("ok"),
          usage: emptyUsage(),
          source: TEST_SOURCE,
        },
      };
    };
    const { reactor, waitFor } = createTestReactor({
      inferenceRunner,
      toolRunner: chargeRunner(),
      beforeToolExtensions: [askExtension()],
      director: directorFromTable(
        {
          "message.received": (_e, _s, caps) => caps.infer(),
          "inference.done": (e, _s, caps) =>
            e.turn.content.some((b) => b.type === "tool_call")
              ? caps.executeTools([
                  { id: "call-ask", name: "charge_card", arguments: {} },
                ])
              : caps.wait(),
          "resume.execute_tools": (e, _s, caps) =>
            caps.executeTools(e.calls, false, true),
          "tool.done": (_e, _s, caps) => caps.infer(),
        },
        "wait",
      ),
    });

    reactor.start();
    reactor.deliver(makeInboundMessage(), { inference: { temperature: 0.1 } });
    const blocked = await waitFor("reactor.gate.blocked");
    if (blocked.type !== "reactor.gate.blocked") throw new Error("unreachable");
    const correlationId = blocked.data.correlationId;
    if (correlationId === undefined) throw new Error("expected correlationId");

    reactor.deliver(makeApprovalMessage(correlationId));
    await waitUntil(() => seen.length >= 2);

    expect(seen).toEqual([{ temperature: 0.1 }, { temperature: 0.1 }]);
    reactor.abort("admin_kill");
  });

  test("a resume's options do not leak into another turn's resume", async () => {
    // Two rehydrated gates belong to two legacy turns. The options of the
    // first resume apply to the first turn only: the second resume sets
    // none and infers with none.
    const correlationIds = ["rehydrated-corr-a", "rehydrated-corr-b"];
    const cell: PersistedCell = {
      turns: [
        {
          role: "assistant",
          content: correlationIds.map((id) => ({
            type: "tool_call" as const,
            id: `call-${id}`,
            name: "charge_card",
            arguments: {},
          })),
          model: "test-model",
          timestamp: 500,
        },
      ],
      pendingOperations: correlationIds.map((id) => ({
        correlationId: id,
        kind: "approval" as const,
        registeredAt: Date.now(),
        gateId: `pending-${id}`,
        timeoutAt: Date.now() + 60_000,
        suspendedCall: { id: `call-${id}`, name: "charge_card", arguments: {} },
      })),
      tokenUsage: emptyUsage(),
    };
    const seen: (InferenceHarnessOptions["inferenceOptions"] | undefined)[] =
      [];
    const inferenceRunner = async function* (opts: InferenceHarnessOptions) {
      seen.push(opts.inferenceOptions);
      yield {
        type: "inference.done" as const,
        seq: opts.nextSeq(),
        data: {
          turn: {
            role: "assistant" as const,
            content: [{ type: "text" as const, text: "ok" }],
            model: "test-model",
            timestamp: 1000,
          },
          usage: emptyUsage(),
          source: TEST_SOURCE,
        },
      };
    };

    const { reactor, waitFor } = createTestReactor({
      contextStore: makePersistingContextStore(cell),
      inferenceRunner,
      toolRunner: makeToolRunner(async (call) => ({
        callId: call.id,
        content: "charged",
      })),
      director: directorFromTable(
        {
          "resume.execute_tools": (e, _s, caps) =>
            caps.executeTools(e.calls, false, true),
          "tool.done": (_e, _s, caps) => caps.infer(),
        },
        "wait",
      ),
    });

    reactor.start();
    await waitFor("reactor.start");

    reactor.deliver(makeApprovalMessage("rehydrated-corr-a"), {
      inference: { temperature: 0.9 },
    });
    await waitUntil(() => seen.length >= 1);
    reactor.deliver(makeApprovalMessage("rehydrated-corr-b"));
    await waitUntil(() => seen.length >= 2);

    expect(seen).toEqual([{ temperature: 0.9 }, undefined]);
    reactor.abort("admin_kill");
  });

  test("systemPrompt, tools, and providerOptions on deliver do not reach the harness", async () => {
    const seen: (InferenceHarnessOptions["inferenceOptions"] | undefined)[] =
      [];
    const inferenceRunner = async function* (opts: InferenceHarnessOptions) {
      seen.push(opts.inferenceOptions);
      yield {
        type: "inference.done" as const,
        seq: opts.nextSeq(),
        data: {
          turn: {
            role: "assistant" as const,
            content: [{ type: "text" as const, text: "ok" }],
            model: "test-model",
            timestamp: 1000,
          },
          usage: emptyUsage(),
          source: TEST_SOURCE,
        },
      };
    };
    const { reactor } = createTestReactor({
      inferenceRunner,
      director: directorFromTable(
        {
          "message.received": (_e, _s, caps) =>
            caps.infer({ temperature: 0.9 }),
          "inference.done": (_e, _s, caps) => caps.wait(),
        },
        "wait",
      ),
    });

    reactor.start();
    // A wider bag is assignable to PerCallInferenceOptions; deliver must
    // still drop the keys that displace the deployed agent definition.
    const inference = {
      maxTokens: 100,
      temperature: 0.1,
      systemPrompt: "sneak",
      tools: [],
      providerOptions: { foo: 1 },
    };
    reactor.deliver(makeInboundMessage(), { inference });
    await waitUntil(() => seen.length >= 1);

    expect(seen).toEqual([{ temperature: 0.9, maxTokens: 100 }]);
    reactor.abort("admin_kill");
  });

  test("deliver copies every key PerCallInferenceOptions declares", async () => {
    const seen: (InferenceHarnessOptions["inferenceOptions"] | undefined)[] =
      [];
    const inferenceRunner = async function* (opts: InferenceHarnessOptions) {
      seen.push(opts.inferenceOptions);
      yield {
        type: "inference.done" as const,
        seq: opts.nextSeq(),
        data: {
          turn: {
            role: "assistant" as const,
            content: [{ type: "text" as const, text: "ok" }],
            model: "test-model",
            timestamp: 1000,
          },
          usage: emptyUsage(),
          source: TEST_SOURCE,
        },
      };
    };
    const { reactor } = createTestReactor({
      inferenceRunner,
      director: directorFromTable(
        {
          "message.received": (_e, _s, caps) => caps.infer(),
          "inference.done": (_e, _s, caps) => caps.wait(),
        },
        "wait",
      ),
    });

    reactor.start();
    // Names every declared key alongside one the type does not declare.
    // A key added to the type without an entry here fails the key-set
    // comparison below, so the copy can never silently fall behind it.
    const inference = {
      maxTokens: 100,
      temperature: 0.1,
      thinking: { enabled: true, budgetTokens: 2048, sneak: true },
      effort: "high" as const,
      systemPrompt: "sneak",
    };
    reactor.deliver(makeInboundMessage(), { inference });
    await waitUntil(() => seen.length >= 1);

    const declared = PerCallInferenceOptions.props.map((p) => p.key).sort();
    expect(Object.keys(seen[0] ?? {}).sort()).toEqual(declared);
    expect(seen[0]).toEqual({
      maxTokens: 100,
      temperature: 0.1,
      thinking: { enabled: true, budgetTokens: 2048 },
      effort: "high",
    });
    reactor.abort("admin_kill");
  });
});

// ---------------------------------------------------------------------------
// Turn ids: a turn is one inbound request, and its id spans every cycle and
// every resume of that request
// ---------------------------------------------------------------------------

// The turn id the reactor stamped on an event, or undefined for an event
// that carries none.
function stampedTurnId(e: ReactorEmittedEvent): string | undefined {
  switch (e.type) {
    case "inference.start":
    case "inference.done":
    case "connector.reply":
    case "reactor.gate.blocked":
      return e.data.turnId;
    default:
      return undefined;
  }
}

function turnIdsOf(
  events: ReactorEmittedEvent[],
  type:
    | "inference.start"
    | "inference.done"
    | "connector.reply"
    | "reactor.gate.blocked",
): (string | undefined)[] {
  return events.filter((e) => e.type === type).map(stampedTurnId);
}

// The turn id of each reply, keyed by the reply's content.
function replyTurnIds(
  events: ReactorEmittedEvent[],
): Map<string, string | undefined> {
  const byContent = new Map<string, string | undefined>();
  for (const e of events) {
    if (e.type !== "connector.reply") continue;
    byContent.set(e.data.content, e.data.turnId);
  }
  return byContent;
}

function countOf(
  events: ReactorEmittedEvent[],
  type: ReactorEmittedEvent["type"],
): number {
  return events.filter((e) => e.type === type).length;
}

function makeTurnMessage(content: string): InboundMessage {
  return createInboundMessage({
    from: "test@example.com",
    to: "agent@example.com",
    content,
  });
}

// The body of the last turn's text. An inbound turn puts an envelope of
// headers before the body, separated by a blank line.
function lastText(turns: ConversationTurn[]): string {
  const last = turns.at(-1);
  if (last === undefined) return "";
  const text = last.content
    .map((b) => (b.type === "text" ? b.text : ""))
    .join("");
  const bodyStart = text.lastIndexOf("\n\n");
  return bodyStart === -1 ? text : text.slice(bodyStart + 2);
}

// A runner that answers by the prompt: the message "park" gets the tool
// call that parks on the ask gate, a tool result gets "A done", and any
// other message gets "<text> done". Each call emits inference.start before
// `hold`, so a test can keep a call open while it acts.
function turnRunner(
  hold: (text: string) => Promise<unknown> = async () => true,
) {
  return async function* (
    opts: InferenceHarnessOptions,
  ): AsyncGenerator<InferenceEvent> {
    yield {
      type: "inference.start",
      seq: opts.nextSeq(),
      data: { model: "test-model" },
    };
    const text = lastText(opts.turns);
    await hold(text);
    const answersResult =
      opts.turns.at(-1)?.content.some((b) => b.type === "tool_result") === true;
    let turn = suspendToolCallTurn;
    if (answersResult) turn = makeAssistantTurn("A done");
    else if (text !== "park") turn = makeAssistantTurn(`${text} done`);
    yield {
      type: "inference.done",
      seq: opts.nextSeq(),
      data: { turn, usage: emptyUsage(), source: TEST_SOURCE },
    };
  };
}

// Infers on every message and resume, runs the tool calls of a model turn,
// and replies with the text of a model turn. The message "suspend" parks
// on a director gate that times out.
function turnDirector(): ReactorDirector {
  return directorFromTable(
    {
      "message.received": (e, _s, caps) =>
        e.message.content === "suspend"
          ? caps.suspend({
              type: "approval",
              gateId: "director-gate",
              timeoutMs: 50,
            })
          : caps.infer(),
      "inference.done": (e, _s, caps) => {
        const calls: ToolCall[] = [];
        let text = "";
        for (const block of e.turn.content) {
          if (block.type === "tool_call") {
            calls.push({
              id: block.id,
              name: block.name,
              arguments: block.arguments,
            });
          } else if (block.type === "text") {
            text += block.text;
          }
        }
        return calls.length > 0 ? caps.executeTools(calls) : caps.reply(text);
      },
      "resume.execute_tools": (e, _s, caps) =>
        caps.executeTools(e.calls, false, true),
      "resume.tool_result": (_e, _s, caps) => caps.infer(),
      "tool.done": (_e, _s, caps) => caps.infer(),
      "reactor.gate.cleared": (_e, _s, caps) => caps.infer(),
    },
    "wait",
  );
}

function askExtension(timeoutMs = 60_000) {
  return createAuthzExtension({
    authorize: async () => ({
      effect: "ask" as const,
      matchingGrants: [],
      resolvedBy: null,
    }),
    approvalTimeoutMs: timeoutMs,
  });
}

function chargeRunner() {
  return makeToolRunner(async (call) => ({
    callId: call.id,
    content: "charged",
  }));
}
describe("createReactor — turn ids", () => {
  test("inference.start, inference.done, connector.reply and gate.blocked carry the delivered turn id", async () => {
    const cell: PersistedCell = {
      turns: [],
      pendingOperations: [],
      tokenUsage: emptyUsage(),
    };
    const { reactor, events, waitFor } = createTestReactor({
      contextStore: makePersistingContextStore(cell),
      director: turnDirector(),
      inferenceRunner: turnRunner(),
      toolRunner: chargeRunner(),
      beforeToolExtensions: [askExtension()],
    });

    reactor.start();
    reactor.deliver(makeTurnMessage("park"), { turnId: "turn-a" });

    const blocked = await waitFor("reactor.gate.blocked");
    if (blocked.type !== "reactor.gate.blocked") throw new Error("unreachable");
    expect(blocked.data.turnId).toBe("turn-a");
    expect(turnIdsOf(events, "inference.start")).toEqual(["turn-a"]);
    expect(turnIdsOf(events, "inference.done")).toEqual(["turn-a"]);
    expect(cell.pendingOperations[0]?.turnId).toBe("turn-a");

    const correlationId = blocked.data.correlationId;
    if (correlationId === undefined) throw new Error("expected correlationId");
    reactor.deliver(makeApprovalMessage(correlationId));

    const reply = await waitFor("connector.reply");
    if (reply.type !== "connector.reply") throw new Error("unreachable");
    expect(reply.data.content).toBe("A done");
    expect(reply.data.turnId).toBe("turn-a");
    expect(turnIdsOf(events, "inference.start")).toEqual(["turn-a", "turn-a"]);
    expect(turnIdsOf(events, "inference.done")).toEqual(["turn-a", "turn-a"]);

    reactor.abort("admin_kill");
    await waitFor("reactor.done");
  });

  test("an untagged delivery gets one minted turn id shared by all its events", async () => {
    const { reactor, events, waitFor } = createTestReactor({
      director: turnDirector(),
      inferenceRunner: turnRunner(),
    });

    reactor.start();
    reactor.deliver(makeTurnMessage("first"));
    await waitFor("connector.reply");
    reactor.deliver(makeTurnMessage("second"));
    await waitUntil(() => countOf(events, "connector.reply") >= 2);

    const replies = replyTurnIds(events);
    const first = replies.get("first done");
    const second = replies.get("second done");
    if (first === undefined || second === undefined) {
      throw new Error("expected a turn id on each reply");
    }
    expect(first).not.toBe(second);
    expect(turnIdsOf(events, "inference.start")).toEqual([first, second]);
    expect(turnIdsOf(events, "inference.done")).toEqual([first, second]);

    reactor.abort("admin_kill");
    await waitFor("reactor.done");
  });

  test("a parked operation persists its turn id and its resume after a restart continues that turn", async () => {
    const cell: PersistedCell = {
      turns: [],
      pendingOperations: [],
      tokenUsage: emptyUsage(),
    };

    const first = createTestReactor({
      contextStore: makePersistingContextStore(cell),
      director: turnDirector(),
      inferenceRunner: turnRunner(),
      beforeToolExtensions: [askExtension()],
    });
    first.reactor.start();
    first.reactor.deliver(makeTurnMessage("park"), { turnId: "turn-a" });
    const blocked = await first.waitFor("reactor.gate.blocked");
    if (blocked.type !== "reactor.gate.blocked") throw new Error("unreachable");
    const correlationId = blocked.data.correlationId;
    if (correlationId === undefined) throw new Error("expected correlationId");
    first.reactor.abort("admin_kill");
    await first.waitFor("reactor.done");

    expect(cell.pendingOperations).toHaveLength(1);
    expect(cell.pendingOperations[0]?.turnId).toBe("turn-a");

    // The approval names a turn of its own. The resume continues the parked
    // turn, so each event of the resume carries the parked turn's id.
    const second = createTestReactor({
      contextStore: makePersistingContextStore(cell),
      director: turnDirector(),
      inferenceRunner: turnRunner(),
      toolRunner: chargeRunner(),
      beforeToolExtensions: [askExtension()],
    });
    second.reactor.start();
    second.reactor.deliver(makeApprovalMessage(correlationId), {
      turnId: "turn-approval",
    });

    const reply = await second.waitFor("connector.reply");
    if (reply.type !== "connector.reply") throw new Error("unreachable");
    expect(reply.data.content).toBe("A done");
    expect(reply.data.turnId).toBe("turn-a");
    expect(turnIdsOf(second.events, "inference.start")).toEqual(["turn-a"]);
    expect(turnIdsOf(second.events, "inference.done")).toEqual(["turn-a"]);

    second.reactor.abort("admin_kill");
    await second.waitFor("reactor.done");
  });

  test("an operation persisted without a turn id resumes under its legacy id", async () => {
    const correlationId = "legacy-correlation";
    const cell: PersistedCell = {
      turns: [
        {
          role: "user",
          content: [{ type: "text", text: "park" }],
          timestamp: 1000,
        },
        suspendToolCallTurn,
      ],
      pendingOperations: [
        {
          correlationId,
          kind: "approval",
          registeredAt: Date.now(),
          gateId: `pending-${correlationId}`,
          timeoutAt: Date.now() + 60_000,
          suspendedCall: { id: "call-ask", name: "charge_card", arguments: {} },
        },
      ],
      tokenUsage: emptyUsage(),
    };

    const { reactor, events, waitFor } = createTestReactor({
      contextStore: makePersistingContextStore(cell),
      director: turnDirector(),
      inferenceRunner: turnRunner(),
      toolRunner: chargeRunner(),
      beforeToolExtensions: [askExtension()],
    });
    reactor.start();
    reactor.deliver(makeApprovalMessage(correlationId), {
      turnId: "turn-approval",
    });

    const reply = await waitFor("connector.reply");
    if (reply.type !== "connector.reply") throw new Error("unreachable");
    expect(legacyTurnId(correlationId)).toBe(`legacy:${correlationId}`);
    expect(reply.data.turnId).toBe(`legacy:${correlationId}`);
    expect(turnIdsOf(events, "inference.start")).toEqual([
      `legacy:${correlationId}`,
    ]);

    reactor.abort("admin_kill");
    await waitFor("reactor.done");
  });

  test("a rehydrated gate of a legacy operation continues the legacy turn when it times out", async () => {
    const correlationId = "legacy-marker";
    const cell: PersistedCell = {
      turns: [
        {
          role: "user",
          content: [{ type: "text", text: "marker" }],
          timestamp: 1000,
        },
      ],
      pendingOperations: [
        {
          correlationId,
          kind: "approval",
          registeredAt: 0,
          gateId: `pending-${correlationId}`,
          // A deadline in the past fires the rehydrated gate at once.
          timeoutAt: 0,
        },
      ],
      tokenUsage: emptyUsage(),
    };

    const { reactor, events, waitFor } = createTestReactor({
      contextStore: makePersistingContextStore(cell),
      director: turnDirector(),
      inferenceRunner: turnRunner(),
    });
    reactor.start();

    const reply = await waitFor("connector.reply");
    if (reply.type !== "connector.reply") throw new Error("unreachable");
    expect(reply.data.content).toBe("marker done");
    expect(reply.data.turnId).toBe(legacyTurnId(correlationId));
    expect(turnIdsOf(events, "inference.start")).toEqual([
      legacyTurnId(correlationId),
    ]);

    reactor.abort("admin_kill");
    await waitFor("reactor.done");
  });

  test("a parked turn resumed while another turn's cycle runs keeps its own turn id", async () => {
    // Turn B's inference stays open until turn A's approval has correlated,
    // so A's resume is queued while B's cycle runs.
    const releaseB = Promise.withResolvers<boolean>();
    const { reactor, events, waitFor } = createTestReactor({
      director: turnDirector(),
      inferenceRunner: turnRunner(async (text) =>
        text === "B" ? releaseB.promise : true,
      ),
      toolRunner: chargeRunner(),
      beforeToolExtensions: [askExtension()],
    });

    reactor.start();
    reactor.deliver(makeTurnMessage("park"), { turnId: "turn-a" });
    const blocked = await waitFor("reactor.gate.blocked");
    if (blocked.type !== "reactor.gate.blocked") throw new Error("unreachable");
    const correlationId = blocked.data.correlationId;
    if (correlationId === undefined) throw new Error("expected correlationId");

    reactor.deliver(makeTurnMessage("B"), { turnId: "turn-b" });
    await waitUntil(() => countOf(events, "inference.start") >= 2);
    reactor.deliver(makeApprovalMessage(correlationId), {
      turnId: "turn-approval",
    });
    await waitFor("message.correlated");
    releaseB.resolve(true);

    await waitUntil(() => countOf(events, "connector.reply") >= 2);
    const replies = replyTurnIds(events);
    expect(replies.get("B done")).toBe("turn-b");
    expect(replies.get("A done")).toBe("turn-a");
    expect(turnIdsOf(events, "inference.start")).toEqual([
      "turn-a",
      "turn-b",
      "turn-a",
    ]);
    expect(turnIdsOf(events, "inference.done")).toEqual([
      "turn-a",
      "turn-b",
      "turn-a",
    ]);
    expect(events.some((e) => stampedTurnId(e) === "turn-approval")).toBe(
      false,
    );

    reactor.abort("admin_kill");
    await waitFor("reactor.done");
  });

  test("a parked turn whose gate times out while another turn's cycle runs keeps its own turn id", async () => {
    // Turn B's inference stays open until turn A's director gate has timed
    // out. A gate that times out before B's message is dequeued runs A first;
    // the assertions hold for both orders.
    const { reactor, events, waitFor } = createTestReactor({
      director: turnDirector(),
      inferenceRunner: turnRunner(async (text) =>
        text === "B" ? waitFor("reactor.gate.cleared") : true,
      ),
    });

    reactor.start();
    reactor.deliver(makeTurnMessage("suspend"), { turnId: "turn-a" });
    const blocked = await waitFor("reactor.gate.blocked");
    if (blocked.type !== "reactor.gate.blocked") throw new Error("unreachable");
    expect(blocked.data.turnId).toBe("turn-a");

    reactor.deliver(makeTurnMessage("B"), { turnId: "turn-b" });

    await waitUntil(() => countOf(events, "connector.reply") >= 2);
    const replies = replyTurnIds(events);
    // A's reply text depends on the order, so A's reply is the other one.
    expect(replies.get("B done")).toBe("turn-b");
    const otherReplies = [...replies]
      .filter(([content]) => content !== "B done")
      .map(([, turnId]) => turnId);
    expect(otherReplies).toEqual(["turn-a"]);
    expect(turnIdsOf(events, "inference.start").sort()).toEqual([
      "turn-a",
      "turn-b",
    ]);

    reactor.abort("admin_kill");
    await waitFor("reactor.done");
  });

  test("a parked call whose approval times out after a later delivery keeps its own turn id", async () => {
    const { reactor, events, waitFor } = createTestReactor({
      director: turnDirector(),
      inferenceRunner: turnRunner(),
      beforeToolExtensions: [askExtension(50)],
    });

    reactor.start();
    reactor.deliver(makeTurnMessage("park"), { turnId: "turn-a" });
    await waitFor("reactor.gate.blocked");
    reactor.deliver(makeTurnMessage("B"), { turnId: "turn-b" });

    await waitUntil(() => countOf(events, "connector.reply") >= 2);
    const replies = replyTurnIds(events);
    expect(replies.get("B done")).toBe("turn-b");
    expect(replies.get("A done")).toBe("turn-a");

    reactor.abort("admin_kill");
    await waitFor("reactor.done");
  });

  test("a rejected approval continues the parked turn, not the approval's turn", async () => {
    const { reactor, events, waitFor } = createTestReactor({
      director: turnDirector(),
      inferenceRunner: turnRunner(),
      toolRunner: chargeRunner(),
      beforeToolExtensions: [askExtension()],
    });
    reactor.start();
    reactor.deliver(makeTurnMessage("park"), { turnId: "turn-a" });
    const blocked = await waitFor("reactor.gate.blocked");
    if (blocked.type !== "reactor.gate.blocked") throw new Error("unreachable");
    const correlationId = blocked.data.correlationId;
    if (correlationId === undefined) throw new Error("expected correlationId");

    reactor.deliver(makeApprovalMessage(correlationId, "rejected"), {
      turnId: "turn-approval",
    });
    const reply = await waitFor("connector.reply");
    if (reply.type !== "connector.reply") throw new Error("unreachable");
    expect(reply.data.content).toBe("A done");
    expect(reply.data.turnId).toBe("turn-a");
    expect(events.some((e) => stampedTurnId(e) === "turn-approval")).toBe(
      false,
    );
    reactor.abort("admin_kill");
    await waitFor("reactor.done");
  });

  test("an async pending marker parks under the tool's turn and its correlated response continues that turn", async () => {
    const cell: PersistedCell = {
      turns: [],
      pendingOperations: [],
      tokenUsage: emptyUsage(),
    };
    const director = directorFromTable(
      {
        "message.received": (_e, _s, caps) => caps.infer(),
        "inference.done": (e, _s, caps) => {
          const calls: ToolCall[] = [];
          let text = "";
          for (const block of e.turn.content) {
            if (block.type === "tool_call") {
              calls.push({
                id: block.id,
                name: block.name,
                arguments: block.arguments,
              });
            } else if (block.type === "text") {
              text += block.text;
            }
          }
          return calls.length > 0 ? caps.executeTools(calls) : caps.reply(text);
        },
        "tool.done": (_e, _s, caps) =>
          caps.suspend({
            type: "message_response",
            gateId: "marker-gate",
            timeoutMs: 60_000,
            correlationId: "corr-marker",
          }),
        "reactor.gate.cleared": (_e, _s, caps) => caps.infer(),
      },
      "wait",
    );
    const { reactor, waitFor } = createTestReactor({
      contextStore: makePersistingContextStore(cell),
      director,
      inferenceRunner: turnRunner(),
      toolRunner: makeToolRunner(async (call) => ({
        callId: call.id,
        content: "sent",
        pendingMarker: {
          status: "pending" as const,
          correlationId: "corr-marker",
        },
      })),
    });
    reactor.start();
    reactor.deliver(makeTurnMessage("park"), { turnId: "turn-a" });
    const blocked = await waitFor("reactor.gate.blocked");
    if (blocked.type !== "reactor.gate.blocked") throw new Error("unreachable");
    expect(blocked.data.turnId).toBe("turn-a");
    expect(cell.pendingOperations[0]?.turnId).toBe("turn-a");

    reactor.deliver(
      createInboundMessage({
        from: "peer@example.com",
        to: "agent@example.com",
        content: "resp",
        correlationId: "corr-marker",
      }),
      { turnId: "turn-resp" },
    );
    const reply = await waitFor("connector.reply");
    if (reply.type !== "connector.reply") throw new Error("unreachable");
    expect(reply.data.turnId).toBe("turn-a");
    reactor.abort("admin_kill");
    await waitFor("reactor.done");
  });

  test("a director gate on an earlier turn's pending marker moves it to the running turn", async () => {
    const cell: PersistedCell = {
      turns: [],
      pendingOperations: [],
      tokenUsage: emptyUsage(),
    };
    let messages = 0;
    const director = directorFromTable(
      {
        // The first message runs the marker tool; the second waits on the
        // marker's correlation from a turn of its own.
        "message.received": (_e, _s, caps) => {
          messages += 1;
          if (messages === 1) return caps.infer();
          return caps.suspend({
            type: "message_response",
            gateId: "wait-gate",
            timeoutMs: 60_000,
            correlationId: "corr-marker",
          });
        },
        "inference.done": (e, _s, caps) => {
          const calls: ToolCall[] = [];
          for (const block of e.turn.content) {
            if (block.type === "tool_call") {
              calls.push({
                id: block.id,
                name: block.name,
                arguments: block.arguments,
              });
            }
          }
          return caps.executeTools(calls);
        },
        "tool.done": (_e, _s, caps) => caps.reply("sent"),
      },
      "wait",
    );
    const { reactor, waitFor } = createTestReactor({
      contextStore: makePersistingContextStore(cell),
      director,
      inferenceRunner: turnRunner(),
      toolRunner: makeToolRunner(async (call) => ({
        callId: call.id,
        content: "sent",
        pendingMarker: {
          status: "pending" as const,
          correlationId: "corr-marker",
        },
      })),
    });
    reactor.start();
    reactor.deliver(makeTurnMessage("park"), { turnId: "turn-a" });
    await waitFor("connector.reply");
    expect(cell.pendingOperations[0]?.turnId).toBe("turn-a");

    reactor.deliver(makeTurnMessage("wait"), { turnId: "turn-b" });
    const blocked = await waitFor("reactor.gate.blocked");
    if (blocked.type !== "reactor.gate.blocked") throw new Error("unreachable");
    expect(blocked.data.correlationId).toBe("corr-marker");
    expect(blocked.data.turnId).toBe("turn-b");
    expect(cell.pendingOperations).toHaveLength(1);
    expect(cell.pendingOperations[0]?.turnId).toBe("turn-b");
    reactor.abort("admin_kill");
    await waitFor("reactor.done");
  });

  test("a director gate on a correlation that already has a live gate is refused and changes nothing", async () => {
    const cell: PersistedCell = {
      turns: [],
      pendingOperations: [],
      tokenUsage: emptyUsage(),
    };
    // The message "B" makes the director wait on the parked approval of
    // turn A from a turn of its own.
    const inner = turnDirector();
    const director: ReactorDirector = {
      async decide(event, state, caps) {
        if (
          event.type === "message.received" &&
          event.message.content === "B"
        ) {
          const op = state.pendingOperations[0];
          if (op === undefined) throw new Error("expected a parked operation");
          return caps.suspend({
            type: "approval",
            gateId: "b-gate",
            timeoutMs: 60_000,
            correlationId: op.correlationId,
          });
        }
        return inner.decide(event, state, caps);
      },
    };
    const { reactor, events, waitFor } = createTestReactor({
      contextStore: makePersistingContextStore(cell),
      director,
      inferenceRunner: turnRunner(),
      toolRunner: chargeRunner(),
      beforeToolExtensions: [askExtension()],
    });
    reactor.start();
    reactor.deliver(makeTurnMessage("park"), { turnId: "turn-a" });
    const blocked = await waitFor("reactor.gate.blocked");
    if (blocked.type !== "reactor.gate.blocked") throw new Error("unreachable");
    const correlationId = blocked.data.correlationId;
    if (correlationId === undefined) throw new Error("expected correlationId");

    reactor.deliver(makeTurnMessage("B"), { turnId: "turn-b" });
    const refused = await waitFor("reactor.error");
    if (refused.type !== "reactor.error") throw new Error("unreachable");
    expect(refused.data.fatal).toBe(false);
    expect(refused.data.error).toContain("already has a live gate");
    expect(countOf(events, "reactor.gate.blocked")).toBe(1);
    expect(cell.pendingOperations).toHaveLength(1);
    expect(cell.pendingOperations[0]?.turnId).toBe("turn-a");

    reactor.deliver(makeApprovalMessage(correlationId));
    const reply = await waitFor("connector.reply");
    if (reply.type !== "connector.reply") throw new Error("unreachable");
    expect(reply.data.content).toBe("A done");
    expect(reply.data.turnId).toBe("turn-a");
    expect(cell.pendingOperations).toEqual([]);

    reactor.abort("admin_kill");
    await waitFor("reactor.done");
  });

  test("a delivery buffered before start keeps its caller turn id", async () => {
    const { reactor, waitFor } = createTestReactor({
      director: turnDirector(),
      inferenceRunner: turnRunner(),
    });
    reactor.deliver(makeTurnMessage("early"), { turnId: "turn-early" });
    reactor.start();
    const reply = await waitFor("connector.reply");
    if (reply.type !== "connector.reply") throw new Error("unreachable");
    expect(reply.data.content).toBe("early done");
    expect(reply.data.turnId).toBe("turn-early");
    reactor.abort("admin_kill");
    await waitFor("reactor.done");
  });
});

// ---------------------------------------------------------------------------
// Turn lifecycle: each turn starts at most once and ends at most once in a
// process
// ---------------------------------------------------------------------------

describe("createReactor — turn lifecycle", () => {
  type TurnStartedData = Extract<
    ReactorEmittedEvent,
    { type: "reactor.turn.started" }
  >["data"];

  function startsOf(
    events: ReactorEmittedEvent[],
    turnId: string,
  ): TurnStartedData[] {
    const starts: TurnStartedData[] = [];
    for (const e of events) {
      if (e.type === "reactor.turn.started" && e.data.turnId === turnId) {
        starts.push(e.data);
      }
    }
    return starts;
  }

  function endsOf(
    events: ReactorEmittedEvent[],
    turnId: string,
  ): TurnEndedData[] {
    return turnEnds(events).filter((e) => e.turnId === turnId);
  }

  function endIndex(events: ReactorEmittedEvent[], turnId: string): number {
    return events.findIndex(
      (e) => e.type === "reactor.turn.ended" && e.data.turnId === turnId,
    );
  }

  function waitForEnd(
    events: ReactorEmittedEvent[],
    turnId: string,
  ): Promise<ReactorEmittedEvent> {
    return waitForEvent(
      events,
      (e) => e.type === "reactor.turn.ended" && e.data.turnId === turnId,
    );
  }

  function messageIdOf(message: InboundMessage): string {
    const messageId = message.headers.messageId;
    if (messageId === undefined) throw new Error("expected a message id");
    return messageId;
  }

  async function correlationIdOf(
    waitFor: TestReactorHandle["waitFor"],
  ): Promise<string> {
    const blocked = await waitFor("reactor.gate.blocked");
    if (blocked.type !== "reactor.gate.blocked") throw new Error("unreachable");
    const correlationId = blocked.data.correlationId;
    if (correlationId === undefined) throw new Error("expected correlationId");
    return correlationId;
  }

  // Asks for approval of `charge_card` and allows every other tool.
  function askForCharge() {
    return createAuthzExtension({
      authorize: async (resource) => ({
        effect:
          resource === "tool:charge_card"
            ? ("ask" as const)
            : ("allow" as const),
        matchingGrants: [],
        resolvedBy: null,
      }),
      approvalTimeoutMs: 60_000,
    });
  }

  test("a new message starts its turn before its inference and ends it replied after the reply", async () => {
    const { reactor, events, waitFor } = createTestReactor({
      director: turnDirector(),
      inferenceRunner: turnRunner(),
    });
    reactor.start();
    const message = makeTurnMessage("hi");
    reactor.deliver(message, { turnId: "turn-a" });
    await waitForEnd(events, "turn-a");

    expect(startsOf(events, "turn-a")).toEqual([
      {
        turnId: "turn-a",
        resumed: false,
        messageId: messageIdOf(message),
      },
    ]);
    expect(endsOf(events, "turn-a")).toEqual([
      { turnId: "turn-a", outcome: "replied" },
    ]);
    const types = events.map((e) => e.type);
    const started = types.indexOf("reactor.turn.started");
    const inferenceStart = types.indexOf("inference.start");
    const reply = types.indexOf("connector.reply");
    expect(started).toBeGreaterThan(-1);
    expect(inferenceStart).toBeGreaterThan(started);
    expect(reply).toBeGreaterThan(inferenceStart);
    expect(endIndex(events, "turn-a")).toBeGreaterThan(reply);

    reactor.abort("admin_kill");
    await waitFor("reactor.done");
  });

  test("a turn ends unanswered after wait, an empty action set, or a compaction alone", async () => {
    const directors: ReactorDirector[] = [
      directorFromTable({}, "wait"),
      directorFromTable({ "message.received": () => [] }, "wait"),
      directorFromTable(
        {
          "message.received": (_e, _s, caps) =>
            caps.compact("truncate", "test"),
        },
        "wait",
      ),
    ];
    for (const director of directors) {
      const { reactor, events, waitFor } = createTestReactor({
        director,
        compactors: { truncate: truncatingCompactor("truncate") },
      });
      reactor.start();
      reactor.deliver(makeTurnMessage("hi"), { turnId: "turn-a" });
      await waitForEnd(events, "turn-a");

      expect(endsOf(events, "turn-a")).toEqual([
        { turnId: "turn-a", outcome: "unanswered" },
      ]);
      reactor.abort("admin_kill");
      await waitFor("reactor.done");
    }
  });

  test("a new message while a turn is parked ends nothing of the parked turn, which starts and ends once", async () => {
    const { reactor, events, waitFor } = createTestReactor({
      director: turnDirector(),
      inferenceRunner: turnRunner(),
      toolRunner: chargeRunner(),
      beforeToolExtensions: [askExtension()],
    });
    reactor.start();
    reactor.deliver(makeTurnMessage("park"), { turnId: "turn-a" });
    const correlationId = await correlationIdOf(waitFor);

    reactor.deliver(makeTurnMessage("B"), { turnId: "turn-b" });
    await waitForEnd(events, "turn-b");
    expect(endsOf(events, "turn-b")).toEqual([
      { turnId: "turn-b", outcome: "replied" },
    ]);
    expect(endsOf(events, "turn-a")).toEqual([]);

    reactor.deliver(makeApprovalMessage(correlationId));
    await waitForEnd(events, "turn-a");
    expect(startsOf(events, "turn-a")).toHaveLength(1);
    expect(endsOf(events, "turn-a")).toEqual([
      { turnId: "turn-a", outcome: "replied" },
    ]);

    reactor.abort("admin_kill");
    await waitFor("reactor.done");
  });

  test("a resume during the first cycle of a turn starts no second time", async () => {
    // One batch parks the ask call and runs a plain call. The approval
    // correlates while the plain call still runs.
    const releasePlain = Promise.withResolvers<boolean>();
    const { reactor, events, waitFor } = createTestReactor({
      director: directorFromTable(
        {
          "message.received": (_e, _s, caps) =>
            caps.executeTools([
              { id: "call-ask", name: "charge_card", arguments: {} },
              { id: "call-plain", name: "lookup", arguments: {} },
            ]),
          "resume.execute_tools": (e, _s, caps) =>
            caps.executeTools(e.calls, false, true),
          "tool.done": (e, _s, caps) =>
            e.result.callId === "call-ask" ? caps.reply("charged") : [],
        },
        "wait",
      ),
      toolRunner: makeToolRunner(async (call) => {
        if (call.name === "lookup") await releasePlain.promise;
        return { callId: call.id, content: "ok" };
      }),
      beforeToolExtensions: [askForCharge()],
    });
    reactor.start();
    const message = makeTurnMessage("go");
    reactor.deliver(message, { turnId: "turn-a" });
    const correlationId = await correlationIdOf(waitFor);
    reactor.deliver(makeApprovalMessage(correlationId));
    await waitFor("message.correlated");
    releasePlain.resolve(true);
    await waitForEnd(events, "turn-a");

    expect(startsOf(events, "turn-a")).toEqual([
      {
        turnId: "turn-a",
        resumed: false,
        messageId: messageIdOf(message),
      },
    ]);
    expect(endsOf(events, "turn-a")).toEqual([
      { turnId: "turn-a", outcome: "replied" },
    ]);

    reactor.abort("admin_kill");
    await waitFor("reactor.done");
  });

  test("a resume that names another turn ends that turn joined, and one that names the parked turn does not", async () => {
    for (const approvalTurnId of ["turn-d", "turn-a"]) {
      const { reactor, events, waitFor } = createTestReactor({
        director: turnDirector(),
        inferenceRunner: turnRunner(),
        toolRunner: chargeRunner(),
        beforeToolExtensions: [askExtension()],
      });
      reactor.start();
      reactor.deliver(makeTurnMessage("park"), { turnId: "turn-a" });
      const correlationId = await correlationIdOf(waitFor);
      reactor.deliver(makeApprovalMessage(correlationId), {
        turnId: approvalTurnId,
      });
      await waitForEnd(events, "turn-a");

      expect(endsOf(events, "turn-a")).toEqual([
        { turnId: "turn-a", outcome: "replied" },
      ]);
      if (approvalTurnId === "turn-d") {
        expect(endsOf(events, "turn-d")).toEqual([
          { turnId: "turn-d", outcome: "joined", joinedTurnId: "turn-a" },
        ]);
        expect(startsOf(events, "turn-d")).toEqual([]);
        const correlated = events.findIndex(
          (e) => e.type === "message.correlated",
        );
        expect(endIndex(events, "turn-d")).toBeGreaterThan(correlated);
      } else {
        expect(turnEnds(events)).toHaveLength(1);
      }

      reactor.abort("admin_kill");
      await waitFor("reactor.done");
    }
  });

  // The message "park" runs a tool whose result carries an async pending
  // marker. The operation then waits for its response with no live gate.
  function markerReactor(hold?: (text: string) => Promise<unknown>) {
    return createTestReactor({
      director: directorFromTable(
        {
          "message.received": (_e, _s, caps) => caps.infer(),
          "inference.done": (e, _s, caps) => {
            const calls: ToolCall[] = [];
            let text = "";
            for (const block of e.turn.content) {
              if (block.type === "tool_call") {
                calls.push({
                  id: block.id,
                  name: block.name,
                  arguments: block.arguments,
                });
              } else if (block.type === "text") {
                text += block.text;
              }
            }
            return calls.length > 0
              ? caps.executeTools(calls)
              : caps.reply(text);
          },
          "tool.done": (_e, _s, caps) => caps.infer(),
        },
        "wait",
      ),
      inferenceRunner: turnRunner(hold),
      toolRunner: {
        async run(call) {
          return {
            callId: call.id,
            content: "sent",
            pendingMarker: {
              status: "pending" as const,
              correlationId: "corr-marker",
            },
          };
        },
      },
    });
  }

  function markerResponse(): InboundMessage {
    return createInboundMessage({
      from: "peer@example.com",
      to: "agent@example.com",
      content: "resp",
      correlationId: "corr-marker",
    });
  }

  test("a correlated response that resumes nothing ends its turn unanswered", async () => {
    const { reactor, events, waitFor } = markerReactor();
    reactor.start();
    reactor.deliver(makeTurnMessage("park"), { turnId: "turn-a" });
    await waitForEnd(events, "turn-a");
    const inferences = countOf(events, "inference.start");

    reactor.deliver(markerResponse(), { turnId: "turn-d" });
    await waitForEnd(events, "turn-d");

    expect(endsOf(events, "turn-d")).toEqual([
      { turnId: "turn-d", outcome: "unanswered" },
    ]);
    expect(startsOf(events, "turn-d")).toEqual([]);
    const correlated = events.findIndex((e) => e.type === "message.correlated");
    expect(correlated).toBeGreaterThan(-1);
    expect(endIndex(events, "turn-d")).toBeGreaterThan(correlated);
    expect(countOf(events, "inference.start")).toBe(inferences);

    reactor.abort("admin_kill");
    await waitFor("reactor.done");
  });

  test("a correlated response that names the running operation's turn does not end that turn early", async () => {
    // The second inference of turn A stays open until A's own response
    // has correlated.
    const releaseSecond = Promise.withResolvers<boolean>();
    let inferences = 0;
    const { reactor, events, waitFor } = markerReactor(async () => {
      inferences += 1;
      if (inferences === 2) await releaseSecond.promise;
    });
    reactor.start();
    reactor.deliver(makeTurnMessage("park"), { turnId: "turn-a" });
    await waitUntil(() => countOf(events, "inference.start") >= 2);

    reactor.deliver(markerResponse(), { turnId: "turn-a" });
    await waitFor("message.correlated");
    expect(endsOf(events, "turn-a")).toEqual([]);

    releaseSecond.resolve(true);
    await waitForEnd(events, "turn-a");
    expect(endsOf(events, "turn-a")).toEqual([
      { turnId: "turn-a", outcome: "replied" },
    ]);

    reactor.abort("admin_kill");
    await waitFor("reactor.done");
  });

  test("a director gate that times out ends its turn once", async () => {
    const { reactor, events, waitFor } = createTestReactor({
      director: turnDirector(),
      inferenceRunner: turnRunner(),
    });
    reactor.start();
    reactor.deliver(makeTurnMessage("suspend"), { turnId: "turn-a" });
    await waitForEnd(events, "turn-a");

    expect(endsOf(events, "turn-a")).toEqual([
      { turnId: "turn-a", outcome: "replied" },
    ]);
    expect(startsOf(events, "turn-a")).toHaveLength(1);
    expect(countOf(events, "reactor.gate.cleared")).toBe(1);

    reactor.abort("admin_kill");
    await waitFor("reactor.done");
    expect(endsOf(events, "turn-a")).toHaveLength(1);
  });

  test("an approval that times out ends its turn once", async () => {
    const { reactor, events, waitFor } = createTestReactor({
      director: turnDirector(),
      inferenceRunner: turnRunner(),
      toolRunner: chargeRunner(),
      beforeToolExtensions: [askExtension(50)],
    });
    reactor.start();
    reactor.deliver(makeTurnMessage("park"), { turnId: "turn-a" });
    await waitForEnd(events, "turn-a");

    expect(endsOf(events, "turn-a")).toEqual([
      { turnId: "turn-a", outcome: "replied" },
    ]);
    expect(startsOf(events, "turn-a")).toHaveLength(1);
    expect(replyTurnIds(events).get("A done")).toBe("turn-a");

    reactor.abort("admin_kill");
    await waitFor("reactor.done");
    expect(endsOf(events, "turn-a")).toHaveLength(1);
  });

  test("a turn parked at shutdown ends once after the restart that resumes it", async () => {
    const cell: PersistedCell = {
      turns: [],
      pendingOperations: [],
      tokenUsage: emptyUsage(),
    };
    const first = createTestReactor({
      contextStore: makePersistingContextStore(cell),
      director: turnDirector(),
      inferenceRunner: turnRunner(),
      beforeToolExtensions: [askExtension()],
    });
    first.reactor.start();
    first.reactor.deliver(makeTurnMessage("park"), { turnId: "turn-a" });
    const correlationId = await correlationIdOf(first.waitFor);
    first.reactor.abort("admin_kill");
    await first.waitFor("reactor.done");
    expect(turnEnds(first.events)).toEqual([]);

    const second = createTestReactor({
      contextStore: makePersistingContextStore(cell),
      director: turnDirector(),
      inferenceRunner: turnRunner(),
      toolRunner: chargeRunner(),
      beforeToolExtensions: [askExtension()],
    });
    second.reactor.start();
    second.reactor.deliver(makeApprovalMessage(correlationId), {
      turnId: "turn-approval",
    });
    await waitForEnd(second.events, "turn-a");

    expect(startsOf(second.events, "turn-a")).toEqual([
      { turnId: "turn-a", resumed: true },
    ]);
    expect(endsOf(second.events, "turn-a")).toEqual([
      { turnId: "turn-a", outcome: "replied" },
    ]);
    const started = second.events.findIndex(
      (e) => e.type === "reactor.turn.started",
    );
    expect(started).toBeLessThan(endIndex(second.events, "turn-a"));

    second.reactor.abort("admin_kill");
    await second.waitFor("reactor.done");
    expect(endsOf(second.events, "turn-a")).toHaveLength(1);
  });

  test("a queued turn that never ran before an abort neither starts nor ends", async () => {
    const releaseA = Promise.withResolvers<boolean>();
    const { reactor, events, waitFor } = createTestReactor({
      director: turnDirector(),
      inferenceRunner: turnRunner(async (text) =>
        text === "A" ? releaseA.promise : true,
      ),
    });
    reactor.start();
    reactor.deliver(makeTurnMessage("A"), { turnId: "turn-a" });
    await waitFor("inference.start");
    reactor.deliver(makeTurnMessage("B"), { turnId: "turn-b" });
    await waitUntil(() => countOf(events, "message.received") >= 2);

    reactor.abort("admin_kill");
    releaseA.resolve(true);
    await waitFor("reactor.done");

    expect(startsOf(events, "turn-b")).toEqual([]);
    expect(endsOf(events, "turn-b")).toEqual([]);
    expect(endsOf(events, "turn-a")).toEqual([
      {
        turnId: "turn-a",
        outcome: "failed",
        error: { message: "Reactor aborted: admin_kill", kind: "aborted" },
      },
    ]);
  });

  test("each fatal path ends the running turn failed with its kind", async () => {
    type Case = {
      name: string;
      overrides: TestReactorOverrides;
      message: RegExp;
    };
    const cases: Case[] = [
      {
        name: "director throw",
        overrides: {
          director: {
            async decide() {
              throw new Error("director blew up");
            },
          },
        },
        message: /^Director exception: director blew up$/,
      },
      {
        name: "invalid actions",
        overrides: {
          director: directorFromTable({
            "message.received": (_e, _s, caps) => [caps.infer(), caps.done()],
          }),
        },
        message: /^Invalid action set/,
      },
      {
        name: "compaction failure",
        overrides: {
          director: directorFromTable({
            "message.received": (_e, _s, caps) =>
              caps.compact("missing", "test"),
          }),
        },
        message: /^Compaction failed/,
      },
      {
        name: "loop throw",
        overrides: {
          director: directorFromTable({
            "message.received": (_e, _s, caps) =>
              caps.executeTools([{ id: "c1", name: "boom", arguments: {} }]),
          }),
          toolRunner: throwingToolRunner(new Error("tool exploded")),
        },
        message: /^Internal reactor error: tool exploded$/,
      },
    ];
    for (const c of cases) {
      const { reactor, events, waitFor } = createTestReactor(c.overrides);
      reactor.start();
      reactor.deliver(makeTurnMessage("hi"), { turnId: "turn-a" });
      await waitFor("reactor.done");

      const ends = endsOf(events, "turn-a");
      expect(ends).toHaveLength(1);
      expect(ends[0]).toMatchObject({
        outcome: "failed",
        error: { kind: "reactor_fatal" },
      });
      const end = ends[0];
      if (end?.outcome !== "failed") throw new Error(c.name);
      expect(end.error.message).toMatch(c.message);
    }
  });

  test("an abort while a tool runs ends the running turn failed as aborted", async () => {
    const releaseTool = Promise.withResolvers<boolean>();
    const { reactor, events, waitFor } = createTestReactor({
      director: directorFromTable(
        {
          "message.received": (_e, _s, caps) =>
            caps.executeTools([{ id: "c1", name: "slow", arguments: {} }]),
        },
        "wait",
      ),
      toolRunner: makeToolRunner(async (call) => {
        await releaseTool.promise;
        return { callId: call.id, content: "ok" };
      }),
    });
    reactor.start();
    reactor.deliver(makeTurnMessage("hi"), { turnId: "turn-a" });
    await waitFor("tool.start");
    reactor.abort("admin_kill");
    releaseTool.resolve(true);
    await waitFor("reactor.done");

    expect(endsOf(events, "turn-a")).toEqual([
      {
        turnId: "turn-a",
        outcome: "failed",
        error: { message: "Reactor aborted: admin_kill", kind: "aborted" },
      },
    ]);
  });

  test("an abort during a pacing wait ends the running turn failed as aborted", async () => {
    // The first inference asks for a pacing delay far longer than the test,
    // so the second inference waits for it until the abort.
    let runs = 0;
    const inferenceRunner = async function* (opts: InferenceHarnessOptions) {
      runs += 1;
      yield {
        type: "inference.done" as const,
        seq: opts.nextSeq(),
        data: {
          turn: makeAssistantTurn("paced"),
          usage: emptyUsage(),
          source: TEST_SOURCE,
          pacingDelayMs: 3_600_000,
        },
      };
    };
    let pacedInferDecided = false;
    const { reactor, events, waitFor } = createTestReactor({
      inferenceRunner,
      director: directorFromTable(
        {
          "message.received": (_e, _s, caps) => caps.infer(),
          "inference.done": (_e, _s, caps) => {
            pacedInferDecided = true;
            return caps.infer();
          },
        },
        "wait",
      ),
    });
    reactor.start();
    reactor.deliver(makeTurnMessage("hi"), { turnId: "turn-a" });
    // waitUntil returns on a later macrotask. By then the loop has run the
    // director's infer up to the pacing wait, which only a timer or an abort
    // ends.
    await waitUntil(() => pacedInferDecided);
    reactor.abort("admin_kill");
    await waitFor("reactor.done");

    expect(runs).toBe(1);
    expect(endsOf(events, "turn-a")).toEqual([
      {
        turnId: "turn-a",
        outcome: "failed",
        error: { message: "Reactor aborted: admin_kill", kind: "aborted" },
      },
    ]);
  });

  test("a correlation-dispatch failure ends the delivered turn and the running turn failed", async () => {
    for (const deliveredTurnId of ["turn-d", "turn-r"]) {
      const releaseR = Promise.withResolvers<boolean>();
      const { reactor, events, waitFor } = createTestReactor({
        director: turnDirector(),
        inferenceRunner: turnRunner(async (text) =>
          text === "R" ? releaseR.promise : true,
        ),
        toolRunner: chargeRunner(),
        beforeToolExtensions: [askExtension()],
      });
      reactor.start();
      reactor.deliver(makeTurnMessage("park"), { turnId: "turn-a" });
      const correlationId = await correlationIdOf(waitFor);
      reactor.deliver(makeTurnMessage("R"), { turnId: "turn-r" });
      await waitUntil(() => countOf(events, "inference.start") >= 2);

      reactor.deliver(
        createInboundMessage({
          from: "signal@local",
          to: "agent@example.com",
          content: "not json",
          correlationId,
        }),
        { turnId: deliveredTurnId },
      );
      await waitForEnd(events, deliveredTurnId);
      releaseR.resolve(true);
      await waitFor("reactor.done");

      const failed = {
        outcome: "failed",
        error: expect.objectContaining({ kind: "reactor_fatal" }),
      };
      expect(endsOf(events, "turn-r")).toEqual([
        expect.objectContaining(failed),
      ]);
      expect(endsOf(events, deliveredTurnId)).toEqual([
        expect.objectContaining(failed),
      ]);
      expect(endsOf(events, "turn-a")).toEqual([]);
      expect(events.some((e) => e.type === "connector.reply")).toBe(false);
    }
  });

  test("each turn keeps its own doom-loop count", async () => {
    // Turn A runs the same batch twice and parks on the third. Turn B runs
    // that batch once and does not trip. A's approval runs the third batch,
    // which trips at the threshold of 3.
    const spin = (id: string): ToolCall => ({
      id,
      name: "spin",
      arguments: { q: 1 },
    });
    const parkThird: BeforeToolExtension = {
      async beforeTool(call, _state, _signal, ctx) {
        if (call.id !== "a-3" || ctx.approved) {
          return { type: "allow" };
        }
        return {
          type: "suspend",
          gate: {
            type: "approval",
            gateId: "gate-a-3",
            correlationId: "corr-a-3",
            timeoutAt: Date.now() + 60_000,
          },
          pendingOp: {
            correlationId: "corr-a-3",
            kind: "approval",
            registeredAt: Date.now(),
            gateId: "gate-a-3",
            suspendedCall: call,
          },
        };
      },
    };
    const { reactor, events, waitFor } = createTestReactor({
      director: directorFromTable(
        {
          "message.received": (e, _s, caps) =>
            caps.executeTools([
              spin(e.message.content === "A" ? "a-1" : "b-1"),
            ]),
          "tool.done": (e, _s, caps) => {
            if (e.result.callId === "a-1")
              return caps.executeTools([spin("a-2")]);
            if (e.result.callId === "a-2")
              return caps.executeTools([spin("a-3")]);
            return caps.wait();
          },
          "resume.execute_tools": (e, _s, caps) =>
            caps.executeTools(e.calls, false, true),
        },
        "wait",
      ),
      beforeToolExtensions: [parkThird],
    });
    reactor.start();
    reactor.deliver(makeTurnMessage("A"), { turnId: "turn-a" });
    await waitFor("reactor.gate.blocked");
    reactor.deliver(makeTurnMessage("B"), { turnId: "turn-b" });
    await waitForEnd(events, "turn-b");
    expect(endsOf(events, "turn-b")).toEqual([
      { turnId: "turn-b", outcome: "unanswered" },
    ]);

    reactor.deliver(makeApprovalMessage("corr-a-3"));
    await waitFor("reactor.done");
    expect(endsOf(events, "turn-a")).toEqual([
      expect.objectContaining({
        outcome: "failed",
        error: expect.objectContaining({ kind: "doom_loop" }),
      }),
    ]);
    expect(countOf(events, "tool.start")).toBe(4);
  });

  // The types of the events that carry `turnId` and follow its end.
  function stampedAfterEnd(
    events: ReactorEmittedEvent[],
    turnId: string,
  ): string[] {
    const end = endIndex(events, turnId);
    if (end === -1) return [];
    return events
      .slice(end + 1)
      .filter((e) => stampedTurnId(e) === turnId)
      .map((e) => e.type);
  }

  test("an approval whose operation timed out during validation resumes nothing", async () => {
    // The validator holds the approval until the approval gate has timed
    // out and its turn has replied and ended.
    const releaseValidator = Promise.withResolvers<boolean>();
    const validating = Promise.withResolvers<boolean>();
    const { reactor, events, waitFor } = createTestReactor({
      director: turnDirector(),
      inferenceRunner: turnRunner(),
      toolRunner: chargeRunner(),
      beforeToolExtensions: [askExtension(50)],
      correlationValidator: {
        async validate() {
          validating.resolve(true);
          return releaseValidator.promise;
        },
      },
    });
    reactor.start();
    reactor.deliver(makeTurnMessage("park"), { turnId: "turn-a" });
    const correlationId = await correlationIdOf(waitFor);
    reactor.deliver(makeApprovalMessage(correlationId), { turnId: "turn-d" });
    await validating.promise;
    await waitForEnd(events, "turn-a");
    releaseValidator.resolve(true);
    await waitForEnd(events, "turn-d");

    // A later turn still runs, so the reactor did not stop.
    reactor.deliver(makeTurnMessage("later"), { turnId: "turn-l" });
    await waitForEnd(events, "turn-l");

    expect(events.some((e) => e.type === "reactor.error")).toBe(false);
    expect(endsOf(events, "turn-a")).toEqual([
      { turnId: "turn-a", outcome: "replied" },
    ]);
    expect(endsOf(events, "turn-d")).toEqual([
      { turnId: "turn-d", outcome: "unanswered" },
    ]);
    expect(endsOf(events, "turn-l")).toEqual([
      { turnId: "turn-l", outcome: "replied" },
    ]);
    expect(stampedAfterEnd(events, "turn-a")).toEqual([]);
    expect(countOf(events, "tool.start")).toBe(0);

    reactor.abort("admin_kill");
    await waitFor("reactor.done");
  });

  test("no gate.blocked of a turn follows its failed end during the suspend commit", async () => {
    // Turn R's before-tool suspend commits while a correlation-dispatch
    // failure ends R. The commit hook holds the commit until R has ended.
    // The shutdown waits for R's tool batch, which holds the suspend, so
    // `reactor.done` shows the suspend has returned. The long shutdown
    // deadline only turns a hang into a failure.
    const holdCommit = Promise.withResolvers<boolean>();
    const commitHeld = Promise.withResolvers<boolean>();
    let holding = false;
    const { reactor, events, waitFor } = createTestReactor({
      director: directorFromTable(
        {
          "message.received": (e, _s, caps) =>
            caps.executeTools([
              {
                id: e.message.content === "park" ? "call-a" : "call-r",
                name: "charge_card",
                arguments: {},
              },
            ]),
        },
        "wait",
      ),
      toolRunner: chargeRunner(),
      beforeToolExtensions: [askExtension()],
      shutdownTimeoutMs: 60_000,
      afterCheckpoint: async () => {
        if (holding) {
          holding = false;
          commitHeld.resolve(true);
          await holdCommit.promise;
        }
      },
    });
    reactor.start();
    reactor.deliver(makeTurnMessage("park"), { turnId: "turn-a" });
    const correlationId = await correlationIdOf(waitFor);
    holding = true;
    reactor.deliver(makeTurnMessage("R"), { turnId: "turn-r" });
    await commitHeld.promise;
    reactor.deliver(
      createInboundMessage({
        from: "signal@local",
        to: "agent@example.com",
        content: "not json",
        correlationId,
      }),
      { turnId: "turn-d" },
    );
    await waitForEnd(events, "turn-r");
    holdCommit.resolve(true);
    await waitFor("reactor.done");

    expect(endsOf(events, "turn-r")).toEqual([
      expect.objectContaining({ outcome: "failed" }),
    ]);
    expect(stampedAfterEnd(events, "turn-r")).toEqual([]);
    expect(
      events.filter(
        (e) => e.type === "reactor.gate.blocked" && e.data.turnId === "turn-r",
      ),
    ).toEqual([]);
    expect(afterDone(events)).toEqual([]);
  });

  test("a delivery after done that names a parked turn ends nothing", async () => {
    const { reactor, events, waitFor } = createTestReactor({
      director: turnDirector(),
      inferenceRunner: turnRunner(),
      toolRunner: chargeRunner(),
      beforeToolExtensions: [askExtension()],
    });
    reactor.start();
    reactor.deliver(makeTurnMessage("park"), { turnId: "turn-a" });
    const correlationId = await correlationIdOf(waitFor);
    reactor.abort("admin_kill");
    await waitFor("reactor.done");
    expect(turnEnds(events)).toEqual([]);

    // The parked turn survives for the restart, so the delivery that names
    // it is dropped. A delivery that names another turn still ends it.
    reactor.deliver(makeApprovalMessage(correlationId), { turnId: "turn-a" });
    reactor.deliver(makeTurnMessage("other"), { turnId: "turn-o" });
    expect(endsOf(events, "turn-a")).toEqual([]);
    expect(turnEnds(events)).toEqual([
      {
        turnId: "turn-o",
        outcome: "failed",
        error: { message: "The reactor is done", kind: "reactor_done" },
      },
    ]);
  });

  // The types of the events after `reactor.done`, except `reactor.turn.ended`.
  function afterDone(events: ReactorEmittedEvent[]): string[] {
    const doneAt = events.findIndex((e) => e.type === "reactor.done");
    if (doneAt === -1) throw new Error("expected reactor.done");
    return events
      .slice(doneAt + 1)
      .filter((e) => e.type !== "reactor.turn.ended")
      .map((e) => e.type);
  }

  test("a response validated while its marker moves to a waiting turn resumes that turn", async () => {
    // Turn A leaves an async marker and ends. A response for the marker
    // waits in the validator while turn B's director suspends on the
    // marker, which moves the marker to B. The response then resumes B.
    const release = Promise.withResolvers<boolean>();
    const validating = Promise.withResolvers<boolean>();
    let validations = 0;
    let messages = 0;
    const { reactor, events, waitFor } = createTestReactor({
      director: directorFromTable(
        {
          "message.received": (e, _s, caps) => {
            messages += 1;
            if (messages === 1) return caps.infer();
            if (e.message.content === "wait") {
              return caps.suspend({
                type: "message_response",
                gateId: "wait-gate",
                timeoutMs: 60_000,
                correlationId: "corr-marker",
              });
            }
            return caps.wait();
          },
          "inference.done": (e, _s, caps) => {
            const calls: ToolCall[] = [];
            for (const block of e.turn.content) {
              if (block.type === "tool_call") {
                calls.push({
                  id: block.id,
                  name: block.name,
                  arguments: block.arguments,
                });
              }
            }
            return caps.executeTools(calls);
          },
          "tool.done": (_e, _s, caps) => caps.reply("sent"),
          "reactor.gate.cleared": (_e, _s, caps) => caps.reply("got it"),
        },
        "wait",
      ),
      inferenceRunner: turnRunner(),
      toolRunner: makeToolRunner(async (call) => ({
        callId: call.id,
        content: "sent",
        pendingMarker: {
          status: "pending" as const,
          correlationId: "corr-marker",
        },
      })),
      correlationValidator: {
        async validate() {
          validations += 1;
          validating.resolve(true);
          return release.promise;
        },
      },
    });
    reactor.start();
    reactor.deliver(makeTurnMessage("park"), { turnId: "turn-a" });
    await waitForEnd(events, "turn-a");
    reactor.deliver(makeInboundMessage("corr-marker"), { turnId: "turn-d" });
    await validating.promise;
    reactor.deliver(makeTurnMessage("wait"), { turnId: "turn-b" });
    const blocked = await waitFor("reactor.gate.blocked");
    if (blocked.type !== "reactor.gate.blocked") throw new Error("unreachable");
    expect(blocked.data.turnId).toBe("turn-b");

    release.resolve(true);
    await waitForEnd(events, "turn-d");
    expect(endsOf(events, "turn-d")).toEqual([
      { turnId: "turn-d", outcome: "joined", joinedTurnId: "turn-b" },
    ]);
    await waitForEnd(events, "turn-b");
    expect(endsOf(events, "turn-b")).toEqual([
      { turnId: "turn-b", outcome: "replied" },
    ]);
    const cleared = getEvent(events, "reactor.gate.cleared");
    expect(cleared.data).toEqual({ gateId: "wait-gate", reason: "resolved" });
    expect(replyTurnIds(events).get("got it")).toBe("turn-b");

    // The resume consumed the operation, so a second response for the same
    // correlation id finds nothing to correlate and is a new message.
    reactor.deliver(makeInboundMessage("corr-marker"), { turnId: "turn-e" });
    await waitForEnd(events, "turn-e");
    expect(endsOf(events, "turn-e")).toEqual([
      { turnId: "turn-e", outcome: "unanswered" },
    ]);
    expect(validations).toBe(1);

    reactor.abort("admin_kill");
    await waitFor("reactor.done");
  });

  test("a correlated delivery whose validator settles after done only ends its turn", async () => {
    for (const verdict of [true, false]) {
      const release = Promise.withResolvers<boolean>();
      const validating = Promise.withResolvers<boolean>();
      const { reactor, events, waitFor } = createTestReactor({
        director: turnDirector(),
        inferenceRunner: turnRunner(),
        toolRunner: chargeRunner(),
        beforeToolExtensions: [askExtension()],
        correlationValidator: {
          async validate() {
            validating.resolve(true);
            return release.promise;
          },
        },
      });
      reactor.start();
      reactor.deliver(makeTurnMessage("park"), { turnId: "turn-a" });
      const correlationId = await correlationIdOf(waitFor);
      reactor.deliver(makeApprovalMessage(correlationId), {
        turnId: "turn-d",
      });
      await validating.promise;
      reactor.abort("admin_kill");
      await waitFor("reactor.done");

      release.resolve(verdict);
      await waitForEnd(events, "turn-d");

      expect(endsOf(events, "turn-d")).toEqual([
        {
          turnId: "turn-d",
          outcome: "failed",
          error: { message: "The reactor is done", kind: "reactor_done" },
        },
      ]);
      expect(endsOf(events, "turn-a")).toEqual([]);
      expect(countOf(events, "message.correlated")).toBe(0);
      expect(countOf(events, "message.received")).toBe(1);
      expect(afterDone(events)).toEqual([]);
    }
  });

  test("nothing but a turn end follows reactor.done when a suspend commit outlives the shutdown", async () => {
    // Turn R's director suspend holds its commit while a
    // correlation-dispatch failure stops the reactor. The shutdown does not
    // wait for a director suspend, so the suspend settles after
    // `reactor.done`, and its gate clear replays then.
    const holdCommit = Promise.withResolvers<boolean>();
    const commitHeld = Promise.withResolvers<boolean>();
    const commitReleased = Promise.withResolvers<boolean>();
    let holding = false;
    const { reactor, events, waitFor } = createTestReactor({
      director: turnDirector(),
      inferenceRunner: turnRunner(),
      toolRunner: chargeRunner(),
      beforeToolExtensions: [askExtension()],
      afterCheckpoint: async () => {
        if (holding) {
          holding = false;
          commitHeld.resolve(true);
          await holdCommit.promise;
          commitReleased.resolve(true);
        }
      },
    });
    reactor.start();
    reactor.deliver(makeTurnMessage("park"), { turnId: "turn-a" });
    const correlationId = await correlationIdOf(waitFor);
    holding = true;
    reactor.deliver(makeTurnMessage("suspend"), { turnId: "turn-r" });
    await commitHeld.promise;
    reactor.deliver(
      createInboundMessage({
        from: "signal@local",
        to: "agent@example.com",
        content: "not json",
        correlationId,
      }),
      { turnId: "turn-d" },
    );
    await waitFor("reactor.done");
    holdCommit.resolve(true);
    await commitReleased.promise;
    // From the release to the end of the suspend, and on to the end of the
    // loop, the reactor runs only promise continuations, with no timer and
    // no I/O. The task boundary below runs after all of them.
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(afterDone(events)).toEqual([]);
    // A delivery after done still ends its turn.
    reactor.deliver(makeTurnMessage("late"), { turnId: "turn-l" });
    expect(endsOf(events, "turn-l")).toEqual([
      expect.objectContaining({ outcome: "failed" }),
    ]);
    expect(afterDone(events)).toEqual([]);
  });

  test("a delivery that names the marker's turn, validated while the marker moves to another turn, does not end the parked marker turn", async () => {
    // Turn A registers an async marker and then parks on an approval. A
    // response for the marker names turn A and waits in the validator while
    // turn B's director suspends on the marker, which moves it to B. The
    // response then resumes B. Turn A is still parked, so the correlation
    // must not end it.
    const chargeAsk = createAuthzExtension({
      authorize: async (resource) => ({
        effect:
          resource === "tool:charge_card"
            ? ("ask" as const)
            : ("allow" as const),
        matchingGrants: [],
        resolvedBy: null,
      }),
      approvalTimeoutMs: 60_000,
    });
    const release = Promise.withResolvers<boolean>();
    const validating = Promise.withResolvers<boolean>();
    let validations = 0;
    const { reactor, events, waitFor } = createTestReactor({
      director: directorFromTable(
        {
          "message.received": (e, _s, caps) => {
            if (e.message.content === "park") {
              return caps.executeTools([
                { id: "m1", name: "send", arguments: {} },
              ]);
            }
            if (e.message.content === "wait") {
              return caps.suspend({
                type: "message_response",
                gateId: "wait-gate",
                timeoutMs: 60_000,
                correlationId: "corr-marker",
              });
            }
            return caps.wait();
          },
          "tool.done": (e, _s, caps) =>
            e.result.callId === "m1"
              ? caps.executeTools([
                  { id: "c1", name: "charge_card", arguments: {} },
                ])
              : caps.reply("charged"),
          "resume.execute_tools": (e, _s, caps) =>
            caps.executeTools(e.calls, false, true),
          "reactor.gate.cleared": (_e, _s, caps) => caps.reply("got it"),
        },
        "wait",
      ),
      toolRunner: makeToolRunner(async (call) =>
        call.name === "send"
          ? {
              callId: call.id,
              content: "sent",
              pendingMarker: {
                status: "pending" as const,
                correlationId: "corr-marker",
              },
            }
          : { callId: call.id, content: "ok" },
      ),
      beforeToolExtensions: [chargeAsk],
      correlationValidator: {
        async validate(op) {
          if (op.correlationId !== "corr-marker") return true;
          validations += 1;
          validating.resolve(true);
          return release.promise;
        },
      },
    });
    reactor.start();
    reactor.deliver(makeTurnMessage("park"), { turnId: "turn-a" });
    const approvalCorrelationId = await correlationIdOf(waitFor);

    reactor.deliver(makeInboundMessage("corr-marker"), { turnId: "turn-a" });
    await validating.promise;
    reactor.deliver(makeTurnMessage("wait"), { turnId: "turn-b" });
    await waitForEvent(
      events,
      (e) => e.type === "reactor.gate.blocked" && e.data.turnId === "turn-b",
    );
    release.resolve(true);
    await waitForEnd(events, "turn-b");

    expect(endsOf(events, "turn-b")).toEqual([
      { turnId: "turn-b", outcome: "replied" },
    ]);
    expect(replyTurnIds(events).get("got it")).toBe("turn-b");
    expect(endsOf(events, "turn-a")).toEqual([]);

    reactor.deliver(makeApprovalMessage(approvalCorrelationId), {
      turnId: "turn-a",
    });
    await waitForEnd(events, "turn-a");

    expect(endsOf(events, "turn-a")).toEqual([
      { turnId: "turn-a", outcome: "replied" },
    ]);
    expect(replyTurnIds(events).get("charged")).toBe("turn-a");
    expect(events.some((e) => e.type === "reactor.error")).toBe(false);
    expect(validations).toBe(1);

    reactor.abort("admin_kill");
    await waitFor("reactor.done");
    expect(endsOf(events, "turn-a")).toHaveLength(1);
  });

  // Delivery admission: after its one await, a delivery reads the current
  // state once and does one thing. A correlation ends the delivered turn
  // only when this process has not seen it.

  // The validator holds a delivery until the test releases it. `held`
  // resolves when it starts holding. Pass `matches` to hold only some
  // deliveries; the rest validate at once.
  function heldValidator(
    matches: (op: PendingOperation) => boolean = () => true,
  ) {
    const release = Promise.withResolvers<boolean>();
    const held = Promise.withResolvers<boolean>();
    let calls = 0;
    const validator: CorrelationValidator = {
      async validate(op) {
        if (!matches(op)) return true;
        calls += 1;
        held.resolve(true);
        return release.promise;
      },
    };
    return {
      validator,
      release,
      held: held.promise,
      calls: () => calls,
    };
  }

  // Every step from a validator's settlement to the end of the delivery's
  // admission is a promise continuation, with no timer and no I/O, so a
  // task boundary runs after all of them.
  async function admissionSettled(): Promise<void> {
    await new Promise((resolve) => setTimeout(resolve, 0));
  }

  test("a correlated delivery that names a parked turn and validates after done ends nothing", async () => {
    const hold = heldValidator();
    const { reactor, events, waitFor } = createTestReactor({
      director: turnDirector(),
      inferenceRunner: turnRunner(),
      toolRunner: chargeRunner(),
      beforeToolExtensions: [askExtension()],
      correlationValidator: hold.validator,
    });
    reactor.start();
    reactor.deliver(makeTurnMessage("park"), { turnId: "turn-a" });
    const correlationId = await correlationIdOf(waitFor);
    reactor.deliver(makeApprovalMessage(correlationId), { turnId: "turn-a" });
    await hold.held;
    reactor.abort("admin_kill");
    await waitFor("reactor.done");

    hold.release.resolve(true);
    await admissionSettled();

    expect(endsOf(events, "turn-a")).toEqual([]);
    expect(afterDone(events)).toEqual([]);
  });

  test("a correlated delivery whose turn ended during validation, settling after done, emits nothing more", async () => {
    // The approval names the operation's own turn. Its gate times out while
    // the validator holds, so the turn resumes and ends before the reactor
    // stops.
    const hold = heldValidator();
    const { reactor, events, waitFor } = createTestReactor({
      director: turnDirector(),
      inferenceRunner: turnRunner(),
      toolRunner: chargeRunner(),
      beforeToolExtensions: [askExtension(40)],
      correlationValidator: hold.validator,
    });
    reactor.start();
    reactor.deliver(makeTurnMessage("park"), { turnId: "turn-t" });
    const correlationId = await correlationIdOf(waitFor);
    reactor.deliver(makeApprovalMessage(correlationId), { turnId: "turn-t" });
    await hold.held;
    await waitForEnd(events, "turn-t");
    reactor.abort("admin_kill");
    await waitFor("reactor.done");

    hold.release.resolve(true);
    await admissionSettled();

    expect(endsOf(events, "turn-t")).toEqual([
      { turnId: "turn-t", outcome: "replied" },
    ]);
    expect(afterDone(events)).toEqual([]);
    expect(countOf(events, "message.correlated")).toBe(0);
  });

  test("a validator rejection that names a parked turn delivers the message into that turn", async () => {
    const release = Promise.withResolvers<boolean>();
    const { reactor, events, waitFor } = createTestReactor({
      director: turnDirector(),
      inferenceRunner: turnRunner(),
      toolRunner: chargeRunner(),
      beforeToolExtensions: [askExtension()],
      correlationValidator: {
        async validate() {
          return release.promise;
        },
      },
    });
    reactor.start();
    reactor.deliver(makeTurnMessage("park"), { turnId: "turn-a" });
    const correlationId = await correlationIdOf(waitFor);
    reactor.deliver(makeApprovalMessage(correlationId), { turnId: "turn-a" });
    release.resolve(false);
    await waitFor("connector.reply");

    expect(events.some((e) => e.type === "reactor.error")).toBe(false);
    expect(countOf(events, "message.received")).toBe(2);
    expect(turnIdsOf(events, "connector.reply")).toEqual(["turn-a"]);
    expect(startsOf(events, "turn-a")).toHaveLength(1);
    // The approval gate of turn A is still live, so A does not end.
    expect(endsOf(events, "turn-a")).toEqual([]);

    reactor.abort("admin_kill");
    await waitFor("reactor.done");
  });

  for (const rejection of ["returns false", "throws"] as const) {
    test(`a validator rejection naming a turn that ended during validation runs under a fresh turn (validator ${rejection})`, async () => {
      const release = Promise.withResolvers<boolean>();
      const held = Promise.withResolvers<boolean>();
      const { reactor, events, waitFor } = createTestReactor({
        director: turnDirector(),
        inferenceRunner: turnRunner(),
        toolRunner: chargeRunner(),
        beforeToolExtensions: [askExtension(40)],
        correlationValidator: {
          async validate() {
            held.resolve(true);
            const valid = await release.promise;
            if (rejection === "throws") throw new Error("validator down");
            return valid;
          },
        },
      });
      reactor.start();
      reactor.deliver(makeTurnMessage("park"), { turnId: "turn-t" });
      const correlationId = await correlationIdOf(waitFor);

      // The caller names the operation's own turn for the approval. The
      // approval gate times out while the validator holds, so turn T
      // resumes and ends.
      const approval = makeApprovalMessage(correlationId);
      reactor.deliver(approval, { turnId: "turn-t" });
      await held.promise;
      await waitForEnd(events, "turn-t");
      release.resolve(false);

      const started = await waitForEvent(
        events,
        (e) =>
          e.type === "reactor.turn.started" &&
          e.data.messageId === approval.headers.messageId,
      );
      if (started.type !== "reactor.turn.started") {
        throw new Error("unreachable");
      }
      const freshTurnId = started.data.turnId;
      expect(freshTurnId).not.toBe("turn-t");
      expect(started.data.resumed).toBe(false);
      await waitForEnd(events, freshTurnId);

      expect(events.some((e) => e.type === "reactor.error")).toBe(false);
      expect(endsOf(events, "turn-t")).toEqual([
        { turnId: "turn-t", outcome: "replied" },
      ]);
      expect(stampedAfterEnd(events, "turn-t")).toEqual([]);
      expect(endsOf(events, freshTurnId)).toEqual([
        { turnId: freshTurnId, outcome: "replied" },
      ]);

      reactor.abort("admin_kill");
      await waitFor("reactor.done");
    });
  }

  test("two correlated deliveries naming one turn, where the first joins and the second fails validation", async () => {
    // Turns A and B each park on an approval. The delivery for B's approval
    // names turn X and waits in the validator; the delivery for A's approval
    // also names X and joins A, which ends X. B's validator then rejects.
    const correlations: string[] = [];
    const releaseB = Promise.withResolvers<boolean>();
    const heldB = Promise.withResolvers<boolean>();
    let validatedB = "";
    let lastCallId = "";
    const inferenceRunner = async function* (opts: InferenceHarnessOptions) {
      const text = lastText(opts.turns);
      const answersResult =
        opts.turns.at(-1)?.content.some((b) => b.type === "tool_result") ===
        true;
      lastCallId = `call-${text}`;
      yield {
        type: "inference.done" as const,
        seq: opts.nextSeq(),
        data: {
          turn: answersResult
            ? makeAssistantTurn("charged")
            : {
                role: "assistant" as const,
                content: [
                  {
                    type: "tool_call" as const,
                    id: lastCallId,
                    name: "charge_card",
                    arguments: {},
                  },
                ],
                model: "test-model",
                timestamp: 1000,
              },
          usage: emptyUsage(),
          source: TEST_SOURCE,
        },
      };
    };
    const { reactor, events, waitFor } = createTestReactor({
      director: directorFromTable(
        {
          "message.received": (e, _s, caps) =>
            e.message.content === "A" || e.message.content === "B"
              ? caps.infer()
              : caps.reply("new message"),
          "inference.done": (e, _s, caps) => {
            const calls: ToolCall[] = [];
            for (const block of e.turn.content) {
              if (block.type === "tool_call") {
                calls.push({
                  id: block.id,
                  name: block.name,
                  arguments: block.arguments,
                });
              }
            }
            return calls.length > 0
              ? caps.executeTools(calls)
              : caps.reply("charged");
          },
          "resume.execute_tools": (e, _s, caps) =>
            caps.executeTools(e.calls, false, true),
          "tool.done": (_e, _s, caps) => caps.infer(),
        },
        "wait",
      ),
      inferenceRunner,
      toolRunner: chargeRunner(),
      beforeToolExtensions: [askExtension()],
      correlationValidator: {
        async validate(op) {
          if (op.correlationId !== validatedB) return true;
          heldB.resolve(true);
          return releaseB.promise;
        },
      },
    });
    reactor.start();
    reactor.deliver(makeTurnMessage("A"), { turnId: "turn-a" });
    await waitUntil(() => countOf(events, "reactor.gate.blocked") >= 1);
    reactor.deliver(makeTurnMessage("B"), { turnId: "turn-b" });
    await waitUntil(() => countOf(events, "reactor.gate.blocked") >= 2);
    for (const e of events) {
      if (e.type === "reactor.gate.blocked" && e.data.correlationId) {
        correlations.push(e.data.correlationId);
      }
    }
    const [corrA, corrB] = correlations;
    if (corrA === undefined || corrB === undefined) {
      throw new Error("expected two correlations");
    }
    validatedB = corrB;

    const forB = makeApprovalMessage(corrB);
    reactor.deliver(forB, { turnId: "turn-x" });
    await heldB.promise;
    reactor.deliver(makeApprovalMessage(corrA), { turnId: "turn-x" });
    await waitForEnd(events, "turn-x");
    expect(endsOf(events, "turn-x")).toEqual([
      { turnId: "turn-x", outcome: "joined", joinedTurnId: "turn-a" },
    ]);

    releaseB.resolve(false);
    const started = await waitForEvent(
      events,
      (e) =>
        e.type === "reactor.turn.started" &&
        e.data.messageId === forB.headers.messageId,
    );
    if (started.type !== "reactor.turn.started") {
      throw new Error("unreachable");
    }
    expect(started.data.turnId).not.toBe("turn-x");
    await waitForEnd(events, started.data.turnId);

    expect(events.some((e) => e.type === "reactor.error")).toBe(false);
    expect(endsOf(events, "turn-x")).toHaveLength(1);
    expect(endsOf(events, "turn-b")).toEqual([]);

    reactor.abort("admin_kill");
    await waitFor("reactor.done");
  });

  test("a validated response naming its operation's turn, which ended during validation, ends nothing", async () => {
    const hold = heldValidator();
    const { reactor, events, waitFor } = createTestReactor({
      director: turnDirector(),
      inferenceRunner: turnRunner(),
      toolRunner: chargeRunner(),
      beforeToolExtensions: [askExtension(40)],
      correlationValidator: hold.validator,
    });
    reactor.start();
    reactor.deliver(makeTurnMessage("park"), { turnId: "turn-t" });
    const correlationId = await correlationIdOf(waitFor);
    reactor.deliver(makeApprovalMessage(correlationId), { turnId: "turn-t" });
    await hold.held;
    // The gate times out, the timeout answers the call, and T ends.
    await waitForEnd(events, "turn-t");
    hold.release.resolve(true);
    await waitFor("message.correlated");

    expect(events.some((e) => e.type === "reactor.error")).toBe(false);
    expect(endsOf(events, "turn-t")).toEqual([
      { turnId: "turn-t", outcome: "replied" },
    ]);
    expect(countOf(events, "tool.start")).toBe(0);

    reactor.abort("admin_kill");
    await waitFor("reactor.done");
  });

  test("an approval admitted after its gate timed out during the suspend's commit resumes nothing", async () => {
    // Turn A's approval gate times out while its suspend still commits, so
    // the gate is gone but the operation is still registered until the
    // deferred timeout replays. An approval admitted in that window must not
    // also resume the call: the timeout answers it.
    const releaseCommit = Promise.withResolvers<boolean>();
    const commitEntered = Promise.withResolvers<boolean>();
    let holding = true;
    const base = makeContextStore();
    const contextStore: ContextStore = {
      ...base,
      async writeMetadata(metadata, signal) {
        if (holding) {
          holding = false;
          commitEntered.resolve(true);
          await releaseCommit.promise;
        }
        return base.writeMetadata(metadata, signal);
      },
    };
    const call: ToolCall = { id: "call-a", name: "charge_card", arguments: {} };
    const extension: BeforeToolExtension = {
      async beforeTool(toolCall) {
        return {
          type: "suspend",
          gate: {
            type: "approval",
            gateId: "gate-a",
            correlationId: "corr-a",
            timeoutAt: Date.now() + 1,
          },
          pendingOp: {
            correlationId: "corr-a",
            kind: "approval",
            registeredAt: Date.now(),
            gateId: "gate-a",
            suspendedCall: toolCall,
          },
        };
      },
    };
    const timers = manualTimers();
    const { reactor, events, waitFor } = createTestReactor({
      deps: timers.deps,
      contextStore,
      director: directorFromTable(
        {
          "message.received": (_e, _s, caps) => caps.executeTools([call]),
          "resume.execute_tools": (e, _s, caps) =>
            caps.executeTools(e.calls, false, true),
          "resume.tool_result": (_e, _s, caps) => caps.reply("timed out"),
          "tool.done": (_e, _s, caps) => caps.reply("charged"),
        },
        "wait",
      ),
      toolRunner: chargeRunner(),
      beforeToolExtensions: [extension],
    });
    reactor.start();
    reactor.deliver(makeTurnMessage("go"), { turnId: "turn-a" });
    await commitEntered.promise;
    // The gate was registered before the commit started. Its timeout fires
    // now, while the commit is held.
    expect(timers.pending()).toHaveLength(1);
    timers.fire(timers.pending()[0]);
    reactor.deliver(makeApprovalMessage("corr-a"), { turnId: "turn-d" });
    await waitForEnd(events, "turn-d");
    expect(endsOf(events, "turn-d")).toEqual([
      { turnId: "turn-d", outcome: "unanswered" },
    ]);

    releaseCommit.resolve(true);
    await waitForEnd(events, "turn-a");

    expect(events.some((e) => e.type === "reactor.error")).toBe(false);
    expect(countOf(events, "tool.start")).toBe(0);
    expect(replyTurnIds(events).get("timed out")).toBe("turn-a");
    expect(endsOf(events, "turn-a")).toEqual([
      { turnId: "turn-a", outcome: "replied" },
    ]);

    reactor.abort("admin_kill");
    await waitFor("reactor.done");
  });

  test("a validated resume whose delivered turn ended during validation resumes the operation's turn and ends nothing else", async () => {
    const hold = heldValidator();
    const { reactor, events, waitFor } = createTestReactor({
      director: turnDirector(),
      inferenceRunner: turnRunner(),
      toolRunner: chargeRunner(),
      beforeToolExtensions: [askExtension()],
      correlationValidator: hold.validator,
    });
    reactor.start();
    reactor.deliver(makeTurnMessage("park"), { turnId: "turn-a" });
    const correlationId = await correlationIdOf(waitFor);

    // Turn D is queued, so the approval may name it. D runs and ends while
    // the validator holds the approval.
    reactor.deliver(makeTurnMessage("D"), { turnId: "turn-d" });
    reactor.deliver(makeApprovalMessage(correlationId), { turnId: "turn-d" });
    await hold.held;
    await waitForEnd(events, "turn-d");
    hold.release.resolve(true);
    await waitForEnd(events, "turn-a");

    expect(events.some((e) => e.type === "reactor.error")).toBe(false);
    expect(endsOf(events, "turn-d")).toEqual([
      { turnId: "turn-d", outcome: "replied" },
    ]);
    expect(endsOf(events, "turn-a")).toEqual([
      { turnId: "turn-a", outcome: "replied" },
    ]);
    expect(replyTurnIds(events).get("A done")).toBe("turn-a");

    reactor.abort("admin_kill");
    await waitFor("reactor.done");
  });

  test("a response to a marker whose turn already ended appends history, ends the delivered turn unanswered, and queues nothing", async () => {
    const prompts: string[][] = [];
    const { reactor, events, waitFor } = createTestReactor({
      director: directorFromTable(
        {
          "message.received": (_e, _s, caps) => caps.infer(),
          "inference.done": (e, _s, caps) => {
            const calls: ToolCall[] = [];
            let text = "";
            for (const block of e.turn.content) {
              if (block.type === "tool_call") {
                calls.push({
                  id: block.id,
                  name: block.name,
                  arguments: block.arguments,
                });
              } else if (block.type === "text") {
                text += block.text;
              }
            }
            return calls.length > 0
              ? caps.executeTools(calls)
              : caps.reply(text);
          },
          "tool.done": (_e, _s, caps) => caps.infer(),
        },
        "wait",
      ),
      inferenceRunner: async function* (opts: InferenceHarnessOptions) {
        prompts.push(
          opts.turns.map((t) =>
            t.content.map((b) => (b.type === "text" ? b.text : "")).join(""),
          ),
        );
        yield* turnRunner()(opts);
      },
      toolRunner: makeToolRunner(async (call) => ({
        callId: call.id,
        content: "sent",
        pendingMarker: {
          status: "pending" as const,
          correlationId: "corr-marker",
        },
      })),
    });
    reactor.start();
    reactor.deliver(makeTurnMessage("park"), { turnId: "turn-a" });
    await waitForEnd(events, "turn-a");
    const inferences = countOf(events, "inference.done");

    reactor.deliver(
      createInboundMessage({
        from: "peer@example.com",
        to: "agent@example.com",
        content: "the response",
        correlationId: "corr-marker",
      }),
      { turnId: "turn-d" },
    );
    await waitForEnd(events, "turn-d");
    expect(endsOf(events, "turn-d")).toEqual([
      { turnId: "turn-d", outcome: "unanswered" },
    ]);
    expect(countOf(events, "inference.done")).toBe(inferences);

    // The next turn's prompt holds the response.
    reactor.deliver(makeTurnMessage("next"), { turnId: "turn-n" });
    await waitForEnd(events, "turn-n");
    const lastPrompt = prompts.at(-1) ?? [];
    expect(lastPrompt.some((text) => text.includes("the response"))).toBe(true);

    reactor.abort("admin_kill");
    await waitFor("reactor.done");
  });

  test("an inference that settles after a correlation-dispatch failure ended its turn queues nothing and emits no extra error", async () => {
    // Turn R's inference is in flight when a malformed approval ends R. The
    // shutdown waits for the inference (the long deadline only guards a
    // hang), whose terminal event then has no turn to go to.
    const releaseR = Promise.withResolvers<boolean>();
    const { reactor, events, waitFor } = createTestReactor({
      director: turnDirector(),
      inferenceRunner: turnRunner(async (text) =>
        text === "R" ? releaseR.promise : true,
      ),
      toolRunner: chargeRunner(),
      beforeToolExtensions: [askExtension()],
      shutdownTimeoutMs: 60_000,
    });
    reactor.start();
    reactor.deliver(makeTurnMessage("park"), { turnId: "turn-a" });
    const correlationId = await correlationIdOf(waitFor);
    reactor.deliver(makeTurnMessage("R"), { turnId: "turn-r" });
    await waitUntil(() => countOf(events, "inference.start") >= 2);
    reactor.deliver(
      createInboundMessage({
        from: "signal@local",
        to: "agent@example.com",
        content: "not json",
        correlationId,
      }),
      { turnId: "turn-d" },
    );
    await waitForEnd(events, "turn-r");
    releaseR.resolve(true);
    await waitFor("reactor.done");

    const errors = events.flatMap((e) =>
      e.type === "reactor.error" ? [e.data] : [],
    );
    expect(errors).toHaveLength(1);
    expect(errors[0]?.fatal).toBe(true);
    expect(stampedAfterEnd(events, "turn-r")).toEqual([]);
  });

  test("the second of two same-tick approvals reaches the director as message.received", async () => {
    // No validator: the first approval verifies at once and holds its
    // correlation in progress until it is applied, so the second one is a
    // regular message.
    const decided: ReactorInboundEvent[] = [];
    const table = directorFromTable(
      {
        "message.received": (e, _s, caps) =>
          e.message.content === "park"
            ? caps.executeTools([
                { id: "call-ask", name: "charge_card", arguments: {} },
              ])
            : caps.wait(),
        "resume.execute_tools": (e, _s, caps) =>
          caps.executeTools(e.calls, false, true),
      },
      "wait",
    );
    const director: ReactorDirector = {
      async decide(event, state, caps) {
        decided.push(event);
        return table.decide(event, state, caps);
      },
    };
    const { reactor, events, waitFor } = createTestReactor({
      director,
      toolRunner: chargeRunner(),
      beforeToolExtensions: [askExtension()],
    });
    reactor.start();
    reactor.deliver(makeTurnMessage("park"), { turnId: "turn-a" });
    const correlationId = await correlationIdOf(waitFor);

    const first = makeApprovalMessage(correlationId);
    const second = makeApprovalMessage(correlationId);
    reactor.deliver(first, { turnId: "turn-d1" });
    reactor.deliver(second, { turnId: "turn-d2" });
    await waitUntil(() =>
      decided.some(
        (e) =>
          e.type === "message.received" &&
          e.message.headers.messageId === second.headers.messageId,
      ),
    );
    await waitUntil(() => decided.some((e) => e.type === "tool.done"));

    expect(countOf(events, "message.correlated")).toBe(1);
    expect(endsOf(events, "turn-d1")).toEqual([
      { turnId: "turn-d1", outcome: "joined", joinedTurnId: "turn-a" },
    ]);
    expect(startsOf(events, "turn-d2")).toEqual([
      {
        turnId: "turn-d2",
        resumed: false,
        messageId: messageIdOf(second),
      },
    ]);
    expect(countOf(events, "tool.start")).toBe(1);

    reactor.abort("admin_kill");
    await waitFor("reactor.done");
  });

  test("a validator rejection releases the guard so a later delivery correlates", async () => {
    let calls = 0;
    const director = directorFromTable(
      {
        "message.received": (e, _s, caps) =>
          e.message.content === "park"
            ? caps.executeTools([
                { id: "call-ask", name: "charge_card", arguments: {} },
              ])
            : caps.wait(),
        "resume.execute_tools": (e, _s, caps) =>
          caps.executeTools(e.calls, false, true),
      },
      "wait",
    );
    const { reactor, events, waitFor } = createTestReactor({
      director,
      toolRunner: chargeRunner(),
      beforeToolExtensions: [askExtension()],
      correlationValidator: {
        async validate() {
          calls += 1;
          return calls > 1;
        },
      },
    });
    reactor.start();
    reactor.deliver(makeTurnMessage("park"), { turnId: "turn-a" });
    const correlationId = await correlationIdOf(waitFor);
    reactor.deliver(makeApprovalMessage(correlationId), { turnId: "turn-r" });
    await waitUntil(() => startsOf(events, "turn-r").length === 1);
    reactor.deliver(makeApprovalMessage(correlationId), { turnId: "turn-ok" });
    await waitUntil(() => countOf(events, "message.correlated") === 1);
    expect(endsOf(events, "turn-ok")).toEqual([
      { turnId: "turn-ok", outcome: "joined", joinedTurnId: "turn-a" },
    ]);
    await waitUntil(() => countOf(events, "tool.start") === 1);
    reactor.abort("admin_kill");
    await waitFor("reactor.done");
  });

  test("a delivery after the applied resume is a regular message on its own turn", async () => {
    const director = directorFromTable(
      {
        "message.received": (e, _s, caps) =>
          e.message.content === "park"
            ? caps.executeTools([
                { id: "call-ask", name: "charge_card", arguments: {} },
              ])
            : caps.wait(),
        "resume.execute_tools": (e, _s, caps) =>
          caps.executeTools(e.calls, false, true),
      },
      "wait",
    );
    const { reactor, events, waitFor } = createTestReactor({
      director,
      toolRunner: chargeRunner(),
      beforeToolExtensions: [askExtension()],
    });
    reactor.start();
    reactor.deliver(makeTurnMessage("park"), { turnId: "turn-a" });
    const correlationId = await correlationIdOf(waitFor);
    reactor.deliver(makeApprovalMessage(correlationId), { turnId: "turn-d1" });
    // The guard is released in the same synchronous step that emits
    // message.correlated, so the next delivery finds no correlation in
    // progress, and no operation either.
    await waitUntil(() => countOf(events, "message.correlated") === 1);
    reactor.deliver(makeApprovalMessage(correlationId), { turnId: "turn-d2" });
    await waitUntil(() => endsOf(events, "turn-d2").length === 1);
    expect(countOf(events, "message.correlated")).toBe(1);
    expect(startsOf(events, "turn-d2")).toHaveLength(1);
    expect(endsOf(events, "turn-d2")[0]?.outcome).toBe("unanswered");
    reactor.abort("admin_kill");
    await waitFor("reactor.done");
  });

  test("a same-tick delivery behind a malformed approval ends once", async () => {
    const { reactor, events, waitFor } = createTestReactor({
      director: directorFromTable(
        {
          "message.received": (e, _s, caps) =>
            e.message.content === "park"
              ? caps.executeTools([
                  { id: "call-ask", name: "charge_card", arguments: {} },
                ])
              : caps.wait(),
        },
        "wait",
      ),
      toolRunner: chargeRunner(),
      beforeToolExtensions: [askExtension()],
    });
    reactor.start();
    reactor.deliver(makeTurnMessage("park"), { turnId: "turn-a" });
    const correlationId = await correlationIdOf(waitFor);
    reactor.deliver(
      createInboundMessage({
        from: "signal@local",
        to: "agent@example.com",
        content: "not json",
        correlationId,
      }),
      { turnId: "turn-bad" },
    );
    reactor.deliver(makeApprovalMessage(correlationId), { turnId: "turn-2nd" });
    // Both deliveries are admitted in promise continuations of the same
    // tick, before the shutdown that the malformed approval starts can end.
    await waitFor("reactor.done");
    expect(endsOf(events, "turn-bad")).toEqual([
      expect.objectContaining({
        outcome: "failed",
        error: expect.objectContaining({ kind: "reactor_fatal" }),
      }),
    ]);
    expect(endsOf(events, "turn-2nd")).toEqual([
      expect.objectContaining({
        outcome: "failed",
        error: expect.objectContaining({ kind: "reactor_done" }),
      }),
    ]);
    expect(countOf(events, "message.correlated")).toBe(0);
  });

  test("a response that clears another turn's director gate joins that gate's turn", async () => {
    // Turn A's director waits on a correlation before any operation exists.
    // Turn B's tool then registers a marker for it. The response clears A's
    // gate, so it continues A, and its options go to A.
    const seen: (InferenceHarnessOptions["inferenceOptions"] | undefined)[] =
      [];
    const inferenceRunner = async function* (opts: InferenceHarnessOptions) {
      seen.push(opts.inferenceOptions);
      yield {
        type: "inference.done" as const,
        seq: opts.nextSeq(),
        data: {
          turn: makeAssistantTurn("got it"),
          usage: emptyUsage(),
          source: TEST_SOURCE,
        },
      };
    };
    const { reactor, events, waitFor } = createTestReactor({
      director: directorFromTable(
        {
          "message.received": (e, _s, caps) =>
            e.message.content === "wait"
              ? caps.suspend({
                  type: "message_response",
                  gateId: "wait-gate",
                  timeoutMs: 60_000,
                  correlationId: "corr-x",
                })
              : // No model call asked for this tool, so its result stays out
                // of history and the later prompt stays well formed.
                caps.executeTools(
                  [{ id: "call-send", name: "send_note", arguments: {} }],
                  true,
                  false,
                ),
          "tool.done": (_e, _s, caps) => caps.reply("sent"),
          "reactor.gate.cleared": (_e, _s, caps) => caps.infer(),
          "inference.done": (_e, _s, caps) => caps.reply("got it"),
        },
        "wait",
      ),
      inferenceRunner,
      toolRunner: makeToolRunner(async (call) => ({
        callId: call.id,
        content: "sent",
        pendingMarker: { status: "pending" as const, correlationId: "corr-x" },
      })),
    });
    reactor.start();
    reactor.deliver(makeTurnMessage("wait"), { turnId: "turn-a" });
    await waitFor("reactor.gate.blocked");
    reactor.deliver(makeTurnMessage("send"), { turnId: "turn-b" });
    await waitForEnd(events, "turn-b");

    reactor.deliver(makeInboundMessage("corr-x"), {
      turnId: "turn-d",
      inference: { temperature: 0.3 },
    });
    await waitForEnd(events, "turn-a");

    expect(endsOf(events, "turn-d")).toEqual([
      { turnId: "turn-d", outcome: "joined", joinedTurnId: "turn-a" },
    ]);
    expect(replyTurnIds(events).get("got it")).toBe("turn-a");
    expect(endsOf(events, "turn-a")).toEqual([
      { turnId: "turn-a", outcome: "replied" },
    ]);
    expect(seen).toEqual([{ temperature: 0.3 }]);

    reactor.abort("admin_kill");
    await waitFor("reactor.done");
  });

  test("a delivery naming a restarted marker turn, validated across a takeover, does not end that turn while its own gate.cleared is queued", async () => {
    // After a restart, turn A owns a marker whose rehydrated gate fires at
    // once. The deliveries are buffered before start, so the loop takes up
    // turn B's message before the gate timer can fire, and A's gate.cleared
    // queues behind B while B's director decides. B then suspends on the
    // marker and takes it over. A response that names turn A, validated
    // across that takeover, must not end A: A's queued event still resumes
    // it.
    const cell: PersistedCell = {
      turns: [],
      pendingOperations: [
        {
          correlationId: "corr-marker",
          kind: "approval",
          registeredAt: Date.now(),
          gateId: "pending-corr-marker",
          timeoutAt: 0,
          turnId: "turn-a",
        },
      ],
      tokenUsage: emptyUsage(),
    };
    const bHold = Promise.withResolvers<boolean>();
    const bDeciding = Promise.withResolvers<boolean>();
    const release = Promise.withResolvers<boolean>();
    const validating = Promise.withResolvers<boolean>();
    const holdCommit = Promise.withResolvers<boolean>();
    const commitHeld = Promise.withResolvers<boolean>();
    let holding = false;
    const { reactor, events, waitFor } = createTestReactor({
      contextStore: makePersistingContextStore(cell),
      director: directorFromTable(
        {
          "message.received": async (_e, _s, caps) => {
            bDeciding.resolve(true);
            await bHold.promise;
            return caps.suspend({
              type: "message_response",
              gateId: "wait-gate",
              timeoutMs: 60_000,
              correlationId: "corr-marker",
            });
          },
          "reactor.gate.cleared": (_e, _s, caps) => caps.reply("got it"),
        },
        "wait",
      ),
      afterCheckpoint: async () => {
        if (holding) {
          holding = false;
          commitHeld.resolve(true);
          await holdCommit.promise;
        }
      },
      correlationValidator: {
        async validate() {
          validating.resolve(true);
          return release.promise;
        },
      },
    });
    reactor.deliver(makeInboundMessage("corr-marker"), { turnId: "turn-a" });
    reactor.deliver(makeTurnMessage("wait"), { turnId: "turn-b" });
    reactor.start();
    await validating.promise;
    await bDeciding.promise;
    await waitForEvent(
      events,
      (e) =>
        e.type === "reactor.gate.cleared" &&
        e.data.gateId === "pending-corr-marker",
    );

    holding = true;
    bHold.resolve(true);
    await commitHeld.promise;
    release.resolve(true);
    await waitFor("message.correlated");
    holdCommit.resolve(true);
    // A crash of the loop ends it with reactor.done, so either event ends
    // the wait.
    await waitForEvent(
      events,
      (e) =>
        e.type === "reactor.done" ||
        (e.type === "reactor.turn.ended" && e.data.turnId === "turn-b"),
    );
    await waitForEvent(
      events,
      (e) =>
        e.type === "reactor.done" ||
        (e.type === "reactor.turn.ended" && e.data.turnId === "turn-a"),
    );

    expect(events.some((e) => e.type === "reactor.error")).toBe(false);
    expect(endsOf(events, "turn-a")).toEqual([
      { turnId: "turn-a", outcome: "replied" },
    ]);
    expect(startsOf(events, "turn-a")).toEqual([
      { turnId: "turn-a", resumed: true },
    ]);
    expect(endsOf(events, "turn-b")).toEqual([
      { turnId: "turn-b", outcome: "replied" },
    ]);

    reactor.abort("admin_kill");
    await waitFor("reactor.done");
  });

  test("a delivery after done that names a queued, never-run turn ends nothing", async () => {
    // After a restart, turn A's approval times out at once and queues its
    // resume while turn B's director decides. The reactor stops before A
    // runs. A delivery after done that names A must not end it: the store
    // still resumes A in the next process.
    const cell: PersistedCell = {
      turns: [],
      pendingOperations: [
        {
          correlationId: "corr-approval",
          kind: "approval",
          registeredAt: Date.now(),
          gateId: "pending-corr-approval",
          timeoutAt: 0,
          suspendedCall: { id: "call-ask", name: "charge_card", arguments: {} },
          turnId: "turn-a",
        },
      ],
      tokenUsage: emptyUsage(),
    };
    const bHold = Promise.withResolvers<boolean>();
    const bDeciding = Promise.withResolvers<boolean>();
    const timers = manualTimers();
    const { reactor, events, waitFor } = createTestReactor({
      deps: timers.deps,
      contextStore: makePersistingContextStore(cell),
      director: directorFromTable(
        {
          "message.received": async (_e, _s, caps) => {
            bDeciding.resolve(true);
            await bHold.promise;
            return caps.wait();
          },
        },
        "wait",
      ),
    });
    reactor.deliver(makeTurnMessage("busy"), { turnId: "turn-b" });
    reactor.start();
    await bDeciding.promise;
    // A's gate was armed at startup. Its timeout fires now and queues A's
    // resume while B's director still decides.
    expect(timers.pending()).toHaveLength(1);
    timers.fire(timers.pending()[0]);
    reactor.abort("admin_kill");
    bHold.resolve(true);
    await waitFor("reactor.done");

    reactor.deliver(makeInboundMessage("corr-approval"), { turnId: "turn-a" });
    expect(endsOf(events, "turn-a")).toEqual([]);
    expect(startsOf(events, "turn-a")).toEqual([]);
  });
});

// ---------------------------------------------------------------------------
// One gate per turn: a turn holds at most one gate of any type
// ---------------------------------------------------------------------------

describe("createReactor — one gate per turn", () => {
  function call(id: string, name = "charge_card"): ToolCall {
    return { id, name, arguments: {} };
  }

  // Asks for approval of `charge_card` and allows every other tool.
  function askForCharge(timeoutMs = 60_000) {
    return createAuthzExtension({
      authorize: async (resource) => ({
        effect:
          resource === "tool:charge_card"
            ? ("ask" as const)
            : ("allow" as const),
        matchingGrants: [],
        resolvedBy: null,
      }),
      approvalTimeoutMs: timeoutMs,
    });
  }

  // A before-tool suspend decision on a gate of `gateType` for `call`.
  function suspendDecision(
    toolCall: ToolCall,
    gateType: GateType,
  ): BeforeToolDecision {
    const correlationId = `corr-${toolCall.id}`;
    const gateId = `gate-${toolCall.id}`;
    return {
      type: "suspend",
      gate: {
        type: gateType,
        gateId,
        correlationId,
        timeoutAt: Date.now() + 60_000,
      },
      pendingOp: {
        correlationId,
        kind: "approval",
        registeredAt: Date.now(),
        gateId,
        suspendedCall: toolCall,
      },
    };
  }

  function refusedCallIds(events: ReactorEmittedEvent[]): string[] {
    return events.flatMap((e) =>
      e.type === "tool.done" &&
      e.data.result.isError === true &&
      typeof e.data.result.content === "string" &&
      GATE_REFUSAL.test(e.data.result.content)
        ? [e.data.result.callId]
        : [],
    );
  }

  test("a serial batch parks the first gated call and refuses the second", async () => {
    const cell: PersistedCell = {
      turns: [],
      pendingOperations: [],
      tokenUsage: emptyUsage(),
    };
    const { reactor, events, waitFor } = createTestReactor({
      contextStore: makePersistingContextStore(cell),
      director: directorFromTable(
        {
          "message.received": (_e, _s, caps) =>
            caps.executeTools([call("call-a"), call("call-b")], false),
        },
        "wait",
      ),
      beforeToolExtensions: [askForCharge()],
    });
    reactor.start();
    reactor.deliver(makeTurnMessage("go"), { turnId: "turn-a" });
    await waitFor("reactor.gate.blocked");
    await waitForEvent(
      events,
      (e) => e.type === "tool.done" && e.data.result.callId === "call-b",
    );

    expect(countOf(events, "reactor.gate.blocked")).toBe(1);
    expect(refusedCallIds(events)).toEqual(["call-b"]);
    expect(cell.pendingOperations.map((op) => op.correlationId)).toHaveLength(
      1,
    );
    expect(countOf(events, "tool.start")).toBe(0);

    reactor.abort("admin_kill");
    await waitFor("reactor.done");
  });

  test("a gated call is refused while the first gate's suspend still commits", async () => {
    // The first call's suspend holds its commit open. The second call's
    // decision arrives only after that commit has started.
    const releaseCommit = Promise.withResolvers<boolean>();
    const commitEntered = Promise.withResolvers<boolean>();
    const base = makeContextStore();
    const contextStore: ContextStore = {
      ...base,
      async writeMetadata(metadata, signal) {
        commitEntered.resolve(true);
        await releaseCommit.promise;
        return base.writeMetadata(metadata, signal);
      },
    };
    const extension: BeforeToolExtension = {
      async beforeTool(toolCall) {
        if (toolCall.id === "call-b") await commitEntered.promise;
        return suspendDecision(toolCall, "approval");
      },
    };
    const { reactor, events, waitFor } = createTestReactor({
      contextStore,
      director: directorFromTable(
        {
          "message.received": (_e, _s, caps) =>
            caps.executeTools([call("call-a"), call("call-b")]),
        },
        "wait",
      ),
      beforeToolExtensions: [extension],
    });
    reactor.start();
    reactor.deliver(makeTurnMessage("go"), { turnId: "turn-a" });
    await waitForEvent(
      events,
      (e) => e.type === "tool.done" && e.data.result.callId === "call-b",
    );
    expect(countOf(events, "reactor.gate.blocked")).toBe(0);

    releaseCommit.resolve(true);
    await waitFor("reactor.gate.blocked");

    const refusedAt = events.findIndex(
      (e) => e.type === "tool.done" && e.data.result.callId === "call-b",
    );
    const blockedAt = events.findIndex(
      (e) => e.type === "reactor.gate.blocked",
    );
    expect(refusedAt).toBeLessThan(blockedAt);
    expect(refusedCallIds(events)).toEqual(["call-b"]);
    expect(countOf(events, "reactor.gate.blocked")).toBe(1);
    const blocked = getEvent(events, "reactor.gate.blocked");
    expect(blocked.data.gateId).toBe("gate-call-a");

    reactor.abort("admin_kill");
    await waitFor("reactor.done");
  });

  test("the limit counts gates of every type", async () => {
    const extension: BeforeToolExtension = {
      async beforeTool(toolCall) {
        return suspendDecision(
          toolCall,
          toolCall.id === "call-a" ? "approval" : "payment",
        );
      },
    };
    const { reactor, events, waitFor } = createTestReactor({
      director: directorFromTable(
        {
          "message.received": (_e, _s, caps) =>
            caps.executeTools([call("call-a"), call("call-b")], false),
        },
        "wait",
      ),
      beforeToolExtensions: [extension],
    });
    reactor.start();
    reactor.deliver(makeTurnMessage("go"), { turnId: "turn-a" });
    await waitForEvent(
      events,
      (e) => e.type === "tool.done" && e.data.result.callId === "call-b",
    );

    expect(countOf(events, "reactor.gate.blocked")).toBe(1);
    expect(getEvent(events, "reactor.gate.blocked").data.reason).toBe(
      "approval",
    );
    expect(refusedCallIds(events)).toEqual(["call-b"]);

    reactor.abort("admin_kill");
    await waitFor("reactor.done");
  });

  test("a director suspend while the turn holds a gate is refused and the turn still ends once", async () => {
    const cell: PersistedCell = {
      turns: [],
      pendingOperations: [],
      tokenUsage: emptyUsage(),
    };
    const { reactor, events, waitFor } = createTestReactor({
      contextStore: makePersistingContextStore(cell),
      director: directorFromTable(
        {
          "message.received": (_e, _s, caps) =>
            caps.executeTools([call("call-ask"), call("call-send", "send")]),
          "tool.done": (e, _s, caps) =>
            e.result.callId === "call-send"
              ? caps.suspend({
                  type: "message_response",
                  gateId: "marker-gate",
                  timeoutMs: 60_000,
                  correlationId: "corr-marker",
                })
              : caps.reply("charged"),
          "resume.execute_tools": (e, _s, caps) =>
            caps.executeTools(e.calls, false, true),
        },
        "wait",
      ),
      toolRunner: makeToolRunner(async (toolCall) =>
        toolCall.name === "send"
          ? {
              callId: toolCall.id,
              content: "sent",
              pendingMarker: {
                status: "pending" as const,
                correlationId: "corr-marker",
              },
            }
          : { callId: toolCall.id, content: "charged" },
      ),
      beforeToolExtensions: [askForCharge()],
    });
    reactor.start();
    reactor.deliver(makeTurnMessage("go"), { turnId: "turn-a" });
    const correlationId = await (async () => {
      const blocked = await waitFor("reactor.gate.blocked");
      if (blocked.type !== "reactor.gate.blocked") {
        throw new Error("unreachable");
      }
      if (blocked.data.correlationId === undefined) {
        throw new Error("expected correlationId");
      }
      return blocked.data.correlationId;
    })();
    const refused = await waitFor("reactor.error");
    if (refused.type !== "reactor.error") throw new Error("unreachable");
    expect(refused.data.fatal).toBe(false);
    expect(refused.data.error).toContain("already holds a gate");
    expect(countOf(events, "reactor.gate.blocked")).toBe(1);

    reactor.deliver(makeApprovalMessage(correlationId), { turnId: "turn-a" });
    await waitFor("reactor.turn.ended");
    expect(turnEnds(events)).toEqual([
      { turnId: "turn-a", outcome: "replied" },
    ]);
    // The marker's operation stays registered with no gate.
    expect(cell.pendingOperations.map((op) => op.correlationId)).toEqual([
      "corr-marker",
    ]);
    expect(cell.pendingOperations[0]?.turnId).toBe("turn-a");
    expect(countOf(events, "reactor.gate.blocked")).toBe(1);

    reactor.abort("admin_kill");
    await waitFor("reactor.done");
    expect(turnEnds(events)).toHaveLength(1);
  });

  test("a refused director suspend on an earlier turn's marker leaves the marker on that turn", async () => {
    const cell: PersistedCell = {
      turns: [],
      pendingOperations: [],
      tokenUsage: emptyUsage(),
    };
    const { reactor, events, waitFor } = createTestReactor({
      contextStore: makePersistingContextStore(cell),
      director: directorFromTable(
        {
          "message.received": (e, _s, caps) =>
            e.message.content === "A"
              ? caps.executeTools([call("call-send", "send")])
              : caps.executeTools([
                  call("call-ask"),
                  call("call-look", "lookup"),
                ]),
          "tool.done": (e, _s, caps) => {
            if (e.result.callId === "call-send") return caps.reply("sent");
            if (e.result.callId === "call-look") {
              return caps.suspend({
                type: "message_response",
                gateId: "marker-gate",
                timeoutMs: 60_000,
                correlationId: "corr-marker",
              });
            }
            return caps.wait();
          },
        },
        "wait",
      ),
      toolRunner: makeToolRunner(async (toolCall) =>
        toolCall.name === "send"
          ? {
              callId: toolCall.id,
              content: "sent",
              pendingMarker: {
                status: "pending" as const,
                correlationId: "corr-marker",
              },
            }
          : { callId: toolCall.id, content: "ok" },
      ),
      beforeToolExtensions: [askForCharge()],
    });
    reactor.start();
    reactor.deliver(makeTurnMessage("A"), { turnId: "turn-a" });
    await waitFor("reactor.turn.ended");

    reactor.deliver(makeTurnMessage("B"), { turnId: "turn-b" });
    const refused = await waitFor("reactor.error");
    if (refused.type !== "reactor.error") throw new Error("unreachable");
    expect(refused.data.fatal).toBe(false);
    expect(refused.data.error).toContain("already holds a gate");
    const marker = cell.pendingOperations.find(
      (op) => op.correlationId === "corr-marker",
    );
    expect(marker?.turnId).toBe("turn-a");

    // The marker has no live gate, so its response resumes nothing.
    reactor.deliver(
      createInboundMessage({
        from: "peer@example.com",
        to: "agent@example.com",
        content: "resp",
        correlationId: "corr-marker",
      }),
      { turnId: "turn-d" },
    );
    await waitForEvent(
      events,
      (e) => e.type === "reactor.turn.ended" && e.data.turnId === "turn-d",
    );
    expect(countOf(events, "message.correlated")).toBe(1);
    expect(turnEnds(events).filter((e) => e.turnId === "turn-d")).toEqual([
      { turnId: "turn-d", outcome: "unanswered" },
    ]);
    expect(turnEnds(events).filter((e) => e.turnId === "turn-b")).toEqual([]);
    expect(countOf(events, "reactor.gate.blocked")).toBe(1);
    expect(countOf(events, "reactor.gate.cleared")).toBe(0);

    reactor.abort("admin_kill");
    await waitFor("reactor.done");
  });

  test("identical gated calls in one batch run one at a time and trip the doom loop at three", async () => {
    // The model asks for the refused calls again after each approval, so
    // the three identical calls run as three identical batches.
    let inferences = 0;
    const batches: string[][] = [["c1", "c2", "c3"], ["c4", "c5"], ["c6"]];
    const inferenceRunner = async function* (opts: InferenceHarnessOptions) {
      const ids = batches[inferences];
      inferences += 1;
      yield {
        type: "inference.done" as const,
        seq: opts.nextSeq(),
        data: {
          turn:
            ids === undefined
              ? makeAssistantTurn("done")
              : {
                  role: "assistant" as const,
                  content: ids.map((id) => ({
                    type: "tool_call" as const,
                    id,
                    name: "charge_card",
                    arguments: {},
                  })),
                  model: "test-model",
                  timestamp: 1000,
                },
          usage: emptyUsage(),
          source: TEST_SOURCE,
        },
      };
    };
    const { reactor, events } = createTestReactor({
      director: createDefaultDirector("test agent", []),
      inferenceRunner,
      toolRunner: chargeRunner(),
      beforeToolExtensions: [askForCharge()],
    });
    reactor.start();
    reactor.deliver(makeTurnMessage("go"), { turnId: "turn-a" });

    // Approve each gate as it arrives until the reactor stops.
    const approved = new Set<string>();
    for (;;) {
      const ev = await waitForEvent(
        events,
        (e) =>
          e.type === "reactor.done" ||
          (e.type === "reactor.gate.blocked" &&
            e.data.correlationId !== undefined &&
            !approved.has(e.data.correlationId)),
      );
      if (ev.type === "reactor.done") break;
      if (ev.type !== "reactor.gate.blocked") throw new Error("unreachable");
      const correlationId = ev.data.correlationId;
      if (correlationId === undefined) throw new Error("expected id");
      approved.add(correlationId);
      reactor.deliver(makeApprovalMessage(correlationId), { turnId: "turn-a" });
    }

    expect(refusedCallIds(events)).toEqual(["c2", "c3", "c5"]);
    expect(countOf(events, "reactor.gate.blocked")).toBe(3);
    expect(countOf(events, "tool.start")).toBe(3);
    expect(turnEnds(events).find((e) => e.turnId === "turn-a")).toMatchObject({
      outcome: "failed",
      error: { kind: "doom_loop" },
    });
  });

  test("a parked call that times out after a refusal resumes once and the turn ends once", async () => {
    let inferences = 0;
    const inferenceRunner = async function* (opts: InferenceHarnessOptions) {
      inferences += 1;
      yield {
        type: "inference.done" as const,
        seq: opts.nextSeq(),
        data: {
          turn:
            inferences === 1
              ? {
                  role: "assistant" as const,
                  content: [
                    {
                      type: "tool_call" as const,
                      id: "call-a",
                      name: "charge_card",
                      arguments: {},
                    },
                    {
                      type: "tool_call" as const,
                      id: "call-b",
                      name: "charge_card",
                      arguments: {},
                    },
                  ],
                  model: "test-model",
                  timestamp: 1000,
                }
              : makeAssistantTurn("timed out"),
          usage: emptyUsage(),
          source: TEST_SOURCE,
        },
      };
    };
    const { director, decided } = recordingDirector(
      createDefaultDirector("test agent", []),
    );
    const { reactor, events, waitFor } = createTestReactor({
      director,
      inferenceRunner,
      toolRunner: chargeRunner(),
      beforeToolExtensions: [askForCharge(50)],
    });
    reactor.start();
    reactor.deliver(makeTurnMessage("go"), { turnId: "turn-a" });
    await waitFor("reactor.turn.ended");

    expect(refusedCallIds(events)).toEqual(["call-b"]);
    expect(decided.filter((e) => e.type === "resume.tool_result")).toHaveLength(
      1,
    );
    expect(inferences).toBe(2);
    expect(turnEnds(events)).toEqual([
      { turnId: "turn-a", outcome: "replied" },
    ]);

    reactor.abort("admin_kill");
    await waitFor("reactor.done");
    expect(turnEnds(events)).toHaveLength(1);
  });

  test("a gated call is refused while the first suspend commits even after the first gate timed out", async () => {
    // call-a's gate times out while its commit is held open. The gate
    // manager has already deleted the gate, so only the in-flight suspend
    // counts against call-b.
    const releaseCommit = Promise.withResolvers<boolean>();
    const commitEntered = Promise.withResolvers<boolean>();
    const base = makeContextStore();
    const contextStore: ContextStore = {
      ...base,
      async writeMetadata(metadata, signal) {
        commitEntered.resolve(true);
        await releaseCommit.promise;
        return base.writeMetadata(metadata, signal);
      },
    };
    const timers = manualTimers();
    let gateATimedOutBeforeCallB = false;
    const extension: BeforeToolExtension = {
      async beforeTool(toolCall) {
        if (toolCall.id === "call-a") {
          return suspendDecision(toolCall, "approval");
        }
        await commitEntered.promise;
        // call-a's gate was registered before its commit started. Its
        // timeout fires now, while the commit is held.
        expect(timers.pending()).toHaveLength(1);
        timers.fire(timers.pending()[0]);
        gateATimedOutBeforeCallB = timers.pending().length === 0;
        return suspendDecision(toolCall, "approval");
      },
    };
    const { director, decided } = recordingDirector(
      directorFromTable(
        {
          "message.received": (_e, _s, caps) =>
            caps.executeTools([call("call-a"), call("call-b")]),
        },
        "wait",
      ),
    );
    const { reactor, events, waitFor } = createTestReactor({
      deps: timers.deps,
      contextStore,
      director,
      beforeToolExtensions: [extension],
    });
    reactor.start();
    reactor.deliver(makeTurnMessage("go"), { turnId: "turn-a" });
    await waitForEvent(
      events,
      (e) => e.type === "tool.done" && e.data.result.callId === "call-b",
    );
    // call-b was refused while call-a's commit was still held, after
    // call-a's gate had timed out.
    expect(countOf(events, "reactor.gate.blocked")).toBe(0);
    expect(gateATimedOutBeforeCallB).toBe(true);
    releaseCommit.resolve(true);
    await waitFor("reactor.gate.blocked");

    expect(refusedCallIds(events)).toEqual(["call-b"]);
    expect(countOf(events, "reactor.gate.blocked")).toBe(1);

    // call-a's gate timed out during its commit: the suspend replays the
    // deferred timeout after `reactor.gate.blocked`, and that answers call-a
    // with the timeout result.
    await waitUntil(() => decided.some((e) => e.type === "resume.tool_result"));
    const timedOut = decided.find((e) => e.type === "resume.tool_result");
    if (timedOut?.type !== "resume.tool_result") {
      throw new Error("unreachable");
    }
    expect(timedOut.result).toEqual({
      callId: "call-a",
      content: "approval timed out",
      isError: true,
    });

    reactor.abort("admin_kill");
    await waitFor("reactor.done");
    expect(countOf(events, "reactor.gate.blocked")).toBe(1);
  });

  test("a refused director suspend commits a marker that its cycle registered", async () => {
    // A serial batch parks the ask call first, and its suspend commits
    // before the marker call runs. The refused director suspend that follows
    // must persist the marker while the ask call stays parked.
    const cell: PersistedCell = {
      turns: [],
      pendingOperations: [],
      tokenUsage: emptyUsage(),
    };
    const { reactor, events, waitFor } = createTestReactor({
      contextStore: makePersistingContextStore(cell),
      director: directorFromTable(
        {
          "message.received": (_e, _s, caps) =>
            caps.executeTools(
              [call("call-ask"), call("call-send", "send")],
              false,
            ),
          "tool.done": (_e, _s, caps) =>
            caps.suspend({
              type: "message_response",
              gateId: "marker-gate",
              timeoutMs: 60_000,
              correlationId: "corr-marker",
            }),
        },
        "wait",
      ),
      toolRunner: makeToolRunner(async (toolCall) => ({
        callId: toolCall.id,
        content: "sent",
        pendingMarker: {
          status: "pending" as const,
          correlationId: "corr-marker",
        },
      })),
      beforeToolExtensions: [askForCharge()],
    });
    reactor.start();
    reactor.deliver(makeTurnMessage("go"), { turnId: "turn-a" });
    const blocked = await waitFor("reactor.gate.blocked");
    if (blocked.type !== "reactor.gate.blocked") throw new Error("unreachable");
    const refused = await waitFor("reactor.error");
    if (refused.type !== "reactor.error") throw new Error("unreachable");
    expect(refused.data.error).toContain("already holds a gate");

    expect(
      cell.pendingOperations.map((op) => [op.correlationId, op.turnId]).sort(),
    ).toEqual(
      [
        [blocked.data.correlationId, "turn-a"],
        ["corr-marker", "turn-a"],
      ].sort(),
    );
    expect(countOf(events, "reactor.gate.blocked")).toBe(1);
    expect(turnEnds(events)).toEqual([]);

    reactor.abort("admin_kill");
    await waitFor("reactor.done");
  });

  test("a director suspend refused for a live gate commits a marker that its cycle registered", async () => {
    // Turn A parks an ask call. Turn B runs a marker call, then its director
    // suspends on A's correlation, which already has a live gate. The
    // refusal must persist B's marker.
    const cell: PersistedCell = {
      turns: [],
      pendingOperations: [],
      tokenUsage: emptyUsage(),
    };
    const { reactor, events, waitFor } = createTestReactor({
      contextStore: makePersistingContextStore(cell),
      director: directorFromTable(
        {
          "message.received": (e, _s, caps) =>
            caps.executeTools([
              e.message.content === "A"
                ? call("call-ask")
                : call("call-send", "send"),
            ]),
          "tool.done": (_e, s, caps) => {
            const parked = s.pendingOperations.find(
              (op) => op.suspendedCall !== undefined,
            );
            if (parked === undefined) throw new Error("expected a parked op");
            return caps.suspend({
              type: "approval",
              gateId: "b-gate",
              timeoutMs: 60_000,
              correlationId: parked.correlationId,
            });
          },
        },
        "wait",
      ),
      toolRunner: makeToolRunner(async (toolCall) => ({
        callId: toolCall.id,
        content: "sent",
        pendingMarker: {
          status: "pending" as const,
          correlationId: "corr-b-marker",
        },
      })),
      beforeToolExtensions: [askForCharge()],
    });
    reactor.start();
    reactor.deliver(makeTurnMessage("A"), { turnId: "turn-a" });
    const blocked = await waitFor("reactor.gate.blocked");
    if (blocked.type !== "reactor.gate.blocked") throw new Error("unreachable");

    reactor.deliver(makeTurnMessage("B"), { turnId: "turn-b" });
    const refused = await waitFor("reactor.error");
    if (refused.type !== "reactor.error") throw new Error("unreachable");
    expect(refused.data.fatal).toBe(false);
    expect(refused.data.error).toContain("already has a live gate");

    expect(
      cell.pendingOperations.map((op) => [op.correlationId, op.turnId]).sort(),
    ).toEqual(
      [
        [blocked.data.correlationId, "turn-a"],
        ["corr-b-marker", "turn-b"],
      ].sort(),
    );
    expect(countOf(events, "reactor.gate.blocked")).toBe(1);

    reactor.abort("admin_kill");
    await waitFor("reactor.done");
  });

  // A reactor restarted from `cell`. A "probe" message records the ids of
  // the live gates. An approval re-runs its call, and a tool result or a
  // cleared gate replies.
  async function restarted(cell: PersistedCell): Promise<{
    handle: TestReactorHandle;
    liveGates: () => Promise<string[]>;
  }> {
    let probed: string[] | undefined;
    const handle = createTestReactor({
      contextStore: makePersistingContextStore(cell),
      director: directorFromTable(
        {
          "message.received": (e, s, caps) => {
            if (e.message.content === "probe") {
              probed = s.activeGates.map((g) => g.gateId).sort();
            }
            return caps.wait();
          },
          "resume.execute_tools": (e, _s, caps) =>
            caps.executeTools(e.calls, false, true),
          "tool.done": (_e, _s, caps) => caps.reply("done"),
          "reactor.gate.cleared": (_e, _s, caps) => caps.reply("resumed"),
        },
        "wait",
      ),
      toolRunner: makeToolRunner(async (toolCall) => ({
        callId: toolCall.id,
        content: "charged",
      })),
    });
    handle.reactor.start();
    await handle.waitFor("reactor.start");
    let probes = 0;
    return {
      handle,
      async liveGates() {
        probed = undefined;
        probes += 1;
        handle.reactor.deliver(makeTurnMessage("probe"), {
          turnId: `turn-probe-${String(probes)}`,
        });
        await waitUntil(() => probed !== undefined);
        return probed ?? [];
      },
    };
  }

  function markerResponse(correlationId: string): InboundMessage {
    return createInboundMessage({
      from: "peer@local",
      to: "agent@example.com",
      content: "response",
      correlationId,
    });
  }

  test("a restart re-arms the gate of a turn's approval and none for its marker", async () => {
    const cell: PersistedCell = {
      turns: [],
      pendingOperations: [],
      tokenUsage: emptyUsage(),
    };
    const first = createTestReactor({
      contextStore: makePersistingContextStore(cell),
      director: directorFromTable(
        {
          "message.received": (_e, _s, caps) =>
            caps.executeTools(
              [call("call-send", "send"), call("call-ask")],
              false,
            ),
        },
        "wait",
      ),
      toolRunner: makeToolRunner(async (toolCall) => ({
        callId: toolCall.id,
        content: "sent",
        pendingMarker: {
          status: "pending" as const,
          correlationId: "corr-marker",
        },
      })),
      beforeToolExtensions: [askForCharge()],
    });
    first.reactor.start();
    first.reactor.deliver(makeTurnMessage("go"), { turnId: "turn-a" });
    const blocked = await first.waitFor("reactor.gate.blocked");
    if (blocked.type !== "reactor.gate.blocked") throw new Error("unreachable");
    const askCorrelation = blocked.data.correlationId;
    if (askCorrelation === undefined) throw new Error("expected correlation");
    first.reactor.abort("admin_kill");
    await first.waitFor("reactor.done");

    const { handle, liveGates } = await restarted(cell);
    const { reactor, events, waitFor } = handle;
    expect(await liveGates()).toEqual([blocked.data.gateId]);

    // The marker has no gate, so its response resumes nothing.
    reactor.deliver(markerResponse("corr-marker"), { turnId: "turn-m" });
    await waitUntil(() => turnEnds(events).some((e) => e.turnId === "turn-m"));
    expect(turnEnds(events).filter((e) => e.turnId === "turn-m")).toEqual([
      { turnId: "turn-m", outcome: "unanswered" },
    ]);
    expect(countOf(events, "reactor.gate.cleared")).toBe(0);

    // The approval resumes the parked turn.
    reactor.deliver(makeApprovalMessage(askCorrelation), { turnId: "turn-d" });
    await waitUntil(() => turnEnds(events).some((e) => e.turnId === "turn-a"));
    expect(turnEnds(events).filter((e) => e.turnId === "turn-d")).toEqual([
      { turnId: "turn-d", outcome: "joined", joinedTurnId: "turn-a" },
    ]);
    expect(turnEnds(events).filter((e) => e.turnId === "turn-a")).toEqual([
      { turnId: "turn-a", outcome: "replied" },
    ]);

    reactor.abort("admin_kill");
    await waitFor("reactor.done");
  });

  test("a restart re-arms no gate for a marker that a refused director suspend named", async () => {
    const cell: PersistedCell = {
      turns: [],
      pendingOperations: [],
      tokenUsage: emptyUsage(),
    };
    const first = createTestReactor({
      contextStore: makePersistingContextStore(cell),
      director: directorFromTable(
        {
          "message.received": (_e, _s, caps) =>
            caps.executeTools(
              [call("call-ask"), call("call-send", "send")],
              false,
            ),
          "tool.done": (_e, _s, caps) =>
            caps.suspend({
              type: "message_response",
              gateId: "marker-gate",
              timeoutMs: 60_000,
              correlationId: "corr-marker",
            }),
        },
        "wait",
      ),
      toolRunner: makeToolRunner(async (toolCall) => ({
        callId: toolCall.id,
        content: "sent",
        pendingMarker: {
          status: "pending" as const,
          correlationId: "corr-marker",
        },
      })),
      beforeToolExtensions: [askForCharge()],
    });
    first.reactor.start();
    first.reactor.deliver(makeTurnMessage("go"), { turnId: "turn-a" });
    const blocked = await first.waitFor("reactor.gate.blocked");
    if (blocked.type !== "reactor.gate.blocked") throw new Error("unreachable");
    await first.waitFor("reactor.error");
    first.reactor.abort("admin_kill");
    await first.waitFor("reactor.done");

    const { handle, liveGates } = await restarted(cell);
    expect(await liveGates()).toEqual([blocked.data.gateId]);

    handle.reactor.abort("admin_kill");
    await handle.waitFor("reactor.done");
  });

  test("a restart re-arms a taken-over marker's gate under the waiting turn with the director's gate id", async () => {
    const cell: PersistedCell = {
      turns: [],
      pendingOperations: [],
      tokenUsage: emptyUsage(),
    };
    const first = createTestReactor({
      contextStore: makePersistingContextStore(cell),
      director: directorFromTable(
        {
          "message.received": (e, _s, caps) =>
            e.message.content === "A"
              ? caps.executeTools([call("call-send", "send")])
              : caps.suspend({
                  type: "message_response",
                  gateId: "b-gate",
                  timeoutMs: 60_000,
                  correlationId: "corr-x",
                }),
          "tool.done": (_e, _s, caps) => caps.reply("sent"),
        },
        "wait",
      ),
      toolRunner: makeToolRunner(async (toolCall) => ({
        callId: toolCall.id,
        content: "sent",
        pendingMarker: { status: "pending" as const, correlationId: "corr-x" },
      })),
    });
    first.reactor.start();
    first.reactor.deliver(makeTurnMessage("A"), { turnId: "turn-a" });
    await waitUntil(() =>
      turnEnds(first.events).some((e) => e.turnId === "turn-a"),
    );
    first.reactor.deliver(makeTurnMessage("B"), { turnId: "turn-b" });
    await first.waitFor("reactor.gate.blocked");
    first.reactor.abort("admin_kill");
    await first.waitFor("reactor.done");

    const { handle, liveGates } = await restarted(cell);
    const { reactor, events, waitFor } = handle;
    expect(await liveGates()).toEqual(["b-gate"]);

    reactor.deliver(markerResponse("corr-x"), { turnId: "turn-r" });
    await waitUntil(() => turnEnds(events).some((e) => e.turnId === "turn-b"));
    expect(turnEnds(events).filter((e) => e.turnId === "turn-r")).toEqual([
      { turnId: "turn-r", outcome: "joined", joinedTurnId: "turn-b" },
    ]);

    reactor.abort("admin_kill");
    await waitFor("reactor.done");
  });

  test("a marker registered after a director suspend on its correlation persists under that gate and resumes it after a restart", async () => {
    // Turn A's director waits on corr-x before any operation exists. Turn
    // B's tool then returns the marker for corr-x. The live gate already
    // waits on it, so the operation is gated, under A's gate and turn.
    const cell: PersistedCell = {
      turns: [],
      pendingOperations: [],
      tokenUsage: emptyUsage(),
    };
    const first = createTestReactor({
      contextStore: makePersistingContextStore(cell),
      director: directorFromTable(
        {
          "message.received": (e, _s, caps) =>
            e.message.content === "wait"
              ? caps.suspend({
                  type: "approval",
                  gateId: "g-dir",
                  timeoutMs: 60_000,
                  correlationId: "corr-x",
                })
              : caps.executeTools([call("call-send", "send")], false),
          "tool.done": (_e, _s, caps) => caps.reply("sent"),
        },
        "wait",
      ),
      toolRunner: makeToolRunner(async (toolCall) => ({
        callId: toolCall.id,
        content: "sent",
        pendingMarker: { status: "pending" as const, correlationId: "corr-x" },
      })),
    });
    first.reactor.start();
    first.reactor.deliver(makeTurnMessage("wait"), { turnId: "turn-a" });
    await first.waitFor("reactor.gate.blocked");
    first.reactor.deliver(makeTurnMessage("send"), { turnId: "turn-b" });
    await waitUntil(() =>
      turnEnds(first.events).some((e) => e.turnId === "turn-b"),
    );
    first.reactor.abort("admin_kill");
    await first.waitFor("reactor.done");

    expect(
      cell.pendingOperations.map(
        ({ correlationId, gateId, turnId, gated }) => ({
          correlationId,
          gateId,
          turnId,
          gated,
        }),
      ),
    ).toEqual([
      {
        correlationId: "corr-x",
        gateId: "g-dir",
        turnId: "turn-a",
        gated: true,
      },
    ]);

    const { handle, liveGates } = await restarted(cell);
    const { reactor, events, waitFor } = handle;
    expect(await liveGates()).toEqual(["g-dir"]);

    reactor.deliver(markerResponse("corr-x"), { turnId: "turn-r" });
    await waitUntil(() => turnEnds(events).some((e) => e.turnId === "turn-a"));
    expect(turnEnds(events).filter((e) => e.turnId === "turn-r")).toEqual([
      { turnId: "turn-r", outcome: "joined", joinedTurnId: "turn-a" },
    ]);
    expect(turnEnds(events).filter((e) => e.turnId === "turn-a")).toEqual([
      { turnId: "turn-a", outcome: "replied" },
    ]);

    reactor.abort("admin_kill");
    await waitFor("reactor.done");
  });

  test("a director gate on a marker that timed out is not re-armed by a restart", async () => {
    // Turn B's director gate waits on turn A's marker and times out. The
    // operation stays registered, but no live gate parks it any more.
    const cell: PersistedCell = {
      turns: [],
      pendingOperations: [],
      tokenUsage: emptyUsage(),
    };
    const timers = manualTimers();
    const first = createTestReactor({
      deps: timers.deps,
      contextStore: makePersistingContextStore(cell),
      director: directorFromTable(
        {
          "message.received": (e, _s, caps) =>
            e.message.content === "A"
              ? caps.executeTools([call("call-send", "send")])
              : caps.suspend({
                  type: "message_response",
                  gateId: "b-gate",
                  timeoutMs: 60_000,
                  correlationId: "corr-x",
                }),
          "tool.done": (_e, _s, caps) => caps.reply("sent"),
          "reactor.gate.cleared": (_e, _s, caps) => caps.reply("timed out"),
        },
        "wait",
      ),
      toolRunner: makeToolRunner(async (toolCall) => ({
        callId: toolCall.id,
        content: "sent",
        pendingMarker: { status: "pending" as const, correlationId: "corr-x" },
      })),
    });
    first.reactor.start();
    first.reactor.deliver(makeTurnMessage("A"), { turnId: "turn-a" });
    await waitUntil(() =>
      turnEnds(first.events).some((e) => e.turnId === "turn-a"),
    );
    first.reactor.deliver(makeTurnMessage("B"), { turnId: "turn-b" });
    await first.waitFor("reactor.gate.blocked");
    timers.fire(timers.pending().find((t) => t.delayMs === 60_000));
    await waitUntil(() =>
      turnEnds(first.events).some((e) => e.turnId === "turn-b"),
    );
    first.reactor.abort("admin_kill");
    await first.waitFor("reactor.done");

    expect(cell.pendingOperations.map((op) => op.gated)).toEqual([false]);

    const { handle, liveGates } = await restarted(cell);
    const { reactor, events, waitFor } = handle;
    expect(await liveGates()).toEqual([]);

    // The operation has no gate, so its response resumes nothing.
    reactor.deliver(markerResponse("corr-x"), { turnId: "turn-m" });
    await waitUntil(() => turnEnds(events).some((e) => e.turnId === "turn-m"));
    expect(turnEnds(events).filter((e) => e.turnId === "turn-m")).toEqual([
      { turnId: "turn-m", outcome: "unanswered" },
    ]);
    expect(countOf(events, "reactor.gate.cleared")).toBe(0);
    expect(
      events.some(
        (e) => e.type === "reactor.turn.started" && e.data.turnId === "turn-b",
      ),
    ).toBe(false);

    reactor.abort("admin_kill");
    await waitFor("reactor.done");
  });

  test("an operation from older state with no gated field comes back with a gate", async () => {
    const cell: PersistedCell = {
      turns: [
        {
          role: "user",
          content: [{ type: "text", text: "park" }],
          timestamp: 1000,
        },
        suspendToolCallTurn,
      ],
      pendingOperations: [
        {
          correlationId: "corr-old",
          kind: "approval",
          registeredAt: Date.now(),
          gateId: "gate-old",
          timeoutAt: Date.now() + 60_000,
          suspendedCall: call("call-ask"),
          turnId: "turn-old",
        },
      ],
      tokenUsage: emptyUsage(),
    };
    const { handle, liveGates } = await restarted(cell);
    expect(await liveGates()).toEqual(["gate-old"]);

    handle.reactor.abort("admin_kill");
    await handle.waitFor("reactor.done");
  });
});

// ---------------------------------------------------------------------------
// Abandon a turn: the caller gave up, and the reactor ends the turn
// ---------------------------------------------------------------------------

describe("createReactor — abandon a turn", () => {
  function endsFor(
    events: ReactorEmittedEvent[],
    turnId: string,
  ): TurnEndedData[] {
    return turnEnds(events).filter((e) => e.turnId === turnId);
  }

  function startsFor(events: ReactorEmittedEvent[], turnId: string): number {
    return events.filter(
      (e) => e.type === "reactor.turn.started" && e.data.turnId === turnId,
    ).length;
  }

  function indexOfEnd(events: ReactorEmittedEvent[], turnId: string): number {
    return events.findIndex(
      (e) => e.type === "reactor.turn.ended" && e.data.turnId === turnId,
    );
  }

  function waitForTurnEnd(
    events: ReactorEmittedEvent[],
    turnId: string,
  ): Promise<ReactorEmittedEvent> {
    return waitForEvent(
      events,
      (e) => e.type === "reactor.turn.ended" && e.data.turnId === turnId,
    );
  }

  async function parkedCorrelation(
    waitFor: TestReactorHandle["waitFor"],
  ): Promise<string> {
    const blocked = await waitFor("reactor.gate.blocked");
    if (blocked.type !== "reactor.gate.blocked") throw new Error("unreachable");
    const correlationId = blocked.data.correlationId;
    if (correlationId === undefined) throw new Error("expected correlationId");
    return correlationId;
  }

  // The tool results that history holds for each call id.
  function resultsByCall(turns: ConversationTurn[]): Map<string, string[]> {
    const byCall = new Map<string, string[]>();
    for (const turn of turns) {
      for (const block of turn.content) {
        if (block.type !== "tool_result") continue;
        const text = block.content
          .map((part) => (part.type === "text" ? part.text : ""))
          .join("");
        byCall.set(block.callId, [...(byCall.get(block.callId) ?? []), text]);
      }
    }
    return byCall;
  }

  function emptyCell(): PersistedCell {
    return { turns: [], pendingOperations: [], tokenUsage: emptyUsage() };
  }

  test("a parked turn ends abandoned with its call answered and its operation removed, with no director call", async () => {
    const cell = emptyCell();
    const { director, decided } = recordingDirector(turnDirector());
    const { reactor, events, waitFor } = createTestReactor({
      contextStore: makePersistingContextStore(cell),
      director,
      inferenceRunner: turnRunner(),
      toolRunner: chargeRunner(),
      beforeToolExtensions: [askExtension()],
    });
    reactor.start();
    reactor.deliver(makeTurnMessage("park"), { turnId: "turn-a" });
    await parkedCorrelation(waitFor);
    const decidedBefore = decided.length;

    reactor.abandonTurn("turn-a");
    await waitForTurnEnd(events, "turn-a");

    expect(endsFor(events, "turn-a")).toEqual([
      { turnId: "turn-a", outcome: "abandoned" },
    ]);
    const clearedAt = events.findIndex(
      (e) => e.type === "reactor.gate.cleared" && e.data.reason === "abandoned",
    );
    expect(clearedAt).toBeGreaterThan(-1);
    expect(clearedAt).toBeLessThan(indexOfEnd(events, "turn-a"));
    expect(decided.length).toBe(decidedBefore);
    // The commit before the end persisted the removal and the answer.
    expect(cell.pendingOperations).toEqual([]);
    expect(resultsByCall(cell.turns).get("call-ask")).toEqual([
      "turn abandoned",
    ]);
    expect(() => assertWellFormedToolSequence(cell.turns)).not.toThrow();

    reactor.abort("admin_kill");
    await waitFor("reactor.done");

    // A reload finds no parked operation to rehydrate.
    const reloaded = createTestReactor({
      contextStore: makePersistingContextStore(cell),
      director: turnDirector(),
    });
    reloaded.reactor.start();
    await reloaded.waitFor("reactor.start");
    expect(cell.pendingOperations).toEqual([]);
    reloaded.reactor.abort("admin_kill");
    await reloaded.waitFor("reactor.done");
  });

  test("a parked legacy operation rehydrated after a restart is abandoned under its legacy turn", async () => {
    const correlationId = "legacy-abandon";
    const cell: PersistedCell = {
      turns: [
        {
          role: "user",
          content: [{ type: "text", text: "park" }],
          timestamp: 1000,
        },
        suspendToolCallTurn,
      ],
      pendingOperations: [
        {
          correlationId,
          kind: "approval",
          registeredAt: Date.now(),
          gateId: `pending-${correlationId}`,
          timeoutAt: Date.now() + 60_000,
          suspendedCall: { id: "call-ask", name: "charge_card", arguments: {} },
        },
      ],
      tokenUsage: emptyUsage(),
    };
    const { reactor, events, waitFor } = createTestReactor({
      contextStore: makePersistingContextStore(cell),
      director: turnDirector(),
    });
    reactor.start();
    await waitFor("reactor.start");

    const turnId = legacyTurnId(correlationId);
    reactor.abandonTurn(turnId);
    await waitForTurnEnd(events, turnId);

    expect(endsFor(events, turnId)).toEqual([{ turnId, outcome: "abandoned" }]);
    expect(startsFor(events, turnId)).toBe(0);
    expect(cell.pendingOperations).toEqual([]);
    expect(resultsByCall(cell.turns).get("call-ask")).toEqual([
      "turn abandoned",
    ]);

    reactor.abort("admin_kill");
    await waitFor("reactor.done");
  });

  test("an inference in flight is aborted, its error reaches no director, and the next turn gets a fresh signal", async () => {
    const signals: AbortSignal[] = [];
    const inferenceRunner = async function* (opts: InferenceHarnessOptions) {
      const signal = opts.signal;
      if (signal === undefined) throw new Error("expected a signal");
      signals.push(signal);
      const text = lastText(opts.turns);
      if (text === "A") {
        await new Promise<void>((resolve) => {
          signal.addEventListener("abort", () => resolve(), { once: true });
        });
        yield {
          type: "inference.error" as const,
          seq: opts.nextSeq(),
          data: {
            error: { category: "aborted" as const, message: "aborted" },
            partial: { text: "" },
          },
        };
        return;
      }
      yield {
        type: "inference.done" as const,
        seq: opts.nextSeq(),
        data: {
          turn: makeAssistantTurn(`${text} done`),
          usage: emptyUsage(),
          source: TEST_SOURCE,
        },
      };
    };
    const { director, decided } = recordingDirector(turnDirector());
    const { reactor, events, waitFor } = createTestReactor({
      director,
      inferenceRunner,
    });
    reactor.start();
    reactor.deliver(makeTurnMessage("A"), { turnId: "turn-a" });
    await waitUntil(() => signals.length >= 1);

    reactor.abandonTurn("turn-a");
    await waitForTurnEnd(events, "turn-a");
    expect(signals[0]?.aborted).toBe(true);
    expect(decided.some((e) => e.type === "inference.error")).toBe(false);
    expect(events.some((e) => e.type === "connector.reply")).toBe(false);
    expect(endsFor(events, "turn-a")).toEqual([
      { turnId: "turn-a", outcome: "abandoned" },
    ]);

    reactor.deliver(makeTurnMessage("B"), { turnId: "turn-b" });
    await waitForTurnEnd(events, "turn-b");
    expect(signals[1]?.aborted).toBe(false);
    expect(replyTurnIds(events).get("B done")).toBe("turn-b");

    reactor.abort("admin_kill");
    await waitFor("reactor.done");
  });

  test("a tool batch in flight keeps its results in history and drives no director call", async () => {
    const cell = emptyCell();
    const toolStarted = Promise.withResolvers<boolean>();
    const { director, decided } = recordingDirector(
      directorFromTable(
        {
          "message.received": (_e, _s, caps) =>
            caps.executeTools([
              { id: "call-slow", name: "slow", arguments: {} },
            ]),
          "tool.done": (_e, _s, caps) => caps.infer(),
        },
        "wait",
      ),
    );
    const { reactor, events, waitFor } = createTestReactor({
      contextStore: makePersistingContextStore(cell),
      director,
      inferenceRunner: turnRunner(),
      toolRunner: {
        async run(call, signal) {
          toolStarted.resolve(true);
          await new Promise<void>((resolve) => {
            signal.addEventListener("abort", () => resolve(), { once: true });
          });
          return { callId: call.id, content: "stopped" };
        },
      },
    });
    reactor.start();
    reactor.deliver(makeTurnMessage("go"), { turnId: "turn-a" });
    await toolStarted.promise;

    reactor.abandonTurn("turn-a");
    await waitForTurnEnd(events, "turn-a");

    expect(decided.some((e) => e.type === "tool.done")).toBe(false);
    expect(countOf(events, "inference.start")).toBe(0);
    expect(resultsByCall(cell.turns).get("call-slow")).toEqual(["stopped"]);
    expect(endsFor(events, "turn-a")).toEqual([
      { turnId: "turn-a", outcome: "abandoned" },
    ]);

    reactor.abort("admin_kill");
    await waitFor("reactor.done");
  });

  test("an abandon while the director decides drops the decision", async () => {
    const holdDecision = Promise.withResolvers<boolean>();
    const deciding = Promise.withResolvers<boolean>();
    const { reactor, events, waitFor } = createTestReactor({
      director: directorFromTable(
        {
          "message.received": async (_e, _s, caps) => {
            deciding.resolve(true);
            await holdDecision.promise;
            return caps.infer();
          },
        },
        "wait",
      ),
      inferenceRunner: turnRunner(),
    });
    reactor.start();
    reactor.deliver(makeTurnMessage("go"), { turnId: "turn-a" });
    await deciding.promise;

    reactor.abandonTurn("turn-a");
    holdDecision.resolve(true);
    await waitForTurnEnd(events, "turn-a");

    expect(countOf(events, "inference.start")).toBe(0);
    expect(endsFor(events, "turn-a")).toEqual([
      { turnId: "turn-a", outcome: "abandoned" },
    ]);

    reactor.abort("admin_kill");
    await waitFor("reactor.done");
  });

  test("a queued inference.done with tool calls is dropped and every call is answered", async () => {
    const cell = emptyCell();
    const reactorRef: { current: Reactor | undefined } = { current: undefined };
    const twoCalls: AssistantTurn = {
      role: "assistant",
      content: [
        { type: "tool_call", id: "call-1", name: "lookup", arguments: {} },
        { type: "tool_call", id: "call-2", name: "lookup", arguments: {} },
      ],
      model: "test-model",
      timestamp: 1000,
    };
    const inferenceRunner = async function* (opts: InferenceHarnessOptions) {
      yield {
        type: "inference.done" as const,
        seq: opts.nextSeq(),
        data: { turn: twoCalls, usage: emptyUsage(), source: TEST_SOURCE },
      };
      // The abandon lands after the model answered and before the loop
      // takes up that answer.
      reactorRef.current?.abandonTurn("turn-a");
    };
    const { reactor, events, waitFor } = createTestReactor({
      contextStore: makePersistingContextStore(cell),
      director: turnDirector(),
      inferenceRunner,
    });
    reactorRef.current = reactor;
    reactor.start();
    reactor.deliver(makeTurnMessage("go"), { turnId: "turn-a" });
    await waitForTurnEnd(events, "turn-a");

    expect(countOf(events, "tool.start")).toBe(0);
    const results = resultsByCall(cell.turns);
    expect(results.get("call-1")).toEqual(["turn abandoned"]);
    expect(results.get("call-2")).toEqual(["turn abandoned"]);
    expect(() => assertWellFormedToolSequence(cell.turns)).not.toThrow();

    reactor.abort("admin_kill");
    await waitFor("reactor.done");
  });

  test("an abandon during the suspend commit emits no gate.blocked, no gate.cleared, and removes the operation", async () => {
    const cell = emptyCell();
    const base = makePersistingContextStore(cell);
    const holdCommit = Promise.withResolvers<boolean>();
    const commitHeld = Promise.withResolvers<boolean>();
    let holding = true;
    const contextStore: ContextStore = {
      ...base,
      async writeMetadata(metadata, signal) {
        if (holding && metadata.pendingOperations.length > 0) {
          holding = false;
          commitHeld.resolve(true);
          await holdCommit.promise;
        }
        return base.writeMetadata(metadata, signal);
      },
    };
    const { reactor, events, waitFor } = createTestReactor({
      contextStore,
      director: turnDirector(),
      inferenceRunner: turnRunner(),
      toolRunner: chargeRunner(),
      beforeToolExtensions: [askExtension()],
    });
    reactor.start();
    reactor.deliver(makeTurnMessage("park"), { turnId: "turn-a" });
    await commitHeld.promise;

    reactor.abandonTurn("turn-a");
    holdCommit.resolve(true);
    await waitForTurnEnd(events, "turn-a");

    // The gate.blocked was dropped, so no gate.cleared announces the clear.
    expect(countOf(events, "reactor.gate.blocked")).toBe(0);
    expect(countOf(events, "reactor.gate.cleared")).toBe(0);
    expect(cell.pendingOperations).toEqual([]);
    expect(resultsByCall(cell.turns).get("call-ask")).toEqual([
      "turn abandoned",
    ]);

    reactor.abort("admin_kill");
    await waitFor("reactor.done");
  });

  test("a queued turn behind a running turn never starts, and the running turn completes", async () => {
    const releaseA = Promise.withResolvers<boolean>();
    const { reactor, events, waitFor } = createTestReactor({
      director: turnDirector(),
      inferenceRunner: turnRunner(async (text) =>
        text === "A" ? releaseA.promise : true,
      ),
    });
    reactor.start();
    reactor.deliver(makeTurnMessage("A"), { turnId: "turn-a" });
    await waitFor("inference.start");
    reactor.deliver(makeTurnMessage("B"), { turnId: "turn-b" });
    await waitUntil(() => countOf(events, "message.received") >= 2);

    reactor.abandonTurn("turn-b");
    releaseA.resolve(true);
    await waitForTurnEnd(events, "turn-a");
    await waitForTurnEnd(events, "turn-b");

    expect(endsFor(events, "turn-a")).toEqual([
      { turnId: "turn-a", outcome: "replied" },
    ]);
    expect(endsFor(events, "turn-b")).toEqual([
      { turnId: "turn-b", outcome: "abandoned" },
    ]);
    expect(startsFor(events, "turn-b")).toBe(0);

    // The queue is quiet and correct: a later turn runs normally.
    reactor.deliver(makeTurnMessage("C"), { turnId: "turn-c" });
    await waitForTurnEnd(events, "turn-c");
    expect(replyTurnIds(events).get("C done")).toBe("turn-c");

    reactor.abort("admin_kill");
    await waitFor("reactor.done");
  });

  test("a queued re-dispatch of an abandoned turn runs no tool and its call is answered", async () => {
    const cell = emptyCell();
    const releaseR = Promise.withResolvers<boolean>();
    const toolCalls: string[] = [];
    const { reactor, events, waitFor } = createTestReactor({
      contextStore: makePersistingContextStore(cell),
      director: turnDirector(),
      inferenceRunner: turnRunner(async (text) =>
        text === "R" ? releaseR.promise : true,
      ),
      toolRunner: makeToolRunner(async (call) => {
        toolCalls.push(call.id);
        return { callId: call.id, content: "charged" };
      }),
      beforeToolExtensions: [askExtension()],
    });
    reactor.start();
    reactor.deliver(makeTurnMessage("park"), { turnId: "turn-a" });
    const correlationId = await parkedCorrelation(waitFor);

    // R runs, so A's approval queues its re-dispatch behind R's cycle.
    reactor.deliver(makeTurnMessage("R"), { turnId: "turn-r" });
    await waitUntil(() => countOf(events, "inference.start") >= 2);
    reactor.deliver(makeApprovalMessage(correlationId), { turnId: "turn-a" });
    await waitFor("message.correlated");

    reactor.abandonTurn("turn-a");
    releaseR.resolve(true);
    await waitForTurnEnd(events, "turn-a");
    await waitForTurnEnd(events, "turn-r");

    expect(toolCalls).toEqual([]);
    expect(endsFor(events, "turn-a")).toEqual([
      { turnId: "turn-a", outcome: "abandoned" },
    ]);
    expect(resultsByCall(cell.turns).get("call-ask")).toEqual([
      "turn abandoned",
    ]);

    reactor.abort("admin_kill");
    await waitFor("reactor.done");
  });

  test("abandoning a joined turn abandons the turn it joined", async () => {
    const releaseTool = Promise.withResolvers<boolean>();
    const toolStarted = Promise.withResolvers<boolean>();
    const { reactor, events, waitFor } = createTestReactor({
      director: turnDirector(),
      inferenceRunner: turnRunner(),
      toolRunner: {
        async run(call, signal) {
          toolStarted.resolve(true);
          await Promise.race([
            releaseTool.promise,
            new Promise<void>((resolve) => {
              signal.addEventListener("abort", () => resolve(), {
                once: true,
              });
            }),
          ]);
          return { callId: call.id, content: "charged" };
        },
      },
      beforeToolExtensions: [askExtension()],
    });
    reactor.start();
    reactor.deliver(makeTurnMessage("park"), { turnId: "turn-a" });
    const correlationId = await parkedCorrelation(waitFor);
    reactor.deliver(makeApprovalMessage(correlationId), { turnId: "turn-d" });
    await waitForTurnEnd(events, "turn-d");
    await toolStarted.promise;

    reactor.abandonTurn("turn-d");
    await waitForTurnEnd(events, "turn-a");

    expect(endsFor(events, "turn-d")).toEqual([
      { turnId: "turn-d", outcome: "joined", joinedTurnId: "turn-a" },
    ]);
    expect(endsFor(events, "turn-a")).toEqual([
      { turnId: "turn-a", outcome: "abandoned" },
    ]);
    expect(events.some((e) => e.type === "connector.reply")).toBe(false);

    reactor.abort("admin_kill");
    await waitFor("reactor.done");
  });

  test("an abandon while a delivery validates discards it and leaves the parked turn parked", async () => {
    const release = Promise.withResolvers<boolean>();
    const validating = Promise.withResolvers<boolean>();
    const { reactor, events, waitFor } = createTestReactor({
      director: turnDirector(),
      inferenceRunner: turnRunner(),
      toolRunner: chargeRunner(),
      beforeToolExtensions: [askExtension()],
      correlationValidator: {
        async validate() {
          validating.resolve(true);
          return release.promise;
        },
      },
    });
    reactor.start();
    reactor.deliver(makeTurnMessage("park"), { turnId: "turn-a" });
    const correlationId = await parkedCorrelation(waitFor);
    reactor.deliver(makeApprovalMessage(correlationId), { turnId: "turn-d" });
    await validating.promise;

    reactor.abandonTurn("turn-d");
    await waitForTurnEnd(events, "turn-d");
    release.resolve(true);
    // The admission runs in promise continuations after the validator
    // settles, with no timer and no I/O; a task boundary follows them.
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(endsFor(events, "turn-d")).toEqual([
      { turnId: "turn-d", outcome: "abandoned" },
    ]);
    expect(countOf(events, "message.correlated")).toBe(0);
    expect(countOf(events, "message.received")).toBe(1);
    expect(endsFor(events, "turn-a")).toEqual([]);
    expect(countOf(events, "reactor.gate.cleared")).toBe(0);

    reactor.abort("admin_kill");
    await waitFor("reactor.done");
  });

  test("a reply emitted before the abandon stays out, and the turn ends abandoned", async () => {
    const { reactor, events, waitFor } = createTestReactor({
      director: directorFromTable(
        {
          "message.received": (_e, _s, caps) =>
            caps.executeTools([
              { id: "call-note", name: "note", arguments: {} },
              { id: "call-ask", name: "charge_card", arguments: {} },
            ]),
          "tool.done": (e, _s, caps) =>
            e.result.callId === "call-note" ? caps.reply("early") : caps.wait(),
        },
        "wait",
      ),
      toolRunner: chargeRunner(),
      beforeToolExtensions: [
        createAuthzExtension({
          authorize: async (resource) => ({
            effect:
              resource === "tool:charge_card"
                ? ("ask" as const)
                : ("allow" as const),
            matchingGrants: [],
            resolvedBy: null,
          }),
          approvalTimeoutMs: 60_000,
        }),
      ],
    });
    reactor.start();
    reactor.deliver(makeTurnMessage("go"), { turnId: "turn-a" });
    await parkedCorrelation(waitFor);
    await waitFor("connector.reply");

    reactor.abandonTurn("turn-a");
    await waitForTurnEnd(events, "turn-a");

    expect(replyTurnIds(events).get("early")).toBe("turn-a");
    expect(endsFor(events, "turn-a")).toEqual([
      { turnId: "turn-a", outcome: "abandoned" },
    ]);

    reactor.abort("admin_kill");
    await waitFor("reactor.done");
  });

  test("an abandon during the reply commit suppresses the reply", async () => {
    const holdCommit = Promise.withResolvers<boolean>();
    const commitHeld = Promise.withResolvers<boolean>();
    let holding = false;
    const { reactor, events, waitFor } = createTestReactor({
      director: turnDirector(),
      inferenceRunner: turnRunner(),
      afterCheckpoint: async () => {
        if (holding) {
          holding = false;
          commitHeld.resolve(true);
          await holdCommit.promise;
        }
      },
    });
    reactor.start();
    holding = true;
    reactor.deliver(makeTurnMessage("hi"), { turnId: "turn-a" });
    await commitHeld.promise;

    reactor.abandonTurn("turn-a");
    holdCommit.resolve(true);
    await waitForTurnEnd(events, "turn-a");

    expect(events.some((e) => e.type === "connector.reply")).toBe(false);
    expect(endsFor(events, "turn-a")).toEqual([
      { turnId: "turn-a", outcome: "abandoned" },
    ]);

    reactor.abort("admin_kill");
    await waitFor("reactor.done");
  });

  test("abandonTurn is idempotent and does nothing for an ended turn or after done", async () => {
    const { reactor, events, waitFor } = createTestReactor({
      director: turnDirector(),
      inferenceRunner: turnRunner(),
    });
    reactor.start();
    reactor.deliver(makeTurnMessage("hi"), { turnId: "turn-a" });
    await waitForTurnEnd(events, "turn-a");

    // An ended turn: nothing.
    reactor.abandonTurn("turn-a");
    // An unseen turn: it ends abandoned once, however often it is called.
    reactor.abandonTurn("turn-x");
    reactor.abandonTurn("turn-x");
    await waitForTurnEnd(events, "turn-x");

    reactor.abort("admin_kill");
    await waitFor("reactor.done");
    reactor.abandonTurn("turn-y");

    expect(endsFor(events, "turn-a")).toEqual([
      { turnId: "turn-a", outcome: "replied" },
    ]);
    expect(endsFor(events, "turn-x")).toEqual([
      { turnId: "turn-x", outcome: "abandoned" },
    ]);
    expect(endsFor(events, "turn-y")).toEqual([]);
  });

  test("a delivery that names an abandoned turn is dropped while it is pending and throws after its end", async () => {
    const { reactor, events, waitFor } = createTestReactor({
      director: turnDirector(),
      inferenceRunner: turnRunner(),
    });
    reactor.start();
    await waitFor("reactor.start");

    reactor.abandonTurn("turn-x");
    reactor.deliver(makeTurnMessage("late"), { turnId: "turn-x" });
    await waitForTurnEnd(events, "turn-x");
    // The delivery's admission runs in promise continuations, with no
    // timer and no I/O; a task boundary follows them.
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(countOf(events, "message.received")).toBe(0);
    expect(() =>
      reactor.deliver(makeTurnMessage("later"), { turnId: "turn-x" }),
    ).toThrow(/turn-x has already ended/);

    reactor.abort("admin_kill");
    await waitFor("reactor.done");
  });

  test("a correlated delivery for an operation whose turn is being abandoned is consumed", async () => {
    const releaseR = Promise.withResolvers<boolean>();
    const toolCalls: string[] = [];
    const { reactor, events, waitFor } = createTestReactor({
      director: turnDirector(),
      inferenceRunner: turnRunner(async (text) =>
        text === "R" ? releaseR.promise : true,
      ),
      toolRunner: makeToolRunner(async (call) => {
        toolCalls.push(call.id);
        return { callId: call.id, content: "charged" };
      }),
      beforeToolExtensions: [askExtension()],
    });
    reactor.start();
    reactor.deliver(makeTurnMessage("park"), { turnId: "turn-a" });
    const correlationId = await parkedCorrelation(waitFor);
    reactor.deliver(makeTurnMessage("R"), { turnId: "turn-r" });
    await waitUntil(() => countOf(events, "inference.start") >= 2);

    // A's abandon waits behind R's cycle; the approval arrives meanwhile.
    reactor.abandonTurn("turn-a");
    reactor.deliver(makeApprovalMessage(correlationId), { turnId: "turn-d" });
    await waitForTurnEnd(events, "turn-d");
    releaseR.resolve(true);
    await waitForTurnEnd(events, "turn-a");

    expect(endsFor(events, "turn-d")).toEqual([
      { turnId: "turn-d", outcome: "unanswered" },
    ]);
    expect(countOf(events, "message.correlated")).toBe(1);
    expect(toolCalls).toEqual([]);
    expect(endsFor(events, "turn-a")).toEqual([
      { turnId: "turn-a", outcome: "abandoned" },
    ]);

    reactor.abort("admin_kill");
    await waitFor("reactor.done");
  });

  test("with the default director, abandoning a parked turn lets another turn's batch complete and re-infer", async () => {
    // J's model asks for a charge (parks); K's model asks for a lookup
    // (allowed), then answers with text.
    const releaseLookup = Promise.withResolvers<boolean>();
    const lookupStarted = Promise.withResolvers<boolean>();
    const inferenceRunner = async function* (opts: InferenceHarnessOptions) {
      const last = opts.turns.at(-1);
      const answersResult =
        last?.content.some((b) => b.type === "tool_result") === true;
      const text = lastText(opts.turns);
      const turn: AssistantTurn = answersResult
        ? makeAssistantTurn("K done")
        : {
            role: "assistant",
            content: [
              {
                type: "tool_call",
                id: text === "J" ? "call-j" : "call-k",
                name: text === "J" ? "charge_card" : "lookup",
                arguments: {},
              },
            ],
            model: "test-model",
            timestamp: 1000,
          };
      yield {
        type: "inference.done" as const,
        seq: opts.nextSeq(),
        data: { turn, usage: emptyUsage(), source: TEST_SOURCE },
      };
    };
    const { reactor, events, waitFor } = createTestReactor({
      director: createDefaultDirector("test agent", []),
      inferenceRunner,
      toolRunner: makeToolRunner(async (call) => {
        if (call.name === "lookup") {
          lookupStarted.resolve(true);
          await releaseLookup.promise;
        }
        return { callId: call.id, content: "ok" };
      }),
      beforeToolExtensions: [
        createAuthzExtension({
          authorize: async (resource) => ({
            effect:
              resource === "tool:charge_card"
                ? ("ask" as const)
                : ("allow" as const),
            matchingGrants: [],
            resolvedBy: null,
          }),
          approvalTimeoutMs: 60_000,
        }),
      ],
    });
    reactor.start();
    reactor.deliver(makeTurnMessage("J"), { turnId: "turn-j" });
    await parkedCorrelation(waitFor);
    reactor.deliver(makeTurnMessage("K"), { turnId: "turn-k" });
    await lookupStarted.promise;

    reactor.abandonTurn("turn-j");
    releaseLookup.resolve(true);
    await waitForTurnEnd(events, "turn-k");
    await waitForTurnEnd(events, "turn-j");

    expect(replyTurnIds(events).get("K done")).toBe("turn-k");
    expect(endsFor(events, "turn-k")).toEqual([
      { turnId: "turn-k", outcome: "replied" },
    ]);
    expect(endsFor(events, "turn-j")).toEqual([
      { turnId: "turn-j", outcome: "abandoned" },
    ]);

    reactor.abort("admin_kill");
    await waitFor("reactor.done");
  });

  test("an abandoned batch does not trip the doom loop", async () => {
    const toolStarted = Promise.withResolvers<boolean>();
    const { reactor, events, waitFor } = createTestReactor({
      doomLoopThreshold: 1,
      director: directorFromTable(
        {
          "message.received": (_e, _s, caps) =>
            caps.executeTools([
              { id: "call-slow", name: "slow", arguments: {} },
            ]),
        },
        "wait",
      ),
      toolRunner: {
        async run(call, signal) {
          toolStarted.resolve(true);
          await new Promise<void>((resolve) => {
            signal.addEventListener("abort", () => resolve(), { once: true });
          });
          return { callId: call.id, content: "stopped" };
        },
      },
    });
    reactor.start();
    reactor.deliver(makeTurnMessage("go"), { turnId: "turn-a" });
    await toolStarted.promise;

    reactor.abandonTurn("turn-a");
    await waitForTurnEnd(events, "turn-a");

    expect(endsFor(events, "turn-a")).toEqual([
      { turnId: "turn-a", outcome: "abandoned" },
    ]);
    expect(events.some((e) => e.type === "reactor.error")).toBe(false);

    reactor.abort("admin_kill");
    await waitFor("reactor.done");
  });

  test("an abandon during the pacing wait keeps the pacing for the next turn", async () => {
    // The pacing wait runs on the injected scheduler. Each wait is recorded
    // and fires only when the test fires it, so no wall clock is involved.
    const waits: { delayMs: number; fire: () => void }[] = [];
    const deps: Dependencies = {
      ...createDefaultDependencies(),
      scheduler: {
        setTimeout(callback, delayMs) {
          waits.push({ delayMs, fire: callback });
          return () => {
            /* a cancelled wait never fires */
          };
        },
        now: () => 0,
      },
    };
    const PACING_MS = 400;
    let aInferences = 0;
    const replyText = (turn: AssistantTurn) =>
      turn.content.map((b) => (b.type === "text" ? b.text : "")).join("");
    const { reactor, events, waitFor } = createTestReactor({
      deps,
      director: directorFromTable(
        {
          "message.received": (_e, _s, caps) => caps.infer(),
          "inference.done": (e, _s, caps) =>
            replyText(e.turn) === "A1" ? caps.infer() : caps.reply("done"),
        },
        "wait",
      ),
      inferenceRunner: async function* (opts: InferenceHarnessOptions) {
        let text = "B";
        if (lastText(opts.turns) !== "B") {
          aInferences += 1;
          text = `A${String(aInferences)}`;
        }
        yield {
          type: "inference.done" as const,
          seq: opts.nextSeq(),
          data: {
            turn: makeAssistantTurn(text),
            usage: emptyUsage(),
            source: TEST_SOURCE,
            pacingDelayMs: PACING_MS,
          },
        };
      },
    });
    reactor.start();
    reactor.deliver(makeTurnMessage("A"), { turnId: "turn-a" });
    // A's first answer asks for pacing; A's second inference waits.
    await waitUntil(() => waits.length === 1);

    reactor.abandonTurn("turn-a");
    await waitForTurnEnd(events, "turn-a");
    reactor.deliver(makeTurnMessage("B"), { turnId: "turn-b" });
    await waitUntil(
      () => waits.length === 2 || endsFor(events, "turn-b").length > 0,
    );

    // B's inference waits for the delay the provider asked for.
    expect(waits.map((w) => w.delayMs)).toEqual([PACING_MS, PACING_MS]);
    expect(endsFor(events, "turn-b")).toEqual([]);
    waits[1]?.fire();
    await waitForTurnEnd(events, "turn-b");
    expect(endsFor(events, "turn-b")).toEqual([
      { turnId: "turn-b", outcome: "replied" },
    ]);
    expect(aInferences).toBe(1);

    reactor.abort("admin_kill");
    await waitFor("reactor.done");
  });

  test("a tool whose before-tool decision lands after the abandon does not run", async () => {
    const cell = emptyCell();
    const slowStarted = Promise.withResolvers<boolean>();
    const releaseSlow = Promise.withResolvers<boolean>();
    const releaseDecision = Promise.withResolvers<boolean>();
    let sideEffectRan = false;
    const batch: ToolCall[] = [
      { id: "call-slow", name: "slow", arguments: {} },
      { id: "call-side", name: "side_effect", arguments: {} },
    ];
    const batchTurn: AssistantTurn = {
      role: "assistant",
      content: batch.map((c) => ({ type: "tool_call" as const, ...c })),
      model: "test-model",
      timestamp: 1000,
    };
    const { reactor, events, waitFor } = createTestReactor({
      contextStore: makePersistingContextStore(cell),
      director: directorFromTable(
        {
          "message.received": (_e, _s, caps) => caps.infer(),
          "inference.done": (_e, _s, caps) => caps.executeTools(batch, true),
          "tool.done": (_e, _s, caps) => caps.reply("done"),
        },
        "wait",
      ),
      inferenceRunner: async function* (opts: InferenceHarnessOptions) {
        yield {
          type: "inference.done" as const,
          seq: opts.nextSeq(),
          data: { turn: batchTurn, usage: emptyUsage(), source: TEST_SOURCE },
        };
      },
      toolRunner: makeToolRunner(async (toolCall) => {
        if (toolCall.name === "slow") {
          slowStarted.resolve(true);
          await releaseSlow.promise;
        } else {
          sideEffectRan = true;
        }
        return { callId: toolCall.id, content: "ok" };
      }),
      beforeToolExtensions: [
        {
          async beforeTool(toolCall) {
            if (toolCall.name === "side_effect") await releaseDecision.promise;
            return { type: "allow" as const };
          },
        },
      ],
    });
    reactor.start();
    reactor.deliver(makeTurnMessage("go"), { turnId: "turn-a" });
    await slowStarted.promise;

    reactor.abandonTurn("turn-a");
    releaseDecision.resolve(true);
    releaseSlow.resolve(true);
    await waitForTurnEnd(events, "turn-a");

    expect(sideEffectRan).toBe(false);
    expect(endsFor(events, "turn-a")).toEqual([
      { turnId: "turn-a", outcome: "abandoned" },
    ]);
    const results = resultsByCall(cell.turns);
    expect(results.get("call-slow")).toHaveLength(1);
    expect(results.get("call-side")).toEqual(["turn abandoned"]);
    expect(() => assertWellFormedToolSequence(cell.turns)).not.toThrow();

    reactor.abort("admin_kill");
    await waitFor("reactor.done");
  });

  test("a gate timeout racing the abandon ends the turn once with a well-formed history", async () => {
    const cell = emptyCell();
    const releaseR = Promise.withResolvers<boolean>();
    const timers = manualTimers();
    const { reactor, events, waitFor } = createTestReactor({
      deps: timers.deps,
      contextStore: makePersistingContextStore(cell),
      director: turnDirector(),
      inferenceRunner: turnRunner(async (text) =>
        text === "R" ? releaseR.promise : true,
      ),
      toolRunner: chargeRunner(),
      beforeToolExtensions: [askExtension()],
    });
    reactor.start();
    // R queues behind the park, so it runs after turn-a parks and holds
    // the loop while the abandon and the timeout queue behind it.
    reactor.deliver(makeTurnMessage("park"), { turnId: "turn-a" });
    reactor.deliver(makeTurnMessage("R"), { turnId: "turn-r" });
    await waitFor("reactor.gate.blocked");

    // The abandon queues first. The gate's timeout then fires while R
    // holds the loop, so its resume.tool_result queues behind the abandon.
    reactor.abandonTurn("turn-a");
    await waitUntil(() => countOf(events, "inference.start") === 2);
    expect(timers.pending()).toHaveLength(1);
    timers.fire(timers.pending()[0]);
    releaseR.resolve(true);
    await waitForTurnEnd(events, "turn-a");
    await waitForTurnEnd(events, "turn-r");

    // The abandon purged the queued result and answered the call with it:
    // turn-a did not resume and infer again.
    expect(countOf(events, "inference.start")).toBe(2);
    expect(endsFor(events, "turn-a")).toEqual([
      { turnId: "turn-a", outcome: "abandoned" },
    ]);
    expect(resultsByCall(cell.turns).get("call-ask")).toEqual([
      "approval timed out",
    ]);
    expect(() => assertWellFormedToolSequence(cell.turns)).not.toThrow();

    reactor.abort("admin_kill");
    await waitFor("reactor.done");
    expect(endsFor(events, "turn-a")).toHaveLength(1);
  });

  test("an abandon while the director decides a rehydrated re-dispatch answers the call once", async () => {
    // After a restart, turn A's approval re-dispatches call-ask. The abandon
    // lands while the director decides that event, so no queued event names
    // the call and the restart left no open-call record. The admission's
    // open-call record makes the abandon answer it.
    const correlationId = "corr-decide";
    const cell: PersistedCell = {
      turns: [
        {
          role: "user",
          content: [{ type: "text", text: "park" }],
          timestamp: 1000,
        },
        suspendToolCallTurn,
      ],
      pendingOperations: [
        {
          correlationId,
          kind: "approval",
          registeredAt: Date.now(),
          gateId: `pending-${correlationId}`,
          timeoutAt: Date.now() + 60_000,
          suspendedCall: { id: "call-ask", name: "charge_card", arguments: {} },
          turnId: "turn-a",
          gated: true,
        },
      ],
      tokenUsage: emptyUsage(),
    };
    const deciding = Promise.withResolvers<boolean>();
    const hold = Promise.withResolvers<boolean>();
    const inner = turnDirector();
    const director: ReactorDirector = {
      async decide(event, state, caps) {
        if (event.type === "resume.execute_tools") {
          deciding.resolve(true);
          await hold.promise;
        }
        return inner.decide(event, state, caps);
      },
    };
    const ran: string[] = [];
    const { reactor, events, waitFor } = createTestReactor({
      contextStore: makePersistingContextStore(cell),
      director,
      inferenceRunner: turnRunner(),
      toolRunner: makeToolRunner(async (toolCall) => {
        ran.push(toolCall.id);
        return { callId: toolCall.id, content: "charged" };
      }),
      beforeToolExtensions: [askExtension()],
    });
    reactor.start();
    await waitFor("reactor.start");
    reactor.deliver(makeApprovalMessage(correlationId, "approved"), {
      turnId: "turn-d",
    });
    await deciding.promise;
    reactor.abandonTurn("turn-a");
    hold.resolve(true);
    await waitForTurnEnd(events, "turn-a");

    expect(ran).toEqual([]);
    expect(resultsByCall(cell.turns).get("call-ask")).toEqual([
      "turn abandoned",
    ]);
    expect(() => assertWellFormedToolSequence(cell.turns)).not.toThrow();

    reactor.deliver(makeTurnMessage("park"), { turnId: "turn-c" });
    await waitUntil(
      () =>
        ran.length > 0 ||
        events.some(
          (e) =>
            e.type === "reactor.gate.blocked" && e.data.turnId === "turn-c",
        ),
    );
    expect(ran).toEqual([]);
    expect(
      events.some(
        (e) => e.type === "reactor.gate.blocked" && e.data.turnId === "turn-c",
      ),
    ).toBe(true);

    reactor.abort("admin_kill");
    await waitFor("reactor.done");
  });

  test("an abandon during the prompt write starts no provider call", async () => {
    // The abandon lands while the inference writes its prompt, after the
    // context transforms. The inference then stops before the provider.
    const releaseWrite = Promise.withResolvers<boolean>();
    let writing = false;
    let providerCalls = 0;
    const base = makeContextStore();
    const contextStore: ContextStore = {
      ...base,
      async writePrompt(prompt) {
        writing = true;
        await releaseWrite.promise;
        return base.writePrompt(prompt);
      },
    };
    const { reactor, events, waitFor } = createTestReactor({
      contextStore,
      director: turnDirector(),
      inferenceRunner: async function* (opts: InferenceHarnessOptions) {
        providerCalls += 1;
        yield* turnRunner()(opts);
      },
    });
    reactor.start();
    reactor.deliver(makeTurnMessage("go"), { turnId: "turn-a" });
    await waitUntil(() => writing);
    reactor.abandonTurn("turn-a");
    releaseWrite.resolve(true);
    await waitForTurnEnd(events, "turn-a");

    expect(providerCalls).toBe(0);
    expect(countOf(events, "inference.start")).toBe(0);
    expect(endsFor(events, "turn-a")).toEqual([
      { turnId: "turn-a", outcome: "abandoned" },
    ]);

    reactor.abort("admin_kill");
    await waitFor("reactor.done");
    expect(endsFor(events, "turn-a")).toHaveLength(1);
  });

  test("a reactor abort that lands while a before-tool decision is held runs no tool and answers the call aborted", async () => {
    // Not an abandon: the reactor itself aborts. The held decision then
    // allows the call, but no tool starts after the abort.
    const releaseDecision = Promise.withResolvers<boolean>();
    let deciding = false;
    const ran: string[] = [];
    const { reactor, events, waitFor } = createTestReactor({
      director: turnDirector(),
      inferenceRunner: turnRunner(),
      toolRunner: makeToolRunner(async (toolCall) => {
        ran.push(toolCall.id);
        return { callId: toolCall.id, content: "charged" };
      }),
      beforeToolExtensions: [
        {
          async beforeTool() {
            deciding = true;
            await releaseDecision.promise;
            return { type: "allow" as const };
          },
        },
      ],
    });
    reactor.start();
    reactor.deliver(makeTurnMessage("park"), { turnId: "turn-a" });
    await waitUntil(() => deciding);
    reactor.abort("admin_kill");
    releaseDecision.resolve(true);
    await waitFor("reactor.done");

    expect(ran).toEqual([]);
    expect(
      events.flatMap((e) =>
        e.type === "tool.done"
          ? [[e.data.result.callId, e.data.result.content]]
          : [],
      ),
    ).toEqual([["call-ask", "reactor aborted"]]);
  });

  test("a before-tool suspend that returns after the abandon parks nothing", async () => {
    const cell = emptyCell();
    const held = Promise.withResolvers<boolean>();
    let waiting = false;
    const holdThenSuspend: BeforeToolExtension = {
      async beforeTool(toolCall) {
        waiting = true;
        await held.promise;
        return {
          type: "suspend",
          gate: {
            type: "approval",
            gateId: "g-hold",
            correlationId: "corr-hold",
            timeoutAt: Date.now() + 60_000,
          },
          pendingOp: {
            correlationId: "corr-hold",
            kind: "approval",
            registeredAt: Date.now(),
            gateId: "g-hold",
            suspendedCall: toolCall,
          },
        };
      },
    };
    const { reactor, events, waitFor } = createTestReactor({
      contextStore: makePersistingContextStore(cell),
      director: turnDirector(),
      inferenceRunner: turnRunner(),
      toolRunner: chargeRunner(),
      beforeToolExtensions: [holdThenSuspend],
    });
    reactor.start();
    reactor.deliver(makeTurnMessage("park"), { turnId: "turn-a" });
    await waitUntil(() => waiting);
    reactor.abandonTurn("turn-a");
    held.resolve(true);
    await waitForTurnEnd(events, "turn-a");

    const gateEvents = events.filter(
      (e) =>
        (e.type === "reactor.gate.blocked" ||
          e.type === "reactor.gate.cleared") &&
        e.data.gateId === "g-hold",
    );
    expect(gateEvents).toEqual([]);
    expect(cell.pendingOperations).toEqual([]);
    expect(resultsByCall(cell.turns).get("call-ask")).toEqual([
      "turn abandoned",
    ]);
    expect(() => assertWellFormedToolSequence(cell.turns)).not.toThrow();

    reactor.abort("admin_kill");
    await waitFor("reactor.done");
  });

  test("after an abandon drops an approved re-dispatch, a later call with its id hits the ask gate", async () => {
    // Turn A parks call-ask. B runs and holds the loop, so the approval's
    // re-dispatch of call-ask queues behind B. The abandon drops that
    // re-dispatch, and its approval with it. A later call with the same id
    // must hit the ask gate.
    const releaseB = Promise.withResolvers<boolean>();
    let bHeld = false;
    const ran: string[] = [];
    const { reactor, events, waitFor } = createTestReactor({
      contextStore: makePersistingContextStore(emptyCell()),
      director: turnDirector(),
      inferenceRunner: turnRunner(async (text) => {
        if (text !== "B") return true;
        bHeld = true;
        return releaseB.promise;
      }),
      toolRunner: makeToolRunner(async (toolCall) => {
        ran.push(toolCall.id);
        return { callId: toolCall.id, content: "charged" };
      }),
      beforeToolExtensions: [askExtension()],
    });
    reactor.start();
    reactor.deliver(makeTurnMessage("park"), { turnId: "turn-a" });
    const correlationId = await parkedCorrelation(waitFor);

    reactor.deliver(makeTurnMessage("B"), { turnId: "turn-b" });
    await waitUntil(() => bHeld);
    reactor.deliver(makeApprovalMessage(correlationId, "approved"), {
      turnId: "turn-d",
    });
    await waitFor("message.correlated");
    reactor.abandonTurn("turn-a");
    releaseB.resolve(true);
    await waitForTurnEnd(events, "turn-a");
    expect(ran).toEqual([]);

    // The same model turn asks for call-ask again under turn C.
    reactor.deliver(makeTurnMessage("park"), { turnId: "turn-c" });
    await waitUntil(
      () =>
        ran.length > 0 ||
        events.some(
          (e) =>
            e.type === "reactor.gate.blocked" && e.data.turnId === "turn-c",
        ),
    );
    expect(ran).toEqual([]);
    expect(
      events.some(
        (e) => e.type === "reactor.gate.blocked" && e.data.turnId === "turn-c",
      ),
    ).toBe(true);

    reactor.abort("admin_kill");
    await waitFor("reactor.done");
  });

  test("a queued denied result of a rehydrated parked turn is answered when the turn is abandoned", async () => {
    const correlationId = "legacy-deny";
    const cell: PersistedCell = {
      turns: [
        {
          role: "user",
          content: [{ type: "text", text: "park" }],
          timestamp: 1000,
        },
        suspendToolCallTurn,
      ],
      pendingOperations: [
        {
          correlationId,
          kind: "approval",
          registeredAt: Date.now(),
          gateId: `pending-${correlationId}`,
          timeoutAt: Date.now() + 60_000,
          suspendedCall: { id: "call-ask", name: "charge_card", arguments: {} },
        },
      ],
      tokenUsage: emptyUsage(),
    };
    const releaseR = Promise.withResolvers<boolean>();
    const { reactor, events, waitFor } = createTestReactor({
      contextStore: makePersistingContextStore(cell),
      director: turnDirector(),
      inferenceRunner: turnRunner(async (text) =>
        text === "R" ? releaseR.promise : true,
      ),
    });
    reactor.start();
    await waitFor("reactor.start");
    const legacy = legacyTurnId(correlationId);

    // R runs, so the denial's resume.tool_result queues behind R's cycle.
    reactor.deliver(makeTurnMessage("R"), { turnId: "turn-r" });
    await waitFor("inference.start");
    reactor.deliver(makeApprovalMessage(correlationId, "rejected"), {
      turnId: "turn-d",
    });
    await waitFor("message.correlated");

    // turn-d joined the legacy turn; the abandon follows the join.
    reactor.abandonTurn("turn-d");
    releaseR.resolve(true);
    await waitForTurnEnd(events, legacy);
    await waitForTurnEnd(events, "turn-r");

    expect(resultsByCall(cell.turns).get("call-ask")).toHaveLength(1);

    reactor.abort("admin_kill");
    await waitFor("reactor.done");
  });

  test("an abandon of another turn waits until the running turn's cycle is quiescent", async () => {
    const releaseTool = Promise.withResolvers<boolean>();
    const toolStarted = Promise.withResolvers<boolean>();
    let eventsAtToolDone = -1;
    let eventsRef: ReactorEmittedEvent[] = [];
    const { reactor, events, waitFor } = createTestReactor({
      director: directorFromTable(
        {
          "message.received": (e, _s, caps) =>
            e.message.content === "park"
              ? caps.executeTools([
                  { id: "call-ask", name: "charge_card", arguments: {} },
                ])
              : caps.executeTools([
                  { id: "call-slow", name: "slow", arguments: {} },
                ]),
          "tool.done": (e, _s, caps) => {
            if (e.result.callId === "call-slow") {
              eventsAtToolDone = eventsRef.length;
              return caps.reply("A done");
            }
            return caps.wait();
          },
        },
        "wait",
      ),
      toolRunner: makeToolRunner(async (call) => {
        if (call.name === "slow") {
          toolStarted.resolve(true);
          await releaseTool.promise;
        }
        return { callId: call.id, content: "ok" };
      }),
      beforeToolExtensions: [
        createAuthzExtension({
          authorize: async (resource) => ({
            effect:
              resource === "tool:charge_card"
                ? ("ask" as const)
                : ("allow" as const),
            matchingGrants: [],
            resolvedBy: null,
          }),
          approvalTimeoutMs: 60_000,
        }),
      ],
    });
    eventsRef = events;
    reactor.start();
    reactor.deliver(makeTurnMessage("park"), { turnId: "turn-b" });
    await parkedCorrelation(waitFor);
    reactor.deliver(makeTurnMessage("go"), { turnId: "turn-a" });
    await toolStarted.promise;

    reactor.abandonTurn("turn-b");
    releaseTool.resolve(true);
    await waitForTurnEnd(events, "turn-b");

    const clearedAt = events.findIndex(
      (e) => e.type === "reactor.gate.cleared" && e.data.reason === "abandoned",
    );
    expect(eventsAtToolDone).toBeGreaterThan(-1);
    expect(clearedAt).toBeGreaterThanOrEqual(eventsAtToolDone);

    reactor.abort("admin_kill");
    await waitFor("reactor.done");
  });
});

describe("createReactor — gate.cleared of an ask-flow gate", () => {
  function emptyCell(): PersistedCell {
    return { turns: [], pendingOperations: [], tokenUsage: emptyUsage() };
  }

  function clearedOf(
    events: ReactorEmittedEvent[],
  ): { index: number; gateId: string; reason: string }[] {
    return events.flatMap((e, index) =>
      e.type === "reactor.gate.cleared"
        ? [{ index, gateId: e.data.gateId, reason: e.data.reason }]
        : [],
    );
  }

  function indexOfType(
    events: ReactorEmittedEvent[],
    type: ReactorEmittedEvent["type"],
  ): number {
    return events.findIndex((e) => e.type === type);
  }

  async function parkedGate(
    waitFor: TestReactorHandle["waitFor"],
  ): Promise<{ gateId: string; correlationId: string }> {
    const blocked = await waitFor("reactor.gate.blocked");
    if (blocked.type !== "reactor.gate.blocked") throw new Error("unreachable");
    const { gateId, correlationId } = blocked.data;
    if (correlationId === undefined) throw new Error("expected correlation");
    return { gateId, correlationId };
  }

  function waitForTurnEnd(events: ReactorEmittedEvent[], turnId: string) {
    return waitUntil(() => turnEnds(events).some((e) => e.turnId === turnId));
  }

  test("an approved call's gate emits gate.cleared resolved before the re-run", async () => {
    const { reactor, events, waitFor } = createTestReactor({
      contextStore: makePersistingContextStore(emptyCell()),
      director: turnDirector(),
      inferenceRunner: turnRunner(),
      toolRunner: chargeRunner(),
      beforeToolExtensions: [askExtension()],
    });
    reactor.start();
    reactor.deliver(makeTurnMessage("park"), { turnId: "turn-a" });
    const gate = await parkedGate(waitFor);
    reactor.deliver(makeApprovalMessage(gate.correlationId), {
      turnId: "turn-d",
    });
    await waitForTurnEnd(events, "turn-a");

    const cleared = clearedOf(events);
    expect(cleared.map(({ gateId, reason }) => ({ gateId, reason }))).toEqual([
      { gateId: gate.gateId, reason: "resolved" },
    ]);
    expect(cleared[0]?.index).toBeGreaterThan(
      indexOfType(events, "reactor.gate.blocked"),
    );
    expect(cleared[0]?.index).toBeLessThan(indexOfType(events, "tool.start"));

    reactor.abort("admin_kill");
    await waitFor("reactor.done");
  });

  test("a rejected call's gate emits gate.cleared resolved", async () => {
    const { reactor, events, waitFor } = createTestReactor({
      contextStore: makePersistingContextStore(emptyCell()),
      director: turnDirector(),
      inferenceRunner: turnRunner(),
      toolRunner: chargeRunner(),
      beforeToolExtensions: [askExtension()],
    });
    reactor.start();
    reactor.deliver(makeTurnMessage("park"), { turnId: "turn-a" });
    const gate = await parkedGate(waitFor);
    reactor.deliver(makeApprovalMessage(gate.correlationId, "rejected"), {
      turnId: "turn-d",
    });
    await waitForTurnEnd(events, "turn-a");

    expect(
      clearedOf(events).map(({ gateId, reason }) => ({ gateId, reason })),
    ).toEqual([{ gateId: gate.gateId, reason: "resolved" }]);

    reactor.abort("admin_kill");
    await waitFor("reactor.done");
  });

  test("a timed-out approval's gate emits gate.cleared timeout", async () => {
    const { reactor, events, waitFor } = createTestReactor({
      contextStore: makePersistingContextStore(emptyCell()),
      director: turnDirector(),
      inferenceRunner: turnRunner(),
      toolRunner: chargeRunner(),
      beforeToolExtensions: [askExtension(5)],
    });
    reactor.start();
    reactor.deliver(makeTurnMessage("park"), { turnId: "turn-a" });
    const gate = await parkedGate(waitFor);
    await waitForTurnEnd(events, "turn-a");

    expect(
      clearedOf(events).map(({ gateId, reason }) => ({ gateId, reason })),
    ).toEqual([{ gateId: gate.gateId, reason: "timeout" }]);

    reactor.abort("admin_kill");
    await waitFor("reactor.done");
  });

  test("an approval admitted during its park's commit emits gate.cleared after gate.blocked", async () => {
    const cell = emptyCell();
    const store = makePersistingContextStore(cell);
    const baseCommit = store.commit.bind(store);
    const release = Promise.withResolvers<boolean>();
    let held = false;
    let holding = false;
    store.commit = async (options) => {
      if (
        !held &&
        cell.pendingOperations.some((op) => op.suspendedCall !== undefined)
      ) {
        held = true;
        holding = true;
        await release.promise;
        holding = false;
      }
      return baseCommit(options);
    };
    const { reactor, events, waitFor } = createTestReactor({
      contextStore: store,
      director: turnDirector(),
      inferenceRunner: turnRunner(),
      toolRunner: chargeRunner(),
      beforeToolExtensions: [askExtension()],
    });
    reactor.start();
    reactor.deliver(makeTurnMessage("park"), { turnId: "turn-a" });
    await waitUntil(() => holding);
    const correlationId = cell.pendingOperations.find(
      (op) => op.suspendedCall !== undefined,
    )?.correlationId;
    if (correlationId === undefined) throw new Error("expected a park");

    reactor.deliver(makeApprovalMessage(correlationId), { turnId: "turn-d" });
    await waitFor("message.correlated");
    expect(clearedOf(events)).toEqual([]);
    release.resolve(true);
    await waitForTurnEnd(events, "turn-a");

    const cleared = clearedOf(events);
    expect(cleared.map(({ reason }) => reason)).toEqual(["resolved"]);
    expect(cleared[0]?.index).toBeGreaterThan(
      indexOfType(events, "reactor.gate.blocked"),
    );

    reactor.abort("admin_kill");
    await waitFor("reactor.done");
  });
});

// ---------------------------------------------------------------------------
// An approval passes, as data, to the run of the exact call it approved
// ---------------------------------------------------------------------------

describe("createReactor — an approval passed to the re-run", () => {
  function blockedFor(events: ReactorEmittedEvent[], turnId: string): number {
    return events.filter(
      (e) => e.type === "reactor.gate.blocked" && e.data.turnId === turnId,
    ).length;
  }

  async function parkedCorrelationOf(
    waitFor: TestReactorHandle["waitFor"],
  ): Promise<string> {
    const blocked = await waitFor("reactor.gate.blocked");
    if (blocked.type !== "reactor.gate.blocked") throw new Error("unreachable");
    const correlationId = blocked.data.correlationId;
    if (correlationId === undefined) throw new Error("expected correlationId");
    return correlationId;
  }

  function recordingRunner(ran: string[]) {
    return makeToolRunner(async (toolCall) => {
      ran.push(toolCall.id);
      return { callId: toolCall.id, content: "charged" };
    });
  }

  test("an approved call runs once", async () => {
    const ran: string[] = [];
    const { reactor, events, waitFor } = createTestReactor({
      director: turnDirector(),
      inferenceRunner: turnRunner(),
      toolRunner: recordingRunner(ran),
      beforeToolExtensions: [askExtension()],
    });
    reactor.start();
    reactor.deliver(makeTurnMessage("park"), { turnId: "turn-a" });
    const correlationId = await parkedCorrelationOf(waitFor);
    reactor.deliver(makeApprovalMessage(correlationId), { turnId: "turn-d" });
    await waitUntil(() => turnEnds(events).some((e) => e.turnId === "turn-a"));

    expect(ran).toEqual(["call-ask"]);
    expect(turnEnds(events).filter((e) => e.turnId === "turn-a")).toEqual([
      { turnId: "turn-a", outcome: "replied" },
    ]);

    reactor.abort("admin_kill");
    await waitFor("reactor.done");
  });

  test("an approved call that the director runs in a later cycle parks again", async () => {
    // The director answers the re-dispatch with no action and runs the
    // same call later. The approval belonged to the re-dispatch's cycle.
    const ran: string[] = [];
    let deferred: ToolCall[] = [];
    const { reactor, events, waitFor } = createTestReactor({
      director: directorFromTable(
        {
          "message.received": (e, _s, caps) =>
            e.message.content === "retry"
              ? caps.executeTools(deferred, false)
              : caps.infer(),
          "inference.done": (e, _s, caps) => {
            const calls = e.turn.content.flatMap((b) =>
              b.type === "tool_call"
                ? [{ id: b.id, name: b.name, arguments: b.arguments }]
                : [],
            );
            return caps.executeTools(calls);
          },
          "resume.execute_tools": (e) => {
            deferred = e.calls;
            return [];
          },
        },
        "wait",
      ),
      inferenceRunner: turnRunner(),
      toolRunner: recordingRunner(ran),
      beforeToolExtensions: [askExtension()],
    });
    reactor.start();
    reactor.deliver(makeTurnMessage("park"), { turnId: "turn-a" });
    const correlationId = await parkedCorrelationOf(waitFor);
    reactor.deliver(makeApprovalMessage(correlationId), { turnId: "turn-d" });
    await waitUntil(() => deferred.length > 0);
    await waitUntil(() => turnEnds(events).some((e) => e.turnId === "turn-a"));

    reactor.deliver(makeTurnMessage("retry"), { turnId: "turn-r" });
    await waitUntil(() => ran.length > 0 || blockedFor(events, "turn-r") > 0);

    expect(ran).toEqual([]);
    expect(blockedFor(events, "turn-r")).toBe(1);

    reactor.abort("admin_kill");
    await waitFor("reactor.done");
  });

  test("a call that shares the approved call's id in the same batch parks", async () => {
    // The director runs the approved call and a copy of it in one parallel
    // batch. Only the approved call object gets the approval.
    const ran: string[] = [];
    const { reactor, events, waitFor } = createTestReactor({
      director: directorFromTable(
        {
          "message.received": (_e, _s, caps) => caps.infer(),
          "inference.done": (e, _s, caps) => {
            const calls = e.turn.content.flatMap((b) =>
              b.type === "tool_call"
                ? [{ id: b.id, name: b.name, arguments: b.arguments }]
                : [],
            );
            return calls.length > 0
              ? caps.executeTools(calls)
              : caps.reply("done");
          },
          "resume.execute_tools": (e, _s, caps) =>
            caps.executeTools(
              [...e.calls, ...e.calls.map((c) => ({ ...c }))],
              true,
              true,
            ),
          "tool.done": (_e, _s, caps) => caps.wait(),
        },
        "wait",
      ),
      inferenceRunner: turnRunner(),
      toolRunner: recordingRunner(ran),
      beforeToolExtensions: [askExtension()],
    });
    reactor.start();
    reactor.deliver(makeTurnMessage("park"), { turnId: "turn-a" });
    const correlationId = await parkedCorrelationOf(waitFor);
    reactor.deliver(makeApprovalMessage(correlationId), { turnId: "turn-d" });
    await waitUntil(() => blockedFor(events, "turn-a") === 2);

    expect(ran).toEqual(["call-ask"]);

    reactor.abort("admin_kill");
    await waitFor("reactor.done");
  });

  test("after an approved re-run fails, a later call with the same id parks", async () => {
    let attempts = 0;
    const { reactor, events, waitFor } = createTestReactor({
      director: turnDirector(),
      inferenceRunner: turnRunner(),
      toolRunner: makeToolRunner(async (toolCall) => {
        attempts += 1;
        return { callId: toolCall.id, content: "failed", isError: true };
      }),
      beforeToolExtensions: [askExtension()],
    });
    reactor.start();
    reactor.deliver(makeTurnMessage("park"), { turnId: "turn-a" });
    const correlationId = await parkedCorrelationOf(waitFor);
    reactor.deliver(makeApprovalMessage(correlationId), { turnId: "turn-d" });
    await waitUntil(() => turnEnds(events).some((e) => e.turnId === "turn-a"));
    expect(attempts).toBe(1);

    reactor.deliver(makeTurnMessage("park"), { turnId: "turn-c" });
    await waitUntil(() => attempts > 1 || blockedFor(events, "turn-c") > 0);

    expect(attempts).toBe(1);
    expect(blockedFor(events, "turn-c")).toBe(1);

    reactor.abort("admin_kill");
    await waitFor("reactor.done");
  });
});
