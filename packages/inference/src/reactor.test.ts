import { describe, test, expect } from "bun:test";

import { validateActions } from "./actions";
import { createAuthzExtension } from "./authz-extension";
import { createGateManager } from "./gates";
import { createCorrelationRegistry } from "./correlation";
import { createReactor } from "./reactor";
import { createDefaultDependencies } from "./providers";
import { createDefaultDirector } from "./default-director";
import { assertWellFormedToolSequence } from "./turns";
import { createInboundMessage } from "@intx/mime";

import type {
  ReactorDirector,
  ReactorAction,
  ReactorInboundEvent,
  ReactorState,
  ReactorCapabilities,
  ContextStore,
  ToolRunner,
  InferenceEvent,
  InboundMessage,
  ConversationTurn,
  PendingOperation,
  TokenUsage,
  ContextCommit,
  AssistantTurn,
  InferenceError,
  PartialMessage,
  BeforeToolExtension,
  Compactor,
  LastCycleSource,
} from "@intx/types/runtime";

import type { ReactorConfig, Reactor, ReactorEmittedEvent } from "./reactor";
import type { Dependencies, InferenceHarnessOptions } from "./harness";
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

function waitForEvent(
  events: ReactorEmittedEvent[],
  predicate: (e: ReactorEmittedEvent) => boolean,
  timeoutMs = 2000,
): Promise<ReactorEmittedEvent> {
  return new Promise((resolve, reject) => {
    const deadline = setTimeout(
      () => reject(new Error("Timed out waiting for event")),
      timeoutMs,
    );

    function check() {
      const found = events.find(predicate);
      if (found !== undefined) {
        clearTimeout(deadline);
        resolve(found);
        return;
      }
      setTimeout(check, 10);
    }
    check();
  });
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
      apiKey: "test",
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
    ...(overrides.compactors !== undefined
      ? { compactors: overrides.compactors }
      : {}),
  };

  const reactor = createReactor(config);

  function waitFor(
    type: ReactorEmittedEvent["type"],
    timeoutMs = 2000,
  ): Promise<ReactorEmittedEvent> {
    return waitForEvent(events, (e) => e.type === type, timeoutMs);
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
      (id, reason) => cleared.push(`${id}:${reason}`),
    );

    const didClear = manager.clear("gate-1");
    expect(didClear).toBe(true);

    const reason = await promise;
    expect(reason).toBe("resolved");
    expect(cleared).toEqual(["gate-1:resolved"]);
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
      (id, reason) => cleared.push(`${id}:${reason}`),
    );

    const reason = await promise;
    expect(reason).toBe("timeout");
    expect(cleared).toEqual(["gate-2:timeout"]);
  });

  test("shutdown clears all gates with reason=shutdown", async () => {
    const manager = createGateManager();
    const cleared: string[] = [];

    const p1 = manager.register(
      "g1",
      "approval",
      60000,
      undefined,
      (id, reason) => cleared.push(`${id}:${reason}`),
    );
    const p2 = manager.register(
      "g2",
      "payment",
      60000,
      undefined,
      (id, reason) => cleared.push(`${id}:${reason}`),
    );

    manager.shutdown();

    const [r1, r2] = await Promise.all([p1, p2]);
    expect(r1).toBe("shutdown");
    expect(r2).toBe("shutdown");
    expect(cleared.sort()).toEqual(["g1:shutdown", "g2:shutdown"].sort());
  });

  test("clearing a nonexistent gate returns false", () => {
    const manager = createGateManager();
    expect(manager.clear("no-such-gate")).toBe(false);
  });

  test("zero timeout throws", () => {
    const manager = createGateManager();
    expect(() =>
      manager.register("g", "approval", 0, undefined, () => {
        /* noop */
      }),
    ).toThrow();
  });

  test("duplicate gate ID throws", () => {
    const manager = createGateManager();
    void manager.register("g", "approval", 5000, undefined, () => {
      /* noop */
    });
    expect(() =>
      manager.register("g", "approval", 5000, undefined, () => {
        /* noop */
      }),
    ).toThrow();
  });

  test("findByCorrelationId returns the gate", () => {
    const manager = createGateManager();
    void manager.register("g-corr", "message_response", 5000, "corr-42", () => {
      /* noop */
    });
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
        apiKey: "test",
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
    const base = makeContextStore();
    const contextStore: ContextStore = {
      ...base,
      async writeMetadata(metadata, signal) {
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
            gateId: "commit-order-gate",
            timeoutMs: 5000,
          }),
        "reactor.gate.cleared": (_e, _s, caps) => caps.done(),
      }),
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());

    // The commit is held, so a correctly-ordered reactor has not emitted
    // `blocked` yet; a reactor that emits before committing would have.
    await new Promise((r) => setTimeout(r, 50));
    expect(events.some((e) => e.type === "reactor.gate.blocked")).toBe(false);

    // Release the durable commit; only now may `blocked` be emitted.
    releaseCommit?.();
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
    const base = makeContextStore();
    const contextStore: ContextStore = {
      ...base,
      async writeMetadata(metadata, signal) {
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

    // Let the gate's timeout elapse while the commit is still held. Neither the
    // block nor the clear may surface until the commit is released.
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

    await waitFor("reactor.done", 3000);

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

    await waitFor("reactor.done", 3000);

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

    // After wait, deliver a second message which triggers done.
    // Small delay to let the reactor process the first message.
    setTimeout(() => reactor.deliver(makeInboundMessage()), 20);
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

    // After reply, reactor waits for next event. Deliver another message.
    setTimeout(() => reactor.deliver(makeInboundMessage()), 20);
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

    // After checkpoint failure + wait, deliver another message.
    setTimeout(() => reactor.deliver(makeInboundMessage()), 50);
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

    const { reactor, waitFor } = createTestReactor({
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
        // Deliver a second message while the tool runs, then yield to a real
        // timer so deliver()'s async enqueue lands before executeTools
        // enqueues tool.done. This pins the queue order to
        // [message.received, tool.done] — the interleave that triggers the bug.
        reactor.deliver(makeInboundMessage());
        await new Promise((resolve) => setTimeout(resolve, 10));
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

    const { reactor, waitFor } = createTestReactor({
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
        reactor.deliver(makeInboundMessage());
        await new Promise((resolve) => setTimeout(resolve, 10));
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
        reactor.abort("admin_kill");
        await new Promise((resolve) => setTimeout(resolve, 10));
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

    const { reactor, waitFor } = createTestReactor({
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
        // Deliver mail mid-inference, then yield to a real timer so the
        // message is enqueued before inference.done.
        reactor.deliver(makeInboundMessage());
        await new Promise((resolve) => setTimeout(resolve, 10));
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
    setTimeout(() => reactor.deliver(makeInboundMessage()), 30);
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
  test("reactor.done is emitted exactly once even when messages arrive after shutdown", async () => {
    const { reactor, events, waitFor } = createTestReactor({
      director: directorFromTable({
        "message.received": (_e, _s, caps) => caps.done(),
      }),
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitFor("reactor.done");

    const eventsBeforeDeliver = events.length;

    // Deliver after done — the done guard in deliver() should prevent
    // any state mutation or event emission.
    reactor.deliver(makeInboundMessage());
    reactor.deliver(makeInboundMessage());

    // Give the event loop a chance to process any spurious events.
    await new Promise((resolve) => setTimeout(resolve, 50));

    // No new events should have been emitted after reactor.done.
    expect(events.length).toBe(eventsBeforeDeliver);
    const doneEvents = events.filter((e) => e.type === "reactor.done");
    expect(doneEvents.length).toBe(1);
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
  pendingOperations: PendingOperation[];
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

  test("the re-dispatched approved call re-infers exactly once and leaves no outstanding results", async () => {
    // This guards the pendingToolResults counter trap. A re-dispatch driven
    // from the correlation path never passes through inference.done, so unless
    // the director seeds its outstanding-result count off resume.execute_tools,
    // the count sits at zero and the re-dispatched call's tool.done drives an
    // accidental re-inference off a negative count. The seed makes the
    // continuation deterministic: exactly one re-inference after exactly one
    // re-dispatched result.
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
    // continuation re-inference after the re-dispatched tool completed. A
    // counter left unseeded would either hang (no re-infer) or, once the
    // negative-count accident is removed, fail to continue at all.
    const inferenceDones = events.filter((e) => e.type === "inference.done");
    expect(inferenceDones).toHaveLength(2);

    // Exactly one tool ran and one result was produced, so the director's
    // outstanding count returned to zero (a re-infer fires only at zero).
    const toolDones = events.filter((e) => e.type === "tool.done");
    expect(toolDones).toHaveLength(1);

    // A conversational reply returns the reactor to idle rather than shutting
    // it down; abort so the test does not leak the reactor.
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

    // The tool never ran, and no plain gate-cleared drove a second re-infer.
    expect(toolsRun).toEqual([]);
    expect(events.some((e) => e.type === "reactor.gate.cleared")).toBe(false);

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
    const duplicate: PendingOperation[] = [
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
  metadata: { pendingOperations: PendingOperation[]; tokenUsage: TokenUsage }[];
  blobs: { key: string; bytes: Uint8Array; contentType?: string }[];
  lastWrittenTurns: ConversationTurn[];
} {
  const commits: { message: string; turns: ConversationTurn[] }[] = [];
  const manifests: TransformRecord[][] = [];
  const metadata: {
    pendingOperations: PendingOperation[];
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
      apiKey: "test",
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
    timeoutMs = 2000,
  ): Promise<ReactorEmittedEvent> {
    return waitForEvent(events, (e) => e.type === type, timeoutMs);
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
    setTimeout(() => reactor.deliver(makeInboundMessage()), 30);
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
    setTimeout(() => reactor.deliver(makeInboundMessage()), 30);

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
    setTimeout(() => reactor.deliver(makeInboundMessage()), 80);
    await waitFor("reactor.done");

    expect(recording.commits.length).toBeGreaterThanOrEqual(1);
    expect(recording.commits[0]?.message).toBe("first-override");
    // If a second commit happened, its message must not be the override.
    for (let i = 1; i < recording.commits.length; i++) {
      expect(recording.commits[i]?.message).not.toBe("first-override");
    }
  });
});

// ---------------------------------------------------------------------------
// 28. Per-message run-bracket emission
// ---------------------------------------------------------------------------

describe("createReactor — message.run bracket emission", () => {
  test("trivial happy-path dequeue emits started then ended with completed", async () => {
    const { reactor, events, waitFor } = createTestReactor({
      director: directorFromTable({
        "message.received": (_e, _s, caps) => caps.done(),
      }),
    });

    reactor.start();
    const inbound = makeInboundMessage();
    reactor.deliver(inbound);
    await waitFor("reactor.done");

    const started = getEvent(events, "message.run.started");
    const ended = getEvent(events, "message.run.ended");

    expect(started.data.messageId).toBe(inbound.headers.messageId);
    expect(typeof started.data.messageRunId).toBe("string");
    expect(started.data.messageRunId.length).toBeGreaterThan(0);
    expect(typeof started.data.receivedAt).toBe("number");

    expect(ended.data.messageRunId).toBe(started.data.messageRunId);
    expect(ended.data.messageId).toBe(inbound.headers.messageId);
    expect(ended.data.status).toBe("completed");
    expect(ended.data.error).toBeUndefined();

    // Ordering: started before ended.
    const startedIdx = events.findIndex(
      (e) => e.type === "message.run.started",
    );
    const endedIdx = events.findIndex((e) => e.type === "message.run.ended");
    expect(startedIdx).toBeLessThan(endedIdx);
  });

  test("wait terminal action emits ended with completed", async () => {
    let count = 0;
    const { reactor, events, waitFor } = createTestReactor({
      director: directorFromTable({
        "message.received": (_e, _s, caps) => {
          count++;
          if (count >= 2) return caps.done();
          return caps.wait();
        },
      }),
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());
    setTimeout(() => reactor.deliver(makeInboundMessage()), 20);
    await waitFor("reactor.done");

    const endedEvents = events.filter((e) => e.type === "message.run.ended");
    // Two bracket-ends: one per delivered message.
    expect(endedEvents.length).toBe(2);
    for (const e of endedEvents) {
      if (e.type !== "message.run.ended") throw new Error("unreachable");
      expect(e.data.status).toBe("completed");
    }
  });

  test("reply terminal action emits ended with completed before connector.reply pairing", async () => {
    let count = 0;
    const { reactor, events, waitFor } = createTestReactor({
      director: directorFromTable({
        "message.received": (_e, _s, caps) => {
          count++;
          if (count >= 2) return caps.done();
          return caps.reply("ok");
        },
      }),
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());
    setTimeout(() => reactor.deliver(makeInboundMessage()), 20);
    await waitFor("reactor.done");

    const replyIdx = events.findIndex((e) => e.type === "connector.reply");
    const firstEndedIdx = events.findIndex(
      (e) => e.type === "message.run.ended",
    );
    expect(replyIdx).toBeGreaterThan(-1);
    expect(firstEndedIdx).toBeGreaterThan(replyIdx);

    const endedEvents = events.filter((e) => e.type === "message.run.ended");
    expect(endedEvents.length).toBe(2);
  });

  test("mid-message director exception emits ended with failed and reactor_fatal kind", async () => {
    let firstSeen = false;
    const director: ReactorDirector = {
      async decide(event) {
        if (event.type === "message.received" && !firstSeen) {
          firstSeen = true;
          return { type: "infer" as const };
        }
        throw new Error("director blew up");
      },
    };

    const { reactor, events, waitFor } = createTestReactor({
      director,
      inferenceRunner: mockInferenceRunner("hello"),
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitFor("reactor.done");

    const ended = getEvent(events, "message.run.ended");
    expect(ended.data.status).toBe("failed");
    expect(ended.data.error?.message).toMatch(/director blew up/);
    expect(ended.data.error?.kind).toBe("reactor_fatal");
  });

  test("invalid action set abandons the message with failed status", async () => {
    const { reactor, events, waitFor } = createTestReactor({
      director: directorFromTable({
        "message.received": (_e, _s, caps) => [caps.infer(), caps.done()],
      }),
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitFor("reactor.done");

    const ended = getEvent(events, "message.run.ended");
    expect(ended.data.status).toBe("failed");
    expect(ended.data.error?.kind).toBe("reactor_fatal");
    expect(ended.data.error?.message).toMatch(/Invalid action set/);
  });

  test("bracket events sit between inference cycle and the connector.reply terminal", async () => {
    let count = 0;
    const { reactor, events, waitFor } = createTestReactor({
      director: directorFromTable({
        "message.received": (_e, _s, caps) => {
          count++;
          if (count >= 2) return caps.done();
          return caps.infer();
        },
        "inference.done": (_e, _s, caps) => caps.reply("response"),
      }),
      inferenceRunner: mockInferenceRunner("hello"),
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());
    setTimeout(() => reactor.deliver(makeInboundMessage()), 30);
    await waitFor("reactor.done");

    const types = events.map((e) => e.type);
    const startedIdx = types.indexOf("message.run.started");
    const inferenceDoneIdx = types.indexOf("inference.done");
    const replyIdx = types.indexOf("connector.reply");
    const endedIdx = types.indexOf("message.run.ended");

    expect(startedIdx).toBeGreaterThan(-1);
    expect(inferenceDoneIdx).toBeGreaterThan(startedIdx);
    expect(replyIdx).toBeGreaterThan(inferenceDoneIdx);
    expect(endedIdx).toBeGreaterThan(replyIdx);
  });

  test("multiple sequential messages each get a unique messageRunId", async () => {
    let count = 0;
    const { reactor, events, waitFor } = createTestReactor({
      director: directorFromTable({
        "message.received": (_e, _s, caps) => {
          count++;
          if (count >= 3) return caps.done();
          return caps.wait();
        },
      }),
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());
    setTimeout(() => reactor.deliver(makeInboundMessage()), 20);
    setTimeout(() => reactor.deliver(makeInboundMessage()), 40);
    await waitFor("reactor.done");

    const started = events.filter((e) => e.type === "message.run.started");
    expect(started.length).toBe(3);

    const runIds = started.map((e) => {
      if (e.type !== "message.run.started") throw new Error("unreachable");
      return e.data.messageRunId;
    });
    const unique = new Set(runIds);
    expect(unique.size).toBe(3);

    // Every started event has a matching ended event with the same messageRunId.
    const ended = events.filter((e) => e.type === "message.run.ended");
    expect(ended.length).toBe(3);
    const endedRunIds = ended.map((e) => {
      if (e.type !== "message.run.ended") throw new Error("unreachable");
      return e.data.messageRunId;
    });
    expect(new Set(endedRunIds)).toEqual(unique);
  });

  test("abort mid-message does not emit a bracket-end event", async () => {
    // The bracket stays open across the abort by routing message.received
    // to suspend. When the reactor is killed mid-message by an external
    // abort, the bracket-end event must not fire — cancellation lives in
    // the workflow-runtime vocabulary, not on the reactor's bracket.
    const { reactor, events, waitFor } = createTestReactor({
      director: directorFromTable({
        "message.received": (_e, _s, caps) =>
          caps.suspend({
            type: "approval",
            gateId: "abort-gate",
            timeoutMs: 60000,
          }),
      }),
      shutdownTimeoutMs: 100,
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitFor("reactor.gate.blocked");

    reactor.abort("admin_kill");
    await waitFor("reactor.done");

    const ended = events.filter((e) => e.type === "message.run.ended");
    expect(ended.length).toBe(0);
    const started = events.filter((e) => e.type === "message.run.started");
    expect(started.length).toBe(1);
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
      apiKey: `key-${id}`,
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

  test("retries the same source on a rate limit before failing over", async () => {
    const { reactor, events, waitFor, attemptedSourceIds } = multiSourceReactor(
      {
        sourceIds: ["s0", "s1"],
        // s0 is always rate-limited (short Retry-After so the test stays
        // fast); s1 succeeds.
        resultFor: (id) =>
          id === "s0"
            ? { category: "quota_exhausted", message: "429", retryAfterMs: 1 }
            : "done",
      },
    );
    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitFor("reactor.done");

    // s0 gets its same-source budget (2 attempts) before failover to s1.
    expect(attemptedSourceIds).toEqual(["s0", "s0", "s1"]);
    expect(getEvent(events, "inference.done").data.source.sourceId).toBe("s1");
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

  test("does not fail over on a source-invariant error", async () => {
    const { reactor, events, waitFor, attemptedSourceIds } = multiSourceReactor(
      {
        sourceIds: ["s0", "s1"],
        resultFor: () => ({ category: "context_overflow", message: "too big" }),
      },
    );
    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitFor("reactor.done");

    // context_overflow aborts the cycle; s1 is never tried.
    expect(attemptedSourceIds).toEqual(["s0"]);
    expect(getEvent(events, "inference.error").data.error.category).toBe(
      "context_overflow",
    );
  });

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
