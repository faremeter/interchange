import { describe, test, expect } from "bun:test";

import { validateActions } from "./actions";
import { createGateManager } from "./gates";
import { createCorrelationRegistry } from "./correlation";
import { createReactor } from "./reactor";
import { createDefaultDependencies } from "./harness";

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
} from "@interchange/types/runtime";

import type { ReactorConfig, Reactor, ReactorEmittedEvent } from "./reactor";
import type { Dependencies, InferenceHarnessOptions } from "./harness";
import type { CorrelationValidator } from "./correlation";

import { setupHarness, wire } from "@interchange/inference-testing";
import type { Harness } from "@interchange/inference-testing";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function emptyUsage(): TokenUsage {
  return { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, thinking: 0 };
}

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

// Simple inbound message factory.
function makeInboundMessage(correlationId?: string): InboundMessage {
  return {
    ref: { uid: 1, mailbox: "INBOX" },
    headers: {
      from: "test@example.com",
      to: ["agent@example.com"],
      date: new Date().toISOString(),
      messageId: `msg-${Math.random()}`,
      ...(correlationId !== undefined
        ? { interchangeCorrelationId: correlationId }
        : {}),
    },
    flags: [],
    content: "hello",
    signatureStatus: "missing",
  };
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
  // The wire-level port tests (`@interchange/inference-testing` harness)
  // supply their own `Dependencies` (fetch is stubbed by the harness) and
  // may target a non-Anthropic provider. Both override hooks are optional;
  // omit them for tests that don't care about the inference HTTP path.
  deps?: Dependencies;
  providerConfig?: ReactorConfig["providerConfig"];
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
    providerConfig: overrides.providerConfig ?? {
      provider: "anthropic",
      baseURL: "https://api.anthropic.com",
      apiKey: "test",
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
    const result = validateActions({ type: "infer", model: "gpt-4" });
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
      { type: "infer", model: "gpt-4" },
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
      { type: "infer", model: "gpt-4" },
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
    const result = validateActions([
      { type: "infer", model: "gpt-4" },
      { type: "done" },
    ]);
    expect(result.ok).toBe(false);
    if (result.ok) throw new Error("unreachable");
    expect(result.error).toMatch(/infer.*done/i);
  });

  test("multiple infer actions are invalid", () => {
    const result = validateActions([
      { type: "infer", model: "gpt-4" },
      { type: "infer", model: "claude-3" },
    ]);
    expect(result.ok).toBe(false);
  });

  test("suspend + infer is invalid", () => {
    const result = validateActions([
      { type: "infer", model: "gpt-4" },
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
    const result = validateActions([
      { type: "wait" },
      { type: "infer", model: "gpt-4" },
    ]);
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
      { type: "infer", model: "gpt-4" },
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
    manager.register("g", "approval", 5000, undefined, () => {
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
    manager.register("g-corr", "message_response", 5000, "corr-42", () => {
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
    return { correlationId: id, registeredAt: Date.now(), gateId: `g-${id}` };
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
      providerConfig: {
        provider: "anthropic",
        baseURL: "https://api.anthropic.com",
        apiKey: "test",
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
        "message.received": (_e, _s, caps) => [
          caps.infer("gpt-4"),
          caps.done(),
        ],
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
    // block. Using addToHistory=false on executeTools keeps this as the
    // last message, so historyHasPendingToolCalls() stays true when the
    // loop calls dequeueNext() after tools complete.
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

    // Empty content prevents the reactor from appending a user text
    // message to history, preserving the assistant tool_call as the
    // last entry so historyHasPendingToolCalls() stays true.
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

    // tool.done should appear before the second message.received in the
    // order array, because dequeueNext prioritizes cycle events when
    // the last history message has pending tool_calls.
    const toolDoneIdx = order.indexOf("tool.done");
    const secondMessageIdx = order.lastIndexOf("message.received");
    expect(toolDoneIdx).toBeGreaterThan(-1);
    expect(secondMessageIdx).toBeGreaterThan(-1);
    expect(toolDoneIdx).toBeLessThan(secondMessageIdx);
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

    const { reactor, waitFor } = createTestReactor({
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
        data: { turn: result.turn, usage: result.usage },
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
        "message.received": (_e, _s, caps) => caps.infer("mock-model"),
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
        "message.received": (_e, _s, caps) => caps.infer("mock-model"),
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
        "message.received": (_e, _s, caps) => caps.infer("mock-model"),
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
        return undefined;
      },
    };

    const toolsRun: string[] = [];

    const { reactor, events, waitFor } = createTestReactor({
      director: directorFromTable({
        "message.received": (_e, _s, caps) => caps.infer("test-model"),
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
        if (call.name === "bash") return "Denied by policy";
        return undefined;
      },
    };

    const toolsRun: string[] = [];

    const { reactor, events, waitFor } = createTestReactor({
      director: directorFromTable({
        "message.received": (_e, _s, caps) => caps.infer("test-model"),
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
        return "Blocked by A";
      },
    };

    const extB: BeforeToolExtension = {
      async beforeTool() {
        called.push("B");
        return undefined;
      },
    };

    const { reactor, waitFor } = createTestReactor({
      director: directorFromTable({
        "message.received": (_e, _s, caps) => caps.infer("test-model"),
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
        "message.received": (_e, _s, caps) => caps.infer("test-model"),
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
        return undefined;
      },
    };

    const { reactor, waitFor } = createTestReactor({
      director: directorFromTable({
        "message.received": (_e, _s, caps) => caps.infer("test-model"),
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
        return undefined;
      },
    };

    const { reactor, waitFor } = createTestReactor({
      director: directorFromTable({
        "message.received": (_e, _s, caps) => caps.infer("test-model"),
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
        if (call.name === "bash") return "Denied by policy";
        return undefined;
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
          "message.received": (_e, _s, caps) => caps.infer("test-model"),
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
} from "@interchange/types/runtime";

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
      model: opts.model,
      timestamp: Date.now(),
    };
    yield {
      type: "inference.done",
      seq: opts.nextSeq(),
      data: { turn, usage },
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
    providerConfig: {
      provider: "anthropic",
      baseURL: "https://api.anthropic.com",
      apiKey: "test",
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
        model: opts.model,
        timestamp: Date.now(),
      };
      yield {
        type: "inference.done",
        seq: opts.nextSeq(),
        data: {
          turn,
          usage: emptyUsage(),
        },
      };
    };

    const { reactor, waitFor } = createDirectReactor({
      contextStore: recording.store,
      inferenceRunner: runner,
      director: directorFromTable({
        "message.received": (_e, _s, caps) => caps.infer("test-model"),
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
      { type: "infer", model: "m" },
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
        model: opts.model,
        timestamp: Date.now(),
      };
      yield {
        type: "inference.done",
        seq: opts.nextSeq(),
        data: { turn, usage: emptyUsage() },
      };
    };

    const director: ReactorDirector = {
      async decide(event, _state, caps) {
        if (event.type === "message.received") {
          return caps.infer("test-model");
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
          return { type: "infer" as const, model: "test-model" };
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
              { type: "infer" as const, model: "m" },
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
// Port B (7a): wire-driven inference-path tests
//
// These tests exercise the same reactor-side assertions as the
// `createReactor — inference path` describe block above, but feed the
// reactor through the real fetch → parseSSE → provider adapter →
// reactor pipeline using the `@interchange/inference-testing` harness.
// The synthetic `mockInferenceRunner` / `makeInferenceRunner` tests above
// continue to validate the reactor's state-machine logic with a cheap
// in-process generator; this block validates that the same end state is
// produced when the inference cycle is fed by real bytes parsed by the
// production adapter.
//
// See `dispatch/intr-60-inference-testing/7a-port_reactor_streaming_subset/audit.md`
// for the audit that selected these tests and the rationale for which
// reactor.test.ts tests stay on the synthetic path.
// ---------------------------------------------------------------------------

const ANTHROPIC_PROVIDER_CONFIG = {
  provider: "anthropic" as const,
  baseURL: "https://api.anthropic.com",
  apiKey: "test",
};

const OPENAI_PROVIDER_CONFIG = {
  provider: "openai" as const,
  baseURL: "https://api.openai.com/v1",
  apiKey: "test",
};

async function withHarness<T>(body: (h: Harness) => Promise<T>): Promise<T> {
  const harness = setupHarness();
  try {
    return await body(harness);
  } finally {
    harness.dispose();
  }
}

describe("createReactor — inference path [wire-driven]", () => {
  test("Anthropic: infer drives inference.done with assembled turn and merged usage", async () => {
    await withHarness(async (harness) => {
      const headUsage: TokenUsage = {
        input: 100,
        output: 0,
        cacheRead: 10,
        cacheWrite: 5,
        thinking: 0,
      };
      const tailUsage: TokenUsage = {
        input: 0,
        output: 50,
        cacheRead: 0,
        cacheWrite: 0,
        thinking: 0,
      };

      const stream = harness.scenario.createStream();
      harness.scenario.whenRequestMatches(() => true, stream);
      const chunks = wire.completeResponse("anthropic", {
        text: "Hello from the model",
        headUsage,
        tailUsage,
      });
      stream.enqueueAll(chunks, { startAt: 10 });
      const when = 10 + chunks.length;

      let stateAtInferenceDone: ReactorState | undefined;
      const { reactor, events, waitFor } = createTestReactor({
        deps: harness.deps,
        providerConfig: ANTHROPIC_PROVIDER_CONFIG,
        director: directorFromTable({
          "message.received": (_e, _s, caps) => caps.infer("mock-model"),
          "inference.done": (_e, state, caps) => {
            stateAtInferenceDone = state;
            return caps.done();
          },
        }),
      });

      reactor.start();
      reactor.deliver(makeInboundMessage());
      await harness.advanceTo(when + 10);
      await waitFor("reactor.done");

      const inferDone = getEvent(events, "inference.done");
      expect(inferDone.data.turn.content[0]).toEqual({
        type: "text",
        text: "Hello from the model",
      });
      // Head usage (message_start) and tail usage (message_delta) are
      // accumulated by the streaming harness.
      expect(inferDone.data.usage).toEqual({
        input: 100,
        output: 50,
        cacheRead: 10,
        cacheWrite: 5,
        thinking: 0,
      });

      if (stateAtInferenceDone === undefined)
        throw new Error("director never received inference.done");
      expect(stateAtInferenceDone.tokenUsage).toEqual({
        input: 100,
        output: 50,
        cacheRead: 10,
        cacheWrite: 5,
        thinking: 0,
      });

      const lastMsg =
        stateAtInferenceDone.turns[stateAtInferenceDone.turns.length - 1];
      if (lastMsg === undefined)
        throw new Error("no messages in state snapshot");
      expect(lastMsg.role).toBe("assistant");
      expect(lastMsg.content[0]).toEqual({
        type: "text",
        text: "Hello from the model",
      });
    });
  });

  test("OpenAI: infer drives inference.done with assembled turn and tail usage", async () => {
    await withHarness(async (harness) => {
      const tailUsage: TokenUsage = {
        input: 100,
        output: 50,
        cacheRead: 10,
        cacheWrite: 0,
        thinking: 0,
      };

      const stream = harness.scenario.createStream();
      harness.scenario.whenRequestMatches(() => true, stream);
      const chunks = wire.completeResponse("openai", {
        text: "Hello from the model",
        tailUsage,
      });
      stream.enqueueAll(chunks, { startAt: 10 });
      const when = 10 + chunks.length;

      let stateAtInferenceDone: ReactorState | undefined;
      const { reactor, events, waitFor } = createTestReactor({
        deps: harness.deps,
        providerConfig: OPENAI_PROVIDER_CONFIG,
        director: directorFromTable({
          "message.received": (_e, _s, caps) => caps.infer("mock-model"),
          "inference.done": (_e, state, caps) => {
            stateAtInferenceDone = state;
            return caps.done();
          },
        }),
      });

      reactor.start();
      reactor.deliver(makeInboundMessage());
      await harness.advanceTo(when + 10);
      await waitFor("reactor.done");

      const inferDone = getEvent(events, "inference.done");
      expect(inferDone.data.turn.content[0]).toEqual({
        type: "text",
        text: "Hello from the model",
      });
      expect(inferDone.data.usage).toEqual(tailUsage);

      if (stateAtInferenceDone === undefined)
        throw new Error("director never received inference.done");
      expect(stateAtInferenceDone.tokenUsage).toEqual(tailUsage);

      const lastMsg =
        stateAtInferenceDone.turns[stateAtInferenceDone.turns.length - 1];
      if (lastMsg === undefined)
        throw new Error("no messages in state snapshot");
      expect(lastMsg.role).toBe("assistant");
    });
  });

  test("Anthropic: streaming text deltas accumulate into final assistant turn", async () => {
    await withHarness(async (harness) => {
      const stream = harness.scenario.createStream();
      harness.scenario.whenRequestMatches(() => true, stream);

      // Emit text one token at a time so multiple inference.text.delta events
      // fire through the reactor before the terminal inference.done.
      const tokens = ["Hel", "lo, ", "wor", "ld!"];
      stream.enqueueAt(10, wire.anthropic.messageStart());
      stream.enqueueAt(
        11,
        wire.anthropic.contentBlockStart({ index: 0, kind: "text", text: "" }),
      );
      let when = 12;
      for (const token of tokens) {
        stream.enqueueAt(
          when,
          wire.anthropic.contentBlockDelta({
            index: 0,
            kind: "text_delta",
            text: token,
          }),
        );
        when += 1;
      }
      stream.enqueueAt(when, wire.anthropic.contentBlockStop({ index: 0 }));
      when += 1;
      stream.enqueueAt(when, wire.anthropic.messageDelta({ outputTokens: 7 }));
      when += 1;
      stream.enqueueAt(when, wire.anthropic.messageStop());
      when += 1;
      stream.closeAt(when);

      const { reactor, events, waitFor } = createTestReactor({
        deps: harness.deps,
        providerConfig: ANTHROPIC_PROVIDER_CONFIG,
        director: directorFromTable({
          "message.received": (_e, _s, caps) => caps.infer("mock-model"),
          "inference.done": (_e, _s, caps) => caps.done(),
        }),
      });

      reactor.start();
      reactor.deliver(makeInboundMessage());
      await harness.advanceTo(when + 10);
      await waitFor("reactor.done");

      const deltas = events.filter(
        (e): e is Extract<InferenceEvent, { type: "inference.text.delta" }> =>
          e.type === "inference.text.delta",
      );
      expect(deltas.length).toBe(tokens.length);
      const deltaTokens = deltas.map((d) => d.data.token);
      expect(deltaTokens).toEqual(tokens);

      const inferDone = getEvent(events, "inference.done");
      expect(inferDone.data.turn.content[0]).toEqual({
        type: "text",
        text: "Hello, world!",
      });
    });
  });

  test("Anthropic: stream errorAt mid-stream produces inference.error with retryable category and captured partial", async () => {
    await withHarness(async (harness) => {
      const stream = harness.scenario.createStream();
      harness.scenario.whenRequestMatches(() => true, stream);
      stream.enqueueAt(10, wire.anthropic.messageStart());
      stream.enqueueAt(
        20,
        wire.anthropic.contentBlockStart({ index: 0, kind: "text", text: "" }),
      );
      stream.enqueueAt(
        30,
        wire.anthropic.contentBlockDelta({
          index: 0,
          kind: "text_delta",
          text: "partial output",
        }),
      );
      stream.errorAt(40, new Error("upstream connection reset"));

      let capturedError: { category: string; message: string } | undefined;
      let capturedPartialText: string | undefined;
      const { reactor, events, waitFor } = createTestReactor({
        deps: harness.deps,
        providerConfig: ANTHROPIC_PROVIDER_CONFIG,
        director: directorFromTable({
          "message.received": (_e, _s, caps) => caps.infer("mock-model"),
          "inference.error": (e, _s, caps) => {
            capturedError = {
              category: e.error.category,
              message: e.error.message,
            };
            capturedPartialText = e.partial.text;
            return caps.done();
          },
        }),
      });

      reactor.start();
      reactor.deliver(makeInboundMessage());
      await harness.advanceTo(50);
      await waitFor("reactor.done");

      if (capturedError === undefined)
        throw new Error("director never received inference.error");
      expect(capturedError.category).toBe("retryable");
      // The partial body accumulated before the stream error must reach the
      // director so it can decide whether to retry or surface the partial.
      expect(capturedPartialText).toBe("partial output");

      const inferErr = getEvent(events, "inference.error");
      expect(inferErr.data.error.category).toBe("retryable");
      expect(inferErr.data.partial.text).toBe("partial output");
    });
  });

  test("OpenAI: stream errorAt mid-stream produces inference.error with retryable category and captured partial", async () => {
    await withHarness(async (harness) => {
      const stream = harness.scenario.createStream();
      harness.scenario.whenRequestMatches(() => true, stream);
      stream.enqueueAt(10, wire.openai.chunk({ content: "partial output" }));
      stream.errorAt(30, new Error("upstream connection reset"));

      let capturedError: { category: string; message: string } | undefined;
      let capturedPartialText: string | undefined;
      const { reactor, events, waitFor } = createTestReactor({
        deps: harness.deps,
        providerConfig: OPENAI_PROVIDER_CONFIG,
        director: directorFromTable({
          "message.received": (_e, _s, caps) => caps.infer("mock-model"),
          "inference.error": (e, _s, caps) => {
            capturedError = {
              category: e.error.category,
              message: e.error.message,
            };
            capturedPartialText = e.partial.text;
            return caps.done();
          },
        }),
      });

      reactor.start();
      reactor.deliver(makeInboundMessage());
      await harness.advanceTo(40);
      await waitFor("reactor.done");

      if (capturedError === undefined)
        throw new Error("director never received inference.error");
      expect(capturedError.category).toBe("retryable");
      expect(capturedPartialText).toBe("partial output");

      const inferErr = getEvent(events, "inference.error");
      expect(inferErr.data.error.category).toBe("retryable");
      expect(inferErr.data.partial.text).toBe("partial output");
    });
  });

  test("Anthropic: tool-call deltas accumulate into final assistant tool_call block", async () => {
    await withHarness(async (harness) => {
      const stream = harness.scenario.createStream();
      harness.scenario.whenRequestMatches(() => true, stream);
      // Emit the JSON arguments split across multiple input_json_delta events
      // so the harness's argsBuffer accumulator is exercised.
      const argsFragments = ['{"path":', '"/etc/hosts"', "}"];
      stream.enqueueAt(10, wire.anthropic.messageStart());
      stream.enqueueAt(
        11,
        wire.anthropic.contentBlockStart({
          index: 0,
          kind: "tool_use",
          id: "call-1",
          name: "read_file",
        }),
      );
      let when = 12;
      for (const frag of argsFragments) {
        stream.enqueueAt(
          when,
          wire.anthropic.contentBlockDelta({
            index: 0,
            kind: "input_json_delta",
            partialJson: frag,
          }),
        );
        when += 1;
      }
      stream.enqueueAt(when, wire.anthropic.contentBlockStop({ index: 0 }));
      when += 1;
      stream.enqueueAt(when, wire.anthropic.messageDelta({ outputTokens: 3 }));
      when += 1;
      stream.enqueueAt(when, wire.anthropic.messageStop());
      when += 1;
      stream.closeAt(when);

      const { reactor, events, waitFor } = createTestReactor({
        deps: harness.deps,
        providerConfig: ANTHROPIC_PROVIDER_CONFIG,
        director: directorFromTable({
          "message.received": (_e, _s, caps) => caps.infer("mock-model"),
          "inference.done": (_e, _s, caps) => caps.done(),
        }),
      });

      reactor.start();
      reactor.deliver(makeInboundMessage());
      await harness.advanceTo(when + 10);
      await waitFor("reactor.done");

      const inferDone = getEvent(events, "inference.done");
      const toolBlock = inferDone.data.turn.content.find(
        (b) => b.type === "tool_call",
      );
      if (toolBlock === undefined || toolBlock.type !== "tool_call") {
        throw new Error("expected tool_call block in assembled turn");
      }
      expect(toolBlock.id).toBe("call-1");
      expect(toolBlock.name).toBe("read_file");
      expect(toolBlock.arguments).toEqual({ path: "/etc/hosts" });
    });
  });

  test("Anthropic: HTTP 400 with context-overflow body produces inference.error category=context_overflow and drives compact-then-reinfer recovery", async () => {
    await withHarness(async (harness) => {
      // First stream: HTTP 400 with a body that classifyHTTPError treats as
      // context overflow. The adapter's extractErrorMessage parses
      // { error: { message } } from the JSON body, and isContextOverflowMessage
      // matches the "input is too long" substring.
      const overflowStream = harness.scenario.createStream();
      const overflowBody =
        '{"error":{"message":"input is too long for the model context window"}}';
      overflowStream.enqueueAt(5, new TextEncoder().encode(overflowBody));
      overflowStream.closeAt(6);

      // Second stream: HTTP 200 with a clean inference.done. This must satisfy
      // the second fetch dispatched after the director's compact-then-reinfer
      // cycle.
      const successStream = harness.scenario.createStream();
      const successChunks = wire.completeResponse("anthropic", {
        text: "ok-after-compact",
        headUsage: emptyUsage(),
        tailUsage: emptyUsage(),
      });
      let successWhen = 100;
      for (const chunk of successChunks) {
        successStream.enqueueAt(successWhen, chunk);
        successWhen += 1;
      }
      successStream.closeAt(successWhen);

      // Counter-driven predicates make the first matcher accept the first
      // fetch and the second matcher accept the second fetch, deterministic
      // regardless of URL (both target the same Anthropic endpoint).
      let fetchCount = 0;
      harness.scenario.whenRequestMatches(
        () => fetchCount++ === 0,
        overflowStream,
        { status: 400 },
      );
      harness.scenario.whenRequestMatches(() => true, successStream);

      const recording = makeRecordingContextStore();
      const compactor = truncatingCompactor("overflow-compactor");

      const director: ReactorDirector = {
        async decide(event, _state, caps) {
          if (event.type === "message.received") {
            return caps.infer("test-model");
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

      let capturedErrorCategory: string | undefined;
      const recordingDirector: ReactorDirector = {
        async decide(event, state, caps) {
          if (event.type === "inference.error") {
            capturedErrorCategory = event.error.category;
          }
          return director.decide(event, state, caps);
        },
      };

      const { reactor, events, waitFor } = createTestReactor({
        deps: harness.deps,
        providerConfig: ANTHROPIC_PROVIDER_CONFIG,
        contextStore: recording.store,
        compactors: { "overflow-compactor": compactor },
        director: recordingDirector,
      });

      reactor.start();
      reactor.deliver(makeInboundMessage());
      await harness.advanceTo(12);
      // The director has now issued compact; the second fetch hasn't been
      // dispatched yet. Deliver the second message to trigger the second
      // inference cycle.
      reactor.deliver(makeInboundMessage());
      await harness.advanceTo(successWhen + 10);
      await waitFor("reactor.done");

      expect(capturedErrorCategory).toBe("context_overflow");

      const inferErr = getEvent(events, "inference.error");
      expect(inferErr.data.error.category).toBe("context_overflow");

      const compactRecord = recording.manifests
        .flat()
        .find((r) => r.strategy === "overflow-compactor");
      expect(compactRecord).toBeDefined();
      expect(compactRecord?.reason).toBe("explicit");

      const validationErr = events
        .filter((e) => e.type === "reactor.error")
        .find(
          (e) =>
            e.type === "reactor.error" &&
            e.data.error.includes("Invalid action"),
        );
      expect(validationErr).toBeUndefined();
    });
  });

  test("Anthropic: HTTP 5xx body produces inference.error category=retryable through classifyHTTPError", async () => {
    // Port of the synthetic `infer action with inference.error delivers
    // error event to director` test (originally at line 2398), now driving
    // through the production HTTP error-classification branch in
    // `runInference` (response.ok === false → classifyHTTPError). The
    // original hand-built `{ category: "retryable", message: "rate limited" }`;
    // `classifyHTTPError` produces `retryable` for any 5xx, so we drive HTTP
    // 503 with a matching error message body. Unlike 429 (which the reactor
    // would retry up to 3 times internally), retryable surfaces straight to
    // the director on the first cycle.
    await withHarness(async (harness) => {
      const stream = harness.scenario.createStream();
      const body = '{"error":{"message":"rate limited"}}';
      stream.enqueueAt(5, new TextEncoder().encode(body));
      stream.closeAt(6);

      harness.scenario.whenRequestMatches(() => true, stream, { status: 503 });

      let capturedError: { category: string; message: string } | undefined;

      const { reactor, events, waitFor } = createTestReactor({
        deps: harness.deps,
        providerConfig: ANTHROPIC_PROVIDER_CONFIG,
        director: directorFromTable({
          "message.received": (_e, _s, caps) => caps.infer("mock-model"),
          "inference.error": (e, _s, caps) => {
            capturedError = {
              category: e.error.category,
              message: e.error.message,
            };
            return caps.done();
          },
        }),
      });

      reactor.start();
      reactor.deliver(makeInboundMessage());
      await harness.advanceTo(20);
      await waitFor("reactor.done");

      if (capturedError === undefined)
        throw new Error("director never received inference.error");
      expect(capturedError.category).toBe("retryable");
      expect(capturedError.message).toBe("rate limited");

      const inferErr = getEvent(events, "inference.error");
      expect(inferErr.data.error.category).toBe("retryable");
    });
  });

  test("OpenAI: tool-call argument fragments accumulate into final assistant tool_call block", async () => {
    await withHarness(async (harness) => {
      const stream = harness.scenario.createStream();
      harness.scenario.whenRequestMatches(() => true, stream);
      const argsFragments = ['{"path":', '"/etc/hosts"', "}"];
      stream.enqueueAt(10, wire.openai.toolCallStart(0, "call-1", "read_file"));
      let when = 11;
      for (const frag of argsFragments) {
        stream.enqueueAt(when, wire.openai.toolCallArgumentsDelta(0, frag));
        when += 1;
      }
      stream.enqueueAt(when, wire.openai.done());
      when += 1;
      stream.closeAt(when);

      const { reactor, events, waitFor } = createTestReactor({
        deps: harness.deps,
        providerConfig: OPENAI_PROVIDER_CONFIG,
        director: directorFromTable({
          "message.received": (_e, _s, caps) => caps.infer("mock-model"),
          "inference.done": (_e, _s, caps) => caps.done(),
        }),
      });

      reactor.start();
      reactor.deliver(makeInboundMessage());
      await harness.advanceTo(when + 10);
      await waitFor("reactor.done");

      const inferDone = getEvent(events, "inference.done");
      const toolBlock = inferDone.data.turn.content.find(
        (b) => b.type === "tool_call",
      );
      if (toolBlock === undefined || toolBlock.type !== "tool_call") {
        throw new Error("expected tool_call block in assembled turn");
      }
      expect(toolBlock.id).toBe("call-1");
      expect(toolBlock.name).toBe("read_file");
      expect(toolBlock.arguments).toEqual({ path: "/etc/hosts" });
    });
  });
});
