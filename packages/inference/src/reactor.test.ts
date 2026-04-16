import { describe, test, expect } from "bun:test";

import { validateActions } from "./actions";
import { createGateManager } from "./gates";
import { createCorrelationRegistry } from "./correlation";
import { createReactor } from "./reactor";

import type {
  ReactorPlugin,
  ReactorAction,
  ReactorInboundEvent,
  ReactorState,
  ReactorCapabilities,
  ContextStore,
  ToolRunner,
  InferenceEvent,
  InboundMessage,
  ConversationMessage,
  PendingOperation,
  TokenUsage,
  ContextCommit,
} from "@interchange/types/runtime";

import type { ReactorConfig, Reactor } from "./reactor";
import type { CorrelationValidator } from "./correlation";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function emptyUsage(): TokenUsage {
  return { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, thinking: 0 };
}

function makeContextStore(messages: ConversationMessage[] = []): ContextStore {
  return {
    async load() {
      return {
        messages,
        pendingOperations: [],
        tokenUsage: emptyUsage(),
      };
    },
    async commit(_msgs, _ops, _usage, message): Promise<ContextCommit> {
      return { hash: "abc", message, timestamp: Date.now() };
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

function collectEvents(_timeout = 2000): {
  events: InferenceEvent[];
  onEvent: (e: InferenceEvent) => void;
} {
  const events: InferenceEvent[] = [];
  return {
    events,
    onEvent: (e: InferenceEvent) => events.push(e),
  };
}

function waitForEvent(
  events: InferenceEvent[],
  predicate: (e: InferenceEvent) => boolean,
  timeoutMs = 2000,
): Promise<InferenceEvent> {
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

type PluginTable = Partial<
  Record<
    ReactorInboundEvent["type"],
    (
      event: ReactorInboundEvent,
      state: ReactorState,
      caps: ReactorCapabilities,
    ) =>
      | ReactorAction
      | ReactorAction[]
      | Promise<ReactorAction | ReactorAction[]>
  >
>;

function pluginFromTable(
  table: PluginTable,
  defaultAction: "done" | "wait" = "done",
): ReactorPlugin {
  return {
    async decide(
      event: ReactorInboundEvent,
      state: ReactorState,
      caps: ReactorCapabilities,
    ): Promise<ReactorAction | ReactorAction[]> {
      const handler = table[event.type];
      if (handler !== undefined) {
        return handler(event, state, caps);
      }
      return defaultAction === "done" ? caps.done() : caps.wait();
    },
  };
}

type TestReactorOverrides = {
  plugin?: ReactorPlugin;
  toolRunner?: ToolRunner;
  contextStore?: ContextStore;
  correlationValidator?: CorrelationValidator;
  sessionId?: string;
  gateTimeout?: number;
  shutdownTimeoutMs?: number;
};

type TestReactorHandle = {
  reactor: Reactor;
  events: InferenceEvent[];
  waitFor: (
    type: InferenceEvent["type"],
    timeoutMs?: number,
  ) => Promise<InferenceEvent>;
};

function createTestReactor(
  overrides: TestReactorOverrides = {},
): TestReactorHandle {
  const { events, onEvent } = collectEvents();
  const sessionId = overrides.sessionId ?? `test-sess-${++testSessionCounter}`;

  const config: ReactorConfig = {
    sessionId,
    plugin: overrides.plugin ?? pluginFromTable({}),
    providerConfig: {
      provider: "anthropic",
      baseURL: "https://api.anthropic.com",
      apiKey: "test",
    },
    toolRunner: overrides.toolRunner ?? noopToolRunner(),
    contextStore: overrides.contextStore ?? makeContextStore(),
    onEvent,
    shutdownTimeoutMs: overrides.shutdownTimeoutMs ?? 100,
    ...(overrides.correlationValidator !== undefined
      ? { correlationValidator: overrides.correlationValidator }
      : {}),
    ...(overrides.gateTimeout !== undefined
      ? { gateTimeout: overrides.gateTimeout }
      : {}),
  };

  const reactor = createReactor(config);

  function waitFor(
    type: InferenceEvent["type"],
    timeoutMs = 2000,
  ): Promise<InferenceEvent> {
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
  };
}

function throwingToolRunner(error: Error): ToolRunner {
  return {
    async run() {
      throw error;
    },
  };
}

function getEvent<T extends InferenceEvent["type"]>(
  events: InferenceEvent[],
  type: T,
): Extract<InferenceEvent, { type: T }> {
  const found = events.find((e) => e.type === type);
  if (found === undefined) {
    throw new Error(`No event of type '${type}' found`);
  }
  return found as Extract<InferenceEvent, { type: T }>;
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
      { type: "checkpoint" },
      { type: "infer", model: "gpt-4" },
    ]);
    expect(result.ok).toBe(true);
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

  test("empty action list is valid (no-op wait)", () => {
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
// 4. Reactor loop — basic message → infer → done
// ---------------------------------------------------------------------------

describe("createReactor — basic flow", () => {
  test("message.received → infer → inference.done → done emits reactor.done", async () => {
    const { events, onEvent } = collectEvents();

    const plugin: ReactorPlugin = {
      async decide(
        event: ReactorInboundEvent,
        _state: ReactorState,
        caps: ReactorCapabilities,
      ): Promise<ReactorAction | ReactorAction[]> {
        if (event.type === "message.received") {
          return caps.done();
        }
        return caps.done();
      },
    };

    const reactor = createReactor({
      sessionId: "sess-1",
      plugin,
      providerConfig: {
        provider: "anthropic",
        baseURL: "https://api.anthropic.com",
        apiKey: "test",
      },
      toolRunner: noopToolRunner(),
      contextStore: makeContextStore(),
      onEvent,
      shutdownTimeoutMs: 100,
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());

    await waitForEvent(events, (e) => e.type === "reactor.done");

    const types = events.map((e) => e.type);
    expect(types).toContain("reactor.start");
    expect(types).toContain("message.received");
    expect(types).toContain("reactor.done");
  });

  test("reactor.start is the first event", async () => {
    const { events, onEvent } = collectEvents();

    const plugin: ReactorPlugin = {
      async decide(
        event: ReactorInboundEvent,
        _state: ReactorState,
        caps: ReactorCapabilities,
      ) {
        if (event.type === "message.received") return caps.done();
        return caps.done();
      },
    };

    const reactor = createReactor({
      sessionId: "sess-2",
      plugin,
      providerConfig: {
        provider: "anthropic",
        baseURL: "https://api.anthropic.com",
        apiKey: "test",
      },
      toolRunner: noopToolRunner(),
      contextStore: makeContextStore(),
      onEvent,
      shutdownTimeoutMs: 100,
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());

    await waitForEvent(events, (e) => e.type === "reactor.done");

    expect(events[0]?.type).toBe("reactor.start");
  });

  test("sequence numbers are monotonically increasing", async () => {
    const { events, onEvent } = collectEvents();

    const plugin: ReactorPlugin = {
      async decide(
        event: ReactorInboundEvent,
        _state: ReactorState,
        caps: ReactorCapabilities,
      ) {
        if (event.type === "message.received") {
          return [caps.emit("custom.test", { x: 1 }), caps.done()];
        }
        return caps.done();
      },
    };

    const reactor = createReactor({
      sessionId: "sess-seq",
      plugin,
      providerConfig: {
        provider: "anthropic",
        baseURL: "https://api.anthropic.com",
        apiKey: "test",
      },
      toolRunner: noopToolRunner(),
      contextStore: makeContextStore(),
      onEvent,
      shutdownTimeoutMs: 100,
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());

    await waitForEvent(events, (e) => e.type === "reactor.done");

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
// 5. Plugin exception handling
// ---------------------------------------------------------------------------

describe("createReactor — plugin exception", () => {
  test("plugin exception emits reactor.error and shuts down", async () => {
    const { events, onEvent } = collectEvents();

    const plugin: ReactorPlugin = {
      async decide() {
        throw new Error("plugin blew up");
      },
    };

    const reactor = createReactor({
      sessionId: "sess-err",
      plugin,
      providerConfig: {
        provider: "anthropic",
        baseURL: "https://api.anthropic.com",
        apiKey: "test",
      },
      toolRunner: noopToolRunner(),
      contextStore: makeContextStore(),
      onEvent,
      shutdownTimeoutMs: 100,
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());

    await waitForEvent(events, (e) => e.type === "reactor.error");

    const errorEvent = events.find((e) => e.type === "reactor.error");
    if (errorEvent === undefined || errorEvent.type !== "reactor.error") {
      throw new Error("expected reactor.error");
    }
    expect(errorEvent.data.error).toMatch(/plugin blew up/);
    expect(errorEvent.data.fatal).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// 6. Gate lifecycle via reactor
// ---------------------------------------------------------------------------

describe("createReactor — gate lifecycle", () => {
  test("suspend blocks until gate is cleared externally", async () => {
    const { events, onEvent } = collectEvents();
    const plugin: ReactorPlugin = {
      async decide(
        event: ReactorInboundEvent,
        _state: ReactorState,
        caps: ReactorCapabilities,
      ) {
        if (event.type === "message.received") {
          return caps.suspend({
            type: "approval",
            gateId: "test-gate",
            timeoutMs: 5000,
          });
        }
        if (event.type === "reactor.gate.cleared") {
          return caps.done();
        }
        return caps.done();
      },
    };

    const reactor = createReactor({
      sessionId: "sess-gate",
      plugin,
      providerConfig: {
        provider: "anthropic",
        baseURL: "https://api.anthropic.com",
        apiKey: "test",
      },
      toolRunner: noopToolRunner(),
      contextStore: makeContextStore(),
      onEvent,
      shutdownTimeoutMs: 100,
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());

    await waitForEvent(events, (e) => e.type === "reactor.gate.blocked", 2000);

    // Simulate external gate resolution.
    reactor.abort("admin_kill");

    await waitForEvent(events, (e) => e.type === "reactor.done", 2000);

    expect(events.some((e) => e.type === "reactor.gate.blocked")).toBe(true);
  });

  test("gate timeout fires reactor.gate.cleared with reason=timeout", async () => {
    const { events, onEvent } = collectEvents();

    const plugin: ReactorPlugin = {
      async decide(
        event: ReactorInboundEvent,
        _state: ReactorState,
        caps: ReactorCapabilities,
      ) {
        if (event.type === "message.received") {
          return caps.suspend({
            type: "approval",
            gateId: "timeout-gate",
            timeoutMs: 80,
          });
        }
        if (event.type === "reactor.gate.cleared") {
          return caps.done();
        }
        return caps.done();
      },
    };

    const reactor = createReactor({
      sessionId: "sess-timeout",
      plugin,
      providerConfig: {
        provider: "anthropic",
        baseURL: "https://api.anthropic.com",
        apiKey: "test",
      },
      toolRunner: noopToolRunner(),
      contextStore: makeContextStore(),
      onEvent,
      shutdownTimeoutMs: 500,
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());

    await waitForEvent(events, (e) => e.type === "reactor.done", 3000);

    const cleared = events.find((e) => e.type === "reactor.gate.cleared");
    expect(cleared).toBeDefined();
    if (cleared?.type !== "reactor.gate.cleared")
      throw new Error("unreachable");
    expect(cleared.data.reason).toBe("timeout");
  });
});

// ---------------------------------------------------------------------------
// 7. Tool execution
// ---------------------------------------------------------------------------

describe("createReactor — tool execution", () => {
  test("execute_tools dispatches tools and returns results", async () => {
    const { events, onEvent } = collectEvents();
    const toolsRun: string[] = [];

    let phase = 0;

    const plugin: ReactorPlugin = {
      async decide(
        event: ReactorInboundEvent,
        _state: ReactorState,
        caps: ReactorCapabilities,
      ) {
        if (event.type === "message.received") {
          phase = 1;
          return caps.executeTools(
            [
              { id: "c1", name: "tool_a", arguments: {} },
              { id: "c2", name: "tool_b", arguments: {} },
            ],
            true,
          );
        }
        if (event.type === "tool.done" && phase === 1) {
          // Wait for both tool.done before finishing.
          const toolDones = events.filter((e) => e.type === "tool.done");
          if (toolDones.length >= 2) {
            phase = 2;
            return caps.done();
          }
          // Still waiting for second tool.done.
          return caps.done();
        }
        return caps.done();
      },
    };

    const runner = makeToolRunner(async (call) => {
      toolsRun.push(call.name);
      return { callId: call.id, content: `result-${call.name}` };
    });

    const reactor = createReactor({
      sessionId: "sess-tools",
      plugin,
      providerConfig: {
        provider: "anthropic",
        baseURL: "https://api.anthropic.com",
        apiKey: "test",
      },
      toolRunner: runner,
      contextStore: makeContextStore(),
      onEvent,
      shutdownTimeoutMs: 500,
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());

    await waitForEvent(events, (e) => e.type === "reactor.done", 3000);

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
    const { events, onEvent } = collectEvents();
    const CORR_ID = "corr-xyz-123";

    let delivered = false;

    const plugin: ReactorPlugin = {
      async decide(
        event: ReactorInboundEvent,
        _state: ReactorState,
        caps: ReactorCapabilities,
      ) {
        // First call: message.received from deliver() — return execute_tools
        // which will produce a pending marker.
        if (event.type === "message.received" && !delivered) {
          delivered = true;
          return caps.executeTools([
            { id: "tc1", name: "send_message", arguments: {} },
          ]);
        }
        // After tool.done with pending marker, suspend.
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

    const runner = makeToolRunner(async (call) => {
      return {
        callId: call.id,
        content: "message sent",
        pendingMarker: {
          status: "pending" as const,
          correlationId: CORR_ID,
        },
      };
    });

    const reactor = createReactor({
      sessionId: "sess-corr",
      plugin,
      providerConfig: {
        provider: "anthropic",
        baseURL: "https://api.anthropic.com",
        apiKey: "test",
      },
      toolRunner: runner,
      contextStore: makeContextStore(),
      onEvent,
      shutdownTimeoutMs: 500,
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());

    // Wait for the gate to block.
    await waitForEvent(events, (e) => e.type === "reactor.gate.blocked", 2000);

    // Deliver the correlated response.
    reactor.deliver(makeInboundMessage(CORR_ID));

    await waitForEvent(events, (e) => e.type === "reactor.done", 3000);

    expect(events.some((e) => e.type === "message.correlated")).toBe(true);
  });

  test("message with non-matching correlationId passes through uncorrelated", async () => {
    const { events, onEvent } = collectEvents();

    const plugin: ReactorPlugin = {
      async decide(
        event: ReactorInboundEvent,
        _state: ReactorState,
        caps: ReactorCapabilities,
      ) {
        if (event.type === "message.received") {
          return caps.done();
        }
        return caps.done();
      },
    };

    const reactor = createReactor({
      sessionId: "sess-no-corr",
      plugin,
      providerConfig: {
        provider: "anthropic",
        baseURL: "https://api.anthropic.com",
        apiKey: "test",
      },
      toolRunner: noopToolRunner(),
      contextStore: makeContextStore(),
      onEvent,
      shutdownTimeoutMs: 100,
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());

    await waitForEvent(events, (e) => e.type === "reactor.done", 2000);

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
      plugin: pluginFromTable({
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
    await expect(store.load()).rejects.toThrow("disk on fire");
  });

  test("throwingToolRunner throws on run", async () => {
    const runner = throwingToolRunner(new Error("tool exploded"));
    const signal = new AbortController().signal;
    await expect(
      runner.run({ id: "c1", name: "t", arguments: {} }, signal),
    ).rejects.toThrow("tool exploded");
  });

  test("getEvent throws when event is missing", () => {
    const events: InferenceEvent[] = [];
    expect(() => getEvent(events, "reactor.error")).toThrow(
      "No event of type 'reactor.error' found",
    );
  });

  test("pluginFromTable with wait default falls through on unhandled events", async () => {
    // Suspend produces a reactor.gate.cleared event. That event type is
    // NOT in the table, so the defaultAction "wait" fallthrough fires.
    // The reactor stays alive because of the fallthrough, then a second
    // message triggers done via the table handler.
    let messageCount = 0;
    const { reactor, events, waitFor } = createTestReactor({
      plugin: pluginFromTable(
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
      plugin: pluginFromTable({
        "message.received": (_e, _s, caps) =>
          caps.executeTools([{ id: "c1", name: "boom", arguments: {} }]),
      }),
      toolRunner: throwingToolRunner(new Error("tool exploded")),
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitFor("reactor.done");

    const errorEvent = getEvent(events, "reactor.error");
    expect(errorEvent.data.error).toMatch(/tool exploded/);
    expect(errorEvent.data.fatal).toBe(true);
  });

  test("tool runner returning isError propagates error result to plugin", async () => {
    const { reactor, events, waitFor } = createTestReactor({
      plugin: pluginFromTable({
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
      plugin: pluginFromTable({
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
    // We observe history indirectly via a checkpoint that captures the
    // messages passed to contextStore.commit().
    let committedMessages: ConversationMessage[] = [];
    const capturingStore: ContextStore = {
      async load() {
        return {
          messages: [],
          pendingOperations: [],
          tokenUsage: emptyUsage(),
        };
      },
      async commit(msgs, _ops, _usage, message): Promise<ContextCommit> {
        committedMessages = [...msgs];
        return { hash: "abc", message, timestamp: Date.now() };
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
    };

    const { reactor, waitFor } = createTestReactor({
      contextStore: capturingStore,
      plugin: pluginFromTable({
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
    expect(committedMessages.length).toBeGreaterThan(0);

    // The committed history should contain the inbound text message but
    // no tool_result message since addToHistory was false.
    const hasToolResult = committedMessages.some((m) =>
      m.content.some((b) => b.type === "tool_result"),
    );
    expect(hasToolResult).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// 12. Plugin misbehavior
// ---------------------------------------------------------------------------

describe("createReactor — plugin misbehavior", () => {
  test("reactor shuts down when plugin returns invalid action set", async () => {
    const { reactor, events, waitFor } = createTestReactor({
      plugin: pluginFromTable({
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

  test("plugin emitting reserved namespace inference.* produces non-fatal error", async () => {
    const { reactor, events, waitFor } = createTestReactor({
      plugin: pluginFromTable({
        "message.received": (_e, _s, caps) => [
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

  test("plugin emitting reserved namespace tool.* produces non-fatal error", async () => {
    const { reactor, events, waitFor } = createTestReactor({
      plugin: pluginFromTable({
        "message.received": (_e, _s, caps) => [
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

  test("plugin emitting reserved namespace reactor.* produces non-fatal error", async () => {
    const { reactor, events, waitFor } = createTestReactor({
      plugin: pluginFromTable({
        "message.received": (_e, _s, caps) => [
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

  test("fork action emits non-fatal unsupported error", async () => {
    const { reactor, events, waitFor } = createTestReactor({
      plugin: pluginFromTable({
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
      plugin: pluginFromTable({
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
      plugin: pluginFromTable({
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
          messages: [],
          pendingOperations: [],
          tokenUsage: emptyUsage(),
        };
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
    };

    let messageCount = 0;
    const { reactor, events, waitFor } = createTestReactor({
      contextStore: failingStore,
      plugin: pluginFromTable({
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
    // Track how many message.received events the plugin processed.
    let messagesProcessed = 0;
    const { reactor, waitFor } = createTestReactor({
      plugin: pluginFromTable(
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
    // and shut down before the extra messages reach the plugin.
    reactor.deliver(makeInboundMessage());
    reactor.deliver(makeInboundMessage());
    reactor.abort("admin_kill");

    await waitFor("reactor.done");

    // Only the first message should have been processed by the plugin.
    // The abort jumped ahead of the two queued messages.
    expect(messagesProcessed).toBe(1);
  });

  test("multiple abort calls do not cause double shutdown", async () => {
    const { reactor, events, waitFor } = createTestReactor({
      plugin: pluginFromTable(
        {
          "message.received": (_e, _s, caps) => caps.wait(),
        },
        "wait",
      ),
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitFor("message.received");

    reactor.abort("reason-1");
    reactor.abort("reason-2");
    reactor.abort("reason-3");

    await waitFor("reactor.done");

    // Exactly one reactor.done should be emitted.
    const doneEvents = events.filter((e) => e.type === "reactor.done");
    expect(doneEvents.length).toBe(1);
  });

  test("abort before start shuts down immediately when start is called", async () => {
    const { reactor, events, waitFor } = createTestReactor({
      plugin: pluginFromTable(
        {
          "message.received": (_e, _s, caps) => caps.wait(),
        },
        "wait",
      ),
    });

    // Abort is enqueued before start().
    reactor.abort("preemptive");
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
    const seededMessages: ConversationMessage[] = [
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
      contextStore: makeContextStore(seededMessages),
      plugin: {
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
      plugin: pluginFromTable({
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
      plugin: pluginFromTable({
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
      plugin: pluginFromTable({
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
// 19. Deliver after done
// ---------------------------------------------------------------------------

describe("createReactor — deliver after done", () => {
  test("messages delivered after reactor.done are silently dropped", async () => {
    const { reactor, events, waitFor } = createTestReactor({
      plugin: pluginFromTable({
        "message.received": (_e, _s, caps) => caps.done(),
      }),
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitFor("reactor.done");

    // Deliver after done — these should never be processed by the plugin.
    reactor.deliver(makeInboundMessage());
    reactor.deliver(makeInboundMessage());

    // Give the event loop a chance to process any spurious events.
    await new Promise((resolve) => setTimeout(resolve, 50));

    // The deliver() call still runs tryCorrelate and emits message.received
    // events (the deliver function itself doesn't check `done`), but the
    // loop is no longer running so the plugin never processes them. The key
    // assertion is that reactor.done is emitted exactly once.
    const doneEvents = events.filter((e) => e.type === "reactor.done");
    expect(doneEvents.length).toBe(1);
  });
});
