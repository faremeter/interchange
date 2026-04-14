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

  test("empty action list is invalid", () => {
    const result = validateActions([]);
    expect(result.ok).toBe(false);
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
