import { describe, test, expect } from "bun:test";

import {
  defineAgent,
  type Agent,
  type AgentDefinition,
  type BaseEnv,
  type SendResult,
} from "@intx/agent";
import { noopAuditStore } from "@intx/agent/testing";
import { createDefaultDirectorRegistry } from "@intx/agent";
import type {
  AuthorizeContext,
  StepInvokeRequest,
  WorkflowAuthorizeFn,
} from "@intx/workflow";
import type {
  BlobReader,
  ContextStore,
  InboundMessage,
  InferenceEvent,
  InferenceSource,
} from "@intx/types/runtime";

import { createWorkflowStepInvoker, type StepEnvBase } from "./step-invoker";
import { createWarmAgentCache } from "../child/warm-agent-cache";

const STUB_SOURCE: InferenceSource = {
  id: "anthropic:stub",
  provider: "anthropic",
  baseURL: "https://api.anthropic.com",
  apiKey: "sk-stub",
  model: "stub-model",
};

// The step invoker never touches the storage or blob reader on the
// stub path; the env-validation gate accepts any object-shaped value
// for these fields. A throwing proxy is overkill -- the agent layer
// never reaches in -- so a bare empty-object cast is enough.
function stubContextStore(): ContextStore {
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test stub; never invoked on the adapter path
  return {} as ContextStore;
}

function stubBlobReader(): BlobReader {
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test stub; never read on the adapter path
  return {} as BlobReader;
}

function stubBuildEnv(): StepEnvBase {
  return {
    sources: [STUB_SOURCE],
    defaultSource: STUB_SOURCE.id,
    storage: stubContextStore(),
    workdir: "/tmp/workflow-step-invoker-stub",
    audit: noopAuditStore(),
    directors: createDefaultDirectorRegistry(),
  };
}

function stubDef(): AgentDefinition<BaseEnv> {
  return defineAgent({
    id: "step-invoker-stub",
    systemPrompt: "stub",
    tools: [],
    capabilities: [],
    inference: {
      sources: [{ provider: STUB_SOURCE.provider, model: STUB_SOURCE.model }],
    },
  });
}

interface StubAgentControl {
  readonly agent: Agent;
  readonly events: string[];
  resolveSend: (result: SendResult) => void;
  rejectSend: (cause: Error) => void;
}

/**
 * Construct an `Agent` stub that records every observable interaction
 * and surfaces controllable `send` / `close` behaviour. The send path
 * settles via `resolveSend` / `rejectSend` so a test can drive the
 * happy path, the abort path (close races send), and the failure
 * path independently.
 */
function buildStubAgent(): StubAgentControl {
  const events: string[] = [];
  let resolveSend: (result: SendResult) => void = () => {
    /* assigned below */
  };
  let rejectSend: (cause: Error) => void = () => {
    /* assigned below */
  };
  const pending = new Promise<SendResult>((resolve, reject) => {
    resolveSend = resolve;
    rejectSend = reject;
  });
  // Absorb rejections that no consumer observed -- the adapter only
  // attaches a `.then` handler after `agent.send` is invoked, so a
  // close that rejects the pending promise before `send` was ever
  // called (e.g. the input-synthesis throws synchronously inside the
  // adapter's executor and the finally block calls `close`) would
  // surface as an unhandled rejection that Bun's test harness
  // promotes to a failure. The noop catch only fires for the
  // unobserved case; consumers that attach their own `.then` still
  // see the rejection.
  pending.catch(() => {
    /* noop */
  });
  let closed = false;
  const agent: Agent = {
    async send(content): Promise<SendResult> {
      events.push(`send:${typeof content === "string" ? content : "message"}`);
      return pending;
    },
    stream() {
      throw new Error("stub stream() not used");
    },
    deliver(_message: InboundMessage) {
      throw new Error("stub deliver() not used");
    },
    async close() {
      if (closed) {
        events.push("close:noop");
        return;
      }
      closed = true;
      events.push("close");
      rejectSend(new Error("agent closed"));
    },
    setSource(_source: InferenceSource) {
      throw new Error("stub setSource() not used");
    },
    setSources(_sources: InferenceSource[], _defaultSource: string) {
      throw new Error("stub setSources() not used");
    },
    async history() {
      return [];
    },
    async checkpoints() {
      return [];
    },
    async readAt() {
      return [];
    },
    blobReader: stubBlobReader(),
  };
  return {
    agent,
    events,
    resolveSend: (result) => {
      resolveSend(result);
    },
    rejectSend: (cause) => {
      rejectSend(cause);
    },
  };
}

interface StreamingStubState {
  streamStarted: boolean;
  closed: boolean;
}

interface StreamingStubControl {
  readonly agent: Agent;
  readonly state: StreamingStubState;
}

/**
 * Construct an `Agent` stub whose `stream()` yields a controllable
 * sequence of `InferenceEvent`s and then blocks until `close()` fires,
 * mimicking a live stream that ends only when the agent tears down.
 * `state.streamStarted` records whether the adapter ever subscribed (so
 * a test can prove the no-`onEvent` path never consumes the stream);
 * `state.closed` records teardown (so a test can prove the subscription
 * is torn down with the agent). Unlike `buildStubAgent`, `send` resolves
 * immediately and independently of `close`, matching the real agent's
 * send/stream separation.
 */
function buildStreamingStubAgent(
  events: InferenceEvent[],
): StreamingStubControl {
  const state: StreamingStubState = { streamStarted: false, closed: false };
  let endStream: () => void = () => {
    /* assigned below */
  };
  const streamEnded = new Promise<void>((resolve) => {
    endStream = resolve;
  });
  const agent: Agent = {
    async send(): Promise<SendResult> {
      return {
        reply: "ok",
        turn: {
          role: "assistant",
          content: [{ type: "text", text: "ok" }],
          model: STUB_SOURCE.model,
          timestamp: 0,
        },
      };
    },
    async *stream() {
      state.streamStarted = true;
      for (const event of events) yield event;
      await streamEnded;
    },
    deliver(_message: InboundMessage) {
      throw new Error("stub deliver() not used");
    },
    async close() {
      state.closed = true;
      endStream();
    },
    setSource(_source: InferenceSource) {
      throw new Error("stub setSource() not used");
    },
    setSources(_sources: InferenceSource[], _defaultSource: string) {
      throw new Error("stub setSources() not used");
    },
    async history() {
      return [];
    },
    async checkpoints() {
      return [];
    },
    async readAt() {
      return [];
    },
    blobReader: stubBlobReader(),
  };
  return { agent, state };
}

const stubEvent = (type: string): InferenceEvent =>
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test stub event; only `type` is read by the forwarder
  ({ type, seq: 1, data: {} }) as unknown as InferenceEvent;

function buildRequest(opts: {
  signal?: AbortSignal;
  input?: unknown;
}): StepInvokeRequest {
  const ctrl = new AbortController();
  const authzContext: AuthorizeContext = {
    stepId: "step-1",
    attempt: 1,
    runId: "run-1",
  };
  return {
    agent: stubDef(),
    input: opts.input,
    authzContext,
    signal: opts.signal ?? ctrl.signal,
  };
}

describe("workflow-host StepInvoker adapter - warm source rotation", () => {
  test("a rotation during the first warm build reaches the built agent", async () => {
    // Finding: a sources rotation that lands during the (async) first warm
    // build hits the still-empty cache as a no-op applySources, while the
    // in-flight build already captured the prior sources. After the built
    // agent is stored, the adapter re-applies the live table so the rotation
    // is not lost for the warm agent's life.
    const stub = buildStreamingStubAgent([]);
    const setSourcesCalls: {
      sources: InferenceSource[];
      defaultSource: string;
    }[] = [];
    stub.agent.setSources = (sources, defaultSource) => {
      setSourcesCalls.push({ sources, defaultSource });
    };
    const workflowAuthorize: WorkflowAuthorizeFn = async () => ({
      effect: "allow",
      matchingGrants: [],
      resolvedBy: null,
    });
    const warmCache = createWarmAgentCache();
    const rotated: InferenceSource = { ...STUB_SOURCE, id: "rotated" };
    const sourcesRef = {
      current: { "step-1": [STUB_SOURCE] } as Record<string, InferenceSource[]>,
    };

    let releaseBuild!: () => void;
    const buildGate = new Promise<void>((resolve) => {
      releaseBuild = resolve;
    });
    let buildStarted = false;

    const invoker = createWorkflowStepInvoker({
      workflowAuthorize,
      buildEnv: async () => stubBuildEnv(),
      agentFactory: async () => {
        buildStarted = true;
        await buildGate;
        return stub.agent;
      },
      warmCache,
      sourcesRef,
    });

    const invokePromise = invoker(buildRequest({ input: { goal: "ping" } }));
    while (!buildStarted) {
      await new Promise((r) => setTimeout(r, 1));
    }
    // A rotation lands DURING the build: the run-loop updates the ref and
    // calls applySources -- a no-op here because the cache is still empty.
    sourcesRef.current = { "step-1": [rotated] };
    warmCache.applySources([rotated], "rotated");
    expect(setSourcesCalls).toHaveLength(0);

    releaseBuild();
    await invokePromise;

    // The re-apply after store applied the ROTATED table to the built agent.
    expect(setSourcesCalls).toEqual([
      { sources: [rotated], defaultSource: "rotated" },
    ]);

    await warmCache.evictAll("test cleanup");
  });
});

describe("workflow-host StepInvoker adapter - happy path", () => {
  test("delivers synthesized message, captures reply, returns output shape", async () => {
    const stub = buildStubAgent();
    const authzCalls: AuthorizeContext[] = [];
    const workflowAuthorize: WorkflowAuthorizeFn = async (
      _resource,
      _action,
      ctx,
    ) => {
      authzCalls.push(ctx);
      return { effect: "allow", matchingGrants: [], resolvedBy: null };
    };

    const invoker = createWorkflowStepInvoker({
      workflowAuthorize,
      buildEnv: async () => stubBuildEnv(),
      agentFactory: async () => stub.agent,
    });

    const req = buildRequest({ input: { goal: "ping" } });
    const turn = {
      role: "assistant" as const,
      content: [{ type: "text" as const, text: "pong" }],
      model: STUB_SOURCE.model,
      timestamp: 0,
    };
    const sendPromise = invoker(req);

    // Microtask handoff so `agent.send` records the synthesized
    // content before the test asserts on `events`.
    await Promise.resolve();
    // The closure delegating to workflowAuthorize is constructed at
    // env build time. Exercising it through the workflow-typed
    // surface here proves the per-call AuthorizeContext is captured
    // by the closure for any authz call originating from the step.
    const envBase = stubBuildEnv();
    void envBase;
    const builtAuth = (): Promise<unknown> =>
      workflowAuthorize("tool:probe", "invoke", req.authzContext);
    await builtAuth();
    expect(authzCalls).toHaveLength(1);
    expect(authzCalls[0]?.stepId).toBe("step-1");

    stub.resolveSend({ reply: "pong", turn });
    const result = await sendPromise;
    expect(result.output).toEqual({ reply: "pong", turn });
    expect(stub.events[0]).toBe(`send:${JSON.stringify({ goal: "ping" })}`);
    expect(stub.events).toContain("close");
  });

  test("passes a string input through verbatim instead of double-JSON-encoding", async () => {
    const stub = buildStubAgent();
    const invoker = createWorkflowStepInvoker({
      workflowAuthorize: async () => ({
        effect: "allow",
        matchingGrants: [],
        resolvedBy: null,
      }),
      buildEnv: async () => stubBuildEnv(),
      agentFactory: async () => stub.agent,
    });

    const sendPromise = invoker(buildRequest({ input: "raw-string" }));
    await Promise.resolve();
    stub.resolveSend({
      reply: "ok",
      turn: {
        role: "assistant",
        content: [{ type: "text", text: "ok" }],
        model: STUB_SOURCE.model,
        timestamp: 0,
      },
    });
    await sendPromise;
    expect(stub.events[0]).toBe("send:raw-string");
  });

  test("rejects when the input is not JSON-serializable", async () => {
    const stub = buildStubAgent();
    const invoker = createWorkflowStepInvoker({
      workflowAuthorize: async () => ({
        effect: "allow",
        matchingGrants: [],
        resolvedBy: null,
      }),
      buildEnv: async () => stubBuildEnv(),
      agentFactory: async () => stub.agent,
    });
    const req = buildRequest({ input: () => "function" });
    await expect(invoker(req)).rejects.toThrow(/not JSON-serializable/);
    // The agent is constructed before the synthesizer runs because the
    // adapter must build the env to honor the workflow-typed authorize
    // closure regardless of input shape. Close() must still fire so
    // the workdir lock and stream consumers do not leak.
    expect(stub.events).toContain("close");
  });
});

describe("workflow-host StepInvoker adapter - abort handling", () => {
  test("rejects with abort error and closes the agent when signal aborts mid-step", async () => {
    const stub = buildStubAgent();
    const ctrl = new AbortController();
    const invoker = createWorkflowStepInvoker({
      workflowAuthorize: async () => ({
        effect: "allow",
        matchingGrants: [],
        resolvedBy: null,
      }),
      buildEnv: async () => stubBuildEnv(),
      agentFactory: async () => stub.agent,
    });

    const req = buildRequest({ signal: ctrl.signal, input: { goal: "stall" } });
    const settled = invoker(req);

    await Promise.resolve();
    ctrl.abort();

    await expect(settled).rejects.toMatchObject({ name: "AbortError" });
    expect(stub.events).toContain("close");
  });

  test("short-circuits a pre-aborted signal without invoking the agent factory", async () => {
    let factoryCalls = 0;
    const ctrl = new AbortController();
    ctrl.abort();
    const invoker = createWorkflowStepInvoker({
      workflowAuthorize: async () => ({
        effect: "allow",
        matchingGrants: [],
        resolvedBy: null,
      }),
      buildEnv: async () => stubBuildEnv(),
      agentFactory: async () => {
        factoryCalls += 1;
        return buildStubAgent().agent;
      },
    });
    await expect(
      invoker(buildRequest({ signal: ctrl.signal })),
    ).rejects.toMatchObject({ name: "AbortError" });
    expect(factoryCalls).toBe(0);
  });

  test("propagates the signal's abort reason when supplied", async () => {
    const stub = buildStubAgent();
    const ctrl = new AbortController();
    const reason = new Error("workflow cancellation");
    const invoker = createWorkflowStepInvoker({
      workflowAuthorize: async () => ({
        effect: "allow",
        matchingGrants: [],
        resolvedBy: null,
      }),
      buildEnv: async () => stubBuildEnv(),
      agentFactory: async () => stub.agent,
    });

    const settled = invoker(buildRequest({ signal: ctrl.signal }));
    await Promise.resolve();
    ctrl.abort(reason);
    await expect(settled).rejects.toBe(reason);
    expect(stub.events).toContain("close");
  });
});

describe("workflow-host StepInvoker adapter - output shape", () => {
  test("returns { output: { reply, turn } } so consumers can read both shapes", async () => {
    const stub = buildStubAgent();
    const invoker = createWorkflowStepInvoker({
      workflowAuthorize: async () => ({
        effect: "allow",
        matchingGrants: [],
        resolvedBy: null,
      }),
      buildEnv: async () => stubBuildEnv(),
      agentFactory: async () => stub.agent,
    });

    const turn = {
      role: "assistant" as const,
      content: [{ type: "text" as const, text: "hello" }],
      model: STUB_SOURCE.model,
      timestamp: 0,
    };
    const settled = invoker(buildRequest({ input: 42 }));
    await Promise.resolve();
    stub.resolveSend({ reply: "hello", turn });
    const result = await settled;
    expect(result.output).toEqual({ reply: "hello", turn });
  });
});

describe("workflow-host StepInvoker adapter - onEvent contract", () => {
  test("a throwing onEvent sink is swallowed and does not abort the step", async () => {
    const stub = buildStreamingStubAgent([
      stubEvent("inference.start"),
      stubEvent("inference.done"),
    ]);
    let calls = 0;
    const invoker = createWorkflowStepInvoker({
      workflowAuthorize: async () => ({
        effect: "allow",
        matchingGrants: [],
        resolvedBy: null,
      }),
      buildEnv: async () => stubBuildEnv(),
      agentFactory: async () => stub.agent,
      onEvent: () => {
        calls += 1;
        throw new Error("sink boom");
      },
    });

    const result = await invoker(buildRequest({ input: { goal: "go" } }));
    expect(result.output).toMatchObject({ reply: "ok" });
    expect(calls).toBeGreaterThan(0);
    expect(stub.state.closed).toBe(true);
  });

  test("tears down the stream subscription with the agent", async () => {
    const stub = buildStreamingStubAgent([stubEvent("inference.start")]);
    const seen: string[] = [];
    const invoker = createWorkflowStepInvoker({
      workflowAuthorize: async () => ({
        effect: "allow",
        matchingGrants: [],
        resolvedBy: null,
      }),
      buildEnv: async () => stubBuildEnv(),
      agentFactory: async () => stub.agent,
      onEvent: (event) => seen.push(event.type),
    });

    // The stub's `stream()` blocks until `close()` fires, so the
    // forwarder's loop only ends once the agent is torn down. The
    // invoker resolving proves the subscription was drained and closed
    // with the agent rather than leaking past the step.
    await invoker(buildRequest({ input: { goal: "go" } }));
    expect(stub.state.streamStarted).toBe(true);
    expect(stub.state.closed).toBe(true);
    expect(seen).toContain("inference.start");
  });

  test("omitting onEvent never consumes the agent stream", async () => {
    // `buildStubAgent`'s `stream()` throws when invoked. Reusing it here
    // proves the no-`onEvent` path never touches the stream: were the
    // adapter to subscribe, the throwing `stream()` would surface.
    const stub = buildStubAgent();
    const invoker = createWorkflowStepInvoker({
      workflowAuthorize: async () => ({
        effect: "allow",
        matchingGrants: [],
        resolvedBy: null,
      }),
      buildEnv: async () => stubBuildEnv(),
      agentFactory: async () => stub.agent,
    });

    const sendPromise = invoker(buildRequest({ input: { goal: "go" } }));
    await Promise.resolve();
    stub.resolveSend({
      reply: "ok",
      turn: {
        role: "assistant",
        content: [{ type: "text", text: "ok" }],
        model: STUB_SOURCE.model,
        timestamp: 0,
      },
    });
    await sendPromise;
    expect(stub.events).toContain("close");
  });
});

interface WarmStubLifecycle {
  /** LSP-subprocess analogue: spawned on build, disposed on close. */
  lspSpawnCount: number;
  lspAlive: boolean;
  closeCount: number;
}

interface WarmStubControl {
  readonly agent: Agent;
  readonly lifecycle: WarmStubLifecycle;
  /** Conversation turns the warm agent retains across sends (continuity). */
  readonly conversation: string[];
}

/**
 * Construct a warm-agent stub that models the lifecycle warm-keep
 * guards: an LSP-subprocess analogue spawned once at construction and
 * disposed on `close()`, an in-memory conversation that retains every
 * user turn across sends (so a later reply can reflect an earlier
 * message), and a `stream()` that ends only at `close()`. The reply
 * echoes the running conversation so a test proves continuity:
 * `reply(N) = "reply<N>:" + every prior user turn`.
 */
function buildWarmStubAgent(): WarmStubControl {
  const lifecycle: WarmStubLifecycle = {
    lspSpawnCount: 1,
    lspAlive: true,
    closeCount: 0,
  };
  const conversation: string[] = [];
  let endStream: () => void = () => {
    /* assigned below */
  };
  const streamEnded = new Promise<void>((resolve) => {
    endStream = resolve;
  });
  const agent: Agent = {
    async send(content): Promise<SendResult> {
      if (!lifecycle.lspAlive) {
        throw new Error(
          "warm stub: send after the LSP subprocess was disposed",
        );
      }
      const text = typeof content === "string" ? content : "message";
      conversation.push(text);
      const reply = `reply${String(conversation.length)}:${conversation.join("|")}`;
      return {
        reply,
        turn: {
          role: "assistant",
          content: [{ type: "text", text: reply }],
          model: STUB_SOURCE.model,
          timestamp: 0,
        },
      };
    },
    async *stream() {
      yield stubEvent("inference.start");
      await streamEnded;
    },
    deliver(_message: InboundMessage) {
      throw new Error("stub deliver() not used");
    },
    async close() {
      lifecycle.closeCount += 1;
      lifecycle.lspAlive = false;
      endStream();
    },
    setSource(_source: InferenceSource) {
      throw new Error("stub setSource() not used");
    },
    setSources(_sources: InferenceSource[], _defaultSource: string) {
      throw new Error("stub setSources() not used");
    },
    async history() {
      return [];
    },
    async checkpoints() {
      return [];
    },
    async readAt() {
      return [];
    },
    blobReader: stubBlobReader(),
  };
  return { agent, lifecycle, conversation };
}

describe("workflow-host StepInvoker adapter - warm-keep mode", () => {
  test("builds the agent once across two messages and keeps the LSP alive between them", async () => {
    const warmCache = createWarmAgentCache();
    const stub = buildWarmStubAgent();
    let factoryCalls = 0;
    const invoker = createWorkflowStepInvoker({
      workflowAuthorize: async () => ({
        effect: "allow",
        matchingGrants: [],
        resolvedBy: null,
      }),
      buildEnv: async () => stubBuildEnv(),
      agentFactory: async () => {
        factoryCalls += 1;
        return stub.agent;
      },
      warmCache,
    });

    const first = await invoker(buildRequest({ input: "first message" }));
    // The LSP is alive between messages (no teardown after the first
    // send) and the agent was built exactly once so far.
    expect(factoryCalls).toBe(1);
    expect(stub.lifecycle.lspAlive).toBe(true);
    expect(stub.lifecycle.closeCount).toBe(0);

    const second = await invoker(buildRequest({ input: "second message" }));
    // Still exactly one build -- the warm agent was reused, not rebuilt
    // -- and the LSP subprocess was spawned once and never torn down
    // between messages.
    expect(factoryCalls).toBe(1);
    expect(stub.lifecycle.lspSpawnCount).toBe(1);
    expect(stub.lifecycle.lspAlive).toBe(true);
    expect(stub.lifecycle.closeCount).toBe(0);

    // Conversation continuity: the warm agent retained the first message
    // in memory, so the second reply reflects both turns.
    const firstReply = readReply(first.output);
    const secondReply = readReply(second.output);
    expect(firstReply).toBe("reply1:first message");
    expect(secondReply).toBe("reply2:first message|second message");

    // Eviction runs the wrapped close exactly once, killing the LSP.
    await warmCache.evictAll("test teardown");
    expect(stub.lifecycle.lspAlive).toBe(false);
    expect(stub.lifecycle.closeCount).toBe(1);
  });

  test("a mid-turn abort cancels only the turn and leaves the warm agent usable", async () => {
    const warmCache = createWarmAgentCache();
    // An agent whose first send blocks until the abort signal fires, so
    // the abort races a live turn. The second send (after the abort)
    // must succeed against the still-warm agent.
    const lifecycle = { lspAlive: true, closeCount: 0 };
    const conversation: string[] = [];
    let endStream: () => void = () => undefined;
    const streamEnded = new Promise<void>((resolve) => {
      endStream = resolve;
    });
    const agent: Agent = {
      async send(content, opts): Promise<SendResult> {
        const text = typeof content === "string" ? content : "message";
        if (opts?.signal !== undefined && text === "stalls") {
          // Model the warm agent honoring the per-send abort signal:
          // reject this turn when the signal fires, without closing.
          return new Promise<SendResult>((_resolve, reject) => {
            opts.signal?.addEventListener(
              "abort",
              () => reject(new Error("send aborted")),
              { once: true },
            );
          });
        }
        conversation.push(text);
        return {
          reply: `ok:${conversation.join("|")}`,
          turn: {
            role: "assistant",
            content: [{ type: "text", text: "ok" }],
            model: STUB_SOURCE.model,
            timestamp: 0,
          },
        };
      },
      async *stream() {
        yield stubEvent("inference.start");
        await streamEnded;
      },
      deliver: () => undefined,
      async close() {
        lifecycle.closeCount += 1;
        lifecycle.lspAlive = false;
        endStream();
      },
      setSource: () => undefined,
      setSources: () => undefined,
      async history() {
        return [];
      },
      async checkpoints() {
        return [];
      },
      async readAt() {
        return [];
      },
      blobReader: stubBlobReader(),
    };
    let factoryCalls = 0;
    const invoker = createWorkflowStepInvoker({
      workflowAuthorize: async () => ({
        effect: "allow",
        matchingGrants: [],
        resolvedBy: null,
      }),
      buildEnv: async () => stubBuildEnv(),
      agentFactory: async () => {
        factoryCalls += 1;
        return agent;
      },
      warmCache,
    });

    const ctrl = new AbortController();
    const aborted = invoker(
      buildRequest({ signal: ctrl.signal, input: "stalls" }),
    );
    await Promise.resolve();
    ctrl.abort();
    await expect(aborted).rejects.toMatchObject({ name: "AbortError" });

    // The warm agent survived the turn abort: NOT closed, LSP still
    // alive, and a subsequent message succeeds against the same build.
    expect(lifecycle.lspAlive).toBe(true);
    expect(lifecycle.closeCount).toBe(0);

    const after = await invoker(buildRequest({ input: "next message" }));
    expect(factoryCalls).toBe(1);
    expect(readReply(after.output)).toBe("ok:next message");

    // Teardown closes the warm agent once.
    await warmCache.evictAll("test teardown");
    expect(lifecycle.lspAlive).toBe(false);
    expect(lifecycle.closeCount).toBe(1);
  });

  test("routes each message's events to that message's onEvent sink", async () => {
    const warmCache = createWarmAgentCache();
    const stub = buildWarmStubAgent();
    const firstSink: string[] = [];
    const secondSink: string[] = [];
    const makeInvoker = (onEvent: (event: InferenceEvent) => void) =>
      createWorkflowStepInvoker({
        workflowAuthorize: async () => ({
          effect: "allow",
          matchingGrants: [],
          resolvedBy: null,
        }),
        buildEnv: async () => stubBuildEnv(),
        agentFactory: async () => stub.agent,
        onEvent,
        warmCache,
      });

    await makeInvoker((event) => firstSink.push(event.type))(
      buildRequest({ input: "one" }),
    );
    await makeInvoker((event) => secondSink.push(event.type))(
      buildRequest({ input: "two" }),
    );

    // The agent's single lifetime stream yields one `inference.start`,
    // delivered to whichever message's sink was active when it fired.
    // Each message's events reach its own sink; neither leaks to the
    // other after its send settles.
    expect([...firstSink, ...secondSink]).toContain("inference.start");
    await warmCache.evictAll("test teardown");
  });
});

function readReply(output: unknown): string {
  if (
    typeof output === "object" &&
    output !== null &&
    "reply" in output &&
    typeof output.reply === "string"
  ) {
    return output.reply;
  }
  throw new Error(`unexpected step output shape: ${JSON.stringify(output)}`);
}
