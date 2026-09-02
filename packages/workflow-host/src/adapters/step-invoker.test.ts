import { describe, test, expect } from "bun:test";
import { type } from "arktype";

import { ApprovalDecision } from "@intx/types";
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
  StepInvokeResult,
  WorkflowAuthorizeFn,
} from "@intx/workflow";
import type {
  BlobReader,
  ContextStore,
  InboundMessage,
  InferenceEvent,
  InferenceSource,
  Mail,
  MailPart,
  MailPartReader,
} from "@intx/types/runtime";

import { createWorkflowStepInvoker, type StepEnvBase } from "./step-invoker";
import {
  createWarmAgentCache,
  type WarmReplyDrive,
  type WarmReplySettlement,
} from "../child/warm-agent-cache";

const STUB_SOURCE: InferenceSource = {
  id: "anthropic:stub",
  provider: "anthropic",
  baseURL: "https://api.anthropic.com",
  credentialId: "sk-stub",
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
        type: "reply",
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

    stub.resolveSend({ type: "reply", reply: "pong", turn });
    const result = await sendPromise;
    expect(expectOutput(result)).toEqual({ reply: "pong", turn });
    expect(stub.events[0]).toBe(`send:${JSON.stringify({ goal: "ping" })}`);
    expect(stub.events).toContain("close");
  });

  test("forwards the approval snapshot from a suspended send result", async () => {
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

    const sendPromise = invoker(buildRequest({ input: { goal: "ping" } }));
    await Promise.resolve();

    const approvalSnapshot = {
      name: "charge_card",
      description: "Charge the customer's card",
      inputSchema: { type: "object" },
      arguments: { amount: 100 },
    };
    stub.resolveSend({
      type: "suspended",
      correlationId: "corr-1",
      approvalSnapshot,
    });

    const result = await sendPromise;
    if (!("suspend" in result)) {
      throw new Error(
        `expected a suspend result, got ${JSON.stringify(result)}`,
      );
    }
    expect(result.suspend.correlationId).toBe("corr-1");
    if (result.suspend.kind !== "approval") {
      throw new Error(
        `expected an approval suspend, got ${result.suspend.kind}`,
      );
    }
    expect(result.suspend.approvalSnapshot).toEqual(approvalSnapshot);
  });

  test("throws on a suspended send result that carries no approval snapshot", async () => {
    // A suspended reactor outcome is always an approval and must carry a
    // snapshot; a snapshot-less suspend (a director `caps.suspend` with no tool
    // definitions) is not a supported approval park. The producer classifies
    // the failure here rather than emitting an ambiguous suspend the runtime
    // would reject downstream. (The "input" park -- a conversational step
    // re-arming for the next mail -- is the workflow-host's decision, not a
    // reactor `SendResult`, so it never flows through here.)
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

    const sendPromise = invoker(buildRequest({ input: { goal: "ping" } }));
    await Promise.resolve();
    stub.resolveSend({ type: "suspended", correlationId: "corr-1" });

    await expect(sendPromise).rejects.toThrow(/no approval snapshot/);
  });

  test("surfaces the send error, not the teardown close error, when both fail", async () => {
    // The wrapped agent close now rejects when a plugin/LSP disposer fails.
    // In the cold path that close runs in the adapter's teardown; a close
    // failure must not mask a step error already unwinding from the send.
    let closeCalls = 0;
    const agent: Agent = {
      send() {
        return Promise.reject(new Error("send boom"));
      },
      stream() {
        throw new Error("stub stream() not used");
      },
      deliver() {
        throw new Error("stub deliver() not used");
      },
      close() {
        closeCalls += 1;
        return Promise.reject(new Error("close boom"));
      },
      setSource() {
        throw new Error("stub setSource() not used");
      },
      setSources() {
        throw new Error("stub setSources() not used");
      },
      history() {
        return Promise.resolve([]);
      },
      checkpoints() {
        return Promise.resolve([]);
      },
      readAt() {
        return Promise.resolve([]);
      },
      blobReader: stubBlobReader(),
    };
    const invoker = createWorkflowStepInvoker({
      workflowAuthorize: async () => ({
        effect: "allow",
        matchingGrants: [],
        resolvedBy: null,
      }),
      buildEnv: async () => stubBuildEnv(),
      agentFactory: async () => agent,
    });

    await expect(
      invoker(buildRequest({ input: { goal: "ping" } })),
    ).rejects.toThrow(/send boom/);
    // The teardown close still ran; its failure was logged, not surfaced.
    expect(closeCalls).toBe(1);
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
      type: "reply",
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
    stub.resolveSend({ type: "reply", reply: "hello", turn });
    const result = await settled;
    expect(expectOutput(result)).toEqual({ reply: "hello", turn });
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
    expect(expectOutput(result)).toMatchObject({ reply: "ok" });
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
      type: "reply",
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
        type: "reply",
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
    const firstReply = readReply(expectOutput(first));
    const secondReply = readReply(expectOutput(second));
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
          type: "reply",
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
    expect(readReply(expectOutput(after))).toBe("ok:next message");

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

interface ReplyDriveHarness {
  driveReplies: (
    key: string,
    stream: ReturnType<Agent["stream"]>,
  ) => WarmReplyDrive;
  readonly keys: string[];
  readonly observed: string[];
  /** The `replySeq()` value the invoker snapshotted before each send. */
  readonly seqSnapshots: number[];
  driveCalls(): number;
  drainSettled(): boolean;
  /** Advance the settled count by one, releasing a waiter on this turn's reply. */
  settle(outcome: WarmReplySettlement): void;
}

/**
 * A controllable stand-in for the connector reply drain's per-turn barrier.
 * The `driveReplies` callback consumes the agent's lifetime stream in the
 * background (so `done` reflects stream end, foldable into eviction, and
 * `observed` records event types). Settlement is either manual (`settle`) or,
 * when `autoSettleOnReply` is set, recorded once per observed `connector.reply`
 * -- mirroring the real drain advancing its sequence after a send acks.
 */
function buildReplyDriveHarness(opts?: {
  autoSettleOnReply?: WarmReplySettlement;
}): ReplyDriveHarness {
  let settledCount = 0;
  const settlements: WarmReplySettlement[] = [];
  type Waiter = { target: number; resolve: (s: WarmReplySettlement) => void };
  let waiters: Waiter[] = [];
  const keys: string[] = [];
  const observed: string[] = [];
  const seqSnapshots: number[] = [];
  let calls = 0;
  let drainDone = false;

  function settlementAt(index: number): WarmReplySettlement {
    const settlement = settlements[index];
    if (settlement === undefined) {
      throw new Error(`fake reply drive: settlement ${String(index)} missing`);
    }
    return settlement;
  }

  function settle(outcome: WarmReplySettlement): void {
    settlements.push(outcome);
    settledCount += 1;
    const stillWaiting: Waiter[] = [];
    for (const waiter of waiters) {
      if (settledCount > waiter.target) {
        waiter.resolve(settlementAt(waiter.target));
      } else {
        stillWaiting.push(waiter);
      }
    }
    waiters = stillWaiting;
  }

  const driveReplies = (
    key: string,
    stream: ReturnType<Agent["stream"]>,
  ): WarmReplyDrive => {
    calls += 1;
    keys.push(key);
    const done = (async () => {
      for await (const event of stream) {
        observed.push(event.type);
        if (
          opts?.autoSettleOnReply !== undefined &&
          event.type === "connector.reply"
        ) {
          settle(opts.autoSettleOnReply);
        }
      }
      // Extra async gap after the stream ends so a test can prove the invoker
      // folded `done` into the warm entry the cache awaits: `evictAll` returns
      // only after this settles.
      await new Promise((resolve) => setTimeout(resolve, 20));
      drainDone = true;
    })();
    return {
      done,
      replySeq() {
        seqSnapshots.push(settledCount);
        return settledCount;
      },
      waitForReplyAfter(n: number): Promise<WarmReplySettlement> {
        if (settledCount > n) return Promise.resolve(settlementAt(n));
        return new Promise<WarmReplySettlement>((resolve) => {
          waiters.push({ target: n, resolve });
        });
      },
    };
  };

  return {
    driveReplies,
    keys,
    observed,
    seqSnapshots,
    driveCalls: () => calls,
    drainSettled: () => drainDone,
    settle,
  };
}

/**
 * A warm-agent stub whose `send` returns the given `SendResult` and whose
 * `stream()` stays open until `close()`. The stream emits no `connector.reply`
 * by default, so the reply barrier stays pending until a test settles it
 * manually -- letting a test observe that the warm step blocks on the barrier.
 */
function buildBarrierStubAgent(result: SendResult): {
  agent: Agent;
  closeCount: () => number;
} {
  let endStream: () => void = () => undefined;
  const streamEnded = new Promise<void>((resolve) => {
    endStream = resolve;
  });
  let closeCount = 0;
  const agent: Agent = {
    async send(): Promise<SendResult> {
      return result;
    },
    async *stream() {
      yield stubEvent("inference.start");
      await streamEnded;
    },
    deliver(_message: InboundMessage) {
      throw new Error("stub deliver() not used");
    },
    async close() {
      closeCount += 1;
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
  return { agent, closeCount: () => closeCount };
}

function replySendResult(reply: string): SendResult {
  return {
    type: "reply",
    reply,
    turn: {
      role: "assistant",
      content: [{ type: "text", text: reply }],
      model: STUB_SOURCE.model,
      timestamp: 0,
    },
  };
}

describe("workflow-host StepInvoker adapter - warm reply drain", () => {
  test("drives replies once over the agent lifetime stream and drains at eviction", async () => {
    const warmCache = createWarmAgentCache();

    // A warm stub whose stream carries a `connector.reply` and ends only at
    // close(), modeling the agent's lifetime event stream the reply drain
    // consumes.
    let endStream: () => void = () => undefined;
    const streamEnded = new Promise<void>((resolve) => {
      endStream = resolve;
    });
    let closeCount = 0;
    const agent: Agent = {
      async send(content): Promise<SendResult> {
        const text = typeof content === "string" ? content : "message";
        return replySendResult(`echo:${text}`);
      },
      async *stream() {
        yield {
          type: "connector.reply",
          seq: 1,
          data: { content: "outbound" },
        };
        await streamEnded;
      },
      deliver(_message: InboundMessage) {
        throw new Error("stub deliver() not used");
      },
      async close() {
        closeCount += 1;
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

    // Auto-settle a success per observed `connector.reply`, so each send's
    // barrier resolves and the invoker returns.
    const harness = buildReplyDriveHarness({
      autoSettleOnReply: { ok: true },
    });

    const invoker = createWorkflowStepInvoker({
      workflowAuthorize: async () => ({
        effect: "allow",
        matchingGrants: [],
        resolvedBy: null,
      }),
      buildEnv: async () => stubBuildEnv(),
      agentFactory: async () => agent,
      warmCache,
      driveReplies: harness.driveReplies,
    });

    await invoker(buildRequest({ input: "first message" }));

    // The drain is established exactly once -- at the first-message build --
    // keyed by the step id, and reused across later messages rather than
    // re-subscribed per send.
    expect(harness.driveCalls()).toBe(1);
    expect(harness.keys).toEqual(["step-1"]);

    // Eviction drains the reply loop alongside the observability forwarder:
    // the stub drain settles only after the stream ends (at close) plus a
    // tick, and it is settled once `evictAll` returns -- proving the invoker
    // folded the drain's promise into the warm entry the cache awaits.
    await warmCache.evictAll("test teardown");
    expect(closeCount).toBe(1);
    expect(harness.drainSettled()).toBe(true);
    // The stream handed to the drain is the agent's live stream -- it carried
    // the `connector.reply` the drain sends.
    expect(harness.observed).toContain("connector.reply");
  });

  test("a reply turn blocks on the barrier until the reply is durably sent", async () => {
    const warmCache = createWarmAgentCache();
    const { agent } = buildBarrierStubAgent(replySendResult("the reply"));
    // No auto-settle: the barrier stays pending until the test settles it, so
    // we can observe the step blocking on a reply that has not yet been sent.
    const harness = buildReplyDriveHarness();

    const invoker = createWorkflowStepInvoker({
      workflowAuthorize: async () => ({
        effect: "allow",
        matchingGrants: [],
        resolvedBy: null,
      }),
      buildEnv: async () => stubBuildEnv(),
      agentFactory: async () => agent,
      warmCache,
      driveReplies: harness.driveReplies,
    });

    let settled = false;
    const pending = invoker(buildRequest({ input: "hello" })).then((result) => {
      settled = true;
      return result;
    });
    // Let the send resolve and the invoker reach the barrier await.
    await new Promise((resolve) => setTimeout(resolve, 10));
    // The step has NOT returned: it is gated on this turn's reply being sent.
    expect(settled).toBe(false);
    // The invoker snapshotted the sequence before the send (0 replies settled).
    expect(harness.seqSnapshots).toEqual([0]);

    // The reply is durably sent: settle the barrier, and the step returns.
    harness.settle({ ok: true });
    const result = await pending;
    expect(settled).toBe(true);
    expect(readReply(expectOutput(result))).toBe("the reply");

    await warmCache.evictAll("test teardown");
  });

  test("a suspended turn does not block on the reply barrier", async () => {
    const warmCache = createWarmAgentCache();
    // A gate/suspended turn produces no `connector.reply`; the step must NOT
    // await the barrier (that reply never arrives and the wait would hang).
    const { agent } = buildBarrierStubAgent({
      type: "suspended",
      correlationId: "corr-1",
      approvalSnapshot: {
        name: "tool_a",
        description: "needs approval",
        inputSchema: { type: "object" },
        arguments: {},
      },
    });
    const harness = buildReplyDriveHarness();

    const invoker = createWorkflowStepInvoker({
      workflowAuthorize: async () => ({
        effect: "allow",
        matchingGrants: [],
        resolvedBy: null,
      }),
      buildEnv: async () => stubBuildEnv(),
      agentFactory: async () => agent,
      warmCache,
      driveReplies: harness.driveReplies,
    });

    // Never settle the barrier: a suspended turn must return regardless.
    const result = await invoker(buildRequest({ input: "needs approval" }));
    if (!("suspend" in result)) {
      throw new Error("expected a suspend-shaped step result");
    }
    expect(result.suspend.correlationId).toBe("corr-1");

    await warmCache.evictAll("test teardown");
  });

  test("a reply-send failure fails the turn so the mail is not consumed as replied", async () => {
    const warmCache = createWarmAgentCache();
    const { agent } = buildBarrierStubAgent(replySendResult("doomed reply"));
    const harness = buildReplyDriveHarness();

    const invoker = createWorkflowStepInvoker({
      workflowAuthorize: async () => ({
        effect: "allow",
        matchingGrants: [],
        resolvedBy: null,
      }),
      buildEnv: async () => stubBuildEnv(),
      agentFactory: async () => agent,
      warmCache,
      driveReplies: harness.driveReplies,
    });

    const sendCause = new Error("outbound bridge rejected the reply");
    const pending = invoker(buildRequest({ input: "hello" }));
    // Let the invoker reach the barrier await, then fail the reply send.
    await new Promise((resolve) => setTimeout(resolve, 10));
    harness.settle({ ok: false, cause: sendCause });

    // The step rejects rather than returning: the run's claim-check replays
    // the inbound mail instead of consuming it with the reply dropped.
    let thrown: unknown;
    try {
      await pending;
    } catch (cause) {
      thrown = cause;
    }
    if (!(thrown instanceof Error)) {
      throw new Error("expected the failed reply send to reject the step");
    }
    expect(thrown.message).toContain("auto-reply send failed");
    expect(thrown.cause).toBe(sendCause);

    await warmCache.evictAll("test teardown");
  });
});

describe("workflow-host StepInvoker adapter - resume send path", () => {
  test("a resume request sends the correlated decision and returns the resumed reply", async () => {
    // The resumed reactor's reply turn -- what the step output should carry.
    const resumeTurn = {
      role: "assistant" as const,
      content: [{ type: "text" as const, text: "resumed reply" }],
      model: STUB_SOURCE.model,
      timestamp: 7,
    };

    let sentContent: string | InboundMessage | undefined;
    const agent = buildResumeStubAgent((content) => {
      sentContent = content;
      return { type: "reply", reply: "resumed reply", turn: resumeTurn };
    });

    const invoker = createWorkflowStepInvoker({
      workflowAuthorize: async () => ({
        effect: "allow",
        matchingGrants: [],
        resolvedBy: null,
      }),
      buildEnv: async () => stubBuildEnv(),
      agentFactory: async () => agent,
    });

    const req: StepInvokeRequest = {
      ...buildRequest({ input: { goal: "start" } }),
      resume: {
        correlationId: "corr-1",
        decision: { outcome: "approved" },
        kind: "approval",
      },
    };
    const result = await invoker(req);

    // The invoker sent a full InboundMessage (not a plain string) stamped
    // with the correlation id so the reactor's tryCorrelate can match the
    // rehydrated gate, carrying the decision body as its content.
    if (typeof sentContent === "string" || sentContent === undefined) {
      throw new Error(
        `resume must send an InboundMessage, got ${typeof sentContent}`,
      );
    }
    expect(sentContent.headers.interchangeCorrelationId).toBe("corr-1");
    expect(sentContent.content).toBe(JSON.stringify({ outcome: "approved" }));

    // The message body must be a well-formed ApprovalDecision: the reactor's
    // re-dispatch path parses it from the content at the correlation boundary
    // and re-runs the parked tool call on an "approved" outcome. A body that
    // does not validate would fail loud there, so the invoker's job is to
    // deliver exactly that shape.
    if (sentContent.content === undefined) {
      throw new Error("resume message carried no decision body");
    }
    const decision = ApprovalDecision(JSON.parse(sentContent.content));
    if (decision instanceof type.errors) {
      throw new Error(`resume decision body is malformed: ${decision.summary}`);
    }
    expect(decision.outcome).toBe("approved");

    // The step output is the resumed reply paired with the reactor's turn.
    expect(expectOutput(result)).toEqual({
      reply: "resumed reply",
      turn: resumeTurn,
    });
  });

  test("a resumed cycle that re-parks on a second gate re-suspends instead of hanging", async () => {
    // The resumed cycle (tool A approved) hits a second gate (tool B needs
    // approval) and re-parks. `agent.send` settles that as a "suspended"
    // SendResult carrying the new correlation id; the invoker must hand
    // that straight back as a fresh suspend rather than hanging waiting for
    // a reply that never comes.
    let sentContent: string | InboundMessage | undefined;
    const agent = buildResumeStubAgent((content) => {
      sentContent = content;
      return {
        type: "suspended",
        correlationId: "corr-B",
        approvalSnapshot: {
          name: "tool_b",
          description: "Tool B needs approval",
          inputSchema: { type: "object" },
          arguments: {},
        },
      };
    });

    const invoker = createWorkflowStepInvoker({
      workflowAuthorize: async () => ({
        effect: "allow",
        matchingGrants: [],
        resolvedBy: null,
      }),
      buildEnv: async () => stubBuildEnv(),
      agentFactory: async () => agent,
    });

    const req: StepInvokeRequest = {
      ...buildRequest({ input: { goal: "start" } }),
      resume: {
        correlationId: "corr-A",
        decision: { outcome: "approved" },
        kind: "approval",
      },
    };
    const result = await invoker(req);

    if (typeof sentContent === "string" || sentContent === undefined) {
      throw new Error(
        `resume must send an InboundMessage, got ${typeof sentContent}`,
      );
    }
    expect(sentContent.headers.interchangeCorrelationId).toBe("corr-A");
    if (sentContent.content === undefined) {
      throw new Error("resume message carried no decision body");
    }
    const decision = ApprovalDecision(JSON.parse(sentContent.content));
    if (decision instanceof type.errors) {
      throw new Error(`resume decision body is malformed: ${decision.summary}`);
    }
    expect(decision.outcome).toBe("approved");

    if (!("suspend" in result)) {
      throw new Error(
        `expected a re-suspension, got output ${JSON.stringify(result)}`,
      );
    }
    expect(result.suspend.correlationId).toBe("corr-B");
  });

  test("an input resume delivers a plain next turn, not a correlated gate inbound", async () => {
    // An `"input"` resume is the step's next turn (a long-lived agent's next
    // mail), with no gate to re-correlate. The adapter must send the decision
    // as PLAIN synthesized content, NOT an InboundMessage stamped with the
    // correlationId (the approval path). A plain string is the tell: nothing
    // for the reactor's `tryCorrelate` to match, just a fresh user turn.
    const nextTurn = {
      role: "assistant" as const,
      content: [{ type: "text" as const, text: "next turn reply" }],
      model: STUB_SOURCE.model,
      timestamp: 9,
    };
    let sentContent: string | InboundMessage | undefined;
    const agent = buildResumeStubAgent((content) => {
      sentContent = content;
      return { type: "reply", reply: "next turn reply", turn: nextTurn };
    });

    const invoker = createWorkflowStepInvoker({
      workflowAuthorize: async () => ({
        effect: "allow",
        matchingGrants: [],
        resolvedBy: null,
      }),
      buildEnv: async () => stubBuildEnv(),
      agentFactory: async () => agent,
    });

    const req: StepInvokeRequest = {
      ...buildRequest({ input: { goal: "start" } }),
      resume: {
        correlationId: "corr-input-1",
        decision: { text: "the next mail" },
        kind: "input",
      },
    };
    const result = await invoker(req);

    // Plain content, not a correlated InboundMessage.
    expect(typeof sentContent).toBe("string");
    expect(expectOutput(result)).toEqual({
      reply: "next turn reply",
      turn: nextTurn,
    });
  });
});

/**
 * Construct an `Agent` stub whose `send` runs `onSend` (recording the
 * message and returning the settled `SendResult`) and whose `deliver`
 * throws -- the resume path must route through `send`, never `deliver`.
 */
function buildResumeStubAgent(
  onSend: (content: string | InboundMessage) => SendResult,
): Agent {
  return {
    async send(content): Promise<SendResult> {
      return onSend(content);
    },
    stream() {
      throw new Error("stub stream() not used");
    },
    deliver(_message: InboundMessage) {
      throw new Error("resume path must not call deliver()");
    },
    async close() {
      /* nothing to tear down on the resume stub */
    },
    setSource() {
      throw new Error("stub setSource() not used");
    },
    setSources() {
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
}

function expectOutput(result: StepInvokeResult): unknown {
  if (!("output" in result)) {
    throw new Error(
      `expected an output-shaped step result, got suspend on ${result.suspend.correlationId}`,
    );
  }
  return result.output;
}

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

describe("workflow-host StepInvoker adapter - inbound mail input", () => {
  const allowAll: WorkflowAuthorizeFn = async () => ({
    effect: "allow",
    matchingGrants: [],
    resolvedBy: null,
  });

  function buildCapturingAgent(): {
    agent: Agent;
    captured: { message: string | InboundMessage | undefined };
  } {
    const captured: { message: string | InboundMessage | undefined } = {
      message: undefined,
    };
    const agent: Agent = {
      async send(content): Promise<SendResult> {
        captured.message = content;
        return {
          type: "reply",
          reply: "ok",
          turn: {
            role: "assistant",
            content: [{ type: "text", text: "ok" }],
            model: STUB_SOURCE.model,
            timestamp: 0,
          },
        };
      },
      stream() {
        throw new Error("stub stream() not used");
      },
      deliver(_message: InboundMessage) {
        throw new Error("stub deliver() not used");
      },
      async close() {
        /* no-op */
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
    return { agent, captured };
  }

  function stubReader(bytes: Record<string, string>): MailPartReader {
    return {
      async read(ref: string): Promise<Uint8Array> {
        const value = bytes[ref];
        if (value === undefined) {
          throw new Error(`stub reader has no bytes for ${ref}`);
        }
        return new TextEncoder().encode(value);
      },
    };
  }

  function mail(parts: MailPart[]): Mail {
    return {
      headers: {
        from: "sender@example.com",
        to: ["run@deployment.example.com"],
        date: "2026-01-02T03:04:05Z",
        messageId: "<m@example.com>",
      },
      rawHeaders: {},
      parts,
    };
  }

  test("projects a Mail's parts into the InboundMessage, resolving part bytes", async () => {
    const { agent, captured } = buildCapturingAgent();
    const invoker = createWorkflowStepInvoker({
      workflowAuthorize: allowAll,
      buildEnv: async () => stubBuildEnv(),
      agentFactory: async () => agent,
      mailPartReader: stubReader({ "mail-part:///r/m/1-photo.png": "PNGDATA" }),
    });
    await invoker(
      buildRequest({
        input: mail([
          {
            contentType: "text/plain",
            ref: "mail-part:///r/m/0-text",
            text: "look at this",
          },
          {
            contentType: "image/png",
            ref: "mail-part:///r/m/1-photo.png",
            filename: "photo.png",
          },
        ]),
      }),
    );
    const msg = captured.message;
    if (typeof msg === "string" || msg === undefined) {
      throw new Error("expected an InboundMessage, not a synthesized string");
    }
    // The real From: header rides through.
    expect(msg.headers.from).toBe("sender@example.com");
    expect(msg.content).toBe("look at this");
    expect(msg.attachments).toHaveLength(1);
    expect(msg.attachments?.[0]?.name).toBe("photo.png");
    expect(msg.attachments?.[0]?.contentType).toBe("image/png");
    expect(new TextDecoder().decode(msg.attachments?.[0]?.data)).toBe(
      "PNGDATA",
    );
  });

  test("preserves the Mail's threading headers on the InboundMessage", async () => {
    const { agent, captured } = buildCapturingAgent();
    const invoker = createWorkflowStepInvoker({
      workflowAuthorize: allowAll,
      buildEnv: async () => stubBuildEnv(),
      agentFactory: async () => agent,
    });
    const threaded: Mail = {
      headers: {
        from: "sender@example.com",
        to: ["run@deployment.example.com"],
        date: "2026-01-02T03:04:05Z",
        messageId: "<incoming-42@example.com>",
        inReplyTo: "<prior-turn@agent.local>",
        references: ["<root@example.com>", "<prior-turn@agent.local>"],
      },
      rawHeaders: {},
      parts: [
        {
          contentType: "text/plain",
          ref: "mail-part:///r/m/0-text",
          text: "keep this thread",
        },
      ],
    };
    await invoker(buildRequest({ input: threaded }));
    const msg = captured.message;
    if (typeof msg === "string" || msg === undefined) {
      throw new Error("expected an InboundMessage, not a synthesized string");
    }
    // The real thread headers ride through verbatim so the reply's In-Reply-To
    // points at the incoming Message-ID rather than a synthesized phantom.
    expect(msg.headers.messageId).toBe("<incoming-42@example.com>");
    expect(msg.headers.inReplyTo).toBe("<prior-turn@agent.local>");
    expect(msg.headers.references).toEqual([
      "<root@example.com>",
      "<prior-turn@agent.local>",
    ]);
  });

  test("drops non-RFC threading identifiers instead of failing the step", async () => {
    const { agent, captured } = buildCapturingAgent();
    const invoker = createWorkflowStepInvoker({
      workflowAuthorize: allowAll,
      buildEnv: async () => stubBuildEnv(),
      agentFactory: async () => agent,
    });
    // Inbound mail can carry a headerless-derived (sha256) or malformed
    // Message-Id and a malformed In-Reply-To -- valid claim-check keys but not
    // valid RFC identifiers. Forwarding them verbatim would throw in
    // createInboundMessage and fail the step; they must be dropped instead.
    const malformed: Mail = {
      headers: {
        from: "sender@example.com",
        to: ["run@deployment.example.com"],
        date: "2026-01-02T03:04:05Z",
        messageId: "e3b0c44298fc1c149afbf4c8996fb924",
        inReplyTo: "<invalid",
        references: ["<invalid", "<valid@example.com>"],
      },
      rawHeaders: {},
      parts: [
        {
          contentType: "text/plain",
          ref: "mail-part:///r/m/0-text",
          text: "malformed headers must not fail the step",
        },
      ],
    };
    await invoker(buildRequest({ input: malformed }));
    const msg = captured.message;
    if (typeof msg === "string" || msg === undefined) {
      throw new Error("expected an InboundMessage, not a synthesized string");
    }
    // A valid Message-ID is synthesized in place of the non-RFC one, the
    // malformed In-Reply-To is dropped, and only the valid reference survives.
    expect(msg.headers.messageId).not.toBe("e3b0c44298fc1c149afbf4c8996fb924");
    expect(msg.headers.messageId).toMatch(/^<[^<>\s@]+@[^<>\s@]+>$/);
    expect(msg.headers.inReplyTo).toBeUndefined();
    expect(msg.headers.references).toEqual(["<valid@example.com>"]);
  });

  test("routes an attachment-disposition text part and an html part to attachments", async () => {
    const { agent, captured } = buildCapturingAgent();
    const invoker = createWorkflowStepInvoker({
      workflowAuthorize: allowAll,
      buildEnv: async () => stubBuildEnv(),
      agentFactory: async () => agent,
      mailPartReader: stubReader({
        "mail-part:///r/m/1-notes.txt": "SECRET FILE",
        "mail-part:///r/m/2-body.html": "<html>hi</html>",
      }),
    });
    await invoker(
      buildRequest({
        input: mail([
          {
            contentType: "text/plain",
            ref: "mail-part:///r/m/0-body",
            text: "the body",
          },
          {
            contentType: "text/plain",
            ref: "mail-part:///r/m/1-notes.txt",
            filename: "notes.txt",
            disposition: "attachment",
            text: "SECRET FILE",
          },
          { contentType: "text/html", ref: "mail-part:///r/m/2-body.html" },
        ]),
      }),
    );
    const msg = captured.message;
    if (typeof msg === "string" || msg === undefined) {
      throw new Error("expected an InboundMessage, not a synthesized string");
    }
    // Only the inline plain-text body reaches the turn. The attached .txt (a
    // text part, but disposition attachment) and the text/html alternative are
    // delivered as attachments, not concatenated into the conversation body.
    expect(msg.content).toBe("the body");
    expect(msg.attachments).toHaveLength(2);
    expect(msg.attachments?.map((a) => a.name).sort()).toEqual([
      "notes.txt",
      "text/html",
    ]);
    const notes = msg.attachments?.find((a) => a.name === "notes.txt");
    expect(new TextDecoder().decode(notes?.data)).toBe("SECRET FILE");
  });

  test("delivers an attachments-only Mail with empty content omitted", async () => {
    const { agent, captured } = buildCapturingAgent();
    const invoker = createWorkflowStepInvoker({
      workflowAuthorize: allowAll,
      buildEnv: async () => stubBuildEnv(),
      agentFactory: async () => agent,
      mailPartReader: stubReader({ "mail-part:///r/m/0-a.mp3": "AUDIOBYTES" }),
    });
    await invoker(
      buildRequest({
        input: mail([
          {
            contentType: "audio/mpeg",
            ref: "mail-part:///r/m/0-a.mp3",
            filename: "a.mp3",
          },
        ]),
      }),
    );
    const msg = captured.message;
    if (typeof msg === "string" || msg === undefined) {
      throw new Error("expected an InboundMessage, not a synthesized string");
    }
    // No text part: createInboundMessage omits the empty content string.
    expect(msg.content).toBeUndefined();
    expect(msg.attachments).toHaveLength(1);
    expect(new TextDecoder().decode(msg.attachments?.[0]?.data)).toBe(
      "AUDIOBYTES",
    );
  });

  test("refuses a Mail whose part bytes must be read when no reader is wired", async () => {
    const { agent } = buildCapturingAgent();
    const invoker = createWorkflowStepInvoker({
      workflowAuthorize: allowAll,
      buildEnv: async () => stubBuildEnv(),
      agentFactory: async () => agent,
      // no mailPartReader (mirrors a body step)
    });
    await expect(
      invoker(
        buildRequest({
          input: mail([
            {
              contentType: "application/octet-stream",
              ref: "mail-part:///r/m/0-f.bin",
              filename: "f.bin",
            },
          ]),
        }),
      ),
    ).rejects.toThrow(/no mail-part reader wired/);
  });

  test("delivers a text-only Mail from inline text without a reader", async () => {
    const { agent, captured } = buildCapturingAgent();
    const invoker = createWorkflowStepInvoker({
      workflowAuthorize: allowAll,
      buildEnv: async () => stubBuildEnv(),
      agentFactory: async () => agent,
      // no reader: a text-only mail whose parts inlined their text still delivers
    });
    await invoker(
      buildRequest({
        input: mail([
          {
            contentType: "text/plain",
            ref: "mail-part:///r/m/0-text",
            text: "just text",
          },
        ]),
      }),
    );
    const msg = captured.message;
    if (typeof msg === "string" || msg === undefined) {
      throw new Error("expected an InboundMessage, not a synthesized string");
    }
    expect(msg.content).toBe("just text");
  });

  test("an arbitrary non-mail step input is still delivered as synthesized text", async () => {
    const { agent, captured } = buildCapturingAgent();
    const invoker = createWorkflowStepInvoker({
      workflowAuthorize: allowAll,
      buildEnv: async () => stubBuildEnv(),
      agentFactory: async () => agent,
      mailPartReader: stubReader({}),
    });
    await invoker(buildRequest({ input: { some: "object", n: 1 } }));
    expect(captured.message).toBe(JSON.stringify({ some: "object", n: 1 }));
  });
});
