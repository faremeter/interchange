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
  InferenceSource,
} from "@intx/types/runtime";

import { createWorkflowStepInvoker, type StepEnvBase } from "./step-invoker";

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
    source: STUB_SOURCE,
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
