// runLocal env fidelity: a local run must model the deployed surface.
//
// Two invariants live here.
//
// A spawned `childWorkflow` inherits the env overrides its parent run was
// given. Production builds the child's env from the parent's -- the same step
// invoker, the same authorize (capped to the child's declared resources), the
// same director registry -- so a local child that reverted to the permissive
// defaults would let a strict env pass a test whose child never saw it. The
// assertions here are positive for that reason: they require the injected
// authorize to have been consulted and the injected invoker to have produced
// the child's output, because a dropped override leaves a green run behind.
//
// The default stub step invoker fails closed on a non-allow decision, the same
// posture `createEffectContext` takes for an action effect, so an
// authorization failure is observable locally instead of surfacing only after
// deploy.
//
// An `actionCredentials` factory reaches every action the local run tree
// executes: top level, loop bodies, and spawned children. Without one, an
// action that declares credentials fails before its handler runs.

import { describe, test, expect } from "bun:test";

import { defineAgent } from "@intx/agent";

import type { MediatedCredential } from "@intx/types";
import {
  action,
  childWorkflow,
  defineWorkflow,
  loop,
  runLocal,
  step,
  type ActionCredentialsFactory,
  type ActionHandler,
  type ActionInvoker,
  type StepInvoker,
  type WorkflowAuthorizeFn,
} from "@intx/workflow";

function makeAgent(id: string) {
  return defineAgent({
    id,
    systemPrompt: "s",
    tools: [],
    capabilities: [],
    inference: { sources: [{ provider: "fake", model: "fake" }] },
  });
}

const allow: WorkflowAuthorizeFn = async () => ({
  effect: "allow",
  matchingGrants: [],
  resolvedBy: null,
});

const childDefinition = defineWorkflow({
  id: "env-fidelity-child",
  trigger: { type: "manual" },
  steps: { work: step({ agent: makeAgent("child-agent") }) },
});

const parentWithChild = defineWorkflow({
  id: "env-fidelity-parent",
  trigger: { type: "manual" },
  steps: { spawn: childWorkflow({ definition: childDefinition }) },
});

const soloStep = defineWorkflow({
  id: "env-fidelity-solo",
  trigger: { type: "manual" },
  steps: { work: step({ agent: makeAgent("solo-agent") }) },
});

describe("runLocal childWorkflow env inheritance", () => {
  test("a spawned child runs its steps through the injected step invoker", async () => {
    const invoked: string[] = [];
    const invokeStep: StepInvoker = async ({ agent }) => {
      invoked.push(agent.id);
      return { output: `output-from-${agent.id}` };
    };

    const result = await runLocal(parentWithChild, {
      authorize: allow,
      hasUpstreamSignalResolver: true,
      invokeStep,
    }).complete;

    expect(result.terminalStatus).toBe("completed");
    expect(invoked).toEqual(["child-agent"]);
  });

  test("a spawned child consults the injected authorize", async () => {
    const resources: string[] = [];
    const authorize: WorkflowAuthorizeFn = async (resource) => {
      resources.push(resource);
      return { effect: "allow", matchingGrants: [], resolvedBy: null };
    };

    const result = await runLocal(parentWithChild, {
      authorize,
      hasUpstreamSignalResolver: true,
    }).complete;

    expect(result.terminalStatus).toBe("completed");
    expect(resources).toEqual(["tool:child-agent"]);
  });

  test("a deny-all authorize fails the spawned child's step", async () => {
    const resources: string[] = [];
    const authorize: WorkflowAuthorizeFn = async (resource) => {
      resources.push(resource);
      return { effect: "deny", matchingGrants: [], resolvedBy: null };
    };

    const result = await runLocal(parentWithChild, {
      authorize,
      hasUpstreamSignalResolver: true,
    }).complete;

    expect(resources).toContain("tool:child-agent");
    expect(result.terminalStatus).toBe("failed");
  });
});

describe("runLocal default step invoker authorization", () => {
  test("a deny-all authorize fails a top-level step", async () => {
    const authorize: WorkflowAuthorizeFn = async () => ({
      effect: "deny",
      matchingGrants: [],
      resolvedBy: null,
    });

    const result = await runLocal(soloStep, {
      authorize,
      hasUpstreamSignalResolver: true,
    }).complete;

    expect(result.terminalStatus).toBe("failed");
  });

  test("an unresolved authorize decision fails a top-level step", async () => {
    const authorize: WorkflowAuthorizeFn = async () => ({
      effect: null,
      matchingGrants: [],
      resolvedBy: null,
    });

    const result = await runLocal(soloStep, {
      authorize,
      hasUpstreamSignalResolver: true,
    }).complete;

    expect(result.terminalStatus).toBe("failed");
  });

  test("an allow decision completes a top-level step", async () => {
    const result = await runLocal(soloStep, {
      authorize: allow,
      hasUpstreamSignalResolver: true,
    }).complete;

    expect(result.terminalStatus).toBe("completed");
  });
});

describe("runLocal action credentials", () => {
  const handle: MediatedCredential = {
    kind: "http",
    fetch: () => Promise.resolve(new Response("ok")),
    dispose() {
      /* the fake handle holds nothing to release */
    },
  };

  // A factory that records which handler each invocation asked for.
  function recordingFactory(): {
    factory: ActionCredentialsFactory;
    handlers: string[];
  } {
    const handlers: string[] = [];
    return {
      handlers,
      factory: ({ handler }) => {
        handlers.push(handler);
        return {
          resolve: () => Promise.resolve(handle),
          dispose: () => Promise.resolve(),
        };
      },
    };
  }

  // Resolve `gh` inside one `perform` and return the handle's kind.
  const resolving: ActionHandler = (_input, ctx) =>
    ctx.perform({
      effectId: "resolve",
      capability: "call",
      run: async ({ credentials }) => (await credentials.resolve("gh")).kind,
    });

  const declaring = (handler: string, after?: readonly string[]) =>
    action({
      handler,
      credentials: ["gh"],
      effect: { requires: ["call"] },
      ...(after !== undefined ? { after } : {}),
    });

  const tree = defineWorkflow({
    id: "local-credentials-tree",
    trigger: { type: "manual" },
    steps: {
      top: declaring("top"),
      rework: loop({
        body: defineWorkflow({
          id: "local-credentials-loop-body",
          trigger: { type: "manual" },
          steps: { inLoop: declaring("inLoop") },
        }),
        while: "stop",
        carry: "keep",
        maxIterations: 2,
        onExhausted: "done",
      }),
      done: action({ handler: "done", after: ["rework"] }),
      spawn: childWorkflow({
        definition: defineWorkflow({
          id: "local-credentials-child",
          trigger: { type: "manual" },
          steps: { inChild: declaring("inChild") },
        }),
      }),
    },
  });

  const options = {
    authorize: allow,
    hasUpstreamSignalResolver: true,
    actionResolver: (ref: string): ActionHandler =>
      ref === "done" ? async () => null : resolving,
    loopFns: (ref: string) =>
      ref === "stop"
        ? () => false
        : (_output: unknown, input: unknown) => input,
  };

  test("the factory builds credentials for top-level, loop-body and child actions", async () => {
    const { factory, handlers } = recordingFactory();
    const result = await runLocal(tree, {
      ...options,
      actionCredentials: factory,
    }).complete;

    expect(result.terminalStatus).toBe("completed");
    // The loop converges, so its onExhausted target `done` is skipped.
    expect([...handlers].sort()).toEqual(["inChild", "inLoop", "top"]);
  });

  test("without a factory, an action that declares credentials fails", async () => {
    const result = await runLocal(
      defineWorkflow({
        id: "local-credentials-none",
        trigger: { type: "manual" },
        steps: { top: declaring("top") },
      }),
      options,
    ).complete;

    expect(result.terminalStatus).toBe("failed");
    const failure = result.events.find((event) => event.kind === "StepFailed");
    if (failure === undefined || failure.kind !== "StepFailed") {
      throw new Error("expected the top step to fail");
    }
    expect(failure.error.message).toContain(
      "action top declares credential handle(s) gh, but no credential source is configured",
    );
  });

  test("a factory together with a custom action-runner is refused", () => {
    const invokeAction: ActionInvoker = async () => ({ output: null });
    expect(() =>
      runLocal(tree, {
        ...options,
        invokeAction,
        actionCredentials: recordingFactory().factory,
      }),
    ).toThrow(
      "runLocal: actionCredentials applies only to the default action-runner",
    );
  });
});
