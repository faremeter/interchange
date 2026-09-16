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

import { describe, test, expect } from "bun:test";

import { defineAgent } from "@intx/agent";

import {
  childWorkflow,
  defineWorkflow,
  runLocal,
  step,
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

    const result = await runLocal(parentWithChild, { authorize }).complete;

    expect(result.terminalStatus).toBe("completed");
    expect(resources).toEqual(["tool:child-agent"]);
  });

  test("a deny-all authorize fails the spawned child's step", async () => {
    const resources: string[] = [];
    const authorize: WorkflowAuthorizeFn = async (resource) => {
      resources.push(resource);
      return { effect: "deny", matchingGrants: [], resolvedBy: null };
    };

    const result = await runLocal(parentWithChild, { authorize }).complete;

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

    const result = await runLocal(soloStep, { authorize }).complete;

    expect(result.terminalStatus).toBe("failed");
  });

  test("an unresolved authorize decision fails a top-level step", async () => {
    const authorize: WorkflowAuthorizeFn = async () => ({
      effect: null,
      matchingGrants: [],
      resolvedBy: null,
    });

    const result = await runLocal(soloStep, { authorize }).complete;

    expect(result.terminalStatus).toBe("failed");
  });

  test("an allow decision completes a top-level step", async () => {
    const result = await runLocal(soloStep, { authorize: allow }).complete;

    expect(result.terminalStatus).toBe("completed");
  });
});
