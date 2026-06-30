import { describe, test, expect } from "bun:test";

import {
  defineAgent,
  defineTool,
  type AgentDefinition,
  type BaseEnv,
} from "@intx/agent";

import {
  awaitSignal,
  defineWorkflow,
  hashDefinition,
  map,
  sleep,
  step,
  type WorkflowDefinition,
} from "./index";

function makeAgent(id: string): AgentDefinition<BaseEnv> {
  return defineAgent({
    id,
    systemPrompt: "you are " + id,
    tools: [],
    capabilities: [],
    inference: {
      sources: [{ provider: "fake", model: "fake" }],
    },
  });
}

describe("defineWorkflow", () => {
  test("rejects an empty steps record", () => {
    expect(() =>
      defineWorkflow({ id: "w", trigger: { type: "manual" }, steps: {} }),
    ).toThrow(/at least one step/);
  });

  test("populates step ids from record keys", () => {
    const planner = makeAgent("planner");
    const def = defineWorkflow({
      id: "w",
      trigger: { type: "manual" },
      steps: { plan: step({ agent: planner }) },
    });
    expect(def.steps.plan?.id).toBe("plan");
  });

  test("applies the default-input convention", () => {
    const planner = makeAgent("planner");
    const impl = makeAgent("impl");
    const def = defineWorkflow({
      id: "w",
      trigger: { type: "manual" },
      steps: {
        plan: step({ agent: planner }),
        impl: step({ agent: impl, after: ["plan"] }),
      },
    });
    const planStep = def.steps.plan;
    const implStep = def.steps.impl;
    expect(planStep?.kind === "step" ? planStep.input : undefined).toEqual({
      from: "trigger.payload",
    });
    expect(implStep?.kind === "step" ? implStep.input : undefined).toEqual({
      from: "steps.plan.output",
    });
  });

  test("singular shorthand deep-equals the plural form", () => {
    const planner = makeAgent("planner");
    const singular = defineWorkflow({
      id: "w",
      agent: planner,
      trigger: { type: "mail", to: "p@x" },
    });
    const plural = defineWorkflow({
      id: "w",
      trigger: { type: "mail", to: "p@x" },
      steps: { default: step({ agent: planner }) },
    });
    expect(singular).toEqual(plural);
  });

  test("validates after references against the steps record", () => {
    const a = makeAgent("a");
    expect(() =>
      defineWorkflow({
        id: "w",
        trigger: { type: "manual" },
        steps: { a: step({ agent: a, after: ["b"] }) },
      }),
    ).toThrow(/after b which is not a known step/);
  });

  test("rejects self-referencing after", () => {
    const a = makeAgent("a");
    expect(() =>
      defineWorkflow({
        id: "w",
        trigger: { type: "manual" },
        steps: { a: step({ agent: a, after: ["a"] }) },
      }),
    ).toThrow(/cannot depend on itself/);
  });

  test("rejects both trigger and triggers supplied", () => {
    const a = makeAgent("a");
    expect(() =>
      defineWorkflow({
        id: "w",
        trigger: { type: "manual" },
        triggers: [{ type: "manual" }],
        steps: { a: step({ agent: a }) },
      }),
    ).toThrow(/not both/);
  });

  test("defaults to a single manual trigger when none supplied", () => {
    const a = makeAgent("a");
    const def = defineWorkflow({
      id: "w",
      steps: { a: step({ agent: a }) },
    });
    expect(def.triggers).toEqual([{ type: "manual" }]);
  });
});

describe("primitive defaults", () => {
  test("step defaults drainBehavior to cancel", () => {
    const s = step({ agent: makeAgent("a") });
    expect(s.drainBehavior).toBe("cancel");
  });

  test("awaitSignal defaults drainBehavior to wait", () => {
    const s = awaitSignal({ name: "approve" });
    expect(s.drainBehavior).toBe("wait");
  });

  test("sleep defaults drainBehavior to cancel and requires one of duration/until", () => {
    const s = sleep({ duration: 1000 });
    expect(s.drainBehavior).toBe("cancel");
    expect(() => sleep({})).toThrow(/duration.*until/);
    expect(() => sleep({ duration: 1000, until: "2026-01-01" })).toThrow(
      /at most one/,
    );
  });

  test("map preserves the inner step's drainBehavior independently", () => {
    const inner = step({ agent: makeAgent("a"), drainBehavior: "wait" });
    const m = map({ over: { from: "trigger.payload" }, step: inner });
    expect(m.step.drainBehavior).toBe("wait");
  });
});

describe("hashDefinition", () => {
  test("produces stable bytes for a definition", () => {
    const a = makeAgent("a");
    const def: WorkflowDefinition = defineWorkflow({
      id: "w",
      trigger: { type: "manual" },
      steps: { a: step({ agent: a }) },
    });
    const h1 = hashDefinition(def);
    const h2 = hashDefinition(def);
    expect(h1).toEqual(h2);
  });

  test("hashes a definition whose agent carries tool factories", () => {
    // Tool factories are functions; `canonicalizeForHash` rejects
    // function values directly. The projection layer in workflow.ts
    // must extract the factory metadata (id, requires) and discard
    // the function before canonicalization. Without that projection,
    // any non-trivial production workflow would fail to hash and
    // crash `RunStarted` emission inside `runtimeRun`.
    const tool = defineTool({
      id: "@x/y/echo",
      factory: () => ({
        definitions: [],
        run: async (call) => ({ callId: call.id, content: "" }),
      }),
    });
    const a = defineAgent({
      id: "with-tool",
      systemPrompt: "you are a",
      tools: [tool],
      capabilities: [],
      inference: { sources: [{ provider: "fake", model: "fake" }] },
    });
    const def: WorkflowDefinition = defineWorkflow({
      id: "wt",
      trigger: { type: "manual" },
      steps: { a: step({ agent: a }) },
    });
    expect(() => hashDefinition(def)).not.toThrow();
  });
});
