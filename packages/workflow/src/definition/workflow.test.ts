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
  gate,
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

describe("acyclicity validation", () => {
  test("rejects a gate whose branch names an ancestor (F2 back-edge)", () => {
    // G runs after A, and G's then-branch points back at A. This is a
    // cycle only in the after-union-gate graph; a pure-after check would
    // accept it and the runtime would silently run the wrong branch.
    const a = makeAgent("a");
    const e = makeAgent("e");
    const edown = makeAgent("edown");
    expect(() =>
      defineWorkflow({
        id: "w",
        trigger: { type: "manual" },
        steps: {
          A: step({ agent: a }),
          G: gate({
            when: { from: "steps.A.output" },
            then: "A",
            else: "E",
            after: ["A"],
          }),
          E: step({ agent: e, after: ["G"] }),
          Edown: step({ agent: edown, after: ["E"] }),
        },
      }),
    ).toThrow(/dependency cycle/);
  });

  test("rejects a transitive after cycle and names the path", () => {
    const a = makeAgent("a");
    const b = makeAgent("b");
    const c = makeAgent("c");
    expect(() =>
      defineWorkflow({
        id: "w",
        trigger: { type: "manual" },
        steps: {
          a: step({ agent: a, after: ["c"] }),
          b: step({ agent: b, after: ["a"] }),
          c: step({ agent: c, after: ["b"] }),
        },
      }),
    ).toThrow(/dependency cycle: .*->.*/);
  });

  test("rejects a two-node cycle that the self-check does not catch", () => {
    // validateAfterRefs only rejects a step depending on itself; a
    // two-node cycle is the minimal case that validateAcyclic owns.
    const x = makeAgent("x");
    const y = makeAgent("y");
    expect(() =>
      defineWorkflow({
        id: "w",
        trigger: { type: "manual" },
        steps: {
          x: step({ agent: x, after: ["y"] }),
          y: step({ agent: y, after: ["x"] }),
        },
      }),
    ).toThrow(/dependency cycle/);
  });

  test("accepts a diamond join (gate branches reconverge)", () => {
    const plan = makeAgent("plan");
    const x = makeAgent("x");
    const y = makeAgent("y");
    const j = makeAgent("j");
    expect(() =>
      defineWorkflow({
        id: "w",
        trigger: { type: "manual" },
        steps: {
          plan: step({ agent: plan }),
          decide: gate({
            when: { from: "steps.plan.output" },
            then: "x",
            else: "y",
            after: ["plan"],
          }),
          x: step({ agent: x, after: ["decide"] }),
          y: step({ agent: y, after: ["decide"] }),
          join: step({ agent: j, after: ["x", "y"] }),
        },
      }),
    ).not.toThrow();
  });

  test("accepts two gates sharing a downstream target", () => {
    const p = makeAgent("p");
    const shared = makeAgent("shared");
    const t1 = makeAgent("t1");
    const t2 = makeAgent("t2");
    expect(() =>
      defineWorkflow({
        id: "w",
        trigger: { type: "manual" },
        steps: {
          p: step({ agent: p }),
          g1: gate({
            when: { from: "steps.p.output" },
            then: "shared",
            else: "t1",
            after: ["p"],
          }),
          g2: gate({
            when: { from: "steps.p.output" },
            then: "shared",
            else: "t2",
            after: ["p"],
          }),
          shared: step({ agent: shared, after: ["g1", "g2"] }),
          t1: step({ agent: t1, after: ["g1"] }),
          t2: step({ agent: t2, after: ["g2"] }),
        },
      }),
    ).not.toThrow();
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
