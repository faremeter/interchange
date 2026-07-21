import { describe, test, expect } from "bun:test";

import {
  defineAgent,
  defineTool,
  type AgentDefinition,
  type BaseEnv,
} from "@intx/agent";

import {
  action,
  awaitSignal,
  childWorkflow,
  defineWorkflow,
  gate,
  hashDefinition,
  loop,
  map,
  sleep,
  step,
  type WorkflowDefinition,
} from "./index";

function simpleBody(): WorkflowDefinition {
  return defineWorkflow({
    id: "body",
    trigger: { type: "manual" },
    steps: { work: step({ agent: makeAgent("w") }) },
  });
}

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

  test("round-trips declared grant requirements", () => {
    const def = defineWorkflow({
      id: "w",
      trigger: { type: "manual" },
      steps: { plan: step({ agent: makeAgent("planner") }) },
      grantRequirements: [
        { resource: "credential:openai", action: "use", source: "creator" },
        {
          resource: "tool:search",
          action: "invoke",
          effect: "ask",
          source: "invoker",
        },
      ],
    });
    expect(def.grantRequirements).toEqual([
      { resource: "credential:openai", action: "use", source: "creator" },
      {
        resource: "tool:search",
        action: "invoke",
        effect: "ask",
        source: "invoker",
      },
    ]);
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

describe("loop validation", () => {
  test("accepts a loop with a valid body and onExhausted target", () => {
    expect(() =>
      defineWorkflow({
        id: "w",
        trigger: { type: "manual" },
        steps: {
          rework: loop({
            body: simpleBody(),
            while: "shouldContinue",
            carry: "next",
            maxIterations: 3,
            onExhausted: "escalate",
          }),
          escalate: step({ agent: makeAgent("e"), after: ["rework"] }),
        },
      }),
    ).not.toThrow();
  });

  test("loop rejects a non-positive-integer maxIterations", () => {
    for (const bad of [0, -1, 2.5, Number.NaN, Number.POSITIVE_INFINITY]) {
      expect(() =>
        loop({
          body: simpleBody(),
          while: "w",
          carry: "c",
          maxIterations: bad,
          onExhausted: "e",
        }),
      ).toThrow(/positive integer maxIterations/);
    }
  });

  test("allows a loop body containing map, action, or gate primitives", () => {
    const bodies: WorkflowDefinition[] = [
      defineWorkflow({
        id: "map-body",
        trigger: { type: "manual" },
        steps: {
          m: map({
            over: { from: "trigger.payload" },
            step: step({ agent: makeAgent("i") }),
          }),
        },
      }),
      defineWorkflow({
        id: "action-body",
        trigger: { type: "manual" },
        steps: { a: action({ handler: "h" }) },
      }),
      defineWorkflow({
        id: "gate-body",
        trigger: { type: "manual" },
        steps: {
          g: gate({ when: { from: "trigger.payload" }, then: "x", else: "y" }),
          x: step({ agent: makeAgent("x") }),
          y: step({ agent: makeAgent("y") }),
        },
      }),
    ];
    for (const body of bodies) {
      expect(() =>
        defineWorkflow({
          id: "w",
          trigger: { type: "manual" },
          steps: {
            rework: loop({
              body,
              while: "w",
              carry: "c",
              maxIterations: 2,
              onExhausted: "esc",
            }),
            esc: step({ agent: makeAgent("e"), after: ["rework"] }),
          },
        }),
      ).not.toThrow();
    }
  });

  test("rejects a loop whose onExhausted does not depend on the loop", () => {
    // onExhausted routes only on exhaustion, so it must name the loop in
    // its after; otherwise it would be schedulable from RunStarted and
    // fire on every run. Naming an ancestor (no after: [loop]) is the
    // canonical way this goes wrong.
    expect(() =>
      defineWorkflow({
        id: "w",
        trigger: { type: "manual" },
        steps: {
          seed: step({ agent: makeAgent("s") }),
          rework: loop({
            body: simpleBody(),
            while: "w",
            carry: "c",
            maxIterations: 2,
            onExhausted: "seed",
            after: ["seed"],
          }),
        },
      }),
    ).toThrow(/must name rework in its after/);
  });

  test("a loop's definition hash reflects its body content", () => {
    const withBodyAgent = (agentId: string) =>
      defineWorkflow({
        id: "w",
        trigger: { type: "manual" },
        steps: {
          rework: loop({
            body: defineWorkflow({
              id: "body",
              trigger: { type: "manual" },
              steps: { work: step({ agent: makeAgent(agentId) }) },
            }),
            while: "w",
            carry: "c",
            maxIterations: 2,
            onExhausted: "esc",
          }),
          esc: step({ agent: makeAgent("e"), after: ["rework"] }),
        },
      });
    expect(hashDefinition(withBodyAgent("a"))).not.toEqual(
      hashDefinition(withBodyAgent("b")),
    );
  });

  test("rejects a loop whose onExhausted is not a known step", () => {
    expect(() =>
      defineWorkflow({
        id: "w",
        trigger: { type: "manual" },
        steps: {
          rework: loop({
            body: simpleBody(),
            while: "w",
            carry: "c",
            maxIterations: 2,
            onExhausted: "nope",
          }),
        },
      }),
    ).toThrow(/onExhausted nope which is not a known step/);
  });

  test("rejects a loop whose body contains a nested loop", () => {
    const nestedBody = defineWorkflow({
      id: "nested-body",
      trigger: { type: "manual" },
      steps: {
        inner: loop({
          body: simpleBody(),
          while: "w",
          carry: "c",
          maxIterations: 2,
          onExhausted: "end",
        }),
        end: step({ agent: makeAgent("end"), after: ["inner"] }),
      },
    });
    expect(() =>
      defineWorkflow({
        id: "w",
        trigger: { type: "manual" },
        steps: {
          outer: loop({
            body: nestedBody,
            while: "w",
            carry: "c",
            maxIterations: 2,
            onExhausted: "esc",
          }),
          esc: step({ agent: makeAgent("esc"), after: ["outer"] }),
        },
      }),
    ).toThrow(/may not contain a loop/);
  });

  test("rejects a loop body containing awaitSignal, sleep, or childWorkflow", () => {
    const forbiddenBodies: WorkflowDefinition[] = [
      defineWorkflow({
        id: "await-body",
        trigger: { type: "manual" },
        steps: { wait: awaitSignal({ name: "go" }) },
      }),
      defineWorkflow({
        id: "sleep-body",
        trigger: { type: "manual" },
        steps: { nap: sleep({ duration: 10 }) },
      }),
      defineWorkflow({
        id: "child-body",
        trigger: { type: "manual" },
        steps: { sub: childWorkflow({ definitionRef: "x" }) },
      }),
    ];
    for (const body of forbiddenBodies) {
      expect(() =>
        defineWorkflow({
          id: "w",
          trigger: { type: "manual" },
          steps: {
            rework: loop({
              body,
              while: "w",
              carry: "c",
              maxIterations: 2,
              onExhausted: "esc",
            }),
            esc: step({ agent: makeAgent("e"), after: ["rework"] }),
          },
        }),
      ).toThrow(/a loop body may not contain/);
    }
  });

  test("hashes a definition with an inline loop body", () => {
    const def = defineWorkflow({
      id: "w",
      trigger: { type: "manual" },
      steps: {
        rework: loop({
          body: simpleBody(),
          while: "w",
          carry: "c",
          maxIterations: 2,
          onExhausted: "esc",
        }),
        esc: step({ agent: makeAgent("e"), after: ["rework"] }),
      },
    });
    expect(() => hashDefinition(def)).not.toThrow();
    expect(hashDefinition(def)).toEqual(hashDefinition(def));
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
      definitions: [],
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

  test("declared grant requirements change the content hash", () => {
    const a = makeAgent("a");
    const base: WorkflowDefinition = defineWorkflow({
      id: "w",
      trigger: { type: "manual" },
      steps: { a: step({ agent: a }) },
    });
    const withGrants: WorkflowDefinition = defineWorkflow({
      id: "w",
      trigger: { type: "manual" },
      steps: { a: step({ agent: a }) },
      grantRequirements: [
        { resource: "tool:search", action: "invoke", source: "invoker" },
      ],
    });
    expect(hashDefinition(withGrants)).not.toEqual(hashDefinition(base));
  });
});
