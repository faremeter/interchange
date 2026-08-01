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
  onTrigger,
  sleep,
  step,
  stepTriggerBudget,
  validateRetryTriggerCombination,
  type Primitive,
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

describe("onTrigger primitive", () => {
  test("constructor carries on/body and defaults drainBehavior to wait", () => {
    const body = simpleBody();
    const prim = onTrigger({ on: { type: "mail", to: "s@x.example" }, body });
    expect(prim.kind).toBe("onTrigger");
    expect(prim.on).toEqual({ type: "mail", to: "s@x.example" });
    // Authored inline: the constructor wraps the WorkflowDefinition as
    // `{ inline }`; deploy later rewrites it to `{ ref }`.
    expect(prim.body).toEqual({ inline: body });
    expect(prim.drainBehavior).toBe("wait");
    // defineWorkflow, not the constructor, assigns the id from the record key.
    expect(prim.id).toBe("");
  });

  test("honors an explicit drainBehavior and after", () => {
    const prim = onTrigger({
      on: { type: "manual" },
      body: simpleBody(),
      drainBehavior: "cancel",
      after: ["setup"],
    });
    expect(prim.drainBehavior).toBe("cancel");
    expect(prim.after).toEqual(["setup"]);
  });

  test("defineWorkflow populates the section id from its record key", () => {
    const def = defineWorkflow({
      id: "wf",
      trigger: { type: "manual" },
      steps: {
        section: onTrigger({
          on: { type: "mail", to: "s@x.example" },
          body: simpleBody(),
        }),
      },
    });
    const section = def.steps.section;
    expect(section?.kind).toBe("onTrigger");
    expect(section?.id).toBe("section");
  });

  test("accepts a deployed ref-form section body, skipping its validation", () => {
    // The deploy step rewrites the inline body to a `{ ref }` arm; the
    // referenced body was validated at its own deploy, so defineWorkflow
    // must accept the ref without descending into (absent) inline steps.
    const section: Primitive = {
      kind: "onTrigger",
      id: "",
      on: { type: "manual" },
      body: { ref: "body-asset-ref" },
      drainBehavior: "wait",
    };
    const def = defineWorkflow({ id: "wf", steps: { section } });
    expect(def.steps.section?.kind).toBe("onTrigger");
    expect(def.triggers).toEqual([{ type: "manual" }]);
  });

  test("collects each section's `on` into the workflow triggers", () => {
    const def = defineWorkflow({
      id: "wf",
      steps: {
        section: onTrigger({
          on: { type: "mail", to: "s@x.example" },
          body: simpleBody(),
        }),
      },
    });
    expect(def.triggers).toEqual([{ type: "mail", to: "s@x.example" }]);
  });

  test("dedupes a section `on` that restates a declared trigger", () => {
    const def = defineWorkflow({
      id: "wf",
      trigger: { type: "mail", to: "s@x.example" },
      steps: {
        section: onTrigger({
          on: { type: "mail", to: "s@x.example" },
          body: simpleBody(),
        }),
      },
    });
    expect(def.triggers).toEqual([{ type: "mail", to: "s@x.example" }]);
  });

  test("a section body may contain an awaitSignal, unlike a loop body", () => {
    const body = defineWorkflow({
      id: "body",
      trigger: { type: "manual" },
      steps: { hold: awaitSignal({ name: "go" }) },
    });
    // Does not throw: an onTrigger body is the sanctioned long-lived loop.
    defineWorkflow({
      id: "wf",
      steps: { section: onTrigger({ on: { type: "manual" }, body }) },
    });
  });

  test("rejects a section body that nests another onTrigger", () => {
    const inner = defineWorkflow({
      id: "inner",
      steps: {
        nested: onTrigger({ on: { type: "manual" }, body: simpleBody() }),
      },
    });
    expect(() =>
      defineWorkflow({
        id: "wf",
        steps: { section: onTrigger({ on: { type: "manual" }, body: inner }) },
      }),
    ).toThrow(/may not nest another section/);
  });

  test("rejects a loop body that contains an onTrigger section", () => {
    const body = defineWorkflow({
      id: "body",
      steps: {
        section: onTrigger({ on: { type: "manual" }, body: simpleBody() }),
      },
    });
    expect(() =>
      defineWorkflow({
        id: "wf",
        trigger: { type: "manual" },
        steps: {
          l: loop({
            body,
            while: "whileFn",
            carry: "carryFn",
            maxIterations: 2,
            onExhausted: "done",
          }),
          done: step({ agent: makeAgent("d"), after: ["l"] }),
        },
      }),
    ).toThrow(/onTrigger/);
  });
});

describe("step triggers budget", () => {
  test("defaults to 1 when unspecified", () => {
    expect(stepTriggerBudget(step({ agent: makeAgent("a") }))).toBe(1);
  });

  test("carries a declared finite budget", () => {
    expect(
      stepTriggerBudget(step({ agent: makeAgent("a"), triggers: 5 })),
    ).toBe(5);
  });

  test("carries the unbounded budget", () => {
    expect(
      stepTriggerBudget(step({ agent: makeAgent("a"), triggers: "unbounded" })),
    ).toBe("unbounded");
  });

  test("rejects a non-positive or fractional trigger count", () => {
    expect(() => step({ agent: makeAgent("a"), triggers: 0 })).toThrow(
      /positive integer or "unbounded"/,
    );
    expect(() => step({ agent: makeAgent("a"), triggers: -1 })).toThrow(
      /positive integer or "unbounded"/,
    );
    expect(() => step({ agent: makeAgent("a"), triggers: 1.5 })).toThrow(
      /positive integer or "unbounded"/,
    );
  });

  test("rejects an invalid trigger count at the read point too", () => {
    // A definition hydrated from workflow.json never passes through `step()`
    // (the envelope schema checks structure only), so `stepTriggerBudget` --
    // the single read point -- must fail loud on a persisted invalid value
    // rather than silently coercing (0 would behave as 1; 1.5 would service
    // an extra trigger). Build the primitive directly, as hydration does.
    const hydrated = { ...step({ agent: makeAgent("a") }), triggers: 0 };
    expect(() => stepTriggerBudget(hydrated)).toThrow(
      /positive integer or "unbounded"/,
    );
    const fractional = { ...step({ agent: makeAgent("a") }), triggers: 1.5 };
    expect(() => stepTriggerBudget(fractional)).toThrow(
      /positive integer or "unbounded"/,
    );
  });

  test("rejects a retry policy on a multi-trigger step", () => {
    // A retried attempt re-invokes the step with its launch input and starts
    // with no resume, so on a step with a trigger budget other than 1 a
    // mid-run failure would re-service the launch trigger and never
    // re-service the consumed one -- the combination fails loud.
    const retry = { maxAttempts: 2, initialBackoffMs: 100 };
    expect(() => step({ agent: makeAgent("a"), triggers: 3, retry })).toThrow(
      /cannot combine with a trigger budget/,
    );
    expect(() =>
      step({ agent: makeAgent("a"), triggers: "unbounded", retry }),
    ).toThrow(/cannot combine with a trigger budget/);
    // A batch step retries fine: re-invoking with the launch input IS the
    // retry semantics for a single trigger.
    step({ agent: makeAgent("a"), retry });
    step({ agent: makeAgent("a"), triggers: 1, retry });
    // A declared maxAttempts of 1 never retries, so it combines with any
    // budget.
    step({
      agent: makeAgent("a"),
      triggers: "unbounded",
      retry: { maxAttempts: 1, initialBackoffMs: 100 },
    });
  });

  test("rejects the retry/budget combination at the read point too", () => {
    // Hydrated definitions never pass through `step()`, so the runtime's
    // read-point guard (applied at runStep entry) must reject the persisted
    // combination. Build the primitive directly, as hydration does.
    const hydrated = {
      ...step({ agent: makeAgent("a"), triggers: 3 }),
      retry: { maxAttempts: 2, initialBackoffMs: 100 },
    };
    expect(() => validateRetryTriggerCombination(hydrated)).toThrow(
      /cannot combine with a trigger budget/,
    );
  });

  test("rejects a map-level retry over a multi-trigger inner step", () => {
    // The map's retry applies to each fan-out instance of an inner step that
    // declares none, so `map()` must validate the COMPOSED shape -- `step()`
    // alone never sees a map-level retry, and without the map-side check the
    // forbidden combination would surface only at the run's first execution.
    const retry = { maxAttempts: 2, initialBackoffMs: 100 };
    expect(() =>
      map({
        over: { from: "trigger.payload" },
        step: step({ agent: makeAgent("a"), triggers: 2 }),
        retry,
      }),
    ).toThrow(/cannot combine with a trigger budget/);
    // The inner step's OWN retry wins over the map's, so a budget-1 inner
    // step with its own retry composes fine under a map-level retry...
    map({
      over: { from: "trigger.payload" },
      step: step({ agent: makeAgent("a"), retry }),
      retry,
    });
    // ...and a multi-trigger inner step is fine when no retry reaches it.
    map({
      over: { from: "trigger.payload" },
      step: step({ agent: makeAgent("a"), triggers: 2 }),
    });
  });

  test("triggers participates in the definition hash", () => {
    const one = defineWorkflow({
      id: "w",
      trigger: { type: "manual" },
      steps: { s: step({ agent: makeAgent("a") }) },
    });
    const unbounded = defineWorkflow({
      id: "w",
      trigger: { type: "manual" },
      steps: { s: step({ agent: makeAgent("a"), triggers: "unbounded" }) },
    });
    expect(hashDefinition(one)).not.toEqual(hashDefinition(unbounded));
  });
});

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
  test("step defaults drainBehavior to cancel (batch)", () => {
    const s = step({ agent: makeAgent("a") });
    expect(s.drainBehavior).toBe("cancel");
    const explicitOne = step({ agent: makeAgent("a"), triggers: 1 });
    expect(explicitOne.drainBehavior).toBe("cancel");
  });

  test("step defaults drainBehavior to wait when triggers is not 1", () => {
    const multi = step({ agent: makeAgent("a"), triggers: 5 });
    expect(multi.drainBehavior).toBe("wait");
    const unbounded = step({
      agent: makeAgent("a"),
      triggers: "unbounded",
    });
    expect(unbounded.drainBehavior).toBe("wait");
  });

  test("explicit drainBehavior overrides the trigger-budget default", () => {
    const multiExplicit = step({
      agent: makeAgent("a"),
      triggers: 5,
      drainBehavior: "cancel",
    });
    expect(multiExplicit.drainBehavior).toBe("cancel");
    const batchExplicit = step({
      agent: makeAgent("a"),
      triggers: 1,
      drainBehavior: "wait",
    });
    expect(batchExplicit.drainBehavior).toBe("wait");
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
