import { describe, test, expect } from "bun:test";

import { defineAgent, type AgentDefinition, type BaseEnv } from "@intx/agent";

import {
  action,
  awaitSignal,
  childWorkflow,
  defineWorkflow,
  escalation,
  executableStepIds,
  gate,
  loop,
  map,
  nestedWorkflowBodies,
  onTrigger,
  sleep,
  step,
  walkNestedWorkflowSteps,
  walkStepTree,
  walkWorkflowSteps,
  EXECUTABLE_STEP_DESCENT,
  LOOP_BODY_DESCENT,
  type Primitive,
  type StepWalkDescent,
  type WorkflowDefinition,
} from "./index";

function makeAgent(id: string): AgentDefinition<BaseEnv> {
  return defineAgent({
    id,
    systemPrompt: "you are " + id,
    tools: [],
    capabilities: [],
    inference: { sources: [{ provider: "fake", model: "fake" }] },
  });
}

const NO_DESCENT: StepWalkDescent = {
  loopBodies: false,
  inlineOnTriggerBodies: false,
  inlineChildWorkflowBodies: false,
};

function visitedIds(
  definition: WorkflowDefinition,
  descent: StepWalkDescent,
): string[] {
  const ids: string[] = [];
  walkWorkflowSteps({
    definition,
    descent,
    context: "step-walk test: ",
    visit: ({ stepId }) => {
      ids.push(stepId);
    },
  });
  return ids;
}

function visitedPaths(
  definition: WorkflowDefinition,
  descent: StepWalkDescent,
): readonly string[][] {
  const paths: string[][] = [];
  walkWorkflowSteps({
    definition,
    descent,
    context: "step-walk test: ",
    visit: ({ path }) => {
      paths.push([...path]);
    },
  });
  return paths;
}

/** A loop whose body holds a single agent step, plus its exhaustion handler. */
function workflowWithLoop(): WorkflowDefinition {
  const body = defineWorkflow({
    id: "loop-body",
    trigger: { type: "manual" },
    steps: {
      work: step({ agent: makeAgent("work") }),
      commit: action({ handler: "commit", after: ["work"] }),
    },
  });
  return defineWorkflow({
    id: "wf_loop",
    trigger: { type: "manual" },
    steps: {
      rework: loop({
        body,
        while: "w",
        carry: "c",
        maxIterations: 3,
        onExhausted: "esc",
      }),
      esc: step({ agent: makeAgent("esc"), after: ["rework"] }),
    },
  });
}

describe("walkWorkflowSteps", () => {
  test("a flat workflow yields its stepOrder under every descent", () => {
    const workflow = defineWorkflow({
      id: "wf_flat",
      trigger: { type: "manual" },
      steps: {
        plan: step({ agent: makeAgent("plan") }),
        run: action({ handler: "run", after: ["plan"] }),
        wrap: step({ agent: makeAgent("wrap"), after: ["run"] }),
      },
    });

    expect(visitedIds(workflow, EXECUTABLE_STEP_DESCENT)).toEqual([
      "plan",
      "run",
      "wrap",
    ]);
    expect(visitedIds(workflow, LOOP_BODY_DESCENT)).toEqual([
      "plan",
      "run",
      "wrap",
    ]);
    expect(visitedIds(workflow, NO_DESCENT)).toEqual(["plan", "run", "wrap"]);
  });

  test("a loop body's steps are visited between the loop and its successor", () => {
    const workflow = workflowWithLoop();

    // Pre-order: the container, then its body in the body's own stepOrder,
    // then the container's sibling.
    expect(visitedIds(workflow, EXECUTABLE_STEP_DESCENT)).toEqual([
      "rework",
      "work",
      "commit",
      "esc",
    ]);
    expect(visitedIds(workflow, LOOP_BODY_DESCENT)).toEqual([
      "rework",
      "work",
      "commit",
      "esc",
    ]);
    // With the loop descent off, the body is invisible.
    expect(visitedIds(workflow, NO_DESCENT)).toEqual(["rework", "esc"]);
  });

  test("a nested loop body is reached at every depth", () => {
    const innerBody = defineWorkflow({
      id: "inner-body",
      trigger: { type: "manual" },
      steps: { deep: step({ agent: makeAgent("deep") }) },
    });
    const outerBody = defineWorkflow({
      id: "outer-body",
      trigger: { type: "manual" },
      steps: {
        inner: loop({
          body: innerBody,
          while: "w",
          carry: "c",
          maxIterations: 2,
          onExhausted: "innerEsc",
        }),
        innerEsc: step({ agent: makeAgent("inner-esc"), after: ["inner"] }),
      },
    });
    const workflow = defineWorkflow({
      id: "wf_nested_loop",
      trigger: { type: "manual" },
      steps: {
        outer: loop({
          body: outerBody,
          while: "w",
          carry: "c",
          maxIterations: 2,
          onExhausted: "outerEsc",
        }),
        outerEsc: step({ agent: makeAgent("outer-esc"), after: ["outer"] }),
      },
    });

    expect(visitedIds(workflow, EXECUTABLE_STEP_DESCENT)).toEqual([
      "outer",
      "inner",
      "deep",
      "innerEsc",
      "outerEsc",
    ]);
  });

  test("an inline onTrigger body is reached only under the executable descent", () => {
    const sectionBody = defineWorkflow({
      id: "section-body",
      trigger: { type: "manual" },
      steps: {
        handle: step({ agent: makeAgent("handle") }),
        reply: action({ handler: "reply", after: ["handle"] }),
      },
    });
    const workflow = defineWorkflow({
      id: "wf_section",
      steps: {
        section: onTrigger({
          on: { type: "mail", to: "desk@example.com" },
          body: sectionBody,
        }),
      },
    });

    expect(visitedIds(workflow, EXECUTABLE_STEP_DESCENT)).toEqual([
      "section",
      "handle",
      "reply",
    ]);
    // A lifted section body keys its steps under its own ref, so the flat
    // namespace descent stops at the section.
    expect(visitedIds(workflow, LOOP_BODY_DESCENT)).toEqual(["section"]);
  });

  test("an inline childWorkflow body is reached only under the executable descent", () => {
    const childDefinition = defineWorkflow({
      id: "child",
      trigger: { type: "manual" },
      steps: {
        task: step({ agent: makeAgent("task") }),
        settle: action({ handler: "settle", after: ["task"] }),
      },
    });
    const workflow = defineWorkflow({
      id: "wf_child",
      trigger: { type: "manual" },
      steps: {
        spawn: childWorkflow({ definition: childDefinition }),
        after: step({ agent: makeAgent("after"), after: ["spawn"] }),
      },
    });

    expect(visitedIds(workflow, EXECUTABLE_STEP_DESCENT)).toEqual([
      "spawn",
      "task",
      "settle",
      "after",
    ]);
    expect(visitedIds(workflow, LOOP_BODY_DESCENT)).toEqual(["spawn", "after"]);
  });

  test("a loop inside a spawned body is reached through the spawning step", () => {
    const loopBody = defineWorkflow({
      id: "spawned-loop-body",
      trigger: { type: "manual" },
      steps: { grind: step({ agent: makeAgent("grind") }) },
    });
    const sectionBody = defineWorkflow({
      id: "spawned-section-body",
      trigger: { type: "manual" },
      steps: {
        rework: loop({
          body: loopBody,
          while: "w",
          carry: "c",
          maxIterations: 2,
          onExhausted: "esc",
        }),
        esc: step({ agent: makeAgent("esc"), after: ["rework"] }),
      },
    });
    const childDefinition = defineWorkflow({
      id: "spawned-child",
      trigger: { type: "manual" },
      steps: {
        childRework: loop({
          body: defineWorkflow({
            id: "child-loop-body",
            trigger: { type: "manual" },
            steps: { polish: step({ agent: makeAgent("polish") }) },
          }),
          while: "w",
          carry: "c",
          maxIterations: 2,
          onExhausted: "childEsc",
        }),
        childEsc: step({
          agent: makeAgent("child-esc"),
          after: ["childRework"],
        }),
      },
    });
    const workflow = defineWorkflow({
      id: "wf_spawned_loops",
      steps: {
        section: onTrigger({
          on: { type: "mail", to: "desk@example.com" },
          body: sectionBody,
        }),
        spawn: childWorkflow({ definition: childDefinition }),
      },
    });

    expect(visitedIds(workflow, EXECUTABLE_STEP_DESCENT)).toEqual([
      "section",
      "rework",
      "grind",
      "esc",
      "spawn",
      "childRework",
      "polish",
      "childEsc",
    ]);
    // The flat-namespace descent never crosses the lift, so the loop bodies
    // inside the spawned bodies are out of reach entirely.
    expect(visitedIds(workflow, LOOP_BODY_DESCENT)).toEqual([
      "section",
      "spawn",
    ]);
  });

  test("a by-ref body carries no inline tree to descend into", () => {
    const workflow: WorkflowDefinition = {
      id: "wf_ref_bodies",
      triggers: [{ type: "manual" }],
      stepOrder: ["section", "spawn"],
      steps: {
        section: {
          kind: "onTrigger",
          id: "section",
          on: { type: "manual" },
          body: { ref: "wf_ref_bodies__section" },
        },
        spawn: {
          kind: "childWorkflow",
          id: "spawn",
          definition: { ref: "wf_ref_bodies__spawn" },
        },
      },
    };

    expect(visitedIds(workflow, EXECUTABLE_STEP_DESCENT)).toEqual([
      "section",
      "spawn",
    ]);
  });

  test("hands each entry the tree its step id keys into", () => {
    const workflow = workflowWithLoop();
    const trees: string[] = [];
    walkWorkflowSteps({
      definition: workflow,
      descent: EXECUTABLE_STEP_DESCENT,
      context: "step-walk test: ",
      visit: ({ tree }) => {
        trees.push(tree.id);
      },
    });

    expect(trees).toEqual(["wf_loop", "loop-body", "loop-body", "wf_loop"]);
  });

  test("hands each entry the chain of step ids it was reached through", () => {
    expect(visitedPaths(workflowWithLoop(), EXECUTABLE_STEP_DESCENT)).toEqual([
      ["rework"],
      ["rework", "work"],
      ["rework", "commit"],
      ["esc"],
    ]);
  });

  test("the chain grows one rung per body the walk descends into", () => {
    // Three rungs, crossing the lifted-body boundary at the second: two steps
    // in different bodies may share an id, so the chain is what tells them
    // apart.
    const grandchild = defineWorkflow({
      id: "grandchild",
      trigger: { type: "manual" },
      steps: { work: step({ agent: makeAgent("work") }) },
    });
    const body = defineWorkflow({
      id: "spin-body",
      trigger: { type: "manual" },
      steps: { spawn: childWorkflow({ definition: grandchild }) },
    });
    const workflow = defineWorkflow({
      id: "wf_three_rungs",
      trigger: { type: "manual" },
      steps: {
        spin: loop({
          body,
          while: "w",
          carry: "c",
          maxIterations: 2,
          onExhausted: "esc",
        }),
        esc: step({ agent: makeAgent("esc"), after: ["spin"] }),
      },
    });

    expect(visitedPaths(workflow, EXECUTABLE_STEP_DESCENT)).toEqual([
      ["spin"],
      ["spin", "spawn"],
      ["spin", "spawn", "work"],
      ["esc"],
    ]);
    // The flat-namespace descent stops at the lift, so the chain stops with it.
    expect(visitedPaths(workflow, LOOP_BODY_DESCENT)).toEqual([
      ["spin"],
      ["spin", "spawn"],
      ["esc"],
    ]);
  });

  test("a stepOrder entry with no matching step throws with the caller's context", () => {
    const workflow: WorkflowDefinition = {
      id: "wf_phantom",
      triggers: [{ type: "manual" }],
      stepOrder: ["ghost"],
      steps: {},
    };

    expect(() => visitedIds(workflow, EXECUTABLE_STEP_DESCENT)).toThrow(
      "step-walk test: step ghost listed in stepOrder is missing from steps",
    );
  });
});

describe("walkNestedWorkflowSteps", () => {
  test("visits a primitive's nested steps and not the primitive itself", () => {
    const workflow = workflowWithLoop();
    const rework = workflow.steps["rework"];
    if (rework === undefined) throw new Error("missing loop step");

    const ids: string[] = [];
    walkNestedWorkflowSteps({
      primitive: rework,
      descent: EXECUTABLE_STEP_DESCENT,
      context: "step-walk test: ",
      visit: ({ stepId }) => {
        ids.push(stepId);
      },
    });

    expect(ids).toEqual(["work", "commit"]);
  });

  test("roots each chain at the nested body, not at the enclosing step", () => {
    // This walk is handed the primitive, not the step id it sits under, so the
    // chain it can honestly report starts inside the body.
    const workflow = workflowWithLoop();
    const rework = workflow.steps["rework"];
    if (rework === undefined) throw new Error("missing loop step");

    const paths: string[][] = [];
    walkNestedWorkflowSteps({
      primitive: rework,
      descent: EXECUTABLE_STEP_DESCENT,
      context: "step-walk test: ",
      visit: ({ path }) => {
        paths.push([...path]);
      },
    });

    expect(paths).toEqual([["work"], ["commit"]]);
  });

  test("a leaf primitive nests nothing", () => {
    const ids: string[] = [];
    walkNestedWorkflowSteps({
      primitive: step({ agent: makeAgent("solo") }),
      descent: EXECUTABLE_STEP_DESCENT,
      context: "step-walk test: ",
      visit: ({ stepId }) => {
        ids.push(stepId);
      },
    });

    expect(ids).toEqual([]);
  });
});

describe("nestedWorkflowBodies", () => {
  test("every leaf primitive kind carries no nested body", () => {
    const leaves: Primitive[] = [
      step({ agent: makeAgent("s") }),
      map({
        over: { from: "trigger.payload" },
        step: step({ agent: makeAgent("m") }),
      }),
      action({ handler: "h" }),
      gate({ when: { from: "trigger.payload" }, then: "a", else: "b" }),
      escalation({ to: "ops@example.com" }),
      awaitSignal({ name: "go" }),
      sleep({ duration: 1 }),
    ];

    for (const leaf of leaves) {
      expect(nestedWorkflowBodies(leaf, EXECUTABLE_STEP_DESCENT)).toEqual([]);
    }
  });

  test("each container kind is gated by its own descent flag", () => {
    const body = defineWorkflow({
      id: "body",
      trigger: { type: "manual" },
      steps: { work: step({ agent: makeAgent("work") }) },
    });
    const loopPrimitive = loop({
      body,
      while: "w",
      carry: "c",
      maxIterations: 2,
      onExhausted: "esc",
    });
    const sectionPrimitive = onTrigger({
      on: { type: "mail", to: "desk@example.com" },
      body,
    });
    const childPrimitive = childWorkflow({ definition: body });

    expect(nestedWorkflowBodies(loopPrimitive, LOOP_BODY_DESCENT)).toEqual([
      body,
    ]);
    expect(nestedWorkflowBodies(sectionPrimitive, LOOP_BODY_DESCENT)).toEqual(
      [],
    );
    expect(nestedWorkflowBodies(childPrimitive, LOOP_BODY_DESCENT)).toEqual([]);

    expect(
      nestedWorkflowBodies(loopPrimitive, EXECUTABLE_STEP_DESCENT),
    ).toEqual([body]);
    expect(
      nestedWorkflowBodies(sectionPrimitive, EXECUTABLE_STEP_DESCENT),
    ).toEqual([body]);
    expect(
      nestedWorkflowBodies(childPrimitive, EXECUTABLE_STEP_DESCENT),
    ).toEqual([body]);
  });
});

describe("executableStepIds", () => {
  // The contract two producers and their deploy-time reconciliation depend on:
  // `EXECUTABLE_STEP_DESCENT` is the setting that yields every step id the
  // deployment can execute, across loop bodies and lifted spawned bodies alike.
  test("reaches every step id the deployment can execute", () => {
    const loopBody = defineWorkflow({
      id: "loop-body",
      trigger: { type: "manual" },
      steps: { grind: step({ agent: makeAgent("grind") }) },
    });
    const sectionBody = defineWorkflow({
      id: "section-body",
      trigger: { type: "manual" },
      steps: {
        rework: loop({
          body: loopBody,
          while: "w",
          carry: "c",
          maxIterations: 2,
          onExhausted: "esc",
        }),
        esc: step({ agent: makeAgent("esc"), after: ["rework"] }),
      },
    });
    const childDefinition = defineWorkflow({
      id: "child",
      trigger: { type: "manual" },
      steps: { task: step({ agent: makeAgent("task") }) },
    });
    const workflow = defineWorkflow({
      id: "wf_everything",
      steps: {
        section: onTrigger({
          on: { type: "mail", to: "desk@example.com" },
          body: sectionBody,
        }),
        spawn: childWorkflow({ definition: childDefinition }),
        wrap: step({ agent: makeAgent("wrap"), after: ["spawn"] }),
      },
    });

    expect(executableStepIds(workflow)).toEqual([
      "section",
      "rework",
      "grind",
      "esc",
      "spawn",
      "task",
      "wrap",
    ]);
    // `stepOrder` alone sees only the top rung -- the gap this walk closes.
    expect(workflow.stepOrder).toEqual(["section", "spawn", "wrap"]);
  });

  test("an id repeated across bodies appears once, in first-reach order", () => {
    const loopBody = defineWorkflow({
      id: "loop-body",
      trigger: { type: "manual" },
      steps: { shared: step({ agent: makeAgent("shared-body") }) },
    });
    const workflow = defineWorkflow({
      id: "wf_repeat",
      trigger: { type: "manual" },
      steps: {
        rework: loop({
          body: loopBody,
          while: "w",
          carry: "c",
          maxIterations: 2,
          onExhausted: "esc",
        }),
        esc: step({ agent: makeAgent("esc"), after: ["rework"] }),
        shared: step({ agent: makeAgent("shared-top"), after: ["esc"] }),
      },
    });

    expect(executableStepIds(workflow)).toEqual(["rework", "shared", "esc"]);
  });
});

describe("walkStepTree", () => {
  // The traversal core is generic so the inert wire projection -- whose step
  // values are `unknown` and are validated as the caller descends -- rides the
  // same walk as the live definition.
  interface OpaqueTree {
    readonly stepOrder: readonly string[];
    readonly steps: Record<string, unknown>;
  }

  function isOpaqueTree(value: unknown): value is OpaqueTree {
    return (
      typeof value === "object" &&
      value !== null &&
      "stepOrder" in value &&
      Array.isArray(value.stepOrder) &&
      "steps" in value &&
      typeof value.steps === "object" &&
      value.steps !== null
    );
  }

  function opaqueLoopBody(stepValue: unknown): readonly OpaqueTree[] {
    if (
      typeof stepValue !== "object" ||
      stepValue === null ||
      !("body" in stepValue)
    ) {
      return [];
    }
    return isOpaqueTree(stepValue.body) ? [stepValue.body] : [];
  }

  test("walks a tree whose steps are untyped", () => {
    const tree: OpaqueTree = {
      stepOrder: ["rework", "esc"],
      steps: {
        rework: {
          kind: "loop",
          body: { stepOrder: ["work"], steps: { work: { kind: "step" } } },
        },
        esc: { kind: "step" },
      },
    };

    const ids: string[] = [];
    const paths: string[][] = [];
    walkStepTree<unknown, OpaqueTree>({
      tree,
      context: "step-walk test: ",
      nestedTrees: opaqueLoopBody,
      visit: ({ stepId, path }) => {
        ids.push(stepId);
        paths.push([...path]);
      },
    });

    expect(ids).toEqual(["rework", "work", "esc"]);
    expect(paths).toEqual([["rework"], ["rework", "work"], ["esc"]]);
  });
});
