// Which validation passes reach a loop body.
//
// A loop body is a `WorkflowDefinition` the parent embeds by value. Nothing in
// the type system says it came from `defineWorkflow`, and nothing marks a
// definition as normalized, so a body can be hand-assembled or spread-swapped
// into place without ever passing through a validation. Every case here builds
// its body BY HAND for that reason: a body built through `defineWorkflow` was
// already validated at its own construction and would prove nothing about what
// the parent's loop-body walk catches.
//
// Each case carries the defect owned by one pass of `validateSteps` and asserts
// the parent rejects it at authoring time. Together they state executably that a
// loop body is validated as thoroughly as an `onTrigger` section body and a
// `childWorkflow` inline body.

import { describe, test, expect } from "bun:test";

import { defineAgent, type AgentDefinition, type BaseEnv } from "@intx/agent";

import {
  action,
  awaitSignal,
  childWorkflow,
  defineWorkflow,
  loop,
  onTrigger,
  sleep,
  step,
  type Primitive,
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

/**
 * Assemble a `WorkflowDefinition` directly, bypassing `defineWorkflow`. Only the
 * record-key-to-`id` assignment is reproduced, because the primitive
 * constructors leave `id` empty and a definition that reached the runtime would
 * carry it. No validation runs, which is the whole point: this is the shape the
 * trust boundary actually has to defend against.
 */
function handBuiltBody(
  id: string,
  steps: Record<string, Primitive>,
): WorkflowDefinition {
  const withIds: Record<string, Primitive> = {};
  for (const [stepId, primitive] of Object.entries(steps)) {
    withIds[stepId] = { ...primitive, id: stepId };
  }
  return {
    id,
    triggers: [{ type: "manual" }],
    steps: withIds,
    stepOrder: Object.keys(withIds),
  };
}

/** A well-formed parent whose single loop carries the body under test. */
function defineAroundLoopBody(body: WorkflowDefinition): () => void {
  return () => {
    defineWorkflow({
      id: "parent",
      trigger: { type: "manual" },
      steps: {
        rework: loop({
          body,
          while: "whileFn",
          carry: "carryFn",
          maxIterations: 2,
          onExhausted: "done",
        }),
        done: action({ handler: "noop", after: ["rework"] }),
      },
    });
  };
}

/** A hand-built body holding one childWorkflow whose inline body is `inline`. */
function bodyHoldingChild(inline: WorkflowDefinition): WorkflowDefinition {
  return handBuiltBody("holds-child", {
    sub: childWorkflow({ definition: inline }),
  });
}

describe("loop body validation passes", () => {
  test("accepts a well-formed hand-assembled loop body", () => {
    const body = handBuiltBody("well-formed", {
      work: step({
        agent: makeAgent("w"),
        input: { from: "trigger.payload" },
      }),
      wrap: action({ handler: "noop", after: ["work"] }),
    });
    expect(defineAroundLoopBody(body)).not.toThrow();
  });

  test("validateAfterRefs reaches a loop body", () => {
    const body = handBuiltBody("dangling-after", {
      work: step({ agent: makeAgent("w"), after: ["ghost"] }),
    });
    expect(defineAroundLoopBody(body)).toThrow(
      /step work declares after ghost which is not a known step/,
    );
  });

  test("validateAcyclic reaches a loop body", () => {
    const body = handBuiltBody("cyclic", {
      a: step({ agent: makeAgent("a"), after: ["b"] }),
      b: step({ agent: makeAgent("b"), after: ["a"] }),
    });
    expect(defineAroundLoopBody(body)).toThrow(/dependency cycle/);
  });

  test("validateConcurrentAwaitSignalNames reaches a loop body", () => {
    // The parent cannot see this one even in principle: it collects both body
    // awaiters as relays of the SAME loop step, and its comparison loop skips
    // pairs sharing a node. Only validating the body as its own step record
    // separates them.
    const body = handBuiltBody("two-awaiters", {
      first: awaitSignal({ name: "go" }),
      second: awaitSignal({ name: "go" }),
    });
    expect(defineAroundLoopBody(body)).toThrow(
      /can concurrently await signal name go/,
    );
  });

  test("the loop-body onFailure ban preempts validateOnFailureStraddlers", () => {
    // validateOnFailureStraddlers is a no-op inside a loop body: a loop
    // iteration threads carry into the next input, so no body step may route on
    // failure at all. The blanket ban fires before the straddler analysis can
    // have anything to reason about.
    const body = handBuiltBody("routes-on-failure", {
      unit: step({ agent: makeAgent("u"), onFailure: "rescue" }),
      rescue: action({ handler: "noop", after: ["unit"] }),
    });
    expect(defineAroundLoopBody(body)).toThrow(
      /step unit may not carry onFailure/,
    );
  });

  test("validateChildWorkflowBody reaches a loop body", () => {
    const grandchild = handBuiltBody("grandchild", {
      g: step({ agent: makeAgent("g"), after: ["missing"] }),
    });
    expect(defineAroundLoopBody(bodyHoldingChild(grandchild))).toThrow(
      /step g declares after missing which is not a known step/,
    );
  });

  test("validateOnTriggerBody reaches a loop body", () => {
    // A section directly in the loop body is already banned by kind. The
    // placement rule adds the level below: a section inside a childWorkflow body
    // inside the loop body is equally unsubscribable, and only re-entry finds it.
    const grandchild = handBuiltBody("grandchild", {
      section: onTrigger({
        on: { type: "manual" },
        body: handBuiltBody("section-body", {
          work: step({ agent: makeAgent("w") }),
        }),
      }),
    });
    expect(defineAroundLoopBody(bodyHoldingChild(grandchild))).toThrow(
      /nested inside a spawned body/,
    );
  });

  test("validateLoopBody reaches a loop nested under a childWorkflow body", () => {
    // The loop-to-loop walk alone steps over a childWorkflow, so a sleep in a
    // loop body two containers down used to escape. Re-entry follows the whole
    // chain.
    const leaf = handBuiltBody("leaf", { nap: sleep({ duration: 1 }) });
    const grandchild = handBuiltBody("grandchild", {
      inner: loop({
        body: leaf,
        while: "whileFn",
        carry: "carryFn",
        maxIterations: 2,
        onExhausted: "wrap",
      }),
      wrap: action({ handler: "noop", after: ["inner"] }),
    });
    expect(defineAroundLoopBody(bodyHoldingChild(grandchild))).toThrow(
      /a loop body may not contain a sleep or onTrigger/,
    );
  });
});

describe("loop body nesting depth accounting", () => {
  /**
   * A chain of `levels` hand-built loop bodies, innermost first. Nothing in the
   * chain is normalized, so the parent's walk is the only thing counting.
   */
  function nestHandBuilt(levels: number): WorkflowDefinition {
    let body = handBuiltBody("leaf", {
      work: step({ agent: makeAgent("w") }),
    });
    for (let i = 0; i < levels; i += 1) {
      body = handBuiltBody(`lvl-${String(i)}`, {
        rework: loop({
          body,
          while: "whileFn",
          carry: "carryFn",
          maxIterations: 2,
          onExhausted: "wrap",
        }),
        wrap: action({ handler: "noop", after: ["rework"] }),
      });
    }
    return body;
  }

  // The parent's own loop is level 1, so `levels` nested bodies reach depth
  // `levels + 1`. Eight is the cap. These two cases are what fails if the
  // re-entry restarts the count at each body instead of carrying it down.
  test("accepts a hand-built nesting chain at the depth limit", () => {
    expect(defineAroundLoopBody(nestHandBuilt(7))).not.toThrow();
  });

  test("rejects a hand-built nesting chain past the depth limit", () => {
    expect(defineAroundLoopBody(nestHandBuilt(8))).toThrow(
      /deeper than the maximum/,
    );
  });
});
