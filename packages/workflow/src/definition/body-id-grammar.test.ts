// Whether the step-id grammar reaches a nested body.
//
// Every id rule -- non-empty, `STEP_ID_PATTERN`, and the `__` ban -- constrains
// the keys of ONE step record. A workflow root's record is normalized, so its
// keys are checked; a `loop`, `childWorkflow` inline, and `onTrigger` inline
// body are plain `WorkflowDefinition` values the parent embeds, so each body
// here is assembled BY HAND: a body built through `defineWorkflow` was already
// checked and would prove nothing about the parent's walk.
//
// The `__` case has teeth: `__` joins a step id into the ids the runtime
// derives from it (inline-body refs, body run ids), and those ids key the
// durable store, so a `__` inside a body step id is a silent shared-state
// collision, not a tidiness complaint.

import { describe, test, expect } from "bun:test";

import {
  action,
  childWorkflow,
  defineWorkflow,
  loop,
  onTrigger,
  type Primitive,
  type WorkflowDefinition,
} from "./index";

/**
 * Assemble a `WorkflowDefinition` directly, bypassing `defineWorkflow`. Only the
 * record-key-to-`id` assignment is reproduced; no validation runs, which is the
 * whole point -- this is the shape the trust boundary has to defend against.
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

/** A well-formed parent whose single childWorkflow carries the body. */
function defineAroundChildBody(body: WorkflowDefinition): () => void {
  return () => {
    defineWorkflow({
      id: "parent",
      trigger: { type: "manual" },
      steps: {
        sub: childWorkflow({ definition: body }),
      },
    });
  };
}

/** A well-formed parent whose single onTrigger section carries the body. */
function defineAroundSectionBody(body: WorkflowDefinition): () => void {
  return () => {
    defineWorkflow({
      id: "parent",
      steps: {
        inbox: onTrigger({
          on: { type: "mail", to: "parent@example.com" },
          body,
        }),
      },
    });
  };
}

const bodyKinds: {
  kind: string;
  defineAround: (body: WorkflowDefinition) => () => void;
}[] = [
  { kind: "loop", defineAround: defineAroundLoopBody },
  { kind: "childWorkflow", defineAround: defineAroundChildBody },
  { kind: "onTrigger", defineAround: defineAroundSectionBody },
];

const violations: { rule: string; stepId: string; expected: RegExp }[] = [
  {
    rule: "a double underscore",
    stepId: "iterate__once",
    expected: /must not contain "__"/,
  },
  {
    rule: "a character outside the step-id pattern",
    stepId: "iterate once",
    expected: /must match/,
  },
  { rule: "an empty id", stepId: "", expected: /step ids cannot be empty/ },
];

describe("step-id grammar in a nested body", () => {
  for (const { kind, defineAround } of bodyKinds) {
    test(`accepts a well-formed hand-assembled ${kind} body`, () => {
      const body = handBuiltBody("well-formed", {
        work: action({ handler: "noop" }),
        wrap: action({ handler: "noop", after: ["work"] }),
      });
      expect(defineAround(body)).not.toThrow();
    });

    for (const { rule, stepId, expected } of violations) {
      test(`rejects ${rule} in a ${kind} body step id`, () => {
        const body = handBuiltBody("bad-id", {
          [stepId]: action({ handler: "noop" }),
        });
        expect(defineAround(body)).toThrow(expected);
      });
    }
  }

  test("rejects a double underscore in a loop body nested in a loop body", () => {
    // The grammar rides the same re-entry as the rest of the suite: an inner
    // body's ids are checked at the outermost parent's authoring time.
    const inner = handBuiltBody("inner", {
      deep__step: action({ handler: "noop" }),
    });
    const outer = handBuiltBody("outer", {
      again: loop({
        body: inner,
        while: "whileFn",
        carry: "carryFn",
        maxIterations: 2,
        onExhausted: "settle",
      }),
      settle: action({ handler: "noop", after: ["again"] }),
    });
    expect(defineAroundLoopBody(outer)).toThrow(/must not contain "__"/);
  });
  test("rejects a body step whose embedded id names a different step", () => {
    // The record key builds every id-derived table (credentials snapshot,
    // grants write, staged body ref), while the runtime authorizes under the
    // primitive's own `id`; normalize assigns the key over the id, so a root
    // cannot diverge. A hand-assembled body skips that assignment, so it can
    // key a step under one name and carry another. Built literally, not via
    // `handBuiltBody`, which would erase the divergence under test.
    const body: WorkflowDefinition = {
      id: "outer",
      triggers: [{ type: "manual" }],
      steps: {
        work: { ...action({ handler: "noop" }), id: "settle" },
        settle: { ...action({ handler: "noop" }), id: "settle" },
      },
      stepOrder: ["work", "settle"],
    };
    expect(defineAroundLoopBody(body)).toThrow(/conflicting embedded id/);
  });
});
