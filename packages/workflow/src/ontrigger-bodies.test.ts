import { describe, test, expect } from "bun:test";

import {
  action,
  childWorkflow,
  defineWorkflow,
  hashDefinition,
  loop,
  onTrigger,
  sleep,
  step,
  type WorkflowDefinition,
} from "./definition/index";
import { defineAgent } from "@intx/agent";
import {
  inlineBodyRef,
  rewriteInlineOnTriggerBodies,
  rewriteInlineChildWorkflowBodies,
  enumerateInlineLoopBodies,
} from "./ontrigger-bodies";

function inlineBodyWorkflow(id: string) {
  return defineWorkflow({
    id,
    trigger: { type: "manual" },
    steps: {
      sect: onTrigger({
        on: { type: "mail", to: "s@acme.test" },
        body: defineWorkflow({
          id: "inner",
          steps: { w: sleep({ duration: 1 }) },
        }),
      }),
    },
  });
}

describe("inlineBodyRef", () => {
  test("joins the workflow id and step id with the `__` scheme", () => {
    expect(inlineBodyRef("wf", "sect")).toBe("wf__sect");
  });

  test("is the ref the live rewrite mints (single owner of the scheme)", () => {
    // A hub that stages a body under this ref and the run child that reads it
    // back must agree; the rewrite must route through the same helper so the
    // two never drift.
    const { bodies } = rewriteInlineOnTriggerBodies(inlineBodyWorkflow("wf"));
    expect(bodies[0]?.ref).toBe(inlineBodyRef("wf", "sect"));
  });
});

describe("rewriteInlineOnTriggerBodies", () => {
  test("lifts an inline onTrigger body to a ref and extracts it", () => {
    const { workflow, bodies } = rewriteInlineOnTriggerBodies(
      inlineBodyWorkflow("wf"),
    );

    expect(bodies).toHaveLength(1);
    expect(bodies[0]?.ref).toBe("wf__sect");
    // The extracted body's id is overridden to the ref (regardless of the
    // inline definition's authored id).
    expect(bodies[0]?.definition.id).toBe("wf__sect");

    const sect = workflow.steps["sect"];
    expect(sect?.kind).toBe("onTrigger");
    if (sect?.kind === "onTrigger") {
      expect(sect.body).toEqual({ ref: "wf__sect" });
    }
  });

  test("is a no-op for a workflow with no inline onTrigger body", () => {
    const wf = defineWorkflow({
      id: "plain",
      trigger: { type: "manual" },
      steps: { w: sleep({ duration: 1 }) },
    });

    const { workflow, bodies } = rewriteInlineOnTriggerBodies(wf);

    expect(bodies).toHaveLength(0);
    // The original object is returned unchanged.
    expect(workflow).toBe(wf);
  });

  test("does not re-extract an already-ref body (idempotent)", () => {
    const first = rewriteInlineOnTriggerBodies(inlineBodyWorkflow("wf"));
    const second = rewriteInlineOnTriggerBodies(first.workflow);

    expect(second.bodies).toHaveLength(0);
    expect(second.workflow).toBe(first.workflow);
  });

  test("extracts every inline body when several are present", () => {
    const wf = defineWorkflow({
      id: "multi",
      trigger: { type: "manual" },
      steps: {
        a: onTrigger({
          on: { type: "mail", to: "a@acme.test" },
          body: defineWorkflow({
            id: "ia",
            steps: { w: sleep({ duration: 1 }) },
          }),
        }),
        b: onTrigger({
          on: { type: "mail", to: "b@acme.test" },
          body: defineWorkflow({
            id: "ib",
            steps: { w: sleep({ duration: 1 }) },
          }),
        }),
      },
    });

    const { bodies } = rewriteInlineOnTriggerBodies(wf);

    expect(bodies.map((b) => b.ref).sort()).toEqual(["multi__a", "multi__b"]);
  });
});

function inlineChildWorkflow(id: string) {
  return defineWorkflow({
    id,
    trigger: { type: "manual" },
    steps: {
      spawn: childWorkflow({
        definition: defineWorkflow({
          id: "authored-child",
          steps: {
            w: step({
              agent: defineAgent({
                id: "child-agent",
                systemPrompt: "child",
                tools: [],
                capabilities: [],
                inference: { sources: [{ provider: "p", model: "m" }] },
              }),
            }),
          },
        }),
      }),
    },
  });
}

describe("rewriteInlineChildWorkflowBodies", () => {
  test("lifts an inline childWorkflow to a ref and extracts it", () => {
    const { workflow, bodies } = rewriteInlineChildWorkflowBodies(
      inlineChildWorkflow("wf"),
    );

    expect(bodies).toHaveLength(1);
    // Mints refs through the same `<workflowId>__<stepId>` scheme onTrigger
    // bodies use; a step carries at most one of the two, so they never collide.
    expect(bodies[0]?.ref).toBe(inlineBodyRef("wf", "spawn"));
    expect(bodies[0]?.definition.id).toBe("wf__spawn");

    const spawn = workflow.steps["spawn"];
    expect(spawn?.kind).toBe("childWorkflow");
    if (spawn?.kind === "childWorkflow") {
      expect(spawn.definition).toEqual({ ref: "wf__spawn" });
    }
  });

  test("is a no-op for a workflow with no inline childWorkflow", () => {
    const wf = defineWorkflow({
      id: "plain",
      trigger: { type: "manual" },
      steps: { w: sleep({ duration: 1 }) },
    });

    const { workflow, bodies } = rewriteInlineChildWorkflowBodies(wf);

    expect(bodies).toHaveLength(0);
    expect(workflow).toBe(wf);
  });

  test("does not re-extract an already-ref child (idempotent)", () => {
    const first = rewriteInlineChildWorkflowBodies(inlineChildWorkflow("wf"));
    const second = rewriteInlineChildWorkflowBodies(first.workflow);

    expect(second.bodies).toHaveLength(0);
    expect(second.workflow).toBe(first.workflow);
  });

  test("leaves inline onTrigger bodies untouched (disjoint from the onTrigger rewrite)", () => {
    // The two rewrites target disjoint primitive kinds; the childWorkflow
    // rewrite must not lift an onTrigger body and vice versa.
    const input = inlineBodyWorkflow("wf");
    const { workflow, bodies } = rewriteInlineChildWorkflowBodies(input);
    expect(bodies).toHaveLength(0);
    expect(workflow).toBe(input);
    const sect = workflow.steps["sect"];
    expect(sect?.kind).toBe("onTrigger");
    if (sect?.kind === "onTrigger") {
      expect("inline" in sect.body).toBe(true);
    }
  });
});

function loopWorkflow(id: string) {
  return defineWorkflow({
    id,
    trigger: { type: "manual" },
    steps: {
      rework: loop({
        body: defineWorkflow({
          id: "authored-loop-body",
          steps: { touch: action({ handler: "noop" }) },
        }),
        while: "cont",
        carry: "next",
        maxIterations: 3,
        onExhausted: "escalate",
      }),
      escalate: action({ handler: "esc", after: ["rework"] }),
    },
  });
}

describe("enumerateInlineLoopBodies", () => {
  test("collects a loop body under the shared `__` ref with its id set to the ref", () => {
    const bodies = enumerateInlineLoopBodies(loopWorkflow("wf"));

    expect(bodies).toHaveLength(1);
    // Same `<workflowId>__<stepId>` scheme the onTrigger/childWorkflow bodies
    // use; a step is exactly one primitive kind, so the refs never collide.
    expect(bodies[0]?.ref).toBe(inlineBodyRef("wf", "rework"));
    // The lifted body's id is the ref, regardless of the authored id.
    expect(bodies[0]?.definition.id).toBe("wf__rework");
    expect(Object.keys(bodies[0]?.definition.steps ?? {})).toEqual(["touch"]);
  });

  test("does not mutate the primitive's inline body, so the hash is unchanged", () => {
    // A loop keeps its body inline (unlike the onTrigger/childWorkflow rewrite
    // to a `{ ref }`), because both hash layers project the body inline. The
    // enumerator must therefore mint a fresh copy and leave the input untouched,
    // or it would change every existing loop definition's hash.
    const wf = loopWorkflow("wf");
    const before = hashDefinition(wf);
    const bodies = enumerateInlineLoopBodies(wf);

    const rework = wf.steps["rework"];
    expect(rework?.kind).toBe("loop");
    if (rework?.kind === "loop") {
      // The primitive still holds its inline body with its authored id --
      // untouched, not rewritten to a `{ ref }` and not re-id'd.
      expect(rework.body.id).toBe("authored-loop-body");
      // The lifted copy is a fresh top-level object, so rebinding its `id` to
      // the ref does not touch the primitive's body. (The spread is shallow, so
      // nested structure is shared -- fine, since the enumerator only rebinds
      // `id` and never mutates the copy.)
      expect(bodies[0]?.definition).not.toBe(rework.body);
    }
    const after = hashDefinition(wf);
    expect(after).toEqual(before);
  });

  test("keeps the loop body inline in the hash, never lifted to a ref", () => {
    // The hash-safety guarantee: both hash layers project a loop body inline
    // under the bare `body` field. If a future change lifts loop bodies to a
    // `{ ref }` (as onTrigger/childWorkflow bodies are), every existing loop's
    // hash changes and deployed loops fail re-verify. `hashDefinition` returns
    // the canonical (sorted-key) form, so the loop body serializes inline with
    // its authored id and its steps, and the workflow carries no `ref` anywhere.
    const canonical = new TextDecoder().decode(
      hashDefinition(loopWorkflow("wf")),
    );
    expect(canonical).toContain('"body":{"id":"authored-loop-body"');
    expect(canonical).toContain('"touch"');
    // A ref-lifted loop body would serialize as `"body":{"ref":"wf__rework"}`.
    expect(canonical).not.toContain('"ref"');
  });

  test("is empty for a workflow with no loop", () => {
    const wf = defineWorkflow({
      id: "plain",
      trigger: { type: "manual" },
      steps: { w: sleep({ duration: 1 }) },
    });

    expect(enumerateInlineLoopBodies(wf)).toHaveLength(0);
  });

  test("collects every loop when several are present", () => {
    const wf = defineWorkflow({
      id: "multi",
      trigger: { type: "manual" },
      steps: {
        a: loop({
          body: defineWorkflow({
            id: "ba",
            steps: { t: action({ handler: "noop" }) },
          }),
          while: "cont",
          carry: "next",
          maxIterations: 2,
          onExhausted: "done",
        }),
        b: loop({
          body: defineWorkflow({
            id: "bb",
            steps: { t: action({ handler: "noop" }) },
          }),
          while: "cont",
          carry: "next",
          maxIterations: 2,
          onExhausted: "done",
        }),
        done: action({ handler: "d", after: ["a", "b"] }),
      },
    });

    expect(
      enumerateInlineLoopBodies(wf)
        .map((b) => b.ref)
        .sort(),
    ).toEqual(["multi__a", "multi__b"]);
  });

  test("recurses into nested loop bodies, minting a ref per depth", () => {
    // The enumerator is a pure structural pass over a `WorkflowDefinition`, so
    // it can be exercised on a nested-loop definition assembled directly --
    // `defineWorkflow` rejects a nested loop at authoring time, but the runtime
    // resolves an inner loop's body from this same map, so the map must carry a
    // ref at every depth. Swap loop bodies to stack three loop levels
    // (outer -> inner -> innermost) over a leaf action body.
    const leaf = defineWorkflow({
      id: "leaf",
      steps: { touch: action({ handler: "noop" }) },
    });
    const withLoopBody = (
      wf: WorkflowDefinition,
      loopStepId: string,
      body: WorkflowDefinition,
    ): WorkflowDefinition => {
      const primitive = wf.steps[loopStepId];
      if (primitive?.kind !== "loop") {
        throw new Error(`fixture: ${loopStepId} is not a loop`);
      }
      return {
        ...wf,
        steps: { ...wf.steps, [loopStepId]: { ...primitive, body } },
      };
    };
    const oneLoop = (id: string, loopStepId: string) =>
      defineWorkflow({
        id,
        trigger: { type: "manual" },
        steps: {
          [loopStepId]: loop({
            body: leaf,
            while: "cont",
            carry: "next",
            maxIterations: 2,
            onExhausted: "esc",
          }),
          esc: action({ handler: "noop", after: [loopStepId] }),
        },
      });

    const innermostBody = oneLoop("b-innermost", "innermost");
    const innerBody = withLoopBody(
      oneLoop("b-inner", "inner"),
      "inner",
      innermostBody,
    );
    const outer = withLoopBody(oneLoop("wf", "outer"), "outer", innerBody);

    const refs = enumerateInlineLoopBodies(outer)
      .map((b) => b.ref)
      .sort();
    expect(refs).toEqual([
      "wf__outer",
      "wf__outer__inner",
      "wf__outer__inner__innermost",
    ]);
    // Each lifted body's id is set to its ref -- the string runLoop reconstructs
    // via `inlineBodyRef(definition.id, primitive.id)` at each nesting level.
    const deepest = enumerateInlineLoopBodies(outer).find(
      (b) => b.ref === "wf__outer__inner__innermost",
    );
    expect(deepest?.definition.id).toBe("wf__outer__inner__innermost");
    expect(Object.keys(deepest?.definition.steps ?? {})).toEqual(["touch"]);
  });
});
