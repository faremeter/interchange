import { describe, test, expect } from "bun:test";

import {
  childWorkflow,
  defineWorkflow,
  onTrigger,
  sleep,
  step,
} from "./definition/index";
import { defineAgent } from "@intx/agent";
import {
  onTriggerBodyRef,
  rewriteInlineOnTriggerBodies,
  rewriteInlineChildWorkflowBodies,
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

describe("onTriggerBodyRef", () => {
  test("joins the workflow id and step id with the `__` scheme", () => {
    expect(onTriggerBodyRef("wf", "sect")).toBe("wf__sect");
  });

  test("is the ref the live rewrite mints (single owner of the scheme)", () => {
    // A hub that stages a body under this ref and the run child that reads it
    // back must agree; the rewrite must route through the same helper so the
    // two never drift.
    const { bodies } = rewriteInlineOnTriggerBodies(inlineBodyWorkflow("wf"));
    expect(bodies[0]?.ref).toBe(onTriggerBodyRef("wf", "sect"));
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
    expect(bodies[0]?.ref).toBe(onTriggerBodyRef("wf", "spawn"));
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
