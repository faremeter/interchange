import { describe, test, expect } from "bun:test";

import { defineWorkflow, onTrigger, sleep } from "./definition/index";
import { rewriteInlineOnTriggerBodies } from "./ontrigger-bodies";

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
