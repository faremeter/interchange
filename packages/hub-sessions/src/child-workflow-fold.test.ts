import { describe, expect, test } from "bun:test";

import {
  childWorkflow,
  defineWorkflow,
  onTrigger,
  sleep,
  step,
  type WorkflowDefinition,
} from "@intx/workflow/definition";
import { computeLiveDefinitionHash, projectLiveToInert } from "@intx/workflow";
import { defineAgent } from "@intx/agent";
import type { ApprovedGrantSurface } from "@intx/types";
import { computeWireDefinitionHash } from "@intx/types/wire-definition-hash";

import {
  enumerateChildWorkflowRefs,
  mergeGrantSurfaces,
} from "./child-workflow-fold";

/** An agent-bearing definition whose inert projection is the shape a child's
 * workflow.json holds. */
function agentWorkflow(): WorkflowDefinition {
  return defineWorkflow({
    id: "agent-child",
    trigger: { type: "manual" },
    steps: {
      run: step({
        agent: defineAgent({
          id: "a",
          systemPrompt: "s",
          tools: [],
          capabilities: [],
          inference: { sources: [{ provider: "anthropic", model: "m" }] },
        }),
      }),
    },
  });
}

describe("enumerateChildWorkflowRefs", () => {
  test("returns no refs for a workflow with no childWorkflow steps", () => {
    const wf = defineWorkflow({
      id: "leaf",
      trigger: { type: "manual" },
      steps: { wait: sleep({ duration: 1 }) },
    });
    expect(enumerateChildWorkflowRefs(wf)).toEqual([]);
  });

  test("collects a top-level childWorkflow reference", () => {
    const wf = defineWorkflow({
      id: "parent",
      trigger: { type: "manual" },
      steps: {
        a: childWorkflow({ definitionRef: "ast_a" }),
        b: childWorkflow({ definitionRef: "ast_b", after: ["a"] }),
      },
    });
    expect(enumerateChildWorkflowRefs(wf)).toEqual([
      { stepId: "a", definitionRef: "ast_a" },
      { stepId: "b", definitionRef: "ast_b" },
    ]);
  });

  test("descends into an onTrigger inline body and labels the step path", () => {
    const address = "wf@example.com";
    const body = defineWorkflow({
      id: "body",
      trigger: { type: "manual" },
      steps: { sub: childWorkflow({ definitionRef: "ast_nested" }) },
    });
    const wf = defineWorkflow({
      id: "parent",
      trigger: { type: "mail", to: address },
      steps: {
        section: onTrigger({ on: { type: "mail", to: address }, body }),
      },
    });
    expect(enumerateChildWorkflowRefs(wf)).toEqual([
      { stepId: "section/sub", definitionRef: "ast_nested" },
    ]);
  });

  test("skips an onTrigger body already extracted to a ref", () => {
    // A deployed `{ ref }` body is an independent asset with its own folded
    // surface. The authored form the fold runs on carries `{ inline }`, so this
    // shape only appears post-extraction; assert the enumerator skips it rather
    // than dereferencing a ref that is not a childWorkflow.
    const base = defineWorkflow({
      id: "parent",
      trigger: { type: "mail", to: "wf@example.com" },
      steps: {
        section: onTrigger({
          on: { type: "mail", to: "wf@example.com" },
          body: defineWorkflow({
            id: "body",
            trigger: { type: "manual" },
            steps: { sub: childWorkflow({ definitionRef: "ast_nested" }) },
          }),
        }),
      },
    });
    const section = base.steps["section"];
    if (section === undefined || section.kind !== "onTrigger") {
      throw new Error("test setup: expected an onTrigger section");
    }
    const extracted: WorkflowDefinition = {
      ...base,
      steps: { section: { ...section, body: { ref: "ast_body" } } },
    };
    expect(enumerateChildWorkflowRefs(extracted)).toEqual([]);
  });
});

describe("mergeGrantSurfaces", () => {
  test("unions grants and sorts them deterministically", () => {
    const a: ApprovedGrantSurface = {
      grants: ["tool:z", "capability:mail"],
      grantEffects: {},
    };
    const b: ApprovedGrantSurface = {
      grants: ["tool:a", "capability:mail"],
      grantEffects: {},
    };
    expect(mergeGrantSurfaces(a, b)).toEqual({
      grants: ["capability:mail", "tool:a", "tool:z"],
      grantEffects: {},
    });
  });

  test("ask wins over allow regardless of side", () => {
    const asking: ApprovedGrantSurface = {
      grants: ["tool:x"],
      grantEffects: { "tool:x": "ask" },
    };
    const allowing: ApprovedGrantSurface = {
      grants: ["tool:x"],
      grantEffects: { "tool:x": "allow" },
    };
    expect(mergeGrantSurfaces(allowing, asking).grantEffects).toEqual({
      "tool:x": "ask",
    });
    expect(mergeGrantSurfaces(asking, allowing).grantEffects).toEqual({
      "tool:x": "ask",
    });
  });

  test("deny wins over ask and allow regardless of side", () => {
    const denying: ApprovedGrantSurface = {
      grants: ["tool:x", "tool:y"],
      grantEffects: { "tool:x": "deny", "tool:y": "deny" },
    };
    const softer: ApprovedGrantSurface = {
      grants: ["tool:x", "tool:y"],
      grantEffects: { "tool:x": "ask", "tool:y": "allow" },
    };
    expect(mergeGrantSurfaces(softer, denying).grantEffects).toEqual({
      "tool:x": "deny",
      "tool:y": "deny",
    });
    expect(mergeGrantSurfaces(denying, softer).grantEffects).toEqual({
      "tool:x": "deny",
      "tool:y": "deny",
    });
  });

  test("keeps disjoint effects and the incoming effect on no conflict", () => {
    const base: ApprovedGrantSurface = {
      grants: ["tool:x"],
      grantEffects: { "tool:x": "allow" },
    };
    const incoming: ApprovedGrantSurface = {
      grants: ["tool:y"],
      grantEffects: { "tool:y": "allow" },
    };
    expect(mergeGrantSurfaces(base, incoming)).toEqual({
      grants: ["tool:x", "tool:y"],
      grantEffects: { "tool:x": "allow", "tool:y": "allow" },
    });
  });

  test("an empty base is the identity for a fold accumulator", () => {
    const empty: ApprovedGrantSurface = { grants: [], grantEffects: {} };
    const surface: ApprovedGrantSurface = {
      grants: ["tool:a"],
      grantEffects: { "tool:a": "ask" },
    };
    expect(mergeGrantSurfaces(empty, surface)).toEqual(surface);
  });
});

describe("child workflow.json hash equivalence (fold precondition)", () => {
  // The fold resolves a child by hashing its workflow.json, which holds the
  // inert projection. These pin the invariant the fold's hash choice rests on,
  // unconditionally (no DB harness needed): the DB-backed resolution test that
  // also exercises this is gated behind the Postgres harness.
  test("computeWireDefinitionHash of the inert workflow.json equals the stored live hash", async () => {
    const live = agentWorkflow();
    const workflowJson: unknown = JSON.parse(
      JSON.stringify(projectLiveToInert(live)),
    );
    expect(await computeWireDefinitionHash(workflowJson)).toBe(
      await computeLiveDefinitionHash(live),
    );
  });

  test("re-projecting the inert workflow.json throws (the fold must not do this)", async () => {
    const live = agentWorkflow();
    const workflowJson = JSON.parse(JSON.stringify(projectLiveToInert(live)));
    // computeLiveDefinitionHash re-projects its argument; an already-inert
    // agent step is no longer a live factory, so this rejects.
    await expect(computeLiveDefinitionHash(workflowJson)).rejects.toThrow();
  });
});
