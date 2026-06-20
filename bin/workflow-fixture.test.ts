import { describe, test, expect } from "bun:test";

import { type } from "arktype";
import { hashDefinition } from "@intx/workflow";
import { workflowDefinitionEnvelopeSchema } from "@intx/hub-sessions";

import {
  buildWorkflowFixture,
  buildWorkflowJson,
  WORKFLOW_FIXTURE_SIGNAL_NAME,
  WORKFLOW_RUN_GRANT_ACTION,
  WORKFLOW_RUN_GRANT_RESOURCE,
} from "./workflow-fixture";

describe("workflow fixture", () => {
  test("defineWorkflow yields the draft -> approve -> publish DAG", () => {
    const def = buildWorkflowFixture();
    expect(def.stepOrder).toEqual(["draft", "approval", "publish"]);

    const draft = def.steps["draft"];
    const approval = def.steps["approval"];
    const publish = def.steps["publish"];

    expect(draft?.kind).toBe("step");
    expect(approval?.kind).toBe("awaitSignal");
    expect(publish?.kind).toBe("step");

    if (approval?.kind !== "awaitSignal") throw new Error("unreachable");
    expect(approval.name).toBe(WORKFLOW_FIXTURE_SIGNAL_NAME);
    expect(approval.after).toEqual(["draft"]);
    expect(publish?.after).toEqual(["approval"]);
  });

  test("step-agents are authored inline with no catalog reference", () => {
    const def = buildWorkflowFixture();
    const draft = def.steps["draft"];
    const publish = def.steps["publish"];
    if (draft?.kind !== "step" || publish?.kind !== "step") {
      throw new Error("unreachable");
    }
    expect(draft.agent.id).toBe("draft-agent");
    expect(draft.agent.systemPrompt.length).toBeGreaterThan(0);
    expect(draft.agent.inference.sources[0]?.provider).toBe("anthropic");
    expect(publish.agent.id).toBe("publish-agent");
    expect(publish.agent.systemPrompt.length).toBeGreaterThan(0);
  });

  test("hashDefinition produces a deterministic content hash", () => {
    const a = hashDefinition(buildWorkflowFixture());
    const b = hashDefinition(buildWorkflowFixture());
    expect(a.length).toBeGreaterThan(0);
    expect(a).toEqual(b);
  });

  test("serialized workflow.json passes the push-time envelope schema", () => {
    const parsed: unknown = JSON.parse(buildWorkflowJson());
    const validated = workflowDefinitionEnvelopeSchema(parsed);
    expect(validated instanceof type.errors).toBe(false);
  });

  test("serialized workflow.json round-trips through defineWorkflow's shape", () => {
    const original = buildWorkflowFixture();
    const parsed: unknown = JSON.parse(buildWorkflowJson());
    expect(parsed).toEqual(JSON.parse(JSON.stringify(original)));
  });

  test("the planted signal grant matches the route's resource gate", () => {
    // The signal route resolves `idResource("workflow-run","deploymentId")`
    // to `workflow-run:<deploymentId>`; the planted wildcard resource must
    // glob-match any concrete deployment id, and the verb must be `manage`.
    expect(WORKFLOW_RUN_GRANT_RESOURCE.endsWith(":*")).toBe(true);
    expect(WORKFLOW_RUN_GRANT_RESOURCE.startsWith("workflow-run:")).toBe(true);
    expect(WORKFLOW_RUN_GRANT_ACTION).toBe("manage");
  });
});
