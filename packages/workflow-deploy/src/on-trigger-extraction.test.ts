// The deploy step materializes each onTrigger section's authored inline body
// into its own workflow asset and rewrites the primitive to a ref, so the
// runtime spawns the body as a child run resolved by ref (the childWorkflow
// production path). This exercises the extraction transform in isolation with
// a mock workflow-asset writer.

import { describe, test, expect } from "bun:test";

import { createDefaultDirectorRegistry, defineAgent } from "@intx/agent";

import { defineWorkflow, onTrigger, step } from "@intx/workflow/definition";

import {
  extractOnTriggerBodies,
  type WorkflowRepoWriter,
} from "./orchestrator";

function makeAgent(id: string) {
  return defineAgent({
    id,
    systemPrompt: "s",
    tools: [],
    capabilities: [],
    inference: { sources: [{ provider: "anthropic", model: "m" }] },
  });
}

describe("extractOnTriggerBodies", () => {
  test("deploys a section body as its own asset and rewrites the primitive to a ref", async () => {
    const writes = new Map<string, ReadonlyMap<string, string>>();
    const writer: WorkflowRepoWriter = {
      writeWorkflowRepo: async ({ workflowRepoId, files }) => {
        writes.set(workflowRepoId, files);
      },
    };
    const body = defineWorkflow({
      id: "authored-body-id",
      trigger: { type: "manual" },
      steps: { work: step({ agent: makeAgent("a") }) },
    });
    const workflow = defineWorkflow({
      id: "wf",
      steps: {
        section: onTrigger({ on: { type: "mail", to: "s@x.example" }, body }),
      },
    });

    const deployed = await extractOnTriggerBodies({
      workflow,
      registry: createDefaultDirectorRegistry(),
      workflowRepo: writer,
    });

    // The body landed as its own workflow asset, keyed by the derived ref
    // (parent id + section step id), and its stored id is that ref so
    // spawnChild resolves it by ref.
    const bodyRef = "wf__section";
    expect([...writes.keys()]).toEqual([bodyRef]);
    const bodyJson = writes.get(bodyRef)?.get("workflow.json");
    expect(bodyJson).toBeDefined();
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test parses its own JSON fixture
    const parsed = JSON.parse(bodyJson ?? "{}") as { id: string };
    expect(parsed.id).toBe(bodyRef);

    // The section primitive now references the asset instead of inlining it.
    const section = deployed.steps.section;
    expect(section?.kind).toBe("onTrigger");
    if (section?.kind === "onTrigger") {
      expect(section.body).toEqual({ ref: bodyRef });
    }
  });

  test("leaves a workflow with no onTrigger section unchanged", async () => {
    const writer: WorkflowRepoWriter = {
      writeWorkflowRepo: async () => {
        throw new Error("no onTrigger body: should not write an asset");
      },
    };
    const workflow = defineWorkflow({
      id: "wf-plain",
      trigger: { type: "manual" },
      steps: { s: step({ agent: makeAgent("a") }) },
    });

    const deployed = await extractOnTriggerBodies({
      workflow,
      registry: createDefaultDirectorRegistry(),
      workflowRepo: writer,
    });

    expect(deployed).toBe(workflow);
  });
});
