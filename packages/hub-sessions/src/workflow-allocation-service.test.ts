import { describe, expect, test } from "bun:test";

import { defineAgent } from "@intx/agent";
import { defineWorkflow, onTrigger, step } from "@intx/workflow/definition";

import { resolveDeclaredWorkflowSidecarPlacement } from "./workflow-allocation-service";

const agent = defineAgent({
  id: "worker",
  systemPrompt: "Do the work",
  tools: [],
  capabilities: [],
  inference: { sources: [{ provider: "test", model: "test" }] },
});

describe("resolveDeclaredWorkflowSidecarPlacement", () => {
  test("strengthens placement from an inline workflow body", () => {
    const body = defineWorkflow({
      id: "body",
      sidecarPlacement: { sharing: "exclusive", reuse: "same-deployment" },
      steps: { work: step({ agent }) },
    });
    const definition = defineWorkflow({
      id: "root",
      steps: {
        section: onTrigger({ on: { type: "manual" }, body }),
      },
    });

    expect(resolveDeclaredWorkflowSidecarPlacement(definition)).toEqual({
      sharing: "exclusive",
      reuse: "same-deployment",
    });
  });

  test("uses the strictest reuse policy in the definition tree", () => {
    const body = defineWorkflow({
      id: "body",
      sidecarPlacement: { sharing: "exclusive" },
      steps: { work: step({ agent }) },
    });
    const definition = defineWorkflow({
      id: "root",
      sidecarPlacement: { sharing: "exclusive", reuse: "same-deployment" },
      steps: {
        section: onTrigger({ on: { type: "manual" }, body }),
      },
    });

    expect(resolveDeclaredWorkflowSidecarPlacement(definition)).toEqual({
      sharing: "exclusive",
      reuse: "never",
    });
  });
});
