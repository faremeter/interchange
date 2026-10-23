import { describe, expect, test } from "bun:test";

import { defineAgent } from "@intx/agent";
import { defineWorkflow, step } from "@intx/workflow";

import { createWorkflowHostDrainController } from "./drain-controller";

const definition = defineWorkflow({
  id: "drain-controller",
  trigger: { type: "manual" },
  steps: {
    only: step({
      agent: defineAgent({
        id: "only",
        systemPrompt: "s",
        tools: [],
        capabilities: [],
        inference: { sources: [{ provider: "anthropic", model: "m" }] },
      }),
    }),
  },
});

describe("createWorkflowHostDrainController hold", () => {
  test("is pending while held and resolves when lifted", async () => {
    const controller = createWorkflowHostDrainController({ definition });
    expect(controller.hold).toBeNull();

    controller.holdSteps();
    const hold = controller.hold;
    if (hold === null) throw new Error("hold is not in force");
    // A second hold keeps the one already in force.
    controller.holdSteps();
    expect(controller.hold).toBe(hold);

    controller.liftHold();
    await hold;
    expect(controller.hold).toBeNull();
    controller.liftHold();
    expect(controller.hold).toBeNull();
  });
});
