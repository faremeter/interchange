// Source entries for the onTrigger correlation deploy proofs.
//
// `onTriggerBodyEntry` stays a single section because the sleep, drain, and
// agent-body deploys use that shape. These builders add the two shapes that
// entry cannot express: two sections on one author name, and a top-level
// `awaitSignal` beside one section on that same name.
//
// The gate-beside workflow carries a successor `next` gate. The first gate's
// completion stays in the runtime buffer while the section is still parked,
// and `next` flushes that buffer when it parks. A test observes `next`, not
// the first gate's `StepCompleted`.

export type OnTriggerTwoSectionsParams = {
  /** The mail address both the workflow trigger and the sections subscribe to. */
  address: string;
  /** The author signal name both section bodies await. */
  signalName: string;
  /** The first section's step id. Defaults to `left`. */
  leftId?: string;
  /** The second section's step id. Defaults to `right`. */
  rightId?: string;
  /** The `defineWorkflow` id. Defaults to a stable fixture-local id. */
  workflowId?: string;
};

export function onTriggerTwoSectionsEntry(
  params: OnTriggerTwoSectionsParams,
): string {
  const leftId = params.leftId ?? "left";
  const rightId = params.rightId ?? "right";
  const workflowId = params.workflowId ?? "wf_on_trigger_two_sections";
  return `
import { awaitSignal, defineWorkflow, onTrigger } from "@intx/workflow/definition";

const leftBody = defineWorkflow({
  id: ${JSON.stringify(`${workflowId}_left`)},
  trigger: { type: "manual" },
  steps: {
    gate: awaitSignal({ name: ${JSON.stringify(params.signalName)} }),
  },
});

const rightBody = defineWorkflow({
  id: ${JSON.stringify(`${workflowId}_right`)},
  trigger: { type: "manual" },
  steps: {
    gate: awaitSignal({ name: ${JSON.stringify(params.signalName)} }),
  },
});

export const workflow = defineWorkflow({
  id: ${JSON.stringify(workflowId)},
  trigger: { type: "mail", to: ${JSON.stringify(params.address)} },
  steps: {
    [${JSON.stringify(leftId)}]: onTrigger({
      on: { type: "mail", to: ${JSON.stringify(params.address)} },
      body: leftBody,
    }),
    [${JSON.stringify(rightId)}]: onTrigger({
      on: { type: "mail", to: ${JSON.stringify(params.address)} },
      body: rightBody,
    }),
  },
});
`;
}

export type OnTriggerGateBesideSectionParams = {
  /** The mail address the workflow trigger and the section subscribe to. */
  address: string;
  /** The author signal name shared by the top-level gate and the section body. */
  signalName: string;
  /** The section step id. Defaults to `section`. */
  sectionId?: string;
  /** The top-level gate step id. Defaults to `gate`. */
  gateId?: string;
  /** The successor gate that flushes the first gate's completion. Defaults to `next`. */
  nextId?: string;
  /** The `defineWorkflow` id. Defaults to a stable fixture-local id. */
  workflowId?: string;
};

export function onTriggerGateBesideSectionEntry(
  params: OnTriggerGateBesideSectionParams,
): string {
  const sectionId = params.sectionId ?? "section";
  const gateId = params.gateId ?? "gate";
  const nextId = params.nextId ?? "next";
  const workflowId = params.workflowId ?? "wf_on_trigger_gate_beside";
  return `
import { awaitSignal, defineWorkflow, onTrigger } from "@intx/workflow/definition";

const body = defineWorkflow({
  id: ${JSON.stringify(`${workflowId}_body`)},
  trigger: { type: "manual" },
  steps: {
    gate: awaitSignal({ name: ${JSON.stringify(params.signalName)} }),
  },
});

export const workflow = defineWorkflow({
  id: ${JSON.stringify(workflowId)},
  trigger: { type: "mail", to: ${JSON.stringify(params.address)} },
  steps: {
    [${JSON.stringify(gateId)}]: awaitSignal({ name: ${JSON.stringify(params.signalName)} }),
    [${JSON.stringify(nextId)}]: awaitSignal({
      name: ${JSON.stringify(nextId)},
      after: [${JSON.stringify(gateId)}],
    }),
    [${JSON.stringify(sectionId)}]: onTrigger({
      on: { type: "mail", to: ${JSON.stringify(params.address)} },
      body,
    }),
  },
});
`;
}
