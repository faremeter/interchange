// Seedable human-in-the-loop workflow fixture.
//
// Authors a three-node `WorkflowDefinition` --
// `draft -> awaitSignal{name:"approve"} -> publish` -- with both
// step-agents authored inline (system prompts and inference preferences
// live on the definition; no foreign key to any seeded agent-catalog
// row). `bin/seed.ts` serializes `workflowJson` into the `workflow.json`
// envelope of a `workflow`-kind asset, and `WORKFLOW_FIXTURE_ASSET_NAME`
// names that asset on the Acme tenant.
//
// The definition is the launch fixture the convergence work is measured
// against: deploying it fires a run that drafts, pauses at the
// `approve` signal, and (once the signal is delivered) publishes. The
// signal-delivery route gates on the `workflow-run:<deploymentId>`
// resource with the `manage` verb; the deployment id is minted at
// deploy time, so the seed plants the grant at the
// `WORKFLOW_RUN_GRANT_RESOURCE` wildcard scope that the authz glob
// matcher resolves against any concrete deployment's resource string.

import {
  awaitSignal,
  defineWorkflow,
  step,
  type WorkflowDefinition,
} from "@intx/workflow";
import { defineAgent } from "@intx/agent";

/**
 * Asset name for the seeded workflow definition. Lowercase-kebab so the
 * smart-HTTP repo path (`assets/workflow/<name>.git`) needs no escaping.
 */
export const WORKFLOW_FIXTURE_ASSET_NAME = "approval-flow";

/**
 * Signal the middle node waits on. The end-to-end launch delivers this
 * signal name to resume the run through the publish step.
 */
export const WORKFLOW_FIXTURE_SIGNAL_NAME = "approve";

/**
 * Inference provider/model the inline step-agents prefer. Matches the
 * `Anthropic` provider the seed wires (with a tenant credential) so a
 * deployed step resolves against a real seeded source.
 */
export const WORKFLOW_FIXTURE_INFERENCE_PROVIDER = "anthropic";
export const WORKFLOW_FIXTURE_INFERENCE_MODEL = "claude-sonnet-4-6";

/**
 * Resource pattern the seed plants (with the `manage` verb) so the
 * seeded operator can deliver the `approve` signal. The signal route
 * resolves the gate to `workflow-run:<deploymentId>`; the deployment id
 * is minted at deploy time, so the planted grant uses the `*` wildcard
 * the authz glob matcher resolves against any concrete deployment.
 */
export const WORKFLOW_RUN_GRANT_RESOURCE = "workflow-run:*";
export const WORKFLOW_RUN_GRANT_ACTION = "manage";

/**
 * Authored trigger address. The capability walk derives `mail.address`
 * / `mail.send` approvals from this value; the runtime inbound address
 * is derived independently from the deployment id at deploy time, so
 * this only has to be a well-formed address whose domain matches the
 * Acme tenant domain (`<slug>.localhost`) for a coherent approval set.
 */
const WORKFLOW_FIXTURE_TRIGGER_ADDRESS = "workflow-launch@acme.localhost";

function inlineStepAgent(args: {
  id: string;
  systemPrompt: string;
}): ReturnType<typeof defineAgent> {
  return defineAgent({
    id: args.id,
    systemPrompt: args.systemPrompt,
    tools: [],
    capabilities: [],
    inference: {
      sources: [
        {
          provider: WORKFLOW_FIXTURE_INFERENCE_PROVIDER,
          model: WORKFLOW_FIXTURE_INFERENCE_MODEL,
        },
      ],
    },
  });
}

/**
 * Build the inline human-in-the-loop workflow definition. Returns a
 * fresh value on each call so callers cannot mutate shared state.
 */
export function buildWorkflowFixture(): WorkflowDefinition {
  const draftAgent = inlineStepAgent({
    id: "draft-agent",
    systemPrompt:
      "You are the drafting step of an approval workflow. Produce a concise draft of the requested deliverable so a human reviewer can approve or reject it.",
  });
  const publishAgent = inlineStepAgent({
    id: "publish-agent",
    systemPrompt:
      "You are the publishing step of an approval workflow. The draft has been approved by a human; finalize and publish it, then report what was published.",
  });

  return defineWorkflow({
    id: "wf_approval_flow",
    trigger: { type: "mail", to: WORKFLOW_FIXTURE_TRIGGER_ADDRESS },
    steps: {
      draft: step({ agent: draftAgent }),
      approval: awaitSignal({
        name: WORKFLOW_FIXTURE_SIGNAL_NAME,
        after: ["draft"],
      }),
      publish: step({ agent: publishAgent, after: ["approval"] }),
    },
  });
}

/**
 * Serialize the fixture into the bytes pushed to the workflow asset's
 * `workflow.json`. Two-space indentation keeps the on-disk envelope
 * human-readable in the asset repo.
 */
export function buildWorkflowJson(): string {
  return JSON.stringify(buildWorkflowFixture(), null, 2);
}
