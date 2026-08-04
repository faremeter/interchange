// @intx/workflow-deploy/testing -- test-only fold synthesis.
//
// `synthesizeFoldedWorkflow` turns a legacy agent's resolved fields into a
// single-step `WorkflowDefinition` (the `workflow.json` a folded agent is
// stored as). Production no longer folds new agents: the one live fold path
// reads a folded definition back with `extractFoldedBody`, it never
// synthesizes one, so this builder exists only to construct folded definitions
// in tests. It is PURE: every impure input (the inference preferences resolved
// against the tenant catalog, the mail trigger address) is supplied by the
// caller, not computed here.
//
// Tools ride as `toolPackagePins` on the step agent (pins-only: the sidecar
// materializes them and its tool floor authorizes them at runtime, exactly as
// an agent instance does). The step agent's `toolFactories` is the empty array
// by construction -- a folded definition has no live factories, and the
// capability walk falls back to the (empty) factories to emit no tool grants.

import type { CredentialBinding, GrantRequirement } from "@intx/types";
import type { ToolPackagePin } from "@intx/types/tool-packages";
import type { InferencePreference } from "@intx/agent";
import {
  defineWorkflow,
  type WorkflowDefinition,
} from "@intx/workflow/definition";

import { buildSingleStepAgentDefinition } from "../orchestrator";

export interface FoldedWorkflowInput {
  /** The workflow definition id (e.g. `wf_<agentId>`). */
  readonly workflowId: string;
  /**
   * The mail trigger address. Supplied, never invented: routing keys on the
   * deployment address, but this is the audit/grant record's mail identity and
   * must match the deployment address the agent is folded onto.
   */
  readonly mailAddress: string;
  /** The agent's system prompt. Null makes the agent undeployable -- fail loud. */
  readonly systemPrompt: string | null;
  /** Carried onto the step agent when present. */
  readonly description: string | null;
  /**
   * The credential-free `{ provider, model }` inference preferences resolved
   * from the agent's model requirements against the tenant catalog.
   */
  readonly inferencePreferences: readonly InferencePreference[];
  /** The agent's tool package pins, carried through as-is (pins-only). */
  readonly toolPackagePins: readonly ToolPackagePin[];
  /** The agent's grant requirements, carried onto the workflow envelope. */
  readonly grantRequirements?: readonly GrantRequirement[];
  /** The agent's credential bindings, carried onto the workflow envelope. */
  readonly credentialBindings?: readonly CredentialBinding[];
}

/**
 * Synthesize a single-step `WorkflowDefinition` from a legacy agent's resolved
 * fields. Raises if the agent has no system prompt (an empty-prompt agent is
 * undeployable; synthesizing one would bury a broken definition rather than
 * surface the source data problem).
 */
export function synthesizeFoldedWorkflow(
  input: FoldedWorkflowInput,
): WorkflowDefinition {
  if (input.systemPrompt === null) {
    throw new Error(
      `cannot synthesize a folded workflow for ${input.workflowId}: the agent has no system prompt`,
    );
  }

  const agent = buildSingleStepAgentDefinition({
    id: input.workflowId,
    systemPrompt: input.systemPrompt,
    inferencePreferences: input.inferencePreferences,
    // Pins-only: no live tool factories. The empty array is load-bearing -- a
    // folded workflow.json must carry `toolFactories: []` explicitly (a missing
    // key hydrates to undefined and the capability walk's factory loop throws).
    toolFactories: [],
    // Dormant on every deployed agent today; the instance wrap emits [] too.
    capabilities: [],
    ...(input.description !== null ? { description: input.description } : {}),
    toolPackagePins: input.toolPackagePins,
  });

  return defineWorkflow({
    id: input.workflowId,
    agent,
    trigger: { type: "mail", to: input.mailAddress },
    ...(input.grantRequirements !== undefined
      ? { grantRequirements: input.grantRequirements }
      : {}),
    ...(input.credentialBindings !== undefined
      ? { credentialBindings: input.credentialBindings }
      : {}),
  });
}
