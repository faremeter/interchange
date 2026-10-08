// The deployment address model is a pure function of `(runId, stepId,
// domain)`: a one-step workflow has no distinct step address -- the lone
// step IS the deployment head (`deriveRunAddress`) -- while a workflow with
// more than one step derives per-step addresses of the form
// `<runId>-<stepId>@<domain>`. Because the derivation is pure, the
// supervisor reconstructs the same addresses at spawn time without any
// per-deploy state, and `resolveStepAddress` is the single owner of the
// head/step collapse decision for a consumer that must choose an address
// from the host-sourced step count alone.

import { formatRunAddress } from "@intx/types";

/**
 * Pure function: derive a step's run address from
 * `(runId, stepId, domain)`. Exported so the supervisor can reconstruct
 * the same addresses at spawn time without sharing storage with the
 * deploy flow.
 *
 * The local part IS the run id with the step suffix appended; the runId is
 * already a minted `run_<hex>` carrying the `run_` marker `parseRunAddress`
 * requires at the substrate boundary. The per-step local-part is concat-only
 * because `stepId` is already constrained to `[a-zA-Z0-9_-]+` by the workflow
 * definition validator.
 */
export function deriveStepAddress(args: {
  runId: string;
  stepId: string;
  domain: string;
}): string {
  return formatRunAddress(`${args.runId}-${args.stepId}`, args.domain);
}

/**
 * Derive the per-step agent id (the `agent-state` repo's id and the
 * `HarnessConfig.agentId`). Pure function of `(runId, stepId)`.
 */
export function deriveStepAgentId(args: {
  runId: string;
  stepId: string;
}): string {
  return `${args.runId}-${args.stepId}`;
}

/**
 * Derive the deployment-level mail address the supervisor registers on
 * the bus. It is the run id `@` the domain; pure function of `(runId, domain)`.
 *
 * The supervisor uses this address as the inbound mail address for the
 * deployment as a whole; per-step bindings carry their own
 * derived-step addresses.
 */
export function deriveRunAddress(args: {
  runId: string;
  domain: string;
}): string {
  return formatRunAddress(args.runId, args.domain);
}

/**
 * Resolve where a step's deploy tree lives, given the deployment's step
 * count. This is the single owner of the head/step collapse DECISION for
 * a consumer that must choose the address without knowing the deploy
 * shape: a one-step workflow has no distinct steps, so its lone step IS
 * the head (`deriveRunAddress`); a multi-step deployment keeps the
 * head distinct from its per-step addresses (`deriveStepAddress`). The
 * sidecar child reads its deploy tree from the address this returns,
 * keyed only off the deployment mailbox and the host-sourced `stepCount`.
 *
 * The producers do not route through here -- each handles one shape
 * unconditionally: the single-step deploy stages the tree at the head,
 * the multi-step deploy at each per-step address. Because `stepCount` is
 * the deployed definition's `stepOrder.length`, sourced from the host,
 * the consumer's collapse always agrees with whichever producer staged
 * the tree; the two processes never derive divergent addresses.
 */
export function resolveStepAddress(args: {
  runId: string;
  stepId: string;
  domain: string;
  stepCount: number;
}): string {
  return args.stepCount === 1
    ? deriveRunAddress({
        runId: args.runId,
        domain: args.domain,
      })
    : deriveStepAddress(args);
}

/**
 * Derive the deployment-level agent id used on the `agent.deploy`
 * frame's `agentId` field. Pure function of `(runId)`.
 */
export function deriveRunAgentId(args: { runId: string }): string {
  return args.runId;
}

/**
 * Project a workflow-deployment run address into the substrate-safe
 * id of its workflow-run repo (`{ kind: "workflow-run", id }`). Pure
 * function of the deployment's run address.
 *
 * The workflow-run repo's `repoId.id` must match `SAFE_REPO_ID`
 * (`/^[a-zA-Z0-9_-]+$/`, the substrate's repo-path-safety contract in
 * `packages/hub-sessions/src/repo-store/types.ts`), and the supervisor
 * principal's `runId` must equal `workflowRunRepoId.id` for the
 * workflow-run kind handler's authz check to pass. That regex rejects
 * `@` and `.`, both of which appear in every run address, so the
 * address is sanitized by substituting every disallowed character with
 * `-`.
 *
 * The mapping is lossy (two distinct addresses can collapse to the same
 * slug) but deterministic. The sidecar's deploy router keys the
 * workflow-run repo by this slug at write time; the hub's read routes
 * reconstruct the deployment address via `deriveRunAddress` and
 * apply this same derivation so read and write address the same repo.
 * A collision implies two deployments are claiming the same workflow-run
 * surface, which the sidecar's deploy router rejects at deploy time.
 */
export function deriveWorkflowRunRepoId(agentAddress: string): string {
  return agentAddress.replaceAll(/[^a-zA-Z0-9_-]/g, "-");
}
