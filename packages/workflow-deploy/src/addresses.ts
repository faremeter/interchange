// The deployment address model is a pure function of `(runId, domain)`, so the
// supervisor reconstructs the same address at spawn time without any per-deploy
// state.

import { formatRunAddress } from "@intx/types/agent-address";

/**
 * Derive the deployment-level mail address the supervisor registers on
 * the bus. It is the run id `@` the domain; pure function of `(runId, domain)`.
 *
 * The supervisor uses this address as the inbound mail address for the
 * deployment as a whole.
 */
export function deriveRunAddress(args: {
  runId: string;
  domain: string;
}): string {
  return formatRunAddress(args.runId, args.domain);
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
