// Operator-approval gating for the deploy-time capability walk.
//
// This module is a skeleton. The walk's output (`CapabilityWalkResult`)
// is the input every approval implementation consumes; the concrete
// approval source (operator-side admin UI, pre-populated set
// reconciled against the agent-deploy's existing per-agent grant
// configuration, scripted policy file) is wired by the orchestrator
// the next commit ships. The shape here is what the orchestrator's
// gate calls into; downstream commits replace the
// `notYetImplemented` body with the real approval source.
//
// Approval semantics (v1):
//   - The orchestrator passes the full `CapabilityWalkResult` plus the
//     approver's already-approved grant set; the gate decides whether
//     to proceed by computing the per-step "needs approval" delta.
//   - A non-empty `unresolvedDirectors` field on the walk result is
//     itself a deploy-time failure; the gate surfaces it as
//     `"unresolvable director"` and the orchestrator aborts.
//   - The trivial-workflow uniformity bridge: when the deployment is
//     migrating from the agent-deploy surface, the approval source is
//     pre-populated from the existing per-agent grant store so the
//     operator does not see a fresh approval prompt for grants that
//     were already approved on the legacy path.

import type { CapabilityWalkResult } from "./capability-walk";

/**
 * Source the approval gate consults. The orchestrator wires this from
 * the deployment context (admin UI cache, legacy grant-store mirror,
 * scripted policy). Returns the set of grant-shape strings the
 * approver has already accepted for this deployment.
 */
export interface ApprovalSource {
  approvedGrants(): Promise<ReadonlySet<string>>;
}

/**
 * The decision the approval gate hands back to the orchestrator.
 *
 * `ok: true` -- every grant the walk surfaced is approved and the
 * orchestrator may continue with deploy.
 * `ok: false` -- one or more grants are missing approval or the walk
 * surfaced unresolvable directors. `pending` carries the
 * per-step delta the operator must approve; `unresolvedDirectors`
 * mirrors the walk's field so the gate's caller does not need to
 * inspect both shapes.
 */
export type ApprovalDecision =
  | { readonly ok: true }
  | {
      readonly ok: false;
      readonly pending: ReadonlyMap<string, readonly string[]>;
      readonly unresolvedDirectors: readonly string[];
    };

/**
 * The approval gate the orchestrator calls. Implementations land in
 * the next commit alongside orchestrator wiring; this signature is
 * the stable contract.
 */
export interface CapabilityApprovalGate {
  evaluate(walk: CapabilityWalkResult): Promise<ApprovalDecision>;
}

/**
 * Stub gate. Throws so an orchestrator that wires it before the
 * concrete implementation lands gets a loud, structured failure
 * rather than a silent green-light. The throw is the correct shape
 * for a not-yet-wired deploy path: the orchestrator must refuse to
 * apply a deploy until the approval source is real.
 */
export function createNotYetImplementedApprovalGate(): CapabilityApprovalGate {
  return {
    evaluate(_walk) {
      return Promise.reject(
        new Error(
          "capability-approval gate is not wired yet; the orchestrator " +
            "must supply a concrete ApprovalSource implementation",
        ),
      );
    },
  };
}
