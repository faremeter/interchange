// Operator-approval gating for the deploy-time capability walk.
//
// The orchestrator hands the walk's `CapabilityWalkResult` to a gate; the
// gate compares the walk-derived per-step grants against an operator-
// supplied `ApprovalSet` and decides whether the deploy may proceed.
//
// Approval semantics (v1):
//   - The operator supplies a flat set of approved grant-shape strings
//     (`ApprovalSet`). Every grant the walk surfaced on every step must
//     appear in that set; any miss is a per-step `pending` entry and
//     fails the gate.
//   - A non-empty `unresolvedDirectors` field on the walk result is
//     itself a deploy-time failure; the gate surfaces it through
//     `ApprovalDecision` and the orchestrator aborts.

import type { CapabilityWalkResult } from "./capability-walk";

/**
 * A flat set of grant-shape strings the operator has approved for this
 * deployment. The orchestrator-side wiring synthesizes the set from the
 * deployment context (admin UI cache, legacy grant-store mirror,
 * scripted policy). Order does not matter; membership is the only thing
 * the gate consults.
 */
export type ApprovalSet = ReadonlySet<string>;

/**
 * Source the approval gate consults. Kept as an indirection so a future
 * implementation can defer the approval-set materialization until the
 * gate actually runs (e.g. a remote operator UI fetch).
 */
export interface ApprovalSource {
  approvedGrants(): Promise<ApprovalSet>;
}

/**
 * The decision the approval gate hands back to the orchestrator.
 *
 * `ok: true` -- every grant the walk surfaced is approved and the
 * orchestrator may continue with deploy.
 * `ok: false` -- one or more grants are missing approval or the walk
 * surfaced unresolvable directors. `pending` carries the per-step delta
 * the operator must approve; `unresolvedDirectors` mirrors the walk's
 * field so the gate's caller does not need to inspect both shapes.
 */
export type ApprovalDecision =
  | { readonly ok: true }
  | {
      readonly ok: false;
      readonly pending: ReadonlyMap<string, readonly string[]>;
      readonly unresolvedDirectors: readonly string[];
    };

/**
 * The approval gate the orchestrator calls. The single method consumes
 * the walk output and yields a decision.
 */
export interface CapabilityApprovalGate {
  evaluate(walk: CapabilityWalkResult): Promise<ApprovalDecision>;
}

/**
 * Build a gate that decides against a fixed `ApprovalSet`. Suitable for
 * the operator-supplied flat-set case and for tests.
 *
 * The gate computes the per-step delta deterministically:
 *
 *   - Walk every step in the walk's `perStep` map (input order
 *     preserved).
 *   - For each step, list grants the walk surfaced that are not in the
 *     approval set, preserving the walk's order to keep the operator-
 *     facing pending list stable.
 *   - Empty per-step deltas are omitted from the result map so the
 *     operator-facing pending output names only steps with something to
 *     approve.
 *   - `unresolvedDirectors` is mirrored verbatim. A non-empty value
 *     forces `ok: false` regardless of whether every per-step grant
 *     happens to be approved -- the orchestrator must not let a deploy
 *     proceed against a walk that could not resolve every director ref.
 */
export function createApprovalSetGate(
  approvals: ApprovalSet,
): CapabilityApprovalGate {
  return {
    async evaluate(walk: CapabilityWalkResult): Promise<ApprovalDecision> {
      const pending = new Map<string, readonly string[]>();
      for (const [stepId, declarations] of walk.perStep) {
        const missing: string[] = [];
        for (const grant of declarations.grants) {
          if (!approvals.has(grant)) {
            missing.push(grant);
          }
        }
        if (missing.length > 0) {
          pending.set(stepId, Object.freeze(missing));
        }
      }
      const unresolved = walk.unresolvedDirectors;
      if (pending.size === 0 && unresolved.length === 0) {
        return { ok: true };
      }
      return {
        ok: false,
        pending,
        unresolvedDirectors: unresolved,
      };
    },
  };
}

/**
 * Build a gate that consults an `ApprovalSource` on every call. Useful
 * when the approved-set materialization is async (e.g. a remote operator
 * UI fetch) or per-call dynamic.
 */
export function createApprovalSourceGate(
  source: ApprovalSource,
): CapabilityApprovalGate {
  return {
    async evaluate(walk: CapabilityWalkResult): Promise<ApprovalDecision> {
      const approvals = await source.approvedGrants();
      return createApprovalSetGate(approvals).evaluate(walk);
    },
  };
}
