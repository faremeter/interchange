// Operator-approval gating for the deploy-time capability walk. The gate
// compares the walk's per-step grants against an operator-supplied
// `ApprovalSet`; every surfaced grant must be approved, any miss is a
// per-step `pending` entry, and a non-empty `unresolvedDirectors` fails the
// gate outright.

import { isDeepStrictEqual } from "node:util";

import { type ApprovalItem, GrantRequirement } from "@intx/types";

import type { CapabilityWalkResult } from "./capability-walk";

/**
 * What the operator has approved for this deployment. `grants` is answered
 * by `Set.has`; `requirements` (records with no useful identity) by a
 * structural comparison over the list. Order does not matter in either
 * field; membership is the only thing the gate consults.
 */
export type ApprovalSet = {
  /**
   * Discriminant against `ApproveProbedGrants` in `ProbeApprovalPolicy`: an
   * explicit literal keeps the choice a compiler-checked union instead of a
   * structural guess that could silently resolve to the approve-everything
   * arm.
   */
  readonly kind: "approval-set";
  readonly grants: ReadonlySet<string>;
  readonly requirements: readonly GrantRequirement[];
};

/**
 * Build an `ApprovalSet` from what the operator approved, validating every
 * requirement here so the gate can trust the records it compares.
 *
 * The validation is what makes the structural comparison correct:
 * `isDeepStrictEqual` treats a present-but-undefined optional key as different
 * from an absent one, and `GrantRequirement` admits an object or `null` for
 * `conditions` but never `undefined`, so a malformed approval fails loudly at
 * construction instead of becoming an unexplained
 * `grant_requirements_not_approved` at deploy. `requirements` is typed as
 * `unknown` so rehydrated or hand-assembled data cannot skip the parse.
 */
export function createApprovalSet(
  grants: Iterable<string>,
  requirements: Iterable<unknown> = [],
): ApprovalSet {
  return {
    kind: "approval-set",
    grants: new Set(grants),
    requirements: [...requirements].map((requirement) =>
      GrantRequirement.assert(requirement),
    ),
  };
}

/**
 * Rehydrate the flat `ApprovalItem` list a freeze persisted, partitioning the
 * items by kind. Routes the requirement half through `createApprovalSet` so
 * the parse applies to rehydrated records too.
 */
export function approvalSetFromItems(items: Iterable<unknown>): ApprovalSet {
  const grants: string[] = [];
  const requirements: unknown[] = [];
  for (const item of items) {
    if (typeof item === "string") {
      grants.push(item);
    } else {
      requirements.push(item);
    }
  }
  return createApprovalSet(grants, requirements);
}

/**
 * Flatten an `ApprovalSet` back into the `ApprovalItem` list a freeze
 * persists. The inverse of `approvalSetFromItems` up to order.
 */
export function approvalItemsFromSet(
  approvals: ApprovalSet,
): readonly ApprovalItem[] {
  return [...approvals.grants, ...approvals.requirements];
}

/**
 * Whether the operator approved this declared grant requirement. Compared as
 * a WHOLE RECORD, not by resource alone: `source`, `effect`, and `conditions`
 * all change the authority the definition mints, so anything not approved
 * exactly is unapproved (fail-closed). Structural over the validated record,
 * not a canonical string form, because `conditions` is an open
 * `Record<string, unknown>`.
 */
export function isApprovedGrantRequirement(
  approvals: ApprovalSet,
  requirement: GrantRequirement,
): boolean {
  return approvals.requirements.some((approved) =>
    isDeepStrictEqual(approved, requirement),
  );
}

/**
 * Source the approval gate consults. Indirection so a future implementation
 * can defer approval-set materialization until the gate runs.
 */
export interface ApprovalSource {
  approvedSurface(): Promise<ApprovalSet>;
}

/**
 * The decision the approval gate hands back. `ok: true` means every surfaced
 * grant is approved; `ok: false` carries the per-step `pending` delta and
 * mirrors the walk's `unresolvedDirectors` so the caller inspects one shape.
 */
export type ApprovalDecision =
  | { readonly ok: true }
  | {
      readonly ok: false;
      readonly pending: ReadonlyMap<string, readonly string[]>;
      readonly unresolvedDirectors: readonly string[];
    };

/** The approval gate the deploy flow calls. */
export interface CapabilityApprovalGate {
  evaluate(walk: CapabilityWalkResult): Promise<ApprovalDecision>;
}

/**
 * Build a gate that decides against a fixed `ApprovalSet`. Suitable for the
 * operator-supplied case and for tests. Missing grants are listed per step in
 * walk order; empty per-step deltas are omitted; a non-empty
 * `unresolvedDirectors` forces `ok: false` even when every grant is approved.
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
          if (!approvals.grants.has(grant)) {
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
 * Build a gate that consults an `ApprovalSource` on every call, for async or
 * per-call dynamic approved-surface materialization.
 */
export function createApprovalSourceGate(
  source: ApprovalSource,
): CapabilityApprovalGate {
  return {
    async evaluate(walk: CapabilityWalkResult): Promise<ApprovalDecision> {
      const approvals = await source.approvedSurface();
      return createApprovalSetGate(approvals).evaluate(walk);
    },
  };
}
