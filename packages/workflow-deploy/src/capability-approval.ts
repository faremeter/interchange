// Operator-approval gating for the deploy-time capability walk.
//
// The deploy flow hands the walk's `CapabilityWalkResult` to a gate; the
// gate compares the walk-derived per-step grants against an operator-
// supplied `ApprovalSet` and decides whether the deploy may proceed.
//
// Approval semantics (v1):
//   - The operator supplies one approved surface (`ApprovalSet`) holding the
//     grant-shape strings and the grant requirements they approved. Every
//     grant the walk surfaced on every step must appear in that surface's
//     `grants`; any miss is a per-step `pending` entry and fails the gate.
//   - A non-empty `unresolvedDirectors` field on the walk result is
//     itself a deploy-time failure; the gate surfaces it through
//     `ApprovalDecision` and the caller aborts the deploy.

import { isDeepStrictEqual } from "node:util";

import { type ApprovalItem, GrantRequirement } from "@intx/types";

import type { CapabilityWalkResult } from "./capability-walk";

/**
 * What the operator has approved for this deployment, in the form the gate
 * consults. The deploy flow's wiring synthesizes it from the deployment
 * context (admin UI cache, legacy grant-store mirror, scripted policy).
 *
 * The two fields hold the two kinds of approved item, each next to the
 * membership test that can actually answer for it:
 *
 *   - `grants` -- the grant-shape strings the capability walk surfaces.
 *     Answered by `Set.has`.
 *   - `requirements` -- the grant requirements a definition declares. A
 *     record has no useful identity, so membership is a structural
 *     comparison over the list (`isApprovedGrantRequirement`).
 *
 * This is ONE approved surface behind ONE operator decision and ONE gate.
 * The split is in the payload, not in the number of approvals: a caller
 * supplies both fields together and the gate consults both.
 *
 * Order does not matter in either field; membership is the only thing the
 * gate consults.
 */
export type ApprovalSet = {
  readonly grants: ReadonlySet<string>;
  readonly requirements: readonly GrantRequirement[];
};

/**
 * Build an `ApprovalSet` from what the operator approved. This is the edge of
 * the approval vocabulary: every requirement is validated here, so the gate
 * and `isApprovedGrantRequirement` can trust the records they compare.
 *
 * Validating here is what makes the structural comparison correct.
 * `isDeepStrictEqual` treats a present-but-undefined optional key as different
 * from an absent one, so an approval carrying `conditions: undefined` would
 * silently fail to match a declared requirement that omits the key. Parsing
 * rejects that record outright -- `GrantRequirement` admits an object or
 * `null` for `conditions`, never `undefined` -- so the malformed approval
 * fails loudly at construction instead of becoming an unexplained
 * `grant_requirements_not_approved` at deploy.
 *
 * `requirements` is typed as `unknown` entries on purpose: a caller handing
 * over an already-typed record loses nothing, and a caller handing over
 * rehydrated or hand-assembled data cannot skip the parse.
 */
export function createApprovalSet(
  grants: Iterable<string>,
  requirements: Iterable<unknown> = [],
): ApprovalSet {
  return {
    grants: new Set(grants),
    requirements: [...requirements].map((requirement) =>
      GrantRequirement.assert(requirement),
    ),
  };
}

/**
 * Rehydrate the flat `ApprovalItem` list a freeze persisted into the
 * `ApprovalSet` the gate and the source-pinning pass consult, partitioning the
 * items by kind.
 *
 * The persisted form stays flat and the in-memory form does not, because the
 * two owe different things. A flat array is what a stored row already holds,
 * and it is append-friendly and order-independent, so it is the shape a wire
 * format should keep. The struct is what the code reading it needs, because
 * the two kinds are tested by two different operations. This function is the
 * one place the two forms meet, and it routes the requirement half through
 * `createApprovalSet` so the parse below applies to rehydrated records too.
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
 * persists. The inverse of `approvalSetFromItems` up to order: the round trip
 * preserves every item, and the partition is by kind, so re-splitting a
 * flattened set yields the same two groups.
 */
export function approvalItemsFromSet(
  approvals: ApprovalSet,
): readonly ApprovalItem[] {
  return [...approvals.grants, ...approvals.requirements];
}

/**
 * Whether the operator approved this declared grant requirement.
 *
 * A requirement is compared as a WHOLE RECORD, not by resource alone:
 * `source` decides whose authority the run path delegates, `effect` decides
 * what the materialized row permits, and `conditions` narrows when it
 * applies, so a change to any of them changes the authority the definition
 * mints. Anything the operator did not approve exactly is unapproved, which
 * is the fail-closed direction.
 *
 * The comparison is structural over the validated record rather than over a
 * canonical string form: `conditions` is an open `Record<string, unknown>`,
 * so no single-axis string can carry a requirement without losing part of it.
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
 * Source the approval gate consults. Kept as an indirection so a future
 * implementation can defer the approval-set materialization until the
 * gate actually runs (e.g. a remote operator UI fetch).
 */
export interface ApprovalSource {
  approvedSurface(): Promise<ApprovalSet>;
}

/**
 * The decision the approval gate hands back to the caller.
 *
 * `ok: true` -- every grant the walk surfaced is approved and the
 * caller may continue with deploy.
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
 * The approval gate the deploy flow calls. The single method consumes
 * the walk output and yields a decision.
 */
export interface CapabilityApprovalGate {
  evaluate(walk: CapabilityWalkResult): Promise<ApprovalDecision>;
}

/**
 * Build a gate that decides against a fixed `ApprovalSet`. Suitable for
 * the operator-supplied case and for tests.
 *
 * The gate computes the per-step delta deterministically:
 *
 *   - Walk every step in the walk's `perStep` map (input order
 *     preserved).
 *   - For each step, list grants the walk surfaced that are not in the
 *     approved grant strings, preserving the walk's order to keep the
 *     operator-facing pending list stable.
 *   - Empty per-step deltas are omitted from the result map so the
 *     operator-facing pending output names only steps with something to
 *     approve.
 *   - `unresolvedDirectors` is mirrored verbatim. A non-empty value
 *     forces `ok: false` regardless of whether every per-step grant
 *     happens to be approved -- the caller must not let a deploy
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
 * Build a gate that consults an `ApprovalSource` on every call. Useful
 * when the approved-surface materialization is async (e.g. a remote operator
 * UI fetch) or per-call dynamic.
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
